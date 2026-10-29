//! `atlatl run`: plan → engine → executor → report (01 §6).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use atlatl_core::config::connection::Connection;
use atlatl_core::config::load::Project;
use atlatl_core::config::model::ConnectionKind;
use atlatl_core::error::{AtlatlError, ErrorClass, Result};
use atlatl_core::exec::{
    ExecOptions, RepResult, RepStatus, ReplicationRunner, RunnerOutput, execute,
};
use atlatl_core::lock::BoxFuture;
use atlatl_core::obs::events::{
    EventBody, EventSink, ReplicationStarted, RunCompleted, RunStarted,
};
use atlatl_core::obs::report::{KillReason, ReportError, ReportHandle, RunReport};
use atlatl_core::plan::{self, Plan, PlanRequest, ReplicationPlan, StreamRole};
use atlatl_core::redact::Redactor;
use atlatl_core::template::{EnvSource, Templates};
use atlatl_engine::budget::{Budget, BudgetParams, Detected};
use atlatl_engine::{Engine, EngineOptions};
use atlatl_state::{LakeState, RunRecord, RunReplicationRecord};
use indexmap::IndexMap;
use tokio_util::sync::CancellationToken;

use crate::lake::{self, LakeOutcome};
use crate::runtime::{self, Runtime};

#[derive(Debug)]
pub struct Output {
    pub up_to_date: bool,
    pub lake_snapshot: Option<i64>,
}

impl RunnerOutput for Output {
    fn status(&self) -> RepStatus {
        if self.up_to_date {
            RepStatus::UpToDate
        } else {
            RepStatus::Succeeded
        }
    }
}

impl From<LakeOutcome> for Output {
    fn from(o: LakeOutcome) -> Self {
        Output {
            up_to_date: !o.changed,
            lake_snapshot: o.lake_snapshot,
        }
    }
}

impl ReplicationRunner for Runtime {
    type Output = Output;

    fn run<'a>(
        &'a self,
        unit: &'a [&'a ReplicationPlan],
        attempt: u32,
        cancel: CancellationToken,
    ) -> BoxFuture<'a, Result<Output>> {
        Box::pin(async move {
            for r in unit {
                self.events
                    .emit(EventBody::ReplicationStarted(ReplicationStarted {
                        replication: r.name.clone(),
                        op: r.op.clone(),
                        streams: r.streams.iter().map(|s| s.id.clone()).collect(),
                    }));
            }
            let work = async {
                if let [one] = unit
                    && one.for_each.is_some()
                {
                    return crate::instances::unexpanded(self, one);
                }
                if let [one] = unit
                    && !one.hops.is_empty()
                {
                    if self.dry_run.is_some() || self.retry_rejects {
                        return crate::dry::hops(self, one, attempt, cancel.clone()).await;
                    }
                    return crate::hops::run_unit(self, one, attempt, cancel.clone()).await;
                }
                dispatch(self, unit, attempt, cancel.clone()).await
            };
            tokio::select! {
                r = work => r,
                // Dropping `work` interrupts this unit's session only; siblings keep running.
                _ = cancel.cancelled() => {
                    Err(AtlatlError::new(ErrorClass::Killed, "killed"))
                }
            }
        })
    }
}

/// Runs a single-hop unit with the runner for its source → target pair.
pub(crate) async fn dispatch(
    rt: &Runtime,
    unit: &[&ReplicationPlan],
    attempt: u32,
    cancel: CancellationToken,
) -> Result<Output> {
    let first = unit[0];
    match (first.source.kind.as_str(), first.target.kind.as_str()) {
        (
            "ducklake" | "delta" | "files" | "redshift" | "rest" | "clickhouse",
            "ducklake" | "duckdb",
        ) => Box::pin(lake::run_unit(rt, unit, attempt, cancel.clone()))
            .await
            .map(Output::from),
        ("ducklake", "postgres") => crate::serve::run_unit(rt, unit, attempt, cancel.clone()).await,
        ("ducklake", "sqlite" | "d1" | "turso") => {
            crate::serve_lite::run_unit(rt, unit, attempt, cancel.clone()).await
        }
        ("postgres", "ducklake" | "duckdb")
            if !unit
                .iter()
                .any(|r| r.streams.iter().any(|s| s.mode == "cdc")) =>
        {
            Box::pin(lake::run_unit(rt, unit, attempt, cancel.clone()))
                .await
                .map(Output::from)
        }
        ("postgres", "ducklake") => crate::pgcdc::run_unit(rt, unit, attempt, cancel.clone()).await,
        (s, t) => Err(AtlatlError::config(format!(
            "{s} → {t} replications are not supported by this build"
        ))),
    }
}

#[derive(Clone, Default)]
pub struct RunArgs {
    pub plan: PlanRequest,
    pub external_run_id: Option<String>,
    pub skip_succeeded: bool,
    pub concurrency: Option<usize>,
    /// Opened by the caller before anything else opens files (an inherited `fd:N`).
    pub events: EventSink,
    pub report: Option<PathBuf>,
    pub extension_dir: Option<PathBuf>,
    /// Stage, validate and diff against the targets; commit nothing (03 §11).
    pub dry_run: Option<crate::dry::DryRun>,
    /// Replay the selected REST streams' open rejects only (`atlatl rejects retry`, 03 §7).
    pub retry_rejects: bool,
    /// How often a progress line is logged when stderr isn't a terminal: `None` for the default
    /// (30 s), zero for never. A terminal gets a live status line instead.
    pub progress_interval: Option<std::time::Duration>,
}

/// Opens the engine with the budget for the selected ops, attaches the lakes the plan uses, and
/// bootstraps the state store.
pub async fn runtime(
    project: Arc<Project>,
    plan: Arc<Plan>,
    vars: Arc<dyn EnvSource>,
    external_run_id: Option<String>,
    extension_dir: Option<PathBuf>,
    events: EventSink,
) -> Result<Runtime> {
    Box::pin(open(
        project,
        plan,
        vars,
        external_run_id,
        extension_dir,
        events,
        None,
    ))
    .await
}

/// DuckDB connections the run writes: planned targets (hops included) and the state store.
fn written_duckdb(project: &Project, plan: &Plan) -> std::collections::HashSet<String> {
    plan.replications
        .iter()
        .flat_map(|r| std::iter::once(&r.target).chain(r.hops.iter().map(|h| &h.target)))
        .filter(|t| t.kind == "duckdb")
        .map(|t| t.name.clone())
        .chain(project.state.as_ref().map(|s| s.lake.value.clone()))
        .collect()
}

/// [`runtime`]; a dry run attaches lakes read-only without creating them (`dry::attach`) and
/// bootstraps no state store (it reads one if the lake has it).
async fn open(
    project: Arc<Project>,
    plan: Arc<Plan>,
    vars: Arc<dyn EnvSource>,
    external_run_id: Option<String>,
    extension_dir: Option<PathBuf>,
    events: EventSink,
    dry_run: Option<crate::dry::DryRun>,
) -> Result<Runtime> {
    let overrides = plan.replications.iter().find_map(|r| {
        project
            .replication(r.config_name())
            .and_then(|x| x.engine.clone())
    });
    let budget = Budget::compute(
        &project.engine,
        overrides.as_ref(),
        &Detected::detect(),
        &BudgetParams::default(),
    )?;
    let engine = Engine::open(EngineOptions {
        budget,
        extension_dir,
        pool_size: None,
    })?;

    let mut connections: IndexMap<String, Connection> = IndexMap::new();
    for c in &plan.connections {
        let name = c
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or_default()
            .to_owned();
        if name.is_empty() {
            continue;
        }
        let resolved =
            runtime::resolve_connection(&project, &name, plan.env.as_deref(), vars.clone())?;
        connections.insert(name, resolved);
    }

    let run_id = plan.run_id.clone().unwrap_or_else(atlatl_core::ids::ulid);
    let mut lakes = IndexMap::new();
    let mut notes = Vec::new();
    let written = written_duckdb(&project, &plan);
    for (name, c) in &connections {
        match &c.kind {
            ConnectionKind::Ducklake(d) => {
                let lake = if dry_run.is_some() {
                    let dir = runtime::workdir(&engine, &run_id);
                    let (l, w) = crate::dry::attach(&engine, name, d, &dir).await?;
                    notes.extend(w);
                    l
                } else {
                    atlatl_conn_ducklake::attach(&engine, name, d).await?
                };
                lakes.insert(name.clone(), lake);
            }
            // Read-only duckdb files (seeds) stay closed; each listing opens its own.
            ConnectionKind::Duckdb(d) if written.contains(name) => {
                let lake = atlatl_conn_ducklake::plain::attach(&engine, name, d, dry_run.is_some())
                    .await?;
                lakes.insert(name.clone(), lake);
            }
            _ => {}
        }
    }
    let state = match project.state.as_ref() {
        Some(s) => {
            let lake = lakes.get(&s.lake.value).ok_or_else(|| {
                AtlatlError::config(format!("state lake `{}` is not attached", s.lake.value))
            })?;
            if dry_run.is_some() {
                Some(crate::dry::state_store(&engine, &lake.alias).await?)
            } else {
                let st = LakeState::new(lake.alias.clone());
                let st2 = st.clone();
                engine.with_conn(move |c| st2.bootstrap(c)).await?;
                Some(st)
            }
        }
        None => None,
    };
    let locks = runtime::lock_backend(&project, &connections)?;
    let report = ReportHandle::from_plan(&plan, external_run_id.clone());
    report.update(|r| {
        r.run_id.clone_from(&run_id);
        r.dry_run = dry_run.is_some();
        r.warnings.extend(notes);
    });
    let b = engine.budget().clone();
    report.update(|r| {
        r.engine.duckdb_version = Some(atlatl_engine::DUCKDB_VERSION.into());
        r.engine.memory_limit = Some(format!("{}B", b.memory_limit));
        r.engine.threads = Some(b.threads);
        r.engine.memory_mode = Some(b.memory_mode);
    });
    Ok(Runtime {
        run_id,
        project,
        plan,
        engine,
        lakes,
        connections,
        state,
        locks,
        report,
        events,
        external_run_id,
        started: SystemTime::now(),
        templates: Templates::new(),
        vars,
        rest_clients: Default::default(),
        clickhouse_slots: Default::default(),
        redshift_wires: Default::default(),
        for_each_errors: Default::default(),
        dry_run,
        retry_rejects: false,
        progress: Default::default(),
    })
}

/// Parses the API spec of every planned REST source up front, so a bad spec fails the run
/// before any replication starts rather than when its first unit opens a client. Also refuses a
/// `full-refresh` stream over an `except_landed` endpoint: each run would replace the target with
/// only that run's new keys (07 §7).
pub fn check_rest_specs(project: &Project, plan: &Plan) -> Result<()> {
    let mut specs: std::collections::HashMap<String, atlatl_http::ApiSpec> = Default::default();
    for r in plan.replications.iter().filter(|r| r.source.kind == "rest") {
        let Some(spec) = atlatl_core::plan::checks::spec_for(project, &r.source.name) else {
            continue;
        };
        if !specs.contains_key(&spec.path) {
            let s = atlatl_http::ApiSpec::load_config(&project.root, &spec.path)
                .map_err(|e| AtlatlError::config(e.to_string()))?;
            specs.insert(spec.path.clone(), s);
        }
        let api = &specs[&spec.path];
        for s in r.streams.iter().filter(|s| s.role == StreamRole::Selected) {
            let landed = s
                .from
                .endpoint
                .as_ref()
                .and_then(|e| api.endpoints.get(e.as_str()))
                .and_then(|e| e.for_each.as_ref())
                .is_some_and(|f| match &f.over {
                    atlatl_http::spec::Over::Queue { except_landed, .. }
                    | atlatl_http::spec::Over::Query { except_landed, .. } => {
                        !except_landed.is_empty()
                    }
                    _ => false,
                });
            if landed && s.mode == "full-refresh" {
                return Err(AtlatlError::config(format!(
                    "stream `{}`: `full-refresh` over an endpoint with `except_landed` would replace `{}` with only each run's new keys; use `incremental` (or `snapshot`)",
                    s.id, s.object
                )));
            }
        }
    }
    Ok(())
}

fn status_str(s: RepStatus) -> String {
    serde_json::to_value(s)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default()
}

/// Lifecycle log (10 §2): failures carry the error, so they reach stderr without `--report`.
fn log_replication<O>(r: &RepResult<O>) {
    let (rep, ms) = (&r.replication, r.duration.as_millis() as u64);
    let status = status_str(r.status);
    match (&r.error, r.status) {
        (Some(e), RepStatus::Failed) => tracing::error!(
            replication = %rep, status, duration_ms = ms, attempts = r.attempts,
            error_class = e.class.as_str(), "replication failed: {e}"
        ),
        (Some(e), _) => tracing::warn!(
            replication = %rep, status, duration_ms = ms, error_class = e.class.as_str(),
            "replication {status}: {e}"
        ),
        (None, RepStatus::NotStarted) => {}
        (None, _) => {
            tracing::info!(replication = %rep, status, duration_ms = ms, "replication {status}")
        }
    }
}

/// A whole run. Returns the finished report; its exit code is the process's (03 §10).
/// `killed_by` reports why `cancel` fired (signal or `--timeout`), for the report.
pub async fn run(
    project: Arc<Project>,
    args: RunArgs,
    vars: Arc<dyn EnvSource>,
    cancel: CancellationToken,
    killed_by: impl Fn() -> Option<KillReason>,
    on_report: impl FnOnce(&ReportHandle, Option<&Path>),
) -> Result<RunReport> {
    if args.plan.static_plan {
        return Err(AtlatlError::config(
            "a static plan (no environment, templates or secrets) can't be run",
        ));
    }
    let plan = match plan::build(&project, &args.plan, vars.clone(), Redactor::global()) {
        Ok((p, _)) => Arc::new(p),
        Err(d) => {
            let msg = d
                .errors()
                .map(|e| e.error.to_string())
                .collect::<Vec<_>>()
                .join("; ");
            return Err(AtlatlError::config(msg));
        }
    };

    check_rest_specs(&project, &plan)?;
    let dry_notes = match args.dry_run {
        Some(_) => crate::dry::check_plan(&plan)?,
        None => Vec::new(),
    };
    if args.retry_rejects {
        crate::rejects_cmd::check_plan(&plan)?;
    }
    let events = args.events.clone();
    let mut rt = Box::pin(open(
        project.clone(),
        plan,
        vars,
        args.external_run_id.clone(),
        args.extension_dir.clone(),
        events.clone(),
        args.dry_run,
    ))
    .await?;
    rt.retry_rejects = args.retry_rejects;
    rt.report.update(|r| r.warnings.extend(dry_notes));
    crate::instances::expand(&mut rt, &args.plan).await?;
    let plan = rt.plan.clone();
    let run_id = rt.run_id.clone();
    on_report(&rt.report, args.report.as_deref());
    events.emit(EventBody::RunStarted(RunStarted {
        env: plan.env.clone(),
        selection: (&plan.selection).into(),
        plan_hash: plan.plan_hash.clone(),
        atlatl_version: env!("CARGO_PKG_VERSION").into(),
        duckdb_version: Some(atlatl_engine::DUCKDB_VERSION.into()),
    }));

    let interval = match args.progress_interval {
        None => Some(crate::progress::DEFAULT_INTERVAL),
        Some(d) if d.is_zero() => None,
        Some(d) => Some(d),
    };
    let _ticker = crate::progress::start(rt.progress.clone(), events.clone(), interval);

    let mut opts = ExecOptions {
        concurrency: args
            .concurrency
            .or(project.engine.replication_concurrency.map(|c| c as usize))
            .unwrap_or(2),
        ..Default::default()
    };
    if args.skip_succeeded
        && let (Some(st), Some(ext)) = (&rt.state, &args.external_run_id)
    {
        let (st, ext) = (st.clone(), ext.clone());
        opts.already_succeeded = rt
            .engine
            .with_conn(move |c| st.succeeded_under(c, &ext))
            .await?;
    }
    let done = |r: &RepResult<Output>| {
        log_replication(r);
        let err = r.error.as_ref().map(ReportError::from);
        if let Some(entry) = rt.report.finish_replication(&r.replication, r.status, err) {
            events.emit(EventBody::ReplicationCompleted(entry));
        }
    };
    let results = execute(&plan, &rt, rt.locks.as_ref(), &opts, cancel.clone(), done).await;
    if cancel.is_cancelled()
        && let Some(r) = killed_by()
    {
        rt.report.update(|rep| rep.killed_by = Some(r));
    }
    if let Some(st) = rt.state.clone().filter(|_| rt.dry_run.is_none()) {
        let records: Vec<RunReplicationRecord> = results
            .iter()
            .filter(|r| !matches!(r.status, RepStatus::NotStarted))
            .map(|r| RunReplicationRecord {
                run_id: run_id.clone(),
                external_run_id: args.external_run_id.clone(),
                replication: r.replication.clone(),
                status: status_str(r.status),
                attempts: r.attempts,
                lake_snapshot: r.output.as_ref().and_then(|o| o.lake_snapshot),
                error_class: r.error.as_ref().map(|e| e.class.as_str().to_owned()),
                error: r.error.as_ref().map(|e| e.redacted_message()),
            })
            .collect();
        let rec = RunRecord {
            run_id: run_id.clone(),
            external_run_id: args.external_run_id.clone(),
            env: plan.env.clone(),
            kind: if rt.retry_rejects {
                "rejects retry"
            } else {
                "run"
            }
            .into(),
            status: "finished".into(),
            plan_hash: Some(plan.plan_hash.clone()),
            atlatl_version: env!("CARGO_PKG_VERSION").into(),
            detail: serde_json::json!({}),
        };
        let r = rt
            .engine
            .with_conn(move |c| {
                for x in &records {
                    st.record_replication(c, x)?;
                }
                st.record_run(c, &rec)
            })
            .await;
        if let Err(e) = r {
            tracing::warn!(error = %e, "could not record run history");
        }
    }
    let _ = std::fs::remove_dir_all(rt.workdir());
    let report = match &args.report {
        Some(p) => rt
            .report
            .write(p)
            .map_err(|e| AtlatlError::internal(format!("writing report: {e}")))?,
        None => rt.report.finish(),
    };
    events.emit(EventBody::RunCompleted(RunCompleted {
        status: report.status,
        exit_code: report.exit_code,
        counts: report.counts(),
        report_path: args.report.as_ref().map(|p| p.display().to_string()),
    }));
    Ok(report)
}
