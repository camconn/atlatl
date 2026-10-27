//! `atlatl schema infer` and `atlatl schema check` (04 §10).

use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

use atlatl_core::config::load::Project;
use atlatl_core::config::model::SourceSchema;
use atlatl_core::obs::report::{SchemaReport, SchemaStatus, Severity};
use atlatl_core::plan::{self, PlanRequest};
use atlatl_core::redact::{Redactor, redact};
use atlatl_core::template::ProcessEnv;
use atlatl_ingest::schema::{self, Staged};
use clap::{Args, Subcommand};

use crate::api::{self, SampleArgs};

#[derive(Subcommand)]
pub enum SchemaCmd {
    /// Draft a source schema (`mode: observe`) for a stream from a sample, for review.
    Infer {
        stream: String,
        #[command(flatten)]
        src: SourceArgs,
        /// Write `schemas/<stream id>.yaml` instead of printing.
        #[arg(long)]
        write: bool,
        /// Overwrite an existing schema file.
        #[arg(long, requires = "write")]
        force: bool,
    },
    /// Check a sample, or the current table, against the stream's source schema without a run.
    /// Exits 1 when a `fail` policy trips.
    Check {
        stream: String,
        #[command(flatten)]
        src: SourceArgs,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Args)]
pub struct SourceArgs {
    /// Sample the stream's REST endpoint live (like `atlatl api test`).
    #[arg(long, conflicts_with_all = ["from_cassette", "from_table"])]
    from_api_test: bool,
    /// Sample from a recorded cassette (name under `apis/<api>/cassettes/`, or a path).
    #[arg(long, value_name = "NAME|PATH", conflicts_with = "from_table")]
    from_cassette: Option<String>,
    /// Profile the stream's existing lake table (all-text br-layer values are parsed).
    #[arg(long)]
    from_table: bool,
    /// Table columns that aren't source fields, beyond `_atlatl_*` (with `--from-table`).
    #[arg(long = "exclude", value_name = "COLUMN")]
    exclude: Vec<String>,
    #[arg(long, env = "ATLATL_EXTENSION_DIR")]
    extension_dir: Option<std::path::PathBuf>,
    #[command(flatten)]
    sample: SampleArgs,
}

enum Loaded {
    /// A connection holding the staged sample as `sample (ordinal, record)`.
    Json(duckdb::Connection, u64),
    Table {
        engine: atlatl_engine::Engine,
        relation: String,
    },
}

async fn load(p: &Project, stream: &str, src: &SourceArgs) -> Result<Loaded, String> {
    let (rep, s) = p
        .stream(stream)
        .ok_or_else(|| format!("no stream `{stream}`"))?;
    if src.from_table {
        return table(p, stream, src).await;
    }
    if !src.from_api_test && src.from_cassette.is_none() {
        return Err(
            "choose a sample: --from-api-test, --from-cassette NAME|PATH or --from-table".into(),
        );
    }
    let endpoint = s
        .config
        .from
        .as_ref()
        .and_then(|f| f.value.endpoint.clone())
        .ok_or_else(|| format!("stream `{stream}` has no `from.endpoint` (not a REST stream)"))?;
    let mut a = src.sample.clone();
    if let Some(c) = &src.from_cassette {
        a.replay = Some(c.clone());
    }
    let vars = s
        .config
        .vars
        .as_ref()
        .map(|v| serde_json::to_value(v).unwrap_or_default());
    let (_, sample) = api::sample(p, &rep.source.value, &endpoint, &a, vars).await?;
    if let Some(e) = sample.error {
        return Err(format!("sampling `{endpoint}`: {e}"));
    }
    let c = duckdb::Connection::open_in_memory().map_err(|e| e.to_string())?;
    let n = schema::stage_json(
        &c,
        "sample",
        sample
            .records
            .iter()
            .map(|r| (r.ordinal, r.record.to_string())),
    )
    .map_err(|e| e.to_string())?;
    Ok(Loaded::Json(c, n))
}

async fn table(p: &Project, stream: &str, src: &SourceArgs) -> Result<Loaded, String> {
    use atlatl_core::config::model::ConnectionKind;
    use atlatl_engine::budget::{Budget, BudgetParams, Detected};
    let req = PlanRequest {
        env: src.sample.env.clone(),
        vars: src.sample.vars.iter().cloned().collect(),
        streams: vec![stream.to_owned()],
        with_deps: false,
        ..Default::default()
    };
    let (plan, _) = plan::build(p, &req, Arc::new(ProcessEnv), Redactor::global())
        .map_err(|d| crate::render(&p.sources, &d))?;
    let (rp, sp) = plan
        .replications
        .iter()
        .find_map(|r| r.streams.iter().find(|s| s.id == stream).map(|s| (r, s)))
        .ok_or_else(|| format!("stream `{stream}` is not in the plan"))?;
    let object = sp
        .members
        .iter()
        .filter(|m| !m.pattern)
        .find_map(|m| m.object.clone())
        .unwrap_or_else(|| sp.object.clone());
    let conn = atlatl_run::runtime::resolve_connection(
        p,
        &rp.target.name,
        plan.env.as_deref(),
        Arc::new(ProcessEnv),
    )
    .map_err(|e| e.to_string())?;
    let ConnectionKind::Ducklake(d) = &conn.kind else {
        return Err(format!(
            "--from-table reads lake tables; `{}` is not a ducklake connection",
            rp.target.name
        ));
    };
    let budget = Budget::compute(
        &p.engine,
        None,
        &Detected::detect(),
        &BudgetParams::default(),
    )
    .map_err(|e| e.to_string())?;
    let engine = atlatl_engine::Engine::open(atlatl_engine::EngineOptions {
        budget,
        extension_dir: src.extension_dir.clone(),
        pool_size: None,
    })
    .map_err(|e| e.to_string())?;
    let lake = atlatl_conn_ducklake::attach(&engine, &rp.target.name, d)
        .await
        .map_err(|e| e.to_string())?;
    let (sch, t) = atlatl_run::lake::split_object(&object);
    let relation = atlatl_ingest::sql::qualified(&[&lake.alias, &sch, &t]);
    Ok(Loaded::Table { engine, relation })
}

fn staged_json() -> Staged<'static> {
    Staged::records("sample")
}

async fn with_staged<T: Send + 'static>(
    l: Loaded,
    exclude: Vec<String>,
    f: impl FnOnce(&duckdb::Connection, &Staged<'_>) -> atlatl_core::error::Result<T> + Send + 'static,
) -> Result<T, String> {
    match l {
        Loaded::Json(c, _) => f(&c, &staged_json()).map_err(|e| e.to_string()),
        Loaded::Table { engine, relation } => engine
            .with_conn(move |c| {
                let cols = atlatl_ingest::staging::describe(c, &relation)?;
                let mut ex: Vec<&str> = cols
                    .iter()
                    .map(|c| c.name.as_str())
                    .filter(|n| n.starts_with("_atlatl_"))
                    .collect();
                ex.extend(exclude.iter().map(String::as_str));
                f(
                    c,
                    &Staged::Columns {
                        relation: &relation,
                        exclude: &ex,
                        ordinal: None,
                    },
                )
            })
            .await
            .map_err(|e| e.to_string()),
    }
}

fn print_report(stream: &str, r: &SchemaReport) {
    let verdict = match (r.status, r.severity) {
        (SchemaStatus::Ok, _) => "ok",
        (_, Some(Severity::Error)) => "violated (fail)",
        _ => "violated (warn)",
    };
    println!("{stream}: {verdict} [{:?}, {} records]", r.mode, r.records);
    if !r.new_fields.is_empty() {
        println!("  new fields: {}", r.new_fields.join(", "));
    }
    if !r.missing_fields.is_empty() {
        println!("  missing fields: {}", r.missing_fields.join(", "));
    }
    for v in &r.violations {
        let samples = v
            .samples
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        println!(
            "  {:?} violations on {}: {} (e.g. {})",
            v.kind,
            v.field,
            v.count,
            redact(&samples)
        );
    }
}

pub fn run(root: &Path, cmd: SchemaCmd) -> Result<ExitCode, String> {
    let (p, _) = crate::load_project(root).map_err(|(s, d)| crate::render(&s, &d))?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    match cmd {
        SchemaCmd::Infer {
            stream,
            src,
            write,
            force,
        } => {
            let l = rt.block_on(load(&p, &stream, &src))?;
            let what = match &l {
                Loaded::Json(_, n) => format!("{n} sampled records"),
                Loaded::Table { relation, .. } => format!("table {relation}"),
            };
            let inf = rt.block_on(with_staged(l, src.exclude.clone(), |c, s| {
                schema::infer(c, s)
            }))?;
            let header = format!(
                "Drafted by `atlatl schema infer` from {what} ({} records).\nReview types, nullability and `values`, then remove `mode: observe` to enforce.",
                inf.records
            );
            let yaml = schema::to_yaml(&inf.schema, &header);
            if write {
                let path = p.root.join("schemas").join(format!("{stream}.yaml"));
                if path.exists() && !force {
                    return Err(format!("{} exists (use --force)", path.display()));
                }
                if let Some(d) = path.parent() {
                    std::fs::create_dir_all(d).map_err(|e| e.to_string())?;
                }
                std::fs::write(&path, yaml).map_err(|e| e.to_string())?;
                println!("wrote {}", path.display());
            } else {
                print!("{yaml}");
            }
            Ok(ExitCode::SUCCESS)
        }
        SchemaCmd::Check { stream, src, json } => {
            let (_, s) = p
                .stream(&stream)
                .ok_or_else(|| format!("no stream `{stream}`"))?;
            let doc: SourceSchema = s
                .schema
                .as_ref()
                .map(|d| d.schema.value.clone())
                .ok_or_else(|| {
                    format!("stream `{stream}` has no source schema (schemas/{stream}.yaml or inline `schema:`)")
                })?;
            let l = rt.block_on(load(&p, &stream, &src))?;
            let ev = rt.block_on(with_staged(l, src.exclude.clone(), move |c, st| {
                schema::evaluate(c, st, &doc)
            }))?;
            if json {
                let v = atlatl_core::obs::redacted_json(&ev.report).map_err(|e| e.to_string())?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&v).map_err(|e| e.to_string())?
                );
            } else {
                print_report(&stream, &ev.report);
            }
            Ok(if ev.failure.is_some() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            })
        }
    }
}
