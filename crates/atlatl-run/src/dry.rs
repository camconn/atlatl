//! `atlatl run --dry-run` (03 §11): every selected ingest stream is extracted, staged and
//! validated as a run would, then compared with its target (`lake::dry_diff`); nothing is
//! committed. The result lands in the report (`dry_run`, per-member `diff`) and is printed
//! bootstrap-shaped ([`render`], like `cdc bootstrap --dry-run`).

use atlatl_core::config::model::{CatalogType, DuckLakeConn};
use atlatl_core::error::{AtlatlError, Result};
use atlatl_core::obs::report::{ReplicationStatus, ReportWarning, RunReport, StreamStatus};
use atlatl_core::plan::{Plan, ReplicationPlan};
use atlatl_engine::Engine;
use atlatl_ingest::sql::literal;
use atlatl_state::LakeState;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::run::{Output, dispatch};
use crate::runtime::Runtime;

pub const DEFAULT_SAMPLES: usize = 5;

/// Where a dry run keeps state tables the lake doesn't have yet: an in-memory database, so
/// nothing is created in the lake.
const STAND_IN: &str = "atlatl_dry_state";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DryRun {
    /// Samples per op (insert, update, delete) and member.
    pub samples: usize,
}

impl Default for DryRun {
    fn default() -> Self {
        Self {
            samples: DEFAULT_SAMPLES,
        }
    }
}

/// Handled by the lake runner (as `run::dispatch` routes it).
fn lake_ingest(r: &ReplicationPlan) -> bool {
    atlatl_core::plan::checks::is_lake_target(&r.target.kind)
        && match r.source.kind.as_str() {
            "ducklake" | "delta" | "files" | "redshift" | "rest" | "clickhouse" => true,
            "postgres" => !r.streams.iter().any(|s| s.mode == "cdc"),
            _ => false,
        }
}

/// Fails (`config`) unless every selected replication ingests into a lake. Returns a warning
/// per multi-hop replication: only its lake hop is dry-run.
pub fn check_plan(plan: &Plan) -> Result<Vec<ReportWarning>> {
    let mut bad = Vec::new();
    let mut notes = Vec::new();
    for r in &plan.replications {
        let first = if r.hops.is_empty() {
            r.clone()
        } else {
            r.hop_plan(0)
        };
        if !lake_ingest(&first) {
            let cdc = if first.streams.iter().any(|s| s.mode == "cdc") {
                " cdc"
            } else {
                ""
            };
            bad.push(format!(
                "`{}` ({} → {}{cdc})",
                r.name, first.source.kind, first.target.kind
            ));
            continue;
        }
        let later: Vec<String> = r
            .hops
            .iter()
            .skip(1)
            .filter(|h| !h.streams.is_empty())
            .map(|h| format!("{} → {}", h.source.name, h.target.name))
            .collect();
        if !later.is_empty() {
            notes.push(ReportWarning {
                code: "dry_run_hops".into(),
                message: format!(
                    "`{}`: only the lake hop is dry-run; its `then:` hops ({}) read that hop's commit and are not run (preview a serving hop with `atlatl cdc bootstrap {} --dry-run`)",
                    r.name,
                    later.join(", "),
                    r.name
                ),
            });
        }
    }
    if !bad.is_empty() {
        return Err(AtlatlError::config(format!(
            "`run --dry-run` stages and diffs ingest into a lake, which {} {}; preview a lake → serving load with `atlatl cdc bootstrap <replication> --dry-run`",
            bad.join(", "),
            if bad.len() == 1 { "isn't" } else { "aren't" }
        ))
        .code("dry_run_unsupported"));
    }
    Ok(notes)
}

/// Attaches a lake without creating it: read-only when its catalog exists, else an empty
/// in-memory stand-in under the same alias (never at the configured location), with a warning.
pub async fn attach(
    engine: &Engine,
    name: &str,
    conn: &DuckLakeConn,
    workdir: &std::path::Path,
) -> Result<(atlatl_conn_ducklake::Lake, Option<ReportWarning>)> {
    if let Some(l) = atlatl_conn_ducklake::attach_existing(engine, name, conn).await? {
        return Ok((l, None));
    }
    let c = &conn.catalog;
    let what = match c.kind {
        CatalogType::Duckdb => format!("{:?}", c.path.as_deref().unwrap_or_default()),
        CatalogType::Postgres => format!(
            "in Postgres database `{}` on {}:{}",
            c.database.as_deref().unwrap_or_default(),
            c.host.as_deref().unwrap_or_default(),
            c.port.unwrap_or(5432)
        ),
    };
    let data = workdir.join(format!("dry-lake-{name}"));
    let lake =
        atlatl_conn_ducklake::attach_empty(engine, name, conn, &format!("{}/", data.display()))
            .await?;
    Ok((
        lake,
        Some(ReportWarning {
            code: "dry_run_no_catalog".into(),
            message: format!(
                "lake `{name}`: its catalog {what} doesn't exist yet (a run would create it); diffed against an empty lake"
            ),
        }),
    ))
}

/// The state lake's `_atlatl` tables when they exist (read, never written), else empty
/// stand-ins in memory: a dry run creates nothing in the lake.
pub async fn state_store(engine: &Engine, alias: &str) -> Result<LakeState> {
    let a = alias.to_owned();
    let present: i64 = engine
        .with_conn(move |c| {
            c.query_row(
                &format!(
                    "SELECT count(*) FROM duckdb_tables() WHERE database_name = {} AND schema_name = '_atlatl' AND table_name IN ('stream_state', 'rejects')",
                    literal(&a)
                ),
                [],
                |r| r.get(0),
            )
            .map_err(atlatl_ingest::sql::db)
        })
        .await?;
    if present == 2 {
        return Ok(LakeState::new(alias));
    }
    let st = LakeState::new(STAND_IN);
    let st2 = st.clone();
    engine
        .with_conn(move |c| {
            c.execute_batch(&format!("ATTACH IF NOT EXISTS ':memory:' AS {STAND_IN}"))
                .map_err(atlatl_ingest::sql::db)?;
            st2.bootstrap(c)
        })
        .await?;
    Ok(st)
}

/// A multi-hop replication: its lake hop only (the rest read that hop's commit).
pub async fn hops(
    rt: &Runtime,
    rep: &ReplicationPlan,
    attempt: u32,
    cancel: CancellationToken,
) -> Result<Output> {
    rt.report.start_replication(&rep.name);
    let r = dispatch(rt, &[&rep.hop_plan(0)], attempt, cancel).await;
    rt.report.replication(&rep.name, |x| x.attempts = attempt);
    r
}

/// The dry run's result, shaped like `cdc bootstrap --dry-run`: one entry per replication with
/// `dry_run: true` and a stream entry per member.
pub fn render(r: &RunReport) -> Value {
    let reps: Vec<Value> = r
        .replications
        .iter()
        .map(|rep| {
            let mut streams = Vec::new();
            for s in &rep.streams {
                if s.status == StreamStatus::FeedOnly {
                    continue;
                }
                let mut any = false;
                for m in &s.members {
                    let Some(d) = &m.diff else { continue };
                    any = true;
                    streams.push(json!({
                        "stream": s.stream_id,
                        "member": m.member,
                        "target": m.object,
                        "mode": d.mode,
                        "staged": m.rows.staged,
                        "target_rows": d.target_rows,
                        "inserts": d.inserts,
                        "updates": d.updates,
                        "deletes": d.deletes,
                        "unchanged": d.unchanged,
                        "counts_only": d.counts_only,
                        "ddl": d.ddl,
                        "state_before": m.state.before,
                        "state_after": m.state.after,
                        "chunks": m.chunks,
                        "schema": m.schema.as_ref().map(|x| x.status),
                        "rejects": m.rejects.new,
                        "samples": d.samples,
                    }));
                }
                if !any && rep.status.is_success() {
                    streams.push(json!({"stream": s.stream_id, "not_run": true}));
                }
            }
            let mut v = json!({
                "replication": rep.replication,
                "dry_run": true,
                "status": rep.status,
                "error": rep.error,
                "streams": streams,
            });
            if let Some(i) = &rep.instance {
                v["instance"] = json!(i);
            }
            v
        })
        .collect();
    json!({
        "dry_run": true,
        "run_id": r.run_id,
        "status": r.status,
        "exit_code": r.exit_code,
        "warnings": r.warnings,
        "replications": reps,
    })
}

/// A few lines per replication for the terminal.
pub fn summary(r: &RunReport) -> String {
    let status = json!(r.status);
    let mut out = format!(
        "dry run {} ({}): nothing committed\n",
        r.run_id,
        status.as_str().unwrap_or_default()
    );
    for w in &r.warnings {
        out.push_str(&format!("warning[{}]: {}\n", w.code, w.message));
    }
    for rep in &r.replications {
        let status = match rep.status {
            ReplicationStatus::UpToDate => "up to date",
            ReplicationStatus::Succeeded => "would change",
            ReplicationStatus::Failed => "failed",
            ReplicationStatus::Skipped => "skipped",
            ReplicationStatus::Killed => "killed",
            ReplicationStatus::NotStarted => "not started",
        };
        out.push_str(&format!("{}: {status}", rep.replication));
        if let Some(e) = &rep.error {
            out.push_str(&format!(" ({}: {})", e.class.as_str(), e.message));
        }
        out.push('\n');
        for s in &rep.streams {
            for m in &s.members {
                let Some(d) = &m.diff else { continue };
                let member = if m.member.is_empty() {
                    String::new()
                } else {
                    format!(" [{}]", m.member)
                };
                out.push_str(&format!(
                    "  {}{member} → {}: {} inserts, {} updates, {} deletes, {} unchanged ({} staged, {} in target)\n",
                    s.stream_id,
                    m.object.as_deref().unwrap_or("?"),
                    d.inserts,
                    d.updates,
                    d.deletes,
                    d.unchanged,
                    m.rows.staged,
                    d.target_rows
                ));
                if let Some(why) = &d.counts_only {
                    out.push_str(&format!("    counts only: {why}\n"));
                }
                for x in &d.ddl {
                    out.push_str(&format!("    ddl: {x}\n"));
                }
                if m.state.before != m.state.after {
                    let j = |v: &Option<Value>| v.as_ref().map_or("null".into(), Value::to_string);
                    out.push_str(&format!(
                        "    state: {} → {}\n",
                        j(&m.state.before),
                        j(&m.state.after)
                    ));
                }
            }
        }
    }
    out
}
