//! `atlatl rejects list | retry | drop` (03 §7). Streams are addressed as in `atlatl state`:
//! `<stream>`, `<stream>@<key>` or `<stream> --instance <key>`; a bare stream id of a `for_each`
//! replication covers every instance. `retry` is a run restricted to replaying open rejects
//! ([`RunArgs::retry_rejects`](crate::RunArgs)).

use std::sync::Arc;

use atlatl_core::config::load::Project;
use atlatl_core::error::{AtlatlError, Result};
use atlatl_core::obs::report::RunReport;
use atlatl_core::plan::{Plan, PlanRequest};
use atlatl_core::template::EnvSource;
use atlatl_state::RejectRow;
use serde_json::{Value, json};

use crate::state_cmd::{Address, StateArgs, action_record, check, open_store};

#[derive(Clone, Debug)]
pub enum RejectsOp {
    List {
        stream: Option<String>,
        instance: Option<String>,
    },
    /// One stream's rejects (optionally one member's, or only `keys`), or every stream's (`all`).
    Drop {
        stream: Option<String>,
        instance: Option<String>,
        member: Option<String>,
        keys: Vec<String>,
        all: bool,
    },
}

/// `list` (the rows, as JSON) and `drop` (`{"dropped": n}`, logged in `_atlatl.runs`).
pub async fn rejects(
    project: Arc<Project>,
    args: StateArgs,
    vars: Arc<dyn EnvSource>,
    op: RejectsOp,
) -> Result<Value> {
    let addr = |stream: &Option<String>, instance: &Option<String>| match stream {
        Some(s) => Address::parse(s, instance.as_deref()).map(Some),
        None if instance.is_some() => Err(AtlatlError::config("--instance needs a stream")),
        None => Ok(None),
    };
    match &op {
        RejectsOp::List { stream, instance } => {
            let a = addr(stream, instance)?;
            let (engine, _, st) = open_store(&project, &args, vars).await?;
            let rows = engine.with_conn(move |c| st.list_rejects(c)).await?;
            let rows: Vec<RejectRow> = rows
                .into_iter()
                .filter(|r| a.as_ref().is_none_or(|a| a.covers(&r.stream_id)))
                .collect();
            Ok(serde_json::to_value(rows).unwrap_or_default())
        }
        RejectsOp::Drop {
            stream,
            instance,
            member,
            keys,
            all,
        } => {
            let a = match (addr(stream, instance)?, all) {
                (Some(_), true) => {
                    return Err(AtlatlError::config("name a stream or `--all`, not both"));
                }
                (None, false) => {
                    return Err(AtlatlError::config(
                        "name the stream whose rejects to drop, or `--all` for every stream's",
                    ));
                }
                (a, _) => a,
            };
            if a.is_none() && (member.is_some() || !keys.is_empty()) {
                return Err(AtlatlError::config("--member and --key need a stream"));
            }
            let (engine, _, st) = open_store(&project, &args, vars).await?;
            let (member, keys) = (member.clone(), keys.clone());
            let (env, shown) = (args.env.clone(), format!("{op:?}"));
            let rec = move |n: usize| {
                action_record(
                    "rejects drop",
                    env.clone(),
                    shown.clone(),
                    json!({"dropped": n}),
                )
            };
            let n = engine
                .with_conn(move |c| {
                    let ids: Vec<String> = {
                        let mut ids: Vec<String> = st
                            .list_rejects(c)?
                            .into_iter()
                            .map(|r| r.stream_id)
                            .filter(|id| a.as_ref().is_none_or(|a| a.covers(id)))
                            .collect();
                        ids.dedup();
                        ids
                    };
                    c.execute_batch("BEGIN").map_err(atlatl_ingest::sql::db)?;
                    let run = || -> Result<usize> {
                        let mut n = 0;
                        for id in &ids {
                            n += st.drop_rejects(c, id, member.as_deref(), &keys)?;
                        }
                        st.record_run(c, &rec(n))?;
                        c.execute_batch("COMMIT").map_err(atlatl_ingest::sql::db)?;
                        Ok(n)
                    };
                    run().inspect_err(|_| {
                        let _ = c.execute_batch("ROLLBACK");
                    })
                })
                .await?;
            Ok(json!({"dropped": n}))
        }
    }
}

/// The run selection for `rejects retry <stream>…`: those streams (their `for_each` instances,
/// or the one named). Queue producers are planned feed-only as usual, but a retry skips them.
pub fn retry_request(
    project: &Project,
    env: Option<String>,
    streams: &[String],
    instance: Option<&str>,
) -> Result<PlanRequest> {
    if streams.is_empty() {
        return Err(AtlatlError::config(
            "name the stream(s) whose rejects to retry",
        ));
    }
    let mut req = PlanRequest {
        env,
        ..PlanRequest::all()
    };
    for s in streams {
        let a = Address::parse(s, instance)?;
        check(project, &a, true, false)?;
        if let (Some(k), Some((rep, _))) = (&a.key, project.stream(&a.stream)) {
            req.instances.push(format!("{}[{k}]", rep.name.value));
        }
        if !req.streams.contains(&a.stream) {
            req.streams.push(a.stream);
        }
    }
    Ok(req)
}

/// Fails (`config`) unless every selected stream is a REST stream into a lake: only those keep
/// rejects.
pub fn check_plan(plan: &Plan) -> Result<()> {
    let bad: Vec<String> = plan
        .replications
        .iter()
        .flat_map(|r| {
            let first = if r.hops.is_empty() {
                r.clone()
            } else {
                r.hop_plan(0)
            };
            let ok = first.source.kind == "rest" && first.target.kind == "ducklake";
            first
                .streams
                .iter()
                .filter(|s| s.role != atlatl_core::plan::StreamRole::FeedOnly && !ok)
                .map(|s| format!("`{}` ({} → {})", s.id, first.source.kind, first.target.kind))
                .collect::<Vec<_>>()
        })
        .collect();
    if bad.is_empty() {
        return Ok(());
    }
    Err(AtlatlError::config(format!(
        "only REST streams into a lake keep rejects; {} {}",
        bad.join(", "),
        if bad.len() == 1 { "doesn't" } else { "don't" }
    ))
    .code("rejects_retry_unsupported"))
}

/// What `rejects retry` prints: per member, what landed and what is still open.
pub fn render_retry(r: &RunReport) -> Value {
    let reps: Vec<Value> = r
        .replications
        .iter()
        .map(|rep| {
            let streams: Vec<Value> = rep
                .streams
                .iter()
                .flat_map(|s| {
                    s.members.iter().map(|m| {
                        json!({
                            "stream": s.stream_id,
                            "member": m.member,
                            "status": s.status,
                            "resolved": m.rejects.resolved,
                            "still_failing": m.rows.rejected,
                            "expired": m.rejects.expired,
                            "open": m.rejects.open,
                            "rows": {"inserted": m.rows.inserted, "updated": m.rows.updated},
                            "lake_snapshot": m.lake_snapshot,
                        })
                    })
                })
                .collect();
            let mut v = json!({
                "replication": rep.replication,
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
        "run_id": r.run_id,
        "status": r.status,
        "exit_code": r.exit_code,
        "warnings": r.warnings,
        "replications": reps,
    })
}

/// `rejects list` for the terminal (from [`rejects`]'s JSON rows): one block per reject.
pub fn list_text(rows: &Value) -> String {
    let rows: Vec<RejectRow> = serde_json::from_value(rows.clone()).unwrap_or_default();
    if rows.is_empty() {
        return "no open rejects\n".into();
    }
    let mut out = String::new();
    let mut last: Option<(&str, &str)> = None;
    for r in &rows {
        if last != Some((&r.stream_id, &r.member)) {
            let member = if r.member.is_empty() {
                String::new()
            } else {
                format!(" [{}]", r.member)
            };
            let n = rows
                .iter()
                .filter(|x| x.stream_id == r.stream_id && x.member == r.member)
                .count();
            out.push_str(&format!("{}{member}: {n} open\n", r.stream_id));
            last = Some((&r.stream_id, &r.member));
        }
        out.push_str(&format!(
            "  {}  {}: {}\n    attempts {}, first seen {} ({}), last seen {} ({})\n    request {}\n",
            r.reject_key,
            r.error_class,
            r.error,
            r.attempts,
            r.first_seen_at.as_deref().unwrap_or("?"),
            r.first_seen_run,
            r.last_seen_at.as_deref().unwrap_or("?"),
            r.last_seen_run,
            r.request
        ));
    }
    out
}
