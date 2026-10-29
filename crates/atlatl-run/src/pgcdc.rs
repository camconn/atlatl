//! Postgres logical replication into the lake (M6, 08 §10.1): one slot per replication, one
//! lake transaction per window, confirmed after the commit.

use std::sync::Arc;
use std::time::Duration;

use atlatl_conn_pgrepl::{Decommissioned, Lsn, PgChangeSource, config};
use atlatl_core::config::load::Project;
use atlatl_core::error::{AtlatlError, ErrorClass, Result};
use atlatl_core::lock::LockKey;
use atlatl_core::obs::report::{CdcReport, SlotReport};
use atlatl_core::plan::{ReplicationPlan, StreamRole};
use atlatl_core::redact::Redactor;
use atlatl_core::template::{EnvSource, Templates};
use tokio_util::sync::CancellationToken;

use crate::run::Output;
use crate::runtime::Runtime;

fn base_env(p: &Project) -> Option<String> {
    p.environments
        .as_ref()
        .and_then(|e| e.value.base.as_ref())
        .map(|b| b.value.clone())
}

fn killed(what: &str) -> AtlatlError {
    AtlatlError::new(ErrorClass::Killed, format!("killed {what}"))
}

fn cursor_json(lsn: Option<Lsn>, slot: &str) -> Option<serde_json::Value> {
    lsn.map(|l| serde_json::json!({"lsn": l.to_string(), "slot": slot}))
}

/// The columns [`run_unit`] would refuse, `(table, line)`, from the source catalog alone: no slot
/// or publication is touched.
pub async fn refused(
    project: &Project,
    rep: &ReplicationPlan,
    env: Option<&str>,
    vars: Arc<dyn EnvSource>,
) -> Result<Vec<(String, String)>> {
    let project_rep = project.replication(&rep.name).ok_or_else(|| {
        AtlatlError::internal(format!("replication `{}` not in project", rep.name))
    })?;
    let conn = crate::runtime::resolve_connection(project, &rep.source.name, env, vars)?;
    let base = base_env(project);
    let resolved = config::resolve(project_rep, &conn, env, base.as_deref())?;
    let mut src = PgChangeSource::open(resolved.opts, resolved.policy, resolved.spec).await?;
    let bad = src.refused().await;
    src.close().await?;
    bad
}

pub async fn run_unit(
    rt: &Runtime,
    unit: &[&ReplicationPlan],
    attempt: u32,
    cancel: CancellationToken,
) -> Result<Output> {
    let _ = attempt;
    let [rep] = unit else {
        return Err(AtlatlError::config(format!(
            "postgres `changes` replication `{}` can't share an atomic_group: each has its own slot and commit",
            unit[0].name
        )));
    };
    rt.report.start_replication(&rep.name);
    let project_rep = rt.project.replication(&rep.name).ok_or_else(|| {
        AtlatlError::internal(format!("replication `{}` not in project", rep.name))
    })?;
    let base = base_env(&rt.project);
    let resolved = config::resolve(
        project_rep,
        rt.connection(&rep.source.name)?,
        rt.plan.env.as_deref(),
        base.as_deref(),
    )?;
    let selected: Vec<&str> = rep
        .streams
        .iter()
        .filter(|s| s.role == StreamRole::Selected)
        .map(|s| s.id.as_str())
        .collect();
    if resolved
        .spec
        .streams
        .iter()
        .any(|s| !selected.contains(&s.id.as_str()))
    {
        tracing::warn!(
            replication = %rep.name,
            "postgres `changes` replications run all their streams (one slot covers them); ignoring the stream selection"
        );
    }
    let mut spec = resolved.spec;
    if let Some(t) = rep.timeout_ms {
        let t = Duration::from_millis(t) / 2;
        spec.limits.max_duration = Some(spec.limits.max_duration.map_or(t, |d| d.min(t)));
    }
    let lake = rt.lake(&rep.target.name)?.clone();

    let mut keys: Vec<LockKey> = spec
        .streams
        .iter()
        .map(|s| {
            LockKey::table(
                rep.target.name.clone(),
                s.target_schema.clone(),
                s.target_table.clone(),
            )
        })
        .collect();
    keys.sort_by_key(LockKey::name);
    keys.dedup();
    let lock_timeout = rep
        .lock_timeout_ms
        .map(Duration::from_millis)
        .unwrap_or(atlatl_core::exec::DEFAULT_LOCK_TIMEOUT);
    let guard = tokio::select! {
        g = rt.locks.acquire(&keys, lock_timeout) => g?,
        _ = cancel.cancelled() => return Err(killed("waiting for table locks")),
    };

    let mut session = rt.engine.session_with_cancel(cancel.clone())?;
    let l = lake.clone();
    let before = session.run(move |c| l.current_snapshot(c)).await?;
    let duck = session
        .run(|c| c.try_clone().map_err(atlatl_ingest::sql::db))
        .await?;
    let mut src = PgChangeSource::open(resolved.opts, resolved.policy, spec.clone()).await?;
    let t0 = std::time::Instant::now();
    let window = tokio::select! {
        w = src.changes(duck, &lake.alias, &rt.run_id) => w?,
        _ = cancel.cancelled() => return Err(killed("while streaming changes")),
    };
    if cancel.is_cancelled() {
        return Err(killed("before the lake commit"));
    }
    // A kill during the commit lets it finish; the unconfirmed window is skipped next run.
    let applied = tokio::task::spawn_blocking(move || window.apply())
        .await
        .map_err(|e| AtlatlError::internal(format!("apply task: {e}")))??;
    if let Some(l) = applied.confirm {
        src.confirm(l).await?;
    }
    let stats = src.stats().await?;
    src.close().await?;
    let _ = guard.release().await;

    let l = lake.clone();
    let after = session.run(move |c| l.current_snapshot(c)).await?;
    let snapshot = (after != before).then_some(after);
    let w = &applied.window;
    for (s, o) in spec.streams.iter().zip(&applied.streams) {
        let Some(sp) = rep.streams.iter().find(|p| p.id == s.id) else {
            continue;
        };
        let member = sp
            .members
            .first()
            .map(|m| m.name.clone())
            .unwrap_or_else(|| format!("{}.{}", s.source_schema, s.source_table));
        let slot = SlotReport {
            slot: w.slot.clone(),
            from_lsn: Some(w.from.to_string()),
            to_lsn: Some(o.cursor.to_string()),
            confirmed_flush_lsn: stats
                .as_ref()
                .and_then(|x| x.confirmed_flush_lsn)
                .map(|l| l.to_string()),
            lag_bytes: stats.as_ref().and_then(|x| x.lag_bytes),
            retained_bytes: stats.as_ref().and_then(|x| x.retained_bytes),
            catalog_xmin_age: stats.as_ref().and_then(|x| x.catalog_xmin_age),
            transactions: w.transactions,
            skipped_transactions: w.skipped_transactions,
        };
        let c = o.counts;
        rt.report.member(&rep.name, &sp.id, &member, |r| {
            r.object = Some(format!("{}.{}", lake.alias, o.target));
            r.rows.extracted = o.staged;
            r.rows.staged = o.staged;
            r.rows.inserted = c.inserted;
            r.rows.updated = c.updated;
            r.rows.deleted = c.deleted;
            r.rows.unchanged = c.unchanged;
            r.state.before = cursor_json(o.cursor_before, &w.slot);
            r.state.after = cursor_json(Some(o.cursor), &w.slot);
            r.lake_snapshot = snapshot.map(|s| s as u64);
            r.duration_ms = t0.elapsed().as_millis() as u64;
            r.cdc = Some(CdcReport {
                touched_keys: c.inserted + c.updated + c.unchanged + c.deleted,
                rows_inserted: c.inserted,
                rows_updated: c.updated,
                rows_unchanged: c.unchanged,
                rows_deleted: c.deleted,
                resynced: o.backfilled,
                slot: Some(slot.clone()),
                ..Default::default()
            });
        });
    }
    Ok(Output {
        up_to_date: w.changes == 0 && w.backfilled.is_empty(),
        lake_snapshot: snapshot,
    })
}

/// `atlatl cdc decommission`: drops a replication's slot and publication. The production guard
/// applies, so a dev environment can't drop a production slot either.
pub async fn decommission(
    project: &Project,
    replication: &str,
    env_flag: Option<&str>,
    force: bool,
    vars: Arc<dyn EnvSource>,
) -> Result<Decommissioned> {
    let envs = project.environments.as_ref().map(|e| &e.value);
    let env = atlatl_core::config::env::select(envs, env_flag, vars.as_ref())
        .map_err(|e| AtlatlError::config(e.to_string()))?;
    let rep = project
        .replication(replication)
        .ok_or_else(|| AtlatlError::config(format!("no replication `{replication}`")))?;
    let raw = project.connections.get(&rep.source.value).ok_or_else(|| {
        AtlatlError::config(format!("source `{}` is not a connection", rep.source.value))
    })?;
    let mut d = atlatl_core::config::Diagnostics::default();
    let conn = atlatl_core::config::env::resolve_connection(
        project,
        raw,
        env.name.as_deref(),
        &atlatl_core::config::env::Resolve::Full {
            vars,
            redactor: Redactor::global(),
        },
        &Templates::new(),
        &mut d,
    )
    .ok_or_else(|| {
        let msg = d
            .errors()
            .map(|e| e.error.to_string())
            .collect::<Vec<_>>()
            .join("; ");
        AtlatlError::config(format!("connection `{}`: {msg}", rep.source.value))
    })?;
    let r = config::resolve(
        rep,
        &conn,
        env.name.as_deref(),
        base_env(project).as_deref(),
    )?;
    atlatl_conn_pgrepl::decommission(&r.opts, &r.policy, replication, force).await
}
