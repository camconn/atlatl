//! Lake → SQLite / D1 / Turso CDC (08 §6.2–6.3) and their `cdc bootstrap` (08 §9). The window
//! logic mirrors `serve` (Postgres); the applier and the bootstrap diff are SQLite-family.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use arrow::array::{BooleanArray, RecordBatch};
use arrow::datatypes::SchemaRef;
use atlatl_cdc::bootstrap::Mode as BootMode;
use atlatl_cdc::{NetChanges, Window};
use atlatl_conn_ducklake::Lake;
use atlatl_conn_postgres::apply::Deletes as PgDeletes;
use atlatl_conn_sqlite::types::{Key, affinity, apply_affinity, column_values, iso};
use atlatl_conn_sqlite::{
    self as lite, Client, CursorMode, CursorPlan, CursorWrite, DeleteKey, Value as LiteValue,
};
use atlatl_core::config::model::{DeleteValue, OnGap};
use atlatl_core::error::{AtlatlError, ErrorClass, Result};
use atlatl_core::lock::LockKey;
use atlatl_core::obs::report::{CdcReport, CdcWindow};
use atlatl_core::plan::ReplicationPlan;
use atlatl_engine::{Engine, Session};
use atlatl_ingest::sql::ident;
use futures::StreamExt;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::run::Output;
use crate::runtime::Runtime;
use crate::serve::{
    BootstrapArgs, CdcStream, batches, chunk_queries, cleanup_sql, complete, memory_table, publish,
    rfc, streams_of,
};

pub const KINDS: &[&str] = &["sqlite", "d1", "turso"];

/// The SQLite-family target of a stream. SQLite has no schemas: `object` is `table` or
/// `main.table`.
fn lite_target(s: &CdcStream) -> Result<lite::Target> {
    let t = &s.target;
    if !matches!(t.schema.as_str(), "public" | "main") {
        return Err(AtlatlError::config(format!(
            "stream `{}`: SQLite-family targets have no schemas; `object` is `{}` or `main.{}`",
            s.id, t.table, t.table
        )));
    }
    Ok(lite::Target {
        table: t.table.clone(),
        primary_key: t.primary_key.clone(),
        columns: t.columns.clone(),
        target_types: t.target_types.clone(),
        deletes: match &t.deletes {
            PgDeletes::Soft {
                column,
                value,
                restore,
            } => lite::Deletes::Soft {
                column: column.clone(),
                value: *value,
                restore: *restore,
            },
            PgDeletes::Hard => lite::Deletes::Hard,
            PgDeletes::Ignore => lite::Deletes::Ignore,
        },
    })
}

/// The Arrow schema of `sql` (run with `LIMIT 0`).
async fn schema_of(session: &mut Session, sql: String) -> Result<SchemaRef> {
    session
        .run(move |c| {
            let mut st = c
                .prepare(&format!("SELECT * FROM ({sql}) LIMIT 0"))
                .map_err(atlatl_ingest::sql::db)?;
            Ok(st
                .query_arrow([])
                .map_err(atlatl_ingest::sql::db)?
                .get_schema())
        })
        .await
}

/// lake@`at` projected to the served columns (and filtered by `where:`).
fn lake_sql(s: &CdcStream, at: i64) -> String {
    let proj: Vec<String> = s
        .src
        .columns
        .iter()
        .map(|(n, e)| format!("{e} AS {}", ident(n)))
        .collect();
    let fq = format!(
        "{}.{}.{}",
        ident(&s.src.catalog),
        ident(&s.src.schema),
        ident(&s.src.table)
    );
    let filter = s
        .src
        .filter
        .as_ref()
        .map(|f| format!(" WHERE {f}"))
        .unwrap_or_default();
    format!(
        "SELECT {} FROM {fq} AT (VERSION => {at}){filter}",
        proj.join(", ")
    )
}

/// A bootstrap diff (adopt or reload) of lake@S against the live target, in SQLite values.
struct LiteDiff {
    schema: SchemaRef,
    upserts: Vec<RecordBatch>,
    upsert_rows: u64,
    deletes: Vec<DeleteKey>,
    samples: Vec<Value>,
}

#[allow(clippy::too_many_arguments)]
async fn lite_diff(
    engine: &Engine,
    session: &mut Session,
    client: &mut Client,
    s: &CdcStream,
    t: &lite::Target,
    at: i64,
    mode: BootMode,
    stamp: Option<String>,
) -> Result<LiteDiff> {
    let sql = lake_sql(s, at);
    let schema = schema_of(session, sql.clone()).await?;
    let info = client.table_info(&t.table).await?;
    let soft = match &t.deletes {
        lite::Deletes::Soft { column, value, .. } => {
            Some((column.clone(), *value == DeleteValue::True))
        }
        _ => None,
    };
    let compared: Vec<String> = t
        .columns
        .iter()
        .filter(|c| !t.primary_key.contains(c) && !s.src.ignore.contains(c))
        .cloned()
        .collect();
    // Target rows by key: compared values, and whether the row is soft-deleted.
    let mut target: HashMap<Key, (Vec<LiteValue>, bool)> = HashMap::new();
    if let Some(info) = &info {
        let soft_col = soft
            .as_ref()
            .filter(|(c, _)| info.decl(c).is_some())
            .map(|(c, b)| (c.clone(), *b));
        let mut cols: Vec<String> = t.primary_key.clone();
        cols.extend(compared.iter().filter(|c| info.decl(c).is_some()).cloned());
        let n_cmp = cols.len() - t.primary_key.len();
        if let Some((c, _)) = &soft_col {
            cols.push(c.clone());
        }
        for mut r in client.select_all(&t.table, &cols, &t.primary_key).await? {
            let deleted = match &soft_col {
                Some((_, b)) => {
                    let v = r.pop().unwrap_or(LiteValue::Null);
                    if *b {
                        v.as_i64().is_some_and(|i| i != 0)
                    } else {
                        v != LiteValue::Null
                    }
                }
                None => false,
            };
            let rest = r.split_off(t.primary_key.len());
            debug_assert_eq!(rest.len(), n_cmp);
            target.insert(Key(r), (rest, deleted));
        }
    }
    let restores = matches!(&t.deletes, lite::Deletes::Soft { restore: true, column, .. } if !t.columns.contains(column));
    let aff = |c: &str| info.as_ref().and_then(|i| i.decl(c)).map(affinity);
    let mut seen: HashSet<Key> = HashSet::new();
    let mut out = LiteDiff {
        schema,
        upserts: Vec::new(),
        upsert_rows: 0,
        deletes: Vec::new(),
        samples: Vec::new(),
    };
    let mut stream = std::pin::pin!(batches(engine, sql).await?);
    while let Some(b) = stream.next().await {
        let b = b?;
        let conv = |c: &str| -> Result<Vec<LiteValue>> {
            let i = b
                .schema()
                .index_of(c)
                .map_err(|_| AtlatlError::internal(format!("lake projection has no `{c}`")))?;
            let a = b.column(i);
            let v = column_values(a, t.store(c, a.data_type())?, c)?;
            Ok(match aff(c) {
                Some(af) => v.into_iter().map(|x| apply_affinity(af, x)).collect(),
                None => v,
            })
        };
        let pks: Vec<Vec<LiteValue>> = t
            .primary_key
            .iter()
            .map(|k| conv(k))
            .collect::<Result<_>>()?;
        let present: Vec<String> = compared
            .iter()
            .filter(|c| info.as_ref().is_none_or(|i| i.decl(c).is_some()))
            .cloned()
            .collect();
        let cmp: Vec<Vec<LiteValue>> = present.iter().map(|c| conv(c)).collect::<Result<_>>()?;
        let mut mask = Vec::with_capacity(b.num_rows());
        for i in 0..b.num_rows() {
            let key = Key(pks.iter().map(|c| c[i].clone()).collect());
            let row: Vec<LiteValue> = cmp.iter().map(|c| c[i].clone()).collect();
            let up = match (mode, target.get(&key)) {
                (BootMode::Reload, _) | (_, None) => true,
                (BootMode::Adopt, Some((have, deleted))) => {
                    lite::types::Key(have.clone()) != lite::types::Key(row)
                        || (*deleted && restores)
                }
            };
            if up && out.samples.len() < 5 {
                out.samples.push(json!({"op": "upsert", "key": key.0.iter().map(LiteValue::to_json).collect::<Vec<_>>()}));
            }
            mask.push(up);
            if !seen.insert(key) {
                return Err(AtlatlError::data(format!(
                    "stream `{}`: duplicate primary key in lake@{at}",
                    s.id
                )));
            }
        }
        let n = mask.iter().filter(|m| **m).count();
        if n > 0 {
            let f = arrow::compute::filter_record_batch(&b, &BooleanArray::from(mask))
                .map_err(|e| AtlatlError::internal(e.to_string()))?;
            out.upsert_rows += n as u64;
            out.upserts.push(f);
        }
    }
    for (key, (_, deleted)) in target {
        if seen.contains(&key) || (deleted && soft.is_some()) {
            continue;
        }
        if out.samples.len() < 10 {
            out.samples.push(json!({"op": "delete", "key": key.0.iter().map(LiteValue::to_json).collect::<Vec<_>>()}));
        }
        out.deletes.push(DeleteKey {
            pk: key.0,
            ts: stamp.clone(),
        });
    }
    Ok(out)
}

/// What one stream applies: net changes published to memory tables, or a bootstrap diff.
enum Changes {
    Published { prefix: String, nc: NetChanges },
    Diff(LiteDiff),
}

struct Planned {
    stream: usize,
    target: lite::Target,
    from: Option<i64>,
    to: i64,
    table_id: i64,
    changes: Changes,
    touched: u64,
    lag: atlatl_cdc::Lag,
    resynced: bool,
}

impl Planned {
    fn rows(&self) -> u64 {
        match &self.changes {
            Changes::Published { nc, .. } => nc.upsert_rows + nc.delete_rows,
            Changes::Diff(d) => d.upsert_rows + d.deletes.len() as u64,
        }
    }
}

#[derive(Default)]
struct Applied {
    up: lite::ApplyCounts,
    del: lite::ApplyCounts,
}

/// Prepares every target (creating missing tables), then applies upserts parent-first,
/// deletes child-first, and the cursors.
#[allow(clippy::too_many_arguments)]
async fn apply(
    engine: &Engine,
    lake: &Lake,
    session: &mut Session,
    client: &mut Client,
    streams: &[CdcStream],
    planned: &[Planned],
    mode: CursorMode,
) -> Result<Vec<Applied>> {
    let mut times = BTreeMap::new();
    for p in planned {
        let (l, to) = (lake.clone(), p.to);
        times.insert(p.to, session.run(move |c| l.snapshot_time(c, to)).await?);
    }
    for p in planned {
        let schema = match &p.changes {
            Changes::Published { prefix, .. } => {
                schema_of(
                    session,
                    format!("SELECT * FROM {}", memory_table(prefix, "up")),
                )
                .await?
            }
            Changes::Diff(d) => d.schema.clone(),
        };
        client.prepare(&p.target, &schema).await?;
    }
    let plans: Vec<CursorPlan> = planned
        .iter()
        .map(|p| {
            let s = &streams[p.stream];
            CursorPlan {
                expected: p.from,
                write: CursorWrite {
                    stream_id: s.id.clone(),
                    source_table: format!("{}.{}", s.src.schema, s.src.table),
                    source_table_id: p.table_id,
                    snapshot_id: p.to,
                    snapshot_time: times.get(&p.to).copied().flatten(),
                    mode,
                    config_hash: s.config_hash.clone(),
                },
            }
        })
        .collect();
    let total = planned.iter().map(Planned::rows).sum();
    let mut out: Vec<Applied> = planned.iter().map(|_| Applied::default()).collect();
    let mut tx = client.begin(plans, total).await?;
    for (i, p) in planned.iter().enumerate() {
        let s = &streams[p.stream];
        match &p.changes {
            Changes::Published { prefix, nc } => {
                for sql in chunk_queries(
                    &memory_table(prefix, "up"),
                    &s.target.primary_key,
                    nc.upsert_rows,
                    s.chunk_rows,
                ) {
                    out[i].up += tx.upsert(&p.target, batches(engine, sql).await?).await?;
                }
            }
            Changes::Diff(d) => {
                out[i].up += tx
                    .upsert(
                        &p.target,
                        futures::stream::iter(d.upserts.iter().cloned().map(Ok)),
                    )
                    .await?
            }
        }
    }
    for (i, p) in planned.iter().enumerate().rev() {
        let s = &streams[p.stream];
        match &p.changes {
            Changes::Published { prefix, nc } => {
                for sql in chunk_queries(
                    &memory_table(prefix, "del"),
                    &s.target.primary_key,
                    nc.delete_rows,
                    s.chunk_rows,
                ) {
                    out[i].del += tx.delete(&p.target, batches(engine, sql).await?).await?;
                }
            }
            Changes::Diff(d) => out[i].del += tx.delete_keys(&p.target, d.deletes.clone()).await?,
        }
    }
    tx.commit().await?;
    Ok(out)
}

async fn lock(
    rt: &Runtime,
    streams: &[CdcStream],
    planned: &[Planned],
    target: &str,
    timeout: Duration,
    cancel: &CancellationToken,
) -> Result<Box<dyn atlatl_core::lock::LockGuard>> {
    let mut keys: Vec<LockKey> = planned
        .iter()
        .map(|p| streams[p.stream].lock_key(target))
        .collect();
    keys.sort_by_key(LockKey::name);
    keys.dedup();
    tokio::select! {
        g = rt.locks.acquire(&keys, timeout) => g,
        _ = cancel.cancelled() => Err(AtlatlError::new(ErrorClass::Killed, "killed waiting for table locks")),
    }
}

async fn stamp(session: &mut Session, lake: &Lake, at: i64) -> Result<Option<String>> {
    let l = lake.clone();
    Ok(session.run(move |c| l.snapshot_time(c, at)).await?.map(iso))
}

pub async fn run_unit(
    rt: &Runtime,
    unit: &[&ReplicationPlan],
    attempt: u32,
    cancel: CancellationToken,
) -> Result<Output> {
    let _ = attempt;
    let first = unit[0];
    let lake = rt.lake(&first.source.name)?.clone();
    for r in unit {
        rt.report.start_replication(&r.name);
    }
    let streams = streams_of(&rt.project, &lake.alias, unit)?;
    let mut client = Client::open(rt.connection(&first.target.name)?).await?;
    client.ensure_state().await?;
    let ids: Vec<&str> = streams.iter().map(|s| s.id.as_str()).collect();
    let cursors: BTreeMap<String, lite::Cursor> = client
        .read_cursors(&ids)
        .await?
        .into_iter()
        .map(|c| (c.stream_id.clone(), c))
        .collect();
    for s in &streams {
        match cursors.get(&s.id) {
            None => {
                return Err(AtlatlError::config(format!(
                    "stream `{}` has no cursor in the target; run `atlatl cdc bootstrap {} --mode adopt|reload` first",
                    s.id, s.replication
                ))
                .code("bootstrap_required"));
            }
            Some(c) if c.config_hash != s.config_hash => {
                return Err(AtlatlError::config(format!(
                    "the cursor for `{}` was produced by a different definition; review, then re-run `atlatl cdc bootstrap {} --mode adopt`",
                    s.id, s.replication
                ))
                .code("config_drift"));
            }
            _ => {}
        }
    }
    // A pending window (08 §6.3) pins E: it is re-applied before anything new.
    let pinned: HashSet<i64> = cursors
        .values()
        .filter_map(|c| c.pending.as_ref().map(|p| p.snapshot_id))
        .collect();
    if pinned.len() > 1 {
        return Err(AtlatlError::config(format!(
            "the streams of `{}` have different pending windows; re-run `atlatl cdc bootstrap`",
            first.name
        ))
        .code("pending_mismatch"));
    }
    let pinned = pinned.into_iter().next();

    let mut session = rt.engine.session_with_cancel(cancel.clone())?;
    let l = lake.clone();
    let head = session.run(move |c| l.current_snapshot(c)).await?;
    let mut streams = streams;
    complete(&mut session, &mut streams, pinned.unwrap_or(head)).await?;

    let mut checked = Vec::new();
    for s in &streams {
        let cur = cursors[&s.id].clone();
        let (src, on_gap, ms, mc) = (s.src.clone(), s.on_gap, s.max_snapshots, s.max_changes);
        let r = session
            .run(move |c| {
                let gap = atlatl_cdc::guard_retention(c, &src.catalog, cur.snapshot_id)
                    .and_then(|_| atlatl_cdc::guard_identity(c, &src, Some(cur.source_table_id)));
                match gap {
                    Ok(tid) if pinned.is_some() => Ok((tid, false, pinned.unwrap_or(head))),
                    Ok(tid) => Ok((
                        tid,
                        false,
                        atlatl_cdc::bound(c, &src, tid, cur.snapshot_id, head, ms, mc)?,
                    )),
                    Err(e)
                        if on_gap == OnGap::Resync
                            && pinned.is_none()
                            && matches!(
                                e.code.as_deref(),
                                Some("cursor_expired" | "resync_required")
                            ) =>
                    {
                        Ok((atlatl_cdc::guard_identity(c, &src, None)?, true, head))
                    }
                    Err(e) => Err(e),
                }
            })
            .await?;
        if let Some(p) = &cursors[&s.id].pending
            && p.fingerprint
                != lite::fingerprint(
                    &s.config_hash,
                    cursors[&s.id].snapshot_id,
                    p.snapshot_id,
                    r.0,
                )
        {
            return Err(AtlatlError::config(format!(
                "the pending window of `{}` was started by a different definition or source table; re-run `atlatl cdc bootstrap {} --mode adopt`",
                s.id, s.replication
            ))
            .code("pending_mismatch"));
        }
        checked.push(r);
    }
    let end = checked.iter().map(|(_, _, e)| *e).min().unwrap_or(head);
    let caught_up = end >= head;

    let prefix_base = format!("_atlatl_cdc_{}", rt.run_id.to_ascii_lowercase());
    let mut planned: Vec<Planned> = Vec::new();
    let mut prefixes = Vec::new();
    let result = async {
        for (i, (s, (table_id, resync, _))) in streams.iter().zip(&checked).enumerate() {
            let from = cursors[&s.id].snapshot_id;
            if !resync && from >= end {
                continue;
            }
            if cancel.is_cancelled() {
                return Err(AtlatlError::new(ErrorClass::Killed, "cancelled"));
            }
            let target = lite_target(s)?;
            let (src, tid) = (s.src.clone(), *table_id);
            let l = session
                .run(move |c| atlatl_cdc::lag(c, &src.catalog, from, end))
                .await?;
            if *resync {
                let ts = stamp(&mut session, &lake, end).await?;
                let d = lite_diff(
                    &rt.engine,
                    &mut session,
                    &mut client,
                    s,
                    &target,
                    end,
                    BootMode::Adopt,
                    ts,
                )
                .await?;
                let touched = d.upsert_rows + d.deletes.len() as u64;
                planned.push(Planned {
                    stream: i,
                    target,
                    from: Some(from),
                    to: end,
                    table_id: tid,
                    changes: Changes::Diff(d),
                    touched,
                    lag: l,
                    resynced: true,
                });
                continue;
            }
            let prefix = format!("{prefix_base}_{i}");
            prefixes.push(prefix.clone());
            let (src, p, stamp_ts) = (s.src.clone(), prefix.clone(), s.stamps_snapshot_time());
            let nc = session
                .run(move |c| {
                    let w = Window { from, to: end };
                    let nc = atlatl_cdc::net_changes(c, &src, w, &p)?;
                    if stamp_ts {
                        atlatl_cdc::stamp_deletes(c, &src, tid, w, &nc)?;
                    }
                    Ok(nc)
                })
                .await?;
            let (st, ncc, pp) = (s.clone(), nc.clone(), prefix.clone());
            session.run(move |c| publish(c, &st, &ncc, &pp)).await?;
            let touched = nc.touched;
            planned.push(Planned {
                stream: i,
                target,
                from: Some(from),
                to: end,
                table_id: tid,
                changes: Changes::Published { prefix, nc },
                touched,
                lag: l,
                resynced: false,
            });
        }
        if planned.is_empty() {
            return Ok(None);
        }
        let timeout = first
            .lock_timeout_ms
            .map(Duration::from_millis)
            .unwrap_or(atlatl_core::exec::DEFAULT_LOCK_TIMEOUT);
        let guard = lock(rt, &streams, &planned, &first.target.name, timeout, &cancel).await?;
        let r = apply(
            &rt.engine,
            &lake,
            &mut session,
            &mut client,
            &streams,
            &planned,
            CursorMode::Cdc,
        )
        .await;
        let _ = guard.release().await;
        r.map(Some)
    }
    .await;
    let sql = cleanup_sql(&prefixes);
    let _ = session
        .run(move |c| c.execute_batch(&sql).map_err(atlatl_ingest::sql::db))
        .await;
    let applied = result?;

    let l = lake.clone();
    let (from_times, to_time) = {
        let froms: Vec<i64> = streams.iter().map(|s| cursors[&s.id].snapshot_id).collect();
        session
            .run(move |c| {
                let f = froms
                    .iter()
                    .map(|x| l.snapshot_time(c, *x))
                    .collect::<Result<Vec<_>>>()?;
                Ok((f, l.snapshot_time(c, end)?))
            })
            .await?
    };
    for (i, s) in streams.iter().enumerate() {
        let from = cursors[&s.id].snapshot_id;
        let p = planned.iter().position(|p| p.stream == i);
        let counts = match (&applied, p) {
            (Some(a), Some(j)) => Some(&a[j]),
            _ => None,
        };
        rt.report.member(&s.replication, &s.id, &s.member, |r| {
            r.object = Some(s.target.table.clone());
            let mut cdc = CdcReport {
                window: CdcWindow {
                    from_snapshot: Some(from as u64),
                    to_snapshot: Some(end.max(from) as u64),
                    from_time: rfc(from_times[i]),
                    to_time: rfc(to_time),
                },
                ..Default::default()
            };
            if let (Some(j), Some(c)) = (p, counts) {
                let pl = &planned[j];
                cdc.touched_keys = pl.touched;
                cdc.upserts_sent = c.up.upserts_sent;
                cdc.rows_inserted = c.up.rows_inserted;
                cdc.rows_updated = c.up.rows_updated;
                cdc.rows_unchanged = c.up.rows_unchanged;
                cdc.rows_deleted = c.del.rows_deleted;
                cdc.deletes_ignored = c.del.deletes_ignored;
                cdc.snapshots_behind = pl.lag.snapshots_behind;
                cdc.seconds_behind = pl.lag.seconds_behind;
                cdc.resynced = pl.resynced;
                r.rows.extracted = pl.rows();
                r.rows.staged = c.up.upserts_sent;
                r.rows.inserted = c.up.rows_inserted;
                r.rows.updated = c.up.rows_updated;
                r.rows.unchanged = c.up.rows_unchanged;
                r.rows.deleted = c.del.rows_deleted;
                r.lake_snapshot = Some(end as u64);
            }
            r.state.before = Some(json!({"snapshot_id": from}));
            r.state.after = Some(
                json!({"snapshot_id": if p.is_some() && applied.is_some() { end } else { from }}),
            );
            r.cdc = Some(cdc);
        });
    }
    if !caught_up {
        for r in unit {
            rt.report.replication(&r.name, |x| x.partial = true);
        }
    }
    Ok(Output {
        up_to_date: applied.is_none(),
        lake_snapshot: applied.map(|_| end),
    })
}

/// `atlatl cdc bootstrap` for one lake → SQLite-family replication: the diff is computed in
/// atlatl against the live target (read back through the connector), then applied with
/// `cursor = S` like a window.
pub async fn bootstrap_rep(
    rt: &Runtime,
    rep: &ReplicationPlan,
    args: &BootstrapArgs,
) -> Result<Value> {
    let lake = rt.lake(&rep.source.name)?.clone();
    let streams = streams_of(&rt.project, &lake.alias, &[rep])?;
    let mut client = Client::open(rt.connection(&rep.target.name)?).await?;
    if !args.dry_run {
        client.ensure_state().await?;
    }
    let ids: Vec<&str> = streams.iter().map(|s| s.id.as_str()).collect();
    let existing: BTreeMap<String, i64> = match client.read_cursors(&ids).await {
        Ok(v) => v
            .into_iter()
            .map(|c| (c.stream_id, c.snapshot_id))
            .collect(),
        Err(_) if args.dry_run => BTreeMap::new(),
        Err(e) => return Err(e),
    };
    let mut session = rt.engine.session()?;
    let l = lake.clone();
    let at = match args.at_snapshot {
        Some(s) => s,
        None => session.run(move |c| l.current_snapshot(c)).await?,
    };
    let mut streams = streams;
    complete(&mut session, &mut streams, at).await?;
    let ts = stamp(&mut session, &lake, at).await?;
    let mut planned = Vec::new();
    let mut report = Vec::new();
    for (i, s) in streams.iter().enumerate() {
        let target = lite_target(s)?;
        let src = s.src.clone();
        let tid = session
            .run(move |c| {
                if !atlatl_cdc::snapshot_exists(c, &src.catalog, at)? {
                    return Err(AtlatlError::config(format!("snapshot {at} does not exist")));
                }
                atlatl_cdc::guard_identity(c, &src, None)
            })
            .await?;
        let d = if args.advance_cursor_only {
            LiteDiff {
                schema: schema_of(&mut session, lake_sql(s, at)).await?,
                upserts: Vec::new(),
                upsert_rows: 0,
                deletes: Vec::new(),
                samples: Vec::new(),
            }
        } else {
            lite_diff(
                &rt.engine,
                &mut session,
                &mut client,
                s,
                &target,
                at,
                args.mode,
                ts.clone(),
            )
            .await?
        };
        report.push(json!({
            "stream": s.id,
            "target": target.table,
            "at_snapshot": at,
            "cursor_before": existing.get(&s.id),
            "upserts": d.upsert_rows,
            "deletes": d.deletes.len(),
            "samples": args.dry_run.then(|| d.samples.clone()),
        }));
        let touched = d.upsert_rows + d.deletes.len() as u64;
        planned.push(Planned {
            stream: i,
            target,
            from: existing.get(&s.id).copied(),
            to: at,
            table_id: tid,
            changes: Changes::Diff(d),
            touched,
            lag: Default::default(),
            resynced: false,
        });
    }
    if !args.dry_run && !planned.is_empty() {
        let guard = lock(
            rt,
            &streams,
            &planned,
            &rep.target.name,
            atlatl_core::exec::DEFAULT_LOCK_TIMEOUT,
            &CancellationToken::new(),
        )
        .await?;
        let r = apply(
            &rt.engine,
            &lake,
            &mut session,
            &mut client,
            &streams,
            &planned,
            CursorMode::Bootstrap,
        )
        .await;
        let _ = guard.release().await;
        for (p, c) in planned.iter().zip(r?) {
            if let Some(v) = report.get_mut(p.stream) {
                v["rows_inserted"] = json!(c.up.rows_inserted);
                v["rows_updated"] = json!(c.up.rows_updated);
                v["rows_deleted"] = json!(c.del.rows_deleted);
            }
        }
    }
    Ok(
        json!({"replication": rep.name, "mode": args.mode, "dry_run": args.dry_run, "advance_cursor_only": args.advance_cursor_only, "streams": report}),
    )
}
