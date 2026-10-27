//! Lake → Postgres CDC (08, M1.6) and `atlatl cdc bootstrap` (08 §9, M1.7). Every stream of a
//! replication (unit) applies in one target transaction together with its cursor.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow::array::RecordBatch;
use atlatl_cdc::bootstrap::{self, Mode as BootMode, Target as BootTarget};
use atlatl_cdc::{NetChanges, Source, Window};
use atlatl_conn_ducklake::Lake;
use atlatl_conn_postgres::apply::{ApplyCounts, ApplyOptions, Deletes, PgApplier, StreamTarget};
use atlatl_conn_postgres::cursors::{
    Cursor, CursorMode, CursorWrite, ensure_state_schema, manages_state_schema, read_cursors,
};
use atlatl_conn_postgres::{ConnectOptions, connect};
use atlatl_core::config::load::Project;
use atlatl_core::config::model::{
    ConnectionKind, DeleteMode, DeleteValue, OnGap, PostgresConn, SslMode,
};
use atlatl_core::error::{AtlatlError, ErrorClass, Result};
use atlatl_core::lock::LockKey;
use atlatl_core::obs::report::{CdcReport, CdcWindow};
use atlatl_core::plan::{self, PlanRequest, ReplicationPlan, StreamPlan, StreamRole};
use atlatl_core::redact::Redactor;
use atlatl_core::template::EnvSource;
use atlatl_engine::{Engine, Session, StreamOptions};
use atlatl_ingest::sql::{ident, literal};
use futures::Stream;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::lake::split_object;
use crate::run::Output;
use crate::runtime::Runtime;

const PG_ATTACH: &str = "atlatl_pg_target";

/// One CDC stream, resolved from config.
#[derive(Clone)]
pub(crate) struct CdcStream {
    pub(crate) replication: String,
    pub(crate) id: String,
    pub(crate) member: String,
    pub(crate) config_hash: String,
    pub(crate) src: Source,
    pub(crate) target: StreamTarget,
    pub(crate) on_gap: OnGap,
    pub(crate) max_snapshots: Option<u64>,
    pub(crate) max_changes: Option<u64>,
    /// Rows per apply chunk (`diff.chunk_rows`); larger diffs go in PK-hash chunks, same transaction.
    pub(crate) chunk_rows: u64,
    /// No `columns.include`: serve every lake column at E except these.
    pub(crate) all_except: Option<Vec<String>>,
    pub(crate) col_cfg: atlatl_core::config::model::ColumnsConfig,
}

impl CdcStream {
    pub(crate) fn stamps_snapshot_time(&self) -> bool {
        matches!(
            &self.target.deletes,
            Deletes::Soft {
                value: DeleteValue::SnapshotTime,
                ..
            }
        )
    }

    /// The soft-delete column for the bootstrap diff, and whether it is boolean.
    fn soft_column(&self) -> (Option<String>, bool) {
        match &self.target.deletes {
            Deletes::Soft { column, value, .. } => {
                (Some(column.clone()), *value == DeleteValue::True)
            }
            _ => (None, false),
        }
    }

    pub(crate) fn lock_key(&self, target_conn: &str) -> LockKey {
        LockKey::table(
            target_conn.to_owned(),
            self.target.schema.clone(),
            self.target.table.clone(),
        )
    }
}

fn resolve_stream(
    project: &Project,
    lake_alias: &str,
    rep: &ReplicationPlan,
    sp: &StreamPlan,
) -> Result<CdcStream> {
    let s = project
        .stream(&sp.id)
        .map(|(_, s)| s)
        .ok_or_else(|| AtlatlError::internal(format!("stream `{}`", sp.id)))?;
    let c = &s.config;
    let table = c
        .from
        .as_ref()
        .and_then(|f| f.table.clone())
        .ok_or_else(|| AtlatlError::config(format!("cdc stream `{}` needs `from.table`", sp.id)))?;
    let (schema, table) = split_object(&table);
    let pk = sp
        .primary_key
        .clone()
        .filter(|p| !p.is_empty())
        .ok_or_else(|| {
            AtlatlError::config(format!("cdc stream `{}` needs a primary_key", sp.id))
        })?;
    let cols_cfg = c.columns.clone().unwrap_or_default();
    let all_except = cols_cfg
        .include
        .is_none()
        .then(|| cols_cfg.exclude.clone().unwrap_or_default());
    let include = cols_cfg.include.clone().unwrap_or_default();
    let (columns, target_types) = served(&include, &cols_cfg);
    if all_except.is_none() {
        for k in &pk {
            if !include.contains(k) {
                return Err(AtlatlError::config(format!(
                    "cdc stream `{}`: primary key `{k}` must be in `columns.include`",
                    sp.id
                )));
            }
        }
    }
    let d = c
        .deletes
        .as_ref()
        .map(|d| d.value.clone())
        .unwrap_or_default();
    let deletes = match d.mode.unwrap_or(DeleteMode::Soft) {
        DeleteMode::Soft => Deletes::Soft {
            column: d.column.unwrap_or_else(|| "deleted_at".into()),
            value: d.value.unwrap_or(DeleteValue::SnapshotTime),
            restore: d.restore.unwrap_or(true),
        },
        DeleteMode::Hard => Deletes::Hard,
        DeleteMode::Ignore => Deletes::Ignore,
    };
    let member = sp
        .members
        .first()
        .ok_or_else(|| AtlatlError::internal(format!("stream `{}` has no member", sp.id)))?;
    let object = member
        .object
        .clone()
        .ok_or_else(|| AtlatlError::config(format!("stream `{}` has no rendered object", sp.id)))?;
    let (t_schema, t_table) = match object.split_once('.') {
        Some((a, b)) => (a.to_owned(), b.to_owned()),
        None => ("public".to_owned(), object),
    };
    Ok(CdcStream {
        replication: rep.name.clone(),
        id: sp.id.clone(),
        member: member.name.clone(),
        config_hash: sp.config_hash.clone(),
        src: Source {
            catalog: lake_alias.to_owned(),
            schema,
            table,
            primary_key: pk.clone(),
            columns,
            ignore: c
                .diff
                .as_ref()
                .and_then(|d| d.ignore_columns.clone())
                .unwrap_or_default(),
            filter: c.window.as_ref().map(|w| w.value.clone()),
        },
        target: StreamTarget {
            schema: t_schema,
            table: t_table,
            primary_key: pk,
            columns: include,
            target_types,
            deletes,
        },
        on_gap: c.on_gap.unwrap_or(OnGap::Fail),
        max_snapshots: c.max_snapshots,
        max_changes: c.max_changes,
        chunk_rows: c
            .diff
            .as_ref()
            .and_then(|d| d.chunk_rows)
            .unwrap_or(1_000_000)
            .max(1),
        all_except,
        col_cfg: cols_cfg,
    })
}

type Served = (Vec<(String, String)>, BTreeMap<String, String>);

fn served(include: &[String], cfg: &atlatl_core::config::model::ColumnsConfig) -> Served {
    let mut columns = Vec::new();
    let mut target_types = BTreeMap::new();
    for col in include {
        let c = cfg.columns.get(col);
        columns.push((
            col.clone(),
            c.and_then(|x| x.transform.clone())
                .unwrap_or_else(|| ident(col)),
        ));
        if let Some(t) = c.and_then(|x| x.target_type.clone()) {
            target_types.insert(col.clone(), t);
        }
    }
    (columns, target_types)
}

/// Fills the served columns of streams without `columns.include` from the lake table at `at`.
pub(crate) async fn complete(
    session: &mut Session,
    streams: &mut [CdcStream],
    at: i64,
) -> Result<()> {
    for s in streams.iter_mut() {
        let Some(except) = s.all_except.clone() else {
            continue;
        };
        let src = s.src.clone();
        let cols = session
            .run(move |c| atlatl_cdc::columns_at(c, &src, at))
            .await?;
        let include: Vec<String> = cols.into_iter().filter(|c| !except.contains(c)).collect();
        let (columns, types) = served(&include, &s.col_cfg);
        s.src.columns = columns;
        s.target.columns = include;
        s.target.target_types = types;
    }
    Ok(())
}

pub(crate) fn streams_of(
    project: &Project,
    lake_alias: &str,
    unit: &[&ReplicationPlan],
) -> Result<Vec<CdcStream>> {
    unit.iter()
        .flat_map(|r| {
            r.streams
                .iter()
                .filter(|s| s.role != StreamRole::FeedOnly)
                .map(move |s| (*r, s))
        })
        .map(|(r, s)| resolve_stream(project, lake_alias, r, s))
        .collect()
}

fn pg_conn(
    conns: &indexmap::IndexMap<String, atlatl_core::config::connection::Connection>,
    name: &str,
) -> Result<(ConnectOptions, PostgresConn)> {
    let c = conns
        .get(name)
        .ok_or_else(|| AtlatlError::internal(format!("connection `{name}` not resolved")))?;
    let ConnectionKind::Postgres(p) = &c.kind else {
        return Err(AtlatlError::config(format!(
            "`{name}` is not a postgres connection"
        )));
    };
    Ok((ConnectOptions::from_connection(c)?, p.clone()))
}

/// libpq keyword/value string for DuckDB's postgres attach.
fn libpq_dsn(o: &ConnectOptions) -> String {
    let q = |v: &str| format!("'{}'", v.replace('\\', "\\\\").replace('\'', "\\'"));
    let mut parts = vec![
        format!("host={}", q(&o.host)),
        format!("port={}", o.port),
        format!("dbname={}", q(&o.database)),
        format!("user={}", q(&o.user)),
        format!(
            "sslmode={}",
            match o.sslmode {
                SslMode::Disable => "disable",
                SslMode::Require => "require",
                SslMode::VerifyFull => "verify-full",
            }
        ),
    ];
    if let Some(p) = &o.password {
        parts.push(format!("password={}", q(p)));
    }
    if let Some(r) = &o.sslrootcert {
        parts.push(format!("sslrootcert={}", q(&r.display().to_string())));
    }
    parts.join(" ")
}

fn attach_pg(c: &duckdb::Connection, o: &ConnectOptions) -> Result<()> {
    c.execute_batch(&format!(
        "LOAD postgres_scanner; DETACH DATABASE IF EXISTS {PG_ATTACH}; ATTACH {} AS {PG_ATTACH} (TYPE postgres, READ_ONLY)",
        literal(&libpq_dsn(o))
    ))
    .map_err(atlatl_ingest::sql::db)
}

pub(crate) fn memory_table(prefix: &str, what: &str) -> String {
    format!("memory.main.{}", ident(&format!("{prefix}_{what}")))
}

/// Copies the session TEMP results into the in-memory catalog so pooled connections can stream
/// them, then drops the TEMP tables.
pub(crate) fn publish(
    c: &duckdb::Connection,
    s: &CdcStream,
    nc: &NetChanges,
    prefix: &str,
) -> Result<()> {
    let cols = s
        .target
        .columns
        .iter()
        .map(|x| ident(x))
        .collect::<Vec<_>>()
        .join(", ");
    let pk = s
        .target
        .primary_key
        .iter()
        .map(|x| ident(x))
        .collect::<Vec<_>>()
        .join(", ");
    c.execute_batch(&format!(
        "CREATE OR REPLACE TABLE {up} AS SELECT {cols} FROM {tu};
         CREATE OR REPLACE TABLE {del} AS SELECT {pk}, _ts::TIMESTAMPTZ AS _ts FROM {td};
         DROP TABLE IF EXISTS {tu}; DROP TABLE IF EXISTS {td};",
        up = memory_table(prefix, "up"),
        del = memory_table(prefix, "del"),
        tu = ident(&nc.upserts),
        td = ident(&nc.deletes),
    ))
    .map_err(atlatl_ingest::sql::db)
}

pub(crate) fn cleanup_sql(prefixes: &[String]) -> String {
    prefixes
        .iter()
        .flat_map(|p| {
            ["up", "del"].map(|w| format!("DROP TABLE IF EXISTS {};", memory_table(p, w)))
        })
        .chain(prefixes.iter().flat_map(|p| {
            ["touched", "before", "after", "up", "del"]
                .map(|w| format!("DROP TABLE IF EXISTS {};", ident(&format!("{p}_{w}"))))
        }))
        .collect()
}

pub(crate) async fn batches(
    engine: &Engine,
    sql: String,
) -> Result<impl Stream<Item = Result<RecordBatch>> + use<>> {
    let s = engine.stream(&sql, StreamOptions::default()).await?;
    Ok(futures::stream::unfold(s, |mut s| async move {
        s.next().await.map(|b| (b.map(|b| b.into_inner()), s))
    }))
}

/// A computed window for one stream, ready to apply.
struct Planned {
    stream: usize,
    prefix: String,
    from: Option<i64>,
    to: i64,
    table_id: i64,
    nc: NetChanges,
    lag: atlatl_cdc::Lag,
    resynced: bool,
}

struct ApplyResult {
    up: ApplyCounts,
    del: ApplyCounts,
}

/// Applies every planned stream (upserts parent-first, deletes child-first) and the cursors in
/// one transaction.
#[allow(clippy::too_many_arguments)]
async fn apply(
    engine: &Engine,
    lake: &Lake,
    session: &mut Session,
    pg_opts: &ConnectOptions,
    streams: &[CdcStream],
    planned: &[Planned],
    mode: CursorMode,
    expected: &BTreeMap<String, Option<i64>>,
) -> Result<Vec<ApplyResult>> {
    let mut times = BTreeMap::new();
    for p in planned {
        let (l, to) = (lake.clone(), p.to);
        times.insert(p.to, session.run(move |c| l.snapshot_time(c, to)).await?);
    }
    let mut pg = connect(pg_opts).await?;
    let mut txn = PgApplier::begin(&mut pg, &ApplyOptions::default()).await?;
    let mut out: Vec<ApplyResult> = planned
        .iter()
        .map(|_| ApplyResult {
            up: ApplyCounts::default(),
            del: ApplyCounts::default(),
        })
        .collect();
    for (i, p) in planned.iter().enumerate() {
        let s = &streams[p.stream];
        for sql in chunk_queries(
            &memory_table(&p.prefix, "up"),
            &s.target.primary_key,
            p.nc.upsert_rows,
            s.chunk_rows,
        ) {
            out[i].up += txn.upsert(&s.target, batches(engine, sql).await?).await?;
        }
    }
    for (i, p) in planned.iter().enumerate().rev() {
        let s = &streams[p.stream];
        for sql in chunk_queries(
            &memory_table(&p.prefix, "del"),
            &s.target.primary_key,
            p.nc.delete_rows,
            s.chunk_rows,
        ) {
            out[i].del += txn.delete(&s.target, batches(engine, sql).await?).await?;
        }
    }
    let tables: Vec<String> = planned
        .iter()
        .map(|p| {
            format!(
                "{}.{}",
                streams[p.stream].src.schema, streams[p.stream].src.table
            )
        })
        .collect();
    let writes: Vec<(Option<i64>, CursorWrite<'_>)> = planned
        .iter()
        .zip(&tables)
        .map(|(p, t)| {
            let s = &streams[p.stream];
            (
                expected.get(&s.id).copied().flatten(),
                CursorWrite {
                    stream_id: &s.id,
                    source_table: t,
                    source_table_id: p.table_id,
                    snapshot_id: p.to,
                    snapshot_time: times.get(&p.to).copied().flatten(),
                    mode,
                    config_hash: &s.config_hash,
                },
            )
        })
        .collect();
    txn.commit(&writes).await?;
    Ok(out)
}

/// One query, or `n` PK-hash slices when `rows` exceeds `chunk_rows` (bounded staging per COPY).
pub(crate) fn chunk_queries(table: &str, pk: &[String], rows: u64, chunk_rows: u64) -> Vec<String> {
    let n = rows.div_ceil(chunk_rows);
    if n <= 1 {
        return vec![format!("SELECT * FROM {table}")];
    }
    let h = format!(
        "hash({})",
        pk.iter().map(|k| ident(k)).collect::<Vec<_>>().join(", ")
    );
    (0..n)
        .map(|i| format!("SELECT * FROM {table} WHERE {h} % {n} = {i}"))
        .collect()
}

pub(crate) fn rfc(t: Option<SystemTime>) -> Option<String> {
    t.map(atlatl_core::ids::rfc3339)
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
    let (pg_opts, pgc) = pg_conn(&rt.connections, &first.target.name)?;
    for r in unit {
        rt.report.start_replication(&r.name);
    }
    let streams = streams_of(&rt.project, &lake.alias, unit)?;
    let mut pg = connect(&pg_opts).await?;
    ensure_state_schema(pg.client_mut(), manages_state_schema(&pgc)).await?;
    let ids: Vec<&str> = streams.iter().map(|s| s.id.as_str()).collect();
    let cursors: BTreeMap<String, Cursor> = read_cursors(pg.client(), &ids)
        .await?
        .into_iter()
        .map(|c| (c.stream_id.clone(), c))
        .collect();
    drop(pg);
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

    let mut session = rt.engine.session_with_cancel(cancel.clone())?;
    let l = lake.clone();
    let head = session.run(move |c| l.current_snapshot(c)).await?;
    let mut streams = streams;
    complete(&mut session, &mut streams, head).await?;

    // Guards and bounds; E is the lowest bound across the unit's streams.
    let mut checked = Vec::new();
    for s in &streams {
        let cur = cursors[&s.id].clone();
        let (src, on_gap, ms, mc) = (s.src.clone(), s.on_gap, s.max_snapshots, s.max_changes);
        let r = session
            .run(move |c| {
                let gap = atlatl_cdc::guard_retention(c, &src.catalog, cur.snapshot_id)
                    .and_then(|_| atlatl_cdc::guard_identity(c, &src, Some(cur.source_table_id)));
                match gap {
                    Ok(tid) => Ok((
                        tid,
                        false,
                        atlatl_cdc::bound(c, &src, tid, cur.snapshot_id, head, ms, mc)?,
                    )),
                    Err(e)
                        if on_gap == OnGap::Resync
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
        checked.push(r);
    }
    let end = checked.iter().map(|(_, _, e)| *e).min().unwrap_or(head);
    let caught_up = end >= head;

    // Unique per unit: the published tables live in the shared in-memory catalog.
    let unit_tag = {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        unit.iter()
            .map(|r| &r.name)
            .collect::<Vec<_>>()
            .hash(&mut h);
        h.finish() as u32
    };
    let prefix_base = format!(
        "_atlatl_cdc_{}_{unit_tag:08x}",
        rt.run_id.to_ascii_lowercase()
    );
    let mut planned = Vec::new();
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
            let prefix = format!("{prefix_base}_{i}");
            prefixes.push(prefix.clone());
            let (src, p, tid, stamp, soft) = (s.src.clone(), prefix.clone(), *table_id, s.stamps_snapshot_time(), s.soft_column());
            let (target_schema, target_table) = (s.target.schema.clone(), s.target.table.clone());
            let pg_opts = pg_opts.clone();
            let resync = *resync;
            let (nc, lag) = session
                .run(move |c| {
                    let nc = if resync {
                        attach_pg(c, &pg_opts)?;
                        let t = BootTarget { attach: PG_ATTACH.into(), schema: target_schema, table: target_table, soft_delete_column: soft.0, soft_delete_bool: soft.1 };
                        let nc = bootstrap::diff(c, &src, end, &t, BootMode::Adopt, &p)?;
                        stamp_at(c, &nc, &src.catalog, end)?;
                        nc
                    } else {
                        let w = Window { from, to: end };
                        let nc = atlatl_cdc::net_changes(c, &src, w, &p)?;
                        if stamp {
                            atlatl_cdc::stamp_deletes(c, &src, tid, w, &nc)?;
                        }
                        nc
                    };
                    let lag = atlatl_cdc::lag(c, &src.catalog, from, end)?;
                    Ok((nc, lag))
                })
                .await?;
            let (st, ncc, pp) = (s.clone(), nc.clone(), prefix.clone());
            session.run(move |c| publish(c, &st, &ncc, &pp)).await?;
            planned.push(Planned { stream: i, prefix, from: Some(from), to: end, table_id: *table_id, nc, lag, resynced: resync });
        }

        if planned.is_empty() {
            return Ok(None);
        }
        let mut keys: Vec<LockKey> = planned.iter().map(|p| streams[p.stream].lock_key(&first.target.name)).collect();
        keys.sort_by_key(LockKey::name);
        keys.dedup();
        let timeout = first.lock_timeout_ms.map(Duration::from_millis).unwrap_or(atlatl_core::exec::DEFAULT_LOCK_TIMEOUT);
        let guard = tokio::select! {
            g = rt.locks.acquire(&keys, timeout) => g?,
            _ = cancel.cancelled() => return Err(AtlatlError::new(ErrorClass::Killed, "killed waiting for table locks")),
        };
        let expected = planned.iter().map(|p| (streams[p.stream].id.clone(), p.from)).collect();
        let r = apply(&rt.engine, &lake, &mut session, &pg_opts, &streams, &planned, CursorMode::Cdc, &expected).await;
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
            r.object = Some(format!("{}.{}", s.target.schema, s.target.table));
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
                cdc.touched_keys = pl.nc.touched;
                cdc.upserts_sent = c.up.upserts_sent;
                cdc.rows_inserted = c.up.rows_inserted;
                cdc.rows_updated = c.up.rows_updated;
                cdc.rows_unchanged = c.up.rows_unchanged;
                cdc.rows_deleted = c.del.rows_deleted;
                cdc.deletes_ignored = c.del.deletes_ignored;
                cdc.snapshots_behind = pl.lag.snapshots_behind;
                cdc.seconds_behind = pl.lag.seconds_behind;
                cdc.resynced = pl.resynced;
                r.rows.extracted = pl.nc.upsert_rows + pl.nc.delete_rows;
                r.rows.staged = pl.nc.upsert_rows;
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

/// Soft-delete stamps for a bootstrap diff: rows missing from lake@S are stamped with S's time.
pub(crate) fn stamp_at(
    c: &duckdb::Connection,
    nc: &NetChanges,
    catalog: &str,
    at: i64,
) -> Result<()> {
    c.execute_batch(&format!(
        "UPDATE {del} SET _ts = (SELECT snapshot_time FROM ducklake_snapshots({cat}) WHERE snapshot_id = {at})",
        del = ident(&nc.deletes),
        cat = literal(catalog)
    ))
    .map_err(atlatl_ingest::sql::db)
}

#[derive(Clone, Debug)]
pub struct BootstrapArgs {
    pub replication: String,
    pub env: Option<String>,
    pub mode: BootMode,
    pub at_snapshot: Option<i64>,
    pub dry_run: bool,
    pub advance_cursor_only: bool,
    pub extension_dir: Option<std::path::PathBuf>,
}

/// `atlatl cdc bootstrap` (08 §9): diff lake@S against the target and apply it with `cursor = S`
/// in one transaction, or report the diff (`--dry-run`), or only set the cursor.
pub async fn bootstrap_cmd(
    project: Arc<Project>,
    args: BootstrapArgs,
    vars: Arc<dyn EnvSource>,
) -> Result<Value> {
    let req = PlanRequest {
        env: args.env.clone(),
        replications: vec![args.replication.clone()],
        with_deps: false,
        ..PlanRequest::all()
    };
    let plan = plan::build(&project, &req, vars.clone(), Redactor::global())
        .map_err(|d| {
            AtlatlError::config(
                d.errors()
                    .map(|e| e.error.to_string())
                    .collect::<Vec<_>>()
                    .join("; "),
            )
        })?
        .0;
    let plan = Arc::new(plan);
    let full = plan
        .replications
        .iter()
        .find(|r| r.name == args.replication)
        .ok_or_else(|| AtlatlError::config(format!("no replication `{}`", args.replication)))?;
    // A multi-hop replication bootstraps each of its lake → postgres hops (11 §3).
    let hops: Vec<(usize, ReplicationPlan)> = if full.hops.is_empty() {
        vec![(0, full.clone())]
    } else {
        (0..full.hops.len())
            .map(|i| (i, full.hop_plan(i)))
            .filter(|(_, h)| !h.streams.is_empty())
            .collect()
    };
    let serving = |t: &str| t == "postgres" || crate::serve_lite::KINDS.contains(&t);
    let hops: Vec<(usize, ReplicationPlan)> = hops
        .into_iter()
        .filter(|(_, r)| r.source.kind == "ducklake" && serving(&r.target.kind))
        .collect();
    if hops.is_empty() {
        return Err(AtlatlError::config(format!(
            "`{}` is not a lake → serving-database (postgres, sqlite, d1, turso) cdc replication and has no such `then` hop",
            full.name
        )));
    }
    let rt = crate::run::runtime(
        project.clone(),
        plan.clone(),
        vars,
        None,
        args.extension_dir.clone(),
        Default::default(),
    )
    .await?;
    let mut out = Vec::new();
    for (_, rep) in &hops {
        out.push(
            if crate::serve_lite::KINDS.contains(&rep.target.kind.as_str()) {
                crate::serve_lite::bootstrap_rep(&rt, rep, &args).await?
            } else {
                bootstrap_rep(&project, &rt, rep, &args).await?
            },
        );
    }
    if full.hops.is_empty() {
        return Ok(out.remove(0));
    }
    let streams: Vec<Value> = hops
        .iter()
        .zip(&out)
        .flat_map(|((i, _), v)| {
            v["streams"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(move |mut s| {
                    s["hop"] = json!(i);
                    s
                })
        })
        .collect();
    Ok(
        json!({"replication": full.name, "mode": args.mode, "dry_run": args.dry_run, "advance_cursor_only": args.advance_cursor_only, "streams": streams}),
    )
}

async fn bootstrap_rep(
    project: &Arc<Project>,
    rt: &Runtime,
    rep: &ReplicationPlan,
    args: &BootstrapArgs,
) -> Result<Value> {
    let lake = rt.lake(&rep.source.name)?.clone();
    let (pg_opts, pgc) = pg_conn(&rt.connections, &rep.target.name)?;
    let streams = streams_of(project, &lake.alias, &[rep])?;
    let mut pg = connect(&pg_opts).await?;
    if !args.dry_run {
        ensure_state_schema(pg.client_mut(), manages_state_schema(&pgc)).await?;
    }
    let ids: Vec<&str> = streams.iter().map(|s| s.id.as_str()).collect();
    let existing: BTreeMap<String, i64> = match read_cursors(pg.client(), &ids).await {
        Ok(v) => v
            .into_iter()
            .map(|c| (c.stream_id, c.snapshot_id))
            .collect(),
        Err(_) if args.dry_run => BTreeMap::new(),
        Err(e) => return Err(e),
    };
    drop(pg);
    let mut session = rt.engine.session()?;
    let l = lake.clone();
    let at = match args.at_snapshot {
        Some(s) => s,
        None => session.run(move |c| l.current_snapshot(c)).await?,
    };
    let mut streams = streams;
    complete(&mut session, &mut streams, at).await?;
    let prefix_base = format!("_atlatl_boot_{}", rt.run_id.to_ascii_lowercase());
    let mut planned = Vec::new();
    let mut prefixes = Vec::new();
    let mut report = Vec::new();
    let result = async {
        for (i, s) in streams.iter().enumerate() {
            let prefix = format!("{prefix_base}_{i}");
            prefixes.push(prefix.clone());
            let (src, p, soft, o) = (
                s.src.clone(),
                prefix.clone(),
                s.soft_column(),
                pg_opts.clone(),
            );
            let (ts, tt) = (s.target.schema.clone(), s.target.table.clone());
            let (mode, dry, only) = (args.mode, args.dry_run, args.advance_cursor_only);
            let (tid, nc, dr) = session
                .run(move |c| {
                    if !atlatl_cdc::snapshot_exists(c, &src.catalog, at)? {
                        return Err(AtlatlError::config(format!("snapshot {at} does not exist")));
                    }
                    let tid = atlatl_cdc::guard_identity(c, &src, None)?;
                    if only {
                        return Ok((tid, NetChanges::default(), None));
                    }
                    attach_pg(c, &o)?;
                    let t = BootTarget {
                        attach: PG_ATTACH.into(),
                        schema: ts,
                        table: tt,
                        soft_delete_column: soft.0,
                        soft_delete_bool: soft.1,
                    };
                    let nc = bootstrap::diff(c, &src, at, &t, mode, &p)?;
                    let dr = if dry {
                        Some(bootstrap::dry_run(c, &src, &nc, &p, 5)?)
                    } else {
                        None
                    };
                    if !dry {
                        stamp_at(c, &nc, &src.catalog, at)?;
                    }
                    Ok((tid, nc, dr))
                })
                .await?;
            report.push(json!({
                "stream": s.id,
                "target": format!("{}.{}", s.target.schema, s.target.table),
                "at_snapshot": at,
                "cursor_before": existing.get(&s.id),
                "upserts": nc.upsert_rows,
                "deletes": nc.delete_rows,
                "samples": dr.as_ref().map(|d| d.samples.clone()),
            }));
            if args.dry_run {
                continue;
            }
            if !args.advance_cursor_only {
                let (st, ncc, pp) = (s.clone(), nc.clone(), prefix.clone());
                session.run(move |c| publish(c, &st, &ncc, &pp)).await?;
            } else {
                let (st, pp) = (s.clone(), prefix.clone());
                session.run(move |c| empty_published(c, &st, &pp)).await?;
            }
            planned.push(Planned {
                stream: i,
                prefix,
                from: existing.get(&s.id).copied(),
                to: at,
                table_id: tid,
                nc,
                lag: Default::default(),
                resynced: false,
            });
        }
        if args.dry_run || planned.is_empty() {
            return Ok(None);
        }
        let mut keys: Vec<LockKey> = planned
            .iter()
            .map(|p| streams[p.stream].lock_key(&rep.target.name))
            .collect();
        keys.sort_by_key(LockKey::name);
        keys.dedup();
        let guard = rt
            .locks
            .acquire(&keys, atlatl_core::exec::DEFAULT_LOCK_TIMEOUT)
            .await?;
        let expected = planned
            .iter()
            .map(|p| (streams[p.stream].id.clone(), p.from))
            .collect();
        let r = apply(
            &rt.engine,
            &lake,
            &mut session,
            &pg_opts,
            &streams,
            &planned,
            CursorMode::Bootstrap,
            &expected,
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
    if let Some(a) = &applied {
        for (p, c) in planned.iter().zip(a) {
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

pub(crate) fn empty_published(c: &duckdb::Connection, s: &CdcStream, prefix: &str) -> Result<()> {
    let cols = s
        .target
        .columns
        .iter()
        .map(|x| ident(x))
        .collect::<Vec<_>>()
        .join(", ");
    let pk = s
        .target
        .primary_key
        .iter()
        .map(|x| ident(x))
        .collect::<Vec<_>>()
        .join(", ");
    let fq = format!(
        "{}.{}.{}",
        ident(&s.src.catalog),
        ident(&s.src.schema),
        ident(&s.src.table)
    );
    c.execute_batch(&format!(
        "CREATE OR REPLACE TABLE {up} AS SELECT {cols} FROM {fq} WHERE false;
         CREATE OR REPLACE TABLE {del} AS SELECT {pk}, NULL::TIMESTAMPTZ AS _ts FROM {fq} WHERE false;",
        up = memory_table(prefix, "up"),
        del = memory_table(prefix, "del"),
    ))
    .map_err(atlatl_ingest::sql::db)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunking() {
        assert_eq!(
            chunk_queries("t", &["id".into()], 10, 100),
            ["SELECT * FROM t"]
        );
        let q = chunk_queries("t", &["a".into(), "b".into()], 250, 100);
        assert_eq!(q.len(), 3);
        assert_eq!(q[2], "SELECT * FROM t WHERE hash(\"a\", \"b\") % 3 = 2");
    }
}
