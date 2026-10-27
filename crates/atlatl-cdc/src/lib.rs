//! DuckLake change reading for serving targets (spec 08 §3–§5). Net changes are the
//! diff of the table's state at C and at E, on the business key; the change feed only says which
//! keys to look at. Results land in engine TEMP tables that the applier streams.

pub mod bootstrap;

use atlatl_core::error::{AtlatlError, ErrorClass, Result};
use duckdb::{Connection, OptionalExt, params};
use serde::Serialize;

pub fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

pub(crate) fn db(e: duckdb::Error) -> AtlatlError {
    let m = atlatl_core::redact::redact(&e.to_string()).into_owned();
    if m.contains("No snapshot found at version") {
        return AtlatlError::data(m).code("cursor_expired");
    }
    AtlatlError::new(ErrorClass::Internal, m)
}

fn count(c: &Connection, sql: &str) -> Result<u64> {
    let n: i64 = c.query_row(sql, [], |r| r.get(0)).map_err(db)?;
    Ok(n as u64)
}

/// The lake side of one CDC stream.
#[derive(Clone, Debug)]
pub struct Source {
    /// ATTACH alias of the lake.
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub primary_key: Vec<String>,
    /// Served columns as (name, expression over the table's columns); PK columns first is not
    /// required. A plain column is `(c, "\"c\"")`; transforms are DuckDB SQL (02 §4.1).
    pub columns: Vec<(String, String)>,
    /// Compared-for-change columns exclude these (`diff.ignore_columns`).
    pub ignore: Vec<String>,
    /// `where:` applied to both images; a row leaving it becomes a delete.
    pub filter: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct Window {
    /// Exclusive start: the cursor (last applied snapshot).
    pub from: i64,
    /// Inclusive end.
    pub to: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct NetChanges {
    /// TEMP table of upserts: the served columns.
    pub upserts: String,
    /// TEMP table of deletes: PK columns plus `_snapshot` and `_ts` (see [`stamp_deletes`]).
    pub deletes: String,
    pub touched: u64,
    pub upsert_rows: u64,
    pub delete_rows: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
pub struct Lag {
    pub snapshots_behind: u64,
    pub seconds_behind: f64,
}

impl Source {
    fn fq(&self) -> String {
        format!(
            "{}.{}.{}",
            ident(&self.catalog),
            ident(&self.schema),
            ident(&self.table)
        )
    }

    fn keys(&self) -> String {
        self.primary_key
            .iter()
            .map(|k| ident(k))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

pub fn head(c: &Connection, catalog: &str) -> Result<i64> {
    c.query_row(
        &format!("SELECT id FROM ducklake_current_snapshot({})", lit(catalog)),
        [],
        |r| r.get(0),
    )
    .map_err(db)
}

pub fn oldest(c: &Connection, catalog: &str) -> Result<i64> {
    c.query_row(
        &format!(
            "SELECT min(snapshot_id) FROM ducklake_snapshots({})",
            lit(catalog)
        ),
        [],
        |r| r.get(0),
    )
    .map_err(db)
}

pub fn snapshot_exists(c: &Connection, catalog: &str, id: i64) -> Result<bool> {
    Ok(count(
        c,
        &format!(
            "SELECT count(*) FROM ducklake_snapshots({}) WHERE snapshot_id = {id}",
            lit(catalog)
        ),
    )? > 0)
}

pub fn table_id(c: &Connection, catalog: &str, schema: &str, table: &str) -> Result<Option<i64>> {
    let sql = format!(
        "SELECT i.table_id FROM ducklake_table_info({}) i JOIN duckdb_tables() d
           ON d.database_name = ? AND d.schema_name = ? AND d.table_name = i.table_name
         WHERE i.table_name = ?",
        lit(catalog)
    );
    c.query_row(&sql, params![catalog, schema, table], |r| r.get(0))
        .optional()
        .map_err(db)
}

/// 08 §5: the cursor's snapshot must still exist. An expired cursor is `cursor_expired`.
pub fn guard_retention(c: &Connection, catalog: &str, cursor: i64) -> Result<()> {
    if snapshot_exists(c, catalog, cursor)? {
        return Ok(());
    }
    let o = oldest(c, catalog)?;
    Err(AtlatlError::data(format!(
        "cursor snapshot {cursor} has expired (oldest retained is {o}); run `atlatl cdc bootstrap --mode adopt` or set `on_gap: resync`"
    ))
    .code("cursor_expired"))
}

/// 08 §3: a recreated table (new `table_id`) needs a resync; the feed would miss its deletes.
pub fn guard_identity(c: &Connection, s: &Source, expected: Option<i64>) -> Result<i64> {
    let now = table_id(c, &s.catalog, &s.schema, &s.table)?.ok_or_else(|| {
        AtlatlError::config(format!(
            "lake table {}.{} does not exist",
            s.schema, s.table
        ))
    })?;
    match expected {
        Some(e) if e != now => Err(AtlatlError::data(format!(
            "{}.{} was recreated (table_id {e} → {now}); the change feed can't bridge it, so a resync is required",
            s.schema, s.table
        ))
        .code("resync_required")),
        _ => Ok(now),
    }
}

pub fn columns_at(c: &Connection, s: &Source, version: i64) -> Result<Vec<String>> {
    let mut st = c
        .prepare(&format!(
            "DESCRIBE SELECT * FROM {} AT (VERSION => {version})",
            s.fq()
        ))
        .map_err(db)?;
    let rows = st.query_map([], |r| r.get::<_, String>(0)).map_err(db)?;
    rows.map(|r| r.map_err(db)).collect()
}

/// Snapshots in `(from, to]` whose `changes` touch `table_id`, ascending.
pub fn touching_snapshots(
    c: &Connection,
    catalog: &str,
    table_id: i64,
    from: i64,
    to: i64,
) -> Result<Vec<i64>> {
    let sql = format!(
        "SELECT snapshot_id FROM ducklake_snapshots({}) WHERE snapshot_id > {from} AND snapshot_id <= {to}
           AND list_contains(flatten(map_values(changes)), {})
         ORDER BY snapshot_id",
        lit(catalog),
        lit(&table_id.to_string())
    );
    let mut st = c.prepare(&sql).map_err(db)?;
    let rows = st.query_map([], |r| r.get::<_, i64>(0)).map_err(db)?;
    rows.map(|r| r.map_err(db)).collect()
}

/// Feed rows in `(from, to]` (for `max_changes`).
pub fn feed_rows(c: &Connection, s: &Source, from: i64, to: i64) -> Result<u64> {
    count(
        c,
        &format!(
            "SELECT count(*) FROM ducklake_table_changes({}, {}, {}, {}, {to})",
            lit(&s.catalog),
            lit(&s.schema),
            lit(&s.table),
            from + 1
        ),
    )
}

/// 08 §8: lowers E so the window holds at most `max_snapshots` table-touching snapshots and
/// `max_changes` feed rows. Always advances by at least one touching snapshot.
pub fn bound(
    c: &Connection,
    s: &Source,
    table_id: i64,
    from: i64,
    head: i64,
    max_snapshots: Option<u64>,
    max_changes: Option<u64>,
) -> Result<i64> {
    let snaps = touching_snapshots(c, &s.catalog, table_id, from, head)?;
    if snaps.is_empty() {
        return Ok(head);
    }
    let mut hi = snaps.len();
    if let Some(m) = max_snapshots {
        hi = hi.min((m as usize).max(1));
    }
    if let Some(m) = max_changes {
        let (mut lo, mut best) = (1usize, 1usize);
        let mut top = hi;
        while lo <= top {
            let mid = (lo + top) / 2;
            if feed_rows(c, s, from, snaps[mid - 1])? <= m {
                best = mid;
                lo = mid + 1;
            } else {
                top = mid - 1;
            }
        }
        hi = best;
    }
    Ok(if hi == snaps.len() {
        head
    } else {
        snaps[hi - 1]
    })
}

pub fn lag(c: &Connection, catalog: &str, from: i64, to: i64) -> Result<Lag> {
    let sql = format!(
        "SELECT count(*) FILTER (snapshot_id > {from} AND snapshot_id <= {to}),
                coalesce(epoch(max(snapshot_time) FILTER (snapshot_id = {to})) - epoch(max(snapshot_time) FILTER (snapshot_id = {from})), 0)
         FROM ducklake_snapshots({})",
        lit(catalog)
    );
    let (n, secs): (i64, f64) = c
        .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?)))
        .map_err(db)?;
    Ok(Lag {
        snapshots_behind: n as u64,
        seconds_behind: secs,
    })
}

/// Computes net changes for `w` into TEMP tables named with `prefix`. Guards should have run.
pub fn net_changes(c: &Connection, s: &Source, w: Window, prefix: &str) -> Result<NetChanges> {
    let t = |n: &str| ident(&format!("{prefix}_{n}"));
    let keys = s.keys();
    let at_c = columns_at(c, s, w.from)?;
    let at_e = columns_at(c, s, w.to)?;
    let raw = |version: i64, present: &[String]| {
        let cols = at_e
            .iter()
            .map(|col| {
                if present.contains(col) {
                    ident(col)
                } else {
                    format!("NULL AS {}", ident(col))
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("(SELECT {cols} FROM {} AT (VERSION => {version}))", s.fq())
    };
    let proj = s
        .columns
        .iter()
        .map(|(n, e)| format!("{e} AS {}", ident(n)))
        .collect::<Vec<_>>()
        .join(", ");
    let filter = s
        .filter
        .as_ref()
        .map(|f| format!("WHERE {f}"))
        .unwrap_or_default();
    let pk_cols = s
        .primary_key
        .iter()
        .map(|k| format!("{} AS {}", ident(k), ident(k)))
        .collect::<Vec<_>>()
        .join(", ");
    c.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE {touched} AS
           SELECT DISTINCT {keys} FROM ducklake_table_changes({cat}, {sch}, {tbl}, {c1}, {e});
         CREATE OR REPLACE TEMP TABLE {before} AS
           SELECT {pk_cols}, {proj} FROM (SELECT * FROM {raw_c} r SEMI JOIN {touched} USING ({keys})) x {filter};
         CREATE OR REPLACE TEMP TABLE {after} AS
           SELECT {pk_cols}, {proj} FROM (SELECT * FROM {raw_e} r SEMI JOIN {touched} USING ({keys})) x {filter};",
        touched = t("touched"),
        before = t("before"),
        after = t("after"),
        cat = lit(&s.catalog),
        sch = lit(&s.schema),
        tbl = lit(&s.table),
        c1 = w.from + 1,
        e = w.to,
        raw_c = raw(w.from, &at_c),
        raw_e = raw(w.to, &at_e),
    ))
    .map_err(db)?;

    let nulls = s
        .primary_key
        .iter()
        .map(|k| format!("{} IS NULL", ident(k)))
        .collect::<Vec<_>>()
        .join(" OR ");
    let n = count(
        c,
        &format!("SELECT count(*) FROM {} WHERE {nulls}", t("after")),
    )?;
    if n > 0 {
        return Err(AtlatlError::data(format!(
            "{n} rows in {}.{} have a NULL primary key",
            s.schema, s.table
        ))
        .code("null_primary_key"));
    }
    let d = count(
        c,
        &format!(
            "SELECT count(*) FROM (SELECT 1 FROM {} GROUP BY {keys} HAVING count(*) > 1)",
            t("after")
        ),
    )?;
    if d > 0 {
        return Err(AtlatlError::data(format!(
            "{d} primary keys repeat in {}.{}",
            s.schema, s.table
        ))
        .code("duplicate_primary_key"));
    }

    let cmp: Vec<&str> = s
        .columns
        .iter()
        .map(|(n, _)| n.as_str())
        .filter(|n| !s.primary_key.iter().any(|k| k == n) && !s.ignore.iter().any(|i| i == n))
        .collect();
    let on = s
        .primary_key
        .iter()
        .map(|k| format!("a.{k} = b.{k}", k = ident(k)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let changed = if cmp.is_empty() {
        "false".to_owned()
    } else {
        let side = |p: &str| {
            cmp.iter()
                .map(|c| format!("{p}.{}", ident(c)))
                .collect::<Vec<_>>()
                .join(", ")
        };
        format!("({}) IS DISTINCT FROM ({})", side("a"), side("b"))
    };
    let served = s
        .columns
        .iter()
        .map(|(n, _)| format!("a.{}", ident(n)))
        .collect::<Vec<_>>()
        .join(", ");
    let pk_a = s
        .primary_key
        .iter()
        .map(|k| format!("b.{}", ident(k)))
        .collect::<Vec<_>>()
        .join(", ");
    c.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE {up} AS
           SELECT {served} FROM {after} a LEFT JOIN {before} b ON {on}
           WHERE b.{pk0} IS NULL OR {changed};
         CREATE OR REPLACE TEMP TABLE {del} AS
           SELECT {pk_a}, NULL::BIGINT AS _snapshot, NULL::TIMESTAMPTZ AS _ts FROM {before} b ANTI JOIN {after} a ON {on};",
        up = t("up"),
        del = t("del"),
        after = t("after"),
        before = t("before"),
        pk0 = ident(&s.primary_key[0]),
    ))
    .map_err(db)?;
    Ok(NetChanges {
        upserts: format!("{prefix}_up"),
        deletes: format!("{prefix}_del"),
        touched: count(c, &format!("SELECT count(*) FROM {}", t("touched")))?,
        upsert_rows: count(c, &format!("SELECT count(*) FROM {}", t("up")))?,
        delete_rows: count(c, &format!("SELECT count(*) FROM {}", t("del")))?,
    })
}

/// Stamps each delete with the snapshot that removed it (08 §6.0 `value: snapshot_time`), by
/// probing the table-touching snapshots for the first where the key is gone from the filtered
/// view. The feed can't say: a row leaving `where:` is an UPDATE there, with no delete
/// (`filter_exits_are_stamped_with_the_update_that_hid_them`), and its delete labels were wrong
/// before DuckDB v1.5.6's ducklake (`tests/feed.rs`).
pub fn stamp_deletes(
    c: &Connection,
    s: &Source,
    table_id: i64,
    w: Window,
    nc: &NetChanges,
) -> Result<()> {
    if nc.delete_rows == 0 {
        return Ok(());
    }
    let del = ident(&nc.deletes);
    let on = s
        .primary_key
        .iter()
        .map(|k| format!("x.{k} = d.{k}", k = ident(k)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let filter = s
        .filter
        .as_ref()
        .map(|f| format!(" AND ({f})"))
        .unwrap_or_default();
    for snap in touching_snapshots(c, &s.catalog, table_id, w.from, w.to)? {
        c.execute_batch(&format!(
            "UPDATE {del} d SET _snapshot = {snap} WHERE _snapshot IS NULL
               AND NOT EXISTS (SELECT 1 FROM (SELECT * FROM {} AT (VERSION => {snap})) x WHERE {on}{filter})",
            s.fq()
        ))
        .map_err(db)?;
    }
    c.execute_batch(&format!(
        "UPDATE {del} d SET _snapshot = coalesce(_snapshot, {to});
         UPDATE {del} d SET _ts = s.snapshot_time FROM ducklake_snapshots({cat}) s WHERE s.snapshot_id = d._snapshot;",
        to = w.to,
        cat = lit(&s.catalog)
    ))
    .map_err(db)
}

#[cfg(test)]
mod tests;
