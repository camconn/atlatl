//! `_atlatl_cursors`: CDC cursors kept in SQLite-family targets (05 §3, 08 §6.2–6.3).

use std::time::SystemTime;

use atlatl_core::AtlatlError;
use atlatl_core::error::Result;
use sha2::{Digest, Sha256};

use crate::conn::Stmt;
use crate::types::{Value, iso, parse_iso};

pub const CURSORS_TABLE: &str = "_atlatl_cursors";

pub const STATE_DDL: &str = "CREATE TABLE IF NOT EXISTS _atlatl_cursors (
  stream_id           TEXT PRIMARY KEY,
  source_table        TEXT NOT NULL,
  source_table_id     INTEGER NOT NULL,
  snapshot_id         INTEGER NOT NULL,
  snapshot_time       TEXT,
  mode                TEXT NOT NULL,
  config_hash         TEXT NOT NULL,
  pending_snapshot    INTEGER,
  pending_fingerprint TEXT,
  updated_at          TEXT NOT NULL
)";

pub const COLUMNS: [&str; 10] = [
    "stream_id",
    "source_table",
    "source_table_id",
    "snapshot_id",
    "snapshot_time",
    "mode",
    "config_hash",
    "pending_snapshot",
    "pending_fingerprint",
    "updated_at",
];

pub const CURSOR_CONFLICT_CODE: &str = "cursor_moved";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorMode {
    Cdc,
    Bootstrap,
}

impl CursorMode {
    pub fn as_str(self) -> &'static str {
        match self {
            CursorMode::Cdc => "cdc",
            CursorMode::Bootstrap => "bootstrap",
        }
    }
}

/// A window being applied across several requests or transactions (08 §6.3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pending {
    pub snapshot_id: i64,
    pub fingerprint: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cursor {
    pub stream_id: String,
    pub source_table: String,
    pub source_table_id: i64,
    pub snapshot_id: i64,
    pub snapshot_time: Option<SystemTime>,
    pub mode: CursorMode,
    pub config_hash: String,
    pub pending: Option<Pending>,
    pub updated_at: Option<SystemTime>,
}

/// What an apply commits for one stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CursorWrite {
    pub stream_id: String,
    pub source_table: String,
    pub source_table_id: i64,
    pub snapshot_id: i64,
    pub snapshot_time: Option<SystemTime>,
    pub mode: CursorMode,
    pub config_hash: String,
}

/// A cursor move: from `expected` (`None`: no cursor yet) to `write`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CursorPlan {
    pub expected: Option<i64>,
    pub write: CursorWrite,
}

impl CursorPlan {
    pub fn fingerprint(&self) -> String {
        fingerprint(
            &self.write.config_hash,
            self.expected.unwrap_or(-1),
            self.write.snapshot_id,
            self.write.source_table_id,
        )
    }
}

/// Identifies a pending window: the definition, both ends and the source table.
pub fn fingerprint(config_hash: &str, from: i64, to: i64, table_id: i64) -> String {
    let h = Sha256::digest(format!("{config_hash}|{from}|{to}|{table_id}"));
    h[..8].iter().map(|b| format!("{b:02x}")).collect()
}

fn now() -> Value {
    Value::Text(iso(SystemTime::now()))
}

fn time(t: Option<SystemTime>) -> Value {
    t.map_or(Value::Null, |t| Value::Text(iso(t)))
}

pub fn select(ids: &[&str]) -> Stmt {
    let marks = vec!["?"; ids.len()].join(", ");
    Stmt::query(
        format!(
            "SELECT {} FROM _atlatl_cursors WHERE stream_id IN ({marks}) ORDER BY stream_id",
            COLUMNS.join(", ")
        ),
        ids.iter().map(|s| Value::Text((*s).to_owned())).collect(),
    )
}

pub fn parse(row: &[Value]) -> Result<Cursor> {
    let bad = || AtlatlError::data(format!("_atlatl_cursors has a malformed row: {row:?}"));
    let text = |i: usize| {
        row.get(i)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(bad)
    };
    let int = |i: usize| row.get(i).and_then(Value::as_i64).ok_or_else(bad);
    let opt_time = |i: usize| row.get(i).and_then(Value::as_str).and_then(parse_iso);
    let mode = match text(5)?.as_str() {
        "cdc" => CursorMode::Cdc,
        "bootstrap" => CursorMode::Bootstrap,
        m => {
            return Err(AtlatlError::data(format!(
                "_atlatl_cursors has an unknown mode `{m}`"
            )));
        }
    };
    let pending = match row.get(7) {
        Some(Value::Null) | None => None,
        Some(_) => Some(Pending {
            snapshot_id: int(7)?,
            fingerprint: text(8).unwrap_or_default(),
        }),
    };
    Ok(Cursor {
        stream_id: text(0)?,
        source_table: text(1)?,
        source_table_id: int(2)?,
        snapshot_id: int(3)?,
        snapshot_time: opt_time(4),
        mode,
        config_hash: text(6)?,
        pending,
        updated_at: opt_time(9),
    })
}

/// Compare-and-set; exactly one row changes when the cursor is where the caller expects.
/// `pending` narrows the match to a window this apply marked (the final flip, 08 §6.3). A CDC
/// apply never moves past another pending window; a bootstrap clears it.
pub fn advance(p: &CursorPlan, pending: Option<&str>) -> Stmt {
    let w = &p.write;
    let set = vec![
        Value::Text(w.source_table.clone()),
        Value::Integer(w.source_table_id),
        Value::Integer(w.snapshot_id),
        time(w.snapshot_time),
        Value::Text(w.mode.as_str().into()),
        Value::Text(w.config_hash.clone()),
        now(),
    ];
    match p.expected {
        Some(prev) => {
            let mut params = set;
            params.push(Value::Text(w.stream_id.clone()));
            params.push(Value::Integer(prev));
            let mut sql = "UPDATE _atlatl_cursors SET source_table = ?, source_table_id = ?, snapshot_id = ?, \
                 snapshot_time = ?, mode = ?, config_hash = ?, updated_at = ?, pending_snapshot = NULL, \
                 pending_fingerprint = NULL WHERE stream_id = ? AND snapshot_id = ?"
                .to_owned();
            match (pending, w.mode) {
                (Some(f), _) => {
                    sql.push_str(" AND pending_snapshot = ? AND pending_fingerprint = ?");
                    params.push(Value::Integer(w.snapshot_id));
                    params.push(Value::Text(f.to_owned()));
                }
                (None, CursorMode::Cdc) => {
                    sql.push_str(" AND (pending_snapshot IS NULL OR pending_snapshot = ?)");
                    params.push(Value::Integer(w.snapshot_id));
                }
                (None, CursorMode::Bootstrap) => {}
            }
            Stmt::exec(sql, params)
        }
        None => {
            let mut params = vec![Value::Text(w.stream_id.clone())];
            params.extend(set);
            Stmt::exec(
                "INSERT INTO _atlatl_cursors (stream_id, source_table, source_table_id, snapshot_id, \
                 snapshot_time, mode, config_hash, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
                 ON CONFLICT (stream_id) DO NOTHING",
                params,
            )
        }
    }
}

/// Step 1 of the idempotent window: record `pending = {E, fingerprint}` unless another window
/// is pending. Re-marking the same window (a re-run after a failure) matches too.
pub fn mark_pending(p: &CursorPlan, expected: i64) -> Stmt {
    let f = p.fingerprint();
    Stmt::exec(
        "UPDATE _atlatl_cursors SET pending_snapshot = ?, pending_fingerprint = ?, updated_at = ? \
         WHERE stream_id = ? AND snapshot_id = ? AND (pending_snapshot IS NULL \
         OR (pending_snapshot = ? AND pending_fingerprint = ?))",
        vec![
            Value::Integer(p.write.snapshot_id),
            Value::Text(f.clone()),
            now(),
            Value::Text(p.write.stream_id.clone()),
            Value::Integer(expected),
            Value::Integer(p.write.snapshot_id),
            Value::Text(f),
        ],
    )
}

pub fn conflict(p: &CursorPlan) -> AtlatlError {
    let expected = p
        .expected
        .map_or_else(|| "no cursor".to_owned(), |s| format!("snapshot {s}"));
    AtlatlError::conflict(format!(
        "cursor for stream `{}` is no longer at {expected}: another applier moved it",
        p.write.stream_id
    ))
    .code(CURSOR_CONFLICT_CODE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_row() {
        let row = vec![
            Value::Text("s".into()),
            Value::Text("src.t".into()),
            Value::Integer(3),
            Value::Integer(10),
            Value::Text("2024-01-02T03:04:05.000006Z".into()),
            Value::Text("cdc".into()),
            Value::Text("h".into()),
            Value::Integer(12),
            Value::Text("abc".into()),
            Value::Text("2024-01-02T03:04:05.000000Z".into()),
        ];
        let c = parse(&row).expect("parse");
        assert_eq!((c.snapshot_id, c.mode), (10, CursorMode::Cdc));
        assert_eq!(
            c.pending,
            Some(Pending {
                snapshot_id: 12,
                fingerprint: "abc".into()
            })
        );
        assert!(c.snapshot_time.is_some());
        assert_ne!(fingerprint("h", 1, 2, 3), fingerprint("h", 1, 3, 3));
        assert_eq!(fingerprint("h", 1, 2, 3).len(), 16);
    }
}
