//! `atlatl.cursors`: CDC cursors kept in the serving target (spec 05 §3, 08 §6.1).

use std::fmt;
use std::time::SystemTime;

use atlatl_core::AtlatlError;
use atlatl_core::config::model::PostgresConn;
use atlatl_core::error::Result;
use tokio_postgres::{Client, GenericClient, Row};

use crate::error::wrap;

pub const CURSORS_TABLE: &str = "atlatl.cursors";

pub const STATE_DDL: &str = "\
CREATE SCHEMA IF NOT EXISTS atlatl;
CREATE TABLE IF NOT EXISTS atlatl.cursors (
  stream_id        text PRIMARY KEY,
  source_table     text NOT NULL,
  source_table_id  bigint NOT NULL,
  snapshot_id      bigint NOT NULL,
  snapshot_time    timestamptz,
  mode             text NOT NULL,
  config_hash      text NOT NULL,
  updated_at       timestamptz NOT NULL DEFAULT now()
);";

const COLUMNS: [&str; 8] = [
    "stream_id",
    "source_table",
    "source_table_id",
    "snapshot_id",
    "snapshot_time",
    "mode",
    "config_hash",
    "updated_at",
];

const SELECT: &str = "SELECT stream_id, source_table, source_table_id, snapshot_id, snapshot_time, \
     mode, config_hash, updated_at FROM atlatl.cursors";

/// Serializes concurrent first-time creation (two `CREATE SCHEMA IF NOT EXISTS` can race).
const DDL_LOCK: &str = "SELECT pg_advisory_xact_lock(hashtextextended('atlatl:state-schema', 0))";

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

    fn parse(s: &str) -> Result<Self> {
        match s {
            "cdc" => Ok(CursorMode::Cdc),
            "bootstrap" => Ok(CursorMode::Bootstrap),
            _ => Err(AtlatlError::data(format!(
                "atlatl.cursors has an unknown mode `{s}`"
            ))),
        }
    }
}

impl fmt::Display for CursorMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A row of `atlatl.cursors`. `updated_at` is set by the server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cursor {
    pub stream_id: String,
    pub source_table: String,
    /// DuckLake `table_id` at `snapshot_id`, for the table-identity guard (08 §3).
    pub source_table_id: i64,
    pub snapshot_id: i64,
    pub snapshot_time: Option<SystemTime>,
    pub mode: CursorMode,
    pub config_hash: String,
    pub updated_at: Option<SystemTime>,
}

impl Cursor {
    fn from_row(r: &Row) -> Result<Self> {
        Ok(Self {
            stream_id: r.get(0),
            source_table: r.get(1),
            source_table_id: r.get(2),
            snapshot_id: r.get(3),
            snapshot_time: r.get(4),
            mode: CursorMode::parse(r.get(5))?,
            config_hash: r.get(6),
            updated_at: Some(r.get(7)),
        })
    }
}

pub fn manages_state_schema(p: &PostgresConn) -> bool {
    p.manage_state_schema != Some(false)
}

/// With `manage`, creates `atlatl.cursors` if it is missing. Either way, checks it has every
/// column; without `manage` a missing table is a `config` error (Atlas owns it).
pub async fn ensure_state_schema(c: &mut Client, manage: bool) -> Result<()> {
    if manage && !exists(c).await? {
        let tx = c.transaction().await.map_err(wrap)?;
        tx.batch_execute(DDL_LOCK).await.map_err(wrap)?;
        tx.batch_execute(STATE_DDL).await.map_err(wrap)?;
        tx.commit().await.map_err(wrap)?;
    }
    if !exists(c).await? {
        return Err(AtlatlError::config(
            "atlatl.cursors does not exist and `manage_state_schema` is false; \
             create it with the DDL in spec 05 §3",
        ));
    }
    let have: Vec<String> = c
        .query(
            "SELECT attname::text FROM pg_attribute \
             WHERE attrelid = 'atlatl.cursors'::regclass AND attnum > 0 AND NOT attisdropped",
            &[],
        )
        .await
        .map_err(wrap)?
        .iter()
        .map(|r| r.get(0))
        .collect();
    let missing: Vec<&str> = COLUMNS
        .into_iter()
        .filter(|c| !have.iter().any(|h| h == c))
        .collect();
    if !missing.is_empty() {
        return Err(AtlatlError::config(format!(
            "atlatl.cursors is missing columns: {}",
            missing.join(", ")
        )));
    }
    Ok(())
}

async fn exists(c: &Client) -> Result<bool> {
    c.query_one("SELECT to_regclass('atlatl.cursors') IS NOT NULL", &[])
        .await
        .map(|r| r.get(0))
        .map_err(wrap)
}

pub async fn read_cursor(c: &impl GenericClient, stream_id: &str) -> Result<Option<Cursor>> {
    c.query_opt(&format!("{SELECT} WHERE stream_id = $1"), &[&stream_id])
        .await
        .map_err(wrap)?
        .as_ref()
        .map(Cursor::from_row)
        .transpose()
}

/// Cursors for `stream_ids` that exist, in `stream_id` order.
pub async fn read_cursors(c: &impl GenericClient, stream_ids: &[&str]) -> Result<Vec<Cursor>> {
    c.query(
        &format!("{SELECT} WHERE stream_id = ANY($1) ORDER BY stream_id"),
        &[&stream_ids],
    )
    .await
    .map_err(wrap)?
    .iter()
    .map(Cursor::from_row)
    .collect()
}

/// What a replication commits for one stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CursorWrite<'a> {
    pub stream_id: &'a str,
    pub source_table: &'a str,
    pub source_table_id: i64,
    pub snapshot_id: i64,
    pub snapshot_time: Option<SystemTime>,
    pub mode: CursorMode,
    pub config_hash: &'a str,
}

/// Compare-and-set, meant to run inside the caller's apply transaction. `expected` is the
/// `snapshot_id` read at start, or `None` if there was no cursor. If another applier moved or
/// created the cursor meanwhile, nothing is written and the error is `conflict`; the caller must
/// roll back.
pub async fn advance_cursor(
    c: &impl GenericClient,
    expected: Option<i64>,
    w: &CursorWrite<'_>,
) -> Result<()> {
    let mode = w.mode.as_str();
    let n = match expected {
        Some(prev) => c
            .execute(
                "UPDATE atlatl.cursors SET source_table = $2, source_table_id = $3, \
                 snapshot_id = $4, snapshot_time = $5, mode = $6, config_hash = $7, \
                 updated_at = now() \
                 WHERE stream_id = $1 AND snapshot_id = $8",
                &[
                    &w.stream_id,
                    &w.source_table,
                    &w.source_table_id,
                    &w.snapshot_id,
                    &w.snapshot_time,
                    &mode,
                    &w.config_hash,
                    &prev,
                ],
            )
            .await
            .map_err(wrap)?,
        None => c
            .execute(
                "INSERT INTO atlatl.cursors (stream_id, source_table, source_table_id, \
                 snapshot_id, snapshot_time, mode, config_hash) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (stream_id) DO NOTHING",
                &[
                    &w.stream_id,
                    &w.source_table,
                    &w.source_table_id,
                    &w.snapshot_id,
                    &w.snapshot_time,
                    &mode,
                    &w.config_hash,
                ],
            )
            .await
            .map_err(wrap)?,
    };
    if n == 1 {
        return Ok(());
    }
    let expected = expected.map_or_else(|| "no cursor".to_owned(), |s| format!("snapshot {s}"));
    Err(AtlatlError::conflict(format!(
        "cursor for stream `{}` is no longer at {expected}: another applier moved it",
        w.stream_id
    ))
    .code(CURSOR_CONFLICT_CODE))
}

pub const CURSOR_CONFLICT_CODE: &str = "cursor_moved";
