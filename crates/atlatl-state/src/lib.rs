//! State in the lake (spec 05): `_atlatl.stream_state`, `runs`, `run_replications`, `rejects`.
//! Writes are plain statements so they join the caller's replication transaction.

use std::collections::HashSet;

use atlatl_core::error::{AtlatlError, ErrorClass, Result};
use duckdb::{Connection, OptionalExt, params};
use serde::{Deserialize, Serialize};

pub const SCHEMA: &str = "_atlatl";

fn db(e: duckdb::Error) -> AtlatlError {
    AtlatlError::new(
        ErrorClass::Internal,
        atlatl_core::redact::redact(&e.to_string()).into_owned(),
    )
}

fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateKind {
    UpdateKey,
    Sync,
    /// Postgres logical-replication cursor: `{"lsn": "0/16B3748", "slot": "atlatl_<replication>"}`.
    Lsn,
}

impl StateKind {
    fn as_str(self) -> &'static str {
        match self {
            StateKind::UpdateKey => "update_key",
            StateKind::Sync => "sync",
            StateKind::Lsn => "lsn",
        }
    }

    fn parse(s: &str) -> Result<Self> {
        match s {
            "update_key" => Ok(StateKind::UpdateKey),
            "sync" => Ok(StateKind::Sync),
            "lsn" => Ok(StateKind::Lsn),
            _ => Err(AtlatlError::internal(format!("unknown state kind `{s}`"))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StateRow {
    pub stream_id: String,
    /// `""` for single-member streams, `schema.table` for wildcard members.
    pub member: String,
    pub kind: StateKind,
    pub value: serde_json::Value,
    pub config_hash: String,
    pub run_id: String,
    pub lake_snapshot: Option<i64>,
    pub updated_at: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RunRecord {
    pub run_id: String,
    pub external_run_id: Option<String>,
    pub env: Option<String>,
    /// `run` or a manual action such as `state set`.
    pub kind: String,
    pub status: String,
    pub plan_hash: Option<String>,
    pub atlatl_version: String,
    pub detail: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RunReplicationRecord {
    pub run_id: String,
    pub external_run_id: Option<String>,
    pub replication: String,
    pub status: String,
    pub attempts: u32,
    pub lake_snapshot: Option<i64>,
    pub error_class: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Reject {
    pub stream_id: String,
    pub member: String,
    pub reject_key: String,
    pub request: serde_json::Value,
    pub raw: String,
    pub error_class: String,
    pub error: String,
    pub run_id: String,
    pub attempts: i32,
}

/// A stored reject as `atlatl rejects list` shows it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RejectRow {
    pub stream_id: String,
    pub member: String,
    pub reject_key: String,
    pub error_class: String,
    pub error: String,
    pub attempts: i32,
    pub first_seen_run: String,
    pub last_seen_run: String,
    pub first_seen_at: Option<String>,
    pub last_seen_at: Option<String>,
    pub request: serde_json::Value,
    pub raw: String,
}

type RawState = (
    String,
    String,
    String,
    String,
    String,
    String,
    Option<i64>,
    Option<String>,
);

/// The lake-hosted state store for one attached catalog.
#[derive(Clone, Debug)]
pub struct LakeState {
    alias: String,
}

impl LakeState {
    pub fn new(alias: impl Into<String>) -> Self {
        Self {
            alias: alias.into(),
        }
    }

    fn t(&self, table: &str) -> String {
        format!("{}.{SCHEMA}.{table}", ident(&self.alias))
    }

    /// Creates `_atlatl` and its tables if missing (idempotent).
    pub fn bootstrap(&self, c: &Connection) -> Result<()> {
        let a = ident(&self.alias);
        c.execute_batch(&format!(
            "CREATE SCHEMA IF NOT EXISTS {a}.{SCHEMA};
             CREATE TABLE IF NOT EXISTS {ss} (
               stream_id VARCHAR, member VARCHAR, kind VARCHAR, value JSON, config_hash VARCHAR,
               run_id VARCHAR, lake_snapshot BIGINT, updated_at TIMESTAMPTZ);
             CREATE TABLE IF NOT EXISTS {runs} (
               run_id VARCHAR, external_run_id VARCHAR, env VARCHAR, kind VARCHAR, status VARCHAR,
               plan_hash VARCHAR, atlatl_version VARCHAR, detail JSON, recorded_at TIMESTAMPTZ);
             CREATE TABLE IF NOT EXISTS {rr} (
               run_id VARCHAR, external_run_id VARCHAR, replication VARCHAR, status VARCHAR, attempts INTEGER,
               lake_snapshot BIGINT, error_class VARCHAR, error VARCHAR, recorded_at TIMESTAMPTZ);
             CREATE TABLE IF NOT EXISTS {rej} (
               stream_id VARCHAR, member VARCHAR, reject_key VARCHAR, request JSON, raw VARCHAR,
               error_class VARCHAR, error VARCHAR, first_seen_run VARCHAR, last_seen_run VARCHAR,
               attempts INTEGER, first_seen_at TIMESTAMPTZ, last_seen_at TIMESTAMPTZ);",
            ss = self.t("stream_state"),
            runs = self.t("runs"),
            rr = self.t("run_replications"),
            rej = self.t("rejects"),
        ))
        .map_err(db)
    }

    fn row(r: &duckdb::Row<'_>) -> duckdb::Result<RawState> {
        Ok((
            r.get(0)?,
            r.get(1)?,
            r.get(2)?,
            r.get(3)?,
            r.get(4)?,
            r.get(5)?,
            r.get(6)?,
            r.get(7)?,
        ))
    }

    fn to_state(t: RawState) -> Result<StateRow> {
        Ok(StateRow {
            stream_id: t.0,
            member: t.1,
            kind: StateKind::parse(&t.2)?,
            value: serde_json::from_str(&t.3)
                .map_err(|e| AtlatlError::internal(format!("state value: {e}")))?,
            config_hash: t.4,
            run_id: t.5,
            lake_snapshot: t.6,
            updated_at: t.7,
        })
    }

    const COLS: &'static str = "stream_id, member, kind, value::VARCHAR, config_hash, run_id, lake_snapshot, strftime(updated_at AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S.%gZ')";

    pub fn get(
        &self,
        c: &Connection,
        stream_id: &str,
        member: &str,
        kind: StateKind,
    ) -> Result<Option<StateRow>> {
        let sql = format!(
            "SELECT {} FROM {} WHERE stream_id = ? AND member = ? AND kind = ?",
            Self::COLS,
            self.t("stream_state")
        );
        let t = c
            .query_row(&sql, params![stream_id, member, kind.as_str()], Self::row)
            .optional()
            .map_err(db)?;
        t.map(Self::to_state).transpose()
    }

    /// State for `stream_id`, failing with `config` if it was produced by a different definition
    /// (05 §4).
    pub fn get_checked(
        &self,
        c: &Connection,
        stream_id: &str,
        member: &str,
        kind: StateKind,
        config_hash: &str,
    ) -> Result<Option<StateRow>> {
        let row = self.get(c, stream_id, member, kind)?;
        if let Some(r) = &row
            && r.config_hash != config_hash
        {
            return Err(AtlatlError::config(format!(
                "state for `{stream_id}` was produced by a different definition (hash {} ≠ {config_hash}); review, then `atlatl state accept {stream_id}` or `atlatl state reset {stream_id}`",
                r.config_hash
            ))
            .code("config_drift"));
        }
        Ok(row)
    }

    pub fn list(&self, c: &Connection, stream_id: Option<&str>) -> Result<Vec<StateRow>> {
        let sql = format!(
            "SELECT {} FROM {} WHERE ? IS NULL OR stream_id = ? ORDER BY stream_id, member, kind",
            Self::COLS,
            self.t("stream_state")
        );
        let mut s = c.prepare(&sql).map_err(db)?;
        let rows = s
            .query_map(params![stream_id, stream_id], Self::row)
            .map_err(db)?;
        rows.map(|r| r.map_err(db).and_then(Self::to_state))
            .collect()
    }

    /// Upserts one state row. Call inside the replication's transaction.
    pub fn put(&self, c: &Connection, r: &StateRow) -> Result<()> {
        let sql = format!(
            "MERGE INTO {t} AS t USING (SELECT ? AS stream_id, ? AS member, ? AS kind, ?::JSON AS value, ? AS config_hash,
                ? AS run_id, ?::BIGINT AS lake_snapshot, now() AS updated_at) s
             ON t.stream_id = s.stream_id AND t.member = s.member AND t.kind = s.kind
             WHEN MATCHED THEN UPDATE SET value = s.value, config_hash = s.config_hash, run_id = s.run_id,
                lake_snapshot = s.lake_snapshot, updated_at = s.updated_at
             WHEN NOT MATCHED THEN INSERT *",
            t = self.t("stream_state")
        );
        c.execute(
            &sql,
            params![
                r.stream_id,
                r.member,
                r.kind.as_str(),
                r.value.to_string(),
                r.config_hash,
                r.run_id,
                r.lake_snapshot
            ],
        )
        .map_err(db)?;
        Ok(())
    }

    /// Deletes a stream's state (all members, or one). Returns rows removed.
    pub fn reset(&self, c: &Connection, stream_id: &str, member: Option<&str>) -> Result<usize> {
        let sql = format!(
            "DELETE FROM {} WHERE stream_id = ? AND (? IS NULL OR member = ?)",
            self.t("stream_state")
        );
        c.execute(&sql, params![stream_id, member, member])
            .map_err(db)
    }

    /// Re-stamps `config_hash` after a reviewed config change.
    pub fn accept(&self, c: &Connection, stream_id: &str, config_hash: &str) -> Result<usize> {
        let sql = format!(
            "UPDATE {} SET config_hash = ?, updated_at = now() WHERE stream_id = ?",
            self.t("stream_state")
        );
        c.execute(&sql, params![config_hash, stream_id]).map_err(db)
    }

    /// Renames a stream id in state and rejects (`atlatl state move`).
    pub fn rename(&self, c: &Connection, old: &str, new: &str) -> Result<usize> {
        let taken: i64 = c
            .query_row(
                &format!(
                    "SELECT count(*) FROM {} WHERE stream_id = ?",
                    self.t("stream_state")
                ),
                params![new],
                |r| r.get(0),
            )
            .map_err(db)?;
        if taken > 0 {
            return Err(AtlatlError::config(format!(
                "state already exists for `{new}`; reset it first"
            )));
        }
        let n = c
            .execute(
                &format!(
                    "UPDATE {} SET stream_id = ? WHERE stream_id = ?",
                    self.t("stream_state")
                ),
                params![new, old],
            )
            .map_err(db)?;
        c.execute(
            &format!(
                "UPDATE {} SET stream_id = ? WHERE stream_id = ?",
                self.t("rejects")
            ),
            params![new, old],
        )
        .map_err(db)?;
        Ok(n)
    }

    /// `update_key` state seeded from `max(column)` of an existing table (`state init --from-target`).
    /// The value keeps its exact type: `{"column", "type", "value"}` (05 §2).
    pub fn seed_from_target(
        &self,
        c: &Connection,
        table: &str,
        column: &str,
    ) -> Result<Option<serde_json::Value>> {
        let sql = format!(
            "SELECT max({col})::VARCHAR, typeof(max({col})) FROM {table}",
            col = ident(column)
        );
        let (v, ty): (Option<String>, String) = c
            .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?)))
            .map_err(db)?;
        Ok(v.map(|v| serde_json::json!({"column": column, "type": ty, "value": v})))
    }

    pub fn record_run(&self, c: &Connection, r: &RunRecord) -> Result<()> {
        let sql = format!(
            "INSERT INTO {} VALUES (?, ?, ?, ?, ?, ?, ?, ?::JSON, now())",
            self.t("runs")
        );
        c.execute(
            &sql,
            params![
                r.run_id,
                r.external_run_id,
                r.env,
                r.kind,
                r.status,
                r.plan_hash,
                r.atlatl_version,
                r.detail.to_string()
            ],
        )
        .map_err(db)?;
        Ok(())
    }

    pub fn record_replication(&self, c: &Connection, r: &RunReplicationRecord) -> Result<()> {
        let sql = format!(
            "INSERT INTO {} VALUES (?, ?, ?, ?, ?, ?, ?, ?, now())",
            self.t("run_replications")
        );
        c.execute(
            &sql,
            params![
                r.run_id,
                r.external_run_id,
                r.replication,
                r.status,
                r.attempts,
                r.lake_snapshot,
                r.error_class,
                r.error
            ],
        )
        .map_err(db)?;
        Ok(())
    }

    /// Replications that succeeded under `external_run_id` (for `--skip-succeeded`).
    pub fn succeeded_under(
        &self,
        c: &Connection,
        external_run_id: &str,
    ) -> Result<HashSet<String>> {
        let sql = format!(
            "SELECT DISTINCT replication FROM {} WHERE external_run_id = ? AND status IN ('succeeded', 'up_to_date')",
            self.t("run_replications")
        );
        let mut s = c.prepare(&sql).map_err(db)?;
        let rows = s
            .query_map(params![external_run_id], |r| r.get::<_, String>(0))
            .map_err(db)?;
        rows.map(|r| r.map_err(db)).collect()
    }

    /// Inserts a new reject or bumps an existing one (by stream, member, key).
    pub fn upsert_reject(&self, c: &Connection, r: &Reject) -> Result<()> {
        let sql = format!(
            "MERGE INTO {t} AS t USING (SELECT ? AS stream_id, ? AS member, ? AS reject_key, ?::JSON AS request, ? AS raw,
                ? AS error_class, ? AS error, ? AS run_id) s
             ON t.stream_id = s.stream_id AND t.member = s.member AND t.reject_key = s.reject_key
             WHEN MATCHED THEN UPDATE SET request = s.request, raw = s.raw, error_class = s.error_class, error = s.error,
                last_seen_run = s.run_id, attempts = t.attempts + 1, last_seen_at = now()
             WHEN NOT MATCHED THEN INSERT (stream_id, member, reject_key, request, raw, error_class, error, first_seen_run,
                last_seen_run, attempts, first_seen_at, last_seen_at)
                VALUES (s.stream_id, s.member, s.reject_key, s.request, s.raw, s.error_class, s.error, s.run_id, s.run_id, 1, now(), now())",
            t = self.t("rejects")
        );
        c.execute(
            &sql,
            params![
                r.stream_id,
                r.member,
                r.reject_key,
                r.request.to_string(),
                r.raw,
                r.error_class,
                r.error,
                r.run_id
            ],
        )
        .map_err(db)?;
        Ok(())
    }

    /// Deletes rejects that now decode (in the same commit as the rows that replaced them).
    pub fn resolve_rejects(
        &self,
        c: &Connection,
        stream_id: &str,
        member: &str,
        keys: &[String],
    ) -> Result<usize> {
        let mut n = 0;
        let sql = format!(
            "DELETE FROM {} WHERE stream_id = ? AND member = ? AND reject_key = ?",
            self.t("rejects")
        );
        for k in keys {
            n += c.execute(&sql, params![stream_id, member, k]).map_err(db)?;
        }
        Ok(n)
    }

    /// Keys of `stream_id`/`member` `skipped` rejects last seen within `ttl`
    /// (`for_each.except_skipped`, 07 §7).
    pub fn recent_skips(
        &self,
        c: &Connection,
        stream_id: &str,
        member: &str,
        ttl: std::time::Duration,
    ) -> Result<Vec<String>> {
        let sql = format!(
            "SELECT reject_key FROM {} WHERE stream_id = ? AND member = ? AND error_class = 'skipped'
               AND last_seen_at >= now() - to_microseconds(?::BIGINT) ORDER BY reject_key",
            self.t("rejects")
        );
        let us = i64::try_from(ttl.as_micros()).unwrap_or(i64::MAX);
        let mut s = c.prepare(&sql).map_err(db)?;
        let rows = s
            .query_map(params![stream_id, member, us], |r| r.get::<_, String>(0))
            .map_err(db)?;
        rows.map(|r| r.map_err(db)).collect()
    }

    /// Keys of `stream_id`/`member` rejects first seen more than `ttl` ago (`reject_ttl`, 03 §7).
    pub fn expired_rejects(
        &self,
        c: &Connection,
        stream_id: &str,
        member: &str,
        ttl: std::time::Duration,
    ) -> Result<Vec<String>> {
        let sql = format!(
            "SELECT reject_key FROM {} WHERE stream_id = ? AND member = ?
               AND first_seen_at < now() - to_microseconds(?::BIGINT) ORDER BY reject_key",
            self.t("rejects")
        );
        let us = i64::try_from(ttl.as_micros()).unwrap_or(i64::MAX);
        let mut s = c.prepare(&sql).map_err(db)?;
        let rows = s
            .query_map(params![stream_id, member, us], |r| r.get::<_, String>(0))
            .map_err(db)?;
        rows.map(|r| r.map_err(db)).collect()
    }

    /// Every stored reject, ordered by stream, member and key.
    pub fn list_rejects(&self, c: &Connection) -> Result<Vec<RejectRow>> {
        let ts = |col: &str| format!("strftime({col} AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S.%gZ')");
        let sql = format!(
            "SELECT stream_id, member, reject_key, error_class, error, attempts, first_seen_run,
                last_seen_run, {}, {}, request::VARCHAR, raw
             FROM {} ORDER BY stream_id, member, reject_key",
            ts("first_seen_at"),
            ts("last_seen_at"),
            self.t("rejects")
        );
        let mut s = c.prepare(&sql).map_err(db)?;
        let rows = s
            .query_map([], |r| {
                Ok(RejectRow {
                    stream_id: r.get(0)?,
                    member: r.get(1)?,
                    reject_key: r.get(2)?,
                    error_class: r.get(3)?,
                    error: r.get(4)?,
                    attempts: r.get(5)?,
                    first_seen_run: r.get(6)?,
                    last_seen_run: r.get(7)?,
                    first_seen_at: r.get(8)?,
                    last_seen_at: r.get(9)?,
                    request: serde_json::from_str(&r.get::<_, String>(10)?)
                        .unwrap_or(serde_json::Value::Null),
                    raw: r.get(11)?,
                })
            })
            .map_err(db)?;
        rows.map(|r| r.map_err(db)).collect()
    }

    /// Deletes a stream's rejects: all, one member's, or only `keys` (`atlatl rejects drop`).
    pub fn drop_rejects(
        &self,
        c: &Connection,
        stream_id: &str,
        member: Option<&str>,
        keys: &[String],
    ) -> Result<usize> {
        let sql = format!(
            "DELETE FROM {} WHERE stream_id = ? AND (? IS NULL OR member = ?) AND (? IS NULL OR reject_key = ?)",
            self.t("rejects")
        );
        if keys.is_empty() {
            let none: Option<&str> = None;
            return c
                .execute(&sql, params![stream_id, member, member, none, none])
                .map_err(db);
        }
        let mut n = 0;
        for k in keys {
            n += c
                .execute(&sql, params![stream_id, member, member, k, k])
                .map_err(db)?;
        }
        Ok(n)
    }

    pub fn open_rejects(&self, c: &Connection, stream_id: &str) -> Result<Vec<Reject>> {
        let sql = format!(
            "SELECT stream_id, member, reject_key, request::VARCHAR, raw, error_class, error, last_seen_run, attempts
             FROM {} WHERE stream_id = ? ORDER BY member, reject_key",
            self.t("rejects")
        );
        let mut s = c.prepare(&sql).map_err(db)?;
        let rows = s
            .query_map(params![stream_id], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, String>(5)?,
                    r.get::<_, String>(6)?,
                    r.get::<_, String>(7)?,
                    r.get::<_, i32>(8)?,
                ))
            })
            .map_err(db)?;
        rows.map(|r| {
            let t = r.map_err(db)?;
            Ok(Reject {
                stream_id: t.0,
                member: t.1,
                reject_key: t.2,
                request: serde_json::from_str(&t.3).unwrap_or(serde_json::Value::Null),
                raw: t.4,
                error_class: t.5,
                error: t.6,
                run_id: t.7,
                attempts: t.8,
            })
        })
        .collect()
    }
}

#[cfg(test)]
mod tests;
