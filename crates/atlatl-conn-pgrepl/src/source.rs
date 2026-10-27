//! The pgoutput change source (08 §10, 11 §2): prepare the owned slot and publication, backfill
//! streams without a cursor, then stream a bounded window into staging. The window is applied
//! to the lake by [`crate::apply`]; only after that commit is the position confirmed.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use atlatl_conn_postgres::{ConnectOptions, Session, wrap};
use atlatl_core::config::model::{DeleteMode, PgProtocol};
use atlatl_core::error::Result;
use atlatl_core::{AtlatlError, ErrorClass};
use atlatl_state::{LakeState, StateKind};
use futures::StreamExt;
use rustls::ClientConfig;
use serde::Serialize;

use crate::binary::{Plans, render};
use crate::lsn::Lsn;
use crate::pgoutput::{Col, Column, Msg, Relation, decode};
use crate::slot::{self, SlotPolicy, SlotStats, quote_ident};
use crate::stage::{KeyChange, Op, Row, Stage, StageCol, copy_fields};
use crate::wire::{Repl, ReplConn};

#[derive(Clone, Debug, PartialEq)]
pub struct StreamSpec {
    pub id: String,
    pub source_schema: String,
    pub source_table: String,
    /// Lake schema and table.
    pub target_schema: String,
    pub target_table: String,
    /// Defaults to the source table's primary key.
    pub primary_key: Option<Vec<String>>,
    pub deletes: DeleteMode,
    /// Append every netted change instead of upserting.
    pub history: bool,
    pub config_hash: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Limits {
    pub max_changes: Option<u64>,
    pub max_duration: Option<Duration>,
    /// Fail when the server sends nothing (not even a keepalive) for this long.
    pub idle_timeout: Duration,
    /// How often to ask the server for a keepalive while waiting.
    pub ping: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_changes: None,
            max_duration: None,
            idle_timeout: Duration::from_secs(60),
            ping: Duration::from_millis(250),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReplicationSpec {
    pub name: String,
    pub streams: Vec<StreamSpec>,
    pub limits: Limits,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    CaughtUp,
    MaxChanges,
    MaxDuration,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WindowSummary {
    pub slot: String,
    pub slot_created: bool,
    /// The slot's confirmed position when streaming started.
    pub from: Lsn,
    /// `pg_current_wal_lsn()` after the backfill: the window's goal.
    pub head: Lsn,
    /// The position the lake will record: the last commit consumed, or an idle keepalive's.
    pub upto: Lsn,
    /// Transactions with changes to this replication's tables.
    pub transactions: u64,
    /// Such transactions skipped as already applied (`end_lsn <= cursor`).
    pub skipped_transactions: u64,
    pub changes: u64,
    /// Changes that arrived with unchanged TOAST columns (carried from earlier values).
    pub unchanged_toast: u64,
    pub stop: StopReason,
    /// Streams copied in full because they had no cursor.
    pub backfilled: Vec<String>,
    /// The pgoutput protocol version negotiated (1, or 2 with `streaming 'on'`).
    pub protocol: u32,
    /// Values arrived in the types' binary format.
    pub binary: bool,
    /// Committed transactions that arrived as in-progress stream blocks.
    pub streamed_transactions: u64,
}

pub(crate) struct TableMeta {
    pub cols: Vec<StageCol>,
    pub pk: Vec<String>,
}

/// One replication's pgoutput source. Holds the replication connection from streaming until
/// [`PgChangeSource::confirm`].
pub struct PgChangeSource {
    opts: ConnectOptions,
    tls: Option<Arc<ClientConfig>>,
    policy: SlotPolicy,
    rep: ReplicationSpec,
    slot: String,
    sql: Session,
    repl: Option<ReplConn>,
    confirmed: Lsn,
    /// `START_REPLICATION` options, fixed per window.
    options: String,
    protocol: u32,
    binary: bool,
    plans: Plans,
}

fn is_busy(e: &AtlatlError) -> bool {
    e.code.as_deref() == Some("55006")
}

fn key_json(vals: &[Option<&str>]) -> String {
    serde_json::Value::from(
        vals.iter()
            .map(|v| v.map_or(serde_json::Value::Null, |s| s.into()))
            .collect::<Vec<_>>(),
    )
    .to_string()
}

/// A column value as the text staging holds; binary values are rendered as the server's text.
fn text(plans: &Plans, rel: &Relation, col: &Column, c: &Col) -> Result<Option<String>> {
    match c {
        Col::Null => Ok(None),
        Col::Unchanged => Ok(None),
        Col::Text(b) => String::from_utf8(b.to_vec())
            .map(Some)
            .map_err(|_| AtlatlError::internal("pgoutput value is not UTF-8")),
        Col::Binary(b) => match plans.get(col.type_oid) {
            Some(p) => render(p, b).map(Some),
            None => Err(AtlatlError::transient(format!(
                "column `{}` of {}.{} has type oid {}, which has no binary decoder",
                col.name, rel.namespace, rel.name, col.type_oid
            ))
            .code("binary_type")),
        },
    }
}

/// `START_REPLICATION` options for `protocol` on a server of `version` (`server_version_num`).
fn negotiate(protocol: PgProtocol, version: i32) -> Result<u32> {
    match protocol {
        PgProtocol::V1 => Ok(1),
        PgProtocol::V2 if version < 140000 => Err(AtlatlError::config(format!(
            "`changes.protocol: v2` needs PostgreSQL 14 or later (the server is {version})"
        ))),
        PgProtocol::V2 => Ok(2),
        PgProtocol::Auto => Ok(if version >= 140000 { 2 } else { 1 }),
    }
}

impl PgChangeSource {
    pub async fn open(
        opts: ConnectOptions,
        policy: SlotPolicy,
        rep: ReplicationSpec,
    ) -> Result<Self> {
        if rep.streams.is_empty() {
            return Err(AtlatlError::config(format!(
                "replication `{}` has no streams",
                rep.name
            )));
        }
        for (i, a) in rep.streams.iter().enumerate() {
            if rep.streams[..i]
                .iter()
                .any(|b| (&b.source_schema, &b.source_table) == (&a.source_schema, &a.source_table))
            {
                return Err(AtlatlError::config(format!(
                    "replication `{}` reads {}.{} in more than one stream",
                    rep.name, a.source_schema, a.source_table
                )));
            }
        }
        let tls = opts.tls()?;
        let sql = atlatl_conn_postgres::connect_with(&opts, tls.as_ref()).await?;
        let slot = slot::slot_name(&rep.name);
        Ok(Self {
            opts,
            tls,
            policy,
            rep,
            slot,
            sql,
            repl: None,
            confirmed: Lsn::ZERO,
            options: String::new(),
            protocol: 1,
            binary: false,
            plans: Plans::default(),
        })
    }

    pub fn slot(&self) -> &str {
        &self.slot
    }

    pub fn spec(&self) -> &ReplicationSpec {
        &self.rep
    }

    /// `pg_current_wal_lsn()`.
    pub async fn head(&self) -> Result<Lsn> {
        slot::current_wal_lsn(&self.sql).await
    }

    pub async fn stats(&self) -> Result<Option<SlotStats>> {
        slot::stats(&self.sql, &self.slot).await
    }

    /// The slot still holds everything after `cursor`. The server silently starts at
    /// `confirmed_flush_lsn`, so a slot confirmed past the cursor means a gap (`data`).
    pub async fn check_retained(&self, cursor: Lsn) -> Result<()> {
        let row = slot::slot_row(&self.sql, &self.slot).await?;
        match row.and_then(|r| r.confirmed_flush) {
            None => Err(AtlatlError::data(format!(
                "replication slot `{}` does not exist, so changes after {cursor} are lost; `atlatl state reset` the replication's streams to resync",
                self.slot
            ))
            .code("slot_missing")),
            Some(c) if c > cursor => Err(AtlatlError::data(format!(
                "replication slot `{}` is confirmed to {c}, past the lake's cursor {cursor}: the changes between were consumed elsewhere; `atlatl state reset` the replication's streams to resync",
                self.slot
            ))
            .code("cursor_gap")),
            Some(_) => Ok(()),
        }
    }

    async fn table_meta(&self, s: &StreamSpec) -> Result<TableMeta> {
        let rows = self
            .sql
            .client()
            .query(
                "SELECT a.attname::text, a.atttypid::int8, a.atttypmod,
                        array_position(i.indkey::int2[], a.attnum)
                 FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
                 LEFT JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary
                 WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'r'
                 ORDER BY a.attnum",
                &[&s.source_schema, &s.source_table],
            )
            .await
            .map_err(wrap)?;
        if rows.is_empty() {
            return Err(AtlatlError::config(format!(
                "stream `{}`: source table {}.{} does not exist or is not a plain table (partitioned tables are not supported)",
                s.id, s.source_schema, s.source_table
            )));
        }
        let mut pk: Vec<(i32, String)> = Vec::new();
        let cols = rows
            .iter()
            .map(|r| {
                let name: String = r.get(0);
                if let Some(p) = r.get::<_, Option<i32>>(3) {
                    pk.push((p, name.clone()));
                }
                StageCol {
                    name,
                    oid: r.get::<_, i64>(1) as u32,
                    typmod: r.get(2),
                }
            })
            .collect::<Vec<_>>();
        pk.sort();
        let pk = match &s.primary_key {
            Some(k) => k.clone(),
            None if !pk.is_empty() => pk.into_iter().map(|(_, n)| n).collect(),
            None => {
                return Err(AtlatlError::config(format!(
                    "stream `{}`: {}.{} has no primary key; set `primary_key`",
                    s.id, s.source_schema, s.source_table
                )));
            }
        };
        for k in &pk {
            if !cols.iter().any(|c| &c.name == k) {
                return Err(AtlatlError::config(format!(
                    "stream `{}`: primary key column `{k}` is not in {}.{}",
                    s.id, s.source_schema, s.source_table
                )));
            }
        }
        Ok(TableMeta { cols, pk })
    }

    fn cursors(&self, duck: &duckdb::Connection, lake: &LakeState) -> Result<Vec<Option<Lsn>>> {
        lake.bootstrap(duck)?;
        self.rep
            .streams
            .iter()
            .map(|s| {
                if self.policy.temporary() {
                    return Ok(None);
                }
                let Some(row) = lake.get_checked(duck, &s.id, "", StateKind::Lsn, &s.config_hash)?
                else {
                    return Ok(None);
                };
                let slot = row.value.get("slot").and_then(|v| v.as_str());
                if slot != Some(self.slot.as_str()) {
                    return Err(AtlatlError::config(format!(
                        "the cursor of stream `{}` belongs to slot {slot:?}, not `{}`; `atlatl state reset {}` to resync",
                        s.id, self.slot, s.id
                    )));
                }
                let lsn = row
                    .value
                    .get("lsn")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| AtlatlError::internal(format!("stream `{}`: malformed lsn state", s.id)))?;
                Ok(Some(lsn.parse()?))
            })
            .collect()
    }

    /// Makes sure the publication covers exactly this replication's tables, and returns whether
    /// the slot must be (re)created.
    async fn prepare(&mut self, cursors: &[Option<Lsn>]) -> Result<(bool, Lsn)> {
        let any_cursor = cursors.iter().any(Option::is_some);
        let mut want: Vec<(String, String)> = self
            .rep
            .streams
            .iter()
            .map(|s| (s.source_schema.clone(), s.source_table.clone()))
            .collect();
        want.sort();
        want.dedup();
        let mut row = slot::slot_row(&self.sql, &self.slot).await?;
        let t = Instant::now();
        // A killed run's walsender can hold the slot for a moment after its client is gone.
        while row.as_ref().is_some_and(|r| r.active) && t.elapsed() < Duration::from_secs(10) {
            tokio::time::sleep(Duration::from_millis(100)).await;
            row = slot::slot_row(&self.sql, &self.slot).await?;
        }
        let mut create = false;
        let mut from = Lsn::ZERO;
        match &row {
            Some(_) if self.policy.temporary() => {
                return Err(AtlatlError::config(format!(
                    "slot `{}` already exists; a temporary slot needs the name free",
                    self.slot
                )));
            }
            Some(r) => {
                if r.slot_type != "logical"
                    || r.plugin.as_deref() != Some("pgoutput")
                    || r.database.as_deref() != Some(self.opts.database.as_str())
                {
                    return Err(AtlatlError::config(format!(
                        "slot `{}` exists but is not a pgoutput slot on database `{}` ({:?} {:?} on {:?})",
                        self.slot, self.opts.database, r.slot_type, r.plugin, r.database
                    ))
                    .code("slot_foreign"));
                }
                if r.active {
                    return Err(AtlatlError::conflict(format!(
                        "slot `{}` is in use by pid {:?} (another run of this replication?)",
                        self.slot, r.active_pid
                    ))
                    .code("slot_active"));
                }
                from = r.confirmed_flush.unwrap_or(Lsn::ZERO);
                for (s, c) in self.rep.streams.iter().zip(cursors) {
                    if let Some(c) = c
                        && from > *c
                    {
                        return Err(AtlatlError::data(format!(
                            "slot `{}` is confirmed to {from}, past stream `{}`'s cursor {c}: those changes were consumed elsewhere; `atlatl state reset {}` to resync",
                            self.slot, s.id, s.id
                        ))
                        .code("cursor_gap"));
                    }
                }
            }
            None if any_cursor => {
                return Err(AtlatlError::data(format!(
                    "slot `{}` does not exist but the lake has cursors for this replication: changes since then are lost; `atlatl state reset` its streams to resync",
                    self.slot
                ))
                .code("slot_missing"));
            }
            None if !self.policy.allow_create() => {
                return Err(AtlatlError::config(format!(
                    "slot `{}` does not exist and `changes.slots.allow_create` is false for this connection",
                    self.slot
                ))
                .code("slot_absent"));
            }
            None => create = true,
        }

        match slot::publication_tables(&self.sql, &self.slot).await? {
            None => {
                if !self.policy.allow_create() {
                    return Err(AtlatlError::config(format!(
                        "publication `{}` does not exist and `changes.slots.allow_create` is false",
                        self.slot
                    ))
                    .code("publication_absent"));
                }
                if !create {
                    // Decoding WAL from before the publication existed fails, so start over.
                    if any_cursor {
                        return Err(AtlatlError::config(format!(
                            "publication `{}` is missing but its slot and cursors exist; decommission the replication and reset its state",
                            self.slot
                        ))
                        .code("publication_missing"));
                    }
                    self.drop_slot().await?;
                    create = true;
                }
                slot::create_publication(&self.sql, &self.slot, &want).await?;
            }
            Some(have) if have != want => {
                if !self.policy.allow_create() {
                    return Err(AtlatlError::config(format!(
                        "publication `{}` covers {have:?}, not {want:?}, and `changes.slots.allow_create` is false",
                        self.slot
                    ))
                    .code("publication_mismatch"));
                }
                for (s, c) in self.rep.streams.iter().zip(cursors) {
                    if c.is_some()
                        && !have.contains(&(s.source_schema.clone(), s.source_table.clone()))
                    {
                        return Err(AtlatlError::data(format!(
                            "stream `{}` has a cursor but its table was not published; `atlatl state reset {}` to resync",
                            s.id, s.id
                        ))
                        .code("cursor_gap"));
                    }
                }
                slot::set_publication_tables(&self.sql, &self.slot, &want).await?;
            }
            Some(_) => {}
        }
        Ok((create, from))
    }

    async fn drop_slot(&self) -> Result<()> {
        self.sql
            .client()
            .execute("SELECT pg_drop_replication_slot($1)", &[&self.slot])
            .await
            .map_err(wrap)?;
        Ok(())
    }

    async fn backfill(
        &self,
        duck: duckdb::Connection,
        stage: &mut Stage,
        s: &StreamSpec,
        seq: &mut i64,
    ) -> Result<duckdb::Connection> {
        *seq += 1;
        stage.push(
            &duck,
            Row {
                seq: *seq,
                txn: 0,
                op: Op::Truncate,
                from: None,
                fb: None,
                toast: vec![],
                values: vec![],
            },
        )?;
        let copy = atlatl_conn_postgres::connect_with(&self.opts, self.tls.as_ref()).await?;
        copy.client()
            .batch_execute(
                "SET TimeZone = 'UTC'; SET DateStyle = 'ISO, MDY'; SET IntervalStyle = 'postgres';
                 SET extra_float_digits = 3; SET bytea_output = 'hex';
                 BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
            )
            .await
            .map_err(wrap)?;
        let names: Vec<String> = stage.cols.iter().map(|c| c.name.clone()).collect();
        let sql = format!(
            "COPY {}.{} ({}) TO STDOUT",
            quote_ident(&s.source_schema),
            quote_ident(&s.source_table),
            names
                .iter()
                .map(|n| quote_ident(n))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let out = copy.client().copy_out(&sql).await.map_err(wrap)?;
        futures::pin_mut!(out);
        let mut pending: Vec<u8> = Vec::new();
        while let Some(chunk) = out.next().await {
            let chunk = chunk.map_err(wrap)?;
            pending.extend_from_slice(&chunk);
            let mut start = 0;
            while let Some(p) = pending[start..].iter().position(|&b| b == b'\n') {
                copy_row(&duck, stage, s, &names, &pending[start..start + p], seq)?;
                start += p + 1;
            }
            pending.drain(..start);
        }
        if !pending.is_empty() {
            copy_row(&duck, stage, s, &names, &pending, seq)?;
        }
        copy.client().batch_execute("COMMIT").await.map_err(wrap)?;
        stage.flush(&duck)?;
        Ok(duck)
    }

    async fn start_replication(&mut self, create: bool) -> Result<Lsn> {
        let mut conn = ReplConn::connect(&self.opts, self.tls.as_ref()).await?;
        let mut consistent = None;
        if create {
            let temp = if self.policy.temporary() {
                "TEMPORARY "
            } else {
                ""
            };
            let r = conn
                .query(&format!(
                    "CREATE_REPLICATION_SLOT {} {temp}LOGICAL pgoutput NOEXPORT_SNAPSHOT",
                    self.slot
                ))
                .await?;
            let p = r
                .first()
                .and_then(|r| r.get(1).cloned().flatten())
                .ok_or_else(|| {
                    AtlatlError::internal("CREATE_REPLICATION_SLOT returned no consistent point")
                })?;
            consistent = Some(p.parse::<Lsn>()?);
            tracing::info!(slot = %self.slot, consistent_point = %p, temporary = self.policy.temporary(), "created replication slot");
        }
        self.repl = Some(conn);
        Ok(consistent.unwrap_or(Lsn::ZERO))
    }

    /// Picks the protocol, and binary values when every published column's type has a decoder.
    async fn negotiate(&mut self, oids: &[u32]) -> Result<()> {
        let version: i32 = self
            .sql
            .client()
            .query_one("SELECT current_setting('server_version_num')::int", &[])
            .await
            .map_err(wrap)?
            .get(0);
        self.protocol = negotiate(self.policy.protocol(), version)?;
        self.binary = false;
        if self.protocol >= 2 {
            let missing = self.plans.resolve(&self.sql, oids).await?;
            self.binary = missing.is_empty();
            if !self.binary {
                tracing::info!(slot = %self.slot, type_oids = ?missing, "binary values off: no decoder for these column types");
            }
        }
        self.options = self.start_options();
        Ok(())
    }

    fn start_options(&self) -> String {
        match (self.protocol, self.binary) {
            (1, _) => format!("proto_version '1', publication_names '{}'", self.slot),
            (_, b) => format!(
                "proto_version '2', publication_names '{}', streaming 'on'{}",
                self.slot,
                if b { ", binary 'true'" } else { "" }
            ),
        }
    }

    /// A column type without a binary decoder appeared mid-window: stream again from the slot's
    /// confirmed position with text values. A temporary slot would not survive the reconnect.
    async fn restart_as_text(&mut self) -> Result<()> {
        if self.policy.temporary() {
            return Err(AtlatlError::transient(format!(
                "slot `{}`: a column type without a binary decoder appeared; the next run reads text",
                self.slot
            ))
            .code("binary_type"));
        }
        self.binary = false;
        self.options = self.start_options();
        if let Some(c) = self.repl.take() {
            let _ = c.terminate().await;
        }
        self.repl = Some(ReplConn::connect(&self.opts, self.tls.as_ref()).await?);
        self.stream_start().await
    }

    async fn stream_start(&mut self) -> Result<()> {
        let options = self.options.clone();
        let t = Instant::now();
        loop {
            let conn = self
                .repl
                .as_mut()
                .ok_or_else(|| AtlatlError::internal("no replication connection"))?;
            match conn.start(&self.slot, Lsn::ZERO, &options).await {
                Ok(()) => return Ok(()),
                // The previous walsender may still hold the slot for a moment.
                Err(e) if is_busy(&e) && t.elapsed() < Duration::from_secs(10) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Stages this replication's next window: backfills streams without a cursor, then consumes
    /// committed transactions up to `pg_current_wal_lsn()` (or a limit). Nothing is confirmed.
    /// `duck` is an engine connection with the lake attached as `lake_alias`; the window owns
    /// it and applies through it.
    pub async fn changes(
        &mut self,
        duck: duckdb::Connection,
        lake_alias: &str,
        run_id: &str,
    ) -> Result<crate::apply::Window> {
        let lake = LakeState::new(lake_alias);
        let cursors = self.cursors(&duck, &lake)?;
        let mut metas = Vec::with_capacity(self.rep.streams.len());
        for s in &self.rep.streams {
            metas.push(self.table_meta(s).await?);
        }
        let oids: Vec<u32> = metas
            .iter()
            .flat_map(|m| m.cols.iter().map(|c| c.oid))
            .collect();
        self.negotiate(&oids).await?;
        let (create, slot_from) = self.prepare(&cursors).await?;
        let consistent = self.start_replication(create).await?;
        let from = if create { consistent } else { slot_from };
        self.confirmed = from;

        let tag: String = run_id
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .rev()
            .take(8)
            .collect::<String>()
            .to_lowercase();
        let mut stages: Vec<Stage> = Vec::with_capacity(metas.len());
        let mut seq = 0i64;
        let mut backfilled = Vec::new();
        let mut duck = duck;
        for (i, m) in metas.into_iter().enumerate() {
            let table = format!("pgrepl_{tag}_{i}");
            let mut st = Stage::create(&duck, &table, m.cols, m.pk)?;
            if cursors[i].is_none() {
                let s = self.rep.streams[i].clone();
                duck = self.backfill(duck, &mut st, &s, &mut seq).await?;
                backfilled.push(s.id.clone());
            }
            stages.push(st);
        }
        let head = self.head().await?;
        self.stream_start().await?;

        let (stop, w) = loop {
            let mut w = Streamer {
                src: self,
                duck,
                stages: &mut stages,
                cursors: &cursors,
                seq,
                rels: HashMap::new(),
                txn: None,
                ordinal: 0,
                txns: Vec::new(),
                upto: from,
                transactions: 0,
                skipped: 0,
                changes: 0,
                unchanged_toast: 0,
                in_stream: None,
                streamed: HashMap::new(),
                pseq: 0,
                at: (0, false),
                streamed_transactions: 0,
            };
            let stop = w.run(head).await?;
            w.discard_streams()?;
            if let Some(stop) = stop {
                break (stop, w);
            }
            duck = w.duck;
            for st in stages.iter_mut() {
                st.discard_changes(&duck)?;
            }
            self.restart_as_text().await?;
        };
        let (unchanged_toast, streamed_transactions) = (w.unchanged_toast, w.streamed_transactions);
        let (duck, txns, upto, transactions, skipped, changes) =
            (w.duck, w.txns, w.upto, w.transactions, w.skipped, w.changes);
        for st in stages.iter_mut() {
            st.flush(&duck)?;
        }
        let summary = WindowSummary {
            slot: self.slot.clone(),
            slot_created: create,
            from,
            head,
            upto,
            transactions,
            skipped_transactions: skipped,
            changes,
            unchanged_toast,
            stop,
            backfilled,
            protocol: self.protocol,
            binary: self.binary,
            streamed_transactions,
        };
        tracing::info!(slot = %self.slot, %from, %head, %upto, transactions, skipped, changes, ?stop, "pgoutput window staged");
        crate::apply::Window::new(
            duck,
            lake,
            lake_alias,
            self.rep.clone(),
            run_id.to_owned(),
            stages,
            cursors,
            txns,
            summary,
        )
    }

    /// Confirms `lsn` as flushed: call only after the lake commit that recorded it. Reconnects
    /// if the replication connection dropped during the commit.
    pub async fn confirm(&mut self, lsn: Lsn) -> Result<()> {
        const ACK: Duration = Duration::from_secs(5);
        let sent = match self.repl.as_mut() {
            Some(conn) => conn.status(lsn, true).await.map_err(Some),
            None => Err(None),
        };
        // A write to a connection the server already dropped can still succeed, so check the
        // slot itself.
        if sent.is_ok() && self.wait_confirmed(lsn, ACK).await? >= lsn {
            self.confirmed = self.confirmed.max(lsn);
            return Ok(());
        }
        if self.policy.temporary() {
            return Err(sent.err().flatten().unwrap_or_else(|| {
                AtlatlError::transient("the temporary slot's session ended before the confirm")
            }));
        }
        tracing::warn!(slot = %self.slot, %lsn, "standby status not applied; reconnecting to confirm");
        if let Some(c) = self.repl.take() {
            let _ = c.terminate().await;
        }
        let mut conn = ReplConn::connect(&self.opts, self.tls.as_ref()).await?;
        let t = Instant::now();
        loop {
            match conn.start(&self.slot, lsn, &self.options).await {
                Ok(()) => break,
                Err(e) if is_busy(&e) && t.elapsed() < Duration::from_secs(10) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => return Err(e),
            }
        }
        conn.status(lsn, true).await?;
        self.repl = Some(conn);
        let got = self.wait_confirmed(lsn, ACK).await?;
        if got < lsn {
            return Err(AtlatlError::transient(format!(
                "slot `{}` did not confirm {lsn} (at {got})",
                self.slot
            )));
        }
        self.confirmed = self.confirmed.max(lsn);
        Ok(())
    }

    /// Waits until the server reports `confirmed_flush_lsn >= lsn` (for reports and tests).
    pub async fn wait_confirmed(&self, lsn: Lsn, timeout: Duration) -> Result<Lsn> {
        let t = Instant::now();
        loop {
            let c = slot::slot_row(&self.sql, &self.slot)
                .await?
                .and_then(|r| r.confirmed_flush)
                .unwrap_or(Lsn::ZERO);
            if c >= lsn || t.elapsed() > timeout {
                return Ok(c);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Ends the replication session (a temporary slot goes with it).
    pub async fn close(mut self) -> Result<()> {
        if let Some(c) = self.repl.take() {
            c.terminate().await?;
        }
        Ok(())
    }
}

struct OpenTxn {
    ordinal: i64,
    final_lsn: Lsn,
    skip: Vec<bool>,
    /// A change for one of this replication's tables arrived (skipped or not).
    touched: bool,
}

struct RelInfo {
    stream: Option<usize>,
    rel: Relation,
    pk_idx: Vec<usize>,
}

/// An in-progress transaction arriving in stream blocks. Each (sub)transaction's changes are
/// staged under their own ordinal, so a subtransaction abort drops just its rows.
struct Streamed {
    ordinal: i64,
    parts: HashMap<u32, i64>,
    touched: bool,
}

/// Streamed changes are staged with sequence numbers above any committed one until their
/// transaction commits and they are renumbered into commit order.
const PROVISIONAL: i64 = 1 << 40;

struct Streamer<'a> {
    src: &'a mut PgChangeSource,
    duck: duckdb::Connection,
    stages: &'a mut Vec<Stage>,
    cursors: &'a [Option<Lsn>],
    seq: i64,
    rels: HashMap<u32, RelInfo>,
    txn: Option<OpenTxn>,
    ordinal: i64,
    txns: Vec<(i64, Lsn)>,
    upto: Lsn,
    transactions: u64,
    skipped: u64,
    changes: u64,
    unchanged_toast: u64,
    /// The top-level xid of the open stream block.
    in_stream: Option<u32>,
    streamed: HashMap<u32, Streamed>,
    pseq: i64,
    /// The ordinal the current change stages under, and whether its seq is provisional.
    at: (i64, bool),
    streamed_transactions: u64,
}

impl Streamer<'_> {
    fn conn(&mut self) -> Result<&mut ReplConn> {
        self.src
            .repl
            .as_mut()
            .ok_or_else(|| AtlatlError::internal("no replication connection"))
    }

    /// Streams until the window is done, or `None` when it must restart with text values.
    async fn run(&mut self, head: Lsn) -> Result<Option<StopReason>> {
        let limits = self.src.rep.limits.clone();
        let started = Instant::now();
        let mut last_msg = Instant::now();
        let confirmed = self.src.confirmed;
        self.conn()?.status(confirmed, true).await?;
        loop {
            if limits.max_duration.is_some_and(|d| started.elapsed() >= d) {
                if let Some(t) = self.txn.take() {
                    for st in self.stages.iter_mut() {
                        st.discard_txn(&self.duck, t.ordinal)?;
                    }
                }
                self.discard_streams()?;
                return Ok(Some(StopReason::MaxDuration));
            }
            let msg = match tokio::time::timeout(limits.ping, self.conn()?.recv()).await {
                Err(_) => {
                    if last_msg.elapsed() > limits.idle_timeout {
                        return Err(AtlatlError::new(
                            ErrorClass::Timeout,
                            format!(
                                "no message from the walsender for {:?} (slot `{}`)",
                                limits.idle_timeout, self.src.slot
                            ),
                        ));
                    }
                    self.conn()?.status(confirmed, true).await?;
                    continue;
                }
                Ok(m) => m?,
            };
            last_msg = Instant::now();
            match msg {
                Repl::Keepalive { wal_end, reply } => {
                    if reply {
                        self.conn()?.status(confirmed, false).await?;
                    }
                    if self.txn.is_none() && wal_end >= head {
                        self.upto = self.upto.max(wal_end);
                        return Ok(Some(StopReason::CaughtUp));
                    }
                }
                Repl::XLogData { data } => {
                    let (xid, m) = decode(data, self.in_stream.is_some())?;
                    if let Msg::Relation(r) = &m
                        && self.src.binary
                    {
                        let oids: Vec<u32> = r.columns.iter().map(|c| c.type_oid).collect();
                        let src = &mut *self.src;
                        src.plans.resolve(&src.sql, &oids).await?;
                        if let Some(c) = r
                            .columns
                            .iter()
                            .find(|c| src.plans.get(c.type_oid).is_none())
                        {
                            tracing::info!(slot = %src.slot, table = %r.name, column = %c.name, type_oid = c.type_oid, "no binary decoder for a column type that appeared mid-window; restarting it with text values");
                            return Ok(None);
                        }
                    }
                    if let Some(stop) = self.on_message(xid, m, head, &limits)? {
                        return Ok(Some(stop));
                    }
                }
            }
        }
    }

    fn on_message(
        &mut self,
        xid: Option<u32>,
        m: Msg,
        head: Lsn,
        limits: &Limits,
    ) -> Result<Option<StopReason>> {
        if matches!(
            m,
            Msg::Insert { .. } | Msg::Update { .. } | Msg::Delete { .. } | Msg::Truncate { .. }
        ) {
            self.at = self.position(xid)?;
        }
        match m {
            Msg::Relation(r) => self.on_relation(r)?,
            Msg::Begin { .. } if self.in_stream.is_some() => {
                return Err(AtlatlError::internal(
                    "pgoutput: Begin inside a stream block",
                ));
            }
            Msg::Begin { final_lsn, .. } => {
                self.ordinal += 1;
                self.txn = Some(OpenTxn {
                    ordinal: self.ordinal,
                    final_lsn,
                    skip: self
                        .cursors
                        .iter()
                        .map(|c| c.is_some_and(|c| final_lsn < c))
                        .collect(),
                    touched: false,
                });
            }
            Msg::Commit { end_lsn, .. } => {
                let t = self
                    .txn
                    .take()
                    .ok_or_else(|| AtlatlError::internal("pgoutput: Commit without Begin"))?;
                for (i, c) in self.cursors.iter().enumerate() {
                    let applied = c.is_some_and(|c| end_lsn <= c);
                    if t.skip[i] && !applied {
                        return Err(AtlatlError::internal(format!(
                            "transaction ending at {end_lsn} (final {}) was skipped for stream {i} but is past its cursor",
                            t.final_lsn
                        )));
                    }
                    if !t.skip[i] && applied {
                        self.stages[i].discard_txn(&self.duck, t.ordinal)?;
                    }
                }
                if t.touched && t.skip.iter().all(|s| *s) {
                    self.skipped += 1;
                } else if t.touched {
                    self.transactions += 1;
                }
                self.txns.push((t.ordinal, end_lsn));
                return Ok(self.committed(end_lsn, head, limits));
            }
            Msg::StreamStart { xid, first } => {
                if self.txn.is_some() || self.in_stream.is_some() {
                    return Err(AtlatlError::internal(
                        "pgoutput: Stream Start inside a transaction",
                    ));
                }
                if !self.streamed.contains_key(&xid) {
                    if !first {
                        return Err(AtlatlError::internal(format!(
                            "pgoutput: a later stream block of transaction {xid} arrived without its first"
                        )));
                    }
                    self.ordinal += 1;
                    self.streamed.insert(
                        xid,
                        Streamed {
                            ordinal: self.ordinal,
                            parts: HashMap::from([(xid, self.ordinal)]),
                            touched: false,
                        },
                    );
                }
                self.in_stream = Some(xid);
            }
            Msg::StreamStop => self.in_stream = None,
            Msg::StreamAbort { xid, subxid } => {
                let parts: Vec<i64> = if xid == subxid {
                    self.streamed
                        .remove(&xid)
                        .map(|s| s.parts.into_values().collect())
                        .unwrap_or_default()
                } else {
                    self.streamed
                        .get_mut(&xid)
                        .and_then(|s| s.parts.remove(&subxid))
                        .into_iter()
                        .collect()
                };
                for o in parts {
                    for st in self.stages.iter_mut() {
                        st.discard_txn(&self.duck, o)?;
                    }
                }
            }
            Msg::StreamCommit { xid, end_lsn, .. } => {
                let t = self.streamed.remove(&xid).ok_or_else(|| {
                    AtlatlError::internal(format!(
                        "pgoutput: Stream Commit for unknown transaction {xid}"
                    ))
                })?;
                let parts: Vec<i64> = t.parts.values().copied().collect();
                let mut all_applied = true;
                for (i, c) in self.cursors.iter().enumerate() {
                    if c.is_some_and(|c| end_lsn <= c) {
                        for o in &parts {
                            self.stages[i].discard_txn(&self.duck, *o)?;
                        }
                    } else {
                        all_applied = false;
                        self.seq += self.stages[i]
                            .commit_streamed(&self.duck, &parts, t.ordinal, self.seq)?;
                    }
                }
                if t.touched {
                    self.streamed_transactions += 1;
                    if all_applied {
                        self.skipped += 1;
                    } else {
                        self.transactions += 1;
                    }
                }
                self.txns.push((t.ordinal, end_lsn));
                return Ok(self.committed(end_lsn, head, limits));
            }
            Msg::Origin | Msg::Type => {}
            Msg::Insert { rel, new } => {
                if let Some((i, info)) = self.target(rel)? {
                    let key = self.key(&info, &new)?;
                    let fb = key_json(&key.iter().map(|k| k.as_deref()).collect::<Vec<_>>());
                    let (values, toast) = row_values(&self.src.plans, &info.rel, &new)?;
                    self.push(i, Op::Insert, None, Some(fb), toast, values)?;
                    self.rels.insert(rel, info);
                }
            }
            Msg::Update { rel, old, new } => {
                if let Some((i, info)) = self.target(rel)? {
                    let key = self.key(&info, &new)?;
                    let old_key = match &old {
                        Some((_, o)) => Some(self.key(&info, o)?),
                        None => None,
                    };
                    let (values, toast) = row_values(&self.src.plans, &info.rel, &new)?;
                    let new_json = key_json(&key.iter().map(|k| k.as_deref()).collect::<Vec<_>>());
                    match old_key.filter(|o| *o != key) {
                        Some(o) => {
                            let old_json =
                                key_json(&o.iter().map(|k| k.as_deref()).collect::<Vec<_>>());
                            let pk_vals = self.key_values(i, &o);
                            self.push(
                                i,
                                Op::Delete,
                                None,
                                Some(old_json.clone()),
                                vec![],
                                pk_vals,
                            )?;
                            let has_toast = !toast.is_empty();
                            let seq = self.push(
                                i,
                                Op::Update,
                                Some(old_json.clone()),
                                Some(old_json.clone()),
                                toast,
                                values,
                            )?;
                            if has_toast {
                                let txn = self.at.0;
                                self.stages[i].keychanges.push(KeyChange {
                                    seq,
                                    txn,
                                    old_key: old_json,
                                });
                            }
                        }
                        None => {
                            self.push(i, Op::Update, None, Some(new_json), toast, values)?;
                        }
                    }
                    self.rels.insert(rel, info);
                }
            }
            Msg::Delete { rel, old: (_, o) } => {
                if let Some((i, info)) = self.target(rel)? {
                    let key = self.key(&info, &o)?;
                    let fb = key_json(&key.iter().map(|k| k.as_deref()).collect::<Vec<_>>());
                    let pk_vals = self.key_values(i, &key);
                    self.push(i, Op::Delete, None, Some(fb), vec![], pk_vals)?;
                    self.rels.insert(rel, info);
                }
            }
            Msg::Truncate { rels } => {
                for rel in rels {
                    if let Some((i, info)) = self.target(rel)? {
                        self.push(i, Op::Truncate, None, None, vec![], vec![])?;
                        self.rels.insert(rel, info);
                    }
                }
            }
        }
        Ok(None)
    }

    fn on_relation(&mut self, r: Relation) -> Result<()> {
        let stream = self
            .src
            .rep
            .streams
            .iter()
            .position(|s| s.source_schema == r.namespace && s.source_table == r.name);
        let mut pk_idx = Vec::new();
        if let Some(i) = stream {
            let s = &self.src.rep.streams[i];
            let st = &mut self.stages[i];
            if r.replica_identity == b'n' {
                return Err(AtlatlError::config(format!(
                    "stream `{}`: {}.{} has REPLICA IDENTITY NOTHING, so updates and deletes carry no key",
                    s.id, r.namespace, r.name
                )));
            }
            for k in &st.pk {
                let Some(p) = r.columns.iter().position(|c| &c.name == k) else {
                    return Err(AtlatlError::config(format!(
                        "stream `{}`: primary key column `{k}` is not in {}.{}",
                        s.id, r.namespace, r.name
                    )));
                };
                if r.replica_identity != b'f' && !r.columns[p].key {
                    return Err(AtlatlError::config(format!(
                        "stream `{}`: primary key column `{k}` is not part of {}.{}'s replica identity; use its primary key, or REPLICA IDENTITY FULL",
                        s.id, r.namespace, r.name
                    )));
                }
                pk_idx.push(p);
            }
            let cols: Vec<StageCol> = r
                .columns
                .iter()
                .map(|c| StageCol {
                    name: c.name.clone(),
                    oid: c.type_oid,
                    typmod: c.typmod,
                })
                .collect();
            st.ensure_cols(&self.duck, &cols)?;
        }
        self.rels.insert(
            r.oid,
            RelInfo {
                stream,
                rel: r,
                pk_idx,
            },
        );
        Ok(())
    }

    /// Where the current change stages: its transaction's ordinal, and whether it is streamed.
    fn position(&mut self, xid: Option<u32>) -> Result<(i64, bool)> {
        match (self.in_stream, xid) {
            (Some(top), Some(x)) => {
                let s = self.streamed.get_mut(&top).ok_or_else(|| {
                    AtlatlError::internal(format!("pgoutput: no open stream for {top}"))
                })?;
                let o = match s.parts.get(&x) {
                    Some(o) => *o,
                    None => {
                        self.ordinal += 1;
                        s.parts.insert(x, self.ordinal);
                        self.ordinal
                    }
                };
                Ok((o, true))
            }
            (None, None) => self
                .txn
                .as_ref()
                .map(|t| (t.ordinal, false))
                .ok_or_else(|| AtlatlError::internal("pgoutput: change outside a transaction")),
            _ => Err(AtlatlError::internal(
                "pgoutput: change xid does not match the stream state",
            )),
        }
    }

    /// After a commit: whether the window is done.
    fn committed(&mut self, end_lsn: Lsn, head: Lsn, limits: &Limits) -> Option<StopReason> {
        self.upto = self.upto.max(end_lsn);
        if end_lsn >= head {
            return Some(StopReason::CaughtUp);
        }
        if limits.max_changes.is_some_and(|m| self.changes >= m) {
            return Some(StopReason::MaxChanges);
        }
        None
    }

    /// Drops streamed transactions still open when the window ends; they are redelivered.
    fn discard_streams(&mut self) -> Result<()> {
        for (_, t) in self.streamed.drain() {
            for o in t.parts.into_values() {
                for st in self.stages.iter_mut() {
                    st.discard_txn(&self.duck, o)?;
                }
            }
        }
        self.in_stream = None;
        Ok(())
    }

    /// The stream a change belongs to, unless this transaction skips it.
    fn target(&mut self, rel: u32) -> Result<Option<(usize, RelInfo)>> {
        let info = self.rels.remove(&rel).ok_or_else(|| {
            AtlatlError::internal(format!("pgoutput: change for unknown relation {rel}"))
        })?;
        let skip = match self.in_stream {
            Some(top) => {
                if let Some(s) = self.streamed.get_mut(&top) {
                    s.touched |= info.stream.is_some();
                }
                false
            }
            None => {
                let t = self.txn.as_mut().ok_or_else(|| {
                    AtlatlError::internal("pgoutput: change outside a transaction")
                })?;
                t.touched |= info.stream.is_some();
                info.stream.is_some_and(|i| t.skip[i])
            }
        };
        match info.stream {
            Some(i) if !skip => Ok(Some((i, info))),
            _ => {
                self.rels.insert(rel, info);
                Ok(None)
            }
        }
    }

    fn key(&self, info: &RelInfo, cols: &[Col]) -> Result<Vec<Option<String>>> {
        info.pk_idx
            .iter()
            .map(|&p| match cols.get(p) {
                Some(c @ (Col::Text(_) | Col::Binary(_))) => {
                    text(&self.src.plans, &info.rel, &info.rel.columns[p], c)
                }
                _ => Err(AtlatlError::internal(format!(
                    "pgoutput: key column `{}` of {}.{} arrived without a value",
                    info.rel.columns[p].name, info.rel.namespace, info.rel.name
                ))),
            })
            .collect()
    }

    fn key_values(&self, i: usize, key: &[Option<String>]) -> Vec<(String, Option<String>)> {
        self.stages[i]
            .pk
            .iter()
            .cloned()
            .zip(key.iter().cloned())
            .collect()
    }

    fn push(
        &mut self,
        i: usize,
        op: Op,
        from: Option<String>,
        fb: Option<String>,
        toast: Vec<String>,
        values: Vec<(String, Option<String>)>,
    ) -> Result<i64> {
        let (txn, provisional) = self.at;
        if !toast.is_empty() {
            self.unchanged_toast += 1;
        }
        let seq = if provisional {
            self.pseq += 1;
            PROVISIONAL + self.pseq
        } else {
            self.seq += 1;
            self.seq
        };
        self.stages[i].push(
            &self.duck,
            Row {
                seq,
                txn,
                op,
                from,
                fb,
                toast,
                values,
            },
        )?;
        self.changes += 1;
        Ok(seq)
    }
}

type Values = (Vec<(String, Option<String>)>, Vec<String>);

fn row_values(plans: &Plans, rel: &Relation, cols: &[Col]) -> Result<Values> {
    let mut values = Vec::with_capacity(cols.len());
    let mut toast = Vec::new();
    for (c, v) in rel.columns.iter().zip(cols) {
        if *v == Col::Unchanged {
            toast.push(c.name.clone());
        }
        values.push((c.name.clone(), text(plans, rel, c, v)?));
    }
    Ok((values, toast))
}

fn copy_row(
    duck: &duckdb::Connection,
    stage: &mut Stage,
    s: &StreamSpec,
    names: &[String],
    line: &[u8],
    seq: &mut i64,
) -> Result<()> {
    let fields = copy_fields(line)?;
    if fields.len() != names.len() {
        return Err(AtlatlError::internal(format!(
            "COPY of stream `{}` returned {} columns, expected {}",
            s.id,
            fields.len(),
            names.len()
        )));
    }
    let key: Vec<Option<&str>> = stage
        .pk
        .iter()
        .map(|k| {
            names
                .iter()
                .position(|n| n == k)
                .and_then(|i| fields[i].as_deref())
        })
        .collect();
    let fb = key_json(&key);
    *seq += 1;
    stage.push(
        duck,
        Row {
            seq: *seq,
            txn: 0,
            op: Op::Insert,
            from: None,
            fb: Some(fb),
            toast: vec![],
            values: names.iter().cloned().zip(fields).collect(),
        },
    )
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RunReport {
    pub applied: crate::apply::Applied,
    /// The slot's `confirmed_flush_lsn` after the confirm.
    pub confirmed: Option<Lsn>,
    pub slot: Option<SlotStats>,
}

/// One bounded run: stage the window, apply it in one lake transaction, then confirm.
pub async fn run_once(
    src: &mut PgChangeSource,
    duck: duckdb::Connection,
    lake_alias: &str,
    run_id: &str,
) -> Result<RunReport> {
    let w = src.changes(duck, lake_alias, run_id).await?;
    let applied = w.apply()?;
    let mut confirmed = None;
    if let Some(l) = applied.confirm {
        src.confirm(l).await?;
        confirmed = Some(src.wait_confirmed(l, Duration::from_secs(5)).await?);
    }
    let slot = src.stats().await?;
    Ok(RunReport {
        applied,
        confirmed,
        slot,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgoutput::Column;
    use atlatl_core::config::model::PgChanges;
    use bytes::Bytes;

    fn col(v: &str) -> Col {
        Col::Text(Bytes::from(v.to_owned()))
    }

    fn insert(id: &str, v: &str) -> Msg {
        Msg::Insert {
            rel: 1,
            new: vec![col(id), col(v)],
        }
    }

    /// Stream blocks interleaved with a regular transaction: a subtransaction abort, a
    /// whole-transaction abort, a later block of a committed transaction, and one left open.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stream_messages_stage_in_commit_order() {
        let pg = atlatl_testkit::pg16();
        let opts = ConnectOptions::new(
            &pg.host,
            pg.port,
            pg.fresh_db().expect("db"),
            &pg.user,
            Some(pg.password.clone()),
        );
        let policy =
            SlotPolicy::resolve("pg", Some(&PgChanges::default()), None, None).expect("policy");
        let rep = ReplicationSpec {
            name: "r".into(),
            streams: vec![StreamSpec {
                id: "pg/t".into(),
                source_schema: "public".into(),
                source_table: "t".into(),
                target_schema: "br".into(),
                target_table: "t".into(),
                primary_key: None,
                deletes: DeleteMode::Hard,
                history: false,
                config_hash: "h".into(),
            }],
            limits: Limits::default(),
        };
        let mut src = PgChangeSource::open(opts, policy, rep).await.expect("open");
        let duck = duckdb::Connection::open_in_memory().expect("duck");
        let cols = vec![
            StageCol {
                name: "id".into(),
                oid: 23,
                typmod: -1,
            },
            StageCol {
                name: "v".into(),
                oid: 25,
                typmod: -1,
            },
        ];
        let mut stages = vec![Stage::create(&duck, "stg", cols, vec!["id".into()]).expect("stage")];
        let cursors = [None];
        let mut w = Streamer {
            src: &mut src,
            duck,
            stages: &mut stages,
            cursors: &cursors,
            seq: 0,
            rels: HashMap::new(),
            txn: None,
            ordinal: 0,
            txns: Vec::new(),
            upto: Lsn::ZERO,
            transactions: 0,
            skipped: 0,
            changes: 0,
            unchanged_toast: 0,
            in_stream: None,
            streamed: HashMap::new(),
            pseq: 0,
            at: (0, false),
            streamed_transactions: 0,
        };
        let rel = Relation {
            oid: 1,
            namespace: "public".into(),
            name: "t".into(),
            replica_identity: b'd',
            columns: vec![
                Column {
                    key: true,
                    name: "id".into(),
                    type_oid: 23,
                    typmod: -1,
                },
                Column {
                    key: false,
                    name: "v".into(),
                    type_oid: 25,
                    typmod: -1,
                },
            ],
        };
        let start = |xid, first| (None, Msg::StreamStart { xid, first });
        let msgs = vec![
            (None, Msg::Relation(rel)),
            start(100, true),
            (Some(100), insert("1", "a")),
            (Some(101), insert("2", "sub")),
            (None, Msg::StreamStop),
            (
                None,
                Msg::Begin {
                    final_lsn: Lsn(0x10),
                    xid: 150,
                },
            ),
            (None, insert("3", "regular")),
            (
                None,
                Msg::Commit {
                    commit_lsn: Lsn(0x10),
                    end_lsn: Lsn(0x18),
                },
            ),
            start(200, true),
            (Some(200), insert("4", "aborted")),
            (None, Msg::StreamStop),
            start(100, false),
            (
                Some(100),
                Msg::Update {
                    rel: 1,
                    old: None,
                    new: vec![col("1"), col("a2")],
                },
            ),
            (None, Msg::StreamStop),
            (
                None,
                Msg::StreamAbort {
                    xid: 100,
                    subxid: 101,
                },
            ),
            (
                None,
                Msg::StreamAbort {
                    xid: 200,
                    subxid: 200,
                },
            ),
            (
                None,
                Msg::StreamCommit {
                    xid: 100,
                    commit_lsn: Lsn(0x20),
                    end_lsn: Lsn(0x28),
                },
            ),
            start(300, true),
            (Some(300), insert("5", "open")),
            (None, Msg::StreamStop),
        ];
        let limits = Limits::default();
        for (xid, m) in msgs {
            assert_eq!(
                w.on_message(xid, m, Lsn(0x1000), &limits).expect("message"),
                None
            );
        }
        assert!(
            w.on_message(Some(1), insert("9", "x"), Lsn(0x1000), &limits)
                .is_err()
        );
        assert!(
            w.on_message(
                None,
                Msg::StreamStart {
                    xid: 400,
                    first: false
                },
                Lsn(0x1000),
                &limits
            )
            .is_err()
        );
        w.discard_streams().expect("discard");
        w.stages[0].flush(&w.duck).expect("flush");
        assert_eq!(
            (w.transactions, w.streamed_transactions, w.upto),
            (2, 1, Lsn(0x28))
        );
        let mut st = w
            .duck
            .prepare("SELECT __seq, __txn, id, v FROM temp.main.stg ORDER BY __seq")
            .expect("prepare");
        let rows: Vec<(i64, i64, String, String)> = st
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .expect("query")
            .collect::<std::result::Result<_, _>>()
            .expect("rows");
        let txn_of = |lsn: u64| w.txns.iter().find(|(_, l)| *l == Lsn(lsn)).expect("txn").0;
        let (regular, streamed) = (txn_of(0x18), txn_of(0x28));
        assert_eq!(
            rows,
            [
                (1, regular, "3".into(), "regular".into()),
                (2, streamed, "1".into(), "a".into()),
                (3, streamed, "1".into(), "a2".into()),
            ]
        );
    }
}
