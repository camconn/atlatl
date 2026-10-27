//! Applying a staged window to the lake (08 §10): net per key, then one lake transaction with
//! every stream's changes and its LSN cursor. The caller confirms the slot only after this
//! commit returns.

use std::collections::HashMap;

use atlatl_core::config::model::DeleteMode;
use atlatl_core::error::Result;
use atlatl_ingest::modes::Counts;
use atlatl_ingest::sql::{db, ident};
use atlatl_ingest::staging::table_columns;
use atlatl_state::{LakeState, StateKind, StateRow};
use duckdb::Connection;
use serde::Serialize;

use crate::lsn::Lsn;
use crate::source::{ReplicationSpec, StreamSpec, WindowSummary};
use crate::stage::Stage;
use crate::types::lake_type;

pub const OP_COLUMN: &str = "_atlatl_op";
pub const SYNCED_AT_COLUMN: &str = "_atlatl_synced_at";
pub const LSN_COLUMN: &str = "_atlatl_lsn";

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StreamOutcome {
    pub stream: String,
    pub target: String,
    pub counts: Counts,
    pub truncated: bool,
    pub backfilled: bool,
    /// Changes staged for this stream (backfilled rows included).
    pub staged: u64,
    pub cursor_before: Option<Lsn>,
    pub cursor: Lsn,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Applied {
    /// Whether a lake transaction was committed (nothing is written when the window is empty
    /// and the cursor would not move).
    pub committed: bool,
    /// The position to confirm on the slot: the lowest cursor now stored.
    pub confirm: Option<Lsn>,
    pub streams: Vec<StreamOutcome>,
    pub window: WindowSummary,
}

/// A staged window. It owns the engine connection its staging lives on.
pub struct Window {
    duck: Connection,
    alias: String,
    lake: LakeState,
    rep: ReplicationSpec,
    run_id: String,
    stages: Vec<Stage>,
    cursors: Vec<Option<Lsn>>,
    txns: Vec<(i64, Lsn)>,
    summary: WindowSummary,
}

fn scalar(c: &Connection, sql: &str) -> Result<u64> {
    let n: i64 = c.query_row(sql, [], |r| r.get(0)).map_err(db)?;
    Ok(n as u64)
}

struct Target {
    rel: String,
    exists: bool,
    types: HashMap<String, String>,
    missing: Vec<(String, String)>,
}

impl Window {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        duck: Connection,
        lake: LakeState,
        alias: &str,
        rep: ReplicationSpec,
        run_id: String,
        stages: Vec<Stage>,
        cursors: Vec<Option<Lsn>>,
        txns: Vec<(i64, Lsn)>,
        summary: WindowSummary,
    ) -> Result<Self> {
        Ok(Self {
            duck,
            alias: alias.to_owned(),
            lake,
            rep,
            run_id,
            stages,
            cursors,
            txns,
            summary,
        })
    }

    pub fn summary(&self) -> &WindowSummary {
        &self.summary
    }

    /// The engine connection (lake attached), for inspection.
    pub fn connection(&self) -> &Connection {
        &self.duck
    }

    /// Nets and applies the window in one lake transaction, with the cursors.
    pub fn apply(self) -> Result<Applied> {
        self.apply_with(|_| Ok(()))
    }

    /// [`Window::apply`] with a hook run inside the transaction just before COMMIT.
    pub fn apply_with(
        mut self,
        before_commit: impl FnOnce(&Connection) -> Result<()>,
    ) -> Result<Applied> {
        let res = self.apply_inner(before_commit);
        self.cleanup();
        res
    }

    fn cleanup(&self) {
        for st in &self.stages {
            let _ = self.duck.execute_batch(&format!(
                "DROP TABLE IF EXISTS temp.main.{}; DROP TABLE IF EXISTS temp.main.{}",
                ident(&st.table),
                ident(&format!("{}_net", st.table))
            ));
        }
        let _ = self.duck.execute_batch(&format!(
            "DROP TABLE IF EXISTS temp.main.{}",
            ident(&self.txns_table())
        ));
    }

    fn txns_table(&self) -> String {
        self.stages
            .first()
            .map(|s| format!("{}_txns", s.table))
            .unwrap_or_else(|| "pgrepl_txns".into())
    }

    fn current_rows(s: &StreamSpec, st: &Stage, rel: &str) -> String {
        if !s.history {
            return rel.to_owned();
        }
        let pk = st
            .pk
            .iter()
            .map(|k| ident(k))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "(SELECT * FROM {rel} QUALIFY row_number() OVER (PARTITION BY {pk} ORDER BY {LSN_COLUMN} DESC NULLS LAST, ({OP_COLUMN} = 'delete')) = 1)"
        )
    }

    fn lsn_of(&self, txn: i64) -> Lsn {
        if txn == 0 {
            return self.summary.head;
        }
        self.txns
            .iter()
            .find(|(t, _)| *t == txn)
            .map_or(self.summary.upto, |(_, l)| *l)
    }

    fn apply_inner(
        &mut self,
        before_commit: impl FnOnce(&Connection) -> Result<()>,
    ) -> Result<Applied> {
        let upto = self.summary.upto;
        let new_cursors: Vec<Lsn> = self
            .cursors
            .iter()
            .map(|c| c.map_or(upto, |c| c.max(upto)))
            .collect();
        let has_work = self.stages.iter().any(|s| s.rows > 0);
        let moves = self
            .cursors
            .iter()
            .zip(&new_cursors)
            .any(|(old, new)| old.is_none_or(|o| o < *new));
        let backfilled = |id: &str| self.summary.backfilled.iter().any(|b| b == id);
        if !has_work && !moves {
            return Ok(Applied {
                committed: false,
                confirm: self.cursors.iter().flatten().min().copied(),
                streams: self
                    .rep
                    .streams
                    .iter()
                    .zip(&new_cursors)
                    .map(|(s, c)| StreamOutcome {
                        stream: s.id.clone(),
                        target: format!("{}.{}", s.target_schema, s.target_table),
                        counts: Counts::default(),
                        truncated: false,
                        backfilled: false,
                        staged: 0,
                        cursor_before: Some(*c),
                        cursor: *c,
                    })
                    .collect(),
                window: self.summary.clone(),
            });
        }

        let txns = self.txns_table();
        self.duck
            .execute_batch(&format!(
                "CREATE OR REPLACE TEMP TABLE {} (txn BIGINT, lsn BIGINT)",
                ident(&txns)
            ))
            .map_err(db)?;
        {
            let mut app = self
                .duck
                .appender_to_catalog_and_db(&txns, "temp", "main")
                .map_err(db)?;
            app.append_row(duckdb::params![0i64, self.summary.head.0 as i64])
                .map_err(db)?;
            for (t, l) in &self.txns {
                app.append_row(duckdb::params![*t, l.0 as i64])
                    .map_err(db)?;
            }
            app.flush().map_err(db)?;
        }
        let txns_rel = format!("temp.main.{}", ident(&txns));

        let mut targets = Vec::with_capacity(self.stages.len());
        for (s, st) in self.rep.streams.iter().zip(self.stages.iter_mut()) {
            if st.rows == 0 {
                targets.push(None);
                continue;
            }
            st.resolve_key_changes(&self.duck)?;
            let t = target_columns(&self.duck, &self.alias, s, st)?;
            let lake = t.exists.then(|| Self::current_rows(s, st, &t.rel));
            let sql = st.net_sql(&txns_rel, lake.as_deref(), &t.types);
            self.duck
                .execute_batch(&format!(
                    "CREATE OR REPLACE TEMP TABLE {} AS {sql}",
                    ident(&format!("{}_net", st.table))
                ))
                .map_err(db)?;
            targets.push(Some(t));
        }

        self.duck.execute_batch("BEGIN TRANSACTION").map_err(db)?;
        let res = (|| {
            let mut outcomes = Vec::with_capacity(self.stages.len());
            for (i, s) in self.rep.streams.iter().enumerate() {
                let st = &self.stages[i];
                let (counts, truncated) = match &targets[i] {
                    Some(t) => self.apply_stream(s, st, t)?,
                    None => (Counts::default(), false),
                };
                self.lake.put(
                    &self.duck,
                    &StateRow {
                        stream_id: s.id.clone(),
                        member: String::new(),
                        kind: StateKind::Lsn,
                        value: serde_json::json!({
                            "lsn": new_cursors[i].to_string(),
                            "slot": self.summary.slot,
                        }),
                        config_hash: s.config_hash.clone(),
                        run_id: self.run_id.clone(),
                        lake_snapshot: None,
                        updated_at: None,
                    },
                )?;
                outcomes.push(StreamOutcome {
                    stream: s.id.clone(),
                    target: format!("{}.{}", s.target_schema, s.target_table),
                    counts,
                    truncated,
                    backfilled: backfilled(&s.id),
                    staged: st.rows,
                    cursor_before: self.cursors[i],
                    cursor: new_cursors[i],
                });
            }
            before_commit(&self.duck)?;
            Ok(outcomes)
        })();
        let outcomes = match res {
            Ok(o) => o,
            Err(e) => {
                let _ = self.duck.execute_batch("ROLLBACK");
                return Err(e);
            }
        };
        if let Err(e) = self.duck.execute_batch("COMMIT").map_err(db) {
            let _ = self.duck.execute_batch("ROLLBACK");
            return Err(e);
        }
        Ok(Applied {
            committed: true,
            confirm: new_cursors.iter().min().copied(),
            streams: outcomes,
            window: self.summary.clone(),
        })
    }

    fn apply_stream(&self, s: &StreamSpec, st: &Stage, t: &Target) -> Result<(Counts, bool)> {
        let c = &self.duck;
        let tgt = &t.rel;
        let net = format!("temp.main.{}", ident(&format!("{}_net", st.table)));
        let cols: Vec<&str> = st.cols.iter().map(|c| c.name.as_str()).collect();
        let quoted = |xs: &[&str], p: &str| {
            xs.iter()
                .map(|x| format!("{p}{}", ident(x)))
                .collect::<Vec<_>>()
                .join(", ")
        };
        if !t.exists {
            let defs = cols
                .iter()
                .map(|n| format!("{} {}", ident(n), t.types[*n]))
                .chain([
                    format!("{OP_COLUMN} VARCHAR"),
                    format!("{SYNCED_AT_COLUMN} TIMESTAMP WITH TIME ZONE"),
                    format!("{LSN_COLUMN} BIGINT"),
                ])
                .collect::<Vec<_>>()
                .join(", ");
            c.execute_batch(&format!(
                "CREATE SCHEMA IF NOT EXISTS {}.{}; CREATE TABLE {tgt} ({defs})",
                ident(&self.alias),
                ident(&s.target_schema)
            ))
            .map_err(db)?;
        } else {
            for (n, ty) in &t.missing {
                c.execute_batch(&format!("ALTER TABLE {tgt} ADD COLUMN {} {ty}", ident(n)))
                    .map_err(db)?;
            }
        }

        let on = st
            .pk
            .iter()
            .map(|k| format!("t.{k} = s.{k}", k = ident(k)))
            .collect::<Vec<_>>()
            .join(" AND ");
        let truncated = st.last_truncate.is_some();
        let t_lsn = st.last_truncate.map(|(_, txn)| self.lsn_of(txn).0 as i64);
        let mut counts = Counts::default();
        let ignore = s.deletes == DeleteMode::Ignore;

        if s.history {
            if let (Some(tl), false) = (t_lsn, ignore) {
                let cur = Self::current_rows(s, st, tgt);
                let pk = quoted(&st.pk.iter().map(String::as_str).collect::<Vec<_>>(), "");
                let n = scalar(
                    c,
                    &format!("SELECT count(*) FROM {cur} WHERE {OP_COLUMN} <> 'delete'"),
                )?;
                if n > 0 {
                    c.execute_batch(&format!(
                        "INSERT INTO {tgt} ({pk}, {OP_COLUMN}, {SYNCED_AT_COLUMN}, {LSN_COLUMN})
                         SELECT {pk}, 'delete', now(), {tl} FROM {cur} WHERE {OP_COLUMN} <> 'delete'"
                    ))
                    .map_err(db)?;
                }
                counts.deleted += n;
            }
            let filter = if ignore { "WHERE __op <> 'D'" } else { "" };
            counts.inserted += scalar(c, &format!("SELECT count(*) FROM {net} WHERE __op <> 'D'"))?;
            if !ignore {
                counts.deleted +=
                    scalar(c, &format!("SELECT count(*) FROM {net} WHERE __op = 'D'"))?;
            }
            c.execute_batch(&format!(
                "INSERT INTO {tgt} ({cl}, {OP_COLUMN}, {SYNCED_AT_COLUMN}, {LSN_COLUMN})
                 SELECT {cl}, CASE __op WHEN 'I' THEN 'insert' WHEN 'U' THEN 'update' ELSE 'delete' END, now(), __lsn
                 FROM {net} {filter}",
                cl = quoted(&cols, "")
            ))
            .map_err(db)?;
            return Ok((counts, truncated));
        }

        if let Some(tl) = t_lsn {
            match s.deletes {
                DeleteMode::Hard => {
                    let n = scalar(c, &format!("SELECT count(*) FROM {tgt}"))?;
                    if n > 0 {
                        c.execute_batch(&format!("DELETE FROM {tgt}")).map_err(db)?;
                    }
                    counts.deleted += n;
                }
                DeleteMode::Soft => {
                    let pred = format!("{OP_COLUMN} IS DISTINCT FROM 'delete'");
                    let n = scalar(c, &format!("SELECT count(*) FROM {tgt} WHERE {pred}"))?;
                    if n > 0 {
                        c.execute_batch(&format!(
                            "UPDATE {tgt} SET {OP_COLUMN} = 'delete', {SYNCED_AT_COLUMN} = now(), {LSN_COLUMN} = {tl} WHERE {pred}"
                        ))
                        .map_err(db)?;
                    }
                    counts.deleted += n;
                }
                DeleteMode::Ignore => {}
            }
        }

        let dels = format!("(SELECT * FROM {net} WHERE __op = 'D')");
        match s.deletes {
            DeleteMode::Hard => {
                let pred = format!("EXISTS (SELECT 1 FROM {dels} s WHERE {on})");
                let n = scalar(c, &format!("SELECT count(*) FROM {tgt} t WHERE {pred}"))?;
                if n > 0 {
                    c.execute_batch(&format!("DELETE FROM {tgt} t WHERE {pred}"))
                        .map_err(db)?;
                }
                counts.deleted += n;
            }
            DeleteMode::Soft => {
                let n = scalar(
                    c,
                    &format!(
                        "SELECT count(*) FROM {tgt} t WHERE t.{OP_COLUMN} IS DISTINCT FROM 'delete' AND EXISTS (SELECT 1 FROM {dels} s WHERE {on})"
                    ),
                )?;
                if n > 0 {
                    c.execute_batch(&format!(
                        "MERGE INTO {tgt} AS t USING {dels} AS s ON {on}
                         WHEN MATCHED AND t.{OP_COLUMN} IS DISTINCT FROM 'delete' THEN UPDATE SET
                           {OP_COLUMN} = 'delete', {SYNCED_AT_COLUMN} = now(), {LSN_COLUMN} = s.__lsn"
                    ))
                    .map_err(db)?;
                }
                counts.deleted += n;
            }
            DeleteMode::Ignore => {}
        }

        let ups = format!("(SELECT * FROM {net} WHERE __op <> 'D')");
        let payload: Vec<&str> = cols
            .iter()
            .copied()
            .filter(|c| !st.pk.iter().any(|k| k == c))
            .collect();
        let changed = if payload.is_empty() {
            format!("t.{OP_COLUMN} = 'delete'")
        } else {
            format!(
                "(({}) IS DISTINCT FROM ({}) OR t.{OP_COLUMN} = 'delete')",
                quoted(&payload, "t."),
                quoted(&payload, "s.")
            )
        };
        let total = scalar(c, &format!("SELECT count(*) FROM {ups}"))?;
        if total == 0 {
            return Ok((counts, truncated));
        }
        let matched = scalar(
            c,
            &format!("SELECT count(*) FROM {ups} s SEMI JOIN {tgt} t ON {on}"),
        )?;
        let updated = scalar(
            c,
            &format!("SELECT count(*) FROM {ups} s JOIN {tgt} t ON {on} WHERE {changed}"),
        )?;
        counts.inserted += total - matched;
        counts.updated += updated;
        counts.unchanged += matched - updated;
        if total - matched + updated == 0 {
            return Ok((counts, truncated));
        }
        let op = "CASE s.__op WHEN 'I' THEN 'insert' ELSE 'update' END";
        let set = payload
            .iter()
            .map(|p| format!("{p} = s.{p}", p = ident(p)))
            .chain([
                format!("{OP_COLUMN} = {op}"),
                format!("{SYNCED_AT_COLUMN} = now()"),
                format!("{LSN_COLUMN} = s.__lsn"),
            ])
            .collect::<Vec<_>>()
            .join(", ");
        c.execute_batch(&format!(
            "MERGE INTO {tgt} AS t USING {ups} AS s ON {on}
             WHEN MATCHED AND {changed} THEN UPDATE SET {set}
             WHEN NOT MATCHED THEN INSERT ({cl}, {OP_COLUMN}, {SYNCED_AT_COLUMN}, {LSN_COLUMN})
               VALUES ({vals}, {op}, now(), s.__lsn)",
            cl = quoted(&cols, ""),
            vals = quoted(&cols, "s."),
        ))
        .map_err(db)?;
        Ok((counts, truncated))
    }
}

fn target_columns(duck: &Connection, alias: &str, s: &StreamSpec, st: &Stage) -> Result<Target> {
    let rel = format!(
        "{}.{}.{}",
        ident(alias),
        ident(&s.target_schema),
        ident(&s.target_table)
    );
    let existing = table_columns(duck, alias, &s.target_schema, &s.target_table)?;
    let have: HashMap<String, String> = existing
        .iter()
        .flatten()
        .map(|c| (c.name.clone(), c.kind.clone()))
        .collect();
    let mut types = HashMap::new();
    let mut missing = Vec::new();
    for c in &st.cols {
        let t = have
            .get(&c.name)
            .cloned()
            .unwrap_or_else(|| lake_type(c.oid, c.typmod));
        if !have.contains_key(&c.name) {
            missing.push((c.name.clone(), t.clone()));
        }
        types.insert(c.name.clone(), t);
    }
    if existing.is_some() {
        for (m, t) in [
            (OP_COLUMN, "VARCHAR"),
            (SYNCED_AT_COLUMN, "TIMESTAMP WITH TIME ZONE"),
            (LSN_COLUMN, "BIGINT"),
        ] {
            if !have.contains_key(m) {
                missing.push((m.to_owned(), t.to_owned()));
            }
        }
    }
    Ok(Target {
        rel,
        exists: existing.is_some(),
        types,
        missing,
    })
}
