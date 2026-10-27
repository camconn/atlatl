//! CDC applier for SQLite, D1 and Turso (08 §6.2–6.3, 11 §2 `Applier`).
//!
//! [`Client::begin`] takes every stream's cursor move up front. Then call [`ApplyTxn::upsert`]
//! for every stream parent-first, [`ApplyTxn::delete`] child-first, and [`ApplyTxn::commit`].
//! An upsert after a delete is an `internal` error; any error rolls back the open transaction.
//!
//! SQLite applies in one transaction with the cursors. D1 (no transactions across requests),
//! and Turso windows above `transaction_rows`, use the idempotent window: mark
//! `pending = {E, fingerprint}` first, send replayable batches, flip the cursor last.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::AddAssign;
use std::pin::pin;
use std::time::SystemTime;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Schema};
use atlatl_core::AtlatlError;
use atlatl_core::config::connection::Connection;
use atlatl_core::config::model::DeleteValue;
use atlatl_core::error::Result;
use futures::{Stream, StreamExt};

use crate::conn::{Conn, Limits, Outcome, Stmt, Txns};
use crate::cursors::{self, Cursor, CursorMode, CursorPlan};
use crate::types::{Affinity, Store, Value, affinity, column_values, iso};

/// Column of a delete batch carrying the deleting snapshot's commit time.
pub const DELETE_TS: &str = "_ts";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Deletes {
    /// When `column` is served, the upsert writes the lake's value and `restore` is ignored.
    Soft {
        column: String,
        value: DeleteValue,
        restore: bool,
    },
    Hard,
    Ignore,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
    pub table: String,
    pub primary_key: Vec<String>,
    /// `columns.include`; contains the primary key. Other target columns are never touched.
    pub columns: Vec<String>,
    /// `columns.<c>.target_type`: a [`Store`] name.
    pub target_types: BTreeMap<String, String>,
    pub deletes: Deletes,
}

impl Target {
    fn restore_column(&self) -> Option<&str> {
        match &self.deletes {
            Deletes::Soft {
                column,
                restore: true,
                ..
            } if !self.columns.contains(column) => Some(column),
            _ => None,
        }
    }

    fn soft_column(&self) -> Option<(&str, DeleteValue)> {
        match &self.deletes {
            Deletes::Soft { column, value, .. } => Some((column, *value)),
            _ => None,
        }
    }

    pub fn store(&self, col: &str, dt: &DataType) -> Result<Store> {
        match self.target_types.get(col) {
            Some(t) => Store::parse(t)
                .map_err(|e| AtlatlError::config(format!("column `{col}`: {}", e.message))),
            None => Store::default_for(dt)
                .map_err(|e| AtlatlError::config(format!("column `{col}`: {}", e.message))),
        }
    }
}

/// Per-stream counts for the report (08 §11).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ApplyCounts {
    pub upserts_sent: u64,
    pub rows_inserted: u64,
    pub rows_updated: u64,
    /// Filtered by the change guard.
    pub rows_unchanged: u64,
    pub rows_deleted: u64,
    pub deletes_ignored: u64,
}

impl AddAssign for ApplyCounts {
    fn add_assign(&mut self, o: Self) {
        self.upserts_sent += o.upserts_sent;
        self.rows_inserted += o.rows_inserted;
        self.rows_updated += o.rows_updated;
        self.rows_unchanged += o.rows_unchanged;
        self.rows_deleted += o.rows_deleted;
        self.deletes_ignored += o.deletes_ignored;
    }
}

/// A key to delete, with its stamp for `value: snapshot_time`.
#[derive(Clone, Debug, PartialEq)]
pub struct DeleteKey {
    pub pk: Vec<Value>,
    pub ts: Option<String>,
}

/// A target table as `PRAGMA table_info` reports it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableInfo {
    /// Name → declared type, in table order.
    pub columns: Vec<(String, String)>,
    pub primary_key: Vec<String>,
    /// Unique, non-partial indexes on plain columns.
    pub unique: Vec<Vec<String>>,
}

impl TableInfo {
    pub fn decl(&self, c: &str) -> Option<&str> {
        self.columns
            .iter()
            .find(|(n, _)| n == c)
            .map(|(_, d)| d.as_str())
    }

    fn covers(&self, pk: &[String]) -> bool {
        let want: HashSet<&str> = pk.iter().map(String::as_str).collect();
        let same = |cols: &[String]| {
            cols.len() == want.len() && cols.iter().all(|c| want.contains(c.as_str()))
        };
        same(&self.primary_key) || self.unique.iter().any(|u| same(u))
    }
}

pub fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn list(cols: &[String]) -> String {
    cols.iter().map(|c| ident(c)).collect::<Vec<_>>().join(", ")
}

fn values(rows: usize, cols: usize) -> String {
    let one = format!("({})", vec!["?"; cols].join(", "));
    vec![one; rows].join(", ")
}

fn keys_in(pk: &[String], rows: usize) -> String {
    format!("({}) IN (VALUES {})", list(pk), values(rows, pk.len()))
}

pub struct Client {
    conn: Conn,
    txns: Txns,
    create_tables: bool,
    tables: HashMap<String, TableInfo>,
    max_rows: usize,
}

impl Client {
    pub async fn open(c: &Connection) -> Result<Self> {
        let (conn, txns, create_tables) = Conn::open(c).await?;
        Ok(Self {
            conn,
            txns,
            create_tables,
            tables: HashMap::new(),
            max_rows: crate::conn::DEFAULT_ROWS,
        })
    }

    pub fn kind(&self) -> &'static str {
        self.conn.kind()
    }

    /// Caps rows per statement (tests use it to force many batches).
    pub fn set_max_rows(&mut self, n: usize) {
        self.max_rows = n.max(1);
    }

    fn limits(&self) -> Limits {
        let l = self.conn.limits();
        Limits {
            rows: l.rows.min(self.max_rows),
            ..l
        }
    }

    pub async fn execute(&mut self, sql: &str) -> Result<u64> {
        Ok(self
            .conn
            .run(vec![Stmt::exec(sql, vec![])])
            .await?
            .pop()
            .map_or(0, |o| o.changes))
    }

    pub async fn query(&mut self, sql: &str, params: Vec<Value>) -> Result<Vec<Vec<Value>>> {
        Ok(self
            .conn
            .run(vec![Stmt::query(sql, params)])
            .await?
            .pop()
            .map(|o| o.rows)
            .unwrap_or_default())
    }

    /// Creates `_atlatl_cursors` if missing and checks its columns.
    pub async fn ensure_state(&mut self) -> Result<()> {
        self.execute(cursors::STATE_DDL).await?;
        let info = self
            .table_info(cursors::CURSORS_TABLE)
            .await?
            .unwrap_or_default();
        let missing: Vec<&str> = cursors::COLUMNS
            .into_iter()
            .filter(|c| info.decl(c).is_none())
            .collect();
        if !missing.is_empty() {
            return Err(AtlatlError::config(format!(
                "_atlatl_cursors is missing columns: {}",
                missing.join(", ")
            )));
        }
        Ok(())
    }

    /// Cursors for `ids` that exist, in `stream_id` order.
    pub async fn read_cursors(&mut self, ids: &[&str]) -> Result<Vec<Cursor>> {
        let mut out = Vec::new();
        for chunk in ids.chunks(self.conn.limits().params.max(1)) {
            for r in self
                .conn
                .run(vec![cursors::select(chunk)])
                .await?
                .remove(0)
                .rows
            {
                out.push(cursors::parse(&r)?);
            }
        }
        out.sort_by(|a, b| a.stream_id.cmp(&b.stream_id));
        Ok(out)
    }

    pub async fn table_info(&mut self, table: &str) -> Result<Option<TableInfo>> {
        let rows = self
            .query(&format!("PRAGMA table_info({})", ident(table)), vec![])
            .await?;
        if rows.is_empty() {
            return Ok(None);
        }
        let text = |v: &Value| v.as_str().unwrap_or_default().to_owned();
        let mut pk: Vec<(i64, String)> = Vec::new();
        let mut columns = Vec::new();
        for r in &rows {
            let name = r.get(1).map(text).unwrap_or_default();
            columns.push((name.clone(), r.get(2).map(text).unwrap_or_default()));
            if let Some(n) = r.get(5).and_then(Value::as_i64).filter(|n| *n > 0) {
                pk.push((n, name));
            }
        }
        pk.sort();
        let mut unique = Vec::new();
        for ix in self
            .query(&format!("PRAGMA index_list({})", ident(table)), vec![])
            .await?
        {
            let is_unique = ix.get(2).and_then(Value::as_i64) == Some(1);
            let partial = ix.get(4).and_then(Value::as_i64) == Some(1);
            if !is_unique || partial {
                continue;
            }
            let name = ix.get(1).map(text).unwrap_or_default();
            let cols = self
                .query(&format!("PRAGMA index_info({})", ident(&name)), vec![])
                .await?;
            let names: Vec<Option<String>> = cols
                .iter()
                .map(|c| c.get(2).and_then(Value::as_str).map(str::to_owned))
                .collect();
            if let Some(names) = names.into_iter().collect::<Option<Vec<_>>>() {
                unique.push(names);
            }
        }
        Ok(Some(TableInfo {
            columns,
            primary_key: pk.into_iter().map(|(_, n)| n).collect(),
            unique,
        }))
    }

    /// Checks `t` against the live table, creating it from `schema` (the served columns' Arrow
    /// types) when it is missing and `create_tables` is on. Failures are `config`.
    pub async fn prepare(&mut self, t: &Target, schema: &Schema) -> Result<TableInfo> {
        let name = t.table.clone();
        let cfg = |m: String| AtlatlError::config(format!("target {name}: {m}"));
        validate(t).map_err(cfg)?;
        let mut stores = Vec::new();
        for c in &t.columns {
            let f = schema
                .field_with_name(c)
                .map_err(|_| cfg(format!("served column `{c}` is not in the lake projection")))?;
            let s = t.store(c, f.data_type())?;
            if s == Store::Blob && matches!(self.conn, Conn::D1(_)) {
                return Err(cfg(format!(
                    "column `{c}`: the D1 REST API cannot bind BLOBs; store it as text"
                )));
            }
            stores.push((c.clone(), s));
        }
        let limits = self.conn.limits();
        if t.columns.len() > limits.params {
            return Err(cfg(format!(
                "{} served columns exceed the {} bound parameters a {} statement takes",
                t.columns.len(),
                limits.params,
                self.kind()
            )));
        }
        let info = match self.table_info(&t.table).await? {
            Some(i) => i,
            None if self.create_tables => {
                self.execute(&create_table_sql(t, &stores)).await?;
                self.table_info(&t.table)
                    .await?
                    .ok_or_else(|| cfg("the table was not created".into()))?
            }
            None => {
                return Err(cfg(
                    "the table does not exist and `create_tables` is false".into()
                ));
            }
        };
        let missing: Vec<&str> = t
            .columns
            .iter()
            .filter(|c| info.decl(c).is_none())
            .map(String::as_str)
            .collect();
        if !missing.is_empty() {
            return Err(cfg(format!(
                "columns.include has columns the table does not: {}",
                missing.join(", ")
            )));
        }
        if !info.covers(&t.primary_key) {
            return Err(cfg(format!(
                "no primary key or unique index covers exactly the primary key ({}); the upsert's ON CONFLICT needs one",
                t.primary_key.join(", ")
            )));
        }
        if let Some((col, value)) = t.soft_column() {
            let decl = info
                .decl(col)
                .ok_or_else(|| cfg(format!("the soft-delete column `{col}` does not exist")))?;
            let a = affinity(decl);
            let ok = match value {
                DeleteValue::True => a != Affinity::Text,
                DeleteValue::SnapshotTime | DeleteValue::Now => {
                    !matches!(a, Affinity::Integer | Affinity::Real)
                }
            };
            if !ok {
                let need = if value == DeleteValue::True {
                    "an integer (0/1)"
                } else {
                    "ISO-8601 text"
                };
                return Err(cfg(format!(
                    "the soft-delete column `{col}` is `{decl}`; the stamp is {need}"
                )));
            }
        }
        self.tables.insert(t.table.clone(), info.clone());
        Ok(info)
    }

    /// Every row of `cols` in `table`, ordered by `order` (paged on remote targets).
    pub async fn select_all(
        &mut self,
        table: &str,
        cols: &[String],
        order: &[String],
    ) -> Result<Vec<Vec<Value>>> {
        let base = format!(
            "SELECT {} FROM {} ORDER BY {}",
            list(cols),
            ident(table),
            list(order)
        );
        if let Conn::Sqlite(_) = self.conn {
            return self.query(&base, vec![]).await;
        }
        const PAGE: usize = 5_000;
        let mut out = Vec::new();
        loop {
            let page = self
                .query(&format!("{base} LIMIT {PAGE} OFFSET {}", out.len()), vec![])
                .await?;
            let n = page.len();
            out.extend(page);
            if n < PAGE {
                return Ok(out);
            }
        }
    }

    /// Starts an apply that moves `plans` (every stream's cursor), expecting about `rows`
    /// upserts plus deletes.
    pub async fn begin(&mut self, plans: Vec<CursorPlan>, rows: u64) -> Result<ApplyTxn<'_>> {
        let (windowed, split) = match self.txns {
            Txns::Single => (false, None),
            Txns::Split(n) => (rows > n, (rows > n).then_some(n)),
            Txns::None => (true, None),
        };
        let mut tx = ApplyTxn {
            c: self,
            plans,
            windowed,
            split,
            open: false,
            in_txn: 0,
            deleting: false,
            failed: false,
        };
        let r = tx.start().await;
        tx.leave(r).await?;
        Ok(tx)
    }
}

fn validate(t: &Target) -> std::result::Result<(), String> {
    if t.primary_key.is_empty() {
        return Err("primary_key is empty".into());
    }
    let mut seen = HashSet::new();
    if let Some(d) = t.columns.iter().find(|c| !seen.insert(c.as_str())) {
        return Err(format!("columns.include lists `{d}` twice"));
    }
    if let Some(p) = t.primary_key.iter().find(|p| !seen.contains(p.as_str())) {
        return Err(format!(
            "primary key column `{p}` is not in columns.include"
        ));
    }
    if let Some(c) = t.target_types.keys().find(|c| !seen.contains(c.as_str())) {
        return Err(format!(
            "column `{c}` has a target_type but is not in columns.include"
        ));
    }
    if let Some((c, _)) = t.soft_column()
        && t.primary_key.iter().any(|p| p == c)
    {
        return Err(format!(
            "the soft-delete column `{c}` is part of the primary key"
        ));
    }
    Ok(())
}

/// `CREATE TABLE` for `create_tables`: served columns in order, the soft-delete column when it
/// isn't served, and the primary key.
pub fn create_table_sql(t: &Target, stores: &[(String, Store)]) -> String {
    let mut cols: Vec<String> = stores
        .iter()
        .map(|(c, s)| {
            let nn = if t.primary_key.contains(c) {
                " NOT NULL"
            } else {
                ""
            };
            format!("{} {}{nn}", ident(c), s.decl())
        })
        .collect();
    if let Some((c, v)) = t.soft_column()
        && !t.columns.iter().any(|x| x == c)
    {
        cols.push(format!(
            "{} {}",
            ident(c),
            if v == DeleteValue::True {
                "INTEGER NOT NULL DEFAULT 0"
            } else {
                "TEXT"
            }
        ));
    }
    cols.push(format!("PRIMARY KEY ({})", list(&t.primary_key)));
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        ident(&t.table),
        cols.join(", ")
    )
}

pub struct ApplyTxn<'a> {
    c: &'a mut Client,
    plans: Vec<CursorPlan>,
    windowed: bool,
    /// Rows per transaction in a windowed Turso apply.
    split: Option<u64>,
    open: bool,
    in_txn: u64,
    deleting: bool,
    failed: bool,
}

impl ApplyTxn<'_> {
    /// Whether this apply uses the idempotent-window scheme.
    pub fn windowed(&self) -> bool {
        self.windowed
    }

    async fn start(&mut self) -> Result<()> {
        if !self.windowed {
            return self.open_txn().await;
        }
        let marks: Vec<(usize, Stmt)> = self
            .plans
            .iter()
            .enumerate()
            .filter(|(_, p)| p.write.mode == CursorMode::Cdc)
            .filter_map(|(i, p)| p.expected.map(|e| (i, cursors::mark_pending(p, e))))
            .collect();
        if !marks.is_empty() {
            let transactional = self.split.is_some();
            if transactional {
                self.c.conn.begin().await?;
            }
            let out = self
                .c
                .conn
                .run(marks.iter().map(|(_, s)| s.clone()).collect())
                .await?;
            if let Some(((i, _), _)) = marks.iter().zip(&out).find(|(_, o)| o.changes != 1) {
                if transactional {
                    let _ = self.c.conn.rollback().await;
                }
                return Err(cursors::conflict(&self.plans[*i]));
            }
            if transactional {
                self.c.conn.commit().await?;
            }
        }
        if self.split.is_some() {
            self.open_txn().await?;
        }
        Ok(())
    }

    async fn open_txn(&mut self) -> Result<()> {
        self.c.conn.begin().await?;
        self.open = true;
        self.in_txn = 0;
        Ok(())
    }

    /// Runs statement groups of `rows` rows each, committing and reopening a windowed Turso
    /// transaction once it holds `split` rows.
    async fn run(&mut self, groups: Vec<(u64, Vec<Stmt>)>) -> Result<Vec<Vec<Outcome>>> {
        let Some(limit) = self.split else {
            return self
                .c
                .conn
                .run_groups(groups.into_iter().map(|(_, g)| g).collect())
                .await;
        };
        let mut out = Vec::with_capacity(groups.len());
        for (rows, g) in groups {
            if self.in_txn > 0 && self.in_txn + rows > limit {
                self.c.conn.commit().await?;
                self.open = false;
                self.open_txn().await?;
            }
            out.push(self.c.conn.run(g).await?);
            self.in_txn += rows;
        }
        Ok(out)
    }

    fn enter(&mut self) -> Result<()> {
        if self.failed {
            return Err(AtlatlError::internal(
                "the apply already failed; start a new one",
            ));
        }
        Ok(())
    }

    async fn leave<T>(&mut self, r: Result<T>) -> Result<T> {
        if r.is_err() {
            self.failed = true;
            if self.open {
                self.open = false;
                let _ = self.c.conn.rollback().await;
            }
        }
        r
    }

    async fn info(&mut self, t: &Target, schema: &Schema) -> Result<TableInfo> {
        match self.c.tables.get(&t.table) {
            Some(i) => Ok(i.clone()),
            None => self.c.prepare(t, schema).await,
        }
    }

    /// Upserts `batches` (the served columns, any order) into `t`.
    pub async fn upsert<S>(&mut self, t: &Target, batches: S) -> Result<ApplyCounts>
    where
        S: Stream<Item = Result<RecordBatch>>,
    {
        self.enter()?;
        if self.deleting {
            let e = AtlatlError::internal(format!(
                "upsert into {} after deletes: apply every upsert before any delete",
                t.table
            ));
            return self.leave(Err(e)).await;
        }
        let mut batches = pin!(batches);
        let mut total = ApplyCounts::default();
        while let Some(b) = batches.next().await {
            let r = match b {
                Ok(b) => self.upsert_batch(t, &b).await,
                Err(e) => Err(e),
            };
            total += self.leave(r).await?;
        }
        Ok(total)
    }

    async fn upsert_batch(&mut self, t: &Target, b: &RecordBatch) -> Result<ApplyCounts> {
        if b.num_rows() == 0 {
            return Ok(ApplyCounts::default());
        }
        let schema = b.schema();
        let bad = |m: String| AtlatlError::internal(format!("upsert batch for {}: {m}", t.table));
        let mut names = HashSet::new();
        for f in schema.fields() {
            if !t.columns.contains(f.name()) {
                return Err(bad(format!("column `{}` is not served", f.name())));
            }
            if !names.insert(f.name().as_str()) {
                return Err(bad(format!("column `{}` is repeated", f.name())));
            }
        }
        if let Some(c) = t.columns.iter().find(|c| !names.contains(c.as_str())) {
            return Err(bad(format!("served column `{c}` is missing")));
        }
        self.info(t, &schema).await?;
        let cols: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let mut columns = Vec::with_capacity(cols.len());
        for (f, a) in schema.fields().iter().zip(b.columns()) {
            columns.push(column_values(
                a,
                t.store(f.name(), f.data_type())?,
                f.name(),
            )?);
        }
        let rows: Vec<Vec<Value>> = (0..b.num_rows())
            .map(|i| columns.iter().map(|c| c[i].clone()).collect())
            .collect();
        self.upsert_rows(t, &cols, rows).await
    }

    async fn upsert_rows(
        &mut self,
        t: &Target,
        cols: &[String],
        rows: Vec<Vec<Value>>,
    ) -> Result<ApplyCounts> {
        let limits = self.c.limits();
        if let Some(max) = limits.row_bytes
            && let Some(r) = rows
                .iter()
                .find(|r| r.iter().map(Value::size).sum::<usize>() > max)
        {
            let pk: Vec<String> = t
                .primary_key
                .iter()
                .filter_map(|k| cols.iter().position(|c| c == k))
                .map(|i| format!("{:?}", r[i]))
                .collect();
            return Err(AtlatlError::data(format!(
                "{}: row {} exceeds the {max}-byte row limit",
                t.table,
                pk.join(", ")
            )));
        }
        let pk_idx: Vec<usize> = t
            .primary_key
            .iter()
            .map(|k| cols.iter().position(|c| c == k).unwrap_or(0))
            .collect();
        let per = (limits.params / cols.len().max(1)).clamp(1, limits.rows);
        let restore = t.restore_column();
        let soft_bool = matches!(t.soft_column(), Some((_, DeleteValue::True)));
        let groups: Vec<(u64, Vec<Stmt>)> = rows
            .chunks(per)
            .map(|chunk| {
                let keys: Vec<Value> = chunk
                    .iter()
                    .flat_map(|r| pk_idx.iter().map(|&i| r[i].clone()))
                    .collect();
                let count = Stmt::query(
                    format!(
                        "SELECT count(*) FROM {} WHERE {}",
                        ident(&t.table),
                        keys_in(&t.primary_key, chunk.len())
                    ),
                    keys,
                );
                let up = Stmt::exec(
                    upsert_sql(t, cols, chunk.len(), restore, soft_bool),
                    chunk.iter().flatten().cloned().collect(),
                );
                (chunk.len() as u64, vec![count, up])
            })
            .collect();
        let sizes: Vec<u64> = groups.iter().map(|(n, _)| *n).collect();
        let out = self.run(groups).await?;
        let mut c = ApplyCounts::default();
        for (n, o) in sizes.into_iter().zip(out) {
            let existing = o
                .first()
                .and_then(|o| o.rows.first())
                .and_then(|r| r.first())
                .and_then(Value::as_i64)
                .unwrap_or(0) as u64;
            let changed = o.get(1).map_or(0, |o| o.changes);
            let ins = n.saturating_sub(existing);
            let upd = changed.saturating_sub(ins);
            c += ApplyCounts {
                upserts_sent: n,
                rows_inserted: ins,
                rows_updated: upd,
                rows_unchanged: n.saturating_sub(ins + upd),
                ..Default::default()
            };
        }
        Ok(c)
    }

    /// Applies `t.deletes` for `batches` (PK columns, plus [`DELETE_TS`] for
    /// `value: snapshot_time`; other columns are ignored).
    pub async fn delete<S>(&mut self, t: &Target, batches: S) -> Result<ApplyCounts>
    where
        S: Stream<Item = Result<RecordBatch>>,
    {
        self.deleting = true;
        self.enter()?;
        let mut batches = pin!(batches);
        let mut total = ApplyCounts::default();
        while let Some(b) = batches.next().await {
            let r = match b {
                Ok(b) => self.delete_batch(t, &b).await,
                Err(e) => Err(e),
            };
            total += self.leave(r).await?;
        }
        Ok(total)
    }

    async fn delete_batch(&mut self, t: &Target, b: &RecordBatch) -> Result<ApplyCounts> {
        if t.deletes == Deletes::Ignore {
            return Ok(ApplyCounts {
                deletes_ignored: b.num_rows() as u64,
                ..Default::default()
            });
        }
        let stamped = matches!(t.soft_column(), Some((_, DeleteValue::SnapshotTime)));
        let col = |c: &str| {
            b.schema()
                .index_of(c)
                .map(|i| b.column(i).clone())
                .map_err(|_| {
                    AtlatlError::internal(format!(
                        "delete batch for {} has no `{c}` column",
                        t.table
                    ))
                })
        };
        let mut pks = Vec::new();
        for k in &t.primary_key {
            let a = col(k)?;
            pks.push(column_values(&a, t.store(k, a.data_type())?, k)?);
        }
        let ts = if stamped {
            let a = col(DELETE_TS)?;
            if !matches!(a.data_type(), DataType::Timestamp(..)) {
                return Err(AtlatlError::internal(format!(
                    "`{DELETE_TS}` is {}, not a timestamp",
                    a.data_type()
                )));
            }
            Some(column_values(&a, Store::Text, DELETE_TS)?)
        } else {
            None
        };
        let mut keys = Vec::with_capacity(b.num_rows());
        for i in 0..b.num_rows() {
            let ts = match &ts {
                Some(v) => match &v[i] {
                    Value::Text(s) => Some(s.clone()),
                    _ => {
                        return Err(AtlatlError::internal(format!(
                            "delete batch for {} has rows without a `{DELETE_TS}` stamp",
                            t.table
                        )));
                    }
                },
                None => None,
            };
            keys.push(DeleteKey {
                pk: pks.iter().map(|c| c[i].clone()).collect(),
                ts,
            });
        }
        self.delete_rows(t, keys).await
    }

    /// Applies `t.deletes` for `keys` already in SQLite values.
    pub async fn delete_keys(&mut self, t: &Target, keys: Vec<DeleteKey>) -> Result<ApplyCounts> {
        self.deleting = true;
        self.enter()?;
        let r = if t.deletes == Deletes::Ignore {
            Ok(ApplyCounts {
                deletes_ignored: keys.len() as u64,
                ..Default::default()
            })
        } else {
            self.delete_rows(t, keys).await
        };
        self.leave(r).await
    }

    async fn delete_rows(&mut self, t: &Target, keys: Vec<DeleteKey>) -> Result<ApplyCounts> {
        if keys.is_empty() {
            return Ok(ApplyCounts::default());
        }
        let limits = self.c.limits();
        let npk = t.primary_key.len();
        let per = ((limits.params - 1) / npk).clamp(1, limits.rows);
        let table = ident(&t.table);
        let mut groups: Vec<(u64, Vec<Stmt>)> = Vec::new();
        let mut push = |set: Option<(String, Option<Value>, &str)>, keys: &[DeleteKey]| {
            for chunk in keys.chunks(per) {
                let mut params: Vec<Value> = Vec::new();
                let sql = match &set {
                    Some((c, v, pending)) => {
                        let rhs = match v {
                            Some(v) => {
                                params.push(v.clone());
                                "?"
                            }
                            None => "1",
                        };
                        format!(
                            "UPDATE {table} SET {c} = {rhs} WHERE {} AND {c} {pending}",
                            keys_in(&t.primary_key, chunk.len())
                        )
                    }
                    None => format!(
                        "DELETE FROM {table} WHERE {}",
                        keys_in(&t.primary_key, chunk.len())
                    ),
                };
                params.extend(chunk.iter().flat_map(|k| k.pk.iter().cloned()));
                groups.push((chunk.len() as u64, vec![Stmt::exec(sql, params)]));
            }
        };
        match &t.deletes {
            Deletes::Hard => push(None, &keys),
            Deletes::Soft { column, value, .. } => {
                let c = ident(column);
                match value {
                    DeleteValue::True => push(Some((c, None, "IS NOT TRUE")), &keys),
                    DeleteValue::Now => push(
                        Some((c, Some(Value::Text(iso(SystemTime::now()))), "IS NULL")),
                        &keys,
                    ),
                    DeleteValue::SnapshotTime => {
                        let mut by_ts: BTreeMap<String, Vec<DeleteKey>> = BTreeMap::new();
                        for k in keys {
                            let ts = k.ts.clone().ok_or_else(|| {
                                AtlatlError::internal(format!(
                                    "delete key for {} without a stamp",
                                    t.table
                                ))
                            })?;
                            by_ts.entry(ts).or_default().push(k);
                        }
                        for (ts, ks) in by_ts {
                            push(Some((c.clone(), Some(Value::Text(ts)), "IS NULL")), &ks);
                        }
                    }
                }
            }
            Deletes::Ignore => unreachable!("handled by the callers"),
        }
        let out = self.run(groups).await?;
        Ok(ApplyCounts {
            rows_deleted: out.iter().flatten().map(|o| o.changes).sum(),
            ..Default::default()
        })
    }

    /// Moves every cursor (compare-and-set) and commits.
    pub async fn commit(mut self) -> Result<()> {
        self.enter()?;
        let r = self.finish().await;
        self.leave(r).await
    }

    async fn finish(&mut self) -> Result<()> {
        let marked = |p: &CursorPlan| {
            self.windowed && p.write.mode == CursorMode::Cdc && p.expected.is_some()
        };
        let stmts: Vec<Stmt> = self
            .plans
            .iter()
            .map(|p| cursors::advance(p, marked(p).then(|| p.fingerprint()).as_deref()))
            .collect();
        let out = self.c.conn.run(stmts).await?;
        for (p, o) in self.plans.clone().iter().zip(out) {
            if o.changes == 1 || (!self.open && self.flipped(p).await?) {
                continue;
            }
            return Err(cursors::conflict(p));
        }
        if self.open {
            self.open = false;
            self.c.conn.commit().await?;
        }
        Ok(())
    }

    /// Without a transaction, a retried request may have moved the cursor already.
    async fn flipped(&mut self, p: &CursorPlan) -> Result<bool> {
        let cur = self.c.read_cursors(&[&p.write.stream_id]).await?;
        Ok(cur.first().is_some_and(|c| {
            c.snapshot_id == p.write.snapshot_id
                && c.pending.is_none()
                && c.config_hash == p.write.config_hash
        }))
    }

    pub async fn rollback(mut self) -> Result<()> {
        if self.open {
            self.open = false;
            return self.c.conn.rollback().await;
        }
        Ok(())
    }
}

impl Drop for ApplyTxn<'_> {
    fn drop(&mut self) {
        if self.open {
            self.c.conn.abandon();
        }
    }
}

/// `INSERT … ON CONFLICT (pk) DO UPDATE SET … WHERE (old) IS NOT (new)` for `rows` rows.
fn upsert_sql(
    t: &Target,
    cols: &[String],
    rows: usize,
    restore: Option<&str>,
    soft_bool: bool,
) -> String {
    let table = ident(&t.table);
    let mut set = Vec::new();
    let mut old = Vec::new();
    let mut new = Vec::new();
    for c in cols.iter().filter(|c| !t.primary_key.contains(c)) {
        let c = ident(c);
        set.push(format!("{c} = excluded.{c}"));
        old.push(format!("{table}.{c}"));
        new.push(format!("excluded.{c}"));
    }
    if let Some(d) = restore {
        let d = ident(d);
        if soft_bool {
            set.push(format!("{d} = 0"));
            old.push(format!("{table}.{d} IS TRUE"));
        } else {
            set.push(format!("{d} = NULL"));
            old.push(format!("{table}.{d} IS NOT NULL"));
        }
        new.push("0".into());
    }
    let action = if set.is_empty() {
        "DO NOTHING".to_owned()
    } else {
        format!(
            "DO UPDATE SET {} WHERE ({}) IS NOT ({})",
            set.join(", "),
            old.join(", "),
            new.join(", ")
        )
    };
    format!(
        "INSERT INTO {table} ({}) VALUES {} ON CONFLICT ({}) {action}",
        list(cols),
        values(rows, cols.len()),
        list(&t.primary_key)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(deletes: Deletes) -> Target {
        Target {
            table: "t".into(),
            primary_key: vec!["id".into()],
            columns: vec!["id".into(), "v".into()],
            target_types: BTreeMap::new(),
            deletes,
        }
    }

    #[test]
    fn statements() {
        let t = target(Deletes::Soft {
            column: "deleted_at".into(),
            value: DeleteValue::SnapshotTime,
            restore: true,
        });
        assert_eq!(
            upsert_sql(&t, &t.columns, 2, t.restore_column(), false),
            "INSERT INTO \"t\" (\"id\", \"v\") VALUES (?, ?), (?, ?) ON CONFLICT (\"id\") DO UPDATE SET \"v\" = excluded.\"v\", \
             \"deleted_at\" = NULL WHERE (\"t\".\"v\", \"t\".\"deleted_at\" IS NOT NULL) IS NOT (excluded.\"v\", 0)"
        );
        let only_pk = Target {
            columns: vec!["id".into()],
            ..target(Deletes::Hard)
        };
        assert!(upsert_sql(&only_pk, &only_pk.columns, 1, None, false).ends_with("DO NOTHING"));
        assert_eq!(
            keys_in(&["a".into(), "b".into()], 2),
            "(\"a\", \"b\") IN (VALUES (?, ?), (?, ?))"
        );
        assert_eq!(
            create_table_sql(
                &t,
                &[("id".into(), Store::Integer), ("v".into(), Store::Text)]
            ),
            "CREATE TABLE IF NOT EXISTS \"t\" (\"id\" INTEGER NOT NULL, \"v\" TEXT, \"deleted_at\" TEXT, PRIMARY KEY (\"id\"))"
        );
        assert_eq!(ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn validation() {
        let mut t = target(Deletes::Hard);
        assert!(validate(&t).is_ok());
        t.target_types.insert("x".into(), "text".into());
        assert!(
            validate(&t)
                .expect_err("tt")
                .contains("not in columns.include")
        );
        let t = Target {
            deletes: Deletes::Soft {
                column: "id".into(),
                value: DeleteValue::Now,
                restore: true,
            },
            ..target(Deletes::Hard)
        };
        assert!(
            validate(&t)
                .expect_err("pk")
                .contains("part of the primary key")
        );
    }
}
