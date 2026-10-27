//! CDC applier: every stream of a replication in one target transaction (spec 08 §6.1, §7).
//!
//! Usage: run [`ensure_state_schema`](crate::ensure_state_schema) once per run, before
//! [`PgApplier::begin`] (it has its own transaction). Then call [`ApplyTxn::upsert`] for every
//! stream in `after` order (parent-first), then [`ApplyTxn::delete`] for every stream in
//! reverse `after` order (child-first), then [`ApplyTxn::commit`] with the cursors. The phase
//! order is enforced: an upsert after any delete is an `internal` error. Dropping the
//! transaction, or any error, rolls everything back.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::AddAssign;
use std::pin::pin;
use std::time::Duration;

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use atlatl_core::AtlatlError;
use atlatl_core::config::model::DeleteValue;
use atlatl_core::error::Result;
use futures::{Stream, StreamExt};
use tokio_postgres::types::Type;
use tokio_postgres::{GenericClient, Transaction};

use crate::connect::Session;
use crate::copy::{CopyEncoder, CopyWriter, copyable};
use crate::cursors::{CursorWrite, advance_cursor};
use crate::error::wrap;

/// Column of a delete batch carrying the deleting snapshot's commit time.
pub const DELETE_TS: &str = "_ts";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplyOptions {
    pub lock_timeout: Option<Duration>,
    pub statement_timeout: Option<Duration>,
    pub idle_in_transaction_session_timeout: Option<Duration>,
}

impl Default for ApplyOptions {
    fn default() -> Self {
        Self {
            lock_timeout: Some(Duration::from_secs(30)),
            statement_timeout: None,
            idle_in_transaction_session_timeout: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Deletes {
    /// The column is a served source column when it is in [`StreamTarget::columns`]; then the
    /// upsert writes the lake's value and `restore` is ignored (08 §6.0).
    Soft {
        column: String,
        value: DeleteValue,
        restore: bool,
    },
    Hard,
    Ignore,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamTarget {
    pub schema: String,
    pub table: String,
    pub primary_key: Vec<String>,
    /// `columns.include`; must contain the primary key. Other target columns are never touched.
    pub columns: Vec<String>,
    /// `columns.<c>.target_type`.
    pub target_types: BTreeMap<String, String>,
    pub deletes: Deletes,
}

impl StreamTarget {
    pub fn qualified(&self) -> String {
        format!("{}.{}", ident(&self.schema), ident(&self.table))
    }

    /// The soft-delete column when the upsert must manage it (not served, `restore: true`).
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
}

/// Per-stream counts for the report (08 §11).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ApplyCounts {
    pub upserts_sent: u64,
    pub rows_inserted: u64,
    pub rows_updated: u64,
    /// Filtered by the `IS DISTINCT FROM` guard.
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

pub struct PgApplier;

impl PgApplier {
    /// `BEGIN` plus the `SET LOCAL`s. `TimeZone` is pinned to UTC so that timestamptz stamps
    /// land in `timestamp` columns as UTC.
    pub async fn begin<'a>(s: &'a mut Session, o: &ApplyOptions) -> Result<ApplyTxn<'a>> {
        let tx = s.client_mut().transaction().await.map_err(wrap)?;
        let mut sql = String::from("SET LOCAL TimeZone = 'UTC';");
        for (name, v) in [
            ("lock_timeout", o.lock_timeout),
            ("statement_timeout", o.statement_timeout),
            (
                "idle_in_transaction_session_timeout",
                o.idle_in_transaction_session_timeout,
            ),
        ] {
            if let Some(d) = v {
                let ms = if d.is_zero() { 0 } else { d.as_millis().max(1) };
                sql.push_str(&format!("SET LOCAL {name} = '{ms}ms';"));
            }
        }
        tx.batch_execute(&sql).await.map_err(wrap)?;
        Ok(ApplyTxn {
            tx,
            seq: 0,
            deleting: false,
            failed: false,
        })
    }
}

pub struct ApplyTxn<'a> {
    tx: Transaction<'a>,
    seq: u32,
    deleting: bool,
    /// Set while a step runs and kept on error: Postgres turns `COMMIT` of an aborted
    /// transaction into a silent `ROLLBACK`.
    failed: bool,
}

impl<'a> ApplyTxn<'a> {
    /// For statements the caller runs inside the apply, such as hooks.
    pub fn transaction(&self) -> &Transaction<'a> {
        &self.tx
    }

    /// Stages `batches` (the served columns, any order) and upserts them into `t`.
    pub async fn upsert<S>(&mut self, t: &StreamTarget, batches: S) -> Result<ApplyCounts>
    where
        S: Stream<Item = Result<RecordBatch>>,
    {
        if self.deleting {
            return Err(AtlatlError::internal(format!(
                "upsert into {} after deletes: apply every upsert before any delete",
                t.qualified()
            )));
        }
        self.enter()?;
        self.seq += 1;
        let r = upsert(&self.tx, self.seq, t, batches).await;
        self.leave(r)
    }

    /// Applies `t.deletes` for `batches` (PK columns, plus [`DELETE_TS`] for
    /// `value: snapshot_time`; other columns are ignored).
    pub async fn delete<S>(&mut self, t: &StreamTarget, batches: S) -> Result<ApplyCounts>
    where
        S: Stream<Item = Result<RecordBatch>>,
    {
        self.deleting = true;
        self.enter()?;
        self.seq += 1;
        let r = delete(&self.tx, self.seq, t, batches).await;
        self.leave(r)
    }

    /// Compare-and-sets each `(expected, cursor)` inside the transaction, then commits.
    pub async fn commit(mut self, cursors: &[(Option<i64>, CursorWrite<'_>)]) -> Result<()> {
        self.enter()?;
        for (expected, w) in cursors {
            if let Err(e) = advance_cursor(&self.tx, *expected, w).await {
                let _ = self.tx.rollback().await;
                return Err(e);
            }
        }
        self.tx.commit().await.map_err(wrap)
    }

    pub async fn rollback(self) -> Result<()> {
        self.tx.rollback().await.map_err(wrap)
    }

    fn enter(&mut self) -> Result<()> {
        if self.failed {
            return Err(AtlatlError::internal(
                "the apply transaction already failed; roll it back",
            ));
        }
        self.failed = true;
        Ok(())
    }

    fn leave<T>(&mut self, r: Result<T>) -> Result<T> {
        if r.is_ok() {
            self.failed = false;
        }
        r
    }
}

/// Plan-time check of `t` against the live target (08 §6.0, 06 §3.3). Failures are `config`.
pub async fn check_target(c: &impl GenericClient, t: &StreamTarget) -> Result<()> {
    inspect(c, t).await.map(|_| ())
}

struct TargetCol {
    oid: u32,
    /// `format_type`, quoted as needed.
    ty: String,
}

struct Resolved {
    cols: HashMap<String, TargetCol>,
    /// Column → canonical `target_type`.
    casts: HashMap<String, String>,
    soft_bool: bool,
}

async fn inspect(c: &impl GenericClient, t: &StreamTarget) -> Result<Resolved> {
    let name = t.qualified();
    let cfg = |m: String| AtlatlError::config(format!("target {name}: {m}"));
    validate(t).map_err(cfg)?;

    let rel = c
        .query_opt(
            "SELECT c.oid, c.relkind::text FROM pg_class c WHERE c.oid = to_regclass($1::text)",
            &[&name],
        )
        .await
        .map_err(wrap)?
        .ok_or_else(|| cfg("the table does not exist".into()))?;
    let oid: u32 = rel.get(0);
    let kind: String = rel.get(1);
    if kind != "r" && kind != "p" {
        return Err(cfg(format!("not a table (relkind `{kind}`)")));
    }

    let cols: HashMap<String, TargetCol> = c
        .query(
            "SELECT attname::text, atttypid, format_type(atttypid, NULL) FROM pg_attribute \
             WHERE attrelid = $1 AND attnum > 0 AND NOT attisdropped",
            &[&oid],
        )
        .await
        .map_err(wrap)?
        .iter()
        .map(|r| {
            (
                r.get(0),
                TargetCol {
                    oid: r.get(1),
                    ty: r.get(2),
                },
            )
        })
        .collect();
    let missing: Vec<&str> = t
        .columns
        .iter()
        .filter(|c| !cols.contains_key(*c))
        .map(String::as_str)
        .collect();
    if !missing.is_empty() {
        return Err(cfg(format!(
            "columns.include has columns the table does not: {}",
            missing.join(", ")
        )));
    }

    let pk: HashSet<&str> = t.primary_key.iter().map(String::as_str).collect();
    let unique = c
        .query(
            "SELECT ARRAY(SELECT a.attname::text FROM unnest(i.indkey::int2[]) k \
               JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k) \
             FROM pg_index i WHERE i.indrelid = $1 AND i.indisunique AND i.indisvalid \
               AND i.indimmediate AND i.indpred IS NULL AND i.indexprs IS NULL",
            &[&oid],
        )
        .await
        .map_err(wrap)?
        .iter()
        .any(|r| {
            let cols: Vec<String> = r.get(0);
            cols.len() == pk.len() && cols.iter().all(|c| pk.contains(c.as_str()))
        });
    if !unique {
        return Err(cfg(format!(
            "no unique index or constraint covers exactly the primary key ({}); the upsert's \
             ON CONFLICT needs one (partial, expression and deferrable indexes don't count)",
            t.primary_key.join(", ")
        )));
    }

    let mut soft_bool = false;
    if let Deletes::Soft { column, value, .. } = &t.deletes {
        let col = cols
            .get(column)
            .ok_or_else(|| cfg(format!("the soft-delete column `{column}` does not exist")))?;
        let ok = match value {
            DeleteValue::True => col.oid == Type::BOOL.oid(),
            DeleteValue::SnapshotTime | DeleteValue::Now => {
                [Type::TIMESTAMP.oid(), Type::TIMESTAMPTZ.oid()].contains(&col.oid)
            }
        };
        if !ok {
            let (v, need) = match value {
                DeleteValue::True => ("true", "boolean"),
                DeleteValue::SnapshotTime => ("snapshot_time", "timestamp or timestamptz"),
                DeleteValue::Now => ("now", "timestamp or timestamptz"),
            };
            return Err(cfg(format!(
                "the soft-delete column `{column}` is {}; `value: {v}` needs {need}",
                col.ty
            )));
        }
        soft_bool = col.oid == Type::BOOL.oid();
    }

    let mut casts = HashMap::new();
    for (col, ty) in &t.target_types {
        let canon: Option<String> = c
            .query_one("SELECT to_regtype($1::text)::text", &[ty])
            .await
            .map_err(|e| {
                let e = wrap(e);
                cfg(format!("column `{col}`: target_type `{ty}`: {}", e.message))
            })?
            .get(0);
        let canon =
            canon.ok_or_else(|| cfg(format!("column `{col}`: unknown target_type `{ty}`")))?;
        casts.insert(col.clone(), canon);
    }
    Ok(Resolved {
        cols,
        casts,
        soft_bool,
    })
}

fn validate(t: &StreamTarget) -> std::result::Result<(), String> {
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
    if let Deletes::Soft { column, .. } = &t.deletes
        && t.primary_key.contains(column)
    {
        return Err(format!(
            "the soft-delete column `{column}` is part of the primary key"
        ));
    }
    Ok(())
}

/// A staged column: its staging type and the expression that reads it as the target type.
struct Staged {
    ty: Type,
    expr: String,
}

fn stage(r: &Resolved, f: &Field, alias: &str) -> Result<Staged> {
    let name = f.name();
    let tc = r
        .cols
        .get(name)
        .ok_or_else(|| AtlatlError::internal(format!("column `{name}` is not resolved")))?;
    let col = format!("{alias}.{}", ident(name));
    let cast = r.casts.get(name);
    if cast.is_none()
        && let Some(ty) = Type::from_oid(tc.oid)
        && copyable(f.data_type(), &ty)
    {
        return Ok(Staged { ty, expr: col });
    }
    let ty = natural(f.data_type()).ok_or_else(|| {
        AtlatlError::config(format!(
            "column `{name}`: arrow {} has no postgres staging type",
            f.data_type()
        ))
    })?;
    let expr = match cast {
        Some(c) => format!("{col}::{c}"),
        None if ty.oid() == tc.oid => col,
        None => format!("{col}::{}", tc.ty),
    };
    Ok(Staged { ty, expr })
}

/// The wire type an arrow type is staged as when it can't go straight into the target type.
fn natural(dt: &DataType) -> Option<Type> {
    use DataType as D;
    Some(match dt {
        D::Boolean => Type::BOOL,
        D::Int8 | D::Int16 | D::UInt8 => Type::INT2,
        D::Int32 | D::UInt16 => Type::INT4,
        D::Int64 | D::UInt32 => Type::INT8,
        D::UInt64 | D::Decimal128(..) => Type::NUMERIC,
        D::Float32 => Type::FLOAT4,
        D::Float64 => Type::FLOAT8,
        D::Utf8 | D::LargeUtf8 | D::Utf8View | D::Null => Type::TEXT,
        D::Timestamp(_, Some(_)) => Type::TIMESTAMPTZ,
        D::Timestamp(_, None) => Type::TIMESTAMP,
        D::Date32 | D::Date64 => Type::DATE,
        D::Dictionary(_, v) => natural(v)?,
        D::List(f) | D::LargeList(f) => match natural(f.data_type())? {
            Type::BOOL => Type::BOOL_ARRAY,
            Type::INT2 => Type::INT2_ARRAY,
            Type::INT4 => Type::INT4_ARRAY,
            Type::INT8 => Type::INT8_ARRAY,
            Type::NUMERIC => Type::NUMERIC_ARRAY,
            Type::FLOAT4 => Type::FLOAT4_ARRAY,
            Type::FLOAT8 => Type::FLOAT8_ARRAY,
            Type::TEXT => Type::TEXT_ARRAY,
            Type::TIMESTAMPTZ => Type::TIMESTAMPTZ_ARRAY,
            Type::TIMESTAMP => Type::TIMESTAMP_ARRAY,
            Type::DATE => Type::DATE_ARRAY,
            _ => return None,
        },
        _ => return None,
    })
}

fn staging_ddl(table: &str, schema: &Schema, types: &[Type]) -> String {
    let cols: Vec<String> = schema
        .fields()
        .iter()
        .zip(types)
        .map(|(f, t)| {
            format!(
                "{} {}.{}",
                ident(f.name()),
                ident(t.schema()),
                ident(t.name())
            )
        })
        .collect();
    format!(
        "CREATE TEMP TABLE {} ({}) ON COMMIT DROP",
        ident(table),
        cols.join(", ")
    )
}

/// Creates `table` for `schema`, then COPYs `first` and the rest of `batches` into it.
async fn load<S>(
    tx: &Transaction<'_>,
    table: &str,
    types: Vec<Type>,
    first: RecordBatch,
    mut batches: std::pin::Pin<&mut S>,
    project: impl Fn(&RecordBatch) -> Result<RecordBatch>,
) -> Result<u64>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    let schema = first.schema();
    tx.batch_execute(&staging_ddl(table, &schema, &types))
        .await
        .map_err(wrap)?;
    let cols: Vec<String> = schema.fields().iter().map(|f| ident(f.name())).collect();
    let sql = format!(
        "COPY pg_temp.{} ({}) FROM STDIN (FORMAT binary)",
        ident(table),
        cols.join(", ")
    );
    let mut w = CopyWriter::open(tx, &sql, CopyEncoder::new(&schema, &types)?).await?;
    w.write(&first).await?;
    while let Some(b) = batches.next().await {
        let b = project(&b?)?;
        if b.schema()
            .fields()
            .iter()
            .map(|f| f.name())
            .ne(schema.fields().iter().map(|f| f.name()))
        {
            return Err(AtlatlError::internal(format!(
                "batch columns changed mid-stream while staging {table}"
            )));
        }
        w.write(&b).await?;
    }
    w.finish().await
}

async fn upsert<S>(
    tx: &Transaction<'_>,
    n: u32,
    t: &StreamTarget,
    batches: S,
) -> Result<ApplyCounts>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    let mut batches = pin!(batches);
    let Some(first) = batches.next().await.transpose()? else {
        return Ok(ApplyCounts::default());
    };
    let schema = first.schema();
    let bad = |m: String| AtlatlError::internal(format!("upsert batch for {}: {m}", t.qualified()));
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
    let r = inspect(tx, t).await?;
    let staged: Vec<Staged> = schema
        .fields()
        .iter()
        .map(|f| stage(&r, f, "s"))
        .collect::<Result<_>>()?;
    let table = format!("_atlatl_up_{n}");
    let sent = load(
        tx,
        &table,
        staged.iter().map(|s| s.ty.clone()).collect(),
        first,
        batches.as_mut(),
        |b| Ok(b.clone()),
    )
    .await?;

    let json = |c: &str| r.cols.get(c).is_some_and(|tc| tc.oid == Type::JSON.oid());
    let cols: Vec<String> = schema.fields().iter().map(|f| ident(f.name())).collect();
    let exprs: Vec<&str> = staged.iter().map(|s| s.expr.as_str()).collect();
    let mut set = Vec::new();
    let mut old = Vec::new();
    let mut new = Vec::new();
    for f in schema.fields() {
        if t.primary_key.contains(f.name()) {
            continue;
        }
        let c = ident(f.name());
        set.push(format!("{c} = EXCLUDED.{c}"));
        let cmp = if json(f.name()) { "::jsonb" } else { "" };
        old.push(format!("t.{c}{cmp}"));
        new.push(format!("EXCLUDED.{c}{cmp}"));
    }
    if let Some(d) = t.restore_column() {
        let c = ident(d);
        if r.soft_bool {
            set.push(format!("{c} = false"));
            old.push(format!("t.{c} IS TRUE"));
        } else {
            set.push(format!("{c} = NULL"));
            old.push(format!("t.{c} IS NOT NULL"));
        }
        new.push("false".into());
    }
    let pk: Vec<String> = t.primary_key.iter().map(|p| ident(p)).collect();
    let action = if set.is_empty() {
        "DO NOTHING".to_owned()
    } else {
        format!(
            "DO UPDATE SET {} WHERE ROW({}) IS DISTINCT FROM ROW({})",
            set.join(", "),
            old.join(", "),
            new.join(", ")
        )
    };
    let sql = format!(
        "WITH r AS (INSERT INTO {} AS t ({}) SELECT {} FROM pg_temp.{} s \
         ON CONFLICT ({}) {action} RETURNING (xmax = 0) AS ins) \
         SELECT count(*) FILTER (WHERE ins), count(*) FILTER (WHERE NOT ins) FROM r",
        t.qualified(),
        cols.join(", "),
        exprs.join(", "),
        ident(&table),
        pk.join(", "),
    );
    let row = tx.query_one(&sql, &[]).await.map_err(wrap)?;
    let (ins, upd): (i64, i64) = (row.get(0), row.get(1));
    drop_staging(tx, &table).await?;
    let (ins, upd) = (ins as u64, upd as u64);
    Ok(ApplyCounts {
        upserts_sent: sent,
        rows_inserted: ins,
        rows_updated: upd,
        rows_unchanged: sent.saturating_sub(ins + upd),
        ..ApplyCounts::default()
    })
}

async fn delete<S>(
    tx: &Transaction<'_>,
    n: u32,
    t: &StreamTarget,
    batches: S,
) -> Result<ApplyCounts>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    let mut batches = pin!(batches);
    if t.deletes == Deletes::Ignore {
        let mut ignored = 0;
        while let Some(b) = batches.next().await {
            ignored += b?.num_rows() as u64;
        }
        return Ok(ApplyCounts {
            deletes_ignored: ignored,
            ..ApplyCounts::default()
        });
    }
    let stamped = matches!(
        t.deletes,
        Deletes::Soft {
            value: DeleteValue::SnapshotTime,
            ..
        }
    );
    let mut want: Vec<&str> = t.primary_key.iter().map(String::as_str).collect();
    if stamped {
        want.push(DELETE_TS);
    }
    let project = |b: &RecordBatch| -> Result<RecordBatch> {
        let idx = want
            .iter()
            .map(|c| {
                b.schema().index_of(c).map_err(|_| {
                    AtlatlError::internal(format!(
                        "delete batch for {} has no `{c}` column",
                        t.qualified()
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let p = b
            .project(&idx)
            .map_err(|e| AtlatlError::internal(e.to_string()))?;
        if stamped && p.column(want.len() - 1).null_count() > 0 {
            return Err(AtlatlError::internal(format!(
                "delete batch for {} has rows without a `{DELETE_TS}` stamp",
                t.qualified()
            )));
        }
        Ok(p)
    };
    let Some(first) = batches.next().await.transpose()? else {
        return Ok(ApplyCounts::default());
    };
    let first = project(&first)?;
    let r = inspect(tx, t).await?;
    let schema = first.schema();
    let mut types = Vec::new();
    let mut on = Vec::new();
    for f in schema.fields() {
        if f.name() == DELETE_TS {
            let ty = match f.data_type() {
                DataType::Timestamp(..) => natural(f.data_type()),
                _ => None,
            }
            .ok_or_else(|| {
                AtlatlError::internal(format!(
                    "`{DELETE_TS}` is {}, not a timestamp",
                    f.data_type()
                ))
            })?;
            types.push(ty);
        } else {
            let s = stage(&r, f, "d")?;
            on.push(format!("t.{} = {}", ident(f.name()), s.expr));
            types.push(s.ty);
        }
    }
    let table = format!("_atlatl_del_{n}");
    load(tx, &table, types, first, batches.as_mut(), project).await?;
    let sql = match &t.deletes {
        Deletes::Soft { column, value, .. } => {
            let c = ident(column);
            let (v, pending) = match value {
                DeleteValue::SnapshotTime => (format!("d.{}", ident(DELETE_TS)), "IS NULL"),
                DeleteValue::Now => ("now()".into(), "IS NULL"),
                DeleteValue::True => ("true".into(), "IS NOT TRUE"),
            };
            format!(
                "UPDATE {} AS t SET {c} = {v} FROM pg_temp.{} d WHERE {} AND t.{c} {pending}",
                t.qualified(),
                ident(&table),
                on.join(" AND ")
            )
        }
        Deletes::Hard => format!(
            "DELETE FROM {} AS t USING pg_temp.{} d WHERE {}",
            t.qualified(),
            ident(&table),
            on.join(" AND ")
        ),
        Deletes::Ignore => unreachable!("handled above"),
    };
    let deleted = tx.execute(&sql, &[]).await.map_err(wrap)?;
    drop_staging(tx, &table).await?;
    Ok(ApplyCounts {
        rows_deleted: deleted,
        ..ApplyCounts::default()
    })
}

async fn drop_staging(tx: &Transaction<'_>, table: &str) -> Result<()> {
    tx.batch_execute(&format!("DROP TABLE pg_temp.{}", ident(table)))
        .await
        .map_err(wrap)
}

fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}
