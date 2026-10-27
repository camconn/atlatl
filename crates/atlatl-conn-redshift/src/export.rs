//! `access: wire` without `unload`: the member's SELECT streamed over the PG protocol (binary
//! results, rendered to text by [`crate::binary`]) into DuckDB. Rows come ordered by the member
//! key and are handed over only at key boundaries, so a dropped connection resumes after the last
//! complete key group instead of starting over (06 §4).

use std::future::Future;
use std::sync::Arc;

use atlatl_conn_files::error::duck;
use atlatl_core::error::{AtlatlError, Result};
use atlatl_core::retry::AttemptRecord;
use atlatl_ingest::sql::ident as duck_ident;
use duckdb::Connection;
use duckdb::arrow::array::{ArrayRef, RecordBatch, StringBuilder};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use tokio_postgres::Row;

use crate::binary::{Raw, render};
use crate::catalog::{Object, SourceColumn, ident, literal};
use crate::unload::select_sql;

pub const DEFAULT_BATCH_ROWS: usize = 50_000;

#[derive(Clone, Debug)]
pub struct ExportOptions {
    /// Rows per batch handed to the [`Sink`].
    pub batch_rows: usize,
    /// No data for this long once rows are flowing: the read is treated as dropped and
    /// resumed. A terminated backend behind a proxy or tunnel can leave the socket open and
    /// silent. Time spent in the sink doesn't count.
    pub stall: std::time::Duration,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            batch_rows: DEFAULT_BATCH_ROWS,
            stall: std::time::Duration::from_secs(300),
        }
    }
}

/// Key values of one row, rendered as text (resume literals).
pub type KeyValues = Vec<Option<String>>;

/// Per export session; values travel in binary, so these only shape server-side casts to text.
pub const SESSION_SQL: &str = "SET extra_float_digits TO 2; SET datestyle TO 'ISO, MDY'";

#[derive(Clone, Debug)]
pub struct ExportRequest {
    pub object: Object,
    /// From [`crate::Redshift::columns`], narrowed to what is read; the staging casts to these.
    pub columns: Vec<SourceColumn>,
    pub filters: Vec<String>,
    /// Resume key, usually the member's primary key. Empty (or not among `columns`): a dropped
    /// read starts over.
    pub key: Vec<String>,
}

impl ExportRequest {
    /// Positions of the key columns in `columns`, if all are read.
    pub fn key_positions(&self) -> Option<Vec<usize>> {
        if self.key.is_empty() {
            return None;
        }
        self.key
            .iter()
            .map(|k| {
                self.columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(k))
            })
            .collect()
    }

    /// The select, ordered by the key (`NULLS LAST`), continuing after `after` when resuming.
    pub fn sql(&self, after: Option<&[Option<String>]>) -> String {
        let keyed = self.key_positions().is_some();
        let mut filters = self.filters.clone();
        if let (true, Some(a)) = (keyed, after) {
            filters.push(after_key(&self.key, a));
        }
        let mut sql = select_sql(&self.object, &self.columns, &filters, None);
        if keyed {
            sql.push_str(" ORDER BY ");
            sql.push_str(
                &self
                    .key
                    .iter()
                    .map(|k| format!("{} ASC NULLS LAST", ident(k)))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        sql
    }
}

/// Rows strictly after `last` in `ORDER BY key ASC NULLS LAST` order. Values are the server's
/// own text, compared as untyped literals so Redshift coerces them to the column type.
pub fn after_key(key: &[String], last: &[Option<String>]) -> String {
    let eq = |j: usize| match &last[j] {
        Some(v) => format!("{} = {}", ident(&key[j]), literal(v)),
        None => format!("{} IS NULL", ident(&key[j])),
    };
    let terms: Vec<String> = (0..key.len())
        .filter_map(|i| {
            let v = last[i].as_ref()?;
            let c = ident(&key[i]);
            let gt = format!("({c} > {} OR {c} IS NULL)", literal(v));
            Some(if i == 0 {
                gt
            } else {
                let eqs: Vec<String> = (0..i).map(eq).collect();
                format!("({} AND {gt})", eqs.join(" AND "))
            })
        })
        .collect();
    if terms.is_empty() {
        "FALSE".into()
    } else {
        terms.join(" OR ")
    }
}

/// What the read did.
#[derive(Clone, Debug, Default)]
pub struct Exported {
    pub rows: u64,
    /// Text bytes received.
    pub bytes: u64,
    /// Dropped reads continued after the last complete key group.
    pub resumes: u32,
    /// Dropped reads started over (no usable key).
    pub restarts: u32,
    pub retries: Vec<AttemptRecord>,
}

/// Where exported rows go, one all-`Utf8` batch at a time.
pub trait Sink: Send {
    fn append(&mut self, batch: RecordBatch) -> impl Future<Output = Result<()>> + Send;
    /// Drops everything appended so far (a keyless read starting over).
    fn reset(&mut self) -> impl Future<Output = Result<()>> + Send;
}

/// Buffers rows, releasing them only up to the last key boundary.
pub(crate) struct Batcher {
    schema: Arc<Schema>,
    key: Option<Vec<usize>>,
    rows: Vec<Row>,
    pub bytes: u64,
}

impl Batcher {
    pub fn new(columns: &[SourceColumn], key: Option<Vec<usize>>) -> Self {
        let fields: Vec<Field> = columns
            .iter()
            .map(|c| Field::new(&c.name, DataType::Utf8, true))
            .collect();
        Self {
            schema: Arc::new(Schema::new(fields)),
            key,
            rows: Vec::new(),
            bytes: 0,
        }
    }

    pub fn has_key(&self) -> bool {
        self.key.is_some()
    }

    pub fn width(&self) -> usize {
        self.schema.fields().len()
    }

    pub fn push(&mut self, r: Row) -> Result<()> {
        if r.len() != self.width() {
            return Err(AtlatlError::internal(format!(
                "Redshift export returned {} columns, expected {}",
                r.len(),
                self.width()
            )));
        }
        self.bytes += (0..r.len())
            .map(|i| raw(&r, i).map_or(0, |v| v.len()) as u64)
            .sum::<u64>();
        self.rows.push(r);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    fn key_of(&self, r: &Row) -> Result<Option<KeyValues>> {
        self.key
            .as_ref()
            .map(|k| k.iter().map(|&i| text(r, i)).collect())
            .transpose()
    }

    fn same_key(&self, a: &Row, b: &Row) -> bool {
        self.key
            .as_ref()
            .is_some_and(|k| k.iter().all(|&i| raw(a, i) == raw(b, i)))
    }

    /// Rows that can be released now: all of them when `all` (the read ended) or keyless,
    /// otherwise everything before the trailing key group.
    fn releasable(&self, all: bool) -> usize {
        if all || self.key.is_none() {
            return self.rows.len();
        }
        let Some(last) = self.rows.last() else {
            return 0;
        };
        let mut i = self.rows.len() - 1;
        while i > 0 && self.same_key(&self.rows[i - 1], last) {
            i -= 1;
        }
        i
    }

    /// Takes the releasable rows as a batch, with the key of its last row.
    pub fn take(&mut self, all: bool) -> Result<Option<(RecordBatch, Option<KeyValues>)>> {
        let n = self.releasable(all);
        if n == 0 {
            return Ok(None);
        }
        let key = self.key_of(&self.rows[n - 1])?;
        let rows: Vec<Row> = self.rows.drain(..n).collect();
        let cols: Vec<ArrayRef> = (0..self.width())
            .map(|i| {
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
                for r in &rows {
                    b.append_option(text(r, i)?);
                }
                Ok(Arc::new(b.finish()) as ArrayRef)
            })
            .collect::<Result<_>>()?;
        let batch = RecordBatch::try_new(self.schema.clone(), cols)
            .map_err(|e| AtlatlError::internal(format!("building an export batch: {e}")))?;
        Ok(Some((batch, key)))
    }

    /// Drops buffered rows (after a failed read: the trailing group may be incomplete).
    pub fn clear(&mut self) {
        self.rows.clear();
    }
}

fn raw(r: &Row, i: usize) -> Option<&[u8]> {
    r.try_get::<_, Option<Raw>>(i).ok().flatten().map(|v| v.0)
}

fn text(r: &Row, i: usize) -> Result<Option<String>> {
    raw(r, i)
        .map(|b| render(r.columns()[i].type_(), b))
        .transpose()
}

/// The DuckDB side: each batch lands in a `VARCHAR` temp table and is cast into the typed one,
/// so cast failures surface at the batch that carries them.
#[derive(Clone, Debug)]
pub struct RawTable {
    /// The typed temp table the member is staged from.
    pub name: String,
    pub columns: Vec<SourceColumn>,
}

impl RawTable {
    pub fn new(name: impl Into<String>, columns: Vec<SourceColumn>) -> Self {
        Self {
            name: name.into(),
            columns,
        }
    }

    fn text_name(&self) -> String {
        format!("{}_text", self.name)
    }

    pub fn create(&self, c: &Connection) -> Result<()> {
        if self.columns.is_empty() {
            return Err(AtlatlError::internal("an export needs the source columns"));
        }
        let typed = self
            .columns
            .iter()
            .map(|c| format!("{} {}", duck_ident(&c.name), c.duck_type))
            .collect::<Vec<_>>()
            .join(", ");
        let text = self
            .columns
            .iter()
            .map(|c| format!("{} VARCHAR", duck_ident(&c.name)))
            .collect::<Vec<_>>()
            .join(", ");
        c.execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE {} ({typed}); CREATE OR REPLACE TEMP TABLE {} ({text})",
            duck_ident(&self.name),
            duck_ident(&self.text_name())
        ))
        .map_err(duck)
    }

    /// Text → typed. `timestamptz` text carries its offset; booleans are `t`/`f`.
    fn projection(&self) -> String {
        self.columns
            .iter()
            .map(|c| {
                let q = duck_ident(&c.name);
                format!("CAST({q} AS {}) AS {q}", c.duck_type)
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Appends one batch; returns its row count.
    pub fn append(&self, c: &Connection, batch: RecordBatch) -> Result<u64> {
        let n = batch.num_rows() as u64;
        let text = self.text_name();
        {
            let mut a = c
                .appender_to_catalog_and_db(&text, "temp", "main")
                .map_err(duck)?;
            a.append_record_batch(batch).map_err(duck)?;
            a.flush().map_err(duck)?;
        }
        c.execute_batch(&format!(
            "INSERT INTO {} SELECT {} FROM {t}; DELETE FROM {t}",
            duck_ident(&self.name),
            self.projection(),
            t = duck_ident(&text)
        ))
        .map_err(|e| {
            let e = duck(e);
            AtlatlError {
                message: format!("casting exported rows to their source types: {}", e.message),
                ..e
            }
        })?;
        Ok(n)
    }

    pub fn reset(&self, c: &Connection) -> Result<()> {
        c.execute_batch(&format!(
            "DELETE FROM {}; DELETE FROM {}",
            duck_ident(&self.name),
            duck_ident(&self.text_name())
        ))
        .map_err(duck)
    }

    /// `DROP` statements for once the unit is done.
    pub fn drop_sql(&self) -> Vec<String> {
        vec![
            format!("DROP TABLE IF EXISTS temp.{}", duck_ident(&self.name)),
            format!(
                "DROP TABLE IF EXISTS temp.{}",
                duck_ident(&self.text_name())
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::duck_type;

    fn col(name: &str, t: &str) -> SourceColumn {
        SourceColumn {
            name: name.into(),
            data_type: t.into(),
            char_len: None,
            precision: None,
            scale: None,
            duck_type: duck_type(t, None, None).expect("type"),
        }
    }

    fn req(key: &[&str]) -> ExportRequest {
        ExportRequest {
            object: Object::parse("db.s.t"),
            columns: vec![col("a", "bigint"), col("b", "varchar"), col("v", "date")],
            filters: vec!["\"v\" >= '2024-01-01'".into()],
            key: key.iter().map(|k| (*k).to_owned()).collect(),
        }
    }

    #[test]
    fn statements() {
        assert_eq!(
            req(&["a", "b"]).sql(None),
            r#"SELECT "a", "b", "v" FROM "db"."s"."t" WHERE ("v" >= '2024-01-01') ORDER BY "a" ASC NULLS LAST, "b" ASC NULLS LAST"#
        );
        assert_eq!(
            req(&["a"]).sql(Some(&[Some("7".into())])),
            r#"SELECT "a", "b", "v" FROM "db"."s"."t" WHERE ("v" >= '2024-01-01') AND (("a" > '7' OR "a" IS NULL)) ORDER BY "a" ASC NULLS LAST"#
        );
        // Keyless, or a key that isn't read: no order, and a resume can't narrow.
        for r in [req(&[]), req(&["zz"])] {
            assert!(r.key_positions().is_none());
            assert_eq!(
                r.sql(Some(&[Some("7".into())])),
                r#"SELECT "a", "b", "v" FROM "db"."s"."t" WHERE ("v" >= '2024-01-01')"#
            );
        }
        assert_eq!(req(&["B", "a"]).key_positions(), Some(vec![1, 0]));
    }

    #[test]
    fn after_keys() {
        let k = ["a".to_owned(), "b".to_owned()];
        assert_eq!(
            after_key(&k, &[Some("1".into()), Some("x'y".into())]),
            r#"("a" > '1' OR "a" IS NULL) OR ("a" = '1' AND ("b" > 'x''y' OR "b" IS NULL))"#
        );
        // NULLS LAST: after a NULL only NULLs follow in that position.
        assert_eq!(
            after_key(&k, &[None, Some("x".into())]),
            r#"("a" IS NULL AND ("b" > 'x' OR "b" IS NULL))"#
        );
        assert_eq!(after_key(&k, &[None, None]), "FALSE");
    }

    #[test]
    fn raw_table_casts_server_text() {
        let c = Connection::open_in_memory().expect("duckdb");
        let cols = vec![
            col("i", "bigint"),
            SourceColumn {
                precision: Some(18),
                scale: Some(4),
                duck_type: "DECIMAL(18,4)".into(),
                ..col("n", "numeric")
            },
            col("b", "boolean"),
            col("ts", "timestamp"),
            col("tz", "timestamp with time zone"),
            col("f", "double precision"),
            col("s", "character varying"),
        ];
        let raw = RawTable::new("stg_raw", cols.clone());
        raw.create(&c).expect("create");
        let mut b = Batcher::new(&cols, None);
        let schema = b.schema.clone();
        let text = |v: &[Option<&str>]| {
            let arrays: Vec<ArrayRef> = v
                .iter()
                .map(|x| {
                    let mut s = StringBuilder::new();
                    s.append_option(*x);
                    Arc::new(s.finish()) as ArrayRef
                })
                .collect();
            RecordBatch::try_new(schema.clone(), arrays).expect("batch")
        };
        let n = raw
            .append(
                &c,
                text(&[
                    Some("1"),
                    Some("12345678901234.5678"),
                    Some("t"),
                    Some("2024-01-31 12:34:56.123456"),
                    Some("2024-01-31 07:34:56.123456+00"),
                    Some("1e-300"),
                    Some("it's"),
                ]),
            )
            .expect("append");
        assert_eq!(n, 1);
        raw.append(
            &c,
            text(&[None, None, Some("f"), None, None, Some("NaN"), None]),
        )
        .expect("nulls");
        let row: (i64, String, bool, String, String, f64) = c
            .query_row(
                "SELECT i, n::VARCHAR, b, ts::VARCHAR, (tz AT TIME ZONE 'UTC')::VARCHAR, f FROM temp.stg_raw WHERE i = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
            )
            .expect("row");
        assert_eq!(
            row,
            (
                1,
                "12345678901234.5678".into(),
                true,
                "2024-01-31 12:34:56.123456".into(),
                "2024-01-31 07:34:56.123456".into(),
                1e-300
            )
        );
        let nan: bool = c
            .query_row(
                "SELECT isnan(f) FROM temp.stg_raw WHERE i IS NULL",
                [],
                |r| r.get(0),
            )
            .expect("nan");
        assert!(nan);
        b.clear();
        let e = raw
            .append(&c, text(&[Some("x"), None, None, None, None, None, None]))
            .expect_err("cast");
        assert!(e.message.contains("casting exported rows"), "{e}");
        raw.reset(&c).expect("reset");
        let n: i64 = c
            .query_row("SELECT count(*) FROM temp.stg_raw", [], |r| r.get(0))
            .expect("count");
        assert_eq!(n, 0);
    }
}
