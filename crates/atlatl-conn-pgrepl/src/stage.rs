//! Per-stream staging of decoded changes: a TEMP table of text values plus change metadata,
//! filled through the appender in bounded batches so a large transaction never sits in memory.

use std::collections::{BTreeSet, HashMap};

use atlatl_core::error::Result;
use atlatl_ingest::sql::{db, ident, literal};
use duckdb::Connection;
use duckdb::types::Value;

const BATCH: usize = 8192;

/// Staging columns ahead of the data columns. `__from` is the old key of a key-changing update
/// (JSON array of key texts); `__fb` is the key whose lake row supplies unchanged TOAST values;
/// `__toast` lists the columns sent as unchanged.
pub(crate) const META: [(&str, &str); 6] = [
    ("__seq", "BIGINT"),
    ("__txn", "BIGINT"),
    ("__op", "VARCHAR"),
    ("__from", "VARCHAR"),
    ("__fb", "VARCHAR"),
    ("__toast", "VARCHAR"),
];

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StageCol {
    pub name: String,
    pub oid: u32,
    pub typmod: i32,
    pub land: crate::types::Land,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Op {
    Insert,
    Update,
    Delete,
    Truncate,
}

impl Op {
    fn code(self) -> &'static str {
        match self {
            Op::Insert => "I",
            Op::Update => "U",
            Op::Delete => "D",
            Op::Truncate => "T",
        }
    }
}

/// Column names the staging and lake metadata use.
fn reserved(name: &str) -> Result<()> {
    if META.iter().any(|(m, _)| *m == name) || name.starts_with("_atlatl_") {
        return Err(atlatl_core::AtlatlError::config(format!(
            "source column `{name}` collides with a name atlatl reserves for change metadata"
        )));
    }
    Ok(())
}

/// A key-changing update that arrived with unchanged TOAST columns; resolved before netting.
#[derive(Clone, Debug)]
pub(crate) struct KeyChange {
    pub seq: i64,
    pub txn: i64,
    pub old_key: String,
}

pub(crate) struct Row {
    pub seq: i64,
    pub txn: i64,
    pub op: Op,
    pub from: Option<String>,
    pub fb: Option<String>,
    pub toast: Vec<String>,
    pub values: Vec<(String, Option<String>)>,
}

pub(crate) struct Stage {
    pub table: String,
    /// The source table, as errors name it (`schema.table`).
    pub label: String,
    pub cols: Vec<StageCol>,
    index: HashMap<String, usize>,
    pub pk: Vec<String>,
    buf: Vec<Vec<Value>>,
    pub rows: u64,
    pub toast: BTreeSet<String>,
    /// `(seq, txn)` of the last truncate (a backfill starts with one).
    pub last_truncate: Option<(i64, i64)>,
    pub keychanges: Vec<KeyChange>,
}

impl Stage {
    pub fn create(
        c: &Connection,
        table: &str,
        label: &str,
        mut cols: Vec<StageCol>,
        pk: Vec<String>,
    ) -> Result<Self> {
        for col in &mut cols {
            reserved(&col.name)?;
            crate::types::canonicalize(c, col)?;
        }
        let defs: Vec<String> = META
            .iter()
            .map(|(n, t)| format!("{n} {t}"))
            .chain(
                cols.iter()
                    .map(|col| format!("{} VARCHAR", ident(&col.name))),
            )
            .collect();
        c.execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE {} ({})",
            ident(table),
            defs.join(", ")
        ))
        .map_err(db)?;
        let index = cols
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        Ok(Self {
            table: table.to_owned(),
            label: label.to_owned(),
            cols,
            index,
            pk,
            buf: Vec::new(),
            rows: 0,
            toast: BTreeSet::new(),
            last_truncate: None,
            keychanges: Vec::new(),
        })
    }

    /// Adds columns first seen in a Relation message; refreshes types of known ones.
    pub fn ensure_cols(&mut self, c: &Connection, cols: &[StageCol]) -> Result<()> {
        for col in cols {
            let mut col = col.clone();
            crate::types::canonicalize(c, &mut col)?;
            if let Some(&i) = self.index.get(&col.name) {
                self.cols[i] = col;
                continue;
            }
            reserved(&col.name)?;
            self.flush(c)?;
            c.execute_batch(&format!(
                "ALTER TABLE temp.main.{} ADD COLUMN {} VARCHAR",
                ident(&self.table),
                ident(&col.name)
            ))
            .map_err(db)?;
            self.index.insert(col.name.clone(), self.cols.len());
            self.cols.push(col);
        }
        Ok(())
    }

    pub fn col(&self, name: &str) -> Option<&StageCol> {
        self.index.get(name).map(|&i| &self.cols[i])
    }

    pub fn push(&mut self, c: &Connection, r: Row) -> Result<()> {
        let mut row = Vec::with_capacity(META.len() + self.cols.len());
        row.push(Value::BigInt(r.seq));
        row.push(Value::BigInt(r.txn));
        row.push(Value::Text(r.op.code().into()));
        row.push(r.from.map_or(Value::Null, Value::Text));
        row.push(r.fb.map_or(Value::Null, Value::Text));
        row.push(if r.toast.is_empty() {
            Value::Null
        } else {
            Value::Text(serde_json::Value::from(r.toast.clone()).to_string())
        });
        row.resize(META.len() + self.cols.len(), Value::Null);
        for (name, v) in r.values {
            if let (Some(&i), Some(v)) = (self.index.get(&name), v) {
                row[META.len() + i] = Value::Text(v);
            }
        }
        self.toast.extend(r.toast);
        if r.op == Op::Truncate {
            self.last_truncate = Some((r.seq, r.txn));
        }
        self.buf.push(row);
        self.rows += 1;
        if self.buf.len() >= BATCH {
            self.flush(c)?;
        }
        Ok(())
    }

    pub fn flush(&mut self, c: &Connection) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let mut app = c
            .appender_to_catalog_and_db(&self.table, "temp", "main")
            .map_err(db)?;
        for row in self.buf.drain(..) {
            app.append_row(duckdb::appender_params_from_iter(row))
                .map_err(db)?;
        }
        app.flush().map_err(db)
    }

    /// Drops the rows of a transaction that will not be committed in this window.
    pub fn discard_txn(&mut self, c: &Connection, txn: i64) -> Result<()> {
        let before = self.buf.len();
        self.buf.retain(|r| r[1] != Value::BigInt(txn));
        let mut dropped = (before - self.buf.len()) as u64;
        dropped += c
            .execute(
                &format!(
                    "DELETE FROM temp.main.{} WHERE __txn = ?",
                    ident(&self.table)
                ),
                [txn],
            )
            .map_err(db)? as u64;
        self.rows -= dropped;
        self.keychanges.retain(|k| k.txn != txn);
        if self.last_truncate.is_some_and(|(_, t)| t == txn) {
            self.refresh_last_truncate(c)?;
        }
        Ok(())
    }

    /// Drops every streamed change, keeping the backfill (transaction 0).
    pub fn discard_changes(&mut self, c: &Connection) -> Result<()> {
        self.buf.retain(|r| r[1] == Value::BigInt(0));
        c.execute(
            &format!(
                "DELETE FROM temp.main.{} WHERE __txn <> 0",
                ident(&self.table)
            ),
            [],
        )
        .map_err(db)?;
        self.flush(c)?;
        self.rows = c
            .query_row(
                &format!("SELECT count(*) FROM temp.main.{}", ident(&self.table)),
                [],
                |r| r.get::<_, i64>(0),
            )
            .map_err(db)? as u64;
        self.keychanges.retain(|k| k.txn == 0);
        self.refresh_last_truncate(c)
    }

    fn refresh_last_truncate(&mut self, c: &Connection) -> Result<()> {
        self.last_truncate = c
            .query_row(
                &format!(
                    "SELECT max(__seq), arg_max(__txn, __seq) FROM temp.main.{} WHERE __op = 'T'",
                    ident(&self.table)
                ),
                [],
                |r| Ok((r.get::<_, Option<i64>>(0)?, r.get::<_, Option<i64>>(1)?)),
            )
            .map_err(db)
            .map(|(s, t)| s.zip(t))?;
        Ok(())
    }

    /// A streamed transaction committed: its rows, staged under the `parts` ordinals (one per
    /// subtransaction) with provisional sequence numbers, move to `txn` and are renumbered from
    /// `base + 1` in arrival order, so `__seq` is commit order again. Returns the rows moved.
    pub fn commit_streamed(
        &mut self,
        c: &Connection,
        parts: &[i64],
        txn: i64,
        base: i64,
    ) -> Result<i64> {
        if parts.is_empty() {
            return Ok(0);
        }
        self.flush(c)?;
        let t = format!("temp.main.{}", ident(&self.table));
        let set = parts
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        for kc in self
            .keychanges
            .iter_mut()
            .filter(|k| parts.contains(&k.txn))
        {
            let rank: i64 = c
                .query_row(
                    &format!("SELECT count(*) FROM {t} WHERE __txn IN ({set}) AND __seq <= ?"),
                    [kc.seq],
                    |r| r.get(0),
                )
                .map_err(db)?;
            kc.seq = base + rank;
            kc.txn = txn;
        }
        let moved = c
            .execute(
                &format!(
                    "UPDATE {t} SET __seq = m.n, __txn = {txn} FROM (
                       SELECT __seq AS o, {base} + row_number() OVER (ORDER BY __seq) AS n
                       FROM {t} WHERE __txn IN ({set})
                     ) m WHERE {t}.__seq = m.o"
                ),
                [],
            )
            .map_err(db)?;
        self.refresh_last_truncate(c)?;
        Ok(moved as i64)
    }

    fn is_toast(&self, col: &str) -> String {
        format!(
            "(__toast IS NOT NULL AND list_contains(from_json(__toast, '[\"VARCHAR\"]'), {}))",
            literal(col)
        )
    }

    fn key_match(&self, key_json: &str) -> String {
        self.pk
            .iter()
            .enumerate()
            .map(|(i, k)| {
                format!(
                    "{} IS NOT DISTINCT FROM json_extract_string({}, '$[{i}]')",
                    ident(k),
                    literal(key_json)
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    /// Fills the unchanged columns of key-changing updates from the old key's latest staged
    /// values; what is still missing falls back to the lake row of that segment's origin key.
    pub fn resolve_key_changes(&mut self, c: &Connection) -> Result<()> {
        self.flush(c)?;
        let t = format!("temp.main.{}", ident(&self.table));
        for kc in std::mem::take(&mut self.keychanges) {
            let toast: Option<String> = c
                .query_row(
                    &format!("SELECT __toast FROM {t} WHERE __seq = ?"),
                    [kc.seq],
                    |r| r.get(0),
                )
                .map_err(db)?;
            let Some(toast) = toast else { continue };
            let cols: Vec<String> = serde_json::from_str(&toast).unwrap_or_default();
            let old = self.key_match(&kc.old_key);
            let trunc = self.last_truncate_before(c, kc.seq)?;
            let start: Option<(i64, Option<String>)> = c
                .query_row(
                    &format!(
                        "SELECT __seq, __fb FROM {t} WHERE {old} AND __seq < ? AND __seq > ?
                         AND (__op = 'I' OR __from IS NOT NULL) ORDER BY __seq DESC LIMIT 1"
                    ),
                    [kc.seq, trunc],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .map(Some)
                .or_else(|e| match e {
                    duckdb::Error::QueryReturnedNoRows => Ok(None),
                    e => Err(e),
                })
                .map_err(db)?;
            let lower = start.as_ref().map_or(trunc, |s| s.0 - 1).max(trunc);
            let mut remaining = Vec::new();
            for col in cols {
                let found: Option<Option<String>> = c
                    .query_row(
                        &format!(
                            "SELECT {q} FROM {t} WHERE {old} AND __seq < ? AND __seq > ?
                             AND __op IN ('I', 'U') AND NOT {toast}
                             ORDER BY __seq DESC LIMIT 1",
                            q = ident(&col),
                            toast = self.is_toast(&col)
                        ),
                        [kc.seq, lower],
                        |r| r.get(0),
                    )
                    .map(Some)
                    .or_else(|e| match e {
                        duckdb::Error::QueryReturnedNoRows => Ok(None),
                        e => Err(e),
                    })
                    .map_err(db)?;
                match found {
                    Some(v) => {
                        c.execute(
                            &format!("UPDATE {t} SET {} = ? WHERE __seq = ?", ident(&col)),
                            duckdb::params![v, kc.seq],
                        )
                        .map_err(db)?;
                    }
                    None => remaining.push(col),
                }
            }
            let fb = match start {
                Some((_, fb)) => fb.unwrap_or_else(|| kc.old_key.clone()),
                None => kc.old_key.clone(),
            };
            let toast =
                (!remaining.is_empty()).then(|| serde_json::Value::from(remaining).to_string());
            c.execute(
                &format!("UPDATE {t} SET __toast = ?, __fb = ? WHERE __seq = ?"),
                duckdb::params![toast, fb, kc.seq],
            )
            .map_err(db)?;
        }
        Ok(())
    }

    fn last_truncate_before(&self, c: &Connection, seq: i64) -> Result<i64> {
        let v: Option<i64> = c
            .query_row(
                &format!(
                    "SELECT max(__seq) FROM temp.main.{} WHERE __op = 'T' AND __seq < ?",
                    ident(&self.table)
                ),
                [seq],
                |r| r.get(0),
            )
            .map_err(db)?;
        Ok(v.unwrap_or(-1))
    }

    /// SQL for the netted changes: the last change per current key after the last truncate,
    /// with unchanged TOAST columns carried from earlier in the key's segment or from `lake`
    /// (a relation with the target's current rows), each column cast to `types`.
    pub fn net_sql(
        &self,
        txns: &str,
        lake: Option<&str>,
        types: &HashMap<String, String>,
    ) -> String {
        let t = format!("temp.main.{}", ident(&self.table));
        let cut = self.last_truncate.map_or(-1, |(s, _)| s);
        let pk = self
            .pk
            .iter()
            .map(|k| ident(k))
            .collect::<Vec<_>>()
            .join(", ");
        let window = |part: &str| {
            format!(
                "OVER (PARTITION BY {part} ORDER BY __seq ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
            )
        };
        let seg_part = format!("{pk}, __seg");
        let mut resolved = String::new();
        for col in &self.toast {
            resolved.push_str(&format!(
                ", last_value(CASE WHEN __op <> 'D' AND NOT {} THEN {{'v': {}}} END IGNORE NULLS) {} AS {}",
                self.is_toast(col),
                ident(col),
                window(&seg_part),
                ident(&format!("__r_{col}"))
            ));
        }
        let cast = |col: &StageCol, v: &str| {
            let target = types
                .get(&col.name)
                .cloned()
                .unwrap_or_else(|| crate::types::land_type(col));
            crate::types::cast(
                v,
                col,
                &target,
                &format!("column `{}` of `{}`", col.name, self.label),
            )
        };
        let join_lake = lake.filter(|_| !self.toast.is_empty());
        let exprs: Vec<String> = self
            .cols
            .iter()
            .map(|col| {
                let q = ident(&col.name);
                if self.pk.contains(&col.name) {
                    format!("{} AS {q}", cast(col, &format!("r.{q}")))
                } else if self.toast.contains(&col.name) {
                    let r = ident(&format!("__r_{}", col.name));
                    let fallback = match join_lake {
                        Some(_) => format!("t.{q}"),
                        None => "NULL".into(),
                    };
                    format!(
                        "CASE WHEN r.__op = 'D' THEN NULL WHEN r.{r} IS NULL THEN {fallback} ELSE {} END AS {q}",
                        cast(col, &format!("struct_extract(r.{r}, 'v')"))
                    )
                } else {
                    format!(
                        "CASE WHEN r.__op = 'D' THEN NULL ELSE {} END AS {q}",
                        cast(col, &format!("r.{q}"))
                    )
                }
            })
            .collect();
        let lake_join = match join_lake {
            Some(l) => {
                let on = self
                    .pk
                    .iter()
                    .enumerate()
                    .map(|(i, k)| {
                        let col = self.col(k).expect("pk column staged");
                        format!(
                            "t.{} = {}",
                            ident(k),
                            cast(col, &format!("json_extract_string(r.__segfb, '$[{i}]')"))
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" AND ");
                format!("LEFT JOIN {l} t ON {on}")
            }
            None => String::new(),
        };
        format!(
            "WITH s AS (SELECT * FROM {t} WHERE __seq > {cut} AND __op <> 'T'),
             g AS (SELECT *, sum(CASE WHEN __op = 'I' OR __from IS NOT NULL THEN 1 ELSE 0 END)
                     OVER (PARTITION BY {pk} ORDER BY __seq ROWS UNBOUNDED PRECEDING) AS __seg FROM s),
             r AS (SELECT *, row_number() OVER (PARTITION BY {pk} ORDER BY __seq DESC) AS __rn,
                     first_value(__fb) {segw} AS __segfb{resolved} FROM g)
             SELECT r.__op AS __op, x.lsn AS __lsn, {exprs}
             FROM r JOIN {txns} x ON x.txn = r.__txn {lake_join}
             WHERE r.__rn = 1",
            segw = window(&seg_part),
            exprs = exprs.join(", "),
        )
    }
}

/// COPY text format: tab-separated, `\N` for NULL, backslash escapes.
pub(crate) fn copy_fields(line: &[u8]) -> Result<Vec<Option<String>>> {
    let bad = || atlatl_core::AtlatlError::internal("COPY row is not valid UTF-8 text");
    line.split(|&b| b == b'\t')
        .map(|f| {
            if f == b"\\N" {
                return Ok(None);
            }
            let mut out = Vec::with_capacity(f.len());
            let mut i = 0;
            while i < f.len() {
                let b = f[i];
                i += 1;
                if b != b'\\' || i >= f.len() {
                    out.push(b);
                    continue;
                }
                let e = f[i];
                i += 1;
                match e {
                    b'b' => out.push(8),
                    b'f' => out.push(12),
                    b'n' => out.push(b'\n'),
                    b'r' => out.push(b'\r'),
                    b't' => out.push(b'\t'),
                    b'v' => out.push(11),
                    b'0'..=b'7' => {
                        let mut v = (e - b'0') as u32;
                        for _ in 0..2 {
                            match f.get(i) {
                                Some(d @ b'0'..=b'7') => {
                                    v = v * 8 + (d - b'0') as u32;
                                    i += 1;
                                }
                                _ => break,
                            }
                        }
                        out.push(v as u8);
                    }
                    b'x' => {
                        let mut v = 0u32;
                        let mut n = 0;
                        while n < 2 {
                            match f.get(i).and_then(|d| (*d as char).to_digit(16)) {
                                Some(d) => {
                                    v = v * 16 + d;
                                    i += 1;
                                    n += 1;
                                }
                                None => break,
                            }
                        }
                        if n == 0 {
                            out.push(b'x');
                        } else {
                            out.push(v as u8);
                        }
                    }
                    other => out.push(other),
                }
            }
            utf8(out).map(Some).ok_or_else(bad)
        })
        .collect()
}

/// A lone non-ASCII byte is a `"char"` that Postgres before 15 sends as is; it's written as the
/// octal escape later versions send.
pub(crate) fn utf8(b: Vec<u8>) -> Option<String> {
    String::from_utf8(b).map_or_else(
        |e| match e.as_bytes() {
            [x] if *x >= 0x80 => Some(format!("\\{x:03o}")),
            _ => None,
        },
        Some,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_text_format() {
        assert_eq!(
            copy_fields(b"1\t\\N\ta\\tb\\\\c\\nd\t\\101\\x42").expect("row"),
            vec![
                Some("1".into()),
                None,
                Some("a\tb\\c\nd".into()),
                Some("AB".into())
            ]
        );
        assert_eq!(copy_fields(b"").expect("row"), vec![Some(String::new())]);
        assert_eq!(
            copy_fields(b"\xe9\t\xc3\xa9").expect("row"),
            vec![Some("\\351".into()), Some("é".into())]
        );
        assert!(copy_fields(b"\xe9\xe9").is_err());
    }

    fn col(n: &str, oid: u32) -> StageCol {
        StageCol {
            name: n.into(),
            oid,
            typmod: -1,
            land: Default::default(),
        }
    }

    fn row(seq: i64, op: Op, id: &str, v: Option<&str>, toast: &[&str]) -> Row {
        Row {
            seq,
            txn: 1,
            op,
            from: None,
            fb: Some(format!("[\"{id}\"]")),
            toast: toast.iter().map(|s| s.to_string()).collect(),
            values: vec![
                ("id".into(), Some(id.into())),
                ("v".into(), v.map(Into::into)),
                (
                    "big".into(),
                    (!toast.contains(&"big")).then(|| format!("b{seq}")),
                ),
            ],
        }
    }

    fn net(c: &Connection, s: &Stage) -> Vec<(String, i32, Option<String>, Option<String>)> {
        c.execute_batch(
            "CREATE OR REPLACE TEMP TABLE txns AS SELECT 1::BIGINT AS txn, 99::BIGINT AS lsn;
             CREATE OR REPLACE TABLE lake AS SELECT 5 AS id, 'lake-v' AS v, 'lake-big' AS big",
        )
        .expect("setup");
        let sql = s.net_sql("temp.main.txns", Some("lake"), &HashMap::new());
        let mut st = c
            .prepare(&format!("SELECT __op, id, v, big FROM ({sql}) ORDER BY id"))
            .expect("prepare");
        st.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .expect("query")
            .collect::<std::result::Result<_, _>>()
            .expect("rows")
    }

    #[test]
    fn netting_keeps_the_last_op_and_carries_toast() {
        let c = Connection::open_in_memory().expect("db");
        let mut s = Stage::create(
            &c,
            "stg",
            "public.t",
            vec![col("id", 23), col("v", 25), col("big", 25)],
            vec!["id".into()],
        )
        .expect("stage");
        s.push(&c, row(1, Op::Insert, "1", Some("a"), &[]))
            .expect("r");
        s.push(&c, row(2, Op::Update, "1", Some("b"), &["big"]))
            .expect("r");
        s.push(&c, row(3, Op::Delete, "2", None, &[])).expect("r");
        s.push(&c, row(4, Op::Insert, "2", Some("c"), &[]))
            .expect("r");
        s.push(&c, row(5, Op::Update, "5", Some("d"), &["big"]))
            .expect("r");
        s.push(&c, row(6, Op::Insert, "3", Some("x"), &[]))
            .expect("r");
        s.push(&c, row(7, Op::Delete, "3", None, &[])).expect("r");
        s.flush(&c).expect("flush");
        assert_eq!(
            net(&c, &s),
            vec![
                ("U".into(), 1, Some("b".into()), Some("b1".into())),
                ("I".into(), 2, Some("c".into()), Some("b4".into())),
                ("D".into(), 3, None, None),
                ("U".into(), 5, Some("d".into()), Some("lake-big".into())),
            ]
        );

        s.push(
            &c,
            Row {
                seq: 8,
                txn: 1,
                op: Op::Truncate,
                from: None,
                fb: None,
                toast: vec![],
                values: vec![],
            },
        )
        .expect("t");
        s.push(&c, row(9, Op::Insert, "7", Some("z"), &[]))
            .expect("r");
        s.flush(&c).expect("flush");
        assert_eq!(
            net(&c, &s),
            vec![("I".into(), 7, Some("z".into()), Some("b9".into()))]
        );
    }

    #[test]
    fn key_changes_carry_toast_from_the_old_key() {
        let c = Connection::open_in_memory().expect("db");
        let mut s = Stage::create(
            &c,
            "stg",
            "public.t",
            vec![col("id", 23), col("v", 25), col("big", 25)],
            vec!["id".into()],
        )
        .expect("stage");
        // 1 is updated (big changes), then renamed to 10 with big unchanged; 5 (lake only)
        // is renamed to 50 and then to 500.
        s.push(&c, row(1, Op::Update, "1", Some("a"), &[]))
            .expect("r");
        s.push(&c, row(2, Op::Delete, "1", None, &[])).expect("r");
        let mut r = row(3, Op::Update, "10", Some("a2"), &["big"]);
        r.from = Some("[\"1\"]".into());
        r.fb = Some("[\"1\"]".into());
        s.push(&c, r).expect("r");
        s.keychanges.push(KeyChange {
            seq: 3,
            txn: 1,
            old_key: "[\"1\"]".into(),
        });
        s.push(&c, row(4, Op::Delete, "5", None, &[])).expect("r");
        let mut r = row(5, Op::Update, "50", Some("m"), &["big"]);
        r.from = Some("[\"5\"]".into());
        r.fb = Some("[\"5\"]".into());
        s.push(&c, r).expect("r");
        s.keychanges.push(KeyChange {
            seq: 5,
            txn: 1,
            old_key: "[\"5\"]".into(),
        });
        s.push(&c, row(6, Op::Delete, "50", None, &[])).expect("r");
        let mut r = row(7, Op::Update, "500", Some("n"), &["big"]);
        r.from = Some("[\"50\"]".into());
        r.fb = Some("[\"50\"]".into());
        s.push(&c, r).expect("r");
        s.keychanges.push(KeyChange {
            seq: 7,
            txn: 1,
            old_key: "[\"50\"]".into(),
        });
        s.resolve_key_changes(&c).expect("resolve");
        assert_eq!(
            net(&c, &s),
            vec![
                ("D".into(), 1, None, None),
                ("D".into(), 5, None, None),
                ("U".into(), 10, Some("a2".into()), Some("b1".into())),
                ("D".into(), 50, None, None),
                ("U".into(), 500, Some("n".into()), Some("lake-big".into())),
            ]
        );
    }

    #[test]
    fn discarding_an_open_transaction() {
        let c = Connection::open_in_memory().expect("db");
        let mut s = Stage::create(
            &c,
            "stg",
            "public.t",
            vec![col("id", 23)],
            vec!["id".into()],
        )
        .expect("stage");
        for seq in 1..=3 {
            let mut r = row(seq, Op::Insert, &seq.to_string(), None, &[]);
            r.txn = seq;
            r.values.truncate(1);
            s.push(&c, r).expect("r");
        }
        s.flush(&c).expect("flush");
        let mut r = row(4, Op::Insert, "4", None, &[]);
        r.txn = 3;
        r.values.truncate(1);
        s.push(&c, r).expect("r");
        s.discard_txn(&c, 3).expect("discard");
        s.flush(&c).expect("flush");
        assert_eq!(s.rows, 2);
        let n: i64 = c
            .query_row("SELECT count(*) FROM temp.main.stg", [], |r| r.get(0))
            .expect("count");
        assert_eq!(n, 2);
    }

    #[test]
    fn committed_streams_move_into_commit_order() {
        const P: i64 = 1 << 40;
        let c = Connection::open_in_memory().expect("db");
        let mut s = Stage::create(
            &c,
            "stg",
            "public.t",
            vec![col("id", 23), col("v", 25)],
            vec!["id".into()],
        )
        .expect("stage");
        let push = |s: &mut Stage, seq: i64, txn: i64, op: Op, id: &str, v: &str| {
            let mut r = row(seq, op, id, Some(v), &[]);
            r.txn = txn;
            r.values.truncate(2);
            s.push(&c, r).expect("push");
        };
        // Streamed txn 2 (subtransaction 3 aborted) writes id 1 before txn 5 commits, but
        // commits after it.
        push(&mut s, P + 1, 2, Op::Insert, "1", "streamed");
        push(&mut s, P + 2, 3, Op::Insert, "9", "aborted");
        push(&mut s, 1, 5, Op::Insert, "1", "regular");
        push(&mut s, P + 3, 4, Op::Truncate, "", "");
        push(&mut s, P + 4, 2, Op::Update, "1", "last");
        s.discard_txn(&c, 3).expect("abort sub");
        assert_eq!(s.last_truncate, Some((P + 3, 4)));
        let moved = s.commit_streamed(&c, &[2, 4], 2, 1).expect("commit");
        assert_eq!(moved, 3);
        assert_eq!(s.last_truncate, Some((3, 2)));
        let mut st = c
            .prepare("SELECT __seq, __txn, __op, coalesce(v, '') FROM temp.main.stg ORDER BY __seq")
            .expect("prepare");
        let rows: Vec<(i64, i64, String, String)> = st
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .expect("query")
            .collect::<std::result::Result<_, _>>()
            .expect("rows");
        assert_eq!(
            rows,
            [
                (1, 5, "I".into(), "regular".into()),
                (2, 2, "I".into(), "streamed".into()),
                (3, 2, "T".into(), "".into()),
                (4, 2, "U".into(), "last".into()),
            ]
        );
    }
}
