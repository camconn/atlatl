//! Mode SQL (04 §4), run inside the replication's commit transaction. DuckLake has no MERGE
//! `RETURNING`, so counts come from queries over the same relation before each change.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

use atlatl_core::error::Result;
use duckdb::Connection;
use serde::Serialize;

use crate::sql::{db, ident};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Counts {
    pub inserted: u64,
    pub updated: u64,
    pub deleted: u64,
    pub unchanged: u64,
}

impl Counts {
    pub fn changed(&self) -> bool {
        self.inserted + self.updated + self.deleted > 0
    }

    pub fn add(&mut self, o: Counts) {
        self.inserted += o.inserted;
        self.updated += o.updated;
        self.deleted += o.deleted;
        self.unchanged += o.unchanged;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// Target ends up exactly the source; `with_pk` makes it a diff-replace.
    FullRefresh,
    /// PK upsert with the change guard; without a PK, append.
    Incremental,
    /// Append (with `metadata.loaded_at`).
    Snapshot,
}

/// One stream's apply. `source` is a relation: a staging table name or `(SELECT …)` over a
/// pinned chunk (04 §7). `columns` pairs target columns with expressions over `source`.
#[derive(Clone, Copy, Debug)]
pub struct Apply<'a> {
    pub kind: Kind,
    pub target: &'a str,
    pub source: &'a str,
    pub columns: &'a [(String, String)],
    pub primary_key: &'a [String],
    /// `null_keys: null_safe` joins with `IS NOT DISTINCT FROM`.
    pub null_safe: bool,
}

fn scalar(c: &Connection, sql: &str) -> Result<u64> {
    let n: i64 = c.query_row(sql, [], |r| r.get(0)).map_err(db)?;
    Ok(n as u64)
}

impl Apply<'_> {
    fn src(&self) -> String {
        format!(
            "(SELECT {} FROM {})",
            self.columns
                .iter()
                .map(|(t, e)| format!("{e} AS {}", ident(t)))
                .collect::<Vec<_>>()
                .join(", "),
            crate::staging::quoted_relation(self.source)
        )
    }

    fn on(&self, a: &str, b: &str) -> String {
        let op = if self.null_safe {
            "IS NOT DISTINCT FROM"
        } else {
            "="
        };
        self.primary_key
            .iter()
            .map(|k| format!("{a}.{k} {op} {b}.{k}", k = ident(k)))
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    fn payload(&self) -> Vec<String> {
        self.columns
            .iter()
            .map(|(t, _)| t.clone())
            .filter(|t| !self.primary_key.iter().any(|k| k == t))
            .collect()
    }

    /// The change guard over the payload minus `unguarded` (run-varying metadata, 04 §8).
    fn changed(&self, t: &str, s: &str, unguarded: &[String]) -> String {
        let p = self.guarded(unguarded);
        if p.is_empty() {
            return "false".into();
        }
        let l = p
            .iter()
            .map(|c| format!("{t}.{}", ident(c)))
            .collect::<Vec<_>>()
            .join(", ");
        let r = p
            .iter()
            .map(|c| format!("{s}.{}", ident(c)))
            .collect::<Vec<_>>()
            .join(", ");
        format!("({l}) IS DISTINCT FROM ({r})")
    }

    fn guarded(&self, unguarded: &[String]) -> Vec<String> {
        self.payload()
            .into_iter()
            .filter(|c| !unguarded.contains(c))
            .collect()
    }

    fn cols(&self) -> String {
        self.columns
            .iter()
            .map(|(t, _)| ident(t))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn insert_all(&self, c: &Connection) -> Result<u64> {
        let n = scalar(c, &format!("SELECT count(*) FROM {}", self.src()))?;
        if n > 0 {
            c.execute_batch(&format!(
                "INSERT INTO {} ({}) SELECT {} FROM {}",
                self.target,
                self.cols(),
                self.cols(),
                self.src()
            ))
            .map_err(db)?;
        }
        Ok(n)
    }

    fn merge_counts(&self, c: &Connection, unguarded: &[String]) -> Result<Counts> {
        let src = self.src();
        let matched = scalar(
            c,
            &format!(
                "SELECT count(*) FROM {src} s SEMI JOIN {} t ON {}",
                self.target,
                self.on("t", "s")
            ),
        )?;
        let updated = scalar(
            c,
            &format!(
                "SELECT count(*) FROM {src} s JOIN {} t ON {} WHERE {}",
                self.target,
                self.on("t", "s"),
                self.changed("t", "s", unguarded)
            ),
        )?;
        let total = scalar(c, &format!("SELECT count(*) FROM {src}"))?;
        Ok(Counts {
            inserted: total - matched,
            updated,
            deleted: 0,
            unchanged: matched - updated,
        })
    }

    fn merge(&self, c: &Connection, unguarded: &[String]) -> Result<Counts> {
        let src = self.src();
        let counts = self.merge_counts(c, unguarded)?;
        if counts.inserted + counts.updated == 0 {
            return Ok(counts);
        }
        let set = self
            .payload()
            .iter()
            .map(|c| format!("{c} = s.{c}", c = ident(c)))
            .collect::<Vec<_>>()
            .join(", ");
        let when_matched = if set.is_empty() {
            String::new()
        } else {
            format!(
                "WHEN MATCHED AND {} THEN UPDATE SET {set}",
                self.changed("t", "s", unguarded)
            )
        };
        let vals = self
            .columns
            .iter()
            .map(|(t, _)| format!("s.{}", ident(t)))
            .collect::<Vec<_>>()
            .join(", ");
        c.execute_batch(&format!(
            "MERGE INTO {tgt} AS t USING {src} AS s ON {on} {when_matched} WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})",
            tgt = self.target,
            on = self.on("t", "s"),
            cols = self.cols(),
        ))
        .map_err(db)?;
        Ok(counts)
    }

    fn missing_pred(&self, keys: &str) -> String {
        format!(
            "NOT EXISTS (SELECT 1 FROM {keys} s WHERE {})",
            self.on("t", "s")
        )
    }

    /// Deletes target keys absent from `keys` (a relation with the PK columns).
    fn delete_missing(&self, c: &Connection, keys: &str) -> Result<u64> {
        let pred = self.missing_pred(keys);
        let n = scalar(
            c,
            &format!("SELECT count(*) FROM {} t WHERE {pred}", self.target),
        )?;
        if n > 0 {
            c.execute_batch(&format!("DELETE FROM {} t WHERE {pred}", self.target))
                .map_err(db)?;
        }
        Ok(n)
    }

    /// Runs the change. Nothing is written when nothing changed, so an unchanged re-sync
    /// commits no snapshot (spike S3 §1).
    pub fn run(&self, c: &Connection) -> Result<Counts> {
        self.run_guarded(c, &[])
    }

    /// [`Apply::run`] with `unguarded` payload columns left out of the change guard: they are
    /// written on insert and on a row that changed otherwise, but don't make a row changed.
    pub fn run_guarded(&self, c: &Connection, unguarded: &[String]) -> Result<Counts> {
        match (self.kind, self.primary_key.is_empty()) {
            (Kind::FullRefresh, true) => {
                let deleted = scalar(c, &format!("SELECT count(*) FROM {}", self.target))?;
                if deleted > 0 {
                    c.execute_batch(&format!("DELETE FROM {}", self.target))
                        .map_err(db)?;
                }
                let inserted = self.insert_all(c)?;
                Ok(Counts {
                    inserted,
                    deleted,
                    ..Default::default()
                })
            }
            (Kind::FullRefresh, false) => {
                let deleted = self.delete_missing(c, &self.src())?;
                let mut m = self.merge(c, unguarded)?;
                m.deleted = deleted;
                Ok(m)
            }
            (Kind::Incremental, false) => self.merge(c, unguarded),
            (Kind::Incremental, true) | (Kind::Snapshot, _) => Ok(Counts {
                inserted: self.insert_all(c)?,
                ..Default::default()
            }),
        }
    }

    /// Chunked full-refresh with a PK: upsert this chunk and remember its keys in `seen` (a TEMP
    /// table created before the transaction); call [`Apply::finish_chunked`] once at the end.
    pub fn run_chunk(&self, c: &Connection, seen: &str) -> Result<Counts> {
        self.run_chunk_guarded(c, seen, &[])
    }

    pub fn run_chunk_guarded(
        &self,
        c: &Connection,
        seen: &str,
        unguarded: &[String],
    ) -> Result<Counts> {
        let keys = self
            .primary_key
            .iter()
            .map(|k| ident(k))
            .collect::<Vec<_>>()
            .join(", ");
        c.execute_batch(&format!(
            "INSERT INTO {} SELECT {keys} FROM {}",
            ident(seen),
            self.src()
        ))
        .map_err(db)?;
        self.merge(c, unguarded)
    }

    pub fn finish_chunked(&self, c: &Connection, seen: &str) -> Result<u64> {
        self.delete_missing(c, &ident(seen))
    }

    /// What [`Apply::run_guarded`] would change, read-only (`atlatl run --dry-run`): counts, plus
    /// up to `limit` samples per operation, ordered by key. `chunks` (empty: `source` alone) are
    /// compared one by one, as they'd be applied; deletes against the keys of all of them.
    /// Keyless kinds have no samples (see [`Diff::keyed`]).
    pub fn diff(
        &self,
        c: &Connection,
        chunks: &[String],
        unguarded: &[String],
        limit: usize,
    ) -> Result<Diff> {
        let rels: Vec<&str> = if chunks.is_empty() {
            vec![self.source]
        } else {
            chunks.iter().map(String::as_str).collect()
        };
        let mut d = Diff {
            keyed: !self.primary_key.is_empty() && self.kind != Kind::Snapshot,
            ..Diff::default()
        };
        let staged = |c: &Connection, r: &str| {
            scalar(
                c,
                &format!(
                    "SELECT count(*) FROM {}",
                    Apply { source: r, ..*self }.src()
                ),
            )
        };
        if !d.keyed {
            for r in &rels {
                d.counts.inserted += staged(c, r)?;
            }
            if self.kind == Kind::FullRefresh {
                d.counts.deleted = scalar(c, &format!("SELECT count(*) FROM {}", self.target))?;
            }
            return Ok(d);
        }
        for r in &rels {
            let a = Apply { source: r, ..*self };
            d.counts.add(a.merge_counts(c, unguarded)?);
            a.samples(c, unguarded, limit, &mut d)?;
        }
        if self.kind == Kind::FullRefresh {
            let keys = self
                .primary_key
                .iter()
                .map(|k| ident(k))
                .collect::<Vec<_>>()
                .join(", ");
            let (rel, seen) = if let [one] = rels.as_slice() {
                (
                    Apply {
                        source: one,
                        ..*self
                    }
                    .src(),
                    None,
                )
            } else {
                let seen = format!("atlatl_diff_keys_{}", KEYS_SEQ.fetch_add(1, Relaxed));
                c.execute_batch(&format!(
                    "CREATE OR REPLACE TEMP TABLE {} AS SELECT {keys} FROM {} WHERE false",
                    ident(&seen),
                    self.src()
                ))
                .map_err(db)?;
                for r in &rels {
                    c.execute_batch(&format!(
                        "INSERT INTO {} SELECT {keys} FROM {}",
                        ident(&seen),
                        Apply { source: r, ..*self }.src()
                    ))
                    .map_err(db)?;
                }
                (ident(&seen), Some(seen))
            };
            let pred = self.missing_pred(&rel);
            let r = (|| -> Result<()> {
                d.counts.deleted = scalar(
                    c,
                    &format!("SELECT count(*) FROM {} t WHERE {pred}", self.target),
                )?;
                let sql = format!(
                    "SELECT {}::VARCHAR FROM {} t WHERE {pred} ORDER BY {} LIMIT {limit}",
                    self.key_json("t"),
                    self.target,
                    self.key_order("t")
                );
                for k in strings(c, &sql)? {
                    d.samples
                        .push(serde_json::json!({"op": "delete", "key": parse(&k)}));
                }
                Ok(())
            })();
            if let Some(s) = seen {
                let _ = c.execute_batch(&format!("DROP TABLE IF EXISTS temp.{}", ident(&s)));
            }
            r?;
        }
        Ok(d)
    }

    fn key_json(&self, a: &str) -> String {
        json_object(a, self.primary_key.iter().map(String::as_str))
    }

    fn key_order(&self, a: &str) -> String {
        self.primary_key
            .iter()
            .map(|k| format!("{a}.{}", ident(k)))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Insert and update samples of one relation, topping `d` up to `limit` per operation.
    fn samples(
        &self,
        c: &Connection,
        unguarded: &[String],
        limit: usize,
        d: &mut Diff,
    ) -> Result<()> {
        let have = |d: &Diff, op: &str| d.samples.iter().filter(|s| s["op"] == op).count();
        let src = self.src();
        let n = limit.saturating_sub(have(d, "insert"));
        if n > 0 && d.counts.inserted > 0 {
            let sql = format!(
                "SELECT {}::VARCHAR FROM {src} s WHERE NOT EXISTS (SELECT 1 FROM {} t WHERE {}) ORDER BY {} LIMIT {n}",
                self.key_json("s"),
                self.target,
                self.on("t", "s"),
                self.key_order("s")
            );
            for k in strings(c, &sql)? {
                d.samples
                    .push(serde_json::json!({"op": "insert", "key": parse(&k)}));
            }
        }
        let n = limit.saturating_sub(have(d, "update"));
        let guarded = self.guarded(unguarded);
        if n > 0 && d.counts.updated > 0 && !guarded.is_empty() {
            let which = guarded
                .iter()
                .map(|g| {
                    format!(
                        "CASE WHEN t.{c} IS DISTINCT FROM s.{c} THEN {} END",
                        crate::sql::literal(g),
                        c = ident(g)
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            let cols = guarded.iter().map(String::as_str);
            let sql = format!(
                "SELECT {}::VARCHAR, concat_ws(chr(31), {which}), {}::VARCHAR, {}::VARCHAR
                 FROM {src} s JOIN {} t ON {} WHERE {} ORDER BY {} LIMIT {n}",
                self.key_json("s"),
                json_object("s", cols.clone()),
                json_object("t", cols),
                self.target,
                self.on("t", "s"),
                self.changed("t", "s", unguarded),
                self.key_order("s")
            );
            let mut st = c.prepare(&sql).map_err(db)?;
            let rows = st
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                    ))
                })
                .map_err(db)?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(db)?;
            for (k, which, s, t) in rows {
                let changed: Vec<&str> = which.split('\u{1f}').filter(|x| !x.is_empty()).collect();
                let only = |v: &str| {
                    let v = parse(v);
                    serde_json::Value::Object(
                        changed
                            .iter()
                            .map(|c| ((*c).to_owned(), v[*c].clone()))
                            .collect(),
                    )
                };
                d.samples.push(serde_json::json!({
                    "op": "update",
                    "key": parse(&k),
                    "changed": changed,
                    "staged": only(&s),
                    "target": only(&t),
                }));
            }
        }
        Ok(())
    }
}

static KEYS_SEQ: AtomicU64 = AtomicU64::new(0);

/// A dry-run comparison of staged rows against the target ([`Apply::diff`]).
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Diff {
    /// What the apply would do (`unchanged`: matched rows the guard leaves alone).
    pub counts: Counts,
    /// Rows were matched by primary key. Otherwise only counts: an append inserts every staged
    /// row, a full refresh without a key replaces the table.
    pub keyed: bool,
    /// `{op: insert|delete, key}` and `{op: update, key, changed, staged, target}`, the last two
    /// holding the changed columns only.
    pub samples: Vec<serde_json::Value>,
}

fn json_object<'a>(a: &str, cols: impl Iterator<Item = &'a str>) -> String {
    let args = cols
        .map(|c| format!("{}, {a}.{}", crate::sql::literal(c), ident(c)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("json_object({args})")
}

fn strings(c: &Connection, sql: &str) -> Result<Vec<String>> {
    let mut st = c.prepare(sql).map_err(db)?;
    st.query_map([], |r| r.get::<_, String>(0))
        .map_err(db)?
        .collect::<std::result::Result<_, _>>()
        .map_err(db)
}

fn parse(s: &str) -> serde_json::Value {
    serde_json::from_str(s).unwrap_or_else(|_| serde_json::Value::String(s.to_owned()))
}

/// `update_key` source filter (04 §4.3): `col >= stored - lookback` (inclusive) or `>`, plus
/// `OR col IS NULL` for `nulls: include`. `None` stored value = full extract.
/// `stored_type` (the watermark's DuckDB type, from [`staged_max`]) types the bound, so `lookback`
/// arithmetic binds and stats pushdown applies.
pub fn update_key_filter(
    column: &str,
    stored: Option<&str>,
    stored_type: Option<&str>,
    lookback: Option<&str>,
    inclusive: bool,
    include_nulls: bool,
) -> Option<String> {
    let col = ident(column);
    let bound = stored.map(|v| {
        let lit = match stored_type {
            Some(t) => format!("CAST({} AS {t})", crate::sql::literal(v)),
            None => crate::sql::literal(v),
        };
        let lit = match lookback {
            Some(l) => format!("({lit} - INTERVAL {})", crate::sql::literal(l)),
            None => lit,
        };
        format!("{col} {} {lit}", if inclusive { ">=" } else { ">" })
    });
    match (bound, include_nulls) {
        (Some(b), true) => Some(format!("({b} OR {col} IS NULL)")),
        (Some(b), false) => Some(b),
        (None, true) => None,
        (None, false) => Some(format!("{col} IS NOT NULL")),
    }
}

/// New watermark: `max(column)` over what was staged, as text plus its DuckDB type.
pub fn staged_max(
    c: &Connection,
    relation: &str,
    column: &str,
) -> Result<Option<(String, String)>> {
    let sql = format!(
        "SELECT max({col})::VARCHAR, typeof(max({col})) FROM {}",
        crate::staging::quoted_relation(relation),
        col = ident(column)
    );
    let (v, t): (Option<String>, String) = c
        .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?)))
        .map_err(db)?;
    Ok(v.map(|v| (v, t)))
}

#[cfg(test)]
mod tests;
