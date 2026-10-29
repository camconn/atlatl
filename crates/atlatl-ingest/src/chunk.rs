//! Chunk relations (04 §7). Each chunk is a predicate over the pinned source, merged directly
//! inside the replication's transaction (never staged into a TEMP table there; spike S3 §4).

use atlatl_core::config::model::{
    CAST_NULLED, ChunkBy, ChunkConfig, Duplicates, Guard, NullKeys, OnEmpty,
};
use atlatl_core::error::{AtlatlError, ErrorClass, Result};
use duckdb::Connection;

use crate::drift::{self, Adoption};
use crate::sql::{db, ident, literal};
use crate::staging::{self, quoted_relation};
use crate::validate;

/// `WHERE` predicates, one per chunk; together they cover every row exactly once.
pub fn predicates(c: &Connection, source: &str, cfg: &ChunkConfig) -> Result<Vec<String>> {
    match cfg.by {
        ChunkBy::Hash => {
            let cols = cfg.columns.as_deref().unwrap_or_default();
            let n = cfg.buckets.unwrap_or(0);
            if cols.is_empty() || n < 2 {
                return Err(AtlatlError::config(
                    "`chunk: {by: hash}` needs `columns` and `buckets` ≥ 2",
                ));
            }
            let h = format!(
                "hash({})",
                cols.iter().map(|c| ident(c)).collect::<Vec<_>>().join(", ")
            );
            Ok((0..n).map(|k| format!("{h} % {n} = {k}")).collect())
        }
        ChunkBy::Range => {
            let (Some(col), Some(step)) = (&cfg.column, &cfg.step) else {
                return Err(AtlatlError::config(
                    "`chunk: {by: range}` needs `column` and `step`",
                ));
            };
            range_predicates(c, source, col, step)
        }
        ChunkBy::Files => Err(AtlatlError::config(
            "`chunk: {by: files}` groups source files; use `file_groups`",
        )),
    }
}

/// A step as SQL: `1y` → `INTERVAL 1 YEAR`; a bare number stays numeric.
pub fn step_sql(step: &str) -> Result<String> {
    let s = step.trim();
    if s.parse::<f64>().is_ok() {
        return Ok(s.to_owned());
    }
    let split = s
        .find(|ch: char| !ch.is_ascii_digit())
        .ok_or_else(|| AtlatlError::config(format!("invalid chunk step `{step}`")))?;
    let (n, unit) = s.split_at(split);
    let unit = match unit {
        "h" => "HOUR",
        "d" => "DAY",
        "w" => "WEEK",
        "mo" => "MONTH",
        "q" => "QUARTER",
        "y" => "YEAR",
        u => {
            return Err(AtlatlError::config(format!(
                "invalid chunk step unit `{u}` (h, d, w, mo, q, y)"
            )));
        }
    };
    if n.is_empty() {
        return Err(AtlatlError::config(format!("invalid chunk step `{step}`")));
    }
    Ok(format!("INTERVAL {n} {unit}"))
}

fn range_predicates(c: &Connection, source: &str, column: &str, step: &str) -> Result<Vec<String>> {
    let col = ident(column);
    let rel = quoted_relation(source);
    let step = step_sql(step)?;
    let mut s = c
        .prepare(&format!(
            "WITH b AS (SELECT min({col}) AS lo, max({col}) AS hi FROM {rel})
             SELECT g::VARCHAR, (g + {step})::VARCHAR FROM b, generate_series(b.lo, b.hi, {step}) t(g) WHERE b.lo IS NOT NULL"
        ))
        .map_err(db)?;
    let bounds: Vec<(String, String)> = s
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .map_err(db)?
        .collect::<Result<_, _>>()
        .map_err(db)?;
    let ty: String = c
        .query_row(
            &format!("SELECT typeof({col}) FROM {rel} LIMIT 1"),
            [],
            |r| r.get(0),
        )
        .unwrap_or_else(|_| "VARCHAR".into());
    let mut out: Vec<String> = bounds
        .iter()
        .map(|(a, b)| {
            format!(
                "{col} >= {}::{ty} AND {col} < {}::{ty}",
                literal(a),
                literal(b)
            )
        })
        .collect();
    out.push(format!("{col} IS NULL"));
    Ok(out)
}

/// Groups `(path, bytes)` in order into groups of at most `target` bytes (a file larger than
/// `target` is a group of its own).
pub fn file_groups(files: &[(String, u64)], target: u64) -> Vec<Vec<String>> {
    let mut out: Vec<Vec<String>> = Vec::new();
    let mut size = 0u64;
    for (p, n) in files {
        if out.is_empty() || (size + n > target && !out.last().is_some_and(Vec::is_empty)) {
            out.push(Vec::new());
            size = 0;
        }
        out.last_mut().expect("group").push(p.clone());
        size += n;
    }
    out
}

/// `(SELECT * FROM <source> WHERE <pred>)`, the relation a chunk merges from.
pub fn relation(source: &str, predicate: &str) -> String {
    format!(
        "(SELECT * FROM {} WHERE {predicate})",
        quoted_relation(source)
    )
}

/// A chunked apply's invariants (04 §7, §9). Nothing is staged, so they run as queries over the
/// pinned source relation before the commit, covering every chunk at once: keys unique across
/// chunks, not just within one.
#[derive(Clone, Debug)]
pub struct Unstaged<'a> {
    pub primary_key: &'a [String],
    pub null_keys: NullKeys,
    pub duplicates: Duplicates,
    pub set_null: bool,
    /// From a manifest; a different count is `decode`.
    pub expected_rows: Option<u64>,
    /// Full refresh: `on_empty`, `guard` and the target's row count (if it exists).
    pub full_refresh: Option<(OnEmpty, Option<&'a Guard>, Option<u64>)>,
    pub update_key: Option<&'a str>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UnstagedCheck {
    /// Rows the chunks will apply (after `null_keys: skip`).
    pub rows: u64,
    /// `on_empty: skip`.
    pub skip: bool,
    /// New `update_key` watermark (value, DuckDB type).
    pub max: Option<(String, String)>,
    /// `null_keys: skip`: a predicate every chunk relation must be filtered by.
    pub key_filter: Option<String>,
}

impl UnstagedCheck {
    /// Chunk relations with [`UnstagedCheck::key_filter`] applied.
    pub fn filter(&self, rels: Vec<String>) -> Vec<String> {
        match &self.key_filter {
            Some(f) => rels.iter().map(|r| relation(r, f)).collect(),
            None => rels,
        }
    }
}

impl Unstaged<'_> {
    pub fn check(&self, c: &Connection, rel: &str, a: &Adoption) -> Result<UnstagedCheck> {
        if matches!(self.duplicates, Duplicates::First | Duplicates::Last)
            && !self.primary_key.is_empty()
        {
            return Err(AtlatlError::config(
                "`duplicates: first|last` needs a staged set; it can't be combined with `chunk`",
            ));
        }
        if self.set_null
            && a.columns.iter().any(|col| {
                col.cast
                    || col
                        .source
                        .as_deref()
                        .is_some_and(|n| n.starts_with(CAST_NULLED))
            })
        {
            return Err(AtlatlError::config(
                "`on_cast_error.set_null` needs a staged set; it can't be combined with `chunk`",
            ));
        }
        let n = staging::row_count(c, rel)?;
        if let Some(want) = self.expected_rows
            && want != n
        {
            return Err(AtlatlError::new(
                ErrorClass::Decode,
                format!("the source holds {n} rows but the manifest lists {want}"),
            ));
        }
        let mut out = UnstagedCheck {
            rows: n,
            ..Default::default()
        };
        let mut rel = rel.to_owned();
        if !self.primary_key.is_empty() && self.null_keys != NullKeys::NullSafe {
            let any_null = self
                .primary_key
                .iter()
                .map(|k| format!("{} IS NULL", ident(k)))
                .collect::<Vec<_>>()
                .join(" OR ");
            let nulls = staging::row_count(c, &relation(&rel, &any_null))?;
            if nulls > 0 {
                if self.null_keys != NullKeys::Skip {
                    validate::null_keys(c, &rel, self.primary_key, NullKeys::Fail)?;
                }
                let f = format!("NOT ({any_null})");
                rel = relation(&rel, &f);
                out.rows -= nulls;
                out.key_filter = Some(f);
            }
        }
        validate::duplicates(c, &rel, self.primary_key, Duplicates::Fail, &[])?;
        drift::check_casts(c, &rel, a, false)?;
        if let Some((on_empty, guard, target_rows)) = self.full_refresh {
            out.skip = validate::empty_and_guard(out.rows, target_rows, on_empty, guard)?
                == validate::EmptyOutcome::Skip;
        }
        if let Some(uk) = self.update_key {
            out.max = crate::modes::staged_max(c, &rel, uk)?;
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(by: ChunkBy) -> ChunkConfig {
        ChunkConfig {
            by,
            size: None,
            columns: None,
            buckets: None,
            column: None,
            step: None,
        }
    }

    fn covered(c: &Connection, preds: &[String]) -> (i64, i64) {
        let total: i64 = c
            .query_row("SELECT count(*) FROM src", [], |r| r.get(0))
            .expect("n");
        let mut sum = 0i64;
        for p in preds {
            sum += c
                .query_row(
                    &format!("SELECT count(*) FROM {}", relation("src", p)),
                    [],
                    |r| r.get::<_, i64>(0),
                )
                .expect("chunk");
        }
        (total, sum)
    }

    #[test]
    fn hash_chunks_partition_rows() {
        let c = Connection::open_in_memory().expect("db");
        c.execute_batch("CREATE TABLE src AS SELECT range AS id, range % 7 AS g FROM range(1000)")
            .expect("src");
        let p = predicates(
            &c,
            "src",
            &ChunkConfig {
                columns: Some(vec!["id".into(), "g".into()]),
                buckets: Some(8),
                ..cfg(ChunkBy::Hash)
            },
        )
        .expect("hash");
        assert_eq!(p.len(), 8);
        let (t, s) = covered(&c, &p);
        assert_eq!(t, s, "every row in exactly one chunk");
        assert!(
            predicates(
                &c,
                "src",
                &ChunkConfig {
                    columns: Some(vec!["id".into()]),
                    buckets: Some(1),
                    ..cfg(ChunkBy::Hash)
                }
            )
            .is_err()
        );
    }

    #[test]
    fn range_chunks_cover_nulls_and_ends() {
        let c = Connection::open_in_memory().expect("db");
        c.execute_batch(
            "CREATE TABLE src AS SELECT DATE '2015-03-01' + INTERVAL (range * 17) DAY AS d FROM range(200)
             UNION ALL SELECT NULL",
        )
        .expect("src");
        let p = predicates(
            &c,
            "src",
            &ChunkConfig {
                column: Some("d".into()),
                step: Some("1y".into()),
                ..cfg(ChunkBy::Range)
            },
        )
        .expect("range");
        assert!(p.len() >= 10 && p.last().is_some_and(|x| x.ends_with("IS NULL")));
        let (t, s) = covered(&c, &p);
        assert_eq!(t, s);
        c.execute_batch("CREATE OR REPLACE TABLE src AS SELECT range AS n FROM range(95)")
            .expect("nums");
        let p = predicates(
            &c,
            "src",
            &ChunkConfig {
                column: Some("n".into()),
                step: Some("10".into()),
                ..cfg(ChunkBy::Range)
            },
        )
        .expect("numeric");
        assert_eq!(p.len(), 11);
        let (t, s) = covered(&c, &p);
        assert_eq!(t, s);
    }

    #[test]
    fn unstaged_invariants_cover_every_chunk() {
        let c = Connection::open_in_memory().expect("db");
        c.execute_batch(
            "CREATE TABLE src AS SELECT * FROM (VALUES (1, 'a', DATE '2025-01-01'), (2, 'b', DATE '2025-01-03'),
               (NULL, 'n', DATE '2025-02-01'), (2, 'b2', DATE '2025-01-02')) t(id, v, d)",
        )
        .expect("src");
        let pk = vec!["id".to_owned()];
        let a = Adoption::default();
        let base = Unstaged {
            primary_key: &pk,
            null_keys: NullKeys::Fail,
            duplicates: Duplicates::Fail,
            set_null: false,
            expected_rows: None,
            full_refresh: None,
            update_key: Some("d"),
        };
        let code = |u: &Unstaged, rel: &str| u.check(&c, rel, &a).expect_err("invalid").code;
        assert_eq!(code(&base, "src").as_deref(), Some("null_primary_key"));
        let skip = Unstaged {
            null_keys: NullKeys::Skip,
            ..base.clone()
        };
        assert_eq!(
            code(&skip, "src").as_deref(),
            Some("duplicate_primary_key"),
            "id 2 in two chunks"
        );
        let e = Unstaged {
            duplicates: Duplicates::Last,
            ..base.clone()
        }
        .check(&c, "src", &a)
        .expect_err("last");
        assert_eq!(e.class, ErrorClass::Config);
        let flagged = Adoption {
            columns: vec![crate::drift::ColumnPlan {
                target: format!("{CAST_NULLED}v"),
                source: Some(format!("{CAST_NULLED}v")),
                from: "BOOLEAN".into(),
                kind: "BOOLEAN".into(),
                cast: false,
            }],
            ..Default::default()
        };
        let set_null = Unstaged {
            set_null: true,
            ..base.clone()
        };
        let e = set_null.check(&c, "src", &flagged).expect_err("flags");
        assert_eq!(e.class, ErrorClass::Config, "{e}");

        c.execute_batch("DELETE FROM src WHERE v = 'b2'")
            .expect("dedupe");
        let ok = skip.check(&c, "src", &a).expect("valid");
        assert_eq!((ok.rows, ok.skip), (2, false));
        assert_eq!(
            ok.max,
            Some(("2025-01-03".into(), "DATE".into())),
            "skipped rows don't move the watermark"
        );
        let rels = ok.filter(vec!["src".into()]);
        assert_eq!(staging::row_count(&c, &rels[0]).expect("n"), 2);
        let e = Unstaged {
            expected_rows: Some(9),
            ..skip.clone()
        }
        .check(&c, "src", &a)
        .expect_err("manifest");
        assert_eq!(e.class, ErrorClass::Decode);

        let guard = Guard {
            min_rows: Some(5),
            max_shrink: None,
        };
        let fr = Unstaged {
            full_refresh: Some((OnEmpty::Mirror, Some(&guard), Some(10))),
            ..skip.clone()
        };
        assert_eq!(code(&fr, "src").as_deref(), Some("guard_min_rows"));
        let empty = "(SELECT * FROM src WHERE false)";
        let fr = Unstaged {
            full_refresh: Some((OnEmpty::Skip, None, Some(10))),
            ..skip
        };
        assert!(fr.check(&c, empty, &a).expect("empty").skip);
    }

    #[test]
    fn steps_and_groups() {
        assert_eq!(step_sql("1y").expect("y"), "INTERVAL 1 YEAR");
        assert_eq!(step_sql("3mo").expect("mo"), "INTERVAL 3 MONTH");
        assert!(step_sql("1fortnight").is_err());
        assert!(step_sql("mo").is_err());
        let files = [
            ("a".to_owned(), 5),
            ("b".to_owned(), 5),
            ("c".to_owned(), 20),
            ("d".to_owned(), 1),
        ];
        assert_eq!(
            file_groups(&files, 10),
            [vec!["a", "b"], vec!["c"], vec!["d"]]
        );
        assert!(file_groups(&[], 10).is_empty());
    }
}
