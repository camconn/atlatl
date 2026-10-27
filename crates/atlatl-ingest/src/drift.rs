//! Adoption of existing tables and table drift (04 §6). The target schema is authoritative: its
//! columns, order and types. Staged values are cast into it and checked value by value.

use atlatl_core::config::model::{MissingColumns, NewColumns, TableDrift, TypeChanges};
use atlatl_core::error::{AtlatlError, Result};
use duckdb::Connection;
use indexmap::IndexMap;

use crate::sql::{db, ident, literal};
use crate::staging::{Column, quoted_relation};
use crate::validate::MAX_SAMPLES;

#[derive(Clone, Copy, Debug)]
pub struct Policy {
    pub new_columns: NewColumns,
    pub type_changes: TypeChanges,
    pub missing_columns: MissingColumns,
}

impl From<Option<&TableDrift>> for Policy {
    fn from(d: Option<&TableDrift>) -> Self {
        Self {
            new_columns: d.and_then(|d| d.new_columns).unwrap_or(NewColumns::Add),
            type_changes: d.and_then(|d| d.type_changes).unwrap_or(TypeChanges::Fail),
            missing_columns: d
                .and_then(|d| d.missing_columns)
                .unwrap_or(MissingColumns::Warn),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnPlan {
    pub target: String,
    /// Staged column feeding it; `None` leaves it NULL on insert and untouched on update.
    pub source: Option<String>,
    pub kind: String,
    pub cast: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Adoption {
    /// Target columns in target order (then any added ones).
    pub columns: Vec<ColumnPlan>,
    pub create: bool,
    pub add: Vec<Column>,
    /// (column, from, to) widened in place.
    pub widen: Vec<(String, String, String)>,
    pub missing: Vec<String>,
    pub ignored: Vec<String>,
    pub warnings: Vec<String>,
}

/// Declared lake types: `columns.<c>.type`, with `"*"` for unlisted columns.
pub type Declared = IndexMap<String, String>;

fn match_column<'a>(staged: &'a [Column], target: &str) -> Result<Option<&'a Column>> {
    if let Some(c) = staged.iter().find(|c| c.name == target) {
        return Ok(Some(c));
    }
    let ci: Vec<&Column> = staged
        .iter()
        .filter(|c| c.name.eq_ignore_ascii_case(target))
        .collect();
    match ci.as_slice() {
        [] => Ok(None),
        [c] => Ok(Some(c)),
        many => Err(AtlatlError::config(format!(
            "target column `{target}` matches several source columns case-insensitively ({})",
            many.iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ))
        .code("ambiguous_column")),
    }
}

fn declared_type(declared: &Declared, col: &str, staged: &str) -> String {
    declared
        .get(col)
        .or_else(|| declared.get("*"))
        .cloned()
        .unwrap_or_else(|| staged.to_owned())
}

fn int_rank(t: &str) -> Option<u8> {
    Some(match t {
        "TINYINT" => 1,
        "SMALLINT" => 2,
        "INTEGER" => 3,
        "BIGINT" => 4,
        "HUGEINT" => 5,
        _ => return None,
    })
}

fn decimal(t: &str) -> Option<(u32, u32)> {
    let inner = t.strip_prefix("DECIMAL(")?.strip_suffix(')')?;
    let (p, s) = inner.split_once(',')?;
    Some((p.trim().parse().ok()?, s.trim().parse().ok()?))
}

/// Promotions DuckLake's `ALTER COLUMN TYPE` accepts (it refuses anything → VARCHAR).
pub fn is_widening(from: &str, to: &str) -> bool {
    if let (Some(a), Some(b)) = (int_rank(from), int_rank(to)) {
        return b > a;
    }
    if from == "FLOAT" && to == "DOUBLE" {
        return true;
    }
    if let (Some((p1, s1)), Some((p2, s2))) = (decimal(from), decimal(to)) {
        return (p2, s2) != (p1, s1) && s2 >= s1 && p2 - s2 >= p1 - s1;
    }
    false
}

/// Plans how staged columns land in `target` (or a new table when `target` is `None`).
pub fn plan(
    staged: &[Column],
    target: Option<&[Column]>,
    policy: Policy,
    declared: &Declared,
) -> Result<Adoption> {
    let Some(target) = target else {
        let columns = staged
            .iter()
            .map(|c| {
                let kind = declared_type(declared, &c.name, &c.kind);
                ColumnPlan {
                    target: c.name.clone(),
                    source: Some(c.name.clone()),
                    cast: kind != c.kind,
                    kind,
                }
            })
            .collect();
        return Ok(Adoption {
            columns,
            create: true,
            ..Default::default()
        });
    };
    let mut a = Adoption::default();
    let mut used: Vec<&str> = Vec::new();
    for t in target {
        match match_column(staged, &t.name)? {
            Some(s) => {
                used.push(&s.name);
                let mut kind = t.kind.clone();
                if s.kind != t.kind
                    && policy.type_changes == TypeChanges::Widen
                    && is_widening(&t.kind, &s.kind)
                {
                    a.widen
                        .push((t.name.clone(), t.kind.clone(), s.kind.clone()));
                    kind = s.kind.clone();
                }
                a.columns.push(ColumnPlan {
                    target: t.name.clone(),
                    source: Some(s.name.clone()),
                    cast: s.kind != kind,
                    kind,
                });
            }
            None => {
                a.missing.push(t.name.clone());
                a.columns.push(ColumnPlan {
                    target: t.name.clone(),
                    source: None,
                    kind: t.kind.clone(),
                    cast: false,
                });
            }
        }
    }
    if !a.missing.is_empty() {
        let msg = format!(
            "target columns missing from the source: {}",
            a.missing.join(", ")
        );
        match policy.missing_columns {
            MissingColumns::Fail => return Err(AtlatlError::data(msg).code("missing_columns")),
            MissingColumns::Warn => a.warnings.push(msg),
        }
    }
    let new: Vec<&Column> = staged
        .iter()
        .filter(|s| !used.contains(&s.name.as_str()))
        .collect();
    if !new.is_empty() {
        let names = new
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        match policy.new_columns {
            NewColumns::Fail => {
                return Err(AtlatlError::data(format!("new source columns not in the target: {names} (`table_drift.new_columns: fail`)"))
                    .code("new_columns"));
            }
            NewColumns::Ignore => {
                a.ignored = new.iter().map(|c| c.name.clone()).collect();
                a.warnings
                    .push(format!("ignoring new source columns: {names}"));
            }
            NewColumns::Add => {
                for c in new {
                    let kind = declared_type(declared, &c.name, &c.kind);
                    a.add.push(Column {
                        name: c.name.clone(),
                        kind: kind.clone(),
                    });
                    a.columns.push(ColumnPlan {
                        target: c.name.clone(),
                        source: Some(c.name.clone()),
                        cast: kind != c.kind,
                        kind,
                    });
                }
            }
        }
    }
    Ok(a)
}

/// DDL that brings the target in line: ADD COLUMN (appended) and widening ALTERs. Runs inside
/// the replication's commit.
pub fn ddl(table: &str, a: &Adoption) -> Vec<String> {
    let mut out = Vec::new();
    for (col, _, to) in &a.widen {
        out.push(format!(
            "ALTER TABLE {table} ALTER COLUMN {} TYPE {to}",
            ident(col)
        ));
    }
    for c in &a.add {
        out.push(format!(
            "ALTER TABLE {table} ADD COLUMN {} {}",
            ident(&c.name),
            c.kind
        ));
    }
    out
}

pub fn create_sql(table: &str, a: &Adoption) -> String {
    let cols = a
        .columns
        .iter()
        .map(|c| format!("{} {}", ident(&c.target), c.kind))
        .collect::<Vec<_>>()
        .join(", ");
    format!("CREATE TABLE {table} ({cols})")
}

/// `(target column, expression over staging)` for every column fed from the source.
pub fn projection(a: &Adoption) -> Vec<(String, String)> {
    a.columns
        .iter()
        .filter_map(|c| {
            let src = ident(c.source.as_ref()?);
            let e = if c.cast {
                format!("CAST({src} AS {})", c.kind)
            } else {
                src
            };
            Some((c.target.clone(), e))
        })
        .collect()
}

/// Rows whose non-NULL value doesn't survive the cast into the target type. With `set_null`
/// they become NULL with a counted warning; otherwise it's a `data` error with samples (04 §5.1).
pub fn check_casts(c: &Connection, stg: &str, a: &Adoption, set_null: bool) -> Result<u64> {
    let rel = quoted_relation(stg);
    let mut total = 0u64;
    let mut samples = Vec::new();
    for col in a.columns.iter().filter(|c| c.cast) {
        let Some(src) = &col.source else { continue };
        let s = ident(src);
        let pred = format!("{s} IS NOT NULL AND TRY_CAST({s} AS {}) IS NULL", col.kind);
        let n: i64 = c
            .query_row(
                &format!("SELECT count(*) FROM {rel} WHERE {pred}"),
                [],
                |r| r.get(0),
            )
            .map_err(db)?;
        if n == 0 {
            continue;
        }
        total += n as u64;
        let mut st = c
            .prepare(&format!(
                "SELECT {s}::VARCHAR FROM {rel} WHERE {pred} LIMIT {}",
                MAX_SAMPLES.saturating_sub(samples.len()).max(1)
            ))
            .map_err(db)?;
        let vals: Vec<String> = st
            .query_map([], |r| r.get(0))
            .map_err(db)?
            .collect::<Result<_, _>>()
            .map_err(db)?;
        for v in vals {
            samples.push(serde_json::json!({"column": col.target, "type": col.kind, "value": v}));
        }
        if set_null {
            c.execute_batch(&format!("UPDATE {rel} SET {s} = NULL WHERE {pred}"))
                .map_err(db)?;
        }
    }
    if total > 0 && !set_null {
        return Err(
            AtlatlError::data(format!("{total} values don't cast into the target types"))
                .code("cast_failed")
                .samples(samples),
        );
    }
    Ok(total)
}

/// Target row count for the shrink guard.
pub fn target_rows(c: &Connection, table: &str) -> Result<u64> {
    let n: i64 = c
        .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
        .map_err(db)?;
    Ok(n as u64)
}

pub fn comment(s: &str) -> String {
    literal(s)
}

#[cfg(test)]
mod tests {
    use atlatl_core::error::ErrorClass;

    use super::*;

    fn col(n: &str, k: &str) -> Column {
        Column {
            name: n.into(),
            kind: k.into(),
        }
    }

    fn pol(n: NewColumns, t: TypeChanges, m: MissingColumns) -> Policy {
        Policy {
            new_columns: n,
            type_changes: t,
            missing_columns: m,
        }
    }

    #[test]
    fn widening_rules() {
        assert!(is_widening("INTEGER", "BIGINT"));
        assert!(!is_widening("BIGINT", "INTEGER"));
        assert!(is_widening("DECIMAL(10,2)", "DECIMAL(18,4)"));
        assert!(!is_widening("DECIMAL(18,4)", "DECIMAL(18,2)"));
        assert!(
            !is_widening("DECIMAL(10,2)", "DECIMAL(11,4)"),
            "integer digits must not shrink"
        );
        assert!(
            !is_widening("INTEGER", "VARCHAR"),
            "DuckLake refuses VARCHAR widening"
        );
    }

    #[test]
    fn target_order_is_authoritative() {
        let staged = [
            col("Name", "VARCHAR"),
            col("ID", "BIGINT"),
            col("extra", "VARCHAR"),
        ];
        let target = [
            col("id", "INTEGER"),
            col("name", "VARCHAR"),
            col("_legacy_loaded_at", "INTEGER"),
        ];
        let a = plan(
            &staged,
            Some(&target),
            pol(NewColumns::Add, TypeChanges::Fail, MissingColumns::Warn),
            &Declared::new(),
        )
        .expect("plan");
        let order: Vec<(&str, Option<&str>, &str, bool)> = a
            .columns
            .iter()
            .map(|c| {
                (
                    c.target.as_str(),
                    c.source.as_deref(),
                    c.kind.as_str(),
                    c.cast,
                )
            })
            .collect();
        assert_eq!(
            order,
            [
                ("id", Some("ID"), "INTEGER", true),
                ("name", Some("Name"), "VARCHAR", false),
                ("_legacy_loaded_at", None, "INTEGER", false),
                ("extra", Some("extra"), "VARCHAR", false),
            ]
        );
        assert_eq!(a.missing, ["_legacy_loaded_at"]);
        assert_eq!(ddl("t", &a), ["ALTER TABLE t ADD COLUMN \"extra\" VARCHAR"]);
        assert_eq!(
            projection(&a)[0],
            ("id".into(), "CAST(\"ID\" AS INTEGER)".into())
        );
    }

    #[test]
    fn policies() {
        let staged = [col("a", "BIGINT"), col("new", "VARCHAR")];
        let target = [col("a", "INTEGER"), col("gone", "VARCHAR")];
        let d = Declared::new();
        let e = plan(
            &staged,
            Some(&target),
            pol(NewColumns::Fail, TypeChanges::Fail, MissingColumns::Warn),
            &d,
        )
        .expect_err("new");
        assert_eq!(
            (e.class, e.code.as_deref()),
            (ErrorClass::Data, Some("new_columns"))
        );
        let e = plan(
            &staged,
            Some(&target),
            pol(NewColumns::Add, TypeChanges::Fail, MissingColumns::Fail),
            &d,
        )
        .expect_err("missing");
        assert_eq!(e.code.as_deref(), Some("missing_columns"));
        let a = plan(
            &staged,
            Some(&target),
            pol(NewColumns::Ignore, TypeChanges::Widen, MissingColumns::Warn),
            &d,
        )
        .expect("ok");
        assert_eq!(a.ignored, ["new"]);
        assert_eq!(
            a.widen,
            [("a".to_owned(), "INTEGER".to_owned(), "BIGINT".to_owned())]
        );
        assert_eq!(
            ddl("t", &a),
            ["ALTER TABLE t ALTER COLUMN \"a\" TYPE BIGINT"]
        );
        assert!(!a.columns[0].cast);
        let dup = [col("x", "VARCHAR"), col("X", "VARCHAR")];
        let e = plan(
            &dup,
            Some(&[col("x2", "VARCHAR"), col("x", "VARCHAR")][..]),
            pol(NewColumns::Ignore, TypeChanges::Fail, MissingColumns::Warn),
            &d,
        );
        assert!(e.is_ok(), "exact match wins over case-insensitive");
        let e = plan(
            &dup,
            Some(&[col("X ", "VARCHAR"), col("x", "VARCHAR")][..]),
            pol(NewColumns::Ignore, TypeChanges::Fail, MissingColumns::Warn),
            &d,
        );
        assert!(e.is_ok());
        let e = plan(
            &[col("aB", "VARCHAR"), col("Ab", "VARCHAR")],
            Some(&[col("ab", "VARCHAR")][..]),
            pol(NewColumns::Ignore, TypeChanges::Fail, MissingColumns::Warn),
            &d,
        )
        .expect_err("ambiguous");
        assert_eq!(e.code.as_deref(), Some("ambiguous_column"));
    }

    #[test]
    fn new_tables_use_declared_types() {
        let staged = [col("id", "VARCHAR"), col("v", "VARCHAR")];
        let d: Declared = [
            ("id".to_owned(), "BIGINT".to_owned()),
            ("*".to_owned(), "VARCHAR".to_owned()),
        ]
        .into_iter()
        .collect();
        let a = plan(&staged, None, Policy::from(None), &d).expect("plan");
        assert!(a.create);
        assert_eq!(
            create_sql("lake.s.t", &a),
            "CREATE TABLE lake.s.t (\"id\" BIGINT, \"v\" VARCHAR)"
        );
    }

    #[test]
    fn cast_failures_are_data_errors() {
        let c = Connection::open_in_memory().expect("db");
        crate::staging::stage_query(
            &c,
            "stg",
            "SELECT * FROM (VALUES ('1', '2.5'), ('x', '3'), (NULL, 'nope')) t(a, b)",
        )
        .expect("stage");
        let staged = crate::staging::describe(&c, "stg").expect("describe");
        let a = plan(
            &staged,
            Some(&[col("a", "INTEGER"), col("b", "DECIMAL(24,6)")][..]),
            Policy::from(None),
            &Declared::new(),
        )
        .expect("plan");
        let e = check_casts(&c, "stg", &a, false).expect_err("cast");
        assert_eq!(
            (e.code.as_deref(), e.samples.len()),
            (Some("cast_failed"), 2)
        );
        assert_eq!(
            e.samples[0],
            serde_json::json!({"column": "a", "type": "INTEGER", "value": "x"})
        );
        assert_eq!(check_casts(&c, "stg", &a, true).expect("set null"), 2);
        assert_eq!(check_casts(&c, "stg", &a, false).expect("clean"), 0);
    }
}
