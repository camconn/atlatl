//! Adoption of existing tables and table drift (04 §6). The target schema is authoritative: its
//! columns, order and types. Staged values are cast into it and checked value by value.

use atlatl_core::config::model::{
    CAST_NULLED, MissingColumns, NewColumns, TableDrift, TypeChanges,
};
use atlatl_core::error::{AtlatlError, Result};
use duckdb::Connection;
use indexmap::IndexMap;

use crate::sql::{CAST_FAILED, db, ident, literal};
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
    /// The staged type (the target's own when there's no source).
    pub from: String,
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
                    from: c.kind.clone(),
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
                    from: s.kind.clone(),
                    cast: s.kind != kind,
                    kind,
                });
            }
            None => {
                a.missing.push(t.name.clone());
                a.columns.push(ColumnPlan {
                    target: t.name.clone(),
                    source: None,
                    from: t.kind.clone(),
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
                        from: c.kind.clone(),
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

fn unsigned(t: &str) -> bool {
    matches!(
        t,
        "UTINYINT" | "USMALLINT" | "UINTEGER" | "UBIGINT" | "UHUGEINT"
    )
}

fn float(t: &str) -> bool {
    matches!(t, "FLOAT" | "DOUBLE")
}

/// Fraction digits a temporal type keeps.
fn temporal(t: &str) -> Option<usize> {
    Some(match t {
        "DATE" | "TIMESTAMP_S" => 0,
        "TIMESTAMP_MS" => 3,
        "TIMESTAMP" | "TIMESTAMP WITH TIME ZONE" | "TIME" => 6,
        "TIMESTAMP_NS" => 9,
        _ => return None,
    })
}

/// A space-padded decimal numeral with optional exponent as exact `[-]digits'e'exp` text (digits
/// without leading or trailing zeros, `0` for zero); NULL for anything else, hex, binary and
/// digit separators included.
fn numeral(e: &str) -> String {
    let t = format!("trim({e})");
    let part = |n: u8| {
        format!(r"regexp_extract({t}, '^([+-]?)(\d*)(?:\.(\d*))?(?:[eE]([+-]?\d+))?$', {n})")
    };
    let (sign, frac, exp) = (part(1), part(3), part(4));
    let lead = format!("ltrim({} || {frac}, '0')", part(2));
    let digits = format!("rtrim({lead}, '0')");
    format!(
        r"CASE WHEN NOT regexp_matches({t}, '^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$') THEN NULL
         WHEN {digits} = '' THEN '0'
         ELSE CASE {sign} WHEN '-' THEN '-' ELSE '' END || {digits} || 'e' ||
           (TRY_CAST(coalesce(nullif({exp}, ''), '0') AS BIGINT)::HUGEINT - length({frac})
            + length({lead}) - length({digits})) END"
    )
}

/// True where a non-NULL `s` of type `from` would land in `to` other than exactly: it's
/// rounded, truncated, re-zoned, or doesn't cast at all. Widenings and anything → text are
/// free; anything → float accepts precision loss and refuses only overflow to infinity.
fn lossy(s: &str, from: &str, to: &str) -> String {
    let landed = format!("TRY_CAST({s} AS {to})");
    let base = format!("{landed} IS NULL");
    let exact = if matches!(to, "VARCHAR" | "JSON") || is_widening(from, to) {
        None
    } else if float(to) {
        (!float(from)).then(|| {
            format!("isinf({landed}) AND NOT regexp_matches(CAST({s} AS VARCHAR), '(?i)inf')")
        })
    } else if from == "VARCHAR" {
        if int_rank(to).is_some() || unsigned(to) || decimal(to).is_some() {
            Some(format!(
                "{} IS DISTINCT FROM {}",
                numeral(s),
                numeral(&format!("CAST({landed} AS VARCHAR)"))
            ))
        } else if let Some(digits) = temporal(to) {
            let frac = format!(r"regexp_matches({s}, ':\d\d\.\d{{{digits}}}\d*[1-9]')");
            Some(match to {
                "TIME" | "TIMESTAMP WITH TIME ZONE" => frac,
                _ => format!(
                    "{frac} OR TRY_CAST({s} AS TIMESTAMPTZ) <> CAST({landed} AS TIMESTAMPTZ)"
                ),
            })
        } else {
            None
        }
    } else {
        Some(format!("TRY_CAST({landed} AS {from}) IS DISTINCT FROM {s}"))
    };
    match exact {
        Some(e) => format!("{s} IS NOT NULL AND ({base} OR {e})"),
        None => format!("{s} IS NOT NULL AND {base}"),
    }
}

const WIDER: &str = "Declare a wider type or convert in `from.query`";

/// `kind` as DuckDB spells it (`numeric(10,2)` → `DECIMAL(10,2)`).
pub fn canonical(c: &Connection, kind: &str) -> Result<String> {
    c.query_row(&format!("SELECT typeof(CAST(NULL AS {kind}))"), [], |r| {
        r.get(0)
    })
    .map_err(db)
}

/// `CAST(e AS to)` for a value of type `from`, raising a `cast_failed` data error rather than
/// landing it inexactly (see [`lossy`]). `to` is canonical.
pub fn exact_cast(e: &str, from: &str, to: &str, column: &str) -> String {
    if from == to {
        return e.to_owned();
    }
    format!(
        "CASE WHEN {} THEN error(concat({}, {e}::VARCHAR, {})) ELSE CAST({e} AS {to}) END",
        lossy(e, from, to),
        literal(&format!(
            "{CAST_FAILED}a value doesn't fit its declared type exactly; it'd fail to cast, round or truncate (`{column}` \""
        )),
        literal(&format!("\" as {to}). {WIDER}")),
    )
}

/// [`exact_cast`] under `on_cast_error.set_null`: NULL where it would raise, and the predicate
/// flagging those (`None` when nothing can be nulled).
pub fn nulling_cast(e: &str, from: &str, to: &str) -> (String, Option<String>) {
    if from == to {
        return (e.to_owned(), None);
    }
    let bad = lossy(e, from, to);
    (
        format!("CASE WHEN {bad} THEN NULL ELSE CAST({e} AS {to}) END"),
        Some(bad),
    )
}

/// Counts the values a source read as NULL under `on_cast_error.set_null` (its
/// [`CAST_NULLED`] flag columns in the staged table `stg`) and drops the flags.
pub fn take_nulled(c: &Connection, stg: &str) -> Result<u64> {
    let flags: Vec<String> = crate::staging::describe(c, stg)?
        .into_iter()
        .map(|col| col.name)
        .filter(|n| n.starts_with(CAST_NULLED))
        .collect();
    if flags.is_empty() {
        return Ok(0);
    }
    let rel = quoted_relation(stg);
    let sum = flags
        .iter()
        .map(|f| format!("count(*) FILTER (WHERE {})", ident(f)))
        .collect::<Vec<_>>()
        .join(" + ");
    let n: i64 = c
        .query_row(&format!("SELECT {sum} FROM {rel}"), [], |r| r.get(0))
        .map_err(db)?;
    for f in &flags {
        c.execute_batch(&format!("ALTER TABLE {rel} DROP COLUMN {}", ident(f)))
            .map_err(db)?;
    }
    Ok(n as u64)
}

/// Rows whose non-NULL value doesn't land exactly in the target type (see [`lossy`]). With
/// `set_null` they become NULL with a counted warning; otherwise it's a `data` error with samples
/// (04 §5.1).
pub fn check_casts(c: &Connection, stg: &str, a: &Adoption, set_null: bool) -> Result<u64> {
    let rel = quoted_relation(stg);
    let mut total = 0u64;
    let mut samples = Vec::new();
    let mut first = Vec::new();
    for col in a.columns.iter().filter(|c| c.cast) {
        let Some(src) = &col.source else { continue };
        let s = ident(src);
        let to = canonical(c, &col.kind)?;
        let pred = lossy(&s, &col.from, &to);
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
        if let Some(v) = vals.first() {
            first.push(format!("`{}` {v:?} as {}", col.target, col.kind));
        }
        for v in vals {
            samples.push(serde_json::json!({"column": col.target, "type": col.kind, "value": v}));
        }
        if set_null {
            c.execute_batch(&format!("UPDATE {rel} SET {s} = NULL WHERE {pred}"))
                .map_err(db)?;
        }
    }
    if total > 0 && !set_null {
        return Err(AtlatlError::data(format!(
            "{total} values don't fit their target types exactly; they'd fail to cast, round or truncate (e.g. {}). {WIDER}",
            first.join(", ")
        ))
        .code("cast_failed")
        .samples(samples));
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

    fn declared_cast(from: &str, value: &str, to: &str) -> Result<u64> {
        let c = Connection::open_in_memory().expect("db");
        c.execute_batch("SET TimeZone = 'UTC'").expect("tz");
        crate::staging::stage_query(&c, "stg", &format!("SELECT CAST({value} AS {from}) AS v"))
            .expect("stage");
        let staged = crate::staging::describe(&c, "stg").expect("describe");
        let d: Declared = [("v".to_owned(), to.to_owned())].into_iter().collect();
        let a = plan(&staged, None, Policy::from(None), &d).expect("plan");
        check_casts(&c, "stg", &a, false)
    }

    #[test]
    fn declared_casts_land_exactly_or_fail() {
        let cases = [
            ("DECIMAL(20,4)", "12345.6789", "DECIMAL(10,2)", false),
            ("DECIMAL(20,4)", "12345.67", "DECIMAL(10,2)", true),
            ("DECIMAL(20,4)", "123456789.67", "DECIMAL(10,2)", false),
            ("DECIMAL(20,4)", "-0.005", "numeric(10,2)", false),
            ("DECIMAL(20,4)", "12345.6789", "DOUBLE", true),
            ("DECIMAL(38,2)", "12345678901234567.89", "DOUBLE", true),
            ("DECIMAL(38,10)", "0.1000000001", "DOUBLE", true),
            ("DECIMAL(38,10)", "0.1000000001", "FLOAT", true),
            (
                "DECIMAL(38,0)",
                "99999999999999999999999999999999999999",
                "FLOAT",
                true,
            ),
            ("BIGINT", "3000000000", "INTEGER", false),
            ("BIGINT", "-42", "int", true),
            ("BIGINT", "9007199254740993", "DOUBLE", true),
            ("BIGINT", "16777217", "FLOAT", true),
            (
                "HUGEINT",
                "170141183460469231731687303715884105727",
                "FLOAT",
                true,
            ),
            (
                "UHUGEINT",
                "340282366920938463463374607431768211455",
                "FLOAT",
                false,
            ),
            ("BOOLEAN", "true", "DOUBLE", true),
            ("DATE", "'2024-01-02'", "DOUBLE", false),
            ("JSON", "'1e40'", "FLOAT", false),
            ("JSON", "'\"inf\"'", "FLOAT", true),
            ("INTEGER", "2147483647", "BIGINT", true),
            ("INTEGER", "2", "BOOLEAN", false),
            ("INTEGER", "1", "BOOLEAN", true),
            ("DOUBLE", "1.5", "BIGINT", false),
            ("DOUBLE", "2.0", "BIGINT", true),
            ("DOUBLE", "12345.6789", "DECIMAL(10,2)", false),
            ("DOUBLE", "0.1", "DECIMAL(10,2)", true),
            ("DOUBLE", "0.1", "FLOAT", true),
            ("DOUBLE", "1e40", "FLOAT", false),
            ("DOUBLE", "'inf'", "FLOAT", true),
            ("DOUBLE", "'nan'", "FLOAT", true),
            ("DOUBLE", "0.1000000001", "FLOAT", true),
            ("VARCHAR", "'1.5'", "BIGINT", false),
            ("VARCHAR", "'15'", "BIGINT", true),
            ("VARCHAR", "' +015 '", "BIGINT", true),
            ("VARCHAR", "'15.000'", "BIGINT", true),
            ("VARCHAR", "'-0'", "BIGINT", true),
            ("VARCHAR", "'x'", "BIGINT", false),
            ("VARCHAR", "'12345.6789'", "DECIMAL(10,2)", false),
            ("VARCHAR", "'12345.6'", "DECIMAL(10,2)", true),
            ("VARCHAR", "'-.5'", "DECIMAL(10,2)", true),
            ("VARCHAR", "'0.1'", "DOUBLE", true),
            ("VARCHAR", "'1e40'", "FLOAT", false),
            ("VARCHAR", "'-Infinity'", "FLOAT", true),
            ("VARCHAR", "'yes'", "BOOLEAN", true),
            ("VARCHAR", "'maybe'", "BOOLEAN", false),
            ("VARCHAR", "'2024-01-02'", "DATE", true),
            ("VARCHAR", "'2024-01-02 00:00:00'", "DATE", true),
            ("VARCHAR", "'2024-01-02 03:04:05'", "DATE", false),
            ("VARCHAR", "'2024-01-02 03:04:05.123456'", "TIMESTAMP", true),
            (
                "VARCHAR",
                "'2024-01-02 03:04:05.1234560'",
                "TIMESTAMP",
                true,
            ),
            (
                "VARCHAR",
                "'2024-01-02 03:04:05.1234567'",
                "TIMESTAMP",
                false,
            ),
            (
                "VARCHAR",
                "'2024-01-02 03:04:05.1234567'",
                "TIMESTAMPTZ",
                false,
            ),
            (
                "VARCHAR",
                "'2024-01-02 03:04:05.1234567'",
                "TIMESTAMP_NS",
                true,
            ),
            ("VARCHAR", "'2024-01-02T03:04:05+02'", "TIMESTAMP", false),
            ("VARCHAR", "'2024-01-02T03:04:05Z'", "TIMESTAMP", true),
            ("VARCHAR", "'2024-01-02T03:04:05+02'", "TIMESTAMPTZ", true),
            ("VARCHAR", "'12:34:56.1234567'", "TIME", false),
            ("TIMESTAMP", "'2024-01-02 03:04:05'", "DATE", false),
            ("TIMESTAMP", "'2024-01-02 00:00:00'", "DATE", true),
            (
                "TIMESTAMP_NS",
                "'2024-01-02 03:04:05.123456789'",
                "TIMESTAMP",
                false,
            ),
            (
                "TIMESTAMP_NS",
                "'2024-01-02 03:04:05.123456'",
                "TIMESTAMP",
                true,
            ),
            ("TIMESTAMPTZ", "'2024-01-02 03:04:05+02'", "TIMESTAMP", true),
            ("DATE", "'2024-01-02'", "TIMESTAMP", true),
            ("DOUBLE", "0.1", "VARCHAR", true),
            ("DECIMAL(20,4)", "NULL", "DECIMAL(10,2)", true),
            ("VARCHAR", "NULL", "BIGINT", true),
        ];
        for (from, value, to, fits) in cases {
            let r = declared_cast(from, value, to);
            match (fits, r) {
                (true, Ok(0)) => {}
                (false, Err(e)) => {
                    assert_eq!(
                        (e.class, e.code.as_deref(), e.samples.len()),
                        (ErrorClass::Data, Some("cast_failed"), 1),
                        "{from} {value} as {to}"
                    );
                }
                (_, r) => panic!("{from} {value} as {to}: {r:?}"),
            }
        }
    }

    #[test]
    fn narrowing_errors_name_the_column_value_and_type() {
        let e = declared_cast("DECIMAL(20,4)", "12345.6789", "DECIMAL(10,2)").expect_err("lossy");
        assert_eq!(
            e.message,
            "1 values don't fit their target types exactly; they'd fail to cast, round or truncate (e.g. `v` \"12345.6789\" as DECIMAL(10,2)). Declare a wider type or convert in `from.query`"
        );
        assert_eq!(
            e.samples[0],
            serde_json::json!({"column": "v", "type": "DECIMAL(10,2)", "value": "12345.6789"})
        );
    }

    #[test]
    fn adopted_columns_are_exact_too() {
        let c = Connection::open_in_memory().expect("db");
        crate::staging::stage_query(
            &c,
            "stg",
            "SELECT * FROM (VALUES (12345.67::DECIMAL(20,4), 7::BIGINT), (12345.6789, 3000000000), (NULL, NULL)) t(amt, n)",
        )
        .expect("stage");
        let staged = crate::staging::describe(&c, "stg").expect("describe");
        let target = [col("amt", "DECIMAL(10,2)"), col("n", "INTEGER")];
        let a = plan(&staged, Some(&target), Policy::from(None), &Declared::new()).expect("plan");
        let e = check_casts(&c, "stg", &a, false).expect_err("lossy");
        assert_eq!(e.samples.len(), 2);
        assert_eq!(check_casts(&c, "stg", &a, true).expect("set null"), 2);
        let landed: String = c
            .query_row(
                &format!(
                    "SELECT string_agg(concat_ws('|', {}), ',') FROM stg",
                    projection(&a)
                        .into_iter()
                        .map(|(_, e)| e)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                [],
                |r| r.get(0),
            )
            .expect("land");
        assert_eq!(landed, "12345.67|7,,");
    }

    #[test]
    fn exact_casts_raise_inline() {
        let c = Connection::open_in_memory().expect("db");
        let to = canonical(&c, "numeric(10,2)").expect("canonical");
        assert_eq!(to, "DECIMAL(10,2)");
        assert_eq!(exact_cast("\"v\"", &to, &to, "v"), "\"v\"");
        let land = |v: &str| {
            c.query_row(
                &format!(
                    "SELECT {}::VARCHAR FROM (SELECT {v} AS v)",
                    exact_cast("\"v\"", "VARCHAR", &to, "amt")
                ),
                [],
                |r| r.get::<_, Option<String>>(0),
            )
            .map_err(db)
        };
        assert_eq!(
            land("'12345.6'").expect("fits").as_deref(),
            Some("12345.60")
        );
        assert_eq!(land("NULL::VARCHAR").expect("null"), None);
        let e = land("'12345.6789'").expect_err("rounds");
        assert_eq!(
            (e.class, e.code.as_deref(), e.message.as_str()),
            (
                ErrorClass::Data,
                Some("cast_failed"),
                "a value doesn't fit its declared type exactly; it'd fail to cast, round or truncate (`amt` \"12345.6789\" as DECIMAL(10,2)). Declare a wider type or convert in `from.query`"
            )
        );
        assert_eq!(
            canonical(&c, "nope").expect_err("unknown").class,
            ErrorClass::Config
        );
    }

    #[test]
    fn nulling_casts_flag_what_they_null() {
        let c = Connection::open_in_memory().expect("db");
        assert_eq!(
            nulling_cast("\"v\"", "DECIMAL(10,2)", "DECIMAL(10,2)"),
            ("\"v\"".to_owned(), None)
        );
        let (e, flag) = nulling_cast("v", "VARCHAR", "DECIMAL(10,2)");
        let flag = flag.expect("flag");
        c.execute_batch(&format!(
            "CREATE TEMP TABLE stg AS SELECT id, {e} AS v, {flag} AS {} FROM (VALUES (1, '12345.6789'), (2, '12.50'), (3, NULL), (4, 'x')) t(id, v)",
            ident(&format!("{CAST_NULLED}v"))
        ))
        .expect("stage");
        assert_eq!(take_nulled(&c, "stg").expect("take"), 2);
        let cols = crate::staging::describe(&c, "stg").expect("cols");
        assert_eq!(
            cols.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            ["id", "v"]
        );
        let landed: String = c
            .query_row(
                "SELECT string_agg(coalesce(v::VARCHAR, '-'), ',' ORDER BY id) FROM stg",
                [],
                |r| r.get(0),
            )
            .expect("landed");
        assert_eq!(landed, "-,12.50,-,-");
        assert_eq!(take_nulled(&c, "stg").expect("none"), 0);
    }

    #[test]
    fn varchar_numerals_land_exactly() {
        let c = Connection::open_in_memory().expect("db");
        let inexact = |v: &str, to: &str| -> bool {
            c.query_row(
                &format!(
                    "SELECT coalesce({}, false) FROM (SELECT {} AS v)",
                    lossy("v", "VARCHAR", to),
                    literal(v)
                ),
                [],
                |r| r.get(0),
            )
            .expect(v)
        };
        for (v, to) in [
            ("1e2", "INTEGER"),
            ("1E+2", "BIGINT"),
            ("1.5e1", "DECIMAL(4,2)"),
            (".5e1", "INTEGER"),
            ("1e-2", "DECIMAL(10,2)"),
            ("150e-2", "DECIMAL(10,1)"),
            ("1e2", "TINYINT"),
            ("-0e5", "INTEGER"),
            (" 12 ", "INTEGER"),
            ("+12", "UBIGINT"),
            ("-007.50", "DECIMAL(4,2)"),
            ("5.", "INTEGER"),
        ] {
            assert!(!inexact(v, to), "{v} as {to} is exact");
        }
        for (v, to) in [
            ("1.25e-1", "DECIMAL(10,2)"),
            ("1e3", "TINYINT"),
            ("1.5", "INTEGER"),
            ("1e400", "DECIMAL(38,0)"),
            ("1e400", "HUGEINT"),
            ("1e99999999999999999999", "DECIMAL(38,0)"),
            ("0x10", "INTEGER"),
            ("0b11", "INTEGER"),
            ("1_000", "INTEGER"),
            ("1_000", "DECIMAL(10,2)"),
            ("\t12", "INTEGER"),
            ("- 12", "INTEGER"),
            ("12abc", "INTEGER"),
            ("e2", "INTEGER"),
            (".", "INTEGER"),
        ] {
            assert!(inexact(v, to), "{v} as {to} is lossy");
        }
    }
}
