//! Source schemas (04 §10): enforce/observe evaluation over staged records, and inference.
//!
//! Evaluation is SQL over the staging relation: one key pass (JSON staging) or `DESCRIBE`
//! (columnar staging), one aggregate pass, and a `LIMIT 3` sample query per violated field.

use std::collections::{BTreeMap, BTreeSet};

use atlatl_core::config::model::{
    FieldSpec, LogicalType, Policy, SchemaMode as ConfigMode, SourceSchema, Threshold,
};
use atlatl_core::error::{AtlatlError, Result};
use atlatl_core::obs::report::{
    SchemaMode, SchemaReport, SchemaStatus, SchemaViolation, Severity, ViolationKind,
};
use atlatl_engine::decimal_text::{parts as decimal_parts, plain as decimal_text};
use duckdb::Connection;
use duckdb::vscalar::VScalar;
use indexmap::IndexMap;
use serde_json::{Value, json};

use crate::sql::{db, ident, literal};
use crate::staging::{describe, quoted_relation};

pub const SAMPLES: usize = 3;

/// How the records under evaluation are staged.
#[derive(Clone, Copy, Debug)]
pub enum Staged<'a> {
    /// One JSON object per row in `column` (JSON or VARCHAR), e.g. REST staging
    /// `(ordinal BIGINT, record JSON)`. Fields are the objects' top-level keys.
    Json {
        relation: &'a str,
        column: &'a str,
        ordinal: Option<&'a str>,
    },
    /// One column per field (files, DB sources, br-layer tables). `exclude` lists columns that are
    /// not source fields (atlatl metadata); `ordinal` is excluded too.
    Columns {
        relation: &'a str,
        exclude: &'a [&'a str],
        ordinal: Option<&'a str>,
    },
}

impl<'a> Staged<'a> {
    /// REST staging: `(ordinal, record)`.
    pub fn records(relation: &'a str) -> Self {
        Staged::Json {
            relation,
            column: "record",
            ordinal: Some("ordinal"),
        }
    }

    fn relation(&self) -> String {
        match self {
            Staged::Json { relation, .. } | Staged::Columns { relation, .. } => {
                quoted_relation(relation)
            }
        }
    }

    fn ordinal(&self) -> Option<&'a str> {
        match self {
            Staged::Json { ordinal, .. } | Staged::Columns { ordinal, .. } => *ordinal,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Evaluation {
    /// The member report's `schema` section.
    pub report: SchemaReport,
    /// `schema_violation` (class `data`) when a `fail` policy tripped under `enforce`.
    pub failure: Option<AtlatlError>,
}

impl Evaluation {
    /// A `warn` or `fail` policy tripped: emit `schema.drift`.
    pub fn drifted(&self) -> bool {
        self.report.severity.is_some()
    }

    pub fn into_result(self) -> Result<SchemaReport> {
        match self.failure {
            Some(e) => Err(e),
            None => Ok(self.report),
        }
    }
}

/// Evaluates `schema` over the staged records. `Err` is reserved for engine or schema-config
/// errors; a violated schema is reported in [`Evaluation`].
pub fn evaluate(c: &Connection, staged: &Staged<'_>, schema: &SourceSchema) -> Result<Evaluation> {
    let mode = match schema.mode {
        Some(ConfigMode::Observe) => SchemaMode::Observe,
        _ => SchemaMode::Enforce,
    };
    let fields = schema
        .fields
        .iter()
        .map(|(name, spec)| FieldSql::new(name, spec))
        .collect::<Result<Vec<_>>>()?;
    check_formats(c, &fields)?;
    ensure_exact(c)?;
    let (seen, sources) = field_sources(c, staged)?;
    let declared: BTreeSet<&str> = schema.fields.keys().map(String::as_str).collect();
    let rel = staged.relation();
    let n = count(c, &rel)?;

    let new_fields: Vec<String> = seen
        .iter()
        .filter(|k| !declared.contains(k.as_str()))
        .cloned()
        .collect();
    let missing_fields: Vec<String> = if n == 0 {
        Vec::new()
    } else {
        schema
            .fields
            .keys()
            .filter(|k| !seen.contains(*k))
            .cloned()
            .collect()
    };

    let present: Vec<(&FieldSql, String)> = fields
        .iter()
        .filter(|f| seen.contains(&f.name))
        .filter_map(|f| sources.get(&f.name).map(|s| (f, s.clone())))
        .collect();
    let mut counted: Vec<(String, ViolationKind, u64, String)> = Vec::new();
    if n > 0 && !present.is_empty() {
        let w = projection(staged, &present);
        let mut aggs = Vec::new();
        for (i, (f, _)) in present.iter().enumerate() {
            let p = f.predicates(i);
            aggs.push(format!("count_if({})", p.null));
            aggs.push(format!("count_if({})", p.kind));
            aggs.push(match &p.domain {
                Some(d) => format!("count_if({d})"),
                None => "0".into(),
            });
        }
        let sql = format!("WITH {w} SELECT {} FROM w", aggs.join(", "));
        let row: Vec<i64> = c
            .query_row(&sql, [], |r| {
                (0..aggs.len()).map(|i| r.get::<_, i64>(i)).collect()
            })
            .map_err(db)?;
        for (i, (f, _)) in present.iter().enumerate() {
            let p = f.predicates(i);
            let nulls = row[3 * i] as u64;
            let bad = row[3 * i + 1] as u64;
            let dom = row[3 * i + 2] as u64;
            if !f.spec.nullable.unwrap_or(true) && nulls > 0 {
                counted.push((f.name.clone(), ViolationKind::Null, nulls, p.null.clone()));
            }
            if bad > 0 {
                counted.push((f.name.clone(), ViolationKind::Type, bad, p.kind.clone()));
            }
            if let (Some(d), true) = (&p.domain, dom > 0) {
                counted.push((f.name.clone(), ViolationKind::Domain, dom, d.clone()));
            }
        }
    }

    let policy = |p: Option<Policy>| {
        let p = p.unwrap_or(Policy::Fail);
        if mode == SchemaMode::Observe && p == Policy::Fail {
            Policy::Warn
        } else {
            p
        }
    };
    let mut tripped: Vec<Policy> = Vec::new();
    let mut trip = |p: Policy| {
        if p != Policy::Ignore {
            tripped.push(p);
        }
    };
    let new_policy = policy(schema.on_new_field);
    let new_fields = if new_policy == Policy::Ignore {
        Vec::new()
    } else {
        if !new_fields.is_empty() {
            trip(new_policy);
        }
        new_fields
    };
    let missing_policy = policy(schema.on_missing_field);
    let missing_fields = if missing_policy == Policy::Ignore {
        Vec::new()
    } else {
        if !missing_fields.is_empty() {
            trip(missing_policy);
        }
        missing_fields
    };
    let tol = Tolerance::from(schema.tolerance.as_ref());
    let mut violations = Vec::new();
    let w = (!present.is_empty()).then(|| projection(staged, &present));
    for (field, kind, count, pred) in counted {
        let p = policy(match kind {
            ViolationKind::Type => schema.on_type_violation,
            ViolationKind::Null => schema.on_null_violation,
            ViolationKind::Domain => schema.on_domain_violation,
        });
        if p == Policy::Ignore {
            continue;
        }
        if tol.acts(count, n) {
            trip(p);
        }
        let i = present
            .iter()
            .position(|(f, _)| f.name == field)
            .unwrap_or_default();
        let samples = match &w {
            Some(w) => samples(c, w, i, &pred, kind)?,
            None => Vec::new(),
        };
        violations.push(SchemaViolation {
            field,
            kind,
            count,
            samples,
        });
    }

    let severity = if tripped.contains(&Policy::Fail) {
        Some(Severity::Error)
    } else if tripped.contains(&Policy::Warn) {
        Some(Severity::Warn)
    } else {
        None
    };
    let report = SchemaReport {
        status: if severity.is_some() {
            SchemaStatus::Violated
        } else {
            SchemaStatus::Ok
        },
        mode,
        severity,
        records: n,
        new_fields,
        missing_fields,
        violations,
    };
    let failure = (severity == Some(Severity::Error)).then(|| failure(&report));
    Ok(Evaluation { report, failure })
}

fn failure(r: &SchemaReport) -> AtlatlError {
    let mut parts = Vec::new();
    if !r.new_fields.is_empty() {
        parts.push(format!("new fields {}", r.new_fields.join(", ")));
    }
    if !r.missing_fields.is_empty() {
        parts.push(format!("missing fields {}", r.missing_fields.join(", ")));
    }
    for v in &r.violations {
        parts.push(format!(
            "{} violations on {} ({} of {})",
            kind_name(v.kind),
            v.field,
            v.count,
            r.records
        ));
    }
    let samples = r
        .violations
        .iter()
        .flat_map(|v| {
            v.samples
                .iter()
                .map(|s| json!({"field": v.field, "kind": kind_name(v.kind), "sample": s}))
        })
        .take(10)
        .collect();
    AtlatlError::data(format!("source schema violated: {}", parts.join("; ")))
        .code("schema_violation")
        .samples(samples)
}

fn kind_name(k: ViolationKind) -> &'static str {
    match k {
        ViolationKind::Type => "type",
        ViolationKind::Null => "null",
        ViolationKind::Domain => "domain",
    }
}

struct Tolerance {
    ratio: f64,
    min_count: u64,
}

impl From<Option<&Threshold>> for Tolerance {
    fn from(t: Option<&Threshold>) -> Self {
        Self {
            ratio: t.and_then(|t| t.ratio).unwrap_or(0.0),
            min_count: t.and_then(|t| t.min_count).unwrap_or(1).max(1),
        }
    }
}

impl Tolerance {
    /// Violations below the threshold are counted but not acted on.
    fn acts(&self, count: u64, records: u64) -> bool {
        count >= self.min_count && (records == 0 || count as f64 >= self.ratio * records as f64)
    }
}

fn count(c: &Connection, rel: &str) -> Result<u64> {
    let n: i64 = c
        .query_row(&format!("SELECT count(*) FROM {rel}"), [], |r| r.get(0))
        .map_err(db)?;
    Ok(n as u64)
}

/// Fields seen in the staged data, and per field the SQL that reads its raw value.
fn field_sources(
    c: &Connection,
    staged: &Staged<'_>,
) -> Result<(BTreeSet<String>, IndexMap<String, String>)> {
    match staged {
        Staged::Json { column, .. } => {
            let sql = format!(
                "SELECT DISTINCT unnest(json_keys(CAST({} AS JSON))) FROM {}",
                ident(column),
                staged.relation()
            );
            let mut st = c.prepare(&sql).map_err(db)?;
            let keys: BTreeSet<String> = st
                .query_map([], |r| r.get::<_, String>(0))
                .map_err(db)?
                .collect::<std::result::Result<_, _>>()
                .map_err(db)?;
            let src = keys.iter().map(|k| (k.clone(), String::new())).collect();
            Ok((keys, src))
        }
        Staged::Columns {
            relation,
            exclude,
            ordinal,
        } => {
            let cols = describe(c, relation)?;
            let mut keys = BTreeSet::new();
            let mut src = IndexMap::new();
            for col in cols {
                if exclude.contains(&col.name.as_str()) || *ordinal == Some(col.name.as_str()) {
                    continue;
                }
                keys.insert(col.name.clone());
                src.insert(col.name, col.kind);
            }
            Ok((keys, src))
        }
    }
}

/// `w(__ord, j{i}, jt{i}, tx{i})`: raw value, JSON-ish type and text per present field.
///
/// `jt` is a `json_type` name for JSON values (`VARCHAR` = a JSON string), `TEXT` for an untyped
/// text column (parsed), or a typed column's category (`BIGINT`, `DOUBLE`, `DECIMAL`, `BOOLEAN`,
/// `DATE`, `TIMESTAMP`, `TIMESTAMPTZ`, `OBJECT`, `ARRAY`, `OTHER`).
fn projection(staged: &Staged<'_>, present: &[(&FieldSql, String)]) -> String {
    let ord = staged
        .ordinal()
        .map(ident)
        .unwrap_or_else(|| "NULL::BIGINT".into());
    let rel = staged.relation();
    let mut cols = vec!["__ord".to_owned()];
    match staged {
        Staged::Json { column, .. } => {
            let mut inner = vec!["__ord".to_owned()];
            // TODO(perf): each field rescans the whole record text, O(fields × record size) per
            // row; one pass returning every field's exact text would fix it.
            for (i, (f, _)) in present.iter().enumerate() {
                inner.push(format!(
                    "json_extract(__r, {}) AS j{i}, {EXACT}(CAST(__r AS VARCHAR), {}) AS x{i}",
                    literal(&json_pointer(&f.name)),
                    literal(&f.name)
                ));
                cols.push(format!(
                    "j{i}, json_type(j{i}) AS jt{i}, {} AS tx{i}",
                    json_text(&format!("j{i}"), &format!("x{i}"))
                ));
            }
            format!(
                "w AS (SELECT {} FROM (SELECT {} FROM (SELECT {ord} AS __ord, CAST({} AS JSON) AS __r FROM {rel})))",
                cols.join(", "),
                inner.join(", "),
                ident(column)
            )
        }
        Staged::Columns { .. } => {
            for (i, (f, kind)) in present.iter().enumerate() {
                let col = ident(&f.name);
                let cat = category(kind);
                if cat == "JSON" {
                    cols.push(format!(
                        "{col} AS j{i}, json_type({col}) AS jt{i}, {} AS tx{i}",
                        json_text(&col, &format!("{EXACT}(CAST({col} AS VARCHAR))"))
                    ));
                } else {
                    cols.push(format!(
                        "{col} AS j{i}, '{cat}' AS jt{i}, CAST({col} AS VARCHAR) AS tx{i}"
                    ));
                }
            }
            format!(
                "w AS (SELECT {ord} AS {}, {} FROM {rel})",
                cols[0],
                cols[1..].join(", ")
            )
        }
    }
}

const EXACT: &str = atlatl_engine::json_exact::FUNCTION;
const DECIMAL_TEXT: &str = atlatl_engine::decimal_text::FUNCTION;

/// `json_extract_string` of JSON `j`, but with the number literals of `exact` (its text by
/// [`EXACT`]): DuckDB's extraction rounds fractional numbers through a double.
fn json_text(j: &str, exact: &str) -> String {
    format!(
        "CASE json_type({j}) WHEN 'VARCHAR' THEN json_extract_string({j}, '$') WHEN 'NULL' THEN NULL \
         ELSE coalesce({exact}, json_extract_string({j}, '$')) END"
    )
}

/// Registers [`EXACT`] and [`DECIMAL_TEXT`] on a connection outside the engine, which registers
/// them itself.
fn ensure_exact(c: &Connection) -> Result<()> {
    ensure_udf::<atlatl_engine::json_exact::JsonExact>(c, EXACT)?;
    ensure_udf::<atlatl_engine::decimal_text::DecimalText>(c, DECIMAL_TEXT)
}

fn ensure_udf<T: VScalar<State = ()>>(c: &Connection, name: &str) -> Result<()> {
    let has = |c: &Connection| -> Result<bool> {
        c.query_row(
            "SELECT count(*) > 0 FROM duckdb_functions() WHERE function_name = ?",
            [name],
            |r| r.get(0),
        )
        .map_err(db)
    };
    if has(c)? {
        return Ok(());
    }
    match c.register_scalar_function::<T>(name) {
        Err(_) if has(c)? => Ok(()),
        r => r.map_err(db),
    }
}

/// RFC 6901.
fn json_pointer(key: &str) -> String {
    format!("/{}", key.replace('~', "~0").replace('/', "~1"))
}

fn category(duck: &str) -> &'static str {
    let t = duck.to_ascii_uppercase();
    if t.ends_with(']') {
        return "ARRAY";
    }
    if t.starts_with("STRUCT") || t.starts_with("MAP") || t.starts_with("UNION") {
        return "OBJECT";
    }
    if t.starts_with("DECIMAL") {
        return "DECIMAL";
    }
    match t.as_str() {
        "VARCHAR" => "TEXT",
        "JSON" => "JSON",
        "TINYINT" | "SMALLINT" | "INTEGER" | "BIGINT" | "HUGEINT" | "UTINYINT" | "USMALLINT"
        | "UINTEGER" | "UBIGINT" | "UHUGEINT" => "BIGINT",
        "FLOAT" | "DOUBLE" => "DOUBLE",
        "BOOLEAN" => "BOOLEAN",
        "DATE" => "DATE",
        "TIMESTAMP" | "TIMESTAMP_S" | "TIMESTAMP_MS" | "TIMESTAMP_NS" => "TIMESTAMP",
        "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => "TIMESTAMPTZ",
        _ => "OTHER",
    }
}

struct FieldSql<'a> {
    name: String,
    spec: &'a FieldSpec,
    number_format: NumberFormat,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum NumberFormat {
    Plain,
    ThousandsSep,
}

struct Predicates {
    null: String,
    /// Non-null and not valid for the declared type.
    kind: String,
    /// Non-null, valid, and outside `values`.
    domain: Option<String>,
}

const INT_RE: &str = "[+-]?[0-9]+";
const DEC_RE: &str = r"[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?";
const TZ_RE: &str = r"[0-9]{2}:[0-9]{2}(:[0-9]{2}(\.[0-9]+)?)?\s*([zZ]|[+-][0-9]{2}(:?[0-9]{2})?)$";

fn is_numeric(t: LogicalType) -> bool {
    matches!(
        t,
        LogicalType::Integer | LogicalType::Decimal | LogicalType::Float
    )
}

fn is_temporal(t: LogicalType) -> bool {
    matches!(
        t,
        LogicalType::Date | LogicalType::Timestamp | LogicalType::Timestamptz
    )
}

impl<'a> FieldSql<'a> {
    fn new(name: &str, spec: &'a FieldSpec) -> Result<Self> {
        let bad = |m: String| {
            Err(AtlatlError::config(format!(
                "source schema field `{name}`: {m}"
            )))
        };
        let number_format = match (spec.kind, spec.format.as_deref()) {
            (_, None) => NumberFormat::Plain,
            (k, Some("plain")) if is_numeric(k) => NumberFormat::Plain,
            (k, Some("thousands_sep")) if is_numeric(k) => NumberFormat::ThousandsSep,
            (k, Some(f)) if is_numeric(k) => {
                return bad(format!(
                    "number format `{f}` is not `plain` or `thousands_sep`"
                ));
            }
            (k, Some(_)) if is_temporal(k) => NumberFormat::Plain,
            (_, Some(f)) => {
                return bad(format!(
                    "`format: {f}` applies only to temporal and numeric types"
                ));
            }
        };
        if let Some(vals) = &spec.values {
            for v in vals {
                if is_numeric(spec.kind) && number_literal(v).is_none() {
                    return bad(format!("`values` entry {v} is not a number"));
                }
                if spec.kind == LogicalType::Boolean && bool_literal(v).is_none() {
                    return bad(format!("`values` entry {v} is not a boolean"));
                }
                if spec.kind == LogicalType::Decimal {
                    match decimal_value(v) {
                        None => return bad(format!("`values` entry {v} is not a decimal number")),
                        Some((_, d, e)) if decimal_width(&d, e) > 38 => {
                            return bad(format!(
                                "`values` entry {v} needs more than 38 digits; DuckDB's DECIMAL holds at most 38"
                            ));
                        }
                        Some(_) => {}
                    }
                }
            }
        }
        Ok(Self {
            name: name.to_owned(),
            spec,
            number_format,
        })
    }

    fn predicates(&self, i: usize) -> Predicates {
        let (j, jt, tx) = (format!("j{i}"), format!("jt{i}"), format!("tx{i}"));
        let null = format!("({j} IS NULL OR coalesce({jt} = 'NULL', FALSE))");
        let valid = valid_sql(
            self.spec.kind,
            self.spec.format.as_deref(),
            self.num(&tx),
            &jt,
            &tx,
        );
        let kind = format!("(NOT {null} AND NOT coalesce({valid}, FALSE))");
        let domain = self.spec.values.as_ref().map(|vals| {
            let inside = domain_sql(self.spec.kind, vals, &self.num(&tx), &jt, &tx);
            format!("(NOT {null} AND coalesce({valid}, FALSE) AND NOT coalesce({inside}, FALSE))")
        });
        Predicates { null, kind, domain }
    }

    fn num(&self, tx: &str) -> String {
        match self.number_format {
            NumberFormat::Plain => tx.to_owned(),
            NumberFormat::ThousandsSep => format!("replace({tx}, ',', '')"),
        }
    }
}

fn valid_sql(kind: LogicalType, format: Option<&str>, num: String, jt: &str, tx: &str) -> String {
    let text = format!("{jt} IN ('VARCHAR', 'TEXT')");
    let parse = |cast: &str| match format {
        Some(f) => format!("try_strptime({tx}, {}) IS NOT NULL", literal(f)),
        // DuckDB's DATE cast also takes timestamps.
        None if cast == "DATE" => format!(
            "(regexp_full_match(trim({tx}), '-?[0-9]{{4,}}-[0-9]{{1,2}}-[0-9]{{1,2}}') AND TRY_CAST({tx} AS DATE) IS NOT NULL)"
        ),
        None => format!("TRY_CAST({tx} AS {cast}) IS NOT NULL"),
    };
    match kind {
        LogicalType::String => format!("{jt} NOT IN ('OBJECT', 'ARRAY')"),
        LogicalType::Integer => format!(
            "CASE WHEN {jt} IN ('BIGINT', 'UBIGINT') THEN TRUE \
             WHEN {jt} IN ('VARCHAR', 'TEXT', 'DOUBLE', 'DECIMAL') THEN regexp_full_match({num}, '{INT_RE}') \
             ELSE FALSE END"
        ),
        LogicalType::Decimal => format!(
            "CASE WHEN {jt} IN ('BIGINT', 'UBIGINT', 'DECIMAL') THEN TRUE \
             WHEN {jt} IN ('VARCHAR', 'TEXT', 'DOUBLE') THEN regexp_full_match({num}, '{DEC_RE}') \
             ELSE FALSE END"
        ),
        LogicalType::Float => format!(
            "CASE WHEN {jt} IN ('BIGINT', 'UBIGINT', 'DOUBLE', 'DECIMAL') THEN TRUE \
             WHEN {text} THEN TRY_CAST({num} AS DOUBLE) IS NOT NULL ELSE FALSE END"
        ),
        LogicalType::Boolean => format!(
            "CASE WHEN {jt} = 'BOOLEAN' THEN TRUE \
             WHEN {jt} IN ('VARCHAR', 'TEXT', 'BIGINT', 'UBIGINT') THEN lower({tx}) IN ('true', 'false', 't', 'f', '1', '0') \
             ELSE FALSE END"
        ),
        LogicalType::Date => format!(
            "CASE WHEN {jt} = 'DATE' THEN TRUE WHEN {text} THEN {} ELSE FALSE END",
            parse("DATE")
        ),
        LogicalType::Timestamp => format!(
            "CASE WHEN {jt} = 'TIMESTAMP' THEN TRUE WHEN {text} THEN {} ELSE FALSE END",
            parse("TIMESTAMP")
        ),
        LogicalType::Timestamptz => {
            let p = match format {
                Some(_) => parse("TIMESTAMPTZ"),
                None => format!(
                    "(TRY_CAST({tx} AS TIMESTAMPTZ) IS NOT NULL AND regexp_matches({tx}, '{TZ_RE}'))"
                ),
            };
            format!("CASE WHEN {jt} = 'TIMESTAMPTZ' THEN TRUE WHEN {text} THEN {p} ELSE FALSE END")
        }
        LogicalType::Json => format!(
            "CASE WHEN {jt} IN ('OBJECT', 'ARRAY') THEN TRUE \
             WHEN {jt} = 'TEXT' THEN json_valid({tx}) AND regexp_matches({tx}, '^\\s*[\\[{{]') \
             ELSE FALSE END"
        ),
    }
}

fn number_literal(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn bool_literal(v: &Value) -> Option<bool> {
    match v {
        Value::Bool(b) => Some(*b),
        Value::String(s) => match s.to_ascii_lowercase().as_str() {
            "true" | "t" | "1" => Some(true),
            "false" | "f" | "0" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn text_literal(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn domain_sql(kind: LogicalType, vals: &[Value], num: &str, jt: &str, tx: &str) -> String {
    let vals: Vec<&Value> = vals.iter().filter(|v| !v.is_null()).collect();
    if vals.is_empty() {
        return "FALSE".into();
    }
    if kind == LogicalType::Integer {
        let list = vals
            .iter()
            .filter_map(|v| integer_literal(v))
            .map(|i| format!("'{i}'::HUGEINT"))
            .collect::<Vec<_>>()
            .join(", ");
        if list.is_empty() {
            return "FALSE".into();
        }
        format!("TRY_CAST({num} AS HUGEINT) IN ({list})")
    } else if kind == LogicalType::Decimal {
        decimal_domain_sql(&vals, num)
    } else if kind == LogicalType::Float {
        let list = vals
            .iter()
            .filter_map(|v| number_literal(v))
            .map(|f| format!("{f:?}::DOUBLE"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("TRY_CAST({num} AS DOUBLE) IN ({list})")
    } else if kind == LogicalType::Boolean {
        let list = vals
            .iter()
            .filter_map(|v| bool_literal(v))
            .map(|b| b.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "(CASE WHEN {jt} = 'BOOLEAN' THEN {tx} = 'true' ELSE lower({tx}) IN ('true', 't', '1') END) IN ({list})"
        )
    } else {
        let list = vals
            .iter()
            .map(|v| literal(&text_literal(v)))
            .collect::<Vec<_>>()
            .join(", ");
        format!("{tx} IN ({list})")
    }
}

fn integer_literal(v: &Value) -> Option<i128> {
    match v {
        Value::Number(n) => n
            .as_i64()
            .map(i128::from)
            .or_else(|| n.as_u64().map(i128::from))
            .or_else(|| {
                n.as_f64()
                    .filter(|f| f.fract() == 0.0 && f.abs() < 2f64.powi(127))
                    .map(|f| f as i128)
            }),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn decimal_value(v: &Value) -> Option<(bool, String, i64)> {
    match v {
        Value::Number(n) => decimal_parts(&n.to_string()),
        Value::String(s) => decimal_parts(s),
        _ => None,
    }
}

fn decimal_scale(exp: i64) -> i64 {
    exp.saturating_neg().max(0)
}

fn decimal_int_digits(digits: &str, exp: i64) -> i64 {
    (digits.len() as i64).saturating_add(exp).max(0)
}

/// Digits needed to hold the value exactly in a `DECIMAL(p,s)`.
fn decimal_width(digits: &str, exp: i64) -> i64 {
    decimal_scale(exp).saturating_add(decimal_int_digits(digits, exp))
}

/// Exact membership, per group of allowed values sharing a scale `s`: the input must equal one at
/// `s` and carry no digits past `s`, checked against a cast at the widest scale `s2` the group's
/// integer digits leave room for. DuckDB rounds digits past `s2`, so the input's own text must
/// also need at most `s2` places. The input is first spelled plainly by [`DECIMAL_TEXT`]: DuckDB
/// won't cast an exponent form with integer mantissa digits to a high-scale DECIMAL.
fn decimal_domain_sql(vals: &[&Value], num: &str) -> String {
    let mut groups: BTreeMap<i64, Vec<(bool, String, i64)>> = BTreeMap::new();
    for (n, d, e) in vals.iter().filter_map(|v| decimal_value(v)) {
        if decimal_width(&d, e) <= 38 {
            groups.entry(decimal_scale(e)).or_default().push((n, d, e));
        }
    }
    if groups.is_empty() {
        return "FALSE".into();
    }
    let num = format!("{DECIMAL_TEXT}({num})");
    let places = format!("length(split_part({num}, '.', 2))");
    let checks = groups
        .iter()
        .map(|(s, parts)| {
            let ip = parts
                .iter()
                .map(|(_, d, e)| decimal_int_digits(d, *e))
                .max()
                .unwrap_or(0);
            let s2 = (38 - ip.max(1)).max(*s);
            let list = parts
                .iter()
                .map(|(n, d, e)| format!("'{}'::DECIMAL(38,{s})", decimal_text(*n, d, *e)))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "(TRY_CAST({num} AS DECIMAL(38,{s})) IN ({list}) AND {places} <= {s2} \
                 AND TRY_CAST(TRY_CAST({num} AS DECIMAL(38,{s})) AS DECIMAL(38,{s2})) = TRY_CAST({num} AS DECIMAL(38,{s2})))"
            )
        })
        .collect::<Vec<_>>();
    format!("({})", checks.join(" OR "))
}

/// Fails early, as a config error, on a strftime format DuckDB rejects.
fn check_formats(c: &Connection, fields: &[FieldSql<'_>]) -> Result<()> {
    for f in fields {
        if let (true, Some(fmt)) = (is_temporal(f.spec.kind), f.spec.format.as_deref()) {
            c.query_row(
                &format!("SELECT try_strptime('2000-01-01', {})", literal(fmt)),
                [],
                |_| Ok(()),
            )
            .map_err(|e| {
                AtlatlError::config(format!(
                    "source schema field `{}`: invalid format `{fmt}`: {e}",
                    f.name
                ))
            })?;
        }
    }
    Ok(())
}

fn samples(
    c: &Connection,
    w: &str,
    i: usize,
    pred: &str,
    kind: ViolationKind,
) -> Result<Vec<Value>> {
    let sql =
        format!("WITH {w} SELECT __ord, tx{i} FROM w WHERE {pred} ORDER BY __ord LIMIT {SAMPLES}");
    let mut st = c.prepare(&sql).map_err(db)?;
    let rows = st
        .query_map([], |r| {
            let ord: Option<i64> = r.get(0)?;
            let v: Option<String> = r.get(1)?;
            Ok((ord, v))
        })
        .map_err(db)?;
    let mut out = Vec::new();
    for r in rows {
        let (ord, v) = r.map_err(db)?;
        let value = match (kind, v) {
            (ViolationKind::Null, _) | (_, None) => Value::Null,
            (_, Some(s)) => Value::String(atlatl_core::redact::redact(&s).into_owned()),
        };
        out.push(match ord {
            Some(o) => json!({"ordinal": o, "value": value}),
            None => json!({"value": value}),
        });
    }
    Ok(out)
}

// ----------------------------------------------------------------------------- inference

/// Per-field profile behind an inferred schema.
#[derive(Clone, Debug, PartialEq)]
pub struct FieldProfile {
    pub name: String,
    pub kind: LogicalType,
    pub nulls: u64,
    pub non_null: u64,
}

#[derive(Clone, Debug)]
pub struct Inferred {
    pub schema: SourceSchema,
    pub records: u64,
    pub fields: Vec<FieldProfile>,
}

/// Narrowest first; `string` always fits.
const CANDIDATES: &[LogicalType] = &[
    LogicalType::Boolean,
    LogicalType::Integer,
    LogicalType::Decimal,
    LogicalType::Float,
    LogicalType::Date,
    LogicalType::Timestamptz,
    LogicalType::Timestamp,
    LogicalType::Json,
];

/// Drafts a source schema (`mode: observe`) from the staged records: for each field, the
/// narrowest logical type every non-null value fits, and `nullable: false` when no nulls were
/// seen. Digit strings with leading zeros stay `string`; booleans must be `true`/`false`.
pub fn infer(c: &Connection, staged: &Staged<'_>) -> Result<Inferred> {
    ensure_exact(c)?;
    let (seen, sources) = field_sources(c, staged)?;
    let rel = staged.relation();
    let n = count(c, &rel)?;
    let names: Vec<String> = match staged {
        Staged::Columns { .. } => sources.keys().cloned().collect(),
        Staged::Json { .. } => first_seen_order(c, staged, &seen)?,
    };
    let specs: Vec<FieldSpec> = names
        .iter()
        .map(|_| FieldSpec {
            kind: LogicalType::String,
            format: None,
            nullable: None,
            values: None,
        })
        .collect();
    let fields: Vec<FieldSql> = names
        .iter()
        .zip(&specs)
        .map(|(n, s)| FieldSql {
            name: n.clone(),
            spec: s,
            number_format: NumberFormat::Plain,
        })
        .collect();
    let present: Vec<(&FieldSql, String)> = fields
        .iter()
        .map(|f| (f, sources.get(&f.name).cloned().unwrap_or_default()))
        .collect();
    let mut profiles = Vec::new();
    if !present.is_empty() && n > 0 {
        let w = projection(staged, &present);
        let mut aggs = Vec::new();
        for (i, (f, _)) in present.iter().enumerate() {
            let p = f.predicates(i);
            aggs.push(format!("count_if({})", p.null));
            let (jt, tx) = (format!("jt{i}"), format!("tx{i}"));
            for k in CANDIDATES {
                let mut v = valid_sql(*k, None, tx.clone(), &jt, &tx);
                match k {
                    LogicalType::Boolean => {
                        v = format!(
                            "({jt} = 'BOOLEAN' OR ({jt} IN ('VARCHAR', 'TEXT') AND lower({tx}) IN ('true', 'false')))"
                        )
                    }
                    LogicalType::Integer | LogicalType::Decimal | LogicalType::Float => {
                        v = format!(
                            "(({v}) AND NOT ({jt} IN ('VARCHAR', 'TEXT') AND regexp_matches({tx}, '^[+-]?0[0-9]')))"
                        )
                    }
                    _ => {}
                }
                aggs.push(format!(
                    "count_if(NOT {} AND NOT coalesce({v}, FALSE))",
                    p.null
                ));
            }
        }
        let sql = format!("WITH {w} SELECT {} FROM w", aggs.join(", "));
        let per = 1 + CANDIDATES.len();
        let row: Vec<i64> = c
            .query_row(&sql, [], |r| {
                (0..aggs.len()).map(|i| r.get::<_, i64>(i)).collect()
            })
            .map_err(db)?;
        for (i, (f, _)) in present.iter().enumerate() {
            let nulls = row[per * i] as u64;
            let non_null = n - nulls;
            let kind = if non_null == 0 {
                LogicalType::String
            } else {
                CANDIDATES
                    .iter()
                    .enumerate()
                    .find(|(k, _)| row[per * i + 1 + k] == 0)
                    .map(|(_, t)| *t)
                    .unwrap_or(LogicalType::String)
            };
            profiles.push(FieldProfile {
                name: f.name.clone(),
                kind,
                nulls,
                non_null,
            });
        }
    }
    let schema = SourceSchema {
        mode: Some(ConfigMode::Observe),
        fields: profiles
            .iter()
            .map(|p| {
                (
                    p.name.clone(),
                    FieldSpec {
                        kind: p.kind,
                        format: None,
                        nullable: (p.nulls == 0 && p.non_null > 0).then_some(false),
                        values: None,
                    },
                )
            })
            .collect(),
        on_new_field: None,
        on_missing_field: None,
        on_type_violation: None,
        on_null_violation: None,
        on_domain_violation: None,
        tolerance: None,
    };
    Ok(Inferred {
        schema,
        records: n,
        fields: profiles,
    })
}

/// JSON keys ordered by the first record (ordinal order) that has them, then key order.
fn first_seen_order(
    c: &Connection,
    staged: &Staged<'_>,
    seen: &BTreeSet<String>,
) -> Result<Vec<String>> {
    let Staged::Json { column, .. } = staged else {
        return Ok(seen.iter().cloned().collect());
    };
    let ord = staged.ordinal().map(ident).unwrap_or_else(|| "0".into());
    let sql = format!(
        "SELECT k FROM (SELECT o, unnest(range(1, len(keys) + 1)) AS p, unnest(keys) AS k \
         FROM (SELECT {ord} AS o, json_keys(CAST({col} AS JSON)) AS keys FROM {rel})) \
         GROUP BY k ORDER BY min(o), min(p), k",
        col = ident(column),
        rel = staged.relation()
    );
    let mut st = c.prepare(&sql).map_err(db)?;
    let rows = st.query_map([], |r| r.get::<_, String>(0)).map_err(db)?;
    rows.map(|r| r.map_err(db)).collect()
}

/// A reviewable `schemas/<stream id>.yaml`.
pub fn to_yaml(schema: &SourceSchema, header: &str) -> String {
    let mut out = String::new();
    for l in header.lines() {
        out.push_str(&format!("# {l}\n"));
    }
    out.push_str("schema:\n");
    if let Some(m) = schema.mode {
        out.push_str(&format!(
            "  mode: {}\n",
            match m {
                ConfigMode::Enforce => "enforce",
                ConfigMode::Observe => "observe",
            }
        ));
    }
    if schema.fields.is_empty() {
        out.push_str("  fields: {}\n");
    } else {
        out.push_str("  fields:\n");
    }
    for (name, f) in &schema.fields {
        let mut parts = vec![format!("type: {}", type_name(f.kind))];
        if let Some(fmt) = &f.format {
            parts.push(format!("format: {}", yaml_str(fmt)));
        }
        if let Some(n) = f.nullable {
            parts.push(format!("nullable: {n}"));
        }
        if let Some(v) = &f.values {
            parts.push(format!(
                "values: [{}]",
                v.iter()
                    .map(|x| match x {
                        Value::String(s) => yaml_str(s),
                        o => o.to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        out.push_str(&format!(
            "    {}: {{{}}}\n",
            yaml_str(name),
            parts.join(", ")
        ));
    }
    let pol = |k: &str, p: Option<Policy>, out: &mut String| {
        if let Some(p) = p {
            let s = match p {
                Policy::Ignore => "ignore",
                Policy::Warn => "warn",
                Policy::Fail => "fail",
            };
            out.push_str(&format!("  {k}: {s}\n"));
        }
    };
    pol("on_new_field", schema.on_new_field, &mut out);
    pol("on_missing_field", schema.on_missing_field, &mut out);
    pol("on_type_violation", schema.on_type_violation, &mut out);
    pol("on_null_violation", schema.on_null_violation, &mut out);
    pol("on_domain_violation", schema.on_domain_violation, &mut out);
    if let Some(t) = &schema.tolerance {
        let mut parts = Vec::new();
        if let Some(r) = t.ratio {
            parts.push(format!("ratio: {r}"));
        }
        if let Some(m) = t.min_count {
            parts.push(format!("min_count: {m}"));
        }
        out.push_str(&format!("  tolerance: {{{}}}\n", parts.join(", ")));
    }
    out
}

pub fn type_name(t: LogicalType) -> &'static str {
    match t {
        LogicalType::String => "string",
        LogicalType::Integer => "integer",
        LogicalType::Decimal => "decimal",
        LogicalType::Float => "float",
        LogicalType::Boolean => "boolean",
        LogicalType::Date => "date",
        LogicalType::Timestamp => "timestamp",
        LogicalType::Timestamptz => "timestamptz",
        LogicalType::Json => "json",
    }
}

/// Plain when unambiguous, else double-quoted (JSON escapes are valid YAML).
fn yaml_str(s: &str) -> String {
    let plain = !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/'))
        && s.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
        && !matches!(
            s.to_ascii_lowercase().as_str(),
            "true" | "false" | "yes" | "no" | "on" | "off" | "null" | "y" | "n"
        );
    if plain {
        s.to_owned()
    } else {
        Value::String(s.to_owned()).to_string()
    }
}

/// Stages `(ordinal BIGINT, record JSON)` rows into a temp table (for `schema infer`/`check`
/// from an API sample). `records` are JSON texts.
pub fn stage_json<I, S>(c: &Connection, name: &str, records: I) -> Result<u64>
where
    I: IntoIterator<Item = (i64, S)>,
    S: AsRef<str>,
{
    c.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE {} (ordinal BIGINT, record JSON)",
        ident(name)
    ))
    .map_err(db)?;
    let mut n = 0u64;
    {
        let mut a = c.appender(name).map_err(db)?;
        for (o, r) in records {
            a.append_row(duckdb::params![o, r.as_ref()]).map_err(db)?;
            n += 1;
        }
        a.flush().map_err(db)?;
    }
    Ok(n)
}

#[cfg(test)]
mod tests;
