//! Postgres as an ingest source (06 §3.2): catalog discovery and column metadata over
//! tokio-postgres in read-only sessions, and reads through DuckDB's postgres extension attached
//! `READ_ONLY` (every scan runs in a server-side `READ ONLY` transaction).

use std::sync::Arc;

use atlatl_core::config::model::{CAST_NULLED, SslMode};
use atlatl_core::error::Result;
use atlatl_core::redact;
use atlatl_core::{AtlatlError, ErrorClass};
use rustls::ClientConfig;
use tokio::sync::Mutex;

use crate::connect::{ConnectOptions, Session, connect_with};
use crate::error::wrap;
pub use crate::types::numeric_precision;
use crate::types::{
    Class, Refusal, Target, Text, classify, duck_literal, guarded, hint_ident, message, nulling,
    refused, target,
};

/// How a column is read so the staged value is exact (06 §3.2, [`crate::types`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Read {
    /// The scanner's own type mapping.
    Native,
    /// A domain over a type read natively: cast to its base type in Postgres (the scanner reads
    /// domains as text).
    Base,
    /// `numeric(p,s)` within 38 digits; `NaN`, which the scanner reads as NULL, fails the read
    /// (see [`unlandable`]).
    Decimal,
    /// Not landed as is; see [`admit`].
    Refused(Refusal),
    /// A refused column opted in by `columns.<c>.type`: its exact text, checked into `kind`.
    /// With `set_null` (`on_cast_error.set_null`) a value that doesn't fit reads as NULL, flagged
    /// in a `CAST_NULLED` column beside it.
    Declared {
        text: Text,
        kind: String,
        target: Target,
        set_null: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceColumn {
    pub name: String,
    /// `format_type(atttypid, atttypmod)`: the declared type, a domain's own name.
    pub pg_type: String,
    /// The base type, `format_type` of it with its typmod (`pg_type` unless a domain).
    pub base: String,
    pub domain: bool,
    pub read: Read,
    /// The base type's `pg_type.typname`.
    pub typname: String,
}

impl SourceColumn {
    pub fn new(
        name: &str,
        pg_type: &str,
        domain: bool,
        base: (&str, &str, &str, i32, &str),
    ) -> Self {
        let (typtype, typcategory, typname, typmod, base_type) = base;
        let read = match classify(typtype, typcategory, typname, typmod) {
            Class::Native if domain => Read::Base,
            Class::Native => Read::Native,
            Class::Decimal => Read::Decimal,
            Class::Refused(r) => Read::Refused(r),
        };
        Self {
            name: name.to_owned(),
            pg_type: pg_type.to_owned(),
            base: base_type.to_owned(),
            domain,
            read,
            typname: typname.to_owned(),
        }
    }

    /// The column as Postgres SQL, as its base type.
    fn base_sql(&self) -> String {
        let i = ident(&self.name);
        if self.domain {
            format!("{i}::{}", self.base)
        } else {
            i
        }
    }
}

/// DuckDB's last timestamp, in microseconds since 1970 UTC. Postgres's run to 294276 AD; the
/// scanner lands the next microsecond as `infinity` and wraps later ones to BC.
pub const LAST_TIMESTAMP: &str = "294247-01-10 04:00:54.775806";

/// A value the scanner can't land exactly, looked for in Postgres: the predicate (Postgres SQL on
/// the column as its base type) and the error's tail after `holds <value>`.
fn unlandable(c: &SourceColumn) -> Option<(String, String)> {
    let (e, h) = (c.base_sql(), hint_ident(&c.name));
    match (&c.read, c.typname.as_str()) {
        (Read::Decimal, _) => Some((
            format!("{e} = 'NaN'"),
            format!(
                ", which a decimal can't hold; convert it in `from.query` (`NULLIF({h}, 'NaN')` to read it as NULL, or `{h}::float8`)"
            ),
        )),
        (Read::Native | Read::Base, t @ ("timestamp" | "timestamptz")) => {
            let last = match t {
                "timestamptz" => format!("{LAST_TIMESTAMP}+00"),
                _ => LAST_TIMESTAMP.to_owned(),
            };
            Some((
                format!("{e} > '{last}'::{t} AND {e} < 'infinity'"),
                format!(
                    ", past the last timestamp the target holds ({last}); convert it in `from.query` (`{h}::text`, or a `CASE` mapping it to 'infinity')"
                ),
            ))
        }
        _ => None,
    }
}

/// Opts refused columns in by their declared type (`declared(name)`: `columns.<c>.type`, or
/// `"*"`); every column still refused, or declared as a type its text can't be checked against,
/// is a line in the error.
pub fn admit(
    cols: Vec<SourceColumn>,
    declared: &dyn Fn(&str) -> Option<String>,
) -> std::result::Result<Vec<SourceColumn>, Vec<String>> {
    let mut bad = Vec::new();
    let out = cols
        .into_iter()
        .map(|mut c| {
            if let Read::Refused(r) = &c.read {
                let base = c.domain.then_some(c.base.as_str());
                match declared(&c.name) {
                    None => bad.push(message(&c.name, &c.pg_type, base, r)),
                    Some(kind) => match target(&kind) {
                        Some(t) => {
                            c.read = Read::Declared {
                                text: r.text,
                                kind,
                                target: t,
                                set_null: false,
                            }
                        }
                        None => bad.push(format!(
                            "column `{}` is {}, and `columns.{}.type: {kind}` can't be checked against exactly from its text; declare VARCHAR, JSON, DECIMAL(p,s), an integer or a float type (e.g. {}), or convert it in `from.query` ({})",
                            c.name,
                            c.pg_type,
                            c.name,
                            r.declare,
                            r.convert.replace("{c}", &hint_ident(&c.name))
                        )),
                    },
                }
            }
            c
        })
        .collect();
    if bad.is_empty() { Ok(out) } else { Err(bad) }
}

/// `cols` narrowed by `columns.include`/`exclude` (exact name, else case-insensitive).
pub fn narrow(
    cols: Vec<SourceColumn>,
    include: Option<&[String]>,
    exclude: &[String],
) -> Vec<SourceColumn> {
    let named = |list: &[String], n: &str| {
        list.iter().any(|x| x == n) || list.iter().any(|x| x.eq_ignore_ascii_case(n))
    };
    cols.into_iter()
        .filter(|c| include.is_none_or(|i| named(i, &c.name)))
        .filter(|c| !named(exclude, &c.name))
        .collect()
}

/// An `update_key` cursor to evaluate in Postgres (06 §3.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cursor {
    pub column: String,
    /// The stored watermark, as text.
    pub stored: Option<String>,
    /// `lookback` in microseconds.
    pub lookback_us: Option<u64>,
    pub inclusive: bool,
    pub include_nulls: bool,
}

/// The type a watermark literal is cast to, for types whose Postgres comparison matches the
/// engine's on the value the scanner reads: integers, floats and numerics, dates and times, and
/// text (compared as `C`, i.e. byte-wise).
fn comparable(typname: &str) -> Option<&'static str> {
    Some(match typname {
        "int2" => "int2",
        "int4" => "int4",
        "int8" => "int8",
        "float4" => "float4",
        "float8" => "float8",
        "numeric" => "numeric",
        "date" => "date",
        "time" => "time",
        "timestamp" => "timestamp",
        "timestamptz" => "timestamptz",
        "text" | "varchar" => "text",
        _ => return None,
    })
}

/// The cursor predicate in Postgres SQL, when it is safe there: the column is read as it is stored
/// (not converted in Postgres) and has a comparable type. The watermark is a quoted literal cast
/// to the column's own type; text compares byte-wise like the engine.
pub fn cursor_predicate(cur: &Cursor, cols: &[SourceColumn]) -> Option<String> {
    let col = cols.iter().find(|c| c.name == cur.column).or_else(|| {
        cols.iter()
            .find(|c| c.name.eq_ignore_ascii_case(&cur.column))
    })?;
    if !matches!(col.read, Read::Native | Read::Decimal) || col.domain {
        return None;
    }
    let ty = comparable(&col.typname)?;
    let text = ty == "text";
    if cur.lookback_us.is_some() && !matches!(ty, "date" | "timestamp" | "timestamptz") {
        return None;
    }
    let i = ident(&col.name);
    let lhs = if text {
        format!("{i} COLLATE \"C\"")
    } else {
        i.clone()
    };
    let bound = cur.stored.as_ref().map(|v| {
        let lit = format!("{}::{ty}", literal(v));
        let lit = match cur.lookback_us {
            Some(n) => format!(
                "({lit} - {}::interval)",
                literal(&atlatl_core::config::types::interval_text(n))
            ),
            None => lit,
        };
        format!("{lhs} {} {lit}", if cur.inclusive { ">=" } else { ">" })
    });
    Some(match (bound, cur.include_nulls) {
        (Some(b), true) => format!("({b} OR {i} IS NULL)"),
        (Some(b), false) => b,
        (None, true) => return None,
        (None, false) => format!("{i} IS NOT NULL"),
    })
}

/// A Postgres predicate keeping at least every row `window` (DuckDB SQL) keeps that a [`guard`]
/// could fail on: its top-level `AND`ed comparisons, `BETWEEN`s and null tests of a column read as
/// stored against a literal both compare it to exactly, and lower bounds on a date or timestamp
/// that only move forward (`current_date - 45`, `now() - INTERVAL 1 HOUR`), taken now, before the
/// read takes them. A column with [`unlandable`] values also keeps those (the guard fails on them
/// wherever they are tested). Anything else is left out, which only widens it; `None` when nothing
/// is left. `eval` runs a query of one `VARCHAR` in the engine (`None` on error).
pub fn window_predicate(
    window: &str,
    cols: &[SourceColumn],
    eval: &dyn Fn(&str) -> Option<String>,
) -> Option<String> {
    let ast = eval(&format!(
        "SELECT json_serialize_sql({})",
        literal(&format!("SELECT * FROM (SELECT 1) AS x WHERE {window}"))
    ))?;
    let v: serde_json::Value = serde_json::from_str(&ast).ok()?;
    if v["error"] != false || v["statements"].as_array().is_none_or(|s| s.len() != 1) {
        return None;
    }
    let bound = |e: &serde_json::Value, duck: &str| {
        let mut q = v.clone();
        let n = &mut q["statements"][0]["node"];
        n["select_list"] = serde_json::Value::Array(vec![e.clone()]);
        n["where_clause"] = serde_json::Value::Null;
        eval(&format!(
            "SELECT CAST(c AS {duck})::VARCHAR FROM json_execute_serialized_sql({}) AS q(c)",
            literal(&q.to_string())
        ))
    };
    let mut out = Vec::new();
    conjuncts(
        &v["statements"][0]["node"]["where_clause"],
        cols,
        &bound,
        &mut out,
    );
    (!out.is_empty()).then(|| out.join(" AND "))
}

type Bound<'a> = &'a dyn Fn(&serde_json::Value, &str) -> Option<String>;

fn conjuncts(e: &serde_json::Value, cols: &[SourceColumn], bound: Bound, out: &mut Vec<String>) {
    if e["type"] == "CONJUNCTION_AND" {
        for c in e["children"].as_array().into_iter().flatten() {
            conjuncts(c, cols, bound, out);
        }
    } else if let Some(p) = conjunct(e, cols, bound) {
        out.push(p);
    }
}

fn conjunct(e: &serde_json::Value, cols: &[SourceColumn], bound: Bound) -> Option<String> {
    let (c, p) = match e["type"].as_str()? {
        "OPERATOR_IS_NULL" | "OPERATOR_IS_NOT_NULL" => {
            let [x] = e["children"].as_array()?.as_slice() else {
                return None;
            };
            let c = column(x, cols)?;
            let not = if e["type"] == "OPERATOR_IS_NULL" {
                ""
            } else {
                " NOT"
            };
            (c, format!("{} IS{not} NULL", ident(&c.name)))
        }
        "COMPARE_BETWEEN" => {
            let c = column(&e["input"], cols)?;
            let (lhs, lo) = compared(c, &e["lower"])?;
            let (_, hi) = compared(c, &e["upper"])?;
            (c, format!("{lhs} >= {lo} AND {lhs} <= {hi}"))
        }
        t => {
            let op = match t {
                "COMPARE_EQUAL" => "=",
                "COMPARE_NOTEQUAL" => "<>",
                "COMPARE_LESSTHAN" => "<",
                "COMPARE_GREATERTHAN" => ">",
                "COMPARE_LESSTHANOREQUALTO" => "<=",
                "COMPARE_GREATERTHANOREQUALTO" => ">=",
                _ => return None,
            };
            let (l, r) = (&e["left"], &e["right"]);
            let (c, k, op) = match (column(l, cols), column(r, cols)) {
                (Some(c), None) => (c, r, op),
                (None, Some(c)) => (
                    c,
                    l,
                    match op {
                        "<" => ">",
                        ">" => "<",
                        "<=" => ">=",
                        ">=" => "<=",
                        o => o,
                    },
                ),
                _ => return None,
            };
            match compared(c, k) {
                Some((lhs, lit)) => (c, format!("{lhs} {op} {lit}")),
                None if matches!(op, ">" | ">=") && rising(k, cols) => {
                    let (ty, duck) = match c.typname.as_str() {
                        "date" => ("date", "DATE"),
                        "timestamp" => ("timestamp", "TIMESTAMP"),
                        "timestamptz" => ("timestamptz", "TIMESTAMPTZ"),
                        _ => return None,
                    };
                    let b = bound(k, duck)?;
                    if !iso(ty, &b) {
                        return None;
                    }
                    (c, format!("{} >= {}::{ty}", ident(&c.name), literal(&b)))
                }
                None => return None,
            }
        }
    };
    Some(match unlandable(c) {
        Some((bad, _)) => format!("({p} OR ({bad}))"),
        None => format!("({p})"),
    })
}

/// `e` only moves forward with time: the current date or time, plus or minus a constant.
fn rising(e: &serde_json::Value, cols: &[SourceColumn]) -> bool {
    fn constant(e: &serde_json::Value) -> bool {
        match e["class"].as_str() {
            Some("CONSTANT") => true,
            Some("CAST") => constant(&e["child"]),
            Some("FUNCTION") => {
                matches!(
                    e["function_name"].as_str(),
                    Some(
                        "trunc"
                            | "to_years"
                            | "to_months"
                            | "to_weeks"
                            | "to_days"
                            | "to_hours"
                            | "to_minutes"
                            | "to_seconds"
                            | "to_milliseconds"
                            | "to_microseconds"
                    )
                ) && e["children"]
                    .as_array()
                    .is_some_and(|c| c.iter().all(constant))
            }
            _ => false,
        }
    }
    let now = |e: &serde_json::Value| match e["class"].as_str() {
        Some("COLUMN_REF") => match e["column_names"].as_array().map(Vec::as_slice) {
            Some([n]) => n.as_str().is_some_and(|n| {
                matches!(n, "current_date" | "current_timestamp")
                    && !cols.iter().any(|c| c.name.eq_ignore_ascii_case(n))
            }),
            _ => false,
        },
        Some("FUNCTION") => {
            e["children"].as_array().is_some_and(Vec::is_empty)
                && matches!(
                    e["function_name"].as_str(),
                    Some(
                        "now"
                            | "today"
                            | "current_date"
                            | "current_timestamp"
                            | "get_current_timestamp"
                            | "transaction_timestamp"
                    )
                )
        }
        _ => false,
    };
    if now(e) {
        return true;
    }
    let Some([a, b]) = e["children"].as_array().map(Vec::as_slice) else {
        return false;
    };
    e["class"] == "FUNCTION"
        && match e["function_name"].as_str() {
            Some("+") => (now(a) && constant(b)) || (constant(a) && now(b)),
            Some("-") => now(a) && constant(b),
            _ => false,
        }
}

/// The column a `COLUMN_REF` names (bare or qualified by the read's alias `x`), matched
/// case-insensitively as the engine binds it, if it is read as stored.
fn column<'a>(e: &serde_json::Value, cols: &'a [SourceColumn]) -> Option<&'a SourceColumn> {
    if e["class"] != "COLUMN_REF" {
        return None;
    }
    let names: Vec<&str> = e["column_names"]
        .as_array()?
        .iter()
        .map(|n| n.as_str())
        .collect::<Option<_>>()?;
    let name = match names.as_slice() {
        [n] => *n,
        [x, n]
            if x.eq_ignore_ascii_case("x")
                && !cols.iter().any(|c| c.name.eq_ignore_ascii_case(x)) =>
        {
            *n
        }
        _ => return None,
    };
    let mut m = cols.iter().filter(|c| c.name.eq_ignore_ascii_case(name));
    let c = m.next()?;
    (m.next().is_none() && matches!(c.read, Read::Native | Read::Decimal) && !c.domain).then_some(c)
}

/// The column's side and the literal `k` as Postgres compares them the way the engine does: an
/// exact number against an integer or decimal; an ISO date, time or timestamp string against its
/// own type (both sessions are in UTC); a string against text, byte-wise.
fn compared(c: &SourceColumn, k: &serde_json::Value) -> Option<(String, String)> {
    let i = ident(&c.name);
    let ty = comparable(&c.typname)?;
    let (k, cast) = match k["class"].as_str()? {
        "CAST" => (&k["child"], k["cast_type"]["id"].as_str()),
        _ => (k, None),
    };
    if k["class"] != "CONSTANT" || k["value"]["is_null"] != false {
        return None;
    }
    let v = &k["value"]["value"];
    match (k["value"]["type"]["id"].as_str()?, cast) {
        ("VARCHAR", cast) => {
            let s = v.as_str()?;
            let duck = match ty {
                "date" => "DATE",
                "time" => "TIME",
                "timestamp" => "TIMESTAMP",
                "timestamptz" => "TIMESTAMP WITH TIME ZONE",
                "text" => "VARCHAR",
                _ => return None,
            };
            if cast.is_some_and(|t| t != duck) || !iso(ty, s) {
                return None;
            }
            Some(match ty {
                "text" => (
                    format!("{i} COLLATE \"C\""),
                    format!("{}::text", literal(s)),
                ),
                _ => (i, format!("{}::{ty}", literal(s))),
            })
        }
        (t, None) if matches!(ty, "int2" | "int4" | "int8" | "numeric") => {
            let n = match (t, v) {
                ("TINYINT" | "SMALLINT" | "INTEGER" | "BIGINT" | "HUGEINT", v) => v
                    .as_i64()
                    .map(|n| n.to_string())
                    .or_else(|| v.as_str().map(str::to_owned))?,
                ("DECIMAL", v) => {
                    let n = v.as_i64()?;
                    let scale = k["value"]["type"]["type_info"]["scale"].as_u64()? as usize;
                    let digits = format!("{:0>w$}", n.unsigned_abs(), w = scale + 1);
                    let (a, b) = digits.split_at(digits.len() - scale);
                    format!("{}{a}.{b}", if n < 0 { "-" } else { "" })
                }
                _ => return None,
            };
            if !n
                .trim_start_matches('-')
                .bytes()
                .all(|b| b.is_ascii_digit() || b == b'.')
            {
                return None;
            }
            Some((i, format!("{}::numeric", literal(&n))))
        }
        _ => None,
    }
}

/// `s` is an ISO literal both engines read the same as `ty`: `YYYY-MM-DD[ HH:MM[:SS[.ffffff]]]`,
/// a time `HH:MM[:SS[.ffffff]]`, and an offset (`Z`, `±HH[[:]MM]`) only on a `timestamptz`; any
/// string for text.
fn iso(ty: &str, s: &str) -> bool {
    fn digits(s: &str, n: usize) -> bool {
        s.len() == n && s.bytes().all(|b| b.is_ascii_digit())
    }
    fn time(s: &str) -> bool {
        let (hms, frac) = s.split_once('.').unwrap_or((s, "0"));
        let p: Vec<_> = hms.split(':').collect();
        matches!(p.len(), 2 | 3)
            && p.iter().all(|x| digits(x, 2))
            && (1..=6).contains(&frac.len())
            && frac.bytes().all(|b| b.is_ascii_digit())
    }
    fn date(s: &str) -> bool {
        let p: Vec<_> = s.split('-').collect();
        p.len() == 3 && digits(p[0], 4) && digits(p[1], 2) && digits(p[2], 2)
    }
    let offset = |s: &str| -> Option<usize> {
        if s.ends_with('Z') {
            return Some(s.len() - 1);
        }
        let at = s.rfind(['+', '-'])?;
        let o = s[at + 1..].replace(':', "");
        (at > 10 && (digits(&o, 2) || digits(&o, 4))).then_some(at)
    };
    match ty {
        "text" => true,
        "date" => date(s),
        "time" => time(s),
        "timestamp" | "timestamptz" => {
            let s = match (ty, offset(s)) {
                ("timestamptz", Some(at)) => &s[..at],
                (_, Some(_)) => return false,
                _ => s,
            };
            match s.split_once([' ', 'T']) {
                Some((d, t)) => date(d) && time(t),
                None => date(s),
            }
        }
        _ => false,
    }
}

pub fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn libpq_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\\' || c == '\'' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('\'');
    out
}

/// A libpq keyword DSN for the DuckDB attach: every value quoted, keepalives and statement
/// timeout carried over, the session in UTC. Registers the password (raw, libpq-quoted, and inside a SQL literal).
pub fn libpq_dsn(o: &ConnectOptions) -> String {
    let mut parts: Vec<(&str, String)> = vec![
        ("host", o.host.clone()),
        ("port", o.port.to_string()),
        ("dbname", o.database.clone()),
        ("user", o.user.clone()),
    ];
    if let Some(p) = &o.password {
        let q = libpq_value(p);
        redact::register(p);
        redact::register(&q);
        redact::register(&q[1..q.len() - 1].replace('\'', "''"));
        parts.push(("password", p.clone()));
    }
    parts.push((
        "sslmode",
        match o.sslmode {
            SslMode::Disable => "disable",
            SslMode::Require => "require",
            SslMode::VerifyFull => "verify-full",
        }
        .into(),
    ));
    if let Some(ca) = &o.sslrootcert {
        parts.push(("sslrootcert", ca.to_string_lossy().into_owned()));
    }
    if let Some(t) = o.connect_timeout {
        parts.push(("connect_timeout", t.as_secs().max(1).to_string()));
    }
    parts.push(("application_name", o.application_name.clone()));
    match o.keepalive {
        Some(k) => {
            parts.push(("keepalives", "1".into()));
            parts.push(("keepalives_idle", k.idle.as_secs().max(1).to_string()));
            parts.push((
                "keepalives_interval",
                k.interval.as_secs().max(1).to_string(),
            ));
            parts.push(("keepalives_count", k.count.to_string()));
        }
        None => parts.push(("keepalives", "0".into())),
    }
    let mut options = String::new();
    if let Some(t) = o.statement_timeout {
        options.push_str(&format!("-c statement_timeout={} ", t.as_millis()));
    }
    // Text Postgres renders (`to_json` of a timestamptz, ranges, `::text`) follows the session's.
    options.push_str("-c TimeZone=UTC");
    parts.push(("options", options));
    parts
        .iter()
        .map(|(k, v)| format!("{k}={}", libpq_value(v)))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Attaches the source as `alias`, read-only.
pub fn attach_sql(alias: &str, o: &ConnectOptions) -> String {
    format!(
        "LOAD postgres_scanner; DETACH DATABASE IF EXISTS {a}; ATTACH {} AS {a} (TYPE postgres, READ_ONLY)",
        literal(&libpq_dsn(o)),
        a = ident(alias)
    )
}

pub fn detach_sql(alias: &str) -> String {
    format!("DETACH DATABASE IF EXISTS {}", ident(alias))
}

/// The engine relation reading `schema.table` through the attach `alias`: the attach's parallel
/// scan, with filter pushdown, behind a [`guard`] when a column needs one. A domain or declared
/// column is converted in Postgres (`postgres_query`); filters then apply engine-side instead of
/// being pushed into the scan. A refused column is a `config` error.
pub fn relation(alias: &str, schema: &str, table: &str, cols: &[SourceColumn]) -> Result<String> {
    relation_where(alias, schema, table, cols, None, &[]).map(|(r, _)| r)
}

/// [`relation`] narrowed by `server_where` (Postgres SQL: [`cursor_predicate`], and anything that
/// keeps at least what `engine_where` keeps, e.g. [`window_predicate`]) and `engine_where` (the
/// engine-side filters the read is wrapped in). Returns whether `server_where` was evaluated in
/// Postgres for the read itself (`postgres_query`). A native scan leaves filtering to the engine,
/// which pushes it into the scan; its guard is narrowed by both.
pub fn relation_where(
    alias: &str,
    schema: &str,
    table: &str,
    cols: &[SourceColumn],
    server_where: Option<&str>,
    engine_where: &[String],
) -> Result<(String, bool)> {
    let label = format!("{schema}.{table}");
    if cols.is_empty() {
        return Err(AtlatlError::config(format!(
            "postgres source `{label}` has no columns to read"
        )));
    }
    check(&label, cols)?;
    let from = format!("{}.{}", ident(schema), ident(table));
    if cols
        .iter()
        .all(|c| matches!(c.read, Read::Native | Read::Decimal) && !c.domain)
    {
        let scan = format!("{}.{from}", ident(alias));
        return Ok((
            guard(
                alias,
                &scan,
                &from,
                &format!("`{label}`"),
                cols,
                server_where,
                engine_where,
            ),
            false,
        ));
    }
    Ok((
        converted(alias, &from, &format!("`{label}`"), cols, server_where),
        server_where.is_some(),
    ))
}

/// `from.query`, run in Postgres, with `cols` from [`PgSource::describe`]: as is when every
/// column reads natively, else nested and converted like a table's.
pub fn query_read(alias: &str, query: &str, cols: &[SourceColumn]) -> Result<String> {
    check("from.query", cols)?;
    if cols
        .iter()
        .all(|c| c.read == Read::Native && unlandable(c).is_none())
    {
        return Ok(query_relation(alias, query));
    }
    let mut seen = std::collections::HashSet::new();
    if let Some(c) = cols.iter().find(|c| !seen.insert(c.name.as_str())) {
        return Err(AtlatlError::config(format!(
            "`from.query` returns column `{}` more than once; give each its own name",
            c.name
        )));
    }
    let q = query.trim_end_matches(|c: char| c.is_whitespace() || c == ';');
    // The newline closes a trailing `--` comment before the parenthesis.
    Ok(converted(
        alias,
        &format!("({q}\n) AS q"),
        "`from.query`",
        cols,
        None,
    ))
}

fn check(label: &str, cols: &[SourceColumn]) -> Result<()> {
    match admit(cols.to_vec(), &|_| None) {
        Ok(_) => Ok(()),
        Err(lines) => Err(refused(
            &lines
                .into_iter()
                .map(|l| (label.to_owned(), l))
                .collect::<Vec<_>>(),
        )),
    }
}

/// `scan` (the attach's `from`), failing before it streams if a row it reads holds an
/// [`unlandable`] value. The rows holding one that `server_where` selects are read through
/// [`converted`] (the same values the scan reads, a flagged one raising when evaluated) and
/// filtered by `engine_where` like the read, then aggregated into one row cross joined into the
/// scan, which keeps its parallelism and filter pushdown. A filter on a column holding one raises
/// too, as it does through `postgres_query`.
fn guard(
    alias: &str,
    scan: &str,
    from: &str,
    label: &str,
    cols: &[SourceColumn],
    server_where: Option<&str>,
    engine_where: &[String],
) -> String {
    let proj = |q: &str| {
        cols.iter()
            .map(|c| format!("{q}{}", ident(&c.name)))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let checks: Vec<_> = cols
        .iter()
        .filter_map(|c| unlandable(c).map(|(bad, _)| (c, bad)))
        .collect();
    if checks.is_empty() {
        return format!("(SELECT {} FROM {scan})", proj(""));
    }
    let any = checks
        .iter()
        .map(|(_, bad)| format!("({bad})"))
        .collect::<Vec<_>>()
        .join(" OR ");
    let w = match server_where {
        Some(w) => format!("({any}) AND ({w})"),
        None => any,
    };
    let touch = checks
        .iter()
        .map(|(c, _)| format!("x.{}", ident(&c.name)))
        .collect::<Vec<_>>()
        .join(", ");
    let keep = if engine_where.is_empty() {
        String::new()
    } else {
        format!(
            " WHERE {}",
            engine_where
                .iter()
                .map(|f| format!("({f})"))
                .collect::<Vec<_>>()
                .join(" AND ")
        )
    };
    format!(
        "(SELECT {} FROM {scan} AS atlatl_t CROSS JOIN (SELECT max(hash({touch})) AS atlatl_guard FROM {} AS x{keep}) AS atlatl_g WHERE atlatl_g.atlatl_guard IS NULL)",
        proj("atlatl_t."),
        converted(alias, from, label, cols, Some(&w)),
    )
}

/// `SELECT … FROM <from>` in Postgres through `postgres_query`, and the engine projection over it
/// when a column is checked there ([`unlandable`] values, nulled and flagged; a declared type).
fn converted(
    alias: &str,
    from: &str,
    label: &str,
    cols: &[SourceColumn],
    server_where: Option<&str>,
) -> String {
    let mut server = Vec::new();
    let mut engine = Vec::new();
    let mut checked = false;
    for (n, c) in cols.iter().enumerate() {
        let i = ident(&c.name);
        let what = format!("column `{}` of {label}", c.name);
        match &c.read {
            Read::Native | Read::Base | Read::Decimal => {
                let e = c.base_sql();
                match unlandable(c) {
                    None => {
                        server.push(if c.domain {
                            format!("{e} AS {i}")
                        } else {
                            i.clone()
                        });
                        engine.push(i);
                    }
                    Some((bad, why)) => {
                        let flag = ident(&format!("atlatl_bad_{n}"));
                        // A `CASE` drops the typmod, and an unconstrained numeric reads as a float.
                        server.push(format!(
                            "(CASE WHEN {bad} THEN NULL ELSE {e} END)::{} AS {i}",
                            c.base
                        ));
                        server.push(format!("CASE WHEN {bad} THEN ({e})::text END AS {flag}"));
                        engine.push(format!(
                            "CASE WHEN {flag} IS NULL THEN {i} ELSE error({} || {flag} || {}) END AS {i}",
                            duck_literal(&format!("{what} holds ")),
                            duck_literal(&why)
                        ));
                        checked = true;
                    }
                }
            }
            Read::Declared {
                text,
                kind,
                target,
                set_null,
            } => {
                server.push(format!("{} AS {i}", text.sql(&i)));
                if *set_null {
                    let (e, flag) = nulling(&i, kind, *target);
                    engine.push(format!("{e} AS {i}"));
                    if let Some(f) = flag {
                        engine.push(format!(
                            "{f} AS {}",
                            ident(&format!("{CAST_NULLED}{}", c.name))
                        ));
                    }
                } else {
                    engine.push(format!("{} AS {i}", guarded(&i, kind, *target, &what)));
                }
                checked = true;
            }
            Read::Refused(_) => unreachable!("checked"),
        }
    }
    let mut sql = format!("SELECT {} FROM {from}", server.join(", "));
    if let Some(w) = server_where {
        sql.push_str(&format!(" WHERE {w}"));
    }
    if !checked {
        return query_relation(alias, &sql);
    }
    format!(
        "(SELECT {} FROM postgres_query({}, {}))",
        engine.join(", "),
        literal(alias),
        literal(&sql)
    )
}

/// Adopts an exported snapshot in the engine's current transaction on `alias`; it must run
/// before anything reads from the attach in that transaction.
pub fn set_snapshot_sql(alias: &str, snapshot: &str) -> String {
    format!(
        "CALL postgres_execute({}, {})",
        literal(alias),
        literal(&format!("SET TRANSACTION SNAPSHOT {}", literal(snapshot)))
    )
}

/// `from.query`, run in Postgres.
pub fn query_relation(alias: &str, query: &str) -> String {
    format!(
        "(SELECT * FROM postgres_query({}, {}))",
        literal(alias),
        literal(query)
    )
}

/// `schema.table` with `*`/`?` globs; no schema means `public`.
pub fn split(pattern: &str) -> (String, String) {
    match pattern.split_once('.') {
        Some((s, t)) => (s.to_owned(), t.to_owned()),
        None => ("public".into(), pattern.to_owned()),
    }
}

fn is_glob(s: &str) -> bool {
    s.contains(['*', '?'])
}

/// `*` and `?` glob, byte-wise.
pub fn glob_match(name: &str, glob: &str) -> bool {
    fn m(n: &[u8], g: &[u8]) -> bool {
        match (g.first(), n.first()) {
            (None, None) => true,
            (Some(b'*'), _) => m(n, &g[1..]) || (!n.is_empty() && m(&n[1..], g)),
            (Some(b'?'), Some(_)) => m(&n[1..], &g[1..]),
            (Some(a), Some(b)) if a == b => m(&n[1..], &g[1..]),
            _ => false,
        }
    }
    m(name.as_bytes(), glob.as_bytes())
}

/// Matching `schema.table` names, sorted byte-wise (independent of the server collation). Names
/// containing `.` are skipped; `exclude` takes names or globs.
pub fn match_tables(
    rows: impl IntoIterator<Item = (String, String)>,
    pattern: &str,
    exclude: &[String],
) -> Vec<String> {
    let (ps, pt) = split(pattern);
    let mut out: Vec<String> = rows
        .into_iter()
        .filter(|(s, t)| !s.contains('.') && !t.contains('.'))
        .filter(|(s, t)| glob_match(s, &ps) && glob_match(t, &pt))
        .map(|(s, t)| format!("{s}.{t}"))
        .filter(|n| !exclude.iter().any(|e| n == e || glob_match(n, e)))
        .collect();
    out.sort();
    out.dedup();
    out
}

/// Base tables (plain and partitioned; partitions are read through their parent), plus views and
/// materialized views when asked. System schemas are never listed.
pub fn tables_sql(include_views: bool, one_schema: bool) -> String {
    let kinds = if include_views {
        "'r', 'p', 'v', 'm'"
    } else {
        "'r', 'p'"
    };
    let mut sql = format!(
        "SELECT n.nspname::text, c.relname::text FROM pg_catalog.pg_class c \
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relkind IN ({kinds}) AND NOT c.relispartition \
         AND n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\\_%'"
    );
    if one_schema {
        sql.push_str(" AND n.nspname = $1");
    }
    sql
}

/// The primary key's columns, in key order (none for views and keyless tables).
pub const PRIMARY_KEY_SQL: &str = "SELECT a.attname::text \
     FROM pg_catalog.pg_index i \
     CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) \
     JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum \
     WHERE i.indrelid = $1::text::regclass AND i.indisprimary \
     ORDER BY k.ord";

/// Columns in ordinal order with their base type: a domain (over a domain…) resolved to the type
/// it's over, with the innermost domain's typmod, as Postgres resolves it.
pub const COLUMNS_SQL: &str = "WITH RECURSIVE b(attnum, oid, typmod) AS ( \
       SELECT a.attnum, a.atttypid, a.atttypmod FROM pg_catalog.pg_attribute a \
       WHERE a.attrelid = $1::text::regclass AND a.attnum > 0 AND NOT a.attisdropped \
       UNION ALL \
       SELECT b.attnum, t.typbasetype, t.typtypmod FROM b \
       JOIN pg_catalog.pg_type t ON t.oid = b.oid WHERE t.typtype = 'd') \
     SELECT a.attname::text, format_type(a.atttypid, a.atttypmod), b.oid <> a.atttypid, \
       t.typtype::text, t.typcategory::text, t.typname::text, b.typmod, format_type(b.oid, b.typmod) \
     FROM b JOIN pg_catalog.pg_type t ON t.oid = b.oid \
     JOIN pg_catalog.pg_attribute a ON a.attrelid = $1::text::regclass AND a.attnum = b.attnum \
     WHERE t.typtype <> 'd' \
     ORDER BY a.attnum";

/// `pg_type` of a described query's columns, by position. A query's columns arrive as their base
/// type, never a domain.
pub const QUERY_TYPES_SQL: &str = "SELECT u.n::int4, t.typtype::text, t.typcategory::text, \
       t.typname::text, format_type(t.oid, u.m) \
     FROM unnest($1::oid[], $2::int4[]) WITH ORDINALITY AS u(o, m, n) \
     JOIN pg_catalog.pg_type t ON t.oid = u.o";

/// A source connection: one lazily opened read-only session for catalog queries, and optionally
/// one holding an exported snapshot.
pub struct PgSource {
    opts: ConnectOptions,
    tls: Option<Arc<ClientConfig>>,
    session: Mutex<Option<Session>>,
    snapshot: Mutex<Option<(Session, String)>>,
}

impl PgSource {
    pub fn new(opts: ConnectOptions) -> Result<Self> {
        let tls = opts.tls()?;
        Ok(Self {
            opts,
            tls,
            session: Mutex::new(None),
            snapshot: Mutex::new(None),
        })
    }

    pub fn options(&self) -> &ConnectOptions {
        &self.opts
    }

    async fn session(&self) -> Result<tokio::sync::MutexGuard<'_, Option<Session>>> {
        let mut g = self.session.lock().await;
        if g.as_ref().is_none_or(Session::is_closed) {
            let s = connect_with(&self.opts, self.tls.as_ref()).await?;
            s.client()
                .batch_execute("SET default_transaction_read_only = on")
                .await
                .map_err(wrap)?;
            *g = Some(s);
        }
        Ok(g)
    }

    /// Sorted `schema.table` members matching a `from.tables` glob.
    pub async fn discover(
        &self,
        pattern: &str,
        exclude: &[String],
        include_views: bool,
    ) -> Result<Vec<String>> {
        let (schema, _) = split(pattern);
        let one = !is_glob(&schema);
        let g = self.session().await?;
        let c = g.as_ref().expect("session").client();
        let sql = tables_sql(include_views, one);
        let rows = if one {
            c.query(&sql, &[&schema]).await
        } else {
            c.query(&sql, &[]).await
        }
        .map_err(|e| context(wrap(e), &format!("discovering `{pattern}`")))?;
        Ok(match_tables(
            rows.iter()
                .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1))),
            pattern,
            exclude,
        ))
    }

    /// Columns of `schema.table` in ordinal order, narrowed by `columns.include`/`exclude`
    /// (exact name, else case-insensitive).
    pub async fn columns(
        &self,
        schema: &str,
        table: &str,
        include: Option<&[String]>,
        exclude: &[String],
    ) -> Result<Vec<SourceColumn>> {
        let g = self.session().await?;
        let c = g.as_ref().expect("session").client();
        let rel = format!("{}.{}", ident(schema), ident(table));
        let rows = c.query(COLUMNS_SQL, &[&rel]).await.map_err(|e| {
            context(
                wrap(e),
                &format!("postgres source table `{schema}.{table}`"),
            )
        })?;
        Ok(narrow(
            rows.iter()
                .map(|r| {
                    SourceColumn::new(
                        r.get(0),
                        r.get(1),
                        r.get(2),
                        (r.get(3), r.get(4), r.get(5), r.get(6), r.get(7)),
                    )
                })
                .collect(),
            include,
            exclude,
        ))
    }

    /// The columns `query` returns, from describing it (nothing runs).
    pub async fn describe(&self, query: &str) -> Result<Vec<SourceColumn>> {
        let g = self.session().await?;
        let c = g.as_ref().expect("session").client();
        let st = c
            .prepare(query)
            .await
            .map_err(|e| context(wrap(e), "describing `from.query`"))?;
        let oids: Vec<u32> = st.columns().iter().map(|c| c.type_().oid()).collect();
        let mods: Vec<i32> = st.columns().iter().map(|c| c.type_modifier()).collect();
        let rows = c
            .query(QUERY_TYPES_SQL, &[&oids, &mods])
            .await
            .map_err(|e| context(wrap(e), "describing `from.query`"))?;
        let mut types: Vec<Option<(String, String, String, String)>> = vec![None; oids.len()];
        for r in &rows {
            let n: i32 = r.get(0);
            types[n as usize - 1] = Some((r.get(1), r.get(2), r.get(3), r.get(4)));
        }
        Ok(st
            .columns()
            .iter()
            .zip(types)
            .map(|(col, t)| {
                let (tt, cat, name, fmt) = t.unwrap_or_default();
                SourceColumn::new(
                    col.name(),
                    &fmt,
                    false,
                    (&tt, &cat, &name, col.type_modifier(), &fmt),
                )
            })
            .collect())
    }

    /// The primary key of `schema.table` from its constraint; `None` without one.
    pub async fn primary_key(&self, schema: &str, table: &str) -> Result<Option<Vec<String>>> {
        let g = self.session().await?;
        let c = g.as_ref().expect("session").client();
        let rel = format!("{}.{}", ident(schema), ident(table));
        let rows = c.query(PRIMARY_KEY_SQL, &[&rel]).await.map_err(|e| {
            context(
                wrap(e),
                &format!("primary key of postgres source table `{schema}.{table}`"),
            )
        })?;
        let cols: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
        Ok((!cols.is_empty()).then_some(cols))
    }

    /// Opens a `REPEATABLE READ READ ONLY` transaction on its own session and exports its
    /// snapshot; it stays valid (and holds back the server's xmin horizon) until
    /// [`Self::release_snapshot`]. Idempotent.
    pub async fn export_snapshot(&self) -> Result<String> {
        let mut g = self.snapshot.lock().await;
        if let Some((s, id)) = g.as_ref()
            && !s.is_closed()
        {
            return Ok(id.clone());
        }
        let s = connect_with(&self.opts, self.tls.as_ref()).await?;
        s.client()
            .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(wrap)?;
        let id: String = s
            .client()
            .query_one("SELECT pg_export_snapshot()", &[])
            .await
            .map_err(|e| context(wrap(e), "exporting a snapshot"))?
            .get(0);
        *g = Some((s, id.clone()));
        Ok(id)
    }

    pub async fn release_snapshot(&self) {
        if let Some((s, _)) = self.snapshot.lock().await.take() {
            let _ = s.client().batch_execute("COMMIT").await;
            s.abort();
        }
    }

    pub async fn close(&self) {
        self.release_snapshot().await;
        if let Some(s) = self.session.lock().await.take() {
            s.abort();
        }
    }
}

fn context(mut e: AtlatlError, what: &str) -> AtlatlError {
    e.message = format!("{what}: {}", e.message);
    e
}

/// Errors from DuckDB's postgres extension carry libpq/server text but no SQLSTATE; map the
/// known ones (`None`: not recognised).
pub fn classify_message(msg: &str) -> Option<(ErrorClass, &'static str)> {
    use ErrorClass as C;
    let has = |n: &str| msg.contains(n);
    Some(
        if has("password authentication failed") || has("no pg_hba.conf entry") {
            (C::Auth, "28P01")
        } else if has("role \"") && has("does not exist") {
            (C::Auth, "28000")
        } else if has("database \"") && has("does not exist") {
            (C::Config, "3D000")
        } else if has("permission denied") {
            (C::Config, "42501")
        } else if has("in a read-only transaction") {
            (C::Config, "25006")
        } else if has("canceling statement due to statement timeout") {
            (C::Timeout, "57014")
        } else if has("timeout expired") {
            (C::Timeout, "08001")
        } else if has("server does not support SSL") || has("certificate verify failed") {
            (C::Config, "08P01")
        } else if has("relation \"") && has("does not exist") {
            (C::Config, "42P01")
        } else if has("invalid snapshot identifier") {
            (C::Transient, "snapshot_lost")
        } else {
            return None;
        },
    )
}

/// Re-maps an engine error raised while reading a postgres source. Classified errors (data,
/// config, conflict, killed…) are kept.
pub fn reclassify(e: AtlatlError) -> AtlatlError {
    if !matches!(e.class, ErrorClass::Internal | ErrorClass::Transient) {
        return e;
    }
    let (class, code) = match classify_message(&e.message) {
        Some((c, code)) => (c, Some(code)),
        None if e.class == ErrorClass::Internal => {
            atlatl_engine::error::classify_message(&e.message)
        }
        None => return e,
    };
    AtlatlError {
        class,
        code: code.map(str::to_owned).or(e.code.clone()),
        ..e
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nm(p: i32, s: i32) -> i32 {
        ((p << 16) | s) + 4
    }

    fn base(name: &str, typname: &str, typmod: i32, fmt: &str) -> SourceColumn {
        let (tt, cat) = match typname {
            "mood" => ("e", "E"),
            t if t.starts_with('_') => ("b", "A"),
            _ => ("b", "X"),
        };
        SourceColumn::new(name, fmt, false, (tt, cat, typname, typmod, fmt))
    }

    fn domain(name: &str, dom: &str, typname: &str, typmod: i32, fmt: &str) -> SourceColumn {
        SourceColumn {
            pg_type: dom.into(),
            ..SourceColumn::new(name, dom, true, ("b", "X", typname, typmod, fmt))
        }
    }

    fn declare(cols: Vec<SourceColumn>, d: &[(&str, &str)]) -> Vec<SourceColumn> {
        admit(cols, &|n| {
            d.iter()
                .find(|(c, _)| *c == n)
                .map(|(_, k)| (*k).to_owned())
        })
        .expect("admit")
    }

    #[test]
    fn read_modes() {
        let reads = |c: SourceColumn| c.read;
        assert_eq!(reads(base("a", "int4", -1, "integer")), Read::Native);
        assert_eq!(reads(base("a", "jsonb", -1, "jsonb")), Read::Native);
        assert_eq!(
            reads(base("a", "numeric", nm(12, 4), "numeric(12,4)")),
            Read::Decimal
        );
        assert!(matches!(
            reads(base("a", "numeric", -1, "numeric")),
            Read::Refused(_)
        ));
        assert!(matches!(
            reads(base("a", "_text", -1, "text[]")),
            Read::Refused(_)
        ));
        assert!(matches!(
            reads(base("a", "mood", -1, "mood")),
            Read::Refused(_)
        ));
        assert_eq!(
            reads(domain("a", "posint", "int4", -1, "integer")),
            Read::Base
        );
        let d = domain("a", "dnum", "numeric", nm(10, 2), "numeric(10,2)");
        assert_eq!((d.read, d.domain), (Read::Decimal, true));
        assert_eq!(numeric_precision(nm(12, 4)), Some((12, 4)));
    }

    #[test]
    fn admits_only_what_is_declared() {
        let cols = vec![
            base("id", "int4", -1, "integer"),
            base("m", "mood", -1, "mood"),
            base("tags", "_text", -1, "text[]"),
            base("big", "numeric", -1, "numeric"),
            domain("cash", "price", "money", -1, "money"),
        ];
        let bad = admit(cols.clone(), &|_| None).expect_err("refused");
        assert_eq!(bad.len(), 4, "{bad:?}");
        assert!(bad[0].starts_with("column `m` is mood, which the target can't hold: there's no enum type; convert it in `from.query` (`m::text`), or declare `columns.m.type` (VARCHAR)"), "{}", bad[0]);
        assert!(
            bad[3].starts_with("column `cash` is price (its money), "),
            "{}",
            bad[3]
        );
        let ok = declare(
            cols.clone(),
            &[
                ("m", "VARCHAR"),
                ("tags", "JSON"),
                ("big", "DECIMAL(38,10)"),
                ("cash", "decimal(19,2)"),
            ],
        );
        assert_eq!(
            ok[3].read,
            Read::Declared {
                text: Text::Plain,
                kind: "DECIMAL(38,10)".into(),
                target: Target::Decimal(38, 10),
                set_null: false,
            }
        );
        assert!(matches!(
            &ok[4].read,
            Read::Declared {
                text: Text::Numeric,
                ..
            }
        ));
        let bad =
            admit(cols, &|n| (n != "id").then(|| "TIMESTAMP".to_owned())).expect_err("unchecked");
        assert_eq!(bad.len(), 4);
        assert!(
            bad[0].contains("`columns.m.type: TIMESTAMP` can't be checked against exactly"),
            "{}",
            bad[0]
        );
    }

    #[test]
    fn relations() {
        let native = [
            base("id", "int4", -1, "integer"),
            base("v", "text", -1, "text"),
        ];
        assert_eq!(
            relation("pg", "appdb", "t", &native).expect("rel"),
            r#"(SELECT "id", "v" FROM "pg"."appdb"."t")"#
        );
        let dom = [
            base("id", "int4", -1, "integer"),
            domain("d", "posint", "int4", -1, "integer"),
        ];
        assert_eq!(
            relation("pg", "appdb", "t", &dom).expect("rel"),
            r#"(SELECT * FROM postgres_query('pg', 'SELECT "id", "d"::integer AS "d" FROM "appdb"."t"'))"#
        );
        let dec = [
            base("id", "int4", -1, "integer"),
            base("n", "numeric", nm(5, 2), "numeric(5,2)"),
            base("at", "timestamp", -1, "timestamp without time zone"),
        ];
        let w = [r#"x."id" < 9"#.to_owned()];
        let (rel, pushed) =
            relation_where("pg", "appdb", "t", &dec, Some(r#""id" > 1"#), &w).expect("rel");
        assert!(!pushed);
        assert_eq!(
            rel,
            r#"(SELECT atlatl_t."id", atlatl_t."n", atlatl_t."at" FROM "pg"."appdb"."t" AS atlatl_t CROSS JOIN (SELECT max(hash(x."n", x."at")) AS atlatl_guard FROM (SELECT "id", CASE WHEN "atlatl_bad_1" IS NULL THEN "n" ELSE error('column `n` of `appdb.t` holds ' || "atlatl_bad_1" || ', which a decimal can''t hold; convert it in `from.query` (`NULLIF(n, ''NaN'')` to read it as NULL, or `n::float8`)') END AS "n", CASE WHEN "atlatl_bad_2" IS NULL THEN "at" ELSE error('column `at` of `appdb.t` holds ' || "atlatl_bad_2" || ', past the last timestamp the target holds (294247-01-10 04:00:54.775806); convert it in `from.query` (`at::text`, or a `CASE` mapping it to ''infinity'')') END AS "at" FROM postgres_query('pg', 'SELECT "id", (CASE WHEN "n" = ''NaN'' THEN NULL ELSE "n" END)::numeric(5,2) AS "n", CASE WHEN "n" = ''NaN'' THEN ("n")::text END AS "atlatl_bad_1", (CASE WHEN "at" > ''294247-01-10 04:00:54.775806''::timestamp AND "at" < ''infinity'' THEN NULL ELSE "at" END)::timestamp without time zone AS "at", CASE WHEN "at" > ''294247-01-10 04:00:54.775806''::timestamp AND "at" < ''infinity'' THEN ("at")::text END AS "atlatl_bad_2" FROM "appdb"."t" WHERE (("n" = ''NaN'') OR ("at" > ''294247-01-10 04:00:54.775806''::timestamp AND "at" < ''infinity'')) AND ("id" > 1)')) AS x WHERE (x."id" < 9)) AS atlatl_g WHERE atlatl_g.atlatl_guard IS NULL)"#,
            "scanned natively behind one guard query over the rows holding a bad value, narrowed in Postgres and filtered like the read"
        );
        let tsdom = [
            domain("at", "dtstz", "timestamptz", -1, "timestamp with time zone"),
            domain("n", "dnum", "numeric", nm(5, 2), "numeric(5,2)"),
        ];
        let rel = relation("pg", "appdb", "t", &tsdom).expect("rel");
        assert!(
            rel.starts_with(r#"(SELECT CASE WHEN "atlatl_bad_0" IS NULL THEN "at" ELSE error('column `at` of `appdb.t` holds ' || "atlatl_bad_0" || ', past the last timestamp the target holds (294247-01-10 04:00:54.775806+00); "#),
            "{rel}"
        );
        assert!(
            rel.contains(r#"FROM postgres_query('pg', 'SELECT (CASE WHEN "at"::timestamp with time zone > ''294247-01-10 04:00:54.775806+00''::timestamptz AND "at"::timestamp with time zone < ''infinity'' THEN NULL ELSE "at"::timestamp with time zone END)::timestamp with time zone AS "at", CASE WHEN "at"::timestamp with time zone > ''294247-01-10 04:00:54.775806+00''::timestamptz AND "at"::timestamp with time zone < ''infinity'' THEN ("at"::timestamp with time zone)::text END AS "atlatl_bad_0", (CASE WHEN "n"::numeric(5,2) = ''NaN'' THEN NULL ELSE "n"::numeric(5,2) END)::numeric(5,2) AS "n", "#),
            "domains are converted in Postgres, their values nulled and flagged: {rel}"
        );
        let mixed = declare(
            vec![
                base("id", "int4", -1, "integer"),
                base("m", "mood", -1, "mood"),
                base("tags", "_text", -1, "text[]"),
                base("it's", "numeric", -1, "numeric"),
            ],
            &[("m", "VARCHAR"), ("tags", "JSON"), ("it's", "VARCHAR")],
        );
        assert_eq!(
            relation("pg", "appdb", "T", &mixed).expect("rel"),
            r#"(SELECT "id", CAST("m" AS VARCHAR) AS "m", CASE WHEN "tags" IS NULL OR TRY_CAST("tags" AS JSON) IS NOT NULL THEN TRY_CAST("tags" AS JSON) ELSE error('column `tags` of `appdb.T` holds ' || "tags" || ', which JSON can''t hold exactly; declare VARCHAR') END AS "tags", CAST("it's" AS VARCHAR) AS "it's" FROM postgres_query('pg', 'SELECT "id", "m"::text AS "m", to_json("tags")::text AS "tags", "it''s"::text AS "it''s" FROM "appdb"."T"'))"#
        );
        let e =
            relation("pg", "appdb", "t", &[base("m", "mood", -1, "mood")]).expect_err("refused");
        assert_eq!(
            (e.class, e.code.as_deref()),
            (ErrorClass::Config, Some("source_type"))
        );
        assert!(
            e.message.starts_with("`appdb.t`: column `m` is mood, "),
            "{e}"
        );
        assert_eq!(
            relation("pg", "a", "t", &[]).expect_err("empty").class,
            ErrorClass::Config
        );
    }

    #[test]
    fn query_reads() {
        let native = [base("id", "int4", -1, "integer")];
        assert_eq!(
            query_read("pg", "SELECT 1 AS id", &native).expect("q"),
            query_relation("pg", "SELECT 1 AS id")
        );
        let cast = declare(
            vec![base("n", "numeric", -1, "numeric")],
            &[("n", "DECIMAL(10,2)")],
        );
        let q = query_read("pg", "SELECT n FROM t -- note\n;", &cast).expect("q");
        assert!(
            q.ends_with(
                r#"FROM postgres_query('pg', 'SELECT "n"::text AS "n" FROM (SELECT n FROM t -- note
) AS q'))"#
            ),
            "{q}"
        );
        assert!(q.contains("column `n` of `from.query` holds "), "{q}");
        let mut nulled = cast.clone();
        if let Read::Declared { set_null, .. } = &mut nulled[0].read {
            *set_null = true;
        }
        let q = query_read("pg", "SELECT n FROM t", &nulled).expect("q");
        assert!(
            !q.contains("error(")
                && q.contains(
                    r#" THEN TRY_CAST("n" AS DECIMAL(10,2)) END AS "n", "n" IS NOT NULL AND NOT ("#
                )
                && q.contains(r#") AS "_atlatl_nulled_n""#),
            "{q}"
        );
        let ts = query_read(
            "pg",
            "SELECT at FROM t",
            &[base("at", "timestamptz", -1, "timestamp with time zone")],
        )
        .expect("q");
        assert!(
            ts.contains(r#"holds ' || "atlatl_bad_0" || ', past the last timestamp"#)
                && ts.contains(r#"FROM (SELECT at FROM t"#),
            "a query's timestamps are checked per row: {ts}"
        );
        let e =
            query_read("pg", "SELECT 1", &[base("m", "mood", -1, "mood")]).expect_err("refused");
        assert!(
            e.message.starts_with("`from.query`: column `m` is mood"),
            "{e}"
        );
        let twice = declare(
            vec![
                base("m", "mood", -1, "mood"),
                base("m", "int4", -1, "integer"),
            ],
            &[("m", "VARCHAR")],
        );
        assert!(
            query_read("pg", "SELECT 1", &twice)
                .expect_err("dup")
                .message
                .contains("more than once")
        );
    }

    #[test]
    fn cursor_predicates_push_only_when_safe() {
        let cols = declare(
            vec![
                base("id", "int4", -1, "integer"),
                base("at", "timestamptz", -1, "timestamp with time zone"),
                base("name", "varchar", -1, "character varying(20)"),
                base("tags", "_text", -1, "text[]"),
                base("big", "numeric", -1, "numeric"),
                base("m", "mood", -1, "mood"),
                base("u", "uuid", -1, "uuid"),
                base("n", "numeric", nm(5, 2), "numeric(5,2)"),
                domain("d", "dint", "int4", -1, "integer"),
            ],
            &[("tags", "VARCHAR"), ("big", "VARCHAR"), ("m", "VARCHAR")],
        );
        let cur = |c: &str, v: Option<&str>| Cursor {
            column: c.into(),
            stored: v.map(Into::into),
            lookback_us: None,
            inclusive: true,
            include_nulls: false,
        };
        assert_eq!(
            cursor_predicate(&cur("at", Some("2024-01-01 00:00:00+00")), &cols).as_deref(),
            Some(r#""at" >= '2024-01-01 00:00:00+00'::timestamptz"#)
        );
        assert_eq!(
            cursor_predicate(&cur("AT", None), &cols).as_deref(),
            Some(r#""at" IS NOT NULL"#),
            "no watermark yet: nulls are still excluded"
        );
        assert_eq!(
            cursor_predicate(
                &Cursor {
                    lookback_us: Some(3_600_000_000),
                    inclusive: false,
                    include_nulls: true,
                    ..cur("at", Some("2024-01-02 00:00:00+00"))
                },
                &cols
            )
            .as_deref(),
            Some(
                r#"("at" > ('2024-01-02 00:00:00+00'::timestamptz - '1 hours 0 seconds 0 microseconds'::interval) OR "at" IS NULL)"#
            )
        );
        assert_eq!(
            cursor_predicate(
                &Cursor {
                    lookback_us: Some(5),
                    ..cur("id", Some("7"))
                },
                &cols
            ),
            None,
            "a lookback only pushes on dates and timestamps"
        );
        assert_eq!(
            cursor_predicate(
                &Cursor {
                    lookback_us: Some(250_000),
                    ..cur("at", Some("2024-01-02 00:00:00+00"))
                },
                &cols
            )
            .as_deref(),
            Some(
                r#""at" >= ('2024-01-02 00:00:00+00'::timestamptz - '0 hours 0 seconds 250000 microseconds'::interval)"#
            ),
            "a sub-second lookback keeps its precision"
        );
        assert_eq!(
            cursor_predicate(&cur("name", Some("it's")), &cols).as_deref(),
            Some(r#""name" COLLATE "C" >= 'it''s'::text"#),
            "text compares byte-wise, as the engine does"
        );
        assert_eq!(
            cursor_predicate(&cur("n", Some("1.50")), &cols).as_deref(),
            Some(r#""n" >= '1.50'::numeric"#),
            "a decimal is read as stored"
        );
        for c in ["tags", "big", "m", "u", "d", "missing"] {
            assert_eq!(cursor_predicate(&cur(c, Some("x")), &cols), None, "{c}");
        }
        assert_eq!(
            cursor_predicate(
                &Cursor {
                    include_nulls: true,
                    ..cur("at", None)
                },
                &cols
            ),
            None,
            "nothing to filter"
        );

        let (rel, pushed) = relation_where(
            "pg",
            "appdb",
            "t",
            &cols[..4],
            Some(r#""id" >= '1'::int4"#),
            &[],
        )
        .expect("rel");
        assert!(pushed);
        assert!(
            rel.ends_with(r#"FROM "appdb"."t" WHERE "id" >= ''1''::int4'))"#),
            "{rel}"
        );
        let (rel, pushed) =
            relation_where("pg", "appdb", "t", &cols[..2], Some("x"), &[]).expect("rel");
        assert!(
            !pushed
                && rel.contains(r#"FROM "pg"."appdb"."t" AS atlatl_t CROSS JOIN"#)
                && rel.contains(r#"AND (x)')) AS x)"#),
            "native scans filter engine-side; only the guard is narrowed: {rel}"
        );
        let (rel, _) = relation_where("pg", "appdb", "t", &cols[..1], Some("x"), &[]).expect("rel");
        assert_eq!(rel, r#"(SELECT "id" FROM "pg"."appdb"."t")"#);
        assert_eq!(
            set_snapshot_sql("pg", "00000003-0000001B-1"),
            "CALL postgres_execute('pg', 'SET TRANSACTION SNAPSHOT ''00000003-0000001B-1''')"
        );
    }

    #[test]
    fn window_predicates_keep_at_least_the_window() {
        let cols = declare(
            vec![
                base("id", "int4", -1, "integer"),
                base("at", "timestamptz", -1, "timestamp with time zone"),
                base("ts", "timestamp", -1, "timestamp without time zone"),
                base("d", "date", -1, "date"),
                base("name", "text", -1, "text"),
                base("n", "numeric", nm(12, 2), "numeric(12,2)"),
                base("f", "float8", -1, "double precision"),
                base("m", "mood", -1, "mood"),
                domain("di", "dint", "int4", -1, "integer"),
            ],
            &[("m", "VARCHAR")],
        );
        let duck = duckdb::Connection::open_in_memory().expect("duckdb");
        let pre =
            |w: &str| window_predicate(w, &cols, &|sql| duck.query_row(sql, [], |r| r.get(0)).ok());
        let at = r#"("at" > '294247-01-10 04:00:54.775806+00'::timestamptz AND "at" < 'infinity')"#;
        assert_eq!(
            pre(r#"x."AT" >= '2024-01-01 00:00:00+00' AND id BETWEEN 1 AND 2.5 AND 7 > id"#)
                .as_deref(),
            Some(
                format!(
                    r#"("at" >= '2024-01-01 00:00:00+00'::timestamptz OR {at}) AND ("id" >= '1'::numeric AND "id" <= '2.5'::numeric) AND ("id" < '7'::numeric)"#
                )
                .as_str()
            ),
            "a column holding values the guard looks for keeps them"
        );
        assert_eq!(
            pre("name < 'b''c' AND d = DATE '2024-02-29' AND n <> -0.05 AND name IS NOT NULL")
                .as_deref(),
            Some(
                r#"("name" COLLATE "C" < 'b''c'::text) AND ("d" = '2024-02-29'::date) AND ("n" <> '-0.05'::numeric OR ("n" = 'NaN')) AND ("name" IS NOT NULL)"#
            )
        );
        assert_eq!(
            pre(r#""at" >= TIMESTAMPTZ '2024-01-01T00:00:00Z' AND (id = 1 OR id = 2) AND id + 1 > 3 AND f > 1 AND m = 'happy' AND di > 1 AND id > '1'"#).as_deref(),
            Some(format!(r#"("at" >= '2024-01-01T00:00:00Z'::timestamptz OR {at})"#).as_str()),
            "anything not compared exactly in both is left out"
        );
        for w in [
            "ts >= '2024-01-01 00:00:00+05'",
            "d >= '2024-01-01 12:00'",
            r#""at" >= TIMESTAMP '2024-01-01'"#,
            r#""at" < now() - INTERVAL 1 DAY"#,
            "d >= current_date - id",
            "d >= current_date - random()",
            "name >= current_date",
            "d >= '01/02/2024'",
            r#"id > 1 OR "at" > '2024-01-01'"#,
            "id > 1; SELECT 1",
            "id >",
        ] {
            assert_eq!(pre(w), None, "{w}");
        }
        let day = |sql: &str| -> String { duck.query_row(sql, [], |r| r.get(0)).expect("day") };
        let (ago, today) = (
            day("SELECT (current_date - 45)::VARCHAR"),
            day("SELECT current_date::VARCHAR"),
        );
        assert_eq!(
            pre("d >= current_date - 45 AND current_date < d"),
            Some(format!(
                r#"("d" >= '{ago}'::date) AND ("d" >= '{today}'::date)"#
            )),
            "a lower bound that only moves forward is taken before the read takes it"
        );
        let p = pre(r#""at" > now() - INTERVAL 1 HOUR AND ts >= today() + 1"#).expect("rising");
        assert!(
            p.starts_with(r#"("at" >= '20"#)
                && p.contains(r#"::timestamptz OR ("at" > "#)
                && p.contains(r#"AND ("ts" >= '20"#),
            "{p}"
        );
        assert_eq!(
            pre("ts > '2024-01-01 10:00:00.123456' AND d >= '2024-01-01'").as_deref(),
            Some(
                r#"("ts" > '2024-01-01 10:00:00.123456'::timestamp OR ("ts" > '294247-01-10 04:00:54.775806'::timestamp AND "ts" < 'infinity')) AND ("d" >= '2024-01-01'::date)"#
            )
        );
    }

    #[test]
    fn discovery_matching_is_bytewise_sorted() {
        let rows = [
            ("appdb", "b"),
            ("appdb", "B"),
            ("appdb", "a_b"),
            ("appdb", "_x"),
            ("appdb", "aB"),
            ("appdb", "old_users"),
            ("other", "a"),
            ("appdb", "dot.ted"),
        ]
        .map(|(s, t)| (s.to_owned(), t.to_owned()));
        assert_eq!(
            match_tables(rows.clone(), "appdb.*", &["appdb.old_*".into()]),
            ["appdb.B", "appdb._x", "appdb.aB", "appdb.a_b", "appdb.b"]
        );
        assert_eq!(match_tables(rows.clone(), "*.a", &[]), ["other.a"]);
        assert_eq!(match_tables(rows, "appdb.a?b", &[]), ["appdb.a_b"]);
        assert_eq!(split("users_*"), ("public".into(), "users_*".into()));
    }

    #[test]
    fn dsn_quotes_and_redacts() {
        let mut o = ConnectOptions::new("h", 5433, "d b", "u", Some("p'w\\x".into()));
        o.statement_timeout = Some(std::time::Duration::from_secs(5));
        let dsn = libpq_dsn(&o);
        assert!(
            dsn.starts_with(
                "host='h' port='5433' dbname='d b' user='u' password='p\\'w\\\\x' sslmode='disable'"
            ),
            "{dsn}"
        );
        assert!(dsn.contains("keepalives='1' keepalives_idle='30'"), "{dsn}");
        assert!(
            dsn.ends_with("options='-c statement_timeout=5000 -c TimeZone=UTC'"),
            "{dsn}"
        );
        let sql = attach_sql("src", &o);
        assert!(
            sql.ends_with(r#"AS "src" (TYPE postgres, READ_ONLY)"#),
            "{sql}"
        );
        assert!(
            !redact::redact(&sql).contains("w\\\\x"),
            "{}",
            redact::redact(&sql)
        );
    }

    #[test]
    fn scanner_errors() {
        let cases = [
            (
                "IO Error: Unable to connect to Postgres at \"…\": FATAL:  password authentication failed for user \"x\"",
                ErrorClass::Auth,
            ),
            (
                "IO Error: Unable to connect to Postgres at \"…\": FATAL:  database \"nope\" does not exist",
                ErrorClass::Config,
            ),
            (
                "Invalid Error: Failed to prepare COPY \"…\": ERROR:  permission denied for table secret",
                ErrorClass::Config,
            ),
            (
                "IO Error: Failed to fetch header for COPY: ERROR:  canceling statement due to statement timeout",
                ErrorClass::Timeout,
            ),
            (
                "Invalid Error: Failed to execute query \"…\": ERROR:  cannot execute CREATE TABLE in a read-only transaction",
                ErrorClass::Config,
            ),
        ];
        for (m, want) in cases {
            assert_eq!(reclassify(AtlatlError::internal(m)).class, want, "{m}");
        }
        let lost = reclassify(AtlatlError::internal(
            "IO Error: … server closed the connection unexpectedly",
        ));
        assert_eq!(
            (lost.class, lost.code.as_deref()),
            (ErrorClass::Transient, Some("connection_lost"))
        );
        let kept = reclassify(AtlatlError::data(
            "NULL in primary key column `id` (permission denied)",
        ));
        assert_eq!(kept.class, ErrorClass::Data);
    }
}
