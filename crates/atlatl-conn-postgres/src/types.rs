//! What a Postgres column lands as (06 §3.2). Only types the scanner reads faithfully pass; the
//! rest are refused with a conversion to write in `from.query`, or land through their exact text
//! when `columns.<c>.type` says what to make of them. Types are judged by their base type, so a
//! domain is read as the type it's over. Floating-point precision loss is accepted, as elsewhere.
//!
//! `numeric(p,s)` within 38 digits passes, but the scanner reads `NaN` as NULL; it's looked for in
//! Postgres and fails the read, as does a timestamp past DuckDB's range ([`crate::source`]).

/// How a type is read when nothing is declared for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Class {
    /// The scanner reads it exactly.
    Native,
    /// `numeric(p,s)` with `p <= 38`: exact, except that `NaN` must be caught.
    Decimal,
    Refused(Refusal),
}

/// Why a type isn't landed as is, and what to do about it. In `convert`, `{c}` is the column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refusal {
    pub why: &'static str,
    /// Postgres SQL for `from.query`.
    pub convert: &'static str,
    /// What to declare in `columns.<c>.type`.
    pub declare: &'static str,
    /// The exact text it's read through when declared.
    pub text: Text,
}

/// A refused column's exact text form, rendered in Postgres.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Text {
    /// `::text`
    Plain,
    /// `::numeric::text` (`money`, whose own text follows `lc_monetary`).
    Numeric,
    /// `to_json(…)::text` (arrays and composites).
    Json,
    /// `"char"`'s byte, escaped like `bytea`: Postgres before 15 renders a non-ASCII one as is.
    Char,
}

impl Text {
    pub fn sql(self, e: &str) -> String {
        match self {
            Text::Plain => format!("{e}::text"),
            Text::Numeric => format!("{e}::numeric::text"),
            Text::Json => format!("to_json({e})::text"),
            Text::Char => CHAR_TEXT.replace("{c}", e),
        }
    }
}

const fn refuse(
    why: &'static str,
    convert: &'static str,
    declare: &'static str,
    text: Text,
) -> Class {
    Class::Refused(Refusal {
        why,
        convert,
        declare,
        text,
    })
}

const AS_TEXT: &str = "`{c}::text`";
const CHAR_TEXT: &str = "encode(set_byte('\\x00'::bytea, 0, {c}::int4 & 255), 'escape')";

/// Classifies a base type (never a domain) from `pg_type` and the column's typmod.
pub fn classify(typtype: &str, typcategory: &str, typname: &str, typmod: i32) -> Class {
    use Text::{Json, Plain};
    if typcategory == "A" {
        return refuse(
            "a Postgres array carries its own bounds and dimensions, which a list can't hold",
            "`to_json({c})::text`",
            "JSON",
            Json,
        );
    }
    match typtype {
        "c" => {
            return refuse(
                "there's no composite type",
                "`to_json({c})::text`",
                "JSON",
                Json,
            );
        }
        "e" => return refuse("there's no enum type", AS_TEXT, "VARCHAR", Plain),
        "r" | "m" => {
            return refuse(
                "there's no range type",
                "`lower({c})`, `upper({c})`, `lower_inc({c})`, `upper_inc({c})`, or `{c}::text`",
                "VARCHAR",
                Plain,
            );
        }
        "b" => {}
        _ => return other(),
    }
    if typcategory == "G" {
        return refuse("there's no geometric type", AS_TEXT, "VARCHAR", Plain);
    }
    match typname {
        "bool" | "int2" | "int4" | "int8" | "float4" | "float8" | "bpchar" | "varchar" | "text"
        | "name" | "bytea" | "date" | "time" | "timetz" | "timestamp" | "timestamptz"
        | "interval" | "uuid" | "json" | "jsonb" => Class::Native,
        "numeric" if numeric_precision(typmod).is_some() => Class::Decimal,
        "numeric" => refuse(
            "decimals hold at most 38 digits at a fixed scale, and it would arrive as a float",
            "`{c}::numeric(38,s)` with a scale that fits, or `{c}::text`",
            "DECIMAL(38,s), or VARCHAR",
            Plain,
        ),
        "money" => refuse(
            "its text follows the server's `lc_monetary`",
            "`{c}::numeric(19,2)`",
            "DECIMAL(19,2)",
            Text::Numeric,
        ),
        "oid" => refuse(
            "it arrives unsigned from a table and as text from a query",
            "`{c}::int8`",
            "BIGINT",
            Plain,
        ),
        "char" => refuse(
            "it's a single byte, and one that isn't valid UTF-8 on its own fails the read",
            "`encode(set_byte('\\x00'::bytea, 0, {c}::int4 & 255), 'escape')`, or `{c}::int4 & 255` for the byte",
            "VARCHAR",
            Text::Char,
        ),
        "xml" => refuse("there's no XML type", AS_TEXT, "VARCHAR", Plain),
        "tsvector" | "tsquery" => refuse("there's no text-search type", AS_TEXT, "VARCHAR", Plain),
        "inet" | "cidr" | "macaddr" | "macaddr8" => {
            refuse("there's no network address type", AS_TEXT, "VARCHAR", Plain)
        }
        "bit" | "varbit" => refuse("there's no bit-string type", AS_TEXT, "VARCHAR", Plain),
        "pg_lsn" => refuse("there's no WAL position type", AS_TEXT, "VARCHAR", Plain),
        n if n.starts_with("reg") => refuse(
            "it's an OID alias whose text follows `search_path`",
            "`{c}::oid::int8`, or `{c}::text`",
            "VARCHAR",
            Plain,
        ),
        _ => other(),
    }
}

fn other() -> Class {
    refuse(
        "atlatl doesn't land this type from Postgres",
        "`{c}::text`, or another type it lands: numbers, text, bytea, dates and times, uuid, json",
        "VARCHAR",
        Text::Plain,
    )
}

/// `(precision, scale)` of a `numeric` typmod when a 38-digit decimal holds it exactly.
pub fn numeric_precision(typmod: i32) -> Option<(u8, u8)> {
    if typmod < 4 {
        return None;
    }
    let m = typmod - 4;
    let (p, s) = ((m >> 16) & 0xffff, m & 0xffff);
    ((1..=38).contains(&p) && s <= p).then_some((p as u8, s as u8))
}

/// What a refused column's text can be declared as and checked against exactly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    Text,
    Json,
    Decimal(u8, u8),
    Integer,
    Float,
}

/// The target family of a declared DuckDB type; `None` for one a text can't be checked against.
pub fn target(kind: &str) -> Option<Target> {
    let k: String = kind
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>()
        .to_ascii_uppercase();
    let (name, args) = match k.split_once('(') {
        Some((n, rest)) => (n.to_owned(), Some(rest.strip_suffix(')')?.to_owned())),
        None => (k.clone(), None),
    };
    Some(match name.as_str() {
        "VARCHAR" | "TEXT" | "STRING" | "CHAR" | "BPCHAR" => Target::Text,
        "JSON" if args.is_none() => Target::Json,
        "DECIMAL" | "NUMERIC" => {
            let (p, s) = match args.as_deref() {
                None => (18, 3),
                Some(a) => match a.split_once(',') {
                    Some((p, s)) => (p.parse().ok()?, s.parse().ok()?),
                    None => (a.parse().ok()?, 0),
                },
            };
            if !(1..=38).contains(&p) || s > p {
                return None;
            }
            Target::Decimal(p, s)
        }
        "TINYINT" | "INT1" | "SMALLINT" | "INT2" | "SHORT" | "INTEGER" | "INT4" | "INT"
        | "SIGNED" | "BIGINT" | "INT8" | "LONG" | "HUGEINT" | "UTINYINT" | "USMALLINT"
        | "UINTEGER" | "UBIGINT" | "UHUGEINT"
            if args.is_none() =>
        {
            Target::Integer
        }
        "FLOAT" | "FLOAT4" | "REAL" | "DOUBLE" | "FLOAT8" if args.is_none() => Target::Float,
        _ => return None,
    })
}

/// DuckDB SQL true where the Postgres text `t` (non-NULL) lands in `kind` exactly, and the
/// error's hint when it doesn't; `None` when every text does. DuckDB's own casts from text round
/// decimals and integers, and raise on overflow or malformed text.
fn fits(t: &str, kind: &str, target: Target) -> Option<(String, &'static str)> {
    let lands = format!("TRY_CAST({t} AS {kind})");
    let (shape, why) = match target {
        Target::Text => return None,
        Target::Json => (None, "; declare VARCHAR"),
        Target::Decimal(p, s) => {
            let int = match p - s {
                0 => String::new(),
                n => format!("[0-9]{{0,{n}}}"),
            };
            let frac = match s {
                0 => String::new(),
                n => format!("[0-9]{{0,{n}}}"),
            };
            (
                Some(format!(
                    "regexp_full_match({t}, {})",
                    duck_literal(&format!("-?0*{int}(\\.{frac}0*)?"))
                )),
                "; declare a wider `DECIMAL(p,s)`",
            )
        }
        Target::Integer => (
            Some(format!("regexp_full_match({t}, '-?[0-9]+')")),
            "; declare a wider integer or a DECIMAL",
        ),
        Target::Float => (
            Some(format!(
                "(NOT isinf({lands}) OR {t} IN ('Infinity', '-Infinity'))"
            )),
            "",
        ),
    };
    let ok = format!("{lands} IS NOT NULL");
    Some((shape.map_or(ok.clone(), |s| format!("{ok} AND {s}")), why))
}

/// DuckDB SQL landing the Postgres text `t` (a quoted column) as `kind`, raising rather than
/// rounding or overflowing. `what` names the column in the error.
pub fn guarded(t: &str, kind: &str, target: Target, what: &str) -> String {
    let Some((ok, why)) = fits(t, kind, target) else {
        return format!("CAST({t} AS {kind})");
    };
    format!(
        "CASE WHEN {t} IS NULL OR {ok} THEN TRY_CAST({t} AS {kind}) ELSE error({} || {t} || {}) END",
        duck_literal(&format!("{what} holds ")),
        duck_literal(&format!(", which {kind} can't hold exactly{why}"))
    )
}

/// [`guarded`] under `on_cast_error.set_null`: NULL where it would raise, and the predicate
/// flagging those (`None` when nothing can be nulled).
pub fn nulling(t: &str, kind: &str, target: Target) -> (String, Option<String>) {
    match fits(t, kind, target) {
        None => (format!("CAST({t} AS {kind})"), None),
        Some((ok, _)) => (
            format!("CASE WHEN {t} IS NULL OR {ok} THEN TRY_CAST({t} AS {kind}) END"),
            Some(format!("{t} IS NOT NULL AND NOT ({ok})")),
        ),
    }
}

pub fn duck_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// A column name as it reads in a hint: bare when Postgres needs no quotes.
pub fn hint_ident(name: &str) -> String {
    let plain = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if plain {
        name.to_owned()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// `column `x` is T, which the target can't hold: …; convert it in `from.query` (…), or declare
/// `columns.x.type` (…)`. `base` is the type a domain is over.
pub fn message(name: &str, pg_type: &str, base: Option<&str>, r: &Refusal) -> String {
    let of = base.map(|b| format!(" (its {b})")).unwrap_or_default();
    format!(
        "column `{name}` is {pg_type}{of}, which the target can't hold: {}; convert it in `from.query` ({}), or declare `columns.{name}.type` ({})",
        r.why,
        r.convert.replace("{c}", &hint_ident(name)),
        r.declare
    )
}

/// One `config` error for every column refused, each line naming its member.
pub fn refused(found: &[(String, String)]) -> atlatl_core::AtlatlError {
    let msg = match found {
        [(member, line)] => format!("`{member}`: {line}"),
        _ => format!(
            "{} source columns can't be landed as they are:\n{}",
            found.len(),
            found
                .iter()
                .map(|(m, l)| format!("  `{m}`: {l}"))
                .collect::<Vec<_>>()
                .join("\n")
        ),
    };
    atlatl_core::AtlatlError::config(msg).code("source_type")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nm(p: i32, s: i32) -> i32 {
        ((p << 16) | s) + 4
    }

    #[test]
    fn classifies_by_base_type() {
        for t in [
            "bool",
            "int2",
            "int4",
            "int8",
            "float4",
            "float8",
            "bpchar",
            "varchar",
            "text",
            "name",
            "bytea",
            "date",
            "time",
            "timetz",
            "timestamp",
            "timestamptz",
            "interval",
            "uuid",
            "json",
            "jsonb",
        ] {
            assert_eq!(classify("b", "X", t, -1), Class::Native, "{t}");
        }
        assert_eq!(classify("b", "N", "numeric", nm(12, 4)), Class::Decimal);
        assert_eq!(classify("b", "N", "numeric", nm(38, 38)), Class::Decimal);
        let why = |tt: &str, cat: &str, n: &str, m: i32| match classify(tt, cat, n, m) {
            Class::Refused(r) => (r.why, r.declare, r.text),
            c => panic!("{n}: {c:?}"),
        };
        assert_eq!(why("b", "N", "numeric", -1).2, Text::Plain, "unconstrained");
        assert!(why("b", "N", "numeric", nm(39, 2)).0.contains("38 digits"));
        assert_eq!(why("b", "N", "money", -1).2, Text::Numeric);
        assert_eq!(why("b", "N", "oid", -1).1, "BIGINT");
        assert_eq!(
            why("b", "A", "_int4", -1),
            (why("b", "A", "_int4", -1).0, "JSON", Text::Json)
        );
        assert_eq!(why("c", "C", "pair", -1).2, Text::Json);
        assert_eq!(why("e", "E", "mood", -1).0, "there's no enum type");
        assert_eq!(why("r", "R", "int4range", -1).0, "there's no range type");
        assert_eq!(
            why("m", "R", "int4multirange", -1).0,
            "there's no range type"
        );
        assert_eq!(why("b", "G", "point", -1).0, "there's no geometric type");
        assert!(why("b", "N", "regclass", -1).0.contains("search_path"));
        for t in [
            "char", "xml", "tsvector", "tsquery", "inet", "cidr", "macaddr", "macaddr8", "bit",
            "varbit", "pg_lsn",
        ] {
            assert_eq!(why("b", "U", t, -1).1, "VARCHAR", "{t}");
        }
        assert!(
            why("b", "U", "citext", -1)
                .0
                .contains("doesn't land this type")
        );
        assert!(
            why("p", "P", "anyelement", -1)
                .0
                .contains("doesn't land this type")
        );
    }

    #[test]
    fn messages_mirror_the_other_sources() {
        let Class::Refused(r) = classify("b", "N", "money", -1) else {
            panic!()
        };
        assert_eq!(
            message("amount", "money", None, &r),
            "column `amount` is money, which the target can't hold: its text follows the server's `lc_monetary`; convert it in `from.query` (`amount::numeric(19,2)`), or declare `columns.amount.type` (DECIMAL(19,2))"
        );
        let Class::Refused(r) = classify("b", "A", "_text", -1) else {
            panic!()
        };
        assert!(message("Tags", "tags_t", Some("text[]"), &r).starts_with(
            "column `Tags` is tags_t (its text[]), which the target can't hold: a Postgres array"
        ));
        assert!(message("Tags", "text[]", None, &r).contains("(`to_json(\"Tags\")::text`)"));
        let Class::Refused(r) = classify("b", "Z", "char", -1) else {
            panic!()
        };
        assert!(
            message("k", "\"char\"", None, &r).contains(
                "convert it in `from.query` (`encode(set_byte('\\x00'::bytea, 0, k::int4 & 255), 'escape')`, or `k::int4 & 255` for the byte)"
            )
        );
        assert_eq!(
            r.text.sql("\"k\""),
            "encode(set_byte('\\x00'::bytea, 0, \"k\"::int4 & 255), 'escape')"
        );
        let one = refused(&[("appdb.t".into(), "column `a` is x".into())]);
        assert_eq!(one.message, "`appdb.t`: column `a` is x");
        assert_eq!(one.code.as_deref(), Some("source_type"));
        assert_eq!(
            refused(&[
                ("appdb.t".into(), "column `a` is x".into()),
                ("appdb.u".into(), "column `b` is y".into()),
            ])
            .message,
            "2 source columns can't be landed as they are:\n  `appdb.t`: column `a` is x\n  `appdb.u`: column `b` is y"
        );
    }

    #[test]
    fn declared_targets() {
        assert_eq!(target("varchar"), Some(Target::Text));
        assert_eq!(target("JSON"), Some(Target::Json));
        assert_eq!(target("decimal(38, 10)"), Some(Target::Decimal(38, 10)));
        assert_eq!(target("NUMERIC(12)"), Some(Target::Decimal(12, 0)));
        assert_eq!(target("DECIMAL"), Some(Target::Decimal(18, 3)));
        assert_eq!(target("bigint"), Some(Target::Integer));
        assert_eq!(target("DOUBLE"), Some(Target::Float));
        for k in [
            "DECIMAL(39,2)",
            "DECIMAL(5,6)",
            "TIMESTAMP",
            "INTEGER[]",
            "STRUCT(a INT)",
            "UUID",
        ] {
            assert_eq!(target(k), None, "{k}");
        }
    }

    #[test]
    fn guards_render() {
        assert_eq!(
            guarded("\"n\"", "DECIMAL(5,2)", Target::Decimal(5, 2), "column `n`"),
            "CASE WHEN \"n\" IS NULL OR TRY_CAST(\"n\" AS DECIMAL(5,2)) IS NOT NULL AND regexp_full_match(\"n\", '-?0*[0-9]{0,3}(\\.[0-9]{0,2}0*)?') THEN TRY_CAST(\"n\" AS DECIMAL(5,2)) ELSE error('column `n` holds ' || \"n\" || ', which DECIMAL(5,2) can''t hold exactly; declare a wider `DECIMAL(p,s)`') END"
        );
        assert!(
            guarded("t", "DECIMAL(3,3)", Target::Decimal(3, 3), "c")
                .contains("'-?0*(\\.[0-9]{0,3}0*)?'")
        );
        assert!(
            guarded("t", "DECIMAL(3,0)", Target::Decimal(3, 0), "c")
                .contains("'-?0*[0-9]{0,3}(\\.0*)?'")
        );
        assert_eq!(
            guarded("t", "VARCHAR", Target::Text, "c"),
            "CAST(t AS VARCHAR)"
        );
    }

    fn land(expr: &str, t: &str) -> duckdb::Result<Option<String>> {
        duckdb::Connection::open_in_memory()?.query_row(
            &format!(
                "SELECT ({expr})::VARCHAR FROM (SELECT {} AS t)",
                duck_literal(t)
            ),
            [],
            |r| r.get(0),
        )
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn declared_casts_never_raise_unnamed() {
        let cases = [
            ("INTEGER", Target::Integer, "2147483648"),
            ("UINTEGER", Target::Integer, "-1"),
            ("TINYINT", Target::Integer, "1.0"),
            ("JSON", Target::Json, "abc"),
            ("DOUBLE", Target::Float, "abc"),
            ("FLOAT", Target::Float, "1e39"),
            ("DECIMAL(4,2)", Target::Decimal(4, 2), "123.4"),
            ("DECIMAL(4,2)", Target::Decimal(4, 2), "x"),
        ];
        for (kind, target, t) in cases {
            let (e, flag) = nulling("t", kind, target);
            assert_eq!(land(&e, t).unwrap(), None, "{kind} {t}");
            assert_eq!(
                land(&flag.unwrap(), t).unwrap().as_deref(),
                Some("true"),
                "{kind} {t}"
            );
            let err = land(&guarded("t", kind, target, "column `c`"), t)
                .unwrap_err()
                .to_string();
            assert!(
                err.contains(&format!(
                    "column `c` holds {t}, which {kind} can't hold exactly"
                )),
                "{kind} {t}: {err}"
            );
        }
        let fine = [
            ("INTEGER", Target::Integer, "-2147483648", "-2147483648"),
            ("UINTEGER", Target::Integer, "4294967295", "4294967295"),
            ("JSON", Target::Json, r#"{"a": 1}"#, r#"{"a": 1}"#),
            ("DOUBLE", Target::Float, "-Infinity", "-inf"),
            ("DOUBLE", Target::Float, "NaN", "nan"),
            ("DECIMAL(4,2)", Target::Decimal(4, 2), "-12.30", "-12.30"),
        ];
        for (kind, target, t, want) in fine {
            assert_eq!(
                land(&guarded("t", kind, target, "c"), t)
                    .unwrap()
                    .as_deref(),
                Some(want),
                "{kind} {t}"
            );
            let (e, flag) = nulling("t", kind, target);
            assert_eq!(land(&e, t).unwrap().as_deref(), Some(want), "{kind} {t}");
            assert_eq!(
                land(&flag.unwrap(), t).unwrap().as_deref(),
                Some("false"),
                "{kind} {t}"
            );
        }
    }

    #[test]
    fn hint_idents() {
        assert_eq!(hint_ident("amount_2"), "amount_2");
        assert_eq!(hint_ident("Amount"), "\"Amount\"");
        assert_eq!(hint_ident("a b"), "\"a b\"");
        assert_eq!(hint_ident("2x"), "\"2x\"");
    }
}
