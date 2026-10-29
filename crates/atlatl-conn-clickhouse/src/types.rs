//! What a query's columns land as. ClickHouse's Parquet output converts some types without a word
//! (an `IPv4` becomes a number, `DateTime64(9)` loses its nanoseconds, a 128-bit integer becomes
//! raw bytes), and whatever lands first becomes the target table's type. So each member's query
//! is `DESCRIBE`d first and only types known to land faithfully pass; the rest are refused with a
//! conversion to write in `from.query`. The one conversion that is lossless is made here:
//! top-level 128-bit integers are fetched as decimal text and cast back to DuckDB's
//! `HUGEINT`/`UHUGEINT`.
//!
//! At the time of writing (DuckDB 1.5) DuckDB has no 256-bit integer type, so `Int256`/`UInt256`
//! are refused for now. Landing them as text or a float would make that the table's type, and
//! moving to a real 256-bit type later would mean altering tables. Revisit once DuckDB (or a target
//! that isn't DuckDB) has one.
//!
//! Floating-point precision loss is accepted, as elsewhere: `Decimal` wider than 38 digits arrives
//! as `DOUBLE`.

use crate::ident;

/// A top-level 128-bit integer column: its name and the DuckDB type it lands as.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Wide {
    pub name: String,
    pub cast: &'static str,
}

/// Checks `cols` (name, ClickHouse type from `DESCRIBE`) and returns the 128-bit integer columns
/// to fetch as text, or a line for every column the target can't hold.
pub fn check(cols: &[(String, String)]) -> std::result::Result<Vec<Wide>, Vec<String>> {
    let mut wide = Vec::new();
    let mut refused = Vec::new();
    for (name, ty) in cols {
        match visit(ty, true) {
            Ok(Some(cast)) => wide.push(Wide {
                name: name.clone(),
                cast,
            }),
            Ok(None) => {}
            Err((inner, why)) => {
                let of = if &inner == ty {
                    String::new()
                } else {
                    format!(" (its {inner})")
                };
                refused.push(format!(
                    "column `{name}` is {ty}{of}, which the target can't hold: {why}"
                ));
            }
        }
    }
    if refused.is_empty() {
        Ok(wide)
    } else {
        Err(refused)
    }
}

/// `Some(cast)` for a top-level 128-bit integer; the offending type and why on a refusal.
fn visit(ty: &str, top: bool) -> std::result::Result<Option<&'static str>, (String, String)> {
    let ty = ty.trim();
    let (name, args) = split(ty);
    let no = |why: String| Err((ty.to_owned(), why));
    match name {
        "Nullable" | "LowCardinality" => visit(args.first().copied().unwrap_or(""), top),
        "Int128" | "UInt128" if top => Ok(Some(if name == "Int128" {
            "HUGEINT"
        } else {
            "UHUGEINT"
        })),
        "Int128" | "UInt128" => no(
            "inside an Array, Map or Tuple a 128-bit integer arrives as raw bytes; convert the values in `from.query` (e.g. `arrayMap(v -> toString(v), x)`)".into(),
        ),
        "Int256" | "UInt256" => no(
            "DuckDB has no 256-bit integer type at the time of writing; convert it in `from.query` (`toString(x)`, `toFloat64(x)` or `toDecimal128(x, 0)`)".into(),
        ),
        "DateTime64" => {
            let p: u32 = args.first().and_then(|a| a.trim().parse().ok()).unwrap_or(3);
            if p > 6 {
                no("timestamps are kept to the microsecond; convert it in `from.query` (`toDateTime64(x, 6)`, or `toString(x)` to keep every digit)".into())
            } else {
                Ok(None)
            }
        }
        "IPv4" | "IPv6" => no(
            "there's no IP address type, and it would arrive as a number or raw bytes; convert it in `from.query` (`toString(x)`)".into(),
        ),
        "Enum" | "Enum8" | "Enum16" => no(
            "there's no enum type; convert it in `from.query` (`toString(x)` for the label, or `CAST(x AS Int16)` for the value)".into(),
        ),
        "FixedString" => no(
            "there's no fixed-length string type, and it would arrive as bytes; convert it in `from.query` (`toString(x)` for text, `hex(x)` for binary)".into(),
        ),
        "Array" | "Map" => {
            for a in &args {
                visit(a, false)?;
            }
            Ok(None)
        }
        "Tuple" | "Nested" => {
            for a in &args {
                visit(element_type(a), false)?;
            }
            Ok(None)
        }
        // Lands as its last argument's type.
        "SimpleAggregateFunction" => visit(args.last().copied().unwrap_or(""), top),
        "Int8" | "Int16" | "Int32" | "Int64" | "UInt8" | "UInt16" | "UInt32" | "UInt64"
        | "Float32" | "Float64" | "Decimal" | "Decimal32" | "Decimal64" | "Decimal128"
        | "Decimal256" | "Bool" | "String" | "Date" | "Date32" | "DateTime" | "UUID" | "JSON" => {
            Ok(None)
        }
        "Nothing" => no(
            "it has no type (an untyped `NULL` or empty array); give it one in `from.query` (`CAST(NULL AS Nullable(String))`, `CAST([] AS Array(String))`)".into(),
        ),
        // Anything not known to land faithfully, from `AggregateFunction` states (opaque bytes)
        // and geo types to types added after this was written.
        _ => no(
            "atlatl doesn't land this type from ClickHouse; convert it in `from.query` to one it does (numbers, strings, dates, timestamps, UUID, JSON, Array, Map, Tuple)".into(),
        ),
    }
}

/// A type's name and its top-level arguments: `Map(String, Array(Int8))` is `Map` and
/// `[String, Array(Int8)]`. Quotes (enum labels, time zones) and backticks are skipped over.
pub(crate) fn split(ty: &str) -> (&str, Vec<&str>) {
    let Some(open) = ty.find('(') else {
        return (ty, Vec::new());
    };
    let name = ty[..open].trim();
    let body = ty[open + 1..].strip_suffix(')').unwrap_or(&ty[open + 1..]);
    let mut args = Vec::new();
    let (mut depth, mut start, mut quote) = (0i32, 0, None::<char>);
    let mut chars = body.char_indices();
    while let Some((i, c)) = chars.next() {
        match (quote, c) {
            (Some(_), '\\') => {
                chars.next();
            }
            (Some(q), c) if c == q => quote = None,
            (Some(_), _) => {}
            (None, '\'' | '`' | '"') => quote = Some(c),
            (None, '(') => depth += 1,
            (None, ')') => depth -= 1,
            (None, ',') if depth == 0 => {
                args.push(body[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = body[start..].trim();
    if !last.is_empty() {
        args.push(last);
    }
    (name, args)
}

/// A tuple element's type, without its name: `x Array(Int8)` and `` `a b` UInt8 `` give the type;
/// an unnamed element is all type.
pub(crate) fn element_type(el: &str) -> &str {
    let el = el.trim();
    if let Some(rest) = el.strip_prefix('`') {
        return rest.find('`').map_or(el, |i| rest[i + 1..].trim());
    }
    match (el.find(' '), el.find('(')) {
        (Some(sp), Some(p)) if sp < p => el[sp..].trim(),
        (Some(sp), None) => el[sp..].trim(),
        _ => el,
    }
}

/// `query` with each 128-bit column sent as text.
pub fn stringify(query: &str, wide: &[Wide]) -> String {
    if wide.is_empty() {
        return query.to_owned();
    }
    let repl: Vec<String> = wide
        .iter()
        .map(|w| {
            let c = ident(&w.name);
            format!("toString({c}) AS {c}")
        })
        .collect();
    // The newline closes a trailing `--` comment before the parenthesis.
    format!(
        "SELECT * REPLACE ({}) FROM ({}\n)",
        repl.join(", "),
        trimmed(query)
    )
}

/// The DuckDB select over `relation` casting the 128-bit columns back; `relation` as is when there
/// are none.
pub fn cast_back(relation: &str, wide: &[Wide]) -> String {
    if wide.is_empty() {
        return relation.to_owned();
    }
    let casts: Vec<String> = wide
        .iter()
        .map(|w| {
            let c = format!("\"{}\"", w.name.replace('"', "\"\""));
            format!("CAST({c} AS {}) AS {c}", w.cast)
        })
        .collect();
    format!("(SELECT * REPLACE ({}) FROM {relation})", casts.join(", "))
}

/// `query` without trailing whitespace and semicolons, to nest it.
pub fn trimmed(query: &str) -> &str {
    query.trim_end_matches(|c: char| c.is_whitespace() || c == ';')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols(cs: &[(&str, &str)]) -> Vec<(String, String)> {
        cs.iter()
            .map(|(n, t)| ((*n).to_owned(), (*t).to_owned()))
            .collect()
    }

    fn refusal(ty: &str) -> String {
        check(&cols(&[("x", ty)])).expect_err(ty).remove(0)
    }

    #[test]
    fn splits_types() {
        assert_eq!(split("UInt8"), ("UInt8", vec![]));
        assert_eq!(
            split("Map(String, Array(Tuple(a Int8, b String)))"),
            ("Map", vec!["String", "Array(Tuple(a Int8, b String))"])
        );
        assert_eq!(
            split("Enum8('a,b' = 1, 'c)\\'' = 2)"),
            ("Enum8", vec!["'a,b' = 1", "'c)\\'' = 2"])
        );
        assert_eq!(
            split("DateTime64(9, 'UTC')"),
            ("DateTime64", vec!["9", "'UTC'"])
        );
        assert_eq!(element_type("x Array(Int8)"), "Array(Int8)");
        assert_eq!(element_type("`a b` UInt8"), "UInt8");
        assert_eq!(element_type("Array(Int8)"), "Array(Int8)");
        assert_eq!(element_type("UInt8"), "UInt8");
    }

    #[test]
    fn lands_what_the_target_holds() {
        let w = check(&cols(&[
            ("id", "UInt32"),
            ("a", "Int128"),
            ("b", "LowCardinality(Nullable(UInt128))"),
            ("d", "Decimal(76, 2)"),
            ("t", "DateTime64(6, 'UTC')"),
            ("t3", "DateTime64(3)"),
            ("m", "Map(String, Array(Nullable(Int64)))"),
            ("tu", "Tuple(\n    IPv4 String,\n    `Enum8` UInt8)"),
            ("u", "UUID"),
            ("s", "SimpleAggregateFunction(sum, UInt64)"),
        ]))
        .expect("check");
        assert_eq!(
            w,
            [
                Wide {
                    name: "a".into(),
                    cast: "HUGEINT"
                },
                Wide {
                    name: "b".into(),
                    cast: "UHUGEINT"
                },
            ]
        );
    }

    #[test]
    fn refuses_what_it_would_coerce() {
        let cases = [
            ("Int256", "no 256-bit integer"),
            ("Nullable(UInt256)", "no 256-bit integer"),
            ("DateTime64(9, 'UTC')", "to the microsecond"),
            ("Nullable(DateTime64(7))", "to the microsecond"),
            ("IPv4", "no IP address type"),
            ("IPv6", "no IP address type"),
            ("Enum8('a' = 1, 'b' = 2)", "no enum type"),
            ("Enum16('a' = 1)", "no enum type"),
            ("FixedString(3)", "no fixed-length string"),
            ("LowCardinality(FixedString(2))", "no fixed-length string"),
            ("Array(Int128)", "inside an Array"),
            ("Map(String, UInt128)", "inside an Array"),
            ("Tuple(a UInt8, b Array(IPv6))", "no IP address type"),
            ("SimpleAggregateFunction(any, Int256)", "no 256-bit integer"),
            ("AggregateFunction(uniq, UInt64)", "doesn't land this type"),
            ("Point", "doesn't land this type"),
            ("Array(Ring)", "doesn't land this type"),
            ("Object('json')", "doesn't land this type"),
            ("SomeFutureType", "doesn't land this type"),
            ("Nullable(Nothing)", "an untyped `NULL`"),
            ("Array(Nothing)", "empty array"),
        ];
        for (ty, want) in cases {
            let m = refusal(ty);
            assert!(
                m.starts_with(&format!("column `x` is {ty}")) && m.contains(want),
                "{ty}: {m}"
            );
        }
        assert_eq!(
            refusal("Array(Nullable(IPv4))"),
            "column `x` is Array(Nullable(IPv4)) (its IPv4), which the target can't hold: there's no IP address type, and it would arrive as a number or raw bytes; convert it in `from.query` (`toString(x)`)"
        );
    }

    #[test]
    fn names_every_refused_column() {
        let bad = check(&cols(&[
            ("id", "UInt8"),
            ("ip", "IPv4"),
            ("big", "Int128"),
            ("e", "Enum8('a' = 1)"),
        ]))
        .expect_err("refused");
        assert_eq!(bad.len(), 2, "{bad:?}");
        assert!(bad[0].starts_with("column `ip` is IPv4"), "{}", bad[0]);
        assert!(
            bad[1].starts_with("column `e` is Enum8('a' = 1)"),
            "{}",
            bad[1]
        );
    }

    #[test]
    fn rewrites_only_wide_columns() {
        let w = check(&cols(&[
            ("id", "UInt32"),
            ("big", "Nullable(Int128)"),
            ("u`x", "UInt128"),
        ]))
        .expect("check");
        assert_eq!(
            stringify("SELECT * FROM t -- note\n;", &w),
            "SELECT * REPLACE (toString(`big`) AS `big`, toString(`u\\`x`) AS `u\\`x`) FROM (SELECT * FROM t -- note\n)"
        );
        assert_eq!(
            cast_back("read_parquet(['a'])", &w),
            "(SELECT * REPLACE (CAST(\"big\" AS HUGEINT) AS \"big\", CAST(\"u`x\" AS UHUGEINT) AS \"u`x\") FROM read_parquet(['a']))"
        );
        assert_eq!(stringify("SELECT 1", &[]), "SELECT 1");
        assert_eq!(cast_back("r", &[]), "r");
    }
}
