//! Postgres text values → lake types. Values are staged as text and cast in DuckDB SQL. Columns
//! are judged as the Postgres source judges them ([`atlatl_conn_postgres::types`]): by their base
//! type, with only the types read faithfully landing natively and the rest refused unless
//! `columns.<c>.type` declares what to make of their text.
//!
//! Changes carry a type's output text, not the `::text` the source reads, so three are brought to
//! that form: an `inet` gets its mask, `money` (rendered under `lc_monetary = 'C'`) its plain
//! digits, a `"char"` the `bytea` escape. Arrays and composites arrive as Postgres's text, not
//! JSON, so they land only as VARCHAR of it. A value cast into any lake type other than its own
//! lands exactly or fails; float targets accept precision loss.

use std::collections::HashMap;

use atlatl_conn_postgres::source::LAST_TIMESTAMP;
use atlatl_conn_postgres::types::{
    Class, Target, Text, classify, duck_literal, guarded, numeric_precision, target,
};
use atlatl_conn_postgres::{Session, wrap};
use atlatl_core::AtlatlError;
use atlatl_core::error::Result;
use atlatl_ingest::drift::{canonical, exact_cast};
use atlatl_ingest::sql::{CAST_FAILED, literal};

use crate::stage::StageCol;

/// How a refused type's staged text becomes the text the Postgres source reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Form {
    Plain,
    /// `inet` output drops a full-length mask; `::text` keeps it.
    Inet,
    /// `$1,234.50` → `1234.50`.
    Money,
    /// `charout` → `encode(…, 'escape')`: the NUL byte and a backslash differ.
    Char,
    /// Postgres's array or composite text, as is.
    Pg,
}

impl Form {
    fn of(text: Text, typname: &str) -> Self {
        match text {
            Text::Json => Form::Pg,
            Text::Numeric => Form::Money,
            Text::Char => Form::Char,
            Text::Plain if typname == "inet" => Form::Inet,
            Text::Plain => Form::Plain,
        }
    }

    fn sql(self, v: &str) -> String {
        match self {
            Form::Plain | Form::Pg => v.to_owned(),
            Form::Inet => format!(
                "CASE WHEN contains({v}, '/') THEN {v} WHEN contains({v}, ':') THEN {v} || '/128' ELSE {v} || '/32' END"
            ),
            Form::Money => format!("regexp_replace({v}, '[^0-9.-]', '', 'g')"),
            Form::Char => {
                format!("CASE {v} WHEN '' THEN '\\000' WHEN '\\' THEN '\\\\' ELSE {v} END")
            }
        }
    }
}

/// How a staged column lands.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) enum Land {
    /// As its own type ([`lake_type`]).
    #[default]
    Native,
    /// A landed type with `columns.<c>.type`: its own type, cast exactly into `kind`.
    Cast { kind: String },
    /// A refused type opted in by `columns.<c>.type`: its text, checked into `kind`.
    Declared {
        kind: String,
        target: Target,
        form: Form,
    },
}

/// The lake type of a base type that lands as itself.
pub fn lake_type(oid: u32, typmod: i32) -> Option<String> {
    Some(
        match oid {
            16 => "BOOLEAN",
            17 => "BLOB",
            19 | 25 | 1042 | 1043 => "VARCHAR",
            20 => "BIGINT",
            21 => "SMALLINT",
            23 => "INTEGER",
            114 | 3802 => "JSON",
            700 => "FLOAT",
            701 => "DOUBLE",
            1082 => "DATE",
            1083 => "TIME",
            1114 => "TIMESTAMP",
            1184 => "TIMESTAMP WITH TIME ZONE",
            1186 => "INTERVAL",
            1266 => "TIME WITH TIME ZONE",
            1700 => {
                let (p, s) = numeric_precision(typmod)?;
                return Some(format!("DECIMAL({p},{s})"));
            }
            2950 => "UUID",
            _ => return None,
        }
        .to_owned(),
    )
}

/// The type a staged column's value has before it's cast into its lake column.
fn own_type(c: &StageCol) -> String {
    match &c.land {
        Land::Declared { kind, .. } => kind.clone(),
        _ => lake_type(c.oid, c.typmod).unwrap_or_else(|| "VARCHAR".into()),
    }
}

/// The lake type a staged column lands as in a new column.
pub(crate) fn land_type(c: &StageCol) -> String {
    match &c.land {
        Land::Declared { kind, .. } | Land::Cast { kind } => kind.clone(),
        Land::Native => own_type(c),
    }
}

/// Spells a declared `columns.<c>.type` as DuckDB does, so casts compare like for like.
pub(crate) fn canonicalize(c: &duckdb::Connection, col: &mut StageCol) -> Result<()> {
    let kind = match &mut col.land {
        Land::Native => return Ok(()),
        Land::Cast { kind } | Land::Declared { kind, .. } => kind,
    };
    *kind = canonical(c, kind).map_err(|_| {
        AtlatlError::config(format!(
            "`columns.{}.type: {kind}` is not a type the lake knows",
            col.name
        ))
    })?;
    Ok(())
}

/// Postgres's ` BC` suffix as DuckDB's astronomical year (1 BC is year 0, 2 BC is -1).
fn bc(v: &str) -> String {
    let y = format!("CAST(split_part({v}, '-', 1) AS INTEGER)");
    format!(
        "CASE WHEN suffix({v}, ' BC') THEN (CASE WHEN {y} = 1 THEN '0000' ELSE '-' || lpad(CAST({y} - 1 AS VARCHAR), 4, '0') END) || substr({v}, strpos({v}, '-'), length({v}) - strpos({v}, '-') - 2) ELSE {v} END"
    )
}

/// A DuckDB expression turning the staged text `v` of `c` into `target`. `what` names the
/// column in the `cast_failed` error a value it can't land exactly raises.
pub(crate) fn cast(v: &str, c: &StageCol, target: &str, what: &str) -> String {
    let own = own_type(c);
    let fail = |why: &str| {
        format!(
            "error({} || {v} || {})",
            duck_literal(&format!("{CAST_FAILED}{what} holds ")),
            duck_literal(why)
        )
    };
    let e = match (&c.land, c.oid) {
        (Land::Declared { kind, target, form }, _) => {
            guarded(&form.sql(v), kind, *target, &format!("{CAST_FAILED}{what}"))
        }
        (_, 17) => format!("from_hex(substr({v}, 3))"),
        (_, 1082) => format!("CAST({} AS DATE)", bc(v)),
        (_, 1114 | 1184) => {
            let b = bc(v);
            let last = match c.oid {
                1184 => format!("{LAST_TIMESTAMP}+00"),
                _ => LAST_TIMESTAMP.to_owned(),
            };
            format!(
                "CASE WHEN {v} IS NULL OR TRY_CAST({b} AS {own}) IS NOT NULL THEN CAST({b} AS {own}) ELSE {} END",
                fail(&format!(
                    ", past the last timestamp the target holds ({last})"
                ))
            )
        }
        (_, 1700) => format!(
            "CASE WHEN {v} = 'NaN' THEN {} ELSE CAST({v} AS {own}) END",
            fail(", which a decimal can't hold")
        ),
        (_, 1186) => format!("CAST(replace({v}, '+', '') AS INTERVAL)"),
        _ if own == "VARCHAR" => v.to_owned(),
        _ => format!("CAST({v} AS {own})"),
    };
    exact(&e, &own, target, c, what)
}

/// `e` (of type `from`) as `to`, failing with `cast_failed` rather than landing inexactly, by
/// the rule ingest applies to declared types ([`exact_cast`]).
fn exact(e: &str, from: &str, to: &str, c: &StageCol, what: &str) -> String {
    if from.eq_ignore_ascii_case(to) {
        return e.to_owned();
    }
    let hint = match &c.land {
        Land::Cast { kind } if kind.eq_ignore_ascii_case(to) => {
            format!("declare a wider `columns.{}.type`", c.name)
        }
        _ => "widen the lake column".to_owned(),
    };
    exact_cast(e, from, to, "")
        .replace(
            &literal(&format!(
                "{CAST_FAILED}a value doesn't fit its declared type exactly; it'd fail to cast, round or truncate (`` \""
            )),
            &literal(&format!("{CAST_FAILED}{what} holds ")),
        )
        .replace(
            &literal(&format!(
                "\" as {to}). Declare a wider type or convert in `from.query`"
            )),
            &literal(&format!(
                ", which {to} can't hold exactly (it'd fail to cast, round or truncate); {hint}"
            )),
        )
}

/// A column type as Postgres names it, and the base type it's judged by.
#[derive(Clone, Debug, PartialEq)]
struct PgType {
    name: String,
    /// The base type's `format_type`, for a domain.
    base: Option<String>,
    typname: String,
    typtype: String,
    typcategory: String,
    /// The base type and its typmod.
    oid: u32,
    typmod: i32,
}

/// A built-in type gone from the catalog since the change was written, judged by OID alone.
fn builtin(oid: u32, typmod: i32) -> PgType {
    let typname = match oid {
        16 => "bool",
        17 => "bytea",
        18 => "char",
        19 => "name",
        20 => "int8",
        21 => "int2",
        23 => "int4",
        25 => "text",
        26 => "oid",
        114 => "json",
        700 => "float4",
        701 => "float8",
        790 => "money",
        1042 => "bpchar",
        1043 => "varchar",
        1082 => "date",
        1083 => "time",
        1114 => "timestamp",
        1184 => "timestamptz",
        1186 => "interval",
        1266 => "timetz",
        1700 => "numeric",
        2950 => "uuid",
        3802 => "jsonb",
        _ => "",
    };
    PgType {
        name: format!("type {oid}"),
        base: None,
        typname: typname.into(),
        typtype: if typname.is_empty() { "" } else { "b" }.into(),
        typcategory: String::new(),
        oid,
        typmod,
    }
}

/// Column types by OID and typmod, a domain resolved to the type it's over.
#[derive(Default)]
pub(crate) struct Catalog(HashMap<(u32, i32), PgType>);

const TYPES_SQL: &str = "WITH RECURSIVE b(o, m, oid, typmod) AS ( \
       SELECT u.o, u.m, u.o, u.m FROM unnest($1::oid[], $2::int4[]) AS u(o, m) \
       UNION ALL \
       SELECT b.o, b.m, t.typbasetype, t.typtypmod FROM b \
       JOIN pg_catalog.pg_type t ON t.oid = b.oid WHERE t.typtype = 'd') \
     SELECT b.o::int8, b.m, format_type(b.o, b.m), b.oid <> b.o, t.typname::text, \
       format_type(b.oid, b.typmod), t.typtype::text, t.typcategory::text, b.oid::int8, b.typmod \
     FROM b JOIN pg_catalog.pg_type t ON t.oid = b.oid WHERE t.typtype <> 'd'";

impl Catalog {
    pub async fn resolve(&mut self, sql: &Session, cols: &[(u32, i32)]) -> Result<()> {
        let mut want: Vec<(u32, i32)> = cols
            .iter()
            .copied()
            .filter(|k| !self.0.contains_key(k))
            .collect();
        want.sort_unstable();
        want.dedup();
        if want.is_empty() {
            return Ok(());
        }
        let (oids, mods): (Vec<u32>, Vec<i32>) = want.iter().copied().unzip();
        let rows = sql
            .client()
            .query(TYPES_SQL, &[&oids, &mods])
            .await
            .map_err(wrap)?;
        for r in rows {
            let domain: bool = r.get(3);
            self.0.insert(
                (r.get::<_, i64>(0) as u32, r.get(1)),
                PgType {
                    name: r.get(2),
                    base: domain.then(|| r.get(5)),
                    typname: r.get(4),
                    typtype: r.get(6),
                    typcategory: r.get(7),
                    oid: r.get::<_, i64>(8) as u32,
                    typmod: r.get(9),
                },
            );
        }
        Ok(())
    }

    /// How column `name` lands, given its declared `columns.<c>.type`, with the base type and
    /// typmod it's read as; a refusal is the line naming it.
    pub fn land(
        &self,
        name: &str,
        oid: u32,
        typmod: i32,
        declared: Option<&str>,
    ) -> std::result::Result<(u32, i32, Land), String> {
        let t = self
            .0
            .get(&(oid, typmod))
            .cloned()
            .unwrap_or_else(|| builtin(oid, typmod));
        let r = match classify(&t.typtype, &t.typcategory, &t.typname, t.typmod) {
            Class::Native | Class::Decimal if lake_type(t.oid, t.typmod).is_some() => {
                let land = declared.map_or(Land::Native, |k| Land::Cast { kind: k.to_owned() });
                return Ok((t.oid, t.typmod, land));
            }
            Class::Refused(r) => r,
            _ => {
                let Class::Refused(r) = classify("", "", "", -1) else {
                    unreachable!("an unknown type is refused")
                };
                r
            }
        };
        let of = t
            .base
            .as_ref()
            .map(|b| format!(" (its {b})"))
            .unwrap_or_default();
        let form = Form::of(r.text, &t.typname);
        let Some(kind) = declared else {
            let declare = match form {
                Form::Pg => "VARCHAR, for its Postgres text",
                _ => r.declare,
            };
            return Err(format!(
                "column `{name}` is {}{of}, which the target can't hold: {}; declare `columns.{name}.type` ({declare}), or leave it out with `columns.exclude`",
                t.name, r.why
            ));
        };
        match target(kind) {
            Some(tg) if form != Form::Pg || tg == Target::Text => Ok((
                t.oid,
                t.typmod,
                Land::Declared {
                    kind: kind.to_owned(),
                    target: tg,
                    form,
                },
            )),
            _ if form == Form::Pg => Err(format!(
                "column `{name}` is {}{of}, and `columns.{name}.type: {kind}` can't be checked against exactly from its text; declare VARCHAR (changes carry its Postgres text, not JSON)",
                t.name
            )),
            _ => Err(format!(
                "column `{name}` is {}{of}, and `columns.{name}.type: {kind}` can't be checked against exactly from its text; declare VARCHAR, JSON, DECIMAL(p,s), an integer or a float type (e.g. {})",
                t.name, r.declare
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nm(p: i32, s: i32) -> i32 {
        ((p << 16) | s) + 4
    }

    fn col(oid: u32, typmod: i32) -> StageCol {
        StageCol {
            name: "c".into(),
            oid,
            typmod,
            land: Land::Native,
        }
    }

    fn declared(kind: &str, target: Target, form: Form) -> StageCol {
        StageCol {
            land: Land::Declared {
                kind: kind.into(),
                target,
                form,
            },
            ..col(0, -1)
        }
    }

    #[test]
    fn mapping() {
        assert_eq!(lake_type(1700, nm(12, 3)).as_deref(), Some("DECIMAL(12,3)"));
        assert_eq!(lake_type(1700, -1), None);
        assert_eq!(lake_type(1007, -1), None, "arrays are refused");
        assert_eq!(lake_type(26, -1), None, "oid is refused");
        assert_eq!(lake_type(1043, 36).as_deref(), Some("VARCHAR"));
        assert_eq!(cast("x", &col(25, -1), "VARCHAR", "c"), "x");
        assert_eq!(
            cast("x", &col(23, -1), "INTEGER", "c"),
            "CAST(x AS INTEGER)"
        );
        assert_eq!(
            cast("x", &col(17, -1), "BLOB", "c"),
            "from_hex(substr(x, 3))"
        );
        let d = declared("DECIMAL(38,2)", Target::Decimal(38, 2), Form::Plain);
        assert_eq!(land_type(&d), "DECIMAL(38,2)");
        let c = StageCol {
            land: Land::Cast {
                kind: "BIGINT".into(),
            },
            ..col(23, -1)
        };
        assert_eq!(land_type(&c), "BIGINT");
        assert_eq!(own_type(&c), "INTEGER");
    }

    fn catalog(rows: &[(u32, &str, &str, &str, Option<&str>)]) -> Catalog {
        let mut c = Catalog::default();
        for (oid, typtype, cat, typname, base) in rows {
            c.0.insert(
                (*oid, -1),
                PgType {
                    name: if base.is_some() {
                        format!("dom_{typname}")
                    } else {
                        (*typname).into()
                    },
                    base: base.map(str::to_owned),
                    typname: (*typname).into(),
                    typtype: (*typtype).into(),
                    typcategory: (*cat).into(),
                    oid: match (base, *typname) {
                        (Some(_), "int4") => 23,
                        (Some(_), _) => 1700,
                        _ => *oid,
                    },
                    typmod: -1,
                },
            );
        }
        c
    }

    #[test]
    fn refuses_what_the_postgres_source_refuses() {
        let c = catalog(&[
            (1700, "b", "N", "numeric", None),
            (790, "b", "N", "money", None),
            (18, "b", "Z", "char", None),
            (26, "b", "N", "oid", None),
            (869, "b", "I", "inet", None),
            (650, "b", "I", "cidr", None),
            (829, "b", "U", "macaddr", None),
            (1560, "b", "V", "bit", None),
            (142, "b", "U", "xml", None),
            (3614, "b", "U", "tsvector", None),
            (3220, "b", "U", "pg_lsn", None),
            (2205, "b", "N", "regclass", None),
            (600, "b", "G", "point", None),
            (3904, "r", "R", "int4range", None),
            (4451, "m", "R", "int4multirange", None),
            (7000, "e", "E", "mood", None),
            (7001, "c", "C", "pair", None),
            (7002, "b", "S", "citext", None),
            (1007, "b", "A", "_int4", None),
            (791, "b", "A", "_money", None),
            (16, "b", "B", "bool", None),
            (7003, "b", "N", "int4", Some("integer")),
            (7004, "b", "N", "numeric", Some("numeric")),
        ]);
        let land = |oid: u32, decl: Option<&str>| c.land("n", oid, -1, decl);
        let why = |oid: u32| land(oid, None).expect_err("refused");
        for oid in [
            1700, 790, 18, 26, 869, 650, 829, 1560, 142, 3614, 3220, 2205, 600, 3904, 4451, 7000,
            7001, 7002, 1007, 791, 7004,
        ] {
            assert!(
                why(oid).ends_with("or leave it out with `columns.exclude`"),
                "{oid}: {}",
                why(oid)
            );
        }
        assert_eq!(
            why(1700),
            "column `n` is numeric, which the target can't hold: decimals hold at most 38 digits at a fixed scale, and it would arrive as a float; declare `columns.n.type` (DECIMAL(38,s), or VARCHAR), or leave it out with `columns.exclude`"
        );
        assert_eq!(
            why(7004),
            "column `n` is dom_numeric (its numeric), which the target can't hold: decimals hold at most 38 digits at a fixed scale, and it would arrive as a float; declare `columns.n.type` (DECIMAL(38,s), or VARCHAR), or leave it out with `columns.exclude`"
        );
        assert_eq!(
            why(1007),
            "column `n` is _int4, which the target can't hold: a Postgres array carries its own bounds and dimensions, which a list can't hold; declare `columns.n.type` (VARCHAR, for its Postgres text), or leave it out with `columns.exclude`"
        );
        assert!(why(791).contains("a Postgres array"), "money[] as an array");
        assert!(why(26).contains("(BIGINT)"));
        assert!(why(790).contains("(DECIMAL(19,2))"));
        assert!(why(7000).contains("there's no enum type"));
        assert!(why(7002).contains("doesn't land this type"));
        assert!(why(600).contains("there's no geometric type"));
        assert!(
            c.land("n", 99999, -1, None)
                .expect_err("unknown")
                .contains("doesn't land this type")
        );
        assert!(
            c.land("n", 18, 5, None)
                .expect_err("judged by OID")
                .contains("single byte")
        );

        assert_eq!(land(16, None), Ok((16, -1, Land::Native)));
        assert_eq!(
            land(7003, None),
            Ok((23, -1, Land::Native)),
            "a domain lands as its base type"
        );
        assert_eq!(
            land(16, Some("VARCHAR")),
            Ok((
                16,
                -1,
                Land::Cast {
                    kind: "VARCHAR".into()
                }
            ))
        );
        let decl = |oid: u32, kind: &str| match land(oid, Some(kind)) {
            Ok((_, _, Land::Declared { target, form, .. })) => Ok((target, form)),
            Ok(l) => panic!("{l:?}"),
            Err(e) => Err(e),
        };
        assert_eq!(
            decl(1700, "decimal(38, 10)"),
            Ok((Target::Decimal(38, 10), Form::Plain))
        );
        assert_eq!(
            decl(790, "DECIMAL(19,2)"),
            Ok((Target::Decimal(19, 2), Form::Money))
        );
        assert_eq!(decl(869, "VARCHAR"), Ok((Target::Text, Form::Inet)));
        assert_eq!(decl(650, "VARCHAR"), Ok((Target::Text, Form::Plain)));
        assert_eq!(decl(18, "VARCHAR"), Ok((Target::Text, Form::Char)));
        assert_eq!(decl(26, "BIGINT"), Ok((Target::Integer, Form::Plain)));
        assert_eq!(decl(1007, "VARCHAR"), Ok((Target::Text, Form::Pg)));
        assert_eq!(decl(7001, "VARCHAR"), Ok((Target::Text, Form::Pg)));
        assert_eq!(
            decl(1007, "JSON"),
            Err("column `n` is _int4, and `columns.n.type: JSON` can't be checked against exactly from its text; declare VARCHAR (changes carry its Postgres text, not JSON)".into())
        );
        assert_eq!(
            decl(1700, "TIMESTAMP"),
            Err("column `n` is numeric, and `columns.n.type: TIMESTAMP` can't be checked against exactly from its text; declare VARCHAR, JSON, DECIMAL(p,s), an integer or a float type (e.g. DECIMAL(38,s), or VARCHAR)".into())
        );
    }

    fn duck() -> duckdb::Connection {
        let c = duckdb::Connection::open_in_memory().expect("db");
        c.execute_batch("SET TimeZone = 'UTC'").expect("tz");
        c
    }

    fn run(
        c: &duckdb::Connection,
        text: &str,
        col: &StageCol,
        target: &str,
    ) -> std::result::Result<String, atlatl_core::AtlatlError> {
        let sql = format!(
            "SELECT ({})::VARCHAR FROM (SELECT ?::VARCHAR AS v)",
            cast("v", col, target, "column `c` of `public.t`")
        );
        c.query_row(&sql, [text], |r| r.get(0))
            .map_err(atlatl_ingest::sql::db)
    }

    #[test]
    fn casts_run_in_duckdb() {
        let c = duck();
        let cases: &[(&str, u32, i32, &str)] = &[
            ("t", 16, -1, "true"),
            ("\\x00ff", 17, -1, "\\x00\\xFF"),
            ("9007199254740993", 20, -1, "9007199254740993"),
            ("12.345", 1700, nm(12, 3), "12.345"),
            (
                "2024-03-10 12:00:00.123456+00",
                1184,
                -1,
                "2024-03-10 12:00:00.123456+00",
            ),
            (
                "294247-01-10 04:00:54.775806",
                1114,
                -1,
                "294247-01-10 04:00:54.775806",
            ),
            ("infinity", 1114, -1, "infinity"),
            ("2024-01-31", 1082, -1, "2024-01-31"),
            ("0044-03-15 BC", 1082, -1, "0044-03-15 (BC)"),
            ("0001-12-31 BC", 1082, -1, "0001-12-31 (BC)"),
            ("4714-11-24 BC", 1082, -1, "4714-11-24 (BC)"),
            (
                "0044-03-15 12:00:00.5 BC",
                1114,
                -1,
                "0044-03-15 (BC) 12:00:00.5",
            ),
            (
                "0001-01-01 00:00:00+00 BC",
                1184,
                -1,
                "0001-01-01 (BC) 00:00:00+00",
            ),
            (
                "-1 years -2 mons +3 days -04:05:06",
                1186,
                -1,
                "-1 year -2 months 3 days -04:05:06",
            ),
            ("{\"k\": [1, 2]}", 3802, -1, "{\"k\": [1, 2]}"),
            (
                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
                2950,
                -1,
                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            ),
            ("1.5", 701, -1, "1.5"),
            ("NaN", 701, -1, "nan"),
        ];
        for (text, oid, typmod, want) in cases {
            let col = col(*oid, *typmod);
            let got = run(&c, text, &col, &land_type(&col))
                .unwrap_or_else(|e| panic!("{text} ({oid}): {e}"));
            assert_eq!(&got, want, "{text} ({oid})");
        }
        let bc_year: i64 = c
            .query_row(
                &format!(
                    "SELECT year({}) FROM (SELECT '0044-03-15 BC' AS v)",
                    cast("v", &col(1082, -1), "DATE", "c")
                ),
                [],
                |r| r.get(0),
            )
            .expect("year");
        assert_eq!(bc_year, -43, "44 BC is astronomical year -43");
        let last: i64 = c
            .query_row(
                &format!(
                    "SELECT epoch_us({}) FROM (SELECT '294247-01-10 04:00:54.775806+00' AS v)",
                    cast("v", &col(1184, -1), "TIMESTAMP WITH TIME ZONE", "c")
                ),
                [],
                |r| r.get(0),
            )
            .expect("last");
        assert_eq!(last, i64::MAX - 1);
    }

    #[test]
    fn declared_text_matches_the_postgres_source() {
        let c = duck();
        let text = |form: Form| declared("VARCHAR", Target::Text, form);
        for (v, form, want) in [
            ("10.0.0.1", Form::Inet, "10.0.0.1/32"),
            ("10.0.0.0/8", Form::Inet, "10.0.0.0/8"),
            ("::1", Form::Inet, "::1/128"),
            ("fe80::1/64", Form::Inet, "fe80::1/64"),
            ("-$1,234.50", Form::Money, "-1234.50"),
            ("$0.00", Form::Money, "0.00"),
            ("", Form::Char, "\\000"),
            ("\\", Form::Char, "\\\\"),
            ("\\351", Form::Char, "\\351"),
            ("a", Form::Char, "a"),
            ("{1,NULL,\"a b\"}", Form::Pg, "{1,NULL,\"a b\"}"),
            ("(1,\"x y\",)", Form::Pg, "(1,\"x y\",)"),
        ] {
            assert_eq!(run(&c, v, &text(form), "VARCHAR").expect(v), want, "{v}");
        }
        let money = declared("DECIMAL(19,2)", Target::Decimal(19, 2), Form::Money);
        assert_eq!(
            run(&c, "$92,233,720,368,547,758.07", &money, "DECIMAL(19,2)").expect("max"),
            "92233720368547758.07"
        );
        let n = declared("DECIMAL(38,10)", Target::Decimal(38, 10), Form::Plain);
        assert_eq!(
            run(
                &c,
                "123456789012345678901234567.0123456789",
                &n,
                "DECIMAL(38,10)"
            )
            .expect("fits"),
            "123456789012345678901234567.0123456789"
        );
        let e = run(&c, "0.00000000001", &n, "DECIMAL(38,10)").expect_err("too fine");
        assert_eq!(
            (e.code.as_deref(), e.message.as_str()),
            (
                Some("cast_failed"),
                "column `c` of `public.t` holds 0.00000000001, which DECIMAL(38,10) can't hold exactly; declare a wider `DECIMAL(p,s)`"
            )
        );
    }

    #[test]
    fn unlandable_values_fail() {
        let c = duck();
        let fails = [
            (
                "294247-01-10 04:00:54.775807",
                col(1114, -1),
                "column `c` of `public.t` holds 294247-01-10 04:00:54.775807, past the last timestamp the target holds (294247-01-10 04:00:54.775806)",
            ),
            (
                "294276-12-31 23:59:59.999999+00",
                col(1184, -1),
                "column `c` of `public.t` holds 294276-12-31 23:59:59.999999+00, past the last timestamp the target holds (294247-01-10 04:00:54.775806+00)",
            ),
            (
                "NaN",
                col(1700, nm(10, 2)),
                "column `c` of `public.t` holds NaN, which a decimal can't hold",
            ),
        ];
        for (text, col, want) in fails {
            let e = run(&c, text, &col, &land_type(&col)).expect_err(text);
            assert_eq!(
                (e.class, e.code.as_deref(), e.message.as_str()),
                (atlatl_core::ErrorClass::Data, Some("cast_failed"), want),
                "{text}"
            );
        }
    }

    #[test]
    fn casts_into_another_lake_type_are_exact() {
        let c = duck();
        let ok: &[(&str, StageCol, &str, &str)] = &[
            ("12.5", col(1700, nm(10, 2)), "DECIMAL(20,4)", "12.5000"),
            ("7", col(23, -1), "BIGINT", "7"),
            ("12.50", col(1700, nm(20, 4)), "DECIMAL(10,2)", "12.50"),
            ("2147483647", col(20, -1), "INTEGER", "2147483647"),
            ("2024-01-02 00:00:00", col(1114, -1), "DATE", "2024-01-02"),
            ("0.1", col(701, -1), "FLOAT", "0.1"),
            ("16777217", col(20, -1), "FLOAT", "16777216.0"),
            ("42", col(23, -1), "VARCHAR", "42"),
        ];
        for (v, col, to, want) in ok {
            assert_eq!(run(&c, v, col, to).expect(v), *want, "{v} as {to}");
        }
        let fails: &[(&str, StageCol, &str, &str)] = &[
            (
                "12.3456",
                col(1700, nm(20, 4)),
                "DECIMAL(10,2)",
                "column `c` of `public.t` holds 12.3456, which DECIMAL(10,2) can't hold exactly (it'd fail to cast, round or truncate); widen the lake column",
            ),
            (
                "2147483648",
                col(20, -1),
                "INTEGER",
                "column `c` of `public.t` holds 2147483648, which INTEGER can't hold exactly (it'd fail to cast, round or truncate); widen the lake column",
            ),
            (
                "2024-01-02 03:04:05",
                col(1114, -1),
                "DATE",
                "column `c` of `public.t` holds 2024-01-02 03:04:05, which DATE can't hold exactly (it'd fail to cast, round or truncate); widen the lake column",
            ),
            (
                "1e300",
                col(701, -1),
                "FLOAT",
                "column `c` of `public.t` holds 1e+300, which FLOAT can't hold exactly (it'd fail to cast, round or truncate); widen the lake column",
            ),
            (
                "70000",
                StageCol {
                    land: Land::Cast {
                        kind: "SMALLINT".into(),
                    },
                    ..col(23, -1)
                },
                "SMALLINT",
                "column `c` of `public.t` holds 70000, which SMALLINT can't hold exactly (it'd fail to cast, round or truncate); declare a wider `columns.c.type`",
            ),
        ];
        for (v, col, to, want) in fails {
            let e = run(&c, v, col, to).expect_err(v);
            assert_eq!(
                (e.class, e.code.as_deref(), e.message.as_str()),
                (atlatl_core::ErrorClass::Data, Some("cast_failed"), *want),
                "{v} as {to}"
            );
        }
    }

    #[test]
    fn declared_kinds_are_canonical() {
        let c = duck();
        let mut d = declared("numeric(10, 2)", Target::Decimal(10, 2), Form::Plain);
        canonicalize(&c, &mut d).expect("canonical");
        assert_eq!(land_type(&d), "DECIMAL(10,2)");
        let mut bad = StageCol {
            land: Land::Cast {
                kind: "NOPE".into(),
            },
            ..col(23, -1)
        };
        let e = canonicalize(&c, &mut bad).expect_err("unknown type");
        assert_eq!(
            (e.class, e.message.as_str()),
            (
                atlatl_core::ErrorClass::Config,
                "`columns.c.type: NOPE` is not a type the lake knows"
            )
        );
    }
}
