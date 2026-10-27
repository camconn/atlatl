//! Source metadata: `svv_columns` precision lookup, Redshift → DuckDB types, and sorted
//! wildcard discovery over `svv_tables` (06 §4, 04 §4).

use atlatl_core::error::{AtlatlError, Result};

use crate::result::ResultSet;

/// A Redshift string literal: `'` doubled and, since Redshift treats `\` as an escape in string
/// literals, `\` doubled too.
pub fn literal(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

pub fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// `schema.table` → (schema, table); a bare name is in `public`. A `database.` prefix is dropped.
pub fn split(name: &str) -> (String, String) {
    let o = Object::parse(name);
    (o.schema, o.table)
}

/// `[database.]schema.table`. With a database the object is read cross-database (a datashare
/// consumer database, say) from the connected one, and the catalog comes from `svv_all_*`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Object {
    pub database: Option<String>,
    pub schema: String,
    pub table: String,
}

impl Object {
    pub fn parse(name: &str) -> Self {
        let parts: Vec<&str> = name.splitn(3, '.').collect();
        let own = |s: &str| s.to_owned();
        match parts.as_slice() {
            [d, s, t] => Self {
                database: Some(own(d)),
                schema: own(s),
                table: own(t),
            },
            [s, t] => Self {
                database: None,
                schema: own(s),
                table: own(t),
            },
            _ => Self {
                database: None,
                schema: "public".into(),
                table: own(name),
            },
        }
    }

    /// `"db"."s"."t"` or `"s"."t"`.
    pub fn sql(&self) -> String {
        match &self.database {
            Some(d) => format!(
                "{}.{}.{}",
                ident(d),
                ident(&self.schema),
                ident(&self.table)
            ),
            None => format!("{}.{}", ident(&self.schema), ident(&self.table)),
        }
    }
}

impl std::fmt::Display for Object {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(d) = &self.database {
            write!(f, "{d}.")?;
        }
        write!(f, "{}.{}", self.schema, self.table)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceColumn {
    pub name: String,
    /// As `svv_columns.data_type` reports it, e.g. `numeric`, `timestamp with time zone`.
    pub data_type: String,
    pub char_len: Option<i64>,
    pub precision: Option<i64>,
    pub scale: Option<i64>,
    /// The DuckDB type the read-back casts to, with the source precision and scale.
    pub duck_type: String,
}

impl SourceColumn {
    pub fn is_timestamptz(&self) -> bool {
        self.duck_type == "TIMESTAMPTZ"
    }
}

pub fn columns_sql(o: &Object) -> String {
    const COLS: &str =
        "column_name, data_type, character_maximum_length, numeric_precision, numeric_scale";
    match &o.database {
        None => format!(
            "SELECT {COLS} FROM svv_columns WHERE table_schema = {} AND table_name = {} ORDER BY ordinal_position",
            literal(&o.schema),
            literal(&o.table)
        ),
        Some(d) => format!(
            "SELECT {COLS} FROM svv_all_columns WHERE database_name = {} AND schema_name = {} AND table_name = {} ORDER BY ordinal_position",
            literal(d),
            literal(&o.schema),
            literal(&o.table)
        ),
    }
}

/// `numeric(18,4)` → (18, 4), for catalogs that report precision only in the type name.
fn type_precision(data_type: &str) -> (Option<i64>, Option<i64>) {
    if !(data_type.starts_with("numeric") || data_type.starts_with("decimal")) {
        return (None, None);
    }
    let Some(args) = data_type
        .split_once('(')
        .and_then(|(_, r)| r.split_once(')'))
        .map(|(a, _)| a)
    else {
        return (None, None);
    };
    let mut it = args.split(',').map(|n| n.trim().parse::<i64>().ok());
    (it.next().flatten(), it.next().flatten())
}

pub fn parse_columns(rs: &ResultSet, object: &str) -> Result<Vec<SourceColumn>> {
    if rs.is_empty() {
        return Err(AtlatlError::config(format!(
            "`{object}` has no columns in svv_columns (missing, or not visible to this user)"
        )));
    }
    (0..rs.len())
        .map(|i| {
            let name = rs
                .text(i, "column_name")
                .ok_or_else(|| AtlatlError::internal("svv_columns row without column_name"))?;
            let data_type = rs
                .text(i, "data_type")
                .unwrap_or_default()
                .to_ascii_lowercase();
            let (tp, ts) = type_precision(&data_type);
            let char_len = rs.int(i, "character_maximum_length");
            let precision = rs.int(i, "numeric_precision").or(tp);
            let scale = rs.int(i, "numeric_scale").or(ts);
            let duck_type = duck_type(&data_type, precision, scale)
                .map_err(|m| AtlatlError::config(format!("{object}.{name}: {m}")))?;
            Ok(SourceColumn {
                name,
                data_type,
                char_len,
                precision,
                scale,
                duck_type,
            })
        })
        .collect()
}

/// Redshift `svv_columns.data_type` → DuckDB. Numerics keep their precision and scale (Redshift's
/// default is `numeric(18,0)`).
pub fn duck_type(
    data_type: &str,
    precision: Option<i64>,
    scale: Option<i64>,
) -> Result<String, String> {
    let t = data_type.trim().to_ascii_lowercase();
    let base = t.split('(').next().unwrap_or(&t).trim();
    Ok(match base {
        "smallint" | "int2" => "SMALLINT".into(),
        "integer" | "int" | "int4" => "INTEGER".into(),
        "bigint" | "int8" => "BIGINT".into(),
        "numeric" | "decimal" => {
            let p = precision.filter(|p| *p > 0).unwrap_or(18);
            let s = scale.unwrap_or(0);
            if !(1..=38).contains(&p) || s < 0 || s > p {
                return Err(format!("unsupported numeric({p},{s})"));
            }
            format!("DECIMAL({p},{s})")
        }
        "real" | "float4" => "FLOAT".into(),
        "double precision" | "float8" | "float" => "DOUBLE".into(),
        "boolean" | "bool" => "BOOLEAN".into(),
        "character varying" | "varchar" | "character" | "char" | "bpchar" | "nchar"
        | "nvarchar" | "text" | "name" => "VARCHAR".into(),
        "date" => "DATE".into(),
        "timestamp without time zone" | "timestamp" => "TIMESTAMP".into(),
        "timestamp with time zone" | "timestamptz" => "TIMESTAMPTZ".into(),
        "time without time zone" | "time" => "TIME".into(),
        "time with time zone" | "timetz" => "TIMETZ".into(),
        "super" => "VARCHAR".into(),
        "binary varying" | "varbyte" | "varbinary" | "geometry" | "geography" | "hllsketch" => {
            return Err(format!(
                "`{data_type}` can't be unloaded to Parquet; exclude the column"
            ));
        }
        _ => return Err(format!("unsupported Redshift type `{data_type}`")),
    })
}

/// Base tables (and views if asked) of the pattern's schema, or of every schema when the
/// schema part is itself a glob. A cross-database pattern names its database literally.
pub fn tables_sql(pattern: &str, include_views: bool) -> Result<String> {
    let o = Object::parse(pattern);
    // `svv_tables` says `BASE TABLE`; `svv_all_tables` says `TABLE`.
    let types = match (include_views, o.database.is_some()) {
        (false, false) => "('BASE TABLE')",
        (true, false) => "('BASE TABLE', 'VIEW')",
        (false, true) => "('TABLE', 'BASE TABLE')",
        (true, true) => "('TABLE', 'BASE TABLE', 'VIEW')",
    };
    let mut sql = match &o.database {
        None => {
            format!("SELECT table_schema, table_name FROM svv_tables WHERE table_type IN {types}")
        }
        Some(d) if is_glob(d) => {
            return Err(AtlatlError::config(format!(
                "`{pattern}`: the database part of a pattern can't be a glob"
            )));
        }
        Some(d) => format!(
            "SELECT schema_name AS table_schema, table_name FROM svv_all_tables WHERE database_name = {} AND table_type IN {types}",
            literal(d)
        ),
    };
    if !is_glob(&o.schema) {
        let col = if o.database.is_some() {
            "schema_name"
        } else {
            "table_schema"
        };
        sql.push_str(&format!(" AND {col} = {}", literal(&o.schema)));
    }
    sql.push_str(" ORDER BY 1, 2");
    Ok(sql)
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

/// Matching `schema.table` names, sorted byte-wise (independent of the server collation).
/// Names containing `.` are skipped; `exclude` takes names or globs.
pub fn match_tables(rs: &ResultSet, pattern: &str, exclude: &[String]) -> Vec<String> {
    let p = Object::parse(pattern);
    let db = p.database.map(|d| format!("{d}.")).unwrap_or_default();
    let mut out: Vec<String> = (0..rs.len())
        .filter_map(|i| Some((rs.text(i, "table_schema")?, rs.text(i, "table_name")?)))
        .filter(|(s, t)| !s.contains('.') && !t.contains('.'))
        .filter(|(s, t)| glob_match(s, &p.schema) && glob_match(t, &p.table))
        .map(|(s, t)| format!("{db}{s}.{t}"))
        .filter(|n| !exclude.iter().any(|e| n == e || glob_match(n, e)))
        .collect();
    out.sort();
    out.dedup();
    out
}

/// DuckDB type (as a stored watermark records it) → Redshift cast target.
pub fn redshift_type(duck: &str) -> String {
    let t = duck.trim().to_ascii_uppercase();
    match t.as_str() {
        "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => "timestamptz".into(),
        "TIMESTAMP" | "TIMESTAMP_US" | "DATETIME" => "timestamp".into(),
        "DATE" => "date".into(),
        "TIME" => "time".into(),
        "BIGINT" | "INT8" | "HUGEINT" | "UBIGINT" => "bigint".into(),
        "INTEGER" | "INT" | "INT4" | "UINTEGER" => "integer".into(),
        "SMALLINT" | "INT2" | "TINYINT" | "USMALLINT" | "UTINYINT" => "smallint".into(),
        "DOUBLE" | "FLOAT8" => "double precision".into(),
        "FLOAT" | "REAL" | "FLOAT4" => "real".into(),
        "BOOLEAN" | "BOOL" => "boolean".into(),
        _ if t.starts_with("DECIMAL") || t.starts_with("NUMERIC") => {
            format!("numeric{}", t.find('(').map(|i| &t[i..]).unwrap_or(""))
        }
        _ => "varchar".into(),
    }
}

/// The incremental bound in Redshift SQL: `atlatl_ingest::modes::update_key_filter`'s semantics,
/// with the stored DuckDB type cast as its Redshift equivalent.
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
            Some(t) => format!("CAST({} AS {})", literal(v), redshift_type(t)),
            None => literal(v),
        };
        let lit = match lookback {
            Some(l) => format!("({lit} - INTERVAL {})", literal(l)),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result::Field;

    fn rs(cols: &[&str], rows: &[&[Option<&str>]]) -> ResultSet {
        ResultSet {
            columns: cols.iter().map(|c| c.to_string()).collect(),
            rows: rows
                .iter()
                .map(|r| {
                    r.iter()
                        .map(|v| match v {
                            Some(s) => Field::String(s.to_string()),
                            None => Field::Null(true),
                        })
                        .collect()
                })
                .collect(),
        }
    }

    #[test]
    fn update_key_bounds() {
        assert_eq!(
            update_key_filter(
                "publication_date",
                Some("2025-05-01"),
                Some("DATE"),
                None,
                false,
                false
            )
            .as_deref(),
            Some(r#""publication_date" > CAST('2025-05-01' AS date)"#)
        );
        assert_eq!(
            update_key_filter(
                "ts",
                Some("2024-01-31 07:34:56+00"),
                Some("TIMESTAMP WITH TIME ZONE"),
                Some("3600 seconds"),
                true,
                true
            )
            .as_deref(),
            Some(
                r#"("ts" >= (CAST('2024-01-31 07:34:56+00' AS timestamptz) - INTERVAL '3600 seconds') OR "ts" IS NULL)"#
            )
        );
        assert_eq!(
            update_key_filter("n", None, None, None, true, false).as_deref(),
            Some(r#""n" IS NOT NULL"#)
        );
        assert_eq!(update_key_filter("n", None, None, None, true, true), None);
        assert_eq!(redshift_type("DECIMAL(18,4)"), "numeric(18,4)");
        assert_eq!(redshift_type("BIGINT"), "bigint");
        assert_eq!(redshift_type("VARCHAR"), "varchar");
    }

    #[test]
    fn literals() {
        assert_eq!(literal(r"it's a\b"), r"'it''s a\\b'");
        assert_eq!(ident(r#"a"b"#), r#""a""b""#);
        assert_eq!(split("est_v2.t"), ("est_v2".into(), "t".into()));
        assert_eq!(split("t").0, "public");
    }

    #[test]
    fn types() {
        let t = |d, p, s| duck_type(d, p, s);
        assert_eq!(
            t("numeric", Some(18), Some(4)).as_deref(),
            Ok("DECIMAL(18,4)")
        );
        assert_eq!(
            t("numeric", Some(38), Some(10)).as_deref(),
            Ok("DECIMAL(38,10)")
        );
        assert_eq!(t("numeric", None, None).as_deref(), Ok("DECIMAL(18,0)"));
        assert_eq!(
            t("timestamp with time zone", None, None).as_deref(),
            Ok("TIMESTAMPTZ")
        );
        assert_eq!(t("character varying", None, None).as_deref(), Ok("VARCHAR"));
        assert_eq!(
            t("double precision", Some(53), None).as_deref(),
            Ok("DOUBLE")
        );
        assert_eq!(t("bigint", Some(64), Some(0)).as_deref(), Ok("BIGINT"));
        assert!(t("binary varying", None, None).is_err());
        assert!(t("numeric", Some(40), Some(2)).is_err());
        assert!(t("interval", None, None).is_err());
    }

    #[test]
    fn columns() {
        let r = rs(
            &[
                "column_name",
                "data_type",
                "character_maximum_length",
                "numeric_precision",
                "numeric_scale",
            ],
            &[
                &[Some("id"), Some("bigint"), None, Some("64"), Some("0")],
                &[Some("n"), Some("numeric"), None, Some("18"), Some("4")],
                &[Some("s"), Some("character varying"), Some("64"), None, None],
                &[Some("m"), Some("numeric(12,3)"), None, None, None],
            ],
        );
        let c = parse_columns(&r, "s.t").expect("cols");
        assert_eq!(
            c.iter().map(|c| c.duck_type.as_str()).collect::<Vec<_>>(),
            ["BIGINT", "DECIMAL(18,4)", "VARCHAR", "DECIMAL(12,3)"]
        );
        assert_eq!(c[2].char_len, Some(64));
        assert!(parse_columns(&ResultSet::default(), "s.t").is_err());
    }

    #[test]
    fn discovery() {
        assert_eq!(
            tables_sql("est_v2.est_core_cons_*f", false).expect("sql"),
            "SELECT table_schema, table_name FROM svv_tables WHERE table_type IN ('BASE TABLE') AND table_schema = 'est_v2' ORDER BY 1, 2"
        );
        assert!(
            !tables_sql("fe_*.x", true)
                .expect("sql")
                .contains("table_schema =")
        );
        assert_eq!(
            tables_sql("vendor_db.est_v2.est_*", true).expect("sql"),
            "SELECT schema_name AS table_schema, table_name FROM svv_all_tables WHERE database_name = 'vendor_db' AND table_type IN ('TABLE', 'BASE TABLE', 'VIEW') AND schema_name = 'est_v2' ORDER BY 1, 2"
        );
        assert!(tables_sql("vendor_*.est_v2.x", false).is_err());
        let r = rs(
            &["table_schema", "table_name"],
            &[
                &[Some("est_v2"), Some("est_core_cons_qf")],
                &[Some("est_v2"), Some("est_core_cons_af")],
                &[Some("est_v2"), Some("est_core_cons_rec")],
                &[Some("est_v2"), Some("est_core_cons_Zf")],
                &[Some("est_v2"), Some("est_core_cons_x.f")],
            ],
        );
        assert_eq!(
            match_tables(
                &r,
                "est_v2.est_core_cons_*f",
                &["est_v2.est_core_cons_q?".into()]
            ),
            ["est_v2.est_core_cons_Zf", "est_v2.est_core_cons_af"]
        );
        assert_eq!(
            match_tables(&r, "vendor_db.est_v2.est_core_cons_a*", &[]),
            ["vendor_db.est_v2.est_core_cons_af"]
        );
    }

    #[test]
    fn objects() {
        let o = Object::parse("vendor_db.est_v2.t");
        assert_eq!(o.database.as_deref(), Some("vendor_db"));
        assert_eq!(o.sql(), r#""vendor_db"."est_v2"."t""#);
        assert_eq!(o.to_string(), "vendor_db.est_v2.t");
        assert_eq!(Object::parse("s.t").sql(), r#""s"."t""#);
        assert_eq!(Object::parse("t").schema, "public");
        assert_eq!(
            columns_sql(&o),
            "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale FROM svv_all_columns WHERE database_name = 'vendor_db' AND schema_name = 'est_v2' AND table_name = 't' ORDER BY ordinal_position"
        );
        assert!(
            columns_sql(&Object::parse("s.t"))
                .contains("FROM svv_columns WHERE table_schema = 's'")
        );
        assert_eq!(type_precision("numeric(18,4)"), (Some(18), Some(4)));
        assert_eq!(type_precision("decimal(9)"), (Some(9), None));
        assert_eq!(type_precision("character varying(64)"), (None, None));
        assert_eq!(type_precision("bigint"), (None, None));
    }
}
