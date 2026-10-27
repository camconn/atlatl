//! Postgres text values → lake types. Values are staged as text and cast in DuckDB SQL by the
//! Relation message's type OID and typmod. The mapping follows DuckDB's postgres
//! scanner so a table reads the same through either path; anything unmapped stays VARCHAR.

/// The lake type for a Postgres column.
pub fn lake_type(oid: u32, typmod: i32) -> String {
    match oid {
        16 => "BOOLEAN".into(),
        17 => "BLOB".into(),
        20 | 26 => "BIGINT".into(),
        21 => "SMALLINT".into(),
        23 => "INTEGER".into(),
        114 | 3802 => "JSON".into(),
        700 => "FLOAT".into(),
        701 => "DOUBLE".into(),
        1082 => "DATE".into(),
        1083 => "TIME".into(),
        1114 => "TIMESTAMP".into(),
        1184 => "TIMESTAMP WITH TIME ZONE".into(),
        1186 => "INTERVAL".into(),
        1266 => "TIME WITH TIME ZONE".into(),
        1700 => match numeric(typmod) {
            Some((p, s)) => format!("DECIMAL({p},{s})"),
            None => "DOUBLE".into(),
        },
        2950 => "UUID".into(),
        _ => match array_element(oid) {
            Some(e) => format!("{}[]", lake_type(e, -1)),
            None => "VARCHAR".into(),
        },
    }
}

/// `numeric(p,s)` from its typmod, when it fits DuckDB's DECIMAL.
fn numeric(typmod: i32) -> Option<(i32, i32)> {
    if typmod < 4 {
        return None;
    }
    let t = typmod - 4;
    let (p, s) = ((t >> 16) & 0xffff, t & 0xffff);
    (p <= 38 && s <= p).then_some((p, s))
}

/// Arrays whose text form (`{1,2,NULL}`) becomes a DuckDB list literal by swapping braces.
fn array_element(oid: u32) -> Option<u32> {
    Some(match oid {
        1000 => 16,
        1005 => 21,
        1007 => 23,
        1016 => 20,
        1021 => 700,
        1022 => 701,
        _ => return None,
    })
}

/// A DuckDB expression turning the Postgres text `v` into `target`.
pub fn cast(v: &str, oid: u32, typmod: i32, target: &str) -> String {
    let native = lake_type(oid, typmod);
    let e = match oid {
        17 => format!("from_hex(substr({v}, 3))"),
        1186 => format!("CAST(replace({v}, '+', '') AS INTERVAL)"),
        _ if array_element(oid).is_some() => {
            format!("CAST(replace(replace({v}, '{{', '['), '}}', ']') AS {native})")
        }
        _ if native == "VARCHAR" => v.to_owned(),
        _ => format!("CAST({v} AS {native})"),
    };
    if native.eq_ignore_ascii_case(target) {
        e
    } else {
        format!("CAST({e} AS {target})")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapping() {
        assert_eq!(lake_type(1700, ((12 << 16) | 3) + 4), "DECIMAL(12,3)");
        assert_eq!(lake_type(1700, -1), "DOUBLE");
        assert_eq!(lake_type(1700, ((50 << 16) | 3) + 4), "DOUBLE");
        assert_eq!(lake_type(1007, -1), "INTEGER[]");
        assert_eq!(lake_type(1009, -1), "VARCHAR");
        assert_eq!(lake_type(1043, 36), "VARCHAR");
        assert_eq!(cast("x", 25, -1, "VARCHAR"), "x");
        assert_eq!(
            cast("x", 23, -1, "BIGINT"),
            "CAST(CAST(x AS INTEGER) AS BIGINT)"
        );
        assert_eq!(cast("x", 17, -1, "BLOB"), "from_hex(substr(x, 3))");
    }

    #[test]
    fn casts_run_in_duckdb() {
        let c = duckdb::Connection::open_in_memory().expect("db");
        let cases: &[(&str, u32, i32, &str)] = &[
            ("t", 16, -1, "true"),
            ("\\x00ff", 17, -1, "\\x00\\xFF"),
            ("9007199254740993", 20, -1, "9007199254740993"),
            ("12.345", 1700, ((12 << 16) | 3) + 4, "12.345"),
            (
                "2024-03-10 12:00:00.123456+00",
                1184,
                -1,
                "2024-03-10 12:00:00.123456+00",
            ),
            (
                "-1 years -2 mons +3 days -04:05:06",
                1186,
                -1,
                "-1 year -2 months 3 days -04:05:06",
            ),
            ("{1,2,NULL}", 1007, -1, "[1, 2, NULL]"),
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
        c.execute_batch("SET TimeZone = 'UTC'").expect("tz");
        for (text, oid, typmod, want) in cases {
            let t = lake_type(*oid, *typmod);
            let sql = format!(
                "SELECT ({})::VARCHAR FROM (SELECT ?::VARCHAR AS v)",
                cast("v", *oid, *typmod, &t)
            );
            let got: String = c
                .query_row(&sql, [text], |r| r.get(0))
                .unwrap_or_else(|e| panic!("{text} ({oid}): {e}"));
            assert_eq!(&got, want, "{text} ({oid})");
        }
    }
}
