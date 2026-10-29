use atlatl_conn_postgres::source::{self, PgSource, Read, SourceColumn};
use atlatl_conn_postgres::{ConnectOptions, connect};
use atlatl_core::{AtlatlError, ErrorClass};
use atlatl_testkit::Postgres;

use crate::fresh;

on_both!(
    refusals_name_every_column,
    declared_types_land_exactly,
    narrowing_declarations_fail_loudly,
    nan_in_a_decimal_fails_loudly,
    domains_read_as_their_base_type,
    queries_are_checked_too,
    rendered_text_is_utc,
    timestamps_past_the_engines_range_fail_loudly,
    dates_land_across_postgres_range,
    nan_guard_follows_the_read,
    char_hint_reads_every_byte,
);

async fn seed(o: &ConnectOptions, sql: &str) {
    connect(o)
        .await
        .expect("connect")
        .client()
        .batch_execute(sql)
        .await
        .expect("seed");
}

fn engine(o: &ConnectOptions) -> duckdb::Connection {
    let c = atlatl_testkit::lake::open_engine().expect("engine");
    c.execute_batch(&source::attach_sql("src", o))
        .expect("attach");
    c
}

fn declared(
    cols: Vec<SourceColumn>,
    d: &'static [(&'static str, &'static str)],
) -> Vec<SourceColumn> {
    source::admit(cols, &|n| {
        d.iter()
            .find(|(c, _)| *c == n)
            .map(|(_, k)| (*k).to_owned())
    })
    .expect("admit")
}

fn rows(c: &duckdb::Connection, sql: &str) -> Result<Vec<String>, AtlatlError> {
    let mut st = c
        .prepare(sql)
        .map_err(|e| AtlatlError::internal(e.to_string()))?;
    st.query_map([], |r| r.get::<_, Option<String>>(0))
        .and_then(|r| r.collect::<Result<Vec<_>, _>>())
        .map(|v| {
            v.into_iter()
                .map(|x| x.unwrap_or_else(|| "NULL".into()))
                .collect()
        })
        .map_err(|e| source::reclassify(AtlatlError::internal(e.to_string())))
}

const TYPES: &str = r#"CREATE TYPE mood AS ENUM ('happy', 'sad');
CREATE TYPE pair AS (a int, b text);
CREATE DOMAIN price AS money;
CREATE TABLE odd (id int PRIMARY KEY, cash money, o oid, c "char", x xml, ip inet, cidr_ cidr,
  mac macaddr, bits bit(4), vb varbit, ts_v tsvector, lsn pg_lsn, rc regclass, r int4range,
  p point, m mood, arr int[], pr pair, big numeric, wide numeric(50,2), dp price, ok text);"#;

async fn refusals_name_every_column(pg: &'static Postgres) {
    let o = fresh(pg);
    seed(&o, TYPES).await;
    let s = PgSource::new(o.clone()).expect("source");
    let cols = s.columns("public", "odd", None, &[]).await.expect("cols");
    let lines = source::admit(cols.clone(), &|_| None).expect_err("refused");
    let named: Vec<&str> = lines
        .iter()
        .map(|l| l.split('`').nth(1).expect("column"))
        .collect();
    assert_eq!(
        named,
        [
            "cash", "o", "c", "x", "ip", "cidr_", "mac", "bits", "vb", "ts_v", "lsn", "rc", "r",
            "p", "m", "arr", "pr", "big", "wide", "dp"
        ],
        "every column but the natives, in order"
    );
    assert!(
        lines
            .iter()
            .all(|l| l.contains(", which the target can't hold: ")
                && l.contains("; convert it in `from.query` (")
                && l.contains("or declare `columns."))
    );
    assert!(
        lines[19].starts_with("column `dp` is price (its money), "),
        "{}",
        lines[19]
    );
    assert!(
        lines[15].contains("(`to_json(arr)::text`)"),
        "{}",
        lines[15]
    );
    let e = source::relation("src", "public", "odd", &cols).expect_err("relation");
    assert_eq!(
        (e.class, e.code.as_deref()),
        (ErrorClass::Config, Some("source_type"))
    );
    assert!(
        e.message.starts_with(
            "20 source columns can't be landed as they are:\n  `public.odd`: column `cash` is money"
        ),
        "{e}"
    );
    s.close().await;
}

async fn declared_types_land_exactly(pg: &'static Postgres) {
    let o = fresh(pg);
    seed(
        &o,
        r#"CREATE TYPE mood AS ENUM ('happy', 'sad');
           CREATE TYPE pair AS (a int, b text);
           CREATE TABLE t (id int, big numeric, huge numeric(50,2), cash money, o oid, m mood,
             arr int[], pr pair, ip inet, r int4range);
           INSERT INTO t VALUES
             (1, 123456789012345678901234567.1234567890, 1234567890.5, 12.34, 4294967295, 'sad',
              '[0:1]={7,8}', ROW(1, 'x'), '10.0.0.1/8', '[1,10)'),
             (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
             (3, -0.5, -1, -92233720368547758.08, 0, 'happy', '{{1,2},{3,4}}', ROW(NULL, NULL), '::1', 'empty');"#,
    )
    .await;
    let s = PgSource::new(o.clone()).expect("source");
    let cols = declared(
        s.columns("public", "t", None, &[]).await.expect("cols"),
        &[
            ("big", "DECIMAL(38,10)"),
            ("huge", "VARCHAR"),
            ("cash", "DECIMAL(19,2)"),
            ("o", "BIGINT"),
            ("m", "VARCHAR"),
            ("arr", "JSON"),
            ("pr", "JSON"),
            ("ip", "VARCHAR"),
            ("r", "VARCHAR"),
        ],
    );
    let c = engine(&o);
    let rel = source::relation("src", "public", "t", &cols).expect("rel");
    let types = rows(
        &c,
        &format!(
            "SELECT concat_ws(',', typeof(big), typeof(huge), typeof(cash), typeof(o), typeof(m), typeof(arr), typeof(pr), typeof(ip), typeof(r)) FROM {rel} LIMIT 1"
        ),
    )
    .expect("types");
    assert_eq!(
        types,
        ["DECIMAL(38,10),VARCHAR,DECIMAL(19,2),BIGINT,VARCHAR,JSON,JSON,VARCHAR,VARCHAR"]
    );
    let got = rows(
        &c,
        &format!(
            "SELECT concat_ws('|', id, coalesce(big::VARCHAR, 'NULL'), coalesce(huge, 'NULL'), coalesce(cash::VARCHAR, 'NULL'), coalesce(o::VARCHAR, 'NULL'), coalesce(m, 'NULL'), coalesce(arr::VARCHAR, 'NULL'), coalesce(pr::VARCHAR, 'NULL'), coalesce(ip, 'NULL'), coalesce(r, 'NULL')) FROM {rel} ORDER BY id"
        ),
    )
    .expect("read");
    assert_eq!(
        got,
        [
            r#"1|123456789012345678901234567.1234567890|1234567890.50|12.34|4294967295|sad|[7,8]|{"a":1,"b":"x"}|10.0.0.1/8|[1,10)"#,
            "2|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL",
            r#"3|-0.5000000000|-1.00|-92233720368547758.08|0|happy|[[1,2],[3,4]]|{"a":null,"b":null}|::1/128|empty"#,
        ]
    );
    s.close().await;
}

async fn narrowing_declarations_fail_loudly(pg: &'static Postgres) {
    let o = fresh(pg);
    seed(
        &o,
        "CREATE TABLE fits (n numeric); INSERT INTO fits VALUES (12345678.12), (-0.10), (NULL);
         CREATE TABLE scale (n numeric); INSERT INTO scale VALUES (1.234);
         CREATE TABLE digits (n numeric); INSERT INTO digits VALUES (123456789.1);
         CREATE TABLE nan (n numeric); INSERT INTO nan VALUES (1), ('NaN');
         CREATE TABLE frac (n numeric); INSERT INTO frac VALUES (1.5);
         CREATE TABLE over (n numeric); INSERT INTO over VALUES (99999999999);",
    )
    .await;
    let s = PgSource::new(o.clone()).expect("source");
    let c = engine(&o);
    let read = |t: &'static str, kind: &'static str| {
        let s = &s;
        let c = &c;
        async move {
            let cols = source::admit(s.columns("public", t, None, &[]).await.expect(t), &|_| {
                Some(kind.to_owned())
            })
            .expect("admit");
            rows(
                c,
                &format!(
                    "SELECT n::VARCHAR FROM {}",
                    source::relation("src", "public", t, &cols).expect("rel")
                ),
            )
        }
    };
    assert_eq!(
        read("fits", "DECIMAL(10,2)").await.expect("fits"),
        ["12345678.12", "-0.10", "NULL"]
    );
    for (t, kind, want) in [
        (
            "scale",
            "DECIMAL(10,2)",
            "holds 1.234, which DECIMAL(10,2) can't hold exactly",
        ),
        (
            "digits",
            "DECIMAL(10,2)",
            "holds 123456789.1, which DECIMAL(10,2)",
        ),
        ("nan", "DECIMAL(38,10)", "holds NaN, which DECIMAL(38,10)"),
        (
            "frac",
            "BIGINT",
            "holds 1.5, which BIGINT can't hold exactly",
        ),
    ] {
        let e = read(t, kind).await.expect_err(t);
        assert_eq!(e.class, ErrorClass::Data, "{t}: {e}");
        assert!(
            e.message
                .contains(&format!("column `n` of `public.{t}` {want}")),
            "{t}: {e}"
        );
    }
    let e = read("over", "INTEGER").await.expect_err("overflow");
    assert_eq!(e.class, ErrorClass::Data, "{e}");
    s.close().await;
}

async fn nan_in_a_decimal_fails_loudly(pg: &'static Postgres) {
    let o = fresh(pg);
    seed(
        &o,
        "CREATE TABLE ok (id int, n numeric(5,2)); INSERT INTO ok VALUES (1, 1.5), (2, NULL);
         CREATE TABLE bad (id int, n numeric(5,2)); INSERT INTO bad VALUES (1, 1.5), (2, 'NaN');",
    )
    .await;
    let s = PgSource::new(o.clone()).expect("source");
    let c = engine(&o);
    let ok = s.columns("public", "ok", None, &[]).await.expect("cols");
    assert_eq!(ok[1].read, Read::Decimal);
    let rel = source::relation("src", "public", "ok", &ok).expect("rel");
    assert_eq!(
        rows(&c, &format!("SELECT concat_ws(':', typeof(n), coalesce(n::VARCHAR, 'NULL')) FROM {rel} ORDER BY id"))
            .expect("ok"),
        ["DECIMAL(5,2):1.50", "DECIMAL(5,2):NULL"]
    );
    let bad = s.columns("public", "bad", None, &[]).await.expect("cols");
    let rel = source::relation("src", "public", "bad", &bad).expect("rel");
    let e = rows(&c, &format!("SELECT n::VARCHAR FROM {rel}")).expect_err("NaN");
    assert_eq!(e.class, ErrorClass::Data, "{e}");
    assert!(
        e.message.contains(
            "column `n` of `public.bad` holds NaN, which a decimal can't hold; convert it in `from.query` (`NULLIF(n, 'NaN')` to read it as NULL"
        ),
        "{e}"
    );
    s.close().await;
}

async fn domains_read_as_their_base_type(pg: &'static Postgres) {
    let o = fresh(pg);
    seed(
        &o,
        r#"CREATE DOMAIN posint AS int CHECK (VALUE > 0);
           CREATE DOMAIN dtstz AS timestamptz;
           CREATE DOMAIN dnum AS numeric(10,2);
           CREATE DOMAIN dnum2 AS dnum;
           CREATE DOMAIN dtext AS text;
           CREATE DOMAIN dbig AS numeric;
           CREATE TABLE t (id posint, at dtstz, n dnum2, s dtext, b dbig);
           INSERT INTO t VALUES (5, '2024-06-01 12:00:00+00', 12.34, 'hé', 1.5);"#,
    )
    .await;
    let s = PgSource::new(o.clone()).expect("source");
    let cols = s.columns("public", "t", None, &[]).await.expect("cols");
    assert_eq!(
        cols.iter()
            .map(|c| (c.pg_type.as_str(), c.base.as_str(), c.domain))
            .collect::<Vec<_>>(),
        [
            ("posint", "integer", true),
            ("dtstz", "timestamp with time zone", true),
            ("dnum2", "numeric(10,2)", true),
            ("dtext", "text", true),
            ("dbig", "numeric", true),
        ]
    );
    assert_eq!(cols[2].read, Read::Decimal);
    let e = source::relation("src", "public", "t", &cols).expect_err("dbig");
    assert!(
        e.message
            .contains("column `b` is dbig (its numeric), which the target can't hold"),
        "{e}"
    );
    let cols = declared(cols, &[("b", "DECIMAL(10,1)")]);
    let c = engine(&o);
    let rel = source::relation("src", "public", "t", &cols).expect("rel");
    assert_eq!(
        rows(
            &c,
            &format!(
                "SELECT concat_ws('|', typeof(id), typeof(\"at\"), typeof(n), typeof(s), typeof(b), id, strftime(\"at\" AT TIME ZONE 'UTC', '%Y-%m-%d %H:%M:%S'), n, s, b) FROM {rel}"
            )
        )
        .expect("read"),
        ["INTEGER|TIMESTAMP WITH TIME ZONE|DECIMAL(10,2)|VARCHAR|DECIMAL(10,1)|5|2024-06-01 12:00:00|12.34|hé|1.5"]
    );
    s.close().await;
}

async fn queries_are_checked_too(pg: &'static Postgres) {
    let o = fresh(pg);
    seed(
        &o,
        "CREATE TYPE mood AS ENUM ('happy', 'sad');
         CREATE DOMAIN dnum AS numeric(10,2);
         CREATE TABLE t (id int, n numeric, m mood, d dnum, cash money);
         INSERT INTO t VALUES (1, 1.25, 'sad', 3.5, 7), (2, 'NaN', NULL, 'NaN', NULL);",
    )
    .await;
    let s = PgSource::new(o.clone()).expect("source");
    let c = engine(&o);

    let cols = s
        .describe("SELECT id, n, m, d, cash FROM t")
        .await
        .expect("describe");
    assert_eq!(
        cols.iter()
            .map(|c| (c.name.as_str(), c.pg_type.as_str()))
            .collect::<Vec<_>>(),
        [
            ("id", "integer"),
            ("n", "numeric"),
            ("m", "mood"),
            ("d", "numeric(10,2)"),
            ("cash", "money")
        ],
        "a query's domains arrive as their base type"
    );
    let e =
        source::query_read("src", "SELECT id, n, m, d, cash FROM t", &cols).expect_err("refused");
    assert!(
        e.message.starts_with(
            "3 source columns can't be landed as they are:\n  `from.query`: column `n` is numeric"
        ),
        "{e}"
    );

    let q =
        "SELECT id, n::text AS n, m::text AS m, cash::numeric(19,2) AS cash FROM t ORDER BY id;";
    let cols = s.describe(q).await.expect("describe cast");
    assert!(
        cols.iter()
            .all(|c| c.read == Read::Native || c.read == Read::Decimal)
    );
    let rel = source::query_read("src", q, &cols).expect("cast in the query");
    assert_eq!(
        rows(
            &c,
            &format!("SELECT concat_ws('|', id, n, coalesce(m, 'NULL'), typeof(cash), coalesce(cash::VARCHAR, 'NULL')) FROM {rel} ORDER BY id")
        )
        .expect("read"),
        ["1|1.25|sad|DECIMAL(19,2)|7.00", "2|NaN|NULL|DECIMAL(19,2)|NULL"]
    );

    let q = "SELECT id, n FROM t ORDER BY id";
    let cols = declared(s.describe(q).await.expect("d"), &[("n", "DOUBLE")]);
    let rel = source::query_read("src", q, &cols).expect("declared");
    assert_eq!(
        rows(&c, &format!("SELECT n::VARCHAR FROM {rel} ORDER BY id")).expect("read"),
        ["1.25", "nan"]
    );
    let q = "SELECT d FROM t WHERE id = 2";
    let cols = s.describe(q).await.expect("d");
    let rel = source::query_read("src", q, &cols).expect("decimal");
    let e = rows(&c, &format!("SELECT d::VARCHAR FROM {rel}")).expect_err("NaN");
    assert!(
        e.message.contains("column `d` of `from.query` holds NaN"),
        "{e}"
    );

    let e = s
        .describe("SELECT nope FROM t")
        .await
        .expect_err("bad query");
    assert_eq!(e.class, ErrorClass::Config, "{e}");
    s.close().await;
}

async fn rendered_text_is_utc(pg: &'static Postgres) {
    let o = fresh(pg);
    seed(
        &o,
        &format!(
            "ALTER DATABASE \"{}\" SET TimeZone = 'Asia/Kolkata';
             CREATE TABLE t (r tstzrange, a timestamptz[]);
             INSERT INTO t VALUES ('[2024-01-01 00:00:00+00,2024-02-01 00:00:00+00)', ARRAY['2024-01-01 00:00:00+00'::timestamptz]);",
            o.database
        ),
    )
    .await;
    let s = PgSource::new(o.clone()).expect("source");
    let cols = declared(
        s.columns("public", "t", None, &[]).await.expect("cols"),
        &[("r", "VARCHAR"), ("a", "JSON")],
    );
    let rel = source::relation("src", "public", "t", &cols).expect("rel");
    assert_eq!(
        rows(
            &engine(&o),
            &format!("SELECT r || ' ' || a::VARCHAR FROM {rel}")
        )
        .expect("read"),
        [r#"["2024-01-01 00:00:00+00","2024-02-01 00:00:00+00") ["2024-01-01T00:00:00+00:00"]"#]
    );
    s.close().await;
}

const LAST: &str = "294247-01-10 04:00:54.775806";

async fn timestamps_past_the_engines_range_fail_loudly(pg: &'static Postgres) {
    let o = fresh(pg);
    seed(
        &o,
        &format!(
            "CREATE DOMAIN dtstz AS timestamptz;
             CREATE TABLE ok (k int, a timestamp, b timestamptz, d dtstz);
             INSERT INTO ok VALUES (1, '{LAST}', '{LAST}+00', '{LAST}+00'),
               (2, 'infinity', 'infinity', 'infinity'), (3, '-infinity', '-infinity', '-infinity'),
               (4, '4713-11-24 00:00:00 BC', '4713-11-24 00:00:00+00 BC', '4713-11-24 00:00:00+00 BC'),
               (5, NULL, NULL, NULL);"
        ),
    )
    .await;
    let s = PgSource::new(o.clone()).expect("source");
    let c = engine(&o);
    let read = "concat_ws('|', k, typeof(a), typeof(b), typeof(d), coalesce(CASE WHEN isfinite(a) THEN epoch_us(a)::VARCHAR ELSE a::VARCHAR END, 'NULL'), coalesce(CASE WHEN isfinite(b) THEN epoch_us(b)::VARCHAR ELSE b::VARCHAR END, 'NULL'), coalesce(CASE WHEN isfinite(d) THEN epoch_us(d)::VARCHAR ELSE d::VARCHAR END, 'NULL'))";
    let cols = s.columns("public", "ok", None, &[]).await.expect("cols");
    let rel = source::relation("src", "public", "ok", &cols).expect("rel");
    let ts = "TIMESTAMP|TIMESTAMP WITH TIME ZONE|TIMESTAMP WITH TIME ZONE";
    assert_eq!(
        rows(&c, &format!("SELECT {read} FROM {rel} ORDER BY k")).expect("in range"),
        [
            format!("1|{ts}|9223372036854775806|9223372036854775806|9223372036854775806"),
            format!("2|{ts}|infinity|infinity|infinity"),
            format!("3|{ts}|-infinity|-infinity|-infinity"),
            format!("4|{ts}|-210835180800000000|-210835180800000000|-210835180800000000"),
            format!("5|{ts}|NULL|NULL|NULL"),
        ],
        "the last microsecond, both infinities and Postgres's first day land exactly"
    );
    let q = "SELECT k, a, b, d FROM ok";
    let rel = source::query_read("src", q, &s.describe(q).await.expect("describe")).expect("q");
    assert_eq!(
        rows(&c, &format!("SELECT {read} FROM {rel} ORDER BY k"))
            .expect("query")
            .len(),
        5
    );

    let past = [
        "294247-01-10 04:00:54.775807",
        "294247-01-10 04:00:54.775808",
        "294276-12-31 23:59:59.999999",
    ];
    for (n, v) in past.iter().enumerate() {
        for (col, ty, zone) in [
            ("a", "timestamp", ""),
            ("b", "timestamptz", "+00"),
            ("d", "dtstz", "+00"),
        ] {
            let t = format!("bad_{col}_{n}");
            seed(
                &o,
                &format!(
                    "CREATE TABLE {t} (k int, {col} {ty}); INSERT INTO {t} VALUES (1, 'infinity'), (2, '{v}{zone}');"
                ),
            )
            .await;
            let cols = s.columns("public", &t, None, &[]).await.expect("cols");
            let rel = source::relation("src", "public", &t, &cols).expect("rel");
            assert_eq!(
                rel.contains(&format!(r#""src"."public"."{t}" AS atlatl_t"#)),
                col != "d",
                "a domain is converted in Postgres, the rest scanned: {rel}"
            );
            let want = format!(
                "column `{col}` of `public.{t}` holds {v}{zone}, past the last timestamp the target holds ({LAST}{zone}); convert it in `from.query` (`{col}::text`"
            );
            for sql in [
                format!("SELECT {col}::VARCHAR FROM {rel}"),
                format!(
                    "SELECT {col}::VARCHAR FROM {}",
                    source::relation_where("src", "public", &t, &cols, Some(r#""k" >= 2"#), &[])
                        .expect("rel")
                        .0
                ),
            ] {
                let e = rows(&c, &sql).expect_err(&t);
                assert_eq!(e.class, ErrorClass::Data, "{t}: {e}");
                assert!(e.message.contains(&want), "{t}: {e}");
            }
            let q = format!("SELECT {col} FROM {t}");
            let rel =
                source::query_read("src", &q, &s.describe(&q).await.expect("describe")).expect("q");
            let e = rows(&c, &format!("SELECT {col}::VARCHAR FROM {rel}")).expect_err("query");
            assert!(
                e.message
                    .contains(&want.replace(&format!("of `public.{t}`"), "of `from.query`")),
                "{t}: {e}"
            );
            let (rel, _) =
                source::relation_where("src", "public", &t, &cols, Some(r#""k" = 1"#), &[])
                    .expect("rel");
            if col != "d" {
                assert_eq!(
                    rows(&c, &format!("SELECT {col}::VARCHAR FROM {rel} WHERE k = 1"))
                        .expect("window"),
                    ["infinity"],
                    "rows outside the pushed range aren't looked at"
                );
            }
        }
    }
    s.close().await;
}

async fn dates_land_across_postgres_range(pg: &'static Postgres) {
    let o = fresh(pg);
    seed(
        &o,
        "CREATE TABLE t (k int, d date);
         INSERT INTO t VALUES (1, '5874897-12-31'), (2, '4713-11-24 BC'), (3, 'infinity'), (4, '-infinity');",
    )
    .await;
    let s = PgSource::new(o.clone()).expect("source");
    let cols = s.columns("public", "t", None, &[]).await.expect("cols");
    let rel = source::relation("src", "public", "t", &cols).expect("rel");
    assert_eq!(rel, r#"(SELECT "k", "d" FROM "src"."public"."t")"#);
    assert_eq!(
        rows(
            &engine(&o),
            &format!("SELECT d::VARCHAR FROM {rel} ORDER BY k")
        )
        .expect("read"),
        ["5874897-12-31", "4713-11-24 (BC)", "infinity", "-infinity"]
    );
    s.close().await;
}

async fn nan_guard_follows_the_read(pg: &'static Postgres) {
    let o = fresh(pg);
    seed(
        &o,
        "CREATE TABLE t (id int, u uuid, n numeric(12,2), at timestamptz);
         INSERT INTO t VALUES (1, '00000000-0000-0000-0000-000000000001', 1.5, '2024-01-01 00:00:00+00'),
           (2, '00000000-0000-0000-0000-000000000002', NULL, '2024-01-02 00:00:00+00'),
           (3, '00000000-0000-0000-0000-000000000003', 'NaN', '2024-01-03 00:00:00+00'),
           (4, '00000000-0000-0000-0000-000000000004', 2, '294250-01-01 00:00:00+00');",
    )
    .await;
    let s = PgSource::new(o.clone()).expect("source");
    let c = engine(&o);
    let cols = s.columns("public", "t", None, &[]).await.expect("cols");
    let pre = |w: &str| {
        source::window_predicate(w, &cols, &|sql| c.query_row(sql, [], |r| r.get(0)).ok())
    };
    // As the run reads a member: the guarded relation, wrapped in the engine-side filters.
    let read = |server: Option<&str>, filters: &[&str]| {
        let filters: Vec<String> = filters.iter().map(|f| (*f).to_owned()).collect();
        let (rel, pushed) =
            source::relation_where("src", "public", "t", &cols, server, &filters).expect("rel");
        assert!(!pushed && rel.contains(r#""src"."public"."t" AS atlatl_t CROSS JOIN"#));
        let w = filters
            .iter()
            .map(|f| format!("({f})"))
            .chain(["TRUE".to_owned()])
            .collect::<Vec<_>>()
            .join(" AND ");
        rows(
            &c,
            &format!(
                "SELECT concat_ws('|', id, coalesce(n::VARCHAR, 'NULL')) FROM {rel} x WHERE {w} ORDER BY id"
            ),
        )
    };
    let window = |w: &str| read(pre(w).as_deref(), &[w]);
    let nan = "column `n` of `public.t` holds NaN, which a decimal can't hold";
    let past = "column `at` of `public.t` holds 294250-01-01 00:00:00+00, past the last timestamp";
    let fails = |r: Result<Vec<String>, AtlatlError>, want: &str| {
        let e = r.expect_err(want);
        assert_eq!(e.class, ErrorClass::Data, "{e}");
        assert!(e.message.contains(want), "{e}");
    };

    fails(read(None, &[]), "column `");
    assert_eq!(pre("id <= 2").as_deref(), Some(r#"("id" <= '2'::numeric)"#));
    assert_eq!(pre("id IN (1, 2)"), None);
    for w in [
        "id <= 2",
        "id IN (1, 2)",
        "id + 0 < 3",
        r#""at" < '2024-01-03' AND id < 4"#,
    ] {
        assert_eq!(
            window(w).expect(w),
            ["1|1.50", "2|NULL"],
            "a NULL isn't a NaN, and values outside `{w}` aren't looked at"
        );
    }
    for w in ["id <= 3", "id IN (1, 2, 3)", "id + 0 = 3"] {
        fails(window(w), nan);
    }
    for w in ["id >= 4", "id IN (4)", "id + 0 = 4"] {
        fails(window(w), past);
    }
    // A filter testing a value the scan can't read fails wherever it sends the row: pushed into
    // the scan, Postgres compares the stored value; applied engine-side, the misread one.
    fails(window("n IS NULL"), nan);
    fails(window("n > 1 AND id < 4"), nan);
    fails(window(r#""at" < '2024-01-03'"#), past);

    // A cursor Postgres can't evaluate (uuid) only filters engine-side.
    let u = |v: &str| source::Cursor {
        column: "u".into(),
        stored: Some(format!("00000000-0000-0000-0000-00000000000{v}")),
        lookback_us: None,
        inclusive: true,
        include_nulls: false,
    };
    assert_eq!(source::cursor_predicate(&u("1"), &cols), None);
    let ucur = |v: &str| format!("\"u\" <= CAST('00000000-0000-0000-0000-00000000000{v}' AS UUID)");
    assert_eq!(
        read(None, &[&ucur("2")]).expect("cursor"),
        ["1|1.50", "2|NULL"]
    );
    fails(read(None, &[&ucur("3")]), nan);
    let ucur = |v: &str| format!("\"u\" >= CAST('00000000-0000-0000-0000-00000000000{v}' AS UUID)");
    fails(read(None, &[&ucur("4")]), past);
    assert_eq!(
        read(None, &[&ucur("4"), "id < 4"]).expect("cursor and window"),
        Vec::<String>::new()
    );

    // A pushed cursor narrows the guard in Postgres and is still applied engine-side.
    let at = source::Cursor {
        column: "at".into(),
        stored: Some("2024-01-02 00:00:00+00".into()),
        ..u("1")
    };
    let p = source::cursor_predicate(&at, &cols).expect("pushable");
    let f = r#""at" >= CAST('2024-01-02 00:00:00+00' AS TIMESTAMPTZ)"#;
    fails(read(Some(&p), &[f]), "column `");
    assert_eq!(
        read(Some(&p), &[f, "id <= 2"]).expect("cursor and window"),
        ["2|NULL"]
    );
    let id = source::Cursor {
        column: "id".into(),
        stored: Some("3".into()),
        ..u("1")
    };
    let p = source::cursor_predicate(&id, &cols).expect("pushable");
    let (rel, _) = source::relation_where("src", "public", "t", &cols, Some(&p), &[]).expect("rel");
    assert!(rel.contains(r#"AND ("id" >= ''3''::int4)')"#), "{rel}");
    fails(read(Some(&p), &[r#""id" >= CAST('3' AS INTEGER)"#]), nan);
    let p = source::cursor_predicate(
        &source::Cursor {
            stored: Some("4".into()),
            ..id
        },
        &cols,
    )
    .expect("pushable");
    fails(read(Some(&p), &[r#""id" >= CAST('4' AS INTEGER)"#]), past);
    s.close().await;
}

async fn char_hint_reads_every_byte(pg: &'static Postgres) {
    let o = fresh(pg);
    seed(
        &o,
        r#"CREATE TABLE t (id int, c "char");
           INSERT INTO t VALUES (1, 'a'), (2, chr(195)::"char"), (3, '\'), (4, NULL), (5, chr(127)::"char");"#,
    )
    .await;
    let s = PgSource::new(o.clone()).expect("source");
    let c = engine(&o);
    let cols = s.columns("public", "t", None, &[]).await.expect("cols");
    let line = source::admit(cols.clone(), &|_| None)
        .expect_err("refused")
        .remove(0);
    let hints: Vec<&str> = line
        .split_once("convert it in `from.query` (`")
        .and_then(|(_, r)| r.split_once(" for the byte)"))
        .expect("hint")
        .0
        .split("`, or `")
        .map(|h| h.trim_end_matches('`'))
        .collect();
    assert_eq!(hints.len(), 2, "{line}");
    let read = |sql: String| {
        let (s, c) = (&s, &c);
        async move {
            let rel = source::query_read("src", &sql, &s.describe(&sql).await.expect("describe"))
                .expect("q");
            rows(c, &format!("SELECT c::VARCHAR FROM {rel} ORDER BY id"))
        }
    };
    for (h, want) in hints.iter().zip([
        ["a", "\\303", "\\\\", "NULL", "\u{7f}"],
        ["97", "195", "92", "NULL", "127"],
    ]) {
        assert_eq!(
            read(format!("SELECT id, {h} AS c FROM t")).await.expect(h),
            want,
            "{h}"
        );
    }
    let cols = declared(cols, &[("c", "VARCHAR")]);
    let rel = source::relation("src", "public", "t", &cols).expect("rel");
    assert_eq!(
        rows(&c, &format!("SELECT c FROM {rel} ORDER BY id")).expect("declared"),
        ["a", "\\303", "\\\\", "NULL", "\u{7f}"]
    );
    let v: i32 = connect(&o)
        .await
        .expect("connect")
        .client()
        .query_one("SELECT current_setting('server_version_num')::int", &[])
        .await
        .expect("version")
        .get(0);
    let plain = read("SELECT id, c::text AS c FROM t".into()).await;
    assert_eq!(
        plain.is_err(),
        v < 150000,
        "`c::text` renders a non-ASCII byte as is before Postgres 15: {plain:?}"
    );
    s.close().await;
}
