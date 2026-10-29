use atlatl_core::ErrorClass;

use atlatl_conn_pgrepl::{PgChangeSource, StreamSpec};
use serde_json::json;

use crate::{Env, columns, stream, versions};

/// Microseconds since 1970 as Postgres counts them (exact through Julian days on every version).
const PG_US: &str = "(to_char({x}, 'J')::int8 - 2440588) * 86400000000 + (extract(epoch from {x}::time) * 1000000)::int8";

const ERA_ROWS: &str =
    "(1, '0044-03-15 BC', '0044-03-15 12:00:00.5 BC', '0044-03-15 12:00:00.5+00 BC'),
    (2, '4713-01-01 BC', '4713-01-01 00:00:00 BC', '4713-01-01 00:00:00+00 BC'),
    (3, '0001-01-01 BC', '0001-12-31 23:59:59.999999 BC', '0001-01-01 00:00:00+00 BC'),
    (4, '0001-01-01', '0001-01-01 00:00:00', '0001-01-01 00:00:00+00'),
    (5, '2024-02-29', '294247-01-10 04:00:54.775806', '294247-01-10 04:00:54.775806+00'),
    (6, 'infinity', '-infinity', 'infinity'),
    (7, NULL, NULL, NULL)";

/// Each value as Postgres renders it and its instant, against what the lake holds: BC dates and
/// timestamps keep their era (DuckDB counts 1 BC as year 0).
async fn same_instants(env: &Env, from: i32) {
    let us = |c: &str| PG_US.replace("{x}", c);
    let pg = env
        .pg_rows(&format!(
            "SELECT id || '|' || coalesce(d::text, '∅') || '@' || coalesce((CASE WHEN isfinite(d) THEN {} END)::text, '') || '|' || coalesce(ts::text, '∅') || '@' || coalesce((CASE WHEN isfinite(ts) THEN {} END)::text, '') || '|' || coalesce((CASE WHEN isfinite(tz) THEN {} END)::text, tz::text, '∅') FROM e WHERE id >= {from} ORDER BY id",
            us("d::timestamp"),
            us("ts"),
            us("(tz AT TIME ZONE 'UTC')"),
        ))
        .await;
    let duck = env.rows(&format!(
        "SELECT id::VARCHAR || '|' || coalesce(replace(d::VARCHAR, ' (BC)', ' BC'), '∅') || '@' || coalesce((CASE WHEN isfinite(d) THEN epoch_us(d) END)::VARCHAR, '') || '|' || coalesce(CASE WHEN ts::VARCHAR LIKE '% (BC) %' THEN replace(ts::VARCHAR, ' (BC)', '') || ' BC' ELSE ts::VARCHAR END, '∅') || '@' || coalesce((CASE WHEN isfinite(ts) THEN epoch_us(ts) END)::VARCHAR, '') || '|' || coalesce((CASE WHEN isfinite(tz) THEN epoch_us(tz) END)::VARCHAR, tz::VARCHAR, '∅') FROM lake.br.e WHERE id >= {from} ORDER BY id"
    ));
    assert_eq!(duck, pg);
    assert!(
        from > 0
            || pg[0].starts_with("1|0044-03-15 BC@-63517824000000000|0044-03-15 12:00:00.5 BC@"),
        "{pg:?}"
    );
}

async fn bc_values_keep_their_era(v: u32) {
    let env = Env::new(v).await;
    env.exec(&format!(
        "CREATE TABLE e (id int PRIMARY KEY, d date, ts timestamp, tz timestamptz);
         INSERT INTO e VALUES {ERA_ROWS}"
    ))
    .await;
    let spec = env.spec(vec![stream("e")]);
    env.run(spec.clone()).await;
    same_instants(&env, 0).await;
    assert_eq!(
        env.rows("SELECT year(d)::VARCHAR FROM lake.br.e WHERE id = 1"),
        ["-43"]
    );

    env.exec("INSERT INTO e SELECT id + 10, d, ts, tz FROM e")
        .await;
    let r = env.run(spec).await;
    assert_eq!(r.applied.window.binary, v >= 14);
    assert_eq!(r.applied.streams[0].counts.inserted, 7);
    same_instants(&env, 10).await;
}
versions!(bc_values_keep_their_era);

async fn data_error(env: &Env, spec: atlatl_conn_pgrepl::ReplicationSpec) -> String {
    let e = env
        .try_run(spec, crate::policy(true, false))
        .await
        .expect_err("unlandable");
    assert_eq!(
        (e.class, e.code.as_deref()),
        (ErrorClass::Data, Some("cast_failed")),
        "{e}"
    );
    e.message
}

async fn unlandable_values_name_their_column(v: u32) {
    let env = Env::new(v).await;
    env.exec(
        "CREATE TABLE u (id int PRIMARY KEY, ts timestamp, tz timestamptz, n numeric(10,2));
         INSERT INTO u VALUES (1, '294247-01-10 04:00:54.775807', NULL, 1.5);",
    )
    .await;
    let spec = env.spec(vec![stream("u")]);
    assert_eq!(
        data_error(&env, spec.clone()).await,
        "column `ts` of `public.u` holds 294247-01-10 04:00:54.775807, past the last timestamp the target holds (294247-01-10 04:00:54.775806)"
    );
    env.exec("UPDATE u SET ts = NULL").await;
    env.run(spec.clone()).await;
    assert_eq!(env.rows("SELECT n::VARCHAR FROM lake.br.u"), ["1.50"]);

    env.exec("INSERT INTO u (id, n) VALUES (2, 'NaN')").await;
    assert_eq!(
        data_error(&env, spec.clone()).await,
        "column `n` of `public.u` holds NaN, which a decimal can't hold"
    );
    env.exec("DELETE FROM u WHERE id = 2; INSERT INTO u (id, tz) VALUES (3, '294276-12-31 23:59:59.999999+00')")
        .await;
    assert_eq!(
        data_error(&env, spec).await,
        "column `tz` of `public.u` holds 294276-12-31 23:59:59.999999+00, past the last timestamp the target holds (294247-01-10 04:00:54.775806+00)"
    );
}
versions!(unlandable_values_name_their_column);

const W_DDL: &str = "CREATE TYPE mood AS ENUM ('ok', 'meh');
    CREATE TYPE pair AS (a int, b text);
    CREATE DOMAIN cash AS money;
    CREATE DOMAIN amount AS numeric;
    CREATE DOMAIN pos AS int CHECK (VALUE > 0);
    CREATE TABLE w (id int PRIMARY KEY, n numeric, m numeric(50,2), k money, c \"char\", h cash,
                    a amount, i inet, md mood, p pair, r int4range, b bit(3), x xml, o oid,
                    ls money[], ns numeric[], ms mood[], ia int[], q pos, mac macaddr, tv tsvector);
    CREATE TABLE w2 (id int PRIMARY KEY, net cidr, ok text);";

/// Every refused column of every stream, in one `config` error, before a slot or publication.
async fn every_refused_column_is_named_before_the_slot(v: u32) {
    let env = Env::new(v).await;
    env.exec(W_DDL).await;
    let spec = env.spec(vec![stream("w"), stream("w2")]);
    let e = env
        .try_run(spec.clone(), crate::policy(true, false))
        .await
        .expect_err("refused");
    assert_eq!(
        (e.class, e.code.as_deref()),
        (ErrorClass::Config, Some("source_type"))
    );
    let numeric = (
        "decimals hold at most 38 digits at a fixed scale, and it would arrive as a float",
        "DECIMAL(38,s), or VARCHAR",
    );
    let array = (
        "a Postgres array carries its own bounds and dimensions, which a list can't hold",
        "VARCHAR, for its Postgres text",
    );
    let net = ("there's no network address type", "VARCHAR");
    let want: &[(&str, &str, &str, (&str, &str))] = &[
        ("w", "n", "numeric", numeric),
        ("w", "m", "numeric(50,2)", numeric),
        (
            "w",
            "k",
            "money",
            (
                "its text follows the server's `lc_monetary`",
                "DECIMAL(19,2)",
            ),
        ),
        (
            "w",
            "c",
            "\"char\"",
            (
                "it's a single byte, and one that isn't valid UTF-8 on its own fails the read",
                "VARCHAR",
            ),
        ),
        (
            "w",
            "h",
            "cash (its money)",
            (
                "its text follows the server's `lc_monetary`",
                "DECIMAL(19,2)",
            ),
        ),
        ("w", "a", "amount (its numeric)", numeric),
        ("w", "i", "inet", net),
        ("w", "md", "mood", ("there's no enum type", "VARCHAR")),
        (
            "w",
            "p",
            "pair",
            (
                "there's no composite type",
                "VARCHAR, for its Postgres text",
            ),
        ),
        ("w", "r", "int4range", ("there's no range type", "VARCHAR")),
        (
            "w",
            "b",
            "bit(3)",
            ("there's no bit-string type", "VARCHAR"),
        ),
        ("w", "x", "xml", ("there's no XML type", "VARCHAR")),
        (
            "w",
            "o",
            "oid",
            (
                "it arrives unsigned from a table and as text from a query",
                "BIGINT",
            ),
        ),
        ("w", "ls", "money[]", array),
        ("w", "ns", "numeric[]", array),
        ("w", "ms", "mood[]", array),
        ("w", "ia", "integer[]", array),
        ("w", "mac", "macaddr", net),
        (
            "w",
            "tv",
            "tsvector",
            ("there's no text-search type", "VARCHAR"),
        ),
        ("w2", "net", "cidr", net),
    ];
    let lines: Vec<&str> = e.message.lines().collect();
    assert_eq!(
        lines[0],
        format!("{} source columns can't be landed as they are:", want.len())
    );
    assert_eq!(lines.len(), want.len() + 1, "{}", e.message);
    for (line, (t, c, ty, (why, declare))) in lines[1..].iter().zip(want) {
        assert_eq!(
            *line,
            format!(
                "  `public.{t}`: column `{c}` is {ty}, which the target can't hold: {why}; declare `columns.{c}.type` ({declare}), or leave it out with `columns.exclude`"
            )
        );
    }
    assert!(env.confirmed().await.is_none(), "refused before the slot");
    let slot = atlatl_conn_pgrepl::slot_name(&env.name);
    assert!(
        env.pg_rows(&format!(
            "SELECT pubname::text FROM pg_publication WHERE pubname = '{slot}'"
        ))
        .await
        .is_empty()
    );
    let mut src = env.source(spec, crate::policy(true, false)).await;
    let plan: Vec<String> = src
        .refused()
        .await
        .expect("refused")
        .into_iter()
        .map(|(t, l)| format!("  `{t}`: {l}"))
        .collect();
    assert_eq!(plan, lines[1..], "plan-time judgement matches the run's");
    src.close().await.expect("close");

    let mut w = stream("w");
    w.columns = columns(json!({
        "ia": {"type": "JSON"},
        "k": {"type": "TIMESTAMP"},
        "*": {"type": "VARCHAR"},
    }));
    let e = env
        .try_run(env.spec(vec![w]), crate::policy(true, false))
        .await
        .expect_err("uncheckable");
    assert_eq!(
        e.message,
        "2 source columns can't be landed as they are:
  `public.w`: column `k` is money, and `columns.k.type: TIMESTAMP` can't be checked against exactly from its text; declare VARCHAR, JSON, DECIMAL(p,s), an integer or a float type (e.g. DECIMAL(19,2))
  `public.w`: column `ia` is integer[], and `columns.ia.type: JSON` can't be checked against exactly from its text; declare VARCHAR (changes carry its Postgres text, not JSON)"
    );
}
versions!(every_refused_column_is_named_before_the_slot);

/// Opted-in columns land the text the Postgres source would read for them, exactly.
async fn declared_types_land_exactly(v: u32) {
    let env = Env::new(v).await;
    env.exec(W_DDL).await;
    env.exec(
        "INSERT INTO w VALUES (1, 1.25, 1.5, 1234.5, 'a', 2, 3.1000, '10.0.0.1', 'ok', '(1,\"x y\")',
           '[1,5)', B'101', '<a>1</a>', 4000000000, '{1.5,NULL}', '{1.10,NaN}', '{ok,meh}',
           '[0:1]={1,2}', 7, '08:00:2b:01:02:03', 'a fat cat');",
    )
    .await;
    let mut w = stream("w");
    w.columns = columns(json!({
        "n": {"type": "DECIMAL(38,10)"},
        "m": {"type": "decimal(10, 2)"},
        "k": {"type": "DECIMAL(19,2)"},
        "h": {"type": "VARCHAR"},
        "o": {"type": "BIGINT"},
        "q": {"type": "BIGINT"},
        "c": {"type": "VARCHAR"}, "a": {"type": "VARCHAR"}, "i": {"type": "VARCHAR"},
        "md": {"type": "VARCHAR"}, "p": {"type": "VARCHAR"}, "r": {"type": "VARCHAR"},
        "b": {"type": "VARCHAR"}, "x": {"type": "VARCHAR"}, "ls": {"type": "VARCHAR"},
        "ns": {"type": "VARCHAR"}, "ms": {"type": "VARCHAR"}, "ia": {"type": "VARCHAR"},
        "mac": {"type": "VARCHAR"}, "tv": {"type": "VARCHAR"},
    }));
    let spec = env.spec(vec![w]);
    env.run(spec.clone()).await;
    assert_eq!(
        env.rows(
            "SELECT column_name || ' ' || data_type FROM information_schema.columns
             WHERE table_catalog = 'lake' AND table_schema = 'br' AND table_name = 'w'
               AND column_name IN ('id', 'n', 'm', 'k', 'h', 'o', 'q', 'i') ORDER BY ordinal_position"
        ),
        [
            "id INTEGER",
            "n DECIMAL(38,10)",
            "m DECIMAL(10,2)",
            "k DECIMAL(19,2)",
            "h VARCHAR",
            "i VARCHAR",
            "o BIGINT",
            "q BIGINT",
        ]
    );
    // What the Postgres source reads for each: `::text`, `money` as numeric, `"char"` escaped.
    let pg_rows = "SELECT concat_ws('|', id, n::numeric(38,10), m::numeric(10,2), k::numeric,
        encode(set_byte('\\x00'::bytea, 0, c::int4 & 255), 'escape'), h::numeric, a, i::text, md, p,
        r, b, x, o, ls, ns, ms, ia, q, mac, tv) FROM w ORDER BY id";
    let rows =
        "SELECT concat_ws('|', id, n, m, k, c, h, a, i, md, p, r, b, x, o, ls, ns, ms, ia, q,
        mac, tv) FROM lake.br.w ORDER BY id";
    assert_eq!(env.rows(rows), env.pg_rows(pg_rows).await);
    assert_eq!(
        env.rows("SELECT k::VARCHAR || ' ' || i || ' ' || ia FROM lake.br.w"),
        ["1234.50 10.0.0.1/32 [0:1]={1,2}"]
    );

    env.exec(
        "INSERT INTO w (id, n, m, k, c, h, i, ia, o) VALUES
           (2, 123456789012345678901234567.0123456789, 12345678.99, -92233720368547758.08,
            'é', -1000000, '::1', '{{1,2},{3,4}}', 0),
           (3, NULL, NULL, 0, '\\', NULL, '192.168.0.0/16', '{}', 4294967295);
         UPDATE w SET c = '' WHERE id = 1;",
    )
    .await;
    env.run(spec.clone()).await;
    assert_eq!(env.rows(rows), env.pg_rows(pg_rows).await);
    assert_eq!(
        env.rows("SELECT c FROM lake.br.w ORDER BY id"),
        ["\\000", "\\303", "\\\\"]
    );

    env.exec("INSERT INTO w (id, n) VALUES (4, 0.00000000001)")
        .await;
    let e = env
        .try_run(spec, crate::policy(true, false))
        .await
        .expect_err("too fine");
    assert_eq!(
        (e.class, e.code.as_deref(), e.message.as_str()),
        (
            ErrorClass::Data,
            Some("cast_failed"),
            "column `n` of `public.w` holds 0.00000000001, which DECIMAL(38,10) can't hold exactly; declare a wider `DECIMAL(p,s)`"
        )
    );
}
versions!(declared_types_land_exactly);

fn lake_columns(env: &Env, table: &str) -> Vec<String> {
    env.rows(&format!(
        "SELECT column_name FROM information_schema.columns
         WHERE table_catalog = 'lake' AND table_schema = 'br' AND table_name = '{table}'
           AND column_name NOT LIKE '\\_atlatl%' ESCAPE '\\' ORDER BY ordinal_position"
    ))
}

/// Left-out columns are never staged or landed, in the backfill, in changes, or when a Relation
/// message brings new ones; a refused type left out needs no declaration.
async fn include_and_exclude_narrow_the_columns(v: u32) {
    let env = Env::new(v).await;
    env.exec(
        "CREATE TABLE pii (id int PRIMARY KEY, name text, ssn text, card money);
         CREATE TABLE inc (id int PRIMARY KEY, a text, b text, c inet);
         INSERT INTO pii VALUES (1, 'ann', '111-11-1111', 5);
         INSERT INTO inc VALUES (1, 'a1', 'b1', '10.0.0.1');",
    )
    .await;
    let pii = StreamSpec {
        columns: columns(json!({"exclude": ["SSN", "card", "secret"]})),
        ..stream("pii")
    };
    let inc = StreamSpec {
        columns: columns(json!({"include": ["id", "b"]})),
        ..stream("inc")
    };
    let spec = env.spec(vec![pii, inc]);
    env.run(spec.clone()).await;
    assert_eq!(lake_columns(&env, "pii"), ["id", "name"]);
    assert_eq!(lake_columns(&env, "inc"), ["id", "b"]);

    env.exec(
        "ALTER TABLE pii ADD COLUMN email text, ADD COLUMN secret text;
         INSERT INTO pii VALUES (2, 'bob', '222-22-2222', 6, 'bob@example.com', 's3cret');
         UPDATE pii SET ssn = '333-33-3333', secret = 'x' WHERE id = 1;
         ALTER TABLE inc ADD COLUMN d text;
         INSERT INTO inc VALUES (2, 'a2', 'b2', '::1', 'd2');
         UPDATE inc SET a = 'a3' WHERE id = 1;",
    )
    .await;
    let r = env.run(spec).await;
    assert_eq!(
        (
            r.applied.streams[0].counts.inserted,
            r.applied.streams[0].counts.unchanged
        ),
        (1, 1),
        "a change to left-out columns only leaves the lake row as is"
    );
    assert_eq!(lake_columns(&env, "pii"), ["id", "name", "email"]);
    assert_eq!(lake_columns(&env, "inc"), ["id", "b"]);
    assert_eq!(
        env.rows("SELECT concat_ws('|', id, name, email) FROM lake.br.pii ORDER BY id"),
        ["1|ann", "2|bob|bob@example.com"]
    );
    assert_eq!(
        env.rows("SELECT concat_ws('|', id, b) FROM lake.br.inc ORDER BY id"),
        ["1|b1", "2|b2"]
    );
}
versions!(include_and_exclude_narrow_the_columns);

async fn key_columns_and_unsupported_settings_are_config_errors(v: u32) {
    let env = Env::new(v).await;
    env.exec("CREATE TABLE k (id int PRIMARY KEY, v text, w text)")
        .await;
    for cols in [json!({"exclude": ["ID"]}), json!({"include": ["v"]})] {
        let s = StreamSpec {
            columns: columns(cols),
            ..stream("k")
        };
        let e = env
            .try_run(env.spec(vec![s]), crate::policy(true, false))
            .await
            .expect_err("key left out");
        assert_eq!(
            (e.class, e.message.as_str()),
            (
                ErrorClass::Config,
                "stream `pg/k`: primary key column `id` is left out by `columns.include`/`exclude`; a changes stream lands its key"
            )
        );
    }
    assert!(env.confirmed().await.is_none());

    for (cols, k) in [
        (json!({"v": {"transform": "upper(v)"}}), "v.transform"),
        (json!({"v": {"target_type": "text"}}), "v.target_type"),
        (json!({"w2": {"source_name": "w"}}), "w2.source_name"),
    ] {
        let s = StreamSpec {
            columns: columns(cols),
            ..stream("k")
        };
        let e = PgChangeSource::open(
            env.opts.clone(),
            crate::policy(true, false),
            env.spec(vec![s]),
        )
        .await
        .err()
        .expect("unsupported");
        assert_eq!(
            (e.class, e.message),
            (
                ErrorClass::Config,
                format!(
                    "`columns.{k}` does not apply to postgres `changes` streams (stream `pg/k`)"
                )
            )
        );
    }
}
versions!(key_columns_and_unsupported_settings_are_config_errors);

/// A source type changed under an existing lake column: values land in the lake's type exactly
/// or fail naming the column; a narrower source type widens freely.
async fn casts_into_lake_columns_are_exact(v: u32) {
    let env = Env::new(v).await;
    env.exec(
        "CREATE TABLE nr (id int PRIMARY KEY, d numeric(10,2), i int, dt date, wd numeric(20,4), wi bigint);
         INSERT INTO nr VALUES (1, 1.5, 7, '2024-01-02', 1.2345, 9000);",
    )
    .await;
    let spec = env.spec(vec![stream("nr")]);
    env.run(spec.clone()).await;
    env.exec(
        "ALTER TABLE nr ALTER d TYPE numeric(20,4), ALTER i TYPE bigint, ALTER dt TYPE timestamp,
           ALTER wd TYPE numeric(10,2), ALTER wi TYPE int;
         INSERT INTO nr VALUES (2, 2.5, 8, '2024-01-03 00:00', 3.25, 5);",
    )
    .await;
    env.run(spec.clone()).await;
    assert_eq!(
        env.rows("SELECT concat_ws('|', id, d, i, dt, wd, wi, typeof(d), typeof(i), typeof(dt), typeof(wd), typeof(wi)) FROM lake.br.nr ORDER BY id"),
        [
            "1|1.50|7|2024-01-02|1.2345|9000|DECIMAL(10,2)|INTEGER|DATE|DECIMAL(20,4)|BIGINT",
            "2|2.50|8|2024-01-03|3.2500|5|DECIMAL(10,2)|INTEGER|DATE|DECIMAL(20,4)|BIGINT",
        ]
    );

    for (row, want) in [
        (
            "(3, 1.2345, 1, '2024-01-04', 0, 0)",
            "column `d` of `public.nr` holds 1.2345, which DECIMAL(10,2) can't hold exactly (it'd fail to cast, round or truncate); widen the lake column",
        ),
        (
            "(3, 1, 3000000000, '2024-01-04', 0, 0)",
            "column `i` of `public.nr` holds 3000000000, which INTEGER can't hold exactly (it'd fail to cast, round or truncate); widen the lake column",
        ),
        (
            "(3, 1, 1, '2024-01-04 01:00', 0, 0)",
            "column `dt` of `public.nr` holds 2024-01-04 01:00:00, which DATE can't hold exactly (it'd fail to cast, round or truncate); widen the lake column",
        ),
    ] {
        env.exec(&format!(
            "DELETE FROM nr WHERE id = 3; INSERT INTO nr VALUES {row}"
        ))
        .await;
        let e = env
            .try_run(spec.clone(), crate::policy(true, false))
            .await
            .expect_err("narrowing");
        assert_eq!(
            (e.class, e.code.as_deref(), e.message.as_str()),
            (ErrorClass::Data, Some("cast_failed"), want),
            "{row}"
        );
    }
    assert_eq!(env.rows("SELECT count(*)::VARCHAR FROM lake.br.nr"), ["2"]);
}
versions!(casts_into_lake_columns_are_exact);
