//! Delta → DuckLake through atlatl-ingest (staging, drift, modes) on the S7 fixtures.

use std::path::{Path, PathBuf};

use atlatl_conn_delta::{self as delta, Pinned};
use atlatl_core::config::model::{
    ChunkBy, ChunkConfig, DeltaConn, MissingColumns, NewColumns, S3Provider, StorageConfig,
    StorageType, TypeChanges, UpdateKey, UrlStyle,
};
use atlatl_core::config::types::{Dur, SecretValue};
use atlatl_core::error::{ErrorClass, Result};
use atlatl_ingest::drift::{self, Adoption, Declared, Policy};
use atlatl_ingest::modes::{Apply, Counts, Kind, staged_max};
use atlatl_ingest::sql::{db, qualified};
use atlatl_ingest::staging::{self, describe, stage_query, table_columns};
use atlatl_testkit::LocalLake;
use duckdb::Connection;

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn local() -> DeltaConn {
    DeltaConn {
        root: fixtures().display().to_string(),
        storage: None,
    }
}

fn setup(conn: &DeltaConn) -> (LocalLake, Connection) {
    let lake = LocalLake::file().expect("lake");
    let c = lake.open("lake").expect("open");
    delta::load(&c, conn).expect("load delta");
    (lake, c)
}

struct Stream<'a> {
    table: &'a str,
    version: Option<u64>,
    target: &'a str,
    kind: Kind,
    pk: Vec<String>,
    window: Option<&'a str>,
    update_key: Option<UpdateKey>,
    stored: Option<String>,
    policy: Policy,
    set_null: bool,
}

impl<'a> Stream<'a> {
    fn new(table: &'a str, target: &'a str, kind: Kind) -> Self {
        Self {
            table,
            version: None,
            target,
            kind,
            pk: Vec::new(),
            window: None,
            update_key: None,
            stored: None,
            policy: Policy::from(None),
            set_null: false,
        }
    }
    fn at(mut self, v: u64) -> Self {
        self.version = Some(v);
        self
    }
    fn pk(mut self, k: &str) -> Self {
        self.pk = vec![k.into()];
        self
    }
}

#[derive(Debug)]
struct Outcome {
    version: u64,
    staged: u64,
    counts: Counts,
    watermark: Option<String>,
    adoption: Adoption,
}

fn split(target: &str) -> (&str, &str) {
    target.split_once('.').expect("schema.table")
}

/// One stream end to end, the way the executor runs it (04 §1).
fn sync(c: &Connection, conn: &DeltaConn, s: &Stream) -> Result<Outcome> {
    let t = delta::discover(c, conn, s.table)?;
    let p = delta::pin(
        c,
        &t.uri,
        s.version.unwrap_or(t.head),
        &delta::pin_alias(0, 0),
    )?;
    let f = p.filter(s.window, s.update_key.as_ref(), s.stored.as_deref())?;
    let stg = staging::staging_name("01TESTRUNABCDEFG", 0, 0);
    let staged = stage_query(c, &stg, &p.select(f.as_deref()))?;
    let cols = describe(c, &stg)?;
    let (schema, table) = split(s.target);
    let existing = table_columns(c, "lake", schema, table)?;
    let a = drift::plan(&cols, existing.as_deref(), s.policy, &Declared::new())?;
    drift::check_casts(c, &stg, &a, s.set_null)?;
    let fq = qualified(&["lake", schema, table]);
    let proj = drift::projection(&a);
    c.execute_batch("BEGIN").map_err(db)?;
    let applied = (|| {
        if a.create {
            c.execute_batch(&format!(
                "CREATE SCHEMA IF NOT EXISTS {}; {}",
                qualified(&["lake", schema]),
                drift::create_sql(&fq, &a)
            ))
            .map_err(db)?;
        }
        for d in drift::ddl(&fq, &a) {
            c.execute_batch(&d).map_err(db)?;
        }
        Apply {
            kind: s.kind,
            target: &fq,
            source: &stg,
            columns: &proj,
            primary_key: &s.pk,
            null_safe: false,
        }
        .run(c)
    })();
    match &applied {
        Ok(_) => c.execute_batch("COMMIT").map_err(db)?,
        Err(_) => c.execute_batch("ROLLBACK").map_err(db)?,
    }
    let counts = applied?;
    let watermark = match &s.update_key {
        Some(k) => staged_max(c, &stg, &k.column)?.map(|(v, _)| v),
        None => None,
    };
    staging::drop(c, &stg)?;
    p.detach(c)?;
    Ok(Outcome {
        version: p.version,
        staged,
        counts,
        watermark,
        adoption: a,
    })
}

fn one<T: duckdb::types::FromSql>(c: &Connection, sql: &str) -> T {
    c.query_row(sql, [], |r| r.get(0))
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn count(c: &Connection, rel: &str) -> i64 {
    one(c, &format!("SELECT count(*) FROM {rel}"))
}

fn types_of(c: &Connection, schema: &str, table: &str) -> Vec<String> {
    table_columns(c, "lake", schema, table)
        .expect("cols")
        .expect("exists")
        .into_iter()
        .map(|c| format!("{} {}", c.name, c.kind))
        .collect()
}

/// `Scanning Files: a/b` from EXPLAIN ANALYZE (S7 §3).
fn files_scanned(c: &Connection, query: &str) -> String {
    let plan: String = c
        .query_row(&format!("EXPLAIN ANALYZE {query}"), [], |r| r.get(1))
        .expect("explain");
    let flat: String = plan
        .chars()
        .filter(|ch| !"│┌└┐┘─┬┴├┤".contains(*ch))
        .collect();
    let at = flat.find("Scanning Files: ").expect("Scanning Files line");
    flat[at + 16..]
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_owned()
}

fn counts(inserted: u64, updated: u64, deleted: u64, unchanged: u64) -> Counts {
    Counts {
        inserted,
        updated,
        deleted,
        unchanged,
    }
}

#[test]
fn full_refresh_follows_versions() {
    let conn = local();
    let (_l, c) = setup(&conn);
    let s = |v| {
        Stream::new("partitioned", "br.part", Kind::FullRefresh)
            .at(v)
            .pk("id")
    };
    let o = sync(&c, &conn, &s(2)).expect("v2");
    assert_eq!((o.version, o.staged), (2, 3000));
    assert!(o.adoption.create);
    assert_eq!(o.counts, counts(3000, 0, 0, 0));
    let o = sync(&c, &conn, &s(3)).expect("v3");
    assert_eq!(o.counts, counts(0, 0, 100, 2900));
    let o = sync(&c, &conn, &s(4)).expect("v4");
    assert_eq!(o.counts, counts(10, 0, 1000, 1900));
    assert_eq!(count(&c, "lake.br.part"), 1910);
    let snaps: i64 = one(
        &c,
        "SELECT max(snapshot_id) FROM ducklake_snapshots('lake')",
    );
    let o = sync(&c, &conn, &s(4)).expect("unchanged");
    assert!(!o.counts.changed());
    assert_eq!(
        one::<i64>(
            &c,
            "SELECT max(snapshot_id) FROM ducklake_snapshots('lake')"
        ),
        snaps,
        "an unchanged re-sync commits no snapshot"
    );

    let head = Stream::new("partitioned", "br.part_nopk", Kind::FullRefresh);
    let o = sync(&c, &conn, &head).expect("head");
    assert_eq!((o.version, o.counts), (4, counts(1910, 0, 0, 0)));
    let o = sync(&c, &conn, &head).expect("again");
    assert_eq!(o.counts, counts(1910, 0, 1910, 0));
    assert_eq!(
        types_of(&c, "br", "part_nopk"),
        ["id BIGINT", "dt VARCHAR", "region INTEGER", "v DOUBLE"],
        "partition columns keep their schema types"
    );
}

#[test]
fn version_pinning() {
    let conn = local();
    let (_l, c) = setup(&conn);
    let t = delta::discover(&c, &conn, "partitioned").expect("discover");
    assert_eq!(t.head, 4, "head read through the checkpoint listing");
    assert!(t.uri.ends_with("/partitioned"));
    for (v, n) in [(0, 1000), (1, 2000), (2, 3000), (3, 2900), (4, 1910)] {
        let p = delta::pin(&c, &t.uri, v, "atlatl_delta_0_0").expect("pin");
        assert_eq!(count(&c, &p.relation()), n, "version {v}");
    }
    let v1 = delta::pin(&c, &t.uri, 1, "atlatl_delta_1_0").expect("v1");
    let v4 = delta::pin(&c, &t.uri, 4, "atlatl_delta_2_0").expect("v4");
    assert_eq!(
        (count(&c, &v1.relation()), count(&c, &v4.relation())),
        (2000, 1910),
        "pins are independent"
    );
    assert_eq!(
        delta::discover(&c, &conn, "evolved").expect("evolved").head,
        2
    );
    assert_eq!(
        delta::discover(&c, &conn, "merged").expect("merged").head,
        1
    );
}

fn uk(column: &str) -> UpdateKey {
    UpdateKey {
        column: column.into(),
        lookback: None,
        inclusive: None,
        nulls: None,
    }
}

#[test]
fn incremental_update_key() {
    let conn = local();
    let (_l, c) = setup(&conn);
    let mut s = Stream::new("partitioned", "br.inc", Kind::Incremental)
        .at(0)
        .pk("id");
    s.update_key = Some(uk("id"));
    let o = sync(&c, &conn, &s).expect("first run");
    assert_eq!((o.staged, o.watermark.as_deref()), (1000, Some("999")));

    s.version = Some(2);
    s.stored = o.watermark;
    let o = sync(&c, &conn, &s).expect("second run");
    assert_eq!(o.staged, 2001, "inclusive bound re-reads the watermark row");
    assert_eq!(o.counts, counts(2000, 0, 0, 1));
    assert_eq!(o.watermark.as_deref(), Some("2999"));
    assert_eq!(count(&c, "lake.br.inc"), 3000);

    let t = delta::discover(&c, &conn, "partitioned").expect("discover");
    let p = delta::pin(&c, &t.uri, 2, "atlatl_delta_9_0").expect("pin");
    let f = p
        .filter(None, Some(&uk("id")), Some("999"))
        .expect("filter")
        .expect("some");
    assert_eq!(
        files_scanned(&c, &p.select(Some(&f))),
        "4/5",
        "file stats skip the even-id v0 file"
    );
    let f = p
        .filter(None, Some(&uk("id")), Some("2500"))
        .expect("filter")
        .expect("some");
    assert_eq!(files_scanned(&c, &p.select(Some(&f))), "1/5");
}

#[test]
fn incremental_window() {
    let conn = local();
    let (_l, c) = setup(&conn);
    let mut s = Stream::new("partitioned", "br.win", Kind::Incremental).pk("id");
    s.window = Some("region = 3 AND dt = '2024-01-04'");
    let o = sync(&c, &conn, &s).expect("window");
    assert_eq!((o.staged, o.counts), (10, counts(10, 0, 0, 0)));
    let t = delta::discover(&c, &conn, "partitioned").expect("discover");
    let p = delta::pin(&c, &t.uri, t.head, "atlatl_delta_9_0").expect("pin");
    let f = p.filter(s.window, None, None).expect("filter");
    assert_eq!(
        files_scanned(&c, &p.select(f.as_deref())),
        "1/5",
        "partition values skip files"
    );
}

#[test]
fn incremental_lookback_on_timestamp() {
    let conn = local();
    let (_l, c) = setup(&conn);
    let mut s = Stream::new("types", "br.lb", Kind::Incremental).pk("id");
    s.update_key = Some(UpdateKey {
        lookback: Some(Dur::parse("2h").expect("dur")),
        ..uk("ts_tz")
    });
    s.stored = Some("2024-03-10 13:00:00+00".into());
    let o = sync(&c, &conn, &s).expect("lookback");
    assert_eq!(o.staged, 1);
    assert_eq!(
        o.watermark.as_deref(),
        Some("2024-03-10 12:00:00.123456+00")
    );
    s.update_key = Some(uk("ts_tz"));
    let o = sync(&c, &conn, &s).expect("no lookback");
    assert_eq!(o.staged, 0);
}

#[test]
fn chunked_full_refresh_in_one_transaction() {
    let conn = local();
    let (_l, c) = setup(&conn);
    sync(
        &c,
        &conn,
        &Stream::new("partitioned", "br.ch", Kind::FullRefresh)
            .at(2)
            .pk("id"),
    )
    .expect("seed v2");
    let t = delta::discover(&c, &conn, "partitioned").expect("discover");
    let p = delta::pin(&c, &t.uri, 4, "atlatl_delta_5_0").expect("pin");
    let cfgs = [
        ChunkConfig {
            by: ChunkBy::Hash,
            size: None,
            columns: Some(vec!["id".into()]),
            buckets: Some(4),
            column: None,
            step: None,
        },
        ChunkConfig {
            by: ChunkBy::Range,
            size: None,
            columns: None,
            buckets: None,
            column: Some("id".into()),
            step: Some("500".into()),
        },
    ];
    for (i, cfg) in cfgs.iter().enumerate() {
        if i == 1 {
            sync(
                &c,
                &conn,
                &Stream::new("partitioned", "br.ch", Kind::FullRefresh)
                    .at(2)
                    .pk("id"),
            )
            .expect("reseed v2");
        }
        let chunks = p.chunks(&c, None, cfg).expect("chunks");
        assert!(chunks.len() >= 4, "{chunks:?}");
        let target = table_columns(&c, "lake", "br", "ch").expect("t");
        let a = drift::plan(
            &p.columns,
            target.as_deref(),
            Policy::from(None),
            &Declared::new(),
        )
        .expect("plan");
        let proj = drift::projection(&a);
        let pk = vec!["id".to_owned()];
        c.execute_batch("CREATE OR REPLACE TEMP TABLE seen (id BIGINT)")
            .expect("seen");
        c.execute_batch("BEGIN").expect("begin");
        let mut total = Counts::default();
        for ch in &chunks {
            let ap = Apply {
                kind: Kind::FullRefresh,
                target: "lake.br.ch",
                source: ch,
                columns: &proj,
                primary_key: &pk,
                null_safe: false,
            };
            total.add(ap.run_chunk(&c, "seen").expect("chunk"));
        }
        total.deleted = Apply {
            kind: Kind::FullRefresh,
            target: "lake.br.ch",
            source: &p.relation(),
            columns: &proj,
            primary_key: &pk,
            null_safe: false,
        }
        .finish_chunked(&c, "seen")
        .expect("finish");
        c.execute_batch("COMMIT").expect("commit");
        assert_eq!(total, counts(10, 0, 1100, 1900), "{cfg:?}");
        assert_eq!(count(&c, "lake.br.ch"), 1910);
    }
    let window = p.filter(Some("region = 3"), None, None).expect("f");
    let chunks = p.chunks(&c, window.as_deref(), &cfgs[0]).expect("filtered");
    let n: i64 = chunks.iter().map(|ch| count(&c, ch)).sum();
    assert_eq!(n, 10, "chunks cover the filtered pin exactly once");
}

#[test]
fn deletion_vectors_and_cdf_tables() {
    let conn = local();
    let (_l, c) = setup(&conn);
    let o = sync(
        &c,
        &conn,
        &Stream::new("dv_spark", "br.dv", Kind::FullRefresh),
    )
    .expect("dv");
    assert_eq!(o.staged, 8);
    assert_eq!(
        one::<String>(
            &c,
            "SELECT string_agg(value::VARCHAR, ',' ORDER BY value) FROM lake.br.dv"
        ),
        "1,2,3,4,5,6,7,8"
    );
    let o = sync(
        &c,
        &conn,
        &Stream::new("dv_spark", "br.dv", Kind::FullRefresh).at(0),
    )
    .expect("dv v0");
    assert_eq!(o.staged, 10);
    let o = sync(
        &c,
        &conn,
        &Stream::new("dv_attempt", "br.dva", Kind::FullRefresh),
    )
    .expect("dv attempt");
    assert_eq!(o.staged, 90);
    sync(
        &c,
        &conn,
        &Stream::new("merged", "br.merged", Kind::FullRefresh).pk("id"),
    )
    .expect("merged");
    assert_eq!(
        one::<String>(
            &c,
            "SELECT string_agg(concat_ws(':', id, p, v), ',' ORDER BY id) FROM lake.br.merged"
        ),
        "1:a:1,2:a:20,3:b:3,4:b:40",
        "_change_data files are not read"
    );
}

#[test]
fn schema_evolution_adds_columns() {
    let conn = local();
    let (_l, c) = setup(&conn);
    let s = |v| {
        Stream::new("evolved", "br.ev", Kind::Incremental)
            .at(v)
            .pk("id")
    };
    sync(&c, &conn, &s(0)).expect("v0");
    assert_eq!(types_of(&c, "br", "ev"), ["id BIGINT", "a VARCHAR"]);
    let mut strict = s(2);
    strict.policy = Policy {
        new_columns: NewColumns::Fail,
        type_changes: TypeChanges::Fail,
        missing_columns: MissingColumns::Warn,
    };
    let e = sync(&c, &conn, &strict).expect_err("new column");
    assert_eq!(
        (e.class, e.code.as_deref()),
        (ErrorClass::Data, Some("new_columns"))
    );
    let o = sync(&c, &conn, &s(2)).expect("v2");
    assert_eq!(o.adoption.add.len(), 1);
    assert_eq!(o.counts, counts(2, 0, 0, 2));
    assert_eq!(
        types_of(&c, "br", "ev"),
        ["id BIGINT", "a VARCHAR", "b INTEGER"]
    );
    assert_eq!(
        one::<String>(
            &c,
            "SELECT string_agg(concat_ws(':', id, a, coalesce(b::VARCHAR, 'NULL')), ',' ORDER BY id) FROM lake.br.ev"
        ),
        "1:x:NULL,2:y:NULL,3:z:30,4:w:40"
    );
}

const DELTA_TYPES: [&str; 18] = [
    "id BIGINT",
    "i32 INTEGER",
    "i16 SMALLINT",
    "i8 TINYINT",
    "f32 FLOAT",
    "f64 DOUBLE",
    "b BOOLEAN",
    "s VARCHAR",
    "bin BLOB",
    "d DATE",
    "dec_10_2 DECIMAL(10,2)",
    "dec_38_10 DECIMAL(38,10)",
    "ts_tz TIMESTAMP WITH TIME ZONE",
    "ts_ntz TIMESTAMP",
    "st STRUCT(a INTEGER, b VARCHAR)",
    "li BIGINT[]",
    "ls STRUCT(k VARCHAR, v DOUBLE)[]",
    "m MAP(VARCHAR, INTEGER)",
];

#[test]
fn new_tables_take_delta_types() {
    let conn = local();
    let (_l, c) = setup(&conn);
    let t = delta::discover(&c, &conn, "types").expect("discover");
    let p: Pinned = delta::pin(&c, &t.uri, t.head, "atlatl_delta_0_0").expect("pin");
    let pinned: Vec<String> = p
        .columns
        .iter()
        .map(|c| format!("{} {}", c.name, c.kind))
        .collect();
    assert_eq!(pinned, DELTA_TYPES);
    let o = sync(
        &c,
        &conn,
        &Stream::new("types", "br.types", Kind::FullRefresh),
    )
    .expect("types");
    assert!(o.adoption.create && o.adoption.columns.iter().all(|c| !c.cast));
    assert_eq!(types_of(&c, "br", "types"), DELTA_TYPES);
    assert_eq!(
        one::<String>(
            &c,
            "SELECT concat_ws('|', dec_10_2, dec_38_10, ts_tz, ts_ntz, st, li, ls, m, d, s, hex(bin)) FROM lake.br.types WHERE id = 1"
        ),
        "12345678.90|1234567890123456789012345678.0123456789|2024-03-10 12:00:00.123456+00|2024-03-10 12:00:00.123456|{'a': 1, 'b': x}|[1, 2]|[{'k': p, 'v': 1.0}]|{k1=1, k2=2}|2024-01-31|a|0001"
    );
    assert_eq!(
        count(
            &c,
            "lake.br.types WHERE ts_tz IS NULL AND st IS NULL AND i32 IS NULL"
        ),
        0
    );
    assert_eq!(
        count(&c, "lake.br.types WHERE ts_tz IS NULL AND st IS NULL"),
        1
    );
}

#[test]
fn adopts_narrower_legacy_tables() {
    let conn = local();
    let (_l, c) = setup(&conn);
    c.execute_batch(
        "CREATE SCHEMA lake.br;
         CREATE TABLE lake.br.legacy (id INTEGER, dec_10_2 DECIMAL(24,6), d VARCHAR, ts_tz TIMESTAMP WITH TIME ZONE, s VARCHAR, _legacy_loaded_at INTEGER);
         INSERT INTO lake.br.legacy VALUES (99, 1, 'x', NULL, 'old', 1);",
    )
    .expect("legacy table");
    let mut s = Stream::new("types", "br.legacy", Kind::FullRefresh).pk("id");
    s.policy = Policy {
        new_columns: NewColumns::Ignore,
        type_changes: TypeChanges::Fail,
        missing_columns: MissingColumns::Warn,
    };
    let o = sync(&c, &conn, &s).expect("adopt");
    assert_eq!(o.counts, counts(3, 0, 1, 0));
    assert_eq!(o.adoption.missing, ["_legacy_loaded_at"]);
    assert_eq!(o.adoption.ignored.len(), 13);
    assert_eq!(
        types_of(&c, "br", "legacy"),
        [
            "id INTEGER",
            "dec_10_2 DECIMAL(24,6)",
            "d VARCHAR",
            "ts_tz TIMESTAMP WITH TIME ZONE",
            "s VARCHAR",
            "_legacy_loaded_at INTEGER"
        ],
        "the existing schema is authoritative"
    );
    assert_eq!(
        one::<String>(
            &c,
            "SELECT string_agg(concat_ws('|', id, dec_10_2, d, coalesce(_legacy_loaded_at::VARCHAR, 'NULL')), ',' ORDER BY id) FROM lake.br.legacy"
        ),
        "1|12345678.900000|2024-01-31|NULL,2|-0.010000|1970-01-01|NULL,3|NULL"
    );

    c.execute_batch(
        "CREATE TABLE lake.br.narrow (id INTEGER, dec_38_10 DECIMAL(24,6));
         INSERT INTO lake.br.narrow VALUES (7, 1);",
    )
    .expect("narrow");
    s.target = "br.narrow";
    let e = sync(&c, &conn, &s).expect_err("overflow");
    assert_eq!(
        (e.class, e.code.as_deref()),
        (ErrorClass::Data, Some("cast_failed"))
    );
    assert_eq!(e.samples.len(), 1);
    assert_eq!(count(&c, "lake.br.narrow"), 1, "target untouched");
    s.set_null = true;
    sync(&c, &conn, &s).expect("set_null");
    assert_eq!(
        count(&c, "lake.br.narrow WHERE dec_38_10 IS NULL"),
        2,
        "the overflow and the source NULL"
    );

    s.set_null = false;
    s.target = "br.widen";
    s.policy.type_changes = TypeChanges::Widen;
    c.execute_batch("CREATE TABLE lake.br.widen (id INTEGER, i16 TINYINT)")
        .expect("widen");
    let o = sync(&c, &conn, &s).expect("widen");
    assert_eq!(o.adoption.widen.len(), 2);
    assert_eq!(types_of(&c, "br", "widen"), ["id BIGINT", "i16 SMALLINT"]);
}

#[test]
fn attaches_are_read_only() {
    let conn = local();
    let (_l, c) = setup(&conn);
    let t = delta::discover(&c, &conn, "partitioned").expect("discover");
    let p = delta::pin(&c, &t.uri, t.head, "atlatl_delta_0_0").expect("pin");
    for w in [
        format!("INSERT INTO {} VALUES (1, 'x', 1, 1.0)", p.relation()),
        format!("CREATE TABLE {}.x (a INTEGER)", p.relation()),
        format!("DELETE FROM {}", p.relation()),
    ] {
        let e = c.execute_batch(&w).expect_err("write refused");
        assert!(
            e.to_string().contains("read-only") || e.to_string().contains("base table"),
            "{w}: {e}"
        );
    }
    assert_eq!(
        count(&c, &p.relation()),
        1910,
        "still usable after refusals"
    );
    assert!(one::<bool>(
        &c,
        "SELECT readonly FROM duckdb_databases() WHERE database_name = 'atlatl_delta_0_0'"
    ));
    let e = delta::pin(&c, &t.uri, 1, "lake").expect_err("alias clash");
    assert_eq!(e.class, ErrorClass::Config);
    assert_eq!(
        one::<String>(
            &c,
            "SELECT type FROM duckdb_databases() WHERE database_name = 'lake'"
        ),
        "ducklake",
        "the lake stays attached"
    );
    p.detach(&c).expect("detach");
    assert!(c.prepare(&p.select(None)).is_err());
}

#[test]
fn missing_tables_and_versions() {
    let conn = local();
    let (_l, c) = setup(&conn);
    let e = delta::discover(&c, &conn, "nope").expect_err("missing");
    assert_eq!(
        (e.class, e.code.as_deref()),
        (ErrorClass::Config, Some("delta_not_found"))
    );
    let t = delta::discover(&c, &conn, "partitioned").expect("discover");
    let e = delta::pin(&c, &t.uri, 9, "atlatl_delta_0_0").expect_err("version");
    assert_eq!(
        (e.class, e.code.as_deref()),
        (ErrorClass::Config, Some("delta_version")),
        "{e}"
    );
    assert_eq!(
        one::<i64>(
            &c,
            "SELECT count(*) FROM duckdb_databases() WHERE database_name = 'atlatl_delta_0_0'"
        ),
        0,
        "a failed pin is detached"
    );
    let e = delta::pin(
        &c,
        &format!("{}/nope", fixtures().display()),
        0,
        "atlatl_delta_0_0",
    )
    .expect_err("missing");
    assert_eq!(e.code.as_deref(), Some("delta_not_found"));
}

fn files_under(dir: &Path, out: &mut Vec<PathBuf>) {
    for e in std::fs::read_dir(dir).expect("read_dir") {
        let p = e.expect("entry").path();
        if p.is_dir() {
            files_under(&p, out);
        } else {
            out.push(p);
        }
    }
}

#[test]
fn s3_root_on_rustfs() {
    let s3 = atlatl_testkit::rustfs();
    let bucket = s3.fresh_bucket().expect("bucket");
    let conn = DeltaConn {
        root: format!("s3://{bucket}/br/"),
        storage: Some(StorageConfig {
            kind: StorageType::S3,
            provider: Some(S3Provider::Config),
            region: Some(s3.region.clone()),
            endpoint: Some(format!("http://{}", s3.endpoint)),
            url_style: Some(UrlStyle::Path),
            use_ssl: None,
            key_id: Some(SecretValue::Literal(s3.key_id.clone())),
            secret: Some(SecretValue::Literal(s3.secret.clone())),
            session_token: None,
            role_arn: None,
        }),
    };
    let (_l, c) = setup(&conn);
    c.execute_batch(
        &delta::secret_sql("atlatl_delta_br", &conn)
            .expect("secret")
            .expect("s3"),
    )
    .expect("create secret");
    let root = fixtures();
    for table in ["partitioned", "dv_spark"] {
        let mut files = Vec::new();
        files_under(&root.join(table), &mut files);
        for f in files {
            let rel = f.strip_prefix(&root).expect("rel").display().to_string();
            c.execute_batch(&format!(
                "COPY (SELECT content FROM read_blob('{}')) TO 's3://{bucket}/br/{rel}' (FORMAT blob)",
                f.display()
            ))
            .unwrap_or_else(|e| panic!("upload {rel}: {e}"));
        }
    }
    let t = delta::discover(&c, &conn, "partitioned").expect("discover");
    assert_eq!(
        (t.uri.as_str(), t.head),
        (format!("s3://{bucket}/br/partitioned").as_str(), 4)
    );
    let p = delta::pin(&c, &t.uri, 1, "atlatl_delta_0_0").expect("pin");
    assert_eq!(count(&c, &p.relation()), 2000);
    let f = p
        .filter(Some("region = 2 AND dt = '2024-01-03'"), None, None)
        .expect("f");
    assert_eq!(files_scanned(&c, &p.select(f.as_deref())), "1/4");
    let o = sync(
        &c,
        &conn,
        &Stream::new("partitioned", "br.s3part", Kind::FullRefresh).pk("id"),
    )
    .expect("sync");
    assert_eq!((o.version, o.counts.inserted), (4, 1910));
    let o = sync(
        &c,
        &conn,
        &Stream::new("dv_spark", "br.s3dv", Kind::FullRefresh),
    )
    .expect("dv");
    assert_eq!(o.staged, 8);
    let e = delta::discover(&c, &conn, "nope").expect_err("missing");
    assert_eq!(e.code.as_deref(), Some("delta_not_found"));
}
