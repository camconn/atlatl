use atlatl_testkit::{LocalLake, TempProject, pg12, pg16, rustfs};

fn query_one(dsn: &str, sql: &str) -> String {
    let (dsn, sql) = (dsn.to_owned(), sql.to_owned());
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt")
        .block_on(async move {
            let (c, conn) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .expect("connect");
            tokio::spawn(conn);
            c.query_one(&sql, &[])
                .await
                .expect("query")
                .get::<_, String>(0)
        })
}

#[test]
fn postgres_12_and_16() {
    for pg in [pg12(), pg16()] {
        let db = pg.fresh_db().expect("db");
        let v = query_one(&pg.dsn(&db), "SHOW server_version");
        assert!(v.starts_with(&pg.version.to_string()), "{v}");
        assert_eq!(
            query_one(&pg.dsn(&db), "SELECT current_database()::text"),
            db
        );
    }
}

#[test]
fn file_lake_round_trip() {
    let lake = LocalLake::file().expect("lake");
    let c = lake.open("lake").expect("open");
    c.execute_batch("CREATE TABLE lake.t AS SELECT range AS i FROM range(5)")
        .expect("ctas");
    let n: i64 = c
        .query_row("SELECT count(*) FROM lake.t", [], |r| r.get(0))
        .expect("count");
    assert_eq!(n, 5);
    let stored: String = c
        .query_row(
            "SELECT value FROM lake.ducklake_options() WHERE option_name = 'data_path'",
            [],
            |r| r.get(0),
        )
        .unwrap_or_default();
    assert!(stored.is_empty() || stored == lake.data_path(), "{stored}");
}

#[test]
fn postgres_lake_round_trip() {
    let lake = LocalLake::on_postgres(pg16()).expect("lake");
    let c = lake.open("lake").expect("open");
    c.execute_batch("CREATE TABLE lake.t AS SELECT range AS i FROM range(3)")
        .expect("ctas");
    let n: i64 = c
        .query_row("SELECT count(*) FROM lake.t", [], |r| r.get(0))
        .expect("count");
    assert_eq!(n, 3);
}

#[test]
fn rustfs_bucket_and_duckdb_write() {
    let s3 = rustfs();
    let bucket = s3.fresh_bucket().expect("bucket");
    let c = atlatl_testkit::lake::open_engine().expect("engine");
    c.execute_batch("LOAD httpfs").expect("httpfs");
    c.execute_batch(&s3.duckdb_secret("t", &bucket))
        .expect("secret");
    c.execute_batch(&format!(
        "COPY (SELECT 42 AS x) TO 's3://{bucket}/x.parquet'"
    ))
    .expect("copy");
    let x: i32 = c
        .query_row(
            &format!("SELECT x FROM 's3://{bucket}/x.parquet'"),
            [],
            |r| r.get(0),
        )
        .expect("read");
    assert_eq!(x, 42);
}

#[test]
fn temp_project_loads() {
    let lake = LocalLake::file().expect("lake");
    let p = TempProject::new()
        .and_then(|p| p.project(&lake.connection_yaml("lake", "catalog"), ""))
        .and_then(|p| {
            p.replication("copy", "replications:\n  copy:\n    source: lake\n    target: lake\n    streams: [{id: a/b, mode: full-refresh, from: {table: s.t}, object: out.t}]\n")
        })
        .expect("project");
    let (proj, d) = atlatl_core::config::load(p.path());
    assert!(!d.has_errors(), "{d:?}");
    assert_eq!(proj.replications.len(), 1);
    let mut d = atlatl_core::config::Diagnostics::default();
    atlatl_core::plan::check_project(&proj, &atlatl_core::template::Templates::new(), &mut d);
    assert!(!d.has_errors(), "{d:?}");
}
