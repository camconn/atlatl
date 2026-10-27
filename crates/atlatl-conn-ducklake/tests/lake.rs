use std::time::{Duration, SystemTime};

use atlatl_conn_ducklake::{
    DATA_PATH_MISMATCH, Lake, VERSION_MISMATCH, attach, attach_empty, attach_existing, lock_manager,
};
use atlatl_core::config::model::{DuckLakeConn, EngineConfig};
use atlatl_core::config::types::{Auto, ByteSize};
use atlatl_core::lock::LockKey;
use atlatl_core::{AtlatlError, ErrorClass};
use atlatl_engine::{
    Budget, BudgetParams, DbResultExt, Detected, Engine, EngineOptions, StreamOptions,
};
use atlatl_testkit::{LocalLake, Postgres, pg12, pg16, rustfs, unique};
use serde_json::json;

struct TestEngine {
    engine: Engine,
    _spill: tempfile::TempDir,
}

fn engine() -> TestEngine {
    let spill = tempfile::tempdir().expect("tempdir");
    let cfg = EngineConfig {
        memory: Some(Auto::Value(ByteSize(2 << 30))),
        threads: Some(Auto::Value(2)),
        temp_directory: Some(spill.path().to_string_lossy().into_owned()),
        ..Default::default()
    };
    let b =
        Budget::compute(&cfg, None, &Detected::detect(), &BudgetParams::default()).expect("budget");
    let mut opts = EngineOptions::new(b);
    opts.extension_dir = Some(atlatl_testkit::extension_dir());
    TestEngine {
        engine: Engine::open(opts).expect("engine"),
        _spill: spill,
    }
}

fn conn(v: serde_json::Value) -> DuckLakeConn {
    serde_json::from_value(v).expect("ducklake connection")
}

fn file_conn(lake: &LocalLake, data_path: &str) -> DuckLakeConn {
    conn(json!({
        "catalog": {"type": "duckdb", "path": lake.dir.path().join("meta.ducklake")},
        "data_path": data_path,
        "storage": {"type": "local"},
    }))
}

async fn exec(e: &Engine, sql: &str) -> atlatl_core::error::Result<()> {
    let sql = sql.to_owned();
    e.with_conn(move |c| c.execute_batch(&sql).db()).await
}

async fn count(e: &Engine, sql: &str) -> i64 {
    let sql = sql.to_owned();
    e.with_conn(move |c| c.query_row(&sql, [], |r| r.get(0)).db())
        .await
        .expect("count")
}

/// Two commits on `lake.s.t`, then every snapshot helper.
async fn exercise(e: &Engine, lake: &Lake) {
    exec(
        e,
        "CREATE SCHEMA lake.s; CREATE TABLE lake.s.t AS SELECT i FROM range(100) r(i)",
    )
    .await
    .expect("write");
    exec(e, "INSERT INTO lake.s.t SELECT 1")
        .await
        .expect("insert");
    assert_eq!(count(e, "SELECT count(*) FROM lake.s.t").await, 101);

    let l = lake.clone();
    let (cur, oldest, exists, missing, t_cur, t_missing, tid, no_tid, no_schema) = e
        .with_conn(move |c| {
            let cur = l.current_snapshot(c)?;
            Ok((
                cur,
                l.oldest_snapshot(c)?,
                l.snapshot_exists(c, cur)?,
                l.snapshot_exists(c, cur + 100)?,
                l.snapshot_time(c, cur)?,
                l.snapshot_time(c, cur + 100)?,
                l.table_id(c, "s", "t")?,
                l.table_id(c, "s", "nope")?,
                l.table_id(c, "nope", "t")?,
            ))
        })
        .await
        .expect("helpers");
    assert!(cur >= 3, "{cur}");
    assert_eq!(oldest, Some(0));
    assert!(exists && !missing);
    let t = t_cur.expect("time");
    let age = SystemTime::now().duration_since(t).expect("in the past");
    assert!(age < Duration::from_secs(600), "{age:?}");
    assert!(t_missing.is_none());
    let tid = tid.expect("table id");
    assert!(no_tid.is_none() && no_schema.is_none());

    exec(e, "CREATE OR REPLACE TABLE lake.s.t AS SELECT 1 AS i")
        .await
        .expect("recreate");
    let l = lake.clone();
    let (new_tid, new_cur) = e
        .with_conn(move |c| Ok((l.table_id(c, "s", "t")?, l.current_snapshot(c)?)))
        .await
        .expect("after recreate");
    assert_ne!(new_tid, Some(tid));
    assert_eq!(new_cur, cur + 1);

    // ATTACH is instance-wide: the streaming reader's own connection sees the lake.
    let batches = e
        .stream("SELECT i FROM lake.s.t", StreamOptions::default())
        .await
        .expect("stream")
        .collect()
        .await
        .expect("collect");
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
async fn file_catalog() {
    let local = LocalLake::file().expect("lake");
    let e = engine();
    let lake = attach(&e.engine, "lake", &file_conn(&local, &local.data_path()))
        .await
        .expect("attach");
    assert_eq!(lake.alias, "lake");
    exercise(&e.engine, &lake).await;
    let l = lake.clone();
    let stored = e
        .engine
        .with_conn(move |c| l.stored_data_path(c))
        .await
        .expect("stored");
    assert_eq!(stored.as_deref(), Some(local.data_path().as_str()));

    let locks = lock_manager(&file_conn(&local, &local.data_path())).expect("locks");
    let g = locks
        .acquire(&[LockKey::group("g")], Duration::from_secs(1))
        .await
        .expect("local lock");
    assert_eq!(g.keys().len(), 1);
}

#[tokio::test]
async fn attach_existing_never_creates_a_catalog() {
    let local = LocalLake::file().expect("lake");
    let meta = local.dir.path().join("meta.ducklake");
    let c = file_conn(&local, &local.data_path());
    let e = engine();
    assert!(
        attach_existing(&e.engine, "lake", &c)
            .await
            .expect("missing")
            .is_none()
    );
    assert!(!meta.exists());

    let mut nodir = c.clone();
    nodir.catalog.path = Some(format!("{}/nope/meta.ducklake", local.dir.path().display()));
    let err = attach_existing(&e.engine, "lake", &nodir)
        .await
        .expect_err("missing dir");
    assert_eq!(err.class, ErrorClass::Config, "{err}");

    // A DuckDB file without DuckLake metadata is missing too, and stays as it was.
    duckdb::Connection::open(&meta)
        .and_then(|d| d.execute_batch("CREATE TABLE other AS SELECT 1 AS x"))
        .expect("plain duckdb file");
    let size = std::fs::metadata(&meta).expect("meta").len();
    assert!(
        attach_existing(&e.engine, "lake", &c)
            .await
            .expect("no metadata")
            .is_none()
    );
    assert_eq!(std::fs::metadata(&meta).expect("meta").len(), size);
    std::fs::remove_file(&meta).expect("rm");

    // An existing catalog attaches read-only.
    local
        .open("seed")
        .and_then(|d| d.execute_batch("CREATE TABLE seed.t AS SELECT 1 AS x"))
        .expect("seed");
    let e = engine();
    let lake = attach_existing(&e.engine, "lake", &c)
        .await
        .expect("attach")
        .expect("exists");
    assert_eq!(lake.alias, "lake");
    assert_eq!(count(&e.engine, "SELECT count(*) FROM lake.t").await, 1);
    let err = exec(&e.engine, "INSERT INTO lake.t VALUES (2)")
        .await
        .expect_err("read-only");
    assert!(err.message.contains("read-only"), "{err}");

    // The stand-in: an empty lake under the alias, nothing on disk.
    let e = engine();
    let dir = tempfile::tempdir().expect("dir");
    let data = format!("{}/stand-in/", dir.path().display());
    let lake = attach_empty(&e.engine, "lake", &c, &data)
        .await
        .expect("stand-in");
    assert_eq!(lake.alias, "lake");
    assert_eq!(
        count(
            &e.engine,
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake'"
        )
        .await,
        0
    );
    let l = lake.clone();
    e.engine
        .with_conn(move |c| l.current_snapshot(c))
        .await
        .expect("snapshot");
    assert_eq!(std::fs::read_dir(dir.path()).expect("dir").count(), 0);
}

#[tokio::test]
async fn data_inlining_row_limit_is_applied() {
    let local = LocalLake::file().expect("lake");
    let e = engine();
    let mut c = file_conn(&local, &local.data_path());
    c.data_inlining_row_limit = Some(0);
    attach(&e.engine, "lake", &c).await.expect("attach");
    exec(&e.engine, "CREATE TABLE lake.main.t AS SELECT 1 AS a")
        .await
        .expect("write");
    let files = count(
        &e.engine,
        "SELECT count(*) FROM ducklake_list_files('lake', 't')",
    )
    .await;
    assert_eq!(files, 1, "limit 0 writes even one row to parquet");
}

fn assert_mismatch(err: &AtlatlError, configured: &str, stored: &str) {
    assert_eq!(err.class, ErrorClass::Config, "{err}");
    assert_eq!(err.code.as_deref(), Some(DATA_PATH_MISMATCH), "{err}");
    assert!(
        err.message.contains(&format!("{configured:?}"))
            && err.message.contains(&format!("{stored:?}")),
        "{err}"
    );
}

#[tokio::test]
async fn data_path_mismatch() {
    let local = LocalLake::file().expect("lake");
    let dp = local.data_path();
    {
        let e = engine();
        attach(&e.engine, "lake", &file_conn(&local, &dp))
            .await
            .expect("create");
    }

    let e = engine();
    // DuckLake's own check.
    let other = format!("{dp}elsewhere/");
    let err = attach(&e.engine, "lake", &file_conn(&local, &other))
        .await
        .expect_err("mismatch");
    assert_mismatch(&err, &other, &dp);

    // DuckLake normalises trailing slashes; atlatl compares byte for byte.
    for configured in [dp.trim_end_matches('/').to_owned(), format!("{dp}/")] {
        let err = attach(&e.engine, "lake", &file_conn(&local, &configured))
            .await
            .expect_err("trailing slash differs");
        assert_mismatch(&err, &configured, &dp);
    }

    // Failed checks detach, so a corrected config attaches on the same engine.
    attach(&e.engine, "lake", &file_conn(&local, &dp))
        .await
        .expect("exact path");
}

#[tokio::test]
async fn missing_local_directories() {
    let local = LocalLake::file().expect("lake");
    let e = engine();
    let mut c = file_conn(&local, &local.data_path());
    let missing = local.dir.path().join("missing/meta.ducklake");
    c.catalog.path = Some(missing.to_string_lossy().into_owned());
    let err = attach(&e.engine, "lake", &c)
        .await
        .expect_err("missing catalog dir");
    assert_eq!(err.class, ErrorClass::Config, "{err}");
    assert!(
        err.message
            .contains(&format!("{:?}", missing.to_string_lossy())),
        "{err}"
    );

    // DuckLake creates a missing local data directory on the first write.
    let data_path = format!("{}/new/data/", local.dir.path().display());
    attach(&e.engine, "lake", &file_conn(&local, &data_path))
        .await
        .expect("attach");
    exec(
        &e.engine,
        "CREATE TABLE lake.main.t AS SELECT i FROM range(100000) r(i)",
    )
    .await
    .expect("write");
}

#[tokio::test]
async fn format_version_mismatch() {
    for v in ["0.3", "9.9"] {
        let local = LocalLake::file().expect("lake");
        let dp = local.data_path();
        {
            let e = engine();
            attach(&e.engine, "lake", &file_conn(&local, &dp))
                .await
                .expect("create");
            exec(
                &e.engine,
                &format!("UPDATE __ducklake_metadata_lake.ducklake_metadata SET value = '{v}' WHERE key = 'version'"),
            )
            .await
            .expect("tamper");
        }
        let e = engine();
        let err = attach(&e.engine, "lake", &file_conn(&local, &dp))
            .await
            .expect_err("other format");
        assert_eq!(
            (err.class, err.code.as_deref()),
            (ErrorClass::Config, Some(VERSION_MISMATCH)),
            "{v}: {err}"
        );
        assert!(err.message.contains(v), "{err}");
    }
}

/// A fresh database owned by a role whose name and password need libpq quoting.
async fn awkward_role(pg: &Postgres) -> (String, String, String) {
    let db = pg.fresh_db().expect("db");
    let user = format!("{} x", unique("atl"));
    let password = format!("p w'x\\y\"{}", unique("z"));
    let (client, connection) = tokio_postgres::connect(&pg.dsn("postgres"), tokio_postgres::NoTls)
        .await
        .expect("connect");
    let h = tokio::spawn(connection);
    client
        .batch_execute(&format!(
            "CREATE ROLE \"{user}\" LOGIN PASSWORD '{}'; ALTER DATABASE {db} OWNER TO \"{user}\"",
            password.replace('\'', "''")
        ))
        .await
        .expect("role");
    drop(client);
    let _ = h.await;
    (db, user, password)
}

fn pg_conn(pg: &Postgres, db: &str, user: &str, password: &str, data_path: &str) -> DuckLakeConn {
    conn(json!({
        "alias": "lake",
        "catalog": {"type": "postgres", "host": pg.host, "port": pg.port, "database": db,
                    "user": user, "password": password, "sslmode": "disable"},
        "data_path": data_path,
        "storage": {"type": "local"},
    }))
}

async fn postgres_catalog(pg: &Postgres) {
    let (db, user, password) = awkward_role(pg).await;
    let data = tempfile::tempdir().expect("data");
    let dp = format!("{}/", data.path().display());
    let c = pg_conn(pg, &db, &user, &password, &dp);

    let e = engine();
    let lake = attach(&e.engine, "ignored", &c)
        .await
        .expect("attach with quoted DSN");
    exercise(&e.engine, &lake).await;

    let err = attach(
        &engine().engine,
        "x",
        &pg_conn(pg, &db, &user, &password, "/elsewhere/"),
    )
    .await
    .expect_err("mismatch");
    assert_mismatch(&err, "/elsewhere/", &dp);

    let locks = lock_manager(&c).expect("pg locks");
    let key = LockKey::group(unique("g"));
    let g = locks
        .acquire(std::slice::from_ref(&key), Duration::from_secs(5))
        .await
        .expect("advisory lock");
    let err = locks
        .acquire(std::slice::from_ref(&key), Duration::from_millis(300))
        .await
        .expect_err("held elsewhere");
    assert_eq!(err.class, ErrorClass::Conflict);
    g.release().await.expect("release");

    // Wrong password: auth, and neither password appears anywhere in the error.
    let wrong = format!("wr'ong {}", unique("pw"));
    let err = attach(&engine().engine, "x", &pg_conn(pg, &db, &user, &wrong, &dp))
        .await
        .expect_err("bad password");
    let shown = format!("{err} {err:?} {}", err.message);
    assert_eq!(err.class, ErrorClass::Auth, "{shown}");
    assert!(
        !shown.contains("wr'ong") && !shown.contains("wr\\'ong"),
        "{shown}"
    );
    assert!(!shown.contains(&password), "{shown}");
}

#[tokio::test]
async fn postgres16_catalog() {
    postgres_catalog(pg16()).await;
}

#[tokio::test]
async fn postgres12_catalog() {
    postgres_catalog(pg12()).await;
}

#[tokio::test]
async fn s3_data_path_on_rustfs() {
    let s3 = rustfs();
    let bucket = s3.fresh_bucket().expect("bucket");
    let local = LocalLake::file().expect("catalog dir");
    let dp = format!("s3://{bucket}/lake/");
    let c = conn(json!({
        "catalog": {"type": "duckdb", "path": local.dir.path().join("meta.ducklake")},
        "data_path": dp,
        "storage": {"type": "s3", "provider": "config", "region": s3.region,
                    "endpoint": format!("http://{}", s3.endpoint), "url_style": "path",
                    "key_id": s3.key_id, "secret": s3.secret},
        "data_inlining_row_limit": 0,
    }));
    let e = engine();
    let lake = attach(&e.engine, "lake", &c).await.expect("attach");
    exercise(&e.engine, &lake).await;
    let files = count(
        &e.engine,
        &format!("SELECT count(*) FROM glob('s3://{bucket}/lake/**')"),
    )
    .await;
    assert!(files >= 2, "parquet files in the bucket: {files}");
    let scoped = |path: String| {
        e.engine.with_conn(move |c| {
            c.query_row("SELECT name FROM which_secret(?, 's3')", [path], |r| {
                r.get::<_, String>(0)
            })
            .map(Some)
            .or_else(|e| match e {
                duckdb::Error::QueryReturnedNoRows => Ok(None),
                e => Err(e),
            })
            .db()
        })
    };
    assert_eq!(
        scoped(format!("s3://{bucket}/lake/x.parquet"))
            .await
            .expect("which_secret"),
        Some("atlatl_lake_lake".to_owned())
    );
    assert_eq!(
        scoped(format!("s3://{bucket}-other/x"))
            .await
            .expect("which_secret"),
        None
    );
}
