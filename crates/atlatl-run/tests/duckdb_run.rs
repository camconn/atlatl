//! Plain DuckDB files as ingest targets (06 §10), and ClickHouse sources with `bind` lists read
//! from that file (06 §11, 04 §12).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use atlatl_core::obs::report::{ReplicationStatus, RowCounts, RunReport};
use atlatl_core::plan::PlanRequest;
use atlatl_core::template::{EnvSource, MapEnv};
use atlatl_run::{RunArgs, run};
use atlatl_testkit::TempProject;
use tokio_util::sync::CancellationToken;

struct Env {
    db: PathBuf,
    project: TempProject,
}

fn delta_fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../atlatl-conn-delta/tests/fixtures")
}

/// A project writing `db.duckdb` (which exists, as a pulled-down file would) with its state in it.
fn setup(extra_conns: &str, reps: &str) -> Env {
    let project = TempProject::new().expect("project");
    let db = project.path().join("db.duckdb");
    duckdb::Connection::open(&db).expect("create db");
    let conns = format!(
        "  db: {{type: duckdb, path: \"{}\"}}\n  files: {{type: files}}\n{extra_conns}",
        db.display()
    );
    let project = project
        .project(&conns, "state: {lake: db}\n")
        .and_then(|p| p.replication("reps", reps))
        .expect("project");
    Env { db, project }
}

async fn go(e: &Env, names: &[&str], dry: bool) -> RunReport {
    let (p, d) = atlatl_core::config::load(e.project.path());
    assert!(!d.has_errors(), "{d:?}");
    let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::default());
    let args = RunArgs {
        plan: PlanRequest {
            replications: names.iter().map(|r| (*r).to_owned()).collect(),
            ..PlanRequest::all()
        },
        extension_dir: Some(atlatl_testkit::extension_dir()),
        dry_run: dry.then(Default::default),
        ..Default::default()
    };
    run(
        Arc::new(p),
        args,
        vars,
        CancellationToken::new(),
        || None,
        |_, _| {},
    )
    .await
    .expect("run")
}

fn ok(r: &RunReport) {
    for x in &r.replications {
        assert!(x.status.is_success(), "{}: {:?}", x.replication, x.error);
    }
}

fn rows(r: &RunReport, rep: &str) -> RowCounts {
    let x = r
        .replications
        .iter()
        .find(|x| x.replication == rep)
        .expect("rep");
    x.streams
        .iter()
        .flat_map(|s| &s.members)
        .fold(RowCounts::default(), |mut a, m| {
            a.add(&m.rows);
            a
        })
}

fn status(r: &RunReport, rep: &str) -> ReplicationStatus {
    r.replications
        .iter()
        .find(|x| x.replication == rep)
        .expect("rep")
        .status
}

/// Opens the file as dbt would once atlatl is done.
fn q<T: duckdb::types::FromSql>(e: &Env, sql: &str) -> T {
    duckdb::Connection::open(&e.db)
        .expect("open")
        .query_row(sql, [], |r| r.get(0))
        .expect(sql)
}

fn exec(e: &Env, sql: &str) {
    duckdb::Connection::open(&e.db)
        .expect("open")
        .execute_batch(sql)
        .expect(sql);
}

const FILES: &str = r#"replications:
  prices:
    source: files
    target: db
    streams:
      - id: f/prices
        mode: full-refresh
        from: {files: {url: "in", glob: "prices_*.csv"}}
        object: br.prices
        primary_key: [ticker]
        columns: {px: {type: DOUBLE}}
  days:
    source: files
    target: db
    streams:
      - id: f/days
        mode: incremental
        from: {files: {url: "in", glob: "days.csv"}}
        object: br.days
        primary_key: [day]
        update_key: {column: day}
"#;

fn files_env() -> Env {
    let e = setup("", FILES);
    let p = e.project.path();
    std::fs::create_dir_all(p.join("in")).expect("fs");
    std::fs::write(p.join("in/prices_a.csv"), "ticker,px\nAAPL,1.5\nMSFT,2\n").expect("fs");
    std::fs::write(p.join("in/days.csv"), "day,v\n2026-10-01,1\n2026-10-02,2\n").expect("fs");
    e
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn files_land_in_a_plain_duckdb_file_with_its_state() {
    let e = files_env();
    let r = go(&e, &["prices", "days"], false).await;
    ok(&r);
    assert_eq!(rows(&r, "prices").inserted, 2);
    assert_eq!(rows(&r, "days").inserted, 2);
    assert_eq!(q::<f64>(&e, "SELECT sum(px) FROM br.prices"), 3.5);
    assert!(
        r.replications
            .iter()
            .all(|x| x.streams[0].members[0].lake_snapshot.is_none()),
        "a plain file has no snapshots"
    );
    let cursor: String = q(
        &e,
        "SELECT value::VARCHAR FROM _atlatl.stream_state WHERE stream_id = 'f/days'",
    );
    assert!(cursor.contains("2026-10-02"), "{cursor}");

    let r = go(&e, &["prices", "days"], false).await;
    ok(&r);
    assert_eq!(status(&r, "prices"), ReplicationStatus::UpToDate);
    assert_eq!(status(&r, "days"), ReplicationStatus::UpToDate);

    let p = e.project.path();
    std::fs::write(p.join("in/prices_a.csv"), "ticker,px\nAAPL,4\n").expect("fs");
    std::fs::write(
        p.join("in/days.csv"),
        "day,v\n2026-10-01,9\n2026-10-02,2\n2026-10-03,3\n",
    )
    .expect("clickhouse");
    let r = go(&e, &["prices", "days"], false).await;
    ok(&r);
    let n = rows(&r, "prices");
    assert_eq!((n.updated, n.deleted), (1, 1));
    // The cursor skips the 10-01 change; only 10-02 (inclusive) onwards is read.
    let n = rows(&r, "days");
    assert_eq!((n.inserted, n.unchanged), (1, 1));
    assert_eq!(q::<i64>(&e, "SELECT count(*) FROM br.days"), 3);
    assert_eq!(
        q::<String>(&e, "SELECT v FROM br.days WHERE day = '2026-10-01'"),
        "1"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dry_run_leaves_the_file_alone() {
    let e = files_env();
    let r = go(&e, &["prices"], true).await;
    ok(&r);
    let tables: i64 = q(
        &e,
        "SELECT count(*) FROM information_schema.tables WHERE table_schema IN ('br', '_atlatl')",
    );
    assert_eq!(tables, 0);
    let diff = r.replications[0].streams[0].members[0]
        .diff
        .as_ref()
        .expect("diff");
    assert_eq!(diff.inserts, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_missing_file_is_a_config_error() {
    let e = files_env();
    std::fs::remove_file(&e.db).expect("fs");
    let (p, d) = atlatl_core::config::load(e.project.path());
    assert!(!d.has_errors(), "{d:?}");
    let err = run(
        Arc::new(p),
        RunArgs {
            extension_dir: Some(atlatl_testkit::extension_dir()),
            ..Default::default()
        },
        Arc::new(MapEnv::default()),
        CancellationToken::new(),
        || None,
        |_, _| {},
    )
    .await
    .expect_err("no file");
    assert_eq!(err.class, atlatl_core::ErrorClass::Config, "{err:?}");
    assert!(err.message.contains("doesn't exist"), "{}", err.message);
    assert!(!e.db.exists(), "never created");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_lands_in_a_plain_duckdb_file() {
    let e = setup(
        &format!(
            "  dl: {{type: delta, root: \"{}\"}}\n",
            delta_fixtures().display()
        ),
        r#"replications:
  delta:
    source: dl
    target: db
    streams:
      - {id: d/full, mode: full-refresh, from: {table: partitioned, version: 2}, object: br.part, primary_key: [id]}
"#,
    );
    let r = go(&e, &["delta"], false).await;
    ok(&r);
    assert_eq!(rows(&r, "delta").inserted, 3000);
    assert_eq!(q::<i64>(&e, "SELECT count(*) FROM br.part"), 3000);
}

const EOD: &str = r#"replications:
  eod:
    source: ch
    target: db
    streams:
      - id: ch/eod
        mode: full-refresh
        object: br.eod
        primary_key: [series_id, date]
        bind:
          series: {query: "SELECT series_id FROM ag.scoped ORDER BY 1", connection: db, batch: 2}
          floor: {query: "SELECT 0", connection: db}
        from:
          query: >
            SELECT series_id, toDate(timestamp) AS date, argMax(close, timestamp) AS eod_close
            FROM eod
            WHERE series_id IN ({{ bind.series }}) AND close > {{ bind.floor }}
            GROUP BY series_id, date
  highs:
    source: ch
    target: db
    streams:
      - id: ch/highs
        mode: full-refresh
        object: br.highs
        primary_key: [series_id]
        bind:
          series: {query: "SELECT series_id FROM ag.scoped ORDER BY 1", connection: db, batch: 2}
        from:
          query: >
            SELECT series_id, max(close) AS high FROM eod
            WHERE series_id IN ({{ bind.series }}) GROUP BY series_id
  broken:
    source: ch
    target: db
    streams:
      - id: ch/broken
        mode: full-refresh
        object: br.broken
        bind:
          series: {query: "SELECT series_id FROM ag.no_such_table", connection: db}
        from:
          query: "SELECT * FROM eod WHERE series_id IN ({{ bind.series }})"
"#;

fn eod_env() -> (Env, String) {
    let ch = atlatl_testkit::clickhouse();
    let chdb = ch.fresh_db().expect("ch db");
    ch.exec(&format!(
        "CREATE TABLE {chdb}.eod (series_id UInt32, timestamp DateTime('UTC'), close Float64) \
         ENGINE = MergeTree ORDER BY (series_id, timestamp)"
    ))
    .expect("clickhouse");
    ch.exec(&format!(
        "INSERT INTO {chdb}.eod VALUES \
         (1, '2026-10-01 15:00:00', 10), (1, '2026-10-01 20:00:00', 11), \
         (2, '2026-10-01 20:00:00', 20), (3, '2026-10-01 20:00:00', 30), \
         (4, '2026-10-01 20:00:00', 40), (5, '2026-10-01 20:00:00', -1)"
    ))
    .expect("clickhouse");
    let e = setup(
        &format!(
            "  ch: {{type: clickhouse, url: \"{}\", user: {}, password: {}, database: {chdb}}}\n",
            ch.url, ch.user, ch.password
        ),
        EOD,
    );
    // What dbt's first phase leaves behind: the series in scope.
    exec(
        &e,
        "CREATE SCHEMA ag; CREATE TABLE ag.scoped AS SELECT * FROM (VALUES (1), (2), (3), (5)) t(series_id)",
    );
    (e, chdb)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clickhouse_pulls_only_the_bound_series_in_batches() {
    let (e, chdb) = eod_env();
    let r = go(&e, &["eod"], false).await;
    ok(&r);
    assert_eq!(
        rows(&r, "eod").inserted,
        3,
        "series 4 is out of scope, 5 under the floor"
    );
    assert_eq!(
        q::<String>(
            &e,
            "SELECT string_agg(series_id || ':' || eod_close, ',' ORDER BY series_id) FROM br.eod"
        ),
        "1:11.0,2:20.0,3:30.0"
    );
    assert_eq!(
        q::<String>(&e, "SELECT typeof(date) FROM br.eod LIMIT 1"),
        "DATE"
    );
    let ch = atlatl_testkit::clickhouse();
    ch.exec("SYSTEM FLUSH LOGS").expect("flush");
    let queries = ch
        .exec(&format!(
            "SELECT count() FROM system.query_log WHERE current_database = '{chdb}' \
             AND type = 'QueryFinish' AND query LIKE '%argMax%' AND query NOT LIKE '%query_log%'"
        ))
        .expect("clickhouse");
    assert_eq!(queries, "2", "4 series in batches of 2");

    // A corrected bar upstream and a series leaving scope: the daily re-pull replaces both.
    ch.exec(&format!(
        "INSERT INTO {chdb}.eod VALUES (2, '2026-10-01 21:00:00', 21)"
    ))
    .expect("clickhouse");
    exec(&e, "DELETE FROM ag.scoped WHERE series_id = 3");
    let r = go(&e, &["eod"], false).await;
    ok(&r);
    let n = rows(&r, "eod");
    assert_eq!((n.updated, n.deleted, n.unchanged), (1, 1, 1));
    assert_eq!(
        q::<String>(
            &e,
            "SELECT string_agg(series_id || ':' || eod_close, ',' ORDER BY series_id) FROM br.eod"
        ),
        "1:11.0,2:21.0"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_empty_bind_list_empties_the_table() {
    let (e, _) = eod_env();
    ok(&go(&e, &["eod"], false).await);
    exec(&e, "DELETE FROM ag.scoped");
    let r = go(&e, &["eod"], false).await;
    ok(&r);
    assert_eq!(rows(&r, "eod").deleted, 3);
    assert_eq!(q::<i64>(&e, "SELECT count(*) FROM br.eod"), 0);
    let m = &r.replications[0].streams[0].members[0];
    assert!(
        m.warnings.iter().any(|w| w.code == "bind_empty"),
        "{:?}",
        m.warnings
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_clickhouse_replications_keep_their_own_files() {
    let (e, _) = eod_env();
    let r = go(&e, &["eod", "highs"], false).await;
    ok(&r);
    assert_eq!(
        q::<String>(
            &e,
            "SELECT string_agg(series_id || ':' || eod_close, ',' ORDER BY series_id) FROM br.eod"
        ),
        "1:11.0,2:20.0,3:30.0"
    );
    assert_eq!(
        q::<String>(
            &e,
            "SELECT string_agg(series_id || ':' || high, ',' ORDER BY series_id) FROM br.highs"
        ),
        "1:11.0,2:20.0,3:30.0,5:-1.0"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_bind_query_reports_its_own_error() {
    let (e, _) = eod_env();
    let r = go(&e, &["broken", "eod"], false).await;
    let broken = r
        .replications
        .iter()
        .find(|x| x.replication == "broken")
        .expect("broken");
    let err = broken.error.as_ref().expect("error");
    assert!(err.message.contains("no_such_table"), "{}", err.message);
    // The same pooled connections still resolve names in the right database afterwards.
    assert_eq!(status(&r, "eod"), ReplicationStatus::Succeeded);
    assert_eq!(q::<i64>(&e, "SELECT count(*) FROM br.eod"), 3);
}
