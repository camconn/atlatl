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

const DECLARED: &str = r#"replications:
  typed:
    source: files
    target: db
    streams:
      - id: f/typed
        mode: full-refresh
        from: {files: {url: "in", glob: "typed.csv"}}
        object: br.typed
        primary_key: [id]
        columns: {id: {type: INTEGER}, amt: {type: "DECIMAL(10,2)"}, n: {type: BIGINT}, d: {type: DATE}}
  legacy:
    source: files
    target: db
    streams:
      - id: f/legacy
        mode: full-refresh
        from: {files: {url: "in", glob: "typed.csv"}}
        object: br.legacy
        primary_key: [id]
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_types_land_exactly_or_fail() {
    let e = setup("", DECLARED);
    let p = e.project.path();
    std::fs::create_dir_all(p.join("in")).expect("fs");
    let csv = |rows: &str| {
        std::fs::write(p.join("in/typed.csv"), format!("id,amt,n,d\n{rows}")).expect("fs")
    };
    let tables = || {
        q::<i64>(
            &e,
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'br'",
        )
    };

    for (bad, column, value, kind) in [
        (
            "1,12345.6789,15,2024-01-02\n",
            "amt",
            "12345.6789",
            "DECIMAL(10,2)",
        ),
        ("1,12345.67,1.5,2024-01-02\n", "n", "1.5", "BIGINT"),
        (
            "1,12345.67,15,2024-01-02 03:04:05\n",
            "d",
            "2024-01-02 03:04:05",
            "DATE",
        ),
        (
            "3000000000,12345.67,15,2024-01-02\n",
            "id",
            "3000000000",
            "INTEGER",
        ),
    ] {
        csv(bad);
        let r = go(&e, &["typed"], false).await;
        let err = r.replications[0].error.as_ref().expect("error");
        assert_eq!(err.code.as_deref(), Some("cast_failed"), "{err:?}");
        assert!(
            err.message
                .contains(&format!("`{column}` \"{value}\" as {kind}")),
            "{}",
            err.message
        );
        assert_eq!(tables(), 0, "nothing lands");
    }

    csv("1,12345.67,15,2024-01-02\n2,-0.5,-7,2024-01-03 00:00:00\n");
    let r = go(&e, &["typed"], false).await;
    ok(&r);
    assert_eq!(
        q::<String>(
            &e,
            "SELECT string_agg(concat_ws('|', id, amt, n, d), ',' ORDER BY id) FROM br.typed"
        ),
        "1|12345.67|15|2024-01-02,2|-0.50|-7|2024-01-03"
    );

    // An existing narrower column is the same: the target's type wins, but only exactly.
    exec(
        &e,
        "CREATE TABLE br.legacy (id INTEGER, amt DECIMAL(10,2), n VARCHAR, d VARCHAR)",
    );
    csv("1,12345.6789,15,2024-01-02\n");
    let r = go(&e, &["legacy"], false).await;
    let err = r.replications[0].error.as_ref().expect("error");
    assert_eq!(err.code.as_deref(), Some("cast_failed"), "{err:?}");
    assert_eq!(q::<i64>(&e, "SELECT count(*) FROM br.legacy"), 0);
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
  fx:
    source: ch
    target: db
    streams:
      - id: ch/fx
        mode: incremental
        object: br.fx
        primary_key: [series_id, date]
        update_key: {column: date, lookback: 1d}
        from:
          query: >
            SELECT series_id, toDate(timestamp) AS date, argMax(close, timestamp) AS eod_close
            FROM eod
            WHERE {incremental_where_cond} AND date >= coalesce({incremental_value}, '2000-01-01')
            GROUP BY series_id, date
  bench:
    source: ch
    target: db
    streams:
      - id: ch/bench
        mode: incremental
        object: br.bench
        primary_key: [series_id, date]
        bind:
          windows:
            query: >
              SELECT s.series_id, coalesce(max(b.date) - 1, DATE '2000-01-01') AS since
              FROM ag.scoped s LEFT JOIN br.bench b USING (series_id) GROUP BY 1 ORDER BY 1
            fallback: "SELECT series_id, DATE '2000-01-01' AS since FROM ag.scoped ORDER BY 1"
            connection: db
            batch: 2
            concurrency: 2
        from:
          query: >
            SELECT e.series_id AS series_id, toDate(e.timestamp) AS date,
                   argMax(e.close, e.timestamp) AS eod_close
            FROM eod e
            JOIN values('series_id UInt32, since Date', {{ bind.windows }}) w ON e.series_id = w.series_id
            WHERE toDate(e.timestamp) >= w.since
            GROUP BY series_id, date
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
             AND type = 'QueryFinish' AND query LIKE '%argMax%' AND query NOT LIKE '%query_log%' \
             AND query NOT LIKE 'DESCRIBE%'"
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

fn closes(e: &Env, table: &str) -> String {
    q(
        e,
        &format!(
            "SELECT string_agg(series_id || '@' || strftime(date, '%m-%d') || ':' || eod_close, ',' \
             ORDER BY series_id, date) FROM {table}"
        ),
    )
}

/// The queries run against `chdb` that contain `like`, oldest first.
fn ch_queries(chdb: &str, like: &str) -> Vec<String> {
    let ch = atlatl_testkit::clickhouse();
    ch.exec("SYSTEM FLUSH LOGS").expect("flush");
    let out = ch
        .exec(&format!(
            "SELECT arrayStringConcat(groupArray(query), ' ;; ') FROM (SELECT query FROM system.query_log \
             WHERE current_database = '{chdb}' AND type = 'QueryFinish' AND query LIKE '%{like}%' \
             AND query NOT LIKE '%query_log%' AND query NOT LIKE 'DESCRIBE%' ORDER BY event_time_microseconds)"
        ))
        .expect("clickhouse");
    out.replace("\\'", "'")
        .split(" ;; ")
        .map(str::to_owned)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clickhouse_update_key_fills_the_incremental_placeholders() {
    let (e, chdb) = eod_env();
    let ch = atlatl_testkit::clickhouse();
    let r = go(&e, &["fx"], false).await;
    ok(&r);
    assert_eq!(rows(&r, "fx").inserted, 5, "the first run reads everything");

    ch.exec(&format!(
        "INSERT INTO {chdb}.eod VALUES (1, '2026-10-03 20:00:00', 13)"
    ))
    .expect("clickhouse");
    ok(&go(&e, &["fx"], false).await);

    // Past the watermark less a day (10-02): a new bar is pulled, a correction to 10-01 isn't.
    ch.exec(&format!(
        "INSERT INTO {chdb}.eod VALUES (1, '2026-10-02 20:00:00', 12), (2, '2026-10-01 21:00:00', 21)"
    ))
    .expect("clickhouse");
    let r = go(&e, &["fx"], false).await;
    ok(&r);
    let n = rows(&r, "fx");
    assert_eq!((n.inserted, n.unchanged), (1, 1));
    assert_eq!(
        closes(&e, "br.fx"),
        "1@10-01:11.0,1@10-02:12.0,1@10-03:13.0,2@10-01:20.0,3@10-01:30.0,4@10-01:40.0,5@10-01:-1.0"
    );
    let runs = ch_queries(&chdb, "argMax");
    assert_eq!(runs.len(), 3, "{runs:?}");
    assert!(
        runs[0].contains("WHERE 1=1 AND date >= coalesce(null, '2000-01-01')"),
        "{}",
        runs[0]
    );
    assert!(runs[1].contains("`date` >= '2026-09-30'"), "{}", runs[1]);
    assert!(
        runs[2].contains("`date` >= '2026-10-02' AND date >= coalesce('2026-10-02',"),
        "{}",
        runs[2]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tuple_binds_give_each_series_its_own_window() {
    let (e, chdb) = eod_env();
    let ch = atlatl_testkit::clickhouse();
    // No target yet: the fallback lists every series from the start.
    let r = go(&e, &["bench"], false).await;
    ok(&r);
    assert_eq!(rows(&r, "bench").inserted, 4);

    ch.exec(&format!(
        "INSERT INTO {chdb}.eod VALUES (1, '2026-10-05 20:00:00', 15)"
    ))
    .expect("clickhouse");
    ok(&go(&e, &["bench"], false).await);

    // Series 1 now starts at 10-04 and series 2 at 09-30; series 4 joins with no history.
    ch.exec(&format!(
        "INSERT INTO {chdb}.eod VALUES (1, '2026-10-02 20:00:00', 99), (2, '2026-10-01 21:00:00', 21)"
    ))
    .expect("clickhouse");
    exec(&e, "INSERT INTO ag.scoped VALUES (4)");
    let r = go(&e, &["bench"], false).await;
    ok(&r);
    let n = rows(&r, "bench");
    assert_eq!((n.inserted, n.updated), (1, 1));
    assert_eq!(
        closes(&e, "br.bench"),
        "1@10-01:11.0,1@10-05:15.0,2@10-01:21.0,3@10-01:30.0,4@10-01:40.0,5@10-01:-1.0"
    );
    let rows_of = |q: &String| {
        let at = q.find("since Date', ").expect("values") + "since Date', ".len();
        q[at..q.find(") w ON").expect("alias")].to_owned()
    };
    // Two batches run at once, so their order within a run varies.
    let mut runs: Vec<String> = ch_queries(&chdb, "values(").iter().map(rows_of).collect();
    runs.sort();
    assert_eq!(
        runs,
        [
            "(1, '2000-01-01'), (2, '2000-01-01')",
            "(1, '2026-09-30'), (2, '2026-09-30')",
            "(1, '2026-10-04'), (2, '2026-09-30')",
            "(3, '2000-01-01'), (5, '2000-01-01')",
            "(3, '2026-09-30'), (4, '2000-01-01')",
            "(3, '2026-09-30'), (5, '2026-09-30')",
            "(5, '2026-09-30')",
        ],
        "the fallback first, then each series from its own last date less a day"
    );
}

const WIDE: &str = r#"replications:
  wide:
    source: ch
    target: db
    streams:
      - id: ch/wide
        mode: incremental
        object: br.wide
        primary_key: [id]
        update_key: {column: i128}
        from:
          query: "SELECT id, i128, u128 FROM wide WHERE {incremental_where_cond}"
  wide256:
    source: ch
    target: db
    streams:
      - {id: ch/wide256, mode: full-refresh, object: br.wide256, from: {table: wide}}
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wide_integers_land_exactly_or_not_at_all() {
    let ch = atlatl_testkit::clickhouse();
    let chdb = ch.fresh_db().expect("ch db");
    ch.exec(&format!(
        "CREATE TABLE {chdb}.wide (id UInt8, i128 Int128, u128 Nullable(UInt128), i256 Int256, u256 UInt256) \
         ENGINE = MergeTree ORDER BY id"
    ))
    .expect("clickhouse");
    ch.exec(&format!(
        "INSERT INTO {chdb}.wide VALUES \
         (1, '-170141183460469231731687303715884105728', '340282366920938463463374607431768211455', \
          '-57896044618658097711785492504343953926634992332820282019728792003956564819968', \
          '115792089237316195423570985008687907853269984665640564039457584007913129639935'), \
         (2, '170141183460469231731687303715884105727', NULL, '1', '0')"
    ))
    .expect("clickhouse");
    let e = setup(
        &format!(
            "  ch: {{type: clickhouse, url: \"{}\", user: {}, password: {}, database: {chdb}}}\n",
            ch.url, ch.user, ch.password
        ),
        WIDE,
    );
    let r = go(&e, &["wide"], false).await;
    ok(&r);
    assert_eq!(rows(&r, "wide").inserted, 2);
    assert_eq!(
        q::<String>(
            &e,
            "SELECT typeof(i128) || ' ' || typeof(u128) FROM br.wide LIMIT 1"
        ),
        "HUGEINT UHUGEINT"
    );
    assert_eq!(
        q::<String>(
            &e,
            "SELECT string_agg(concat_ws('|', id, i128, coalesce(u128::VARCHAR, '-')), ',' ORDER BY id) FROM br.wide"
        ),
        "1|-170141183460469231731687303715884105728|340282366920938463463374607431768211455,\
         2|170141183460469231731687303715884105727|-"
    );

    // A 128-bit watermark is a typed cast: a bare literal that wide would be a Float64.
    ch.exec(&format!(
        "INSERT INTO {chdb}.wide VALUES (3, '5', NULL, '0', '0')"
    ))
    .expect("clickhouse");
    let r = go(&e, &["wide"], false).await;
    ok(&r);
    assert_eq!(rows(&r, "wide").unchanged, 1, "only the max row again");
    let runs = ch_queries(&chdb, "REPLACE");
    assert!(
        runs.last()
            .expect("query")
            .contains("WHERE `i128` >= CAST('170141183460469231731687303715884105727' AS Int128)"),
        "{runs:?}"
    );

    // No 256-bit integer in DuckDB yet: refused before anything lands.
    let r = go(&e, &["wide256"], false).await;
    let err = r.replications[0].error.as_ref().expect("error");
    assert!(
        err.message
            .contains("`wide`: column `i256` is Int256, which the target can't hold"),
        "{}",
        err.message
    );
    assert_eq!(
        q::<i64>(
            &e,
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'wide256'"
        ),
        0
    );
}

const NETS: &str = r#"replications:
  table:
    source: ch
    target: db
    streams:
      - {id: ch/t, mode: full-refresh, object: br.nets_t, from: {table: nets}, columns: {exclude: [ip]}}
  query:
    source: ch
    target: db
    streams:
      - id: ch/q
        mode: full-refresh
        object: br.nets_q
        from: {query: "SELECT * FROM nets ORDER BY id"}
        columns: {exclude: [IP]}
  include:
    source: ch
    target: db
    streams:
      - {id: ch/i, mode: full-refresh, object: br.nets_i, from: {table: nets}, columns: {include: [big, id]}}
  missing:
    source: ch
    target: db
    streams:
      - {id: ch/m, mode: full-refresh, object: br.nets_m, from: {table: nets}, columns: {include: [id, nope]}}
  all:
    source: ch
    target: db
    streams:
      - {id: ch/a, mode: full-refresh, object: br.nets_a, from: {table: nets}}
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn excluded_columns_are_neither_checked_nor_read() {
    let ch = atlatl_testkit::clickhouse();
    let chdb = ch.fresh_db().expect("ch db");
    ch.exec(&format!(
        "CREATE TABLE {chdb}.nets (id UInt8, ip IPv4, big Int128, name String) ENGINE = MergeTree ORDER BY id"
    ))
    .expect("clickhouse");
    ch.exec(&format!(
        "INSERT INTO {chdb}.nets VALUES (1, '10.0.0.1', '170141183460469231731687303715884105727', 'a'), (2, '10.0.0.2', '-1', 'b')"
    ))
    .expect("clickhouse");
    let e = setup(
        &format!(
            "  ch: {{type: clickhouse, url: \"{}\", user: {}, password: {}, database: {chdb}}}\n",
            ch.url, ch.user, ch.password
        ),
        NETS,
    );
    let r = go(&e, &["table", "query", "include"], false).await;
    ok(&r);
    let cols = |t: &str| {
        q::<String>(
            &e,
            &format!(
                "SELECT string_agg(column_name || ' ' || data_type, ', ' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'br' AND table_name = '{t}' AND column_name NOT LIKE '\\_%' ESCAPE '\\'"
            ),
        )
    };
    assert_eq!(cols("nets_t"), "id UTINYINT, big HUGEINT, name VARCHAR");
    assert_eq!(cols("nets_q"), "id UTINYINT, big HUGEINT, name VARCHAR");
    assert_eq!(cols("nets_i"), "big HUGEINT, id UTINYINT");
    assert_eq!(
        q::<String>(
            &e,
            "SELECT string_agg(id || '|' || big || '|' || name, ',' ORDER BY id) FROM br.nets_q"
        ),
        "1|170141183460469231731687303715884105727|a,2|-1|b"
    );
    let reads = ch_queries(&chdb, "nets");
    assert!(
        reads.len() == 3 && reads.iter().all(|q| q.contains("SELECT `id`, `big`")),
        "{reads:?}"
    );

    let r = go(&e, &["missing"], false).await;
    let err = r.replications[0].error.as_ref().expect("error");
    assert!(
        err.message.contains(
            "stream `ch/m`: `columns.include` names `nope`, which the source doesn't return"
        ),
        "{}",
        err.message
    );
    let r = go(&e, &["all"], false).await;
    let err = r.replications[0].error.as_ref().expect("error");
    assert!(
        err.message
            .contains("`nets`: column `ip` is IPv4, which the target can't hold"),
        "{}",
        err.message
    );
}

const EARLY: &str = r#"replications:
  early:
    source: ch
    target: db
    streams:
      - {id: ch/fine, mode: full-refresh, object: br.fine, from: {table: fine}}
      - {id: ch/bad, mode: full-refresh, object: br.bad, from: {table: bad}, columns: {exclude: [skipped]}}
      - id: ch/q
        mode: full-refresh
        object: br.q
        from: {query: "SELECT id, toFixedString(name, 2) AS code FROM fine"}
      - id: ch/since
        mode: incremental
        object: br.since
        primary_key: [id]
        update_key: {column: ts}
        from: {query: "SELECT id, ts, toIPv4('10.0.0.1') AS addr, {incremental_value} AS since FROM evt WHERE ts >= coalesce({incremental_value}, toDateTime('2000-01-01', 'UTC'))"}
  inc:
    source: ch
    target: db
    streams:
      - id: ch/inc
        mode: incremental
        object: br.inc
        primary_key: [id]
        update_key: {column: ts}
        from: {query: "SELECT id, ts FROM evt WHERE ts >= coalesce({incremental_value}, toDateTime('2000-01-01', 'UTC'))"}
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_refused_column_is_reported_before_anything_is_read() {
    let ch = atlatl_testkit::clickhouse();
    let chdb = ch.fresh_db().expect("ch db");
    ch.exec(&format!(
        "CREATE TABLE {chdb}.fine (id UInt8, name String) ENGINE = MergeTree ORDER BY id"
    ))
    .expect("clickhouse");
    ch.exec(&format!("INSERT INTO {chdb}.fine VALUES (1, 'ab')"))
        .expect("clickhouse");
    ch.exec(&format!(
        "CREATE TABLE {chdb}.bad (id UInt8, ip IPv4, e Enum8('a' = 1), skipped IPv6) ENGINE = MergeTree ORDER BY id"
    ))
    .expect("clickhouse");
    ch.exec(&format!(
        "CREATE TABLE {chdb}.evt (id UInt8, ts DateTime('UTC')) ENGINE = MergeTree ORDER BY id"
    ))
    .expect("clickhouse");
    ch.exec(&format!(
        "INSERT INTO {chdb}.evt VALUES (1, '2026-10-01 00:00:00'), (2, '2026-10-02 00:00:00')"
    ))
    .expect("clickhouse");
    let e = setup(
        &format!(
            "  ch: {{type: clickhouse, url: \"{}\", user: {}, password: {}, database: {chdb}}}\n",
            ch.url, ch.user, ch.password
        ),
        EARLY,
    );
    // `since` is typed by the watermark alone (`Nothing` before the first one): left to the read.
    let want = [
        "4 source columns can't be landed as they are:",
        "  `from.query of ch/since`: column `addr` is IPv4, which the target can't hold",
        "  `bad`: column `ip` is IPv4, which the target can't hold",
        "  `bad`: column `e` is Enum8('a' = 1), which the target can't hold",
        "  `from.query of ch/q`: column `code` is FixedString(2), which the target can't hold",
    ];
    let has_all = |m: &str| {
        want.iter().all(|w| m.contains(w))
            && !m.contains("skipped")
            && !m.contains("column `since`")
    };

    let (p, d) = atlatl_core::config::load(e.project.path());
    assert!(!d.has_errors(), "{d:?}");
    let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::default());
    let req = PlanRequest {
        discover: true,
        ..PlanRequest::all()
    };
    let (mut plan, _) = atlatl_core::plan::build(
        &p,
        &req,
        vars.clone(),
        atlatl_core::redact::Redactor::global(),
    )
    .expect("plan");
    let err = atlatl_run::pgsrc::discover_plan(&p, &mut plan, vars)
        .await
        .expect_err("plan --discover checks types");
    assert_eq!(err.code.as_deref(), Some("source_type"));
    assert!(has_all(&err.message), "{}", err.message);

    let r = go(&e, &["early"], false).await;
    let err = r.replications[0].error.as_ref().expect("error");
    assert!(has_all(&err.message), "{}", err.message);
    for t in ["fine", "evt"] {
        let reads = ch_queries(&chdb, t);
        assert!(reads.iter().all(String::is_empty), "{t}: {reads:?}");
    }
    ch.exec("SYSTEM FLUSH LOGS").expect("flush");
    let described = ch
        .exec(&format!(
            "SELECT count() FROM system.query_log WHERE current_database = '{chdb}' \
             AND type = 'QueryFinish' AND query LIKE 'DESCRIBE%fine%'"
        ))
        .expect("clickhouse");
    assert_eq!(
        described, "4",
        "two streams described by plan, two by the run"
    );
    assert_eq!(
        q::<i64>(
            &e,
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'br'"
        ),
        0
    );

    // A valid `{incremental_value}` query passes the early check and reads by its watermark.
    let r = go(&e, &["inc"], false).await;
    ok(&r);
    assert_eq!(rows(&r, "inc").inserted, 2);
    ch.exec(&format!(
        "INSERT INTO {chdb}.evt VALUES (3, '2026-10-03 00:00:00')"
    ))
    .expect("clickhouse");
    let r = go(&e, &["inc"], false).await;
    ok(&r);
    assert_eq!(rows(&r, "inc").inserted, 1);
    assert_eq!(rows(&r, "inc").unchanged, 1, "only the max row again");
    assert_eq!(q::<i64>(&e, "SELECT count(*) FROM br.inc"), 3);
    let runs = ch_queries(&chdb, "evt");
    assert!(
        runs.last()
            .expect("query")
            .contains("coalesce(toDateTime64('2026-10-02 00:00:00"),
        "{runs:?}"
    );
}

const ZONED: &str = r#"replications:
  ny:
    source: ch
    target: db
    streams:
      - {id: ch/ny, mode: incremental, object: br.ny, primary_key: [id], update_key: {column: ts}, from: {table: ny}}
  ny_q:
    source: ch
    target: db
    streams:
      - id: ch/ny_q
        mode: incremental
        object: br.ny_q
        primary_key: [id]
        update_key: {column: ts}
        from: {query: "SELECT * FROM ny WHERE ts >= coalesce({incremental_value}, '2000-01-01')"}
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timestamp_watermarks_are_instants_whatever_the_column_zone() {
    let ch = atlatl_testkit::clickhouse();
    let chdb = ch.fresh_db().expect("ch db");
    ch.exec(&format!(
        "CREATE TABLE {chdb}.ny (id UInt8, ts DateTime64(6, 'America/New_York')) ENGINE = MergeTree ORDER BY id"
    ))
    .expect("clickhouse");
    // 20:00:00.5 in New York is 00:00:00.5 UTC the next day.
    ch.exec(&format!(
        "INSERT INTO {chdb}.ny VALUES (1, '2026-10-01 20:00:00.500000')"
    ))
    .expect("clickhouse");
    let e = setup(
        &format!(
            "  ch: {{type: clickhouse, url: \"{}\", user: {}, password: {}, database: {chdb}}}\n",
            ch.url, ch.user, ch.password
        ),
        ZONED,
    );
    ok(&go(&e, &["ny"], false).await);
    // Half a second and an hour past the watermark: read in New York time, a bare
    // '2026-10-02 00:00:00' bound would sit four hours later and skip both.
    ch.exec(&format!(
        "INSERT INTO {chdb}.ny VALUES (2, '2026-10-01 20:00:01'), (3, '2026-10-01 21:00:00'), (4, '2026-10-01 20:00:00.400000')"
    ))
    .expect("clickhouse");
    let r = go(&e, &["ny"], false).await;
    ok(&r);
    let n = rows(&r, "ny");
    assert_eq!((n.inserted, n.unchanged), (2, 1), "{n:?}");
    assert_eq!(
        q::<String>(
            &e,
            "SELECT string_agg(id::VARCHAR, ',' ORDER BY id) FROM br.ny"
        ),
        "1,2,3"
    );
    let runs = ch_queries(&chdb, "FROM `");
    assert!(
        runs.last()
            .expect("query")
            .contains("WHERE `ts` >= toDateTime64('2026-10-02 00:00:00.500000', 6, 'UTC')"),
        "{runs:?}"
    );

    // The documented start-date pattern: a string default beside a DateTime64 watermark.
    ok(&go(&e, &["ny_q"], false).await);
    let r = go(&e, &["ny_q"], false).await;
    ok(&r);
    let n = rows(&r, "ny_q");
    assert_eq!(
        (n.inserted, n.unchanged),
        (0, 1),
        "only the max row again: {n:?}"
    );
    let runs = ch_queries(&chdb, "coalesce(");
    assert!(
        runs.last().expect("query").contains(
            "ts >= coalesce(toDateTime64('2026-10-02 01:00:00.000000', 6, 'UTC'), '2000-01-01')"
        ),
        "{runs:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wide_bind_values_reach_clickhouse_exactly() {
    let ch = atlatl_testkit::clickhouse();
    let chdb = ch.fresh_db().expect("ch db");
    let e = setup(
        &format!(
            "  ch: {{type: clickhouse, url: \"{}\", user: {}, password: {}, database: {chdb}}}\n",
            ch.url, ch.user, ch.password
        ),
        r#"replications:
  exact:
    source: ch
    target: db
    streams:
      - id: ch/exact
        mode: full-refresh
        object: br.exact
        bind:
          w:
            query: >
              SELECT 170141183460469231731687303715884105727::HUGEINT AS id,
                     1.12345678901234567890::DECIMAL(38,20) AS d,
                     1.1::DECIMAL(38,20) AS s
            connection: db
        from:
          query: >
            SELECT *, s = toDecimal128('1.1', 20) AS s_exact
            FROM values('id Int128, d Decimal(38,20), s Decimal(38,20)', {{ bind.w }})
  sets:
    source: ch
    target: db
    streams:
      - id: ch/sets
        mode: full-refresh
        object: br.sets
        from: {query: "SELECT now() AS t SETTINGS output_format_parquet_use_custom_encoder = 0"}
"#,
    );
    let r = go(&e, &["exact"], false).await;
    ok(&r);
    assert_eq!(
        q::<String>(
            &e,
            "SELECT id::VARCHAR || ' ' || d::VARCHAR || ' ' || s::VARCHAR || ' ' || s_exact::VARCHAR FROM br.exact"
        ),
        "170141183460469231731687303715884105727 1.12345678901234567890 1.10000000000000000000 1"
    );

    let r = go(&e, &["sets"], false).await;
    let err = r.replications[0].error.as_ref().expect("error");
    assert!(
        err.message
            .contains("`from.query` sets `output_format_parquet_use_custom_encoder`"),
        "{}",
        err.message
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_concurrency_caps_the_connection_across_units() {
    let ch = atlatl_testkit::clickhouse();
    let chdb = ch.fresh_db().expect("ch db");
    let e = setup(
        &format!(
            "  ch: {{type: clickhouse, url: \"{}\", user: {}, password: {}, database: {chdb}, max_concurrency: 1}}\n",
            ch.url, ch.user, ch.password
        ),
        r#"replications:
  a:
    source: ch
    target: db
    streams:
      - id: ch/a
        mode: full-refresh
        object: br.a
        bind: {n: {query: "SELECT range AS n FROM range(6)", connection: db, batch: 1, concurrency: 4}}
        from: {query: "SELECT {{ bind.n }} AS n, sleep(0.2) AS z"}
  b:
    source: ch
    target: db
    streams:
      - id: ch/b
        mode: full-refresh
        object: br.b
        bind: {n: {query: "SELECT range AS n FROM range(6)", connection: db, batch: 1, concurrency: 4}}
        from: {query: "SELECT {{ bind.n }} AS n, sleep(0.2) AS z"}
"#,
    );
    let r = go(&e, &["a", "b"], false).await;
    ok(&r);
    assert_eq!(
        q::<i64>(
            &e,
            "SELECT (SELECT count(*) FROM br.a) + (SELECT count(*) FROM br.b)"
        ),
        12
    );
    ch.exec("SYSTEM FLUSH LOGS").expect("flush");
    // Every query on the connection, DESCRIBE included, ran alone.
    let overlaps = ch
        .exec(&format!(
            "WITH q AS (SELECT query_id, query_start_time_microseconds AS s, event_time_microseconds AS f \
             FROM system.query_log WHERE current_database = '{chdb}' AND type = 'QueryFinish' \
             AND query NOT LIKE '%query_log%') \
             SELECT count() FROM q a, q b WHERE a.query_id < b.query_id AND a.s < b.f AND b.s < a.f"
        ))
        .expect("clickhouse");
    assert_eq!(overlaps, "0");
}

const TYPED_BINDS: &str = r#"replications:
  dec_in_narrow:
    source: ch
    target: db
    streams:
      - id: ch/dec_in_narrow
        mode: full-refresh
        object: br.dec_in_narrow
        bind: {w: {query: "SELECT 1.239::DECIMAL(4,3) AS d", connection: db}}
        from: {query: "SELECT id FROM px WHERE d IN ({{ bind.w }})"}
  dec_values_narrow:
    source: ch
    target: db
    streams:
      - id: ch/dec_values_narrow
        mode: full-refresh
        object: br.dec_values_narrow
        bind: {w: {query: "SELECT 1.239::DECIMAL(4,3) AS d", connection: db}}
        from: {query: "SELECT * FROM values('d Decimal(5,2)', {{ bind.w }})"}
  dec_exact:
    source: ch
    target: db
    streams:
      - id: ch/dec_exact
        mode: full-refresh
        object: br.dec_exact
        bind: {w: {query: "SELECT 1.23::DECIMAL(5,2) AS d", connection: db}}
        from:
          query: >
            SELECT px.id AS id, v.d AS vd FROM px JOIN values('d Decimal(5,2)', {{ bind.w }}) v ON px.d = v.d
            WHERE px.d IN ({{ bind.w }})
  float_exact:
    source: ch
    target: db
    streams:
      - id: ch/float_exact
        mode: full-refresh
        object: br.float_exact
        bind: {w: {query: "SELECT 1.5::DOUBLE AS f", connection: db}}
        from:
          query: >
            SELECT px.id AS id FROM px JOIN values('f Float64', {{ bind.w }}) v ON px.f = v.f
            WHERE px.f IN ({{ bind.w }})
  float32_in:
    source: ch
    target: db
    streams:
      - id: ch/float32_in
        mode: full-refresh
        object: br.float32_in
        bind: {w: {query: "SELECT 0.1::DOUBLE AS g", connection: db}}
        from: {query: "SELECT id FROM px WHERE g IN ({{ bind.w }})"}
  float_values_dec:
    source: ch
    target: db
    streams:
      - id: ch/float_values_dec
        mode: full-refresh
        object: br.float_values_dec
        bind: {w: {query: "SELECT 1.5::DOUBLE AS d", connection: db}}
        from: {query: "SELECT * FROM values('d Decimal(5,2)', {{ bind.w }})"}
  dec_in_float:
    source: ch
    target: db
    streams:
      - id: ch/dec_in_float
        mode: full-refresh
        object: br.dec_in_float
        bind: {w: {query: "SELECT 1.5::DECIMAL(2,1) AS f", connection: db}}
        from: {query: "SELECT id FROM px WHERE f IN ({{ bind.w }})"}
  pg_numeric:
    source: ch
    target: db
    streams:
      - id: ch/pg_numeric
        mode: full-refresh
        object: br.pg_numeric
        bind: {w: {query: "SELECT 1.5::numeric AS amt", connection: pg}}
        from: {query: "SELECT id FROM px WHERE d IN ({{ bind.w }})"}
  pg_domain:
    source: ch
    target: db
    streams:
      - id: ch/pg_domain
        mode: full-refresh
        object: br.pg_domain
        bind: {w: {query: "SELECT 2.5::money52 AS amt", connection: pg}}
        from: {query: "SELECT id FROM px WHERE d IN ({{ bind.w }})"}
  pg_domain_bare:
    source: ch
    target: db
    streams:
      - id: ch/pg_domain_bare
        mode: full-refresh
        object: br.pg_domain_bare
        bind: {w: {query: "SELECT 1.5::anynum AS amt", connection: pg}}
        from: {query: "SELECT id FROM px WHERE d IN ({{ bind.w }})"}
  dec_in_string:
    source: ch
    target: db
    streams:
      - id: ch/dec_in_string
        mode: full-refresh
        object: br.dec_in_string
        bind: {w: {query: "SELECT 1.23::DECIMAL(5,2) AS s", connection: db}}
        from: {query: "SELECT id FROM px WHERE s IN ({{ bind.w }})"}
  varchar_in_string:
    source: ch
    target: db
    streams:
      - id: ch/varchar_in_string
        mode: full-refresh
        object: br.varchar_in_string
        bind: {w: {query: "SELECT 1.23::DECIMAL(5,2)::VARCHAR AS s", connection: db}}
        from: {query: "SELECT id FROM px WHERE s IN ({{ bind.w }})"}
  pg_numeric_typed:
    source: ch
    target: db
    streams:
      - id: ch/pg_numeric_typed
        mode: full-refresh
        object: br.pg_numeric_typed
        bind: {w: {query: "SELECT 2.5::numeric(3,1) AS amt", connection: pg}}
        from: {query: "SELECT id FROM px WHERE d IN ({{ bind.w }})"}
"#;

/// Fractional bind values are typed by their bind query's column: a decimal compares exactly
/// (never truncated to a narrower column) and a float as the column's own float type.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bind_numbers_compare_exactly_or_fail_loudly() {
    let ch = atlatl_testkit::clickhouse();
    let chdb = ch.fresh_db().expect("ch db");
    let pg = atlatl_testkit::pg16();
    let pgdb = pg.fresh_db().expect("pg db");
    ch.exec(&format!(
        "CREATE TABLE {chdb}.px (id UInt8, d Decimal(5,2), f Float64, g Float32, s String) ENGINE = MergeTree ORDER BY id"
    ))
    .expect("clickhouse");
    ch.exec(&format!(
        "INSERT INTO {chdb}.px VALUES (1, 1.23, 1.5, 0.1, '1.23'), (2, 2.50, 2.25, 0.2, '2.5')"
    ))
    .expect("clickhouse");
    let e = setup(
        &format!(
            "  ch: {{type: clickhouse, url: \"{}\", user: {}, password: {}, database: {chdb}}}\n  pg: {{type: postgres, host: {}, port: {}, database: {pgdb}, user: {}, password: \"{}\", sslmode: disable}}\n",
            ch.url, ch.user, ch.password, pg.host, pg.port, pg.user, pg.password
        ),
        TYPED_BINDS,
    );
    let ids = |t: &str| {
        q::<String>(
            &e,
            &format!("SELECT coalesce(string_agg(id::VARCHAR, ',' ORDER BY id), '') FROM br.{t}"),
        )
    };
    let fails = async |rep: &str| {
        let r = go(&e, &[rep], false).await;
        r.replications[0].error.clone().expect("error").message
    };

    let m = fails("dec_values_narrow").await;
    assert!(
        m.contains("stream `ch/dec_values_narrow`: `bind.w` is spliced into `values()` where its column `d` (DECIMAL(4,3)) meets structure column `d Decimal(5,2)`, which would round it to 2 digits; widen the structure's scale, or round in the bind query"),
        "{m}"
    );
    // Refused while rendering: ClickHouse never saw the query, not even to describe it.
    ch.exec("SYSTEM FLUSH LOGS").expect("flush");
    let seen = ch
        .exec(&format!(
            "SELECT count() FROM system.query_log WHERE current_database = '{chdb}' AND query LIKE '%1.239%' AND query NOT LIKE '%query_log%'"
        ))
        .expect("clickhouse");
    assert_eq!(seen, "0");

    // 1.239 is no Decimal(5,2) 1.23: before, `IN ('1.239')` truncated it and matched.
    let r = go(&e, &["dec_in_narrow"], false).await;
    ok(&r);
    assert_eq!(ids("dec_in_narrow"), "");

    let r = go(&e, &["dec_exact"], false).await;
    ok(&r);
    assert_eq!(
        q::<String>(
            &e,
            "SELECT string_agg(id || ':' || vd, ',') FROM br.dec_exact"
        ),
        "1:1.23"
    );

    let r = go(&e, &["float_exact"], false).await;
    ok(&r);
    assert_eq!(ids("float_exact"), "1");
    // A quoted 0.1 is parsed as the column's Float32, which a Float64 literal would miss.
    let r = go(&e, &["float32_in"], false).await;
    ok(&r);
    assert_eq!(ids("float32_in"), "1");

    let m = fails("float_values_dec").await;
    assert!(
        m.contains("where its column `d` (DOUBLE) meets structure column `d Decimal(5,2)`, which would round it to the structure's scale"),
        "{m}"
    );

    let m = fails("dec_in_float").await;
    assert!(m.contains("TYPE_MISMATCH"), "{m}");
    assert!(
        m.contains("hint: `bind.w` column `f` (DECIMAL(2,1)) is sent as a typed `Decimal`, which ClickHouse won't compare with a float or integer column; cast it in the bind query to match (`CAST(x AS DOUBLE)` for a float column)"),
        "{m}"
    );

    let m = fails("pg_numeric").await;
    assert!(
        m.contains("stream `ch/pg_numeric`: `bind.w`: column `amt` is numeric without a precision and scale, so its values have no exact ClickHouse type; cast it in the bind query (`amt::numeric(18,4)`, or `amt::float8`)"),
        "{m}"
    );
    let r = go(&e, &["pg_numeric_typed"], false).await;
    ok(&r);
    assert_eq!(ids("pg_numeric_typed"), "2");

    // Domains count as their base type, through a domain over a domain.
    let (c, conn) = tokio_postgres::connect(&pg.dsn(&pgdb), tokio_postgres::NoTls)
        .await
        .expect("pg");
    tokio::spawn(conn);
    c.batch_execute(
        "CREATE DOMAIN num52 AS numeric(5,2); CREATE DOMAIN money52 AS num52; CREATE DOMAIN anynum AS numeric",
    )
    .await
    .expect("domains");
    let r = go(&e, &["pg_domain"], false).await;
    ok(&r);
    assert_eq!(ids("pg_domain"), "2");
    let m = fails("pg_domain_bare").await;
    assert!(
        m.contains("column `amt` is numeric without a precision and scale"),
        "{m}"
    );

    // A typed decimal never matches a String column, silently; as text it does.
    let r = go(&e, &["dec_in_string"], false).await;
    ok(&r);
    assert_eq!(ids("dec_in_string"), "");
    let r = go(&e, &["varchar_in_string"], false).await;
    ok(&r);
    assert_eq!(ids("varchar_in_string"), "1");
}

/// Postgres keeps both columns in `to_json(row)` under one key, so one would be lost (DuckDB
/// renames the second `id_1`, and `values()` then refuses the extra column itself).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bind_row_with_a_repeated_column_is_refused() {
    let ch = atlatl_testkit::clickhouse();
    let chdb = ch.fresh_db().expect("ch db");
    let pg = atlatl_testkit::pg16();
    let pgdb = pg.fresh_db().expect("pg db");
    let e = setup(
        &format!(
            "  ch: {{type: clickhouse, url: \"{}\", user: {}, password: {}, database: {chdb}}}\n  pg: {{type: postgres, host: {}, port: {}, database: {pgdb}, user: {}, password: \"{}\", sslmode: disable}}\n",
            ch.url, ch.user, ch.password, pg.host, pg.port, pg.user, pg.password
        ),
        r#"replications:
  dup:
    source: ch
    target: db
    streams:
      - id: ch/dup
        mode: full-refresh
        object: br.dup
        bind: {w: {query: "SELECT 1 AS id, 2 AS id, DATE '2026-01-01' AS since", connection: pg}}
        from: {query: "SELECT * FROM values('id UInt32, since Date', {{ bind.w }})"}
"#,
    );
    let r = go(&e, &["dup"], false).await;
    let err = r.replications[0].error.as_ref().expect("error");
    assert!(
        err.message.contains(
            "stream `ch/dup`: `bind.w`: the query returns two columns named `id`; alias one of them"
        ),
        "{}",
        err.message
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_float32_watermark_keeps_rows_at_the_watermark() {
    let ch = atlatl_testkit::clickhouse();
    let chdb = ch.fresh_db().expect("ch db");
    ch.exec(&format!(
        "CREATE TABLE {chdb}.f (id UInt8, v Float32) ENGINE = MergeTree ORDER BY id"
    ))
    .expect("clickhouse");
    ch.exec(&format!("INSERT INTO {chdb}.f VALUES (1, 1.3)"))
        .expect("clickhouse");
    let e = setup(
        &format!(
            "  ch: {{type: clickhouse, url: \"{}\", user: {}, password: {}, database: {chdb}}}\n",
            ch.url, ch.user, ch.password
        ),
        r#"replications:
  f:
    source: ch
    target: db
    streams:
      - {id: ch/f, mode: incremental, object: br.f, primary_key: [id], update_key: {column: v}, from: {table: f}}
"#,
    );
    ok(&go(&e, &["f"], false).await);
    // Another row at exactly the watermark: a bare 1.3 (a Float64) sits above Float32 1.3.
    ch.exec(&format!("INSERT INTO {chdb}.f VALUES (2, 1.3)"))
        .expect("clickhouse");
    let r = go(&e, &["f"], false).await;
    ok(&r);
    let n = rows(&r, "f");
    assert_eq!((n.inserted, n.unchanged), (1, 1), "{n:?}");
}
