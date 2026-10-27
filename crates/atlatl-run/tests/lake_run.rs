use std::sync::Arc;

use atlatl_core::obs::report::{ReplicationStatus, RunReport};
use atlatl_core::plan::PlanRequest;
use atlatl_core::template::{EnvSource, MapEnv};
use atlatl_run::{RunArgs, run};
use atlatl_testkit::{LocalLake, TempProject};
use tokio_util::sync::CancellationToken;

const REPS: &str = r#"replications:
  copy_full:
    source: lake
    target: lake
    streams:
      - id: full/pk
        mode: full-refresh
        from: {table: src.people}
        object: br.people_pk
        primary_key: [id]
      - id: full/nopk
        mode: full-refresh
        from: {table: src.people}
        object: br.people_nopk
        metadata: {loaded_at: {name: _legacy_loaded_at, type: epoch_s_int}}
  copy_inc:
    source: lake
    target: lake
    streams:
      - id: inc/events
        mode: incremental
        from: {table: src.events}
        object: br.events
        primary_key: [id]
        update_key: {column: updated_at}
  wild:
    source: lake
    target: lake
    defaults:
      mode: full-refresh
      object: "br.w_{{ member.table }}"
    streams:
      - id: wild/shards
        from: {tables: "src.shard_*"}
  chunked:
    source: lake
    target: lake
    streams:
      - id: chunked/big
        mode: incremental
        from: {table: src.big}
        object: br.big
        primary_key: [id]
        chunk: {by: hash, columns: [id], buckets: 4}
  slow:
    source: lake
    target: lake
    timeout: 1s
    streams:
      - id: slow/big
        mode: full-refresh
        from: {query: "SELECT a.range AS id, md5(a.range::VARCHAR) AS h FROM range(3000000000) a ORDER BY h"}
        object: br.slow
  atomic_fail:
    source: lake
    target: lake
    streams:
      - id: atomic/ok
        mode: full-refresh
        from: {table: src.people}
        object: br.atomic_ok
      - id: atomic/dups
        mode: incremental
        from: {table: src.dups}
        object: br.atomic_dups
        primary_key: [id]
"#;

struct Env {
    lake: LocalLake,
    project: TempProject,
}

fn setup() -> Env {
    let lake = LocalLake::file().expect("lake");
    let c = lake.open("lake").expect("open");
    c.execute_batch(
        "CREATE SCHEMA lake.src;
         CREATE TABLE lake.src.people AS SELECT range AS id, 'p' || range AS name FROM range(1, 11);
         CREATE TABLE lake.src.events AS SELECT range AS id, TIMESTAMP '2025-01-01' + INTERVAL (range) HOUR AS updated_at, 'v1' AS v FROM range(1, 6);
         CREATE TABLE lake.src.shard_a AS SELECT 1 AS x;
         CREATE TABLE lake.src.shard_b AS SELECT 2 AS x;
         CREATE TABLE lake.src.big AS SELECT range AS id, md5(range::VARCHAR) AS h FROM range(1000);
         CREATE TABLE lake.src.dups AS SELECT * FROM (VALUES (1, 'a'), (1, 'b')) t(id, v);",
    )
    .expect("seed");
    drop(c);
    let project = TempProject::new()
        .and_then(|p| {
            p.project(
                &lake.connection_yaml("lake", "lake"),
                "state: {lake: lake}\n",
            )
        })
        .and_then(|p| p.replication("all", REPS))
        .expect("project");
    Env { lake, project }
}

async fn go(e: &Env, args: RunArgs) -> RunReport {
    let (p, d) = atlatl_core::config::load(e.project.path());
    assert!(!d.has_errors(), "{d:?}");
    let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::default());
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

fn args(reps: &[&str]) -> RunArgs {
    RunArgs {
        plan: PlanRequest {
            replications: reps.iter().map(|r| (*r).to_owned()).collect(),
            ..PlanRequest::all()
        },
        extension_dir: Some(atlatl_testkit::extension_dir()),
        ..Default::default()
    }
}

fn status(r: &RunReport, rep: &str) -> ReplicationStatus {
    r.replications
        .iter()
        .find(|x| x.replication == rep)
        .map(|x| x.status)
        .expect("rep")
}

fn rows(r: &RunReport, rep: &str, stream: &str) -> atlatl_core::obs::report::RowCounts {
    let x = r
        .replications
        .iter()
        .find(|x| x.replication == rep)
        .expect("rep");
    let s = x
        .streams
        .iter()
        .find(|s| s.stream_id == stream)
        .expect("stream");
    s.members.iter().fold(
        Default::default(),
        |mut acc: atlatl_core::obs::report::RowCounts, m| {
            acc.add(&m.rows);
            acc
        },
    )
}

fn q(e: &Env, sql: &str) -> i64 {
    let c = e.lake.open("lake").expect("open");
    c.query_row(sql, [], |r| r.get(0)).expect(sql)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_refresh_and_reruns() {
    let e = setup();
    let r = go(&e, args(&["copy_full"])).await;
    assert_eq!(
        status(&r, "copy_full"),
        ReplicationStatus::Succeeded,
        "{:?}",
        r.replications[0].error
    );
    assert_eq!(rows(&r, "copy_full", "full/pk").inserted, 10);
    assert_eq!(
        q(
            &e,
            "SELECT count(*) FROM lake.br.people_nopk WHERE _legacy_loaded_at > 0"
        ),
        10
    );
    let typ: String = e
        .lake
        .open("lake")
        .expect("o")
        .query_row(
            "SELECT typeof(_legacy_loaded_at) FROM lake.br.people_nopk LIMIT 1",
            [],
            |r| r.get(0),
        )
        .expect("t");
    assert_eq!(typ, "INTEGER");
    let snap_before = q(
        &e,
        "SELECT max(snapshot_id) FROM ducklake_snapshots('lake')",
    );

    // Re-run with nothing changed on the PK stream; the no-PK stream still rewrites.
    let c = e.lake.open("lake").expect("o");
    c.execute_batch("DELETE FROM lake.src.people WHERE id = 10; UPDATE lake.src.people SET name = 'changed' WHERE id = 1;").expect("mutate");
    drop(c);
    let r = go(&e, args(&["copy_full"])).await;
    let pk = rows(&r, "copy_full", "full/pk");
    assert_eq!(
        (pk.inserted, pk.updated, pk.deleted, pk.unchanged),
        (0, 1, 1, 8),
        "diff-replace"
    );
    assert_eq!(q(&e, "SELECT count(*) FROM lake.br.people_pk"), 9);
    let after = q(
        &e,
        "SELECT max(snapshot_id) FROM ducklake_snapshots('lake')",
    );
    assert!(after > snap_before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_update_key_keeps_state() {
    let e = setup();
    let r = go(&e, args(&["copy_inc"])).await;
    assert_eq!(
        status(&r, "copy_inc"),
        ReplicationStatus::Succeeded,
        "{:?}",
        r.replications[0].error
    );
    assert_eq!(rows(&r, "copy_inc", "inc/events").inserted, 5);
    let c = e.lake.open("lake").expect("o");
    let st: String = c
        .query_row(
            "SELECT value::VARCHAR FROM lake._atlatl.stream_state WHERE stream_id = 'inc/events'",
            [],
            |r| r.get(0),
        )
        .expect("state");
    assert!(st.contains("2025-01-01 05:00:00"), "{st}");
    c.execute_batch("INSERT INTO lake.src.events VALUES (6, TIMESTAMP '2025-01-01 07:00:00', 'v1'); UPDATE lake.src.events SET v = 'v2', updated_at = TIMESTAMP '2025-01-01 08:00:00' WHERE id = 2;").expect("more");
    drop(c);
    let r = go(&e, args(&["copy_inc"])).await;
    let n = rows(&r, "copy_inc", "inc/events");
    assert_eq!(
        (n.staged, n.inserted, n.updated),
        (3, 1, 1),
        "inclusive watermark re-reads the boundary row, unchanged"
    );
    assert_eq!(
        q(&e, "SELECT count(*) FROM lake.br.events WHERE v = 'v2'"),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wildcard_members_and_chunks() {
    let e = setup();
    let r = go(&e, args(&["wild", "chunked"])).await;
    assert_eq!(
        status(&r, "wild"),
        ReplicationStatus::Succeeded,
        "{:?}",
        r.replications.iter().map(|x| &x.error).collect::<Vec<_>>()
    );
    assert_eq!(
        status(&r, "chunked"),
        ReplicationStatus::Succeeded,
        "{:?}",
        r.replications.iter().map(|x| &x.error).collect::<Vec<_>>()
    );
    assert_eq!(
        q(&e, "SELECT count(*) FROM lake.br.w_shard_a")
            + q(&e, "SELECT count(*) FROM lake.br.w_shard_b"),
        2
    );
    assert_eq!(rows(&r, "chunked", "chunked/big").inserted, 1000);
    assert_eq!(q(&e, "SELECT count(*) FROM lake.br.big"), 1000);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replication_is_atomic_and_independent() {
    let e = setup();
    let r = go(&e, args(&["atomic_fail", "copy_full"])).await;
    assert_eq!(status(&r, "atomic_fail"), ReplicationStatus::Failed);
    let err = r
        .replications
        .iter()
        .find(|x| x.replication == "atomic_fail")
        .and_then(|x| x.error.clone())
        .expect("error");
    assert_eq!(err.code.as_deref(), Some("duplicate_primary_key"));
    assert_eq!(status(&r, "copy_full"), ReplicationStatus::Succeeded);
    let c = e.lake.open("lake").expect("o");
    let exists: i64 = c.query_row("SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = 'atomic_ok'", [], |r| r.get(0)).expect("q");
    assert_eq!(exists, 0, "the sibling stream's write rolled back");
    assert_eq!(r.exit_code, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skip_succeeded_uses_run_history() {
    let e = setup();
    let mut a = args(&["copy_full"]);
    a.external_run_id = Some("dagster-1".into());
    let r = go(&e, a.clone()).await;
    assert_eq!(status(&r, "copy_full"), ReplicationStatus::Succeeded);
    a.skip_succeeded = true;
    let r = go(&e, a).await;
    assert_eq!(status(&r, "copy_full"), ReplicationStatus::Skipped);
    assert_eq!(r.exit_code, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timeout_kills_only_its_replication() {
    let e = setup();
    let t = std::time::Instant::now();
    let r = go(&e, args(&["slow", "copy_full"])).await;
    assert_eq!(status(&r, "slow"), ReplicationStatus::Killed);
    assert_eq!(
        r.replications
            .iter()
            .find(|x| x.replication == "slow")
            .and_then(|x| x.error.as_ref())
            .and_then(|e| e.code.clone())
            .as_deref(),
        Some("timeout")
    );
    assert_eq!(
        status(&r, "copy_full"),
        ReplicationStatus::Succeeded,
        "{:?}",
        r.replications.iter().map(|x| &x.error).collect::<Vec<_>>()
    );
    assert!(
        t.elapsed() < std::time::Duration::from_secs(20),
        "{:?}",
        t.elapsed()
    );
    assert_eq!(q(&e, "SELECT count(*) FROM lake.br.people_pk"), 10);
    let skipped: i64 = q(
        &e,
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = 'slow'",
    );
    assert_eq!(skipped, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_history_uses_the_reports_run_id() {
    let e = setup();
    let mut a = args(&["copy_full"]);
    a.plan.run_id = None;
    let r = go(&e, a).await;
    assert!(!r.run_id.is_empty());
    let c = e.lake.open("lake").expect("o");
    let runs: String = c
        .query_row(
            "SELECT string_agg(DISTINCT run_id, ',') FROM lake._atlatl.runs",
            [],
            |r| r.get(0),
        )
        .expect("runs");
    let reps: String = c
        .query_row(
            "SELECT string_agg(DISTINCT run_id, ',') FROM lake._atlatl.run_replications",
            [],
            |r| r.get(0),
        )
        .expect("reps");
    assert_eq!(
        (runs.as_str(), reps.as_str()),
        (r.run_id.as_str(), r.run_id.as_str())
    );
    drop(c);

    let (p, _) = atlatl_core::config::load(e.project.path());
    let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::default());
    let mut a = args(&["copy_full"]);
    a.plan.static_plan = true;
    let err = run(
        Arc::new(p),
        a,
        vars,
        CancellationToken::new(),
        || None,
        |_, _| {},
    )
    .await
    .expect_err("static");
    assert!(err.message.contains("static plan"), "{err}");
}
