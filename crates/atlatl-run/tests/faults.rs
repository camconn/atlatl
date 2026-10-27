//! In-process fault injection (12 §3) against a DuckLake catalog on Postgres: a concurrent writer
//! committing between apply and `COMMIT`, and the catalog connection dropping mid-commit
//! (toxiproxy). The fault rules are process-global, so these tests run one at a time.

use std::sync::Arc;
use std::time::{Duration, Instant};

use atlatl_core::error::ErrorClass;
use atlatl_core::fault;
use atlatl_core::obs::report::{ReplicationReport, ReplicationStatus, RetryReport, RunReport};
use atlatl_core::plan::PlanRequest;
use atlatl_core::template::{EnvSource, MapEnv};
use atlatl_run::{RunArgs, run};
use atlatl_testkit::{LocalLake, TempProject, pg16, toxiproxy};
use tokio_util::sync::CancellationToken;

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const REPS: &str = r#"replications:
  conf:
    source: lake
    target: lake
    retry: {base: 50ms, max: 1s}
    streams:
      - {id: conf/people, mode: full-refresh, from: {table: src.people}, object: br.people, primary_key: [id]}
  netr:
    source: lake
    target: lake
    replication_retries: 1
    streams:
      - {id: netr/people, mode: full-refresh, from: {table: src.people}, object: br.people_netr, primary_key: [id]}
  once:
    source: lake
    target: lake
    retry: {max_attempts: 1}
    streams:
      - {id: once/people, mode: full-refresh, from: {table: src.people}, object: br.people_once, primary_key: [id]}
"#;

struct Env {
    lake: Arc<LocalLake>,
    project: TempProject,
}

fn seed(lake: &LocalLake) {
    let c = lake.open("lake").expect("open");
    c.execute_batch("CREATE SCHEMA lake.src; CREATE TABLE lake.src.people AS SELECT range AS id, 'p' || range AS name FROM range(1, 11);").expect("seed");
}

fn setup() -> Env {
    let lake = LocalLake::on_postgres(pg16()).expect("lake");
    seed(&lake);
    let project = TempProject::new()
        .and_then(|p| {
            p.project(
                &lake.connection_yaml("lake", "lake"),
                "state: {lake: lake}\n",
            )
        })
        .and_then(|p| p.replication("all", REPS))
        .expect("project");
    Env {
        lake: Arc::new(lake),
        project,
    }
}

async fn go(project: &TempProject, rep: &str) -> RunReport {
    let (p, d) = atlatl_core::config::load(project.path());
    assert!(!d.has_errors(), "{d:?}");
    let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::default());
    let args = RunArgs {
        plan: PlanRequest {
            replications: vec![rep.to_owned()],
            ..PlanRequest::all()
        },
        extension_dir: Some(atlatl_testkit::extension_dir()),
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

fn rep<'a>(r: &'a RunReport, name: &str) -> &'a ReplicationReport {
    r.replications
        .iter()
        .find(|x| x.replication == name)
        .expect("rep")
}

fn retries(r: &ReplicationReport) -> Vec<&RetryReport> {
    r.streams
        .iter()
        .flat_map(|s| s.members.iter())
        .flat_map(|m| m.retries.iter())
        .collect()
}

fn q(lake: &LocalLake, sql: &str) -> String {
    let c = lake.open("lake").expect("open");
    c.query_row(&format!("SELECT ({sql})::VARCHAR"), [], |r| {
        r.get::<_, Option<String>>(0)
    })
    .expect(sql)
    .unwrap_or_default()
}

fn rows(lake: &LocalLake, t: &str) -> String {
    q(
        lake,
        &format!("SELECT string_agg(id || ':' || name, ',' ORDER BY id) FROM lake.{t}"),
    )
}

fn snapshot(lake: &LocalLake) -> i64 {
    q(
        lake,
        "SELECT max(snapshot_id) FROM ducklake_snapshots('lake')",
    )
    .parse()
    .expect("snapshot")
}

/// Snapshots after `since` that changed `br.<t>`.
fn table_snapshots(lake: &LocalLake, t: &str, since: i64) -> i64 {
    let now = snapshot(lake);
    q(lake, &format!("SELECT count(DISTINCT snapshot_id) FROM ducklake_table_changes('lake', 'br', '{t}', {}, {now})", since + 1)).parse().expect("count")
}

async fn wait_fired(point: &str, limit: Duration) {
    let t = Instant::now();
    while fault::fired(point) == 0 {
        assert!(t.elapsed() < limit, "`{point}` never fired");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Another writer (no atlatl locks) commits to `br.<t>` while atlatl's transaction is open.
async fn concurrent_insert(lake: Arc<LocalLake>, t: &'static str) {
    wait_fired("lake.commit", Duration::from_secs(60)).await;
    tokio::task::spawn_blocking(move || {
        lake.open("lake")
            .expect("open")
            .execute_batch(&format!("INSERT INTO lake.br.{t} VALUES (999, 'writer')"))
            .expect("concurrent insert");
    })
    .await
    .expect("writer");
}

fn mutate_source(lake: &LocalLake) {
    lake.open("lake").expect("open").execute_batch("UPDATE lake.src.people SET name = 'changed' WHERE id = 1; DELETE FROM lake.src.people WHERE id = 10;").expect("mutate");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_conflict_is_retried_against_staging() {
    let _g = SERIAL.lock().await;
    fault::clear();
    let e = setup();
    let r = go(&e.project, "conf").await;
    assert_eq!(
        rep(&r, "conf").status,
        ReplicationStatus::Succeeded,
        "{:?}",
        rep(&r, "conf").error
    );
    mutate_source(&e.lake);
    let before = snapshot(&e.lake);

    fault::set("lake.commit@conf:sleep:2000:once").expect("fault");
    let (r, ()) = tokio::join!(
        go(&e.project, "conf"),
        concurrent_insert(e.lake.clone(), "people")
    );
    fault::clear();
    let x = rep(&r, "conf");
    assert_eq!(
        x.status,
        ReplicationStatus::Succeeded,
        "{:?} after {:?}",
        x.error,
        retries(x)
    );
    let rs = retries(x);
    assert_eq!(rs.len(), 1, "{rs:?}");
    assert_eq!(rs[0].class, ErrorClass::Conflict, "{rs:?}");
    assert_eq!(rs[0].level, atlatl_core::retry::RetryLevel::Commit);
    assert_eq!(
        rows(&e.lake, "br.people"),
        rows(&e.lake, "src.people"),
        "the retried full refresh also removed the writer's row"
    );
    assert_eq!(
        table_snapshots(&e.lake, "people", before),
        2,
        "the writer's snapshot and exactly one of ours"
    );
    assert_eq!(r.exit_code, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_conflict_fails_when_attempts_run_out() {
    let _g = SERIAL.lock().await;
    fault::clear();
    let e = setup();
    let r = go(&e.project, "once").await;
    assert_eq!(
        rep(&r, "once").status,
        ReplicationStatus::Succeeded,
        "{:?}",
        rep(&r, "once").error
    );
    let pre = rows(&e.lake, "br.people_once");
    mutate_source(&e.lake);

    fault::set("lake.commit@once:sleep:2000").expect("fault");
    let (r, ()) = tokio::join!(
        go(&e.project, "once"),
        concurrent_insert(e.lake.clone(), "people_once")
    );
    fault::clear();
    let x = rep(&r, "once");
    assert_eq!(x.status, ReplicationStatus::Failed);
    let err = x.error.as_ref().expect("error");
    assert_eq!(
        (err.class, err.code.as_deref()),
        (ErrorClass::Conflict, Some("commit_conflict")),
        "{err:?}"
    );
    assert!(
        err.message.contains("table `br.people_once` (index"),
        "06 §2: the table id is named: {}",
        err.message
    );
    assert!(retries(x).is_empty());
    assert_eq!(
        rows(&e.lake, "br.people_once"),
        format!("{pre},999:writer"),
        "only the writer's commit landed"
    );
    assert_eq!(r.exit_code, 1);

    let r = go(&e.project, "once").await;
    assert_eq!(
        rep(&r, "once").status,
        ReplicationStatus::Succeeded,
        "{:?}",
        rep(&r, "once").error
    );
    assert_eq!(rows(&e.lake, "br.people_once"), rows(&e.lake, "src.people"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catalog_connection_drop_fails_transient_and_leaves_lake_unchanged() {
    let _g = SERIAL.lock().await;
    fault::clear();
    let pg = pg16();
    let lake = Arc::new(LocalLake::on_postgres(pg).expect("lake"));
    seed(&lake);
    let proxy = toxiproxy().postgres(pg).expect("proxy");
    let project = TempProject::new()
        .and_then(|p| {
            p.project(
                &lake.connection_yaml_via("lake", "lake", &proxy.host, proxy.port),
                "state: {lake: lake}\n",
            )
        })
        .and_then(|p| p.replication("all", REPS))
        .expect("project");
    // State bootstrap first, so the only lake writes left are the replication's.
    let r = go(&project, "once").await;
    assert_eq!(
        rep(&r, "once").status,
        ReplicationStatus::Succeeded,
        "{:?}",
        rep(&r, "once").error
    );
    let before = snapshot(&lake);

    fault::set("lake.commit@conf:sleep:1000:once").expect("fault");
    let drop_link = async {
        wait_fired("lake.commit", Duration::from_secs(60)).await;
        proxy.set_enabled(false).expect("disable proxy");
    };
    let (r, ()) = tokio::join!(go(&project, "conf"), drop_link);
    fault::clear();
    let x = rep(&r, "conf");
    assert_eq!(x.status, ReplicationStatus::Failed);
    let err = x.error.as_ref().expect("error");
    assert_eq!(err.class, ErrorClass::Transient, "{err:?}");
    assert!(err.retryable);
    assert_eq!(
        q(
            &lake,
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'br' AND table_name = 'people'"
        ),
        "0",
        "nothing committed"
    );
    assert_eq!(snapshot(&lake), before);

    proxy.set_enabled(true).expect("enable proxy");
    let r = go(&project, "conf").await;
    assert_eq!(
        rep(&r, "conf").status,
        ReplicationStatus::Succeeded,
        "{:?}",
        rep(&r, "conf").error
    );
    assert_eq!(rows(&lake, "br.people"), rows(&lake, "src.people"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_catalog_connection_fails_only_the_attempt() {
    let _g = SERIAL.lock().await;
    fault::clear();
    let pg = pg16();
    let lake = Arc::new(LocalLake::on_postgres(pg).expect("lake"));
    seed(&lake);
    let proxy = toxiproxy().postgres(pg).expect("proxy");
    let project = TempProject::new()
        .and_then(|p| {
            p.project(
                &lake.connection_yaml_via("lake", "lake", &proxy.host, proxy.port),
                "state: {lake: lake}\n",
            )
        })
        .and_then(|p| p.replication("all", REPS))
        .expect("project");

    // The link drops and comes back while the commit transaction is open; the retry reconnects.
    fault::set("lake.commit@netr:sleep:1000:once").expect("fault");
    let blip = async {
        wait_fired("lake.commit", Duration::from_secs(60)).await;
        proxy.set_enabled(false).expect("disable proxy");
        tokio::time::sleep(Duration::from_millis(200)).await;
        proxy.set_enabled(true).expect("enable proxy");
    };
    let (r, ()) = tokio::join!(go(&project, "netr"), blip);
    fault::clear();
    let x = rep(&r, "netr");
    assert_eq!(x.status, ReplicationStatus::Succeeded, "{:?}", x.error);
    assert_eq!(x.attempts, 2);
    assert_eq!(rows(&lake, "br.people_netr"), rows(&lake, "src.people"));
}
