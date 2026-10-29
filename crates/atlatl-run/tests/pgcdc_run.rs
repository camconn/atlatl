//! `atlatl run` of a postgres → lake `mode: cdc` replication (M6), on PG 12 and PG 16.

use std::sync::Arc;

use atlatl_conn_postgres::{ConnectOptions, Session, connect};
use atlatl_core::obs::report::{MemberReport, ReplicationStatus, RunReport};
use atlatl_core::plan::PlanRequest;
use atlatl_core::template::{EnvSource, MapEnv};
use atlatl_run::{RunArgs, run};
use atlatl_testkit::{LocalLake, Postgres, TempProject};
use tokio_util::sync::CancellationToken;

const REP: &str = "admin_cdc";

struct Env {
    _pg: Postgres,
    sql: Session,
    lake: LocalLake,
    project: TempProject,
}

async fn setup(version: u32) -> Env {
    let pg = Postgres::builder(version)
        .setting("wal_level", "logical")
        .start()
        .expect("postgres");
    let opts = ConnectOptions::new(
        &pg.host,
        pg.port,
        "postgres",
        &pg.user,
        Some(pg.password.clone()),
    );
    let sql = connect(&opts).await.expect("connect");
    sql.client()
        .batch_execute(
            "CREATE TABLE users (id int PRIMARY KEY, name text, score numeric(10,2));
             INSERT INTO users SELECT i, 'u' || i, i * 1.5 FROM generate_series(1, 5) i;",
        )
        .await
        .expect("seed");
    let lake = LocalLake::file().expect("lake");
    let pg_yaml = format!(
        "  appdb_pg:\n    type: postgres\n    host: {}\n    port: {}\n    database: postgres\n    user: {}\n    password: \"{}\"\n    sslmode: disable\n    read_only: true\n    changes: {{production: false, slots: {{allow_create: true}}}}\n",
        pg.host, pg.port, pg.user, pg.password
    );
    let reps = format!(
        "replications:\n  {REP}:\n    source: appdb_pg\n    target: lake\n    streams:\n      - {{id: appdb/users, mode: cdc, from: {{table: public.users}}, object: br.users}}\n"
    );
    let project = TempProject::new()
        .and_then(|p| {
            p.project(
                &format!("{}{pg_yaml}", lake.connection_yaml("lake", "lake")),
                "state: {lake: lake}\n",
            )
        })
        .and_then(|p| p.replication("appdb", &reps))
        .expect("project");
    Env {
        _pg: pg,
        sql,
        lake,
        project,
    }
}

async fn go(e: &Env) -> RunReport {
    let (p, d) = atlatl_core::config::load(e.project.path());
    assert!(!d.has_errors(), "{d:?}");
    let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::default());
    let args = RunArgs {
        plan: PlanRequest {
            replications: vec![REP.into()],
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

fn member(r: &RunReport) -> (ReplicationStatus, MemberReport) {
    let x = &r.replications[0];
    assert!(x.error.is_none(), "{:?}", x.error);
    let s = x
        .streams
        .iter()
        .find(|s| s.stream_id == "appdb/users")
        .expect("stream");
    (x.status, s.members[0].clone())
}

fn lake_rows(e: &Env) -> Vec<String> {
    let c = e.lake.open("lake").expect("open");
    let mut s = c
        .prepare(
            "SELECT id::VARCHAR || ':' || name || ':' || score::VARCHAR FROM lake.br.users ORDER BY id",
        )
        .expect("prepare");
    s.query_map([], |r| r.get::<_, String>(0))
        .expect("query")
        .collect::<Result<_, _>>()
        .expect("rows")
}

async fn backfill_changes_up_to_date(version: u32) {
    let e = setup(version).await;

    let (status, m) = member(&go(&e).await);
    assert_eq!(status, ReplicationStatus::Succeeded);
    assert_eq!(m.rows.inserted, 5);
    assert!(m.lake_snapshot.is_some());
    assert!(m.state.before.is_none());
    let lsn = m.state.after.clone().expect("cursor");
    assert_eq!(lsn["slot"], "atlatl_admin_cdc");
    let cdc = m.cdc.expect("cdc");
    assert!(cdc.resynced);
    let slot = cdc.slot.expect("slot report");
    assert_eq!(slot.slot, "atlatl_admin_cdc");
    assert!(
        slot.lag_bytes.is_some() && slot.catalog_xmin_age.is_some(),
        "{slot:?}"
    );
    assert_eq!(slot.confirmed_flush_lsn.as_deref(), lsn["lsn"].as_str());
    assert_eq!(lake_rows(&e).len(), 5);

    for s in [
        "INSERT INTO users VALUES (6, 'u6', 9)",
        "UPDATE users SET name = 'renamed' WHERE id = 1",
        "DELETE FROM users WHERE id = 2",
    ] {
        e.sql.client().batch_execute(s).await.expect(s);
    }
    let (status, m) = member(&go(&e).await);
    assert_eq!(status, ReplicationStatus::Succeeded);
    assert_eq!(
        (m.rows.inserted, m.rows.updated, m.rows.deleted),
        (1, 1, 1),
        "{:?}",
        m.rows
    );
    assert_eq!(m.state.before.as_ref(), Some(&lsn));
    assert!(!m.cdc.expect("cdc").resynced);
    assert_eq!(
        lake_rows(&e),
        [
            "1:renamed:1.50",
            "3:u3:4.50",
            "4:u4:6.00",
            "5:u5:7.50",
            "6:u6:9.00"
        ]
    );

    let (status, m) = member(&go(&e).await);
    assert_eq!(status, ReplicationStatus::UpToDate);
    assert_eq!(m.rows.written(), 0);

    let (p, _) = atlatl_core::config::load(e.project.path());
    let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::default());
    let d = atlatl_run::pgcdc::decommission(&p, REP, None, false, vars)
        .await
        .expect("decommission");
    assert!(d.slot_dropped && d.publication_dropped, "{d:?}");
    let r = go(&e).await;
    let x = &r.replications[0];
    assert_eq!(x.status, ReplicationStatus::Failed);
    let err = x.error.as_ref().expect("error");
    assert_eq!(err.code.as_deref(), Some("slot_missing"), "{err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg16_backfill_changes_up_to_date() {
    backfill_changes_up_to_date(16).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg12_backfill_changes_up_to_date() {
    backfill_changes_up_to_date(12).await
}

async fn discover(e: &Env) -> atlatl_core::error::Result<()> {
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
    atlatl_run::pgsrc::discover_plan(&p, &mut plan, vars).await
}

async fn slots_and_publications(e: &Env) -> (i64, i64) {
    let r = e
        .sql
        .client()
        .query_one(
            "SELECT (SELECT count(*) FROM pg_replication_slots), (SELECT count(*) FROM pg_publication)",
            &[],
        )
        .await
        .expect("count");
    (r.get(0), r.get(1))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg16_plan_discover_reports_refused_columns() {
    let e = setup(16).await;
    e.sql
        .client()
        .batch_execute("CREATE TABLE ledger (id int PRIMARY KEY, cash money, note text)")
        .await
        .expect("ledger");
    let reps = |columns: &str| {
        format!(
            "replications:\n  {REP}:\n    source: appdb_pg\n    target: lake\n    streams:\n      - {{id: appdb/users, mode: cdc, from: {{table: public.users}}, object: br.users}}\n      - {{id: appdb/ledger, mode: cdc, from: {{table: public.ledger}}, object: br.ledger{columns}}}\n"
        )
    };
    let path = e.project.path().join("replications/appdb.yaml");
    std::fs::write(&path, reps("")).expect("reps");

    let err = discover(&e)
        .await
        .expect_err("plan --discover checks cdc types");
    assert_eq!(
        (err.class.to_string().as_str(), err.code.as_deref()),
        ("config", Some("source_type")),
        "{err}"
    );
    assert!(
        err.message
            .contains("`public.ledger`: column `cash` is money"),
        "{}",
        err.message
    );
    assert_eq!(
        slots_and_publications(&e).await,
        (0, 0),
        "plan creates nothing"
    );

    let r = go(&e).await;
    let x = &r.replications[0];
    assert_eq!(x.status, ReplicationStatus::Failed);
    let run = x.error.as_ref().expect("error");
    assert_eq!(
        run.message, err.message,
        "the plan reports what the run would"
    );
    assert_eq!(slots_and_publications(&e).await, (0, 0));

    std::fs::write(&path, reps(", columns: {cash: {type: VARCHAR}}")).expect("reps");
    discover(&e).await.expect("declared column passes");
    assert_eq!(
        slots_and_publications(&e).await,
        (0, 0),
        "plan creates nothing"
    );
}
