//! Lake → Postgres CDC through the runner (M1.6) and `cdc bootstrap` (M1.7), on PG 16.

use std::sync::Arc;

use atlatl_core::obs::report::{MemberReport, ReplicationStatus, RunReport};
use atlatl_core::plan::PlanRequest;
use atlatl_core::template::{EnvSource, MapEnv};
use atlatl_run::serve::{BootstrapArgs, bootstrap_cmd};
use atlatl_run::{RunArgs, run};
use atlatl_testkit::{LocalLake, Postgres, TempProject};
use tokio_util::sync::CancellationToken;

const REPS: &str = r#"replications:
  serve:
    source: lake
    target: app
    defaults: {mode: cdc}
    streams:
      - id: s/entity
        from: {table: src.entity}
        object: dagster.entity
        primary_key: [object_id]
        columns:
          include: [object_id, name, kind]
          kind: {target_type: dagster.kind, transform: "lower(kind)"}
      - id: s/event
        after: [s/entity]
        from: {table: src.event}
        object: dagster.event
        primary_key: [id]
        columns: {include: [id, entity_id, v]}
        deletes: {mode: hard}
"#;

struct Env {
    lake: LocalLake,
    project: TempProject,
    pg: atlatl_conn_postgres::ConnectOptions,
}

async fn pg_exec(e: &Env, sql: &str) {
    let s = atlatl_conn_postgres::connect(&e.pg).await.expect("pg");
    s.client().batch_execute(sql).await.expect(sql);
}

async fn pg_rows(e: &Env, sql: &str) -> Vec<String> {
    let s = atlatl_conn_postgres::connect(&e.pg).await.expect("pg");
    s.client()
        .query(sql, &[])
        .await
        .expect(sql)
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect()
}

fn lake(e: &Env, sql: &str) {
    e.lake
        .open("lake")
        .expect("open")
        .execute_batch(sql)
        .expect(sql);
}

const PG: &str = "CREATE SCHEMA dagster;
CREATE TYPE dagster.kind AS ENUM ('a', 'b');
CREATE TABLE dagster.entity (object_id bigint PRIMARY KEY, name text, kind dagster.kind, deleted_at timestamptz, inserted_at timestamptz DEFAULT now());
CREATE TABLE dagster.event (id bigint PRIMARY KEY, entity_id bigint REFERENCES dagster.entity (object_id), v text);
INSERT INTO dagster.entity (object_id, name, kind) VALUES (1, 'stale', 'a'), (99, 'gone', 'b');";

async fn setup(pg: &'static Postgres) -> Env {
    setup_with(
        pg,
        "CREATE SCHEMA lake.src;
         CREATE TABLE lake.src.entity AS SELECT * FROM (VALUES (1, 'one', 'A'), (2, 'two', 'B'), (3, 'three', 'A')) t(object_id, name, kind);
         CREATE TABLE lake.src.event AS SELECT * FROM (VALUES (10, 1, 'x'), (11, 3, 'y')) t(id, entity_id, v);",
        REPS,
        PG,
    )
    .await
}

async fn setup_with(pg: &'static Postgres, lake_sql: &str, reps: &str, pg_sql: &str) -> Env {
    let lake = LocalLake::file().expect("lake");
    lake.open("lake")
        .expect("open")
        .execute_batch(lake_sql)
        .expect("seed");
    let db = pg.fresh_db().expect("db");
    let mut opts = atlatl_conn_postgres::ConnectOptions::new(
        &pg.host,
        pg.port,
        &db,
        &pg.user,
        Some(pg.password.clone()),
    );
    opts.sslmode = atlatl_core::config::model::SslMode::Disable;
    let conns = format!(
        "{}  app: {{type: postgres, host: {}, port: {}, database: {db}, user: {}, password: \"{}\", sslmode: disable}}\n",
        lake.connection_yaml("lake", "lake"),
        pg.host,
        pg.port,
        pg.user,
        pg.password
    );
    let project = TempProject::new()
        .and_then(|p| p.project(&conns, ""))
        .and_then(|p| p.replication("serve", reps))
        .expect("project");
    let e = Env {
        lake,
        project,
        pg: opts,
    };
    pg_exec(&e, pg_sql).await;
    e
}

fn project(e: &Env) -> Arc<atlatl_core::config::load::Project> {
    let (p, d) = atlatl_core::config::load(e.project.path());
    assert!(!d.has_errors(), "{d:?}");
    Arc::new(p)
}

fn vars() -> Arc<dyn EnvSource> {
    Arc::new(MapEnv::default())
}

async fn go(e: &Env) -> RunReport {
    let args = RunArgs {
        plan: PlanRequest::all(),
        extension_dir: Some(atlatl_testkit::extension_dir()),
        ..Default::default()
    };
    run(
        project(e),
        args,
        vars(),
        CancellationToken::new(),
        || None,
        |_, _| {},
    )
    .await
    .expect("run")
}

async fn boot(e: &Env, dry_run: bool) -> serde_json::Value {
    try_boot(e, dry_run).await.expect("bootstrap")
}

async fn try_boot(e: &Env, dry_run: bool) -> atlatl_core::error::Result<serde_json::Value> {
    let a = BootstrapArgs {
        replication: "serve".into(),
        env: None,
        mode: atlatl_cdc::bootstrap::Mode::Adopt,
        at_snapshot: None,
        dry_run,
        advance_cursor_only: false,
        extension_dir: Some(atlatl_testkit::extension_dir()),
    };
    bootstrap_cmd(project(e), a, vars()).await
}

fn member<'a>(r: &'a RunReport, stream: &str) -> &'a MemberReport {
    r.replications[0]
        .streams
        .iter()
        .find(|s| s.stream_id == stream)
        .and_then(|s| s.members.first())
        .expect("member")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_then_serve_changes() {
    let e = setup(atlatl_testkit::pg16()).await;

    let r = go(&e).await;
    let err = r.replications[0].error.clone().expect("needs bootstrap");
    assert_eq!(err.code.as_deref(), Some("bootstrap_required"), "{err:?}");

    let d = boot(&e, true).await;
    assert_eq!(
        (
            d["streams"][0]["upserts"].as_u64(),
            d["streams"][0]["deletes"].as_u64()
        ),
        (Some(3), Some(1)),
        "{d}"
    );
    assert_eq!(d["streams"][1]["upserts"], 2);
    assert!(
        d["streams"][0]["samples"]
            .as_array()
            .is_some_and(|s| !s.is_empty())
    );
    assert_eq!(
        pg_rows(&e, "SELECT name FROM dagster.entity ORDER BY object_id").await,
        ["stale", "gone"],
        "dry run writes nothing"
    );

    let d = boot(&e, false).await;
    assert_eq!(d["streams"][0]["rows_inserted"], 2, "{d}");
    assert_eq!(d["streams"][0]["rows_updated"], 1);
    assert_eq!(
        pg_rows(&e, "SELECT object_id || ':' || name || ':' || kind || ':' || (deleted_at IS NOT NULL) FROM dagster.entity ORDER BY object_id").await,
        ["1:one:a:false", "2:two:b:false", "3:three:a:false", "99:gone:b:true"]
    );
    assert_eq!(
        pg_rows(&e, "SELECT mode FROM atlatl.cursors ORDER BY stream_id").await,
        ["bootstrap", "bootstrap"]
    );

    let r = go(&e).await;
    assert_eq!(
        r.replications[0].status,
        ReplicationStatus::UpToDate,
        "{:?}",
        r.replications[0].error
    );

    lake(
        &e,
        "UPDATE lake.src.entity SET name = 'TWO' WHERE object_id = 2;
         DELETE FROM lake.src.entity WHERE object_id = 1; INSERT INTO lake.src.entity VALUES (1, 'one', 'A');
         DELETE FROM lake.src.event WHERE entity_id = 3; DELETE FROM lake.src.entity WHERE object_id = 3;
         INSERT INTO lake.src.event VALUES (12, 2, 'z');",
    );
    let r = go(&e).await;
    assert_eq!(
        r.replications[0].status,
        ReplicationStatus::Succeeded,
        "{:?}",
        r.replications[0].error
    );
    let ent = member(&r, "s/entity");
    let cdc = ent.cdc.clone().expect("cdc");
    assert_eq!(
        (cdc.rows_inserted, cdc.rows_updated, cdc.rows_deleted),
        (0, 1, 1),
        "{cdc:?}"
    );
    assert_eq!(
        cdc.touched_keys, 3,
        "delete+insert of 1 is touched but unchanged"
    );
    assert!(cdc.snapshots_behind >= 1 && cdc.window.from_snapshot < cdc.window.to_snapshot);
    let ev = member(&r, "s/event").cdc.clone().expect("cdc");
    assert_eq!((ev.rows_inserted, ev.rows_deleted), (1, 1));
    assert_eq!(
        pg_rows(&e, "SELECT object_id || ':' || name || ':' || (deleted_at IS NOT NULL) FROM dagster.entity ORDER BY object_id").await,
        ["1:one:false", "2:TWO:false", "3:three:true", "99:gone:true"]
    );
    assert_eq!(
        pg_rows(&e, "SELECT id::text FROM dagster.event ORDER BY id").await,
        ["10", "12"]
    );
    let stamped = pg_rows(
        &e,
        "SELECT (deleted_at > now() - interval '1 hour' AND deleted_at <= now())::text FROM dagster.entity WHERE object_id = 3",
    )
    .await;
    assert_eq!(
        stamped,
        ["true"],
        "soft delete stamped with the snapshot time"
    );
    let cursors = pg_rows(
        &e,
        "SELECT mode || ':' || snapshot_id FROM atlatl.cursors ORDER BY stream_id",
    )
    .await;
    let to = cdc.window.to_snapshot.expect("to");
    assert_eq!(
        cursors,
        [format!("cdc:{to}"), format!("cdc:{to}")],
        "one E for every stream"
    );

    lake(&e, "INSERT INTO lake.src.entity VALUES (3, 'back', 'B');");
    let r = go(&e).await;
    assert_eq!(
        member(&r, "s/entity").cdc.as_ref().map(|c| c.rows_updated),
        Some(1)
    );
    assert_eq!(
        pg_rows(
            &e,
            "SELECT name || ':' || (deleted_at IS NULL) FROM dagster.entity WHERE object_id = 3"
        )
        .await,
        ["back:true"],
        "restore"
    );
    assert_eq!(
        go(&e).await.replications[0].status,
        ReplicationStatus::UpToDate
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adopt_compares_guarded_decimals_exactly() {
    let e = setup_with(
        atlatl_testkit::pg16(),
        "CREATE SCHEMA lake.src;
         CREATE TABLE lake.src.m AS SELECT * FROM (VALUES
           (1::BIGINT, 12345678901234567.88::DECIMAL(20,2)), (2, 1.50)) t(id, amt);",
        r#"replications:
  serve:
    source: lake
    target: app
    streams:
      - id: s/m
        mode: cdc
        from: {table: src.m}
        object: app.m
        primary_key: [id]
        columns: {include: [id, amt]}
        deletes: {mode: hard}
"#,
        "CREATE SCHEMA app;
         CREATE TABLE app.m (id bigint PRIMARY KEY, amt numeric(20,2));
         INSERT INTO app.m VALUES (1, 12345678901234567.89), (2, 1.5);",
    )
    .await;

    let d = boot(&e, true).await;
    assert_eq!(d["streams"][0]["upserts"], 1, "past double precision: {d}");
    let d = boot(&e, false).await;
    assert_eq!(d["streams"][0]["rows_updated"], 1, "{d}");
    assert_eq!(
        pg_rows(&e, "SELECT amt::text FROM app.m ORDER BY id").await,
        ["12345678901234567.88", "1.50"]
    );

    pg_exec(&e, "UPDATE app.m SET amt = 'NaN' WHERE id = 2;").await;
    let err = try_boot(&e, true).await.expect_err("NaN sways adopt");
    assert!(
        err.to_string()
            .contains("column `amt` of `app.m` holds NaN, which a decimal can't hold"),
        "{err}"
    );
}
