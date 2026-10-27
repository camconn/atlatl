//! M1.9: lake → Postgres CDC on PG 12 and 16 with the `org_event` (hub + shards, FKs)
//! and `metric` (no include, source tombstones) shapes.

use std::sync::Arc;

use atlatl_cdc::bootstrap::Mode;
use atlatl_core::obs::report::{CdcReport, ReplicationStatus, RunReport};
use atlatl_core::plan::PlanRequest;
use atlatl_core::template::{EnvSource, MapEnv};
use atlatl_run::serve::{BootstrapArgs, bootstrap_cmd};
use atlatl_run::{RunArgs, run};
use atlatl_testkit::{LocalLake, Postgres, TempProject};
use tokio_util::sync::CancellationToken;

const REPS: &str = r#"replications:
  org_event:
    source: lake
    target: app_pg
    defaults:
      mode: cdc
      deletes: {mode: soft, column: deleted_at}
    streams:
      - id: load/org_event
        from: {table: au.org_event}
        object: app.org_event
        primary_key: [object_id]
        diff: {ignore_columns: [updated_at]}
        columns:
          include: [object_id, org_id, type, date, updated_at]
          type: {target_type: app.event_type}
      - id: load/org_event_funding
        after: [load/org_event]
        from: {table: au.org_event_funding}
        object: app.org_event_funding
        primary_key: [event_id]
        where: "name IS NOT NULL"
        columns:
          include: [event_id, name, total_raised, unlinked_names, updated_at, deleted_at]
          unlinked_names: {target_type: "text[]"}
      - id: load/org_event_rating
        after: [load/org_event]
        from: {table: au.org_event_rating}
        object: app.org_event_rating
        primary_key: [event_id]
        columns:
          include: [event_id, rating_current, updated_at]
          rating_current:
            transform: "trim(regexp_replace(upper(rating_current), '[^A-Z0-9]+', '_', 'g'), '_')"
            target_type: app.rating
  metric:
    source: lake
    target: app_pg
    streams:
      - id: load/metric
        from: {table: au.metric_feed}
        object: app.metric
        mode: cdc
        primary_key: [org_id, fiscal_year, periodicity]
        deletes: {mode: soft, column: deleted_at}
        columns:
          periodicity: {target_type: app.periodicity}
  bounded:
    source: lake
    target: app_pg
    streams:
      - id: load/bounded
        mode: cdc
        from: {table: au.b}
        object: app.b
        primary_key: [id]
        columns: {include: [id, v]}
        deletes: {mode: hard}
        max_snapshots: 1
  recreated:
    source: lake
    target: app_pg
    streams:
      - id: load/recreated
        mode: cdc
        from: {table: au.r}
        object: app.r
        primary_key: [id]
        columns: {include: [id, v]}
        deletes: {mode: hard}
        diff: {chunk_rows: 2}
"#;

const LAKE: &str = "CREATE SCHEMA lake.au;
CREATE TABLE lake.au.org_event AS SELECT * FROM (VALUES
  (1::BIGINT, 100::BIGINT, 'funding', DATE '2025-01-02', TIMESTAMP '2025-01-02 10:00'),
  (2, 100, 'rating', DATE '2025-01-03', TIMESTAMP '2025-01-03 10:00'),
  (3, 101, 'funding', DATE '2025-01-04', TIMESTAMP '2025-01-04 10:00')) t(object_id, org_id, type, date, updated_at);
CREATE TABLE lake.au.org_event_funding AS SELECT * FROM (VALUES
  (1::BIGINT, 'Seed', 1250000.500000::DECIMAL(24,6), ['a', 'b'], TIMESTAMP '2025-01-02 10:00', NULL::TIMESTAMPTZ),
  (3, 'Series A', 9000000::DECIMAL(24,6), []::VARCHAR[], TIMESTAMP '2025-01-04 10:00', NULL)) t(event_id, name, total_raised, unlinked_names, updated_at, deleted_at);
CREATE TABLE lake.au.org_event_rating AS SELECT * FROM (VALUES
  (2::BIGINT, 'buy', TIMESTAMP '2025-01-03 10:00')) t(event_id, rating_current, updated_at);
CREATE TABLE lake.au.metric_feed AS SELECT * FROM (VALUES
  (100::BIGINT, 2024, 'annual', 1.5::DOUBLE, '{\"src\": \"x\", \"n\": 12345678901234567}'::JSON, NULL::TIMESTAMPTZ),
  (100, 2024, 'quarterly', 0.25, '{}'::JSON, NULL)) t(org_id, fiscal_year, periodicity, value, meta, deleted_at);
CREATE TABLE lake.au.b AS SELECT 1 AS id, 'v0' AS v;
CREATE TABLE lake.au.r AS SELECT 1 AS id, 'v0' AS v;";

const PG: &str = "CREATE SCHEMA app;
CREATE TYPE app.event_type AS ENUM ('funding', 'rating', 'listing');
CREATE TYPE app.rating AS ENUM ('BUY', 'STRONG_BUY', 'HOLD');
CREATE TYPE app.periodicity AS ENUM ('annual', 'quarterly');
CREATE TABLE app.org_event (object_id bigint PRIMARY KEY, org_id bigint NOT NULL, type app.event_type NOT NULL,
  date date, updated_at timestamp, deleted_at timestamptz, inserted_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE app.org_event_funding (event_id bigint PRIMARY KEY REFERENCES app.org_event (object_id),
  name text, total_raised numeric(24,6), unlinked_names text[], updated_at timestamp, deleted_at timestamptz);
CREATE TABLE app.org_event_rating (event_id bigint PRIMARY KEY REFERENCES app.org_event (object_id),
  rating_current app.rating, updated_at timestamp, deleted_at timestamptz);
CREATE TABLE app.metric (org_id bigint, fiscal_year int, periodicity app.periodicity, value double precision, meta jsonb,
  deleted_at timestamptz, PRIMARY KEY (org_id, fiscal_year, periodicity));
CREATE TABLE app.b (id int PRIMARY KEY, v text);
CREATE TABLE app.r (id int PRIMARY KEY, v text);";

macro_rules! on_both {
    ($($name:ident),* $(,)?) => {$(
        mod $name {
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn pg12() { super::$name(atlatl_testkit::pg12()).await }
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn pg16() { super::$name(atlatl_testkit::pg16()).await }
        }
    )*};
}

on_both!(
    hub_and_shards,
    metric_tombstones,
    enum_failure_rolls_back_everything,
    bounded_windows_catch_up,
    recreated_table_needs_resync
);

struct Env {
    lake: LocalLake,
    project: TempProject,
    pg: atlatl_conn_postgres::ConnectOptions,
}

impl Env {
    async fn setup(pg: &'static Postgres) -> Self {
        let lake = LocalLake::file().expect("lake");
        lake.open("lake")
            .expect("open")
            .execute_batch(LAKE)
            .expect("lake seed");
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
            "{}  app_pg: {{type: postgres, host: {}, port: {}, database: {db}, user: {}, password: \"{}\", sslmode: disable}}\n",
            lake.connection_yaml("lake", "lake"),
            pg.host,
            pg.port,
            pg.user,
            pg.password
        );
        let project = TempProject::new()
            .and_then(|p| p.project(&conns, ""))
            .and_then(|p| p.replication("load", REPS))
            .expect("project");
        let e = Env {
            lake,
            project,
            pg: opts,
        };
        e.pg(PG).await;
        e
    }

    async fn pg(&self, sql: &str) {
        let s = atlatl_conn_postgres::connect(&self.pg).await.expect("pg");
        s.client().batch_execute(sql).await.expect(sql);
    }

    async fn rows(&self, sql: &str) -> Vec<String> {
        let s = atlatl_conn_postgres::connect(&self.pg).await.expect("pg");
        s.client()
            .query(sql, &[])
            .await
            .expect(sql)
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect()
    }

    fn lake(&self, sql: &str) {
        self.lake
            .open("lake")
            .expect("open")
            .execute_batch(sql)
            .expect(sql);
    }

    fn project(&self) -> Arc<atlatl_core::config::load::Project> {
        let (p, d) = atlatl_core::config::load(self.project.path());
        assert!(!d.has_errors(), "{d:?}");
        Arc::new(p)
    }

    async fn boot(&self, rep: &str, mode: Mode) -> serde_json::Value {
        let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::default());
        let a = BootstrapArgs {
            replication: rep.into(),
            env: None,
            mode,
            at_snapshot: None,
            dry_run: false,
            advance_cursor_only: false,
            extension_dir: Some(atlatl_testkit::extension_dir()),
        };
        bootstrap_cmd(self.project(), a, vars)
            .await
            .expect("bootstrap")
    }

    async fn run(&self, rep: &str) -> RunReport {
        let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::default());
        let args = RunArgs {
            plan: PlanRequest {
                replications: vec![rep.into()],
                ..PlanRequest::all()
            },
            extension_dir: Some(atlatl_testkit::extension_dir()),
            ..Default::default()
        };
        run(
            self.project(),
            args,
            vars,
            CancellationToken::new(),
            || None,
            |_, _| {},
        )
        .await
        .expect("run")
    }
}

fn cdc(r: &RunReport, stream: &str) -> CdcReport {
    r.replications[0]
        .streams
        .iter()
        .find(|s| s.stream_id == stream)
        .and_then(|s| s.members[0].cdc.clone())
        .expect("cdc")
}

fn ok(r: &RunReport) {
    assert_eq!(
        r.replications[0].status,
        ReplicationStatus::Succeeded,
        "{:?}",
        r.replications[0].error
    );
}

async fn hub_and_shards(pg: &'static Postgres) {
    let e = Env::setup(pg).await;
    let b = e.boot("org_event", Mode::Reload).await;
    assert_eq!(b["streams"][0]["rows_inserted"], 3, "{b}");
    assert_eq!(
        e.rows("SELECT event_id || ':' || total_raised || ':' || array_to_string(unlinked_names, ',') FROM app.org_event_funding ORDER BY 1").await,
        ["1:1250000.500000:a,b", "3:9000000.000000:"]
    );
    assert_eq!(
        e.rows("SELECT rating_current::text FROM app.org_event_rating")
            .await,
        ["BUY"]
    );

    e.lake(
        "UPDATE lake.au.org_event SET updated_at = TIMESTAMP '2025-02-01' WHERE object_id = 1;
         UPDATE lake.au.org_event_funding SET unlinked_names = ['a', 'b', 'c'] WHERE event_id = 1;
         UPDATE lake.au.org_event_funding SET name = NULL WHERE event_id = 3;
         UPDATE lake.au.org_event_rating SET rating_current = 'strong  buy!' WHERE event_id = 2;
         INSERT INTO lake.au.org_event VALUES (4, 102, 'listing', DATE '2025-02-02', TIMESTAMP '2025-02-02 10:00');",
    );
    let r = e.run("org_event").await;
    ok(&r);
    let hub = cdc(&r, "load/org_event");
    assert_eq!(
        (hub.rows_inserted, hub.rows_updated, hub.touched_keys),
        (1, 0, 2),
        "updated_at-only change is ignored: {hub:?}"
    );
    let fin = cdc(&r, "load/org_event_funding");
    assert_eq!(
        (fin.rows_updated, fin.rows_deleted),
        (1, 1),
        "leaving `where` is a delete: {fin:?}"
    );
    assert_eq!(cdc(&r, "load/org_event_rating").rows_updated, 1);
    assert_eq!(
        e.rows("SELECT rating_current::text FROM app.org_event_rating")
            .await,
        ["STRONG_BUY"]
    );
    assert_eq!(
        e.rows("SELECT event_id || ':' || coalesce(name, '-') || ':' || (deleted_at IS NOT NULL) || ':' || coalesce(array_length(unlinked_names, 1)::text, '') FROM app.org_event_funding ORDER BY 1").await,
        ["1:Seed:false:3", "3:Series A:true:"]
    );
    assert_eq!(
        e.rows("SELECT to_char(updated_at, 'YYYY-MM-DD') FROM app.org_event WHERE object_id = 1")
            .await,
        ["2025-01-02"],
        "ignored columns aren't pushed alone"
    );
    let snaps = e
        .rows("SELECT DISTINCT snapshot_id::text FROM atlatl.cursors")
        .await;
    assert_eq!(snaps.len(), 1, "every stream shares E");
}

async fn metric_tombstones(pg: &'static Postgres) {
    let e = Env::setup(pg).await;
    e.boot("metric", Mode::Adopt).await;
    assert_eq!(
        e.rows("SELECT (meta->>'n') FROM app.metric WHERE periodicity = 'annual'")
            .await,
        ["12345678901234567"],
        "json numbers are exact"
    );
    e.lake("UPDATE lake.au.metric_feed SET deleted_at = TIMESTAMPTZ '2025-03-01 00:00:00+00' WHERE periodicity = 'quarterly';");
    let r = e.run("metric").await;
    ok(&r);
    let m = cdc(&r, "load/metric");
    assert_eq!(
        (m.rows_updated, m.rows_deleted),
        (1, 0),
        "a served tombstone is data: {m:?}"
    );
    assert_eq!(e.rows("SELECT to_char(deleted_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') FROM app.metric WHERE periodicity = 'quarterly'").await, ["2025-03-01"]);
    e.lake("UPDATE lake.au.metric_feed SET deleted_at = NULL WHERE periodicity = 'quarterly';");
    ok(&e.run("metric").await);
    assert_eq!(
        e.rows("SELECT (deleted_at IS NULL)::text FROM app.metric WHERE periodicity = 'quarterly'")
            .await,
        ["true"]
    );
}

async fn enum_failure_rolls_back_everything(pg: &'static Postgres) {
    let e = Env::setup(pg).await;
    e.boot("org_event", Mode::Reload).await;
    let before = e.rows("SELECT string_agg(stream_id || '=' || snapshot_id, ',' ORDER BY stream_id) FROM atlatl.cursors").await;
    e.lake(
        "UPDATE lake.au.org_event SET org_id = 999 WHERE object_id = 2;
         UPDATE lake.au.org_event_rating SET rating_current = 'sell' WHERE event_id = 2;",
    );
    let r = e.run("org_event").await;
    assert_eq!(r.replications[0].status, ReplicationStatus::Failed);
    let err = r.replications[0].error.clone().expect("error");
    assert_eq!(err.class, atlatl_core::error::ErrorClass::Data, "{err:?}");
    assert_eq!(
        e.rows("SELECT org_id::text FROM app.org_event WHERE object_id = 2")
            .await,
        ["100"],
        "hub upsert rolled back"
    );
    assert_eq!(e.rows("SELECT string_agg(stream_id || '=' || snapshot_id, ',' ORDER BY stream_id) FROM atlatl.cursors").await, before);
}

async fn bounded_windows_catch_up(pg: &'static Postgres) {
    let e = Env::setup(pg).await;
    e.boot("bounded", Mode::Reload).await;
    for i in 1..=3 {
        e.lake(&format!("UPDATE lake.au.b SET v = 'v{i}'"));
    }
    let mut seen = Vec::new();
    for _ in 0..3 {
        let r = e.run("bounded").await;
        ok(&r);
        seen.push((
            r.replications[0].partial,
            e.rows("SELECT v FROM app.b").await[0].clone(),
        ));
    }
    assert_eq!(
        seen,
        [
            (true, "v1".to_owned()),
            (true, "v2".to_owned()),
            (false, "v3".to_owned())
        ]
    );
    assert_eq!(
        e.run("bounded").await.replications[0].status,
        ReplicationStatus::UpToDate
    );
}

async fn recreated_table_needs_resync(pg: &'static Postgres) {
    let e = Env::setup(pg).await;
    e.boot("recreated", Mode::Reload).await;
    e.lake("DROP TABLE lake.au.r; CREATE TABLE lake.au.r AS SELECT range::INT + 2 AS id, 'new' AS v FROM range(9);");
    let r = e.run("recreated").await;
    assert_eq!(
        r.replications[0]
            .error
            .as_ref()
            .and_then(|x| x.code.clone())
            .as_deref(),
        Some("resync_required")
    );
    let y = std::fs::read_to_string(e.project.path().join("replications/load.yaml")).expect("read");
    std::fs::write(
        e.project.path().join("replications/load.yaml"),
        y.replace(
            "        from: {table: au.r}\n",
            "        from: {table: au.r}\n        on_gap: resync\n",
        ),
    )
    .expect("write");
    // on_gap isn't part of the state hash, so the cursor still matches.
    let r = e.run("recreated").await;
    ok(&r);
    let c = cdc(&r, "load/recreated");
    assert!(c.resynced);
    assert_eq!(
        (c.rows_inserted, c.rows_deleted),
        (9, 1),
        "applied in PK-hash chunks of 2: {c:?}"
    );
    assert_eq!(
        e.rows("SELECT count(*)::text || ':' || min(id) FROM app.r")
            .await,
        ["9:2"]
    );
    assert_eq!(
        e.run("recreated").await.replications[0].status,
        ReplicationStatus::UpToDate
    );
}
