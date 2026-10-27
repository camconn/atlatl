//! M8.7: `atlatl run --dry-run` stages and validates every selected ingest stream, diffs it
//! against its target and commits nothing (03 §11).

use std::sync::Arc;

use atlatl_core::obs::report::{DiffReport, HopStatus, MemberReport, ReplicationStatus, RunReport};
use atlatl_core::plan::PlanRequest;
use atlatl_core::template::{EnvSource, MapEnv};
use atlatl_run::dry::DryRun;
use atlatl_run::{RunArgs, run};
use atlatl_testkit::{LocalLake, TempProject};
use serde_json::json;
use tokio_util::sync::CancellationToken;

const REPS: &str = r#"replications:
  full:
    source: lake
    target: lake
    streams:
      - {id: d/full, mode: full-refresh, from: {table: src.people}, object: br.people_pk, primary_key: [id]}
      - {id: d/nopk, mode: full-refresh, from: {table: src.people}, object: br.people_nopk}
      - {id: d/new, mode: incremental, from: {table: src.people}, object: br.people_new, primary_key: [id]}
  inc:
    source: lake
    target: lake
    streams:
      - {id: d/uk, mode: incremental, from: {table: src.events}, object: br.events, primary_key: [id], update_key: updated_at}
  chunked:
    source: lake
    target: lake
    streams:
      - {id: d/chunked, mode: full-refresh, from: {table: src.big}, object: br.big, primary_key: [id], chunk: {by: hash, columns: [id], buckets: 4}}
  dups:
    source: lake
    target: lake
    streams:
      - {id: d/dups, mode: incremental, from: {table: src.dups}, object: br.dups, primary_key: [id]}
  tenants:
    source: lake
    target: lake
    for_each: {over: [acme, beta], as: tenant}
    streams:
      - {id: d/tenant, mode: full-refresh, from: {query: "SELECT '{{ tenant }}' AS tenant, 1 AS n"}, object: "br.t_{{ tenant }}", primary_key: [tenant]}
  hopped:
    source: lake
    target: lake
    streams:
      - {id: d/hop, mode: full-refresh, from: {table: src.people}, object: br.hop_people, primary_key: [id]}
    then:
      - target: app
        streams:
          - {id: d/serve, mode: cdc, from: {table: br.hop_people}, object: public.people, primary_key: [id]}
  served:
    source: lake
    target: app
    streams:
      - {id: d/served, mode: cdc, from: {table: br.people_pk}, object: public.people_pk, primary_key: [id]}
"#;

struct Env {
    lake: LocalLake,
    project: TempProject,
}

fn setup() -> Env {
    let lake = LocalLake::file().expect("lake");
    lake.open("lake")
        .expect("open")
        .execute_batch(
            "CREATE SCHEMA lake.src;
             CREATE TABLE lake.src.people AS SELECT range AS id, 'p' || range AS name FROM range(1, 11);
             CREATE TABLE lake.src.events AS SELECT range AS id, TIMESTAMP '2025-01-01' + INTERVAL (range) HOUR AS updated_at, 'v1' AS v FROM range(1, 6);
             CREATE TABLE lake.src.big AS SELECT range AS id, md5(range::VARCHAR) AS h FROM range(1000);
             CREATE TABLE lake.src.dups AS SELECT * FROM (VALUES (1, 'a'), (1, 'b')) t(id, v);
             CREATE SCHEMA lake.br;
             CREATE TABLE lake.br.people_pk AS SELECT * FROM lake.src.people WHERE id < 10;
             UPDATE lake.br.people_pk SET name = 'old' WHERE id = 1;
             INSERT INTO lake.br.people_pk VALUES (99, 'gone');
             CREATE TABLE lake.br.people_nopk AS SELECT * FROM lake.src.people LIMIT 3;
             CREATE TABLE lake.br.big AS SELECT * FROM lake.src.big WHERE id < 900;
             UPDATE lake.br.big SET h = 'x' WHERE id < 5;
             INSERT INTO lake.br.big VALUES (5000, 'gone');",
        )
        .expect("seed");
    let conns = format!(
        "{}  app: {{type: postgres, host: 127.0.0.1, port: 9, database: app, user: u, password: p, sslmode: disable}}\n",
        lake.connection_yaml("lake", "lake")
    );
    let project = TempProject::new()
        .and_then(|p| p.project(&conns, "state: {lake: lake}\n"))
        .and_then(|p| p.replication("all", REPS))
        .expect("project");
    Env { lake, project }
}

fn args(reps: &[&str], dry: bool) -> RunArgs {
    RunArgs {
        plan: PlanRequest {
            replications: reps.iter().map(|r| (*r).to_owned()).collect(),
            ..PlanRequest::all()
        },
        extension_dir: Some(atlatl_testkit::extension_dir()),
        dry_run: dry.then(DryRun::default),
        ..Default::default()
    }
}

async fn try_go(e: &Env, a: RunArgs) -> atlatl_core::error::Result<RunReport> {
    run_in(&e.project, a).await
}

async fn run_in(project: &TempProject, a: RunArgs) -> atlatl_core::error::Result<RunReport> {
    let (p, d) = atlatl_core::config::load(project.path());
    assert!(!d.has_errors(), "{d:?}");
    let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::default());
    run(
        Arc::new(p),
        a,
        vars,
        CancellationToken::new(),
        || None,
        |_, _| {},
    )
    .await
}

async fn go(e: &Env, reps: &[&str], dry: bool) -> RunReport {
    try_go(e, args(reps, dry)).await.expect("run")
}

fn status(r: &RunReport, name: &str) -> ReplicationStatus {
    let x = r
        .replications
        .iter()
        .find(|x| x.replication == name)
        .unwrap_or_else(|| panic!("no {name}"));
    x.status
}

fn member<'a>(r: &'a RunReport, stream: &str) -> &'a MemberReport {
    r.replications
        .iter()
        .flat_map(|x| &x.streams)
        .find(|s| s.stream_id == stream)
        .and_then(|s| s.members.first())
        .unwrap_or_else(|| panic!("no {stream}"))
}

fn diff<'a>(r: &'a RunReport, stream: &str) -> &'a DiffReport {
    member(r, stream)
        .diff
        .as_ref()
        .unwrap_or_else(|| panic!("{stream}: no diff"))
}

fn counts(d: &DiffReport) -> (u64, u64, u64, u64) {
    (d.inserts, d.updates, d.deletes, d.unchanged)
}

fn q(e: &Env, sql: &str) -> String {
    let c = e.lake.open("lake").expect("open");
    c.query_row(sql, [], |r| r.get::<_, String>(0)).expect(sql)
}

fn head(e: &Env) -> String {
    q(
        e,
        "SELECT max(snapshot_id)::VARCHAR FROM ducklake_snapshots('lake')",
    )
}

fn tables(e: &Env) -> String {
    q(
        e,
        "SELECT string_agg(schema_name || '.' || table_name, ',' ORDER BY 1) FROM duckdb_tables() WHERE database_name = 'lake'",
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diffs_every_stream_and_commits_nothing() {
    let e = setup();
    let (snap, before) = (head(&e), tables(&e));
    let r = go(&e, &["full", "chunked", "tenants", "hopped"], true).await;
    assert!(r.dry_run);
    for rep in [
        "full",
        "chunked",
        "tenants[acme]",
        "tenants[beta]",
        "hopped",
    ] {
        assert_eq!(
            status(&r, rep),
            ReplicationStatus::Succeeded,
            "{rep}: {:?}",
            r.replications.iter().map(|x| &x.error).collect::<Vec<_>>()
        );
    }
    assert_eq!(r.exit_code, 0);
    assert_eq!(
        head(&e),
        snap,
        "no snapshot: no data, no state, no run history"
    );
    assert_eq!(tables(&e), before, "nothing created, `_atlatl` included");

    let d = diff(&r, "d/full");
    assert_eq!((d.mode.as_str(), d.target_rows), ("full-refresh", 10));
    assert_eq!(counts(d), (1, 1, 1, 8));
    assert_eq!(
        d.samples,
        [
            json!({"op": "insert", "key": {"id": 10}}),
            json!({"op": "update", "key": {"id": 1}, "changed": ["name"], "staged": {"name": "p1"}, "target": {"name": "old"}}),
            json!({"op": "delete", "key": {"id": 99}}),
        ]
    );
    assert!(d.counts_only.is_none() && d.ddl.is_empty());
    let m = member(&r, "d/full");
    assert_eq!(
        (m.rows.staged, m.rows.written(), m.lake_snapshot),
        (10, 0, None)
    );

    let d = diff(&r, "d/nopk");
    assert_eq!((d.inserts, d.deletes, d.target_rows), (10, 3, 3));
    assert!(
        d.counts_only
            .as_deref()
            .is_some_and(|w| w.contains("no primary_key"))
            && d.samples.is_empty(),
        "{d:?}"
    );

    let d = diff(&r, "d/new");
    assert_eq!((counts(d), d.target_rows), ((10, 0, 0, 0), 0));
    assert_eq!(d.ddl.len(), 1);
    assert!(
        d.ddl[0].starts_with("CREATE TABLE") && d.ddl[0].contains("people_new"),
        "{:?}",
        d.ddl
    );

    let m = member(&r, "d/chunked");
    assert_eq!(m.chunks, 4);
    assert_eq!(
        counts(m.diff.as_ref().expect("diff")),
        (100, 5, 1, 895),
        "chunk by chunk, deletes against every chunk's keys"
    );

    for t in ["tenants[acme]", "tenants[beta]"] {
        let x = r.replications.iter().find(|x| x.replication == t).expect(t);
        let d = x.streams[0].members[0].diff.as_ref().expect("diff");
        assert_eq!((d.inserts, d.ddl.len()), (1, 1), "{t}");
    }

    // `then:`: the lake hop only; the serving hop reads its commit.
    assert_eq!(counts(diff(&r, "d/hop")), (10, 0, 0, 0));
    assert!(member(&r, "d/serve").diff.is_none());
    let hopped = r
        .replications
        .iter()
        .find(|x| x.replication == "hopped")
        .expect("hopped");
    assert!(
        hopped
            .hops
            .iter()
            .all(|h| h.status == HopStatus::NotStarted)
    );
    assert!(
        r.warnings
            .iter()
            .any(|w| w.code == "dry_run_hops"
                && w.message.contains("cdc bootstrap hopped --dry-run")),
        "{:?}",
        r.warnings
    );

    let v = atlatl_run::dry::render(&r);
    assert_eq!(v["dry_run"], true);
    let full = v["replications"]
        .as_array()
        .and_then(|a| a.iter().find(|x| x["replication"] == "full"))
        .expect("full");
    assert_eq!(full["dry_run"], true);
    let s = &full["streams"][0];
    assert_eq!(
        (
            &s["stream"],
            &s["target"],
            &s["inserts"],
            &s["updates"],
            &s["deletes"]
        ),
        (
            &json!("d/full"),
            &json!("\"lake\".\"br\".\"people_pk\""),
            &json!(1),
            &json!(1),
            &json!(1)
        ),
        "{s:#}"
    );
    let serve = v["replications"]
        .as_array()
        .and_then(|a| a.iter().find(|x| x["replication"] == "hopped"))
        .map(|x| x["streams"].clone())
        .expect("hopped");
    assert!(
        serve
            .as_array()
            .is_some_and(|a| a.contains(&json!({"stream": "d/serve", "not_run": true}))),
        "{serve:#}"
    );
    assert!(atlatl_run::dry::summary(&r).contains("d/full"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cursors_are_read_not_advanced() {
    let e = setup();
    let r = go(&e, &["inc"], false).await;
    assert_eq!(status(&r, "inc"), ReplicationStatus::Succeeded);
    let state = || {
        q(
            &e,
            "SELECT value::VARCHAR || ' ' || run_id FROM lake._atlatl.stream_state WHERE stream_id = 'd/uk'",
        )
    };
    let stored = state();

    let r = go(&e, &["inc"], true).await;
    assert_eq!(
        status(&r, "inc"),
        ReplicationStatus::UpToDate,
        "nothing new"
    );
    assert_eq!(counts(diff(&r, "d/uk")), (0, 0, 0, 1));

    e.lake
        .open("lake")
        .expect("open")
        .execute_batch(
            "INSERT INTO lake.src.events VALUES (6, TIMESTAMP '2025-01-01 07:00:00', 'v1');
             UPDATE lake.src.events SET v = 'v2' WHERE id = 5;",
        )
        .expect("more");
    let snap = head(&e);
    let r = go(&e, &["inc"], true).await;
    assert_eq!(status(&r, "inc"), ReplicationStatus::Succeeded);
    let m = member(&r, "d/uk");
    assert_eq!(counts(m.diff.as_ref().expect("diff")), (1, 1, 0, 0));
    assert_eq!(
        m.state.before.as_ref().map(|v| v["value"].clone()),
        Some(json!("2025-01-01 05:00:00"))
    );
    assert_eq!(
        m.state.after.as_ref().map(|v| v["value"].clone()),
        Some(json!("2025-01-01 07:00:00")),
        "the cursor a run would store"
    );
    assert_eq!(state(), stored, "not advanced");
    assert_eq!(head(&e), snap);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validation_failures_fail_the_dry_run() {
    let e = setup();
    let snap = head(&e);
    let r = go(&e, &["dups", "full"], true).await;
    assert_eq!(status(&r, "dups"), ReplicationStatus::Failed);
    let err = r
        .replications
        .iter()
        .find(|x| x.replication == "dups")
        .and_then(|x| x.error.clone())
        .expect("error");
    assert_eq!(err.code.as_deref(), Some("duplicate_primary_key"));
    assert_eq!(status(&r, "full"), ReplicationStatus::Succeeded);
    assert_eq!(r.exit_code, 1);
    assert_eq!(head(&e), snap);
    let v = atlatl_run::dry::render(&r);
    let dups = v["replications"]
        .as_array()
        .and_then(|a| a.iter().find(|x| x["replication"] == "dups"))
        .expect("dups");
    assert_eq!(dups["status"], "failed");
    assert_eq!(dups["error"]["code"], "duplicate_primary_key");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serving_replications_point_at_cdc_bootstrap() {
    let e = setup();
    let err = try_go(&e, args(&["served", "full"], true))
        .await
        .expect_err("serving");
    assert_eq!(err.class, atlatl_core::error::ErrorClass::Config);
    assert_eq!(err.code.as_deref(), Some("dry_run_unsupported"));
    assert!(
        err.message.contains("`served` (ducklake → postgres cdc)")
            && err
                .message
                .contains("atlatl cdc bootstrap <replication> --dry-run"),
        "{err}"
    );
}

// ----------------------------------------------------------------------------- missing catalogs

const LOAD: &str = r#"replications:
  load:
    source: files
    target: lake
    streams:
      - {id: n/prices, mode: full-refresh, from: {files: {url: in, glob: "*.csv"}}, object: br.prices, primary_key: [ticker]}
"#;

fn files_project(lake_yaml: &str) -> TempProject {
    TempProject::new()
        .and_then(|p| {
            p.project(
                &format!("{lake_yaml}  files: {{type: files}}\n"),
                "state: {lake: lake}\n",
            )
        })
        .and_then(|p| p.replication("load", LOAD))
        .and_then(|p| p.file("in/prices.csv", "ticker,px\nAAPL,1.5\nMSFT,2\nNVDA,3\n"))
        .expect("project")
}

/// A dry run into a lake whose catalog doesn't exist: every row is an insert, with a warning.
fn assert_empty_target_diff(r: &RunReport) {
    assert_eq!(
        status(r, "load"),
        ReplicationStatus::Succeeded,
        "{:?}",
        r.replications[0].error
    );
    let d = diff(r, "n/prices");
    assert_eq!((counts(d), d.target_rows), ((3, 0, 0, 0), 0));
    assert!(d.ddl.iter().any(|x| x.starts_with("CREATE TABLE")), "{d:?}");
    assert!(
        r.warnings
            .iter()
            .any(|w| w.code == "dry_run_no_catalog" && w.message.contains("lake `lake`")),
        "{:?}",
        r.warnings
    );
}

fn entries(dir: &std::path::Path) -> Vec<String> {
    std::fs::read_dir(dir)
        .expect("dir")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_file_catalog_is_not_created() {
    let lake = LocalLake::file().expect("lake");
    let project = files_project(&lake.connection_yaml("lake", "lake"));
    let meta = lake.dir.path().join("meta.ducklake");
    let before = entries(lake.dir.path());

    let r = run_in(&project, args(&["load"], true))
        .await
        .expect("dry run");
    assert_empty_target_diff(&r);
    assert!(!meta.exists(), "no catalog file");
    assert_eq!(entries(lake.dir.path()), before);
    assert!(
        entries(&lake.dir.path().join("data")).is_empty(),
        "no data files"
    );

    // A real run still creates the catalog; then the dry run reads it (read-only) as usual.
    let r = run_in(&project, args(&["load"], false)).await.expect("run");
    assert_eq!(status(&r, "load"), ReplicationStatus::Succeeded);
    assert!(meta.exists());
    let e = Env { lake, project };
    assert_eq!(q(&e, "SELECT count(*)::VARCHAR FROM lake.br.prices"), "3");
    let snap = head(&e);
    let r = try_go(&e, args(&["load"], true)).await.expect("dry run");
    assert_eq!(status(&r, "load"), ReplicationStatus::UpToDate);
    assert!(r.warnings.iter().all(|w| w.code != "dry_run_no_catalog"));
    assert_eq!(head(&e), snap);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_catalog_directory_fails_like_a_run() {
    let lake = LocalLake::file().expect("lake");
    let yaml = lake
        .connection_yaml("lake", "lake")
        .replace("meta.ducklake", "nope/meta.ducklake");
    let err = run_in(&files_project(&yaml), args(&["load"], true))
        .await
        .expect_err("no directory");
    assert_eq!(err.class, atlatl_core::error::ErrorClass::Config);
    assert!(
        err.message.contains("doesn't create missing directories"),
        "{err}"
    );
}

async fn pg_tables(pg: &atlatl_testkit::Postgres, db: &str) -> i64 {
    let (c, conn) = tokio_postgres::connect(&pg.dsn(db), tokio_postgres::NoTls)
        .await
        .expect("connect");
    let h = tokio::spawn(conn);
    let n: i64 = c
        .query_one(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg_toast%'",
            &[],
        )
        .await
        .expect("count")
        .get(0);
    drop(c);
    let _ = h.await;
    n
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_postgres_catalog_is_not_created() {
    let pg = atlatl_testkit::pg16();
    let db = pg.fresh_db().expect("db");
    let data = tempfile::tempdir().expect("data");
    let yaml = |password: &str| {
        format!(
            "  lake:\n    type: ducklake\n    alias: lake\n    catalog: {{type: postgres, host: {}, port: {}, database: {db}, user: {}, password: \"{password}\", sslmode: disable}}\n    data_path: \"{}/\"\n    storage: {{type: local}}\n",
            pg.host,
            pg.port,
            pg.user,
            data.path().display()
        )
    };
    let project = files_project(&yaml(&pg.password));
    let r = run_in(&project, args(&["load"], true))
        .await
        .expect("dry run");
    assert_empty_target_diff(&r);
    assert!(
        r.warnings.iter().any(|w| w.message.contains(&db)),
        "{:?}",
        r.warnings
    );
    assert_eq!(pg_tables(pg, &db).await, 0, "no catalog tables");
    assert!(entries(data.path()).is_empty());

    // Unreachable is not missing.
    let err = run_in(&files_project(&yaml("wrong")), args(&["load"], true))
        .await
        .expect_err("auth");
    assert!(
        err.message.contains("password authentication failed"),
        "{err}"
    );

    let r = run_in(&project, args(&["load"], false)).await.expect("run");
    assert_eq!(status(&r, "load"), ReplicationStatus::Succeeded);
    assert!(
        pg_tables(pg, &db).await > 0,
        "a real run creates the catalog"
    );
    let r = run_in(&project, args(&["load"], true))
        .await
        .expect("dry run");
    assert_eq!(status(&r, "load"), ReplicationStatus::UpToDate);
    assert!(r.warnings.iter().all(|w| w.code != "dry_run_no_catalog"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_catalog_on_s3_writes_no_objects() {
    let s3 = atlatl_testkit::rustfs();
    let bucket = s3.fresh_bucket().expect("bucket");
    let dir = tempfile::tempdir().expect("dir");
    let yaml = format!(
        "  lake:\n    type: ducklake\n    alias: lake\n    catalog: {{type: duckdb, path: \"{}/meta.ducklake\"}}\n    data_path: \"s3://{bucket}/lake/\"\n    storage: {{type: s3, provider: config, region: {}, endpoint: \"http://{}\", url_style: path, key_id: {}, secret: {}}}\n",
        dir.path().display(),
        s3.region,
        s3.endpoint,
        s3.key_id,
        s3.secret
    );
    let r = run_in(&files_project(&yaml), args(&["load"], true))
        .await
        .expect("dry run");
    assert_empty_target_diff(&r);
    assert!(entries(dir.path()).is_empty());
    let (st, body) = s3.request("GET", &format!("/{bucket}"), "").expect("list");
    assert_eq!(st, 200, "{body}");
    assert!(!body.contains("<Key>"), "{body}");
}
