//! Redshift → lake through the runner (M5), against the local Data API fake and RustFS.

use std::sync::Arc;
use std::time::Duration;

use atlatl_core::obs::report::{ReplicationStatus, RowCounts, RunReport};
use atlatl_core::plan::PlanRequest;
use atlatl_core::template::{EnvSource, MapEnv};
use atlatl_run::{RunArgs, run};
use atlatl_testkit::redshift_fake::{Fake, KEY, REGION, SECRET};
use atlatl_testkit::{LocalLake, TempProject};
use tokio_util::sync::CancellationToken;

const REPS: &str = r#"replications:
  full:
    source: warehouse
    target: lake
    retry: {max_attempts: 2, base: 10ms, max: 50ms}
    streams:
      - {id: warehouse/cons_af, mode: full-refresh, from: {table: est_v2.est_core_cons_af}, object: br.cons_af, primary_key: [id]}
  inc:
    source: warehouse
    target: lake
    retry: {max_attempts: 2, base: 10ms, max: 50ms}
    streams:
      - {id: warehouse/act, mode: incremental, from: {table: est_v2.est_core_act_af}, object: br.act_af, primary_key: [id], update_key: {column: updated}}
  wild:
    source: warehouse
    target: lake
    retry: {max_attempts: 2, base: 10ms, max: 50ms}
    streams:
      - {id: warehouse/consf, mode: full-refresh, from: {tables: "est_v2.est_core_cons_*f"}, object: "br.{{ member.table }}", primary_key: [id]}
  chunked:
    source: warehouse
    target: lake
    retry: {max_attempts: 2, base: 10ms, max: 50ms}
    streams:
      - {id: warehouse/chunked, mode: full-refresh, from: {table: est_v2.est_core_cons_af}, object: br.cons_chunked, primary_key: [id], chunk: {by: files, size: 1B}}
  super:
    source: warehouse
    target: lake
    retry: {max_attempts: 2, base: 10ms, max: 50ms}
    streams:
      - {id: warehouse/sup_json, mode: full-refresh, from: {table: est_v2.est_core_sup}, object: br.sup_json, primary_key: [id], columns: {payload: {type: JSON}}}
      - {id: warehouse/sup_text, mode: full-refresh, from: {table: est_v2.est_core_sup}, object: br.sup_text, primary_key: [id], columns: {"*": {type: VARCHAR}}}
  super_undeclared:
    source: warehouse
    target: lake
    retry: {max_attempts: 2, base: 10ms, max: 50ms}
    streams:
      - {id: warehouse/sup_raw, mode: full-refresh, from: {table: est_v2.est_core_sup}, object: br.sup_raw, primary_key: [id]}
"#;

const TABLES: &str = r#"
CREATE SCHEMA est_v2;
CREATE TABLE est_v2.est_core_cons_af (id BIGINT, n DECIMAL(18,4), big DECIMAL(38,10), tz TIMESTAMPTZ, s VARCHAR);
INSERT INTO est_v2.est_core_cons_af SELECT range, (range * 1.2345)::DECIMAL(18,4),
  '12345678901234567890.1234567890'::DECIMAL(38,10), TIMESTAMPTZ '2024-01-31 12:34:56.123456+05', 'it''s ' || range
  FROM range(11);
CREATE TABLE est_v2.est_core_cons_qf (id BIGINT, v VARCHAR);
INSERT INTO est_v2.est_core_cons_qf VALUES (1, 'q');
CREATE TABLE est_v2.est_core_cons_rec (id BIGINT);
CREATE TABLE est_v2.est_core_act_af (id BIGINT, updated TIMESTAMP, v DECIMAL(18,2));
INSERT INTO est_v2.est_core_act_af VALUES (1, '2025-05-01 00:00:00', 1.10), (2, '2025-05-02 00:00:00', 2.20);
"#;

struct Env {
    lake: LocalLake,
    project: TempProject,
    fake: Fake,
    bucket: String,
}

async fn setup() -> Env {
    let fake = Fake::start().await;
    fake.sql(TABLES);
    let s3 = atlatl_testkit::rustfs();
    let bucket = s3.fresh_bucket().expect("bucket");
    let lake = LocalLake::file().expect("lake");
    let conns = format!(
        "{}  warehouse:
    type: redshift
    read_only: true
    data_api: {{workgroup: wg, database: dev, region: {REGION}, endpoint: \"{}\", key_id: {KEY}, secret: {SECRET}, poll: {{wait: 0s, initial: 5ms, max: 20ms}}}}
    unload:
      bucket: {bucket}
      iam_role: arn:aws:iam::000000000000:role/unload
      read_role: none
      storage: {{type: s3, region: {REGION}, endpoint: \"http://{}\", url_style: path, key_id: {}, secret: {}}}
",
        lake.connection_yaml("lake", "lake"),
        fake.endpoint,
        s3.endpoint,
        s3.key_id,
        s3.secret
    );
    let project = TempProject::new()
        .and_then(|p| p.project(&conns, "state: {lake: lake}\n"))
        .and_then(|p| p.replication("warehouse", REPS))
        .expect("project");
    Env {
        lake,
        project,
        fake,
        bucket,
    }
}

async fn run_reps(e: &Env, names: &[&str], cancel: CancellationToken) -> RunReport {
    let (p, d) = atlatl_core::config::load(e.project.path());
    assert!(!d.has_errors(), "{d:?}");
    let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::default());
    let args = RunArgs {
        plan: PlanRequest {
            replications: names.iter().map(|r| (*r).to_owned()).collect(),
            ..PlanRequest::all()
        },
        extension_dir: Some(atlatl_testkit::extension_dir()),
        ..Default::default()
    };
    run(Arc::new(p), args, vars, cancel, || None, |_, _| {})
        .await
        .expect("run")
}

async fn go(e: &Env, names: &[&str]) -> RunReport {
    let r = run_reps(e, names, CancellationToken::new()).await;
    for x in &r.replications {
        assert_eq!(
            x.status,
            ReplicationStatus::Succeeded,
            "{}: {:?}",
            x.replication,
            x.error
        );
    }
    r
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

fn q<T: duckdb::types::FromSql>(e: &Env, sql: &str) -> T {
    e.lake
        .open("lake")
        .expect("open")
        .query_row(sql, [], |r| r.get(0))
        .expect(sql)
}

async fn unload_objects(e: &Env) -> i64 {
    let s3 = atlatl_testkit::rustfs();
    let c = atlatl_testkit::lake::open_engine().expect("engine");
    c.execute_batch("LOAD httpfs").expect("httpfs");
    c.execute_batch(&s3.duckdb_secret("list", &e.bucket))
        .expect("secret");
    c.query_row(
        &format!(
            "SELECT count(*) FROM glob('s3://{}/atlatl_unload/**')",
            e.bucket
        ),
        [],
        |r| r.get(0),
    )
    .expect("glob")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_refresh_keeps_precision_and_cleans_up() {
    let e = setup().await;
    let r = go(&e, &["full"]).await;
    assert_eq!(rows(&r, "full").inserted, 11);
    let m = &r.replications[0].streams[0].members[0];
    assert!(m.bytes.read > 0);
    assert_eq!(
        q::<String>(&e, "SELECT big::VARCHAR FROM lake.br.cons_af WHERE id = 3"),
        "12345678901234567890.1234567890"
    );
    assert_eq!(
        q::<String>(&e, "SELECT typeof(big) FROM lake.br.cons_af LIMIT 1"),
        "DECIMAL(38,10)"
    );
    assert_eq!(
        q::<String>(&e, "SELECT typeof(n) FROM lake.br.cons_af LIMIT 1"),
        "DECIMAL(18,4)"
    );
    assert_eq!(
        q::<String>(&e, "SELECT tz::VARCHAR FROM lake.br.cons_af WHERE id = 3"),
        "2024-01-31 07:34:56.123456+00"
    );
    let sql = e
        .fake
        .calls("ExecuteStatement")
        .last()
        .expect("unload")
        .body["Sql"]
        .as_str()
        .expect("sql")
        .to_owned();
    assert!(
        sql.starts_with(
            r#"UNLOAD ('SELECT "id", "n", "big", "tz", "s" FROM "est_v2"."est_core_cons_af"')"#
        ),
        "{sql}"
    );
    assert_eq!(
        unload_objects(&e).await,
        0,
        "the member prefix is removed after the commit"
    );

    e.fake.sql("DELETE FROM est_v2.est_core_cons_af WHERE id = 10; UPDATE est_v2.est_core_cons_af SET s = 'x' WHERE id = 0;");
    let r = go(&e, &["full"]).await;
    let n = rows(&r, "full");
    assert_eq!(
        (n.inserted, n.updated, n.deleted, n.unchanged),
        (0, 1, 1, 9)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_update_key_pushes_the_watermark_down() {
    let e = setup().await;
    let r = go(&e, &["inc"]).await;
    assert_eq!(rows(&r, "inc").inserted, 2);
    let st: String = q(
        &e,
        "SELECT value::VARCHAR FROM lake._atlatl.stream_state WHERE stream_id = 'warehouse/act'",
    );
    assert!(st.contains("2025-05-02 00:00:00"), "{st}");

    e.fake.sql("INSERT INTO est_v2.est_core_act_af VALUES (3, '2025-05-03 00:00:00', 3.30), (0, '2025-04-01 00:00:00', 9.99)");
    let r = go(&e, &["inc"]).await;
    let n = rows(&r, "inc");
    assert_eq!(
        (n.inserted, n.extracted),
        (1, 2),
        "the bound is inclusive: id 2 again, id 0 is below it"
    );
    let sql = e
        .fake
        .calls("ExecuteStatement")
        .last()
        .expect("unload")
        .body["Sql"]
        .as_str()
        .expect("sql")
        .to_owned();
    assert!(
        sql.contains(r#"WHERE ("updated" >= CAST(''2025-05-02 00:00:00'' AS timestamp))"#),
        "{sql}"
    );
    assert_eq!(q::<i64>(&e, "SELECT count(*) FROM lake.br.act_af"), 3);
    assert_eq!(
        q::<String>(&e, "SELECT v::VARCHAR FROM lake.br.act_af WHERE id = 3"),
        "3.30"
    );
    let st: String = q(
        &e,
        "SELECT value::VARCHAR FROM lake._atlatl.stream_state WHERE stream_id = 'warehouse/act'",
    );
    assert!(st.contains("2025-05-03 00:00:00"), "{st}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wildcard_members_are_discovered_in_order() {
    let e = setup().await;
    let r = go(&e, &["wild"]).await;
    let names: Vec<&str> = r.replications[0].streams[0]
        .members
        .iter()
        .map(|m| m.member.as_str())
        .collect();
    assert_eq!(
        names,
        ["est_v2.est_core_cons_af", "est_v2.est_core_cons_qf"]
    );
    assert_eq!(
        q::<i64>(&e, "SELECT count(*) FROM lake.br.est_core_cons_af"),
        11
    );
    assert_eq!(
        q::<String>(&e, "SELECT v FROM lake.br.est_core_cons_qf"),
        "q"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chunks_by_unloaded_files() {
    let e = setup().await;
    e.fake.state().slices = 4;
    let r = go(&e, &["chunked"]).await;
    let m = &r.replications[0].streams[0].members[0];
    assert_eq!(m.chunks, 4);
    assert_eq!(rows(&r, "chunked").inserted, 11);
    assert_eq!(
        q::<String>(
            &e,
            "SELECT big::VARCHAR FROM lake.br.cons_chunked WHERE id = 7"
        ),
        "12345678901234567890.1234567890"
    );
    assert_eq!(unload_objects(&e).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_mid_unload_cancels_the_statement() {
    let e = setup().await;
    e.fake.state().hang_unload = true;
    let cancel = CancellationToken::new();
    let (c2, fake) = (cancel.clone(), e.fake.clone());
    tokio::spawn(async move {
        // Kill once the UNLOAD is in flight (a fixed delay races the column lookup under load).
        for _ in 0..600 {
            if fake.calls("ExecuteStatement").iter().any(|c| {
                c.body["Sql"]
                    .as_str()
                    .is_some_and(|s| s.starts_with("UNLOAD"))
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        c2.cancel();
    });
    let r = run_reps(&e, &["full"], cancel).await;
    assert_eq!(
        r.replications[0].status,
        ReplicationStatus::Killed,
        "{:?}",
        r.replications[0].error
    );
    let mut cancels = Vec::new();
    for _ in 0..100 {
        cancels = e.fake.calls("CancelStatement");
        if !cancels.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let ops: Vec<String> = e.fake.state().calls.iter().map(|c| c.op.clone()).collect();
    assert_eq!(
        cancels.len(),
        1,
        "the kill reaches CancelStatement: {ops:?} {:?}",
        r.replications[0].error
    );
    let unload = e
        .fake
        .calls("ExecuteStatement")
        .into_iter()
        .filter(|c| {
            c.body["Sql"]
                .as_str()
                .is_some_and(|s| s.starts_with("UNLOAD"))
        })
        .count();
    assert_eq!(unload, 1);
    let exists: i64 = q(
        &e,
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = 'cons_af'",
    );
    assert_eq!(exists, 0, "the lake is unchanged");
}

/// A `super` column: a DuckDB struct the catalog reports as `super`, so only `JSON_SERIALIZE`
/// (here a macro emitting JSON) turns it into JSON text; a plain cast would give `{'a': 1, …}`.
fn seed_super(e: &Env) {
    e.fake.sql(
        "CREATE TABLE est_v2.est_core_sup (id BIGINT, payload STRUCT(a INTEGER, tags VARCHAR[]));
         INSERT INTO est_v2.est_core_sup VALUES (1, {'a': 1, 'tags': ['x', 'it''s']}), (2, NULL);
         ALTER VIEW svv_columns RENAME TO svv_columns_base;
         CREATE VIEW svv_columns AS SELECT * REPLACE
           (CASE WHEN data_type LIKE 'struct%' THEN 'super' ELSE data_type END AS data_type)
           FROM svv_columns_base;
         CREATE MACRO json_serialize(x) AS CAST(to_json(x) AS VARCHAR);",
    );
}

fn unloads(e: &Env) -> Vec<String> {
    e.fake
        .calls("ExecuteStatement")
        .iter()
        .filter_map(|c| c.body["Sql"].as_str())
        .filter(|s| s.starts_with("UNLOAD"))
        .map(str::to_owned)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_super_unloads_serialized_as_its_declared_type() {
    let e = setup().await;
    seed_super(&e);
    let r = go(&e, &["super"]).await;
    assert_eq!(rows(&r, "super").inserted, 4);
    let u = unloads(&e);
    assert_eq!(u.len(), 2, "{u:?}");
    for sql in &u {
        assert!(
            sql.starts_with(
                r#"UNLOAD ('SELECT "id", JSON_SERIALIZE("payload") AS "payload" FROM "est_v2"."est_core_sup"')"#
            ),
            "{sql}"
        );
    }
    for (t, ty) in [("sup_json", "JSON"), ("sup_text", "VARCHAR")] {
        assert_eq!(
            q::<String>(
                &e,
                &format!("SELECT typeof(payload) FROM lake.br.{t} LIMIT 1")
            ),
            ty
        );
        assert_eq!(
            q::<String>(
                &e,
                &format!("SELECT payload::VARCHAR FROM lake.br.{t} WHERE id = 1")
            ),
            r#"{"a":1,"tags":["x","it's"]}"#
        );
        assert!(q::<bool>(
            &e,
            &format!("SELECT payload IS NULL FROM lake.br.{t} WHERE id = 2")
        ));
    }
    assert_eq!(
        q::<i64>(
            &e,
            "SELECT (payload->>'$.a')::INT FROM lake.br.sup_json WHERE id = 1"
        ),
        1
    );
    assert_eq!(
        q::<String>(&e, "SELECT typeof(id) FROM lake.br.sup_text LIMIT 1"),
        "VARCHAR",
        "`*` declares every column"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn undeclared_super_is_refused_before_reading() {
    let e = setup().await;
    seed_super(&e);
    let r = run_reps(&e, &["super_undeclared"], CancellationToken::new()).await;
    let x = &r.replications[0];
    assert_eq!(x.status, ReplicationStatus::Failed);
    let err = x.error.as_ref().expect("error");
    assert_eq!(
        (err.class.to_string().as_str(), err.code.as_deref()),
        ("config", Some("source_type")),
        "{}",
        err.message
    );
    assert_eq!(
        err.message,
        "`est_v2.est_core_sup`: column `payload` is super, which the target can't hold: there's no semi-structured type, and its text depends on the read path; declare `columns.payload.type` (JSON or VARCHAR) to read it as `JSON_SERIALIZE(\"payload\")`, or exclude it with `columns.exclude`"
    );
    assert!(unloads(&e).is_empty(), "nothing is unloaded");
    let exists: i64 = q(
        &e,
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = 'sup_raw'",
    );
    assert_eq!(exists, 0);
}
