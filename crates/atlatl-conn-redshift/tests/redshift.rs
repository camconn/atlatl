#![allow(clippy::unwrap_used)]

use std::time::Duration;

use atlatl_conn_redshift::unload::select_sql;
use atlatl_conn_redshift::{Access, Object, Options, Redshift, UnloadRequest, Unloaded};
use atlatl_core::config::model::RedshiftConn;
use atlatl_core::error::ErrorClass;
use atlatl_core::retry::{AttemptRecord, RetryPolicy};
use atlatl_testkit::redshift_fake::{Fake, Faulty, KEY, REGION, SECRET};
use duckdb::Connection;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

fn fast(n: u32) -> RetryPolicy {
    RetryPolicy {
        max_attempts: n,
        base: Duration::from_millis(5),
        max: Duration::from_millis(20),
        ..RetryPolicy::default()
    }
}

fn opts() -> Options {
    Options {
        query_retry: fast(3),
        request_retry: fast(4),
        object_retry: fast(3),
        http_timeout: Duration::from_secs(20),
        ..Options::default()
    }
}

fn rustfs_storage(endpoint: Option<&str>) -> Value {
    let s3 = atlatl_testkit::rustfs();
    json!({
        "type": "s3", "key_id": s3.key_id, "secret": s3.secret, "region": REGION,
        "endpoint": format!("http://{}", endpoint.unwrap_or(&s3.endpoint)), "url_style": "path"
    })
}

fn conn(fake: &Fake, bucket: &str, extra: Value) -> RedshiftConn {
    let mut v = json!({
        "access": "data_api",
        "data_api": {
            "workgroup": "wg", "database": "dev", "region": REGION, "endpoint": fake.endpoint,
            "key_id": KEY, "secret": SECRET, "poll": {"wait": "0s", "initial": "5ms", "max": "20ms"}
        },
        "unload": {
            "bucket": bucket, "iam_role": "arn:aws:iam::000000000000:role/unload",
            "read_role": "none", "storage": rustfs_storage(None)
        }
    });
    merge(&mut v, extra);
    serde_json::from_value(v).unwrap()
}

fn merge(a: &mut Value, b: Value) {
    match (a, b) {
        (Value::Object(a), Value::Object(b)) => {
            for (k, v) in b {
                merge(a.entry(k).or_insert(Value::Null), v);
            }
        }
        (a, b) => *a = b,
    }
}

async fn setup() -> (Fake, String) {
    let fake = Fake::start().await;
    let bucket = atlatl_testkit::rustfs().fresh_bucket().unwrap();
    (fake, bucket)
}

const TABLE: &str = r#"
CREATE SCHEMA est_v2;
CREATE TABLE est_v2.est_core_cons_af (id BIGINT, n DECIMAL(18,4), big DECIMAL(38,10), ts TIMESTAMP,
  tz TIMESTAMPTZ, s VARCHAR, b BOOLEAN, d DATE);
INSERT INTO est_v2.est_core_cons_af SELECT range, (range * 1.2345)::DECIMAL(18,4),
  '12345678901234567890.1234567890'::DECIMAL(38,10), TIMESTAMP '2024-01-31 12:34:56.123456' + to_days(range::INT),
  TIMESTAMPTZ '2024-01-31 12:34:56.123456+05', 'it''s ' || range, range % 2 = 0, DATE '2024-02-29'
  FROM range(10);
INSERT INTO est_v2.est_core_cons_af VALUES (100, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
"#;

fn engine() -> Connection {
    atlatl_testkit::lake::open_engine().unwrap()
}

fn req(run: &str, select: String, cols: Vec<atlatl_conn_redshift::SourceColumn>) -> UnloadRequest {
    UnloadRequest {
        run_id: run.into(),
        stream: "warehouse/est_v2/est_core_cons".into(),
        member: "est_v2.est_core_cons_af".into(),
        attempt: 1,
        select,
        columns: cols,
    }
}

async fn unload_table(
    rs: &Redshift,
    run: &str,
    filter: &[String],
) -> atlatl_core::error::Result<Unloaded> {
    let c = CancellationToken::new();
    let cols = rs.columns("est_v2.est_core_cons_af", &c).await?;
    let sel = select_sql(
        &Object::parse("est_v2.est_core_cons_af"),
        &cols,
        filter,
        None,
    );
    rs.unload(&req(run, sel, cols), &c).await
}

fn stage(
    c: &Connection,
    u: &Unloaded,
    name: &str,
) -> atlatl_core::error::Result<(u64, Vec<AttemptRecord>)> {
    u.prepare(c)?;
    let mut recs = Vec::new();
    let n = u.stage(c, name, &fast(3), &mut |a| recs.push(a.clone()))?;
    Ok((n, recs))
}

fn types(c: &Connection, t: &str) -> Vec<(String, String)> {
    let mut s = c.prepare(&format!("DESCRIBE {t}")).unwrap();
    s.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn one(c: &Connection, sql: &str) -> String {
    c.query_row(sql, [], |r| r.get::<_, String>(0)).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn happy_path_unload_read_back_and_cleanup() {
    let (fake, bucket) = setup().await;
    fake.sql(TABLE);
    let s3 = atlatl_testkit::rustfs();
    fake.state().sts_keys = Some((s3.key_id.clone(), s3.secret.clone()));
    // read_role `same`: the read-back assumes the UNLOAD role through (fake) STS.
    let cfg = conn(
        &fake,
        &bucket,
        json!({"unload": {"read_role": "same", "storage": {"key_id": null, "secret": null}}}),
    );
    let o = Options {
        sts_endpoint: Some(fake.endpoint.clone()),
        ..opts()
    };
    let rs = Redshift::new(&cfg, o).unwrap();

    let u = unload_table(&rs, "01JRUNHAPPY", &[]).await.unwrap();
    assert_eq!(u.files.len(), 2, "{u:?}");
    assert_eq!(u.expected_rows, Some(11));
    assert_eq!(u.statement.rows, Some(11));
    assert!(
        u.prefix
            .ends_with("01JRUNHAPPY/warehouse_est_v2_est_core_cons/est_v2.est_core_cons_af/a1/")
    );
    assert!(
        u.files
            .iter()
            .all(|f| f.url.contains("/a1/part_") && f.url.ends_with("_part_00.parquet"))
    );

    let sts = fake.calls("sts:AssumeRole");
    assert_eq!(sts.len(), 1);
    assert_eq!(
        sts[0].body["RoleArn"],
        "arn:aws:iam::000000000000:role/unload"
    );

    let ex = fake.calls("ExecuteStatement");
    let sql = ex.last().unwrap().body["Sql"].as_str().unwrap().to_owned();
    assert!(sql.starts_with(r#"UNLOAD ('SELECT "id", "n", "big", "ts", "tz", "s", "b", "d" FROM "est_v2"."est_core_cons_af"') TO 's3://"#), "{sql}");
    assert!(sql.ends_with("IAM_ROLE 'arn:aws:iam::000000000000:role/unload' FORMAT PARQUET MANIFEST VERBOSE MAXFILESIZE 256 MB PARALLEL ON"));
    assert_eq!(ex.last().unwrap().body["WorkgroupName"], "wg");
    assert_eq!(
        ex.last().unwrap().body["StatementName"],
        "atlatl:01JRUNHAPPY:warehouse/est_v2/est_core_cons:est_v2.est_core_cons_af"
    );

    let c = engine();
    let (n, recs) = stage(&c, &u, "stg").unwrap();
    assert_eq!(n, 11);
    assert!(recs.is_empty());
    let t = types(&c, "stg");
    assert_eq!(
        t.iter().map(|(_, t)| t.as_str()).collect::<Vec<_>>(),
        [
            "BIGINT",
            "DECIMAL(18,4)",
            "DECIMAL(38,10)",
            "TIMESTAMP",
            "TIMESTAMP WITH TIME ZONE",
            "VARCHAR",
            "BOOLEAN",
            "DATE"
        ]
    );
    assert_eq!(
        one(&c, "SELECT big::VARCHAR FROM stg WHERE id = 3"),
        "12345678901234567890.1234567890"
    );
    assert_eq!(one(&c, "SELECT n::VARCHAR FROM stg WHERE id = 3"), "3.7035");
    assert_eq!(
        one(&c, "SELECT tz::VARCHAR FROM stg WHERE id = 3"),
        "2024-01-31 07:34:56.123456+00"
    );
    assert_eq!(
        one(&c, "SELECT ts::VARCHAR FROM stg WHERE id = 3"),
        "2024-02-03 12:34:56.123456"
    );
    assert_eq!(one(&c, "SELECT s FROM stg WHERE id = 3"), "it's 3");
    assert_eq!(
        one(&c, "SELECT count(*)::VARCHAR FROM stg WHERE n IS NULL"),
        "1"
    );
    u.release(&c).unwrap();

    let s3c = rs.s3().unwrap();
    assert!(
        !s3c.list(&bucket, &u.member_prefix)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(rs.cleanup(&u).await.unwrap(), 3);
    assert!(
        s3c.list(&bucket, &u.member_prefix)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn polls_with_backoff_through_statuses() {
    let (fake, bucket) = setup().await;
    fake.state().describes = 3;
    let rs = Redshift::new(&conn(&fake, &bucket, json!({})), opts()).unwrap();
    let r = rs
        .query("SELECT 1 AS one, 'x' AS s", "q", &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(r.columns, ["one", "s"]);
    assert_eq!(r.int(0, "one"), Some(1));
    let d = fake.calls("DescribeStatement");
    assert_eq!(d.len(), 4);
    assert!(d.iter().all(|c| c.body.get("WaitTimeSeconds").is_none()));
}

#[tokio::test(flavor = "multi_thread")]
async fn long_poll_falls_back_to_polling_when_refused() {
    let (fake, bucket) = setup().await;
    {
        let mut s = fake.state();
        s.describes = 2;
        s.refuse_long_poll = true;
    }
    let cfg = conn(
        &fake,
        &bucket,
        json!({"data_api": {"poll": {"wait": "5s"}}}),
    );
    let rs = Redshift::new(&cfg, opts()).unwrap();
    let r = rs
        .query("SELECT 42 AS x", "q", &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(r.int(0, "x"), Some(42));
    let d = fake.calls("DescribeStatement");
    assert_eq!(d[0].body["WaitTimeSeconds"], 5);
    assert!(
        d[1..]
            .iter()
            .all(|c| c.body.get("WaitTimeSeconds").is_none())
    );
    assert_eq!(
        d.len(),
        4,
        "one refused long poll, then SUBMITTED, PICKED, FINISHED"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn paginated_results() {
    let (fake, bucket) = setup().await;
    let rs = Redshift::new(&conn(&fake, &bucket, json!({})), opts()).unwrap();
    let r = rs
        .query(
            "SELECT range AS i FROM range(5) ORDER BY 1",
            "q",
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        (0..5).map(|i| r.int(i, "i").unwrap()).collect::<Vec<_>>(),
        [0, 1, 2, 3, 4]
    );
    assert_eq!(fake.calls("GetStatementResult").len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_statements_map_to_error_classes() {
    let (fake, bucket) = setup().await;
    fake.sql(TABLE);
    let rs = Redshift::new(&conn(&fake, &bucket, json!({})), opts()).unwrap();
    let c = CancellationToken::new();

    let e = rs
        .query("SELECT * FROM est_v2.nope", "q", &c)
        .await
        .unwrap_err();
    assert_eq!(e.class, ErrorClass::Config, "{e}");
    assert!(e.message.contains("does not exist"), "{e}");
    assert_eq!(
        fake.calls("ExecuteStatement").len(),
        1,
        "config isn't retried"
    );

    fake.state()
        .fail
        .push_back("ERROR: permission denied for relation est_core_cons_af".into());
    assert_eq!(
        rs.query("SELECT 1", "q", &c).await.unwrap_err().class,
        ErrorClass::Config
    );

    fake.state()
        .fail
        .push_back("ERROR: Insufficient capacity to run the query. Try again later.".into());
    let r = rs.query("SELECT 7 AS x", "q", &c).await.unwrap();
    assert_eq!(
        r.int(0, "x"),
        Some(7),
        "transient FAILED is retried at query level"
    );

    fake.state()
        .fail
        .extend(["ERROR: division by zero".to_owned()]);
    assert_eq!(
        rs.query("SELECT 1", "q", &c).await.unwrap_err().class,
        ErrorClass::Data
    );

    // A second UNLOAD into the same attempt prefix fails: UNLOAD won't overwrite.
    unload_table(&rs, "01JRUNDUP", &[]).await.unwrap();
    let e = unload_table(&rs, "01JRUNDUP", &[]).await.unwrap_err();
    assert_eq!(e.class, ErrorClass::Internal, "{e}");
    assert_eq!(e.code.as_deref(), Some("unload_prefix_not_empty"));

    // A transient UNLOAD failure is retried into a fresh prefix.
    let cols = rs.columns("est_v2.est_core_cons_af", &c).await.unwrap();
    let sel = select_sql(&Object::parse("est_v2.est_core_cons_af"), &cols, &[], None);
    fake.state()
        .fail
        .push_back("ERROR: could not connect to the S3 endpoint".into());
    let u = rs.unload(&req("01JRUNRETRY", sel, cols), &c).await.unwrap();
    assert!(u.prefix.ends_with("/a1r2/"), "{}", u.prefix);
    assert_eq!(u.expected_rows, Some(11));
}

#[tokio::test(flavor = "multi_thread")]
async fn throttling_is_retried_with_the_same_client_token() {
    let (fake, bucket) = setup().await;
    let rs = Redshift::new(&conn(&fake, &bucket, json!({})), opts()).unwrap();
    {
        let mut s = fake.state();
        for _ in 0..2 {
            s.inject
                .push_back(("ExecuteStatement".into(), "ThrottlingException".into(), 400));
        }
        s.inject.push_back((
            "DescribeStatement".into(),
            "InternalServerException".into(),
            500,
        ));
    }
    let r = rs
        .query("SELECT 1 AS x", "q", &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(r.int(0, "x"), Some(1));
    let ex = fake.calls("ExecuteStatement");
    assert_eq!(ex.len(), 3);
    let tok = &ex[0].body["ClientToken"];
    assert!(tok.as_str().is_some_and(|t| t.starts_with("atlatl-")));
    assert!(ex.iter().all(|c| &c.body["ClientToken"] == tok));

    {
        let mut s = fake.state();
        for _ in 0..10 {
            s.inject.push_back((
                "ExecuteStatement".into(),
                "ActiveStatementsExceededException".into(),
                400,
            ));
        }
    }
    let e = rs
        .execute("SELECT 1", "q", &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.class, ErrorClass::Throttled, "{e}");
    assert_eq!(e.code.as_deref(), Some("ActiveStatementsExceededException"));
    fake.state().inject.clear();

    fake.state()
        .inject
        .push_back(("ExecuteStatement".into(), "ValidationException".into(), 400));
    assert_eq!(
        rs.execute("SELECT 1", "q", &CancellationToken::new())
            .await
            .unwrap_err()
            .class,
        ErrorClass::Config
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_signature_is_auth() {
    let (fake, bucket) = setup().await;
    let cfg = conn(&fake, &bucket, json!({"data_api": {"secret": "wrong"}}));
    let rs = Redshift::new(&cfg, opts()).unwrap();
    let e = rs
        .execute("SELECT 1", "q", &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.class, ErrorClass::Auth, "{e}");
    assert_eq!(e.code.as_deref(), Some("InvalidSignatureException"));
    assert!(fake.calls("ExecuteStatement").is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_statement_whose_status_is_lost_is_cancelled() {
    let (fake, bucket) = setup().await;
    {
        let mut s = fake.state();
        s.hang = true;
        for _ in 0..200 {
            s.inject.push_back((
                "DescribeStatement".into(),
                "InternalServerException".into(),
                500,
            ));
        }
    }
    let rs = Redshift::new(&conn(&fake, &bucket, json!({})), opts()).unwrap();
    let e = rs
        .execute("SELECT 1", "q", &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.class, ErrorClass::Transient, "{e}");
    assert!(e.message.contains("CancelStatement: true"), "{e}");
    let submitted: Vec<String> = fake
        .calls("ExecuteStatement")
        .iter()
        .map(|c| c.body["Sql"].to_string())
        .collect();
    let cancelled = fake.calls("CancelStatement");
    assert!(!submitted.is_empty());
    assert_eq!(
        cancelled.len(),
        submitted.len(),
        "every resubmitted attempt cancels the one it lost"
    );
    for c in &cancelled {
        assert_eq!(
            fake.statuses(c.body["Id"].as_str().unwrap()),
            Some("ABORTED")
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn kill_and_timeout_cancel_the_statement() {
    let (fake, bucket) = setup().await;
    fake.state().hang = true;
    let rs = std::sync::Arc::new(Redshift::new(&conn(&fake, &bucket, json!({})), opts()).unwrap());
    let cancel = CancellationToken::new();
    let (rs2, c2) = (rs.clone(), cancel.clone());
    let h = tokio::spawn(async move { rs2.execute("SELECT 1", "q", &c2).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!h.is_finished());
    cancel.cancel();
    let e = tokio::time::timeout(Duration::from_secs(5), h)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(e.class, ErrorClass::Killed, "{e}");
    assert!(e.message.contains("CancelStatement: true"), "{e}");
    let cs = fake.calls("CancelStatement");
    assert_eq!(cs.len(), 1);
    let id = cs[0].body["Id"].as_str().unwrap().to_owned();
    assert_eq!(fake.statuses(&id), Some("ABORTED"));

    let o = Options {
        statement_timeout: Duration::from_millis(300),
        ..opts()
    };
    let rs = Redshift::new(&conn(&fake, &bucket, json!({})), o).unwrap();
    let e = rs
        .execute("SELECT 1", "q", &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.class, ErrorClass::Timeout, "{e}");
    assert_eq!(fake.calls("CancelStatement").len(), 2);

    // A dropped (abandoned) wait still cancels, from a spawned task.
    let before = fake.calls("CancelStatement").len();
    fake.state().hang = true;
    let r = tokio::time::timeout(
        Duration::from_millis(200),
        rs.execute("SELECT 1", "q", &CancellationToken::new()),
    )
    .await;
    assert!(r.is_err(), "still waiting");
    let mut n = before;
    for _ in 0..100 {
        n = fake.calls("CancelStatement").len();
        if n > before {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(n, before + 1);

    // ABORTED by someone else is transient.
    fake.state().hang = false;
    fake.state().describes = 50;
    let rs = std::sync::Arc::new(Redshift::new(&conn(&fake, &bucket, json!({})), opts()).unwrap());
    let rs2 = rs.clone();
    let n = fake.calls("ExecuteStatement").len();
    let h = tokio::spawn(async move {
        rs2.execute("SELECT 1", "q", &CancellationToken::new())
            .await
    });
    while fake.calls("ExecuteStatement").len() == n {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let Access::DataApi(d) = rs.access() else {
        panic!("data api")
    };
    assert!(d.cancel(&fake.last_id()).await.unwrap());
    let e = h.await.unwrap().unwrap_err();
    assert_eq!(
        (e.class, e.code.as_deref()),
        (ErrorClass::Transient, Some("statement_aborted")),
        "{e}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_unload_stages_an_empty_typed_table() {
    for with_manifest in [false, true] {
        let (fake, bucket) = setup().await;
        fake.sql(TABLE);
        fake.state().empty_manifest = with_manifest;
        let rs = Redshift::new(&conn(&fake, &bucket, json!({})), opts()).unwrap();
        let u = unload_table(&rs, "01JRUNEMPTY", &["\"id\" < 0".into()])
            .await
            .unwrap();
        assert!(u.is_empty());
        assert_eq!(u.manifest_found, with_manifest);
        assert_eq!(u.expected_rows, Some(0));
        let c = engine();
        let (n, _) = stage(&c, &u, "stg").unwrap();
        assert_eq!(n, 0);
        assert_eq!(types(&c, "stg")[1].1, "DECIMAL(18,4)");
        assert!(u.chunks(1 << 20).unwrap().is_empty());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn precision_lookup() {
    let (fake, bucket) = setup().await;
    fake.sql(TABLE);
    let rs = Redshift::new(&conn(&fake, &bucket, json!({})), opts()).unwrap();
    let cols = rs
        .columns("est_v2.est_core_cons_af", &CancellationToken::new())
        .await
        .unwrap();
    type Col<'a> = (&'a str, &'a str, Option<i64>, Option<i64>, &'a str);
    let got: Vec<Col<'_>> = cols
        .iter()
        .map(|c| {
            (
                c.name.as_str(),
                c.data_type.as_str(),
                c.precision,
                c.scale,
                c.duck_type.as_str(),
            )
        })
        .collect();
    assert_eq!(got[1], ("n", "numeric", Some(18), Some(4), "DECIMAL(18,4)"));
    assert_eq!(
        got[2],
        ("big", "numeric", Some(38), Some(10), "DECIMAL(38,10)")
    );
    assert_eq!(
        got[4],
        ("tz", "timestamp with time zone", None, None, "TIMESTAMPTZ")
    );
    assert_eq!(got.len(), 8);
    let e = rs
        .columns("est_v2.missing", &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.class, ErrorClass::Config);
}

#[tokio::test(flavor = "multi_thread")]
async fn wildcard_discovery_is_sorted() {
    let (fake, bucket) = setup().await;
    fake.sql(
        "CREATE SCHEMA est_v2; CREATE SCHEMA other;
         CREATE TABLE est_v2.est_core_cons_qf (x INT); CREATE TABLE est_v2.est_core_cons_af (x INT);
         CREATE TABLE est_v2.est_core_cons_rec (x INT); CREATE TABLE est_v2.est_core_cons_Zf (x INT);
         CREATE TABLE other.est_core_cons_bf (x INT);
         CREATE VIEW est_v2.est_core_cons_vf AS SELECT 1 AS x;",
    );
    let rs = Redshift::new(&conn(&fake, &bucket, json!({})), opts()).unwrap();
    let c = CancellationToken::new();
    let got = rs
        .discover("est_v2.est_core_cons_*f", &[], false, &c)
        .await
        .unwrap();
    assert_eq!(
        got,
        [
            "est_v2.est_core_cons_Zf",
            "est_v2.est_core_cons_af",
            "est_v2.est_core_cons_qf"
        ]
    );
    let got = rs
        .discover(
            "est_v2.est_core_cons_*f",
            &["est_v2.est_core_cons_q*".into()],
            true,
            &c,
        )
        .await
        .unwrap();
    assert_eq!(
        got,
        [
            "est_v2.est_core_cons_Zf",
            "est_v2.est_core_cons_af",
            "est_v2.est_core_cons_vf"
        ]
    );
    let got = rs
        .discover("*.est_core_cons_?f", &[], false, &c)
        .await
        .unwrap();
    assert_eq!(
        got.first().map(String::as_str),
        Some("est_v2.est_core_cons_Zf")
    );
    assert_eq!(
        got.last().map(String::as_str),
        Some("other.est_core_cons_bf")
    );
    assert!(
        rs.discover("est_v2.nothing_*", &[], false, &c)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn chunks_by_file_groups() {
    let (fake, bucket) = setup().await;
    fake.sql(TABLE);
    fake.state().slices = 4;
    let rs = Redshift::new(&conn(&fake, &bucket, json!({})), opts()).unwrap();
    let u = unload_table(&rs, "01JRUNCHUNK", &[]).await.unwrap();
    assert_eq!(u.files.len(), 4);
    let sizes: Vec<u64> = u.files.iter().map(|f| f.size.unwrap()).collect();
    let chunks = u
        .chunks((sizes[0] + sizes[1]).max(sizes[2] + sizes[3]))
        .unwrap();
    assert_eq!(chunks.len(), 2);
    assert_eq!(
        chunks.iter().map(|c| c.files.len()).collect::<Vec<_>>(),
        [2, 2]
    );
    let c = engine();
    u.prepare(&c).unwrap();
    let mut total = 0;
    for (i, ch) in chunks.iter().enumerate() {
        let n = u
            .stage_files(&c, &format!("chunk_{i}"), &ch.files, &fast(2), &mut |_| {})
            .unwrap();
        assert_eq!(Some(n), ch.expected_rows);
        let m: i64 = c
            .query_row(&format!("SELECT count(*) FROM ({})", ch.select), [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(m as u64, n);
        total += n;
    }
    assert_eq!(Some(total), u.expected_rows);
}

#[tokio::test(flavor = "multi_thread")]
async fn transient_s3_read_errors_are_retried_per_object() {
    let (fake, bucket) = setup().await;
    fake.sql(TABLE);
    let s3 = atlatl_testkit::rustfs();
    let proxy = Faulty::start(s3.endpoint.clone(), "0001_part_00.parquet".into(), 2).await;
    let cfg = conn(
        &fake,
        &bucket,
        json!({"unload": {"storage": rustfs_storage(Some(&proxy.addr))}}),
    );
    let rs = Redshift::new(&cfg, opts()).unwrap();
    let u = unload_table(&rs, "01JRUNFAULT", &[]).await.unwrap();
    let c = engine();
    c.execute_batch("LOAD httpfs; SET http_keep_alive = false; SET http_retries = 0;")
        .unwrap();
    let (n, recs) = stage(&c, &u, "stg").unwrap();
    assert_eq!(n, 11);
    assert!(*proxy.hits.lock().unwrap() >= 1);
    assert!(!recs.is_empty(), "the failed object was retried");
    assert!(
        recs.iter().all(|r| r.class == ErrorClass::Transient),
        "{recs:?}"
    );
    assert!(
        recs.iter().all(|r| r.message.contains("0001_part_00")),
        "{recs:?}"
    );

    // A count that differs from the manifest is `decode`, retried, and rolled back.
    let mut bad = u.clone();
    bad.files[1].record_count = bad.files[1].record_count.map(|n| n + 1);
    bad.expected_rows = None;
    let mut recs = Vec::new();
    let e = bad
        .stage(&c, "stg2", &fast(2), &mut |a| recs.push(a.clone()))
        .unwrap_err();
    assert_eq!(e.class, ErrorClass::Decode, "{e}");
    assert_eq!(recs.len(), 1);
    let n: i64 = c
        .query_row("SELECT count(*) FROM stg2", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        n as u64,
        bad.files[0].record_count.unwrap(),
        "only the good object stayed"
    );

    // An UNLOAD whose manifest disagrees with the statement's row count is `decode`.
    let (fake, bucket) = setup().await;
    fake.sql(TABLE);
    fake.state().lie_record_count = 1;
    let rs = Redshift::new(&conn(&fake, &bucket, json!({})), opts()).unwrap();
    let e = unload_table(&rs, "01JRUNLIE", &[]).await.unwrap_err();
    assert_eq!(e.class, ErrorClass::Decode, "{e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn gc_finds_and_removes_stale_run_prefixes() {
    let (fake, bucket) = setup().await;
    fake.sql(TABLE);
    let rs = Redshift::new(&conn(&fake, &bucket, json!({})), opts()).unwrap();
    unload_table(&rs, "01JRUNOLD", &[]).await.unwrap();
    unload_table(&rs, "01JRUNLIVE", &[]).await.unwrap();
    assert!(
        rs.stale_prefixes(Duration::from_secs(3600), &[])
            .await
            .unwrap()
            .is_empty()
    );
    let stale = rs
        .stale_prefixes(Duration::ZERO, &["01JRUNLIVE".into()])
        .await
        .unwrap();
    assert_eq!(stale.len(), 1);
    assert_eq!(stale[0].prefix, "atlatl_unload/01JRUNOLD/");
    assert_eq!(stale[0].objects, 3);
    assert!(stale[0].bytes > 0);
    assert_eq!(rs.remove_prefixes(&stale).await.unwrap(), 3);
    let left = rs.stale_prefixes(Duration::ZERO, &[]).await.unwrap();
    assert_eq!(
        left.iter().map(|p| p.run_id.as_str()).collect::<Vec<_>>(),
        ["01JRUNLIVE"]
    );
    assert_eq!(rs.cleanup_run("01JRUNLIVE").await.unwrap(), 3);
    assert!(
        rs.stale_prefixes(Duration::ZERO, &[])
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn config_validation() {
    let (fake, bucket) = setup().await;
    let bad = conn(
        &fake,
        &bucket,
        json!({"data_api": {"cluster_identifier": "c"}}),
    );
    assert_eq!(
        Redshift::new(&bad, opts()).unwrap_err().class,
        ErrorClass::Config
    );
    let bad = conn(
        &fake,
        &bucket,
        json!({"data_api": {"poll": {"wait": "40s"}}}),
    );
    assert_eq!(
        Redshift::new(&bad, opts()).unwrap_err().class,
        ErrorClass::Config
    );
    let bad = conn(&fake, &bucket, json!({"data_api": {"auth": "secret_arn"}}));
    assert_eq!(
        Redshift::new(&bad, opts()).unwrap_err().class,
        ErrorClass::Config
    );
    let ok = conn(
        &fake,
        &bucket,
        json!({"data_api": {"auth": "secret_arn", "secret_arn": "arn:x"}}),
    );
    let rs = Redshift::new(&ok, opts()).unwrap();
    rs.execute("SELECT 1", "q", &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(fake.calls("ExecuteStatement")[0].body["SecretArn"], "arn:x");
    let mut no_unload = conn(&fake, &bucket, json!({}));
    no_unload.unload = None;
    let rs = Redshift::new(&no_unload, opts()).unwrap();
    let e = rs
        .unload(
            &req("r", "SELECT 1".into(), Vec::new()),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(e.class, ErrorClass::Config);
}

// ----------------------------------------------------------------------------- wire (PG16)

async fn wire() -> (Redshift, String) {
    let pg = atlatl_testkit::pg16();
    let db = pg.fresh_db().unwrap();
    let (c, conn) = tokio_postgres::connect(&pg.dsn(&db), tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(conn);
    c.batch_execute(
        "CREATE VIEW svv_columns AS SELECT * FROM information_schema.columns;
         CREATE VIEW svv_tables AS SELECT * FROM information_schema.tables;
         CREATE SCHEMA est_v2;
         CREATE TABLE est_v2.est_core_cons_af (id bigint, n numeric(18,4), big numeric(38,10), tz timestamptz, s varchar(64));
         CREATE TABLE est_v2.est_core_cons_qf (id int);
         CREATE TABLE est_v2.est_core_cons_rec (id int);
         CREATE VIEW est_v2.est_core_cons_vf AS SELECT 1 AS id;",
    )
    .await
    .unwrap();
    let cfg: RedshiftConn = serde_json::from_value(json!({
        "access": "wire",
        "wire": {"host": pg.host, "port": pg.port, "database": db, "user": pg.user, "password": pg.password, "sslmode": "disable"}
    }))
    .unwrap();
    (Redshift::new(&cfg, opts()).unwrap(), db)
}

#[tokio::test(flavor = "multi_thread")]
async fn wire_catalog_discovery_and_errors() {
    let (rs, _) = wire().await;
    let c = CancellationToken::new();
    let cols = rs.columns("est_v2.est_core_cons_af", &c).await.unwrap();
    assert_eq!(
        cols.iter()
            .map(|c| c.duck_type.as_str())
            .collect::<Vec<_>>(),
        [
            "BIGINT",
            "DECIMAL(18,4)",
            "DECIMAL(38,10)",
            "TIMESTAMPTZ",
            "VARCHAR"
        ]
    );
    assert_eq!(cols[4].char_len, Some(64));
    assert_eq!(
        rs.discover("est_v2.est_core_cons_*f", &[], false, &c)
            .await
            .unwrap(),
        ["est_v2.est_core_cons_af", "est_v2.est_core_cons_qf"]
    );
    assert_eq!(
        rs.discover("est_v2.est_core_cons_*f", &[], true, &c)
            .await
            .unwrap()
            .len(),
        3
    );
    let e = rs
        .query("SELECT * FROM est_v2.nope", "q", &c)
        .await
        .unwrap_err();
    assert_eq!(e.class, ErrorClass::Config);
    assert_eq!(e.code.as_deref(), Some("42P01"));
    let e = rs
        .execute(
            "UNLOAD ('SELECT 1') TO 's3://b/p' IAM_ROLE 'r' FORMAT PARQUET",
            "q",
            &c,
        )
        .await
        .unwrap_err();
    assert_eq!(e.class, ErrorClass::Config, "{e}");
    let r = rs
        .execute(
            "INSERT INTO est_v2.est_core_cons_qf VALUES (1), (2)",
            "q",
            &c,
        )
        .await
        .unwrap();
    assert_eq!(r.rows, Some(2));
}

#[tokio::test(flavor = "multi_thread")]
async fn wire_kill_cancels_the_backend() {
    let (rs, _) = wire().await;
    let rs = std::sync::Arc::new(rs);
    let cancel = CancellationToken::new();
    let (rs2, c2) = (rs.clone(), cancel.clone());
    let t0 = std::time::Instant::now();
    let h = tokio::spawn(async move { rs2.execute("SELECT pg_sleep(30)", "q", &c2).await });
    tokio::time::sleep(Duration::from_millis(500)).await;
    cancel.cancel();
    let e = tokio::time::timeout(Duration::from_secs(10), h)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(e.class, ErrorClass::Killed, "{e}");
    assert!(e.message.contains("pg_cancel_backend: true"), "{e}");
    assert!(t0.elapsed() < Duration::from_secs(10));
    let r = rs
        .query("SELECT count(*) AS n FROM pg_stat_activity WHERE query LIKE 'SELECT pg_sleep(30)%' AND state = 'active'", "q", &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(r.int(0, "n"), Some(0));
}
