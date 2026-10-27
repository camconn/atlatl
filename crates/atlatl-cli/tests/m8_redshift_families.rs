//! M8.3 readiness: a vendor warehouse moved family by family, each family's daily and hourly
//! replications in one op writing the same lake tables, end to end through the `atlatl` binary
//! against the Redshift Data API fake and RustFS. Every name is synthetic.

use std::path::{Path, PathBuf};

use atlatl_testkit::LocalLake;
use atlatl_testkit::redshift_fake::{Fake, KEY, SECRET};
use serde_json::Value;

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/m8/redshift")
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("mkdir");
    for e in std::fs::read_dir(from).expect("read fixture dir") {
        let e = e.expect("entry");
        let dst = to.join(e.file_name());
        if e.file_type().expect("type").is_dir() {
            copy_dir(&e.path(), &dst);
        } else {
            std::fs::copy(e.path(), dst).expect("copy");
        }
    }
}

struct Env {
    fake: Fake,
    lake: LocalLake,
    bucket: String,
    project: tempfile::TempDir,
    out: tempfile::TempDir,
}

struct Run {
    code: i32,
    stderr: String,
    report: Value,
}

impl Run {
    fn ok(&self) -> &Self {
        assert_eq!(
            self.code, 0,
            "{}\n{:#}",
            self.stderr, self.report["replications"]
        );
        self
    }

    fn replication(&self, name: &str) -> &Value {
        self.report["replications"]
            .as_array()
            .and_then(|a| a.iter().find(|r| r["replication"] == name))
            .unwrap_or_else(|| panic!("no replication {name} in {:#}", self.report))
    }

    fn members(&self, stream: &str) -> Vec<&Value> {
        self.report["replications"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|r| r["streams"].as_array().into_iter().flatten())
            .filter(|s| s["stream_id"] == stream)
            .flat_map(|s| s["members"].as_array().into_iter().flatten())
            .collect()
    }

    fn member(&self, stream: &str, member: &str) -> &Value {
        self.members(stream)
            .into_iter()
            .find(|m| m["member"] == member)
            .unwrap_or_else(|| panic!("no member {member} of {stream}"))
    }

    fn rows(&self, stream: &str, member: &str, what: &str) -> u64 {
        self.member(stream, member)["rows"][what]
            .as_u64()
            .expect("count")
    }
}

async fn setup(families: &[&str]) -> Env {
    let fake = Fake::start().await;
    for f in families {
        let sql = std::fs::read_to_string(fixture().join(format!("source/{f}.sql"))).expect("sql");
        fake.sql(&sql);
    }
    let bucket = atlatl_testkit::rustfs().fresh_bucket().expect("bucket");
    let project = tempfile::tempdir().expect("tmp");
    copy_dir(&fixture(), project.path());
    Env {
        fake,
        lake: LocalLake::file().expect("lake"),
        bucket,
        project,
        out: tempfile::tempdir().expect("tmp"),
    }
}

impl Env {
    async fn atlatl(&self, args: &[&str]) -> Run {
        let s3 = atlatl_testkit::rustfs();
        let report = self.out.path().join("report.json");
        let _ = std::fs::remove_file(&report);
        let o = tokio::process::Command::new(env!("CARGO_BIN_EXE_atlatl"))
            .arg("--project")
            .arg(self.project.path())
            .arg("--report")
            .arg(&report)
            .args(["--log-format", "json"])
            .args(args)
            .env("ATLATL_EXTENSION_DIR", atlatl_testkit::extension_dir())
            .env("M8_TMP", self.out.path().join("spill"))
            .env("M8_CATALOG", self.lake.dir.path().join("meta.ducklake"))
            .env("M8_DATA_PATH", self.lake.data_path())
            .env("M8_RS_ENDPOINT", &self.fake.endpoint)
            .env("M8_RS_KEY", KEY)
            .env("M8_RS_SECRET", SECRET)
            .env("M8_BUCKET", &self.bucket)
            .env("M8_S3_ENDPOINT", format!("http://{}", s3.endpoint))
            .env("M8_S3_KEY", &s3.key_id)
            .env("M8_S3_SECRET", &s3.secret)
            .env_remove("ATLATL_PROJECT")
            .output()
            .await
            .expect("run atlatl");
        let report = std::fs::read(&report)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or(Value::Null);
        Run {
            code: o.status.code().expect("exit code"),
            stderr: String::from_utf8_lossy(&o.stderr).into_owned(),
            report,
        }
    }

    /// One op: every named replication in one `atlatl run`, as `dagster-atlatl` runs an op.
    async fn run(&self, replications: &[&str]) -> Run {
        let mut args = vec!["run"];
        for r in replications {
            args.extend(["--replication", r]);
        }
        self.atlatl(&args).await
    }

    fn lake_q(&self, sql: &str) -> Vec<String> {
        let c = self.lake.open("lake").expect("open lake");
        let mut s = c.prepare(sql).expect(sql);
        s.query_map([], |r| r.get::<_, Option<String>>(0))
            .expect(sql)
            .map(|r| r.expect("row").unwrap_or_else(|| "NULL".into()))
            .collect()
    }

    fn lake_one(&self, sql: &str) -> String {
        self.lake_q(sql)
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("no row: {sql}"))
    }

    fn source_q(&self, sql: &str) -> Vec<String> {
        let st = self.fake.state();
        let mut s = st.db.prepare(sql).expect(sql);
        s.query_map([], |r| r.get::<_, Option<String>>(0))
            .expect(sql)
            .map(|r| r.expect("row").unwrap_or_else(|| "NULL".into()))
            .collect()
    }

    /// Every row of `lake.br.<table>` equals the source table's (same columns, same order).
    fn assert_mirrors(&self, source: &str, table: &str) {
        let q = |rel: &str| format!("SELECT t::VARCHAR FROM {rel} t ORDER BY 1");
        let want = self.source_q(&q(source));
        let got = self.lake_q(&q(&format!("lake.br.{table}")));
        assert_eq!(got.len(), want.len(), "{table}: row count");
        assert!(got == want, "{table} differs from {source}");
    }

    fn table_exists(&self, table: &str) -> bool {
        self.lake_one(&format!(
            "SELECT count(*)::VARCHAR FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'br' AND lower(table_name) = lower('{table}')"
        )) == "1"
    }

    fn type_of(&self, table: &str, col: &str) -> String {
        self.lake_one(&format!(
            "SELECT data_type FROM duckdb_columns() WHERE database_name = 'lake' AND schema_name = 'br' AND lower(table_name) = lower('{table}') AND column_name = '{col}'"
        ))
    }

    fn unloads(&self) -> Vec<String> {
        self.fake
            .calls("ExecuteStatement")
            .into_iter()
            .filter_map(|c| c.body["Sql"].as_str().map(str::to_owned))
            .filter(|s| s.starts_with("UNLOAD"))
            .collect()
    }

    fn unload_for(&self, table: &str, since: usize) -> Vec<String> {
        let needle = format!(
            "FROM \"{}\".\"{}\"",
            table.split('.').next().expect("schema"),
            table.split('.').nth(1).expect("table")
        );
        self.unloads()
            .into_iter()
            .skip(since)
            .filter(|s| s.contains(&needle))
            .collect()
    }

    fn unload_objects(&self) -> i64 {
        let s3 = atlatl_testkit::rustfs();
        let c = atlatl_testkit::lake::open_engine().expect("engine");
        c.execute_batch("LOAD httpfs").expect("httpfs");
        c.execute_batch(&s3.duckdb_secret("list", &self.bucket))
            .expect("secret");
        c.query_row(
            &format!(
                "SELECT count(*) FROM glob('s3://{}/atlatl_unload/**')",
                self.bucket
            ),
            [],
            |r| r.get(0),
        )
        .expect("glob")
    }

    fn state(&self, stream: &str, member: &str) -> String {
        self.lake_one(&format!(
            "SELECT value::VARCHAR FROM lake._atlatl.stream_state WHERE stream_id = '{stream}' AND member = '{member}'"
        ))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reference_adopts_existing_tables_with_source_precision() {
    let e = setup(&["reference"]).await;
    // The table as the previous loader left it: its own column order and a narrower integer.
    let c = e.lake.open("lake").expect("lake");
    c.execute_batch(
        "CREATE SCHEMA lake.br;
         CREATE TABLE lake.br.fund_sec_cover (shares INTEGER, sec_id VARCHAR, name VARCHAR, country VARCHAR, active BOOLEAN);
         INSERT INTO lake.br.fund_sec_cover VALUES (7, 'S0001-A', 'stale', 'US', true), (1, 'S0999-Z', 'gone upstream', 'FR', false);",
    )
    .expect("seed");
    drop(c);

    let r = e.run(&["acme_reference"]).await;
    r.ok();
    assert_eq!(r.replication("acme_reference")["op"], "acme_reference");
    assert_eq!(
        e.type_of("fund_sec_cover", "shares"),
        "INTEGER",
        "the existing schema is authoritative"
    );
    assert_eq!(
        e.lake_q("SELECT concat_ws('|', sec_id, name, coalesce(country, 'NULL'), active, coalesce(shares::VARCHAR, 'NULL')) FROM lake.br.fund_sec_cover ORDER BY sec_id"),
        [
            "S0001-A|Alpha Holdings|US|true|1000000",
            "S0002-B|Beta's Group|GB|true|2500000",
            "S0003-C|Gamma \\ Co|NULL|false|NULL",
            "S0999-Z|gone upstream|FR|false|1",
        ],
        "an incremental merge upserts and never deletes"
    );
    let (c, u) = (
        r.rows(
            "acme/fund/fund_sec_cover",
            "fund.fund_sec_cover",
            "inserted",
        ),
        r.rows("acme/fund/fund_sec_cover", "fund.fund_sec_cover", "updated"),
    );
    assert_eq!((c, u), (2, 1));
    e.assert_mirrors("fund.fund_sec_hist", "fund_sec_hist");
    assert_eq!(e.type_of("fund_sec_hist", "start_date"), "DATE");
    assert_eq!(
        e.type_of("est_sec_map", "mapped_at"),
        "TIMESTAMP WITH TIME ZONE"
    );
    assert_eq!(
        e.lake_q("SELECT mapped_at::VARCHAR FROM lake.br.est_sec_map ORDER BY sec_id"),
        ["2024-02-01 07:30:00+00", "2024-02-03 04:15:00+00"],
        "timestamptz survives Parquet as UTC"
    );
    let lookups: Vec<String> = e
        .fake
        .calls("ExecuteStatement")
        .iter()
        .filter_map(|c| c.body["Sql"].as_str().map(str::to_owned))
        .filter(|s| s.contains("FROM svv_columns"))
        .collect();
    assert_eq!(
        lookups.len(),
        3,
        "one precision lookup per member: {lookups:?}"
    );
    assert!(
        lookups
            .iter()
            .any(|s| s.contains("table_schema = 'fund' AND table_name = 'fund_sec_cover'")),
        "{lookups:?}"
    );
    assert_eq!(
        e.unload_objects(),
        0,
        "UNLOAD prefixes are removed after the commit"
    );

    // A value the adopted type can't hold fails the replication; nothing of it commits.
    e.fake.sql("INSERT INTO fund.fund_sec_cover VALUES ('S0004-D', 'Delta', 'US', true, 3000000000); INSERT INTO fund.fund_sec_hist VALUES ('S0004-D', '2025-01-01', NULL, 'E-400');");
    let r = e.run(&["acme_reference"]).await;
    assert_eq!(r.code, 1, "{}", r.stderr);
    let err = &r.replication("acme_reference")["error"];
    assert_eq!(err["code"], "cast_failed", "{err:#}");
    assert_eq!(
        e.lake_one("SELECT count(*)::VARCHAR FROM lake.br.fund_sec_hist"),
        "3",
        "the replication is one transaction"
    );
    assert_eq!(
        e.unload_objects(),
        0,
        "a failed unit's UNLOADs are removed too"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actuals_daily_and_hourly_share_one_op() {
    let e = setup(&["actuals"]).await;
    let v = e.atlatl(&["validate"]).await;
    assert_eq!(v.code, 0, "{}", v.stderr);
    assert!(
        v.stderr.trim().is_empty(),
        "one op, a shared lock_group: no diagnostics (e.g. shared-target)\n{}",
        v.stderr
    );

    let r = e.run(&["acme_actuals", "acme_hourly_actuals"]).await;
    r.ok();
    for rep in ["acme_actuals", "acme_hourly_actuals"] {
        assert_eq!(r.replication(rep)["op"], "acme_actuals");
        assert_eq!(r.replication(rep)["status"], "succeeded");
    }
    let names: Vec<&str> = r
        .members("acme/est/est_basic_act")
        .iter()
        .map(|m| m["member"].as_str().expect("m"))
        .collect();
    assert_eq!(
        names,
        [
            "est.est_basic_act_af",
            "est.est_basic_act_qf",
            "est.est_basic_act_saf"
        ]
    );
    assert_eq!(
        r.rows(
            "acme_hourly/est/est_basic_act",
            "est.est_basic_act_af",
            "extracted"
        ),
        3,
        "NULL published_on rows don't move hourly"
    );
    for t in [
        "est_basic_act_af",
        "est_basic_act_qf",
        "est_basic_act_saf",
        "est_adv_act_af",
    ] {
        e.assert_mirrors(&format!("est.{t}"), t);
    }
    assert_eq!(e.type_of("est_adv_act_af", "value"), "DECIMAL(28,8)");
    assert_eq!(
        e.lake_one("SELECT value::VARCHAR FROM lake.br.est_adv_act_af WHERE sec_id = 'S0001-A'"),
        "99999999999999999999.12345678"
    );
    let st = e.state("acme_hourly/est/est_basic_act", "est.est_basic_act_af");
    assert!(st.contains("2025-02-10") && st.contains("DATE"), "{st}");
    assert_eq!(
        e.lake_one(
            "SELECT count(*)::VARCHAR FROM lake._atlatl.stream_state WHERE stream_id LIKE 'acme/%'"
        ),
        "0",
        "the daily merge keeps no cursor"
    );

    // Upstream: a re-publication, a new drop, and a revision to a row without a publish date.
    e.fake.sql(
        "UPDATE est.est_basic_act_af SET value = 1.3, published_on = '2025-06-01' WHERE sec_id = 'S0001-A' AND metric = 'EPS';
         INSERT INTO est.est_basic_act_af VALUES ('S0004-D', 'EPS', '2025-03-31', 'EUR', 2.5, '2025-06-02');
         UPDATE est.est_basic_act_af SET value = -0.75 WHERE sec_id = 'S0002-B';",
    );
    let before = e.unloads().len();
    let r = e.run(&["acme_hourly_actuals"]).await;
    r.ok();
    let sql = e.unload_for("est.est_basic_act_af", before);
    assert_eq!(sql.len(), 1);
    assert!(
        sql[0].contains(r#"WHERE ("published_on" >= CAST(''2025-02-10'' AS date))"#),
        "the watermark is pushed into the UNLOAD: {}",
        sql[0]
    );
    let m = r.member("acme_hourly/est/est_basic_act", "est.est_basic_act_af");
    assert_eq!(
        m["chunks"], 2,
        "applied per UNLOADed file, the watermark still advances"
    );
    assert_eq!(
        (
            m["rows"]["extracted"].as_u64(),
            m["rows"]["inserted"].as_u64(),
            m["rows"]["updated"].as_u64()
        ),
        (Some(3), Some(1), Some(1))
    );
    assert_eq!(
        e.lake_one("SELECT value::VARCHAR FROM lake.br.est_basic_act_af WHERE sec_id = 'S0002-B'"),
        "-0.50000000",
        "not seen hourly"
    );
    assert!(
        e.state("acme_hourly/est/est_basic_act", "est.est_basic_act_af")
            .contains("2025-06-02")
    );

    let r = e.run(&["acme_actuals", "acme_hourly_actuals"]).await;
    r.ok();
    for t in [
        "est_basic_act_af",
        "est_basic_act_qf",
        "est_basic_act_saf",
        "est_adv_act_af",
    ] {
        e.assert_mirrors(&format!("est.{t}"), t);
    }
    assert_eq!(
        r.rows("acme/est/est_basic_act", "est.est_basic_act_af", "updated"),
        1,
        "the daily merge picks up the undated revision"
    );
    assert_eq!(e.unload_objects(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn financials_discovery_is_sorted_and_the_hourly_window_is_pushed_down() {
    let e = setup(&["financials"]).await;
    // A server collation that isn't byte-wise must not change the member order.
    e.fake.sql(
        "CREATE OR REPLACE VIEW svv_tables AS SELECT table_catalog, table_schema COLLATE NOCASE AS table_schema,
           table_name COLLATE NOCASE AS table_name, table_type
         FROM information_schema.tables WHERE table_schema NOT IN ('main', 'information_schema', 'pg_catalog');",
    );
    let r = e.run(&["acme_financials", "acme_hourly_financials"]).await;
    r.ok();
    let want = [
        "fund.fund_basic_Zf",
        "fund.fund_basic_af",
        "fund.fund_basic_der_af",
        "fund.fund_basic_qf",
        "fund.fund_basic_saf",
    ];
    for s in ["acme/fund/fund_basic", "acme_hourly/fund/fund_basic"] {
        let names: Vec<&str> = r
            .members(s)
            .iter()
            .map(|m| m["member"].as_str().expect("m"))
            .collect();
        assert_eq!(
            names, want,
            "{s}: byte-wise, views, excluded and dotted names skipped"
        );
    }
    for t in ["fund_basic_tmp", "fund_basic_view", "fund_basic_x.y"] {
        assert!(!e.table_exists(t), "{t}");
    }
    for t in [
        "fund_basic_af",
        "fund_basic_qf",
        "fund_basic_saf",
        "fund_basic_der_af",
        "fund_basic_Zf",
        "fund_adv_af",
        "fund_adv_qf",
    ] {
        e.assert_mirrors(&format!("fund.\"{t}\""), &format!("\"{t}\""));
    }
    assert_eq!(e.type_of("fund_basic_af", "sales"), "DECIMAL(38,10)");
    assert_eq!(e.type_of("fund_basic_af", "eps"), "DECIMAL(18,4)");
    assert_eq!(e.type_of("fund_basic_af", "employees"), "INTEGER");
    assert_eq!(e.type_of("fund_basic_der_af", "margin"), "DOUBLE");
    assert_eq!(
        e.lake_one("SELECT sales::VARCHAR FROM lake.br.fund_basic_af WHERE employees = 1200"),
        "12345678901234567890.1234567890"
    );

    let hourly = e
        .unload_for("fund.fund_basic_af", 0)
        .into_iter()
        .filter(|s| s.contains("current_date"))
        .collect::<Vec<_>>();
    assert_eq!(hourly.len(), 1, "{:?}", e.unloads());
    assert!(
        hourly[0].contains("WHERE (period_end >= current_date - 120)"),
        "{}",
        hourly[0]
    );
    let x = |s: &str, m: &str| r.rows(s, m, "extracted");
    assert_eq!(
        (
            x("acme/fund/fund_basic", "fund.fund_basic_af"),
            x("acme_hourly/fund/fund_basic", "fund.fund_basic_af")
        ),
        (3, 2)
    );
    assert_eq!(
        (
            x("acme/fund/fund_adv", "fund.fund_adv_af"),
            x("acme_hourly/fund/fund_adv", "fund.fund_adv_af")
        ),
        (2, 1)
    );

    // A late filing for an old period only moves daily.
    e.fake.sql("INSERT INTO fund.fund_basic_qf VALUES ('S0002-B', current_date - 300, 'GBP', 1, 0.1, 10), ('S0002-B', current_date - 1, 'GBP', 2, 0.2, 10);");
    let r = e.run(&["acme_hourly_financials"]).await;
    r.ok();
    assert_eq!(
        r.rows(
            "acme_hourly/fund/fund_basic",
            "fund.fund_basic_qf",
            "inserted"
        ),
        1
    );
    let r = e.run(&["acme_financials", "acme_hourly_financials"]).await;
    r.ok();
    assert_eq!(
        r.rows("acme/fund/fund_basic", "fund.fund_basic_qf", "inserted")
            + r.rows(
                "acme_hourly/fund/fund_basic",
                "fund.fund_basic_qf",
                "inserted"
            ),
        1
    );
    e.assert_mirrors("fund.fund_basic_qf", "fund_basic_qf");
    assert_eq!(e.unload_objects(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consensus_applies_file_groups_in_one_commit() {
    let e = setup(&["consensus"]).await;
    e.fake.state().slices = 4;
    let r = e.run(&["acme_consensus", "acme_hourly_consensus"]).await;
    r.ok();
    for t in [
        "est_basic_cons_af",
        "est_basic_cons_qf",
        "est_basic_cons_rec",
        "est_adv_cons_af",
    ] {
        e.assert_mirrors(&format!("est.{t}"), t);
    }
    let big = r.member("acme/est/est_adv_cons", "est.est_adv_cons_af");
    assert_eq!(big["chunks"], 4, "one chunk per UNLOADed file");
    assert_eq!(big["rows"]["extracted"], 20000, "{big:#}");
    assert_eq!(
        r.member("acme/est/est_basic_cons_rec", "est.est_basic_cons_rec")["chunks"],
        2
    );
    assert_eq!(e.type_of("est_adv_cons_af", "mean"), "DECIMAL(38,10)");
    assert_eq!(
        e.type_of("est_adv_cons_af", "updated_ts"),
        "TIMESTAMP WITH TIME ZONE"
    );
    assert_eq!(
        e.lake_one("SELECT mean::VARCHAR FROM lake.br.est_adv_cons_af WHERE n_est = 5 ORDER BY mean LIMIT 1"),
        "12345678901234567890.0000000005"
    );
    let within = |t: &str| {
        e.source_q(&format!(
            "SELECT count(*)::VARCHAR FROM est.{t} WHERE window_start >= current_date - 45"
        ))[0]
            .parse::<u64>()
            .expect("n")
    };
    assert_eq!(
        r.rows(
            "acme_hourly/est/est_adv_cons",
            "est.est_adv_cons_af",
            "extracted"
        ),
        within("est_adv_cons_af")
    );
    assert_eq!(
        r.rows(
            "acme_hourly/est/est_basic_consf",
            "est.est_basic_cons_af",
            "extracted"
        ),
        within("est_basic_cons_af")
    );
    assert!(within("est_adv_cons_af") < 20000);

    e.fake.sql(
        "UPDATE est.est_adv_cons_af SET mean = mean + 1 WHERE n_est = 0;
         INSERT INTO est.est_adv_cons_af VALUES ('S9999', 'M0', '2026-12-31', current_date, 1, 1, TIMESTAMPTZ '2026-01-01 00:00:00+00');",
    );
    let changed = e.source_q("SELECT count(*)::VARCHAR FROM est.est_adv_cons_af WHERE n_est = 0")
        [0]
    .parse::<u64>()
    .expect("n");
    let r = e.run(&["acme_consensus"]).await;
    r.ok();
    let m = &r.member("acme/est/est_adv_cons", "est.est_adv_cons_af")["rows"];
    assert_eq!(
        (m["inserted"].as_u64(), m["updated"].as_u64()),
        (Some(1), Some(changed)),
        "{m:#}"
    );
    e.assert_mirrors("est.est_adv_cons_af", "est_adv_cons_af");
    assert_eq!(e.unload_objects(), 0);

    // A key repeated across two file groups is a data error, not a silent last-wins merge.
    e.fake.sql("INSERT INTO est.est_adv_cons_af SELECT sec_id, metric, fp_end, window_start, mean + 7, n_est, updated_ts FROM est.est_adv_cons_af WHERE sec_id = 'S0002' AND metric = 'M0' AND window_start = current_date;");
    let before = e.lake_one("SELECT count(*)::VARCHAR FROM lake.br.est_adv_cons_af");
    let r = e.run(&["acme_consensus"]).await;
    assert_eq!(r.code, 1, "{}", r.stderr);
    assert_eq!(
        r.replication("acme_consensus")["error"]["code"],
        "duplicate_primary_key",
        "{:#}",
        r.replication("acme_consensus")["error"]
    );
    assert_eq!(
        e.lake_one("SELECT count(*)::VARCHAR FROM lake.br.est_adv_cons_af"),
        before
    );
    assert_eq!(e.unload_objects(), 0);
}
