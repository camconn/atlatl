//! M8.4 readiness: vendor bulk exports through the files connector, end to end through the
//! `atlatl` binary: archive member selection, `clean_csv` header naming, the previous loader's
//! `_legacy_loaded_at` column adopted through `metadata.loaded_at`, archives and UNLOAD manifests
//! in RustFS. Every name is synthetic.

use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use atlatl_testkit::LocalLake;
use serde_json::{Value, json};

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/m8/bulk")
}

fn member(name: &str) -> Vec<u8> {
    std::fs::read(fixture().join("members").join(name)).expect("member")
}

fn zip(members: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let mut z = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    for (name, body) in members {
        z.start_file(*name, zip::write::SimpleFileOptions::default())
            .expect("zip");
        z.write_all(body).expect("zip");
    }
    z.finish().expect("zip").into_inner()
}

fn tar_gz(members: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let mut b = tar::Builder::new(flate2::write::GzEncoder::new(
        Vec::new(),
        flate2::Compression::default(),
    ));
    for (name, body) in members {
        let mut h = tar::Header::new_gnu();
        h.set_size(body.len() as u64);
        h.set_mode(0o644);
        h.set_cksum();
        b.append_data(&mut h, name, body.as_slice()).expect("tar");
    }
    b.into_inner().expect("tar").finish().expect("gz")
}

fn epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs() as i64
}

struct Env {
    lake: LocalLake,
    project: tempfile::TempDir,
    bulk: tempfile::TempDir,
    out: tempfile::TempDir,
    bucket: String,
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

    fn member(&self, stream: &str) -> &Value {
        self.report["replications"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|r| r["streams"].as_array().into_iter().flatten())
            .find(|s| s["stream_id"] == stream)
            .and_then(|s| s["members"].get(0))
            .unwrap_or_else(|| panic!("no stream {stream}"))
    }
}

fn setup() -> Env {
    let project = tempfile::tempdir().expect("tmp");
    for f in [
        "atlatl.yaml",
        "replications/statements.yaml",
        "replications/exports.yaml",
    ] {
        let dst = project.path().join(f);
        std::fs::create_dir_all(dst.parent().expect("parent")).expect("mkdir");
        std::fs::copy(fixture().join(f), dst).expect("copy");
    }
    let bulk = tempfile::tempdir().expect("tmp");
    let income = zip(&[
        ("ALPHA_INCOME.csv", member("ALPHA_INCOME.csv")),
        ("ALPHA_INCOME_KEY.csv", member("ALPHA_INCOME_KEY.csv")),
    ]);
    std::fs::write(bulk.path().join("ALPHA_INCOME.zip"), &income).expect("zip");
    let balance = tar_gz(&[
        (
            "export/ALPHA_BALANCE_KEY.csv",
            member("ALPHA_BALANCE_KEY.csv"),
        ),
        ("export/ALPHA_BALANCE.csv", member("ALPHA_BALANCE.csv")),
    ]);
    std::fs::write(bulk.path().join("ALPHA_BALANCE.tar.gz"), balance).expect("tgz");
    let s3 = atlatl_testkit::rustfs();
    let bucket = s3.fresh_bucket().expect("bucket");
    let (st, body) = s3
        .request_bytes("PUT", &format!("/{bucket}/bulk/ALPHA_INCOME.zip"), &income)
        .expect("put");
    assert_eq!(st, 200, "{body}");
    Env {
        lake: LocalLake::file().expect("lake"),
        project,
        bulk,
        out: tempfile::tempdir().expect("tmp"),
        bucket,
    }
}

impl Env {
    fn run(&self, replications: &[&str]) -> Run {
        let s3 = atlatl_testkit::rustfs();
        let report = self.out.path().join("report.json");
        let _ = std::fs::remove_file(&report);
        let mut c = Command::new(env!("CARGO_BIN_EXE_atlatl"));
        c.arg("--project")
            .arg(self.project.path())
            .arg("--report")
            .arg(&report)
            .args(["--log-format", "json", "run"]);
        for r in replications {
            c.args(["--replication", r]);
        }
        let o = c
            .env("ATLATL_EXTENSION_DIR", atlatl_testkit::extension_dir())
            .env("M8_TMP", self.out.path().join("spill"))
            .env("M8_CATALOG", self.lake.dir.path().join("meta.ducklake"))
            .env("M8_DATA_PATH", self.lake.data_path())
            .env("M8_BULK_DIR", self.bulk.path())
            .env("M8_BUCKET", &self.bucket)
            .env("M8_S3_ENDPOINT", format!("http://{}", s3.endpoint))
            .env("M8_S3_KEY", &s3.key_id)
            .env("M8_S3_SECRET", &s3.secret)
            .env_remove("ATLATL_PROJECT")
            .output()
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

    fn q(&self, sql: &str) -> Vec<String> {
        let c = self.lake.open("lake").expect("open lake");
        let mut s = c.prepare(sql).expect(sql);
        s.query_map([], |r| r.get::<_, Option<String>>(0))
            .expect(sql)
            .map(|r| r.expect("row").unwrap_or_else(|| "NULL".into()))
            .collect()
    }

    fn one(&self, sql: &str) -> String {
        self.q(sql)
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("no row: {sql}"))
    }

    /// `name type` per column, in table order.
    fn columns(&self, table: &str) -> Vec<String> {
        self.q(&format!(
            "SELECT column_name || ' ' || data_type FROM duckdb_columns() WHERE database_name = 'lake' AND schema_name = 'br' AND table_name = '{table}' ORDER BY column_index"
        ))
    }

    fn table_id(&self, table: &str) -> String {
        self.one(&format!(
            "SELECT table_id::VARCHAR FROM __ducklake_metadata_lake.ducklake_table WHERE table_name = '{table}' AND end_snapshot IS NULL"
        ))
    }

    fn exists(&self, table: &str) -> bool {
        self.one(&format!("SELECT count(*)::VARCHAR FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = '{table}'")) == "1"
    }
}

const INCOME_ROWS: [&str; 3] = [
    "AAA|2024|FY|2.98e18|-4.34e20|1.23|NULL|USD|AAA",
    "BBB|2024|FY||NULL|0.5|12.5%|USD|BBB",
    "CCC|2023|FY|100|NULL|1,234.5|-3|EUR|CCC",
];

fn income_rows(e: &Env, table: &str) -> Vec<String> {
    e.q(&format!(
        "SELECT concat_ws('|', ticker, fiscal_year, fiscal_period, coalesce(total_revenue, 'NULL'), coalesce(net_income_loss, 'NULL'), eps_diluted,
           coalesce(_1y_growth, 'NULL'), currency, ticker1) FROM lake.br.{table} ORDER BY ticker"
    ))
}

#[test]
fn bulk_statements_adopt_the_previous_loaders_tables() {
    let e = setup();
    // As the previous loader left it: cleaned headers (its order), all text, an INTEGER epoch.
    let c = e.lake.open("lake").expect("lake");
    c.execute_batch(
        "CREATE SCHEMA lake.br;
         CREATE TABLE lake.br.alpha_income (ticker VARCHAR, fiscal_year VARCHAR, fiscal_period VARCHAR, currency VARCHAR,
           total_revenue VARCHAR, net_income_loss VARCHAR, eps_diluted VARCHAR, _1y_growth VARCHAR, ticker1 VARCHAR, _legacy_loaded_at INTEGER);
         INSERT INTO lake.br.alpha_income VALUES ('OLD', '2019', 'FY', 'USD', '1', '1', '1', '1', 'OLD', 1600000000);",
    )
    .expect("seed");
    drop(c);
    let before = e.columns("alpha_income");
    let id = e.table_id("alpha_income");

    let t0 = epoch();
    let r = e.run(&["bulk_statements"]);
    let t1 = epoch();
    r.ok();
    assert_eq!(
        e.columns("alpha_income"),
        before,
        "the adopted schema is kept: no new columns, no retyping"
    );
    assert_eq!(
        e.table_id("alpha_income"),
        id,
        "full refresh keeps the table's identity"
    );
    assert_eq!(
        income_rows(&e, "alpha_income"),
        INCOME_ROWS,
        "only the data member; NULL and quoted NULL are null, empty stays empty"
    );
    let loaded: Vec<i64> = e
        .q("SELECT DISTINCT _legacy_loaded_at::VARCHAR FROM lake.br.alpha_income")
        .iter()
        .map(|v| v.parse().expect("epoch"))
        .collect();
    assert_eq!(loaded.len(), 1, "one value per run: {loaded:?}");
    assert!(
        (t0..=t1).contains(&loaded[0]),
        "{t0} <= {} <= {t1}",
        loaded[0]
    );
    let m = &r.member("bulk/alpha_income")["rows"];
    assert_eq!(
        (
            m["extracted"].as_u64(),
            m["inserted"].as_u64(),
            m["deleted"].as_u64()
        ),
        (Some(3), Some(3), Some(1)),
        "{m:#}"
    );

    assert_eq!(
        e.columns("alpha_balance"),
        [
            "ticker VARCHAR",
            "fiscal_year VARCHAR",
            "total_assets VARCHAR",
            "total_liabilities VARCHAR",
            "equity VARCHAR",
            "_legacy_loaded_at INTEGER"
        ],
        "a new table: clean_csv names, all text, then the epoch column"
    );
    assert_eq!(
        e.q("SELECT concat_ws('|', ticker, coalesce(total_assets, 'NULL'), total_liabilities, equity) FROM lake.br.alpha_balance ORDER BY ticker"),
        ["AAA|5.5e12|3.1e12|2.4e12", "BBB|NULL||7"]
    );

    // Rerun: the new load time replaces the old; rows are unchanged otherwise.
    std::thread::sleep(std::time::Duration::from_millis(1100));
    let r = e.run(&["bulk_statements"]);
    r.ok();
    let again: i64 = e
        .one("SELECT max(_legacy_loaded_at)::VARCHAR FROM lake.br.alpha_income")
        .parse()
        .expect("epoch");
    assert!(again > loaded[0]);
    assert_eq!(income_rows(&e, "alpha_income"), INCOME_ROWS);
}

#[test]
fn archives_in_object_storage_and_member_globs() {
    let e = setup();
    let r = e.run(&["bulk_statements_s3"]);
    r.ok();
    assert_eq!(income_rows(&e, "alpha_income_s3"), INCOME_ROWS);
    assert_eq!(
        e.columns("alpha_income_s3").last().map(String::as_str),
        Some("_legacy_loaded_at INTEGER")
    );

    let r = e.run(&["bulk_both_members"]);
    assert_eq!(r.code, 1, "{}", r.stderr);
    let err = &r.replication("bulk_both_members")["error"];
    assert_eq!(err["class"], "config", "{err:#}");
    assert!(
        err["message"]
            .as_str()
            .is_some_and(|m| m.contains("union_by_name")),
        "{err:#}"
    );
    assert!(!e.exists("both"));
}

/// Parquet parts plus a VERBOSE manifest in RustFS, as a warehouse UNLOAD leaves them.
fn put_export(e: &Env, rows: &[u64], lie: u64) {
    let s3 = atlatl_testkit::rustfs();
    let c = atlatl_testkit::lake::open_engine().expect("engine");
    c.execute_batch("LOAD httpfs").expect("httpfs");
    c.execute_batch(&s3.duckdb_secret("w", &e.bucket))
        .expect("secret");
    let mut entries = Vec::new();
    let mut start = 0;
    for (i, n) in rows.iter().enumerate() {
        let url = format!("s3://{}/export/run1/{i:04}_part_00.parquet", e.bucket);
        c.execute_batch(&format!(
            "COPY (SELECT range AS id, 'p' || range AS name, (range * 1.5)::DECIMAL(18,2) AS qty FROM range({start}, {})) TO '{url}' (FORMAT parquet)",
            start + n
        ))
        .expect("copy");
        let size: i64 = c
            .query_row(&format!("SELECT size FROM read_blob('{url}')"), [], |r| {
                r.get(0)
            })
            .expect("size");
        entries
            .push(json!({"url": url, "meta": {"content_length": size, "record_count": n + lie}}));
        start += n;
    }
    let total: u64 = rows.iter().sum::<u64>() + lie * rows.len() as u64;
    let m = json!({"entries": entries, "meta": {"record_count": total}});
    let (st, body) = s3
        .request(
            "PUT",
            &format!("/{}/export/run1/manifest", e.bucket),
            &m.to_string(),
        )
        .expect("put");
    assert_eq!(st, 200, "{body}");
}

#[test]
fn unload_manifests_in_object_storage() {
    let e = setup();
    put_export(&e, &[3, 4, 5], 0);
    let r = e.run(&["export_chunked", "export_whole"]);
    r.ok();
    let m = r.member("export/positions");
    assert_eq!(m["chunks"], 3, "one chunk per manifest entry");
    assert_eq!(m["rows"]["extracted"], 12, "{m:#}");
    assert_eq!(r.member("export/positions_whole")["rows"]["inserted"], 12);
    for t in ["positions", "positions_whole"] {
        assert_eq!(
            e.one(&format!(
                "SELECT count(*)::VARCHAR || '/' || sum(qty)::VARCHAR FROM lake.br.{t}"
            )),
            "12/99.00"
        );
        assert_eq!(
            e.columns(t),
            ["id BIGINT", "name VARCHAR", "qty DECIMAL(18,2)"]
        );
    }

    // A manifest whose counts disagree with its objects fails both ways; nothing commits.
    put_export(&e, &[3, 4, 4], 1);
    let r = e.run(&["export_chunked", "export_whole"]);
    assert_eq!(r.code, 1, "{}", r.stderr);
    for rep in ["export_chunked", "export_whole"] {
        let err = &r.replication(rep)["error"];
        assert_eq!(err["class"], "decode", "{rep}: {err:#}");
    }
    for t in ["positions", "positions_whole"] {
        assert_eq!(
            e.one(&format!("SELECT count(*)::VARCHAR FROM lake.br.{t}")),
            "12",
            "{t}"
        );
    }

    put_export(&e, &[3, 4, 4], 0);
    let r = e.run(&["export_chunked"]);
    r.ok();
    let m = &r.member("export/positions")["rows"];
    assert_eq!(
        (m["deleted"].as_u64(), m["unchanged"].as_u64()),
        (Some(1), Some(11)),
        "keyed full refresh across chunks: {m:#}"
    );
}
