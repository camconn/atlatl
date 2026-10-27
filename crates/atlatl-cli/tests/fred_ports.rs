//! M8.1a–M8.4a: public-data analogues of the consumer ports, run on `examples/fred/` (keyless
//! FRED graph CSVs, local infrastructure only). Each follows 13 §2 as far as it applies to an
//! ingest port: port (reviewed schema, cassettes) → parity against the tables a stand-in legacy
//! loader left → cut-over onto those tables → a second run with nothing to write.
//!
//! Parity is `atlatl run --dry-run` against the legacy tables (03 §11): the port's staged rows,
//! cast to the legacy column types, diffed by key with every difference sampled ([`Diff`]), and
//! each one needs a named cause. A dry run in the `parity` environment (an empty scratch lake)
//! shows the DDL a fresh table would get.
//!
//! CI replays the committed cassettes (`examples/fred/apis/fred/cassettes/`) and the FRED data
//! recorded with them (`examples/fred/data/`). `record_fred_cassettes` and `live_*` call
//! fred.stlouisfed.org: they only run with the `network` feature (`cargo test-network`, 12 §5).

mod dogfood_support;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use atlatl_testkit::LocalLake;
use dogfood_support::{copy_dir, out};
use serde_json::Value;

const FRED: &str = "https://fred.stlouisfed.org";

fn example() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/fred")
}

// ----------------------------------------------------------------------------- harness

/// A copy of `examples/fred` with a production lake and a scratch (parity) lake.
struct Fred {
    dir: tempfile::TempDir,
    lake: LocalLake,
    parity: LocalLake,
    env: Vec<(String, String)>,
    live: bool,
}

struct Run {
    code: i32,
    stderr: String,
    stdout: String,
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

    fn member(&self, stream: &str, member: Option<&str>) -> &Value {
        self.members(stream)
            .into_iter()
            .find(|m| member.is_none_or(|n| m["member"] == n))
            .unwrap_or_else(|| panic!("no member {member:?} of {stream} in {:#}", self.report))
    }

    /// `(inserted, updated, deleted)`.
    fn written(&self, stream: &str, member: Option<&str>) -> (u64, u64, u64) {
        let r = &self.member(stream, member)["rows"];
        let n = |k: &str| r[k].as_u64().expect("count");
        (n("inserted"), n("updated"), n("deleted"))
    }

    fn status(&self, replication: &str) -> &str {
        self.replication(replication)["status"]
            .as_str()
            .expect("status")
    }

    /// A dry run's entry for one member (the first, with `member: None`).
    fn diff(&self, stream: &str, member: Option<&str>) -> Diff {
        let v: Value = serde_json::from_str(&self.stdout)
            .unwrap_or_else(|e| panic!("dry-run JSON: {e}\n{}", self.stdout));
        assert_eq!(v["dry_run"], true, "{v:#}");
        let entry = v["replications"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|r| r["streams"].as_array().into_iter().flatten())
            .find(|e| e["stream"] == stream && member.is_none_or(|m| e["member"] == m))
            .unwrap_or_else(|| panic!("no {stream} {member:?} in {v:#}"))
            .clone();
        Diff(entry)
    }
}

impl Fred {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tmp");
        for e in std::fs::read_dir(example()).expect("example") {
            let e = e.expect("entry");
            let name = e.file_name();
            if [".lake", ".lake-parity", ".bulk"].contains(&name.to_str().unwrap_or_default()) {
                continue;
            }
            if e.file_type().expect("type").is_dir() {
                copy_dir(&e.path(), &dir.path().join(&name));
            } else {
                std::fs::copy(e.path(), dir.path().join(&name)).expect("copy");
            }
        }
        Self {
            dir,
            lake: LocalLake::file().expect("lake"),
            parity: LocalLake::file().expect("lake"),
            env: Vec::new(),
            live: false,
        }
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }

    fn set(&mut self, k: &str, v: impl Into<String>) {
        self.env.push((k.to_owned(), v.into()));
    }

    /// Replaces `from` (which must occur) in a project file.
    fn edit(&self, file: &str, from: &str, to: &str) {
        let p = self.path().join(file);
        let text = std::fs::read_to_string(&p).expect(file);
        assert!(text.contains(from), "{file}: no `{from}`");
        std::fs::write(&p, text.replace(from, to)).expect(file);
    }

    fn cmd(&self, args: &[&str]) -> Command {
        let mut c = Command::new(env!("CARGO_BIN_EXE_atlatl"));
        c.arg("--project")
            .arg(self.path())
            .args(["--log-format", "json"])
            .args(args)
            .env_remove("ATLATL_PROJECT")
            .env_remove("ATLATL_CASSETTE")
            .env_remove("ATLATL_CASSETTE_RECORD")
            .env("ATLATL_EXTENSION_DIR", atlatl_testkit::extension_dir())
            .env("FRED_LAKE", self.lake.dir.path())
            .env("FRED_PARITY_LAKE", self.parity.dir.path());
        if !self.live {
            // Loopback only: a request that escapes the cassettes fails instead of reaching FRED.
            for k in [
                "HTTP_PROXY",
                "HTTPS_PROXY",
                "ALL_PROXY",
                "http_proxy",
                "https_proxy",
                "all_proxy",
            ] {
                c.env(k, "http://127.0.0.1:9");
            }
            for k in ["NO_PROXY", "no_proxy"] {
                c.env(k, "127.0.0.1,localhost");
            }
        }
        for (k, v) in &self.env {
            c.env(k, v);
        }
        c
    }

    fn atlatl(&self, args: &[&str]) -> Output {
        self.cmd(args).output().expect("atlatl")
    }

    /// `atlatl run` of `replications` (one op), in `env` (`None`: dev), replaying `cassette`.
    fn run_in(&self, env: Option<&str>, replications: &[&str], cassette: Option<&str>) -> Run {
        self.run_with(env, replications, cassette, &[])
    }

    /// `atlatl run --dry-run`, every difference sampled.
    fn dry_run(&self, env: Option<&str>, replications: &[&str], cassette: Option<&str>) -> Run {
        self.run_with(
            env,
            replications,
            cassette,
            &["--dry-run", "--diff-samples", "100000"],
        )
    }

    fn run_with(
        &self,
        env: Option<&str>,
        replications: &[&str],
        cassette: Option<&str>,
        extra: &[&str],
    ) -> Run {
        let report = self.path().join("report.json");
        let _ = std::fs::remove_file(&report);
        let mut c = self.cmd(&["--report", report.to_str().expect("utf8"), "run"]);
        if let Some(e) = env {
            c.args(["--env", e]);
        }
        for r in replications {
            c.args(["--replication", r]);
        }
        c.args(extra);
        if let Some(k) = cassette {
            c.env("ATLATL_CASSETTE", k);
        }
        let o = c.output().expect("atlatl run");
        let report = std::fs::read(&report)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or(Value::Null);
        Run {
            code: o.status.code().expect("exit code"),
            stderr: String::from_utf8_lossy(&o.stderr).into_owned(),
            stdout: String::from_utf8_lossy(&o.stdout).into_owned(),
            report,
        }
    }

    fn run(&self, replications: &[&str], cassette: Option<&str>) -> Run {
        self.run_in(None, replications, cassette)
    }

    /// A session with the production lake as `lake` and the scratch lake as `parity`.
    fn conn(&self) -> duckdb::Connection {
        let c = self.lake.open("lake").expect("open lake");
        c.execute_batch(&self.parity.attach_sql("parity"))
            .expect("attach parity");
        c
    }

    fn sql(&self, sql: &str) {
        self.conn().execute_batch(sql).expect(sql);
    }

    fn q(&self, sql: &str) -> Vec<String> {
        let c = self.conn();
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

    fn count(&self, table: &str) -> u64 {
        self.one(&format!("SELECT count(*)::VARCHAR FROM {table}"))
            .parse()
            .expect("count")
    }

    /// `name type` per column of `lake.br.<table>`, in table order.
    fn columns(&self, db: &str, table: &str) -> Vec<String> {
        self.q(&format!(
            "SELECT column_name || ' ' || data_type FROM duckdb_columns() WHERE database_name = '{db}' AND schema_name = 'br' AND table_name = '{table}' ORDER BY column_index"
        ))
    }

    fn exists_in(&self, db: &str, schema: &str, table: &str) -> bool {
        self.one(&format!(
            "SELECT count(*)::VARCHAR FROM duckdb_tables() WHERE database_name = '{db}' AND schema_name = '{schema}' AND table_name = '{table}'"
        )) == "1"
    }

    fn exists(&self, db: &str, table: &str) -> bool {
        self.exists_in(db, "br", table)
    }

    fn table_id(&self, table: &str) -> String {
        self.one(&format!(
            "SELECT table_id::VARCHAR FROM __ducklake_metadata_lake.ducklake_table WHERE table_name = '{table}' AND end_snapshot IS NULL"
        ))
    }

    /// Snapshots of the production lake that changed `br.<table>`.
    fn table_snapshots(&self, table: &str) -> u64 {
        let id = self.table_id(table);
        self.q("SELECT changes::VARCHAR FROM ducklake_snapshots('lake')")
            .iter()
            .filter(|ch| {
                ch.contains(&format!("br.{table}"))
                    || ch
                        .split(['[', ']', ',', ' '])
                        .any(|t| t.trim() == id.as_str())
            })
            .count() as u64
    }
}

/// One member's `run --dry-run` entry (03 §11) against the table the legacy loader left.
#[derive(Debug)]
struct Diff(Value);

impl Diff {
    fn n(&self, k: &str) -> u64 {
        self.0[k]
            .as_u64()
            .unwrap_or_else(|| panic!("{k}: {:#}", self.0))
    }

    /// `(inserts, updates, deletes)`.
    fn counts(&self) -> (u64, u64, u64) {
        (self.n("inserts"), self.n("updates"), self.n("deletes"))
    }

    fn ddl(&self) -> Vec<String> {
        self.0["ddl"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|d| d.as_str().expect("ddl").to_owned())
            .collect()
    }

    /// Nothing for a cut-over or rerun to change.
    fn is_clean(&self) -> bool {
        self.counts() == (0, 0, 0) && self.ddl().is_empty()
    }

    /// Counts per cause. Every difference is sampled and must have one: an unexplained row
    /// fails the review. `a` is the port's row (key and changed values), `b` the legacy one.
    fn explain(
        &self,
        cause: impl Fn(&str, &Value, Option<&Value>) -> Option<&'static str>,
    ) -> BTreeMap<String, u64> {
        assert!(self.0["counts_only"].is_null(), "{:#}", self.0);
        let samples = self.0["samples"].as_array().expect("samples");
        for op in ["insert", "update", "delete"] {
            let n = samples.iter().filter(|x| x["op"] == op).count() as u64;
            assert_eq!(n, self.n(&format!("{op}s")), "every {op} sampled");
        }
        let merged = |x: &Value, side: &str| {
            let mut m = x["key"].as_object().cloned().unwrap_or_default();
            if let Some(o) = x[side].as_object() {
                m.extend(o.clone());
            }
            Value::Object(m)
        };
        let mut out = BTreeMap::new();
        for x in samples {
            let op = x["op"].as_str().expect("op");
            let a = merged(x, "staged");
            let b = (op == "update").then(|| merged(x, "target"));
            let c = cause(op, &a, b.as_ref()).unwrap_or_else(|| panic!("unexplained {op}: {x}"));
            *out.entry(format!("{op}: {c}")).or_insert(0) += 1;
        }
        out
    }
}

fn s<'a>(v: &'a Value, k: &str) -> &'a str {
    v[k].as_str().unwrap_or("NULL")
}

fn counts(pairs: &[(&str, u64)]) -> BTreeMap<String, u64> {
    pairs
        .iter()
        .filter(|(_, n)| *n > 0)
        .map(|(k, n)| ((*k).to_owned(), *n))
        .collect()
}

// ----------------------------------------------------------------------------- recorded data

fn cassette(name: &str) -> Value {
    let p = example().join(format!("apis/fred/cassettes/{name}.json"));
    serde_json::from_str(&std::fs::read_to_string(&p).expect("cassette")).expect("cassette json")
}

/// `(url, body)` of every recorded interaction.
fn bodies(name: &str) -> Vec<(String, String)> {
    cassette(name)["interactions"]
        .as_array()
        .expect("interactions")
        .iter()
        .map(|i| {
            (
                i["request"]["url"].as_str().expect("url").to_owned(),
                i["response"]["body"].as_str().expect("body").to_owned(),
            )
        })
        .collect()
}

/// `(date, value)` rows of a `fredgraph.csv` body; an empty value is `None`.
fn csv_rows(body: &str) -> Vec<(String, Option<String>)> {
    body.lines()
        .skip(1)
        .filter(|l| !l.is_empty())
        .map(|l| {
            let (d, v) = l.split_once(',').expect("csv row");
            (d.to_owned(), (!v.is_empty()).then(|| v.to_owned()))
        })
        .collect()
}

/// A recorded series (`examples/fred/data/<ID>.csv`).
fn data(id: &str) -> Vec<(String, Option<String>)> {
    csv_rows(&std::fs::read_to_string(example().join(format!("data/{id}.csv"))).expect("data"))
}

fn data_path(id: &str) -> String {
    example()
        .join(format!("data/{id}.csv"))
        .canonicalize()
        .expect("data file")
        .display()
        .to_string()
}

/// `(year, month)` of a cassette's `recorded_at`: its replay's pinned `now()`.
fn recorded_month(name: &str) -> (i32, u32) {
    let v = cassette(name);
    let t = v["recorded_at"].as_str().expect("recorded_at");
    (
        t[..4].parse().expect("year"),
        t[5..7].parse().expect("month"),
    )
}

fn month_index((y, m): (i32, u32)) -> i32 {
    y * 12 + m as i32 - 1
}

fn from_index(i: i32) -> (i32, u32) {
    (i.div_euclid(12), i.rem_euclid(12) as u32 + 1)
}

/// `range(start, now, "<step>mo")` for starts on the 1st: the number of windows.
fn windows(start: (i32, u32), now: (i32, u32), step: i32) -> u64 {
    ((month_index(now) - month_index(start)).div_euclid(step) + 1) as u64
}

/// `date_trunc(date_add(now(), -6, "month"), "quarter")`.
fn floor(now: (i32, u32)) -> (i32, u32) {
    let (y, m) = from_index(month_index(now) - 6);
    (y, (m - 1) / 3 * 3 + 1)
}

fn first_of((y, m): (i32, u32)) -> String {
    format!("{y:04}-{m:02}-01")
}

// ----------------------------------------------------------------------------- M8.1a: REST

/// The series of the `series` endpoint and their `cosd` (apis/fred.yaml).
const SERIES: [(&str, &str); 4] = [
    ("DFF", "2025-07-01"),
    ("DGS10", "2025-07-01"),
    ("UNRATE", "2020-01-01"),
    ("GDPC1", "2015-01-01"),
];

struct Case {
    endpoint: &'static str,
    stream: &'static str,
    cassette: &'static str,
    args: &'static [&'static str],
}

const CASES: [Case; 3] = [
    Case {
        endpoint: "dff",
        stream: "fred/dff",
        cassette: "fred_dff_run1",
        args: &[],
    },
    Case {
        endpoint: "dff_windows",
        stream: "fred/dff_windows",
        cassette: "fred_dff_daily_run1",
        args: &["--items", "20"],
    },
    Case {
        endpoint: "series",
        stream: "fred/series",
        cassette: "fred_series_run1",
        args: &["--items", "4"],
    },
];

/// The `id` query parameter of a recorded URL.
fn series_of(url: &str) -> String {
    url.split(['?', '&'])
        .find_map(|p| p.strip_prefix("id="))
        .expect("id")
        .to_owned()
}

fn recorded_rows(c: &Case) -> u64 {
    let rows: Vec<(String, String)> = bodies(c.cassette)
        .iter()
        .flat_map(|(u, b)| {
            let id = series_of(u);
            csv_rows(b).into_iter().map(move |(d, _)| (id.clone(), d))
        })
        .collect();
    let mut keys: Vec<&(String, String)> = rows.iter().collect();
    keys.sort();
    keys.dedup();
    keys.len() as u64
}

#[test]
fn api_test_and_schema_check_replay_every_endpoint_offline() {
    let f = Fred::new();
    for c in &CASES {
        let mut args = vec![
            "api", "test", "fred", c.endpoint, "--replay", c.cassette, "--json",
        ];
        args.extend(c.args);
        let o = f.atlatl(&args);
        let ctx = format!("{}: {}", c.endpoint, out(&o));
        assert!(o.status.success(), "{ctx}");
        let v: Value = serde_json::from_slice(&o.stdout).expect("json");
        assert!(v["error"].is_null(), "{ctx}");
        assert_eq!(v["record_count"], recorded_rows(c), "{ctx}");
        for i in v["interactions"].as_array().expect("interactions") {
            let u = i["request"]["url"].as_str().expect("url");
            assert!(
                u.starts_with(&format!("{FRED}/graph/fredgraph.csv?")),
                "{u}"
            );
        }

        assert!(
            f.path().join(format!("schemas/{}.yaml", c.stream)).exists(),
            "{}: no reviewed schema",
            c.stream
        );
        let mut args = vec!["schema", "check", c.stream, "--from-cassette", c.cassette];
        args.extend(c.args);
        let o = f.atlatl(&args);
        assert!(o.status.success(), "{}: {}", c.stream, out(&o));
        assert!(
            String::from_utf8_lossy(&o.stdout).contains(&format!("{}: ok", c.stream)),
            "{}",
            out(&o)
        );
    }
}

#[test]
fn committed_recordings_hold_no_cookies_and_match_the_data_files() {
    let dir = example().join("apis/fred/cassettes");
    let mut n = 0;
    for e in std::fs::read_dir(&dir).expect("cassettes") {
        let text = std::fs::read_to_string(e.expect("entry").path()).expect("read");
        let v: Value = serde_json::from_str(&text).expect("json");
        for i in v["interactions"].as_array().expect("interactions") {
            for h in i["response"]["headers"].as_array().into_iter().flatten() {
                if h[0] == "set-cookie" {
                    assert_eq!(h[1], "****", "{h}");
                }
            }
            for h in i["request"]["headers"].as_array().into_iter().flatten() {
                assert_ne!(h[0], "cookie", "{h}");
            }
        }
        assert!(
            !text.contains("_abck=") && !text.contains("bm_sz="),
            "cookies"
        );
        n += 1;
    }
    assert_eq!(n, 4, "one cassette per recorded run");
    let dff: Vec<(String, Option<String>)> = bodies("fred_dff_daily_run1")
        .iter()
        .flat_map(|(_, b)| csv_rows(b))
        .collect();
    assert_eq!(data("DFF"), dff, "data/DFF.csv is the recorded windows");
    for (id, _) in &SERIES[1..] {
        let (_, body) = bodies("fred_series_run1")
            .into_iter()
            .find(|(u, _)| series_of(u) == *id)
            .expect("recorded series");
        assert_eq!(data(id), csv_rows(&body), "data/{id}.csv");
    }
}

#[test]
fn windowed_walk_then_the_stored_floor() {
    let f = Fred::new();
    let r = f.run(&["fred_dff"], Some("fred_dff_run1"));
    r.ok();
    assert_eq!(
        r.member("fred/dff", None)["schema"]["status"],
        "ok",
        "single endpoint"
    );
    assert_eq!(f.count("lake.br.fred_dff"), recorded_rows(&CASES[0]));

    let now = recorded_month("fred_dff_daily_run1");
    let r = f.run(&["fred_dff_daily"], Some("fred_dff_daily_run1"));
    r.ok();
    let m = r.member("fred/dff_windows", None);
    assert_eq!(m["schema"]["status"], "ok", "{m:#}");
    assert_eq!(
        m["http"]["requests"].as_u64(),
        Some(windows((2024, 1), now, 6)),
        "half-year windows from the fixed start through now: {m:#}"
    );
    assert_eq!(f.count("lake.br.fred_dff_daily"), data("DFF").len() as u64);
    // 07 §9 / spike S10: end-inclusive, calendar steps from the start, the window's last day
    // one day before the next start.
    let half = |i: i32| {
        let (y, m) = from_index(month_index((2024, 1)) + 6 * i);
        let (ey, em) = from_index(month_index((2024, 1)) + 6 * i + 5);
        let last = if em == 6 { 30 } else { 31 };
        format!("cosd={y:04}-{m:02}-01&coed={ey:04}-{em:02}-{last}")
    };
    let urls: Vec<String> = bodies("fred_dff_daily_run1")
        .into_iter()
        .map(|(u, _)| u.split_once("&").expect("params").1.to_owned())
        .collect();
    let want: Vec<String> = (0..windows((2024, 1), now, 6) as i32).map(half).collect();
    assert_eq!(urls, want);
    let fl = floor(now);
    assert_eq!(
        m["state"]["after"]["floor"],
        format!("{}T00:00:00Z", first_of(fl))
    );
    assert!(
        f.columns("lake", "fred_dff_daily")
            .iter()
            .all(|c| c.ends_with(" VARCHAR")),
        "br stays all-text"
    );
    let snaps = f.table_snapshots("fred_dff_daily");

    let now2 = recorded_month("fred_dff_daily_run2");
    let r = f.run(&["fred_dff_daily"], Some("fred_dff_daily_run2"));
    r.ok();
    let m = r.member("fred/dff_windows", None);
    assert_eq!(
        m["http"]["requests"].as_u64(),
        Some(windows(fl, now2, 6)),
        "from the stored floor: {m:#}"
    );
    assert_eq!(r.written("fred/dff_windows", None), (0, 0, 0));
    assert_eq!(
        f.table_snapshots("fred_dff_daily"),
        snaps,
        "an unchanged re-walk doesn't touch the table"
    );
}

#[test]
fn source_schema_drift_fails_before_writing() {
    let f = Fred::new();
    // The vendor switches its missing-value marker from an empty field to "." (market holidays in
    // DGS10, and the UNRATE month BLS never published).
    let path = "apis/fred/cassettes/fred_series_run1.json";
    let text = std::fs::read_to_string(f.path().join(path)).expect("cassette");
    let marked = text.replace(",\\n", ",.\\n");
    assert_ne!(text, marked);
    std::fs::write(f.path().join(path), marked).expect("write");
    let r = f.run(&["fred_series"], Some("fred_series_run1"));
    assert_eq!(r.code, 1, "{}", r.stderr);
    let err = &r.replication("fred_series")["error"];
    assert_eq!(err["code"], "schema_violation", "{err:#}");
    let s = &r.member("fred/series", None)["schema"];
    assert_eq!(s["severity"], "error", "{s:#}");
    let v = &s["violations"][0];
    assert_eq!(
        (v["field"].as_str(), v["kind"].as_str()),
        (Some("value"), Some("type")),
        "{s:#}"
    );
    let missing = bodies("fred_series_run1")
        .iter()
        .flat_map(|(_, b)| csv_rows(b))
        .filter(|(_, v)| v.is_none())
        .count();
    assert!(missing > 0);
    assert_eq!(v["count"].as_u64(), Some(missing as u64), "{s:#}");

    // Or renames the date column: the primary key is gone, and the source schema says why.
    f.edit(
        "apis/fred/cassettes/fred_dff_run1.json",
        "\"body\": \"observation_date,DFF\\n",
        "\"body\": \"DATE,DFF\\n",
    );
    let r = f.run(&["fred_dff"], Some("fred_dff_run1"));
    assert_eq!(r.code, 1, "{}", r.stderr);
    let err = &r.replication("fred_dff")["error"];
    assert_eq!(
        (err["class"].as_str(), err["code"].as_str()),
        (Some("data"), Some("schema_violation")),
        "{err:#}"
    );
    let s = &r.member("fred/dff", None)["schema"];
    assert_eq!(
        (&s["new_fields"], &s["missing_fields"]),
        (
            &serde_json::json!(["DATE"]),
            &serde_json::json!(["observation_date"])
        ),
        "{s:#}"
    );
    assert!(!f.exists("lake", "fred_observations") && !f.exists("lake", "fred_dff"));
}

/// The date the stand-in legacy loader was paused: two weeks before the newest daily observation.
fn paused_at() -> String {
    let last = data("DFF").last().expect("rows").0.clone();
    let c = atlatl_testkit::lake::open_engine().expect("engine");
    c.query_row(&format!("SELECT (DATE '{last}' - 14)::VARCHAR"), [], |r| {
        r.get(0)
    })
    .expect("date")
}

/// GDPC1's latest quarter before the pause, and the (made-up) advance estimate the legacy loader
/// kept for it.
fn gdp_revision(paused: &str) -> (String, String) {
    let (d, v) = data("GDPC1")
        .into_iter()
        .rfind(|(d, v)| d.as_str() <= paused && v.is_some())
        .expect("a quarter");
    let v: f64 = v.expect("value").parse().expect("number");
    (d, format!("{:.3}", v - 12.5))
}

/// What the legacy REST ports left: all-text tables in their own column order, FRED's API marker
/// "." kept for missing observations, nothing after the pause, one revision missed, and DFF
/// history from before the port's start.
fn legacy_rest(f: &Fred) -> (String, (String, String)) {
    let paused = paused_at();
    let (gq, advance) = gdp_revision(&paused);
    let union = SERIES
        .iter()
        .map(|(id, since)| {
            let since = if *id == "DFF" { "2025-06-01" } else { since };
            format!(
                "SELECT '{id}' AS series_id, observation_date::VARCHAR AS d, {id}::VARCHAR AS v FROM read_csv('{}', all_varchar = true) WHERE observation_date >= '{since}'",
                data_path(id)
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    f.sql(&format!(
        "CREATE SCHEMA lake.br;
         CREATE TABLE lake.br.fred_observations (value VARCHAR, observation_date VARCHAR, series_id VARCHAR);
         INSERT INTO lake.br.fred_observations
           SELECT coalesce(v, '.'), d, series_id FROM ({union}) WHERE d <= '{paused}' ORDER BY series_id, d;
         UPDATE lake.br.fred_observations SET value = '{advance}' WHERE series_id = 'GDPC1' AND observation_date = '{gq}';
         CREATE TABLE lake.br.fred_dff_daily (DFF VARCHAR, observation_date VARCHAR);
         INSERT INTO lake.br.fred_dff_daily
           SELECT DFF, observation_date FROM read_csv('{}', all_varchar = true) WHERE observation_date <= '{paused}';",
        data_path("DFF")
    ));
    (paused, (gq, advance))
}

#[test]
fn rest_ports_parity_then_cut_over() {
    let f = Fred::new();
    let (paused, (gq, advance)) = legacy_rest(&f);
    let tables = ["fred_observations", "fred_dff_daily"];
    let before: Vec<(Vec<String>, String)> = tables
        .iter()
        .map(|t| (f.columns("lake", t), f.table_id(t)))
        .collect();

    // Parity: a dry run of the full walk against the legacy tables.
    let snaps: Vec<u64> = tables.iter().map(|t| f.table_snapshots(t)).collect();
    let series = f.dry_run(None, &["fred_series"], Some("fred_series_run1"));
    series.ok();
    let daily = f.dry_run(None, &["fred_dff_daily"], Some("fred_dff_daily_run1"));
    daily.ok();
    assert!(
        !f.exists_in("lake", "_atlatl", "stream_state"),
        "a dry run stores no state"
    );
    assert_eq!(
        tables
            .iter()
            .map(|t| f.table_snapshots(t))
            .collect::<Vec<_>>(),
        snaps,
        "nor touches the tables"
    );
    let since = |id: &str| SERIES.iter().find(|x| x.0 == id).expect("series").1;
    let series_diff = series.diff("fred/series", None);
    let why = series_diff.explain(|op, a, b| {
        let (id, d) = (s(a, "series_id"), s(a, "observation_date"));
        match (op, b) {
            ("insert", None) if d > paused.as_str() => Some("published after the pause"),
            ("update", Some(l)) if s(l, "value") == "." && a["value"].is_null() => {
                Some("\".\" kept for a missing observation")
            }
            ("update", Some(l)) if id == "GDPC1" && d == gq && s(l, "value") == advance => {
                Some("revision missed")
            }
            _ => None,
        }
    });
    assert_eq!(
        f.count(&format!(
            "lake.br.fred_observations WHERE series_id = 'DFF' AND observation_date < '{}'",
            since("DFF")
        )),
        30,
        "history before the port's start: kept, an incremental port never deletes"
    );
    let after_pause = SERIES
        .iter()
        .map(|(id, since)| {
            data(id)
                .iter()
                .filter(|(d, _)| d.as_str() >= *since && d.as_str() > paused.as_str())
                .count() as u64
        })
        .sum::<u64>();
    let dots = SERIES
        .iter()
        .map(|(id, since)| {
            data(id)
                .iter()
                .filter(|(d, v)| {
                    d.as_str() >= *since && d.as_str() <= paused.as_str() && v.is_none()
                })
                .count() as u64
        })
        .sum::<u64>();
    assert!(dots > 0, "DGS10 has market holidays");
    assert_eq!(
        why,
        counts(&[
            ("insert: published after the pause", after_pause),
            ("update: \".\" kept for a missing observation", dots),
            ("update: revision missed", 1),
        ]),
        "{series_diff:#?}"
    );
    let daily_diff = daily.diff("fred/dff_windows", None);
    let why = daily_diff.explain(|op, a, _| {
        (op == "insert" && s(a, "observation_date") > paused.as_str())
            .then_some("published after the pause")
    });
    assert_eq!(why, counts(&[("insert: published after the pause", 14)]));

    // Cut-over: the same runs onto the legacy tables, which are adopted as they are.
    let r = f.run(&["fred_series"], Some("fred_series_run1"));
    r.ok();
    assert_eq!(
        r.written("fred/series", None),
        (after_pause, dots + 1, 0),
        "exactly the reviewed differences; incremental keeps the older history"
    );
    let r = f.run(&["fred_dff_daily"], Some("fred_dff_daily_run1"));
    r.ok();
    assert_eq!(r.written("fred/dff_windows", None), (14, 0, 0));
    for (t, (cols, id)) in tables.iter().zip(&before) {
        assert_eq!(&f.columns("lake", t), cols, "{t}: schema and order kept");
        assert_eq!(&f.table_id(t), id, "{t}: same table");
    }
    let again = f.dry_run(None, &["fred_series"], Some("fred_series_run1"));
    again.ok();
    assert!(again.diff("fred/series", None).is_clean());
    assert_eq!(again.status("fred_series"), "up_to_date");
    assert_eq!(
        f.count("lake.br.fred_observations WHERE value = '.'"),
        0,
        "missing observations are NULL now"
    );

    // The next runs: `sync` walks from the stored floor, and nothing is left to write. The
    // floor itself still commits if it moved.
    let snaps: Vec<u64> = tables.iter().map(|t| f.table_snapshots(t)).collect();
    let r = f.run(&["fred_dff_daily"], Some("fred_dff_daily_run2"));
    r.ok();
    assert_eq!(r.written("fred/dff_windows", None), (0, 0, 0));
    let moved = floor(recorded_month("fred_dff_daily_run1"))
        != floor(recorded_month("fred_dff_daily_run2"));
    assert_eq!(
        r.status("fred_dff_daily"),
        if moved { "succeeded" } else { "up_to_date" }
    );
    let r = f.run(&["fred_series"], Some("fred_series_run1"));
    r.ok();
    assert_eq!(r.written("fred/series", None), (0, 0, 0));
    assert_eq!(r.status("fred_series"), "up_to_date");
    let now: Vec<u64> = tables.iter().map(|t| f.table_snapshots(t)).collect();
    assert_eq!(now, snaps, "no snapshot touched the adopted tables");
}

// ----------------------------------------------------------------------------- M8.2a: Postgres

/// `(series_id, title, frequency, units, adjustment, source)`: the app's series catalogue.
const CATALOGUE: [(&str, &str, &str, &str, &str, &str); 4] = [
    (
        "DFF",
        "Federal Funds Effective Rate",
        "daily",
        "Percent",
        "NSA",
        "Board of Governors of the Federal Reserve System (US)",
    ),
    (
        "DGS10",
        "Market Yield on U.S. Treasury Securities at 10-Year Constant Maturity, Quoted on an Investment Basis",
        "daily",
        "Percent",
        "NSA",
        "Board of Governors of the Federal Reserve System (US)",
    ),
    (
        "UNRATE",
        "Unemployment Rate",
        "monthly",
        "Percent",
        "SA",
        "U.S. Bureau of Labor Statistics",
    ),
    (
        "GDPC1",
        "Real Gross Domestic Product",
        "quarterly",
        "Billions of Chained 2017 Dollars",
        "SAAR",
        "U.S. Bureau of Economic Analysis",
    ),
];

fn sql_str(v: &str) -> String {
    format!("'{}'", v.replace('\'', "''"))
}

/// `VALUES` rows `(series_id, date, value)` of recorded series.
fn obs_values(ids: &[&str]) -> String {
    ids.iter()
        .flat_map(|id| {
            data(id).into_iter().map(move |(d, v)| {
                format!("('{id}', '{d}', {})", v.unwrap_or_else(|| "NULL".into()))
            })
        })
        .collect::<Vec<_>>()
        .join(",\n  ")
}

/// The app database: enums, numerics of three precisions, and the observation tables created
/// out of name order. `obs_weekly` was emptied when the app stopped deriving weekly averages.
fn appdb_seed() -> String {
    let series = CATALOGUE
        .iter()
        .map(|(id, title, freq, units, adj, source)| {
            let rows = data(id);
            format!(
                "({}, {}, '{freq}', {}, '{adj}', {}, '{}', '{}')",
                sql_str(id),
                sql_str(title),
                sql_str(units),
                sql_str(source),
                rows.first().expect("rows").0,
                rows.last().expect("rows").0
            )
        })
        .collect::<Vec<_>>()
        .join(",\n  ");
    format!(
        "CREATE SCHEMA fred;
CREATE TYPE fred.frequency AS ENUM ('daily', 'weekly', 'monthly', 'quarterly');
CREATE TYPE fred.adjustment AS ENUM ('NSA', 'SA', 'SAAR');
CREATE TABLE fred.series (series_id text PRIMARY KEY, title text NOT NULL, frequency fred.frequency NOT NULL,
  units text NOT NULL, adjustment fred.adjustment NOT NULL, source text NOT NULL, observation_start date, observation_end date);
INSERT INTO fred.series VALUES
  {series};
CREATE TABLE fred.obs_quarterly (series_id text REFERENCES fred.series, observation_date date, value numeric(14,3),
  PRIMARY KEY (series_id, observation_date));
CREATE TABLE fred.obs_daily (series_id text REFERENCES fred.series, observation_date date, value numeric(12,4),
  PRIMARY KEY (series_id, observation_date));
CREATE TABLE fred.obs_weekly (series_id text, observation_date date, value numeric(12,4), PRIMARY KEY (series_id, observation_date));
CREATE TABLE fred.obs_monthly (series_id text REFERENCES fred.series, observation_date date, value numeric(6,2),
  PRIMARY KEY (series_id, observation_date));
INSERT INTO fred.obs_daily VALUES
  {daily};
INSERT INTO fred.obs_monthly VALUES
  {monthly};
INSERT INTO fred.obs_quarterly VALUES
  {quarterly};
CREATE VIEW fred.obs_latest AS SELECT DISTINCT ON (series_id) * FROM fred.obs_daily ORDER BY series_id, observation_date DESC;",
        daily = obs_values(&["DFF", "DGS10"]),
        monthly = obs_values(&["UNRATE"]),
        quarterly = obs_values(&["GDPC1"]),
    )
}

/// What the legacy `appdb` full refresh left: enums as VARCHAR, `decimal(24,6)` numerics, its own
/// column order; nothing after the pause, the advance GDPC1 estimate, and the weekly averages it
/// kept when that table was emptied (the legacy loader skipped empty sources).
fn legacy_appdb(f: &Fred, paused: &str, gdp: &(String, String)) {
    let series = CATALOGUE
        .iter()
        .map(|(id, title, freq, units, adj, source)| {
            let rows: Vec<_> = data(id)
                .into_iter()
                .filter(|(d, _)| d.as_str() <= paused)
                .collect();
            format!(
                "({}, {}, {}, '{adj}', '{freq}', {}, DATE '{}', DATE '{}')",
                sql_str(id),
                sql_str(source),
                sql_str(title),
                sql_str(units),
                rows.first().expect("rows").0,
                rows.last().expect("rows").0
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let obs = |ids: &[&str]| {
        ids.iter()
            .map(|id| {
                format!(
                    "SELECT observation_date, '{id}' AS series_id, {id}::DECIMAL(24,6) AS value FROM read_csv('{}', types = {{'observation_date': 'DATE'}}) WHERE observation_date <= DATE '{paused}'",
                    data_path(id)
                )
            })
            .collect::<Vec<_>>()
            .join(" UNION ALL ")
    };
    f.sql(&format!(
        "CREATE SCHEMA lake.br;
         CREATE TABLE lake.br.appdb_series (series_id VARCHAR, source VARCHAR, title VARCHAR, adjustment VARCHAR,
           frequency VARCHAR, units VARCHAR, observation_start DATE, observation_end DATE);
         INSERT INTO lake.br.appdb_series VALUES {series}, ('DFF_WEEKLY_AVG', 'derived', 'DFF, weekly average', 'NSA', 'weekly', 'Percent', DATE '2024-01-01', DATE '2024-02-26');
         CREATE TABLE lake.br.appdb_obs_daily (observation_date DATE, series_id VARCHAR, value DECIMAL(24,6));
         INSERT INTO lake.br.appdb_obs_daily {daily};
         CREATE TABLE lake.br.appdb_obs_monthly (observation_date DATE, series_id VARCHAR, value DECIMAL(24,6));
         INSERT INTO lake.br.appdb_obs_monthly {monthly};
         CREATE TABLE lake.br.appdb_obs_quarterly (observation_date DATE, series_id VARCHAR, value DECIMAL(24,6));
         INSERT INTO lake.br.appdb_obs_quarterly {quarterly};
         UPDATE lake.br.appdb_obs_quarterly SET value = {advance} WHERE observation_date = DATE '{gq}';
         CREATE TABLE lake.br.appdb_obs_weekly (observation_date DATE, series_id VARCHAR, value DECIMAL(24,6));
         INSERT INTO lake.br.appdb_obs_weekly
           SELECT date_trunc('week', observation_date)::DATE, 'DFF_WEEKLY_AVG', avg(DFF)::DECIMAL(24,6)
           FROM read_csv('{dff}', types = {{'observation_date': 'DATE'}}) WHERE observation_date < DATE '2024-03-04' GROUP BY 1;",
        daily = obs(&["DFF", "DGS10"]),
        monthly = obs(&["UNRATE"]),
        quarterly = obs(&["GDPC1"]),
        gq = gdp.0,
        advance = gdp.1,
        dff = data_path("DFF"),
    ));
}

const APPDB_TABLES: [&str; 5] = [
    "appdb_series",
    "appdb_obs_daily",
    "appdb_obs_monthly",
    "appdb_obs_quarterly",
    "appdb_obs_weekly",
];

/// `(table, (stream, member), counts per cause)`.
type AppdbExpect = (
    &'static str,
    (&'static str, &'static str),
    BTreeMap<String, u64>,
);

#[test]
fn appdb_parity_then_cut_over() {
    let pg = atlatl_testkit::pg16();
    let db = pg.fresh_db().expect("db");
    let seed = appdb_seed();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt")
        .block_on(async {
            let (c, conn) = tokio_postgres::connect(&pg.dsn(&db), tokio_postgres::NoTls)
                .await
                .expect("connect");
            tokio::spawn(conn);
            c.batch_execute(&seed).await.expect("seed");
        });
    let mut f = Fred::new();
    f.edit(
        "atlatl.yaml",
        "    port: 5432 ",
        &format!("    port: {} ", pg.port),
    );
    f.set("FRED_PG_HOST", &pg.host);
    f.set("FRED_PG_DATABASE", &db);
    f.set("FRED_PG_USER", &pg.user);
    f.set("FRED_PG_PASSWORD", &pg.password);

    let paused = paused_at();
    let gdp = gdp_revision(&paused);
    legacy_appdb(&f, &paused, &gdp);
    let before: Vec<(Vec<String>, String)> = APPDB_TABLES
        .iter()
        .map(|t| (f.columns("lake", t), f.table_id(t)))
        .collect();

    // What a fresh table would get: a dry run into the empty scratch lake.
    let fresh = f.dry_run(Some("parity"), &["appdb"], None);
    fresh.ok();
    let members: Vec<&str> = fresh
        .members("appdb/observations")
        .iter()
        .map(|m| m["member"].as_str().expect("member"))
        .collect();
    assert_eq!(
        members,
        [
            "fred.obs_daily",
            "fred.obs_monthly",
            "fred.obs_quarterly",
            "fred.obs_weekly"
        ],
        "sorted, the view skipped"
    );
    assert_eq!(
        fresh
            .diff("appdb/observations", Some("fred.obs_daily"))
            .ddl(),
        [
            r#"CREATE TABLE "lake"."br"."appdb_obs_daily" ("series_id" VARCHAR, "observation_date" DATE, "value" DECIMAL(12,4))"#
        ],
        "a new table takes the source precision"
    );
    let series_ddl = fresh.diff("appdb/series", None).ddl();
    assert!(
        series_ddl.len() == 1 && series_ddl[0].contains(r#""frequency" VARCHAR, "units" VARCHAR"#),
        "enums read as text: {series_ddl:?}"
    );
    assert!(!f.exists("parity", "appdb_series"), "nothing created");

    // Parity: the same dry run against the legacy tables.
    let r = f.dry_run(None, &["appdb"], None);
    r.ok();

    let after = |id: &str| {
        data(id)
            .iter()
            .filter(|(d, _)| d.as_str() > paused.as_str())
            .count() as u64
    };
    let expect: [AppdbExpect; 5] = [
        (
            "appdb_series",
            ("appdb/series", "fred.series"),
            counts(&[
                ("update: catalogue moved on after the pause", 2),
                ("delete: removed from the app", 1),
            ]),
        ),
        (
            "appdb_obs_daily",
            ("appdb/observations", "fred.obs_daily"),
            counts(&[(
                "insert: added after the pause",
                after("DFF") + after("DGS10"),
            )]),
        ),
        (
            "appdb_obs_monthly",
            ("appdb/observations", "fred.obs_monthly"),
            counts(&[("insert: added after the pause", after("UNRATE"))]),
        ),
        (
            "appdb_obs_quarterly",
            ("appdb/observations", "fred.obs_quarterly"),
            counts(&[
                ("insert: added after the pause", after("GDPC1")),
                ("update: revised after the pause", 1),
            ]),
        ),
        (
            "appdb_obs_weekly",
            ("appdb/observations", "fred.obs_weekly"),
            counts(&[(
                "delete: kept by the legacy loader when the source emptied",
                9,
            )]),
        ),
    ];
    for (t, (st, m), want) in &expect {
        let d = r.diff(st, Some(*m));
        let why = d.explain(|op, a, b| match (op, b) {
            ("insert", None) if s(a, "observation_date") > paused.as_str() => {
                Some("added after the pause")
            }
            ("update", Some(l))
                if *t == "appdb_series" && s(l, "observation_end") <= paused.as_str() =>
            {
                Some("catalogue moved on after the pause")
            }
            ("update", Some(l))
                if s(a, "series_id") == "GDPC1" && l["value"].as_f64() == gdp.1.parse().ok() =>
            {
                Some("revised after the pause")
            }
            ("delete", None) if s(a, "series_id") == "DFF_WEEKLY_AVG" => {
                Some(if *t == "appdb_series" {
                    "removed from the app"
                } else {
                    "kept by the legacy loader when the source emptied"
                })
            }
            _ => None,
        });
        assert_eq!(&why, want, "{t}: {d:#?}");
    }

    // Cut-over: the full refresh by key applies exactly those, into the adopted tables.
    let r = f.run(&["appdb"], None);
    r.ok();
    let got: Vec<(u64, u64, u64)> = [
        ("appdb/series", "fred.series"),
        ("appdb/observations", "fred.obs_daily"),
        ("appdb/observations", "fred.obs_monthly"),
        ("appdb/observations", "fred.obs_quarterly"),
        ("appdb/observations", "fred.obs_weekly"),
    ]
    .iter()
    .map(|(st, m)| r.written(st, Some(m)))
    .collect();
    let want: Vec<(u64, u64, u64)> = expect
        .iter()
        .map(|(_, _, c)| {
            let n = |p: &str| {
                c.iter()
                    .filter(|(k, _)| k.starts_with(p))
                    .map(|(_, v)| v)
                    .sum()
            };
            (n("insert"), n("update"), n("delete"))
        })
        .collect();
    assert_eq!(
        got, want,
        "a full refresh deletes what the source no longer has"
    );
    assert_eq!(
        f.count("lake.br.appdb_obs_weekly"),
        0,
        "emptiness is mirrored"
    );
    let snap = r.member("appdb/series", None)["lake_snapshot"].clone();
    assert!(
        snap.is_u64()
            && r.members("appdb/observations")
                .iter()
                .all(|m| m["lake_snapshot"] == snap),
        "one commit for every member"
    );
    for (t, (cols, id)) in APPDB_TABLES.iter().zip(&before) {
        assert_eq!(&f.columns("lake", t), cols, "{t}: schema and order kept");
        assert_eq!(&f.table_id(t), id, "{t}: same table");
    }
    let d = f.dry_run(None, &["appdb"], None);
    d.ok();
    assert_eq!(d.status("appdb"), "up_to_date");
    for (t, (st, m), _) in &expect {
        let x = d.diff(st, Some(*m));
        assert!(x.is_clean(), "{t}: {x:#?}");
    }

    let r = f.run(&["appdb"], None);
    r.ok();
    assert_eq!(r.status("appdb"), "up_to_date");
    for m in r
        .members("appdb/series")
        .into_iter()
        .chain(r.members("appdb/observations"))
    {
        assert!(m["lake_snapshot"].is_null(), "nothing to commit: {m:#}");
        assert_eq!(
            (
                m["rows"]["inserted"].as_u64(),
                m["rows"]["updated"].as_u64(),
                m["rows"]["deleted"].as_u64()
            ),
            (Some(0), Some(0), Some(0))
        );
    }
}

// ----------------------------------------------------------------------------- M8.3a: Redshift

/// The warehouse (DuckDB behind the Data API fake): each table carries a vintage date,
/// `realtime_start` (the day after for daily rates, four months after the quarter for GDP).
fn warehouse_seed() -> String {
    let rates = |id: &str, t: &str| {
        format!(
            "CREATE TABLE fred.{t} (series_id VARCHAR, observation_date DATE, value DECIMAL(12,4), realtime_start DATE);
             INSERT INTO fred.{t} SELECT '{id}', observation_date, {id}, observation_date + 1
               FROM read_csv('{}', types = {{'observation_date': 'DATE', '{id}': 'VARCHAR'}});",
            data_path(id)
        )
    };
    format!(
        "CREATE SCHEMA fred;
         {}
         {}
         CREATE TABLE fred.gdp_quarterly (series_id VARCHAR, observation_date DATE, value DECIMAL(14,3), realtime_start DATE);
         INSERT INTO fred.gdp_quarterly SELECT 'GDPC1', observation_date, GDPC1, (observation_date + INTERVAL 4 MONTH)::DATE
           FROM read_csv('{}', types = {{'observation_date': 'DATE', 'GDPC1': 'VARCHAR'}});",
        rates("DFF", "rates_dff"),
        rates("DGS10", "rates_dgs10"),
        data_path("GDPC1")
    )
}

/// What the legacy warehouse replications left: `DOUBLE` values (it lost the precision), its own
/// column order, nothing after the pause, and the GDPC1 revision the warehouse republished at the
/// same vintage, which the legacy strict `realtime_start > max()` never picked up.
fn legacy_warehouse(f: &Fred, paused: &str, gdp: &(String, String)) {
    let table = |t: &str, source: &str| {
        format!(
            "CREATE TABLE lake.br.{t} (realtime_start DATE, value DOUBLE, series_id VARCHAR, observation_date DATE);
             INSERT INTO lake.br.{t} {source} WHERE observation_date <= DATE '{paused}';"
        )
    };
    let rates = |id: &str| {
        format!(
            "SELECT observation_date + 1, {id}::DOUBLE, '{id}', observation_date FROM read_csv('{}', types = {{'observation_date': 'DATE'}})",
            data_path(id)
        )
    };
    f.sql(&format!(
        "CREATE SCHEMA lake.br;
         {}
         {}
         {}
         UPDATE lake.br.dw_gdp_quarterly SET value = {} WHERE observation_date = DATE '{}';",
        table("dw_rates_dff", &rates("DFF")),
        table("dw_rates_dgs10", &rates("DGS10")),
        table(
            "dw_gdp_quarterly",
            &format!(
                "SELECT (observation_date + INTERVAL 4 MONTH)::DATE, GDPC1::DOUBLE, 'GDPC1', observation_date FROM read_csv('{}', types = {{'observation_date': 'DATE'}})",
                data_path("GDPC1")
            )
        ),
        gdp.1,
        gdp.0,
    ));
}

const DW_TABLES: [&str; 3] = ["dw_rates_dff", "dw_rates_dgs10", "dw_gdp_quarterly"];
/// The daily merges' `(stream, member)` of each table.
const DW_MEMBERS: [(&str, &str); 3] = [
    ("dw/rates", "fred.rates_dff"),
    ("dw/rates", "fred.rates_dgs10"),
    ("dw/gdp", "fred.gdp_quarterly"),
];

#[test]
fn warehouse_families_parity_then_cut_over() {
    use atlatl_testkit::redshift_fake::{Fake, KEY, SECRET};
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("rt");
    let fake = rt.block_on(Fake::start());
    fake.sql(&warehouse_seed());
    fake.state().slices = 4;
    let s3 = atlatl_testkit::rustfs();
    let bucket = s3.fresh_bucket().expect("bucket");
    let mut f = Fred::new();
    f.set("FRED_RS_ENDPOINT", &fake.endpoint);
    f.set("FRED_RS_KEY", KEY);
    f.set("FRED_RS_SECRET", SECRET);
    f.set("FRED_BUCKET", &bucket);
    f.set("FRED_S3_ENDPOINT", format!("http://{}", s3.endpoint));
    f.set("FRED_S3_KEY", &s3.key_id);
    f.set("FRED_S3_SECRET", &s3.secret);
    let unloads = || -> Vec<String> {
        fake.calls("ExecuteStatement")
            .into_iter()
            .filter_map(|c| c.body["Sql"].as_str().map(str::to_owned))
            .filter(|s| s.starts_with("UNLOAD"))
            .collect()
    };
    let leftovers = || -> i64 {
        let c = atlatl_testkit::lake::open_engine().expect("engine");
        c.execute_batch("LOAD httpfs").expect("httpfs");
        c.execute_batch(&s3.duckdb_secret("list", &bucket))
            .expect("secret");
        c.query_row(
            &format!("SELECT count(*) FROM glob('s3://{bucket}/atlatl_unload/**')"),
            [],
            |r| r.get(0),
        )
        .expect("glob")
    };

    let paused = paused_at();
    let gdp = gdp_revision(&paused);
    legacy_warehouse(&f, &paused, &gdp);
    let before: Vec<(Vec<String>, String)> = DW_TABLES
        .iter()
        .map(|t| (f.columns("lake", t), f.table_id(t)))
        .collect();

    // A fresh table (the empty scratch lake) would take the source precision, from svv_columns.
    let fresh = f.dry_run(Some("parity"), &["rates_1d", "gdp_1d"], None);
    fresh.ok();
    for (st, m, ty) in [
        ("dw/rates", "fred.rates_dff", "DECIMAL(12,4)"),
        ("dw/gdp", "fred.gdp_quarterly", "DECIMAL(14,3)"),
    ] {
        let ddl = fresh.diff(st, Some(m)).ddl();
        assert!(
            ddl.len() == 1 && ddl[0].contains(&format!("\"value\" {ty}")),
            "{m}: {ddl:?}"
        );
    }

    // Parity: each family's daily merge, dry-run against the legacy tables.
    let r = f.dry_run(None, &["rates_1d", "gdp_1d"], None);
    r.ok();
    let members: Vec<&str> = r
        .members("dw/rates")
        .iter()
        .map(|m| m["member"].as_str().expect("member"))
        .collect();
    assert_eq!(members, ["fred.rates_dff", "fred.rates_dgs10"]);
    for m in r.members("dw/rates") {
        assert_eq!(m["chunks"], 4, "compared per UNLOADed file: {m:#}");
    }
    assert_eq!(
        leftovers(),
        0,
        "a dry run's UNLOAD prefixes are removed too"
    );
    let after = |id: &str| {
        data(id)
            .iter()
            .filter(|(d, _)| d.as_str() > paused.as_str())
            .count() as u64
    };
    let expect = [
        counts(&[("insert: after the pause", after("DFF"))]),
        counts(&[("insert: after the pause", after("DGS10"))]),
        counts(&[
            ("insert: after the pause", after("GDPC1")),
            ("update: republished at the same vintage", 1),
        ]),
    ];
    for ((st, m), want) in DW_MEMBERS.iter().zip(&expect) {
        let d = r.diff(st, Some(m));
        let why = d.explain(|op, a, b| match (op, b) {
            ("insert", None) if s(a, "observation_date") > paused.as_str() => {
                Some("after the pause")
            }
            ("update", Some(l))
                if s(a, "observation_date") == gdp.0
                    && l["value"].as_f64() == gdp.1.parse().ok()
                    && a.get("realtime_start").is_none() =>
            {
                Some("republished at the same vintage")
            }
            _ => None,
        });
        assert_eq!(&why, want, "{m}: {d:#?}");
    }

    // Cut-over: seed the hourly cursors from the adopted tables (the legacy watermark was their
    // `max()`), then each family's two replications together, as their op runs them.
    let cursor = |m: &str| {
        f.one(&format!(
            "SELECT value::VARCHAR FROM lake._atlatl.stream_state WHERE stream_id = 'dw_1h/rates' AND member = '{m}'"
        ))
    };
    let o = f.atlatl(&[
        "state",
        "init",
        "dw_1h/rates",
        "--from-target",
        "--member",
        "fred.rates_dff",
        "--member",
        "fred.rates_dgs10",
    ]);
    assert!(o.status.success(), "{}", out(&o));
    let legacy_max = f.one("SELECT max(realtime_start)::VARCHAR FROM lake.br.dw_rates_dff");
    let seeded: Value = serde_json::from_slice(&o.stdout).expect("json");
    assert_eq!(
        (
            seeded["after"]["fred.rates_dff"]["value"].as_str(),
            seeded["after"]["fred.rates_dff"]["type"].as_str()
        ),
        (Some(legacy_max.as_str()), Some("DATE")),
        "{seeded:#}"
    );
    let n0 = unloads().len();
    let r = f.run(&["rates_1d", "rates_1h"], None);
    r.ok();
    let r2 = f.run(&["gdp_1d", "gdp_1h"], None);
    r2.ok();
    for (rep, op) in [("rates_1d", "rates"), ("rates_1h", "rates")] {
        assert_eq!(r.replication(rep)["op"], op);
    }
    let cut_over = unloads()[n0..].to_vec();
    let hourly: Vec<&String> = cut_over
        .iter()
        .filter(|s| s.contains("\"realtime_start\" >="))
        .collect();
    assert_eq!(hourly.len(), 2, "one delta UNLOAD per member: {hourly:?}");
    assert!(
        hourly[0].contains(&format!("CAST(''{legacy_max}'' AS date)")),
        "the seeded watermark is pushed into the UNLOAD: {}",
        hourly[0]
    );
    let windowed: Vec<&String> = cut_over
        .iter()
        .filter(|s| s.contains("current_date - 400"))
        .collect();
    assert_eq!(windowed.len(), 1, "{cut_over:?}");
    let total = |run: &Run, streams: [&str; 2], member: &str| {
        streams.iter().fold((0, 0, 0), |a, st| {
            let w = run.written(st, Some(member));
            (a.0 + w.0, a.1 + w.1, a.2 + w.2)
        })
    };
    assert_eq!(
        total(&r, ["dw/rates", "dw_1h/rates"], "fred.rates_dff"),
        (after("DFF"), 0, 0)
    );
    assert_eq!(
        total(&r, ["dw/rates", "dw_1h/rates"], "fred.rates_dgs10"),
        (after("DGS10"), 0, 0)
    );
    assert_eq!(
        total(&r2, ["dw/gdp", "dw_1h/gdp"], "fred.gdp_quarterly"),
        (after("GDPC1"), 1, 0),
        "the daily merge (and the window) pick up the revision the legacy loader missed"
    );
    for (t, (cols, id)) in DW_TABLES.iter().zip(&before) {
        assert_eq!(
            &f.columns("lake", t),
            cols,
            "{t}: DOUBLE and the order kept"
        );
        assert_eq!(&f.table_id(t), id, "{t}: same table");
    }
    assert!(
        cursor("fred.rates_dff")
            .contains(&f.one("SELECT max(realtime_start)::VARCHAR FROM lake.br.dw_rates_dff")),
        "the hourly watermark advanced: {}",
        cursor("fred.rates_dff")
    );

    let d = f.dry_run(None, &["rates_1d", "gdp_1d"], None);
    d.ok();
    for (st, m) in DW_MEMBERS {
        assert!(d.diff(st, Some(m)).is_clean(), "{m}");
    }

    let snaps: Vec<u64> = DW_TABLES.iter().map(|t| f.table_snapshots(t)).collect();
    let r = f.run(&["rates_1d", "rates_1h"], None);
    r.ok();
    let r2 = f.run(&["gdp_1d", "gdp_1h"], None);
    r2.ok();
    for rep in ["rates_1d", "rates_1h"] {
        assert_eq!(r.status(rep), "up_to_date", "{rep}");
    }
    for rep in ["gdp_1d", "gdp_1h"] {
        assert_eq!(r2.status(rep), "up_to_date", "{rep}");
    }
    for m in ["fred.rates_dff", "fred.rates_dgs10"] {
        assert_eq!(total(&r, ["dw/rates", "dw_1h/rates"], m), (0, 0, 0), "{m}");
    }
    assert_eq!(
        total(&r2, ["dw/gdp", "dw_1h/gdp"], "fred.gdp_quarterly"),
        (0, 0, 0)
    );
    let now: Vec<u64> = DW_TABLES.iter().map(|t| f.table_snapshots(t)).collect();
    assert_eq!(now, snaps, "nothing left to write");
    assert_eq!(leftovers(), 0, "UNLOAD prefixes are removed");
    drop(rt);
}

// ----------------------------------------------------------------------------- M8.4a: bulk files

fn zip(members: &[(&str, String)]) -> Vec<u8> {
    use std::io::Write;
    let mut z = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    for (name, body) in members {
        z.start_file(*name, zip::write::SimpleFileOptions::default())
            .expect("zip");
        z.write_all(body.as_bytes()).expect("zip");
    }
    z.finish().expect("zip").into_inner()
}

fn tar_gz(members: &[(&str, String)]) -> Vec<u8> {
    let mut b = tar::Builder::new(flate2::write::GzEncoder::new(
        Vec::new(),
        flate2::Compression::default(),
    ));
    for (name, body) in members {
        let mut h = tar::Header::new_gnu();
        h.set_size(body.len() as u64);
        h.set_mode(0o644);
        h.set_cksum();
        b.append_data(&mut h, name, body.as_bytes()).expect("tar");
    }
    b.into_inner().expect("tar").finish().expect("gz")
}

/// `observation_date,DFF,DGS10` from 2025-07-01, "." where DGS10 has no observation (weekends,
/// market holidays), as a FRED API export writes it.
fn rates_member() -> String {
    let dgs10: BTreeMap<String, Option<String>> = data("DGS10").into_iter().collect();
    let mut out = String::from("observation_date,DFF,DGS10\n");
    for (d, v) in data("DFF")
        .into_iter()
        .filter(|(d, _)| d.as_str() >= "2025-07-01")
    {
        let g = dgs10
            .get(&d)
            .cloned()
            .flatten()
            .unwrap_or_else(|| ".".into());
        out.push_str(&format!("{d},{},{g}\n", v.unwrap_or_else(|| ".".into())));
    }
    out
}

fn dictionary(ids: &[&str]) -> String {
    let mut out = String::from("Series ID,Title,Units,Source\n");
    for (id, title, _, units, _, source) in CATALOGUE.iter().filter(|c| ids.contains(&c.0)) {
        out.push_str(&format!("{id},\"{title}\",{units},\"{source}\"\n"));
    }
    out
}

fn dotted(id: &str) -> String {
    let mut out = format!("observation_date,{id}\n");
    for (d, v) in data(id) {
        out.push_str(&format!("{d},{}\n", v.unwrap_or_else(|| ".".into())));
    }
    out
}

/// What the previous loader left: `clean_csv` names, all text in its own order, the missing
/// markers kept as text (an empty field, or "."), its epoch `_legacy_loaded_at`, and nothing after
/// the pause. `bulk_gdp` it never loaded.
fn legacy_bulk(f: &Fred, paused: &str, bulk: &Path) {
    let rates = bulk.join("legacy_rates.csv");
    std::fs::write(&rates, rates_member()).expect("write");
    f.sql(&format!(
        "CREATE SCHEMA lake.br;
         CREATE TABLE lake.br.bulk_dgs10 (dgs10 VARCHAR, observation_date VARCHAR, _legacy_loaded_at INTEGER);
         INSERT INTO lake.br.bulk_dgs10
           SELECT coalesce(DGS10, ''), observation_date, 1758000000
           FROM read_csv('{dgs10}', all_varchar = true) WHERE observation_date <= '{paused}';
         CREATE TABLE lake.br.bulk_rates (observation_date VARCHAR, dff VARCHAR, dgs10 VARCHAR, _legacy_loaded_at INTEGER);
         INSERT INTO lake.br.bulk_rates
           SELECT observation_date, DFF, DGS10, 1758000000
           FROM read_csv('{rates}', all_varchar = true) WHERE observation_date <= '{paused}';",
        dgs10 = data_path("DGS10"),
        rates = rates.display(),
    ));
}

#[test]
fn bulk_files_parity_then_cut_over() {
    use dogfood_support::{Req, Resp, serve};
    use std::sync::Arc;
    let graph: Arc<dogfood_support::Handler> = Arc::new(|r: &Req| {
        // Like FRED's CDN: an unknown User-Agent gets nothing.
        if !r
            .header("User-Agent")
            .is_some_and(|u| u.starts_with("curl/"))
        {
            return Resp(403, Some("text/html"), "denied".into());
        }
        let id = r.param("id").unwrap_or_default();
        if r.path != "/graph/fredgraph.csv" || !id.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Resp(404, Some("text/html"), "not found".into());
        }
        match std::fs::read_to_string(example().join(format!("data/{id}.csv"))) {
            Ok(body) => Resp(200, Some("application/csv"), body),
            Err(_) => Resp(404, Some("text/html"), "not found".into()),
        }
    });
    let (url, log) = serve(graph);
    let s3 = atlatl_testkit::rustfs();
    let bucket = s3.fresh_bucket().expect("bucket");
    let bulk = tempfile::tempdir().expect("tmp");
    let rates_zip = zip(&[
        ("FRED_RATES_KEY.csv", dictionary(&["DFF", "DGS10"])),
        ("FRED_RATES.csv", rates_member()),
    ]);
    let (st, body) = s3
        .request_bytes("PUT", &format!("/{bucket}/bulk/FRED_RATES.zip"), &rates_zip)
        .expect("put");
    assert_eq!(st, 200, "{body}");
    std::fs::write(
        bulk.path().join("FRED_GDP.tar.gz"),
        tar_gz(&[
            ("fred/series.csv", dictionary(&["GDPC1", "UNRATE"])),
            ("fred/UNRATE.csv", dotted("UNRATE")),
            ("fred/GDPC1.csv", dotted("GDPC1")),
        ]),
    )
    .expect("tgz");
    let mut f = Fred::new();
    f.set("FRED_GRAPH", format!("{url}/graph"));
    f.set("FRED_BUCKET", &bucket);
    f.set("FRED_BULK_DIR", bulk.path().display().to_string());
    f.set("FRED_S3_ENDPOINT", format!("http://{}", s3.endpoint));
    f.set("FRED_S3_KEY", &s3.key_id);
    f.set("FRED_S3_SECRET", &s3.secret);

    let paused = paused_at();
    legacy_bulk(&f, &paused, bulk.path());
    let tables = ["bulk_dgs10", "bulk_rates"];
    let before: Vec<(Vec<String>, String)> = tables
        .iter()
        .map(|t| (f.columns("lake", t), f.table_id(t)))
        .collect();
    let reps = ["bulk_dgs10", "bulk_rates", "bulk_gdp"];

    // Parity: a dry run against the legacy tables. The load stamp isn't compared (04 §8).
    let r = f.dry_run(None, &reps, None);
    r.ok();
    assert_eq!(
        log.lock().expect("log").as_slice(),
        ["/graph/fredgraph.csv?id=DGS10&cosd=2025-07-01"],
        "downloaded once"
    );
    let gdp = r.diff("bulk/gdp", None);
    assert_eq!(
        gdp.ddl(),
        [
            r#"CREATE TABLE "lake"."br"."bulk_gdp" ("observation_date" text, "gdpc1" text, "_legacy_loaded_at" INTEGER)"#
        ],
        "a new table: clean_csv names, all text, then the epoch column"
    );
    assert_eq!(
        gdp.counts(),
        (data("GDPC1").len() as u64, 0, 0),
        "only the selected member"
    );
    assert!(!f.exists("lake", "bulk_gdp"));
    let dgs10_diff = r.diff("bulk/dgs10", None);
    let why = dgs10_diff.explain(|op, a, b| match (op, b) {
        ("insert", None) if s(a, "observation_date") > paused.as_str() => {
            Some("delivered after the pause")
        }
        ("update", Some(l)) if s(l, "dgs10").is_empty() && a["dgs10"].is_null() => {
            Some("empty field kept as text")
        }
        _ => None,
    });
    let dgs10 = data("DGS10");
    let (old, new): (Vec<_>, Vec<_>) = dgs10
        .iter()
        .partition(|(d, _)| d.as_str() <= paused.as_str());
    let (dgs10_new, dgs10_empty) = (
        new.len() as u64,
        old.iter().filter(|(_, v)| v.is_none()).count() as u64,
    );
    assert_eq!(
        why,
        counts(&[
            ("insert: delivered after the pause", dgs10_new),
            ("update: empty field kept as text", dgs10_empty),
        ])
    );
    let rates_diff = r.diff("bulk/rates", None);
    let why = rates_diff.explain(|op, a, b| match (op, b) {
        ("insert", None) if s(a, "observation_date") > paused.as_str() => {
            Some("delivered after the pause")
        }
        ("update", Some(l)) if s(l, "dgs10") == "." && a["dgs10"].is_null() => {
            Some("\".\" kept as text")
        }
        _ => None,
    });
    let member = rates_member();
    let rows: Vec<Vec<&str>> = member
        .lines()
        .skip(1)
        .map(|l| l.split(',').collect())
        .collect();
    let (rates_new, rates_dots) = (
        rows.iter().filter(|r| r[0] > paused.as_str()).count() as u64,
        rows.iter()
            .filter(|r| r[0] <= paused.as_str() && r[2] == ".")
            .count() as u64,
    );
    assert!(rates_dots > dgs10_empty, "weekends too");
    assert_eq!(
        why,
        counts(&[
            ("insert: delivered after the pause", rates_new),
            ("update: \".\" kept as text", rates_dots),
        ])
    );

    // Cut-over: exactly the reviewed rows. `_legacy_loaded_at` is when a row was last written:
    // the new and changed rows get this load's time, the rest keep the legacy stamp.
    let t0 = epoch();
    let r = f.run(&reps, None);
    let t1 = epoch();
    r.ok();
    assert_eq!(r.written("bulk/dgs10", None), (dgs10_new, dgs10_empty, 0));
    assert_eq!(r.written("bulk/rates", None), (rates_new, rates_dots, 0));
    for (t, (cols, id)) in tables.iter().zip(&before) {
        assert_eq!(
            &f.columns("lake", t),
            cols,
            "{t}: its order and INTEGER epoch kept"
        );
        assert_eq!(&f.table_id(t), id, "{t}: same table");
    }
    let stamps = |t: &str| {
        f.q(&format!(
            "SELECT _legacy_loaded_at::VARCHAR || ' ' || count(*) FROM lake.br.{t} GROUP BY _legacy_loaded_at ORDER BY _legacy_loaded_at"
        ))
        .iter()
        .map(|v| {
            let (a, n) = v.split_once(' ').expect("stamp");
            (a.parse::<i64>().expect("epoch"), n.parse::<u64>().expect("n"))
        })
        .collect::<Vec<_>>()
    };
    let loaded = stamps("bulk_rates");
    assert!(
        loaded.len() == 2
            && loaded[0]
                == (
                    1_758_000_000,
                    f.count("lake.br.bulk_rates") - rates_new - rates_dots
                )
            && (t0..=t1).contains(&loaded[1].0)
            && loaded[1].1 == rates_new + rates_dots,
        "one load time per run, on the rows it wrote: {loaded:?}"
    );
    let d = f.dry_run(None, &reps, None);
    d.ok();
    for st in ["bulk/dgs10", "bulk/rates", "bulk/gdp"] {
        assert!(d.diff(st, None).is_clean(), "{st}");
    }

    // Again: the data is unchanged, so nothing is written and the stamps stay.
    std::thread::sleep(std::time::Duration::from_millis(1100));
    let snaps: Vec<u64> = reps.iter().map(|t| f.table_snapshots(t)).collect();
    let r = f.run(&reps, None);
    r.ok();
    for (rep, st) in [
        ("bulk_dgs10", "bulk/dgs10"),
        ("bulk_rates", "bulk/rates"),
        ("bulk_gdp", "bulk/gdp"),
    ] {
        assert_eq!(r.written(st, None), (0, 0, 0), "{st}");
        assert_eq!(r.status(rep), "up_to_date", "{rep}");
    }
    assert_eq!(
        reps.iter()
            .map(|t| f.table_snapshots(t))
            .collect::<Vec<_>>(),
        snaps,
        "a real no-op"
    );
    assert_eq!(stamps("bulk_rates"), loaded);
}

fn epoch() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs() as i64
}

// ----------------------------------------------------------------------------- recording

/// Live-internet tests need the `network` feature (12 §5); `--ignored` alone doesn't reach FRED.
fn network() -> bool {
    if !cfg!(feature = "network") {
        eprintln!("skipped: calls fred.stlouisfed.org; run with `cargo test-network`");
    }
    cfg!(feature = "network")
}

/// Re-records `examples/fred/apis/fred/cassettes/` and `examples/fred/data/` against FRED
/// (`cargo test-network -- --ignored record_fred_cassettes`): 13 requests.
#[test]
#[ignore = "calls fred.stlouisfed.org and rewrites the committed recordings (needs --features network)"]
fn record_fred_cassettes() {
    if !network() {
        return;
    }
    let mut f = Fred::new();
    f.live = true;
    for (rep, name) in [
        ("fred_dff", "fred_dff_run1"),
        ("fred_dff_daily", "fred_dff_daily_run1"),
        ("fred_dff_daily", "fred_dff_daily_run2"),
        ("fred_series", "fred_series_run1"),
    ] {
        let report = f.path().join("report.json");
        let o = f
            .cmd(&[
                "--report",
                report.to_str().expect("utf8"),
                "run",
                "--replication",
                rep,
            ])
            .env("ATLATL_CASSETTE_RECORD", name)
            .output()
            .expect("atlatl");
        assert!(o.status.success(), "{rep}: {}", out(&o));
    }
    let src = f.path().join("apis/fred/cassettes");
    let dst = example().join("apis/fred/cassettes");
    std::fs::create_dir_all(&dst).expect("mkdir");
    for e in std::fs::read_dir(&src).expect("cassettes") {
        let e = e.expect("entry");
        let text = std::fs::read_to_string(e.path()).expect("read");
        assert!(
            !text.contains("_abck=") && !text.contains("bm_sz=") && !text.contains("127.0.0.1"),
            "{}: cookies or loopback recorded",
            e.path().display()
        );
        std::fs::write(dst.join(e.file_name()), text).expect("write");
    }
    let write = |id: &str, rows: Vec<(String, Option<String>)>| {
        let mut s = format!("observation_date,{id}\n");
        for (d, v) in rows {
            s.push_str(&format!("{d},{}\n", v.unwrap_or_default()));
        }
        std::fs::write(example().join(format!("data/{id}.csv")), s).expect("data");
    };
    write(
        "DFF",
        bodies("fred_dff_daily_run1")
            .iter()
            .flat_map(|(_, b)| csv_rows(b))
            .collect(),
    );
    for (u, b) in bodies("fred_series_run1") {
        let id = series_of(&u);
        if id != "DFF" {
            write(&id, csv_rows(&b));
        }
    }
}

/// The spec and the bulk-file URL against FRED itself (2 requests): the live responses still
/// pass the reviewed schema and load (`cargo test-network`).
#[test]
#[cfg_attr(
    not(feature = "network"),
    ignore = "calls fred.stlouisfed.org (needs --features network)"
)]
fn live_fred_endpoints() {
    if !network() {
        return;
    }
    let mut f = Fred::new();
    f.live = true;
    let o = f.atlatl(&["schema", "check", "fred/dff", "--from-api-test"]);
    assert!(o.status.success(), "{}", out(&o));
    assert!(
        String::from_utf8_lossy(&o.stdout).contains("fred/dff: ok"),
        "{}",
        out(&o)
    );
    let r = f.run(&["bulk_dgs10"], None);
    r.ok();
    assert!(f.count("lake.br.bulk_dgs10") > 0);
}
