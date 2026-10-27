use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use atlatl_testkit::{LocalLake, TempProject};

const REPS: &str = r#"replications:
  copy:
    source: lake
    target: lake
    streams:
      - {id: c/people, mode: full-refresh, from: {table: src.people}, object: br.people, primary_key: [id]}
  slow:
    source: lake
    target: lake
    streams:
      - id: s/big
        mode: full-refresh
        from: {query: "SELECT a.range AS id, md5(a.range::VARCHAR) AS h FROM range(3000000000) a ORDER BY h"}
        object: br.big
  broken:
    source: lake
    target: lake
    streams:
      - {id: b/boom, mode: full-refresh, from: {query: "SELECT error('boom-at-runtime') AS x"}, object: br.boom}
"#;

struct Env {
    lake: LocalLake,
    project: TempProject,
    out: tempfile::TempDir,
}

fn setup() -> Env {
    let lake = LocalLake::file().expect("lake");
    let c = lake.open("lake").expect("open");
    c.execute_batch("CREATE SCHEMA lake.src; CREATE TABLE lake.src.people AS SELECT range AS id, 'p' || range AS name FROM range(1, 6);")
        .expect("seed");
    drop(c);
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
        lake,
        project,
        out: tempfile::tempdir().expect("tmp"),
    }
}

impl Env {
    fn report(&self) -> PathBuf {
        self.out.path().join("report.json")
    }
    fn events(&self) -> PathBuf {
        self.out.path().join("events.ndjson")
    }
    fn cmd(&self, args: &[&str]) -> Command {
        self.cmd_fmt("json", args)
    }
    fn cmd_fmt(&self, log_format: &str, args: &[&str]) -> Command {
        let mut c = Command::new(env!("CARGO_BIN_EXE_atlatl"));
        c.arg("--project")
            .arg(self.project.path())
            .arg("--report")
            .arg(self.report())
            .arg("--events")
            .arg(self.events())
            .args(["--log-format", log_format, "run"])
            .args(args)
            .env("ATLATL_EXTENSION_DIR", atlatl_testkit::extension_dir())
            .env_remove("ATLATL_PROJECT");
        c
    }
    fn read_report(&self) -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(self.report()).expect("report")).expect("json")
    }
    fn event_types(&self) -> Vec<String> {
        std::fs::read_to_string(self.events())
            .unwrap_or_default()
            .lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .filter_map(|v| v["type"].as_str().map(str::to_owned))
            .collect()
    }
}

fn wait_for_event(path: &Path, ty: &str, limit: Duration) {
    let t = Instant::now();
    while t.elapsed() < limit {
        if std::fs::read_to_string(path)
            .unwrap_or_default()
            .contains(&format!("\"type\":\"{ty}\""))
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("no `{ty}` event within {limit:?}");
}

fn wait(child: &mut Child, limit: Duration) -> (i32, Duration) {
    let t = Instant::now();
    loop {
        if let Some(s) = child.try_wait().expect("wait") {
            return (s.code().expect("exit code"), t.elapsed());
        }
        if t.elapsed() > limit {
            let _ = child.kill();
            panic!("still running after {limit:?}");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn run_writes_report_events_and_data() {
    let e = setup();
    let o = e.cmd(&["--replication", "copy"]).output().expect("run");
    assert_eq!(
        o.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&o.stderr)
    );
    let r = e.read_report();
    assert_eq!(r["status"], "succeeded");
    assert_eq!(r["replications"][0]["status"], "succeeded");
    let types = e.event_types();
    assert_eq!(types.first().map(String::as_str), Some("run.started"));
    assert_eq!(types.last().map(String::as_str), Some("run.completed"));
    assert!(types.iter().any(|t| t == "replication.completed"));
    let run_ids: std::collections::BTreeSet<String> = std::fs::read_to_string(e.events())
        .expect("events")
        .lines()
        .map(|l| {
            serde_json::from_str::<serde_json::Value>(l).expect("line")["run_id"]
                .as_str()
                .expect("id")
                .to_owned()
        })
        .collect();
    assert_eq!(run_ids.len(), 1);
    assert_eq!(
        run_ids.first(),
        r["run_id"].as_str().map(str::to_owned).as_ref(),
        "events and report share the run id"
    );
    let n: i64 = e
        .lake
        .open("lake")
        .expect("o")
        .query_row("SELECT count(*) FROM lake.br.people", [], |r| r.get(0))
        .expect("q");
    assert_eq!(n, 5);
}

#[test]
fn failed_replication_logs_its_error() {
    let e = setup();
    for fmt in ["text", "json"] {
        let o = e
            .cmd_fmt(fmt, &["--replication", "broken"])
            .output()
            .expect("run");
        let err = String::from_utf8_lossy(&o.stderr);
        assert_eq!(o.status.code(), Some(1), "{err}");
        assert_eq!(e.read_report()["replications"][0]["status"], "failed");
        let line = err
            .lines()
            .find(|l| l.contains("replication failed"))
            .unwrap_or_else(|| panic!("{fmt}: no failure log in {err}"));
        assert!(line.contains("ERROR"), "{fmt}: {line}");
        assert!(line.contains("broken"), "{fmt}: {line}");
        assert!(err.contains("boom-at-runtime"), "{fmt}: {err}");
    }
}

#[test]
fn successful_replication_logs_its_status() {
    let e = setup();
    let o = e
        .cmd_fmt("text", &["--replication", "copy"])
        .output()
        .expect("run");
    let err = String::from_utf8_lossy(&o.stderr);
    assert_eq!(o.status.code(), Some(0), "{err}");
    assert!(
        err.lines()
            .any(|l| l.contains("replication succeeded") && l.contains("copy")),
        "{err}"
    );
}

#[test]
fn config_errors_exit_2() {
    let e = setup();
    let o = e.cmd(&["--replication", "nope"]).output().expect("run");
    assert_eq!(
        o.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&o.stderr)
    );
}

#[test]
fn sigterm_kills_promptly_and_reports() {
    let e = setup();
    let mut child = e
        .cmd(&["--replication", "slow"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn");
    wait_for_event(&e.events(), "replication.started", Duration::from_secs(60));
    std::thread::sleep(Duration::from_millis(300));
    let st = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .expect("kill");
    assert!(st.success());
    let (code, took) = wait(&mut child, Duration::from_secs(10));
    assert_eq!(code, 143);
    assert!(took < Duration::from_millis(2500), "exit took {took:?}");
    let r = e.read_report();
    assert_eq!(r["killed_by"], "sigterm");
    assert_eq!(r["replications"][0]["status"], "killed", "{r}");
    let exists: i64 = e
        .lake
        .open("lake")
        .expect("o")
        .query_row("SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = 'big'", [], |r| r.get(0))
        .expect("q");
    assert_eq!(exists, 0, "killed replication committed nothing");
}

#[test]
fn timeout_kills_with_exit_1() {
    let e = setup();
    let mut child = e
        .cmd(&["--replication", "slow", "--timeout", "2s"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn");
    let (code, took) = wait(&mut child, Duration::from_secs(30));
    assert_eq!(code, 1);
    assert!(took < Duration::from_secs(10), "{took:?}");
    let r = e.read_report();
    assert_eq!(r["killed_by"], "timeout");
    assert_eq!(r["replications"][0]["status"], "killed", "{r}");
}
