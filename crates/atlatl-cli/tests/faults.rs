//! Fault injection through the shipped binary (12 §3): signals at controlled points (`ATLATL_FAULT`),
//! SIGKILL mid-commit, and panics inside a replication.

use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use atlatl_testkit::{LocalLake, TempProject};

const REPS: &str = r#"replications:
  copy:
    source: lake
    target: lake
    streams:
      - {id: c/people, mode: full-refresh, from: {table: src.people}, object: br.people, primary_key: [id]}
  boom:
    source: lake
    target: lake
    streams:
      - {id: b/people, mode: full-refresh, from: {table: src.people}, object: br.boom, primary_key: [id]}
"#;

struct Env {
    lake: LocalLake,
    project: TempProject,
    out: tempfile::TempDir,
}

fn setup() -> Env {
    let lake = LocalLake::file().expect("lake");
    let c = lake.open("lake").expect("open");
    c.execute_batch("CREATE SCHEMA lake.src; CREATE TABLE lake.src.people AS SELECT range AS id, 'p' || range AS name FROM range(1, 6);").expect("seed");
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
    fn stderr(&self) -> PathBuf {
        self.out.path().join("stderr.log")
    }
    fn cmd(&self, fault: Option<&str>, args: &[&str]) -> Command {
        let mut c = Command::new(env!("CARGO_BIN_EXE_atlatl"));
        c.arg("--project")
            .arg(self.project.path())
            .arg("--report")
            .arg(self.report())
            .args(["--log-format", "json", "run"])
            .args(args)
            .env("ATLATL_EXTENSION_DIR", atlatl_testkit::extension_dir())
            .env_remove("ATLATL_PROJECT")
            .env_remove(atlatl_core::fault::ENV)
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(self.stderr()).expect("stderr"));
        if let Some(f) = fault {
            c.env(atlatl_core::fault::ENV, f);
        }
        c
    }
    fn run(&self, fault: Option<&str>, args: &[&str]) -> ExitStatus {
        let mut child = self.cmd(fault, args).spawn().expect("spawn");
        wait(&mut child, Duration::from_secs(60)).0
    }
    fn read_report(&self) -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(self.report()).expect("report")).expect("json")
    }
    fn q(&self, sql: &str) -> String {
        let c = self.lake.open("lake").expect("open");
        c.query_row(&format!("SELECT ({sql})::VARCHAR"), [], |r| {
            r.get::<_, Option<String>>(0)
        })
        .expect(sql)
        .unwrap_or_default()
    }
    /// Runs `boom` once so the state tables exist, and returns the snapshot after it.
    fn bootstrapped(&self) -> String {
        assert_eq!(
            self.run(None, &["--replication", "boom"]).code(),
            Some(0),
            "{}",
            self.stderr_text()
        );
        self.snapshot()
    }
    fn snapshot(&self) -> String {
        self.q("SELECT max(snapshot_id) FROM ducklake_snapshots('lake')")
    }
    /// Snapshots after `before` that touched anything but the state store (`_atlatl` run history
    /// is recorded even for a killed run).
    fn data_snapshots_since(&self, before: &str) -> String {
        self.q(&format!(
            "SELECT count(*) FROM ducklake_snapshots('lake') s WHERE snapshot_id > {before}
               AND EXISTS (SELECT 1 FROM (SELECT unnest(flatten(map_values(s.changes))) AS v) x
                           WHERE x.v NOT IN (SELECT table_id::VARCHAR FROM ducklake_table_info('lake') WHERE table_name IN ('runs', 'run_replications', 'stream_state', 'rejects'))
                             AND x.v NOT LIKE '%_atlatl%')"
        ))
    }
    fn table_exists(&self, t: &str) -> bool {
        self.q(&format!("SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'br' AND table_name = '{t}'")) == "1"
    }
    fn rows(&self, t: &str) -> String {
        self.q(&format!(
            "SELECT string_agg(id || ':' || name, ',' ORDER BY id) FROM lake.br.{t}"
        ))
    }
    fn source_rows(&self) -> String {
        self.q("SELECT string_agg(id || ':' || name, ',' ORDER BY id) FROM lake.src.people")
    }
    fn stderr_text(&self) -> String {
        std::fs::read_to_string(self.stderr()).unwrap_or_default()
    }
}

fn rep<'a>(r: &'a serde_json::Value, name: &str) -> &'a serde_json::Value {
    r["replications"]
        .as_array()
        .expect("reps")
        .iter()
        .find(|x| x["replication"] == name)
        .unwrap_or_else(|| panic!("no {name} in {r}"))
}

/// Waits until the child logged that `point` fired.
fn wait_for_fault(e: &Env, point: &str, limit: Duration) {
    let t = Instant::now();
    while t.elapsed() < limit {
        if e.stderr_text()
            .lines()
            .any(|l| l.contains("fault injected") && l.contains(point))
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!(
        "`{point}` never fired within {limit:?}; stderr:\n{}",
        e.stderr_text()
    );
}

fn wait(child: &mut Child, limit: Duration) -> (ExitStatus, Duration) {
    let t = Instant::now();
    loop {
        if let Some(s) = child.try_wait().expect("wait") {
            return (s, t.elapsed());
        }
        if t.elapsed() > limit {
            let _ = child.kill();
            panic!("still running after {limit:?}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn sigterm(child: &Child) {
    assert!(
        Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .expect("kill")
            .success()
    );
}

#[test]
fn sigterm_during_stage_exits_143_and_commits_nothing() {
    let e = setup();
    let before = e.bootstrapped();
    let mut child = e
        .cmd(
            Some("lake.after_stage:sleep:5000"),
            &["--replication", "copy"],
        )
        .spawn()
        .expect("spawn");
    wait_for_fault(&e, "lake.after_stage", Duration::from_secs(60));
    sigterm(&child);
    let (st, took) = wait(&mut child, Duration::from_secs(10));
    assert_eq!(st.code(), Some(143), "{}", e.stderr_text());
    assert!(took < Duration::from_millis(1500), "exit took {took:?}");
    let r = e.read_report();
    assert_eq!(r["killed_by"], "sigterm");
    assert_eq!(r["exit_code"], 143);
    assert_eq!(rep(&r, "copy")["status"], "killed", "{r}");
    assert!(!e.table_exists("people"), "nothing committed");
    assert_eq!(
        e.data_snapshots_since(&before),
        "0",
        "only run history was written"
    );
}

#[test]
fn sigterm_during_commit_rolls_back_and_rerun_succeeds() {
    let e = setup();
    let before = e.bootstrapped();
    let mut child = e
        .cmd(Some("lake.commit:sleep:5000"), &["--replication", "copy"])
        .spawn()
        .expect("spawn");
    wait_for_fault(&e, "lake.commit", Duration::from_secs(60));
    sigterm(&child);
    let (st, took) = wait(&mut child, Duration::from_secs(10));
    assert_eq!(st.code(), Some(143), "{}", e.stderr_text());
    assert!(took < Duration::from_millis(1500), "exit took {took:?}");
    let r = e.read_report();
    assert_eq!(rep(&r, "copy")["status"], "killed", "{r}");
    assert!(
        !e.table_exists("people"),
        "the open transaction rolled back"
    );
    assert_eq!(e.data_snapshots_since(&before), "0");

    let st = e.run(None, &["--replication", "copy"]);
    assert_eq!(st.code(), Some(0), "{}", e.stderr_text());
    assert_eq!(e.rows("people"), e.source_rows());
    assert_eq!(rep(&e.read_report(), "copy")["status"], "succeeded");
}

#[test]
fn sigkill_mid_commit_leaves_pre_commit_state() {
    let e = setup();
    assert_eq!(
        e.run(None, &["--replication", "copy"]).code(),
        Some(0),
        "{}",
        e.stderr_text()
    );
    let pre = e.rows("people");
    let pre_snap = e.snapshot();
    let c = e.lake.open("lake").expect("open");
    c.execute_batch("DELETE FROM lake.src.people WHERE id = 5; UPDATE lake.src.people SET name = 'changed' WHERE id = 1; INSERT INTO lake.src.people VALUES (6, 'p6');").expect("mutate");
    drop(c);
    let src_snap = e.snapshot();
    std::fs::remove_file(e.report()).expect("rm report");

    let st = e.run(Some("lake.commit:kill"), &["--replication", "copy"]);
    assert_eq!(st.signal(), Some(9), "{st:?}: {}", e.stderr_text());
    assert!(!e.report().exists(), "SIGKILL writes no report");
    assert_eq!(
        e.rows("people"),
        pre,
        "target is exactly the pre-commit state"
    );
    assert_eq!(e.snapshot(), src_snap);
    assert_ne!(pre_snap, src_snap);

    assert_eq!(
        e.run(None, &["--replication", "copy"]).code(),
        Some(0),
        "{}",
        e.stderr_text()
    );
    assert_eq!(
        e.rows("people"),
        "1:changed,2:p2,3:p3,4:p4,6:p6",
        "post-commit state after a clean rerun"
    );
}

#[test]
fn panic_in_replication_fails_it_alone_with_exit_3() {
    let e = setup();
    let st = e.run(Some("exec.replication@boom:panic"), &[]);
    assert_eq!(st.code(), Some(3), "{}", e.stderr_text());
    let r = e.read_report();
    assert_eq!(r["exit_code"], 3);
    let boom = rep(&r, "boom");
    assert_eq!(boom["status"], "failed", "{r}");
    assert_eq!(boom["error"]["class"], "internal");
    assert_eq!(boom["error"]["code"], "panic");
    assert_eq!(rep(&r, "copy")["status"], "succeeded", "{r}");
    assert_eq!(e.rows("people"), e.source_rows());
    assert!(!e.table_exists("boom"));
}

#[test]
fn panic_inside_commit_rolls_back_with_exit_3() {
    let e = setup();
    let st = e.run(Some("lake.commit@boom:panic"), &[]);
    assert_eq!(st.code(), Some(3), "{}", e.stderr_text());
    let r = e.read_report();
    let boom = rep(&r, "boom");
    assert_eq!(boom["status"], "failed", "{r}");
    assert_eq!(boom["error"]["class"], "internal");
    assert_eq!(boom["error"]["code"], "panic", "{r}");
    assert_eq!(rep(&r, "copy")["status"], "succeeded", "{r}");
    assert!(
        !e.table_exists("boom"),
        "the panicking transaction was abandoned"
    );
    assert_eq!(e.rows("people"), e.source_rows());

    assert_eq!(
        e.run(None, &["--replication", "boom"]).code(),
        Some(0),
        "{}",
        e.stderr_text()
    );
    assert_eq!(e.rows("boom"), e.source_rows());
}

#[test]
fn bad_fault_spec_is_rejected() {
    let e = setup();
    let st = e.run(Some("lake.commit:explode"), &["--replication", "copy"]);
    assert_eq!(st.code(), Some(3));
    assert!(
        e.stderr_text().contains("ATLATL_FAULT"),
        "{}",
        e.stderr_text()
    );
    assert!(!e.table_exists("people"));
}
