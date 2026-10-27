//! `atlatl rejects list | retry | drop` (03 §7) against an in-process JSONL feed.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::process::{Command, Output};
use std::sync::{Arc, Mutex};

use atlatl_testkit::{LocalLake, TempProject};
use serde_json::Value;

const BROKEN: &str = "{\"id\": 1}\n{\"id\": 2, \"v\": \n{\"id\": 3}\n";
const FIXED: &str = "{\"id\": 1}\n{\"id\": 2, \"v\": \"ok\"}\n{\"id\": 3}\n";

/// Serves `body` for every request.
fn server(body: Arc<Mutex<String>>) -> String {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind");
    let url = format!("http://{}", l.local_addr().expect("addr"));
    std::thread::spawn(move || {
        for s in l.incoming() {
            let Ok(mut s) = s else { return };
            let mut r = BufReader::new(s.try_clone().expect("clone"));
            loop {
                let mut x = String::new();
                if r.read_line(&mut x).is_err() || x.trim().is_empty() {
                    break;
                }
            }
            let b = body.lock().expect("body").clone();
            let _ = write!(
                s,
                "HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{b}",
                b.len()
            );
        }
    });
    url
}

struct Env {
    lake: LocalLake,
    project: TempProject,
    body: Arc<Mutex<String>>,
}

fn setup() -> Env {
    let body = Arc::new(Mutex::new(BROKEN.to_owned()));
    let url = server(body.clone());
    let lake = LocalLake::file().expect("lake");
    let conns = format!(
        "{}  api: {{type: rest, spec: apis/feed.yaml, retry: {{max_attempts: 2, backoff: constant, base: 5ms}}}}\n",
        lake.connection_yaml("lake", "lake")
    );
    let project = TempProject::new()
        .and_then(|p| p.project(&conns, "state: {lake: lake}\n"))
        .and_then(|p| {
            p.file(
                "apis/feed.yaml",
                &format!(
                    "base_url: {url}\nendpoints:\n  feed:\n    request: {{path: /feed}}\n    decode: {{format: jsonl}}\n    records: {{primary_key: [id]}}\n"
                ),
            )
        })
        .and_then(|p| {
            p.replication(
                "r",
                "replications:\n  r:\n    source: api\n    target: lake\n    streams:\n      - {id: api/feed, from: {endpoint: feed}, object: br.feed, mode: full-refresh, primary_key: [id], on_decode_error: {retry: row, reject_threshold: {ratio: 0.5, min_count: 1}}}\n",
            )
        })
        .expect("project");
    Env {
        lake,
        project,
        body,
    }
}

impl Env {
    fn atlatl(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_atlatl"))
            .arg("--project")
            .arg(self.project.path())
            .args(args)
            .env("ATLATL_EXTENSION_DIR", atlatl_testkit::extension_dir())
            .env_remove("ATLATL_PROJECT")
            .output()
            .expect("atlatl")
    }

    fn ok(&self, args: &[&str]) -> String {
        let o = self.atlatl(args);
        assert!(
            o.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&o.stderr)
        );
        String::from_utf8_lossy(&o.stdout).into_owned()
    }

    fn json(&self, args: &[&str]) -> Value {
        serde_json::from_str(&self.ok(args)).expect("json")
    }

    fn count(&self, sql: &str) -> i64 {
        self.lake
            .open("lake")
            .expect("open")
            .query_row(sql, [], |r| r.get(0))
            .expect(sql)
    }
}

#[test]
fn list_retry_and_drop() {
    let e = setup();
    assert_eq!(
        e.json(&["rejects", "list", "--json"]),
        serde_json::json!([])
    );
    assert!(e.ok(&["rejects", "list"]).contains("no open rejects"));
    e.ok(&["run"]);

    let rows = e.json(&["rejects", "list", "--json", "--stream", "api/feed"]);
    let r = &rows[0];
    assert_eq!(rows.as_array().map(Vec::len), Some(1));
    for k in [
        "reject_key",
        "error_class",
        "error",
        "attempts",
        "first_seen_at",
        "last_seen_at",
        "first_seen_run",
        "request",
    ] {
        assert!(!r[k].is_null(), "{k}: {r}");
    }
    assert_eq!(
        (&r["member"], &r["error_class"]),
        (&"".into(), &"decode".into())
    );
    let key = r["reject_key"].as_str().expect("key").to_owned();
    let text = e.ok(&["rejects", "list"]);
    assert!(
        text.contains("api/feed: 1 open") && text.contains(&key) && text.contains("attempts 1"),
        "{text}"
    );
    assert_eq!(
        e.json(&["rejects", "list", "--json", "--stream", "other/x"]),
        serde_json::json!([])
    );

    // Fixed upstream: the retry lands the row and resolves the reject in one commit.
    *e.body.lock().expect("body") = FIXED.into();
    let out = e.json(&["rejects", "retry", "api/feed"]);
    assert_eq!(out["exit_code"], 0, "{out:#}");
    let s = &out["replications"][0]["streams"][0];
    assert_eq!(
        (&s["resolved"], &s["open"]),
        (&1.into(), &0.into()),
        "{out:#}"
    );
    assert_eq!(
        e.count("SELECT count(*) FROM lake.br.feed WHERE v = 'ok'"),
        1
    );
    assert_eq!(e.count("SELECT count(*) FROM lake._atlatl.rejects"), 0);

    // Broken again; drop needs a stream or --all.
    *e.body.lock().expect("body") = BROKEN.replace("{\"id\": 2,", "{\"id\": 4,");
    e.ok(&["run"]);
    assert_eq!(e.count("SELECT count(*) FROM lake._atlatl.rejects"), 1);
    let o = e.atlatl(&["rejects", "drop"]);
    assert_eq!(
        o.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&o.stderr)
    );
    let o = e.atlatl(&["rejects", "drop", "api/feed", "--all"]);
    assert!(!o.status.success());
    assert_eq!(
        e.json(&["rejects", "drop", "api/feed", "--key", "nope"]),
        serde_json::json!({"dropped": 0})
    );
    let key = e.json(&["rejects", "list", "--json"])[0]["reject_key"]
        .as_str()
        .expect("key")
        .to_owned();
    assert_eq!(
        e.json(&["rejects", "drop", "api/feed", "--key", &key]),
        serde_json::json!({"dropped": 1})
    );
    assert_eq!(e.count("SELECT count(*) FROM lake._atlatl.rejects"), 0);
    assert_eq!(
        e.count(
            "SELECT count(*) FROM lake._atlatl.runs WHERE kind IN ('rejects drop', 'rejects retry')"
        ),
        3
    );

    e.ok(&["run"]);
    assert_eq!(
        e.json(&["rejects", "drop", "--all"]),
        serde_json::json!({"dropped": 1})
    );
}

#[test]
fn selection_and_environment_are_checked() {
    let e = setup();
    for args in [
        &["rejects", "retry", "nope/x"][..],
        &["rejects", "retry", "api/feed@k"],
        &["rejects", "list", "--env", "nosuch"],
        &["rejects", "drop", "--all", "--member", "m"],
    ] {
        let o = e.atlatl(args);
        assert_eq!(
            o.status.code(),
            Some(2),
            "{args:?}: {}",
            String::from_utf8_lossy(&o.stderr)
        );
    }
}
