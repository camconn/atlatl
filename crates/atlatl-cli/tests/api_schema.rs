//! `atlatl api test|explain` and `atlatl schema infer|check` against an in-process HTTP server
//! and recorded cassettes.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Output};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

const KEY: &str = "monthly-live-key-4242";

fn atlatl(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_atlatl"))
        .arg("--project")
        .arg(root)
        .args(args)
        .env_remove("ATLATL_PROJECT")
        .env("MONTHLY_KEY", KEY)
        .output()
        .expect("run atlatl")
}

fn out(o: &Output) -> String {
    format!(
        "status {:?}\nstdout:\n{}\nstderr:\n{}",
        o.status.code(),
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    )
}

/// Serves `/cal?page=N` (two pages); counts requests.
fn server() -> (String, Arc<AtomicU32>) {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind");
    let url = format!("http://{}", l.local_addr().expect("addr"));
    let hits = Arc::new(AtomicU32::new(0));
    let h = hits.clone();
    std::thread::spawn(move || {
        for s in l.incoming() {
            let Ok(mut s) = s else { return };
            h.fetch_add(1, Ordering::SeqCst);
            let mut r = BufReader::new(s.try_clone().expect("clone"));
            let mut line = String::new();
            let _ = r.read_line(&mut line);
            loop {
                let mut x = String::new();
                if r.read_line(&mut x).is_err() || x.trim().is_empty() {
                    break;
                }
            }
            let body = if !line.contains(&format!("c={KEY}")) {
                r#"{"error": "bad key"}"#.to_owned()
            } else if line.contains("page=1") {
                r#"{"items": [{"Symbol": "AAPL", "Importance": 1, "Actual": "1.10"}, {"Symbol": "MSFT", "Importance": 2, "Actual": null}], "last": false}"#.to_owned()
            } else {
                r#"{"items": [{"Symbol": "IBM", "Importance": 3, "Actual": "2"}], "last": true}"#
                    .to_owned()
            };
            let status = if body.contains("bad key") { 401 } else { 200 };
            let _ = write!(
                s,
                "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    (url, hits)
}

fn project(url: &str, schema: Option<&str>) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tmp");
    let files = [
        (
            "atlatl.yaml",
            "version: 1\nconnections:\n  lake: {type: ducklake, catalog: {type: duckdb, path: cat.db}, data_path: data/}\n  monthly: {type: rest, spec: apis/monthly.yaml, secrets: {key: {secret: env:MONTHLY_KEY}}}\n".to_owned(),
        ),
        (
            "apis/monthly.yaml",
            format!(
                "name: monthly\nbase_url: {url}\ndefaults:\n  params: {{c: \"{{{{ secrets.key }}}}\"}}\nendpoints:\n  cal:\n    request: {{path: /cal}}\n    paginate: {{type: page, param: page, start: 1, stop_when: 'response.json.last'}}\n    records: {{path: items, primary_key: [Symbol]}}\n"
            ),
        ),
        (
            "replications/monthly.yaml",
            "replications:\n  monthly:\n    source: monthly\n    target: lake\n    streams:\n      - {id: monthly/cal, from: {endpoint: cal}, object: br.cal, mode: full-refresh, primary_key: [Symbol]}\n".to_owned(),
        ),
    ];
    for (p, t) in files {
        let p = dir.path().join(p);
        std::fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
        std::fs::write(p, t).expect("write");
    }
    if let Some(s) = schema {
        let p = dir.path().join("schemas/monthly/cal.yaml");
        std::fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
        std::fs::write(p, s).expect("write");
    }
    dir
}

#[test]
fn api_test_records_replays_and_explains() {
    let (url, hits) = server();
    let d = project(&url, None);

    let o = atlatl(
        d.path(),
        &["api", "test", "monthly", "cal", "--record", "cal"],
    );
    assert!(o.status.success(), "{}", out(&o));
    let stdout = String::from_utf8_lossy(&o.stdout);
    assert!(!stdout.contains(KEY), "{stdout}");
    assert!(
        stdout.contains("→ GET ") && stdout.contains("c=****"),
        "{stdout}"
    );
    assert!(stdout.contains("3 records over 2 pages"), "{stdout}");
    assert!(stdout.contains("Importance: integer"), "{stdout}");
    assert!(stdout.contains("Actual: decimal, 1 nulls"), "{stdout}");
    let cas = d.path().join("apis/monthly/cassettes/cal.json");
    let text = std::fs::read_to_string(&cas).expect("cassette");
    assert!(!text.contains(KEY));
    assert_eq!(hits.load(Ordering::SeqCst), 2);

    let o = atlatl(
        d.path(),
        &["api", "test", "monthly", "cal", "--replay", "cal", "--json"],
    );
    assert!(o.status.success(), "{}", out(&o));
    assert_eq!(hits.load(Ordering::SeqCst), 2, "replay made no request");
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).expect("json");
    assert_eq!(v["record_count"], 3);
    assert_eq!(v["records"][0]["record"]["Actual"], "1.10");

    let o = atlatl(d.path(), &["api", "test", "monthly", "cal", "--pages", "1"]);
    assert!(o.status.success(), "{}", out(&o));
    assert!(String::from_utf8_lossy(&o.stdout).contains("2 records over 1 pages"));

    let o = atlatl(d.path(), &["api", "explain", "monthly", "cal"]);
    assert!(o.status.success(), "{}", out(&o));
    let s = String::from_utf8_lossy(&o.stdout);
    assert!(s.contains("type: page") && s.contains("builtin:"), "{s}");
    let o = atlatl(d.path(), &["api", "explain", "apis/monthly.yaml", "--json"]);
    assert!(o.status.success(), "{}", out(&o));
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).expect("json");
    assert_eq!(v["endpoints"]["cal"]["paginate"], "page");
}

#[test]
fn schema_infer_and_check_from_cassette() {
    let (url, _) = server();
    let d = project(
        &url,
        Some(
            "schema:\n  fields:\n    Symbol: {type: string, nullable: false}\n    Importance: {type: integer, values: [1, 2]}\n    Actual: {type: decimal}\n",
        ),
    );
    let o = atlatl(
        d.path(),
        &["api", "test", "monthly", "cal", "--record", "cal"],
    );
    assert!(o.status.success(), "{}", out(&o));

    let o = atlatl(
        d.path(),
        &["schema", "check", "monthly/cal", "--from-cassette", "cal"],
    );
    assert_eq!(o.status.code(), Some(1), "{}", out(&o));
    let s = String::from_utf8_lossy(&o.stdout);
    assert!(s.contains("monthly/cal: violated (fail)"), "{s}");
    assert!(s.contains("Domain violations on Importance: 1"), "{s}");

    let o = atlatl(
        d.path(),
        &[
            "schema",
            "check",
            "monthly/cal",
            "--from-cassette",
            "cal",
            "--json",
        ],
    );
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).expect("json");
    assert_eq!(v["severity"], "error");
    assert_eq!(v["violations"][0]["samples"][0]["value"], "3");

    let o = atlatl(
        d.path(),
        &[
            "schema",
            "infer",
            "monthly/cal",
            "--from-cassette",
            "cal",
            "--write",
            "--force",
        ],
    );
    assert!(o.status.success(), "{}", out(&o));
    let y = std::fs::read_to_string(d.path().join("schemas/monthly/cal.yaml")).expect("yaml");
    assert!(
        y.contains("mode: observe") && y.contains("Importance: {type: integer, nullable: false}"),
        "{y}"
    );
    let o = atlatl(
        d.path(),
        &["schema", "check", "monthly/cal", "--from-cassette", "cal"],
    );
    assert!(o.status.success(), "{}", out(&o));
    assert!(String::from_utf8_lossy(&o.stdout).contains("monthly/cal: ok"));

    let o = atlatl(d.path(), &["schema", "infer", "monthly/cal"]);
    assert!(!o.status.success());
}
