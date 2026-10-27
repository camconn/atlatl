//! Shared by the dogfood tests: a copied fixture project with a local lake, run with HTTP(S)
//! proxies pointed at a dead port (loopback exempt) so a request that escaped the cassette fails
//! instead of reaching a vendor, and a tiny HTTP/1.1 server for the vendor emulators.

#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Arc, Mutex};

use atlatl_testkit::LocalLake;
use serde_json::Value;

pub fn fixture(sub: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(sub)
}

pub fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("mkdir");
    for e in std::fs::read_dir(from).expect("read_dir") {
        let e = e.expect("entry");
        let dst = to.join(e.file_name());
        if e.file_type().expect("type").is_dir() {
            copy_dir(&e.path(), &dst);
        } else {
            std::fs::copy(e.path(), dst).expect("copy");
        }
    }
}

pub fn out(o: &Output) -> String {
    format!(
        "status {:?}\nstdout:\n{}\nstderr:\n{}",
        o.status.code(),
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    )
}

pub fn ty<'a>(types: &'a [(String, String)], col: &str) -> &'a str {
    types
        .iter()
        .find(|(n, _)| n == col)
        .map(|(_, t)| t.as_str())
        .unwrap_or_else(|| panic!("no column `{col}` in {types:?}"))
}

/// A copy of a fixture project, with a lake.
pub struct Proj {
    pub dir: tempfile::TempDir,
    pub lake: LocalLake,
    secrets: Vec<(&'static str, String)>,
    /// Secret env vars cleared from every command; only `secrets` sets them.
    env_names: &'static [&'static str],
    /// Values that must never appear in a run report, besides `secrets`.
    pub forbidden: Vec<String>,
}

impl Proj {
    pub fn new(
        fixture: &Path,
        env_names: &'static [&'static str],
        secrets: &[(&'static str, &str)],
    ) -> Self {
        let dir = tempfile::tempdir().expect("tmp");
        copy_dir(fixture, dir.path());
        Self {
            dir,
            lake: LocalLake::file().expect("lake"),
            secrets: secrets.iter().map(|(k, v)| (*k, (*v).to_owned())).collect(),
            env_names,
            forbidden: Vec::new(),
        }
    }

    pub fn path(&self) -> &Path {
        self.dir.path()
    }

    pub fn secret_values(&self) -> impl Iterator<Item = &str> {
        self.secrets.iter().map(|(_, s)| s.as_str())
    }

    pub fn cmd(&self, args: &[&str]) -> Command {
        let mut c = Command::new(env!("CARGO_BIN_EXE_atlatl"));
        c.arg("--project").arg(self.path()).args(args);
        c.env_remove("ATLATL_PROJECT")
            .env_remove("ATLATL_CASSETTE")
            .env_remove("ATLATL_CASSETTE_RECORD")
            .env("ATLATL_EXTENSION_DIR", atlatl_testkit::extension_dir())
            .env(
                "DOGFOOD_LAKE_CATALOG",
                self.lake.dir.path().join("meta.ducklake"),
            )
            .env("DOGFOOD_LAKE_DATA", self.lake.data_path());
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
        for k in self.env_names {
            c.env_remove(k);
        }
        for (k, v) in &self.secrets {
            c.env(k, v);
        }
        c
    }

    pub fn run(&self, args: &[&str]) -> Output {
        self.cmd(args).output().expect("atlatl")
    }

    /// `atlatl run --replication <rep>` with a cassette (`ATLATL_CASSETTE[_RECORD]=<name>`).
    pub fn lake_run(&self, rep: &str, cassette: (&str, &str)) -> Value {
        let o = self.try_lake_run(rep, cassette);
        assert!(o.0.status.success(), "{}", out(&o.0));
        o.1
    }

    /// Like [`lake_run`](Self::lake_run), without asserting success.
    pub fn try_lake_run(&self, rep: &str, cassette: (&str, &str)) -> (Output, Value) {
        let report = self
            .path()
            .join(format!("report-{rep}-{}.json", cassette.1));
        let o = self
            .cmd(&[
                "--report",
                report.to_str().expect("utf8"),
                "run",
                "--replication",
                rep,
            ])
            .env(cassette.0, cassette.1)
            .output()
            .expect("atlatl run");
        let text =
            std::fs::read_to_string(&report).unwrap_or_else(|e| panic!("report: {e}\n{}", out(&o)));
        for s in self
            .secret_values()
            .chain(self.forbidden.iter().map(String::as_str))
        {
            assert!(!text.contains(s), "secret in the run report");
        }
        (o, serde_json::from_str(&text).expect("report json"))
    }

    pub fn q(&self, sql: &str) -> Vec<String> {
        let c = self.lake.open("lake").expect("open");
        let mut s = c
            .prepare(&format!("SELECT to_json(q)::VARCHAR FROM ({sql}) q"))
            .expect(sql);
        s.query_map([], |r| r.get::<_, String>(0))
            .expect(sql)
            .collect::<Result<_, _>>()
            .expect(sql)
    }

    pub fn count(&self, table: &str) -> i64 {
        let c = self.lake.open("lake").expect("open");
        c.query_row(&format!("SELECT count(*) FROM lake.{table}"), [], |r| {
            r.get(0)
        })
        .expect(table)
    }

    pub fn types(&self, table: &str) -> Vec<(String, String)> {
        let (schema, t) = table.split_once('.').expect("schema.table");
        let c = self.lake.open("lake").expect("open");
        let mut s = c
            .prepare("SELECT column_name, data_type FROM information_schema.columns WHERE table_catalog = 'lake' AND table_schema = ? AND table_name = ? ORDER BY ordinal_position")
            .expect("prepare");
        s.query_map([schema, t], |r| Ok((r.get(0)?, r.get(1)?)))
            .expect("types")
            .collect::<Result<_, _>>()
            .expect("types")
    }

    pub fn sync(&self, stream: &str) -> Value {
        let c = self.lake.open("lake").expect("open");
        let v: String = c
            .query_row(
                "SELECT value::VARCHAR FROM lake._atlatl.stream_state WHERE stream_id = ? AND kind = 'sync'",
                [stream],
                |r| r.get(0),
            )
            .expect(stream);
        serde_json::from_str(&v).expect("state json")
    }

    /// Points `spec`'s `base_url` host at an emulator.
    pub fn retarget(&self, spec: &str, from: &str, to: &str) {
        let path = self.path().join(spec);
        let text = std::fs::read_to_string(&path).expect("spec");
        let line = format!("base_url: {from}/");
        assert!(text.contains(&line), "{spec}");
        std::fs::write(&path, text.replace(&line, &format!("base_url: {to}/"))).expect("write");
    }
}

/// The report entry for stream `id`.
pub fn stream<'a>(report: &'a Value, id: &str) -> &'a Value {
    report["replications"]
        .as_array()
        .expect("replications")
        .iter()
        .flat_map(|r| r["streams"].as_array().expect("streams"))
        .find(|s| s["stream_id"] == id)
        .unwrap_or_else(|| panic!("no stream {id} in {report}"))
}

/// The first member's `http` block of stream `id`.
pub fn http<'a>(report: &'a Value, id: &str) -> &'a Value {
    &stream(report, id)["members"][0]["http"]
}

// ----------------------------------------------------------------------------- emulator server

#[derive(Debug)]
pub struct Req {
    pub path: String,
    pub query: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

pub fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let hex = b
            .get(i + 1..i + 3)
            .and_then(|h| std::str::from_utf8(h).ok())
            .and_then(|h| u8::from_str_radix(h, 16).ok());
        match (b[i], hex) {
            (b'%', Some(v)) => {
                out.push(v);
                i += 3;
            }
            (b'+', _) => {
                out.push(b' ');
                i += 1;
            }
            (c, _) => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

impl Req {
    pub fn param(&self, k: &str) -> Option<String> {
        self.query.split('&').find_map(|p| {
            let (n, v) = p.split_once('=').unwrap_or((p, ""));
            (pct_decode(n) == k).then(|| pct_decode(v))
        })
    }

    pub fn header(&self, k: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(k))
            .map(|(_, v)| v.as_str())
    }
}

/// A response: status, `Content-Type` (none when `None`) and body.
pub struct Resp(pub u16, pub Option<&'static str>, pub String);

impl Resp {
    pub fn json(status: u16, body: impl Into<String>) -> Self {
        Self(status, Some("application/json"), body.into())
    }
}

impl From<(u16, String)> for Resp {
    fn from((s, b): (u16, String)) -> Self {
        Self::json(s, b)
    }
}

pub type Handler = dyn Fn(&Req) -> Resp + Send + Sync;

/// A one-request-per-connection HTTP/1.1 server; returns its base URL and the request log.
pub fn serve(h: Arc<Handler>) -> (String, Arc<Mutex<Vec<String>>>) {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind");
    let url = format!("http://{}", l.local_addr().expect("addr"));
    let log: Arc<Mutex<Vec<String>>> = Arc::default();
    let lg = log.clone();
    std::thread::spawn(move || {
        for s in l.incoming() {
            let Ok(mut s) = s else { return };
            let (h, lg) = (h.clone(), lg.clone());
            std::thread::spawn(move || {
                let mut r = BufReader::new(s.try_clone().expect("clone"));
                let mut line = String::new();
                if r.read_line(&mut line).is_err() {
                    return;
                }
                let target = line
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or_default()
                    .to_owned();
                let mut headers = Vec::new();
                loop {
                    let mut x = String::new();
                    if r.read_line(&mut x).is_err() || x.trim().is_empty() {
                        break;
                    }
                    if let Some((k, v)) = x.trim_end().split_once(':') {
                        headers.push((k.trim().to_owned(), v.trim().to_owned()));
                    }
                }
                let len = headers
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                    .and_then(|(_, v)| v.parse().ok())
                    .unwrap_or(0usize);
                let mut body = vec![0; len];
                let _ = r.read_exact(&mut body);
                let (path, query) = target.split_once('?').unwrap_or((&target, ""));
                let req = Req {
                    path: path.to_owned(),
                    query: query.to_owned(),
                    headers,
                    body,
                };
                lg.lock().expect("log").push(target.clone());
                let Resp(status, ct, body) = h(&req);
                let ct = ct
                    .map(|c| format!("Content-Type: {c}\r\n"))
                    .unwrap_or_default();
                let _ = write!(
                    s,
                    "HTTP/1.1 {status} X\r\n{ct}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            });
        }
    });
    (url, log)
}

pub fn in_range(date: &str, from: &str, to: &str) -> bool {
    from <= date && date <= to
}

/// Copies a recording's cassettes into the committed fixture, with the emulator's URL swapped
/// back to the spec's host, after checking that no secret or loopback address was recorded.
/// Returns the number of files.
pub fn commit_cassettes(src: &Path, dst: &Path, url: &str, base: &str, secrets: &[&str]) -> usize {
    std::fs::create_dir_all(dst).expect("mkdir");
    let mut n = 0;
    for e in std::fs::read_dir(src).expect("cassettes") {
        let e = e.expect("entry");
        let text = std::fs::read_to_string(e.path())
            .expect("read")
            .replace(url, base);
        for s in secrets {
            assert!(!text.contains(s), "{}: secret recorded", e.path().display());
        }
        assert!(
            !text.contains("127.0.0.1") && !text.to_ascii_lowercase().contains("\"signature\""),
            "{}",
            e.path().display()
        );
        std::fs::write(dst.join(e.file_name()), text).expect("write");
        n += 1;
    }
    n
}
