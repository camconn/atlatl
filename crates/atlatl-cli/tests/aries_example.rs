//! `examples/aries/` against an in-process Aries fake: members' personal-token clients from a DuckDB
//! seed, one OAuth2 token per member, every account's order history walked in 90-day windows with
//! Aries' `nextCursor` paging, and raw responses landed with no credential in the lake or logs.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};

use atlatl_testkit::LocalLake;
use serde_json::{Value, json};

const MEMBERS: &[(&str, &str, &str, &[&str])] = &[
    (
        "alice",
        "ARI-PERSONAL-TK.alice",
        "cs-alice-aries-ex",
        &["AL001", "SIMAL"],
    ),
    ("bob", "ARI-PERSONAL-TK.bob", "cs-bob-aries-ex", &["BO001"]),
];

/// Query or form pairs, still percent-encoded (the values compared here are unreserved).
fn query(target: &str) -> HashMap<String, String> {
    let q = target.split_once('?').map(|(_, q)| q).unwrap_or_default();
    q.split('&')
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
}

fn reply(target: &str, auth: &str, body: &str) -> (u16, Value) {
    let path = target.split('?').next().unwrap_or_default();
    if path == "/v1/oauth2/token" {
        let f = query(&format!("?{body}"));
        return match MEMBERS.iter().find(|m| {
            f.get("client_id").map(String::as_str) == Some(m.1)
                && f.get("client_secret").map(String::as_str) == Some(m.2)
        }) {
            Some(m) => (
                200,
                json!({"access_token": format!("at-{}-aries-ex", m.0), "token_type": "Bearer", "expires_in": 900}),
            ),
            None => (401, json!({"error": "invalid_client"})),
        };
    }
    let Some(member) = MEMBERS
        .iter()
        .find(|m| auth == format!("Bearer at-{}-aries-ex", m.0))
    else {
        return (401, json!({"error": "invalid token"}));
    };
    if path == "/v2/users/me/accounts" {
        let accounts: Vec<Value> = member
            .3
            .iter()
            .map(|a| json!({"trading_account_id": a, "is_sim": a.starts_with("SIM"), "status": "OPEN"}))
            .collect();
        return (200, json!({"accounts": accounts}));
    }
    let account = path.split('/').nth(3).unwrap_or_default();
    if !member.3.contains(&account) {
        return (403, json!({"error": "not your account"}));
    }
    let q = query(target);
    if path.ends_with("/orders/history") {
        let from = q.get("from").cloned().unwrap_or_default();
        let cursor = q.get("cursor").map_or(0, |c| c.parse().expect("cursor"));
        let order = json!({"clOrdId": format!("{account}-{from}-{cursor}"), "accountId": account, "qty": "1.50", "createdAt": from});
        let next = if cursor == 0 { 41 } else { 0 };
        return (200, json!({"orders": [order], "nextCursor": next}));
    }
    if path.ends_with("/orders") {
        return (200, json!({"orders": [], "nextCursor": 0}));
    }
    if path.ends_with("/positions") {
        return (
            200,
            json!({"positions": [{"accountId": account, "symbol": "AAPL", "quantity": "10", "todayPl": "1.20"}]}),
        );
    }
    if path.ends_with("/balance") {
        return (200, json!({"accountId": account, "dayTotalPnl": "549.64"}));
    }
    (404, json!({}))
}

/// Serves [`reply`]; returns the base URL and the `METHOD target` log.
fn server() -> (String, Arc<Mutex<Vec<String>>>) {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind");
    let url = format!("http://{}", l.local_addr().expect("addr"));
    let log: Arc<Mutex<Vec<String>>> = Arc::default();
    let lg = log.clone();
    std::thread::spawn(move || {
        for s in l.incoming() {
            let Ok(mut s) = s else { return };
            let mut r = BufReader::new(s.try_clone().expect("clone"));
            let mut line = String::new();
            if r.read_line(&mut line).is_err() {
                continue;
            }
            let mut parts = line.split_whitespace();
            let method = parts.next().unwrap_or_default().to_owned();
            let target = parts.next().unwrap_or_default().to_owned();
            let (mut len, mut auth) = (0usize, String::new());
            loop {
                let mut h = String::new();
                if r.read_line(&mut h).is_err() || h.trim().is_empty() {
                    break;
                }
                if let Some((k, v)) = h.split_once(':') {
                    if k.eq_ignore_ascii_case("content-length") {
                        len = v.trim().parse().unwrap_or(0);
                    } else if k.eq_ignore_ascii_case("authorization") {
                        v.trim().clone_into(&mut auth);
                    }
                }
            }
            let mut body = vec![0; len];
            let _ = r.read_exact(&mut body);
            let (status, out) = reply(&target, &auth, &String::from_utf8_lossy(&body));
            lg.lock()
                .expect("log")
                .push(format!("{method} {target} {status}"));
            let b = out.to_string();
            let _ = write!(
                s,
                "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{b}",
                b.len()
            );
        }
    });
    (url, log)
}

fn files_containing(dir: &Path, needle: &[u8]) -> Vec<String> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir).expect("dir") {
        let p = e.expect("entry").path();
        if p.is_dir() {
            out.extend(files_containing(&p, needle));
        } else if std::fs::read(&p)
            .expect("read")
            .windows(needle.len())
            .any(|w| w == needle)
        {
            out.push(p.display().to_string());
        }
    }
    out
}

fn strings(c: &duckdb::Connection, sql: &str) -> Vec<String> {
    let mut s = c.prepare(sql).expect(sql);
    s.query_map([], |r| r.get::<_, String>(0))
        .expect(sql)
        .collect::<Result<_, _>>()
        .expect(sql)
}

#[test]
fn club_scrape_lands_raw_responses_per_member() {
    let (url, log) = server();
    let lake = LocalLake::file().expect("lake");
    let dir = tempfile::tempdir().expect("tmp");
    let seed = dir.path().join("seed.duckdb");
    let example = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/aries");
    {
        let c = duckdb::Connection::open(&seed).expect("seed");
        c.execute_batch(&std::fs::read_to_string(example.join("seed.sql")).expect("seed.sql"))
            .expect("schema");
        for (m, id, secret, _) in MEMBERS {
            c.execute("INSERT INTO members VALUES (?, ?, ?)", [m, id, secret])
                .expect("member");
        }
    }
    let report = dir.path().join("report.json");
    let o = Command::new(env!("CARGO_BIN_EXE_atlatl"))
        .arg("--project")
        .arg(&example)
        .args(["--log-format", "json", "--report"])
        .arg(&report)
        .arg("run")
        .env_remove("ATLATL_PROJECT")
        .env_remove("ATLATL_CASSETTE")
        .env("ATLATL_EXTENSION_DIR", atlatl_testkit::extension_dir())
        .env("ARIES_BASE_URL", &url)
        .env("ARIES_SEED", &seed)
        .env("ARIES_LAKE", lake.dir.path())
        .output()
        .expect("atlatl run");
    let stderr = String::from_utf8_lossy(&o.stderr);
    assert!(o.status.success(), "{stderr}");

    let log = log.lock().expect("log").clone();
    let tokens = log.iter().filter(|l| l.contains("/oauth2/token")).count();
    assert_eq!(
        tokens, 2,
        "one token per member, shared by all endpoints: {log:#?}"
    );
    let failed: Vec<&String> = log.iter().filter(|l| !l.ends_with(" 200")).collect();
    assert!(failed.is_empty(), "{failed:#?}");
    let first: Vec<&String> = log
        .iter()
        .filter(|l| l.contains("/AL001/orders/history"))
        .take(3)
        .collect();
    assert!(first[0].contains("from=2025-01-01T00%3A00%3A00Z") && !first[0].contains("cursor"));
    assert!(first[1].contains("from=2025-01-01T00%3A00%3A00Z") && first[1].contains("cursor=41"));
    assert!(first[2].contains("from=2025-04-01T00%3A00%3A00Z") && !first[2].contains("cursor"));

    let c = lake.open("lake").expect("lake");
    assert_eq!(
        strings(
            &c,
            "SELECT member || ':' || trading_account_id FROM lake.br.aries_accounts ORDER BY 1"
        ),
        ["alice:AL001", "alice:SIMAL", "bob:BO001"]
    );
    let per_account = strings(
        &c,
        "SELECT a || ':' || count(*) FROM (SELECT regexp_extract(request_key, 'accounts/([A-Z0-9]+)/', 1) AS a FROM lake.br.aries_orders_history_raw) GROUP BY a ORDER BY a",
    );
    let pages: Vec<&str> = per_account
        .iter()
        .map(|s| s.split_once(':').expect("row").1)
        .collect();
    assert_eq!(per_account.len(), 3, "{per_account:?}");
    assert!(pages.windows(2).all(|w| w[0] == w[1]), "{per_account:?}");
    assert_eq!(
        strings(
            &c,
            "SELECT response_json FROM lake.br.aries_positions_raw WHERE request_key LIKE '%/BO001/%'"
        ),
        [
            r#"{"positions":[{"accountId":"BO001","symbol":"AAPL","quantity":"10","todayPl":"1.20"}]}"#
        ]
    );
    drop(c);

    for (needle, in_seed) in [
        ("cs-alice-aries-ex", 1),
        ("cs-bob-aries-ex", 1),
        ("at-alice-aries-ex", 0),
    ] {
        assert!(!stderr.contains(needle), "{needle} in logs");
        assert_eq!(
            files_containing(lake.dir.path(), needle.as_bytes()),
            Vec::<String>::new()
        );
        assert_eq!(
            files_containing(dir.path(), needle.as_bytes()).len(),
            in_seed,
            "{needle} outside the seed (report?)"
        );
    }
}
