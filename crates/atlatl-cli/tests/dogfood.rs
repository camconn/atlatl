//! M3.12 dogfood: a windowed/queued API and an HMAC-signed per-user API (`tests/fixtures/dogfood`),
//! replayed from the committed cassettes with no network (see `dogfood_support`).
//! `record_cassettes` (ignored) re-records them against the in-process emulators below; nothing
//! here talks to a real vendor.

mod dogfood_support;

use std::process::Output;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use base64::Engine as _;
use dogfood_support::{
    Handler, Proj, Req, Resp, commit_cassettes, fixture, in_range, out, serve, stream, ty,
};
use hmac::{Hmac, KeyInit, Mac};
use serde_json::Value;

const WINDOWED_BASE: &str = "https://api.windowed.example";
const SIGNED_BASE: &str = "https://api.signed.example";

// Recording credentials; the emulators accept only these.
const WD_KEY: &str = "wd-dogfood-live-7Qx2Lk";
const SG_CLIENT: &str = "DOGFOOD-CLIENT-5KXW";
const SG_CONSUMER: &str = "sg-dogfood-consumer-9Zr4Tb";
const USERS: [(&str, &str); 2] = [
    ("dogfood-user-a", "us-dogfood-secret-a-3141"),
    ("dogfood-user-b", "us-dogfood-secret-b-2718"),
];
// Replay credentials: different, so a replay provably needs none of the recording's.
const REPLAY_WD_KEY: &str = "wd-replay-dummy-key-51";
const REPLAY_SG_CLIENT: &str = "REPLAY-DUMMY-CLIENT-52";
const REPLAY_SG_CONSUMER: &str = "sg-replay-dummy-consumer-53";
const REPLAY_USER_SECRETS: [&str; 2] = ["us-replay-dummy-a-54", "us-replay-dummy-b-55"];

const ENV: &[&str] = &[
    "WINDOWED_API_KEY",
    "SIGNED_CLIENT_ID",
    "SIGNED_CONSUMER_KEY",
];

fn proj(secrets: &[(&'static str, &str)]) -> Proj {
    let mut p = Proj::new(&fixture("dogfood"), ENV, secrets);
    p.forbidden = USERS
        .iter()
        .map(|u| u.1)
        .chain(REPLAY_USER_SECRETS)
        .map(str::to_owned)
        .collect();
    p
}

fn replay() -> Proj {
    proj(&[
        ("WINDOWED_API_KEY", REPLAY_WD_KEY),
        ("SIGNED_CLIENT_ID", REPLAY_SG_CLIENT),
        ("SIGNED_CONSUMER_KEY", REPLAY_SG_CONSUMER),
    ])
}

fn seed_users(p: &Proj, secrets: [&str; 2]) {
    let c = p.lake.open("lake").expect("open");
    c.execute_batch(&format!(
        "CREATE SCHEMA IF NOT EXISTS lake.app; CREATE OR REPLACE TABLE lake.app.signed_users AS SELECT * FROM (VALUES ('{}', '{}'), ('{}', '{}')) t(user_id, user_secret);",
        USERS[0].0, secrets[0], USERS[1].0, secrets[1]
    ))
    .expect("seed users");
}

// ----------------------------------------------------------------------------- api test cases

struct Case {
    conn: &'static str,
    endpoint: &'static str,
    args: Vec<String>,
    records: u64,
}

fn user_item(user: usize, secret: &str) -> String {
    format!(
        r#"{{"user_id": "{}", "user_secret": "{secret}"}}"#,
        USERS[user].0
    )
}

fn account_item(account: &str, user: usize, secret: &str) -> String {
    format!(
        r#"{{"account_id": "{account}", "user_id": "{}", "user_secret": "{secret}"}}"#,
        USERS[user].0
    )
}

/// One cassette per endpoint, named after it. `secrets` are the per-user secrets passed as items.
fn cases(secrets: [&str; 2]) -> Vec<Case> {
    let s = |v: &[&str]| v.iter().map(|x| (*x).to_owned()).collect::<Vec<_>>();
    let ts = ["--ignore-param", "timestamp"];
    vec![
        Case {
            conn: "windowed",
            endpoint: "symbols",
            args: vec![],
            records: 3,
        },
        Case {
            conn: "windowed",
            endpoint: "profile",
            args: s(&["--item", "\"ACME\"", "--item", "\"ZZZZ\""]),
            records: 1,
        },
        Case {
            conn: "windowed",
            endpoint: "events",
            args: s(&["--var", "as_of=2026-09-28", "--items", "20"]),
            records: 8,
        },
        Case {
            conn: "windowed",
            endpoint: "news",
            args: s(&["--var", "as_of=2026-09-28", "--var", "news_from=2026-09-21"]),
            records: 5,
        },
        Case {
            conn: "signed",
            endpoint: "users",
            args: s(&ts),
            records: 2,
        },
        Case {
            conn: "signed",
            endpoint: "accounts",
            args: [
                s(&ts),
                vec![
                    "--item".into(),
                    user_item(0, secrets[0]),
                    "--item".into(),
                    user_item(1, secrets[1]),
                ],
            ]
            .concat(),
            records: 3,
        },
        Case {
            conn: "signed",
            endpoint: "positions",
            args: [
                s(&ts),
                vec!["--item".into(), account_item("acc-a1", 0, secrets[0])],
            ]
            .concat(),
            records: 2,
        },
        Case {
            conn: "signed",
            endpoint: "activities",
            args: [
                s(&ts),
                vec!["--item".into(), account_item("acc-a1", 0, secrets[0])],
                s(&[
                    "--var",
                    "start_date=2026-09-01",
                    "--var",
                    "end_date=2026-09-28",
                ]),
            ]
            .concat(),
            records: 4,
        },
    ]
}

fn api_test(p: &Proj, c: &Case, mode: &str) -> Output {
    let mut args = vec![
        "api", "test", c.conn, c.endpoint, mode, c.endpoint, "--json",
    ];
    args.extend(c.args.iter().map(String::as_str));
    p.run(&args)
}

#[test]
fn api_test_replays_every_endpoint_offline_and_redacted() {
    let p = replay();
    let dummies: Vec<&str> = [REPLAY_WD_KEY, REPLAY_SG_CLIENT, REPLAY_SG_CONSUMER]
        .into_iter()
        .chain(REPLAY_USER_SECRETS)
        .collect();
    for c in cases(REPLAY_USER_SECRETS) {
        let o = api_test(&p, &c, "--replay");
        let ctx = format!("{} {}: {}", c.conn, c.endpoint, out(&o));
        assert!(o.status.success(), "{ctx}");
        let stdout = String::from_utf8_lossy(&o.stdout);
        for d in &dummies {
            assert!(!stdout.contains(d), "{ctx}");
        }
        let v: Value = serde_json::from_str(&stdout).expect("json");
        assert!(v["error"].is_null(), "{ctx}");
        assert_eq!(v["record_count"], c.records, "{ctx}");
        let urls: Vec<&str> = v["interactions"]
            .as_array()
            .expect("interactions")
            .iter()
            .map(|i| i["request"]["url"].as_str().expect("url"))
            .collect();
        assert!(!urls.is_empty(), "{ctx}");
        for (i, u) in urls.iter().enumerate() {
            match c.conn {
                "windowed" => assert!(
                    u.starts_with("https://api.windowed.example/api/v1/")
                        && u.contains("token=****"),
                    "{u}"
                ),
                _ => {
                    assert!(
                        u.starts_with("https://api.signed.example/api/v1/")
                            && u.contains("clientId=****")
                            && u.contains("timestamp="),
                        "{u}"
                    );
                    if c.endpoint != "users" {
                        assert!(u.contains("userSecret=****"), "{u}");
                    }
                }
            }
            let headers = &v["interactions"][i]["request"]["headers"];
            assert!(
                headers
                    .as_array()
                    .is_none_or(|h| h.iter().all(|x| x[0] != "signature")),
                "signatures are not recorded: {headers}"
            );
        }
        let recs = &v["records"];
        match c.endpoint {
            "symbols" => {
                let syms: Vec<&str> = recs
                    .as_array()
                    .expect("r")
                    .iter()
                    .map(|r| r["record"]["symbol"].as_str().expect("s"))
                    .collect();
                assert_eq!(syms, ["ACME", "BOLT", "ZZZZ"], "funds are filtered out");
            }
            "profile" => assert_eq!(
                recs[0]["record"]["ticker"], "ACME",
                "`{{}}` for ZZZZ is dropped"
            ),
            "events" => {
                let embr = recs
                    .as_array()
                    .expect("r")
                    .iter()
                    .find(|r| r["record"]["symbol"] == "EMBR")
                    .expect("EMBR");
                assert_eq!(embr["record"]["year"], 2026);
            }
            "news" => assert!(
                recs.as_array()
                    .expect("r")
                    .iter()
                    .all(|r| r["record"]["symbol"] == r["record"]["related"])
            ),
            "users" => assert_eq!(recs[0]["record"]["value"], USERS[0].0),
            "accounts" => {
                let a: Vec<(&str, &str)> = recs
                    .as_array()
                    .expect("r")
                    .iter()
                    .map(|r| {
                        (
                            r["record"]["id"].as_str().expect("id"),
                            r["record"]["user_id"].as_str().expect("u"),
                        )
                    })
                    .collect();
                assert_eq!(
                    a,
                    [
                        ("acc-a1", USERS[0].0),
                        ("acc-a2", USERS[0].0),
                        ("acc-b1", USERS[1].0)
                    ]
                );
                assert!(
                    !stdout.contains("user_secret"),
                    "secret item fields are never records"
                );
            }
            "positions" => assert_eq!(recs[0]["record"]["symbol"], "ACME"),
            "activities" => {
                assert_eq!(
                    v["interactions"].as_array().map(Vec::len),
                    Some(2),
                    "two offset pages"
                );
                assert!(
                    urls[1].contains("offset=2") && urls[1].contains("limit=2"),
                    "{}",
                    urls[1]
                );
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn api_explain_is_redacted() {
    // Real-looking values in the environment: explain never reads them, and prints templates only.
    let p = proj(&[
        ("WINDOWED_API_KEY", WD_KEY),
        ("SIGNED_CLIENT_ID", SG_CLIENT),
        ("SIGNED_CONSUMER_KEY", SG_CONSUMER),
    ]);
    for args in [
        vec!["api", "explain", "signed", "--json"],
        vec!["api", "explain", "signed", "accounts", "--json"],
        vec!["api", "explain", "signed", "activities"],
        vec!["api", "explain", "windowed", "news"],
        vec!["api", "explain", "apis/windowed.yaml", "--json"],
    ] {
        let o = p.run(&args);
        assert!(o.status.success(), "{args:?}: {}", out(&o));
        let s = String::from_utf8_lossy(&o.stdout);
        for secret in [WD_KEY, SG_CLIENT, SG_CONSUMER] {
            assert!(!s.contains(secret), "{args:?}: {s}");
        }
    }
    let o = p.run(&["api", "explain", "signed", "accounts", "--json"]);
    let v: Value = serde_json::from_slice(&o.stdout).expect("json");
    let auth = &v["auth"];
    assert_eq!(auth["type"], "hmac");
    assert_eq!(auth["key"], "{{ secrets.consumer_key }}");
    assert_eq!(auth["placement"]["header"], "Signature");
    assert_eq!(auth["params"]["clientId"], "{{ secrets.client_id }}");
    let ep = &v["endpoint"];
    assert_eq!(ep["for_each"]["secret"], serde_json::json!(["user_secret"]));
    assert_eq!(
        ep["request"]["params"]["userSecret"],
        "{{ secrets.item.user_secret }}"
    );
    let o = p.run(&["api", "explain", "windowed", "--json"]);
    let v: Value = serde_json::from_slice(&o.stdout).expect("json");
    assert_eq!(v["auth"]["params"]["token"], "{{ secrets.api_key }}");
}

// ----------------------------------------------------------------------------- atlatl run

fn assert_windowed(p: &Proj, report: &Value, run: u32) {
    assert_eq!(report["status"], "succeeded", "{report}");
    assert_eq!(p.count("br.symbols"), 3);
    assert_eq!(p.count("br.profile"), 2);
    let prof = p.types("br.profile");
    assert_eq!(ty(&prof, "ipo"), "DATE");
    assert!(
        ty(&prof, "marketCapitalization").starts_with("DECIMAL"),
        "{prof:?}"
    );
    assert_eq!(
        p.q("SELECT marketCapitalization::VARCHAR AS m FROM lake.br.profile WHERE ticker = 'ACME'"),
        [r#"{"m":"3423041.50"}"#],
        "exact decimals"
    );
    let earn = p.types("br.events");
    assert_eq!(ty(&earn, "date"), "DATE");
    assert_eq!(ty(&earn, "year"), "BIGINT");
    assert!(ty(&earn, "epsEstimate").starts_with("DECIMAL"), "{earn:?}");
    let news = p.types("br.news");
    assert_eq!(ty(&news, "datetime"), "BIGINT");
    assert_eq!(ty(&news, "symbol"), "VARCHAR");
    assert_eq!(p.sync("windowed/events")["floor"], "2026-08-24T00:00:00Z");
    let windows = stream(report, "windowed/events")["members"][0]["http"]["requests"].as_u64();
    if run == 1 {
        assert_eq!(p.count("br.events"), 8);
        assert_eq!(windows, Some(9), "weekly windows from as_of - 1 month");
        assert_eq!(p.count("br.news"), 5);
        assert_eq!(p.sync("windowed/news")["since"], 1790423100_i64);
        assert_eq!(
            p.q("SELECT epsActual FROM lake.br.events WHERE symbol = 'FERN'"),
            [r#"{"epsActual":null}"#]
        );
    } else {
        assert_eq!(
            p.count("br.events"),
            9,
            "CRUX is inside the window from the stored floor"
        );
        assert_eq!(windows, Some(10), "weekly windows from the stored floor");
        assert_eq!(
            p.q("SELECT epsActual::VARCHAR AS e FROM lake.br.events WHERE symbol = 'FERN'"),
            [r#"{"e":"3.72"}"#],
            "FERN reported: merged by key"
        );
        assert_eq!(p.count("br.news"), 7);
        assert_eq!(p.sync("windowed/news")["since"], 1790586000_i64);
    }
}

fn assert_signed(p: &Proj, report: &Value, run: u32) {
    assert_eq!(report["status"], "succeeded", "{report}");
    assert_eq!(p.count("br.users"), 2);
    assert_eq!(p.count("br.accounts"), 3);
    assert_eq!(p.count("br.positions"), 4);
    assert_eq!(
        p.q("SELECT count(*) AS n FROM information_schema.columns WHERE table_catalog = 'lake' AND column_name ILIKE '%secret%'"),
        [r#"{"n":1}"#],
        "only the seeded users table holds a secret column"
    );
    let pos = p.types("br.positions");
    assert!(ty(&pos, "units").starts_with("DECIMAL"), "{pos:?}");
    assert_eq!(ty(&pos, "account_id"), "VARCHAR");
    let act = p.types("br.activities");
    assert_eq!(ty(&act, "trade_date"), "TIMESTAMP WITH TIME ZONE");
    assert!(ty(&act, "amount").starts_with("DECIMAL"), "{act:?}");
    let reqs = stream(report, "signed/activities")["members"][0]["http"]["requests"].as_u64();
    if run == 1 {
        assert_eq!(p.count("br.activities"), 7);
        assert_eq!(reqs, Some(4), "acc-a1 takes two offset pages");
        assert_eq!(p.sync("signed/activities")["since"], "2026-09-22T14:30:00Z");
    } else {
        assert_eq!(p.count("br.activities"), 9);
        assert_eq!(reqs, Some(3));
        assert_eq!(p.sync("signed/activities")["since"], "2026-09-25T00:00:00Z");
    }
}

#[test]
fn run_replays_windowed_into_a_local_lake() {
    let p = replay();
    let r = p.lake_run("windowed", ("ATLATL_CASSETTE", "run1"));
    assert_windowed(&p, &r, 1);
    let r = p.lake_run("windowed", ("ATLATL_CASSETTE", "run2"));
    assert_windowed(&p, &r, 2);
}

#[test]
fn run_replays_signed_into_a_local_lake() {
    let p = replay();
    seed_users(&p, REPLAY_USER_SECRETS);
    let r = p.lake_run("signed", ("ATLATL_CASSETTE", "run1"));
    assert_signed(&p, &r, 1);
    let r = p.lake_run("signed", ("ATLATL_CASSETTE", "run2"));
    assert_signed(&p, &r, 2);
}

#[test]
fn run_fails_on_a_cassette_miss_instead_of_going_live() {
    let p = replay();
    seed_users(&p, REPLAY_USER_SECRETS);
    // run2's requests start from the stored sync state; on a fresh lake they don't match.
    let report = p.path().join("miss.json");
    let o = p
        .cmd(&[
            "--report",
            report.to_str().expect("utf8"),
            "run",
            "--replication",
            "signed",
        ])
        .env("ATLATL_CASSETTE", "run2")
        .output()
        .expect("run");
    assert!(!o.status.success(), "{}", out(&o));
    let text = std::fs::read_to_string(&report).expect("report");
    assert!(text.contains("cassette_miss"), "{text}");
}

// ----------------------------------------------------------------------------- vendor emulators

fn json_handler(f: impl Fn(&Req) -> (u16, String) + Send + Sync + 'static) -> Arc<Handler> {
    Arc::new(move |r| Resp::from(f(r)))
}

/// The windowed API, keyed by `token`. `phase` 2 is "a few days later".
fn windowed(phase: Arc<AtomicU32>) -> Arc<Handler> {
    json_handler(move |r: &Req| {
        if r.param("token").as_deref() != Some(WD_KEY) {
            return (401, r#"{"error":"Invalid API key"}"#.into());
        }
        let p2 = phase.load(Ordering::SeqCst) >= 2;
        match r.path.as_str() {
            "/api/v1/symbols" if r.param("venue").as_deref() == Some("XA") => (200, format!("[{}]", [
                r#"{"currency":"USD","description":"ACME CORP","displaySymbol":"ACME","uid":"UID00000ACM1","mic":"XAXA","symbol":"ACME","symbol2":"","type":"Common"}"#,
                r#"{"currency":"USD","description":"INDEX FUND TRUST","displaySymbol":"IDXF","uid":"UID00000IDX1","mic":"XAXB","symbol":"IDXF","symbol2":"","type":"Fund"}"#,
                r#"{"currency":"USD","description":"BOLT INDUSTRIES","displaySymbol":"BOLT","uid":"UID00000BLT1","mic":"XAXA","symbol":"BOLT","symbol2":"","type":"Common"}"#,
                r#"{"currency":"USD","description":"DOGFOOD HOLDINGS","displaySymbol":"ZZZZ","uid":"UID00000ZZZ1","mic":"XAXC","symbol":"ZZZZ","symbol2":"","type":"Common"}"#,
            ].join(","))),
            "/api/v1/profile" => match r.param("symbol").as_deref() {
                Some("ACME") => (200, r#"{"country":"XA","currency":"USD","estimateCurrency":"USD","exchange":"XA MAIN BOARD","industry":"Technology","ipo":"1980-12-12","logo":"https://static.example.test/logo/ACME.png","marketCapitalization":3423041.50,"name":"Acme Corp","phone":"15550100100","shareOutstanding":14840.39,"ticker":"ACME","weburl":"https://acme.example.test/"}"#.into()),
                Some("BOLT") => (200, r#"{"country":"XA","currency":"USD","estimateCurrency":"USD","exchange":"XA MAIN BOARD","industry":"Industrials","ipo":"1986-03-13","logo":"https://static.example.test/logo/BOLT.png","marketCapitalization":3780512.25,"name":"Bolt Industries","phone":"15550100200","shareOutstanding":7433.98,"ticker":"BOLT","weburl":"https://bolt.example.test/"}"#.into()),
                _ => (200, "{}".into()),
            },
            "/api/v1/calendar/events" => {
                let (from, to) = (r.param("from").unwrap_or_default(), r.param("to").unwrap_or_default());
                let fdx = if p2 { r#""epsActual":3.72,"epsEstimate":3.60,"hour":"amc","quarter":1,"revenueActual":22300000000"# } else { r#""epsActual":null,"epsEstimate":3.60,"hour":"amc","quarter":1,"revenueActual":null"# };
                let all = [
                    ("2026-08-27", r#"{"date":"2026-08-27","epsActual":1.95,"epsEstimate":1.83,"hour":"amc","quarter":3,"revenueActual":15952000000,"revenueEstimate":15840000000,"symbol":"CRUX","year":2026}"#.to_owned()),
                    ("2026-09-09", r#"{"date":"2026-09-09","epsActual":1.47,"epsEstimate":1.48,"hour":"amc","quarter":1,"revenueActual":14926000000,"revenueEstimate":15039000000,"symbol":"DYNE","year":2027}"#.to_owned()),
                    ("2026-09-10", r#"{"date":"2026-09-10","epsActual":5.31,"epsEstimate":5.10,"hour":"amc","quarter":3,"revenueActual":5990000000,"revenueEstimate":5910000000,"symbol":"EMBR","year":2026}"#.to_owned()),
                    ("2026-09-22", format!(r#"{{"date":"2026-09-22",{fdx},"revenueEstimate":22100000000,"symbol":"FERN","year":2027}}"#)),
                    ("2026-09-29", r#"{"date":"2026-09-29","epsActual":null,"epsEstimate":0.27,"hour":"amc","quarter":1,"revenueActual":null,"revenueEstimate":11650000000,"symbol":"GLYF","year":2027}"#.to_owned()),
                    ("2026-10-13", r#"{"date":"2026-10-13","epsActual":null,"epsEstimate":4.85,"hour":"bmo","quarter":3,"revenueActual":null,"revenueEstimate":44200000000,"symbol":"HALO","year":2026}"#.to_owned()),
                    ("2026-10-15", r#"{"date":"2026-10-15","epsActual":null,"epsEstimate":6.97,"hour":"amc","quarter":3,"revenueActual":null,"revenueEstimate":11500000000,"symbol":"IRIS","year":2026}"#.to_owned()),
                    ("2026-10-27", r#"{"date":"2026-10-27","epsActual":null,"epsEstimate":3.10,"hour":"amc","quarter":1,"revenueActual":null,"revenueEstimate":69400000000,"symbol":"BOLT","year":2027}"#.to_owned()),
                    ("2026-10-29", r#"{"date":"2026-10-29","epsActual":null,"epsEstimate":1.75,"hour":"amc","quarter":4,"revenueActual":null,"revenueEstimate":101200000000,"symbol":"ACME","year":2026}"#.to_owned()),
                ];
                let hits: Vec<String> = all.into_iter().filter(|(d, _)| in_range(d, &from, &to)).map(|(_, j)| j).collect();
                (200, format!(r#"{{"events":[{}]}}"#, hits.join(",")))
            }
            "/api/v1/news" => {
                let (sym, from, to) = (r.param("symbol").unwrap_or_default(), r.param("from").unwrap_or_default(), r.param("to").unwrap_or_default());
                let mut all = vec![
                    ("ACME", "2026-09-22", 1790082000_i64, 131000001),
                    ("BOLT", "2026-09-23", 1790177400, 131000002),
                    ("ACME", "2026-09-24", 1790241300, 131000003),
                    ("ACME", "2026-09-25", 1790366400, 131000004),
                    ("BOLT", "2026-09-26", 1790423100, 131000005),
                ];
                if p2 {
                    all.push(("ACME", "2026-09-27", 1790496000, 131000006));
                    all.push(("BOLT", "2026-09-28", 1790586000, 131000007));
                }
                // Newest first.
                let hits: Vec<String> = all
                    .into_iter()
                    .rev()
                    .filter(|(s, d, _, _)| *s == sym && in_range(d, &from, &to))
                    .map(|(s, _, t, id)| format!(r#"{{"category":"company","datetime":{t},"headline":"{s} dogfood headline {id}","id":{id},"image":"","related":"{s}","source":"Dogfood Wire","summary":"Synthetic article {id}.","url":"https://news.example.test/{id}"}}"#))
                    .collect();
                (200, format!("[{}]", hits.join(",")))
            }
            _ => (404, r#"{"error":"not found"}"#.into()),
        }
    })
}

/// The signed API's request signature: base64 HMAC-SHA256 (consumer key) of the sorted-key compact JSON
/// `{"content": <body or null>, "path": <path>, "query": <query string as sent>}`.
fn signature(key: &str, path: &str, query: &str, body: &[u8]) -> String {
    fn sorted(v: &Value) -> String {
        match v {
            Value::Object(m) => {
                let mut keys: Vec<&String> = m.keys().collect();
                keys.sort();
                let parts: Vec<String> = keys
                    .iter()
                    .map(|k| format!("{}:{}", Value::String((*k).clone()), sorted(&m[*k])))
                    .collect();
                format!("{{{}}}", parts.join(","))
            }
            Value::Array(a) => format!("[{}]", a.iter().map(sorted).collect::<Vec<_>>().join(",")),
            other => other.to_string(),
        }
    }
    let content = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(body).unwrap_or(Value::Null)
    };
    let canonical = format!(
        r#"{{"content":{},"path":{},"query":{}}}"#,
        sorted(&content),
        Value::String(path.to_owned()),
        Value::String(query.to_owned())
    );
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(key.as_bytes()).expect("key");
    mac.update(canonical.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

/// `(units, price, amount)` as JSON number text.
fn activity(
    id: &str,
    sym: Option<(&str, &str)>,
    ty: &str,
    (units, price, amount): (&str, &str, &str),
    trade: &str,
    inst: &str,
) -> (String, String) {
    let symbol = match sym {
        Some((s, ccy)) => format!(
            r#"{{"id":"sym-{s}","symbol":"{s}","raw_symbol":"{s}","description":"{s} dogfood","currency":{{"id":"ccy-{ccy}","code":"{ccy}","name":"{ccy}"}}}}"#
        ),
        None => "null".into(),
    };
    let ccy = sym.map(|s| s.1).unwrap_or("USD");
    (
        trade[..10].to_owned(),
        format!(
            r#"{{"id":"{id}","symbol":{symbol},"option_symbol":null,"price":{price},"units":{units},"amount":{amount},"currency":{{"id":"ccy-{ccy}","code":"{ccy}","name":"{ccy}"}},"type":"{ty}","option_type":"","description":"{ty} dogfood","trade_date":"{trade}","settlement_date":"{}T00:00:00Z","fee":0.00,"fx_rate":null,"institution":"{inst}","external_reference_id":"ext-{id}"}}"#,
            &trade[..10]
        ),
    )
}

fn position(
    sym: &str,
    desc: &str,
    ccy: &str,
    units: &str,
    price: &str,
    avg: &str,
    pnl: &str,
) -> String {
    format!(
        r#"{{"symbol":{{"symbol":{{"id":"sym-{sym}","symbol":"{sym}","raw_symbol":"{sym}","description":"{desc}","currency":{{"id":"ccy-{ccy}","code":"{ccy}","name":"{ccy}"}},"type":{{"code":"cs","description":"Common Stock"}}}},"id":"usym-{sym}","description":"{desc}","is_quotable":true,"is_tradable":true}},"units":{units},"price":{price},"open_pnl":{pnl},"fractional_units":null,"average_purchase_price":{avg},"currency":{{"id":"ccy-{ccy}","code":"{ccy}","name":"{ccy}"}},"cash_equivalent":false}}"#
    )
}

fn account(id: &str, user: &str, name: &str, inst: &str, amount: &str, ccy: &str) -> String {
    format!(
        r#"{{"id":"{id}","brokerage_authorization":"auth-{user}","portfolio_group":"pg-{id}","name":"{name}","number":"NO-{id}","institution_name":"{inst}","created_date":"2025-01-15T18:04:11.123Z","balance":{{"total":{{"amount":{amount},"currency":"{ccy}"}}}},"meta":{{"type":"INDIVIDUAL","status":"ACTIVE","institution_name":"{inst}"}},"is_paper":false}}"#
    )
}

/// The signed API: verifies `clientId`, a fresh `timestamp` and the `Signature` (401 otherwise),
/// then the user's `userSecret` for per-user endpoints.
fn signed(phase: Arc<AtomicU32>) -> Arc<Handler> {
    json_handler(move |r: &Req| {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs() as i64;
        let fresh = r
            .param("timestamp")
            .and_then(|t| t.parse::<i64>().ok())
            .is_some_and(|t| (now - t).abs() <= 300);
        let sig = signature(SG_CONSUMER, &r.path, &r.query, &r.body);
        if r.param("clientId").as_deref() != Some(SG_CLIENT)
            || !fresh
            || r.header("Signature") != Some(sig.as_str())
        {
            return (
                401,
                r#"{"detail":"Unable to verify signature sent","status_code":401}"#.into(),
            );
        }
        if r.path == "/api/v1/users/list" {
            return (200, format!(r#"["{}","{}"]"#, USERS[0].0, USERS[1].0));
        }
        let user = r.param("userId").unwrap_or_default();
        let known = USERS.iter().find(|(u, _)| *u == user);
        if known.is_none_or(|(_, s)| r.param("userSecret").as_deref() != Some(*s)) {
            return (
                401,
                r#"{"detail":"Invalid userID or userSecret provided","status_code":401}"#.into(),
            );
        }
        let p2 = phase.load(Ordering::SeqCst) >= 2;
        let accounts: Vec<(&str, &str, String)> = vec![
            (
                "acc-a1",
                USERS[0].0,
                account(
                    "acc-a1",
                    USERS[0].0,
                    "Broker One Individual",
                    "Broker One",
                    "15363.23",
                    "USD",
                ),
            ),
            (
                "acc-a2",
                USERS[0].0,
                account(
                    "acc-a2",
                    USERS[0].0,
                    "Broker Two Savings",
                    "Broker Two",
                    "8120.50",
                    "CAD",
                ),
            ),
            (
                "acc-b1",
                USERS[1].0,
                account(
                    "acc-b1",
                    USERS[1].0,
                    "Broker Three Personal",
                    "Broker Three",
                    "3412.00",
                    "CAD",
                ),
            ),
        ];
        if r.path == "/api/v1/accounts" {
            let mine: Vec<&str> = accounts
                .iter()
                .filter(|a| a.1 == user)
                .map(|a| a.2.as_str())
                .collect();
            return (200, format!("[{}]", mine.join(",")));
        }
        let rest = r.path.strip_prefix("/api/v1/accounts/").unwrap_or_default();
        let (acc, what) = rest.split_once('/').unwrap_or((rest, ""));
        if !accounts.iter().any(|a| a.0 == acc && a.1 == user) {
            return (
                404,
                r#"{"detail":"Account not found","status_code":404}"#.into(),
            );
        }
        match what {
            "positions" => {
                let ps = match acc {
                    "acc-a1" => vec![
                        position(
                            "ACME",
                            "Acme Corp",
                            "USD",
                            "10",
                            "227.52",
                            "180.10",
                            "474.20",
                        ),
                        position(
                            "IDXT",
                            "Total Market Fund",
                            "USD",
                            "3.5",
                            "285.40",
                            "250.00",
                            "123.90",
                        ),
                    ],
                    "acc-a2" => vec![position(
                        "MAPL.XB",
                        "Maple Commerce",
                        "CAD",
                        "20",
                        "112.30",
                        "98.75",
                        "271.00",
                    )],
                    _ => vec![position(
                        "EQTY.XB",
                        "Core Equity Fund",
                        "CAD",
                        "100",
                        "34.12",
                        "30.50",
                        "362.00",
                    )],
                };
                (200, format!("[{}]", ps.join(",")))
            }
            "activities" => {
                let mut all = match acc {
                    "acc-a1" => vec![
                        activity(
                            "act-a1-1",
                            Some(("ACME", "USD")),
                            "BUY",
                            ("2", "227.52", "-455.04"),
                            "2026-09-02T14:30:00Z",
                            "Broker One",
                        ),
                        activity(
                            "act-a1-2",
                            Some(("ACME", "USD")),
                            "DIVIDEND",
                            ("0", "0", "2.60"),
                            "2026-09-12T00:00:00Z",
                            "Broker One",
                        ),
                        activity(
                            "act-a1-3",
                            Some(("IDXT", "USD")),
                            "BUY",
                            ("1.5", "285.40", "-428.10"),
                            "2026-09-15T15:05:00Z",
                            "Broker One",
                        ),
                        activity(
                            "act-a1-4",
                            Some(("ACME", "USD")),
                            "SELL",
                            ("-1", "231.10", "231.10"),
                            "2026-09-22T14:30:00Z",
                            "Broker One",
                        ),
                    ],
                    "acc-a2" => vec![
                        activity(
                            "act-a2-1",
                            None,
                            "CONTRIBUTION",
                            ("0", "0", "1000.00"),
                            "2026-09-05T00:00:00Z",
                            "Broker Two",
                        ),
                        activity(
                            "act-a2-2",
                            Some(("MAPL.XB", "CAD")),
                            "BUY",
                            ("5", "110.05", "-550.25"),
                            "2026-09-08T13:45:00Z",
                            "Broker Two",
                        ),
                    ],
                    _ => vec![activity(
                        "act-b1-1",
                        Some(("EQTY.XB", "CAD")),
                        "BUY",
                        ("10", "33.90", "-339.00"),
                        "2026-09-18T16:00:00Z",
                        "Broker Three",
                    )],
                };
                if p2 {
                    match acc {
                        "acc-a1" => all.push(activity(
                            "act-a1-5",
                            None,
                            "FEE",
                            ("0", "0", "-4.99"),
                            "2026-09-25T00:00:00Z",
                            "Broker One",
                        )),
                        "acc-b1" => all.push(activity(
                            "act-b1-2",
                            Some(("EQTY.XB", "CAD")),
                            "BUY",
                            ("10", "34.02", "-340.20"),
                            "2026-09-24T16:00:00Z",
                            "Broker Three",
                        )),
                        _ => {}
                    }
                }
                let (from, to) = (
                    r.param("startDate").unwrap_or_default(),
                    r.param("endDate").unwrap_or_default(),
                );
                let hits: Vec<String> = all
                    .into_iter()
                    .filter(|(d, _)| in_range(d, &from, &to))
                    .map(|(_, j)| j)
                    .collect();
                let offset: usize = r.param("offset").and_then(|o| o.parse().ok()).unwrap_or(0);
                let limit: usize = r
                    .param("limit")
                    .and_then(|o| o.parse().ok())
                    .unwrap_or(1000);
                let page: Vec<&str> = hits
                    .iter()
                    .skip(offset)
                    .take(limit)
                    .map(String::as_str)
                    .collect();
                (
                    200,
                    format!(
                        r#"{{"data":[{}],"pagination":{{"offset":{offset},"limit":{limit},"total":{}}}}}"#,
                        page.join(","),
                        hits.len()
                    ),
                )
            }
            _ => (404, r#"{"detail":"Not found","status_code":404}"#.into()),
        }
    })
}

// ----------------------------------------------------------------------------- recording

fn recording_proj(windowed_url: &str, signed_url: &str, consumer_key: &str) -> Proj {
    let p = proj(&[
        ("WINDOWED_API_KEY", WD_KEY),
        ("SIGNED_CLIENT_ID", SG_CLIENT),
        ("SIGNED_CONSUMER_KEY", consumer_key),
    ]);
    p.retarget("apis/windowed.yaml", WINDOWED_BASE, windowed_url);
    p.retarget("apis/signed.yaml", SIGNED_BASE, signed_url);
    p
}

/// Re-records every cassette under `tests/fixtures/dogfood/apis/*/cassettes/` against the
/// emulators (`cargo test -p atlatl-cli --test dogfood -- --ignored record_cassettes`).
#[test]
#[ignore = "re-records the committed cassettes"]
fn record_cassettes() {
    let phase = Arc::new(AtomicU32::new(1));
    let (wd_url, wd_log) = serve(windowed(phase.clone()));
    let (sg_url, sg_log) = serve(signed(phase.clone()));

    // The emulator really checks signatures and user secrets.
    let bad = recording_proj(&wd_url, &sg_url, "sg-wrong-consumer-key");
    let o = bad.run(&["api", "test", "signed", "users", "--json"]);
    assert!(!o.status.success(), "{}", out(&o));
    let v: Value = serde_json::from_slice(&o.stdout).expect("json");
    assert_eq!(v["interactions"][0]["response"]["status"], 401, "{v}");
    assert_eq!(v["error"]["class"], "auth", "{v}");
    let p = recording_proj(&wd_url, &sg_url, SG_CONSUMER);
    let o = p.run(&[
        "api",
        "test",
        "signed",
        "accounts",
        "--json",
        "--item",
        &user_item(0, "us-wrong-secret"),
    ]);
    assert!(!o.status.success(), "{}", out(&o));
    let v: Value = serde_json::from_slice(&o.stdout).expect("json");
    assert_eq!(v["interactions"][0]["response"]["status"], 401, "{v}");

    let secrets = [USERS[0].1, USERS[1].1];
    for c in cases(secrets) {
        let o = api_test(&p, &c, "--record");
        assert!(o.status.success(), "{} {}: {}", c.conn, c.endpoint, out(&o));
        let v: Value =
            serde_json::from_slice(&o.stdout).unwrap_or_else(|e| panic!("{e}: {}", out(&o)));
        assert_eq!(
            v["record_count"], c.records,
            "{} {}: {v}",
            c.conn, c.endpoint
        );
    }

    seed_users(&p, secrets);
    for (run, ph) in [("run1", 1), ("run2", 2)] {
        phase.store(ph, Ordering::SeqCst);
        let r = p.lake_run("windowed", ("ATLATL_CASSETTE_RECORD", run));
        assert_windowed(&p, &r, ph);
        let r = p.lake_run("signed", ("ATLATL_CASSETTE_RECORD", run));
        assert_signed(&p, &r, ph);
    }
    assert!(wd_log.lock().expect("log").len() > 20 && sg_log.lock().expect("log").len() > 10);

    let all_secrets = [WD_KEY, SG_CLIENT, SG_CONSUMER, USERS[0].1, USERS[1].1];
    for (api, url, base) in [
        ("windowed", &wd_url, WINDOWED_BASE),
        ("signed", &sg_url, SIGNED_BASE),
    ] {
        let n = commit_cassettes(
            &p.path().join("apis").join(api).join("cassettes"),
            &fixture("dogfood").join("apis").join(api).join("cassettes"),
            url,
            base,
            &all_secrets,
        );
        assert_eq!(n, 6, "{api}: one cassette per endpoint, plus run1 and run2");
    }
}
