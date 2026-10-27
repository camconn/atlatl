//! M8.1 ports (`tests/fixtures/dogfood/ports`): synthetic APIs in the shapes of the legacy REST
//! vendor ports, replayed from committed cassettes with no network (see `dogfood_support`):
//!
//! - single endpoints: `monthly` (no auth, browser headers, monthly `range()` windows, categories
//!   merged) and `queue_chain.listings` (cursor pages, nested objects flattened);
//! - queue chains: `queue_chain` (entities -> reports -> line_items), `record_queue` (whole records
//!   as items) and `paged_queue` (header auth, page numbers, a skip guard);
//! - `throttled`: quarterly windows, a setup fan-out, and plain-text 409 / "slow down" throttling.
//!
//! Every stream has a reviewed source schema (`schemas/`), checked on each replayed run. The specs
//! use `now()`; replays pin it to the cassette's `recorded_at`, so expected windows are derived
//! from that. `record_cassettes` (ignored) re-records against the emulators below.

mod dogfood_support;

use std::collections::HashSet;
use std::process::Output;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use dogfood_support::{
    Handler, Proj, Req, Resp, commit_cassettes, fixture, http, in_range, out, serve, stream,
};
use serde_json::{Value, json};

const APIS: [(&str, &str); 5] = [
    ("monthly", "https://api.monthly.example"),
    ("queue_chain", "https://api.queue-chain.example"),
    ("record_queue", "https://api.record-queue.example"),
    ("paged_queue", "https://api.paged-queue.example"),
    ("throttled", "https://api.throttled.example"),
];

const ENV: &[&str] = &[
    "QUEUE_CHAIN_API_KEY",
    "RECORD_QUEUE_API_KEY",
    "PAGED_QUEUE_API_KEY",
    "THROTTLED_API_KEY",
];
// Recording keys; the emulators accept only these. Replays use different ones.
const LIVE: [&str; 4] = [
    "qc-live-key-81Hq",
    "rq-live-key-72Jw",
    "pq-live-key-63Ke",
    "th-live-key-54Lr",
];
const REPLAY: [&str; 4] = [
    "qc-replay-dummy-11",
    "rq-replay-dummy-12",
    "pq-replay-dummy-13",
    "th-replay-dummy-14",
];

fn proj(keys: [&str; 4]) -> Proj {
    let secrets: Vec<(&'static str, &str)> = ENV.iter().copied().zip(keys).collect();
    Proj::new(&fixture("dogfood/ports"), ENV, &secrets)
}

fn replay() -> Proj {
    proj(REPLAY)
}

// ----------------------------------------------------------------------------- calendar math

/// `(year, month)` of a cassette's `recorded_at`: the pinned `now()` of its replay.
fn recorded_month(p: &Proj, api: &str, cassette: &str) -> (i32, u32) {
    let path = p
        .path()
        .join(format!("apis/{api}/cassettes/{cassette}.json"));
    let v: Value = serde_json::from_str(&std::fs::read_to_string(path).expect("cassette"))
        .expect("cassette json");
    let t = v["recorded_at"].as_str().expect("recorded_at");
    (
        t[..4].parse().expect("year"),
        t[5..7].parse().expect("month"),
    )
}

fn month_index(y: i32, m: u32) -> i32 {
    y * 12 + m as i32 - 1
}

fn from_index(i: i32) -> (i32, u32) {
    (i.div_euclid(12), i.rem_euclid(12) as u32 + 1)
}

/// Window starts on the 1st, every `step` months from `start` while `<= now + ahead` months.
fn windows(start: (i32, u32), now: (i32, u32), ahead: i32, step: i32) -> u64 {
    let span = month_index(now.0, now.1) + ahead - month_index(start.0, start.1);
    (span.div_euclid(step) + 1) as u64
}

fn first_of((y, m): (i32, u32)) -> String {
    format!("{y:04}-{m:02}-01T00:00:00Z")
}

/// Today (UTC), for placing the emulators' recent data.
fn today() -> (i32, u32, u32) {
    let days = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs()
        / 86_400) as i64;
    // Civil from days (H. Hinnant).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = (yoe + era * 400 + i64::from(m <= 2)) as i32;
    (y, m, d)
}

// ----------------------------------------------------------------------------- api test cases

struct Case {
    conn: &'static str,
    endpoint: &'static str,
    stream: &'static str,
    args: &'static [&'static str],
    records: u64,
    skipped: u64,
}

const CASES: &[Case] = &[
    Case {
        conn: "monthly",
        endpoint: "calendar",
        stream: "monthly/calendar",
        args: &["--items", "60"],
        records: 6,
        skipped: 0,
    },
    Case {
        conn: "queue_chain",
        endpoint: "listings",
        stream: "queue_chain/listings",
        args: &["--pages", "10"],
        records: 3,
        skipped: 0,
    },
    Case {
        conn: "queue_chain",
        endpoint: "entities",
        stream: "queue_chain/entities",
        args: &["--pages", "10"],
        records: 3,
        skipped: 0,
    },
    Case {
        conn: "queue_chain",
        endpoint: "reports",
        stream: "queue_chain/reports",
        args: &["--item", "\"ent_1\"", "--item", "\"ent_4\""],
        records: 2,
        skipped: 1,
    },
    Case {
        conn: "queue_chain",
        endpoint: "line_items",
        stream: "queue_chain/line_items",
        args: &["--item", "\"rep_11\"", "--item", "\"rep_31\""],
        records: 2,
        skipped: 1,
    },
    Case {
        conn: "record_queue",
        endpoint: "instruments",
        stream: "record_queue/instruments",
        args: &[],
        records: 3,
        skipped: 0,
    },
    Case {
        conn: "record_queue",
        endpoint: "ratings",
        stream: "record_queue/ratings",
        args: &[
            "--item",
            r#"{"code": "ACME", "uid": "UID0001"}"#,
            "--item",
            r#"{"code": "CRUX", "uid": "UID0003"}"#,
        ],
        records: 2,
        skipped: 1,
    },
    Case {
        conn: "paged_queue",
        endpoint: "orgs",
        stream: "paged_queue/orgs",
        args: &["--pages", "10"],
        records: 3,
        skipped: 0,
    },
    Case {
        conn: "paged_queue",
        endpoint: "rounds",
        stream: "paged_queue/rounds",
        args: &[
            "--items",
            "3",
            "--item",
            "\"org_a\"",
            "--item",
            "\"org_b\"",
            "--item",
            "\"org_c\"",
        ],
        records: 3,
        skipped: 1,
    },
    Case {
        conn: "throttled",
        endpoint: "releases",
        stream: "throttled/releases",
        args: &["--items", "60"],
        records: 6,
        skipped: 0,
    },
    Case {
        conn: "throttled",
        endpoint: "symbols",
        stream: "throttled/symbols",
        args: &["--items", "10"],
        records: 3,
        skipped: 0,
    },
];

fn api_test(p: &Proj, c: &Case, mode: &str) -> Output {
    let mut args = vec![
        "api", "test", c.conn, c.endpoint, mode, c.endpoint, "--json",
    ];
    args.extend(c.args);
    p.run(&args)
}

#[test]
fn api_test_replays_every_port_endpoint_offline_and_redacted() {
    let p = replay();
    for c in CASES {
        let o = api_test(&p, c, "--replay");
        let ctx = format!("{} {}: {}", c.conn, c.endpoint, out(&o));
        assert!(o.status.success(), "{ctx}");
        let stdout = String::from_utf8_lossy(&o.stdout);
        for k in REPLAY {
            assert!(!stdout.contains(k), "{ctx}");
        }
        let v: Value = serde_json::from_str(&stdout).expect("json");
        assert!(v["error"].is_null(), "{ctx}");
        assert_eq!(v["record_count"], c.records, "{ctx}");
        let base = APIS.iter().find(|a| a.0 == c.conn).expect("api").1;
        for i in v["interactions"].as_array().expect("interactions") {
            let u = i["request"]["url"].as_str().expect("url");
            assert!(u.starts_with(base), "{u}");
            assert!(
                !u.contains("key=") || u.contains("key=****"),
                "keys are masked: {u}"
            );
            let headers = i["request"]["headers"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            assert!(
                headers.iter().all(|h| h[0] != "api_key"),
                "auth headers are not recorded: {headers:?}"
            );
        }
        let skipped = v["interactions"]
            .as_array()
            .expect("interactions")
            .iter()
            .filter(|i| i["response"]["status"] == 404)
            .count() as u64;
        assert_eq!(skipped, c.skipped, "{ctx}");
    }
}

#[test]
fn every_port_stream_passes_its_reviewed_schema() {
    let p = replay();
    for c in CASES {
        assert!(
            p.path().join(format!("schemas/{}.yaml", c.stream)).exists(),
            "{}: no reviewed schema",
            c.stream
        );
        let mut args = vec!["schema", "check", c.stream, "--from-cassette", c.endpoint];
        args.extend(c.args);
        let o = p.run(&args);
        assert!(o.status.success(), "{}: {}", c.stream, out(&o));
        let s = String::from_utf8_lossy(&o.stdout);
        assert!(s.contains(&format!("{}: ok", c.stream)), "{s}");
    }
}

// ----------------------------------------------------------------------------- atlatl run

/// Exactly one `for_each` item of stream `id` was skipped, and its request names `who`.
fn skipped(report: &Value, id: &str, who: &str) {
    let s = &http(report, id)["skipped_items"];
    assert_eq!(s.as_array().map(Vec::len), Some(1), "{id}: {s}");
    assert!(
        s[0]["key"].as_str().is_some_and(|k| k.contains(who)),
        "{id}: {s}"
    );
}

/// The member's retries: `(class, message)`.
fn retries(report: &Value, id: &str) -> Vec<(String, String)> {
    stream(report, id)["members"][0]["retries"]
        .as_array()
        .expect("retries")
        .iter()
        .map(|r| {
            (
                r["class"].as_str().expect("class").to_owned(),
                r["message"].as_str().expect("message").to_owned(),
            )
        })
        .collect()
}

fn schema_ok(report: &Value, id: &str) {
    let s = &stream(report, id)["members"][0]["schema"];
    assert_eq!(s["status"], "ok", "{id}: {s}");
}

#[test]
fn run_replays_monthly_windows_then_the_stored_floor() {
    let p = replay();
    let now = recorded_month(&p, "monthly", "monthly_run1");
    let r = p.lake_run("monthly", ("ATLATL_CASSETTE", "monthly_run1"));
    assert_eq!(r["status"], "succeeded", "{r}");
    schema_ok(&r, "monthly/calendar");
    assert_eq!(
        http(&r, "monthly/calendar")["requests"].as_u64(),
        Some(windows((2026, 1), now, 1, 1)),
        "one request per month from the fixed start through next month"
    );
    assert_eq!(p.count("br.calendar"), 6);
    assert_eq!(
        p.q("SELECT dealId, category FROM lake.br.calendar WHERE dealId IN ('D100', 'D101') ORDER BY 1, 2"),
        [
            r#"{"dealId":"D100","category":"closed"}"#,
            r#"{"dealId":"D100","category":"open"}"#,
            r#"{"dealId":"D101","category":"closed"}"#,
            r#"{"dealId":"D101","category":"open"}"#
        ],
        "a deal is keyed per category"
    );
    let floor = from_index(month_index(now.0, now.1) - 2);
    assert_eq!(p.sync("monthly/calendar")["floor"], first_of(floor));

    let now = recorded_month(&p, "monthly", "monthly_run2");
    let r = p.lake_run("monthly", ("ATLATL_CASSETTE", "monthly_run2"));
    schema_ok(&r, "monthly/calendar");
    assert_eq!(
        http(&r, "monthly/calendar")["requests"].as_u64(),
        Some(windows(floor, now, 1, 1)),
        "from the stored floor"
    );
    assert_eq!(p.count("br.calendar"), 7);
    assert_eq!(
        p.q("SELECT proposedPrice AS p FROM lake.br.calendar WHERE dealId = 'D199'"),
        [r#"{"p":"$14.00"}"#],
        "repriced: merged by key"
    );
    assert!(
        p.types("br.calendar")
            .iter()
            .all(|(c, t)| t == "VARCHAR" || c.starts_with("_atlatl")),
        "br stays all-text"
    );
}

#[test]
fn run_replays_the_queue_chain() {
    let p = replay();
    let r = p.lake_run("queue_chain", ("ATLATL_CASSETTE", "queue_chain_run1"));
    assert_eq!(r["status"], "succeeded", "{r}");
    schema_ok(&r, "queue_chain/listings");
    assert_eq!(p.count("br.listings"), 3);
    assert_eq!(
        http(&r, "queue_chain/listings")["requests"],
        2,
        "two cursor pages"
    );
    assert_eq!(
        p.q("SELECT entity_reg_no AS r, security_uid AS s FROM lake.br.listings ORDER BY id"),
        [
            r#"{"r":"0000001","s":"UID0001"}"#,
            r#"{"r":"0000003","s":null}"#,
            r#"{"r":"0000004","s":"UID0004"}"#
        ],
        "nested objects flattened; leading zeros kept"
    );

    let r = p.lake_run(
        "queue_chain_history",
        ("ATLATL_CASSETTE", "queue_chain_history_run1"),
    );
    assert_eq!(r["status"], "succeeded", "{r}");
    for s in [
        "queue_chain/entities",
        "queue_chain/reports",
        "queue_chain/line_items",
    ] {
        schema_ok(&r, s);
    }
    assert_eq!(p.count("br.entities"), 3, "a null reg_no is filtered");
    assert_eq!(
        p.q("SELECT id, entity_id AS e, reg_no AS r FROM lake.br.reports ORDER BY id"),
        [
            r#"{"id":"rep_11","e":"ent_1","r":"0000001"}"#,
            r#"{"id":"rep_12","e":"ent_1","r":"0000001"}"#,
            r#"{"id":"rep_31","e":"ent_3","r":"0000003"}"#
        ],
        "TTM and post-2014 reports are filtered; parent fields from the response"
    );
    skipped(&r, "queue_chain/reports", "ent_4");
    assert_eq!(
        p.q("SELECT report_id AS r, item, value, year, kind FROM lake.br.line_items ORDER BY 1, 2"),
        [
            r#"{"r":"rep_11","item":"net_income","value":"120.25","year":"2013","kind":"income"}"#,
            r#"{"r":"rep_11","item":"revenue","value":"1000.5","year":"2013","kind":"income"}"#,
            r#"{"r":"rep_12","item":"revenue","value":"300","year":"2014","kind":"balance"}"#
        ],
        "exact numbers as text"
    );
    skipped(&r, "queue_chain/line_items", "rep_31");
}

#[test]
fn run_replays_record_and_paged_queues() {
    let p = replay();
    let r = p.lake_run("record_queue", ("ATLATL_CASSETTE", "record_queue_run1"));
    assert_eq!(r["status"], "succeeded", "{r}");
    schema_ok(&r, "record_queue/instruments");
    schema_ok(&r, "record_queue/ratings");
    assert_eq!(
        p.count("br.instruments"),
        3,
        "empty and null uids are filtered"
    );
    assert_eq!(
        p.q("SELECT uid, code, instrument_name AS n, venue_timezone AS tz, author FROM lake.br.ratings ORDER BY uid, date"),
        [
            r#"{"uid":"UID0001","code":"ACME","n":"Acme Corp","tz":"Etc/UTC","author":""}"#,
            r#"{"uid":"UID0001","code":"ACME","n":"Acme Corp","tz":"Etc/UTC","author":"J. Doe"}"#,
            r#"{"uid":"UID0002","code":"BOLT","n":"Bolt Industries","tz":"Etc/UTC","author":"A. Roe"}"#
        ]
    );
    skipped(&r, "record_queue/ratings", "CRUX");

    let r = p.lake_run("paged_queue", ("ATLATL_CASSETTE", "paged_queue_run1"));
    assert_eq!(r["status"], "succeeded", "{r}");
    schema_ok(&r, "paged_queue/orgs");
    schema_ok(&r, "paged_queue/rounds");
    assert_eq!(p.count("br.orgs"), 3);
    assert_eq!(
        http(&r, "paged_queue/orgs")["requests"],
        2,
        "pages 1 and 2 of 2"
    );
    assert_eq!(p.count("br.rounds"), 3);
    assert_eq!(
        http(&r, "paged_queue/rounds")["requests"],
        4,
        "org_a has two pages, org_b one, org_c 404s"
    );
    skipped(&r, "paged_queue/rounds", "org_c");
}

#[test]
fn run_replays_the_throttled_vendor() {
    let p = replay();
    let now = recorded_month(&p, "throttled", "throttled_releases_run1");
    let r = p.lake_run(
        "throttled_releases",
        ("ATLATL_CASSETTE", "throttled_releases_run1"),
    );
    assert_eq!(r["status"], "succeeded", "{r}");
    schema_ok(&r, "throttled/releases");
    let h = http(&r, "throttled/releases");
    let quarters = windows((2025, 1), now, 12, 3);
    assert_eq!(
        (h["requests"].as_u64(), h["retries"].as_u64()),
        (Some(quarters + 1), Some(1)),
        "quarterly windows through a year ahead, one retried 409: {h}"
    );
    assert_eq!(h["status_counts"]["409"], 1, "{h}");
    let rt = retries(&r, "throttled/releases");
    assert_eq!(rt.len(), 1, "{rt:?}");
    assert_eq!(rt[0].0, "throttled", "{rt:?}");
    assert!(
        rt[0].1.ends_with("throttled (status=409, attempt=1)"),
        "the rule's message: {rt:?}"
    );
    assert_eq!(p.count("br.releases"), 6);
    assert_eq!(
        p.q("SELECT count(*) AS n FROM lake.br.releases WHERE Code = 'BOLT:XA' AND Tag = ''"),
        [r#"{"n":2}"#],
        "blank tags are told apart by Date"
    );
    let m6 = month_index(now.0, now.1) - 6;
    let floor = from_index(m6 - m6.rem_euclid(3));
    assert_eq!(p.sync("throttled/releases")["floor"], first_of(floor));

    let r = p.lake_run(
        "throttled_symbols",
        ("ATLATL_CASSETTE", "throttled_symbols_run1"),
    );
    assert_eq!(r["status"], "succeeded", "{r}");
    schema_ok(&r, "throttled/symbols");
    let h = http(&r, "throttled/symbols");
    assert_eq!(
        (h["requests"].as_u64(), h["retries"].as_u64()),
        (Some(5), Some(1)),
        "the setup's \"slow down\" 200 is retried, then one request per region: {h}"
    );
    // Not the rule (`in` is case-sensitive): the undecodable body matches `throttle_patterns`.
    let rt = retries(&r, "throttled/symbols");
    assert!(
        rt.len() == 1 && rt[0].0 == "throttled" && rt[0].1.contains("Slow down!"),
        "{rt:?}"
    );
    assert_eq!(
        p.count("br.symbols"),
        3,
        "a region without symbols is empty"
    );

    let now = recorded_month(&p, "throttled", "throttled_releases_run2");
    let r = p.lake_run(
        "throttled_releases",
        ("ATLATL_CASSETTE", "throttled_releases_run2"),
    );
    schema_ok(&r, "throttled/releases");
    assert_eq!(
        http(&r, "throttled/releases")["requests"].as_u64(),
        Some(windows(floor, now, 12, 3)),
        "from the stored floor"
    );
    assert_eq!(p.count("br.releases"), 7);
    assert_eq!(
        p.q("SELECT Actual FROM lake.br.releases WHERE Code = 'DYNE:XB'"),
        [r#"{"Actual":"0.42"}"#],
        "reported: merged by key"
    );
    p.lake_run(
        "throttled_symbols",
        ("ATLATL_CASSETTE", "throttled_symbols_run2"),
    );
    assert_eq!(p.count("br.symbols"), 4);
}

#[test]
fn source_schema_drift_fails_the_run_before_writing() {
    let p = replay();
    let path = p.path().join("schemas/throttled/releases.yaml");
    let text = std::fs::read_to_string(&path).expect("schema");
    let tightened = text.replace(
        "Importance: {type: integer, values: [1, 2, 3]}",
        "Importance: {type: integer, values: [2, 3]}",
    );
    assert_ne!(text, tightened);
    std::fs::write(&path, tightened).expect("write");
    let (o, r) = p.try_lake_run(
        "throttled_releases",
        ("ATLATL_CASSETTE", "throttled_releases_run1"),
    );
    assert!(!o.status.success(), "{}", out(&o));
    let text = r.to_string();
    assert!(text.contains("schema_violation"), "{text}");
    let s = &stream(&r, "throttled/releases")["members"][0]["schema"];
    assert_eq!(s["severity"], "error", "{s}");
    assert_eq!(s["violations"][0]["field"], "Importance", "{s}");
    assert!(
        p.q("SELECT count(*) AS n FROM information_schema.tables WHERE table_catalog = 'lake' AND table_name = 'releases'")
            == [r#"{"n":0}"#],
        "nothing was written"
    );
}

// ----------------------------------------------------------------------------- vendor emulators

/// Deals by month; the current month changes in phase 2 (a repriced deal and a new one).
fn monthly(phase: Arc<AtomicU32>) -> Arc<Handler> {
    Arc::new(move |r: &Req| {
        if !r
            .header("User-Agent")
            .is_some_and(|u| u.starts_with("Mozilla/"))
        {
            return Resp(
                403,
                Some("text/html"),
                "<html><body>Access Denied</body></html>".into(),
            );
        }
        if r.path != "/api/listings/calendar" {
            return Resp::json(404, r#"{"message":"not found"}"#);
        }
        let deal = |id: &str,
                    sym: &str,
                    price: &str,
                    units: &str,
                    dates: [Option<&str>; 3],
                    status: &str| {
            let d = |x: Option<&str>| x.map_or(Value::Null, |s| json!(s));
            json!({
                "dealId": id, "proposedSymbol": sym, "orgName": format!("{sym} Holdings"),
                "proposedVenue": if sym.starts_with('B') { "XB" } else { "XA" },
                "proposedPrice": price, "unitsOffered": units,
                "openedDate": d(dates[0]), "filedDate": d(dates[1]), "cancelledDate": d(dates[2]),
                "dollarValue": "$15,000,000", "dealStatus": status,
            })
        };
        let (y, m, _) = today();
        let cur = format!("{y:04}-{m:02}");
        let p2 = phase.load(Ordering::SeqCst) >= 2;
        let month = r.param("month").unwrap_or_default();
        let (open, closed, cancelled) = match month.as_str() {
            "2026-01" => (
                Some(vec![deal(
                    "D100",
                    "ACME",
                    "$12.00",
                    "1,250,000",
                    [Some("01/15/2026"), Some("01/02/2026"), None],
                    "Open",
                )]),
                None,
                Some(vec![]),
            ),
            "2026-02" => (
                Some(vec![deal(
                    "D101",
                    "BOLT",
                    "$10.00-$12.00",
                    "800,000",
                    [Some("02/20/2026"), Some("02/01/2026"), None],
                    "Open",
                )]),
                Some(vec![deal(
                    "D100",
                    "ACME",
                    "$12.50",
                    "1,250,000",
                    [Some("01/15/2026"), Some("01/02/2026"), None],
                    "Closed",
                )]),
                None,
            ),
            "2026-03" => (
                None,
                Some(vec![deal(
                    "D101",
                    "BOLT",
                    "$11.00",
                    "800,000",
                    [Some("02/20/2026"), Some("02/01/2026"), None],
                    "Closed",
                )]),
                Some(vec![deal(
                    "D102",
                    "CRUX",
                    "$8.00",
                    "2,000,000",
                    [None, Some("03/03/2026"), Some("03/30/2026")],
                    "Cancelled",
                )]),
            ),
            x if x == cur => {
                let opened = format!("{m:02}/01/{y:04}");
                let mut v = vec![deal(
                    "D199",
                    "DYNE",
                    if p2 { "$14.00" } else { "$13.00" },
                    "500,000",
                    [Some(&opened), Some(&opened), None],
                    "Open",
                )];
                if p2 {
                    v.push(deal(
                        "D200",
                        "EMBR",
                        "$9.00",
                        "3,000,000",
                        [None, Some(&opened), None],
                        "Open",
                    ));
                }
                (Some(v), None, None)
            }
            _ => (None, None, None),
        };
        Resp::json(
            200,
            json!({"data": {
                "open": {"rows": open}, "closed": {"rows": closed}, "cancelled": {"rows": cancelled}
            }, "message": null})
            .to_string(),
        )
    })
}

fn queue_chain(key: &'static str) -> Arc<Handler> {
    Arc::new(move |r: &Req| {
        if r.param("api_key").as_deref() != Some(key) {
            return Resp::json(401, r#"{"error":"unauthorized"}"#);
        }
        let entity = |id: &str, reg: Option<&str>, code: &str| json!({"id": id, "reg_no": reg, "code": code, "name": format!("{code} Corp")});
        let page = |items: Vec<Value>, key: &str, next: Option<&str>| {
            let mut o = serde_json::Map::new();
            o.insert(key.into(), Value::Array(items));
            o.insert("next_page".into(), next.map_or(Value::Null, |n| json!(n)));
            Resp::json(200, Value::Object(o).to_string())
        };
        let cursor = r.param("next_page");
        let listing = |id: &str,
                       code: &str,
                       ent: (&str, &str),
                       status: &str,
                       security: Option<&str>,
                       price: Option<f64>| {
            json!({
                "id": id, "code": code, "date": "2026-02-10", "status": status, "venue": "XA",
                "entity": {"id": ent.0, "name": format!("{code} Corp"), "reg_no": ent.1},
                "security": security.map(|s| json!({"uid": s})),
                "offer_value": 250000000.00, "unit_price": price, "unit_price_low": 18.00,
                "unit_price_high": 21.00, "unit_count": 12500000,
            })
        };
        match (r.path.as_str(), cursor.as_deref()) {
            ("/api/listings", None) => page(
                vec![
                    listing(
                        "lst_1",
                        "ACME",
                        ("ent_1", "0000001"),
                        "priced",
                        Some("UID0001"),
                        Some(20.00),
                    ),
                    listing(
                        "lst_2",
                        "CRUX",
                        ("ent_3", "0000003"),
                        "expected",
                        None,
                        None,
                    ),
                ],
                "listings",
                Some("l2"),
            ),
            ("/api/listings", Some("l2")) => page(
                vec![listing(
                    "lst_3",
                    "DYNE",
                    ("ent_4", "0000004"),
                    "withdrawn",
                    Some("UID0004"),
                    None,
                )],
                "listings",
                None,
            ),
            ("/api/entities", None) => page(
                vec![
                    entity("ent_1", Some("0000001"), "ACME"),
                    entity("ent_2", None, "BOLT"),
                    entity("ent_3", Some("0000003"), "CRUX"),
                ],
                "entities",
                Some("e2"),
            ),
            ("/api/entities", Some("e2")) => page(
                vec![entity("ent_4", Some("0000004"), "DYNE")],
                "entities",
                None,
            ),
            (p, c) if p.starts_with("/api/entities/") && p.ends_with("/reports") => {
                let id = p
                    .trim_start_matches("/api/entities/")
                    .trim_end_matches("/reports");
                let report = |rid: &str, year: i64, period: &str, kind: &str| {
                    json!({
                        "id": rid, "year": year, "period": period, "start_date": format!("{year}-01-01"),
                        "end_date": format!("{year}-12-31"), "filed_date": format!("{}-02-15", year + 1), "kind_code": kind,
                    })
                };
                let (items, next) = match (id, c) {
                    ("ent_1", None) => (
                        vec![
                            report("rep_11", 2013, "FY", "income"),
                            report("rep_12", 2014, "P1", "balance"),
                        ],
                        Some("r2"),
                    ),
                    ("ent_1", Some("r2")) => (
                        vec![
                            report("rep_13", 2014, "TTM", "income"),
                            report("rep_14", 2015, "FY", "income"),
                        ],
                        None,
                    ),
                    ("ent_3", None) => (vec![report("rep_31", 2012, "P2", "income")], None),
                    _ => return Resp::json(404, r#"{"error":"not found"}"#),
                };
                let (code, reg) = if id == "ent_1" {
                    ("ACME", "0000001")
                } else {
                    ("CRUX", "0000003")
                };
                Resp::json(200, json!({"reports": items, "entity": {"id": id, "reg_no": reg, "code": code}, "next_page": next}).to_string())
            }
            (p, None) if p.starts_with("/api/reports/") && p.ends_with("/line_items") => {
                let id = p
                    .trim_start_matches("/api/reports/")
                    .trim_end_matches("/line_items");
                let (year, period, kind, items) = match id {
                    "rep_11" => (
                        2013,
                        "FY",
                        "income",
                        json!([
                            {"tag": {"name": "revenue"}, "value": 1000.50},
                            {"tag": {"name": "net_income"}, "value": 120.25}
                        ]),
                    ),
                    "rep_12" => (
                        2014,
                        "P1",
                        "balance",
                        json!([{"tag": {"name": "revenue"}, "value": 300}]),
                    ),
                    _ => return Resp::json(404, r#"{"error":"not found"}"#),
                };
                Resp::json(200, json!({
                    "line_items": items,
                    "report": {
                        "id": id, "entity": {"id": "ent_1", "reg_no": "0000001", "code": "ACME"},
                        "year": year, "period": period, "start_date": format!("{year}-01-01"),
                        "end_date": format!("{year}-12-31"), "filed_date": format!("{}-02-15", year + 1),
                        "kind_code": kind,
                    },
                }).to_string())
            }
            _ => Resp::json(404, r#"{"error":"not found"}"#),
        }
    })
}

fn record_queue(key: &'static str) -> Arc<Handler> {
    Arc::new(move |r: &Req| {
        if r.param("apikey").as_deref() != Some(key) {
            return Resp::json(401, r#"{"code":401,"status":"error"}"#);
        }
        let inst = |code: &str, uid: Option<&str>, name: &str| {
            json!({
                "code": code, "uid": uid, "name": name, "currency": "USD", "venue": "XA", "country": "Nowhere", "type": "Common",
            })
        };
        match r.path.as_str() {
            "/api/instruments" => Resp::json(
                200,
                json!({"data": [
                    inst("ACME", Some("UID0001"), "Acme Corp"),
                    inst("NOID", Some(""), "No Id Inc"),
                    inst("BOLT", Some("UID0002"), "Bolt Industries"),
                    inst("NULL", None, "Null Co"),
                    inst("CRUX", Some("UID0003"), "Crux Ltd"),
                ]})
                .to_string(),
            ),
            "/api/ratings/listed" => {
                let rating = |date: &str,
                              firm: &str,
                              author: &str,
                              action: &str,
                              cur: &str,
                              prior: &str,
                              tc: f64,
                              tp: f64| {
                    json!({
                        "date": date, "source_name": firm, "author": author, "action": action,
                        "rating_current": cur, "rating_prior": prior, "target_current": tc, "target_prior": tp,
                    })
                };
                let (code, name, ratings) =
                    match (r.param("code").as_deref(), r.param("uid").as_deref()) {
                        (Some("ACME"), Some("UID0001")) => (
                            "ACME",
                            "Acme Corp",
                            vec![
                                rating(
                                    "2026-09-01",
                                    "Firm One",
                                    "J. Doe",
                                    "upgrade",
                                    "Buy",
                                    "Hold",
                                    210.0,
                                    180.0,
                                ),
                                rating(
                                    "2026-08-15",
                                    "Firm Two",
                                    "",
                                    "maintain",
                                    "Overweight",
                                    "Overweight",
                                    25.5,
                                    25.5,
                                ),
                            ],
                        ),
                        (Some("BOLT"), Some("UID0002")) => (
                            "BOLT",
                            "Bolt Industries",
                            vec![rating(
                                "2026-07-30",
                                "Firm One",
                                "A. Roe",
                                "initiate",
                                "Hold",
                                "",
                                40.0,
                                0.0,
                            )],
                        ),
                        _ => {
                            return Resp::json(
                                404,
                                r#"{"code":404,"message":"no ratings","status":"error"}"#,
                            );
                        }
                    };
                Resp::json(200, json!({
                    "meta": {"code": code, "name": name, "currency": "USD", "venue": "XA", "venue_timezone": "Etc/UTC"},
                    "ratings": ratings, "status": "ok",
                }).to_string())
            }
            _ => Resp::json(404, r#"{"code":404,"status":"error"}"#),
        }
    })
}

fn paged_queue(key: &'static str) -> Arc<Handler> {
    Arc::new(move |r: &Req| {
        if r.header("api_key") != Some(key) {
            return Resp::json(401, r#"{"error":"Invalid API key"}"#);
        }
        let n: u32 = r
            .param("pageNumber")
            .and_then(|p| p.parse().ok())
            .unwrap_or(0);
        let pagination = |pages: u32| json!({"pageNumber": n, "numPages": pages, "pageSize": 2});
        let org = |id: &str, name: &str, raised: f64| json!({"orgId": id, "name": name, "sector": "Industrials", "totalRaised": raised});
        let round = |id: &str,
                     org: &str,
                     kind: &str,
                     date: &str,
                     amount: f64,
                     pre: Option<f64>,
                     investors: &[&str]| {
            json!({
                "id": id, "orgId": org, "roundType": kind, "announcedDate": date, "amountRaised": amount,
                "preMoney": pre, "investors": investors,
            })
        };
        match (r.path.as_str(), n) {
            ("/api/v1/orgs/all", 1) => Resp::json(200, json!({"orgs": [org("org_a", "Alpha Works", 42000000.0), org("org_b", "Beta Labs", 9500000.5)], "pagination": pagination(2)}).to_string()),
            ("/api/v1/orgs/all", 2) => Resp::json(200, json!({"orgs": [org("org_c", "Gamma Systems", 0.0)], "pagination": pagination(2)}).to_string()),
            ("/api/v2/orgs/org_a/rounds", 1) => Resp::json(200, json!({"rounds": [round("r_a1", "org_a", "Seed", "2024-03-01", 2000000.0, None, &["Fund One"])], "pagination": pagination(2)}).to_string()),
            ("/api/v2/orgs/org_a/rounds", 2) => Resp::json(200, json!({"rounds": [round("r_a2", "org_a", "Series A", "2025-06-12", 40000000.0, Some(160000000.0), &["Fund One", "Fund Two"])], "pagination": pagination(2)}).to_string()),
            ("/api/v2/orgs/org_b/rounds", 1) => Resp::json(200, json!({"rounds": [round("r_b1", "org_b", "Seed", "2025-11-20", 9500000.5, None, &[])], "pagination": pagination(1)}).to_string()),
            _ => Resp::json(404, r#"{"error":"Not found"}"#),
        }
    })
}

/// Throttles the first `/regions` call (a 200 "slow down") and the first request for the
/// 2025-04-01 window (a plain-text 409 without Content-Type) of each recorded command; a release
/// dated today is reported in phase 2, when a second one and a new symbol appear.
fn throttled(
    key: &'static str,
    phase: Arc<AtomicU32>,
    once: Arc<Mutex<HashSet<String>>>,
) -> Arc<Handler> {
    Arc::new(move |r: &Req| {
        if r.param("key").as_deref() != Some(key) || r.param("fmt").as_deref() != Some("json") {
            return Resp(401, Some("text/plain"), "Invalid key".into());
        }
        let first = |k: &str| once.lock().expect("once").insert(k.to_owned());
        let p2 = phase.load(Ordering::SeqCst) >= 2;
        let (y, m, d) = today();
        let today = format!("{y:04}-{m:02}-{d:02}");
        match r.path.as_str() {
            "/api/regions" => {
                if first("regions") {
                    return Resp(
                        200,
                        Some("text/plain"),
                        "Slow down! Too many requests.".into(),
                    );
                }
                Resp::json(
                    200,
                    json!([{"Region": "North"}, {"Region": "South"}, {"Region": "Nowhere"}])
                        .to_string(),
                )
            }
            p if p.starts_with("/api/symbols/region/") => {
                let sym = |code: &str, name: &str, region: &str, isin: &str| json!({"Code": code, "Name": name, "Region": region, "Isin": isin});
                let v = match p.trim_start_matches("/api/symbols/region/") {
                    "North" => {
                        let mut v = vec![
                            sym("ACME:XA", "Acme Corp", "North", "XA0000000011"),
                            sym("BOLT:XA", "Bolt Industries", "North", "XA0000000029"),
                        ];
                        if p2 {
                            v.push(sym("DYNE:XB", "Dyne Energy", "North", "XB0000000037"));
                        }
                        v
                    }
                    "South" => vec![sym("CRUX:XB", "Crux Ltd", "South", "XB0000000045")],
                    _ => vec![],
                };
                Resp::json(200, Value::Array(v).to_string())
            }
            "/api/releases" => {
                let (start, end) = (
                    r.param("start").unwrap_or_default(),
                    r.param("end").unwrap_or_default(),
                );
                if start == "2025-04-01" && first("releases:2025-04-01") {
                    return Resp(
                        409,
                        None,
                        "You are sending requests too fast, slow down".into(),
                    );
                }
                let rel = |code: &str,
                           tag: &str,
                           date: &str,
                           actual: Option<f64>,
                           importance: i64,
                           session: &str| {
                    json!({
                        "Code": code, "Name": format!("{code} results"), "Region": if code.ends_with("XA") { "North" } else { "South" },
                        "Date": date, "Tag": tag,
                        "Actual": actual.map_or(String::new(), |a| format!("{a:.2}")), "ActualValue": actual,
                        "Forecast": "1.05", "ForecastValue": 1.05, "Previous": "0.98", "PreviousValue": 0.98,
                        "Importance": importance, "Revenue": "253.15B", "Currency": "USD", "Session": session,
                        "LastUpdate": date,
                    })
                };
                let mut all = vec![
                    rel(
                        "ACME:XA",
                        "Q4",
                        "2025-02-10T21:00:00",
                        Some(1.10),
                        3,
                        "after",
                    ),
                    rel(
                        "ACME:XA",
                        "Q1",
                        "2025-05-12T21:00:00",
                        Some(1.20),
                        3,
                        "after",
                    ),
                    rel(
                        "BOLT:XA",
                        "",
                        "2025-05-12T12:00:00",
                        Some(-0.05),
                        1,
                        "before",
                    ),
                    rel("BOLT:XA", "", "2025-08-20T12:00:00", None, 1, "before"),
                    rel(
                        "CRUX:XB",
                        "Q2",
                        "2026-02-03T12:30:00",
                        Some(0.33),
                        2,
                        "during",
                    ),
                    rel(
                        "DYNE:XB",
                        "Q3",
                        &format!("{today}T12:00:00"),
                        p2.then_some(0.42),
                        2,
                        "after",
                    ),
                ];
                if p2 {
                    all.push(rel(
                        "EMBR:XA",
                        "Q3",
                        &format!("{today}T20:00:00"),
                        None,
                        1,
                        "after",
                    ));
                }
                let hits: Vec<Value> = all
                    .into_iter()
                    .filter(|x| in_range(&x["Date"].as_str().expect("date")[..10], &start, &end))
                    .collect();
                Resp::json(200, Value::Array(hits).to_string())
            }
            _ => Resp(404, Some("text/plain"), "Not found".into()),
        }
    })
}

// ----------------------------------------------------------------------------- recording

/// Re-records every cassette under `tests/fixtures/dogfood/ports/apis/*/cassettes/` against the
/// emulators (`cargo test -p atlatl-cli --test dogfood_ports -- --ignored record_cassettes`).
#[test]
#[ignore = "re-records the committed cassettes"]
fn record_cassettes() {
    let phase = Arc::new(AtomicU32::new(1));
    let once: Arc<Mutex<HashSet<String>>> = Arc::default();
    let servers = [
        serve(monthly(phase.clone())),
        serve(queue_chain(LIVE[0])),
        serve(record_queue(LIVE[1])),
        serve(paged_queue(LIVE[2])),
        serve(throttled(LIVE[3], phase.clone(), once.clone())),
    ];
    let p = proj(LIVE);
    for ((api, base), (url, _)) in APIS.iter().zip(&servers) {
        p.retarget(&format!("apis/{api}.yaml"), base, url);
    }

    // The emulators really check credentials and headers.
    let bad = proj(["x", "x", "x", "x"]);
    for ((api, base), (url, _)) in APIS.iter().zip(&servers) {
        bad.retarget(&format!("apis/{api}.yaml"), base, url);
    }
    for (conn, ep) in [
        ("queue_chain", "listings"),
        ("record_queue", "instruments"),
        ("paged_queue", "orgs"),
        ("throttled", "releases"),
    ] {
        let o = bad.run(&["api", "test", conn, ep, "--json"]);
        assert!(!o.status.success(), "{conn}: {}", out(&o));
        let v: Value = serde_json::from_slice(&o.stdout).expect("json");
        assert_eq!(v["error"]["class"], "auth", "{conn}: {v}");
    }
    let spec = bad.path().join("apis/monthly.yaml");
    let text = std::fs::read_to_string(&spec).expect("spec");
    std::fs::write(
        &spec,
        text.replace("User-Agent: \"Mozilla/5.0", "User-Agent: \"curl/8.0"),
    )
    .expect("write");
    let o = bad.run(&["api", "test", "monthly", "calendar", "--json"]);
    assert!(!o.status.success(), "{}", out(&o));

    for c in CASES {
        once.lock().expect("once").clear();
        let o = api_test(&p, c, "--record");
        assert!(o.status.success(), "{} {}: {}", c.conn, c.endpoint, out(&o));
        let v: Value =
            serde_json::from_slice(&o.stdout).unwrap_or_else(|e| panic!("{e}: {}", out(&o)));
        assert_eq!(
            v["record_count"], c.records,
            "{} {}: {v}",
            c.conn, c.endpoint
        );
    }
    for (run, ph) in [("run1", 1), ("run2", 2)] {
        phase.store(ph, Ordering::SeqCst);
        for rep in [
            "monthly",
            "queue_chain",
            "queue_chain_history",
            "record_queue",
            "paged_queue",
            "throttled_releases",
            "throttled_symbols",
        ] {
            if run == "run2"
                && !["monthly", "throttled_releases", "throttled_symbols"].contains(&rep)
            {
                continue;
            }
            once.lock().expect("once").clear();
            let r = p.lake_run(rep, ("ATLATL_CASSETTE_RECORD", &format!("{rep}_{run}")));
            assert_eq!(r["status"], "succeeded", "{rep} {run}: {r}");
        }
    }

    for ((api, base), (url, log)) in APIS.iter().zip(&servers) {
        assert!(!log.lock().expect("log").is_empty(), "{api}");
        let n = commit_cassettes(
            &p.path().join("apis").join(api).join("cassettes"),
            &fixture("dogfood/ports")
                .join("apis")
                .join(api)
                .join("cassettes"),
            url,
            base,
            &LIVE,
        );
        let endpoints = CASES.iter().filter(|c| c.conn == *api).count();
        let runs = match *api {
            "monthly" => 2,
            "queue_chain" => 2,
            "throttled" => 4,
            _ => 1,
        };
        assert_eq!(
            n,
            endpoints + runs,
            "{api}: one cassette per endpoint, plus the runs"
        );
    }
}
