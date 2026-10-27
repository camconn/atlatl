//! Cassettes (record → redacted file → replay without network), per-item secrets and explain.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use atlatl_core::ErrorClass;
use atlatl_http::cassette::CassetteFile;
use atlatl_http::json::JValue;
use atlatl_http::{ApiClient, Cassette, ClientOptions, RunVars};
use common::{Mock, Reply, opts, spec, values};
use indexmap::IndexMap;

fn secrets(s: &[(&str, &str)]) -> IndexMap<String, String> {
    s.iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

fn with(c: Cassette) -> ClientOptions {
    ClientOptions {
        cassette: Some(c),
        ..opts()
    }
}

fn oauth_paged(url: &str) -> String {
    format!(
        r#"
name: casstest
base_url: {url}
defaults:
  params: {{apikey: "{{{{ secrets.key }}}}"}}
auth:
  type: oauth2
  flow: client_credentials
  token_url: {url}/oauth2/token
  client_id: cid
  client_secret: "{{{{ secrets.client_secret }}}}"
endpoints:
  items:
    request: {{path: /items, params: {{since: "{{{{ vars.since }}}}", ts: "{{{{ vars.ts }}}}"}}}}
    paginate: {{type: page, param: page, start: 1, stop_when: 'response.json.last'}}
    records: {{path: data}}
"#
    )
}

fn run_vars(ts: &str) -> RunVars {
    let mut v = RunVars::default();
    v.globals.insert(
        "vars".into(),
        JValue::from(&serde_json::json!({"since": "2026-01-01", "ts": ts})),
    );
    v
}

#[tokio::test(start_paused = true)]
async fn record_redacts_and_replays_offline() {
    let hits = Arc::new(AtomicU32::new(0));
    let h = hits.clone();
    let m = Mock::start(move |r, _| {
        h.fetch_add(1, Ordering::SeqCst);
        if r.path() == "/oauth2/token" {
            return Reply::json(
                200,
                r#"{"access_token": "at-cass-token-1", "expires_in": 3600, "token_type": "Bearer"}"#,
            )
            .header("Set-Cookie", "sid=abc");
        }
        match r.param("page").as_deref() {
            Some("1") => Reply::json(200, r#"{"data": [{"id": 1, "px": 1.10}], "last": false}"#),
            _ => Reply::json(200, r#"{"data": [{"id": 2, "px": 2.00}], "last": true}"#),
        }
    })
    .await;
    let dir = tempfile::tempdir().expect("tmp");
    let path = atlatl_http::cassette::default_path(dir.path(), "casstest", "items");
    let s = secrets(&[("key", "qkey-cass-9"), ("client_secret", "csecret-cass-9")]);
    let rec = Cassette::record(&path).api("casstest").ignore_query(["ts"]);
    let c = ApiClient::new(spec(&oauth_paged(&m.url)), s.clone(), with(rec.clone())).expect("c");
    let (live, _) = c
        .endpoint("items", run_vars("100"))
        .expect("run")
        .collect()
        .await
        .expect("live");
    assert_eq!(
        values(&live),
        [r#"{"id":1,"px":1.10}"#, r#"{"id":2,"px":2.00}"#]
    );
    assert_eq!(rec.save().expect("save"), Some(path.clone()));
    let n = hits.load(Ordering::SeqCst);
    assert_eq!(n, 3);

    let text = std::fs::read_to_string(&path).expect("read");
    for secret in [
        "qkey-cass-9",
        "csecret-cass-9",
        "at-cass-token-1",
        "sid=abc",
    ] {
        assert!(!text.contains(secret), "{secret} leaked into\n{text}");
    }
    let file: CassetteFile = serde_json::from_str(&text).expect("json");
    assert_eq!(file.interactions.len(), 3);
    assert_eq!(file.matching.ignore_query, vec!["ts".to_owned()]);
    assert!(
        file.interactions.iter().all(|i| i
            .request
            .headers
            .iter()
            .all(|(k, _)| k != "authorization"))
    );
    assert!(file.interactions[1].request.url.contains("apikey=****"));
    assert!(
        file.interactions[0]
            .response
            .body
            .as_deref()
            .unwrap_or_default()
            .contains("****")
    );

    // Replay: no server traffic, same exact records, even with a different `ts`.
    let play = Cassette::replay(&path).expect("load");
    let c = ApiClient::new(spec(&oauth_paged(&m.url)), s.clone(), with(play.clone())).expect("c");
    let (replayed, stats) = c
        .endpoint("items", run_vars("200"))
        .expect("run")
        .collect()
        .await
        .expect("replay");
    assert_eq!(values(&replayed), values(&live));
    assert_eq!(stats.auth_requests, 1);
    assert_eq!(hits.load(Ordering::SeqCst), n);
    assert_eq!(play.unused(), 0);

    // Everything consumed: another run misses.
    let err = c
        .endpoint("items", run_vars("200"))
        .expect("run")
        .collect()
        .await
        .expect_err("miss");
    assert_eq!(err.class, ErrorClass::Config);
    assert_eq!(err.code.as_deref(), Some("cassette_miss"));
    assert!(!err.message.contains("qkey-cass-9"));
}

#[tokio::test(start_paused = true)]
async fn replay_mismatch_is_a_miss() {
    let m = Mock::start(|r, _| {
        if r.path() == "/oauth2/token" {
            return Reply::json(200, r#"{"access_token": "at-miss", "expires_in": 3600}"#);
        }
        Reply::json(200, r#"{"data": [], "last": true}"#)
    })
    .await;
    let s = secrets(&[("key", "k2-cass"), ("client_secret", "c2-cass")]);
    let rec = Cassette::memory();
    let yaml = oauth_paged(&m.url);
    let c = ApiClient::new(spec(&yaml), s.clone(), with(rec.clone())).expect("c");
    c.endpoint("items", run_vars("1"))
        .expect("run")
        .collect()
        .await
        .expect("live");
    let play = Cassette::from_file(rec.to_file()).expect("file");
    let c = ApiClient::new(spec(&yaml), s, with(play)).expect("c");
    let err = c
        .endpoint("items", run_vars("2"))
        .expect("run")
        .collect()
        .await
        .expect_err("ts differs and is matched");
    assert_eq!(err.code.as_deref(), Some("cassette_miss"));
    assert!(err.message.contains("ts=2"), "{}", err.message);
}

#[tokio::test(start_paused = true)]
async fn per_item_secrets_end_to_end() {
    let m = Mock::start(|r, _| {
        let ok = r.header("x-user-secret") == Some("us-item-secret-77")
            && r.param("user").as_deref() == Some("u1")
            && r.param("sig").as_deref() == Some("us-item-secret-77:u1");
        if ok {
            Reply::json(200, r#"[{"acct": "a1"}]"#)
        } else {
            Reply::json(403, r#"{"error": "bad user secret"}"#)
        }
    })
    .await;
    let yaml = format!(
        r#"
base_url: {}
endpoints:
  accounts:
    request:
      path: /accounts
      headers: {{X-User-Secret: "{{{{ secrets.item.user_secret }}}}"}}
      params: {{user: "{{{{ item.user_id }}}}", sig: "{{{{ secrets.item.user_secret }}}}:{{{{ item.user_id }}}}"}}
    records: {{path: "[]"}}
"#,
        m.url
    );
    let rec = Cassette::memory();
    let c = ApiClient::new(spec(&yaml), IndexMap::new(), with(rec.clone())).expect("c");
    let mut vars = RunVars {
        item: Some(JValue::from(&serde_json::json!({"user_id": "u1"}))),
        ..RunVars::default()
    };
    vars.item_secrets
        .insert("user_secret".into(), "us-item-secret-77".into());
    let (recs, _) = c
        .endpoint("accounts", vars)
        .expect("run")
        .collect()
        .await
        .expect("ok");
    assert_eq!(values(&recs), [r#"{"acct":"a1"}"#]);
    let file = serde_json::to_string(&rec.to_file()).expect("json");
    assert!(!file.contains("us-item-secret-77"), "{file}");
    assert!(file.contains("sig=****"), "{file}");
    assert_eq!(atlatl_core::redact::redact("x us-item-secret-77"), "x ****");
}

#[test]
fn explain_lists_effective_config() {
    let c = ApiClient::new(
        spec(
            r#"
name: ex
base_url: https://api.example.test
auth: {type: bearer, token: "{{ secrets.token }}"}
rate_limit: {rps: 2, burst: 4}
rules:
  - {when: 'response.status == 409', action: retry, class: throttled}
endpoints:
  list:
    request: {path: /things, params: {q: "x", literal_key: "lit-explain-secret"}}
    paginate: {type: cursor, param: cursor, from: response.json.next}
    records: {path: items, primary_key: [id]}
    emit: {ids: record.id}
  detail:
    for_each: {over: {queue: ids}, as: id}
    request: {path: "/things/{{ id }}"}
"#,
        ),
        secrets(&[("token", "lit-explain-secret")]),
        opts(),
    )
    .expect("c");
    let all = c.explain(None).expect("explain");
    assert_eq!(all["api"], "ex");
    assert_eq!(all["auth"]["type"], "bearer");
    assert_eq!(all["auth"]["token"], "{{ secrets.token }}");
    assert_eq!(all["rate_limit"]["rps"], 2.0);
    assert_eq!(all["endpoints"]["detail"]["consumes"], "ids");
    let one = c.explain(Some("list")).expect("explain");
    let ep = &one["endpoint"];
    assert_eq!(ep["paginate"]["type"], "cursor");
    assert_eq!(ep["request"]["params"]["literal_key"], "****");
    assert_eq!(ep["records"]["dedup"], "first");
    let rules: Vec<&str> = ep["rules"]
        .as_array()
        .expect("rules")
        .iter()
        .filter_map(|r| r.as_str())
        .collect();
    assert!(
        rules[0].starts_with("spec: when response.status == 409 -> retry"),
        "{rules:?}"
    );
    assert!(rules.iter().any(|r| r.starts_with("builtin:")), "{rules:?}");
    let text = atlatl_http::explain::render(&one);
    assert!(text.contains("  paginate:\n    type: cursor\n"), "{text}");
    assert!(c.explain(Some("nope")).is_err());
}

/// A throttled vendor replays at full speed: pacing and backoff advance a virtual clock, and the
/// stats still show the waits a live run would have taken.
#[tokio::test]
async fn replay_paces_on_a_virtual_clock() {
    let yaml = |url: &str| {
        format!(
            r#"
name: slow
base_url: {url}
rate_limit: {{rps: 0.2, min_interval: 250ms}}
rules:
  - {{when: 'response.status == 409 or "slow down" in response.text', action: retry, class: throttled, backoff: {{type: exponential, base: 5s}}, max_attempts: 5}}
endpoints:
  windows:
    for_each: {{over: [a, b, c], as: w}}
    request: {{path: "/w/{{{{ w }}}}"}}
    records: {{path: "[]"}}
"#
        )
    };
    let file: CassetteFile = serde_json::from_value(serde_json::json!({
        "version": 1,
        "interactions": [
            {"request": {"method": "GET", "url": "http://slow.test/w/a"}, "response": {"status": 409, "body": "Slow down"}},
            {"request": {"method": "GET", "url": "http://slow.test/w/a"}, "response": {"status": 200, "body": "slow down please"}},
            {"request": {"method": "GET", "url": "http://slow.test/w/a"}, "response": {"status": 200, "headers": [["content-type", "application/json"]], "body": "[{\"n\": 1}]"}},
            {"request": {"method": "GET", "url": "http://slow.test/w/b"}, "response": {"status": 200, "body": "[{\"n\": 2}]"}},
            {"request": {"method": "GET", "url": "http://slow.test/w/c"}, "response": {"status": 200, "body": "[]"}}
        ]
    }))
    .expect("cassette");
    let play = Cassette::from_file(file).expect("load");
    let c = ApiClient::new(
        spec(&yaml("http://slow.test")),
        IndexMap::new(),
        with(play.clone()),
    )
    .expect("c");
    let t0 = std::time::Instant::now();
    let mut d = atlatl_http::Driver::start(&c, "windows", Default::default())
        .await
        .expect("start");
    let mut recs = Vec::new();
    while let Some(ch) = d.next().await.expect("chunk") {
        recs.extend(ch.records);
    }
    let out = d.finish().expect("finish");
    assert!(
        t0.elapsed() < std::time::Duration::from_secs(3),
        "{:?}",
        t0.elapsed()
    );
    assert_eq!(values(&recs), [r#"{"n":1}"#, r#"{"n":2}"#]);
    assert_eq!(play.unused(), 0);
    let http = &out.stats.http;
    assert_eq!((http.requests, http.retries), (5, 2));
    assert!(
        (14_900..=15_000).contains(&http.throttle_wait_ms),
        "5s + 10s backoff, virtual: {}",
        http.throttle_wait_ms
    );
    assert!(
        out.stats.rate_wait_ms >= 9_900,
        "{}",
        out.stats.rate_wait_ms
    );
}

/// `now()` is pinned to the recording's start, written as `recorded_at`, and replays pin to it:
/// clock-derived requests (windows, `now()` params) match on any later day.
#[tokio::test(start_paused = true)]
async fn replay_pins_now_to_recorded_at() {
    let m = Mock::start(|r, _| {
        Reply::json(
            200,
            &format!(r#"[{{"at": "{}"}}]"#, r.param("to").unwrap_or_default()),
        )
    })
    .await;
    let yaml = format!(
        r#"
name: clocked
base_url: {}
endpoints:
  days:
    for_each: {{over: 'range(date_add(now(), -2, "day"), now(), "1d")', as: d}}
    request: {{path: /d, params: {{from: '{{{{ d | date_format("%Y-%m-%d") }}}}', to: '{{{{ now() | date_format("%Y-%m-%dT%H:%M:%S") }}}}'}}}}
    records: {{path: "[]"}}
"#,
        m.url
    );
    let dir = tempfile::tempdir().expect("tmp");
    let path = dir.path().join("days.json");
    let rec = Cassette::record(&path);
    let c = ApiClient::new(spec(&yaml), IndexMap::new(), with(rec.clone())).expect("c");
    let run = async |c: &ApiClient| {
        let mut d = atlatl_http::Driver::start(c, "days", Default::default())
            .await
            .expect("start");
        let mut recs = Vec::new();
        while let Some(ch) = d.next().await.expect("chunk") {
            recs.extend(ch.records);
        }
        values(&recs)
    };
    let live = run(&c).await;
    assert_eq!(live.len(), 3);
    assert!(
        live.iter().all(|r| r.contains("2026-09-28T12:00:00")),
        "{live:?}"
    );
    rec.save().expect("save");
    let file: CassetteFile =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("read")).expect("json");
    assert_eq!(
        file.recorded_at.as_deref(),
        Some("2026-09-28T12:00:00.000Z")
    );

    let later = ClientOptions {
        clock: atlatl_http::funcs::fixed_clock("2027-03-01T08:30:00Z"),
        ..with(Cassette::replay(&path).expect("load"))
    };
    let c = ApiClient::new(spec(&yaml), IndexMap::new(), later).expect("c");
    assert_eq!(run(&c).await, live);
}

#[tokio::test(start_paused = true)]
async fn header_auth_is_not_recorded() {
    let m = Mock::start(|r, _| match r.header("x-vendor-token") {
        Some("hdr-cass-secret") => Reply::json(200, "[]"),
        _ => Reply::json(401, "{}"),
    })
    .await;
    let yaml = format!(
        "name: hdr\nbase_url: {}\nauth: {{type: headers, headers: {{X-Vendor-Token: \"{{{{ secrets.k }}}}\", Accept: application/json}}}}\nendpoints:\n  e: {{request: {{path: /e}}, records: {{path: \"[]\"}}}}\n",
        m.url
    );
    let rec = Cassette::memory();
    let c = ApiClient::new(
        spec(&yaml),
        secrets(&[("k", "hdr-cass-secret")]),
        with(rec.clone()),
    )
    .expect("c");
    c.endpoint("e", RunVars::default())
        .expect("run")
        .collect()
        .await
        .expect("ok");
    let headers = &rec.interactions()[0].request.headers;
    assert!(
        headers
            .iter()
            .all(|(k, _)| !k.eq_ignore_ascii_case("x-vendor-token")
                && !k.eq_ignore_ascii_case("accept")),
        "{headers:?}"
    );
}
