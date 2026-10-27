//! Response pipeline, rules, limiter and records against an in-process server (paused clock).

mod common;

use std::time::Duration;

use atlatl_core::ErrorClass;
use atlatl_http::RunVars;
use common::{Mock, Reply, client, values};
use futures::TryStreamExt;
use tokio::time::Instant;

fn throttled(url: &str, extra: &str) -> String {
    format!(
        r#"
name: throttled
base_url: {url}
defaults:
  params: {{key: "{{{{ secrets.api_key }}}}", fmt: json}}
rate_limit: {{min_interval: 250ms}}
throttle_patterns: ["slow down"]
{extra}
endpoints:
  cal:
    request: {{path: /releases, params: {{start: "2026-01-01"}}}}
    records: {{path: "[]", primary_key: [Symbol]}}
"#
    )
}

#[tokio::test(start_paused = true)]
async fn plain_text_409_is_retried_as_throttled() {
    let m = Mock::start(|_, n| match n {
        0 | 1 => Reply::raw(409, None, "Slow down, you are sending requests too fast"),
        _ => Reply::json(
            200,
            r#"[{"Symbol": "AAPL", "EPS": 1.230}, {"Symbol": "MSFT"}]"#,
        ),
    })
    .await;
    let spec = throttled(
        &m.url,
        r#"rules:
  - when: 'response.status == 409 or "slow down" in response.text'
    action: retry
    class: throttled
    backoff: {type: exponential, base: 5s}
    max_attempts: 5
    message: "Throttle (status={{ response.status }}, attempt={{ request.attempts }})""#,
    );
    let c = client(&spec, &[("api_key", "th-key-pipeline")]);
    let t0 = Instant::now();
    let (recs, stats) = c
        .endpoint("cal", RunVars::default())
        .expect("run")
        .collect()
        .await
        .expect("ok");
    assert_eq!(
        values(&recs),
        [r#"{"Symbol":"AAPL","EPS":1.230}"#, r#"{"Symbol":"MSFT"}"#]
    );
    assert_eq!(recs.iter().map(|r| r.ordinal).collect::<Vec<_>>(), [0, 1]);
    assert_eq!(
        t0.elapsed(),
        Duration::from_secs(15),
        "5s + 10s throttle pauses"
    );
    assert_eq!((stats.http.requests, stats.http.retries), (3, 2));
    assert_eq!(stats.http.throttle_wait_ms, 15_000);
    assert_eq!(stats.http.status_counts.get("409"), Some(&2));
    assert_eq!(
        stats.attempts[1].message,
        "endpoint `cal`: Throttle (status=409, attempt=2)"
    );
    assert!(
        stats
            .attempts
            .iter()
            .all(|a| a.class == ErrorClass::Throttled)
    );
    let reqs = m.requests();
    assert_eq!(
        reqs[0].target,
        "/releases?key=th-key-pipeline&fmt=json&start=2026-01-01"
    );
}

#[tokio::test(start_paused = true)]
async fn plain_409_without_a_rule_is_classified_throttled_by_default() {
    let m = Mock::start(|_, n| match n {
        0 => Reply::raw(409, None, "slow down"),
        _ => Reply::json(200, "[]"),
    })
    .await;
    let c = client(&throttled(&m.url, ""), &[("api_key", "k")]);
    let (recs, stats) = c
        .endpoint("cal", RunVars::default())
        .expect("run")
        .collect()
        .await
        .expect("ok");
    assert!(recs.is_empty());
    assert_eq!(stats.attempts.len(), 1);
    assert_eq!(stats.attempts[0].class, ErrorClass::Throttled);
    assert!(
        stats.attempts[0].message.contains("HTTP 409"),
        "{}",
        stats.attempts[0].message
    );
    assert!(
        stats.attempts[0].message.contains("no content-type"),
        "{}",
        stats.attempts[0].message
    );
}

#[tokio::test(start_paused = true)]
async fn retry_after_pauses_the_shared_limiter() {
    let m = Mock::start(|_, n| match n {
        0 => Reply::json(429, r#"{"error": "rate"}"#).header("Retry-After", "30"),
        _ => Reply::json(200, r#"[{"Symbol": "A"}]"#),
    })
    .await;
    let c = client(&throttled(&m.url, ""), &[("api_key", "k")]);
    let t0 = Instant::now();
    let (recs, stats) = c
        .endpoint("cal", RunVars::default())
        .expect("run")
        .collect()
        .await
        .expect("ok");
    assert_eq!(recs.len(), 1);
    assert_eq!(t0.elapsed(), Duration::from_secs(30));
    assert_eq!(stats.http.throttle_wait_ms, 30_000);
    assert_eq!(stats.attempts[0].wait_ms, 30_000);
    let reqs = m.requests();
    assert_eq!(reqs[1].at - reqs[0].at, Duration::from_secs(30));
}

#[tokio::test(start_paused = true)]
async fn html_200_is_a_decode_error_and_repeats_count_as_throttling() {
    let m = Mock::start(|_, _| {
        Reply::raw(
            200,
            Some("text/html"),
            "<!DOCTYPE html><h1>Please slow down</h1>",
        )
    })
    .await;
    let c = client(&throttled(&m.url, ""), &[("api_key", "k")]);
    let e = c
        .endpoint("cal", RunVars::default())
        .expect("run")
        .collect()
        .await
        .expect_err("fails");
    assert_eq!(e.class, ErrorClass::Throttled, "{e}");
    assert!(e.message.contains("gave up after 5 attempts"), "{e}");
    assert!(e.message.contains("the body is HTML"), "{e}");
    assert_eq!(m.requests().len(), 5);

    let m = Mock::start(|_, n| match n {
        0 => Reply::raw(200, Some("text/html"), "<html>oops</html>"),
        _ => Reply::json(200, r#"[{"Symbol": "A"}]"#),
    })
    .await;
    let c = client(&throttled(&m.url, ""), &[("api_key", "k")]);
    let (recs, stats) = c
        .endpoint("cal", RunVars::default())
        .expect("run")
        .collect()
        .await
        .expect("ok");
    assert_eq!(recs.len(), 1);
    assert_eq!(stats.attempts[0].class, ErrorClass::Decode);
    assert_eq!(
        stats.http.throttle_wait_ms, 0,
        "decode retries back off without pausing the limiter"
    );
}

#[tokio::test(start_paused = true)]
async fn truncated_body_is_transient() {
    let m = Mock::start(|_, n| match n {
        0 => Reply::Truncated {
            status: 200,
            body: br#"[{"Symbol": "#.to_vec(),
            claimed: 500,
        },
        _ => Reply::json(200, r#"[{"Symbol": "A"}]"#),
    })
    .await;
    let c = client(&throttled(&m.url, ""), &[("api_key", "k")]);
    let (recs, stats) = c
        .endpoint("cal", RunVars::default())
        .expect("run")
        .collect()
        .await
        .expect("ok");
    assert_eq!(recs.len(), 1);
    assert_eq!(stats.attempts[0].class, ErrorClass::Transient);
    assert_eq!(stats.http.requests, 2);
}

#[tokio::test(start_paused = true)]
async fn server_errors_retry_then_fail() {
    let m = Mock::start(|_, n| match n {
        0 | 1 => Reply::raw(503, Some("text/html"), "<html>down</html>"),
        _ => Reply::json(200, r#"[{"Symbol": "A"}]"#),
    })
    .await;
    let c = client(&throttled(&m.url, ""), &[("api_key", "k")]);
    let (recs, stats) = c
        .endpoint("cal", RunVars::default())
        .expect("run")
        .collect()
        .await
        .expect("ok");
    assert_eq!(recs.len(), 1);
    assert_eq!(
        stats
            .attempts
            .iter()
            .map(|a| (a.class, a.wait_ms))
            .collect::<Vec<_>>(),
        [(ErrorClass::Transient, 2000), (ErrorClass::Transient, 4000)]
    );

    let m = Mock::start(|_, _| Reply::json(500, r#"{"error": "boom"}"#)).await;
    let c = client(&throttled(&m.url, ""), &[("api_key", "k-pipeline-500")]);
    let e = c
        .endpoint("cal", RunVars::default())
        .expect("run")
        .collect()
        .await
        .expect_err("fails");
    assert_eq!(e.class, ErrorClass::Transient);
    assert_eq!(e.code.as_deref(), Some("http_500"));
    assert_eq!(m.requests().len(), 5);
    assert!(!e.to_string().contains("k-pipeline-500"), "{e}");
}

#[tokio::test(start_paused = true)]
async fn unauthorized_reauths_once_then_fails_with_auth() {
    let m = Mock::start(|_, _| Reply::json(401, r#"{"error": "bad key"}"#)).await;
    let spec = format!(
        "base_url: {}\nauth: {{type: bearer, token: \"{{{{ secrets.t }}}}\"}}\nendpoints:\n  a: {{request: {{path: /a}}}}\n",
        m.url
    );
    let c = client(&spec, &[("t", "tok-pipeline-401")]);
    let e = c
        .endpoint("a", RunVars::default())
        .expect("run")
        .collect()
        .await
        .expect_err("auth");
    assert_eq!(e.class, ErrorClass::Auth);
    assert_eq!(m.requests().len(), 2);
    assert_eq!(
        m.requests()[1].header("authorization"),
        Some("Bearer tok-pipeline-401")
    );
}

#[tokio::test(start_paused = true)]
async fn client_errors_fail_and_skip_rules_skip() {
    let m = Mock::start(|r, _| match r.path() {
        "/missing" => Reply::raw(404, Some("text/html"), "<html>nope</html>"),
        _ => Reply::json(400, r#"{"error": "bad param"}"#),
    })
    .await;
    let spec = format!(
        r#"base_url: {}
endpoints:
  skip:
    request: {{path: /missing}}
    rules: [{{when: "response.status == 404", action: skip, message: "no data for this item"}}]
  fail: {{request: {{path: /bad}}}}
  custom:
    request: {{path: /bad}}
    rules: [{{when: "response.json.error == 'bad param'", action: fail, class: data, message: "vendor says {{{{ response.json.error }}}}"}}]
  stop:
    request: {{path: /bad}}
    rules: [{{when: "response.status == 400", action: stop}}]
"#,
        m.url
    );
    let c = client(&spec, &[]);
    let (recs, stats) = c
        .endpoint("skip", RunVars::default())
        .expect("run")
        .collect()
        .await
        .expect("skip");
    assert!(recs.is_empty());
    assert_eq!(stats.http.skipped_items, 1);
    assert_eq!(stats.skips[0].reason, "no data for this item");
    assert!(
        stats.skips[0].request_key.ends_with("/missing"),
        "{:?}",
        stats.skips
    );
    let e = c
        .endpoint("fail", RunVars::default())
        .expect("run")
        .collect()
        .await
        .expect_err("fail");
    assert_eq!(
        (e.class, e.code.as_deref()),
        (ErrorClass::Config, Some("http_400"))
    );
    assert!(e.message.contains("bad param"), "{e}");
    let e = c
        .endpoint("custom", RunVars::default())
        .expect("run")
        .collect()
        .await
        .expect_err("fail");
    assert_eq!(e.class, ErrorClass::Data);
    assert_eq!(e.message, "endpoint `custom`: vendor says bad param");
    let (_, stats) = c
        .endpoint("stop", RunVars::default())
        .expect("run")
        .collect()
        .await
        .expect("stop");
    assert!(stats.stopped);
}

// Real clock: concurrent in-flight I/O plus pending limiter timers would let a paused clock
// auto-advance before the server sees the requests.
#[tokio::test]
async fn limiter_is_shared_across_endpoints() {
    let m = Mock::start(|_, _| Reply::json(200, "[]")).await;
    let spec = format!(
        "base_url: {}\nrate_limit: {{min_interval: 100ms, max_in_flight: 4}}\nendpoints:\n  a: {{request: {{path: /a}}}}\n  b: {{request: {{path: /b}}}}\n",
        m.url
    );
    let c = client(&spec, &[]);
    let runs: Vec<_> = ["a", "b", "a", "b"]
        .iter()
        .map(|n| c.endpoint(n, RunVars::default()).expect("run").collect())
        .collect();
    let t0 = Instant::now();
    futures::future::try_join_all(runs).await.expect("ok");
    assert!(t0.elapsed() >= Duration::from_millis(300));
    let mut at: Vec<Instant> = m.requests().iter().map(|r| r.at).collect();
    at.sort();
    // Starts are exactly 100ms apart (see the limiter unit tests); arrivals add connect latency.
    assert!(
        at[3] - at[0] >= Duration::from_millis(200),
        "{:?}",
        at[3] - at[0]
    );
    assert_eq!(c.limiter().requests(), 4);
}

#[tokio::test(start_paused = true)]
async fn budget_and_body_cap() {
    let m = Mock::start(|_, _| Reply::json(200, r#"{"items": [{"id": 1}], "next": "x"}"#)).await;
    let spec = format!(
        "base_url: {}\nrate_limit: {{budget: {{requests: 3, per: run}}}}\nmax_body: 1KB\nendpoints:\n  a:\n    request: {{path: /a}}\n    paginate: {{type: page, param: p}}\n    records: {{path: items}}\n  big: {{request: {{path: /big}}, decode: {{max_body: 10B}}}}\n",
        m.url
    );
    let c = client(&spec, &[]);
    let e = c
        .endpoint("a", RunVars::default())
        .expect("run")
        .collect()
        .await
        .expect_err("budget");
    assert_eq!(
        (e.class, e.code.as_deref()),
        (ErrorClass::Config, Some("request_budget"))
    );
    assert_eq!(m.requests().len(), 3);
    let c = client(&spec, &[]);
    let e = c
        .endpoint("big", RunVars::default())
        .expect("run")
        .collect()
        .await
        .expect_err("cap");
    assert_eq!(
        (e.class, e.code.as_deref()),
        (ErrorClass::Data, Some("body_too_large"))
    );
}

#[tokio::test(start_paused = true)]
async fn records_fields_filter_dedup_and_exact_numbers() {
    let m = Mock::start(|r, _| {
        let p = r.param("page").unwrap_or_default();
        Reply::json(
            200,
            &match p.as_str() {
                "1" => r#"{"fundamental": {"id": "f1", "cik": 320193}, "rows": [{"item": "revenue", "value": 394328000000.123456789012345}, {"item": "eps", "value": 6.1100}, {"item": "junk", "value": null}]}"#.to_owned(),
                "2" => r#"{"fundamental": {"id": "f1", "cik": 320193}, "rows": [{"item": "eps", "value": 6.12}]}"#.to_owned(),
                _ => r#"{"rows": []}"#.to_owned(),
            },
        )
    })
    .await;
    let spec = format!(
        r#"base_url: {}
endpoints:
  fin:
    request: {{path: "/fundamentals/{{{{ item.id }}}}/standardized"}}
    paginate: {{type: page, param: page}}
    records: {{path: rows, primary_key: [fundamental_id, item], filter: "record.value != none"}}
    fields:
      fundamental_id: response.json.fundamental.id
      cik: response.json.fundamental.cik
      tag: item.tag
"#,
        m.url
    );
    let c = client(&spec, &[]);
    let vars = RunVars {
        item: Some(serde_json::json!({"id": "f 1", "tag": "x"}).into()),
        ..Default::default()
    };
    let run = c.endpoint("fin", vars).expect("run");
    let stats = run.stats();
    let recs: Vec<_> = run.into_stream().try_collect().await.expect("stream");
    assert_eq!(
        values(&recs),
        [
            r#"{"item":"revenue","value":394328000000.123456789012345,"fundamental_id":"f1","cik":320193,"tag":"x"}"#,
            r#"{"item":"eps","value":6.1100,"fundamental_id":"f1","cik":320193,"tag":"x"}"#,
        ]
    );
    let s = stats.get();
    assert_eq!((s.pages, s.records, s.filtered, s.duplicates), (3, 2, 1, 1));
    assert_eq!(
        m.requests()[0].target,
        "/fundamentals/f%201/standardized?page=1"
    );
}

#[tokio::test(start_paused = true)]
async fn raw_mode_lands_the_response() {
    let m = Mock::start(|_, _| Reply::json(200, r#"{"accounts": [{"id": 1, "balance": 10.50}]}"#))
        .await;
    let spec = format!(
        "base_url: {}\nendpoints:\n  cap: {{request: {{path: /accounts, params: {{userId: u1}}}}, raw: true}}\n",
        m.url
    );
    let c = client(&spec, &[]);
    let (recs, _) = c
        .endpoint("cap", RunVars::default())
        .expect("run")
        .collect()
        .await
        .expect("ok");
    assert_eq!(recs.len(), 1);
    let r = &recs[0].record;
    assert_eq!(r["status"].to_string(), "200");
    assert_eq!(r["fetched_at"].as_str(), Some("2026-09-28T12:00:00Z"));
    assert!(
        r["request_key"]
            .as_str()
            .is_some_and(|k| k.starts_with("GET http://") && k.ends_with("/accounts?userId=u1"))
    );
    assert_eq!(
        r["response_json"].to_string(),
        r#"{"accounts":[{"id":1,"balance":10.50}]}"#
    );
}

#[tokio::test(start_paused = true)]
async fn templates_are_validated_before_any_request() {
    let m = Mock::start(|_, _| Reply::json(200, "[]")).await;
    let spec = format!(
        "base_url: {}\nendpoints:\n  a: {{request: {{path: /a, params: {{k: \"{{{{ secrets.api_kye }}}}\"}}}}}}\n  b:\n    request: {{path: /b}}\n    rules: [{{when: \"respnse.status == 1\", action: skip}}]\n  c: {{request: {{path: \"/{{{{ sync.floor }}}}\"}}}}\n",
        m.url
    );
    let c = client(&spec, &[("api_key", "k")]);
    let e = c.endpoint("a", RunVars::default()).expect_err("secret");
    assert!(
        e.message.contains("unknown secret `secrets.api_kye`") && e.message.contains("api_key"),
        "{e}"
    );
    let e = c.endpoint("b", RunVars::default()).expect_err("root");
    assert!(
        e.message.contains("unknown name `respnse`") && e.message.contains("apis/test.yaml:6"),
        "{e}"
    );
    let e = c.endpoint("c", RunVars::default()).expect_err("sync");
    assert!(e.message.contains("`sync.floor` is not declared"), "{e}");
    assert!(m.requests().is_empty());
}

#[tokio::test(start_paused = true)]
async fn csv_null_values_per_endpoint() {
    let m =
        Mock::start(|_, _| Reply::raw(200, Some("text/csv"), "d,v\n2026-01-01,\n2026-01-02,.\n"))
            .await;
    let spec = format!(
        r#"
name: csvnulls
base_url: {}
endpoints:
  default:
    request: {{path: /a.csv}}
    decode: {{format: csv}}
    fields: {{missing: "record.v is none"}}
  dots:
    request: {{path: /a.csv}}
    decode: {{format: csv, null_values: ["."]}}
  none:
    request: {{path: /a.csv}}
    decode: {{format: csv, null_values: []}}
"#,
        m.url
    );
    let c = client(&spec, &[]);
    let run = |e: &'static str| {
        let c = c.clone();
        async move {
            let (recs, _) = c
                .endpoint(e, RunVars::default())
                .expect("run")
                .collect()
                .await
                .expect("ok");
            values(&recs)
        }
    };
    assert_eq!(
        run("default").await,
        [
            r#"{"d":"2026-01-01","v":null,"missing":true}"#,
            r#"{"d":"2026-01-02","v":".","missing":false}"#
        ]
    );
    assert_eq!(
        run("dots").await,
        [
            r#"{"d":"2026-01-01","v":""}"#,
            r#"{"d":"2026-01-02","v":null}"#
        ]
    );
    assert_eq!(
        run("none").await,
        [
            r#"{"d":"2026-01-01","v":""}"#,
            r#"{"d":"2026-01-02","v":"."}"#
        ]
    );
    let ex = c.explain(Some("dots")).expect("explain");
    assert_eq!(
        ex["endpoint"]["decode"]["null_values"],
        serde_json::json!(["."])
    );
}
