//! `for_each`, setup, emit, sync folds, skip guard and row-mode rejects through [`Driver`].

mod common;

use atlatl_core::ErrorClass;
use atlatl_http::iterate::{Chunk, OpenReject};
use atlatl_http::json::JValue;
use atlatl_http::{DriveOptions, DriveOutcome, Driver, RangeOverride, RunVars};
use common::{Mock, Reply, client};

fn j(s: &str) -> JValue {
    atlatl_http::json::parse_str(s).expect("json")
}

async fn drive(
    c: &atlatl_http::ApiClient,
    ep: &str,
    opts: DriveOptions,
) -> Result<(Vec<Chunk>, DriveOutcome), atlatl_core::AtlatlError> {
    let mut d = Driver::start(c, ep, opts).await?;
    let mut out = Vec::new();
    while let Some(ch) = d.next().await? {
        out.push(ch);
    }
    Ok((out, d.finish()?))
}

fn recs(chunks: &[Chunk]) -> Vec<String> {
    chunks
        .iter()
        .flat_map(|c| c.records.iter().map(|r| r.record.to_string()))
        .collect()
}

#[tokio::test(start_paused = true)]
async fn list_items_concurrency_dedup_and_emit() {
    let m = Mock::start(|r, _| {
        let ex = r.param("exchange").unwrap_or_default();
        Reply::json(200, &format!(r#"[{{"id": 900000000000000000001, "ex": "{ex}", "px": 1.10}}, {{"id": 7, "ex": "{ex}"}}]"#))
    })
    .await;
    let spec = format!(
        r#"
base_url: {}
rate_limit: {{max_in_flight: 4}}
endpoints:
  listings:
    for_each: {{over: [XA, XB, XC], as: exchange, concurrency: 3}}
    request: {{path: /l, params: {{exchange: "{{{{ exchange }}}}"}}}}
    records: {{path: "[]", primary_key: [id]}}
    fields: {{venue: item}}
    emit: {{ids: record.id, whole: record}}
"#,
        m.url
    );
    let c = client(&spec, &[]);
    let (chunks, out) = drive(&c, "listings", DriveOptions::default())
        .await
        .expect("drive");
    // Dedup by PK spans items: the first item's records win; item order is kept under concurrency.
    assert_eq!(
        recs(&chunks),
        vec![
            r#"{"id":900000000000000000001,"ex":"XA","px":1.10,"venue":"XA"}"#,
            r#"{"id":7,"ex":"XA","venue":"XA"}"#
        ]
    );
    let ords: Vec<i64> = chunks
        .iter()
        .flat_map(|c| c.records.iter().map(|r| r.ordinal))
        .collect();
    assert_eq!(ords, vec![0, 1]);
    let ids: Vec<String> = chunks
        .iter()
        .flat_map(|c| c.emits.iter())
        .filter(|(q, _)| q == "ids")
        .map(|(_, v)| v.to_string())
        .collect();
    assert_eq!(
        ids,
        vec!["900000000000000000001", "7"],
        "exact values, no float64 round trip"
    );
    assert_eq!(out.items, 3);
    assert_eq!(out.stats.http.requests, 3);
    assert_eq!(out.stats.duplicates, 4);
}

#[tokio::test(start_paused = true)]
async fn setup_collect_feeds_for_each_over_state() {
    let m = Mock::start(|r, _| match r.path() {
        "/country" => Reply::json(
            200,
            r#"[{"Country": "United States"}, {"Country": "Côte d'Ivoire"}, {"Country": null}]"#,
        ),
        p => Reply::json(
            200,
            &format!(
                r#"[{{"Symbol": "{}"}}]"#,
                p.rsplit('/').next().unwrap_or_default()
            ),
        ),
    })
    .await;
    let spec = format!(
        r#"
base_url: {}
rate_limit: {{min_interval: 250ms}}
endpoints:
  symbology:
    setup:
      - name: countries
        request: {{path: /country}}
        records: {{path: "[].Country"}}
        collect: countries
    for_each: {{over: state.countries, as: country}}
    request: {{path: "/markets/symbology/country/{{{{ country }}}}"}}
    records: {{path: "[]", primary_key: [Symbol]}}
"#,
        m.url
    );
    let c = client(&spec, &[]);
    let (chunks, out) = drive(&c, "symbology", DriveOptions::default())
        .await
        .expect("drive");
    assert_eq!(
        m.paths(),
        vec![
            "/country",
            "/markets/symbology/country/United%20States",
            "/markets/symbology/country/C%C3%B4te%20d%27Ivoire"
        ]
    );
    assert_eq!(recs(&chunks).len(), 2);
    assert_eq!(out.stats.http.requests, 3, "setup requests are counted");
    assert_eq!(out.stats.records, 2, "setup values are not records");
    // Setup and the first iteration share the limiter: no back-to-back requests.
    let at: Vec<_> = m.requests().iter().map(|r| r.at).collect();
    assert!(at[1] - at[0] >= std::time::Duration::from_millis(250));
}

fn monthly_spec(url: &str) -> String {
    format!(
        r#"
base_url: {url}
endpoints:
  cal:
    for_each:
      over: 'range(coalesce(sync.floor, date("2025-01-01")), date("2025-12-31"), "3mo")'
      as: window_start
    request:
      path: /cal
      params:
        d1: '{{{{ window_start | date_format("%Y-%m-%d") }}}}'
    records: {{path: "rows", primary_key: [id]}}
    sync:
      floor: {{value: 'date_trunc(date_add(now(), -6, "month"), "quarter")', aggregate: set}}
      max_id: {{value: record.id, aggregate: max}}
      since: {{value: response.json.meta.updated, aggregate: last}}
      never: {{value: record.missing, aggregate: max}}
"#
    )
}

#[tokio::test(start_paused = true)]
async fn range_sync_folds_and_stored_values() {
    let m = Mock::start(|r, n| {
        let d1 = r.param("d1").unwrap_or_default();
        Reply::json(
            200,
            &format!(
                r#"{{"meta": {{"updated": "u{n}"}}, "rows": [{{"id": {}, "d": "{d1}"}}]}}"#,
                10 + n
            ),
        )
    })
    .await;
    let c = client(&monthly_spec(&m.url), &[]);
    let (_, out) = drive(&c, "cal", DriveOptions::default())
        .await
        .expect("drive");
    let d1s: Vec<String> = m.requests().iter().filter_map(|r| r.param("d1")).collect();
    assert_eq!(
        d1s,
        vec!["2025-01-01", "2025-04-01", "2025-07-01", "2025-10-01"]
    );
    assert_eq!(
        out.sync["floor"].to_string(),
        r#""2026-01-01T00:00:00Z""#,
        "clock is 2026-09-28"
    );
    assert_eq!(out.sync["max_id"].to_string(), "13");
    assert_eq!(out.sync["since"].to_string(), r#""u3""#);
    assert!(
        !out.sync.contains_key("never"),
        "a fold over zero inputs stores nothing new"
    );
    assert!(out.sync_changed);

    // Next run starts from the stored floor; the untouched stored value is kept.
    let m = Mock::start(|_, _| Reply::json(200, r#"{"meta": {}, "rows": [{"id": 5}]}"#)).await;
    let c = client(&monthly_spec(&m.url), &[]);
    let mut vars = RunVars::default();
    vars.sync
        .insert("floor".into(), j(r#""2025-10-01T00:00:00Z""#));
    vars.sync.insert("never".into(), j("42"));
    vars.sync.insert("max_id".into(), j("13"));
    let (_, out) = drive(
        &c,
        "cal",
        DriveOptions {
            vars: vars.clone(),
            ..Default::default()
        },
    )
    .await
    .expect("drive");
    assert_eq!(m.requests().len(), 1, "one window from the floor");
    assert_eq!(out.sync["never"].to_string(), "42");
    assert_eq!(
        out.sync["max_id"].to_string(),
        "5",
        "a normal run's max replaces the stored one"
    );

    // --range replaces the bounds; max folds don't regress under a backfill.
    let m = Mock::start(|_, _| {
        Reply::json(200, r#"{"meta": {"updated": "old"}, "rows": [{"id": 1}]}"#)
    })
    .await;
    let c = client(&monthly_spec(&m.url), &[]);
    let range = Some(RangeOverride::parse("2020-01-01..2020-06-30").expect("r"));
    let (_, out) = drive(
        &c,
        "cal",
        DriveOptions {
            vars,
            range,
            ..Default::default()
        },
    )
    .await
    .expect("drive");
    let d1s: Vec<String> = m.requests().iter().filter_map(|r| r.param("d1")).collect();
    assert_eq!(d1s, vec!["2020-01-01", "2020-04-01"]);
    assert!(out.range_applied);
    assert_eq!(out.sync["max_id"].to_string(), "13");
    assert!(
        !out.sync.contains_key("since"),
        "`last` isn't taken from a backfill"
    );
}

#[tokio::test(start_paused = true)]
async fn snapshot_only_rejects_range() {
    let spec = "base_url: http://127.0.0.1:9\nendpoints:\n  pos:\n    history: snapshot_only\n    request: {path: /p}\n";
    let c = client(spec, &[]);
    let err = Driver::start(
        &c,
        "pos",
        DriveOptions {
            range: Some(RangeOverride::parse("1..2").expect("r")),
            ..Default::default()
        },
    )
    .await
    .expect_err("config");
    assert_eq!(err.class, ErrorClass::Config);
    assert!(err.message.contains("snapshot_only"), "{}", err.message);
}

fn skip_spec(url: &str, extra: &str) -> String {
    format!(
        r#"
base_url: {url}
endpoints:
  quotes:
    for_each: {{over: [a, b, c, d], as: sym}}
    request: {{path: "/q/{{{{ sym }}}}"}}
    records: {{path: "[]"}}
    rules:
      - {{when: "response.status == 404", action: skip, message: "no data for {{{{ sym }}}}"}}
{extra}
"#
    )
}

#[tokio::test(start_paused = true)]
async fn skip_guard_and_skip_rejects() {
    let m = Mock::start(|r, _| {
        if r.path() == "/q/a" {
            Reply::json(200, r#"[{"s": "a"}]"#)
        } else {
            Reply::json(404, "{}")
        }
    })
    .await;
    let c = client(
        &skip_spec(&m.url, "    max_skipped: {ratio: 0.5, min_items: 4}"),
        &[],
    );
    let err = drive(&c, "quotes", DriveOptions::default())
        .await
        .expect_err("guard");
    assert_eq!(
        (err.class, err.code.as_deref()),
        (ErrorClass::Data, Some("too_many_skipped"))
    );

    let c = client(&skip_spec(&m.url, "    record_skips: true"), &[]);
    let (_, out) = drive(&c, "quotes", DriveOptions::default())
        .await
        .expect("drive");
    assert_eq!(out.skipped.len(), 3);
    assert_eq!(out.skipped[0].reason, "no data for b");
    assert_eq!(out.rejects.len(), 3);
    assert!(out.rejects.iter().all(|r| r.class == "skipped"));
    let b = out
        .rejects
        .iter()
        .find(|r| r.request.get("item").and_then(JValue::as_str) == Some("b"))
        .expect("b")
        .clone();

    // Next run: `b` has data now and is resolved; an open reject for an item no longer iterated
    // (`z`) is replayed on its own.
    let m = Mock::start(|r, _| {
        if r.path() == "/q/c" || r.path() == "/q/d" {
            Reply::json(404, "{}")
        } else {
            Reply::json(200, &format!(r#"[{{"s": "{}"}}]"#, &r.path()[3..]))
        }
    })
    .await;
    let c = client(&skip_spec(&m.url, "    record_skips: true"), &[]);
    let z = OpenReject {
        key: "zkey".into(),
        request: j(r#"{"kind": "skip", "endpoint": "quotes", "item": "z"}"#),
        class: "skipped".into(),
    };
    let open = vec![
        OpenReject {
            key: b.key.clone(),
            request: b.request.clone(),
            class: "skipped".into(),
        },
        z,
    ];
    let (chunks, out) = drive(
        &c,
        "quotes",
        DriveOptions {
            open_rejects: open,
            ..Default::default()
        },
    )
    .await
    .expect("drive");
    assert_eq!(
        recs(&chunks),
        vec![r#"{"s":"a"}"#, r#"{"s":"b"}"#, r#"{"s":"z"}"#]
    );
    assert_eq!(out.resolved, {
        let mut v = vec![b.key, "zkey".to_owned()];
        v.sort();
        v
    });
    assert_eq!(out.items, 4, "replays aren't iteration items");
    assert_eq!(out.rejects.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn row_mode_keeps_good_rows_and_replays_bad_ones() {
    let m = Mock::start(|_, _| {
        Reply::raw(
            200,
            Some("application/x-ndjson"),
            "{\"id\": 1}\n{\"id\": 2,\n{\"id\": 3}\n",
        )
    })
    .await;
    let spec = format!(
        "base_url: {}\nendpoints:\n  feed:\n    request: {{path: /f, params: {{k: \"{{{{ secrets.key }}}}\"}}}}\n    decode: {{format: jsonl}}\n    on_decode_error: {{retry: row}}\n",
        m.url
    );
    let c = client(&spec, &[("key", "sekrit-row-mode")]);
    let (chunks, out) = drive(&c, "feed", DriveOptions::default())
        .await
        .expect("drive");
    assert_eq!(recs(&chunks), vec![r#"{"id":1}"#, r#"{"id":3}"#]);
    assert_eq!(
        m.requests().len(),
        5,
        "the request is refetched up to the attempt budget first"
    );
    assert_eq!(out.rejects.len(), 1);
    let rj = out.rejects[0].clone();
    assert_eq!(rj.class, "decode");
    assert_eq!(rj.raw, "{\"id\": 2,");
    assert_eq!(
        rj.request.get("row").map(|r| r.to_string()).as_deref(),
        Some("1")
    );
    assert!(!rj.request.to_string().contains("sekrit"), "{}", rj.request);

    // Request mode fails the endpoint; `none` fails without refetching.
    let c = client(
        &spec.replace("retry: row", "retry: request"),
        &[("key", "sekrit-row-mode")],
    );
    let err = drive(&c, "feed", DriveOptions::default())
        .await
        .expect_err("decode");
    assert_eq!(
        err.class,
        ErrorClass::Throttled,
        "repeated decode failures count as throttling"
    );
    let before = m.requests().len();
    let c = client(
        &spec.replace("retry: row", "retry: none"),
        &[("key", "sekrit-row-mode")],
    );
    assert_eq!(
        drive(&c, "feed", DriveOptions::default())
            .await
            .expect_err("decode")
            .class,
        ErrorClass::Decode
    );
    assert_eq!(m.requests().len() - before, 1);

    // A later run replays the stored descriptor; the row decodes now and is resolved.
    let m2 = Mock::start(|_, _| {
        Reply::raw(
            200,
            Some("application/x-ndjson"),
            "{\"id\": 1}\n{\"id\": 2}\n{\"id\": 3}\n",
        )
    })
    .await;
    let mut request = rj.request.clone();
    if let JValue::Object(o) = &mut request {
        let url = o["url"].as_str().expect("url").replace(&m.url, &m2.url);
        o.insert("url".into(), url.into());
    }
    let spec2 = spec.replace(&m.url, &m2.url);
    let c = client(&spec2, &[("key", "sekrit-row-mode")]);
    let open = vec![OpenReject {
        key: rj.key.clone(),
        request,
        class: "decode".into(),
    }];
    let (chunks, out) = drive(
        &c,
        "feed",
        DriveOptions {
            open_rejects: open,
            ..Default::default()
        },
    )
    .await
    .expect("drive");
    assert_eq!(
        recs(&chunks),
        vec![r#"{"id":1}"#, r#"{"id":2}"#, r#"{"id":3}"#, r#"{"id":2}"#]
    );
    assert_eq!(out.resolved, vec![rj.key]);
    assert!(out.rejects.is_empty());
    assert!(
        m2.requests()[1].param("k").as_deref() == Some("sekrit-row-mode"),
        "secrets are unsealed on replay"
    );
}

#[tokio::test(start_paused = true)]
async fn batches_bind_the_list() {
    let m = Mock::start(|r, _| {
        Reply::json(
            200,
            &format!(r#"[{{"q": "{}"}}]"#, r.param("ids").unwrap_or_default()),
        )
    })
    .await;
    let spec = format!(
        "base_url: {}\nendpoints:\n  b:\n    for_each: {{over: [1, 2, 3, 4, 5], as: id, batch: {{size: 2, as: ids}}}}\n    request: {{path: /b, params: {{ids: \"{{{{ ids | join(',') }}}}\"}}}}\n    records: {{path: \"[]\"}}\n",
        m.url
    );
    let c = client(&spec, &[]);
    let (chunks, out) = drive(&c, "b", DriveOptions::default())
        .await
        .expect("drive");
    assert_eq!(
        recs(&chunks),
        vec![r#"{"q":"1,2"}"#, r#"{"q":"3,4"}"#, r#"{"q":"5"}"#]
    );
    assert_eq!(out.items, 3);
}

#[tokio::test(start_paused = true)]
async fn given_items_and_stop() {
    let m = Mock::start(|r, _| {
        if r.param("i").as_deref() == Some("2") {
            Reply::json(200, r#"{"stop": true}"#)
        } else {
            Reply::json(200, r#"[{"x": 1}]"#)
        }
    })
    .await;
    let spec = format!(
        "base_url: {}\nendpoints:\n  c:\n    for_each: {{over: {{queue: ids}}, as: i}}\n    request: {{path: /c, params: {{i: \"{{{{ i }}}}\"}}}}\n    records: {{path: \"[]\"}}\n    rules:\n      - {{when: \"response.json.stop\", action: stop}}\n",
        m.url
    );
    let c = client(&spec, &[]);
    let items = Some(vec![j("1"), j("2"), j("3")]);
    let (chunks, out) = drive(
        &c,
        "c",
        DriveOptions {
            items,
            ..Default::default()
        },
    )
    .await
    .expect("drive");
    assert_eq!(recs(&chunks).len(), 1);
    assert_eq!(m.requests().len(), 2, "stop ends the endpoint");
    assert!(out.stats.stopped);
}

#[tokio::test(start_paused = true)]
async fn secret_item_fields_bind_as_secrets_and_are_sealed_in_rejects() {
    let m = Mock::start(|r, _| {
        let ok = matches!(
            (
                r.param("userId").as_deref(),
                r.param("userSecret").as_deref()
            ),
            (Some("u1"), Some("us-iter-secret-1")) | (Some("u2"), Some("us-iter-secret-2"))
        );
        match (ok, r.param("userId").as_deref()) {
            (false, _) => Reply::json(401, r#"{"error": "bad user secret"}"#),
            (true, Some("u1")) => Reply::raw(
                200,
                Some("application/x-ndjson"),
                "{\"id\": 1}\n{\"id\": 2,\n",
            ),
            _ => Reply::raw(200, Some("application/x-ndjson"), "{\"id\": 3}\n"),
        }
    })
    .await;
    let spec = format!(
        r#"
base_url: {}
endpoints:
  accounts:
    for_each:
      over: [{{user_id: u1, user_secret: us-iter-secret-1}}, {{user_id: u2, user_secret: us-iter-secret-2}}]
      as: user
      secret: [user_secret]
    request: {{path: /accounts, params: {{userId: "{{{{ user.user_id }}}}", userSecret: "{{{{ secrets.item.user_secret }}}}"}}}}
    decode: {{format: jsonl}}
    on_decode_error: {{retry: row}}
    fields: {{who: item}}
    emit: {{users: item}}
"#,
        m.url
    );
    let c = client(&spec, &[]);
    let (chunks, out) = drive(&c, "accounts", DriveOptions::default())
        .await
        .expect("drive");
    assert_eq!(
        recs(&chunks),
        vec![
            r#"{"id":1,"who":{"user_id":"u1"}}"#,
            r#"{"id":3,"who":{"user_id":"u2"}}"#
        ]
    );
    let emits: Vec<String> = chunks
        .iter()
        .flat_map(|c| c.emits.iter().map(|(_, v)| v.to_string()))
        .collect();
    assert!(emits.iter().all(|e| !e.contains("secret")), "{emits:?}");
    assert_eq!(out.rejects.len(), 1);
    let desc = out.rejects[0].request.to_string();
    assert!(!desc.contains("us-iter-secret"), "{desc}");
    assert!(
        desc.contains("userSecret={{secrets.item.user_secret}}"),
        "{desc}"
    );
    assert_eq!(
        atlatl_core::redact::redact("us-iter-secret-2"),
        "****",
        "registered"
    );

    // Replaying the reject restores the item's secret from this run's items.
    let before = m.requests().len();
    let open = vec![OpenReject {
        key: out.rejects[0].key.clone(),
        request: out.rejects[0].request.clone(),
        class: "decode".into(),
    }];
    let (_, out) = drive(
        &c,
        "accounts",
        DriveOptions {
            open_rejects: open,
            ..Default::default()
        },
    )
    .await
    .expect("replay");
    let replayed = &m.requests()[before..];
    assert!(
        replayed
            .iter()
            .filter(|r| r.param("userId").as_deref() == Some("u1"))
            .all(|r| r.param("userSecret").as_deref() == Some("us-iter-secret-1")),
        "{:?}",
        replayed
            .iter()
            .map(|r| r.target.clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(out.rejects.len(), 1, "still failing, not an error");
}

#[tokio::test(start_paused = true)]
async fn oauth2_tokens_are_per_item_credentials() {
    let m = Mock::start(|r, _| {
        if r.path() == "/token" {
            let form = String::from_utf8_lossy(&r.body).into_owned();
            let who = if form.contains("client_id=cid-a") && form.contains("cs-a-oauth-item") {
                "a"
            } else if form.contains("client_id=cid-b") && form.contains("cs-b-oauth-item") {
                "b"
            } else {
                return Reply::json(401, r#"{"error": "invalid_client"}"#);
            };
            return Reply::json(
                200,
                &format!(r#"{{"access_token": "at-{who}-oauth-item", "expires_in": 900}}"#),
            );
        }
        let owner = r.path().trim_start_matches("/accounts/");
        match r.header("authorization") {
            Some(h) if h == format!("Bearer at-{owner}-oauth-item") => {
                Reply::json(200, &format!(r#"[{{"owner": "{owner}"}}]"#))
            }
            _ => Reply::json(403, r#"{"error": "not your account"}"#),
        }
    })
    .await;
    let spec = format!(
        r#"
base_url: {0}
auth:
  type: oauth2
  flow: client_credentials
  token_url: {0}/token
  client_auth: body
  client_id: "{{{{ secrets.item.client_id }}}}"
  client_secret: "{{{{ secrets.item.client_secret }}}}"
endpoints:
  accounts:
    for_each:
      over:
        - {{member: a, client_id: cid-a, client_secret: cs-a-oauth-item}}
        - {{member: b, client_id: cid-b, client_secret: cs-b-oauth-item}}
        - {{member: a, client_id: cid-a, client_secret: cs-a-oauth-item, again: true}}
      as: m
      secret: [client_id, client_secret]
    request: {{path: "/accounts/{{{{ m.member }}}}"}}
    records: {{path: "[]"}}
"#,
        m.url
    );
    let c = client(&spec, &[]);
    let (chunks, out) = drive(&c, "accounts", DriveOptions::default())
        .await
        .expect("drive");
    assert_eq!(
        recs(&chunks),
        [r#"{"owner":"a"}"#, r#"{"owner":"b"}"#, r#"{"owner":"a"}"#]
    );
    assert!(out.rejects.is_empty());
    let tokens = m.requests().iter().filter(|r| r.path() == "/token").count();
    assert_eq!(
        tokens, 2,
        "one token per credential set, reused by the third item"
    );
    assert_eq!(atlatl_core::redact::redact("cs-b-oauth-item"), "****");
}

#[tokio::test(start_paused = true)]
async fn for_each_validates_once_not_per_item() {
    let m = Mock::start(|r, _| {
        Reply::json(
            200,
            &format!(r#"[{{"id": "{}"}}]"#, r.param("n").unwrap_or_default()),
        )
    })
    .await;
    let items: Vec<String> = (0..50).map(|i| i.to_string()).collect();
    let spec = format!(
        r#"
base_url: {}
endpoints:
  one:
    for_each: {{over: [{}], as: n}}
    request: {{path: /x, params: {{n: "{{{{ n }}}}"}}}}
    records: {{path: "[]"}}
"#,
        m.url,
        items.join(", ")
    );
    let c = client(&spec, &[]);
    let (chunks, out) = drive(&c, "one", DriveOptions::default())
        .await
        .expect("drive");
    assert_eq!((recs(&chunks).len(), out.items), (50, 50));
    assert_eq!(c.validation_runs(), 1, "50 items, one validation");
    // A different set of global names is a different validation.
    let mut v = RunVars::default();
    v.globals.insert("vars".into(), j("{}"));
    c.validate("one", &v).expect("valid");
    assert_eq!(c.validation_runs(), 2);
    c.validate("one", &v).expect("valid");
    assert_eq!(c.validation_runs(), 2);
}
