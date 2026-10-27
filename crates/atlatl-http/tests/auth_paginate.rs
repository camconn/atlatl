//! Auth flows and paginators against an in-process server (paused clock).

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use atlatl_core::ErrorClass;
use atlatl_http::RunVars;
use common::{Mock, Reply, client, values};

fn form(body: &[u8]) -> Vec<(String, String)> {
    reqwest::Url::parse(&format!("http://x/?{}", String::from_utf8_lossy(body)))
        .expect("form")
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
}

fn oauth_spec(url: &str, extra: &str) -> String {
    format!(
        r#"
base_url: {url}
auth:
  type: oauth2
  flow: client_credentials
  token_url: {url}/oauth2/token
  client_id: "{{{{ secrets.client_id }}}}"
  client_secret: "{{{{ secrets.client_secret }}}}"
  scopes: [sessions.list, sessions.read]
{extra}
endpoints:
  sessions: {{request: {{path: /sessions}}, records: {{path: data.items}}}}
"#
    )
}

#[tokio::test(start_paused = true)]
async fn oauth2_client_credentials_and_reacquire_on_401() {
    let tokens = Arc::new(AtomicU32::new(0));
    let t = tokens.clone();
    let m = Mock::start(move |r, _| {
        if r.path() == "/oauth2/token" {
            let n = t.fetch_add(1, Ordering::SeqCst) + 1;
            return Reply::json(200, &format!(r#"{{"access_token": "at-{n}-oauth-test", "expires_in": 3600, "token_type": "Bearer"}}"#));
        }
        match r.header("authorization") {
            Some("Bearer at-2-oauth-test") => Reply::json(200, r#"{"data": {"items": [{"id": "w1"}]}}"#),
            _ => Reply::json(401, r#"{"message": "expired"}"#),
        }
    })
    .await;
    let c = client(
        &oauth_spec(&m.url, ""),
        &[
            ("client_id", "cid"),
            ("client_secret", "csecret-oauth-test"),
        ],
    );
    let (recs, stats) = c
        .endpoint("sessions", RunVars::default())
        .expect("run")
        .collect()
        .await
        .expect("ok");
    assert_eq!(values(&recs), [r#"{"id":"w1"}"#]);
    assert_eq!(stats.auth_requests, 2);
    assert_eq!(stats.http.requests, 4);
    assert_eq!(stats.attempts[0].class, ErrorClass::Auth);
    let reqs = m.requests();
    let tok = &reqs[0];
    assert_eq!(tok.method, "POST");
    assert_eq!(
        tok.header("authorization"),
        Some(
            format!(
                "Basic {}",
                atlatl_http::funcs::base64(b"cid:csecret-oauth-test")
            )
            .as_str()
        )
    );
    assert_eq!(
        form(&tok.body),
        [
            ("grant_type".to_owned(), "client_credentials".to_owned()),
            ("scope".to_owned(), "sessions.list sessions.read".to_owned())
        ]
    );
    assert_eq!(atlatl_core::redact::redact("at-1-oauth-test"), "****");
}

#[tokio::test(start_paused = true)]
async fn oauth2_refreshes_proactively_before_expiry() {
    let m = Mock::start(move |r, n| {
        if r.path() == "/token" {
            let f = form(&r.body);
            let grant = f.iter().find(|(k, _)| k == "grant_type").map(|x| x.1.clone()).unwrap_or_default();
            return Reply::json(200, &format!(r#"{{"access_token": "{grant}-{n}", "expires_in": 120, "refresh_token": "rt-{n}-refresh-test"}}"#));
        }
        Reply::json(200, r#"[{"ok": true}]"#)
    })
    .await;
    let spec = format!(
        "base_url: {0}\nauth: {{type: oauth2, flow: password, token_url: {0}/token, client_id: app, client_auth: body, username: \"{{{{ secrets.u }}}}\", password: \"{{{{ secrets.p }}}}\", extra: {{takeExclusiveSignOnControl: \"true\"}}}}\nendpoints:\n  a: {{request: {{path: /a}}}}\n",
        m.url
    );
    let c = client(&spec, &[("u", "user"), ("p", "pw-refresh-test")]);
    c.endpoint("a", RunVars::default())
        .expect("run")
        .collect()
        .await
        .expect("1");
    tokio::time::advance(Duration::from_secs(59)).await;
    c.endpoint("a", RunVars::default())
        .expect("run")
        .collect()
        .await
        .expect("2");
    tokio::time::advance(Duration::from_secs(2)).await;
    c.endpoint("a", RunVars::default())
        .expect("run")
        .collect()
        .await
        .expect("3");
    let reqs = m.requests();
    let seq: Vec<String> = reqs
        .iter()
        .map(|r| match r.path() {
            "/token" => format!(
                "token:{}",
                form(&r.body)
                    .iter()
                    .find(|(k, _)| k == "grant_type")
                    .map(|x| x.1.clone())
                    .unwrap_or_default()
            ),
            _ => format!("api:{}", r.header("authorization").unwrap_or_default()),
        })
        .collect();
    assert_eq!(
        seq,
        [
            "token:password",
            "api:Bearer password-0",
            "api:Bearer password-0",
            "token:refresh_token",
            "api:Bearer refresh_token-3"
        ]
    );
    let first = form(&reqs[0].body);
    assert!(first.contains(&("client_id".to_owned(), "app".to_owned())));
    assert!(first.contains(&("password".to_owned(), "pw-refresh-test".to_owned())));
    assert!(first.contains(&("takeExclusiveSignOnControl".to_owned(), "true".to_owned())));
    assert!(
        form(&reqs[3].body).contains(&("refresh_token".to_owned(), "rt-0-refresh-test".to_owned()))
    );
}

#[tokio::test(start_paused = true)]
async fn oauth2_reauth_when_body_signals_expiry() {
    let m = Mock::start(move |r, n| {
        if r.path() == "/token" {
            return Reply::json(200, &format!(r#"{{"access_token": "tok{n}"}}"#));
        }
        match r.header("authorization") {
            Some("Bearer tok0") => Reply::json(200, r#"{"error": {"message": "token expired"}}"#),
            _ => Reply::json(200, r#"{"data": {"items": [{"id": 1}]}}"#),
        }
    })
    .await;
    let spec = oauth_spec(
        &m.url,
        "  reauth_when: 'response.json.error.message == \"token expired\"'",
    )
    .replace("/oauth2/token", "/token");
    let c = client(&spec, &[("client_id", "cid"), ("client_secret", "cs")]);
    let (recs, stats) = c
        .endpoint("sessions", RunVars::default())
        .expect("run")
        .collect()
        .await
        .expect("ok");
    assert_eq!(recs.len(), 1);
    assert_eq!(stats.auth_requests, 2);
}

#[tokio::test(start_paused = true)]
async fn hmac_signs_every_request_and_endpoint_override() {
    let m = Mock::start(|r, _| {
        if r.path() == "/public" {
            return match r.header("signature") {
                None => Reply::json(200, "[]"),
                Some(_) => Reply::json(400, "{}"),
            };
        }
        let canonical = format!(
            r#"{{"content":null,"path":"{}","query":"{}"}}"#,
            r.path(),
            r.query()
        );
        let expected = atlatl_http::funcs::base64(
            &atlatl_http::funcs::hmac("sha256", b"consumer-hmac-test", canonical.as_bytes())
                .expect("mac"),
        );
        match r.header("signature") {
            Some(s)
                if s == expected
                    && r.param("clientId").as_deref() == Some("CID")
                    && r.param("timestamp").is_some() =>
            {
                Reply::json(200, r#"[{"id": "acct-1", "balance": 12.3400}]"#)
            }
            _ => Reply::json(401, r#"{"detail": "bad signature"}"#),
        }
    })
    .await;
    let spec = format!(
        r#"
base_url: {}/api/v1
auth:
  type: hmac
  algorithm: sha256
  key: "{{{{ secrets.consumer_key }}}}"
  params: {{clientId: "{{{{ secrets.client_id }}}}", timestamp: "{{{{ timestamp }}}}"}}
  canonical: '{{{{ {{"content": body_json, "path": path, "query": query}} | tojson(sort_keys=true) }}}}'
  placement: {{header: Signature}}
endpoints:
  accounts: {{request: {{path: /accounts, params: {{userId: "{{{{ secrets.item.user_id }}}}", userSecret: "{{{{ secrets.item.user_secret }}}}"}}}}}}
  public: {{request: {{url: "{}/public"}}, auth: none}}
"#,
        m.url, m.url
    );
    let c = client(
        &spec,
        &[("consumer_key", "consumer-hmac-test"), ("client_id", "CID")],
    );
    let vars = RunVars {
        item_secrets: [
            ("user_id".to_owned(), "u-1".to_owned()),
            ("user_secret".to_owned(), "usec-hmac-test".to_owned()),
        ]
        .into_iter()
        .collect(),
        ..Default::default()
    };
    let (recs, _) = c
        .endpoint("accounts", vars)
        .expect("run")
        .collect()
        .await
        .expect("signed");
    assert_eq!(values(&recs), [r#"{"id":"acct-1","balance":12.3400}"#]);
    assert_eq!(
        m.requests()[0].param("userSecret").as_deref(),
        Some("usec-hmac-test")
    );
    assert_eq!(atlatl_core::redact::redact("usec-hmac-test"), "****");
    c.endpoint("public", RunVars::default())
        .expect("run")
        .collect()
        .await
        .expect("unsigned");
}

// ----------------------------------------------------------------------------- pagination

async fn paged(
    spec_tail: &str,
    f: impl Fn(&common::Req, usize) -> Reply + Send + Sync + 'static,
) -> (
    Mock,
    Result<(Vec<atlatl_http::Record>, atlatl_http::EndpointStats), atlatl_core::AtlatlError>,
) {
    let m = Mock::start(f).await;
    let spec = format!("base_url: {}\nendpoints:\n  e:\n{spec_tail}", m.url);
    let c = client(&spec, &[]);
    let r = c
        .endpoint("e", RunVars::default())
        .expect("run")
        .collect()
        .await;
    (m, r)
}

#[tokio::test(start_paused = true)]
async fn cursor_is_omitted_on_the_first_request() {
    let (m, r) = paged(
        "    request: {path: /sessions, params: {region: XA}}\n    paginate: {type: cursor, param: cursor_next, from: data.cursor.next}\n    records: {path: \"data.items[?period=='P1']\"}\n",
        |r, _| match r.param("cursor_next").as_deref() {
            None => Reply::json(200, r#"{"data": {"items": [{"id": 1, "period": "FY"}], "cursor": {"next": "c2"}}}"#),
            Some("c2") => Reply::json(200, r#"{"data": {"items": [{"id": 2, "period": "P1"}], "cursor": {"next": ""}}}"#),
            _ => Reply::json(500, "{}"),
        },
    )
    .await;
    let (recs, stats) = r.expect("ok");
    assert_eq!(values(&recs), [r#"{"id":2,"period":"P1"}"#]);
    assert_eq!(
        m.paths(),
        ["/sessions?region=XA", "/sessions?region=XA&cursor_next=c2"]
    );
    assert_eq!(
        stats.pages, 2,
        "an all-filtered page doesn't stop a cursor walk"
    );
}

#[tokio::test(start_paused = true)]
async fn page_with_stop_when() {
    let (m, r) = paged(
        "    request: {path: /orgs}\n    paginate: {type: page, param: pageNumber, stop_when: 'response.json.pagination.pageNumber >= response.json.pagination.numPages'}\n    records: {path: \"orgs[]\", primary_key: [orgId]}\n",
        |r, _| {
            let n: u32 = r.param("pageNumber").and_then(|p| p.parse().ok()).unwrap_or(0);
            Reply::json(200, &format!(r#"{{"orgs": [{{"orgId": "c{n}"}}], "pagination": {{"pageNumber": {n}, "numPages": 3}}}}"#))
        },
    )
    .await;
    assert_eq!(values(&r.expect("ok").0).len(), 3);
    assert_eq!(
        m.paths(),
        [
            "/orgs?pageNumber=1",
            "/orgs?pageNumber=2",
            "/orgs?pageNumber=3"
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn offset_with_total() {
    let (m, r) = paged(
        "    request: {path: /activities}\n    paginate: {type: offset, param: offset, limit_param: limit, limit: 2, total_from: pagination.total}\n    records: {path: data}\n",
        |r, _| {
            let o: u32 = r.param("offset").and_then(|p| p.parse().ok()).unwrap_or(0);
            let rows: Vec<String> = (o..(o + 2).min(5)).map(|i| format!(r#"{{"i": {i}}}"#)).collect();
            Reply::json(200, &format!(r#"{{"data": [{}], "pagination": {{"total": 5}}}}"#, rows.join(",")))
        },
    )
    .await;
    assert_eq!(r.expect("ok").0.len(), 5);
    assert_eq!(
        m.paths(),
        [
            "/activities?offset=0&limit=2",
            "/activities?offset=2&limit=2",
            "/activities?offset=4&limit=2"
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn next_url_and_link_header() {
    let (m, r) = paged(
        "    request: {path: /v3/trades, params: {limit: 2}}\n    paginate: {type: next_url, from: next_url}\n    records: {path: results}\n",
        |r, _| match r.param("cursor").as_deref() {
            None => Reply::json(200, r#"{"results": [{"t": 1}], "next_url": "/v3/trades?cursor=abc&limit=2"}"#),
            _ => Reply::json(200, r#"{"results": [{"t": 2}]}"#),
        },
    )
    .await;
    assert_eq!(r.expect("ok").0.len(), 2);
    assert_eq!(
        m.paths(),
        ["/v3/trades?limit=2", "/v3/trades?cursor=abc&limit=2"]
    );

    let (m, r) = paged(
        "    request: {path: /items}\n    paginate: {type: link_header}\n",
        |r, _| match r.param("page").as_deref() {
            None => Reply::json(200, r#"[{"p": 1}]"#).header(
                "Link",
                r#"</items?page=2>; rel="next", </items?page=9>; rel="last""#,
            ),
            _ => {
                Reply::json(200, r#"[{"p": 2}]"#).header("Link", r#"</items?page=1>; rel="first""#)
            }
        },
    )
    .await;
    assert_eq!(values(&r.expect("ok").0), [r#"{"p":1}"#, r#"{"p":2}"#]);
    assert_eq!(m.paths(), ["/items", "/items?page=2"]);
}

#[tokio::test(start_paused = true)]
async fn expression_paginator() {
    let (m, r) = paged(
        "    request: {path: /search, params: {from: \"{{ coalesce(page.from, 0) }}\"}}\n    paginate:\n      type: expression\n      next: {from: 'response.json.from + 2'}\n      stop_when: 'response.json.from + 2 >= response.json.total'\n    records: {path: hits}\n",
        |r, _| {
            let f: u32 = r.param("from").and_then(|p| p.parse().ok()).unwrap_or(0);
            Reply::json(200, &format!(r#"{{"from": {f}, "total": 5, "hits": [{{"f": {f}}}]}}"#))
        },
    )
    .await;
    assert_eq!(r.expect("ok").0.len(), 3);
    assert_eq!(
        m.paths(),
        ["/search?from=0", "/search?from=2", "/search?from=4"]
    );
}

#[tokio::test(start_paused = true)]
async fn guards_no_progress_max_pages_and_empty() {
    let (m, r) = paged(
        "    request: {path: /loop}\n    paginate: {type: cursor, param: c, from: next}\n",
        |_, _| Reply::json(200, r#"{"next": "same"}"#),
    )
    .await;
    let e = r.expect_err("no progress");
    assert_eq!(
        (e.class, e.code.as_deref()),
        (ErrorClass::Config, Some("no_progress"))
    );
    assert_eq!(m.requests().len(), 2);

    let (m, r) = paged(
        "    request: {path: /many}\n    paginate: {type: page, param: p, max_pages: 3}\n",
        |_, _| Reply::json(200, r#"[{"x": 1}]"#),
    )
    .await;
    let e = r.expect_err("max pages");
    assert_eq!(e.code.as_deref(), Some("max_pages"));
    assert_eq!(m.requests().len(), 3);

    let (m, r) = paged(
        "    request: {path: /pages}\n    paginate: {type: page, param: p, start: 0}\n",
        |r, _| match r.param("p").as_deref() {
            Some("0") | Some("1") => Reply::json(200, r#"[{"x": 1}]"#),
            _ => Reply::json(200, "[]"),
        },
    )
    .await;
    assert_eq!(r.expect("ok").0.len(), 2);
    assert_eq!(m.requests().len(), 3);
}

#[tokio::test(start_paused = true)]
async fn skip_ends_the_item_mid_pagination() {
    let (m, r) = paged(
        "    request: {path: /x}\n    paginate: {type: page, param: p}\n    rules: [{when: \"response.status == 404\", action: skip}]\n",
        |r, _| match r.param("p").as_deref() {
            Some("1") => Reply::json(200, r#"[{"x": 1}]"#),
            _ => Reply::json(404, "{}"),
        },
    )
    .await;
    let (recs, stats) = r.expect("ok");
    assert_eq!(recs.len(), 1);
    assert_eq!(stats.http.skipped_items, 1);
    assert_eq!(m.requests().len(), 2);
}
