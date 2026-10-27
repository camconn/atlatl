//! Synthetic specs in the shapes of the ported REST vendors: both core's dependency
//! summary and the typed model parse them, and every endpoint's templates validate. No requests
//! are sent.

mod common;

use std::path::Path;
use std::sync::Arc;

use atlatl_core::config::Diagnostics;
use atlatl_core::config::apis::parse_api_spec;
use atlatl_core::config::node::parse_yaml;
use atlatl_http::spec::{AuthSpec, Over, PaginateKind};
use atlatl_http::{ApiClient, ApiSpec, RunVars};

const SPECS: &[(&str, &[&str])] = &[
    ("throttled", &["api_key"]),
    ("windowed", &["api_key"]),
    ("oauth_cursor", &["client_id", "client_secret"]),
    ("paged_queue", &["api_key"]),
    ("queue_chain", &["api_key"]),
    ("monthly", &[]),
    ("record_queue", &["api_key"]),
];

fn load(name: &str) -> (atlatl_core::config::node::Node, String) {
    let rel = format!("tests/fixtures/apis/{name}.yaml");
    let text =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(&rel)).expect("read");
    (
        parse_yaml(&text, Arc::from(Path::new(&rel))).expect("yaml"),
        rel,
    )
}

#[test]
fn ported_specs_parse_and_validate() {
    for (name, secrets) in SPECS {
        let (node, rel) = load(name);
        let mut d = Diagnostics::default();
        let summary = parse_api_spec(rel.clone(), &node, &mut d);
        assert!(!d.has_errors(), "{name}: {d:?}");
        let spec = ApiSpec::parse(&node).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(
            spec.endpoints.keys().collect::<Vec<_>>(),
            summary.endpoints.keys().collect::<Vec<_>>(),
            "{name}"
        );
        let secrets = secrets
            .iter()
            .map(|s| ((*s).to_owned(), format!("{s}-vendor-test")))
            .collect();
        let c = ApiClient::new(Arc::new(spec), secrets, common::opts()).expect("client");
        for ep in c.spec().endpoints.keys() {
            c.validate(ep, &RunVars::default())
                .unwrap_or_else(|e| panic!("{name}.{ep}: {e}"));
        }
    }
}

#[test]
fn ported_spec_shapes() {
    let (th, _) = load("throttled");
    let th = ApiSpec::parse(&th).expect("throttled");
    assert_eq!(
        th.endpoints["releases"]
            .request
            .params
            .keys()
            .collect::<Vec<_>>(),
        ["key", "fmt", "start", "end"]
    );
    assert_eq!(
        th.endpoints["symbols"].setup[0].request.params.len(),
        2,
        "defaults reach setup requests"
    );
    assert!(th.rate_limit.declared);

    let (pq, _) = load("paged_queue");
    let pq = ApiSpec::parse(&pq).expect("paged_queue");
    assert!(matches!(pq.auth, AuthSpec::Headers(_)));
    assert!(
        matches!(pq.endpoints["rounds"].for_each.as_ref().map(|f| &f.over), Some(Over::Queue { queue, .. }) if queue == "org_ids")
    );
    assert!(matches!(
        pq.endpoints["orgs"].paginate.as_ref().map(|p| &p.kind),
        Some(PaginateKind::Page { start: 1, .. })
    ));

    let (a, _) = load("oauth_cursor");
    let a = ApiSpec::parse(&a).expect("oauth_cursor");
    assert!(matches!(&a.auth, AuthSpec::OAuth2(o) if o.scopes.len() == 4));
    assert_eq!(
        a.endpoints["sessions"].rules.len(),
        1,
        "defaults.rules reach endpoints"
    );
}

#[test]
fn validate_catches_typos_in_ported_specs() {
    let (node, _) = load("record_queue");
    let spec = ApiSpec::parse(&node).expect("spec");
    let c = ApiClient::new(
        Arc::new(spec),
        [("apikey".to_owned(), "x".to_owned())]
            .into_iter()
            .collect(),
        common::opts(),
    )
    .expect("client");
    let e = c
        .validate("instruments", &RunVars::default())
        .expect_err("wrong secret name");
    assert!(
        e.message.contains("unknown secret `secrets.api_key`"),
        "{e}"
    );
}
