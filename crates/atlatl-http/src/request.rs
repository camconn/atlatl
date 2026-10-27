//! Request building (07 §4): request-phase templating, omission of null/`""`, URL encoding of
//! path interpolations and typed whole-value templates.

use std::time::Duration;

use atlatl_core::AtlatlError;
use reqwest::Url;

use crate::expr::{Engine, Scope};
use crate::funcs::urlencode_str;
use crate::json::JValue;
use crate::spec::{Body, Method, RequestSpec};

#[derive(Clone, Debug, PartialEq)]
pub struct PreparedBody {
    pub bytes: Vec<u8>,
    pub content_type: &'static str,
    /// The rendered JSON body, for signers.
    pub json: Option<JValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Prepared {
    pub method: Method,
    pub url: Url,
    pub headers: Vec<(String, String)>,
    pub body: Option<PreparedBody>,
    pub timeout: Option<Duration>,
}

fn scalar_text(v: &JValue) -> Option<String> {
    match v {
        JValue::Null => None,
        JValue::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// `k=v` pairs from a rendered params/form map; lists repeat the key.
fn pairs(
    engine: &Engine,
    map: &indexmap::IndexMap<String, serde_json::Value>,
    scope: &Scope,
    keep_empty: bool,
) -> Result<Vec<(String, String)>, AtlatlError> {
    let mut out = Vec::new();
    for (k, t) in map {
        match engine.render_value(t, scope, keep_empty)? {
            None => {}
            Some(JValue::Array(a)) => {
                for x in &a {
                    if let Some(s) = scalar_text(x).filter(|s| keep_empty || !s.is_empty()) {
                        out.push((k.clone(), s));
                    }
                }
            }
            Some(v) => {
                if let Some(s) = scalar_text(&v) {
                    out.push((k.clone(), s));
                }
            }
        }
    }
    Ok(out)
}

fn form_encode(pairs: &[(String, String)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", urlencode_str(k), urlencode_str(v)))
        .collect::<Vec<_>>()
        .join("&")
}

fn join(base: &str, path: &str) -> String {
    if path.is_empty() {
        return base.to_owned();
    }
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

pub fn build(
    engine: &Engine,
    base_url: Option<&str>,
    req: &RequestSpec,
    scope: &Scope,
    default_timeout: Option<Duration>,
) -> Result<Prepared, AtlatlError> {
    let raw = match (&req.url, &req.path) {
        (Some(u), _) => engine.render_path(u, scope)?,
        (None, p) => {
            let base = base_url.ok_or_else(|| {
                AtlatlError::config("the request has no `url`, and the spec has no `base_url`")
            })?;
            let base = engine.render(base, scope)?;
            let path = match p {
                Some(p) => engine.render_path(p, scope)?,
                None => String::new(),
            };
            join(&base, &path)
        }
    };
    let mut url = Url::parse(&raw).map_err(|e| {
        AtlatlError::config(format!(
            "invalid request URL `{}`: {e}",
            atlatl_core::redact::redact(&raw)
        ))
    })?;
    let params = pairs(engine, &req.params, scope, req.keep_empty)?;
    if !params.is_empty() {
        let mut q = url.query().map(str::to_owned).unwrap_or_default();
        if !q.is_empty() {
            q.push('&');
        }
        q.push_str(&form_encode(&params));
        url.set_query(Some(&q));
    }
    let mut headers = Vec::new();
    for (k, t) in &req.headers {
        if let Some(v) = engine.render_value(t, scope, req.keep_empty)?
            && let Some(s) = scalar_text(&v)
        {
            headers.push((k.clone(), s));
        }
    }
    let body = match &req.body {
        None => None,
        Some(Body::Json(t)) => {
            let v = engine
                .render_value(t, scope, req.keep_empty)?
                .unwrap_or(JValue::Null);
            Some(PreparedBody {
                bytes: v.to_string().into_bytes(),
                content_type: "application/json",
                json: Some(v),
            })
        }
        Some(Body::Form(m)) => Some(PreparedBody {
            bytes: form_encode(&pairs(engine, m, scope, req.keep_empty)?).into_bytes(),
            content_type: "application/x-www-form-urlencoded",
            json: None,
        }),
        Some(Body::Text(t)) => Some(PreparedBody {
            bytes: engine.render(t, scope)?.into_bytes(),
            content_type: "text/plain; charset=utf-8",
            json: None,
        }),
    };
    Ok(Prepared {
        method: req.method,
        url,
        headers,
        body,
        timeout: req.timeout.map(|d| d.0).or(default_timeout),
    })
}

impl Prepared {
    /// Sets (`Some`) or removes (`None`) every occurrence of a query parameter.
    pub fn set_param(&mut self, key: &str, value: Option<&str>) {
        let raw: Vec<String> = self
            .url
            .query()
            .unwrap_or_default()
            .split('&')
            .filter(|p| !p.is_empty())
            .filter(|p| {
                let k = p.split('=').next().unwrap_or_default();
                percent_encoding::percent_decode_str(&k.replace('+', " ")).decode_utf8_lossy()
                    != key
            })
            .map(str::to_owned)
            .collect();
        let mut q = raw.join("&");
        if let Some(v) = value {
            if !q.is_empty() {
                q.push('&');
            }
            q.push_str(&format!("{}={}", urlencode_str(key), urlencode_str(v)));
        }
        self.url.set_query((!q.is_empty()).then_some(q.as_str()));
    }

    pub fn set_header(&mut self, key: &str, value: String) {
        self.headers.retain(|(k, _)| !k.eq_ignore_ascii_case(key));
        self.headers.push((key.to_owned(), value));
    }

    /// Identity for no-progress detection.
    pub fn fingerprint(&self) -> (Method, String, Option<Vec<u8>>) {
        (
            self.method,
            self.url.to_string(),
            self.body.as_ref().map(|b| b.bytes.clone()),
        )
    }

    /// Redacted `METHOD url[ #sha256-prefix-of-body]`, for skip reports and raw rows.
    pub fn key(&self) -> String {
        let mut k = format!(
            "{} {}",
            self.method.as_str(),
            atlatl_core::redact::redact(self.url.as_str())
        );
        if let Some(b) = &self.body {
            k.push_str(" #");
            k.push_str(&crate::funcs::hex(&crate::funcs::sha256(&b.bytes))[..16]);
        }
        k
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    macro_rules! json {
        ($($t:tt)+) => { JValue::from(serde_json::json!($($t)+)) };
    }

    use super::*;

    fn req(yaml: &str) -> RequestSpec {
        let n =
            atlatl_core::config::node::parse_yaml(yaml, Arc::from(std::path::Path::new("t.yaml")))
                .expect("yaml");
        atlatl_core::config::de::from_node(&n).expect("request")
    }

    fn scope() -> Scope {
        let mut s = Scope::new();
        s.set_json("secrets", Arc::new(json!({"k": "s3cr3t"})));
        s.set_json("page", Arc::new(json!({"cursor": null, "n": 2})));
        s.set_json(
            "item",
            Arc::new(json!({"country": "United States", "figi": "BBG 1/2", "ids": [1, 2]})),
        );
        s
    }

    #[test]
    fn params_omit_encode_and_type() {
        let r = req(r#"
path: "/markets/{{ item.country }}/x/{{ item.figi | safe }}"
params: {c: "{{ secrets.k }}", next: "{{ page.cursor }}", empty: "", size: 1000, n: "{{ page.n }}", q: "a&b c", id: "{{ item.ids }}"}
headers: {Accept: application/json, X-Empty: "{{ page.cursor }}"}
"#);
        let p = build(
            &Engine::default(),
            Some("https://api.x.com/v2/"),
            &r,
            &scope(),
            None,
        )
        .expect("build");
        assert_eq!(
            p.url.as_str(),
            "https://api.x.com/v2/markets/United%20States/x/BBG%201/2?c=s3cr3t&size=1000&n=2&q=a%26b%20c&id=1&id=2"
        );
        assert_eq!(
            p.headers,
            [("Accept".to_owned(), "application/json".to_owned())]
        );
        let keep = req("path: /a\nkeep_empty: true\nparams: {e: \"\", n: \"{{ page.cursor }}\"}\n");
        let p = build(&Engine::default(), Some("https://x"), &keep, &scope(), None).expect("keep");
        assert_eq!(p.url.as_str(), "https://x/a?e=");
    }

    #[test]
    fn bodies() {
        let r = req(
            "method: POST\nurl: https://x/y\nbody: {json: {a: \"{{ page.n }}\", b: \"{{ page.cursor }}\", c: [\"{{ item.country }}\"]}}\n",
        );
        let p = build(&Engine::default(), None, &r, &scope(), None).expect("json");
        assert_eq!(
            p.body.as_ref().map(|b| b.bytes.clone()),
            Some(br#"{"a":2,"c":["United States"]}"#.to_vec())
        );
        let r = req(
            "method: POST\nurl: https://x/y\nbody: {form: {grant_type: client_credentials, scope: \"a b\"}}\n",
        );
        let p = build(&Engine::default(), None, &r, &scope(), None).expect("form");
        assert_eq!(
            p.body.map(|b| (b.bytes, b.content_type)),
            Some((
                b"grant_type=client_credentials&scope=a%20b".to_vec(),
                "application/x-www-form-urlencoded"
            ))
        );
    }

    #[test]
    fn set_param_and_keys() {
        let r = req("path: /a\nparams: {page: 1, x: \"a b\"}\n");
        let mut p = build(
            &Engine::default(),
            Some("https://x"),
            &r,
            &scope(),
            Some(Duration::from_secs(5)),
        )
        .expect("b");
        p.set_param("page", Some("2"));
        assert_eq!(p.url.as_str(), "https://x/a?x=a%20b&page=2");
        p.set_param("x", None);
        p.set_param("page", None);
        assert_eq!(p.url.as_str(), "https://x/a");
        p.set_param("api_key", Some("zzz"));
        assert_eq!(p.key(), "GET https://x/a?api_key=****");
        assert_eq!(p.timeout, Some(Duration::from_secs(5)));
        let e = build(&Engine::default(), None, &req("path: /a\n"), &scope(), None)
            .expect_err("no base");
        assert_eq!(e.class, atlatl_core::ErrorClass::Config);
    }
}
