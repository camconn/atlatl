//! Cassettes (07 §13): transport-level recordings of request/response pairs, replayed without
//! network. Files live at `apis/<api>/cassettes/<name>.json` by default (JSON, so the `apis/**/*.yaml`
//! spec scan never sees them).
//!
//! - **Redaction** happens when a cassette is saved (so tokens registered *after* their response,
//!   like OAuth access tokens, are masked too): registered secrets and sensitive query parameters
//!   become `****`, sensitive request headers (`Authorization`, `Cookie`, `X-Api-Key`, …, plus the
//!   spec's `headers` auth and HMAC signature headers) are dropped, and response `Set-Cookie` is
//!   masked.
//! - **Matching** is deterministic: method, the redacted URL with its query pairs sorted (minus
//!   `match.ignore_query`), and the redacted body (unless `match.body: false`). Request headers are
//!   never matched, so signatures and auth headers don't matter. Each recorded interaction is used
//!   once, in recorded order among equal keys; a request with none left is a `cassette_miss`
//!   (class `config`, not retried).
//! - **Clock**: a client's `now()` is pinned to the recording's start, saved as `recorded_at`, and
//!   replays pin it there too, so clock-derived requests (`range()` windows) match on any day.
//!   Replays pace on a virtual clock: rate limits and backoff are computed (and reported) as live,
//!   without sleeping.

use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use atlatl_core::redact::{MASK, Redactor, SENSITIVE_HEADERS};
use atlatl_core::{AtlatlError, ErrorClass};
use base64::Engine;
use chrono::{DateTime, SecondsFormat, SubsecRound, Utc};
use serde::{Deserialize, Serialize};

use crate::request::Prepared;
use crate::response::Captured;

pub const VERSION: u32 = 1;

/// `<root>/apis/<api>/cassettes/<name>.json`.
pub fn default_path(project_root: &Path, api: &str, name: &str) -> PathBuf {
    project_root
        .join("apis")
        .join(api)
        .join("cassettes")
        .join(format!("{name}.json"))
}

/// `apis/wx.yaml` → `wx`, `apis/v/wx.yaml` → `v/wx`: the cassette directory for a spec.
pub fn api_dir(spec_path: &str) -> String {
    let p = spec_path.trim_start_matches("apis/");
    p.rsplit_once('.').map(|x| x.0).unwrap_or(p).to_owned()
}

/// A cassette argument: a bare name under `apis/<api>/cassettes/`, else a path.
pub fn resolve(project_root: &Path, spec_path: &str, arg: &str) -> PathBuf {
    if arg.contains('/') || arg.ends_with(".json") {
        PathBuf::from(arg)
    } else {
        default_path(project_root, &api_dir(spec_path), arg)
    }
}

/// HMAC-added query params that differ per request (their template uses `timestamp` or
/// `now()`); recording leaves them out of matching so replays hit. Unparseable templates count
/// as volatile.
pub fn volatile_params(spec: &crate::spec::ApiSpec) -> Vec<String> {
    let crate::spec::AuthSpec::Hmac(h) = &spec.auth else {
        return Vec::new();
    };
    let env = minijinja::Environment::new();
    h.params
        .iter()
        .filter(|(_, v)| {
            env.template_from_str(v)
                .map(|t| {
                    let vars = t.undeclared_variables(false);
                    vars.contains("timestamp") || vars.contains("now")
                })
                .unwrap_or(true)
        })
        .map(|(k, _)| k.clone())
        .collect()
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatchOptions {
    /// Query parameters left out of matching (volatile values such as `timestamp`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ignore_query: Vec<String>,
    /// Match request bodies (default true).
    #[serde(default = "yes", skip_serializing_if = "is_true")]
    pub body: bool,
}

fn yes() -> bool {
    true
}

fn is_true(b: &bool) -> bool {
    *b
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CassetteFile {
    pub version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recorded_at: Option<String>,
    #[serde(default, rename = "match")]
    pub matching: MatchOptions,
    pub interactions: Vec<Interaction>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Interaction {
    pub request: RecordedRequest,
    pub response: RecordedResponse,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordedRequest {
    pub method: String,
    pub url: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<(String, String)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordedResponse {
    pub status: u16,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    /// UTF-8 bodies; others are in `body_base64`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_base64: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u64>,
}

impl RecordedResponse {
    pub fn bytes(&self) -> Result<Vec<u8>, AtlatlError> {
        match (&self.body, &self.body_base64) {
            (Some(s), _) => Ok(s.as_bytes().to_vec()),
            (None, Some(b)) => base64::engine::general_purpose::STANDARD
                .decode(b)
                .map_err(|e| AtlatlError::config(format!("cassette body_base64: {e}"))),
            (None, None) => Ok(Vec::new()),
        }
    }
}

/// A live exchange as sent/received, unredacted until saved or read.
#[derive(Clone, Debug)]
struct Raw {
    method: String,
    url: String,
    headers: Vec<(String, String)>,
    body: Option<Vec<u8>>,
    status: u16,
    resp_headers: Vec<(String, String)>,
    resp_body: Vec<u8>,
    elapsed: Duration,
}

enum Mode {
    /// Live requests, kept for [`Cassette::save`] (with a path) or [`Cassette::interactions`].
    Record {
        path: Option<PathBuf>,
        raw: Vec<Raw>,
    },
    Replay {
        file: CassetteFile,
        used: Vec<bool>,
    },
}

struct Inner {
    mode: Mode,
    matching: MatchOptions,
    api: Option<String>,
    /// Request headers left out of recordings besides [`SENSITIVE_HEADERS`] (request signatures).
    drop_headers: Vec<String>,
    /// The instant `now()` is pinned to: set by the first client when recording, `recorded_at`
    /// when replaying.
    clock: Option<DateTime<Utc>>,
}

/// A transport recorder or player; clones share state. Set it on
/// [`ClientOptions::cassette`](crate::ClientOptions::cassette).
#[derive(Clone)]
pub struct Cassette(Arc<Mutex<Inner>>);

impl std::fmt::Debug for Cassette {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let g = self.lock();
        let m = match &g.mode {
            Mode::Record { path, raw } => format!("record({path:?}, {})", raw.len()),
            Mode::Replay { file, .. } => format!("replay({})", file.interactions.len()),
        };
        f.debug_tuple("Cassette").field(&m).finish()
    }
}

impl Cassette {
    fn new(mode: Mode, matching: MatchOptions, api: Option<String>) -> Self {
        Self(Arc::new(Mutex::new(Inner {
            mode,
            matching,
            api,
            drop_headers: Vec::new(),
            clock: None,
        })))
    }

    /// Records live traffic; [`save`](Self::save) writes it to `path`.
    pub fn record(path: impl Into<PathBuf>) -> Self {
        Self::new(
            Mode::Record {
                path: Some(path.into()),
                raw: Vec::new(),
            },
            MatchOptions {
                body: true,
                ..MatchOptions::default()
            },
            None,
        )
    }

    /// Records live traffic in memory only (`atlatl api test` printing).
    pub fn memory() -> Self {
        Self::new(
            Mode::Record {
                path: None,
                raw: Vec::new(),
            },
            MatchOptions {
                body: true,
                ..MatchOptions::default()
            },
            None,
        )
    }

    /// Plays back a cassette file; no request reaches the network.
    pub fn replay(path: impl AsRef<Path>) -> Result<Self, AtlatlError> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|e| {
            AtlatlError::config(format!("cassette `{}`: {e}", path.display())).code("cassette")
        })?;
        let file: CassetteFile = serde_json::from_str(&text).map_err(|e| {
            AtlatlError::config(format!("cassette `{}`: {e}", path.display())).code("cassette")
        })?;
        Self::from_file(file)
    }

    pub fn from_file(file: CassetteFile) -> Result<Self, AtlatlError> {
        if file.version != VERSION {
            return Err(AtlatlError::config(format!(
                "cassette version {} is not supported (expected {VERSION})",
                file.version
            )));
        }
        let used = vec![false; file.interactions.len()];
        let matching = file.matching.clone();
        let api = file.api.clone();
        let clock = match file.recorded_at.as_deref() {
            Some(t) => Some(
                DateTime::parse_from_rfc3339(t)
                    .map_err(|e| AtlatlError::config(format!("cassette `recorded_at` `{t}`: {e}")))?
                    .with_timezone(&Utc),
            ),
            None => None,
        };
        let c = Self::new(Mode::Replay { file, used }, matching, api);
        c.lock().clock = clock;
        Ok(c)
    }

    /// Leaves these query parameters out of matching; persisted in the file.
    pub fn ignore_query<I: IntoIterator<Item = S>, S: Into<String>>(self, names: I) -> Self {
        self.lock()
            .matching
            .ignore_query
            .extend(names.into_iter().map(Into::into));
        self
    }

    pub fn match_body(self, on: bool) -> Self {
        self.lock().matching.body = on;
        self
    }

    pub fn api(self, name: impl Into<String>) -> Self {
        self.lock().api = Some(name.into());
        self
    }

    /// Also drops these request headers from recordings (the client adds HMAC signature and
    /// `headers` auth names).
    pub fn drop_request_headers<I: IntoIterator<Item = S>, S: Into<String>>(&self, names: I) {
        self.lock()
            .drop_headers
            .extend(names.into_iter().map(Into::into));
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The instant a client's `now()` is pinned to, so windows and other clock-derived requests
    /// replay exactly: `recorded_at` for a replay (or `now` when the file has none); for a
    /// recording, `now` (millisecond precision) at the first call, which becomes `recorded_at`.
    pub fn pin_clock(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        *self
            .lock()
            .clock
            .get_or_insert_with(|| now.trunc_subsecs(3))
    }

    pub fn is_replay(&self) -> bool {
        matches!(self.lock().mode, Mode::Replay { .. })
    }

    /// Redacted interactions so far (recorded) or in the file (replayed).
    pub fn interactions(&self) -> Vec<Interaction> {
        let g = self.lock();
        match &g.mode {
            Mode::Record { raw, .. } => raw
                .iter()
                .map(|r| redacted(r, Redactor::global(), &g.drop_headers))
                .collect(),
            Mode::Replay { file, .. } => file.interactions.clone(),
        }
    }

    /// Replay: interactions never requested.
    pub fn unused(&self) -> usize {
        match &self.lock().mode {
            Mode::Replay { used, .. } => used.iter().filter(|u| !**u).count(),
            Mode::Record { .. } => 0,
        }
    }

    /// The redacted cassette file.
    pub fn to_file(&self) -> CassetteFile {
        let g = self.lock();
        match &g.mode {
            Mode::Replay { file, .. } => file.clone(),
            Mode::Record { raw, .. } => CassetteFile {
                version: VERSION,
                api: g.api.clone(),
                recorded_at: Some(match g.clock {
                    Some(t) => t.to_rfc3339_opts(SecondsFormat::Millis, true),
                    None => atlatl_core::ids::rfc3339(std::time::SystemTime::now()),
                }),
                matching: g.matching.clone(),
                interactions: raw
                    .iter()
                    .map(|r| redacted(r, Redactor::global(), &g.drop_headers))
                    .collect(),
            },
        }
    }

    /// Writes a recording (redacted, atomically) to its path; returns it. A no-op for replay and
    /// memory cassettes.
    pub fn save(&self) -> Result<Option<PathBuf>, AtlatlError> {
        let path = match &self.lock().mode {
            Mode::Record { path: Some(p), .. } => p.clone(),
            _ => return Ok(None),
        };
        self.save_to(&path)?;
        Ok(Some(path))
    }

    pub fn save_to(&self, path: &Path) -> Result<(), AtlatlError> {
        let mut bytes = serde_json::to_vec_pretty(&self.to_file())
            .map_err(|e| AtlatlError::internal(e.to_string()))?;
        bytes.push(b'\n');
        if let Some(d) = path.parent() {
            std::fs::create_dir_all(d).map_err(|e| {
                AtlatlError::internal(format!("cassette dir `{}`: {e}", d.display()))
            })?;
        }
        atlatl_core::obs::report::write_atomic(path, &bytes)
            .map_err(|e| AtlatlError::internal(format!("cassette `{}`: {e}", path.display())))
    }

    pub(crate) async fn send(
        &self,
        live: &crate::transport::Transport,
        req: &Prepared,
        max_body: u64,
    ) -> Result<Captured, AtlatlError> {
        if self.is_replay() {
            return self.play(req, max_body);
        }
        let cap = live.send_live(req, max_body).await?;
        let mut headers = req.headers.clone();
        if let Some(b) = &req.body
            && !headers
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("content-type"))
        {
            headers.push(("content-type".into(), b.content_type.into()));
        }
        let raw = Raw {
            method: req.method.as_str().to_owned(),
            url: req.url.to_string(),
            headers,
            body: req.body.as_ref().map(|b| b.bytes.clone()),
            status: cap.status,
            resp_headers: cap.headers.clone(),
            resp_body: cap.body.clone(),
            elapsed: cap.elapsed,
        };
        if let Mode::Record { raw: v, .. } = &mut self.lock().mode {
            v.push(raw);
        }
        Ok(cap)
    }

    fn play(&self, req: &Prepared, max_body: u64) -> Result<Captured, AtlatlError> {
        let r = Redactor::global();
        let mut g = self.lock();
        let Inner { mode, matching, .. } = &mut *g;
        let Mode::Replay { file, used } = mode else {
            return Err(AtlatlError::internal("not a replay cassette"));
        };
        let body = req
            .body
            .as_ref()
            .map(|b| r.redact(&String::from_utf8_lossy(&b.bytes)).into_owned());
        let want = match_key(
            req.method.as_str(),
            &r.redact(req.url.as_str()),
            body.as_deref(),
            matching,
        );
        let mut same = 0;
        let mut hit = None;
        for (i, it) in file.interactions.iter().enumerate() {
            let k = match_key(
                &it.request.method,
                &it.request.url,
                it.request.body.as_deref(),
                matching,
            );
            if k == want {
                same += 1;
                if !used[i] {
                    hit = Some(i);
                    break;
                }
            }
        }
        let Some(i) = hit else {
            return Err(AtlatlError::new(
                ErrorClass::Config,
                format!(
                    "cassette miss: no unused recorded interaction for {} {} ({same} recorded, all used)",
                    req.method.as_str(),
                    r.redact(req.url.as_str())
                ),
            )
            .code("cassette_miss"));
        };
        used[i] = true;
        let resp = &file.interactions[i].response;
        let body = resp.bytes()?;
        let url = r.redact(req.url.as_str()).into_owned();
        if body.len() as u64 > max_body {
            return Err(AtlatlError::data(format!(
                "response body from {url} exceeds max_body ({max_body} bytes)"
            ))
            .code("body_too_large"));
        }
        Ok(Captured {
            status: resp.status,
            headers: resp
                .headers
                .iter()
                .map(|(k, v)| (k.to_ascii_lowercase(), v.clone()))
                .collect(),
            body,
            elapsed: Duration::from_millis(resp.elapsed_ms.unwrap_or(0)),
            url,
        })
    }
}

/// `METHOD url-with-sorted-query [body]`.
fn match_key(method: &str, url: &str, body: Option<&str>, m: &MatchOptions) -> String {
    let (base, query) = match url.split_once('?') {
        Some((b, q)) => (b, q),
        None => (url, ""),
    };
    let mut pairs: Vec<&str> = query
        .split('&')
        .filter(|p| !p.is_empty())
        .filter(|p| {
            let name = p.split_once('=').map(|x| x.0).unwrap_or(p);
            let name = percent_encoding::percent_decode_str(name).decode_utf8_lossy();
            !m.ignore_query.iter().any(|i| *i == name)
        })
        .collect();
    pairs.sort_unstable();
    let mut k = format!("{} {base}?{}", method.to_ascii_uppercase(), pairs.join("&"));
    if m.body
        && let Some(b) = body
    {
        k.push('\n');
        k.push_str(b);
    }
    k
}

fn text_or_b64(b: &[u8]) -> (Option<String>, Option<String>) {
    match std::str::from_utf8(b) {
        Ok(s) => (Some(s.to_owned()), None),
        Err(_) => (
            None,
            Some(base64::engine::general_purpose::STANDARD.encode(b)),
        ),
    }
}

fn redacted(r: &Raw, red: &Redactor, drop: &[String]) -> Interaction {
    let redact = |s: &str| -> String { red.redact(s).into_owned() };
    let sensitive = |k: &str| SENSITIVE_HEADERS.iter().any(|h| h.eq_ignore_ascii_case(k));
    let dropped = |k: &str| sensitive(k) || drop.iter().any(|h| h.eq_ignore_ascii_case(k));
    let (body, body_base64) = text_or_b64(&r.resp_body);
    Interaction {
        request: RecordedRequest {
            method: r.method.clone(),
            url: redact(&r.url),
            headers: r
                .headers
                .iter()
                .filter(|(k, _)| !dropped(k))
                .map(|(k, v)| (k.to_ascii_lowercase(), redact(v)))
                .collect(),
            body: r.body.as_ref().map(|b| redact(&String::from_utf8_lossy(b))),
        },
        response: RecordedResponse {
            status: r.status,
            headers: r
                .resp_headers
                .iter()
                .map(|(k, v)| {
                    let v: Cow<'_, str> = if sensitive(k) {
                        Cow::Borrowed(MASK)
                    } else {
                        red.redact(v)
                    };
                    (k.clone(), v.into_owned())
                })
                .collect(),
            body: body.map(|b| redact(&b)),
            body_base64,
            elapsed_ms: Some(r.elapsed.as_millis() as u64),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(y: &str) -> crate::spec::ApiSpec {
        let n =
            atlatl_core::config::node::parse_yaml(y, Arc::from(Path::new("t.yaml"))).expect("yaml");
        crate::spec::ApiSpec::parse(&n).expect("spec")
    }

    #[test]
    fn volatile_hmac_params_come_from_the_template_not_the_name() {
        let s = spec(
            r#"base_url: https://x
auth:
  type: hmac
  algorithm: sha256
  key: "{{ secrets.k }}"
  params: {clientId: "{{ secrets.client_id }}", ts: "{{ timestamp }}", nonce: "{{ now() }}", mixed: "a{{ timestamp ~ 'b' }}", timestamp_label: "fixed"}
  canonical: c
  placement: {header: Signature}
endpoints: {}
"#,
        );
        assert_eq!(volatile_params(&s), ["ts", "nonce", "mixed"]);
        assert!(volatile_params(&spec("base_url: https://x\nendpoints: {}\n")).is_empty());
    }

    #[test]
    fn cassette_locations() {
        assert_eq!(api_dir("apis/wx.yaml"), "wx");
        assert_eq!(api_dir("apis/v/wx.yml"), "v/wx");
        let root = Path::new("/p");
        assert_eq!(
            resolve(root, "apis/v/wx.yaml", "run1"),
            PathBuf::from("/p/apis/v/wx/cassettes/run1.json")
        );
        assert_eq!(
            resolve(root, "apis/wx.yaml", "x/y.json"),
            PathBuf::from("x/y.json")
        );
        assert_eq!(
            resolve(root, "apis/wx.yaml", "y.json"),
            PathBuf::from("y.json")
        );
    }

    #[test]
    fn keys_sort_query_and_ignore_params() {
        let m = MatchOptions {
            ignore_query: vec!["timestamp".into()],
            body: true,
        };
        assert_eq!(
            match_key("get", "https://x/a?b=2&a=1&timestamp=9", None, &m),
            match_key("GET", "https://x/a?a=1&b=2&timestamp=10", None, &m)
        );
        assert_ne!(
            match_key("GET", "https://x/a?a=1", Some("{}"), &m),
            match_key("GET", "https://x/a?a=1", Some("[]"), &m)
        );
        let m = MatchOptions { body: false, ..m };
        assert_eq!(
            match_key("POST", "https://x/a", Some("{}"), &m),
            match_key("POST", "https://x/a", Some("[]"), &m)
        );
    }

    #[test]
    fn redaction_drops_auth_headers_and_masks_secrets() {
        let red = Redactor::new();
        red.register("sekrit");
        let raw = Raw {
            method: "POST".into(),
            url: "https://x/a?api_key=abc&q=sekrit".into(),
            headers: vec![
                ("Authorization".into(), "Bearer sekrit".into()),
                ("X-Trace".into(), "t-sekrit".into()),
                ("Signature".into(), "c2ln".into()),
            ],
            body: Some(b"secret=sekrit".to_vec()),
            status: 200,
            resp_headers: vec![("set-cookie".into(), "sid=1".into())],
            resp_body: br#"{"access_token": "sekrit"}"#.to_vec(),
            elapsed: Duration::from_millis(5),
        };
        let i = redacted(&raw, &red, &["signature".to_owned()]);
        assert_eq!(i.request.url, "https://x/a?api_key=****&q=****");
        assert_eq!(i.request.headers, vec![("x-trace".into(), "t-****".into())]);
        assert_eq!(i.request.body.as_deref(), Some("secret=****"));
        assert_eq!(
            i.response.headers,
            vec![("set-cookie".into(), "****".into())]
        );
        assert_eq!(
            i.response.body.as_deref(),
            Some(r#"{"access_token": "****"}"#)
        );
    }

    #[test]
    fn binary_bodies_roundtrip() {
        let (t, b) = text_or_b64(&[0xff, 0x00, 0x10]);
        assert!(t.is_none());
        let r = RecordedResponse {
            status: 200,
            headers: vec![],
            body: None,
            body_base64: b,
            elapsed_ms: None,
        };
        assert_eq!(r.bytes().expect("bytes"), vec![0xff, 0x00, 0x10]);
    }
}
