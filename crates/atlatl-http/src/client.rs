//! [`ApiClient`] (one per REST connection per run) and [`EndpointRun`] (one endpoint for one set of
//! template variables), yielding `(ordinal, record)` pairs and per-endpoint stats (10 §4 `http`).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use atlatl_core::retry::{
    AttemptRecord, FixedJitter, Jitter, RandomJitter, RetryLevel, RetryPolicy,
};
use atlatl_core::template::{EnvSource, ProcessEnv};
use atlatl_core::{AtlatlError, ErrorClass};
use futures::{Stream, TryStreamExt};
use indexmap::IndexMap;
use minijinja::value::{Enumerator, Object, ObjectRepr};
use minijinja::{Error, ErrorKind, State, Value};
use serde::Serialize;

use crate::auth::{Auth, AuthEnv, Ticket};
use crate::expr::{Engine, Scope};
use crate::funcs::{self, Clock};
use crate::json::{JValue, Map};
use crate::limiter::{Limiter, LimiterConfig};
use crate::paginate::Pager;
use crate::records::Records;
use crate::rejects::{self, RowReject, Rows};
use crate::request::{self, Prepared};
use crate::response::ResponseView;
use crate::rules::{self, Classifier, Origin, Rule};
use crate::spec::{
    Action, ApiSpec, AuthSpec, DecodeRetry, DecodeSpec, Endpoint, Format, LimiterScope, Placement,
    RequestSpec,
};
use crate::transport::Transport;

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct ClientOptions {
    /// Connection-level retry policy: attempts and backoff for rules that don't set their own.
    pub retry: RetryPolicy,
    /// Per-request timeout unless the request sets `timeout`. `None` disables it.
    pub timeout: Option<Duration>,
    pub connect_timeout: Option<Duration>,
    pub user_agent: String,
    pub env: Arc<dyn EnvSource>,
    pub clock: Clock,
    /// A fixed jitter factor in [1, 1.5) (tests); random when `None`.
    pub jitter: Option<f64>,
    /// Record through, or replay from, a cassette (auth token requests included).
    pub cassette: Option<crate::cassette::Cassette>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            retry: RetryPolicy::default(),
            timeout: Some(DEFAULT_TIMEOUT),
            connect_timeout: Some(Duration::from_secs(30)),
            user_agent: format!("atlatl/{}", env!("CARGO_PKG_VERSION")),
            env: Arc::new(ProcessEnv),
            clock: funcs::system_clock(),
            jitter: None,
            cassette: None,
        }
    }
}

impl fmt::Debug for ClientOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientOptions")
            .field("retry", &self.retry)
            .field("timeout", &self.timeout)
            .field("user_agent", &self.user_agent)
            .finish_non_exhaustive()
    }
}

/// Template inputs for one endpoint run.
#[derive(Clone, Debug, Default)]
pub struct RunVars {
    /// Plan-phase names bound at the top level (`vars`, `stream`, `run`, `member`, …).
    pub globals: IndexMap<String, JValue>,
    /// Merged over the spec's `state`.
    pub state: IndexMap<String, JValue>,
    /// Persisted sync values (`null` on the first run).
    pub sync: IndexMap<String, JValue>,
    /// The current `for_each` item, bound as `item` and under the endpoint's `for_each.as`.
    pub item: Option<JValue>,
    /// Per-item credentials, bound as `secrets.item.*` and redacted (seam for 07 §3).
    pub item_secrets: IndexMap<String, String>,
    /// Request retry for this run (a replication's `retry`), over the client's; the client is
    /// shared by every replication on the connection.
    pub retry: Option<RetryPolicy>,
    /// Live counters for progress reporting, shared by every item and setup step of the endpoint.
    pub live: Option<Arc<Live>>,
}

/// An endpoint's counters as it runs, for progress lines and `stream.progress` (10 §3). The
/// report's numbers come from [`EndpointStats`]; these only mirror them early.
#[derive(Debug)]
pub struct Live {
    pub items_done: AtomicU64,
    /// `u64::MAX` until known; a plain endpoint (no `for_each`) never knows.
    items_total: AtomicU64,
    pub requests: AtomicU64,
    pub retries: AtomicU64,
    pub throttle_wait_ms: AtomicU64,
    pub records: AtomicU64,
    status_counts: Mutex<BTreeMap<String, u64>>,
}

impl Default for Live {
    fn default() -> Self {
        Self {
            items_done: AtomicU64::new(0),
            items_total: AtomicU64::new(u64::MAX),
            requests: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            throttle_wait_ms: AtomicU64::new(0),
            records: AtomicU64::new(0),
            status_counts: Mutex::new(BTreeMap::new()),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LiveSnapshot {
    pub items_done: u64,
    pub items_total: Option<u64>,
    pub requests: u64,
    pub retries: u64,
    pub throttle_wait_ms: u64,
    pub records: u64,
    pub status_counts: BTreeMap<String, u64>,
}

impl Live {
    pub fn set_items_total(&self, n: u64) {
        self.items_total.store(n, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> LiveSnapshot {
        let total = self.items_total.load(Ordering::Relaxed);
        LiveSnapshot {
            items_done: self.items_done.load(Ordering::Relaxed),
            items_total: (total != u64::MAX).then_some(total),
            requests: self.requests.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            throttle_wait_ms: self.throttle_wait_ms.load(Ordering::Relaxed),
            records: self.records.load(Ordering::Relaxed),
            status_counts: self
                .status_counts
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct HttpStats {
    pub requests: u64,
    pub retries: u64,
    pub skipped_items: u64,
    pub throttle_wait_ms: u64,
    pub status_counts: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Skip {
    pub request_key: String,
    pub reason: String,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct EndpointStats {
    /// The run report's `http` block.
    pub http: HttpStats,
    /// Also counted in `http.requests`.
    pub auth_requests: u64,
    /// Waiting on `rps`/`min_interval`/`max_in_flight` (not throttling).
    pub rate_wait_ms: u64,
    pub pages: u64,
    pub records: u64,
    pub filtered: u64,
    pub duplicates: u64,
    pub bytes_read: u64,
    pub attempts: Vec<AttemptRecord>,
    pub skips: Vec<Skip>,
    /// Row mode (03 §6.1): rows or responses that still failed after their refetches.
    pub rejects: Vec<RowReject>,
    pub stopped: bool,
}

#[derive(Clone, Debug, Default)]
pub struct StatsHandle(Arc<Mutex<EndpointStats>>, Option<Arc<Live>>);

impl StatsHandle {
    fn new(live: Option<Arc<Live>>) -> Self {
        Self(Arc::default(), live)
    }

    pub fn get(&self) -> EndpointStats {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Updates the stats, mirroring the counters the change moved into the live ones. Status
    /// counts go through [`Self::status`] instead, so this never walks the status map.
    fn with<R>(&self, f: impl FnOnce(&mut EndpointStats) -> R) -> R {
        let mut s = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let Some(live) = &self.1 else {
            return f(&mut s);
        };
        let before = (
            s.http.requests,
            s.http.retries,
            s.http.throttle_wait_ms,
            s.records,
        );
        let r = f(&mut s);
        let add = |a: &AtomicU64, now: u64, was: u64| {
            if now > was {
                a.fetch_add(now - was, Ordering::Relaxed);
            }
        };
        add(&live.requests, s.http.requests, before.0);
        add(&live.retries, s.http.retries, before.1);
        add(&live.throttle_wait_ms, s.http.throttle_wait_ms, before.2);
        add(&live.records, s.records, before.3);
        r
    }

    /// Counts one response with `status`.
    fn status(&self, status: u16) {
        let key = status.to_string();
        if let Some(live) = &self.1 {
            *live
                .status_counts
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .entry(key.clone())
                .or_default() += 1;
        }
        *self
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .http
            .status_counts
            .entry(key)
            .or_default() += 1;
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Record {
    pub ordinal: i64,
    pub record: JValue,
}

/// `env.NAME` (undefined if unset) and `env("NAME", "default")`.
#[derive(Debug)]
struct EnvObject(Arc<dyn EnvSource>);

impl Object for EnvObject {
    fn repr(self: &Arc<Self>) -> ObjectRepr {
        ObjectRepr::Map
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        key.as_str().and_then(|k| self.0.var(k)).map(Value::from)
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::NonEnumerable
    }

    fn call(self: &Arc<Self>, _: &mut State<'_, '_>, args: &[Value]) -> Result<Value, Error> {
        let name = args.first().and_then(|v| v.as_str()).ok_or_else(|| {
            Error::new(ErrorKind::InvalidOperation, "env() takes (name[, default])")
        })?;
        match (self.0.var(name), args.get(1)) {
            (Some(v), _) => Ok(Value::from(v)),
            (None, Some(d)) => Ok(d.clone()),
            (None, None) => Err(Error::new(
                ErrorKind::UndefinedError,
                format!("environment variable `{name}` is not set"),
            )),
        }
    }
}

struct Inner {
    spec: Arc<ApiSpec>,
    engine: Engine,
    transport: Transport,
    secrets: Map,
    env: Value,
    limiter: Arc<Limiter>,
    endpoint_limiters: Mutex<HashMap<String, Arc<Limiter>>>,
    auth: Arc<Auth>,
    endpoint_auth: HashMap<String, Arc<Auth>>,
    classifier: Classifier,
    opts: ClientOptions,
    jitter: Mutex<Box<dyn Jitter>>,
    /// Validations that passed, by endpoint and the names (not values) of globals and state:
    /// the only inputs `validate` reads, so per-item `endpoint()` calls don't repeat it.
    validated: Mutex<HashSet<String>>,
    validation_runs: std::sync::atomic::AtomicUsize,
}

/// The REST client for one connection. Cheap to clone; clones share the limiter, auth tokens and
/// decode-failure window.
#[derive(Clone)]
pub struct ApiClient {
    inner: Arc<Inner>,
}

impl fmt::Debug for ApiClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApiClient")
            .field("spec", &self.inner.spec.name)
            .finish_non_exhaustive()
    }
}

impl ApiClient {
    /// `secrets` are the connection's resolved secrets; every value is registered for redaction.
    pub fn new(
        spec: Arc<ApiSpec>,
        secrets: IndexMap<String, String>,
        mut opts: ClientOptions,
    ) -> Result<Self, AtlatlError> {
        for v in secrets.values() {
            atlatl_core::redact::register(v);
        }
        let mut cfg = LimiterConfig::from_spec(&spec.rate_limit);
        if let Some(c) = &opts.cassette {
            cfg.virtual_time = c.is_replay();
            let t = c.pin_clock((opts.clock)());
            opts.clock = Arc::new(move || t);
        }
        let endpoint_auth = spec
            .endpoints
            .iter()
            .filter_map(|(n, e)| e.auth.clone().map(|a| (n.clone(), Arc::new(Auth::new(a)))))
            .collect();
        if let Some(c) = &opts.cassette {
            let auths = std::iter::once(&spec.auth)
                .chain(spec.endpoints.values().filter_map(|e| e.auth.as_ref()));
            c.drop_request_headers(auths.flat_map(|a| match a {
                AuthSpec::Hmac(h) => match &h.placement {
                    Placement::Header(n) => vec![n.clone()],
                    Placement::Param(_) => Vec::new(),
                },
                AuthSpec::Headers(h) => h.keys().cloned().collect(),
                _ => Vec::new(),
            }));
        }
        let jitter: Box<dyn Jitter> = match opts.jitter {
            Some(f) => Box::new(FixedJitter(f)),
            None => Box::new(RandomJitter::new()),
        };
        Ok(Self {
            inner: Arc::new(Inner {
                engine: Engine::new(opts.clock.clone()),
                transport: Transport::new(&opts.user_agent, opts.connect_timeout)?
                    .with_cassette(opts.cassette.clone()),
                secrets: secrets
                    .into_iter()
                    .map(|(k, v)| (k, JValue::String(v)))
                    .collect(),
                env: Value::from_object(EnvObject(opts.env.clone())),
                limiter: Limiter::new(cfg),
                endpoint_limiters: Mutex::new(HashMap::new()),
                auth: Arc::new(Auth::new(spec.auth.clone())),
                endpoint_auth,
                classifier: Classifier::new(
                    &spec.throttle_patterns,
                    spec.decode_failures_as_throttle,
                )?,
                jitter: Mutex::new(jitter),
                validated: Mutex::default(),
                validation_runs: std::sync::atomic::AtomicUsize::new(0),
                opts,
                spec,
            }),
        })
    }

    pub fn spec(&self) -> &ApiSpec {
        &self.inner.spec
    }

    pub fn engine(&self) -> &Engine {
        &self.inner.engine
    }

    /// The connection-scoped limiter.
    pub fn limiter(&self) -> &Arc<Limiter> {
        &self.inner.limiter
    }

    fn limiter_for(&self, endpoint: &str) -> Arc<Limiter> {
        let i = &self.inner;
        if i.spec.rate_limit.scope == LimiterScope::Connection {
            return i.limiter.clone();
        }
        i.endpoint_limiters
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(endpoint.to_owned())
            .or_insert_with(|| Limiter::new(i.limiter.config().clone()))
            .clone()
    }

    /// Effective rules for an endpoint, in evaluation order (for `atlatl api explain`).
    pub fn rules(&self, endpoint: &str) -> Result<Vec<Rule>, AtlatlError> {
        let ep = self.endpoint_spec(endpoint)?;
        let auth = self.auth_for(endpoint);
        let mut out = Vec::new();
        if let Some(w) = auth.reauth_when() {
            out.push(Rule {
                when: rules::When::Expr(w.clone()),
                action: Action::Reauth,
                class: Some(ErrorClass::Auth),
                backoff: None,
                max_attempts: None,
                message: None,
                origin: Origin::Auth,
            });
        }
        out.extend(
            ep.rules
                .iter()
                .map(|r| Rule::from_spec(r, Origin::Endpoint)),
        );
        out.extend(
            self.inner
                .spec
                .rules
                .iter()
                .map(|r| Rule::from_spec(r, Origin::Spec)),
        );
        out.extend(rules::builtin_rules());
        Ok(out)
    }

    fn endpoint_spec(&self, name: &str) -> Result<&Endpoint, AtlatlError> {
        self.inner.spec.endpoint(name).ok_or_else(|| {
            AtlatlError::config(format!(
                "API spec `{}` has no endpoint `{name}`",
                self.inner.spec.name.as_deref().unwrap_or("?")
            ))
        })
    }

    fn auth_for(&self, endpoint: &str) -> Arc<Auth> {
        self.inner
            .endpoint_auth
            .get(endpoint)
            .cloned()
            .unwrap_or_else(|| self.inner.auth.clone())
    }

    /// Checks every template and expression an endpoint uses: syntax, known roots, and that
    /// `secrets.*`, `sync.*` and `state.*` keys exist.
    pub fn validate(&self, endpoint: &str, vars: &RunVars) -> Result<(), AtlatlError> {
        let mut g: Vec<&str> = vars.globals.keys().map(String::as_str).collect();
        let mut st: Vec<&str> = vars.state.keys().map(String::as_str).collect();
        g.sort_unstable();
        st.sort_unstable();
        let key = format!("{endpoint}\0{}\0{}", g.join("\u{1}"), st.join("\u{1}"));
        if self
            .inner
            .validated
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&key)
        {
            return Ok(());
        }
        self.validate_uncached(endpoint, vars)?;
        self.inner
            .validated
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key);
        Ok(())
    }

    /// How many full validations ran (the memo's misses); for tests.
    #[doc(hidden)]
    pub fn validation_runs(&self) -> usize {
        self.inner
            .validation_runs
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn validate_uncached(&self, endpoint: &str, vars: &RunVars) -> Result<(), AtlatlError> {
        self.inner
            .validation_runs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let ep = self.endpoint_spec(endpoint)?;
        let spec = &self.inner.spec;
        let mut roots: BTreeSet<String> = [
            "env",
            "state",
            "secrets",
            "sync",
            "page",
            "response",
            "request",
            "record",
            "item",
            "queue",
            "timestamp",
            "method",
            "path",
            "query",
            "body",
            "body_json",
            "url",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        roots.extend(vars.globals.keys().cloned());
        if let Some(f) = &ep.for_each {
            roots.insert(f.var.clone());
            if let Some(b) = &f.batch {
                roots.insert(b.var.clone());
            }
        }
        let state: BTreeSet<&str> = spec
            .state
            .keys()
            .chain(vars.state.keys())
            .map(String::as_str)
            .chain(
                ep.setup
                    .iter()
                    .filter_map(|s| s.output.as_ref().map(|o| o.1.as_str())),
            )
            .collect();
        let mut items: Vec<(String, bool, String)> = Vec::new();
        let mut push =
            |src: &str, expr: bool, what: String| items.push((src.to_owned(), expr, what));
        let at = |s: &atlatl_core::config::node::Span| s.to_string();
        let req_items =
            |r: &crate::spec::RequestSpec, push: &mut dyn FnMut(&str, bool, String), w: &str| {
                if let Some(p) = &r.path {
                    push(p, false, at(&p.span));
                }
                if let Some(u) = &r.url {
                    push(u, false, at(&u.span));
                }
                let mut strings = Vec::new();
                for v in r.params.values().chain(r.headers.values()) {
                    collect_strings(v, &mut strings);
                }
                match &r.body {
                    Some(crate::spec::Body::Json(v)) => collect_strings(v, &mut strings),
                    Some(crate::spec::Body::Form(m)) => {
                        m.values().for_each(|v| collect_strings(v, &mut strings))
                    }
                    Some(crate::spec::Body::Text(t)) => strings.push(t.clone()),
                    None => {}
                }
                for s in strings {
                    push(&s, false, format!("{w}.request"));
                }
            };
        let epw = format!("endpoint `{endpoint}` ({})", ep.span);
        req_items(&ep.request, &mut push, &epw);
        for s in &ep.setup {
            req_items(&s.request, &mut push, &format!("setup ({})", s.span));
        }
        for r in ep.rules.iter().chain(spec.rules.iter()) {
            push(&r.when, true, at(&r.when.span));
            if let Some(m) = &r.message {
                push(m, false, at(&r.when.span));
            }
        }
        for e in ep.fields.values().chain(ep.emit.values()) {
            push(e, true, at(&e.span));
        }
        if let Some(f) = &ep.records.filter {
            push(f, true, at(&f.span));
        }
        for s in ep.sync.values() {
            push(&s.value, true, at(&s.value.span));
        }
        if let Some(p) = &ep.paginate {
            if let Some(w) = &p.stop_when {
                push(w, true, at(&w.span));
            }
            if let crate::spec::PaginateKind::Expression { next } = &p.kind {
                for e in next.values() {
                    push(e, true, at(&e.span));
                }
            }
        }
        if let Some(crate::spec::Over::Expr(e)) = ep.for_each.as_ref().map(|f| &f.over) {
            push(e, true, at(&e.span));
        }
        let auth = self.auth_for(endpoint);
        let auth_strings: Vec<String> = match auth.spec() {
            crate::spec::AuthSpec::None => vec![],
            crate::spec::AuthSpec::Headers(m) | crate::spec::AuthSpec::Query(m) => {
                m.values().cloned().collect()
            }
            crate::spec::AuthSpec::Bearer { token } => vec![token.clone()],
            crate::spec::AuthSpec::Basic { username, password } => {
                vec![username.clone(), password.clone()]
            }
            crate::spec::AuthSpec::OAuth2(o) => {
                if let Some(w) = &o.reauth_when {
                    push(w, true, at(&w.span));
                }
                [
                    &o.client_id,
                    &o.client_secret,
                    &o.username,
                    &o.password,
                    &o.refresh_token,
                ]
                .into_iter()
                .flatten()
                .cloned()
                .chain([o.token_url.clone()])
                .chain(o.extra.values().cloned())
                .collect()
            }
            crate::spec::AuthSpec::Hmac(h) => [h.key.clone(), h.canonical.clone()]
                .into_iter()
                .chain(h.params.values().cloned())
                .chain(h.headers.values().cloned())
                .collect(),
        };
        for s in auth_strings {
            push(&s, false, "auth".into());
        }
        let engine = &self.inner.engine;
        for (src, expr, what) in items {
            let names = engine
                .undeclared(&src, expr)
                .map_err(|e| AtlatlError::config(format!("{what}: {}", e.message)))?;
            for n in names {
                let mut parts = n.splitn(3, '.');
                let root = parts.next().unwrap_or_default();
                let key = parts.next();
                let bad = |m: String| Err(AtlatlError::config(format!("{what}: {m} in `{src}`")));
                if !roots.contains(root) {
                    return bad(format!("unknown name `{root}`"));
                }
                match (root, key) {
                    ("secrets", Some(k)) if k != "item" && !self.inner.secrets.contains_key(k) => {
                        return bad(format!(
                            "unknown secret `secrets.{k}` (the connection defines: {})",
                            self.inner
                                .secrets
                                .keys()
                                .cloned()
                                .collect::<Vec<_>>()
                                .join(", ")
                        ));
                    }
                    ("sync", Some(k)) if !ep.sync.contains_key(k) => {
                        return bad(format!(
                            "`sync.{k}` is not declared in the endpoint's `sync`"
                        ));
                    }
                    ("state", Some(k)) if !state.contains(k) => {
                        return bad(format!(
                            "`state.{k}` is never set (spec `state`, or a setup `collect`/`set`/`max`/`min`)"
                        ));
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    /// Starts an endpoint for one set of template variables. Validates its templates first.
    pub fn endpoint(&self, name: &str, vars: RunVars) -> Result<EndpointRun, AtlatlError> {
        self.start(name, None, vars)
    }

    /// Runs one of an endpoint's `setup` steps (07 §12). Records are the values its records path
    /// yields, scalars included.
    pub fn setup_step(
        &self,
        name: &str,
        step: usize,
        vars: RunVars,
    ) -> Result<EndpointRun, AtlatlError> {
        let ep = self.endpoint_spec(name)?;
        if step >= ep.setup.len() {
            return Err(AtlatlError::internal(format!(
                "endpoint `{name}` has no setup step {step}"
            )));
        }
        self.start(name, Some(step), vars)
    }

    /// `(name, value)` of every connection secret, for sealing reject descriptors.
    pub(crate) fn secret_pairs(&self) -> Vec<(String, String)> {
        self.inner
            .secrets
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_owned())))
            .collect()
    }

    fn start(
        &self,
        name: &str,
        step: Option<usize>,
        vars: RunVars,
    ) -> Result<EndpointRun, AtlatlError> {
        self.validate(name, &vars)?;
        let live = vars.live.clone();
        let ep = self.endpoint_spec(name)?;
        let i = &self.inner;
        let mut secrets = i.secrets.clone();
        if !vars.item_secrets.is_empty() {
            for v in vars.item_secrets.values() {
                atlatl_core::redact::register(v);
            }
            secrets.insert(
                "item".into(),
                JValue::Object(
                    vars.item_secrets
                        .iter()
                        .map(|(k, v)| (k.clone(), JValue::String(v.clone())))
                        .collect(),
                ),
            );
        }
        let mut state: Map = i
            .spec
            .state
            .iter()
            .map(|(k, v)| (k.clone(), JValue::from(v)))
            .collect();
        state.extend(vars.state.iter().map(|(k, v)| (k.clone(), v.clone())));
        let mut sync: Map = ep.sync.keys().map(|k| (k.clone(), JValue::Null)).collect();
        sync.extend(vars.sync.iter().map(|(k, v)| (k.clone(), v.clone())));
        let mut base = Scope::new();
        for (k, v) in &vars.globals {
            base.set_json(k, Arc::new(v.clone()));
        }
        base.set("env", i.env.clone())
            .set_json("secrets", Arc::new(JValue::Object(secrets)))
            .set_json("state", Arc::new(JValue::Object(state)))
            .set_json("sync", Arc::new(JValue::Object(sync)));
        let item = vars.item.clone().unwrap_or_default();
        base.set_json("item", Arc::new(item.clone()));
        if let Some(f) = &ep.for_each {
            base.set_json(&f.var, Arc::new(item));
        }
        let (rules, pager, records, decode) = match step.map(|n| &ep.setup[n]) {
            None => (
                self.rules(name)?,
                Pager::new(ep.paginate.as_ref())?,
                Records::new(&ep.records, &ep.fields)?,
                &ep.decode,
            ),
            Some(s) => {
                let mut rules: Vec<Rule> = s
                    .rules
                    .iter()
                    .map(|r| Rule::from_spec(r, Origin::Endpoint))
                    .collect();
                rules.extend(
                    i.spec
                        .rules
                        .iter()
                        .map(|r| Rule::from_spec(r, Origin::Spec)),
                );
                rules.extend(rules::builtin_rules());
                (
                    rules,
                    Pager::new(s.paginate.as_ref())?,
                    Records::new(&s.records, &IndexMap::new())?,
                    &s.decode,
                )
            }
        };
        let mut seal = self.secret_pairs();
        seal.extend(
            vars.item_secrets
                .iter()
                .map(|(k, v)| (format!("item.{k}"), v.clone())),
        );
        Ok(EndpointRun {
            seal,
            client: self.clone(),
            name: name.to_owned(),
            step,
            rules,
            limiter: self.limiter_for(name),
            auth: self.auth_for(name),
            pager,
            records,
            max_body: decode.max_body.map(|b| b.0).unwrap_or(i.spec.max_body),
            decode_mode: ep.on_decode_error.retry,
            retry: vars.retry.clone(),
            observe: Vec::new(),
            observed: Vec::new(),
            base,
            stats: StatsHandle::new(live),
            prev: None,
            done: false,
            flushed: false,
            ordinal: 0,
        })
    }
}

fn collect_strings(v: &serde_json::Value, out: &mut Vec<String>) {
    match v {
        serde_json::Value::String(s) => out.push(s.clone()),
        serde_json::Value::Array(a) => a.iter().for_each(|x| collect_strings(x, out)),
        serde_json::Value::Object(m) => m.values().for_each(|x| collect_strings(x, out)),
        _ => {}
    }
}

enum Outcome {
    Process(Arc<ResponseView>),
    Skip(String),
    Stop,
    /// Row mode: the body still didn't decode after the attempt budget.
    Undecodable(Arc<ResponseView>),
}

/// What re-issuing a stored reject produced.
#[derive(Clone, Debug)]
pub enum Replayed {
    /// The row (or whole response) decodes now; its records, after fields and filter.
    Recovered(Vec<Record>),
    /// Still failing, with the latest error and raw excerpt.
    Failing { error: String, raw: String },
}

type Fingerprint = (crate::spec::Method, String, Option<Vec<u8>>);

/// One endpoint, paginated, for one set of variables. Drive it with [`EndpointRun::next_page`] or
/// [`EndpointRun::into_stream`].
pub struct EndpointRun {
    client: ApiClient,
    name: String,
    /// A setup step instead of the endpoint's main request.
    step: Option<usize>,
    rules: Vec<Rule>,
    limiter: Arc<Limiter>,
    auth: Arc<Auth>,
    pager: Pager,
    records: Records,
    max_body: u64,
    decode_mode: DecodeRetry,
    retry: Option<RetryPolicy>,
    /// Expressions evaluated against each processed response (`response.*` sync folds).
    observe: Vec<(String, String)>,
    observed: Vec<(String, JValue)>,
    base: Scope,
    /// Connection secrets plus `item.*` per-item secrets, for sealing reject descriptors.
    seal: Vec<(String, String)>,
    stats: StatsHandle,
    prev: Option<Fingerprint>,
    done: bool,
    flushed: bool,
    ordinal: i64,
}

impl fmt::Debug for EndpointRun {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EndpointRun")
            .field("endpoint", &self.name)
            .finish_non_exhaustive()
    }
}

impl EndpointRun {
    pub fn stats(&self) -> StatsHandle {
        self.stats.clone()
    }

    fn ep(&self) -> &Endpoint {
        &self.client.inner.spec.endpoints[&self.name]
    }

    fn req_spec(&self) -> &RequestSpec {
        match self.step {
            Some(n) => &self.ep().setup[n].request,
            None => &self.ep().request,
        }
    }

    fn decode_spec(&self) -> &DecodeSpec {
        match self.step {
            Some(n) => &self.ep().setup[n].decode,
            None => &self.ep().decode,
        }
    }

    fn raw(&self) -> bool {
        self.step.is_none() && self.ep().raw
    }

    /// The template scope requests render in (`item`, `state`, `sync`, `secrets`, globals).
    pub fn scope(&self) -> &Scope {
        &self.base
    }

    /// Overrides the endpoint's `on_decode_error.retry` (a stream's `on_decode_error`).
    pub fn with_decode_retry(mut self, mode: DecodeRetry) -> Self {
        self.decode_mode = mode;
        self
    }

    /// Evaluates `(name, expression)` against every processed response; see [`Self::take_observed`].
    pub fn observe(mut self, exprs: Vec<(String, String)>) -> Self {
        self.observe = exprs;
        self
    }

    /// Values of the [`Self::observe`] expressions, in response order, since the last call.
    pub fn take_observed(&mut self) -> Vec<(String, JValue)> {
        std::mem::take(&mut self.observed)
    }

    fn row_mode(&self) -> bool {
        self.step.is_none() && self.decode_mode == DecodeRetry::Row
    }

    fn extract(
        &mut self,
        body: &JValue,
        scope: &Scope,
    ) -> Result<(Vec<JValue>, usize), AtlatlError> {
        let items = if self.step.is_some() {
            self.records.extract_values(body)?
        } else {
            self.records.extract(body)?
        };
        let n = items.len();
        let engine = &self.client.inner.engine;
        Ok((self.records.process(items, engine, scope)?, n))
    }

    fn reject(&self, req: &Prepared, row: Option<u64>, raw: String, error: String) {
        let mut request = rejects::descriptor(&self.name, req, &self.seal);
        if let JValue::Object(m) = &mut request {
            m.insert("row".into(), row.map(JValue::from).unwrap_or(JValue::Null));
        }
        self.stats.with(|s| {
            s.rejects.push(RowReject {
                request,
                row,
                raw,
                error,
            })
        });
    }

    fn number(&mut self, recs: Vec<JValue>) -> Vec<Record> {
        let n = recs.len() as u64;
        self.stats.with(|s| s.records += n);
        recs.into_iter()
            .map(|record| {
                let r = Record {
                    ordinal: self.ordinal,
                    record,
                };
                self.ordinal += 1;
                r
            })
            .collect()
    }

    /// The next page of records; `None` once the endpoint is finished.
    pub async fn next_page(&mut self) -> Result<Option<Vec<Record>>, AtlatlError> {
        if self.done {
            if !self.flushed {
                self.flushed = true;
                let rest = self.records.finish();
                if !rest.is_empty() {
                    return Ok(Some(self.number(rest)));
                }
            }
            return Ok(None);
        }
        let inner = self.client.inner.clone();
        let engine = &inner.engine;
        let mut scope = self.base.clone();
        scope.set_json("page", Arc::new(self.pager.namespace()));
        let mut req = request::build(
            engine,
            inner.spec.base_url.as_deref(),
            self.req_spec(),
            &scope,
            inner.opts.timeout,
        )?;
        self.pager.apply(&mut req);
        let fp = req.fingerprint();
        if self.prev.as_ref() == Some(&fp) {
            return Err(AtlatlError::config(format!(
                "endpoint `{}`: pagination made no progress (the next request is identical to the previous one: {})",
                self.name,
                req.key()
            ))
            .code("no_progress"));
        }
        self.prev = Some(fp);
        let key = req.key();
        let url = req.url.clone();
        let sent = self.row_mode().then(|| req.clone());
        match self.exchange(req, &scope).await? {
            Outcome::Skip(reason) => {
                self.stats.with(|s| {
                    s.http.skipped_items += 1;
                    s.skips.push(Skip {
                        request_key: key,
                        reason,
                    });
                });
                self.done = true;
                Ok(Some(Vec::new()))
            }
            Outcome::Stop => {
                self.stats.with(|s| s.stopped = true);
                self.done = true;
                Ok(Some(Vec::new()))
            }
            Outcome::Undecodable(view) => {
                // Keep the rows that decode; the rest become rejects. The page's cursor can't be
                // read, so this item's pagination ends here.
                self.done = true;
                let sent =
                    sent.ok_or_else(|| AtlatlError::internal("undecodable outside row mode"))?;
                self.stats.with(|s| {
                    s.pages += 1;
                    s.bytes_read += view.cap.body.len() as u64;
                });
                scope.set("response", Value::from_dyn_object(view.clone()));
                let good = match rejects::decode_rows(&view.cap, self.decode_spec()) {
                    Rows::Whole(Ok(v)) => v,
                    Rows::Whole(Err(b)) => {
                        self.reject(&sent, None, b.raw, b.error);
                        JValue::Array(Vec::new())
                    }
                    Rows::Rows { good, bad } => {
                        for b in bad {
                            self.reject(&sent, b.index, b.raw, b.error);
                        }
                        JValue::Array(good.into_iter().map(|(_, v)| v).collect())
                    }
                };
                let (items, _) = self.extract(&good, &scope)?;
                self.stats.with(|s| {
                    s.filtered = self.records.filtered;
                    s.duplicates = self.records.duplicates;
                });
                Ok(Some(self.number(items)))
            }
            Outcome::Process(view) => {
                self.stats.with(|s| {
                    s.pages += 1;
                    s.bytes_read += view.cap.body.len() as u64;
                });
                scope.set("response", Value::from_dyn_object(view.clone()));
                for (k, e) in &self.observe {
                    let v = engine.eval_json(e, &scope)?;
                    self.observed.push((k.clone(), v));
                }
                let (items, count) = if self.raw() {
                    let body = match view.decoded() {
                        Ok(b) => (**b).clone(),
                        Err(_) => JValue::String(view.cap.text().into_owned()),
                    };
                    let row = JValue::object([
                        (
                            "fetched_at".to_owned(),
                            funcs::Date(engine.now()).to_string().into(),
                        ),
                        ("request_key".to_owned(), key.into()),
                        ("status".to_owned(), view.status().into()),
                        ("response_json".to_owned(), body),
                    ]);
                    (vec![row], 1)
                } else {
                    let body = match view.decoded() {
                        Ok(b) => b.clone(),
                        Err(e) => return Err(AtlatlError::new(ErrorClass::Decode, e.to_string())),
                    };
                    self.extract(&body, &scope)?
                };
                self.stats.with(|s| {
                    s.filtered = self.records.filtered;
                    s.duplicates = self.records.duplicates;
                });
                if !self.pager.advance(&view, &url, count, engine, &scope)? {
                    self.done = true;
                }
                Ok(Some(self.number(items)))
            }
        }
    }

    /// Records until the endpoint finishes. The stream ends after the first error.
    pub fn into_stream(self) -> impl Stream<Item = Result<Record, AtlatlError>> + Send {
        futures::stream::try_unfold(self, |mut run| async move {
            Ok::<_, AtlatlError>(
                run.next_page()
                    .await?
                    .map(|recs| (futures::stream::iter(recs.into_iter().map(Ok)), run)),
            )
        })
        .try_flatten()
    }

    /// Runs to completion, collecting every record.
    pub async fn collect(mut self) -> Result<(Vec<Record>, EndpointStats), AtlatlError> {
        let mut out = Vec::new();
        while let Some(p) = self.next_page().await? {
            out.extend(p);
        }
        Ok((out, self.stats.get()))
    }

    /// Re-issues a stored reject's request (03 §7) through this run's auth, limiter and rules,
    /// and re-decodes its row. Errors other than a decode failure are reported as still failing.
    pub async fn replay(&mut self, desc: &JValue) -> Result<Replayed, AtlatlError> {
        let inner = self.client.inner.clone();
        let req = rejects::prepared_from(desc, &self.seal, inner.opts.timeout)?;
        let row = desc.get("row").and_then(JValue::as_u64);
        let scope = self.base.clone();
        self.decode_mode = DecodeRetry::Row;
        let view = match self.exchange(req, &scope).await {
            Ok(Outcome::Process(v) | Outcome::Undecodable(v)) => v,
            Ok(Outcome::Skip(reason)) => {
                return Ok(Replayed::Failing {
                    error: format!("skipped: {reason}"),
                    raw: String::new(),
                });
            }
            Ok(Outcome::Stop) => {
                return Ok(Replayed::Failing {
                    error: "stopped by a rule".into(),
                    raw: String::new(),
                });
            }
            Err(e) => {
                return Ok(Replayed::Failing {
                    error: e.redacted_message(),
                    raw: String::new(),
                });
            }
        };
        let mut scope = scope;
        scope.set("response", Value::from_dyn_object(view.clone()));
        let body = match (rejects::decode_rows(&view.cap, self.decode_spec()), row) {
            (Rows::Whole(Ok(v)), _) => v,
            (Rows::Whole(Err(b)), _) => {
                return Ok(Replayed::Failing {
                    error: b.error,
                    raw: b.raw,
                });
            }
            (Rows::Rows { good, .. }, None) => {
                JValue::Array(good.into_iter().map(|(_, v)| v).collect())
            }
            (Rows::Rows { good, bad }, Some(i)) => match good.into_iter().find(|(n, _)| *n == i) {
                Some((_, v)) => JValue::Array(vec![v]),
                None => {
                    let b = bad.into_iter().find(|b| b.index == Some(i));
                    return Ok(Replayed::Failing {
                        error: b
                            .as_ref()
                            .map(|b| b.error.clone())
                            .unwrap_or_else(|| format!("row {i} is no longer in the response")),
                        raw: b.map(|b| b.raw).unwrap_or_default(),
                    });
                }
            },
        };
        let (items, _) = self.extract(&body, &scope)?;
        let mut out = self.number(items);
        let rest = self.records.finish();
        out.extend(self.number(rest));
        Ok(Replayed::Recovered(out))
    }

    fn jitter(&self) -> f64 {
        self.client
            .inner
            .jitter
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .factor()
    }

    fn record_attempt(&self, attempt: u32, class: ErrorClass, message: String, wait: Duration) {
        self.stats.with(|s| {
            s.http.retries += 1;
            s.attempts.push(AttemptRecord {
                level: RetryLevel::Request,
                attempt,
                class,
                message: atlatl_core::redact::redact(&message).into_owned(),
                wait_ms: wait.as_millis() as u64,
            });
        });
    }

    fn failure(
        &self,
        class: ErrorClass,
        rule: Option<&Rule>,
        view: &ResponseView,
        scope: &Scope,
        gave_up_after: Option<u32>,
    ) -> AtlatlError {
        let engine = &self.client.inner.engine;
        let mut msg = match rule.and_then(|r| r.message.as_deref()) {
            Some(m) => engine.render(m, scope).unwrap_or_else(|_| m.to_owned()),
            None => {
                let mut m = format!("HTTP {} from {}", view.status(), view.cap.url);
                if let Some(d) = view.decode_error() {
                    m.push_str(&format!(" ({d})"));
                }
                let snip = view.cap.snippet();
                if !snip.is_empty() {
                    m.push_str(&format!(": {snip}"));
                }
                m
            }
        };
        msg = format!("endpoint `{}`: {msg}", self.name);
        if let Some(n) = gave_up_after {
            msg.push_str(&format!(" (gave up after {n} attempts)"));
        }
        AtlatlError::new(class, msg).code(format!("http_{}", view.status()))
    }

    async fn exchange(
        &mut self,
        prepared: Prepared,
        scope: &Scope,
    ) -> Result<Outcome, AtlatlError> {
        let inner = self.client.inner.clone();
        let engine = &inner.engine;
        let retry = self
            .retry
            .clone()
            .unwrap_or_else(|| inner.opts.retry.clone());
        let retry = &retry;
        let decode_spec = self.decode_spec().clone();
        let needs_decode =
            !self.raw() || !matches!(decode_spec.format, Format::Text | Format::Bytes);
        let auth_requests = AtomicU64::new(0);
        let mut attempt = 1u32;
        let mut reauthed = false;
        loop {
            let ticket: Ticket = {
                let env = AuthEnv {
                    transport: &inner.transport,
                    limiter: &self.limiter,
                    engine,
                    scope,
                    default_request: &inner.spec.default_request,
                    default_timeout: inner.opts.timeout,
                    retry,
                    jitter: &inner.jitter,
                    max_body: self.max_body,
                    requests: &auth_requests,
                };
                let t = self.auth.prepare(&env).await;
                let n = auth_requests.swap(0, Ordering::Relaxed);
                self.stats.with(|s| {
                    s.auth_requests += n;
                    s.http.requests += n;
                });
                t?
            };
            let permit = self.limiter.acquire().await?;
            self.stats.with(|s| {
                s.http.throttle_wait_ms += permit.throttle_waited.as_millis() as u64;
                s.rate_wait_ms += (permit.waited - permit.throttle_waited).as_millis() as u64;
            });
            let mut req = prepared.clone();
            self.auth.apply(&ticket, &mut req, engine, scope)?;
            let res = inner.transport.send(&req, self.max_body).await;
            drop(permit);
            self.stats.with(|s| s.http.requests += 1);
            let cap = match res {
                Ok(c) => c,
                Err(e) if matches!(e.class, ErrorClass::Transient | ErrorClass::Timeout) => {
                    if attempt >= retry.max_attempts {
                        return Err(AtlatlError::new(
                            e.class,
                            format!(
                                "endpoint `{}`: {} (gave up after {attempt} attempts)",
                                self.name, e.message
                            ),
                        ));
                    }
                    let wait = retry.computed_wait(attempt, self.jitter());
                    self.record_attempt(attempt, e.class, e.message.clone(), wait);
                    self.limiter.sleep(wait).await;
                    attempt += 1;
                    continue;
                }
                Err(e) => return Err(e),
            };
            self.stats.status(cap.status);
            self.limiter.observe(cap.headers_iter());
            let view = ResponseView::new(cap, decode_spec.clone());
            let mut s = scope.clone();
            s.set("response", Value::from_dyn_object(view.clone())).set(
                "request",
                Value::from_pairs([
                    ("method", Value::from(req.method.as_str())),
                    ("url", Value::from(view.cap.url.as_str())),
                    ("attempts", Value::from(attempt)),
                ]),
            );
            let matched = rules::evaluate(&self.rules, engine, &s, &view, &inner.classifier)?;
            let (rule, mut action, mut class) = match matched {
                Some(m) => (Some(m.rule.clone()), m.rule.action, m.class),
                None => (None, Action::Ignore, ErrorClass::Decode),
            };
            let mut rule = rule;
            if action == Action::Ignore && needs_decode && view.decode_error().is_some() {
                // Processing needs a body that didn't decode: the built-in decode rule applies.
                rule = Some(rules::decode_rule());
                action = Action::Retry;
                class = inner.classifier.decode_class(&view);
            }
            match action {
                Action::Ignore => return Ok(Outcome::Process(view)),
                Action::Skip => {
                    let reason = match rule.as_ref().and_then(|r| r.message.as_deref()) {
                        Some(m) => engine.render(m, &s).unwrap_or_else(|_| m.to_owned()),
                        None => format!(
                            "HTTP {} matched `{}`",
                            view.status(),
                            rule.as_ref().map(Rule::describe).unwrap_or_default()
                        ),
                    };
                    return Ok(Outcome::Skip(reason));
                }
                Action::Stop => return Ok(Outcome::Stop),
                Action::Fail => return Err(self.failure(class, rule.as_ref(), &view, &s, None)),
                Action::Reauth => {
                    if reauthed {
                        return Err(self.failure(
                            ErrorClass::Auth,
                            rule.as_ref(),
                            &view,
                            &s,
                            Some(attempt),
                        ));
                    }
                    reauthed = true;
                    self.auth.invalidate(&ticket).await;
                    self.record_attempt(
                        attempt,
                        ErrorClass::Auth,
                        format!("HTTP {}: re-acquiring credentials", view.status()),
                        Duration::ZERO,
                    );
                    attempt += 1;
                }
                Action::Retry => {
                    let max = rule
                        .as_ref()
                        .and_then(|r| r.max_attempts)
                        .unwrap_or(retry.max_attempts);
                    let undecoded = class == ErrorClass::Decode && view.decode_error().is_some();
                    if undecoded && self.decode_mode == DecodeRetry::None && self.step.is_none() {
                        return Err(self.failure(class, rule.as_ref(), &view, &s, Some(attempt)));
                    }
                    if attempt >= max {
                        // Row mode: repeated failures may have been re-classed as throttling
                        // (`decode_failures_as_throttle`); the rows still split out as rejects.
                        let throttled_decode =
                            class == ErrorClass::Throttled && view.decode_error().is_some();
                        if (undecoded || throttled_decode) && self.row_mode() {
                            return Ok(Outcome::Undecodable(view));
                        }
                        return Err(self.failure(class, rule.as_ref(), &view, &s, Some(attempt)));
                    }
                    if class == ErrorClass::Auth {
                        self.auth.invalidate(&ticket).await;
                    }
                    let policy = rules::backoff_policy(
                        retry,
                        rule.as_ref().and_then(|r| r.backoff.as_ref()),
                    );
                    let mut wait = policy.computed_wait(attempt, self.jitter());
                    let msg = self.failure(class, rule.as_ref(), &view, &s, None).message;
                    if class == ErrorClass::Throttled {
                        if self.limiter.config().respect_headers
                            && let Some(h) = atlatl_core::error::wait_hint(
                                view.cap.headers_iter(),
                                std::time::SystemTime::now(),
                            )
                        {
                            wait = h;
                        }
                        self.limiter.throttled(wait);
                        self.record_attempt(attempt, class, msg, wait);
                    } else {
                        self.record_attempt(attempt, class, msg, wait);
                        self.limiter.sleep(wait).await;
                    }
                    attempt += 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod live_tests {
    use super::*;

    #[test]
    fn stats_changes_mirror_into_live_counters() {
        let live = Arc::new(Live::default());
        let h = StatsHandle::new(Some(live.clone()));
        h.with(|s| s.http.requests += 2);
        h.status(200);
        h.status(429);
        h.with(|s| {
            s.http.retries += 1;
            s.http.throttle_wait_ms += 1500;
            s.records += 10;
        });
        h.status(200);
        live.items_done.fetch_add(1, Ordering::Relaxed);
        let snap = live.snapshot();
        assert_eq!(
            (
                snap.requests,
                snap.retries,
                snap.throttle_wait_ms,
                snap.records,
                snap.items_done
            ),
            (2, 1, 1500, 10, 1)
        );
        assert_eq!(
            snap.status_counts,
            [("200".to_owned(), 2), ("429".to_owned(), 1)].into()
        );
        assert_eq!(snap.items_total, None);
        live.set_items_total(5);
        assert_eq!(live.snapshot().items_total, Some(5));
        assert_eq!(
            h.get().http.requests,
            2,
            "the report's stats are unchanged by mirroring"
        );
    }
}
