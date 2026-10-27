//! Typed API spec (07 §2). Parsed from the spanned node tree after deep-merging `defaults` into
//! every endpoint and setup request, so errors carry file:line.

use std::time::Duration;

use atlatl_core::ErrorClass;
use atlatl_core::config::ConfigError;
use atlatl_core::config::de::{Raw, Spanned, from_node};
use atlatl_core::config::model::BackoffKind;
use atlatl_core::config::node::{Entry, Map, Node, Span, Value, merge};
use atlatl_core::config::types::{ByteSize, Dur};
use indexmap::IndexMap;
use serde::Deserialize;

pub const DEFAULT_MAX_BODY: u64 = 256 * 1000 * 1000;
pub const DEFAULT_MAX_PAGES: u64 = 10_000;

pub type Expr = Spanned<String>;

#[derive(Clone, Debug)]
pub struct ApiSpec {
    pub name: Option<String>,
    pub description: Option<String>,
    pub base_url: Option<String>,
    pub auth: AuthSpec,
    /// `defaults.{headers, params, body, timeout}`, applied to auth token requests.
    pub default_request: RequestSpec,
    pub rate_limit: RateLimitSpec,
    pub throttle_patterns: Vec<String>,
    pub decode_failures_as_throttle: DecodeThrottle,
    pub max_body: u64,
    /// Spec-wide rules, then `defaults.rules`, in evaluation order.
    pub rules: Vec<RuleSpec>,
    pub state: IndexMap<String, serde_json::Value>,
    pub endpoints: IndexMap<String, Endpoint>,
    pub span: Span,
}

#[derive(Clone, Debug)]
pub struct Endpoint {
    pub name: String,
    pub span: Span,
    pub description: Option<String>,
    pub setup: Vec<SetupStep>,
    pub for_each: Option<ForEach>,
    pub request: RequestSpec,
    pub paginate: Option<Paginate>,
    pub decode: DecodeSpec,
    pub records: RecordsSpec,
    pub fields: IndexMap<String, Expr>,
    pub raw: bool,
    pub emit: IndexMap<String, Expr>,
    /// Endpoint rules then `defaults.rules`.
    pub rules: Vec<RuleSpec>,
    pub sync: IndexMap<String, SyncSpec>,
    pub history: History,
    pub depends_on: Vec<String>,
    /// `None` inherits the spec's auth.
    pub auth: Option<AuthSpec>,
    pub on_decode_error: OnDecodeError,
    pub max_skipped: Option<MaxSkipped>,
    pub record_skips: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Method {
    #[default]
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl Method {
    pub fn as_str(self) -> &'static str {
        match self {
            Method::Get => "GET",
            Method::Post => "POST",
            Method::Put => "PUT",
            Method::Patch => "PATCH",
            Method::Delete => "DELETE",
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestSpec {
    #[serde(default)]
    pub method: Method,
    pub path: Option<Expr>,
    pub url: Option<Expr>,
    #[serde(default)]
    pub params: IndexMap<String, serde_json::Value>,
    #[serde(default)]
    pub headers: IndexMap<String, serde_json::Value>,
    pub body: Option<Body>,
    pub timeout: Option<Dur>,
    #[serde(default)]
    pub keep_empty: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Body {
    Json(serde_json::Value),
    Form(IndexMap<String, serde_json::Value>),
    Text(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Retry,
    Skip,
    Ignore,
    Reauth,
    Stop,
    Fail,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleSpec {
    pub when: Expr,
    pub action: Action,
    pub class: Option<ErrorClass>,
    pub backoff: Option<BackoffSpec>,
    pub max_attempts: Option<u32>,
    pub message: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackoffSpec {
    #[serde(rename = "type")]
    pub kind: BackoffKind,
    pub base: Option<Dur>,
    pub max: Option<Dur>,
}

#[derive(Clone, Debug)]
pub struct Paginate {
    pub kind: PaginateKind,
    pub max_pages: u64,
    pub stop_on_empty: bool,
    pub stop_when: Option<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug)]
pub enum PaginateKind {
    Cursor {
        param: String,
        from: Expr,
    },
    Page {
        param: String,
        start: i64,
        step: i64,
    },
    Offset {
        param: String,
        start: u64,
        limit_param: Option<String>,
        limit: u64,
        total_from: Option<Expr>,
    },
    NextUrl {
        from: Expr,
    },
    LinkHeader {
        rel: String,
    },
    Expression {
        next: IndexMap<String, Expr>,
    },
}

impl PaginateKind {
    pub fn name(&self) -> &'static str {
        match self {
            PaginateKind::Cursor { .. } => "cursor",
            PaginateKind::Page { .. } => "page",
            PaginateKind::Offset { .. } => "offset",
            PaginateKind::NextUrl { .. } => "next_url",
            PaginateKind::LinkHeader { .. } => "link_header",
            PaginateKind::Expression { .. } => "expression",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    #[default]
    Json,
    Jsonl,
    Csv,
    Xml,
    Text,
    Bytes,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentTypeMode {
    #[default]
    Sniff,
    Trust,
    Ignore,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecodeSpec {
    #[serde(default)]
    pub format: Format,
    #[serde(default)]
    pub content_type: ContentTypeMode,
    pub max_body: Option<ByteSize>,
    /// CSV fields (quoted or not) equal to one of these decode as `null`; default `[""]`.
    pub null_values: Option<Vec<String>>,
}

impl DecodeSpec {
    pub fn csv_nulls(&self) -> &[String] {
        static EMPTY: std::sync::LazyLock<Vec<String>> =
            std::sync::LazyLock::new(|| vec![String::new()]);
        self.null_values.as_deref().unwrap_or(&EMPTY)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Dedup {
    First,
    Last,
    None,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalarAs {
    Value,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordsSpec {
    pub path: Option<Expr>,
    pub filter: Option<Expr>,
    #[serde(default)]
    pub primary_key: Vec<String>,
    pub dedup: Option<Dedup>,
    pub scalar_as: Option<ScalarAs>,
}

impl RecordsSpec {
    pub fn effective_dedup(&self) -> Dedup {
        match (self.dedup, self.primary_key.is_empty()) {
            (Some(d), _) => d,
            (None, false) => Dedup::First,
            (None, true) => Dedup::None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SetupStep {
    pub name: Option<String>,
    pub request: RequestSpec,
    pub paginate: Option<Paginate>,
    pub decode: DecodeSpec,
    pub records: RecordsSpec,
    pub rules: Vec<RuleSpec>,
    pub output: Option<(SetupOutput, String)>,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetupOutput {
    Collect,
    Set,
    Max,
    Min,
}

#[derive(Clone, Debug)]
pub struct ForEach {
    pub over: Over,
    pub var: String,
    pub concurrency: Option<u32>,
    pub batch: Option<Batch>,
    /// Item fields moved into `secrets.item.*` (07 §3): registered for redaction, never bound as
    /// `item.*`, so never landed, emitted or stored in a reject.
    pub secret: Vec<String>,
    /// At most this many items per run (07 §7); the rest are deferred to later runs.
    pub max_items: Option<MaxItems>,
    /// Leave out items skipped (`record_skips`) within this long; retry the rest after new items.
    pub except_skipped: Option<Dur>,
}

#[derive(Clone, Debug)]
pub enum MaxItems {
    N(u64),
    /// Evaluated like `over`, so `vars.*` (and `--var`) can set it per run.
    Expr(Expr),
}

#[derive(Clone, Debug)]
pub enum Over {
    List(Vec<serde_json::Value>),
    Expr(Expr),
    /// Items an earlier endpoint emitted this run; `except_landed` as for `Query`, after `unique`.
    Queue {
        queue: String,
        unique: bool,
        except_landed: Vec<String>,
    },
    /// SQL against a lake or Postgres connection (07 §7). One column yields scalars, more yield
    /// maps. `except_landed` drops items whose values for these columns are already in the
    /// stream's target table (the "keys not yet landed" worklist).
    Query {
        query: String,
        connection: String,
        except_landed: Vec<String>,
    },
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Batch {
    pub size: u32,
    #[serde(rename = "as")]
    pub var: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Aggregate {
    Set,
    Max,
    Min,
    Last,
    First,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncSpec {
    pub value: Expr,
    pub aggregate: Aggregate,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum History {
    #[default]
    Replayable,
    SnapshotOnly,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecodeRetry {
    #[default]
    Request,
    Row,
    None,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OnDecodeError {
    #[serde(default)]
    pub retry: DecodeRetry,
    pub reject_threshold: Option<RejectThreshold>,
    /// Open rejects older than this expire (03 §7); a stream's `on_decode_error.reject_ttl` wins.
    pub reject_ttl: Option<Dur>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RejectThreshold {
    pub ratio: f64,
    pub min_count: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaxSkipped {
    pub ratio: f64,
    pub min_items: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LimiterScope {
    #[default]
    Connection,
    Endpoint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetPer {
    Run,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Budget {
    pub requests: u64,
    pub per: BudgetPer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnThrottle {
    Halve,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Adaptive {
    pub on_throttle: OnThrottle,
    pub recover_after: Option<Dur>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimitSpec {
    pub rps: Option<f64>,
    pub burst: Option<u32>,
    pub min_interval: Option<Dur>,
    pub max_in_flight: Option<u32>,
    #[serde(default)]
    pub scope: LimiterScope,
    pub budget: Option<Budget>,
    #[serde(default = "yes")]
    pub respect_headers: bool,
    pub adaptive: Option<Adaptive>,
    /// Whether the spec declared `rate_limit` at all (`atlatl validate` warns otherwise).
    #[serde(skip)]
    pub declared: bool,
}

fn yes() -> bool {
    true
}

impl Default for RateLimitSpec {
    fn default() -> Self {
        Self {
            rps: None,
            burst: None,
            min_interval: None,
            max_in_flight: None,
            scope: LimiterScope::Connection,
            budget: None,
            respect_headers: true,
            adaptive: None,
            declared: false,
        }
    }
}

/// More than `count` decode failures within `window` on one connection count as throttling.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodeThrottle {
    pub count: u32,
    pub window: Duration,
}

impl Default for DecodeThrottle {
    fn default() -> Self {
        Self {
            count: 3,
            window: Duration::from_secs(60),
        }
    }
}

impl DecodeThrottle {
    /// `3/60s` or `{count: 3, window: 60s}`.
    fn parse(n: &Node) -> Result<Self, ConfigError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct M {
            count: u32,
            window: Dur,
        }
        if n.as_map().is_some() {
            let m: M = from_node(n)?;
            return Ok(Self {
                count: m.count,
                window: m.window.0,
            });
        }
        let s = n.as_str().unwrap_or_default();
        let bad = || {
            ConfigError::new(format!(
                "`decode_failures_as_throttle` must be `N/<duration>` (e.g. `3/60s`), found `{s}`"
            ))
            .at(n.span.clone())
        };
        let (c, w) = s.split_once('/').ok_or_else(bad)?;
        Ok(Self {
            count: c.trim().parse().map_err(|_| bad())?,
            window: Dur::parse(w).map_err(|_| bad())?.0,
        })
    }
}

// ----------------------------------------------------------------------------- auth

#[derive(Clone, Debug, Default)]
pub enum AuthSpec {
    #[default]
    None,
    Headers(IndexMap<String, String>),
    Query(IndexMap<String, String>),
    Bearer {
        token: String,
    },
    Basic {
        username: String,
        password: String,
    },
    OAuth2(Box<OAuth2Spec>),
    Hmac(Box<HmacSpec>),
}

impl AuthSpec {
    pub fn type_name(&self) -> &'static str {
        match self {
            AuthSpec::None => "none",
            AuthSpec::Headers(_) => "headers",
            AuthSpec::Query(_) => "query",
            AuthSpec::Bearer { .. } => "bearer",
            AuthSpec::Basic { .. } => "basic",
            AuthSpec::OAuth2(_) => "oauth2",
            AuthSpec::Hmac(_) => "hmac",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OAuthFlow {
    ClientCredentials,
    Password,
    RefreshToken,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientAuth {
    #[default]
    Basic,
    Body,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OAuth2Spec {
    pub flow: OAuthFlow,
    pub token_url: String,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    #[serde(default)]
    pub scopes: Vec<String>,
    #[serde(default)]
    pub client_auth: ClientAuth,
    pub username: Option<String>,
    pub password: Option<String>,
    pub refresh_token: Option<String>,
    #[serde(default)]
    pub extra: IndexMap<String, String>,
    pub reauth_when: Option<Expr>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HmacAlg {
    Sha256,
    Sha512,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SigEncoding {
    #[default]
    Base64,
    Hex,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Placement {
    Header(String),
    Param(String),
}

#[derive(Clone, Debug)]
pub struct HmacSpec {
    pub algorithm: HmacAlg,
    pub key: String,
    pub canonical: String,
    pub encoding: SigEncoding,
    pub placement: Placement,
    /// Added to the query string before signing (e.g. `clientId`, `timestamp`).
    pub params: IndexMap<String, String>,
    pub headers: IndexMap<String, String>,
}

fn parse_auth(n: &Node) -> Result<AuthSpec, ConfigError> {
    if n.as_str() == Some("none") || n.is_null() {
        return Ok(AuthSpec::None);
    }
    if n.as_map().is_none() {
        return Err(
            ConfigError::new("`auth` must be `none` or a map with a `type`").at(n.span.clone()),
        );
    }
    let ty = n
        .get("type")
        .ok_or_else(|| ConfigError::new("`auth` is missing `type`").at(n.span.clone()))?;
    let (rest, _) = n.take_keys(&["type"]);
    let name = ty.as_str().unwrap_or_default();
    Ok(match name {
        "none" => {
            no_keys(&rest, "auth.type none")?;
            AuthSpec::None
        }
        "headers" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct H {
                headers: IndexMap<String, String>,
            }
            AuthSpec::Headers(from_node::<H>(&rest)?.headers)
        }
        "query" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Q {
                params: IndexMap<String, String>,
            }
            AuthSpec::Query(from_node::<Q>(&rest)?.params)
        }
        "bearer" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct B {
                token: String,
            }
            AuthSpec::Bearer {
                token: from_node::<B>(&rest)?.token,
            }
        }
        "basic" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct B {
                username: String,
                #[serde(default)]
                password: String,
            }
            let b: B = from_node(&rest)?;
            AuthSpec::Basic {
                username: b.username,
                password: b.password,
            }
        }
        "oauth2" => {
            let o: OAuth2Spec = from_node(&rest)?;
            let need = |v: &Option<String>, f: &str| {
                if v.is_none() {
                    Err(ConfigError::new(format!(
                        "oauth2 flow `{}` needs `{f}`",
                        flow_name(o.flow)
                    ))
                    .at(n.span.clone()))
                } else {
                    Ok(())
                }
            };
            match o.flow {
                OAuthFlow::ClientCredentials => need(&o.client_id, "client_id")?,
                OAuthFlow::Password => {
                    need(&o.username, "username")?;
                    need(&o.password, "password")?;
                }
                OAuthFlow::RefreshToken => need(&o.refresh_token, "refresh_token")?,
            }
            AuthSpec::OAuth2(Box::new(o))
        }
        "hmac" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct H {
                algorithm: HmacAlg,
                key: String,
                canonical: String,
                #[serde(default)]
                encoding: SigEncoding,
                placement: Spanned<PlacementDe>,
                #[serde(default)]
                params: IndexMap<String, String>,
                #[serde(default)]
                headers: IndexMap<String, String>,
            }
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct PlacementDe {
                header: Option<String>,
                param: Option<String>,
            }
            let h: H = from_node(&rest)?;
            let placement = match (&h.placement.header, &h.placement.param) {
                (Some(x), None) => Placement::Header(x.clone()),
                (None, Some(x)) => Placement::Param(x.clone()),
                _ => {
                    return Err(ConfigError::new(
                        "hmac `placement` needs exactly one of `header` or `param`",
                    )
                    .at(h.placement.span.clone()));
                }
            };
            AuthSpec::Hmac(Box::new(HmacSpec {
                algorithm: h.algorithm,
                key: h.key,
                canonical: h.canonical,
                encoding: h.encoding,
                placement,
                params: h.params,
                headers: h.headers,
            }))
        }
        "sigv4" => {
            return Err(
                ConfigError::new("auth type `sigv4` is not supported yet").at(ty.span.clone())
            );
        }
        other => {
            return Err(ConfigError::new(format!(
                "unknown auth type `{other}` (supported: none, headers, query, bearer, basic, oauth2, hmac)"
            ))
            .at(ty.span.clone()));
        }
    })
}

fn flow_name(f: OAuthFlow) -> &'static str {
    match f {
        OAuthFlow::ClientCredentials => "client_credentials",
        OAuthFlow::Password => "password",
        OAuthFlow::RefreshToken => "refresh_token",
    }
}

fn no_keys(n: &Node, what: &str) -> Result<(), ConfigError> {
    if let Some((k, e)) = n.as_map().and_then(|m| m.iter().next()) {
        return Err(
            ConfigError::new(format!("unknown field `{k}` for {what}")).at(e.key_span.clone())
        );
    }
    Ok(())
}

// ----------------------------------------------------------------------------- pagination

fn parse_paginate(n: &Node) -> Result<Paginate, ConfigError> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Common {
        max_pages: Option<u64>,
        stop_on_empty: Option<bool>,
        stop_when: Option<Expr>,
    }
    if n.as_map().is_none() {
        return Err(ConfigError::new("`paginate` must be a map with a `type`").at(n.span.clone()));
    }
    let ty = n
        .get("type")
        .ok_or_else(|| ConfigError::new("`paginate` is missing `type`").at(n.span.clone()))?;
    let (rest, common) = n.take_keys(&["type", "max_pages", "stop_on_empty", "stop_when"]);
    let mut common_map = Map::new();
    for (k, e) in common {
        if k != "type" {
            common_map.insert(k, e);
        }
    }
    let c: Common = from_node(&Node::map(common_map, n.span.clone()))?;
    let kind = match ty.as_str().unwrap_or_default() {
        "cursor" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct C {
                param: String,
                from: Expr,
            }
            let v: C = from_node(&rest)?;
            PaginateKind::Cursor {
                param: v.param,
                from: v.from,
            }
        }
        "page" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct P {
                param: String,
                start: Option<i64>,
                step: Option<i64>,
            }
            let v: P = from_node(&rest)?;
            PaginateKind::Page {
                param: v.param,
                start: v.start.unwrap_or(1),
                step: v.step.unwrap_or(1),
            }
        }
        "offset" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct O {
                param: String,
                start: Option<u64>,
                limit_param: Option<String>,
                limit: u64,
                total_from: Option<Expr>,
            }
            let v: O = from_node(&rest)?;
            if v.limit == 0 {
                return Err(
                    ConfigError::new("offset `limit` must be at least 1").at(rest.span.clone())
                );
            }
            PaginateKind::Offset {
                param: v.param,
                start: v.start.unwrap_or(0),
                limit_param: v.limit_param,
                limit: v.limit,
                total_from: v.total_from,
            }
        }
        "next_url" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct U {
                from: Expr,
            }
            PaginateKind::NextUrl {
                from: from_node::<U>(&rest)?.from,
            }
        }
        "link_header" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct L {
                rel: Option<String>,
            }
            PaginateKind::LinkHeader {
                rel: from_node::<L>(&rest)?.rel.unwrap_or_else(|| "next".into()),
            }
        }
        "expression" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct E {
                next: IndexMap<String, Expr>,
            }
            let v: E = from_node(&rest)?;
            if c.stop_when.is_none() {
                return Err(
                    ConfigError::new("`paginate: {type: expression}` needs `stop_when`")
                        .at(n.span.clone()),
                );
            }
            PaginateKind::Expression { next: v.next }
        }
        "body_cursor" => {
            return Err(
                ConfigError::new("paginate type `body_cursor` is not supported yet")
                    .at(ty.span.clone()),
            );
        }
        other => {
            return Err(ConfigError::new(format!(
                "unknown paginate type `{other}` (supported: cursor, page, offset, next_url, link_header, expression)"
            ))
            .at(ty.span.clone()));
        }
    };
    // Token-driven strategies end on their own signal; a page whose records are all filtered out
    // must not end them.
    let token_driven = matches!(
        kind,
        PaginateKind::Cursor { .. }
            | PaginateKind::NextUrl { .. }
            | PaginateKind::LinkHeader { .. }
    );
    Ok(Paginate {
        kind,
        max_pages: c.max_pages.unwrap_or(DEFAULT_MAX_PAGES),
        stop_on_empty: c.stop_on_empty.unwrap_or(!token_driven),
        stop_when: c.stop_when,
        span: n.span.clone(),
    })
}

// ----------------------------------------------------------------------------- for_each

fn parse_for_each(n: &Node) -> Result<ForEach, ConfigError> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct F {
        over: Raw<()>,
        #[serde(rename = "as")]
        var: Option<String>,
        concurrency: Option<u32>,
        batch: Option<Batch>,
        #[serde(default)]
        secret: Vec<String>,
        max_items: Option<Raw<()>>,
        except_skipped: Option<ExceptSkipped>,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ExceptSkipped {
        ttl: Dur,
    }
    let f: F = from_node(n)?;
    if f.except_skipped.is_some() && f.batch.is_some() {
        return Err(ConfigError::new(
            "`for_each.except_skipped` can't be combined with `batch` (a skip is recorded per batch)",
        )
        .at(n.span.clone()));
    }
    let max_items = match f.max_items.as_ref().map(|r| &r.node) {
        None => None,
        Some(m) => match &m.value {
            Value::Scalar(s) if s.as_u64().is_some() => s.as_u64().map(MaxItems::N),
            Value::Scalar(s) if !s.text.trim().starts_with('-') => {
                Some(MaxItems::Expr(Spanned::new(s.text.clone(), m.span.clone())))
            }
            _ => {
                return Err(ConfigError::new(
                    "`for_each.max_items` must be a non-negative integer or an expression",
                )
                .at(m.span.clone()));
            }
        },
    };
    if !f.secret.is_empty() && f.batch.is_some() {
        return Err(
            ConfigError::new("`for_each.secret` can't be combined with `batch`").at(n.span.clone()),
        );
    }
    let o = &f.over.node;
    let over = match &o.value {
        Value::Seq(_) => Over::List(o.to_json().as_array().cloned().unwrap_or_default()),
        Value::Scalar(s) => Over::Expr(Spanned::new(s.text.clone(), o.span.clone())),
        Value::Map(m) if m.contains_key("queue") => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Q {
                queue: String,
                #[serde(default)]
                unique: bool,
                #[serde(default)]
                except_landed: Vec<String>,
            }
            let q: Q = from_node(o)?;
            Over::Queue {
                queue: q.queue,
                unique: q.unique,
                except_landed: q.except_landed,
            }
        }
        Value::Map(m) if m.contains_key("query") => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Q {
                query: String,
                connection: String,
                #[serde(default)]
                except_landed: Vec<String>,
            }
            let q: Q = from_node(o)?;
            Over::Query {
                query: q.query,
                connection: q.connection,
                except_landed: q.except_landed,
            }
        }
        _ => {
            return Err(ConfigError::new(
                "`for_each.over` must be a list, an expression, `{queue: …}` or `{query: …, connection: …}`",
            )
            .at(o.span.clone()));
        }
    };
    Ok(ForEach {
        over,
        var: f.var.unwrap_or_else(|| "item".into()),
        concurrency: f.concurrency,
        batch: f.batch,
        secret: f.secret,
        max_items,
        except_skipped: f.except_skipped.map(|e| e.ttl),
    })
}

// ----------------------------------------------------------------------------- spec

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RootDe {
    name: Option<String>,
    description: Option<String>,
    base_url: Option<String>,
    auth: Option<Raw<()>>,
    rate_limit: Option<RateLimitSpec>,
    #[serde(default)]
    throttle_patterns: Vec<String>,
    decode_failures_as_throttle: Option<Raw<()>>,
    max_body: Option<ByteSize>,
    #[serde(default)]
    rules: Vec<RuleSpec>,
    #[serde(default)]
    state: IndexMap<String, serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DefaultsDe {
    #[serde(default)]
    headers: IndexMap<String, serde_json::Value>,
    #[serde(default)]
    params: IndexMap<String, serde_json::Value>,
    body: Option<Body>,
    timeout: Option<Dur>,
    #[serde(default)]
    rules: Vec<RuleSpec>,
    records: Option<RecordsSpec>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EndpointDe {
    description: Option<String>,
    #[serde(default)]
    setup: Vec<Raw<()>>,
    for_each: Option<Raw<()>>,
    #[serde(default)]
    request: RequestSpec,
    paginate: Option<Raw<()>>,
    #[serde(default)]
    decode: DecodeSpec,
    #[serde(default)]
    records: RecordsSpec,
    #[serde(default)]
    fields: IndexMap<String, Expr>,
    #[serde(default)]
    raw: bool,
    #[serde(default)]
    emit: IndexMap<String, Expr>,
    #[serde(default)]
    rules: Vec<RuleSpec>,
    #[serde(default)]
    sync: IndexMap<String, SyncSpec>,
    #[serde(default)]
    history: History,
    #[serde(default)]
    depends_on: Vec<String>,
    auth: Option<Raw<()>>,
    #[serde(default)]
    on_decode_error: OnDecodeError,
    max_skipped: Option<MaxSkipped>,
    #[serde(default)]
    record_skips: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SetupDe {
    name: Option<String>,
    #[serde(default)]
    request: RequestSpec,
    paginate: Option<Raw<()>>,
    #[serde(default)]
    decode: DecodeSpec,
    #[serde(default)]
    records: RecordsSpec,
    #[serde(default)]
    rules: Vec<RuleSpec>,
    collect: Option<String>,
    set: Option<String>,
    max: Option<String>,
    min: Option<String>,
}

/// Replaces (or inserts) `key` in a map node with `merge(base, node[key])`.
fn merged_child(node: &Node, key: &str, base: Option<&Node>) -> Node {
    let Some(base) = base else {
        return node.clone();
    };
    let Some(map) = node.as_map() else {
        return node.clone();
    };
    let mut out = map.clone();
    let value = match map.get(key) {
        Some(e) if e.value.is_null() => return node.clone(),
        Some(e) => merge(base, &e.value),
        None => base.clone(),
    };
    let key_span = map
        .get(key)
        .map(|e| e.key_span.clone())
        .unwrap_or_else(|| base.span.clone());
    out.insert(key.to_owned(), Entry { key_span, value });
    Node::map(out, node.span.clone())
}

fn with_defaults(node: &Node, request: Option<&Node>, records: Option<&Node>) -> Node {
    let n = merged_child(node, "request", request);
    merged_child(&n, "records", records)
}

impl ApiSpec {
    /// Reads and parses `root/rel` (a project's `apis/*.yaml`).
    pub fn load(root: &std::path::Path, rel: &str) -> Result<Self, atlatl_core::AtlatlError> {
        Self::load_config(root, rel).map_err(|e| atlatl_core::AtlatlError::config(e.to_string()))
    }

    /// Valid but likely unintended (`atlatl validate` warnings).
    pub fn lints(&self) -> Vec<ConfigError> {
        let rl = &self.rate_limit;
        let parallel = rl.max_in_flight.unwrap_or(0) > 1 || rl.rps.unwrap_or(0.0) > 1.0;
        let mut out = Vec::new();
        for ep in self.endpoints.values() {
            let Some(f) = &ep.for_each else { continue };
            if f.concurrency.is_none() && parallel {
                out.push(
                    ConfigError::new(format!(
                        "endpoint `{}`: `for_each` runs one item at a time (`concurrency` defaults to 1), so `rate_limit` ({}) can't be reached; set `for_each.concurrency`",
                        ep.name,
                        match (rl.rps, rl.max_in_flight) {
                            (Some(r), Some(m)) => format!("rps {r}, max_in_flight {m}"),
                            (Some(r), None) => format!("rps {r}"),
                            (None, Some(m)) => format!("max_in_flight {m}"),
                            (None, None) => String::new(),
                        }
                    ))
                    .at(ep.span.clone()),
                );
            }
            let landed = match &f.over {
                Over::Queue { except_landed, .. } | Over::Query { except_landed, .. } => {
                    !except_landed.is_empty()
                }
                _ => false,
            };
            let skips = ep
                .rules
                .iter()
                .chain(&self.rules)
                .any(|r| r.action == Action::Skip);
            if f.max_items.is_some() && f.except_skipped.is_none() && skips {
                out.push(
                    ConfigError::new(format!(
                        "endpoint `{}`: `for_each.max_items` with a `skip` rule but no `except_skipped`: an item a rule skips never lands, so it heads every batch",
                        ep.name
                    ))
                    .at(ep.span.clone()),
                );
            }
            if f.max_items.is_some() && !landed {
                out.push(
                    ConfigError::new(format!(
                        "endpoint `{}`: `for_each.max_items` without `except_landed` runs the same first items every run",
                        ep.name
                    ))
                    .at(ep.span.clone()),
                );
            }
        }
        out
    }

    /// [`Self::load`] keeping the span, for diagnostics. Spans name the file as the project
    /// loader does (`root.join(rel)`), so they render with its snippets.
    pub fn load_config(root: &std::path::Path, rel: &str) -> Result<Self, ConfigError> {
        let path = root.join(rel);
        let text = std::fs::read_to_string(&path)
            .map_err(|e| ConfigError::new(format!("reading API spec `{rel}`: {e}")))?;
        let node = atlatl_core::config::node::parse_yaml(&text, std::sync::Arc::from(path))?;
        Self::parse(&node)
    }

    pub fn parse(node: &Node) -> Result<Self, ConfigError> {
        if node.as_map().is_none() {
            return Err(ConfigError::new("an API spec must be a map").at(node.span.clone()));
        }
        let (rest, taken) = node.take_keys(&["defaults", "endpoints"]);
        let root: RootDe = from_node(&rest)?;
        let defaults_node = taken.get("defaults").map(|e| e.value.clone());
        let defaults: DefaultsDe = match &defaults_node {
            Some(n) if !n.is_null() => from_node(n)?,
            _ => from_node(&Node::map(Map::new(), node.span.clone()))?,
        };
        let (req_node, records_node) = match &defaults_node {
            Some(d) if d.as_map().is_some() => {
                let (_, req) = d.take_keys(&["headers", "params", "body", "timeout"]);
                let req = (!req.is_empty()).then(|| Node::map(req, d.span.clone()));
                (req, d.get("records").cloned())
            }
            _ => (None, None),
        };
        let default_request = match &req_node {
            Some(r) => from_node(r)?,
            None => RequestSpec::default(),
        };

        let eps_entry = taken
            .get("endpoints")
            .ok_or_else(|| ConfigError::new("API spec has no `endpoints`").at(node.span.clone()))?;
        let eps = eps_entry.value.as_map().ok_or_else(|| {
            ConfigError::new("`endpoints` must be a map").at(eps_entry.value.span.clone())
        })?;
        let mut endpoints = IndexMap::new();
        for (name, e) in eps {
            let ep_node = if e.value.is_null() {
                Node::map(Map::new(), e.key_span.clone())
            } else {
                e.value.clone()
            };
            let merged = with_defaults(&ep_node, req_node.as_ref(), records_node.as_ref());
            let de: EndpointDe = from_node(&merged)?;
            let mut setup = Vec::new();
            for s in &de.setup {
                let sm = with_defaults(&s.node, req_node.as_ref(), None);
                let sd: SetupDe = from_node(&sm)?;
                let outs: Vec<_> = [
                    (SetupOutput::Collect, sd.collect),
                    (SetupOutput::Set, sd.set),
                    (SetupOutput::Max, sd.max),
                    (SetupOutput::Min, sd.min),
                ]
                .into_iter()
                .filter_map(|(k, v)| v.map(|v| (k, v)))
                .collect();
                if outs.len() > 1 {
                    return Err(ConfigError::new(
                        "a setup step takes at most one of `collect`, `set`, `max`, `min`",
                    )
                    .at(s.node.span.clone()));
                }
                let mut rules = sd.rules;
                rules.extend(defaults.rules.iter().cloned());
                setup.push(SetupStep {
                    name: sd.name,
                    request: sd.request,
                    paginate: sd
                        .paginate
                        .as_ref()
                        .map(|p| parse_paginate(&p.node))
                        .transpose()?,
                    decode: sd.decode,
                    records: sd.records,
                    rules,
                    output: outs.into_iter().next(),
                    span: s.node.span.clone(),
                });
            }
            let mut rules = de.rules;
            rules.extend(defaults.rules.iter().cloned());
            let ep = Endpoint {
                name: name.clone(),
                span: e.key_span.clone(),
                description: de.description,
                setup,
                for_each: de
                    .for_each
                    .as_ref()
                    .map(|f| parse_for_each(&f.node))
                    .transpose()?,
                request: de.request,
                paginate: de
                    .paginate
                    .as_ref()
                    .map(|p| parse_paginate(&p.node))
                    .transpose()?,
                decode: de.decode,
                records: de.records,
                fields: de.fields,
                raw: de.raw,
                emit: de.emit,
                rules,
                sync: de.sync,
                history: de.history,
                depends_on: de.depends_on,
                auth: de.auth.as_ref().map(|a| parse_auth(&a.node)).transpose()?,
                on_decode_error: de.on_decode_error,
                max_skipped: de.max_skipped,
                record_skips: de.record_skips,
            };
            if ep
                .for_each
                .as_ref()
                .is_some_and(|f| f.except_skipped.is_some())
                && !ep.record_skips
            {
                return Err(ConfigError::new(format!(
                    "endpoint `{name}`: `for_each.except_skipped` needs `record_skips: true`, which records the skips it reads"
                ))
                .at(ep.span.clone()));
            }
            validate_endpoint(&ep)?;
            endpoints.insert(name.clone(), ep);
        }
        let _ = defaults.records;
        let _ = (
            defaults.headers,
            defaults.params,
            defaults.body,
            defaults.timeout,
        );
        let rate_limit = match (&root.rate_limit, rest.get("rate_limit")) {
            (Some(r), _) => RateLimitSpec {
                declared: true,
                ..r.clone()
            },
            _ => RateLimitSpec::default(),
        };
        if let Some(rps) = rate_limit.rps
            && !(rps > 0.0 && rps.is_finite())
        {
            return Err(ConfigError::new("`rate_limit.rps` must be positive")
                .at_opt(rest.get("rate_limit").map(|n| &n.span)));
        }
        if rate_limit.max_in_flight == Some(0) || rate_limit.burst == Some(0) {
            return Err(ConfigError::new(
                "`rate_limit.max_in_flight` and `burst` must be at least 1",
            )
            .at_opt(rest.get("rate_limit").map(|n| &n.span)));
        }
        Ok(ApiSpec {
            name: root.name,
            description: root.description,
            base_url: root.base_url,
            auth: root
                .auth
                .as_ref()
                .map(|a| parse_auth(&a.node))
                .transpose()?
                .unwrap_or_default(),
            default_request,
            rate_limit,
            throttle_patterns: root.throttle_patterns,
            decode_failures_as_throttle: root
                .decode_failures_as_throttle
                .as_ref()
                .map(|n| DecodeThrottle::parse(&n.node))
                .transpose()?
                .unwrap_or_default(),
            max_body: root.max_body.map(|b| b.0).unwrap_or(DEFAULT_MAX_BODY),
            rules: root.rules,
            state: root.state,
            endpoints,
            span: node.span.clone(),
        })
    }

    pub fn endpoint(&self, name: &str) -> Option<&Endpoint> {
        self.endpoints.get(name)
    }
}

fn validate_endpoint(ep: &Endpoint) -> Result<(), ConfigError> {
    if ep.decode.format == Format::Xml {
        return Err(ConfigError::new(format!(
            "endpoint `{}`: decode format `xml` is not supported yet",
            ep.name
        ))
        .at(ep.span.clone()));
    }
    if ep.request.path.is_some() && ep.request.url.is_some() {
        return Err(ConfigError::new(format!(
            "endpoint `{}`: set either `request.path` or `request.url`, not both",
            ep.name
        ))
        .at(ep.span.clone()));
    }
    if ep.records.dedup.is_some_and(|d| d != Dedup::None) && ep.records.primary_key.is_empty() {
        return Err(ConfigError::new(format!(
            "endpoint `{}`: `records.dedup` needs `records.primary_key`",
            ep.name
        ))
        .at(ep.span.clone()));
    }
    check_queries(
        &format!("endpoint `{}`", ep.name),
        &ep.records,
        ep.paginate.as_ref(),
    )?;
    for (i, s) in ep.setup.iter().enumerate() {
        let name = s
            .name
            .as_ref()
            .map(|n| format!(" (`{n}`)"))
            .unwrap_or_default();
        let ctx = format!("endpoint `{}` setup step {}{name}", ep.name, i + 1);
        check_queries(&ctx, &s.records, s.paginate.as_ref())?;
    }
    Ok(())
}

/// Compiles the JMESPath a request's `Records` and `Pager` will run.
fn check_queries(
    ctx: &str,
    records: &RecordsSpec,
    paginate: Option<&Paginate>,
) -> Result<(), ConfigError> {
    let pager = match paginate.map(|p| &p.kind) {
        Some(PaginateKind::Cursor { from, .. } | PaginateKind::NextUrl { from }) => Some(from),
        Some(PaginateKind::Offset { total_from, .. }) => total_from.as_ref(),
        _ => None,
    };
    for q in records.path.iter().chain(pager) {
        crate::records::Query::parse(q)
            .map_err(|e| ConfigError::new(format!("{ctx}: {}", e.message)).at(q.span.clone()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use atlatl_core::config::node::parse_yaml;

    use super::*;

    pub(crate) fn parse(s: &str) -> Result<ApiSpec, ConfigError> {
        ApiSpec::parse(&parse_yaml(s, Arc::from(Path::new("apis/t.yaml"))).expect("yaml"))
    }

    #[test]
    fn defaults_deep_merge() {
        let s = parse(
            r#"
name: t
base_url: https://x
defaults:
  params: {c: "{{ secrets.k }}", f: json}
  headers: {Accept: application/json}
  timeout: 30s
  rules: [{when: "response.status == 404", action: skip}]
  records: {primary_key: [id]}
rules:
  - {when: "response.status == 409", action: retry, class: throttled}
endpoints:
  a:
    request: {path: /a, params: {f: null, d: 1}}
    rules: [{when: "response.status == 500", action: fail}]
  b:
    records: {path: "[]"}
"#,
        )
        .expect("parse");
        let a = &s.endpoints["a"];
        assert_eq!(
            a.request.params.keys().collect::<Vec<_>>(),
            ["c", "d"],
            "null removes an inherited key"
        );
        assert_eq!(a.request.params["d"], serde_json::json!(1));
        assert_eq!(
            a.request.timeout.map(|d| d.0),
            Some(Duration::from_secs(30))
        );
        assert_eq!(a.request.headers["Accept"], "application/json");
        assert_eq!(
            a.rules
                .iter()
                .map(|r| r.when.value.as_str())
                .collect::<Vec<_>>(),
            ["response.status == 500", "response.status == 404"]
        );
        assert_eq!(a.records.primary_key, ["id"]);
        let b = &s.endpoints["b"];
        assert_eq!(b.request.params.len(), 2);
        assert_eq!(b.records.path.as_deref().map(String::as_str), Some("[]"));
        assert_eq!(b.records.primary_key, ["id"]);
        assert_eq!(s.default_request.params.len(), 2);
        assert_eq!(s.rules.len(), 1);
        assert!(!s.rate_limit.declared);
    }

    #[test]
    fn errors_carry_location() {
        let e =
            parse("endpoints:\n  a:\n    request: {path: /a, parms: {}}\n").expect_err("unknown");
        assert!(e.message.contains("unknown field `parms`"), "{e}");
        assert_eq!(e.span.as_ref().map(|s| s.line), Some(3));
        let e =
            parse("endpoints:\n  a:\n    paginate: {type: cursor, param: p}\n").expect_err("from");
        assert!(e.message.contains("missing field `from`"), "{e}");
        assert_eq!(e.span.as_ref().map(|s| s.line), Some(3));
        let e = parse("endpoints:\n  a:\n    paginate:\n      type: spiral\n").expect_err("type");
        assert!(e.message.contains("unknown paginate type `spiral`"), "{e}");
        assert_eq!(e.span.as_ref().map(|s| s.line), Some(4));
        let e = parse("auth: {type: oauth2, flow: password, token_url: u}\nendpoints: {}\n")
            .expect_err("oauth");
        assert!(e.message.contains("needs `username`"), "{e}");
        let e = parse("endpoints:\n  a:\n    rules:\n      - {when: x, action: explode}\n")
            .expect_err("action");
        assert!(e.message.contains("unknown variant `explode`"), "{e}");
        assert_eq!(e.span.as_ref().map(|s| s.line), Some(4));
    }

    #[test]
    fn invalid_jmespath_is_a_parse_error() {
        for (yaml, line) in [
            ("endpoints:\n  a:\n    records: {path: \".\"}\n", 3),
            ("endpoints:\n  a:\n    records:\n      path: \"a[?\"\n", 4),
            (
                "endpoints:\n  a:\n    paginate: {type: cursor, param: c, from: \"next[\"}\n",
                3,
            ),
            (
                "endpoints:\n  a:\n    paginate: {type: next_url, from: \"..\"}\n",
                3,
            ),
            (
                "endpoints:\n  a:\n    paginate: {type: offset, param: o, limit: 10, total_from: \"t[?\"}\n",
                3,
            ),
            (
                "defaults:\n  records: {path: \".\"}\nendpoints:\n  a: {}\n",
                2,
            ),
        ] {
            let e = parse(yaml).expect_err(yaml);
            assert!(e.message.contains("endpoint `a`: invalid JMESPath"), "{e}");
            assert_eq!(e.span.as_ref().map(|s| s.line), Some(line), "{yaml}");
        }
        let setup = |step: &str| {
            format!(
                "endpoints:\n  a:\n    setup:\n      - {{request: {{path: /x}}, records: {{path: \"@\"}}, collect: xs}}\n      - {step}\n    request: {{path: /a}}\n"
            )
        };
        for (step, ctx) in [
            (
                "{name: ids, request: {path: /ids}, records: {path: \".\"}, collect: ids}",
                "endpoint `a` setup step 2 (`ids`): invalid JMESPath",
            ),
            (
                "{request: {path: /ids}, paginate: {type: cursor, param: c, from: \"next[\"}, collect: ids}",
                "endpoint `a` setup step 2: invalid JMESPath",
            ),
        ] {
            let e = parse(&setup(step)).expect_err(step);
            assert!(e.message.contains(ctx), "{e}");
            assert_eq!(e.span.as_ref().map(|s| s.line), Some(5), "{step}");
        }
        parse(&setup(
            "{request: {path: /ids}, records: {path: \"ids[]\"}, paginate: {type: next_url, from: links.next}, collect: ids}",
        ))
        .expect("valid setup");
        for ok in ["@", "", "data.items[]", "entities[?id!=null].{id: id}"] {
            parse(&format!(
                "endpoints:\n  a:\n    records: {{path: \"{ok}\"}}\n"
            ))
            .expect(ok);
        }
    }

    #[test]
    fn auth_variants_and_override() {
        let s = parse(
            r#"
auth:
  type: hmac
  algorithm: sha256
  key: "{{ secrets.consumer_key }}"
  canonical: "{{ path }}"
  placement: {header: Signature}
  params: {clientId: "{{ secrets.client_id }}"}
endpoints:
  a: {auth: none}
  b: {auth: {type: bearer, token: "{{ secrets.t }}"}}
  c: {}
"#,
        )
        .expect("parse");
        let AuthSpec::Hmac(h) = &s.auth else {
            panic!("hmac")
        };
        assert_eq!(h.placement, Placement::Header("Signature".into()));
        assert_eq!(h.encoding, SigEncoding::Base64);
        assert!(matches!(s.endpoints["a"].auth, Some(AuthSpec::None)));
        assert!(matches!(
            s.endpoints["b"].auth,
            Some(AuthSpec::Bearer { .. })
        ));
        assert!(s.endpoints["c"].auth.is_none());
        let e = parse(
            "auth: {type: hmac, algorithm: sha256, key: k, canonical: c, placement: {}}\nendpoints: {}\n",
        )
        .expect_err("placement");
        assert!(e.message.contains("exactly one"), "{e}");
    }

    #[test]
    fn paginate_defaults() {
        let s = parse(
            r#"
endpoints:
  c: {paginate: {type: cursor, param: next_page, from: next_page}}
  p: {paginate: {type: page, param: pageNumber, stop_when: "response.json.last"}}
  o: {paginate: {type: offset, param: offset, limit_param: limit, limit: 100, max_pages: 3}}
  l: {paginate: {type: link_header}}
  e: {paginate: {type: expression, next: {cursor: "response.json.n"}, stop_when: "true"}}
"#,
        )
        .expect("parse");
        let pg = |n: &str| s.endpoints[n].paginate.clone().expect("paginate");
        assert!(!pg("c").stop_on_empty);
        assert!(pg("p").stop_on_empty);
        assert!(matches!(
            pg("p").kind,
            PaginateKind::Page {
                start: 1,
                step: 1,
                ..
            }
        ));
        assert_eq!(pg("o").max_pages, 3);
        assert_eq!(pg("c").max_pages, DEFAULT_MAX_PAGES);
        assert!(matches!(&pg("l").kind, PaginateKind::LinkHeader { rel } if rel == "next"));
        let e =
            parse("endpoints:\n  e: {paginate: {type: expression, next: {}}}\n").expect_err("stop");
        assert!(e.message.contains("stop_when"), "{e}");
    }

    #[test]
    fn except_skipped_needs_record_skips_and_no_batch() {
        let ok = parse(
            "endpoints:\n  x:\n    record_skips: true\n    for_each: {over: {queue: q, except_landed: [id]}, max_items: 5, except_skipped: {ttl: 7d}}\n    rules:\n      - {when: 'response.status == 404', action: skip}\n",
        )
        .expect("parse");
        let f = ok.endpoints["x"].for_each.as_ref().expect("for_each");
        assert_eq!(f.except_skipped.map(|d| d.0.as_secs()), Some(7 * 86400));
        assert!(ok.lints().is_empty(), "{:?}", ok.lints());
        let e = parse("endpoints:\n  x:\n    for_each: {over: [a], except_skipped: {ttl: 1d}}\n")
            .expect_err("no record_skips");
        assert!(e.message.contains("needs `record_skips: true`"), "{e:?}");
        let e = parse(
            "endpoints:\n  x:\n    record_skips: true\n    for_each: {over: [a], batch: {size: 2, as: b}, except_skipped: {ttl: 1d}}\n",
        )
        .expect_err("batch");
        assert!(e.message.contains("`batch`"), "{e:?}");
        let starving = parse(
            "rules:\n  - {when: 'response.status == 404', action: skip}\nendpoints:\n  x:\n    for_each: {over: {queue: q, except_landed: [id]}, concurrency: 2, max_items: 5}\n",
        )
        .expect("parse");
        let lints = starving.lints();
        assert_eq!(lints.len(), 1, "{lints:?}");
        assert!(
            lints[0].message.contains("no `except_skipped`"),
            "{:?}",
            lints[0]
        );
    }

    #[test]
    fn max_items_and_lints() {
        let s = parse(
            r#"
rate_limit: {rps: 13.3, max_in_flight: 16}
endpoints:
  serial:
    for_each: {over: {queue: ids}}
  parallel:
    for_each: {over: {queue: ids, except_landed: [id]}, concurrency: 16, max_items: 500}
  capped_list:
    for_each: {over: [a, b], concurrency: 2, max_items: 'vars.batch'}
"#,
        )
        .expect("parse");
        let cap = |e: &str| {
            s.endpoints[e]
                .for_each
                .as_ref()
                .and_then(|f| f.max_items.clone())
        };
        assert!(matches!(cap("parallel"), Some(MaxItems::N(500))));
        assert!(matches!(cap("capped_list"), Some(MaxItems::Expr(e)) if e.value == "vars.batch"));
        assert!(cap("serial").is_none());
        let lints: Vec<String> = s.lints().into_iter().map(|e| e.message).collect();
        assert_eq!(lints.len(), 2, "{lints:?}");
        assert!(lints[0].contains("`serial`") && lints[0].contains("rps 13.3, max_in_flight 16"));
        assert!(lints[1].contains("`capped_list`") && lints[1].contains("without `except_landed`"));
        let e = parse("endpoints:\n  x:\n    for_each: {over: [a], max_items: -1}\n")
            .expect_err("negative");
        assert!(e.message.contains("non-negative"), "{e:?}");
        let quiet = parse("endpoints:\n  x:\n    for_each: {over: [a]}\n").expect("parse");
        assert!(quiet.lints().is_empty(), "no rate limit, nothing to reach");
    }

    #[test]
    fn for_each_setup_sync() {
        let s = parse(
            r#"
rate_limit: {rps: 0.2, min_interval: 250ms}
throttle_patterns: ["slow down"]
decode_failures_as_throttle: 5/30s
endpoints:
  sym:
    setup:
      - name: countries
        request: {path: /country}
        records: {path: "[].Country"}
        collect: countries
    for_each: {over: state.countries, as: country}
    request: {path: "/x/{{ country }}"}
    sync:
      floor: {value: 'now()', aggregate: set}
    history: snapshot_only
  q:
    for_each: {over: {queue: ids, unique: true}}
  ql:
    for_each: {over: {queue: ids, unique: true, except_landed: [usid, venue]}}
  l:
    for_each: {over: [XA, XB], as: exchange, concurrency: 2}
  u:
    for_each: {over: {queue: users}, as: user, secret: [user_secret]}
"#,
        )
        .expect("parse");
        assert_eq!(
            s.endpoints["u"].for_each.as_ref().map(|f| f.secret.clone()),
            Some(vec!["user_secret".to_owned()])
        );
        let e = parse(
            "endpoints:\n  e: {for_each: {over: [1], secret: [s], batch: {size: 2, as: b}}}\n",
        )
        .expect_err("batch");
        assert!(e.message.contains("batch"), "{e}");
        let sym = &s.endpoints["sym"];
        assert_eq!(
            sym.setup[0].output,
            Some((SetupOutput::Collect, "countries".into()))
        );
        let fe = sym.for_each.as_ref().expect("for_each");
        assert!(matches!(&fe.over, Over::Expr(e) if e.value == "state.countries"));
        assert_eq!(fe.var, "country");
        assert_eq!(sym.sync["floor"].aggregate, Aggregate::Set);
        assert_eq!(sym.history, History::SnapshotOnly);
        assert!(matches!(
            s.endpoints["q"].for_each.as_ref().map(|f| &f.over),
            Some(Over::Queue { unique: true, except_landed, .. }) if except_landed.is_empty()
        ));
        assert!(matches!(
            s.endpoints["ql"].for_each.as_ref().map(|f| &f.over),
            Some(Over::Queue { unique: true, except_landed, .. }) if except_landed == &["usid", "venue"]
        ));
        assert_eq!(
            s.endpoints["q"].for_each.as_ref().map(|f| f.var.as_str()),
            Some("item")
        );
        assert!(matches!(
            s.endpoints["l"].for_each.as_ref().map(|f| &f.over),
            Some(Over::List(v)) if v.len() == 2
        ));
        assert!(s.rate_limit.declared);
        assert_eq!(
            s.rate_limit.min_interval.map(|d| d.0),
            Some(Duration::from_millis(250))
        );
        assert_eq!(
            s.decode_failures_as_throttle,
            DecodeThrottle {
                count: 5,
                window: Duration::from_secs(30)
            }
        );
    }
}
