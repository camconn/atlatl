//! The plan: selection, dependency closure, and a DAG of typed steps per replication (spec 03 §1).

pub mod checks;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::SystemTime;

use indexmap::IndexMap;
use minijinja::Value;
use schemars::JsonSchema;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::config::connection::Connection;
use crate::config::diag::{ConfigError, Diagnostics};
use crate::config::env::{Resolve, resolve_connection, select};
use crate::config::load::{Project, Replication, Stream};
use crate::config::model::{ForEachConfig, ForEachOver, FromConfig, Mode, SourceSnapshot};
use crate::redact::Redactor;
use crate::retry::RetryPolicy;
use crate::template::{Context, EnvSource, Templates};

pub use checks::{asset_key, check_project};

/// A stream's `vars` as JSON, with this run's `--var` overrides applied. An override keeps the
/// type of the var it replaces when it parses as that type (`--var limit=200` over `limit: 100` is
/// the number 200); otherwise, and for a new or string var, it's the string given.
pub fn stream_vars(
    vars: serde_json::Value,
    overrides: &IndexMap<String, String>,
) -> serde_json::Value {
    use serde_json::Value as J;
    let mut v = match vars {
        J::Null => serde_json::json!({}),
        v => v,
    };
    if let Some(m) = v.as_object_mut() {
        for (k, val) in overrides {
            let typed = match m.get(k) {
                Some(J::String(_)) | Some(J::Null) | None => None,
                Some(old) => serde_json::from_str::<J>(val)
                    .ok()
                    .filter(|new| std::mem::discriminant(new) == std::mem::discriminant(old)),
            };
            m.insert(k.clone(), typed.unwrap_or_else(|| J::String(val.clone())));
        }
    }
    v
}

pub const PLAN_VERSION: u32 = 1;

#[derive(Clone, Debug, Default)]
pub struct PlanRequest {
    pub env: Option<String>,
    /// `--var k=v`: overrides stream `vars` for this run (09 §2 `vars=`).
    pub vars: IndexMap<String, String>,
    /// Use this run id (the caller may need it before planning, e.g. for the event sink).
    pub run_id: Option<String>,
    pub replications: Vec<String>,
    pub streams: Vec<String>,
    pub tags: Vec<String>,
    /// `--instance <replication>[<key>]`: only these `for_each` instances (and only their
    /// replications) run.
    pub instances: Vec<String>,
    /// Pull unselected queue producers in as feed-only (the default); `false` = `--no-deps`.
    pub with_deps: bool,
    /// No environment, no templates rendered, no secrets read (for `dagster-atlatl` at load).
    pub static_plan: bool,
    /// Expand wildcards against the source catalog.
    pub discover: bool,
    /// `--range a..b`: overrides REST iteration bounds (and `update_key`/window) for this run.
    pub range: Option<String>,
}

impl PlanRequest {
    pub fn all() -> Self {
        Self {
            with_deps: true,
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct Plan {
    pub v: u32,
    #[serde(rename = "static")]
    pub static_plan: bool,
    pub env: Option<String>,
    /// How the environment was chosen: `flag`, `env:<VAR>`, `default`, `none`.
    pub env_source: Option<String>,
    pub run_id: Option<String>,
    pub project_root: String,
    /// Hash of the replications section; stable for identical config and selection.
    pub plan_hash: String,
    pub selection: Selection,
    pub ops: Vec<OpPlan>,
    pub replications: Vec<ReplicationPlan>,
    /// Resolved connections, secrets redacted (non-static plans only).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[schemars(with = "Vec<serde_json::Value>")]
    pub connections: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<PlanWarning>,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct Selection {
    pub replications: Vec<String>,
    pub streams: Vec<String>,
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub instances: Vec<String>,
    pub with_deps: bool,
    /// `--range a..b` (04 §4.6).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range: Option<String>,
    /// `--var k=v`, applied over stream `vars` at run time too. Not in plan output or events.
    #[serde(skip)]
    #[schemars(skip)]
    pub vars: IndexMap<String, String>,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct PlanWarning {
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct OpPlan {
    pub name: String,
    pub replications: Vec<String>,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct ConnRef {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: String,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct RetryPlan {
    pub max_attempts: u32,
    pub backoff: String,
    pub base_ms: u64,
    pub max_ms: u64,
    pub retry_on: Vec<String>,
    pub budget_ms: Option<u64>,
}

impl From<&RetryPolicy> for RetryPlan {
    fn from(p: &RetryPolicy) -> Self {
        Self {
            max_attempts: p.max_attempts,
            backoff: serde_json::to_value(p.backoff)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_default(),
            base_ms: p.base.as_millis() as u64,
            max_ms: p.max.as_millis() as u64,
            retry_on: p.retry_on.iter().map(|c| c.as_str().to_owned()).collect(),
            budget_ms: p.budget.map(|b| b.as_millis() as u64),
        }
    }
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct ReplicationPlan {
    pub name: String,
    pub op: String,
    /// Path relative to the project root.
    pub file: String,
    pub source: ConnRef,
    pub target: ConnRef,
    /// Selected replications that must succeed first.
    pub after: Vec<String>,
    pub atomic_group: Option<String>,
    pub lock_group: Option<String>,
    pub lock_timeout_ms: Option<u64>,
    pub replication_retries: u32,
    pub timeout_ms: Option<u64>,
    pub retry: RetryPlan,
    pub streams: Vec<StreamPlan>,
    pub steps: Vec<Step>,
    /// Multi-hop replications (`then:`): every planned hop, `hops[0]` being `source` → `target`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hops: Vec<HopPlan>,
    /// A `for_each` replication not expanded into instances: a static plan, a query iteration
    /// before the run, or one whose iteration found no items.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub for_each: Option<ForEachPlan>,
    /// One `for_each` instance, named `<replication>[<key>]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<InstancePlan>,
    /// `shared`: a postgres source reads every member under one snapshot (06 §3.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_snapshot: Option<SourceSnapshot>,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct HopPlan {
    pub hop: usize,
    /// The upstream hop, which commits first; `None` for hop 0.
    pub parent: Option<usize>,
    pub source: ConnRef,
    pub target: ConnRef,
    /// This hop's planned streams.
    pub streams: Vec<String>,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct ForEachPlan {
    pub over: ForEachOver,
    #[serde(rename = "as")]
    pub as_name: String,
    pub key: Option<String>,
}

impl From<&ForEachConfig> for ForEachPlan {
    fn from(f: &ForEachConfig) -> Self {
        Self {
            over: f.over.clone(),
            as_name: f.as_name.clone(),
            key: f.key.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct InstancePlan {
    /// The replication this instantiates.
    pub of: String,
    pub key: String,
    #[serde(rename = "as")]
    pub as_name: String,
    #[schemars(with = "serde_json::Value")]
    pub item: serde_json::Value,
}

impl ReplicationPlan {
    /// The configured replication: an instance's `of`, else the name.
    pub fn config_name(&self) -> &str {
        self.instance.as_ref().map_or(&self.name, |i| &i.of)
    }

    /// Hop `hop` as a single-hop plan under the same name (so report updates land on this
    /// replication): that hop's connections, streams and steps.
    pub fn hop_plan(&self, hop: usize) -> ReplicationPlan {
        let mut r = self.clone();
        if let Some(h) = self.hops.get(hop) {
            r.source = h.source.clone();
            r.target = h.target.clone();
        }
        r.streams.retain(|s| s.hop == hop);
        r.steps.retain(|s| s.hop == hop);
        r.hops.clear();
        r
    }
}

/// An instance's key: a scalar item's text, or the `key` field of a map item.
pub fn instance_key(item: &serde_json::Value, key: Option<&str>) -> Result<String, String> {
    use serde_json::Value as J;
    let text = |v: &J| match v {
        J::String(s) if !s.is_empty() => Some(s.clone()),
        J::Number(n) => Some(n.to_string()),
        J::Bool(b) => Some(b.to_string()),
        _ => None,
    };
    match (item, key) {
        (J::Object(m), Some(k)) => m.get(k).and_then(text).ok_or_else(|| {
            format!("`for_each` item {item} has no scalar, non-empty `{k}` (the `key`)")
        }),
        (J::Object(_), None) => Err(format!(
            "`for_each` item {item} is a map: set `for_each.key` to the field naming each instance"
        )),
        (_, Some(_)) => Err(format!(
            "`for_each.key` applies to map items, but {item} is not a map"
        )),
        (v, None) => text(v).ok_or_else(|| {
            format!("`for_each` item {item} must be a non-empty string, number, boolean or map")
        }),
    }
}

/// `<replication>[<key>]`, the name the plan and report give an instance.
pub fn parse_instance(s: &str) -> Option<(&str, &str)> {
    let (r, rest) = s.split_once('[')?;
    let k = rest.strip_suffix(']')?;
    (!r.is_empty() && !k.is_empty()).then_some((r, k))
}

/// `--instance` keys by replication (well-formed ones only; [`select_streams`] reports the rest).
fn wanted_instances(req: &PlanRequest) -> IndexMap<&str, Vec<&str>> {
    let mut out: IndexMap<&str, Vec<&str>> = IndexMap::new();
    for (r, k) in req.instances.iter().filter_map(|i| parse_instance(i)) {
        out.entry(r).or_default().push(k);
    }
    out
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StreamRole {
    Selected,
    /// A queue producer pulled in to feed a selected consumer; writes nothing.
    FeedOnly,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct StreamPlan {
    pub id: String,
    pub role: StreamRole,
    pub mode: String,
    #[schemars(with = "serde_json::Value")]
    pub from: FromConfig,
    /// The `object` template as written.
    pub object: String,
    pub primary_key: Option<Vec<String>>,
    pub update_key: Option<String>,
    pub window: Option<String>,
    pub tags: Vec<String>,
    /// Streams in this replication applied first.
    pub after: Vec<String>,
    pub members: Vec<MemberPlan>,
    pub asset: AssetPlan,
    /// Hash of the fields that give stored state its meaning (05 §4).
    pub config_hash: String,
    /// `inline`, a path, or `null`.
    pub source_schema: Option<String>,
    /// Index into the replication's `hops` (0 for its own streams).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub hop: usize,
    /// The key the stream's state (watermarks, sync values, rejects) is stored under when it
    /// isn't the id: `<id>@<instance key>` for `for_each` instances.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_id: Option<String>,
    /// `members.<name or glob>.primary_key` overrides of a `from.tables` stream.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    #[schemars(with = "BTreeMap<String, Vec<String>>")]
    pub member_primary_keys: IndexMap<String, Vec<String>>,
}

impl StreamPlan {
    pub fn state_id(&self) -> &str {
        self.state_id.as_deref().unwrap_or(&self.id)
    }
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct MemberPlan {
    pub name: String,
    pub schema: Option<String>,
    pub table: Option<String>,
    /// A wildcard pattern not yet expanded (static plans, or no `--discover`).
    pub pattern: bool,
    /// Rendered target table (non-static, non-pattern members).
    pub object: Option<String>,
    pub window: Option<String>,
    pub query: Option<String>,
    /// Rendered `from.files.url`.
    pub url: Option<String>,
    /// The member's key where it is known: the stream's `primary_key` for a single table, or
    /// (once discovered) a `members` override, the stream's key or the source's own key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<Vec<String>>,
    /// Where a discovered member's `primary_key` came from: `members`, `stream` or `source`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_key_from: Option<String>,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct AssetPlan {
    /// Slash-separated key.
    pub key: String,
    pub key_path: Vec<String>,
    pub group: Option<String>,
    pub deps: Vec<String>,
    pub description: Option<String>,
    pub kinds: Vec<String>,
    pub tags: BTreeMap<String, String>,
    /// Hash of the stream's full merged config; Dagster `code_version`.
    pub code_version: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    Extract,
    Stage,
    Validate,
    Apply,
    Commit,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct Step {
    pub id: String,
    pub kind: StepKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member: Option<String>,
    pub depends_on: Vec<String>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub hop: usize,
}

fn sha(v: &serde_json::Value) -> String {
    let h = Sha256::digest(canonical(v).as_bytes());
    h.iter().take(8).map(|b| format!("{b:02x}")).collect()
}

/// JSON with object keys sorted recursively.
pub fn canonical(v: &serde_json::Value) -> String {
    fn sort(v: &serde_json::Value) -> serde_json::Value {
        match v {
            serde_json::Value::Object(m) => {
                let sorted: BTreeMap<&String, serde_json::Value> =
                    m.iter().map(|(k, v)| (k, sort(v))).collect();
                serde_json::Value::Object(sorted.into_iter().map(|(k, v)| (k.clone(), v)).collect())
            }
            serde_json::Value::Array(a) => serde_json::Value::Array(a.iter().map(sort).collect()),
            _ => v.clone(),
        }
    }
    serde_json::to_string(&sort(v)).unwrap_or_default()
}

/// The fields that change what a stream's stored state means (05 §4).
pub fn state_config_hash(s: &Stream) -> String {
    let c = &s.config;
    let mut v = serde_json::json!({
        "mode": c.mode.as_ref().map(|m| m.value.as_str()),
        // A Delta `from.version` pins where to read, not what the state means.
        "from": c.from.as_ref().map(|f| FromConfig { version: None, ..f.value.clone() }),
        "window": c.window.as_ref().map(|w| &w.value),
        "update_key": c.update_key.as_ref().map(|u| &u.value.column),
        "primary_key": c.primary_key.as_ref().map(|p| &p.value),
    });
    // Only when set, so existing hashes don't move.
    let keys = member_primary_keys(c);
    if !keys.is_empty() {
        v["member_primary_keys"] = serde_json::json!(keys);
    }
    sha(&v)
}

/// `members.*.primary_key` of a stream, in declaration order.
pub fn member_primary_keys(
    c: &crate::config::model::StreamConfig,
) -> IndexMap<String, Vec<String>> {
    c.members
        .iter()
        .flatten()
        .filter_map(|(m, cfg)| cfg.primary_key.clone().map(|k| (m.clone(), k)))
        .collect()
}

fn members(from: &FromConfig) -> Vec<MemberPlan> {
    let base = MemberPlan {
        name: String::new(),
        schema: None,
        table: None,
        pattern: false,
        object: None,
        window: None,
        query: None,
        url: None,
        primary_key: None,
        primary_key_from: None,
    };
    if let Some(t) = &from.table {
        let parts: Vec<&str> = t.split('.').collect();
        let (schema, table) = match parts.as_slice() {
            [.., s, t] => (Some((*s).to_owned()), (*t).to_owned()),
            [t] => (None, (*t).to_owned()),
            [] => (None, String::new()),
        };
        return vec![MemberPlan {
            name: t.clone(),
            schema,
            table: Some(table),
            ..base
        }];
    }
    if let Some(p) = &from.tables {
        return vec![MemberPlan {
            name: p.clone(),
            pattern: true,
            ..base
        }];
    }
    if let Some(e) = &from.endpoint {
        return vec![MemberPlan {
            name: e.clone(),
            ..base
        }];
    }
    if let Some(f) = &from.files {
        return vec![MemberPlan {
            name: f.url.clone(),
            ..base
        }];
    }
    vec![MemberPlan {
        name: "query".into(),
        ..base
    }]
}

fn render_json(
    v: &serde_json::Value,
    t: &Templates,
    ctx: &Context,
) -> Result<serde_json::Value, String> {
    Ok(match v {
        serde_json::Value::String(s) => {
            serde_json::Value::String(t.render(s, ctx).map_err(|e| e.message)?)
        }
        serde_json::Value::Array(a) => serde_json::Value::Array(
            a.iter()
                .map(|x| render_json(x, t, ctx))
                .collect::<Result<_, _>>()?,
        ),
        serde_json::Value::Object(m) => serde_json::Value::Object(
            m.iter()
                .map(|(k, x)| Ok((k.clone(), render_json(x, t, ctx)?)))
                .collect::<Result<_, String>>()?,
        ),
        other => other.clone(),
    })
}

struct Selected<'a> {
    rep: &'a Replication,
    streams: Vec<(&'a Stream, StreamRole)>,
}

fn select_streams<'a>(
    p: &'a Project,
    req: &PlanRequest,
    diags: &mut Diagnostics,
) -> Vec<Selected<'a>> {
    for r in &req.replications {
        if p.replication(r).is_none() {
            diags.error(
                "selection",
                ConfigError::new(format!("--replication `{r}`: no such replication")),
            );
        }
    }
    for i in &req.instances {
        let sel_err = |m: String| ConfigError::new(format!("--instance `{i}`: {m}"));
        let Some((r, k)) = parse_instance(i) else {
            diags.error(
                "selection",
                sel_err(
                    "expected `<replication>[<key>]`, as the plan and report name instances".into(),
                ),
            );
            continue;
        };
        let Some(rep) = p.replication(r) else {
            diags.error("selection", sel_err(format!("no such replication `{r}`")));
            continue;
        };
        let Some(fe) = rep.for_each.as_ref().map(|f| &f.value) else {
            diags.error(
                "selection",
                sel_err(format!("replication `{r}` has no `for_each`")),
            );
            continue;
        };
        if !req.replications.is_empty() && !req.replications.iter().any(|x| x == r) {
            diags.error(
                "selection",
                sel_err(format!("replication `{r}` is not selected")),
            );
        }
        if let ForEachOver::List(items) = &fe.over {
            let keys: Vec<String> = items
                .iter()
                .filter_map(|it| instance_key(it, fe.key.as_deref()).ok())
                .collect();
            if !keys.iter().any(|x| x == k) {
                diags.error(
                    "selection",
                    sel_err(format!(
                        "replication `{r}` has no instance `{k}` (its instances: {})",
                        keys.join(", ")
                    )),
                );
            }
        }
    }
    let wanted = wanted_instances(req);
    for s in &req.streams {
        match p.stream(s) {
            None => diags.error(
                "selection",
                ConfigError::new(format!("--stream `{s}`: no such stream")),
            ),
            Some((r, st)) => {
                if !st.enabled() {
                    diags.error(
                        "selection",
                        ConfigError::new(format!("--stream `{s}` is disabled (`enabled: false`)"))
                            .at(st.id.span.clone()),
                    );
                }
                if !req.replications.is_empty() && !req.replications.contains(&r.name.value) {
                    diags.error(
                        "selection",
                        ConfigError::new(format!(
                            "--stream `{s}` belongs to replication `{}`, which is not selected",
                            r.name.value
                        )),
                    );
                }
            }
        }
    }
    let mut out = Vec::new();
    for rep in &p.replications {
        if !req.replications.is_empty() && !req.replications.contains(&rep.name.value) {
            continue;
        }
        if !wanted.is_empty() && !wanted.contains_key(rep.name.value.as_str()) {
            continue;
        }
        let chosen: Vec<&Stream> = rep
            .streams
            .iter()
            .filter(|s| s.enabled())
            .filter(|s| req.streams.is_empty() || req.streams.contains(&s.id.value))
            .filter(|s| {
                req.tags.is_empty() || s.config.tags.iter().flatten().any(|t| req.tags.contains(t))
            })
            .collect();
        if chosen.is_empty() {
            continue;
        }
        let mut streams: Vec<(&Stream, StreamRole)> = Vec::new();
        // Queue-producer closure within the replication (03 §1 step 2).
        let spec = checks::spec_for(p, &rep.source.value);
        let mut need: Vec<&Stream> = chosen.clone();
        let mut feed: Vec<&Stream> = Vec::new();
        while let Some(s) = need.pop() {
            let (Some(spec), Some(ep)) = (
                spec,
                s.config.from.as_ref().and_then(|f| f.endpoint.as_deref()),
            ) else {
                continue;
            };
            for prod in spec.producers_of(ep) {
                let Some(ps) = rep.streams.iter().find(|o| {
                    o.config.from.as_ref().and_then(|f| f.endpoint.as_deref())
                        == Some(prod.as_str())
                }) else {
                    continue; // reported by the static checks
                };
                if chosen.iter().any(|c| c.id.value == ps.id.value)
                    || feed.iter().any(|c| c.id.value == ps.id.value)
                {
                    continue;
                }
                if !req.with_deps {
                    diags.error(
                        "queue-producer",
                        ConfigError::new(format!(
                            "stream `{}` consumes endpoint `{prod}` (stream `{}`), which is not selected; select it or drop --no-deps",
                            s.id.value, ps.id.value
                        )),
                    );
                    continue;
                }
                feed.push(ps);
                need.push(ps);
            }
        }
        for s in &rep.streams {
            if chosen.iter().any(|c| c.id.value == s.id.value) {
                streams.push((s, StreamRole::Selected));
            } else if feed.iter().any(|c| c.id.value == s.id.value) {
                streams.push((s, StreamRole::FeedOnly));
            }
        }
        out.push(Selected { rep, streams });
    }
    if out.is_empty() && !diags.has_errors() {
        diags.error(
            "selection",
            ConfigError::new("the selection matched no enabled streams"),
        );
    }
    out
}

/// `producers` maps an endpoint to the endpoints whose extracts must finish before it runs.
/// Each hop gets its own applies and commit; a hop's extracts wait for its nearest planned
/// upstream hop's commit (11 §3).
fn steps_for(
    sel: &Selected<'_>,
    member_names: &IndexMap<String, Vec<String>>,
    producers: &IndexMap<String, Vec<String>>,
) -> Vec<Step> {
    let r = sel.rep;
    let planned: Vec<usize> = (0..r.hops.len())
        .filter(|h| sel.streams.iter().any(|(s, _)| s.hop == *h))
        .collect();
    let commit_id = |h: usize| {
        if h == 0 {
            "commit".to_owned()
        } else {
            format!("commit:{h}")
        }
    };
    let upstream = |h: usize| {
        let mut p = r.hops[h].parent;
        while let Some(x) = p {
            if planned.contains(&x) {
                return Some(x);
            }
            p = r.hops[x].parent;
        }
        None
    };
    let endpoint = |s: &Stream| s.config.from.as_ref().and_then(|f| f.endpoint.clone());
    let extracts = |s: &Stream| -> Vec<String> {
        member_names[&s.id.value]
            .iter()
            .map(|m| format!("extract:{}:{m}", s.id.value))
            .collect()
    };
    let mut steps = Vec::new();
    for &hop in &planned {
        let streams: Vec<&(&Stream, StreamRole)> =
            sel.streams.iter().filter(|(s, _)| s.hop == hop).collect();
        let gate: Vec<String> = upstream(hop).map(commit_id).into_iter().collect();
        let mut validates = Vec::new();
        for (s, role) in &streams {
            let needs: Vec<String> = endpoint(s)
                .and_then(|e| producers.get(&e).cloned())
                .unwrap_or_default();
            let mut deps: Vec<String> = streams
                .iter()
                .filter(|(o, _)| endpoint(o).is_some_and(|e| needs.contains(&e)))
                .flat_map(|(o, _)| extracts(o))
                .collect();
            deps.extend(gate.iter().cloned());
            for m in &member_names[&s.id.value] {
                let x = format!("extract:{}:{m}", s.id.value);
                let step = |id: String, kind, depends_on| Step {
                    id,
                    kind,
                    stream: Some(s.id.value.clone()),
                    member: Some(m.clone()),
                    depends_on,
                    hop,
                };
                steps.push(step(x.clone(), StepKind::Extract, deps.clone()));
                if *role == StreamRole::FeedOnly {
                    continue;
                }
                let st = format!("stage:{}:{m}", s.id.value);
                let v = format!("validate:{}:{m}", s.id.value);
                steps.push(step(st.clone(), StepKind::Stage, vec![x]));
                steps.push(step(v.clone(), StepKind::Validate, vec![st]));
                validates.push(v);
            }
        }
        // Applies run in the hop's one transaction, in `after` order, ties by declaration.
        let writers: Vec<&Stream> = streams
            .iter()
            .filter(|(_, r)| *r == StreamRole::Selected)
            .map(|(s, _)| *s)
            .collect();
        let ids: Vec<&str> = writers.iter().map(|s| s.id.value.as_str()).collect();
        let order = crate::config::graph::topo_order(&ids, |id| {
            writers
                .iter()
                .find(|s| s.id.value == id)
                .map(|s| {
                    s.config
                        .after
                        .iter()
                        .flatten()
                        .map(|a| a.value.clone())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        });
        let mut applies: Vec<String> = Vec::new();
        for id in order {
            let a = format!("apply:{id}");
            let mut deps = validates.clone();
            deps.extend(applies.last().cloned());
            steps.push(Step {
                id: a.clone(),
                kind: StepKind::Apply,
                stream: Some(id),
                member: None,
                depends_on: deps,
                hop,
            });
            applies.push(a);
        }
        steps.push(Step {
            id: commit_id(hop),
            kind: StepKind::Commit,
            stream: None,
            member: None,
            depends_on: applies,
            hop,
        });
    }
    steps
}

/// Connections a live plan must resolve: the selected replications' sources and targets, plus
/// the state lake and lock backend. Unused connections' secrets are never read.
fn used_connections(p: &Project, req: &PlanRequest) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for r in &p.replications {
        let picked = (req.replications.is_empty() || req.replications.contains(&r.name.value))
            && (req.instances.is_empty()
                || req
                    .instances
                    .iter()
                    .filter_map(|i| parse_instance(i))
                    .any(|(x, _)| x == r.name.value))
            && r.streams.iter().any(|s| {
                s.enabled()
                    && (req.streams.is_empty() || req.streams.contains(&s.id.value))
                    && (req.tags.is_empty()
                        || s.config.tags.iter().flatten().any(|t| req.tags.contains(t)))
            });
        if picked {
            out.extend([r.source.value.clone(), r.target.value.clone()]);
            out.extend(r.hops.iter().map(|h| h.target.value.clone()));
            if let Some(ForEachOver::Query(q)) = r.for_each.as_ref().map(|f| &f.value.over) {
                out.push(q.connection.clone());
            }
            out.extend(
                r.streams
                    .iter()
                    .filter_map(|s| s.config.bind.as_ref())
                    .flat_map(|b| b.values().map(|c| c.connection.clone())),
            );
            if let Some(spec) = checks::spec_for(p, &r.source.value) {
                out.extend(
                    spec.endpoints
                        .values()
                        .flat_map(|e| e.query_connections.iter().map(|c| c.value.clone())),
                );
            }
        }
    }
    if let Some(st) = &p.state {
        out.push(st.lake.value.clone());
        if let Some(l) = &st.locks {
            out.push(l.value.split('.').next().unwrap_or(&l.value).to_owned());
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Builds the plan. Static checks run first; any error aborts with the diagnostics.
pub fn build(
    p: &Project,
    req: &PlanRequest,
    vars: Arc<dyn EnvSource>,
    redactor: &Redactor,
) -> Result<(Plan, Diagnostics), Diagnostics> {
    build_with(p, req, vars, redactor, &IndexMap::new())
}

/// [`build`], with the items of `for_each.over.query` replications (by replication name) that
/// the runner resolved; those replications expand into one instance per item.
pub fn build_with(
    p: &Project,
    req: &PlanRequest,
    vars: Arc<dyn EnvSource>,
    redactor: &Redactor,
    for_each_items: &IndexMap<String, Vec<serde_json::Value>>,
) -> Result<(Plan, Diagnostics), Diagnostics> {
    let templates = Templates::new();
    let mut diags = Diagnostics::default();
    check_project(p, &templates, &mut diags);
    if diags.has_errors() {
        return Err(diags);
    }

    let (env, env_source, connections) = if req.static_plan {
        if req.env.is_some() {
            diags.warn(
                "static-env",
                ConfigError::new("--env is ignored by a static plan"),
            );
        }
        (None, None, IndexMap::new())
    } else {
        let active = match select(
            p.environments.as_ref().map(|e| &e.value),
            req.env.as_deref(),
            vars.as_ref(),
        ) {
            Ok(a) => a,
            Err(e) => {
                diags.error("environment", e);
                return Err(diags);
            }
        };
        let used = used_connections(p, req);
        let mut conns: IndexMap<String, Connection> = IndexMap::new();
        for c in p.connections.values().filter(|c| used.contains(&c.name)) {
            let mode = Resolve::Full {
                vars: vars.clone(),
                redactor,
            };
            if let Some(r) =
                resolve_connection(p, c, active.name.as_deref(), &mode, &templates, &mut diags)
            {
                conns.insert(c.name.clone(), r);
            }
        }
        (active.name, Some(active.source), conns)
    };

    if let Some(r) = &req.range
        && !r.contains("..")
    {
        diags.error(
            "range",
            ConfigError::new(format!(
                "`--range {r}`: expected `start..end`, e.g. `2015-01-01..2020-01-01`"
            )),
        );
        return Err(diags);
    }
    let selected = select_streams(p, req, &mut diags);
    let wanted = wanted_instances(req);
    if req.discover {
        // Postgres wildcards are expanded by the caller (`atlatl-run::pgsrc::discover_plan`).
        let types = checks::connection_types(p);
        let wild = selected
            .iter()
            .flat_map(|sel| sel.streams.iter().map(move |(s, _)| (sel.rep, s)))
            .find(|(r, s)| {
                s.config.from.as_ref().is_some_and(|f| f.tables.is_some())
                    && types.get(&r.hop_of(s).source.value).map(String::as_str) != Some("postgres")
            })
            .map(|(_, s)| (s, ()));
        if req.static_plan {
            diags.error(
                "discover",
                ConfigError::new("--discover needs a live (non-static) plan"),
            );
        } else if let Some((s, _)) = wild {
            diags.error(
                "discover",
                ConfigError::new(format!("--discover: wildcard discovery for stream `{}` needs a source connector, which isn't available yet", s.id.value)),
            );
        }
    }
    if diags.has_errors() {
        return Err(diags);
    }

    let types = checks::connection_types(p);
    let started = SystemTime::now();
    let run_id = (!req.static_plan).then(|| req.run_id.clone().unwrap_or_else(crate::ids::ulid));
    let selected_names: Vec<&str> = selected.iter().map(|s| s.rep.name.value.as_str()).collect();
    let mut reps = Vec::new();
    let mut warnings = Vec::new();
    for sel in &selected {
        let r = sel.rep;
        let fe = r.for_each.as_ref().map(|f| &f.value);
        let items: Option<&Vec<serde_json::Value>> = match fe.map(|f| &f.over) {
            _ if req.static_plan => None,
            Some(ForEachOver::List(l)) => Some(l),
            Some(ForEachOver::Query(_)) => for_each_items.get(&r.name.value),
            None => None,
        };
        let mut instances: Vec<Option<InstancePlan>> = Vec::new();
        let only = wanted.get(r.name.value.as_str());
        match (fe, items) {
            (Some(f), Some(items)) => {
                for it in items {
                    match instance_key(it, f.key.as_deref()) {
                        Ok(key) if only.is_some_and(|o| !o.contains(&key.as_str())) => {}
                        Ok(key) => instances.push(Some(InstancePlan {
                            of: r.name.value.clone(),
                            key,
                            as_name: f.as_name.clone(),
                            item: it.clone(),
                        })),
                        Err(e) => diags.error(
                            "for-each",
                            ConfigError::new(format!("replication `{}`: {e}", r.name.value)),
                        ),
                    }
                }
                if instances.is_empty() {
                    instances.push(None);
                }
            }
            (Some(_), None) => {
                if !req.static_plan {
                    warnings.push(PlanWarning {
                        code: "unexpanded".into(),
                        message: format!(
                            "replication `{}`: `for_each.over.query` is expanded when the run starts",
                            r.name.value
                        ),
                    });
                }
                instances.push(None);
            }
            (None, _) => instances.push(None),
        }
        for inst in &instances {
            let producers: IndexMap<String, Vec<String>> = checks::spec_for(p, &r.source.value)
                .map(|spec| {
                    spec.endpoints
                        .keys()
                        .map(|e| (e.clone(), spec.producers_of(e)))
                        .collect()
                })
                .unwrap_or_default();
            let retry = RetryPolicy::from_config(&r.retry).unwrap_or_default();
            let mut streams = Vec::new();
            let mut member_names: IndexMap<String, Vec<String>> = IndexMap::new();
            for (s, role) in &sel.streams {
                let c = &s.config;
                let from = c.from.as_ref().map(|f| f.value.clone()).unwrap_or_default();
                let mut ms = members(&from);
                let target_type = types
                    .get(&r.hop_of(s).target.value)
                    .cloned()
                    .unwrap_or_default();
                // An unexpanded `for_each` has no item to render with.
                let render = !req.static_plan && (fe.is_none() || inst.is_some());
                if render {
                    let load = Context::load(vars.clone(), &p.root, env.as_deref());
                    let vars_rendered = match c
                        .vars
                        .as_ref()
                        .map(|v| {
                            render_json(
                                &serde_json::to_value(v).unwrap_or_default(),
                                &templates,
                                &load,
                            )
                        })
                        .transpose()
                    {
                        Ok(v) => stream_vars(v.unwrap_or_default(), &req.vars),
                        Err(e) => {
                            diags.error(
                                "template",
                                ConfigError::new(format!("stream `{}` vars: {e}", s.id.value))
                                    .at(s.id.span.clone()),
                            );
                            continue;
                        }
                    };
                    for m in ms.iter_mut().filter(|m| !m.pattern) {
                        let mut member = BTreeMap::new();
                        member.insert("name", Value::from(m.name.clone()));
                        if let Some(sc) = &m.schema {
                            member.insert("schema", Value::from(sc.clone()));
                        }
                        if let Some(t) = &m.table {
                            member.insert("table", Value::from(t.clone()));
                        }
                        let ctx = load
                            .clone()
                            .with("member", Value::from_pairs(member))
                            .with(
                                "stream",
                                Value::from_pairs([
                                    ("id", Value::from(s.id.value.clone())),
                                    ("vars", crate::template::to_value(&vars_rendered)),
                                ]),
                            )
                            .with("vars", crate::template::to_value(&vars_rendered))
                            .with(
                                "run",
                                Value::from_pairs([
                                    ("id", Value::from(run_id.clone().unwrap_or_default())),
                                    ("started_at", Value::from(crate::ids::rfc3339(started))),
                                ]),
                            );
                        let ctx = match inst {
                            Some(i) => ctx.with(&i.as_name, crate::template::to_value(&i.item)),
                            None => ctx,
                        };
                        let bind_ctx = c.bind.as_ref().map(|b| {
                            let markers: BTreeMap<String, String> = b
                                .keys()
                                .map(|k| (k.clone(), crate::template::bind_marker(k)))
                                .collect();
                            ctx.clone().with("bind", crate::template::to_value(markers))
                        });
                        let mut render = |src: Option<&String>,
                                          what: &str,
                                          ctx: &crate::template::Context|
                         -> Option<String> {
                            let src = src?;
                            match templates.render(src, ctx) {
                                Ok(v) => Some(v),
                                Err(e) => {
                                    diags.error(
                                        "template",
                                        ConfigError::new(format!(
                                            "stream `{}` {what}: {}",
                                            s.id.value, e.message
                                        ))
                                        .at(s.id.span.clone()),
                                    );
                                    None
                                }
                            }
                        };
                        m.object = render(c.object.as_ref().map(|o| &o.value), "object", &ctx);
                        m.window = render(c.window.as_ref().map(|w| &w.value), "window", &ctx);
                        m.query = render(
                            from.query.as_ref(),
                            "from.query",
                            bind_ctx.as_ref().unwrap_or(&ctx),
                        );
                        m.url = render(from.files.as_ref().map(|f| &f.url), "from.files.url", &ctx);
                        if let Some(u) = &m.url {
                            m.name = u.clone();
                        }
                    }
                } else if ms.iter().any(|m| m.pattern) {
                    warnings.push(PlanWarning {
                        code: "unexpanded".into(),
                        message: format!(
                            "stream `{}`: wildcard `{}` is listed as a pattern (no discovery)",
                            s.id.value, ms[0].name
                        ),
                    });
                }
                member_names.insert(
                    s.id.value.clone(),
                    ms.iter().map(|m| m.name.clone()).collect(),
                );
                let a = c.asset.clone().unwrap_or_default();
                let key = asset_key(s);
                let kinds = a
                    .kinds
                    .clone()
                    .unwrap_or_else(|| vec!["atlatl".into(), target_type.clone()]);
                streams.push(StreamPlan {
                    id: s.id.value.clone(),
                    role: *role,
                    mode: c
                        .mode
                        .as_ref()
                        .map(|m| m.value.as_str())
                        .unwrap_or(Mode::FullRefresh.as_str())
                        .to_owned(),
                    from,
                    object: c
                        .object
                        .as_ref()
                        .map(|o| o.value.clone())
                        .unwrap_or_default(),
                    primary_key: c.primary_key.as_ref().map(|p| p.value.clone()),
                    update_key: c.update_key.as_ref().map(|u| u.value.column.clone()),
                    window: c.window.as_ref().map(|w| w.value.clone()),
                    tags: c.tags.clone().unwrap_or_default(),
                    after: c
                        .after
                        .iter()
                        .flatten()
                        .map(|a| a.value.clone())
                        .filter(|a| sel.streams.iter().any(|(o, _)| &o.id.value == a))
                        .collect(),
                    members: ms,
                    asset: AssetPlan {
                        key_path: key.split('/').map(str::to_owned).collect(),
                        key,
                        group: a.group.clone(),
                        deps: a.deps.clone().unwrap_or_default(),
                        description: a.description.clone(),
                        kinds,
                        tags: a
                            .tags
                            .clone()
                            .map(|t| t.into_iter().collect())
                            .unwrap_or_default(),
                        code_version: sha(&s.node.to_json()),
                    },
                    config_hash: state_config_hash(s),
                    source_schema: s.schema.as_ref().map(|d| match &d.file {
                        Some(f) => p.relative(f).display().to_string(),
                        None => "inline".into(),
                    }),
                    hop: s.hop,
                    state_id: inst.as_ref().map(|i| format!("{}@{}", s.id.value, i.key)),
                    member_primary_keys: member_primary_keys(c),
                });
            }
            let steps = steps_for(sel, &member_names, &producers);
            let conn = |name: &str| ConnRef {
                name: name.to_owned(),
                kind: types.get(name).cloned().unwrap_or_default(),
            };
            let hops: Vec<HopPlan> = if r.is_multi_hop() {
                r.hops
                    .iter()
                    .enumerate()
                    .map(|(i, h)| HopPlan {
                        hop: i,
                        parent: h.parent,
                        source: conn(&h.source.value),
                        target: conn(&h.target.value),
                        streams: streams
                            .iter()
                            .filter(|s| s.hop == i)
                            .map(|s| s.id.clone())
                            .collect(),
                    })
                    .collect()
            } else {
                Vec::new()
            };
            reps.push(ReplicationPlan {
                name: match inst {
                    Some(i) => format!("{}[{}]", r.name.value, i.key),
                    None => r.name.value.clone(),
                },
                op: r.op.value.clone(),
                file: p.relative(&r.file).display().to_string(),
                source: ConnRef {
                    name: r.source.value.clone(),
                    kind: types.get(&r.source.value).cloned().unwrap_or_default(),
                },
                target: ConnRef {
                    name: r.target.value.clone(),
                    kind: types.get(&r.target.value).cloned().unwrap_or_default(),
                },
                after: r
                    .after
                    .iter()
                    .map(|a| a.value.clone())
                    .filter(|a| selected_names.contains(&a.as_str()))
                    .collect(),
                atomic_group: r.atomic_group.as_ref().map(|g| g.value.clone()),
                lock_group: r.lock_group.as_ref().map(|g| g.value.clone()),
                lock_timeout_ms: r
                    .lock_timeout
                    .or(p.state.as_ref().and_then(|s| s.lock_timeout))
                    .map(|d| d.0.as_millis() as u64),
                replication_retries: r.replication_retries,
                timeout_ms: r.timeout.map(|d| d.0.as_millis() as u64),
                retry: RetryPlan::from(&retry),
                streams,
                steps,
                hops,
                for_each: match inst {
                    None => fe.map(ForEachPlan::from),
                    Some(_) => None,
                },
                instance: inst.clone(),
                source_snapshot: r.source_snapshot.as_ref().map(|s| s.value),
            });
        }
    }
    if diags.has_errors() {
        return Err(diags);
    }

    let mut ops: IndexMap<String, Vec<String>> = IndexMap::new();
    for r in &reps {
        ops.entry(r.op.clone()).or_default().push(r.name.clone());
    }
    let plan_hash = sha(&serde_json::to_value(&reps).unwrap_or_default());
    for w in diags.warnings() {
        warnings.push(PlanWarning {
            code: w.code.into(),
            message: w.error.to_string(),
        });
    }
    let plan = Plan {
        v: PLAN_VERSION,
        static_plan: req.static_plan,
        env,
        env_source,
        run_id,
        project_root: p.root.display().to_string(),
        plan_hash,
        selection: Selection {
            replications: req.replications.clone(),
            streams: req.streams.clone(),
            tags: req.tags.clone(),
            instances: req.instances.clone(),
            with_deps: req.with_deps,
            range: req.range.clone(),
            vars: req.vars.clone(),
        },
        ops: ops
            .into_iter()
            .map(|(name, replications)| OpPlan { name, replications })
            .collect(),
        replications: reps,
        connections: connections
            .values()
            .filter_map(|c| serde_json::to_value(c).ok())
            .collect(),
        warnings,
    };
    Ok((plan, diags))
}

#[cfg(test)]
mod tests;
