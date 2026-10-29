//! One endpoint for a whole run (07 §7, §11, §12): `setup` into `state`, `for_each` items (lists,
//! `range()`, `state.*`, or items the caller resolved from a queue or query), batching and
//! concurrency, run-wide dedup, `emit`, `sync` folds, the skip guard, and reject bookkeeping
//! (03 §7). Queues, staging and state persistence are the caller's (DuckDB-side) job.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::Pin;
use std::sync::Arc;

use atlatl_core::{AtlatlError, ErrorClass};
use futures::{Stream, StreamExt};
use indexmap::IndexMap;
use minijinja::Value;
use serde::Serialize;

use crate::client::{ApiClient, EndpointStats, Record, Replayed, RunVars};
use crate::expr::{Scope, to_json};
use crate::json::JValue;
use crate::spec::{Aggregate, DecodeRetry, Dedup, Endpoint, History, MaxItems, Over, SetupOutput};

/// Name bound while `for_each.over` is evaluated; `range()` reads it.
const RANGE_VAR: &str = "__atlatl_range";

/// `--range a..b` (04 §4.6): replaces the bounds of `range()` in `for_each.over`. Either side may
/// be empty to keep the spec's bound.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct RangeOverride {
    pub start: Option<String>,
    pub end: Option<String>,
}

impl RangeOverride {
    pub fn parse(s: &str) -> Result<Self, AtlatlError> {
        let (a, b) = s.split_once("..").ok_or_else(|| {
            AtlatlError::config(format!(
                "`--range {s}`: expected `start..end` (e.g. `2015-01-01..2020-01-01`)"
            ))
        })?;
        let side = |x: &str| (!x.trim().is_empty()).then(|| x.trim().to_owned());
        let r = Self {
            start: side(a),
            end: side(b),
        };
        if r.start.is_none() && r.end.is_none() {
            return Err(AtlatlError::config(format!(
                "`--range {s}` sets neither bound"
            )));
        }
        Ok(r)
    }

    fn value(&self) -> Value {
        Value::from_pairs([
            (
                "start",
                self.start
                    .clone()
                    .map(Value::from)
                    .unwrap_or(Value::from(())),
            ),
            (
                "end",
                self.end.clone().map(Value::from).unwrap_or(Value::from(())),
            ),
        ])
    }
}

fn override_bound(orig: Value, over: Value) -> Value {
    if over.is_none() || over.is_undefined() {
        return orig;
    }
    let s = over.as_str().unwrap_or_default();
    let int_orig = orig.is_integer()
        || orig
            .as_str()
            .is_some_and(|x| x.trim().parse::<i64>().is_ok());
    match s.trim().parse::<i64>() {
        Ok(n) if int_orig => Value::from(n),
        _ => over,
    }
}

/// `range()`'s bounds, with the `--range` override applied while `for_each.over` is evaluated.
pub(crate) fn range_bounds(
    state: &minijinja::State<'_, '_>,
    start: Value,
    end: Value,
) -> (Value, Value) {
    match state.lookup(RANGE_VAR) {
        Some(o) if !o.is_undefined() && !o.is_none() => {
            let s = o.get_attr("start").unwrap_or_default();
            let e = o.get_attr("end").unwrap_or_default();
            (override_bound(start, s), override_bound(end, e))
        }
        _ => (start, end),
    }
}

/// An open reject from an earlier run (`_atlatl.rejects`), to be retried.
#[derive(Clone, Debug)]
pub struct OpenReject {
    pub key: String,
    pub request: JValue,
    /// `decode` or `skipped`.
    pub class: String,
}

#[derive(Clone, Default)]
pub struct DriveOptions {
    /// `globals`, stored `sync`, extra `state`. `item` is set per iteration.
    pub vars: RunVars,
    /// Items for `over: {queue}` / `over: {query}`, resolved by the caller.
    pub items: Option<Vec<JValue>>,
    pub range: Option<RangeOverride>,
    /// A stream's `on_decode_error.retry`, over the endpoint's.
    pub decode_retry: Option<DecodeRetry>,
    pub open_rejects: Vec<OpenReject>,
    /// Replay `open_rejects` only: no iteration items (`atlatl rejects retry`).
    pub rejects_only: bool,
    /// Keys of `open_rejects` skipped within `for_each.except_skipped`'s TTL.
    pub recent_skips: HashSet<String>,
}

/// Records ready for staging, and what they emit to queues.
#[derive(Clone, Debug, Default)]
pub struct Chunk {
    pub records: Vec<Record>,
    pub emits: Vec<(String, JValue)>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SkippedItem {
    pub item: Option<JValue>,
    pub request_key: String,
    pub reason: String,
}

/// A reject to upsert in the replication's commit.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct NewReject {
    pub key: String,
    pub request: JValue,
    pub raw: String,
    pub error: String,
    /// `decode` or `skipped`.
    pub class: String,
}

#[derive(Clone, Debug, Default)]
pub struct DriveOutcome {
    pub stats: EndpointStats,
    /// The sync values to persist: stored values, overwritten by this run's folds.
    pub sync: IndexMap<String, JValue>,
    pub sync_changed: bool,
    /// Iteration items run (replays excluded).
    pub items: u64,
    /// Items left for later runs by `for_each.max_items`.
    pub items_deferred: u64,
    /// Items left out by `for_each.except_skipped`: skipped within its TTL.
    pub items_held: u64,
    pub skipped: Vec<SkippedItem>,
    pub rejects: Vec<NewReject>,
    /// Keys of open rejects that now landed.
    pub resolved: Vec<String>,
    /// `--range` was given and `for_each.over` calls `range()`.
    pub range_applied: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FoldOver {
    Record,
    Response,
    End,
}

struct Fold {
    agg: Aggregate,
    expr: String,
    over: FoldOver,
    value: Option<JValue>,
}

impl Fold {
    fn push(&mut self, v: JValue) {
        if v.is_null() {
            return;
        }
        self.value = Some(match (self.agg, self.value.take()) {
            (_, None) => v,
            (Aggregate::First, Some(old)) => old,
            (Aggregate::Last | Aggregate::Set, Some(_)) => v,
            (Aggregate::Max, Some(old)) => {
                if cmp_json(&v, &old) == Ordering::Greater {
                    v
                } else {
                    old
                }
            }
            (Aggregate::Min, Some(old)) => {
                if cmp_json(&v, &old) == Ordering::Less {
                    v
                } else {
                    old
                }
            }
        });
    }
}

/// Numbers exactly by value, everything else by its text.
pub fn cmp_json(a: &JValue, b: &JValue) -> Ordering {
    match (a, b) {
        (JValue::Number(x), JValue::Number(y)) => crate::json::cmp_numbers(x.as_str(), y.as_str()),
        (JValue::String(x), JValue::String(y)) => x.cmp(y),
        _ => a.to_string().cmp(&b.to_string()),
    }
}

struct ItemResult {
    item: Option<JValue>,
    records: Vec<Record>,
    stats: EndpointStats,
    observed: Vec<(String, JValue)>,
    scope: Scope,
    /// An open reject this run replays.
    replay: Option<OpenReject>,
    recovered: Option<bool>,
    failing: Option<(String, String)>,
}

type ItemStream = Pin<Box<dyn Stream<Item = Result<ItemResult, AtlatlError>> + Send>>;

fn skip_key(endpoint: &str, item: Option<&JValue>) -> String {
    let desc = JValue::object([
        ("kind".to_owned(), "skip".into()),
        ("endpoint".to_owned(), endpoint.into()),
        ("item".to_owned(), item.cloned().unwrap_or_default()),
    ]);
    crate::rejects::reject_key(&desc, None)
}

fn item_text(item: Option<&JValue>) -> String {
    item.map(|i| i.clone().sort_keys().to_string())
        .unwrap_or_default()
}

/// Removes `fields` from a map item; returns their text values (nulls dropped).
fn split_secrets(item: &mut Option<JValue>, fields: &[String]) -> IndexMap<String, String> {
    let mut out = IndexMap::new();
    let Some(JValue::Object(m)) = item else {
        return out;
    };
    for f in fields {
        match m.shift_remove(f) {
            None | Some(JValue::Null) => {}
            Some(JValue::String(s)) => {
                out.insert(f.clone(), s);
            }
            Some(v) => {
                out.insert(f.clone(), v.to_string());
            }
        }
    }
    out
}

fn pk_key(pk: &[String], rec: &JValue) -> String {
    pk.iter()
        .map(|k| rec.get(k).unwrap_or(&JValue::Null).to_string())
        .collect::<Vec<_>>()
        .join("\u{1f}")
}

/// Drives one endpoint through a run. Pull [`Chunk`]s with [`Driver::next`], then [`Driver::finish`].
pub struct Driver {
    client: ApiClient,
    endpoint: String,
    ep: Endpoint,
    vars: RunVars,
    decode_retry: Option<DecodeRetry>,
    base_scope: Scope,
    stream: Option<ItemStream>,
    replays: VecDeque<OpenReject>,
    open_skips: HashMap<String, String>,
    folds: IndexMap<String, Fold>,
    observe: Vec<(String, String)>,
    seen: HashSet<String>,
    held: IndexMap<String, (JValue, Scope)>,
    flushed: bool,
    ordinal: i64,
    stats: EndpointStats,
    items: u64,
    deferred: u64,
    held_back: u64,
    skipped: Vec<SkippedItem>,
    rejects: IndexMap<String, NewReject>,
    resolved: Vec<String>,
    range: Option<RangeOverride>,
    range_applied: bool,
    /// `for_each.secret` values by item text, for replaying an item's rejects.
    item_secrets: HashMap<String, IndexMap<String, String>>,
}

impl std::fmt::Debug for Driver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Driver")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

fn merge_stats(into: &mut EndpointStats, s: EndpointStats) {
    into.http.requests += s.http.requests;
    into.http.retries += s.http.retries;
    into.http.skipped_items += s.http.skipped_items;
    into.http.throttle_wait_ms += s.http.throttle_wait_ms;
    for (k, v) in s.http.status_counts {
        *into.http.status_counts.entry(k).or_default() += v;
    }
    into.auth_requests += s.auth_requests;
    into.rate_wait_ms += s.rate_wait_ms;
    into.pages += s.pages;
    into.records += s.records;
    into.filtered += s.filtered;
    into.duplicates += s.duplicates;
    into.bytes_read += s.bytes_read;
    into.attempts.extend(s.attempts);
    into.skips.extend(s.skips);
    into.rejects.extend(s.rejects);
    into.stopped |= s.stopped;
}

async fn run_item(
    client: ApiClient,
    endpoint: String,
    vars: RunVars,
    decode_retry: Option<DecodeRetry>,
    observe: Vec<(String, String)>,
) -> Result<ItemResult, AtlatlError> {
    let item = vars.item.clone();
    let mut run = client.endpoint(&endpoint, vars)?.observe(observe);
    if let Some(m) = decode_retry {
        run = run.with_decode_retry(m);
    }
    let scope = run.scope().clone();
    let stats = run.stats();
    let (mut records, mut observed) = (Vec::new(), Vec::new());
    while let Some(p) = run.next_page().await? {
        records.extend(p);
        observed.extend(run.take_observed());
    }
    Ok(ItemResult {
        item,
        records,
        stats: stats.get(),
        observed,
        scope,
        replay: None,
        recovered: None,
        failing: None,
    })
}

async fn replay_item(
    client: ApiClient,
    endpoint: String,
    vars: RunVars,
    rej: OpenReject,
) -> Result<ItemResult, AtlatlError> {
    let item = vars.item.clone();
    let mut run = client.endpoint(&endpoint, vars)?;
    let scope = run.scope().clone();
    let stats = run.stats();
    let (records, recovered, failing) = match run.replay(&rej.request).await? {
        Replayed::Recovered(r) => (r, true, None),
        Replayed::Failing { error, raw } => (Vec::new(), false, Some((error, raw))),
    };
    Ok(ItemResult {
        item,
        records,
        stats: stats.get(),
        observed: Vec::new(),
        scope,
        replay: Some(rej),
        recovered: Some(recovered),
        failing,
    })
}

fn referenced_roots(client: &ApiClient, src: &str) -> HashSet<String> {
    client
        .engine()
        .undeclared(src, true)
        .map(|n| {
            n.into_iter()
                .map(|x| x.split('.').next().unwrap_or_default().to_owned())
                .collect()
        })
        .unwrap_or_default()
}

impl Driver {
    /// Runs `setup`, resolves the iteration items and starts the first ones.
    pub async fn start(
        client: &ApiClient,
        endpoint: &str,
        mut opts: DriveOptions,
    ) -> Result<Driver, AtlatlError> {
        let ep =
            client.spec().endpoint(endpoint).cloned().ok_or_else(|| {
                AtlatlError::config(format!("API spec has no endpoint `{endpoint}`"))
            })?;
        if ep.history == History::SnapshotOnly && opts.range.is_some() {
            return Err(AtlatlError::config(format!(
                "endpoint `{endpoint}` is `history: snapshot_only` (a live snapshot); `--range` can't backfill it"
            )));
        }
        client.validate(endpoint, &opts.vars)?;
        let mut stats = EndpointStats::default();

        // Setup (07 §12): each step sees the state earlier steps wrote.
        for (i, step) in ep.setup.iter().enumerate() {
            let mut run = client.setup_step(endpoint, i, opts.vars.clone())?;
            let h = run.stats();
            let mut values = Vec::new();
            while let Some(p) = run.next_page().await? {
                values.extend(p.into_iter().map(|r| r.record));
            }
            let mut s = h.get();
            s.records = 0;
            s.duplicates = 0;
            s.filtered = 0;
            merge_stats(&mut stats, s);
            if let Some((kind, name)) = &step.output {
                let v = match kind {
                    SetupOutput::Collect => JValue::Array(values),
                    SetupOutput::Set => values.pop().unwrap_or_default(),
                    SetupOutput::Max | SetupOutput::Min => {
                        let agg = if *kind == SetupOutput::Max {
                            Aggregate::Max
                        } else {
                            Aggregate::Min
                        };
                        let mut f = Fold {
                            agg,
                            expr: String::new(),
                            over: FoldOver::Record,
                            value: None,
                        };
                        values.into_iter().for_each(|v| f.push(v));
                        f.value.unwrap_or_default()
                    }
                };
                opts.vars.state.insert(name.clone(), v);
            }
        }

        let probe = client.endpoint(endpoint, opts.vars.clone())?;
        let base_scope = probe.scope().clone();
        let mut range_applied = false;
        let items: Vec<Option<JValue>> = match (&ep.for_each, opts.items.take()) {
            _ if opts.rejects_only => Vec::new(),
            (None, _) => vec![None],
            (Some(_), Some(given)) => given.into_iter().map(Some).collect(),
            (Some(f), None) => match &f.over {
                Over::List(v) => v.iter().map(|x| Some(JValue::from(x))).collect(),
                Over::Expr(e) => {
                    let mut scope = base_scope.clone();
                    if let Some(r) = &opts.range {
                        range_applied = e.value.contains("range(");
                        scope.set(RANGE_VAR, r.value());
                    }
                    let v = client.engine().eval(&e.value, &scope).map_err(|err| {
                        AtlatlError::new(
                            err.class,
                            format!("{}: `for_each.over`: {}", e.span, err.message),
                        )
                    })?;
                    match to_json(&v) {
                        JValue::Array(a) => a.into_iter().map(Some).collect(),
                        JValue::Null => Vec::new(),
                        other => {
                            return Err(AtlatlError::config(format!(
                                "{}: `for_each.over` must evaluate to a list, found {}",
                                e.span,
                                other.kind()
                            )));
                        }
                    }
                }
                Over::Queue { queue, .. } => {
                    return Err(AtlatlError::internal(format!(
                        "queue `{queue}` items were not supplied"
                    )));
                }
                Over::Query { .. } => {
                    return Err(AtlatlError::internal("query items were not supplied"));
                }
            },
        };
        let secret_fields = ep
            .for_each
            .as_ref()
            .map(|f| f.secret.clone())
            .unwrap_or_default();
        let mut item_secrets: HashMap<String, IndexMap<String, String>> = HashMap::new();
        let mut items: Vec<Option<JValue>> = items
            .into_iter()
            .map(|mut i| {
                let s = split_secrets(&mut i, &secret_fields);
                if !s.is_empty() {
                    item_secrets.insert(item_text(i.as_ref()), s);
                }
                i
            })
            .collect();
        // `except_skipped` (07 §7): items a rule skipped within the TTL sit out; the rest of the
        // earlier skips go after new items, so dead keys can't fill a capped batch.
        let skipped_before: HashMap<String, &str> = opts
            .open_rejects
            .iter()
            .filter(|r| r.class == "skipped")
            .map(|r| {
                (
                    item_text(r.request.get("item").filter(|i| !i.is_null())),
                    r.key.as_str(),
                )
            })
            .collect();
        let mut recent: HashSet<String> = HashSet::new();
        let mut held_back = 0;
        if ep
            .for_each
            .as_ref()
            .is_some_and(|f| f.except_skipped.is_some())
        {
            recent = skipped_before
                .iter()
                .filter(|(_, k)| opts.recent_skips.contains(**k))
                .map(|(t, _)| t.clone())
                .collect();
            let before = items.len();
            items.retain(|i| !recent.contains(&item_text(i.as_ref())));
            held_back = (before - items.len()) as u64;
            items.sort_by_key(|i| skipped_before.contains_key(&item_text(i.as_ref())));
        }
        let all_texts: HashSet<String> = items.iter().map(|i| item_text(i.as_ref())).collect();
        let mut deferred = 0;
        if let Some(m) = ep.for_each.as_ref().and_then(|f| f.max_items.as_ref()) {
            let cap = match m {
                MaxItems::N(n) => *n,
                MaxItems::Expr(e) => {
                    let v = client.engine().eval(&e.value, &base_scope).map_err(|err| {
                        AtlatlError::new(
                            err.class,
                            format!("{}: `for_each.max_items`: {}", e.span, err.message),
                        )
                    })?;
                    let v = to_json(&v);
                    v.as_u64()
                        .or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
                        .ok_or_else(|| {
                            AtlatlError::config(format!(
                                "{}: `for_each.max_items` must evaluate to a non-negative integer, found {}",
                                e.span,
                                v.kind()
                            ))
                        })?
                }
            };
            if items.len() as u64 > cap {
                deferred = items.len() as u64 - cap;
                items.truncate(cap as usize);
            }
        }
        let items: Vec<Option<JValue>> = match ep.for_each.as_ref().and_then(|f| f.batch.as_ref()) {
            Some(b) if b.size > 0 => items
                .chunks(b.size as usize)
                .map(|c| {
                    Some(JValue::Array(
                        c.iter().map(|x| x.clone().unwrap_or_default()).collect(),
                    ))
                })
                .collect(),
            _ => items,
        };

        if let (Some(l), Some(_)) = (&opts.vars.live, &ep.for_each) {
            l.set_items_total(items.len() as u64);
        }

        let mut folds = IndexMap::new();
        let mut observe = Vec::new();
        for (k, s) in &ep.sync {
            let roots = referenced_roots(client, &s.value.value);
            let over = if s.aggregate == Aggregate::Set {
                FoldOver::End
            } else if roots.contains("response") && !roots.contains("record") {
                observe.push((k.clone(), s.value.value.clone()));
                FoldOver::Response
            } else {
                FoldOver::Record
            };
            folds.insert(
                k.clone(),
                Fold {
                    agg: s.aggregate,
                    expr: s.value.value.clone(),
                    over,
                    value: None,
                },
            );
        }

        let texts: HashSet<String> = items.iter().map(|i| item_text(i.as_ref())).collect();
        let mut open_skips = HashMap::new();
        let mut replays = VecDeque::new();
        for r in opts.open_rejects.drain(..) {
            if r.class == "skipped" {
                let t = item_text(r.request.get("item").filter(|i| !i.is_null()));
                if texts.contains(&t) {
                    open_skips.insert(t, r.key);
                    continue;
                }
                // Sitting out its TTL, or left for a later run by `max_items`: it stays open.
                if recent.contains(&t) || all_texts.contains(&t) {
                    continue;
                }
                open_skips.insert(t, r.key.clone());
            }
            replays.push_back(r);
        }

        let n = ep
            .for_each
            .as_ref()
            .and_then(|f| f.concurrency)
            .unwrap_or(1)
            .max(1) as usize;
        let batch_var = ep
            .for_each
            .as_ref()
            .and_then(|f| f.batch.as_ref())
            .map(|b| b.var.clone());
        let (c, name, vars, dr, obs, secrets) = (
            client.clone(),
            endpoint.to_owned(),
            opts.vars.clone(),
            opts.decode_retry,
            observe.clone(),
            item_secrets.clone(),
        );
        let stream = futures::stream::iter(items)
            .map(move |item| {
                let mut v = vars.clone();
                if let (Some(bv), Some(it)) = (&batch_var, &item) {
                    v.globals.insert(bv.clone(), it.clone());
                }
                if let Some(s) = secrets.get(&item_text(item.as_ref())) {
                    v.item_secrets.extend(s.clone());
                }
                v.item = item;
                run_item(c.clone(), name.clone(), v, dr, obs.clone())
            })
            .buffered(n)
            .boxed();

        Ok(Driver {
            client: client.clone(),
            endpoint: endpoint.to_owned(),
            ep,
            vars: opts.vars,
            decode_retry: opts.decode_retry,
            base_scope,
            stream: Some(stream),
            replays,
            open_skips,
            folds,
            observe,
            seen: HashSet::new(),
            held: IndexMap::new(),
            flushed: false,
            ordinal: 0,
            stats,
            items: 0,
            deferred,
            held_back,
            skipped: Vec::new(),
            rejects: IndexMap::new(),
            resolved: Vec::new(),
            range: opts.range,
            range_applied,
            item_secrets,
        })
    }

    /// The next item's records (after run-wide dedup), or `None` when the endpoint is done.
    pub async fn next(&mut self) -> Result<Option<Chunk>, AtlatlError> {
        if let Some(s) = self.stream.as_mut() {
            match s.next().await {
                Some(r) => {
                    let r = r?;
                    let stop = r.stats.stopped;
                    self.items += 1;
                    if let Some(l) = &self.vars.live {
                        l.items_done
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    let chunk = self.absorb(r)?;
                    if stop {
                        self.stream = None;
                    }
                    return Ok(Some(chunk));
                }
                None => self.stream = None,
            }
        }
        if let Some(rej) = self.replays.pop_front() {
            let mut vars = self.vars.clone();
            vars.item = rej.request.get("item").filter(|i| !i.is_null()).cloned();
            if let Some(s) = self.item_secrets.get(&item_text(vars.item.as_ref())) {
                vars.item_secrets.extend(s.clone());
            }
            if let (Some(bv), Some(it)) = (
                self.ep
                    .for_each
                    .as_ref()
                    .and_then(|f| f.batch.as_ref())
                    .map(|b| b.var.clone()),
                &vars.item,
            ) {
                vars.globals.insert(bv, it.clone());
            }
            let r = if rej.class == "skipped" {
                run_item(
                    self.client.clone(),
                    self.endpoint.clone(),
                    vars,
                    self.decode_retry,
                    self.observe.clone(),
                )
                .await?
            } else {
                replay_item(self.client.clone(), self.endpoint.clone(), vars, rej).await?
            };
            return self.absorb(r).map(Some);
        }
        if !self.flushed {
            self.flushed = true;
            let held: Vec<(JValue, Scope)> = std::mem::take(&mut self.held).into_values().collect();
            if !held.is_empty() {
                let mut chunk = Chunk::default();
                for (rec, scope) in held {
                    self.emit(rec, &scope, &mut chunk)?;
                }
                return Ok(Some(chunk));
            }
        }
        Ok(None)
    }

    fn emit(&mut self, rec: JValue, scope: &Scope, chunk: &mut Chunk) -> Result<(), AtlatlError> {
        let engine = self.client.engine();
        let needs_scope =
            !self.ep.emit.is_empty() || self.folds.values().any(|f| f.over == FoldOver::Record);
        if needs_scope {
            let mut s = scope.clone();
            s.set_json("record", Arc::new(rec.clone()));
            for (q, e) in &self.ep.emit {
                let v = engine.eval_json(e, &s).map_err(|err| {
                    AtlatlError::new(
                        err.class,
                        format!("{}: `emit.{q}`: {}", e.span, err.message),
                    )
                })?;
                if !v.is_null() {
                    chunk.emits.push((q.clone(), v));
                }
            }
            for f in self
                .folds
                .values_mut()
                .filter(|f| f.over == FoldOver::Record)
            {
                let v = engine.eval_json(&f.expr, &s)?;
                f.push(v);
            }
        }
        chunk.records.push(Record {
            ordinal: self.ordinal,
            record: rec,
        });
        self.ordinal += 1;
        Ok(())
    }

    fn absorb(&mut self, mut r: ItemResult) -> Result<Chunk, AtlatlError> {
        let item = r.item.clone();
        let text = item_text(item.as_ref());
        let skips = std::mem::take(&mut r.stats.skips);
        let row_rejects = std::mem::take(&mut r.stats.rejects);
        merge_stats(&mut self.stats, std::mem::take(&mut r.stats));
        self.stats.skips.extend(skips.iter().cloned());
        let skipped = !skips.is_empty();
        for s in skips {
            if self.ep.record_skips || self.open_skips.contains_key(&text) {
                let key = skip_key(&self.endpoint, item.as_ref());
                let request = JValue::object([
                    ("kind".to_owned(), "skip".into()),
                    ("endpoint".to_owned(), self.endpoint.as_str().into()),
                    ("item".to_owned(), item.clone().unwrap_or_default()),
                    ("request_key".to_owned(), s.request_key.as_str().into()),
                ]);
                self.rejects.insert(
                    key.clone(),
                    NewReject {
                        key,
                        request,
                        raw: String::new(),
                        error: s.reason.clone(),
                        class: "skipped".into(),
                    },
                );
            }
            self.skipped.push(SkippedItem {
                item: item.clone(),
                request_key: s.request_key,
                reason: s.reason,
            });
        }
        if !skipped
            && r.replay.as_ref().is_none_or(|x| x.class == "skipped")
            && let Some(k) = self.open_skips.remove(&text)
        {
            self.resolved.push(k);
        }
        for rj in row_rejects {
            let mut request = rj.request;
            if let JValue::Object(m) = &mut request {
                m.insert("item".into(), item.clone().unwrap_or_default());
            }
            let key = crate::rejects::reject_key(&request, rj.row);
            self.rejects.insert(
                key.clone(),
                NewReject {
                    key,
                    request,
                    raw: rj.raw,
                    error: rj.error,
                    class: "decode".into(),
                },
            );
        }
        if let Some(rep) = r.replay.take()
            && rep.class != "skipped"
        {
            match (r.recovered, r.failing.take()) {
                (Some(true), _) => self.resolved.push(rep.key),
                (_, Some((error, raw))) => {
                    self.rejects.insert(
                        rep.key.clone(),
                        NewReject {
                            key: rep.key,
                            request: rep.request,
                            raw,
                            error,
                            class: "decode".into(),
                        },
                    );
                }
                _ => {}
            }
        }
        for (k, v) in r.observed {
            if let Some(f) = self.folds.get_mut(&k) {
                f.push(v);
            }
        }
        let dedup = self.ep.records.effective_dedup();
        let pk = self.ep.records.primary_key.clone();
        let mut chunk = Chunk::default();
        for rec in r.records {
            match dedup {
                Dedup::None => self.emit(rec.record, &r.scope, &mut chunk)?,
                Dedup::First => {
                    if self.seen.insert(pk_key(&pk, &rec.record)) {
                        self.emit(rec.record, &r.scope, &mut chunk)?;
                    } else {
                        self.stats.duplicates += 1;
                    }
                }
                Dedup::Last => {
                    let k = pk_key(&pk, &rec.record);
                    if self.held.shift_remove(&k).is_some() {
                        self.stats.duplicates += 1;
                    }
                    self.held.insert(k, (rec.record, r.scope.clone()));
                }
            }
        }
        Ok(chunk)
    }

    /// Applies the skip guard and computes the sync values to persist.
    pub fn finish(mut self) -> Result<DriveOutcome, AtlatlError> {
        if let Some(m) = &self.ep.max_skipped {
            let n = self.skipped.len() as f64;
            if self.items >= m.min_items && self.items > 0 && n / self.items as f64 > m.ratio {
                return Err(AtlatlError::new(
                    ErrorClass::Data,
                    format!(
                        "endpoint `{}`: {} of {} items were skipped, over `max_skipped` ({}); first: {}",
                        self.endpoint,
                        self.skipped.len(),
                        self.items,
                        m.ratio,
                        self.skipped.first().map(|s| s.reason.as_str()).unwrap_or_default()
                    ),
                )
                .code("too_many_skipped"));
            }
        }
        let engine = self.client.engine();
        let stored = self.vars.sync.clone();
        let mut sync: IndexMap<String, JValue> = IndexMap::new();
        for (k, f) in self.folds.iter_mut() {
            let old = stored.get(k).cloned().filter(|v| !v.is_null());
            let new = match f.over {
                FoldOver::End => {
                    Some(engine.eval_json(&f.expr, &self.base_scope)?).filter(|v| !v.is_null())
                }
                _ if self.range.is_some() => match (f.agg, old.clone(), f.value.take()) {
                    (Aggregate::Max | Aggregate::Min, Some(o), Some(v)) => {
                        let mut g = Fold {
                            agg: f.agg,
                            expr: String::new(),
                            over: f.over,
                            value: Some(o),
                        };
                        g.push(v);
                        g.value
                    }
                    (Aggregate::Max | Aggregate::Min, None, v) => v,
                    _ => None,
                },
                _ => f.value.take(),
            };
            if let Some(v) = new.or(old) {
                sync.insert(k.clone(), v);
            }
        }
        let before: IndexMap<String, JValue> =
            stored.into_iter().filter(|(_, v)| !v.is_null()).collect();
        let sync_changed = !self.folds.is_empty() && sync != before;
        let reject_keys: HashSet<&String> = self.rejects.keys().collect();
        let mut resolved: Vec<String> = self
            .resolved
            .iter()
            .filter(|k| !reject_keys.contains(k))
            .cloned()
            .collect();
        resolved.sort();
        resolved.dedup();
        Ok(DriveOutcome {
            stats: self.stats,
            sync,
            sync_changed,
            items: self.items,
            items_deferred: self.deferred,
            items_held: self.held_back,
            skipped: self.skipped,
            rejects: self.rejects.into_values().collect(),
            resolved,
            range_applied: self.range_applied,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_override_parse() {
        assert_eq!(
            RangeOverride::parse("2015-01-01..2020-01-01").expect("ok"),
            RangeOverride {
                start: Some("2015-01-01".into()),
                end: Some("2020-01-01".into())
            }
        );
        assert_eq!(RangeOverride::parse("..5").expect("ok").start, None);
        assert!(RangeOverride::parse("2015").is_err());
        assert!(RangeOverride::parse("..").is_err());
    }

    #[test]
    fn range_uses_the_override_only_when_bound() {
        let e = crate::expr::Engine::default();
        let mut s = Scope::new();
        let v = e.eval_json("range(1, 3)", &s).expect("eval");
        assert_eq!(v.to_string(), "[1,2,3]");
        s.set(RANGE_VAR, RangeOverride::parse("5..6").expect("r").value());
        assert_eq!(
            e.eval_json("range(1, 3)", &s).expect("eval").to_string(),
            "[5,6]"
        );
        s.set(
            RANGE_VAR,
            RangeOverride::parse("2015-07-01..").expect("r").value(),
        );
        let d = e
            .eval_json(r#"range("2015-01-01", "2016-01-01", "3mo")"#, &s)
            .expect("eval");
        assert_eq!(
            d.to_string(),
            r#"["2015-07-01T00:00:00Z","2015-10-01T00:00:00Z","2016-01-01T00:00:00Z"]"#
        );
    }

    #[test]
    fn folds() {
        let j = |s: &str| crate::json::parse_str(s).expect("json");
        let mut f = Fold {
            agg: Aggregate::Max,
            expr: String::new(),
            over: FoldOver::Record,
            value: None,
        };
        for v in ["3", "12345678901234567890123", "null", "7"] {
            f.push(j(v));
        }
        assert_eq!(
            f.value.map(|v| v.to_string()).as_deref(),
            Some("12345678901234567890123")
        );
        let mut f = Fold {
            agg: Aggregate::Min,
            expr: String::new(),
            over: FoldOver::Record,
            value: None,
        };
        for v in [r#""2025-02-01""#, r#""2025-01-01""#] {
            f.push(j(v));
        }
        assert_eq!(
            f.value.map(|v| v.to_string()).as_deref(),
            Some(r#""2025-01-01""#)
        );
        let mut f = Fold {
            agg: Aggregate::First,
            expr: String::new(),
            over: FoldOver::Record,
            value: None,
        };
        f.push(j("1"));
        f.push(j("2"));
        assert_eq!(f.value.map(|v| v.to_string()).as_deref(), Some("1"));
    }
}
