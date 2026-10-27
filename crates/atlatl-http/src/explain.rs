//! `atlatl api explain` (07 §13): the effective merged config of a spec or one endpoint —
//! defaults, auth (templates only, redacted), limiter, pagination, records, rules in evaluation
//! order (built-ins included) and dependencies.

use serde_json::{Map, Value, json};

use crate::client::ApiClient;
use crate::spec::{
    AuthSpec, Body, Endpoint, Format, Over, Paginate, PaginateKind, RecordsSpec, RequestSpec,
};
use atlatl_core::AtlatlError;

impl ApiClient {
    /// The spec overview, or one endpoint in full. Every string is redacted.
    pub fn explain(&self, endpoint: Option<&str>) -> Result<Value, AtlatlError> {
        let spec = self.spec();
        let lim = self.limiter().config();
        let mut out = json!({
            "api": spec.name,
            "description": spec.description,
            "base_url": spec.base_url,
            "auth": auth(&spec.auth),
            "defaults": request(&spec.default_request),
            "rate_limit": {
                "scope": format!("{:?}", spec.rate_limit.scope).to_lowercase(),
                "rps": lim.rps,
                "burst": lim.burst,
                "min_interval_ms": lim.min_interval.as_millis() as u64,
                "max_in_flight": lim.max_in_flight,
                "budget_requests": lim.budget,
                "respect_headers": lim.respect_headers,
                "adaptive_recover_after_ms": lim.adaptive.map(|d| d.as_millis() as u64),
                "declared": spec.rate_limit.declared,
            },
            "throttle_patterns": spec.throttle_patterns,
            "decode_failures_as_throttle": format!(
                "{}/{}s",
                spec.decode_failures_as_throttle.count,
                spec.decode_failures_as_throttle.window.as_secs()
            ),
            "max_body": spec.max_body,
            "state": spec.state,
        });
        let obj = out
            .as_object_mut()
            .ok_or_else(|| AtlatlError::internal("explain"))?;
        match endpoint {
            None => {
                obj.insert(
                    "endpoints".into(),
                    Value::Object(
                        spec.endpoints
                            .iter()
                            .map(|(n, e)| (n.clone(), summary(e)))
                            .collect(),
                    ),
                );
            }
            Some(name) => {
                let ep = spec.endpoint(name).ok_or_else(|| {
                    AtlatlError::config(format!(
                        "API spec `{}` has no endpoint `{name}` (endpoints: {})",
                        spec.name.as_deref().unwrap_or("?"),
                        spec.endpoints
                            .keys()
                            .cloned()
                            .collect::<Vec<_>>()
                            .join(", ")
                    ))
                })?;
                let rules: Vec<Value> = self
                    .rules(name)?
                    .iter()
                    .map(|r| Value::String(r.describe()))
                    .collect();
                let mut e = endpoint_detail(ep);
                if let Some(m) = e.as_object_mut() {
                    m.insert("rules".into(), Value::Array(rules));
                    if let Some(a) = &ep.auth {
                        m.insert("auth".into(), auth(a));
                    }
                }
                obj.insert("endpoint".into(), e);
            }
        }
        atlatl_core::obs::redacted_json(&out).map_err(|e| AtlatlError::internal(e.to_string()))
    }
}

fn auth(a: &AuthSpec) -> Value {
    let mut m = Map::new();
    m.insert("type".into(), a.type_name().into());
    match a {
        AuthSpec::None => {}
        AuthSpec::Headers(h) => {
            m.insert("headers".into(), json!(h));
        }
        AuthSpec::Query(q) => {
            m.insert("params".into(), json!(q));
        }
        AuthSpec::Bearer { token } => {
            m.insert("token".into(), token.clone().into());
        }
        AuthSpec::Basic { username, password } => {
            m.insert("username".into(), username.clone().into());
            m.insert("password".into(), password.clone().into());
        }
        AuthSpec::OAuth2(o) => {
            m.insert("flow".into(), format!("{:?}", o.flow).to_lowercase().into());
            m.insert("token_url".into(), o.token_url.clone().into());
            m.insert(
                "client_auth".into(),
                format!("{:?}", o.client_auth).to_lowercase().into(),
            );
            for (k, v) in [
                ("client_id", &o.client_id),
                ("client_secret", &o.client_secret),
                ("username", &o.username),
                ("password", &o.password),
                ("refresh_token", &o.refresh_token),
            ] {
                if let Some(v) = v {
                    m.insert(k.into(), v.clone().into());
                }
            }
            if !o.scopes.is_empty() {
                m.insert("scopes".into(), json!(o.scopes));
            }
            if !o.extra.is_empty() {
                m.insert("extra".into(), json!(o.extra));
            }
            if let Some(w) = &o.reauth_when {
                m.insert("reauth_when".into(), w.value.clone().into());
            }
        }
        AuthSpec::Hmac(h) => {
            m.insert(
                "algorithm".into(),
                format!("{:?}", h.algorithm).to_lowercase().into(),
            );
            m.insert("key".into(), h.key.clone().into());
            m.insert("canonical".into(), h.canonical.clone().into());
            m.insert(
                "encoding".into(),
                format!("{:?}", h.encoding).to_lowercase().into(),
            );
            m.insert(
                "placement".into(),
                match &h.placement {
                    crate::spec::Placement::Header(n) => json!({"header": n}),
                    crate::spec::Placement::Param(n) => json!({"param": n}),
                },
            );
            if !h.params.is_empty() {
                m.insert("params".into(), json!(h.params));
            }
            if !h.headers.is_empty() {
                m.insert("headers".into(), json!(h.headers));
            }
        }
    }
    Value::Object(m)
}

fn request(r: &RequestSpec) -> Value {
    let mut m = Map::new();
    m.insert("method".into(), r.method.as_str().into());
    if let Some(p) = &r.path {
        m.insert("path".into(), p.value.clone().into());
    }
    if let Some(u) = &r.url {
        m.insert("url".into(), u.value.clone().into());
    }
    if !r.params.is_empty() {
        m.insert("params".into(), json!(r.params));
    }
    if !r.headers.is_empty() {
        m.insert("headers".into(), json!(r.headers));
    }
    match &r.body {
        Some(Body::Json(v)) => {
            m.insert("body".into(), json!({"json": v}));
        }
        Some(Body::Form(f)) => {
            m.insert("body".into(), json!({"form": f}));
        }
        Some(Body::Text(t)) => {
            m.insert("body".into(), json!({"text": t}));
        }
        None => {}
    }
    if let Some(t) = &r.timeout {
        m.insert("timeout_ms".into(), (t.0.as_millis() as u64).into());
    }
    if r.keep_empty {
        m.insert("keep_empty".into(), true.into());
    }
    Value::Object(m)
}

fn paginate(p: &Paginate) -> Value {
    let mut m = Map::new();
    m.insert("type".into(), p.kind.name().into());
    match &p.kind {
        PaginateKind::Cursor { param, from } => {
            m.insert("param".into(), param.clone().into());
            m.insert("from".into(), from.value.clone().into());
        }
        PaginateKind::Page { param, start, step } => {
            m.insert("param".into(), param.clone().into());
            m.insert("start".into(), (*start).into());
            m.insert("step".into(), (*step).into());
        }
        PaginateKind::Offset {
            param,
            start,
            limit_param,
            limit,
            total_from,
        } => {
            m.insert("param".into(), param.clone().into());
            m.insert("start".into(), (*start).into());
            m.insert("limit_param".into(), json!(limit_param));
            m.insert("limit".into(), (*limit).into());
            m.insert(
                "total_from".into(),
                json!(total_from.as_ref().map(|e| &e.value)),
            );
        }
        PaginateKind::NextUrl { from } => {
            m.insert("from".into(), from.value.clone().into());
        }
        PaginateKind::LinkHeader { rel } => {
            m.insert("rel".into(), rel.clone().into());
        }
        PaginateKind::Expression { next } => {
            m.insert(
                "next".into(),
                Value::Object(
                    next.iter()
                        .map(|(k, v)| (k.clone(), v.value.clone().into()))
                        .collect(),
                ),
            );
        }
    }
    m.insert("max_pages".into(), p.max_pages.into());
    m.insert("stop_on_empty".into(), p.stop_on_empty.into());
    if let Some(w) = &p.stop_when {
        m.insert("stop_when".into(), w.value.clone().into());
    }
    Value::Object(m)
}

fn records(r: &RecordsSpec) -> Value {
    json!({
        "path": r.path.as_ref().map(|p| &p.value),
        "filter": r.filter.as_ref().map(|p| &p.value),
        "primary_key": r.primary_key,
        "dedup": format!("{:?}", r.effective_dedup()).to_lowercase(),
    })
}

fn over(o: &Over) -> Value {
    match o {
        Over::List(l) => json!({"list": l}),
        Over::Expr(e) => json!({"expr": e.value}),
        Over::Queue {
            queue,
            except_landed,
            ..
        } if !except_landed.is_empty() => json!({"queue": queue, "except_landed": except_landed}),
        Over::Queue { queue, .. } => json!({"queue": queue}),
        other => json!({"other": format!("{other:?}")}),
    }
}

fn consumes(ep: &Endpoint) -> Option<String> {
    match ep.for_each.as_ref().map(|f| &f.over) {
        Some(Over::Queue { queue, .. }) => Some(queue.clone()),
        _ => None,
    }
}

fn summary(ep: &Endpoint) -> Value {
    let mut m = Map::new();
    if let Some(d) = &ep.description {
        m.insert("description".into(), d.clone().into());
    }
    let r = &ep.request;
    m.insert(
        "request".into(),
        format!(
            "{} {}",
            r.method.as_str(),
            r.url
                .as_ref()
                .or(r.path.as_ref())
                .map(|p| p.value.as_str())
                .unwrap_or("")
        )
        .into(),
    );
    if let Some(p) = &ep.paginate {
        m.insert("paginate".into(), p.kind.name().into());
    }
    if let Some(q) = consumes(ep) {
        m.insert("consumes".into(), q.into());
    }
    if !ep.emit.is_empty() {
        m.insert("emits".into(), json!(ep.emit.keys().collect::<Vec<_>>()));
    }
    if !ep.depends_on.is_empty() {
        m.insert("depends_on".into(), json!(ep.depends_on));
    }
    Value::Object(m)
}

fn endpoint_detail(ep: &Endpoint) -> Value {
    let mut m = Map::new();
    m.insert("name".into(), ep.name.clone().into());
    if let Some(d) = &ep.description {
        m.insert("description".into(), d.clone().into());
    }
    if !ep.setup.is_empty() {
        m.insert(
            "setup".into(),
            Value::Array(
                ep.setup
                    .iter()
                    .map(|s| {
                        json!({
                            "name": s.name,
                            "request": request(&s.request),
                            "paginate": s.paginate.as_ref().map(paginate),
                            "records": records(&s.records),
                            "output": s.output.as_ref().map(|(k, n)| format!("{}: {n}", format!("{k:?}").to_lowercase())),
                        })
                    })
                    .collect(),
            ),
        );
    }
    if let Some(f) = &ep.for_each {
        m.insert(
            "for_each".into(),
            json!({
                "over": over(&f.over),
                "as": f.var,
                "concurrency": f.concurrency,
                "batch": f.batch.as_ref().map(|b| json!({"size": b.size, "as": b.var})),
                "secret": f.secret,
            }),
        );
    }
    m.insert("request".into(), request(&ep.request));
    if let Some(p) = &ep.paginate {
        m.insert("paginate".into(), paginate(p));
    }
    let mut decode = json!({
        "format": format!("{:?}", ep.decode.format).to_lowercase(),
        "content_type": format!("{:?}", ep.decode.content_type).to_lowercase(),
        "max_body": ep.decode.max_body.map(|b| b.0),
    });
    if ep.decode.format == Format::Csv
        && let Some(d) = decode.as_object_mut()
    {
        d.insert("null_values".into(), json!(ep.decode.csv_nulls()));
    }
    m.insert("decode".into(), decode);
    m.insert("raw".into(), ep.raw.into());
    m.insert("records".into(), records(&ep.records));
    if !ep.fields.is_empty() {
        m.insert(
            "fields".into(),
            Value::Object(
                ep.fields
                    .iter()
                    .map(|(k, v)| (k.clone(), v.value.clone().into()))
                    .collect(),
            ),
        );
    }
    if !ep.emit.is_empty() {
        m.insert(
            "emit".into(),
            Value::Object(
                ep.emit
                    .iter()
                    .map(|(k, v)| (k.clone(), v.value.clone().into()))
                    .collect(),
            ),
        );
    }
    if !ep.sync.is_empty() {
        m.insert(
            "sync".into(),
            Value::Object(
                ep.sync
                    .iter()
                    .map(|(k, s)| {
                        (
                            k.clone(),
                            json!({"value": s.value.value, "aggregate": format!("{:?}", s.aggregate).to_lowercase()}),
                        )
                    })
                    .collect(),
            ),
        );
    }
    m.insert(
        "history".into(),
        format!("{:?}", ep.history).to_lowercase().into(),
    );
    let mut deps: Vec<Value> = ep
        .depends_on
        .iter()
        .map(|d| json!({"endpoint": d}))
        .collect();
    if let Some(q) = consumes(ep) {
        deps.push(json!({"queue": q}));
    }
    m.insert("dependencies".into(), Value::Array(deps));
    Value::Object(m)
}

/// Indented `key: value` text for the terminal.
pub fn render(v: &Value) -> String {
    let mut out = String::new();
    walk(v, 0, &mut out);
    out
}

fn scalar(v: &Value) -> Option<String> {
    match v {
        Value::Null => Some("~".into()),
        Value::String(s) => Some(s.clone()),
        Value::Array(a) if a.is_empty() => Some("[]".into()),
        Value::Object(o) if o.is_empty() => Some("{}".into()),
        Value::Array(a) if a.iter().all(|x| !x.is_object() && !x.is_array()) && a.len() <= 8 => {
            Some(format!(
                "[{}]",
                a.iter()
                    .map(|x| scalar(x).unwrap_or_default())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        }
        Value::Array(_) | Value::Object(_) => None,
        other => Some(other.to_string()),
    }
}

fn walk(v: &Value, depth: usize, out: &mut String) {
    let pad = "  ".repeat(depth);
    match v {
        Value::Object(m) => {
            for (k, x) in m {
                match scalar(x) {
                    Some(s) => out.push_str(&format!("{pad}{k}: {s}\n")),
                    None => {
                        out.push_str(&format!("{pad}{k}:\n"));
                        walk(x, depth + 1, out);
                    }
                }
            }
        }
        Value::Array(a) => {
            for x in a {
                match scalar(x) {
                    Some(s) => out.push_str(&format!("{pad}- {s}\n")),
                    None => {
                        out.push_str(&format!("{pad}-\n"));
                        walk(x, depth + 1, out);
                    }
                }
            }
        }
        other => out.push_str(&format!("{pad}{}\n", scalar(other).unwrap_or_default())),
    }
}
