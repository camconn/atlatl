//! REST API spec discovery. Until the REST engine (M3) only the dependency-bearing parts of a spec
//! are read (`endpoints.*.{emit, for_each.over.queue, depends_on}`); the rest is not yet validated.

use std::path::Path;
use std::sync::Arc;

use indexmap::IndexMap;

use super::de::Spanned;
use super::diag::{ConfigError, Diagnostics};
use super::node::{Node, Span, Value};

#[derive(Clone, Debug)]
pub struct ApiSpec {
    /// Path relative to the project root, `/`-separated.
    pub path: String,
    pub file: Arc<Path>,
    pub name: Option<String>,
    pub endpoints: IndexMap<String, EndpointSummary>,
}

#[derive(Clone, Debug, Default)]
pub struct EndpointSummary {
    pub span: Span,
    pub emits: Vec<String>,
    pub consumes: Vec<Spanned<String>>,
    pub depends_on: Vec<Spanned<String>>,
    /// `for_each.over.connection` of a `query` iteration (resolved by live plans).
    pub query_connections: Vec<Spanned<String>>,
}

impl ApiSpec {
    /// Endpoints `endpoint` needs to have run first: producers of the queues it consumes, plus
    /// explicit `depends_on`.
    pub fn producers_of(&self, endpoint: &str) -> Vec<String> {
        let Some(ep) = self.endpoints.get(endpoint) else {
            return Vec::new();
        };
        let mut out: Vec<String> = Vec::new();
        for q in &ep.consumes {
            for (name, other) in &self.endpoints {
                if other.emits.iter().any(|e| e == &q.value) && !out.contains(name) {
                    out.push(name.clone());
                }
            }
        }
        for d in &ep.depends_on {
            if !out.contains(&d.value) {
                out.push(d.value.clone());
            }
        }
        out
    }
}

fn str_at(n: &Node, what: &str) -> Result<Spanned<String>, ConfigError> {
    n.as_str()
        .map(|s| Spanned::new(s.to_owned(), n.span.clone()))
        .ok_or_else(|| ConfigError::new(format!("{what} must be a string")).at(n.span.clone()))
}

pub fn parse_api_spec(path: String, node: &Node, diags: &mut Diagnostics) -> ApiSpec {
    let mut spec = ApiSpec {
        path,
        file: node.span.file.clone(),
        name: None,
        endpoints: IndexMap::new(),
    };
    if node.as_map().is_none() {
        diags.error(
            "api-spec",
            ConfigError::new("an API spec must be a map").at(node.span.clone()),
        );
        return spec;
    }
    spec.name = node.get("name").and_then(Node::as_str).map(str::to_owned);
    let Some(eps) = node.get("endpoints") else {
        diags.error(
            "api-spec",
            ConfigError::new("API spec has no `endpoints`").at(node.span.clone()),
        );
        return spec;
    };
    let Some(eps) = eps.as_map() else {
        diags.error(
            "api-spec",
            ConfigError::new("`endpoints` must be a map").at(eps.span.clone()),
        );
        return spec;
    };
    for (name, e) in eps {
        let ep = &e.value;
        let mut s = EndpointSummary {
            span: e.key_span.clone(),
            ..Default::default()
        };
        if let Some(emit) = ep.get("emit") {
            match emit.as_map() {
                Some(m) => s.emits = m.keys().cloned().collect(),
                None => diags.error(
                    "api-spec",
                    ConfigError::new("`emit` must be a map of queue: expression")
                        .at(emit.span.clone()),
                ),
            }
        }
        if let Some(c) = ep
            .get("for_each")
            .and_then(|f| f.get("over"))
            .filter(|o| o.get("query").is_some())
            .and_then(|o| o.get("connection"))
        {
            match str_at(c, "`over.connection`") {
                Ok(c) => s.query_connections.push(c),
                Err(err) => diags.error("api-spec", err),
            }
        }
        if let Some(over) = ep.get("for_each").and_then(|f| f.get("over"))
            && let Some(q) = over.get("queue")
        {
            match str_at(q, "`over.queue`") {
                Ok(q) => s.consumes.push(q),
                Err(err) => diags.error("api-spec", err),
            }
        }
        if let Some(d) = ep.get("depends_on") {
            match &d.value {
                Value::Seq(items) => {
                    for i in items {
                        match str_at(i, "`depends_on` entries") {
                            Ok(v) => s.depends_on.push(v),
                            Err(err) => diags.error("api-spec", err),
                        }
                    }
                }
                _ => match str_at(d, "`depends_on`") {
                    Ok(v) => s.depends_on.push(v),
                    Err(err) => diags.error("api-spec", err),
                },
            }
        }
        spec.endpoints.insert(name.clone(), s);
    }
    check_spec(&spec, diags);
    spec
}

fn check_spec(spec: &ApiSpec, diags: &mut Diagnostics) {
    for (name, ep) in &spec.endpoints {
        for q in &ep.consumes {
            if !spec.endpoints.values().any(|e| e.emits.contains(&q.value)) {
                diags.error(
                    "api-queue",
                    ConfigError::new(format!(
                        "endpoint `{name}` iterates queue `{}`, which no endpoint emits",
                        q.value
                    ))
                    .at(q.span.clone()),
                );
            }
        }
        for d in &ep.depends_on {
            if !spec.endpoints.contains_key(&d.value) {
                diags.error(
                    "api-depends-on",
                    ConfigError::new(format!(
                        "endpoint `{name}` depends on unknown endpoint `{}`",
                        d.value
                    ))
                    .at(d.span.clone()),
                );
            }
        }
    }
    // Endpoint dependency cycles.
    let names: Vec<&String> = spec.endpoints.keys().collect();
    let deps = |n: &str| spec.producers_of(n);
    if let Some(cycle) =
        super::graph::find_cycle(&names.iter().map(|s| s.as_str()).collect::<Vec<_>>(), deps)
    {
        let first = &spec.endpoints[cycle[0].as_str()];
        diags.error(
            "api-cycle",
            ConfigError::new(format!("endpoint dependency cycle: {}", cycle.join(" -> ")))
                .at(first.span.clone()),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::node::parse_yaml;

    fn spec(s: &str) -> (ApiSpec, Diagnostics) {
        let n = parse_yaml(s, Arc::from(Path::new("apis/x.yaml"))).expect("yaml");
        let mut d = Diagnostics::default();
        let spec = parse_api_spec("apis/x.yaml".into(), &n, &mut d);
        (spec, d)
    }

    #[test]
    fn producers_from_queues_and_depends_on() {
        let (s, d) = spec(
            "name: x\nendpoints:\n  companies:\n    emit: {companies: record.id}\n  fundamentals:\n    for_each: {over: {queue: companies}, as: c}\n  other:\n    depends_on: [companies]\n  plain: {}\n",
        );
        assert!(!d.has_errors(), "{d:?}");
        assert_eq!(s.producers_of("fundamentals"), ["companies"]);
        assert_eq!(s.producers_of("other"), ["companies"]);
        assert!(s.producers_of("plain").is_empty());
    }

    #[test]
    fn unknown_queue_and_cycles() {
        let (_, d) = spec("endpoints:\n  a:\n    for_each: {over: {queue: nope}}\n");
        assert!(d.errors().any(|e| e.code == "api-queue"));
        let (_, d) = spec("endpoints:\n  a: {depends_on: [b]}\n  b: {depends_on: [a]}\n");
        assert!(d.errors().any(|e| e.code == "api-cycle"), "{d:?}");
        let (_, d) = spec("endpoints:\n  a: {depends_on: [zz]}\n");
        assert!(d.errors().any(|e| e.code == "api-depends-on"));
    }
}
