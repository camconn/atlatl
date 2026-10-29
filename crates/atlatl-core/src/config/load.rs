//! Project discovery and assembly (spec 02 §1, §3). Loading is environment-agnostic; environments
//! are applied by [`super::env`].

use std::path::{Path, PathBuf};
use std::sync::Arc;

use indexmap::IndexMap;

use super::apis::{ApiSpec, parse_api_spec};
use super::de::Raw;
use super::de::{Spanned, from_node};
use super::diag::{ConfigError, Diagnostics, SourceMap};
use super::model::{
    EngineConfig, EngineOverrides, EnvironmentsConfig, ForEachConfig, HopConfig, Mode, ProjectFile,
    ReplicationConfig, ReplicationsFile, RetryConfig, SchemaFile, SourceSchema, SourceSnapshot,
    StateConfig, StreamConfig,
};
use super::node::{Entry, Map, Node, Span, merge, parse_yaml};
use super::types::Dur;

pub const PROJECT_FILE: &str = "atlatl.yaml";

#[derive(Clone, Debug)]
pub struct RawConnection {
    pub name: String,
    pub key_span: Span,
    /// The full definition, including its `environments` block.
    pub node: Node,
}

#[derive(Clone, Debug)]
pub struct SchemaDoc {
    pub schema: Spanned<SourceSchema>,
    /// `None` when inline.
    pub file: Option<Arc<Path>>,
}

#[derive(Clone, Debug)]
pub struct Stream {
    pub id: Spanned<String>,
    /// Position within the replication.
    pub seq: usize,
    /// The merged node (project defaults → replication defaults → stream).
    pub node: Node,
    pub config: StreamConfig,
    pub schema: Option<SchemaDoc>,
    /// Index into [`Replication::hops`]; 0 for the replication's own streams.
    pub hop: usize,
}

impl Stream {
    pub fn enabled(&self) -> bool {
        self.config.enabled.unwrap_or(true)
    }
}

#[derive(Clone, Debug)]
pub struct Replication {
    pub name: Spanned<String>,
    pub file: Arc<Path>,
    pub op: Spanned<String>,
    pub source: Spanned<String>,
    pub target: Spanned<String>,
    pub after: Vec<Spanned<String>>,
    pub atomic_group: Option<Spanned<String>>,
    pub lock_group: Option<Spanned<String>>,
    pub lock_timeout: Option<Dur>,
    /// Project `defaults.retry` merged with the replication's `retry`.
    pub retry: RetryConfig,
    pub replication_retries: u32,
    pub timeout: Option<Dur>,
    pub engine: Option<EngineOverrides>,
    /// Every hop's streams, in hop then declaration order.
    pub streams: Vec<Stream>,
    /// `hops[0]` is the replication itself; the rest come from `then:`, parents before children.
    pub hops: Vec<Hop>,
    pub for_each: Option<Spanned<ForEachConfig>>,
    pub source_snapshot: Option<Spanned<SourceSnapshot>>,
}

#[derive(Clone, Debug)]
pub struct Hop {
    /// The upstream hop, whose target is this hop's source; `None` for hop 0.
    pub parent: Option<usize>,
    pub source: Spanned<String>,
    pub target: Spanned<String>,
}

impl Replication {
    pub fn hop_of(&self, s: &Stream) -> &Hop {
        &self.hops[s.hop]
    }

    pub fn is_multi_hop(&self) -> bool {
        self.hops.len() > 1
    }
}

#[derive(Debug, Default)]
pub struct Project {
    pub root: PathBuf,
    pub sources: SourceMap,
    pub environments: Option<Spanned<EnvironmentsConfig>>,
    pub engine: EngineConfig,
    pub state: Option<StateConfig>,
    pub defaults: Option<Node>,
    pub connections: IndexMap<String, RawConnection>,
    pub replications: Vec<Replication>,
    /// Keyed by path relative to the root.
    pub api_specs: IndexMap<String, ApiSpec>,
    /// Source schema files, keyed by stream id.
    pub schema_files: IndexMap<String, SchemaDoc>,
}

impl Project {
    pub fn replication(&self, name: &str) -> Option<&Replication> {
        self.replications.iter().find(|r| r.name.value == name)
    }

    pub fn stream(&self, id: &str) -> Option<(&Replication, &Stream)> {
        self.replications
            .iter()
            .find_map(|r| r.streams.iter().find(|s| s.id.value == id).map(|s| (r, s)))
    }

    pub fn relative<'a>(&self, p: &'a Path) -> &'a Path {
        p.strip_prefix(&self.root).unwrap_or(p)
    }
}

fn is_yaml(p: &Path) -> bool {
    matches!(p.extension().and_then(|e| e.to_str()), Some("yaml" | "yml"))
}

fn walk(dir: &Path, recursive: bool, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for e in std::fs::read_dir(dir)? {
        let p = e?.path();
        if p.is_dir() {
            if recursive {
                walk(&p, true, out)?;
            }
        } else if is_yaml(&p) {
            out.push(p);
        }
    }
    Ok(())
}

fn yaml_files(root: &Path, sub: &str, recursive: bool, diags: &mut Diagnostics) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Err(e) = walk(&root.join(sub), recursive, &mut out) {
        diags.error("io", ConfigError::new(format!("reading {sub}/: {e}")));
    }
    out.sort();
    out
}

fn read_node(path: &Path, sources: &SourceMap, diags: &mut Diagnostics) -> Option<Node> {
    let text = match std::fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) => {
            diags.error(
                "io",
                ConfigError::new(format!("reading {}: {e}", path.display())),
            );
            return None;
        }
    };
    let file: Arc<Path> = Arc::from(path);
    sources.insert(file.clone(), Arc::from(text.as_str()));
    match parse_yaml(&text, file) {
        Ok(n) => Some(n),
        Err(e) => {
            diags.error("yaml", e);
            None
        }
    }
}

pub fn valid_stream_id(id: &str) -> bool {
    !id.is_empty()
        && id
            .bytes()
            .all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'/' | b'-'))
        && !id.starts_with('/')
        && !id.ends_with('/')
        && !id.contains("//")
}

pub fn valid_op_name(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn valid_replication_name(s: &str) -> bool {
    s.bytes()
        .next()
        .is_some_and(|b| b.is_ascii_alphanumeric() || b == b'_')
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}

/// Loads a project. The result is best-effort: check `diags.has_errors()`.
pub fn load(root: &Path) -> (Project, Diagnostics) {
    let mut diags = Diagnostics::default();
    let root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let mut p = Project {
        root: root.clone(),
        ..Default::default()
    };

    let project_path = root.join(PROJECT_FILE);
    if !project_path.is_file() {
        diags.error(
            "project",
            ConfigError::new(format!("no {PROJECT_FILE} in {}", root.display())),
        );
        return (p, diags);
    }
    if let Some(node) = read_node(&project_path, &p.sources, &mut diags) {
        match from_node::<ProjectFile>(&node) {
            Ok(f) => {
                if f.version.value != 1 {
                    diags.error(
                        "version",
                        ConfigError::new(format!(
                            "unsupported config version {} (expected 1)",
                            f.version.value
                        ))
                        .at(f.version.span.clone()),
                    );
                }
                p.environments = f.environments;
                p.engine = f.engine.unwrap_or_default();
                p.state = f.state;
                p.defaults = f.defaults.map(|r| r.node);
                let conns_key = node.as_map().and_then(|m| m.get("connections"));
                for (name, raw) in f.connections {
                    let key_span = conns_key
                        .and_then(|c| c.value.as_map())
                        .and_then(|m| m.get(&name))
                        .map(|e| e.key_span.clone())
                        .unwrap_or_else(|| raw.node.span.clone());
                    p.connections.insert(
                        name.clone(),
                        RawConnection {
                            name,
                            key_span,
                            node: raw.node,
                        },
                    );
                }
            }
            Err(e) => diags.error("project", e),
        }
    }
    if let Some(d) = &p.defaults
        && !check_defaults(d, "project `defaults`", &mut diags)
    {
        // Reported once here rather than once per merged stream.
        p.defaults = None;
    }

    for path in yaml_files(&root, "apis", false, &mut diags) {
        if let Some(node) = read_node(&path, &p.sources, &mut diags) {
            let rel = rel_path(&root, &path);
            let spec = parse_api_spec(rel.clone(), &node, &mut diags);
            p.api_specs.insert(rel, spec);
        }
    }
    // Specs referenced from outside `apis/`.
    let referenced: Vec<(String, Span)> = p
        .connections
        .values()
        .filter(|c| c.node.get("type").and_then(Node::as_str) == Some("rest"))
        .filter_map(|c| {
            c.node
                .get("spec")
                .and_then(|s| Some((s.as_str()?.to_owned(), s.span.clone())))
        })
        .collect();
    for (spec, span) in referenced {
        if crate::template::is_template(&spec) {
            continue;
        }
        let rel = normalize_rel(&spec);
        if p.api_specs.contains_key(&rel) {
            continue;
        }
        let path = root.join(&rel);
        if !path.is_file() {
            diags.error(
                "api-spec",
                ConfigError::new(format!("API spec `{spec}` not found")).at(span),
            );
            continue;
        }
        if let Some(node) = read_node(&path, &p.sources, &mut diags) {
            let s = parse_api_spec(rel.clone(), &node, &mut diags);
            p.api_specs.insert(rel, s);
        }
    }

    let schemas_dir = root.join("schemas");
    for path in yaml_files(&root, "schemas", true, &mut diags) {
        let Some(node) = read_node(&path, &p.sources, &mut diags) else {
            continue;
        };
        let id = path
            .strip_prefix(&schemas_dir)
            .unwrap_or(&path)
            .with_extension("")
            .components()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join("/");
        match from_node::<SchemaFile>(&node) {
            Ok(f) => {
                p.schema_files.insert(
                    id,
                    SchemaDoc {
                        schema: f.schema,
                        file: Some(Arc::from(path.as_path())),
                    },
                );
            }
            Err(e) => diags.error("schema", e),
        }
    }

    let mut rep_spans: IndexMap<String, Span> = IndexMap::new();
    for path in yaml_files(&root, "replications", true, &mut diags) {
        let Some(node) = read_node(&path, &p.sources, &mut diags) else {
            continue;
        };
        let file = match from_node::<ReplicationsFile>(&node) {
            Ok(f) => f,
            Err(e) => {
                diags.error("replication", e);
                continue;
            }
        };
        let stem = path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let keys = node.get("replications").and_then(Node::as_map);
        for (name, raw) in file.replications {
            let key_span = keys
                .and_then(|m| m.get(&name))
                .map(|e| e.key_span.clone())
                .unwrap_or_else(|| raw.node.span.clone());
            if !valid_replication_name(&name) {
                diags.error(
                    "replication-name",
                    ConfigError::new(format!(
                        "invalid replication name `{name}` (letters, digits, `_`, `.`, `-`)"
                    ))
                    .at(key_span.clone()),
                );
            }
            if let Some(prev) = rep_spans.get(&name) {
                diags.error(
                    "duplicate-replication",
                    ConfigError::new(format!("replication `{name}` is defined twice"))
                        .at(key_span.clone())
                        .related("first defined", prev.clone()),
                );
                continue;
            }
            rep_spans.insert(name.clone(), key_span.clone());
            let cfg = match from_node::<ReplicationConfig>(&raw.node) {
                Ok(c) => c,
                Err(e) => {
                    diags.error("replication", e);
                    continue;
                }
            };
            if let Some(r) = build_replication(
                &p,
                Spanned::new(name, key_span),
                &stem,
                &path,
                cfg,
                &mut diags,
            ) {
                p.replications.push(r);
            }
        }
    }

    attach_schemas(&mut p, &mut diags);
    check_unique_ids(&p, &mut diags);
    (p, diags)
}

fn rel_path(root: &Path, p: &Path) -> String {
    p.strip_prefix(root)
        .unwrap_or(p)
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

pub fn normalize_rel(s: &str) -> String {
    s.trim_start_matches("./").to_owned()
}

fn check_defaults(node: &Node, what: &str, diags: &mut Diagnostics) -> bool {
    if let Some(id) = node.get("id") {
        diags.error(
            "defaults",
            ConfigError::new(format!("`id` cannot be set in {what}")).at(id.span.clone()),
        );
        return false;
    }
    match from_node::<StreamConfig>(node) {
        Ok(_) => true,
        Err(e) => {
            diags.error("defaults", e);
            false
        }
    }
}

fn single(key: &str, value: Node) -> Node {
    let span = value.span.clone();
    let mut m = Map::new();
    m.insert(
        key.to_owned(),
        Entry {
            key_span: span.clone(),
            value,
        },
    );
    Node::map(m, span)
}

fn build_replication(
    p: &Project,
    name: Spanned<String>,
    stem: &str,
    path: &Path,
    cfg: ReplicationConfig,
    diags: &mut Diagnostics,
) -> Option<Replication> {
    let op = cfg
        .op
        .clone()
        .unwrap_or_else(|| Spanned::new(stem.to_owned(), name.span.clone()));
    if !valid_op_name(&op.value) {
        diags.error(
            "op-name",
            ConfigError::new(format!(
                "op name `{}` must match [A-Za-z0-9_]+ (Dagster); set `op:` explicitly or rename the file",
                op.value
            ))
            .at(op.span.clone()),
        );
    }

    // Defaults chain: project defaults → replication `asset`/`retry` shorthand → replication defaults.
    let empty = Node::map(Map::new(), name.span.clone());
    let mut base = p.defaults.clone().unwrap_or_else(|| empty.clone());
    let mut ok = true;
    if let Some(a) = &cfg.asset {
        base = merge(&base, &single("asset", a.node.clone()));
    }
    if let Some(r) = &cfg.retry {
        base = merge(&base, &single("retry", r.node.clone()));
    }
    let shorthand = base.clone();
    if let Some(d) = &cfg.defaults {
        ok &= check_defaults(
            &d.node,
            &format!("replication `{}` defaults", name.value),
            diags,
        );
        base = merge(&base, &d.node);
    }

    let retry_node = {
        let project_retry = p.defaults.as_ref().and_then(|d| d.get("retry")).cloned();
        let rep_retry = cfg.retry.as_ref().map(|r| r.node.clone());
        match (project_retry, rep_retry) {
            (Some(a), Some(b)) => Some(merge(&a, &b)),
            (a, b) => a.or(b),
        }
    };
    let retry = match retry_node
        .as_ref()
        .map(from_node::<RetryConfig>)
        .transpose()
    {
        Ok(r) => r.unwrap_or_default(),
        Err(e) => {
            diags.error("retry", e);
            RetryConfig::default()
        }
    };
    if let Err(e) = crate::retry::RetryPolicy::from_config(&retry) {
        diags.error(
            "retry",
            ConfigError::new(e).at_opt(retry_node.as_ref().map(|n| &n.span)),
        );
    }

    let mut streams = Vec::new();
    if cfg.streams.is_empty() {
        diags.error(
            "streams",
            ConfigError::new(format!("replication `{}` has no streams", name.value))
                .at(name.span.clone()),
        );
    }
    build_streams(
        &cfg.streams,
        ok.then_some((&base, cfg.defaults.as_ref().map(|d| &d.node))),
        0,
        &mut streams,
        diags,
    );
    let mut hops = vec![Hop {
        parent: None,
        source: cfg.source.clone(),
        target: cfg.target.clone(),
    }];
    if let Some(then) = &cfg.then {
        build_hops(then, 0, &shorthand, &mut hops, &mut streams, diags);
    }

    Some(Replication {
        name,
        file: Arc::from(path),
        op,
        source: cfg.source,
        target: cfg.target,
        after: cfg.after,
        atomic_group: cfg.atomic_group,
        lock_group: cfg.lock_group,
        lock_timeout: cfg.lock_timeout,
        retry,
        replication_retries: cfg.replication_retries.unwrap_or(0),
        timeout: cfg.timeout,
        engine: cfg.engine,
        streams,
        hops,
        for_each: cfg.for_each,
        source_snapshot: cfg.source_snapshot,
    })
}

/// Streams merged over `base` (`None`: the defaults were invalid and already reported), with
/// `own` the hop's own `defaults`. A cdc stream drops ingest-only keys set only above `own`.
fn build_streams(
    raws: &[Raw<StreamConfig>],
    base: Option<(&Node, Option<&Node>)>,
    hop: usize,
    out: &mut Vec<Stream>,
    diags: &mut Diagnostics,
) {
    let set = |n: Option<&Node>, k: &str| n.and_then(|n| n.get(k)).is_some_and(|v| !v.is_null());
    for raw in raws {
        let mut node = match base {
            Some((b, _)) => merge(b, &raw.node),
            None => raw.node.clone(),
        };
        let mut config = match from_node::<StreamConfig>(&node) {
            Ok(c) => c,
            Err(e) => {
                diags.error("stream", e);
                continue;
            }
        };
        if let Some((_, own)) = base
            && config.mode.as_ref().map(|m| m.value) == Some(Mode::Cdc)
        {
            let inherited: Vec<&str> = StreamConfig::INGEST_ONLY
                .into_iter()
                .filter(|k| set(Some(&node), k) && !set(Some(&raw.node), k) && !set(own, k))
                .collect();
            if !inherited.is_empty() {
                node = node.take_keys(&inherited).0;
                config = match from_node::<StreamConfig>(&node) {
                    Ok(c) => c,
                    Err(e) => {
                        diags.error("stream", e);
                        continue;
                    }
                };
            }
        }
        let Some(id) = config.id.clone() else {
            diags.error(
                "stream-id",
                ConfigError::new("stream is missing `id`").at(raw.node.span.clone()),
            );
            continue;
        };
        if !valid_stream_id(&id.value) {
            diags.error(
                "stream-id",
                ConfigError::new(format!(
                    "invalid stream id `{}` (allowed: [a-z0-9_./-], no empty segments)",
                    id.value
                ))
                .at(id.span.clone()),
            );
        }
        let schema = config.schema.clone().map(|s| SchemaDoc {
            schema: s,
            file: None,
        });
        out.push(Stream {
            id,
            seq: out.len(),
            node,
            config,
            schema,
            hop,
        });
    }
}

/// `then:` entries below hop `parent`, depth-first so parents precede children.
fn build_hops(
    raws: &[Raw<HopConfig>],
    parent: usize,
    shorthand: &Node,
    hops: &mut Vec<Hop>,
    streams: &mut Vec<Stream>,
    diags: &mut Diagnostics,
) {
    for raw in raws {
        let cfg = match from_node::<HopConfig>(&raw.node) {
            Ok(c) => c,
            Err(e) => {
                diags.error("then", e);
                continue;
            }
        };
        let idx = hops.len();
        hops.push(Hop {
            parent: Some(parent),
            source: hops[parent].target.clone(),
            target: cfg.target.clone(),
        });
        if cfg.streams.is_empty() {
            diags.error(
                "streams",
                ConfigError::new(format!(
                    "`then` hop to `{}` has no streams",
                    cfg.target.value
                ))
                .at(raw.node.span.clone()),
            );
        }
        let mut base = shorthand.clone();
        let mut ok = true;
        if let Some(a) = &cfg.asset {
            base = merge(&base, &single("asset", a.node.clone()));
        }
        if let Some(d) = &cfg.defaults {
            ok &= check_defaults(
                &d.node,
                &format!("`then` hop to `{}` defaults", cfg.target.value),
                diags,
            );
            base = merge(&base, &d.node);
        }
        build_streams(
            &cfg.streams,
            ok.then_some((&base, cfg.defaults.as_ref().map(|d| &d.node))),
            idx,
            streams,
            diags,
        );
        if let Some(then) = &cfg.then {
            build_hops(then, idx, shorthand, hops, streams, diags);
        }
    }
}

fn attach_schemas(p: &mut Project, diags: &mut Diagnostics) {
    let mut used: Vec<String> = Vec::new();
    for r in &mut p.replications {
        for s in &mut r.streams {
            if let Some(doc) = p.schema_files.get(&s.id.value) {
                used.push(s.id.value.clone());
                match &s.schema {
                    Some(inline) => diags.error(
                        "schema-conflict",
                        ConfigError::new(format!(
                            "stream `{}` has both an inline `schema` and a schema file",
                            s.id.value
                        ))
                        .at(inline.schema.span.clone())
                        .related("schema file", doc.schema.span.clone()),
                    ),
                    None => s.schema = Some(doc.clone()),
                }
            }
        }
    }
    for (id, doc) in &p.schema_files {
        if !used.contains(id) {
            diags.warn(
                "schema-orphan",
                ConfigError::new(format!("source schema for `{id}` matches no stream"))
                    .at(doc.schema.span.clone()),
            );
        }
    }
}

fn check_unique_ids(p: &Project, diags: &mut Diagnostics) {
    let mut seen: IndexMap<&str, &Span> = IndexMap::new();
    for r in &p.replications {
        for s in &r.streams {
            if let Some(prev) = seen.get(s.id.value.as_str()) {
                diags.error(
                    "duplicate-stream",
                    ConfigError::new(format!("stream id `{}` is not unique", s.id.value))
                        .at(s.id.span.clone())
                        .related("first defined", (*prev).clone()),
                );
            } else {
                seen.insert(&s.id.value, &s.id.span);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::model::Mode;

    fn project(files: &[(&str, &str)]) -> (Project, Diagnostics, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tmp");
        for (path, text) in files {
            let p = dir.path().join(path);
            std::fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
            std::fs::write(p, text).expect("write");
        }
        let (p, d) = load(dir.path());
        (p, d, dir)
    }

    const PROJECT: &str = "version: 1\ndefaults:\n  retry: {max_attempts: 5, base: 2s}\n  on_empty: mirror\nconnections:\n  lake: {type: ducklake, catalog: {type: duckdb, path: cat.db}, data_path: /tmp/lake/}\n";

    #[test]
    fn defaults_chain_precedence() {
        let (p, d, _t) = project(&[
            ("atlatl.yaml", PROJECT),
            (
                "replications/warehouse_hourly.yaml",
                "replications:\n  est:\n    source: lake\n    target: lake\n    asset: {group: warehouse_hourly}\n    retry: {base: 5s}\n    defaults:\n      mode: incremental\n      object: \"br.{{ member.table }}\"\n      window: x >= 1\n    streams:\n      - id: a/b\n        from: {table: s.t}\n      - id: a/c\n        from: {table: s.u}\n        window: null\n        retry: {max_attempts: 2}\n        asset: {key: br/a/c}\n",
            ),
        ]);
        assert!(!d.has_errors(), "{d:?}");
        let r = &p.replications[0];
        assert_eq!(r.op.value, "warehouse_hourly");
        assert_eq!(
            (r.retry.max_attempts, r.retry.base.map(|b| b.to_string())),
            (Some(5), Some("5s".into()))
        );
        let [a, c] = &r.streams[..] else { panic!() };
        assert_eq!(
            a.config.mode.as_ref().map(|m| m.value),
            Some(Mode::Incremental)
        );
        assert_eq!(
            a.config.window.as_deref().map(String::as_str),
            Some("x >= 1")
        );
        assert!(c.config.window.is_none(), "null clears inherited");
        let cr = c.config.retry.as_ref().expect("retry");
        assert_eq!(
            (cr.max_attempts, cr.base.map(|b| b.to_string())),
            (Some(2), Some("5s".into()))
        );
        let ca = c.config.asset.as_ref().expect("asset");
        assert_eq!(
            (ca.group.as_deref(), ca.key.as_deref().map(String::as_str)),
            (Some("warehouse_hourly"), Some("br/a/c"))
        );
        assert_eq!(
            a.config.on_empty.as_ref().map(|o| o.value),
            Some(crate::config::model::OnEmpty::Mirror)
        );
    }

    #[test]
    fn clean_csv_naming_and_its_alias() {
        use crate::config::model::Naming;
        let (p, d, _t) = project(&[
            ("atlatl.yaml", PROJECT),
            (
                "replications/r.yaml",
                "replications:\n  r:\n    source: lake\n    target: lake\n    streams:\n      - id: a/b\n        from: {table: s.t}\n        naming: clean_csv\n      - id: a/c\n        from: {table: s.u}\n        naming: sling_csv\n",
            ),
        ]);
        assert!(!d.has_errors(), "{d:?}");
        let got: Vec<_> = p.replications[0]
            .streams
            .iter()
            .map(|s| s.config.naming)
            .collect();
        assert_eq!(got, [Some(Naming::CleanCsv), Some(Naming::CleanCsv)]);
    }

    #[test]
    fn errors_carry_file_and_line() {
        let (_, d, _t) = project(&[
            ("atlatl.yaml", PROJECT),
            (
                "replications/x.yaml",
                "replications:\n  x:\n    source: lake\n    target: lake\n    streams:\n      - id: a\n        mode: full-refresh\n        from: {table: s.t}\n        primary_kye: [id]\n",
            ),
        ]);
        let e = d.errors().next().expect("error");
        assert!(
            e.error.message.contains("unknown field `primary_kye`"),
            "{e:?}"
        );
        let s = e.error.span.as_ref().expect("span");
        assert!(s.file.ends_with("replications/x.yaml"));
        assert_eq!((s.line, s.col), (9, 9));
    }

    #[test]
    fn errors_in_defaults_point_at_defaults() {
        let (_, d, _t) = project(&[
            ("atlatl.yaml", "version: 1\ndefaults:\n  on_empty: mirrr\n"),
            (
                "replications/x.yaml",
                "replications:\n  x: {source: l, target: l, streams: [{id: a}, {id: b}]}\n",
            ),
        ]);
        let errs: Vec<_> = d.errors().collect();
        assert_eq!(errs.len(), 1, "reported once, not per stream: {errs:?}");
        assert!(
            errs[0]
                .error
                .span
                .as_ref()
                .expect("span")
                .file
                .ends_with("atlatl.yaml")
        );
    }

    #[test]
    fn duplicate_ids_and_replications() {
        let (_, d, _t) = project(&[
            ("atlatl.yaml", PROJECT),
            (
                "replications/a.yaml",
                "replications:\n  x: {source: lake, target: lake, streams: [{id: s1}]}\n",
            ),
            (
                "replications/b.yaml",
                "replications:\n  x: {source: lake, target: lake, streams: [{id: s2}]}\n  y: {source: lake, target: lake, streams: [{id: s1}]}\n",
            ),
        ]);
        let codes: Vec<_> = d.errors().map(|e| e.code).collect();
        assert!(codes.contains(&"duplicate-replication"), "{codes:?}");
        assert!(codes.contains(&"duplicate-stream"), "{codes:?}");
    }

    #[test]
    fn stream_id_rules_and_ops() {
        let (_, d, _t) = project(&[
            ("atlatl.yaml", PROJECT),
            (
                "replications/my-file.yaml",
                "replications:\n  x: {source: lake, target: lake, streams: [{id: Bad/ID}, {mode: cdc}]}\n",
            ),
        ]);
        let codes: Vec<_> = d.errors().map(|e| e.code).collect();
        assert!(codes.contains(&"stream-id"), "{codes:?}");
        assert!(codes.contains(&"op-name"), "{codes:?}");
        assert!(valid_stream_id("warehouse_hourly/est_v2/est_core_consf"));
        assert!(!valid_stream_id("a//b") && !valid_stream_id("/a") && !valid_stream_id(""));
    }

    #[test]
    fn schemas_and_apis_discovered() {
        let (p, d, _t) = project(&[
            (
                "atlatl.yaml",
                "version: 1\nconnections:\n  monthly: {type: rest, spec: apis/monthly.yaml}\n  lake: {type: ducklake, catalog: {type: duckdb, path: c}, data_path: /d/}\n",
            ),
            (
                "apis/monthly.yaml",
                "name: monthly\nendpoints:\n  calendar: {}\n",
            ),
            (
                "schemas/monthly/calendar.yaml",
                "schema:\n  fields:\n    Symbol: {type: string, nullable: false}\n",
            ),
            ("schemas/monthly/orphan.yaml", "schema:\n  fields: {}\n"),
            (
                "replications/monthly.yaml",
                "replications:\n  monthly: {source: monthly, target: lake, streams: [{id: monthly/calendar, from: {endpoint: calendar}, mode: full-refresh, object: br.e}]}\n",
            ),
        ]);
        assert!(!d.has_errors(), "{d:?}");
        assert!(p.api_specs.contains_key("apis/monthly.yaml"));
        let (_, s) = p.stream("monthly/calendar").expect("stream");
        assert!(s.schema.as_ref().and_then(|d| d.file.as_ref()).is_some());
        assert!(d.warnings().any(|w| w.code == "schema-orphan"));
    }

    #[test]
    fn missing_project_file() {
        let dir = tempfile::tempdir().expect("tmp");
        let (_, d) = load(dir.path());
        assert!(
            d.errors()
                .any(|e| e.error.message.contains("no atlatl.yaml"))
        );
    }

    #[test]
    fn then_hops_and_their_defaults() {
        let (p, d, _t) = project(&[
            ("atlatl.yaml", PROJECT),
            (
                "replications/x.yaml",
                "replications:\n  x:\n    source: lake\n    target: lake\n    asset: {group: g}\n    defaults: {mode: incremental}\n    streams: [{id: a}]\n    then:\n      - target: pg\n        defaults: {mode: cdc}\n        streams: [{id: b}]\n        then: [{target: pg2, streams: [{id: c, mode: cdc}]}]\n      - target: lake2\n        asset: {group: h}\n        streams: [{id: e, mode: full-refresh}]\n",
            ),
        ]);
        assert!(!d.has_errors(), "{d:?}");
        let r = &p.replications[0];
        let hops: Vec<_> = r
            .hops
            .iter()
            .map(|h| (h.parent, h.source.value.as_str(), h.target.value.as_str()))
            .collect();
        assert_eq!(
            hops,
            [
                (None, "lake", "lake"),
                (Some(0), "lake", "pg"),
                (Some(1), "pg", "pg2"),
                (Some(0), "lake", "lake2")
            ]
        );
        let got: Vec<_> = r
            .streams
            .iter()
            .map(|s| {
                (
                    s.id.value.as_str(),
                    s.hop,
                    s.config.mode.as_ref().map(|m| m.value),
                    s.config.asset.as_ref().and_then(|a| a.group.clone()),
                )
            })
            .collect();
        let g = |x: &str| Some(x.to_owned());
        assert_eq!(
            got,
            [
                ("a", 0, Some(Mode::Incremental), g("g")),
                ("b", 1, Some(Mode::Cdc), g("g")),
                ("c", 2, Some(Mode::Cdc), g("g")),
                ("e", 3, Some(Mode::FullRefresh), g("h")),
            ],
            "hops inherit the shorthand, not the replication defaults"
        );
        assert!(p.stream("c").is_some());
    }

    #[test]
    fn then_and_for_each_shape_errors() {
        let (_, d, _t) = project(&[
            ("atlatl.yaml", PROJECT),
            (
                "replications/x.yaml",
                "replications:\n  x:\n    source: lake\n    target: lake\n    then: [{target: pg, streams: []}, {target: pg, bogus: 1, streams: [{id: q}]}]\n    streams: [{id: a}]\n  y:\n    source: lake\n    target: lake\n    for_each: {over: {query: q}, as: u}\n    streams: [{id: b}]\n  z:\n    source: lake\n    target: lake\n    for_each: {over: [1, {k: 2}], as: u, key: k}\n    streams: [{id: c}]\n",
            ),
        ]);
        let msgs: Vec<String> = d
            .errors()
            .map(|e| format!("[{}] {}", e.code, e.error.message))
            .collect();
        assert!(
            msgs.iter()
                .any(|m| m.contains("[streams] `then` hop to `pg` has no streams")),
            "{msgs:?}"
        );
        assert!(
            msgs.iter()
                .any(|m| m.starts_with("[then]") && m.contains("bogus")),
            "{msgs:?}"
        );
        assert!(
            msgs.iter().any(|m| m.contains("connection")),
            "missing connection: {msgs:?}"
        );
        assert_eq!(msgs.len(), 3, "{msgs:?}");
    }
}
