//! YAML parsed into a spanned tree. Deep merge happens here, before typed deserialization, so every
//! value keeps the file and line it came from.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::sync::Arc;

use indexmap::IndexMap;
use saphyr_parser::{Event, Parser, ScalarStyle, Tag};

use super::diag::ConfigError;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Span {
    pub file: Arc<Path>,
    pub line: u32,
    pub col: u32,
}

impl Span {
    pub fn new(file: Arc<Path>, line: usize, col: usize) -> Self {
        Self {
            file,
            line: line as u32,
            col: col as u32,
        }
    }

    /// A span for values synthesised by atlatl rather than read from a file.
    pub fn synthetic(what: &str) -> Self {
        Self {
            file: Arc::from(Path::new(what)),
            line: 0,
            col: 0,
        }
    }
}

impl Default for Span {
    fn default() -> Self {
        Span::synthetic("<unknown>")
    }
}

impl fmt::Display for Span {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.line == 0 {
            write!(f, "{}", self.file.display())
        } else {
            write!(f, "{}:{}:{}", self.file.display(), self.line, self.col)
        }
    }
}

#[derive(Clone, Debug)]
pub struct Node {
    pub value: Value,
    pub span: Span,
}

#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Scalar(Scalar),
    Seq(Vec<Node>),
    Map(Map),
}

/// Scalars are resolved lazily against the type being deserialized; `quoted` scalars are always
/// strings.
#[derive(Clone, Debug, PartialEq)]
pub struct Scalar {
    pub text: String,
    pub quoted: bool,
}

pub type Map = IndexMap<String, Entry>;

#[derive(Clone, Debug)]
pub struct Entry {
    pub key_span: Span,
    pub value: Node,
}

impl Node {
    pub fn null(span: Span) -> Self {
        Self {
            value: Value::Null,
            span,
        }
    }

    pub fn string(s: impl Into<String>, span: Span) -> Self {
        Self {
            value: Value::Scalar(Scalar {
                text: s.into(),
                quoted: true,
            }),
            span,
        }
    }

    pub fn map(map: Map, span: Span) -> Self {
        Self {
            value: Value::Map(map),
            span,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self.value, Value::Null)
    }

    pub fn as_map(&self) -> Option<&Map> {
        match &self.value {
            Value::Map(m) => Some(m),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match &self.value {
            Value::Scalar(s) => Some(&s.text),
            _ => None,
        }
    }

    pub fn get(&self, key: &str) -> Option<&Node> {
        self.as_map()?.get(key).map(|e| &e.value)
    }

    pub fn kind(&self) -> &'static str {
        match &self.value {
            Value::Null => "null",
            Value::Scalar(_) => "scalar",
            Value::Seq(_) => "sequence",
            Value::Map(_) => "map",
        }
    }

    /// Splits a map node into (the rest, the removed entries).
    pub fn take_keys(&self, keys: &[&str]) -> (Node, IndexMap<String, Entry>) {
        let Some(map) = self.as_map() else {
            return (self.clone(), IndexMap::new());
        };
        let mut rest = Map::new();
        let mut taken = IndexMap::new();
        for (k, e) in map {
            if keys.contains(&k.as_str()) {
                taken.insert(k.clone(), e.clone());
            } else {
                rest.insert(k.clone(), e.clone());
            }
        }
        (Node::map(rest, self.span.clone()), taken)
    }

    /// Visits every scalar string in the tree, depth first.
    pub fn visit_strings<'a>(&'a self, f: &mut impl FnMut(&'a str, &'a Span)) {
        match &self.value {
            Value::Scalar(s) => f(&s.text, &self.span),
            Value::Seq(items) => items.iter().for_each(|n| n.visit_strings(f)),
            Value::Map(m) => m.values().for_each(|e| e.value.visit_strings(f)),
            Value::Null => {}
        }
    }

    /// Rewrites every scalar string in place.
    pub fn try_map_strings<E>(
        &mut self,
        f: &mut impl FnMut(&str, &Span) -> Result<Option<String>, E>,
    ) -> Result<(), E> {
        let span = self.span.clone();
        match &mut self.value {
            Value::Scalar(s) => {
                if let Some(new) = f(&s.text, &span)? {
                    s.text = new;
                    s.quoted = true;
                }
            }
            Value::Seq(items) => {
                for n in items {
                    n.try_map_strings(f)?;
                }
            }
            Value::Map(m) => {
                for e in m.values_mut() {
                    e.value.try_map_strings(f)?;
                }
            }
            Value::Null => {}
        }
        Ok(())
    }

    /// A plain JSON rendering (used for config hashes and plan output).
    pub fn to_json(&self) -> serde_json::Value {
        match &self.value {
            Value::Null => serde_json::Value::Null,
            Value::Scalar(s) => s.resolve_json(),
            Value::Seq(items) => {
                serde_json::Value::Array(items.iter().map(Node::to_json).collect())
            }
            Value::Map(m) => serde_json::Value::Object(
                m.iter()
                    .map(|(k, e)| (k.clone(), e.value.to_json()))
                    .collect(),
            ),
        }
    }
}

impl Scalar {
    pub fn as_bool(&self) -> Option<bool> {
        if self.quoted {
            return None;
        }
        match self.text.as_str() {
            "true" | "True" | "TRUE" => Some(true),
            "false" | "False" | "FALSE" => Some(false),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        if self.quoted {
            return None;
        }
        let t = self.text.replace('_', "");
        let (neg, digits) = match t.strip_prefix('-') {
            Some(d) => (true, d),
            None => (false, t.strip_prefix('+').unwrap_or(&t)),
        };
        let v = if let Some(h) = digits.strip_prefix("0x") {
            i64::from_str_radix(h, 16).ok()?
        } else if let Some(o) = digits.strip_prefix("0o") {
            i64::from_str_radix(o, 8).ok()?
        } else if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) {
            digits.parse().ok()?
        } else {
            return None;
        };
        Some(if neg { -v } else { v })
    }

    pub fn as_u64(&self) -> Option<u64> {
        if self.quoted {
            return None;
        }
        let t = self.text.replace('_', "");
        let t = t.strip_prefix('+').unwrap_or(&t);
        if !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit()) {
            t.parse().ok()
        } else {
            None
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        if self.quoted {
            return None;
        }
        match self.text.as_str() {
            ".inf" | ".Inf" | ".INF" | "+.inf" => return Some(f64::INFINITY),
            "-.inf" | "-.Inf" | "-.INF" => return Some(f64::NEG_INFINITY),
            ".nan" | ".NaN" | ".NAN" => return Some(f64::NAN),
            _ => {}
        }
        let t = self.text.replace('_', "");
        let looks_numeric = t
            .bytes()
            .all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'e' | b'E' | b'+' | b'-'))
            && t.bytes().any(|b| b.is_ascii_digit());
        if looks_numeric { t.parse().ok() } else { None }
    }

    fn resolve_json(&self) -> serde_json::Value {
        if let Some(b) = self.as_bool() {
            return b.into();
        }
        if let Some(i) = self.as_i64() {
            return i.into();
        }
        if let Some(f) = self.as_f64().and_then(serde_json::Number::from_f64) {
            return serde_json::Value::Number(f);
        }
        serde_json::Value::String(self.text.clone())
    }
}

/// Deep merge: maps merge per key, a `null` override removes the inherited key, anything else
/// replaces. The merged map takes the override's span.
pub fn merge(base: &Node, over: &Node) -> Node {
    match (&base.value, &over.value) {
        (Value::Map(b), Value::Map(o)) => {
            let mut out = Map::new();
            for (k, be) in b {
                match o.get(k) {
                    Some(oe) if oe.value.is_null() => {}
                    Some(oe) => {
                        out.insert(
                            k.clone(),
                            Entry {
                                key_span: oe.key_span.clone(),
                                value: merge(&be.value, &oe.value),
                            },
                        );
                    }
                    None => {
                        out.insert(k.clone(), be.clone());
                    }
                }
            }
            for (k, oe) in o {
                if !b.contains_key(k) && !oe.value.is_null() {
                    out.insert(
                        k.clone(),
                        Entry {
                            key_span: oe.key_span.clone(),
                            value: strip_nulls(&oe.value),
                        },
                    );
                }
            }
            Node::map(out, over.span.clone())
        }
        _ => strip_nulls(over),
    }
}

fn strip_nulls(n: &Node) -> Node {
    match &n.value {
        Value::Map(m) => Node::map(
            m.iter()
                .filter(|(_, e)| !e.value.is_null())
                .map(|(k, e)| {
                    (
                        k.clone(),
                        Entry {
                            key_span: e.key_span.clone(),
                            value: strip_nulls(&e.value),
                        },
                    )
                })
                .collect(),
            n.span.clone(),
        ),
        _ => n.clone(),
    }
}

/// Parses one YAML document. Empty input is `null`.
pub fn parse_yaml(text: &str, file: Arc<Path>) -> Result<Node, ConfigError> {
    parse(text, file, false)
}

/// As [`parse_yaml`], but resolving YAML 1.1 merge keys as PyYAML (and so dbt) does: `<<` takes a
/// map or a list of maps, explicit keys win, and earlier maps in a list win over later ones.
/// For files atlatl reads but doesn't own, such as dbt's profiles.yml.
pub fn parse_yaml_with_merge_keys(text: &str, file: Arc<Path>) -> Result<Node, ConfigError> {
    parse(text, file, true)
}

fn parse(text: &str, file: Arc<Path>, merge_keys: bool) -> Result<Node, ConfigError> {
    let mut b = Builder {
        anchors: HashMap::new(),
        stack: Vec::new(),
        root: None,
        docs: 0,
        merge_keys,
    };
    for ev in Parser::new_from_str(text) {
        let (ev, span) = ev.map_err(|e| {
            ConfigError::new(format!("invalid YAML: {}", e.info())).at(Span::new(
                file.clone(),
                e.marker().line(),
                e.marker().col() + 1,
            ))
        })?;
        b.event(
            ev,
            Span::new(file.clone(), span.start.line(), span.start.col() + 1),
        )?;
    }
    Ok(b.root.unwrap_or_else(|| Node::null(Span::new(file, 1, 1))))
}

enum Frame {
    Seq {
        items: Vec<Node>,
        span: Span,
        anchor: usize,
    },
    Map {
        map: Map,
        pending: Option<(String, Span)>,
        /// `<<` values, in order, applied at the end of the map.
        merges: Vec<Node>,
        span: Span,
        anchor: usize,
    },
}

struct Builder {
    anchors: HashMap<usize, Node>,
    stack: Vec<Frame>,
    root: Option<Node>,
    docs: usize,
    merge_keys: bool,
}

impl Builder {
    fn event(&mut self, ev: Event<'_>, span: Span) -> Result<(), ConfigError> {
        match ev {
            Event::StreamStart | Event::StreamEnd | Event::Nothing | Event::DocumentEnd => Ok(()),
            Event::DocumentStart(_) => {
                self.docs += 1;
                if self.docs > 1 {
                    return Err(ConfigError::new(
                        "multiple YAML documents in one file are not supported",
                    )
                    .at(span));
                }
                Ok(())
            }
            Event::Alias(id) => {
                let node = self
                    .anchors
                    .get(&id)
                    .cloned()
                    .ok_or_else(|| ConfigError::new("unknown YAML alias").at(span.clone()))?;
                self.push(Node {
                    value: node.value,
                    span,
                })
            }
            Event::Scalar(text, style, anchor, tag) => {
                let node = scalar_node(text, style, tag.as_deref(), span)?;
                if anchor > 0 {
                    self.anchors.insert(anchor, node.clone());
                }
                self.push(node)
            }
            Event::SequenceStart(anchor, tag) => {
                reject_tag(tag.as_deref(), &span)?;
                self.stack.push(Frame::Seq {
                    items: Vec::new(),
                    span,
                    anchor,
                });
                Ok(())
            }
            Event::MappingStart(anchor, tag) => {
                reject_tag(tag.as_deref(), &span)?;
                self.stack.push(Frame::Map {
                    map: Map::new(),
                    pending: None,
                    merges: Vec::new(),
                    span,
                    anchor,
                });
                Ok(())
            }
            Event::SequenceEnd => match self.stack.pop() {
                Some(Frame::Seq {
                    items,
                    span,
                    anchor,
                }) => {
                    let node = Node {
                        value: Value::Seq(items),
                        span,
                    };
                    if anchor > 0 {
                        self.anchors.insert(anchor, node.clone());
                    }
                    self.push(node)
                }
                _ => Err(ConfigError::new("unbalanced YAML sequence").at(span)),
            },
            Event::MappingEnd => match self.stack.pop() {
                Some(Frame::Map {
                    mut map,
                    merges,
                    span,
                    anchor,
                    ..
                }) => {
                    for m in &merges {
                        for (k, e) in m.as_map().into_iter().flatten() {
                            if !map.contains_key(k) {
                                map.insert(k.clone(), e.clone());
                            }
                        }
                    }
                    let node = Node::map(map, span);
                    if anchor > 0 {
                        self.anchors.insert(anchor, node.clone());
                    }
                    self.push(node)
                }
                _ => Err(ConfigError::new("unbalanced YAML mapping").at(span)),
            },
        }
    }

    fn push(&mut self, node: Node) -> Result<(), ConfigError> {
        match self.stack.last_mut() {
            None => {
                self.root = Some(node);
                Ok(())
            }
            Some(Frame::Seq { items, .. }) => {
                items.push(node);
                Ok(())
            }
            Some(Frame::Map {
                map,
                pending,
                merges,
                ..
            }) => match pending.take() {
                None => {
                    let key = match &node.value {
                        Value::Scalar(s) => s.text.clone(),
                        _ => return Err(ConfigError::new("map keys must be strings").at(node.span)),
                    };
                    if key == "<<" && !self.merge_keys {
                        return Err(ConfigError::new("YAML merge keys (`<<`) are not supported")
                            .at(node.span));
                    }
                    if let Some(prev) = map.get(&key) {
                        return Err(ConfigError::new(format!(
                            "duplicate key `{key}` (first defined at {})",
                            prev.key_span
                        ))
                        .at(node.span));
                    }
                    *pending = Some((key, node.span));
                    Ok(())
                }
                Some((key, _)) if key == "<<" => {
                    match node.value {
                        Value::Map(_) => merges.push(node),
                        Value::Seq(items) if items.iter().all(|i| i.as_map().is_some()) => {
                            merges.extend(items)
                        }
                        _ => {
                            return Err(ConfigError::new(
                                "a merge key (`<<`) takes a map or a list of maps",
                            )
                            .at(node.span));
                        }
                    }
                    Ok(())
                }
                Some((key, key_span)) => {
                    map.insert(
                        key,
                        Entry {
                            key_span,
                            value: node,
                        },
                    );
                    Ok(())
                }
            },
        }
    }
}

fn reject_tag(tag: Option<&Tag>, span: &Span) -> Result<(), ConfigError> {
    match tag {
        None => Ok(()),
        Some(t) if t.is_yaml_core_schema() && matches!(t.suffix.as_str(), "map" | "seq") => Ok(()),
        Some(t) => Err(ConfigError::new(format!(
            "unsupported YAML tag `{}{}`",
            t.handle, t.suffix
        ))
        .at(span.clone())),
    }
}

fn scalar_node(
    text: Cow<'_, str>,
    style: ScalarStyle,
    tag: Option<&Tag>,
    span: Span,
) -> Result<Node, ConfigError> {
    let plain = matches!(style, ScalarStyle::Plain);
    if let Some(t) = tag {
        if !t.is_yaml_core_schema() {
            return Err(ConfigError::new(format!(
                "unsupported YAML tag `{}{}`",
                t.handle, t.suffix
            ))
            .at(span));
        }
        return match t.suffix.as_str() {
            "str" => Ok(Node::string(text.into_owned(), span)),
            "null" => Ok(Node::null(span)),
            "int" | "float" | "bool" => Ok(Node {
                value: Value::Scalar(Scalar {
                    text: text.into_owned(),
                    quoted: false,
                }),
                span,
            }),
            other => Err(ConfigError::new(format!("unsupported YAML tag `!!{other}`")).at(span)),
        };
    }
    if plain && matches!(text.as_ref(), "" | "~" | "null" | "Null" | "NULL") {
        return Ok(Node::null(span));
    }
    Ok(Node {
        value: Value::Scalar(Scalar {
            text: text.into_owned(),
            quoted: !plain,
        }),
        span,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Node {
        parse_yaml(s, Arc::from(Path::new("t.yaml"))).expect("parse")
    }

    #[test]
    fn spans_are_one_based() {
        let n = parse("a:\n  b: 1\n");
        let b = n.get("a").and_then(|a| a.get("b")).expect("b");
        assert_eq!((b.span.line, b.span.col), (2, 6));
        let key = &n.get("a").and_then(Node::as_map).expect("map")["b"].key_span;
        assert_eq!((key.line, key.col), (2, 3));
    }

    #[test]
    fn plain_nulls_and_quoted_strings() {
        let n = parse("a: ~\nb: null\nc: 'null'\nd:\ne: \"1\"\n");
        assert!(n.get("a").expect("a").is_null());
        assert!(n.get("b").expect("b").is_null());
        assert!(n.get("d").expect("d").is_null());
        assert_eq!(n.get("c").and_then(Node::as_str), Some("null"));
        let Value::Scalar(e) = &n.get("e").expect("e").value else {
            panic!()
        };
        assert!(e.quoted && e.as_i64().is_none());
    }

    #[test]
    fn duplicate_keys_fail() {
        let e = parse_yaml("a: 1\na: 2\n", Arc::from(Path::new("t.yaml"))).expect_err("dup");
        assert!(e.message.contains("duplicate key `a`"), "{e}");
        assert_eq!(e.span.as_ref().map(|s| s.line), Some(2));
    }

    #[test]
    fn merge_keys_resolve_like_pyyaml_when_asked() {
        let f: Arc<Path> = Arc::from(Path::new("t.yaml"));
        let m = |y: &str| parse_yaml_with_merge_keys(y, f.clone()).map(|n| n.to_json());
        let y = "d: &d {type: duckdb, threads: 4, path: a.db}\nx: {<<: *d, path: b.db}\ny: {path: c.db, <<: *d}\n";
        let j = m(y).expect("merge");
        assert_eq!(
            j["x"],
            serde_json::json!({"path": "b.db", "type": "duckdb", "threads": 4})
        );
        assert_eq!(
            j["y"]["path"], "c.db",
            "explicit keys win wherever they appear"
        );
        let j = m("a: &a {k: 1, a: 1}\nb: &b {k: 2, b: 2}\nc: {<<: [*a, *b]}\n").expect("list");
        assert_eq!(
            j["c"],
            serde_json::json!({"k": 1, "a": 1, "b": 2}),
            "earlier maps win"
        );
        let e = m("a: {<<: 1}\n").expect_err("scalar");
        assert!(e.message.contains("map or a list of maps"), "{e:?}");
    }

    #[test]
    fn merge_keys_and_multi_docs_fail() {
        let f: Arc<Path> = Arc::from(Path::new("t.yaml"));
        assert!(parse_yaml("a: &x {b: 1}\nc:\n  <<: *x\n", f.clone()).is_err());
        assert!(parse_yaml("a: 1\n---\nb: 2\n", f.clone()).is_err());
        assert!(parse_yaml("a: !custom 1\n", f).is_err());
    }

    #[test]
    fn aliases_expand() {
        let n = parse("a: &x [1, 2]\nb: *x\n");
        assert_eq!(n.get("b").expect("b").to_json(), serde_json::json!([1, 2]));
    }

    #[test]
    fn scalar_resolution() {
        let s = |t: &str| Scalar {
            text: t.into(),
            quoted: false,
        };
        assert_eq!(s("0x1f").as_i64(), Some(31));
        assert_eq!(s("-1_000").as_i64(), Some(-1000));
        assert_eq!(s("1.5e3").as_f64(), Some(1500.0));
        assert_eq!(s("True").as_bool(), Some(true));
        assert_eq!(s("yes").as_bool(), None);
        assert_eq!(s("2s").as_i64(), None);
    }

    #[test]
    fn deep_merge_semantics() {
        let base =
            parse("retry: {max_attempts: 5, base: 2s}\ntags: [a, b]\nwindow: x > 1\nkeep: 1\n");
        let over = parse("retry: {base: 5s}\ntags: [c]\nwindow: null\nnew: {a: null, b: 2}\n");
        let m = merge(&base, &over).to_json();
        assert_eq!(
            m,
            serde_json::json!({
                "retry": {"max_attempts": 5, "base": "5s"},
                "tags": ["c"],
                "keep": 1,
                "new": {"b": 2}
            })
        );
    }

    #[test]
    fn merged_values_keep_origin_spans() {
        let f1: Arc<Path> = Arc::from(Path::new("a.yaml"));
        let f2: Arc<Path> = Arc::from(Path::new("b.yaml"));
        let base = parse_yaml("retry: {max_attempts: 5}\n", f1).expect("a");
        let over = parse_yaml("retry: {base: 5s}\n", f2).expect("b");
        let m = merge(&base, &over);
        let r = m.get("retry").expect("retry");
        assert_eq!(
            &*r.get("max_attempts").expect("m").span.file,
            Path::new("a.yaml")
        );
        assert_eq!(&*r.get("base").expect("b").span.file, Path::new("b.yaml"));
    }
}
