//! Request-phase templates and expressions (02 §5, 07 §9).
//!
//! JSON ([`JValue`], exact numbers) enters minijinja through [`Json`], a lazy view over an `Arc`'d
//! tree. Plain path expressions (`response.json.company.id`, `record`) are resolved on the JSON
//! itself, so they keep exact numbers; computed expressions go through minijinja's i64/i128/f64.
//!
//! Undefined behaviour is chainable so that `response.json.a.b.c` on a sparse body is `none`;
//! unknown roots and secret/state/sync keys are rejected up front by [`Engine::undeclared`].

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, LazyLock};

use atlatl_core::AtlatlError;
use minijinja::value::{Enumerator, Kwargs, Object, ObjectRepr, Rest, ValueKind};
use minijinja::{Environment, Error, ErrorKind, UndefinedBehavior, Value};
use regex::Regex;

use crate::funcs::{self, Clock};
use crate::json::{JValue, Map, Num};

#[derive(Clone, Debug)]
enum Seg {
    Key(String),
    Idx(usize),
}

/// A lazily converted view of `root` at `path`.
#[derive(Debug)]
pub struct Json {
    root: Arc<JValue>,
    path: Vec<Seg>,
}

impl Json {
    pub fn value(&self) -> &JValue {
        let mut cur = &*self.root;
        for s in &self.path {
            cur = match (s, cur) {
                (Seg::Key(k), JValue::Object(m)) => &m[k.as_str()],
                (Seg::Idx(i), JValue::Array(a)) => &a[*i],
                _ => unreachable!("Json paths are built from existing children"),
            };
        }
        cur
    }

    fn child(&self, seg: Seg) -> Value {
        let mut path = self.path.clone();
        path.push(seg);
        let j = Json {
            root: self.root.clone(),
            path,
        };
        let v = j.value();
        if v.is_object() || v.is_array() {
            Value::from_object(j)
        } else {
            scalar(v)
        }
    }
}

impl Object for Json {
    fn repr(self: &Arc<Self>) -> ObjectRepr {
        if self.value().is_array() {
            ObjectRepr::Seq
        } else {
            ObjectRepr::Map
        }
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match self.value() {
            JValue::Object(m) => {
                let k = key.as_str()?;
                // Missing keys are `none`, like a JSON null, so `x == none` works on sparse bodies.
                Some(if m.contains_key(k) {
                    self.child(Seg::Key(k.to_owned()))
                } else {
                    Value::from(())
                })
            }
            JValue::Array(a) => {
                let i = i64::try_from(key.clone()).ok()?;
                let i = if i < 0 { a.len() as i64 + i } else { i };
                (0..a.len() as i64)
                    .contains(&i)
                    .then(|| self.child(Seg::Idx(i as usize)))
            }
            _ => None,
        }
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        match self.value() {
            JValue::Object(m) => {
                Enumerator::Values(m.keys().map(|k| Value::from(k.as_str())).collect())
            }
            JValue::Array(a) => Enumerator::Seq(a.len()),
            _ => Enumerator::Empty,
        }
    }

    fn is_true(self: &Arc<Self>) -> bool {
        match self.value() {
            JValue::Object(m) => !m.is_empty(),
            JValue::Array(a) => !a.is_empty(),
            _ => true,
        }
    }
}

fn scalar(v: &JValue) -> Value {
    match v {
        JValue::Null => Value::from(()),
        JValue::Bool(b) => Value::from(*b),
        JValue::Number(n) => number(n),
        JValue::String(s) => Value::from(s.as_str()),
        _ => json_value(Arc::new(v.clone())),
    }
}

fn number(n: &Num) -> Value {
    if let Some(i) = n.as_i64() {
        return Value::from(i);
    }
    if let Some(u) = n.as_u64() {
        return Value::from(u);
    }
    if let Some(i) = n.as_i128() {
        return Value::from(i);
    }
    Value::from(n.as_f64())
}

/// Wraps an owned JSON tree for minijinja.
pub fn json_value(root: Arc<JValue>) -> Value {
    if root.is_object() || root.is_array() {
        Value::from_object(Json {
            root,
            path: Vec::new(),
        })
    } else {
        scalar(&root)
    }
}

/// minijinja → JSON. [`Json`] views return their exact subtree.
pub fn to_json(v: &Value) -> JValue {
    if let Some(j) = v.downcast_object_ref::<Json>() {
        return j.value().clone();
    }
    if let Some(d) = v.downcast_object_ref::<funcs::Date>() {
        return JValue::String(d.to_string());
    }
    match v.kind() {
        ValueKind::Undefined | ValueKind::None => JValue::Null,
        ValueKind::Bool => JValue::Bool(v.is_true()),
        ValueKind::Number => {
            if v.is_integer() {
                if let Ok(i) = i64::try_from(v.clone()) {
                    return i.into();
                }
                if let Ok(u) = u64::try_from(v.clone()) {
                    return u.into();
                }
                if let Ok(i) = i128::try_from(v.clone()) {
                    return JValue::Number(i.into());
                }
            }
            f64::try_from(v.clone())
                .ok()
                .and_then(Num::from_f64)
                .map(JValue::Number)
                .unwrap_or(JValue::Null)
        }
        ValueKind::String => JValue::String(v.as_str().unwrap_or_default().to_owned()),
        ValueKind::Bytes => JValue::String(funcs::hex(v.as_bytes().unwrap_or_default())),
        ValueKind::Map => {
            let mut m = Map::new();
            if let Ok(keys) = v.try_iter() {
                for k in keys {
                    let val = v.get_item(&k).unwrap_or_default();
                    let key = k
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| k.to_string());
                    m.insert(key, to_json(&val));
                }
            }
            JValue::Object(m)
        }
        ValueKind::Seq | ValueKind::Iterable => JValue::Array(
            v.try_iter()
                .map(|it| it.map(|x| to_json(&x)).collect())
                .unwrap_or_default(),
        ),
        _ => JValue::String(v.to_string()),
    }
}

/// Text form used when a value is interpolated into a string.
pub fn plain(v: &Value) -> String {
    match v.kind() {
        ValueKind::Undefined | ValueKind::None => String::new(),
        ValueKind::Bool => if v.is_true() { "true" } else { "false" }.to_owned(),
        ValueKind::String => v.as_str().unwrap_or_default().to_owned(),
        ValueKind::Bytes => funcs::hex(v.as_bytes().unwrap_or_default()),
        ValueKind::Map | ValueKind::Seq | ValueKind::Iterable
            if v.downcast_object_ref::<funcs::Date>().is_none() =>
        {
            to_json(v).to_string()
        }
        _ => {
            if v.downcast_object_ref::<Json>().is_some() {
                to_json(v).to_string()
            } else {
                v.to_string()
            }
        }
    }
}

/// Names bound for one evaluation.
#[derive(Clone, Debug, Default)]
pub struct Scope {
    vars: BTreeMap<String, Value>,
}

impl Scope {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&mut self, name: &str, v: Value) -> &mut Self {
        self.vars.insert(name.to_owned(), v);
        self
    }

    pub fn set_json(&mut self, name: &str, v: Arc<JValue>) -> &mut Self {
        self.set(name, json_value(v))
    }

    pub fn with(mut self, name: &str, v: Value) -> Self {
        self.set(name, v);
        self
    }

    pub fn get(&self, name: &str) -> Option<&Value> {
        self.vars.get(name)
    }

    fn value(&self) -> Value {
        Value::from(self.vars.clone())
    }
}

static SIMPLE_PATH: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*|\[\d+\]|\['[^']*'\]|\["[^"]*"\])*$"#,
    )
    .expect("regex")
});
static SEGMENT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"\.?([A-Za-z_][A-Za-z0-9_]*)|\[(\d+)\]|\['([^']*)'\]|\["([^"]*)"\]"#)
        .expect("regex")
});

const LITERALS: &[&str] = &["true", "false", "none", "True", "False", "None", "null"];

fn exact_lookup(scope: &Scope, src: &str) -> Option<JValue> {
    let src = src.trim();
    if !SIMPLE_PATH.is_match(src) || LITERALS.contains(&src) {
        return None;
    }
    let mut segs = SEGMENT.captures_iter(src).map(|c| {
        if let Some(m) = c.get(1).or(c.get(3)).or(c.get(4)) {
            Seg::Key(m.as_str().to_owned())
        } else {
            Seg::Idx(c[2].parse().unwrap_or(usize::MAX))
        }
    });
    let Some(Seg::Key(root)) = segs.next() else {
        return None;
    };
    let mut cur = scope.get(&root)?.clone();
    let segs: Vec<Seg> = segs.collect();
    for (i, s) in segs.iter().enumerate() {
        if let Some(j) = cur.downcast_object_ref::<Json>() {
            let mut v = j.value();
            for s in &segs[i..] {
                v = match (s, v) {
                    (Seg::Key(k), JValue::Object(m)) => match m.get(k) {
                        Some(x) => x,
                        None => return Some(JValue::Null),
                    },
                    (Seg::Idx(i), JValue::Array(a)) => match a.get(*i) {
                        Some(x) => x,
                        None => return Some(JValue::Null),
                    },
                    _ => return Some(JValue::Null),
                };
            }
            return Some(v.clone());
        }
        cur = match s {
            Seg::Key(k) => cur.get_attr(k).ok()?,
            Seg::Idx(i) => cur.get_item(&Value::from(*i)).ok()?,
        };
        if cur.is_undefined() || cur.is_none() {
            return Some(JValue::Null);
        }
    }
    Some(to_json(&cur))
}

/// `{{ expr }}` as the whole value (optionally with whitespace control), returning `expr`.
pub fn whole_expression(s: &str) -> Option<&str> {
    let t = s.trim();
    let inner = t.strip_prefix("{{")?.strip_suffix("}}")?;
    if inner.contains("{{") || inner.contains("}}") || t.contains("{%") || t.contains("{#") {
        return None;
    }
    let inner = inner.strip_prefix(['-', '+']).unwrap_or(inner);
    let inner = inner.strip_suffix(['-', '+']).unwrap_or(inner);
    Some(inner.trim())
}

pub fn is_template(s: &str) -> bool {
    s.contains("{{") || s.contains("{%") || s.contains("{#")
}

fn err(src: &str, e: &Error) -> AtlatlError {
    let msg = match e.detail() {
        Some(d) => format!("{}: {d}", e.kind()),
        None => e.kind().to_string(),
    };
    AtlatlError::config(format!("{msg} in `{src}`"))
}

pub struct Engine {
    env: Environment<'static>,
    path_env: Environment<'static>,
    functions: BTreeSet<&'static str>,
    clock: Clock,
}

impl Default for Engine {
    fn default() -> Self {
        Self::new(funcs::system_clock())
    }
}

impl Engine {
    pub fn new(clock: Clock) -> Self {
        let mut env = atlatl_core::template::environment(UndefinedBehavior::Chainable);
        let functions = register(&mut env, clock.clone());
        let mut path_env = env.clone();
        env.set_formatter(|out, _state, v| out.write_str(&plain(v)).map_err(Error::from));
        path_env.set_formatter(|out, _state, v| {
            let s = plain(v);
            if v.is_safe() {
                out.write_str(&s)
            } else {
                out.write_str(&funcs::urlencode_str(&s))
            }
            .map_err(Error::from)
        });
        Self {
            env,
            path_env,
            functions,
            clock,
        }
    }

    pub fn now(&self) -> chrono::DateTime<chrono::Utc> {
        (self.clock)()
    }

    pub fn eval(&self, src: &str, scope: &Scope) -> Result<Value, AtlatlError> {
        let e = self.env.compile_expression(src).map_err(|e| err(src, &e))?;
        e.eval(scope.value()).map_err(|e| err(src, &e))
    }

    /// Evaluates to JSON; plain paths are resolved exactly.
    pub fn eval_json(&self, src: &str, scope: &Scope) -> Result<JValue, AtlatlError> {
        if let Some(v) = exact_lookup(scope, src) {
            return Ok(v);
        }
        self.eval(src, scope).map(|v| to_json(&v))
    }

    pub fn eval_bool(&self, src: &str, scope: &Scope) -> Result<bool, AtlatlError> {
        self.eval(src, scope).map(|v| v.is_true())
    }

    pub fn render(&self, src: &str, scope: &Scope) -> Result<String, AtlatlError> {
        if !is_template(src) {
            return Ok(src.to_owned());
        }
        let t = self.env.template_from_str(src).map_err(|e| err(src, &e))?;
        t.render(scope.value()).map_err(|e| err(src, &e))
    }

    /// Renders a URL or path: interpolations are percent-encoded unless piped through `safe`.
    pub fn render_path(&self, src: &str, scope: &Scope) -> Result<String, AtlatlError> {
        if !is_template(src) {
            return Ok(src.to_owned());
        }
        let t = self
            .path_env
            .template_from_str(src)
            .map_err(|e| err(src, &e))?;
        t.render(scope.value()).map_err(|e| err(src, &e))
    }

    /// Renders a template tree. A whole-value template keeps its type; `null` (and `""` unless
    /// `keep_empty`) map entries are dropped. `None` means the value itself is omitted.
    pub fn render_value(
        &self,
        t: &serde_json::Value,
        scope: &Scope,
        keep_empty: bool,
    ) -> Result<Option<JValue>, AtlatlError> {
        let v = match t {
            serde_json::Value::String(s) => match whole_expression(s) {
                Some(e) => self.eval_json(e, scope)?,
                None => JValue::String(self.render(s, scope)?),
            },
            serde_json::Value::Array(a) => JValue::Array(
                a.iter()
                    .map(|x| {
                        self.render_value(x, scope, true)
                            .map(|v| v.unwrap_or(JValue::Null))
                    })
                    .collect::<Result<_, _>>()?,
            ),
            serde_json::Value::Object(m) => {
                let mut out = Map::new();
                for (k, x) in m {
                    if let Some(v) = self.render_value(x, scope, keep_empty)? {
                        out.insert(k.clone(), v);
                    }
                }
                JValue::Object(out)
            }
            other => JValue::from(other),
        };
        Ok(match &v {
            JValue::Null => None,
            JValue::String(s) if s.is_empty() && !keep_empty => None,
            _ => Some(v),
        })
    }

    /// Root-qualified names (`secrets.api_key`, `page`) a template or expression reads, excluding
    /// functions. Syntax errors are config errors.
    pub fn undeclared(&self, src: &str, expression: bool) -> Result<BTreeSet<String>, AtlatlError> {
        let names = if expression {
            self.env
                .compile_expression(src)
                .map_err(|e| err(src, &e))?
                .undeclared_variables(true)
        } else if is_template(src) {
            self.env
                .template_from_str(src)
                .map_err(|e| err(src, &e))?
                .undeclared_variables(true)
        } else {
            return Ok(BTreeSet::new());
        };
        Ok(names
            .into_iter()
            .filter(|n| !self.functions.contains(n.split('.').next().unwrap_or(n)))
            .collect())
    }
}

fn register(env: &mut Environment<'static>, clock: Clock) -> BTreeSet<&'static str> {
    let c = clock.clone();
    env.add_function("now", move || Value::from_object(funcs::Date(c())));
    macro_rules! both {
        ($name:literal, $f:expr) => {
            env.add_function($name, $f);
            env.add_filter($name, $f);
        };
    }
    both!("date", |v: Value| funcs::to_date(&v).map(funcs::date_value));
    both!("date_parse", |v: Value, fmt: String| funcs::date_parse(
        &v, &fmt
    )
    .map(funcs::date_value));
    both!("date_format", |v: Value, fmt: String| funcs::date_format(
        &v, &fmt
    ));
    both!("date_add", |v: Value, n: i64, unit: String| {
        funcs::date_add(&v, n, &unit).map(funcs::date_value)
    });
    both!("date_trunc", |v: Value, unit: String| funcs::date_trunc(
        &v, &unit
    )
    .map(funcs::date_value));
    env.add_function(
        "range",
        |state: &minijinja::State<'_, '_>, start: Value, end: Value, step: Option<Value>| {
            let (start, end) = crate::iterate::range_bounds(state, start, end);
            funcs::range(start, end, step)
        },
    );
    env.add_function("coalesce", |args: Rest<Value>| {
        args.iter()
            .find(|v| !v.is_undefined() && !v.is_none())
            .cloned()
            .unwrap_or(Value::from(()))
    });
    env.add_function("first_nonempty", |args: Rest<Value>| {
        args.iter()
            .find(|v| !funcs::is_empty(v))
            .cloned()
            .unwrap_or(Value::from(()))
    });
    both!("urlencode", |v: Value| Value::from(funcs::urlencode_str(
        &plain(&v)
    )));
    both!("tojson", |v: Value, kw: Kwargs| -> Result<Value, Error> {
        let sort: Option<bool> = kw.get("sort_keys")?;
        let indent: Option<usize> = kw.get("indent")?;
        kw.assert_all_used()?;
        let mut j = to_json(&v);
        if sort.unwrap_or(false) {
            j = j.sort_keys();
        }
        Ok(Value::from(match indent {
            Some(_) => j.to_pretty(),
            None => j.to_string(),
        }))
    });
    both!("fromjson", |s: String| -> Result<Value, Error> {
        crate::json::parse_str(&s)
            .map(|j| json_value(Arc::new(j)))
            .map_err(|e| Error::new(ErrorKind::InvalidOperation, format!("fromjson: {e}")))
    });
    env.add_filter(
        "default",
        |v: Value, d: Option<Value>, boolean: Option<bool>| {
            let missing =
                v.is_undefined() || v.is_none() || (boolean == Some(true) && !v.is_true());
            if missing {
                d.unwrap_or(Value::from(""))
            } else {
                v
            }
        },
    );
    env.add_function("hmac", funcs::hmac_fn);
    both!("sha256", |v: Value| Value::from_bytes(funcs::sha256(
        &funcs::bytes_of(&v)
    )));
    both!("base64", |v: Value| Value::from(funcs::base64(
        &funcs::bytes_of(&v)
    )));
    both!("hex", |v: Value| Value::from(funcs::hex(&funcs::bytes_of(
        &v
    ))));
    [
        "now",
        "date",
        "date_parse",
        "date_format",
        "date_add",
        "date_trunc",
        "range",
        "coalesce",
        "first_nonempty",
        "urlencode",
        "tojson",
        "fromjson",
        "hmac",
        "sha256",
        "base64",
        "hex",
    ]
    .into_iter()
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! json {
        ($($t:tt)+) => { JValue::from(serde_json::json!($($t)+)) };
    }

    fn engine() -> Engine {
        Engine::new(funcs::fixed_clock("2026-09-28T12:34:56Z"))
    }

    fn scope(pairs: &[(&str, JValue)]) -> Scope {
        let mut s = Scope::new();
        for (k, v) in pairs {
            s.set_json(k, Arc::new(v.clone()));
        }
        s
    }

    fn big() -> JValue {
        crate::json::parse_str(r#"{"id": 123456789012345678901234567890, "px": 1.10, "n": 7, "o": {"x": 0.30000000000000004440892098500626}}"#)
            .expect("json")
    }

    #[test]
    fn exact_numbers_through_paths() {
        let e = engine();
        let s = scope(&[("record", big())]);
        let get = |x| e.eval_json(x, &s).expect("eval").to_string();
        assert_eq!(get("record.id"), "123456789012345678901234567890");
        assert_eq!(get("record.px"), "1.10");
        assert_eq!(get("record['px']"), "1.10");
        assert_eq!(
            get("record.o"),
            r#"{"x":0.30000000000000004440892098500626}"#
        );
        assert_eq!(get("record.missing.deeper"), "null");
        assert_eq!(get("record.n + 1"), "8");
        assert_eq!(get("record.id + 1"), "123456789012345678901234567891");
        assert_eq!(
            get("record | tojson"),
            r#""{\"id\":123456789012345678901234567890,\"px\":1.10,\"n\":7,\"o\":{\"x\":0.30000000000000004440892098500626}}""#
        );
    }

    #[test]
    fn sparse_bodies_are_chainable() {
        let e = engine();
        let s = scope(&[("response", json!({"json": {"data": {}}}))]);
        assert!(
            e.eval_bool(r#"coalesce(response.json.data.cursor.next, "") == """#, &s)
                .expect("eval")
        );
        assert!(
            e.eval_bool("response.json.data.cursor == none", &s)
                .expect("eval")
        );
        assert!(!e.eval_bool("response.json.data.cursor", &s).expect("eval"));
        assert_eq!(
            e.render("[{{ response.json.nope }}]", &s).expect("render"),
            "[]"
        );
    }

    #[test]
    fn typed_whole_values_and_omission() {
        let e = engine();
        let s = scope(&[
            ("page", json!({"cursor": null, "n": 2})),
            ("item", json!({"sym": "A B"})),
        ]);
        let t = serde_json::json!({
            "n": "{{ page.n }}", "c": "{{ page.cursor }}", "e": "", "s": "x-{{ page.n }}",
            "l": "{{ [1, page.n] }}", "lit": 5, "nested": {"drop": "{{ none }}", "keep": true}
        });
        let out = e
            .render_value(&t, &s, false)
            .expect("render")
            .expect("some");
        assert_eq!(
            out,
            json!({"n": 2, "s": "x-2", "l": [1, 2], "lit": 5, "nested": {"keep": true}})
        );
        let kept = e
            .render_value(&serde_json::json!({"e": ""}), &s, true)
            .expect("render")
            .expect("some");
        assert_eq!(kept, json!({"e": ""}));
        assert_eq!(whole_expression("{{- x -}}"), Some("x"));
        assert_eq!(whole_expression("{{ a }}-{{ b }}"), None);
    }

    #[test]
    fn path_encoding_unless_safe() {
        let e = engine();
        let s = scope(&[("item", json!({"c": "United States/x", "p": "a/b"}))]);
        assert_eq!(
            e.render_path("/country/{{ item.c }}/{{ item.p | safe }}", &s)
                .expect("path"),
            "/country/United%20States%2Fx/a/b"
        );
        assert_eq!(
            e.render("{{ item.c | urlencode }}", &s).expect("r"),
            "United%20States%2Fx"
        );
    }

    #[test]
    fn undeclared_names() {
        let e = engine();
        let names = e
            .undeclared("coalesce(sync.floor, date('2015-01-01')) ~ secrets.k", true)
            .expect("ok");
        assert_eq!(
            names.into_iter().collect::<Vec<_>>(),
            ["secrets.k", "sync.floor"]
        );
        let names = e
            .undeclared("x/{{ item.a | urlencode }}{{ now() }}", false)
            .expect("ok");
        assert_eq!(names.into_iter().collect::<Vec<_>>(), ["item.a"]);
        assert!(e.undeclared("a ==", true).is_err());
    }

    #[test]
    fn misc_functions() {
        let e = engine();
        let s = scope(&[(
            "r",
            json!({"a": null, "b": "", "c": "x", "t": "slow down please"}),
        )]);
        let j = |x| e.eval_json(x, &s).expect(x);
        assert_eq!(j("coalesce(r.a, r.b, r.c)"), json!(""));
        assert_eq!(j("first_nonempty(r.a, r.b, r.c)"), json!("x"));
        assert_eq!(j("'slow down' in r.t"), json!(true));
        assert_eq!(j("'b' in ['a', 'b']"), json!(true));
        assert_eq!(j("r.a | default('d')"), json!("d"));
        assert_eq!(j("r.c | upper"), json!("X"));
        assert_eq!(j(r#"('{"k": 1.50}' | fromjson).k"#), json!(1.5));
        assert_eq!(
            e.eval_json(r#"'{"k": 1.50}' | fromjson"#, &s)
                .expect("x")
                .to_string(),
            r#"{"k":1.50}"#
        );
        assert_eq!(
            j("{'b': 1, 'a': [2]} | tojson(sort_keys=true)"),
            json!(r#"{"a":[2],"b":1}"#)
        );
        assert_eq!(
            j("'abc' | sha256 | hex"),
            json!("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        );
        assert_eq!(
            j("hmac('sha256', 'key', 'The quick brown fox jumps over the lazy dog') | hex"),
            json!("f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8")
        );
        assert_eq!(j("'hi' | base64"), json!("aGk="));
        assert_eq!(
            e.render("{{ r }}", &s).expect("r"),
            r#"{"a":null,"b":"","c":"x","t":"slow down please"}"#
        );
    }
}
