//! Record extraction (07 §6): JMESPath over the decoded body (never templated), then `fields`,
//! `filter` and dedup by primary key across the run.
//!
//! JSON is converted to JMESPath variables by hand so numbers stay exact. `jmespath` objects are
//! `BTreeMap`s, so general expressions return keys sorted; plain paths (`[]`, `data.items`,
//! `companies[]`) are evaluated directly and keep the vendor's key order.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use atlatl_core::AtlatlError;
use atlatl_core::config::ConfigError;
use indexmap::IndexMap;
use jmespath::functions::{ArgumentType, CustomFunction, Signature};
use jmespath::{Context, Rcvar, Runtime, Variable};
use regex::Regex;

use crate::expr::{Engine, Scope, json_value};
use crate::json::{JValue, Num};
use crate::spec::{Dedup, Expr, RecordsSpec, ScalarAs};

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    let mut rt = Runtime::new();
    rt.register_builtin_functions();
    for (name, upper) in [("lower", false), ("upper", true)] {
        rt.register_function(
            name,
            Box::new(CustomFunction::new(
                Signature::new(vec![ArgumentType::String], None),
                Box::new(move |args: &[Rcvar], _: &mut Context<'_>| {
                    let s = args[0].as_string().cloned().unwrap_or_default();
                    Ok(Arc::new(Variable::String(if upper {
                        s.to_uppercase()
                    } else {
                        s.to_lowercase()
                    })))
                }),
            )),
        );
    }
    rt.register_function(
        "to_string",
        Box::new(CustomFunction::new(
            Signature::new(vec![ArgumentType::Any], None),
            Box::new(|args: &[Rcvar], _: &mut Context<'_>| {
                let s = match &*args[0] {
                    Variable::String(s) => s.clone(),
                    _ => EXACT.with(|e| from_var(&args[0], &e.borrow()).to_string()),
                };
                Ok(Arc::new(Variable::String(s)))
            }),
        )),
    );
    rt
});

thread_local! {
    /// The exact-number table of the search running on this thread, for `to_string`.
    static EXACT: std::cell::RefCell<Exact> = std::cell::RefCell::new(Exact::new());
}

/// Numbers whose text an f64/i64 `jmespath` number can't reproduce, keyed by node address.
/// `jmespath` hands input subtrees through as the same `Rcvar`s (field access, projections,
/// filters, multiselect, `merge`), so exact text is restored on the way out.
type Exact = HashMap<usize, Num>;

fn to_var(v: &JValue, exact: &mut Exact) -> Rcvar {
    match v {
        JValue::Number(n) => {
            let num = n
                .as_i64()
                .map(serde_json::Number::from)
                .or_else(|| n.as_u64().map(serde_json::Number::from))
                .or_else(|| serde_json::Number::from_f64(n.as_f64()));
            let lossless = num.as_ref().is_some_and(|x| x.to_string() == n.as_str());
            let rc = Arc::new(Variable::Number(num.unwrap_or_else(|| 0.into())));
            if !lossless {
                exact.insert(Arc::as_ptr(&rc) as usize, n.clone());
            }
            rc
        }
        JValue::Null => Arc::new(Variable::Null),
        JValue::Bool(b) => Arc::new(Variable::Bool(*b)),
        JValue::String(s) => Arc::new(Variable::String(s.clone())),
        JValue::Array(a) => Arc::new(Variable::Array(
            a.iter().map(|x| to_var(x, exact)).collect(),
        )),
        JValue::Object(m) => Arc::new(Variable::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), to_var(v, exact)))
                .collect::<BTreeMap<_, _>>(),
        )),
    }
}

fn from_var(v: &Rcvar, exact: &Exact) -> JValue {
    match &**v {
        Variable::Null | Variable::Expref(_) => JValue::Null,
        Variable::Bool(b) => JValue::Bool(*b),
        Variable::Number(n) => JValue::Number(
            exact
                .get(&(Arc::as_ptr(v) as usize))
                .cloned()
                .or_else(|| Num::parse(&n.to_string()))
                .unwrap_or_else(|| 0i64.into()),
        ),
        Variable::String(s) => JValue::String(s.clone()),
        Variable::Array(a) => JValue::Array(a.iter().map(|x| from_var(x, exact)).collect()),
        Variable::Object(m) => JValue::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), from_var(v, exact)))
                .collect(),
        ),
    }
}

#[derive(Debug)]
enum Path {
    /// `a.b.c` plus an optional trailing `[]`.
    Simple(Vec<String>, bool),
    Jmes(jmespath::Expression<'static>),
}

static SIMPLE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(?:[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)?(\[\])?$").expect("regex")
});

/// A compiled JMESPath query.
#[derive(Debug)]
pub struct Query(Path);

impl Query {
    pub fn compile(src: &Expr) -> Result<Self, AtlatlError> {
        Self::parse(src).map_err(|e| AtlatlError::config(e.to_string()))
    }

    pub fn parse(src: &Expr) -> Result<Self, ConfigError> {
        let s = src.value.trim();
        if s.is_empty() || s == "@" {
            return Ok(Query(Path::Simple(Vec::new(), false)));
        }
        if let Some(c) = SIMPLE.captures(s) {
            let flatten = c.get(1).is_some();
            let fields = s.trim_end_matches("[]");
            let fields = if fields.is_empty() {
                Vec::new()
            } else {
                fields.split('.').map(str::to_owned).collect()
            };
            return Ok(Query(Path::Simple(fields, flatten)));
        }
        RUNTIME
            .compile(s)
            .map(|e| Query(Path::Jmes(e)))
            .map_err(|e| ConfigError::new(format!("invalid JMESPath: {e}")).at(src.span.clone()))
    }

    pub fn search(&self, v: &JValue) -> Result<JValue, AtlatlError> {
        match &self.0 {
            Path::Simple(fields, flatten) => {
                let mut cur = v;
                for f in fields {
                    match cur.get(f) {
                        Some(x) if cur.is_object() => cur = x,
                        _ => return Ok(JValue::Null),
                    }
                }
                if !*flatten {
                    return Ok(cur.clone());
                }
                let Some(a) = cur.as_array() else {
                    return Ok(JValue::Null);
                };
                let mut out = Vec::with_capacity(a.len());
                for x in a {
                    match x {
                        JValue::Array(inner) => out.extend(inner.iter().cloned()),
                        other => out.push(other.clone()),
                    }
                }
                Ok(JValue::Array(out))
            }
            Path::Jmes(e) => {
                let mut ctx = Context::new(e.as_str(), &RUNTIME);
                let mut exact = Exact::new();
                let input = to_var(v, &mut exact);
                EXACT.with(|t| *t.borrow_mut() = exact);
                let out = jmespath::interpret(&input, e.as_ast(), &mut ctx);
                let exact = EXACT.with(|t| std::mem::take(&mut *t.borrow_mut()));
                out.map(|r| from_var(&r, &exact)).map_err(|err| {
                    AtlatlError::new(
                        atlatl_core::ErrorClass::Decode,
                        format!("JMESPath `{e}`: {err}"),
                    )
                })
            }
        }
    }
}

/// Stateful record pipeline for one endpoint run.
#[derive(Debug)]
pub struct Records {
    path: Query,
    spec: RecordsSpec,
    fields: IndexMap<String, Expr>,
    dedup: Dedup,
    seen: HashSet<String>,
    last: IndexMap<String, (u64, JValue)>,
    seq: u64,
    pub filtered: u64,
    pub duplicates: u64,
}

impl Records {
    pub fn new(spec: &RecordsSpec, fields: &IndexMap<String, Expr>) -> Result<Self, AtlatlError> {
        let default = Expr::new(
            "@".into(),
            atlatl_core::config::node::Span::synthetic("<records>"),
        );
        Ok(Self {
            path: Query::compile(spec.path.as_ref().unwrap_or(&default))?,
            spec: spec.clone(),
            fields: fields.clone(),
            dedup: spec.effective_dedup(),
            seen: HashSet::new(),
            last: IndexMap::new(),
            seq: 0,
            filtered: 0,
            duplicates: 0,
        })
    }

    /// Values the path yields, scalars included (setup steps collect them).
    pub fn extract_values(&self, body: &JValue) -> Result<Vec<JValue>, AtlatlError> {
        Ok(match self.path.search(body)? {
            JValue::Null => Vec::new(),
            JValue::Array(a) => a.into_iter().filter(|x| !x.is_null()).collect(),
            other => vec![other],
        })
    }

    /// Records the path yields for one body, before fields/filter/dedup.
    pub fn extract(&self, body: &JValue) -> Result<Vec<JValue>, AtlatlError> {
        let found = self.path.search(body)?;
        let items = match found {
            JValue::Null => Vec::new(),
            JValue::Array(a) => a.into_iter().filter(|x| !x.is_null()).collect(),
            other => vec![other],
        };
        items
            .into_iter()
            .map(|x| match (x, self.spec.scalar_as) {
                (o @ JValue::Object(_), _) => Ok(o),
                (s, Some(ScalarAs::Value)) => {
                    Ok(JValue::Object([("value".to_owned(), s)].into_iter().collect()))
                }
                (s, None) => Err(AtlatlError::data(format!(
                    "records path `{}` yielded a {} (`{}`); set `records.scalar_as: value` to wrap scalars",
                    self.spec.path.as_ref().map(|p| p.value.as_str()).unwrap_or("@"),
                    s.kind(),
                    crate::expr::plain(&json_value(Arc::new(s.clone())))
                        .chars()
                        .take(80)
                        .collect::<String>()
                ))
                .code("scalar_record")),
            })
            .collect()
    }

    /// Applies `fields`, `filter` and dedup. `scope` carries `response`, `item`, `state`, … but
    /// not `record`. Returns the records to emit now.
    pub fn process(
        &mut self,
        items: Vec<JValue>,
        engine: &Engine,
        scope: &Scope,
    ) -> Result<Vec<JValue>, AtlatlError> {
        let mut out = Vec::new();
        for mut rec in items {
            if !self.fields.is_empty() || self.spec.filter.is_some() {
                let mut s = scope.clone();
                for (k, e) in &self.fields {
                    s.set_json("record", Arc::new(rec.clone()));
                    let v = engine.eval_json(e, &s).map_err(|err| at(e, err))?;
                    if let JValue::Object(m) = &mut rec {
                        m.insert(k.clone(), v);
                    }
                }
                if let Some(f) = &self.spec.filter {
                    s.set_json("record", Arc::new(rec.clone()));
                    if !engine.eval_bool(f, &s).map_err(|err| at(f, err))? {
                        self.filtered += 1;
                        continue;
                    }
                }
            }
            match self.dedup {
                Dedup::None => out.push(rec),
                Dedup::First => {
                    if self.seen.insert(self.key(&rec)) {
                        out.push(rec);
                    } else {
                        self.duplicates += 1;
                    }
                }
                Dedup::Last => {
                    let k = self.key(&rec);
                    self.seq += 1;
                    if self.last.shift_remove(&k).is_some() {
                        self.duplicates += 1;
                    }
                    self.last.insert(k, (self.seq, rec));
                }
            }
        }
        Ok(out)
    }

    /// `dedup: last` holds records until the run ends; this releases them in arrival order of
    /// their last occurrence.
    pub fn finish(&mut self) -> Vec<JValue> {
        std::mem::take(&mut self.last)
            .into_values()
            .map(|(_, r)| r)
            .collect()
    }

    fn key(&self, rec: &JValue) -> String {
        let vals: Vec<&JValue> = self
            .spec
            .primary_key
            .iter()
            .map(|k| rec.get(k).unwrap_or(&JValue::Null))
            .collect();
        vals.iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join("\u{1f}")
    }
}

fn at(e: &Expr, err: AtlatlError) -> AtlatlError {
    AtlatlError::new(err.class, format!("{}: {}", e.span, err.message))
}

#[cfg(test)]
mod tests {
    macro_rules! jv {
        ($($t:tt)+) => { JValue::from(serde_json::json!($($t)+)) };
    }

    use atlatl_core::config::node::Span;

    use super::*;

    fn ex(s: &str) -> Expr {
        Expr::new(s.into(), Span::synthetic("t"))
    }

    fn body(s: &str) -> JValue {
        crate::json::parse_str(s).expect("json")
    }

    fn q(path: &str, b: &str) -> String {
        Query::compile(&ex(path))
            .expect("compile")
            .search(&body(b))
            .expect("search")
            .to_string()
    }

    #[test]
    fn paths_keep_exact_numbers_and_order() {
        let b = r#"{"companies": [{"z": 1, "cik": null, "id": 1.10}, {"z": 2, "cik": "9", "id": 123456789012345678901234567890}]}"#;
        assert_eq!(
            q("companies", b),
            r#"[{"z":1,"cik":null,"id":1.10},{"z":2,"cik":"9","id":123456789012345678901234567890}]"#
        );
        assert_eq!(q("companies[]", b), q("companies", b));
        assert_eq!(
            q("companies[?cik!=null].{id: id, z: z}", b),
            r#"[{"id":123456789012345678901234567890,"z":2}]"#
        );
        assert_eq!(q("[]", "[[1, 2], 3]"), "[1,2,3]");
        assert_eq!(q("@", "[1]"), "[1]");
        assert_eq!(q("a.b", r#"{"a": [1]}"#), "null");
        assert_eq!(
            q(
                "fundamentals[?fiscal_year <= `2014`].id",
                r#"{"fundamentals": [{"fiscal_year": 2013, "id": "a"}, {"fiscal_year": 2015, "id": "b"}]}"#
            ),
            r#"["a"]"#
        );
        assert!(Query::compile(&ex("a[?")).is_err());
    }

    #[test]
    fn category_merge_and_custom_functions() {
        let b = r#"{"data": {"open": {"rows": [{"dealId": "1"}]}, "closed": {"rows": null}, "cancelled": {"rows": [{"dealId": "2"}]}}}"#;
        assert_eq!(
            q(
                "[ data.open.rows[].merge(@, {category: 'open'}), data.closed.rows[].merge(@, {category: 'closed'}), data.cancelled.rows[].merge(@, {category: 'cancelled'}) ][]",
                b
            ),
            r#"[{"category":"open","dealId":"1"},{"category":"cancelled","dealId":"2"}]"#
        );
        assert_eq!(q("[].lower(name)", r#"[{"name": "AbC"}]"#), r#"["abc"]"#);
        assert_eq!(q("[].to_string(n)", r#"[{"n": 1.50}]"#), r#"["1.50"]"#);
    }

    fn records(spec: &str) -> Records {
        let n =
            atlatl_core::config::node::parse_yaml(spec, Arc::from(std::path::Path::new("t.yaml")))
                .expect("yaml");
        #[derive(serde::Deserialize)]
        struct W {
            records: RecordsSpec,
            #[serde(default)]
            fields: IndexMap<String, Expr>,
        }
        let w: W = atlatl_core::config::de::from_node(&n).expect("spec");
        Records::new(&w.records, &w.fields).expect("records")
    }

    #[test]
    fn shapes_and_scalars() {
        let r = records("records: {path: user}\n");
        assert_eq!(
            r.extract(&body(r#"{"user": {"a": 1}}"#))
                .expect("obj")
                .len(),
            1
        );
        assert!(r.extract(&body("{}")).expect("none").is_empty());
        let r = records("records: {path: \"[].Country\"}\n");
        let e = r
            .extract(&body(r#"[{"Country": "US"}]"#))
            .expect_err("scalar");
        assert_eq!(e.class, atlatl_core::ErrorClass::Data);
        assert!(e.message.contains("scalar_as"), "{e}");
        let r = records("records: {path: \"[].Country\", scalar_as: value}\n");
        assert_eq!(
            r.extract(&body(r#"[{"Country": "US"}, {"x": 1}]"#))
                .expect("wrapped"),
            vec![jv!({"value": "US"})]
        );
    }

    #[test]
    fn fields_filter_and_dedup() {
        let e = Engine::default();
        let mut s = Scope::new();
        s.set_json(
            "response",
            Arc::new(body(
                r#"{"json": {"fundamental": {"id": 99999999999999999999, "fy": 2014}}}"#,
            )),
        );
        let mut r = records(
            "records: {path: \"items\", primary_key: [fundamental_id, item], filter: 'record.value != none'}\nfields:\n  fundamental_id: response.json.fundamental.id\n  fy: response.json.fundamental.fy\n",
        );
        let page = body(
            r#"{"items": [{"item": "a", "value": 1.0}, {"item": "a", "value": 2}, {"item": "b", "value": null}, {"item": "c", "value": 3}]}"#,
        );
        let out = r.process(r.extract(&page).expect("x"), &e, &s).expect("p");
        assert_eq!(
            JValue::Array(out).to_string(),
            r#"[{"item":"a","value":1.0,"fundamental_id":99999999999999999999,"fy":2014},{"item":"c","value":3,"fundamental_id":99999999999999999999,"fy":2014}]"#
        );
        assert_eq!((r.filtered, r.duplicates), (1, 1));
        let again = r.process(r.extract(&page).expect("x"), &e, &s).expect("p");
        assert!(again.is_empty(), "dedup spans pages");

        let mut last = records("records: {path: \"[]\", primary_key: [id], dedup: last}\n");
        let p1 = last
            .process(
                last.extract(&body(r#"[{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]"#))
                    .expect("x"),
                &e,
                &s,
            )
            .expect("p");
        let p2 = last
            .process(
                last.extract(&body(r#"[{"id": 1, "v": "c"}]"#)).expect("x"),
                &e,
                &s,
            )
            .expect("p");
        assert!(p1.is_empty() && p2.is_empty());
        assert_eq!(
            last.finish(),
            vec![jv!({"id": 2, "v": "b"}), jv!({"id": 1, "v": "c"})]
        );

        let mut none = records("records: {path: \"[]\", primary_key: [id], dedup: none}\n");
        assert_eq!(
            none.process(
                none.extract(&body(r#"[{"id": 1}, {"id": 1}]"#)).expect("x"),
                &e,
                &s
            )
            .expect("p")
            .len(),
            2
        );
    }
}
