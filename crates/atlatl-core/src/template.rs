//! minijinja templating with strict undefined behaviour and per-phase namespaces (spec 02 §5).

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::path::Path;
use std::sync::Arc;

use minijinja::syntax::SyntaxConfig;
use minijinja::value::{Enumerator, Object, ObjectRepr, Serde};
use minijinja::{Environment, ErrorKind, State, UndefinedBehavior, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// Connections, `vars`, API spec paths.
    Load,
    /// `object`, `window`, `from.*`.
    Plan,
    /// Hooks: the plan namespace plus `staging` and `target`.
    Hook,
}

impl Phase {
    pub fn name(self) -> &'static str {
        match self {
            Phase::Load => "load",
            Phase::Plan => "plan",
            Phase::Hook => "hook",
        }
    }

    /// Root names available in this phase, with their known attributes (`None` = any).
    fn roots(self) -> Vec<(&'static str, Option<&'static [&'static str]>)> {
        let mut r: Vec<(&'static str, Option<&'static [&'static str]>)> = vec![
            ("env", None),
            ("project_root", Some(&[])),
            ("environment", Some(&[])),
        ];
        if matches!(self, Phase::Plan | Phase::Hook) {
            r.extend([
                ("member", Some(&["schema", "table", "name"][..])),
                ("stream", Some(&["id", "vars"][..])),
                ("run", Some(&["id", "started_at"][..])),
                ("vars", None),
                ("bind", None),
            ]);
        }
        if self == Phase::Hook {
            r.extend([("staging", Some(&[][..])), ("target", Some(&[][..]))]);
        }
        r
    }
}

pub trait EnvSource: fmt::Debug + Send + Sync {
    fn var(&self, name: &str) -> Option<String>;
}

#[derive(Debug, Default)]
pub struct ProcessEnv;

impl EnvSource for ProcessEnv {
    fn var(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }
}

/// A fixed environment, for tests and `--static`.
#[derive(Debug, Default, Clone)]
pub struct MapEnv(pub HashMap<String, String>);

impl MapEnv {
    pub fn new<I: IntoIterator<Item = (K, V)>, K: Into<String>, V: Into<String>>(vars: I) -> Self {
        Self(
            vars.into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        )
    }
}

impl EnvSource for MapEnv {
    fn var(&self, name: &str) -> Option<String> {
        self.0.get(name).cloned()
    }
}

/// `env.NAME` (strict: undefined if unset) and `env("NAME", "default")`.
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

    fn call(
        self: &Arc<Self>,
        _: &mut State<'_, '_>,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        let (name, default) = match args {
            [n] => (n, None),
            [n, d] => (n, Some(d)),
            _ => {
                return Err(minijinja::Error::new(
                    ErrorKind::InvalidOperation,
                    "env() takes (name) or (name, default)",
                ));
            }
        };
        let name = name.as_str().ok_or_else(|| {
            minijinja::Error::new(ErrorKind::InvalidOperation, "env() name must be a string")
        })?;
        match (self.0.var(name), default) {
            (Some(v), _) => Ok(Value::from(v)),
            (None, Some(d)) => Ok(d.clone()),
            (None, None) => Err(minijinja::Error::new(
                ErrorKind::UndefinedError,
                format!("environment variable `{name}` is not set"),
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TemplateError {
    pub message: String,
}

impl fmt::Display for TemplateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for TemplateError {}

/// What `{{ bind.<name> }}` renders to at plan time; the runner replaces it with the listed
/// values (04 §12). A comment, so an unreplaced one fails as a syntax error.
pub fn bind_marker(name: &str) -> String {
    format!("/*atlatl:bind:{name}*/")
}

/// `update_key` placeholders in a clickhouse `from.query` (06 §11), filled by the runner with the
/// stored watermark's condition and value. Single braces, so templating leaves them alone.
pub const INCREMENTAL_WHERE_COND: &str = "{incremental_where_cond}";
pub const INCREMENTAL_VALUE: &str = "{incremental_value}";

/// Whether `query` reads the watermark through either placeholder.
pub fn uses_incremental_placeholders(query: &str) -> bool {
    query.contains(INCREMENTAL_WHERE_COND) || query.contains(INCREMENTAL_VALUE)
}

pub fn is_template(s: &str) -> bool {
    s.contains("{{") || s.contains("{%") || s.contains("{#")
}

/// Values bound for rendering. Build with the phase's constructors.
#[derive(Clone, Debug)]
pub struct Context {
    env: Arc<dyn EnvSource>,
    vars: BTreeMap<String, Value>,
}

impl Context {
    pub fn load(env: Arc<dyn EnvSource>, project_root: &Path, environment: Option<&str>) -> Self {
        let mut vars = BTreeMap::new();
        vars.insert(
            "project_root".into(),
            Value::from(project_root.display().to_string()),
        );
        vars.insert(
            "environment".into(),
            environment.map(Value::from).unwrap_or(Value::from(())),
        );
        Self { env, vars }
    }

    pub fn with(mut self, name: &str, value: Value) -> Self {
        self.vars.insert(name.into(), value);
        self
    }

    fn value(&self) -> Value {
        let mut m = self.vars.clone();
        m.insert(
            "env".into(),
            Value::from_object(EnvObject(self.env.clone())),
        );
        Value::from_pairs(m)
    }
}

pub struct Templates {
    env: Environment<'static>,
}

impl Default for Templates {
    fn default() -> Self {
        Self::new()
    }
}

/// The base for every template environment: trailing newlines kept.
pub fn environment(undefined: UndefinedBehavior) -> Environment<'static> {
    let mut env = Environment::new();
    env.set_undefined_behavior(undefined);
    env.set_syntax(
        SyntaxConfig::builder()
            .keep_trailing_newline(true)
            .build()
            .expect("default syntax"),
    );
    env
}

/// A serializable value as a template value; maps stay maps.
pub fn to_value<T: serde::Serialize>(v: T) -> Value {
    Value::from(Serde(v))
}

impl Templates {
    pub fn new() -> Self {
        Self {
            env: environment(UndefinedBehavior::Strict),
        }
    }

    /// Renders `src`. Non-template strings are returned unchanged.
    pub fn render(&self, src: &str, ctx: &Context) -> Result<String, TemplateError> {
        if !is_template(src) {
            return Ok(src.to_owned());
        }
        let tmpl = self.env.template_from_str(src).map_err(|e| TemplateError {
            message: describe(&e),
        })?;
        tmpl.render(ctx.value()).map_err(|e| {
            let mut msg = describe(&e);
            if e.kind() == ErrorKind::UndefinedError {
                let missing: Vec<_> = tmpl
                    .undeclared_variables(true)
                    .into_iter()
                    .filter_map(|v| v.strip_prefix("env.").map(str::to_owned))
                    .filter(|v| ctx.env.var(v).is_none())
                    .collect();
                if !missing.is_empty() {
                    msg = format!("environment variable `{}` is not set", missing.join("`, `"));
                }
            }
            TemplateError {
                message: format!("{msg} in template `{src}`"),
            }
        })
    }

    /// Syntax and namespace check without rendering. `vars` lists known `vars.*` keys, if known.
    pub fn check(
        &self,
        src: &str,
        phase: Phase,
        vars: Option<&[String]>,
    ) -> Result<(), TemplateError> {
        self.check_with(src, phase, vars, &[])
    }

    /// Root variable names `src` reads (empty for a non-template or a syntax error).
    pub fn roots_used(&self, src: &str) -> Vec<String> {
        if !is_template(src) {
            return Vec::new();
        }
        let Ok(t) = self.env.template_from_str(src) else {
            return Vec::new();
        };
        let mut out: Vec<String> = t.undeclared_variables(false).into_iter().collect();
        out.sort();
        out
    }

    /// Dotted variable paths `src` reads (`bind.series`); empty for a non-template or a syntax error.
    pub fn paths_used(&self, src: &str) -> Vec<String> {
        if !is_template(src) {
            return Vec::new();
        }
        let Ok(t) = self.env.template_from_str(src) else {
            return Vec::new();
        };
        let mut out: Vec<String> = t.undeclared_variables(true).into_iter().collect();
        out.sort();
        out
    }

    /// [`check`](Self::check) with extra roots of any shape (a replication `for_each` item).
    pub fn check_with(
        &self,
        src: &str,
        phase: Phase,
        vars: Option<&[String]>,
        extra: &[&str],
    ) -> Result<(), TemplateError> {
        if !is_template(src) {
            return Ok(());
        }
        let tmpl = self.env.template_from_str(src).map_err(|e| TemplateError {
            message: describe(&e),
        })?;
        let mut roots: Vec<(&str, Option<&[&str]>)> = phase.roots();
        roots.extend(extra.iter().map(|&r| (r, None)));
        let mut used: Vec<String> = tmpl.undeclared_variables(true).into_iter().collect();
        used.sort();
        for v in used {
            let (root, attr) = match v.split_once('.') {
                Some((r, a)) => (r, Some(a.split('.').next().unwrap_or(a))),
                None => (v.as_str(), None),
            };
            let Some((_, attrs)) = roots.iter().find(|(r, _)| *r == root) else {
                let names: Vec<_> = roots.iter().map(|(r, _)| *r).collect();
                return Err(TemplateError {
                    message: format!(
                        "`{root}` is not available in the {} phase (available: {}) in template `{src}`",
                        phase.name(),
                        names.join(", ")
                    ),
                });
            };
            let known: Option<Vec<&str>> = match (root, vars) {
                ("vars", Some(vs)) => Some(vs.iter().map(String::as_str).collect()),
                _ => attrs.map(|a| a.to_vec()),
            };
            if let (Some(attr), Some(known)) = (attr, known)
                && !known.contains(&attr)
            {
                return Err(TemplateError {
                    message: format!(
                        "`{root}.{attr}` is not defined (known: {}) in template `{src}`",
                        if known.is_empty() {
                            "none".to_owned()
                        } else {
                            known.join(", ")
                        }
                    ),
                });
            }
        }
        Ok(())
    }
}

fn describe(e: &minijinja::Error) -> String {
    match e.detail() {
        Some(d) => format!("{}: {d}", e.kind()),
        None => e.kind().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load_ctx(vars: &[(&str, &str)]) -> Context {
        Context::load(
            Arc::new(MapEnv::new(vars.iter().copied())),
            Path::new("/p"),
            Some("dev"),
        )
    }

    #[test]
    fn env_attribute_and_function() {
        let t = Templates::new();
        let c = load_ctx(&[("X", "1")]);
        assert_eq!(
            t.render("{{ env.X }}-{{ env('Y', 'd') }}-{{ environment }}", &c)
                .as_deref(),
            Ok("1-d-dev")
        );
        assert_eq!(
            t.render("{{ project_root }}/apis", &c).as_deref(),
            Ok("/p/apis")
        );
        assert_eq!(t.render("no template", &c).as_deref(), Ok("no template"));
    }

    #[test]
    fn strict_undefined_names_the_variable() {
        let t = Templates::new();
        let c = load_ctx(&[]);
        let e = t.render("s3://{{ env.BUCKET }}/x", &c).expect_err("strict");
        assert!(
            e.message
                .contains("environment variable `BUCKET` is not set"),
            "{e}"
        );
        let e = t.render("{{ env('BUCKET') }}", &c).expect_err("strict fn");
        assert!(e.message.contains("`BUCKET` is not set"), "{e}");
        let e = t.render("{{ member.table }}", &c).expect_err("undefined");
        assert!(e.message.contains("undefined"), "{e}");
        assert!(t.render("{{ ", &c).is_err());
    }

    #[test]
    fn plan_context_renders_members() {
        let t = Templates::new();
        let c = load_ctx(&[]).with(
            "member",
            Value::from_pairs([("schema", "est_v2"), ("table", "est_core_af")]),
        );
        assert_eq!(
            t.render("br.{{ member.table }}", &c).as_deref(),
            Ok("br.est_core_af")
        );
    }

    #[test]
    fn check_enforces_phase_namespaces() {
        let t = Templates::new();
        assert!(t.check("{{ env.X }}", Phase::Load, None).is_ok());
        let e = t
            .check("{{ member.table }}", Phase::Load, None)
            .expect_err("load");
        assert!(e.message.contains("not available in the load phase"), "{e}");
        assert!(t.check("x.{{ member.table }}", Phase::Plan, None).is_ok());
        let e = t
            .check("{{ member.tabel }}", Phase::Plan, None)
            .expect_err("attr");
        assert!(e.message.contains("`member.tabel` is not defined"), "{e}");
        let e = t
            .check("{{ sync.floor }}", Phase::Plan, None)
            .expect_err("sync");
        assert!(e.message.contains("`sync`"), "{e}");
        let vars = vec!["date".to_owned()];
        assert!(t.check("{{ vars.date }}", Phase::Plan, Some(&vars)).is_ok());
        assert!(t.check("{{ vars.dat }}", Phase::Plan, Some(&vars)).is_err());
        assert!(t.check("{{ staging }}", Phase::Plan, None).is_err());
        assert!(
            t.check(
                "insert into {{ target }} select * from {{ staging }}",
                Phase::Hook,
                None
            )
            .is_ok()
        );
        assert!(
            t.check("{% for x in [1] %}{{ x }}{% endfor %}", Phase::Load, None)
                .is_ok()
        );
    }
}
