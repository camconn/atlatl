//! Environments (spec 02 §2.1): selection, per-connection overrides, and the rule that a
//! connection may only fall back to its base definition when that is explicitly safe.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use indexmap::IndexMap;

use super::connection::{Connection, parse_connection};
use super::diag::{ConfigError, Diagnostics};
use super::load::{Project, RawConnection};
use super::model::{ConnectionCommon, ConnectionKind, EnvironmentsConfig};
use super::node::{Node, Scalar, Span, Value, merge};
use super::types::SecretValue;
use crate::redact::Redactor;
use crate::secret::resolve_in_place;
use crate::template::{Context, EnvSource, Phase, ProcessEnv, Templates, is_template};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct ActiveEnv {
    pub name: Option<String>,
    /// `flag`, `env:<VAR>`, `default`, or `none`.
    pub source: String,
}

/// `--env`, then `select_from_env`, then `default`.
pub fn select(
    envs: Option<&EnvironmentsConfig>,
    flag: Option<&str>,
    vars: &dyn EnvSource,
) -> Result<ActiveEnv, ConfigError> {
    let Some(envs) = envs else {
        return match flag {
            Some(f) => Err(ConfigError::new(format!(
                "--env {f} given, but the project defines no `environments`"
            ))),
            None => Ok(ActiveEnv {
                name: None,
                source: "none".into(),
            }),
        };
    };
    let (name, source) = if let Some(f) = flag {
        (Some(f.to_owned()), "flag".to_owned())
    } else if let Some(v) = envs
        .select_from_env
        .as_deref()
        .and_then(|var| vars.var(var).map(|val| (var, val)))
    {
        (Some(v.1), format!("env:{}", v.0))
    } else if let Some(d) = &envs.default {
        (Some(d.value.clone()), "default".to_owned())
    } else {
        (None, "none".to_owned())
    };
    let Some(name) = name else {
        return Err(ConfigError::new(
            "no environment selected: pass --env, set the `select_from_env` variable, or set `environments.default`",
        ));
    };
    if !envs.names.iter().any(|n| n.value == name) {
        let names: Vec<_> = envs.names.iter().map(|n| n.value.as_str()).collect();
        return Err(ConfigError::new(format!(
            "unknown environment `{name}` (from {source}; defined: {})",
            names.join(", ")
        )));
    }
    Ok(ActiveEnv {
        name: Some(name),
        source,
    })
}

/// Structural checks on `environments` and every connection's override block.
pub fn check_environments(p: &Project, diags: &mut Diagnostics) {
    let envs = p.environments.as_ref().map(|e| &e.value);
    if let Some(envs) = envs {
        let mut seen: Vec<&str> = Vec::new();
        for n in &envs.names {
            if seen.contains(&n.value.as_str()) {
                diags.error(
                    "environments",
                    ConfigError::new(format!("environment `{}` listed twice", n.value))
                        .at(n.span.clone()),
                );
            }
            seen.push(&n.value);
        }
        if envs.names.is_empty() {
            diags.error(
                "environments",
                ConfigError::new("`environments.names` is empty")
                    .at_opt(p.environments.as_ref().map(|e| &e.span)),
            );
        }
        for (label, v) in [("default", &envs.default), ("base", &envs.base)] {
            if let Some(v) = v
                && !seen.contains(&v.value.as_str())
            {
                diags.error(
                    "environments",
                    ConfigError::new(format!(
                        "`environments.{label}` names unknown environment `{}`",
                        v.value
                    ))
                    .at(v.span.clone()),
                );
            }
        }
        for (name, block) in &envs.per_env {
            if !seen.contains(&name.as_str()) {
                diags.error(
                    "environments",
                    ConfigError::new(format!(
                        "unknown key `{name}` in `environments` (not a listed environment name)"
                    ))
                    .at(block.span.clone()),
                );
            }
            for c in &block.value.allow_base {
                if !p.connections.contains_key(&c.value) {
                    diags.error(
                        "environments",
                        ConfigError::new(format!(
                            "`environments.{name}.allow_base` names unknown connection `{}`",
                            c.value
                        ))
                        .at(c.span.clone()),
                    );
                }
            }
        }
    }
    for c in p.connections.values() {
        let Some(block) = c.node.get("environments") else {
            continue;
        };
        let Some(m) = block.as_map() else {
            diags.error(
                "environments",
                ConfigError::new("`environments` must be a map of environment name to overrides")
                    .at(block.span.clone()),
            );
            continue;
        };
        for (env, e) in m {
            let known = envs.is_some_and(|x| x.names.iter().any(|n| &n.value == env));
            if !known {
                diags.error(
                    "environments",
                    ConfigError::new(format!(
                        "connection `{}` overrides unknown environment `{env}`",
                        c.name
                    ))
                    .at(e.key_span.clone()),
                );
            }
            if let Some(t) = e.value.get("type") {
                diags.error(
                    "environments",
                    ConfigError::new(format!(
                        "an environment override cannot change a connection's `type` ({})",
                        c.name
                    ))
                    .at(t.span.clone()),
                );
            }
        }
    }
}

/// The node for `c` in environment `env`, and where it came from.
pub fn connection_node(
    c: &RawConnection,
    env: Option<&str>,
    envs: Option<&EnvironmentsConfig>,
) -> Result<(Node, String), ConfigError> {
    let (base, taken) = c.node.take_keys(&["environments"]);
    let overrides = taken.get("environments").map(|e| &e.value);
    let Some(env) = env else {
        return Ok((base, "base".into()));
    };
    if let Some(block) = overrides.and_then(|o| o.get(env)) {
        return Ok((merge(&base, block), format!("environments.{env}")));
    }
    let envs = envs.ok_or_else(|| ConfigError::new("environments not configured"))?;
    let is_base_env = envs.base.as_ref().is_some_and(|b| b.value == env);
    let read_only = base.get("read_only").and_then(|n| match &n.value {
        super::node::Value::Scalar(s) => s.as_bool(),
        _ => None,
    }) == Some(true)
        || base.get("type").and_then(|n| n.as_str()) == Some("duckdb");
    let allowed = envs
        .per_env
        .get(env)
        .is_some_and(|b| b.value.allow_base.iter().any(|a| a.value == c.name));
    if is_base_env || read_only || allowed {
        return Ok((base, "base".into()));
    }
    Err(ConfigError::new(format!(
        "connection `{}` has no `{env}` override and would silently use its base definition; add `environments.{env}` to it, mark it `read_only: true`, list it in `environments.{env}.allow_base`, or set `environments.base: {env}` if the base definitions describe `{env}`",
        c.name
    ))
    .at(c.key_span.clone()))
}

/// How much of a connection to resolve.
pub enum Resolve<'a> {
    /// Structure only: no templates rendered, no secrets read.
    Static,
    /// Render load-phase templates and resolve secrets for `env`.
    Full {
        vars: Arc<dyn EnvSource>,
        redactor: &'a Redactor,
    },
}

pub fn resolve_connection(
    p: &Project,
    c: &RawConnection,
    env: Option<&str>,
    mode: &Resolve<'_>,
    templates: &Templates,
    diags: &mut Diagnostics,
) -> Option<Connection> {
    let envs = p.environments.as_ref().map(|e| &e.value);
    let (node, from) = match connection_node(c, env, envs) {
        Ok(x) => x,
        Err(e) => {
            diags.error("environment-base", e);
            return None;
        }
    };
    // Locating and choosing a dbt target reads the process environment even when static.
    let dbt_env: Arc<dyn EnvSource> = match mode {
        Resolve::Full { vars, .. } => vars.clone(),
        Resolve::Static => Arc::new(ProcessEnv),
    };
    let node = match super::dbt::expand(&node, &p.root, dbt_env, &p.sources) {
        Ok(n) => n,
        Err(e) => {
            diags.error("dbt", e);
            return None;
        }
    };
    // Parse unrendered first: structural errors, template syntax, and the literal-secret lint.
    let (common, mut kind) = match parse_templated(&node, None) {
        Ok(x) => x,
        Err(e) => {
            diags.error("connection", e);
            return None;
        }
    };
    let mut template_ok = true;
    node.visit_strings(&mut |s, span| {
        if let Err(e) = templates.check(s, Phase::Load, None) {
            diags.error("template", ConfigError::new(e.message).at(span.clone()));
            template_ok = false;
        }
    });
    let mut unrendered = Connection {
        name: c.name.clone(),
        span: c.key_span.clone(),
        common,
        kind: kind.clone(),
        resolved_from: from.clone(),
    };
    for (path, v) in unrendered.secrets_mut() {
        if let SecretValue::Literal(s) = v
            && !is_template(s)
        {
            diags.warn(
                    "literal-secret",
                    ConfigError::new(format!(
                        "connection `{}` has a literal `{path}`; prefer `{{secret: env:NAME}}` or a template",
                        c.name
                    ))
                    .at(c.key_span.clone()),
                );
        }
    }
    let Resolve::Full { vars, redactor } = mode else {
        return Some(unrendered);
    };
    if !template_ok {
        return None;
    }

    let ctx = Context::load(vars.clone(), &p.root, env);
    let mut rendered = node.clone();
    let mut failed = false;
    let mut spans = HashSet::new();
    let _ = rendered.try_map_strings::<()>(&mut |s, span| {
        if !is_template(s) {
            return Ok(None);
        }
        match templates.render(s, &ctx) {
            Ok(r) => {
                spans.insert(span.clone());
                Ok(Some(r))
            }
            Err(e) => {
                diags.error("template", ConfigError::new(e.message).at(span.clone()));
                failed = true;
                Ok(None)
            }
        }
    });
    if failed {
        return None;
    }
    let (common, k) = match parse_templated(&rendered, Some(&spans)) {
        Ok(x) => x,
        Err(e) => {
            diags.error("connection", e);
            return None;
        }
    };
    kind = k;
    let was_template: IndexMap<String, bool> = unrendered
        .secrets_mut()
        .into_iter()
        .map(|(path, v)| (path, matches!(v, SecretValue::Literal(s) if is_template(s))))
        .collect();
    let mut conn = Connection {
        name: c.name.clone(),
        span: c.key_span.clone(),
        common,
        kind,
        resolved_from: from,
    };
    let root: &Path = &p.root;
    for (path, v) in conn.secrets_mut() {
        let t = was_template.get(&path).copied().unwrap_or(false);
        if let Err(e) = resolve_in_place(v, t, vars.as_ref(), root, redactor) {
            diags.error(
                "secret",
                ConfigError::new(format!("connection `{}` {path}: {e}", c.name))
                    .at(c.key_span.clone()),
            );
            failed = true;
        }
    }
    (!failed).then_some(conn)
}

/// Parses a connection whose typed fields (ports, counts, durations, sizes, flags) may be
/// templates. Unrendered (`rendered: None`), a template a typed field rejects stands in as `0`
/// or `false`. Rendered, a field that rejects its rendered string reads it as a plain YAML
/// scalar, so `port: "{{ env.PG_PORT }}"` becomes an integer or fails at the template's span.
fn parse_templated(
    node: &Node,
    rendered: Option<&HashSet<Span>>,
) -> Result<(ConnectionCommon, ConnectionKind), ConfigError> {
    let mut err = match parse_connection(node) {
        Ok(x) => return Ok(x),
        Err(e) => e,
    };
    let mut n = node.clone();
    loop {
        let Some(span) = err.span.clone() else {
            return Err(err);
        };
        let Some(s) = scalar_at(&mut n, &span) else {
            return Err(err);
        };
        let templated = match rendered {
            None => is_template(&s.text),
            Some(r) => r.contains(&span),
        };
        if !s.quoted || !templated {
            return Err(err);
        }
        let original = s.clone();
        let stand_ins = match rendered {
            None => vec!["0".to_owned(), "false".to_owned()],
            Some(_) => vec![original.text.clone()],
        };
        let mut next = None;
        for text in stand_ins {
            *scalar_at(&mut n, &span).expect("scalar") = Scalar {
                text,
                quoted: false,
            };
            match parse_connection(&n) {
                Ok(x) => return Ok(x),
                Err(e) if e.span.as_ref() == Some(&span) => {}
                Err(e) => {
                    next = Some(e);
                    break;
                }
            }
        }
        err = match (next, rendered) {
            (Some(e), _) => e,
            (None, None) => return Err(err),
            (None, Some(_)) => {
                return Err(ConfigError::new(format!(
                    "template rendered to {:?}: {}",
                    original.text, err.message
                ))
                .at(span));
            }
        };
    }
}

fn scalar_at<'a>(n: &'a mut Node, span: &Span) -> Option<&'a mut Scalar> {
    let here = n.span == *span;
    match &mut n.value {
        Value::Scalar(s) if here => Some(s),
        Value::Seq(items) => items.iter_mut().find_map(|c| scalar_at(c, span)),
        Value::Map(m) => m.values_mut().find_map(|e| scalar_at(&mut e.value, span)),
        _ => None,
    }
}

/// Resolves every connection for `env`.
pub fn resolve_all(
    p: &Project,
    env: Option<&str>,
    mode: &Resolve<'_>,
    templates: &Templates,
    diags: &mut Diagnostics,
) -> IndexMap<String, Connection> {
    p.connections
        .values()
        .filter_map(|c| {
            resolve_connection(p, c, env, mode, templates, diags).map(|r| (c.name.clone(), r))
        })
        .collect()
}

/// All environments a static check must cover (or `[None]` without environments).
pub fn all_envs(p: &Project) -> Vec<Option<String>> {
    match &p.environments {
        Some(e) => e
            .value
            .names
            .iter()
            .map(|n| Some(n.value.clone()))
            .collect(),
        None => vec![None],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::load;
    use crate::config::model::ConnectionKind;
    use crate::template::MapEnv;

    fn project(atlatl: &str) -> (Project, Diagnostics, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tmp");
        std::fs::write(dir.path().join("atlatl.yaml"), atlatl).expect("write");
        std::fs::create_dir_all(dir.path().join("apis")).expect("mkdir");
        std::fs::write(dir.path().join("apis/v.yaml"), "endpoints: {}\n").expect("write");
        let (p, mut d) = load::load(dir.path());
        check_environments(&p, &mut d);
        (p, d, dir)
    }

    const P: &str = r#"version: 1
environments:
  names: [dev, prod]
  default: dev
  base: prod
  select_from_env: DBT_PROFILE
  dev:
    allow_base: [vendor]
connections:
  lake:
    type: ducklake
    catalog: {type: postgres, host: prod-host, database: meta, user: dagster, password: {secret: env:PG_PW}, sslmode: require}
    data_path: s3://prod/lake/
    storage: {type: s3, provider: credential_chain, region: us-east-1}
    environments:
      dev:
        catalog: {host: localhost, port: 20947, password: password, sslmode: disable}
        data_path: "{{ env.DUCKLAKE_DATA_PATH }}"
        storage: {type: local}
  appdb:
    type: postgres
    host: prod-appdb
    database: appdb
    user: dagster
    read_only: true
  app_pg:
    type: postgres
    host: prod-app
    database: postgres
    user: dagster
  vendor:
    type: rest
    spec: apis/v.yaml
    secrets: {api_key: {secret: env:VENDOR_KEY}}
"#;

    #[test]
    fn selection_order() {
        let (p, d, _t) = project(P);
        assert!(!d.has_errors(), "{d:?}");
        let envs = p.environments.as_ref().map(|e| &e.value);
        let none = MapEnv::default();
        let set = MapEnv::new([("DBT_PROFILE", "prod")]);
        assert_eq!(
            select(envs, None, &none).expect("default").name.as_deref(),
            Some("dev")
        );
        let a = select(envs, None, &set).expect("env var");
        assert_eq!(
            (a.name.as_deref(), a.source.as_str()),
            (Some("prod"), "env:DBT_PROFILE")
        );
        assert_eq!(
            select(envs, Some("dev"), &set)
                .expect("flag")
                .name
                .as_deref(),
            Some("dev")
        );
        assert!(select(envs, Some("staging"), &none).is_err());
        assert!(select(None, Some("dev"), &none).is_err());
        assert_eq!(select(None, None, &none).expect("none").name, None);
    }

    #[test]
    fn deep_merge_and_base_rules() {
        let (p, _, _t) = project(P);
        let envs = p.environments.as_ref().map(|e| &e.value);
        let (lake, from) =
            connection_node(&p.connections["lake"], Some("dev"), envs).expect("lake dev");
        assert_eq!(from, "environments.dev");
        let cat = lake.get("catalog").expect("catalog");
        assert_eq!(cat.get("host").and_then(Node::as_str), Some("localhost"));
        assert_eq!(
            cat.get("user").and_then(Node::as_str),
            Some("dagster"),
            "base keys kept"
        );
        assert!(
            connection_node(&p.connections["appdb"], Some("dev"), envs).is_ok(),
            "read_only may use base"
        );
        assert!(
            connection_node(&p.connections["vendor"], Some("dev"), envs).is_ok(),
            "allow_base"
        );
        let e =
            connection_node(&p.connections["app_pg"], Some("dev"), envs).expect_err("app_pg dev");
        assert!(e.message.contains("no `dev` override"), "{e}");
        assert!(
            connection_node(&p.connections["app_pg"], Some("prod"), envs).is_ok(),
            "base env"
        );
    }

    #[test]
    fn full_resolution_renders_and_redacts() {
        let (p, _, _t) = project(P);
        let r = Redactor::new();
        let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::new([
            ("DUCKLAKE_DATA_PATH", "/tmp/dl/"),
            ("PG_PW", "pg-secret"),
            ("VENDOR_KEY", "vk-secret"),
        ]));
        let t = Templates::new();
        let mut d = Diagnostics::default();
        let conns = resolve_all(
            &p,
            Some("dev"),
            &Resolve::Full { vars, redactor: &r },
            &t,
            &mut d,
        );
        assert!(
            d.errors().any(|e| e.code == "environment-base"),
            "app_pg has no dev block"
        );
        let ConnectionKind::Ducklake(lake) = &conns["lake"].kind else {
            panic!()
        };
        assert_eq!(lake.data_path, "/tmp/dl/");
        assert!(
            d.warnings().any(|w| w.code == "literal-secret"),
            "dev password is a literal"
        );
        assert_eq!(r.redact("vk-secret"), "****");
        let ConnectionKind::Rest(v) = &conns["vendor"].kind else {
            panic!()
        };
        assert_eq!(
            v.secrets["api_key"],
            SecretValue::Literal("vk-secret".into())
        );
        let json = serde_json::to_string(&conns["vendor"]).expect("json");
        assert!(!json.contains("vk-secret"), "{json}");
    }

    #[test]
    fn full_resolution_reports_missing_env() {
        let (p, _, _t) = project(P);
        let r = Redactor::new();
        let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::default());
        let mut d = Diagnostics::default();
        let conns = resolve_all(
            &p,
            Some("prod"),
            &Resolve::Full { vars, redactor: &r },
            &Templates::new(),
            &mut d,
        );
        assert!(!conns.contains_key("lake"));
        let msgs: Vec<_> = d.errors().map(|e| e.error.message.clone()).collect();
        assert!(msgs.iter().any(|m| m.contains("PG_PW")), "{msgs:?}");
    }

    #[test]
    fn static_resolution_needs_no_env() {
        let (p, _, _t) = project(P);
        let mut d = Diagnostics::default();
        let conns = resolve_all(
            &p,
            Some("prod"),
            &Resolve::Static,
            &Templates::new(),
            &mut d,
        );
        assert!(!d.has_errors(), "{d:?}");
        assert_eq!(conns.len(), 4);
    }

    #[test]
    fn dbt_profile_connections_render_and_redact() {
        let (p, d, _t) = project(
            r#"version: 1
environments: {names: [dev, prod], default: dev, base: dev}
connections:
  wh:
    type: duckdb
    dbt: {profile: analytics, profiles_dir: dbt}
  app:
    type: postgres
    read_only: true
    dbt: {profile: analytics, target: pg, profiles_dir: dbt}
    environments:
      prod: {dbt: {target: pg_prod}}
"#,
        );
        assert!(!d.has_errors(), "{d:?}");
        std::fs::create_dir_all(p.root.join("dbt")).expect("mkdir");
        std::fs::write(
            p.root.join("dbt/profiles.yml"),
            r#"analytics:
  target: dev
  outputs:
    dev: {type: duckdb, path: "{{ env_var('WH_PATH', 'wh.duckdb') }}"}
    pg: {type: postgres, host: h, port: "{{ env_var('PG_PORT') | as_number }}", user: u, pass: "{{ env_var('PG_PW') }}", dbname: app, schema: s}
    pg_prod: {type: postgres, host: prod-h, port: 5432, user: u, pass: "{{ env_var('PG_PW') }}", dbname: app, schema: s}
"#,
        )
        .expect("write");

        let mut d = Diagnostics::default();
        let conns = resolve_all(&p, Some("dev"), &Resolve::Static, &Templates::new(), &mut d);
        assert!(!d.has_errors(), "{d:?}");
        assert_eq!(conns["wh"].type_name(), "duckdb");

        let r = Redactor::new();
        let vars: Arc<dyn EnvSource> =
            Arc::new(MapEnv::new([("PG_PORT", "6543"), ("PG_PW", "pg-secret")]));
        for (env, host, port) in [("dev", "h", Some(6543)), ("prod", "prod-h", Some(5432))] {
            let mut d = Diagnostics::default();
            let conns = resolve_all(
                &p,
                Some(env),
                &Resolve::Full {
                    vars: vars.clone(),
                    redactor: &r,
                },
                &Templates::new(),
                &mut d,
            );
            assert!(!d.has_errors(), "{env}: {d:?}");
            let ConnectionKind::Duckdb(wh) = &conns["wh"].kind else {
                panic!()
            };
            assert_eq!(wh.path, "wh.duckdb");
            let ConnectionKind::Postgres(pg) = &conns["app"].kind else {
                panic!()
            };
            assert_eq!((pg.host.as_str(), pg.port), (host, port));
            assert_eq!(pg.password, Some(SecretValue::Literal("pg-secret".into())));
        }
        assert_eq!(r.redact("pg-secret"), "****");
    }

    #[test]
    fn dbt_profile_errors_point_at_the_connection() {
        let (p, _, _t) = project(
            "version: 1\nconnections:\n  wh:\n    type: duckdb\n    dbt: {profile: analytics, profiles_dir: nowhere}\n",
        );
        let mut d = Diagnostics::default();
        let conns = resolve_all(&p, None, &Resolve::Static, &Templates::new(), &mut d);
        assert!(conns.is_empty());
        let e = d.errors().find(|e| e.code == "dbt").expect("dbt error");
        assert!(e.error.message.contains("reading dbt profiles"), "{e:?}");
        assert_eq!(e.error.span.as_ref().map(|s| s.line), Some(5));
    }

    const TYPED: &str = r#"version: 1
connections:
  pg:
    type: postgres
    host: "{{ env.PG_HOST }}"
    port: "{{ env.PG_PORT }}"
    database: d
    user: u
    statement_timeout: "{{ env.PG_TIMEOUT }}"
    keepalives: {count: "{{ env.PG_KEEPALIVES }}"}
    manage_state_schema: "{{ env.PG_MANAGE }}"
    max_concurrency: "{{ env.PG_CONC }}"
    timeouts: {connect: "{{ env.PG_TIMEOUT }}"}
  lake:
    type: ducklake
    catalog: {type: postgres, host: h, port: "{{ env.PG_PORT }}", database: m, user: u}
    data_path: /d/
    data_inlining_row_limit: "{{ env.PG_CONC }}"
  rs:
    type: redshift
    access: wire
    wire: {host: h, port: "{{ env.PG_PORT }}", database: d, user: u}
  sf:
    type: files
    sftp: {host: h, port: "{{ env.PG_PORT }}", connect_timeout: "{{ env.PG_TIMEOUT }}"}
"#;

    fn resolve_typed(vars: &[(&str, &str)]) -> (IndexMap<String, Connection>, Diagnostics) {
        let (p, d, _t) = project(TYPED);
        assert!(!d.has_errors(), "{d:?}");
        let r = Redactor::new();
        let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::new(vars.iter().copied()));
        let mut d = Diagnostics::default();
        let conns = resolve_all(
            &p,
            None,
            &Resolve::Full { vars, redactor: &r },
            &Templates::new(),
            &mut d,
        );
        (conns, d)
    }

    #[test]
    fn typed_fields_can_be_templated() {
        let (p, _, _t) = project(TYPED);
        let mut d = Diagnostics::default();
        let conns = resolve_all(&p, None, &Resolve::Static, &Templates::new(), &mut d);
        assert!(!d.has_errors(), "{d:?}");
        assert_eq!(conns.len(), 4);
        let ConnectionKind::Postgres(pg) = &conns["pg"].kind else {
            panic!()
        };
        assert_eq!(
            pg.host, "{{ env.PG_HOST }}",
            "string templates stay unrendered"
        );

        let good = [
            ("PG_HOST", "db"),
            ("PG_PORT", "15432"),
            ("PG_TIMEOUT", "90s"),
            ("PG_KEEPALIVES", "3"),
            ("PG_MANAGE", "false"),
            ("PG_CONC", "4"),
        ];
        let (conns, d) = resolve_typed(&good);
        assert!(!d.has_errors(), "{d:?}");
        let ConnectionKind::Postgres(pg) = &conns["pg"].kind else {
            panic!()
        };
        assert_eq!(
            (pg.host.as_str(), pg.port, pg.manage_state_schema),
            ("db", Some(15432), Some(false))
        );
        assert_eq!(pg.statement_timeout.map(|t| t.0.as_secs()), Some(90));
        assert_eq!(pg.keepalives.as_ref().and_then(|k| k.count), Some(3));
        let common = &conns["pg"].common;
        assert_eq!(common.max_concurrency, Some(4));
        assert_eq!(
            common
                .timeouts
                .as_ref()
                .and_then(|t| t.connect)
                .map(|t| t.0.as_secs()),
            Some(90)
        );
        let ConnectionKind::Ducklake(l) = &conns["lake"].kind else {
            panic!()
        };
        assert_eq!(
            (l.catalog.port, l.data_inlining_row_limit),
            (Some(15432), Some(4))
        );
        let ConnectionKind::Redshift(rs) = &conns["rs"].kind else {
            panic!()
        };
        assert_eq!(rs.wire.as_ref().and_then(|w| w.port), Some(15432));
        let ConnectionKind::Files(f) = &conns["sf"].kind else {
            panic!()
        };
        let sftp = f.sftp.as_ref().expect("sftp");
        assert_eq!(sftp.port, Some(15432));
        assert_eq!(sftp.connect_timeout.map(|t| t.0.as_secs()), Some(90));
    }

    #[test]
    fn templated_typed_fields_are_validated_at_their_span() {
        for (bad, value) in [
            ("PG_PORT", "5432x"),
            ("PG_PORT", "70000"),
            ("PG_TIMEOUT", "soon"),
        ] {
            let mut vars = vec![
                ("PG_HOST", "db"),
                ("PG_PORT", "5432"),
                ("PG_TIMEOUT", "1s"),
                ("PG_KEEPALIVES", "3"),
                ("PG_MANAGE", "true"),
                ("PG_CONC", "4"),
            ];
            vars.retain(|(k, _)| *k != bad);
            vars.push((bad, value));
            let (conns, d) = resolve_typed(&vars);
            assert!(!conns.contains_key("pg"), "{bad}={value}");
            let e = d
                .errors()
                .find(|e| e.code == "connection")
                .map(|e| &e.error)
                .expect("error");
            assert!(
                e.message.contains(&format!("rendered to {value:?}")),
                "{bad}={value}: {e}"
            );
            let span = e.span.as_ref().expect("span");
            let line = TYPED.lines().nth(span.line as usize - 1).expect("line");
            assert!(line.contains(&format!("env.{bad} ")), "{bad}={value}: {e}");
        }
    }

    #[test]
    fn override_checks() {
        let (_, d, _t) = project(
            "version: 1\nenvironments: {names: [dev], bogus: {}}\nconnections:\n  a:\n    type: postgres\n    host: h\n    database: d\n    user: u\n    environments:\n      stage: {host: x}\n      dev: {type: files}\n",
        );
        let msgs: Vec<_> = d.errors().map(|e| e.error.message.clone()).collect();
        assert!(
            msgs.iter().any(|m| m.contains("unknown key `bogus`")),
            "{msgs:?}"
        );
        assert!(
            msgs.iter()
                .any(|m| m.contains("unknown environment `stage`")),
            "{msgs:?}"
        );
        assert!(
            msgs.iter()
                .any(|m| m.contains("cannot change a connection's `type`")),
            "{msgs:?}"
        );
    }
}
