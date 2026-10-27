//! Connections filled from a dbt profile target (spec 06 §12): `dbt: {profile, target,
//! profiles_dir}` on a `duckdb`, `postgres` or `redshift` connection.
//!
//! The target's fields become the connection's base definition, and the connection's own keys
//! merge over them. dbt's `env_var('X', 'default')` is rewritten to atlatl's `env('X', 'default')`,
//! so the values render, and secrets resolve, with the rest of the connection.

use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use regex::Regex;
use serde::Deserialize;

use super::de::from_node;
use super::diag::{ConfigError, SourceMap};
use super::node::{Entry, Map, Node, merge, parse_yaml_with_merge_keys};
use crate::template::{Context, EnvSource, Templates, is_template};

pub const KEY: &str = "dbt";
pub const TYPES: &[&str] = &["duckdb", "postgres", "redshift"];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DbtRef {
    profile: String,
    target: Option<String>,
    profiles_dir: Option<String>,
}

static ENV_VAR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\benv_var\s*\(").expect("env_var regex"));
static TYPE_FILTER: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\s*\|\s*(as_text|as_number|as_bool|as_native)\b").expect("filter regex")
});

/// dbt's Jinja helpers as atlatl's: `env_var(` → `env(`; the type-hint filters dropped, since the
/// typed connection fields read their rendered strings anyway.
fn rewrite(s: &str) -> String {
    if !is_template(s) {
        return s.to_owned();
    }
    let s = ENV_VAR.replace_all(s, "env(");
    TYPE_FILTER.replace_all(&s, "").into_owned()
}

/// `profiles_dir` (relative to the project root), else `DBT_PROFILES_DIR`, else the working
/// directory when it has a profiles.yml, else `~/.dbt`: dbt's own order.
fn profiles_file(dir: Option<&str>, root: &Path, env: &dyn EnvSource) -> Option<PathBuf> {
    if let Some(d) = dir {
        return Some(root.join(d).join("profiles.yml"));
    }
    if let Some(d) = env.var("DBT_PROFILES_DIR").filter(|d| !d.is_empty()) {
        return Some(PathBuf::from(d).join("profiles.yml"));
    }
    let cwd = std::env::current_dir().ok()?.join("profiles.yml");
    if cwd.is_file() {
        return Some(cwd);
    }
    env.var("HOME")
        .map(|h| PathBuf::from(h).join(".dbt").join("profiles.yml"))
}

fn names(n: &Node) -> String {
    n.as_map()
        .map(|m| {
            m.keys()
                .filter(|k| k.as_str() != "config")
                .map(|k| format!("`{k}`"))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "none".into())
}

fn field(out: &mut Map, key: &str, value: Option<&Node>) {
    if let Some(v) = value.filter(|v| !v.is_null()) {
        let mut v = v.clone();
        let _ = v.try_map_strings::<()>(&mut |s, _| Ok(is_template(s).then(|| rewrite(s))));
        out.insert(
            key.into(),
            Entry {
                key_span: v.span.clone(),
                value: v,
            },
        );
    }
}

/// The atlatl fields for a dbt output of adapter `ty`.
fn map_output(ty: &str, o: &Node) -> Result<Map, ConfigError> {
    let mut m = Map::new();
    let pick = |keys: &[&str]| keys.iter().find_map(|k| o.get(k));
    match ty {
        "duckdb" => {
            let path = o.get("path").and_then(Node::as_str).unwrap_or(":memory:");
            if path == ":memory:" || path.starts_with("md:") || path.starts_with("motherduck:") {
                return Err(ConfigError::new(format!(
                    "the dbt target's DuckDB `path` must be a local file, not `{path}`"
                ))
                .at(o.span.clone()));
            }
            field(&mut m, "path", o.get("path"));
        }
        "postgres" | "redshift" => {
            if let Some(method) = o.get("method").and_then(Node::as_str)
                && method != "database"
            {
                return Err(ConfigError::new(format!(
                    "dbt `method: {method}` is not supported; only password authentication maps to an atlatl connection"
                ))
                .at(o.span.clone()));
            }
            let mut c = Map::new();
            field(&mut c, "host", o.get("host"));
            field(&mut c, "port", o.get("port"));
            field(&mut c, "database", pick(&["dbname", "database"]));
            field(&mut c, "user", o.get("user"));
            field(&mut c, "password", pick(&["password", "pass"]));
            field(&mut c, "sslmode", o.get("sslmode"));
            if ty == "postgres" {
                field(&mut c, "sslrootcert", o.get("sslrootcert"));
                m = c;
            } else {
                m.insert(
                    "wire".into(),
                    Entry {
                        key_span: o.span.clone(),
                        value: Node::map(c, o.span.clone()),
                    },
                );
            }
        }
        _ => unreachable!("checked against TYPES"),
    }
    Ok(m)
}

/// `node` with its `dbt` reference replaced by the profile target's fields; unchanged without one.
pub fn expand(
    node: &Node,
    root: &Path,
    env: Arc<dyn EnvSource>,
    sources: &SourceMap,
) -> Result<Node, ConfigError> {
    let (own, taken) = node.take_keys(&[KEY]);
    let Some(entry) = taken.get(KEY) else {
        return Ok(node.clone());
    };
    let at = entry.key_span.clone();
    let ty = node.get("type").and_then(Node::as_str).unwrap_or_default();
    if !TYPES.contains(&ty) {
        return Err(ConfigError::new(format!(
            "`dbt` can fill a {} connection, not `{ty}`",
            TYPES.join(", ")
        ))
        .at(at));
    }
    let r: DbtRef = from_node(&entry.value)?;
    let file = profiles_file(r.profiles_dir.as_deref(), root, env.as_ref()).ok_or_else(|| {
        ConfigError::new("no dbt profiles.yml: set `dbt.profiles_dir` or DBT_PROFILES_DIR")
            .at(at.clone())
    })?;
    let text = std::fs::read_to_string(&file).map_err(|e| {
        ConfigError::new(format!("reading dbt profiles {}: {e}", file.display())).at(at.clone())
    })?;
    let path: Arc<Path> = Arc::from(file.as_path());
    sources.insert(path.clone(), Arc::from(text.as_str()));
    // dbt reads profiles.yml with PyYAML, which accepts merge keys; atlatl's own config doesn't.
    let profiles = parse_yaml_with_merge_keys(&text, path)?;
    let profile = profiles.get(&r.profile).ok_or_else(|| {
        ConfigError::new(format!(
            "dbt profile `{}` is not in {} (profiles: {})",
            r.profile,
            file.display(),
            names(&profiles)
        ))
        .at(at.clone())
    })?;
    let target = match r
        .target
        .or_else(|| env.var("DBT_TARGET").filter(|t| !t.is_empty()))
    {
        Some(t) => t,
        None => {
            let t = profile
                .get("target")
                .and_then(Node::as_str)
                .ok_or_else(|| {
                    ConfigError::new(format!(
                        "dbt profile `{}` has no default `target`; set `dbt.target`",
                        r.profile
                    ))
                    .at(profile.span.clone())
                })?;
            let ctx = Context::load(env.clone(), root, None);
            Templates::new()
                .render(&rewrite(t), &ctx)
                .map_err(|e| ConfigError::new(e.message).at(profile.span.clone()))?
        }
    };
    let outputs = profile.get("outputs");
    let output = outputs.and_then(|o| o.get(&target)).ok_or_else(|| {
        ConfigError::new(format!(
            "dbt profile `{}` has no target `{target}` (targets: {})",
            r.profile,
            outputs.map(names).unwrap_or_else(|| "none".into())
        ))
        .at(profile.span.clone())
    })?;
    let adapter = output
        .get("type")
        .and_then(Node::as_str)
        .unwrap_or_default();
    if adapter != ty {
        return Err(ConfigError::new(format!(
            "dbt target `{}.{target}` is `type: {adapter}`, but this connection is `{ty}`",
            r.profile
        ))
        .at(output.span.clone())
        .related("connection type", at));
    }
    let base = Node::map(map_output(ty, output)?, output.span.clone());
    Ok(merge(&base, &own))
}

#[cfg(test)]
mod tests {
    use super::super::node::parse_yaml;
    use super::*;
    use crate::template::MapEnv;

    const PROFILES: &str = r#"
analytics:
  target: "{{ env_var('DBT_TARGET_DEFAULT', 'dev') }}"
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('WAREHOUSE_DUCKDB_PATH', 'warehouse.duckdb') }}"
      threads: 4
      extensions: [httpfs]
    mem: {type: duckdb, path: ":memory:"}
    pg:
      type: postgres
      host: localhost
      port: "{{ env_var('PG_PORT') | as_number }}"
      user: dagster
      pass: "{{ env_var('PG_PASSWORD') }}"
      dbname: app
      schema: public
      sslmode: disable
    rs: &rs
      type: redshift
      host: rs.example
      port: 5439
      user: u
      password: p
      dbname: dev
      schema: br
    iam: {type: redshift, method: iam, host: h, user: u, dbname: d, schema: s}
    rs_merged:
      <<: *rs
      dbname: other
"#;

    fn setup() -> (tempfile::TempDir, SourceMap) {
        let dir = tempfile::tempdir().expect("tmp");
        std::fs::create_dir_all(dir.path().join("dbt")).expect("mkdir");
        std::fs::write(dir.path().join("dbt/profiles.yml"), PROFILES).expect("write");
        (dir, SourceMap::default())
    }

    fn conn(yaml: &str) -> Node {
        parse_yaml(yaml, Arc::from(Path::new("atlatl.yaml"))).expect("yaml")
    }

    fn expand_with(dir: &Path, yaml: &str, env: &[(&str, &str)]) -> Result<Node, ConfigError> {
        expand(
            &conn(yaml),
            dir,
            Arc::new(MapEnv::new(env.iter().copied())),
            &SourceMap::default(),
        )
    }

    #[test]
    fn rewrites_dbt_jinja() {
        assert_eq!(
            rewrite("{{ env_var('A', 'x') }}/{{ env_var ( \"B\" ) | as_number }}"),
            "{{ env('A', 'x') }}/{{ env( \"B\" ) }}"
        );
        assert_eq!(rewrite("env_var(plain)"), "env_var(plain)");
    }

    #[test]
    fn duckdb_from_the_default_target() {
        let (dir, _) = setup();
        let n = expand_with(
            dir.path(),
            "type: duckdb\ndbt: {profile: analytics, profiles_dir: dbt}\nalias: wh\n",
            &[],
        )
        .expect("expand");
        assert_eq!(
            n.to_json(),
            serde_json::json!({
                "path": "{{ env('WAREHOUSE_DUCKDB_PATH', 'warehouse.duckdb') }}",
                "type": "duckdb",
                "alias": "wh",
            })
        );
        assert!(
            n.get("path")
                .expect("path")
                .span
                .file
                .ends_with("dbt/profiles.yml")
        );
    }

    #[test]
    fn target_order_explicit_then_env_then_profile() {
        let (dir, _) = setup();
        let y = "type: postgres\ndbt: {profile: analytics, profiles_dir: dbt}\n";
        let e = expand_with(dir.path(), y, &[("DBT_TARGET_DEFAULT", "pg")]).expect("profile");
        assert_eq!(e.get("database").and_then(Node::as_str), Some("app"));
        let e = expand_with(dir.path(), y, &[("DBT_TARGET", "pg")]).expect("env");
        assert_eq!(e.get("user").and_then(Node::as_str), Some("dagster"));
        let y = "type: postgres\ndbt: {profile: analytics, target: pg, profiles_dir: dbt}\n";
        let e = expand_with(dir.path(), y, &[("DBT_TARGET", "dev")]).expect("explicit");
        assert_eq!(
            e.get("password").and_then(Node::as_str),
            Some("{{ env('PG_PASSWORD') }}")
        );
        assert_eq!(
            e.get("port").and_then(Node::as_str),
            Some("{{ env('PG_PORT') }}")
        );
        assert!(e.get("schema").is_none(), "dbt-only keys are dropped");
    }

    #[test]
    fn own_keys_override_and_redshift_maps_to_wire() {
        let (dir, _) = setup();
        let n = expand_with(
            dir.path(),
            "type: redshift\ndbt: {profile: analytics, target: rs, profiles_dir: dbt}\nwire: {user: other}\nunload: {bucket: b}\n",
            &[],
        )
        .expect("expand");
        assert_eq!(
            n.to_json(),
            serde_json::json!({
                "wire": {"host": "rs.example", "port": 5439, "database": "dev", "user": "other", "password": "p"},
                "type": "redshift",
                "unload": {"bucket": "b"},
            })
        );
    }

    #[test]
    fn merge_keys_in_profiles() {
        let (dir, _) = setup();
        let y = "type: redshift\ndbt: {profile: analytics, target: rs_merged, profiles_dir: dbt}\n";
        let n = expand_with(dir.path(), y, &[]).expect("expand");
        let wire = n.get("wire").expect("wire");
        assert_eq!(wire.get("host").and_then(Node::as_str), Some("rs.example"));
        assert_eq!(wire.get("database").and_then(Node::as_str), Some("other"));
        // A merge elsewhere in the file doesn't stop an unrelated target from loading.
        let y = "type: duckdb\ndbt: {profile: analytics, target: dev, profiles_dir: dbt}\n";
        assert!(expand_with(dir.path(), y, &[]).is_ok());
    }

    #[test]
    fn profiles_dir_from_env() {
        let (dir, _) = setup();
        let d = dir.path().join("dbt").display().to_string();
        let n = expand_with(
            Path::new("/elsewhere"),
            "type: duckdb\ndbt: {profile: analytics, target: dev}\n",
            &[("DBT_PROFILES_DIR", d.as_str())],
        )
        .expect("expand");
        assert!(n.get("path").is_some());
    }

    #[test]
    fn errors() {
        let (dir, _) = setup();
        let cases = [
            (
                "type: rest\nspec: a.yaml\ndbt: {profile: analytics}\n",
                "not `rest`",
            ),
            (
                "type: duckdb\ndbt: {profile: nope, profiles_dir: dbt}\n",
                "profiles: `analytics`",
            ),
            (
                "type: duckdb\ndbt: {profile: analytics, target: x, profiles_dir: dbt}\n",
                "no target `x`",
            ),
            (
                "type: duckdb\ndbt: {profile: analytics, target: pg, profiles_dir: dbt}\n",
                "is `type: postgres`",
            ),
            (
                "type: duckdb\ndbt: {profile: analytics, target: mem, profiles_dir: dbt}\n",
                "must be a local file",
            ),
            (
                "type: redshift\ndbt: {profile: analytics, target: iam, profiles_dir: dbt}\n",
                "`method: iam`",
            ),
            (
                "type: duckdb\ndbt: {profile: analytics, profiles_dir: missing}\n",
                "reading dbt profiles",
            ),
            (
                "type: duckdb\ndbt: {profile: analytics, tagret: dev}\n",
                "tagret",
            ),
        ];
        for (y, want) in cases {
            let e = expand_with(dir.path(), y, &[]).expect_err(y);
            assert!(e.message.contains(want), "{y}: {}", e.message);
        }
    }

    #[test]
    fn without_dbt_unchanged() {
        let y = "type: duckdb\npath: a.duckdb\n";
        let n = expand_with(Path::new("/"), y, &[]).expect("expand");
        assert_eq!(n.to_json(), conn(y).to_json());
    }
}
