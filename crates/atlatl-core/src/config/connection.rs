use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::Serialize;

use super::de::from_node;
use super::diag::ConfigError;
use super::model::{
    ClickhouseConn, ConnectionCommon, ConnectionKind, D1Conn, DeltaConn, DuckLakeConn, DuckdbConn,
    FilesConn, PostgresConn, RedshiftConn, RestConn, SqliteConn, TursoConn,
};
use super::node::{Node, Span};
use super::types::SecretValue;

pub const TYPES: &[&str] = &[
    "ducklake",
    "postgres",
    "redshift",
    "rest",
    "files",
    "delta",
    "sqlite",
    "duckdb",
    "clickhouse",
    "d1",
    "turso",
];
const COMMON_KEYS: &[&str] = &[
    "type",
    "max_concurrency",
    "read_only",
    "retry",
    "timeouts",
    "environments",
];

/// A connection resolved for one environment.
#[derive(Clone, Debug, Serialize)]
pub struct Connection {
    pub name: String,
    #[serde(skip)]
    pub span: Span,
    #[serde(flatten)]
    pub common: ConnectionCommon,
    #[serde(flatten)]
    pub kind: ConnectionKind,
    /// Which definition was used: `base` or `environments.<env>`.
    pub resolved_from: String,
}

impl Connection {
    pub fn type_name(&self) -> &'static str {
        self.kind.type_name()
    }

    pub fn can_be_target(&self) -> bool {
        !self.common.read_only
            && !matches!(
                self.kind,
                ConnectionKind::Delta(_) | ConnectionKind::Clickhouse(_)
            )
    }

    /// Mutable access to every secret-bearing field, labelled by path.
    pub fn secrets_mut(&mut self) -> Vec<(String, &mut SecretValue)> {
        let mut out: Vec<(String, &mut SecretValue)> = Vec::new();
        match &mut self.kind {
            ConnectionKind::Ducklake(d) => {
                if let Some(p) = d.catalog.password.as_mut() {
                    out.push(("catalog.password".into(), p));
                }
                if let Some(s) = d.storage.as_mut() {
                    push_storage("storage", s, &mut out);
                }
            }
            ConnectionKind::Postgres(p) => {
                if let Some(pw) = p.password.as_mut() {
                    out.push(("password".into(), pw));
                }
            }
            ConnectionKind::Redshift(r) => {
                if let Some(pw) = r.wire.as_mut().and_then(|w| w.password.as_mut()) {
                    out.push(("wire.password".into(), pw));
                }
                if let Some(d) = r.data_api.as_mut() {
                    for (k, v) in [
                        ("key_id", d.key_id.as_mut()),
                        ("secret", d.secret.as_mut()),
                        ("session_token", d.session_token.as_mut()),
                    ] {
                        if let Some(v) = v {
                            out.push((format!("data_api.{k}"), v));
                        }
                    }
                }
                if let Some(s) = r.unload.as_mut().and_then(|u| u.storage.as_mut()) {
                    push_storage("unload.storage", s, &mut out);
                }
            }
            ConnectionKind::Rest(r) => {
                for (k, v) in r.secrets.iter_mut() {
                    out.push((format!("secrets.{k}"), v));
                }
            }
            ConnectionKind::Files(f) => {
                if let Some(s) = f.storage.as_mut() {
                    push_storage("storage", s, &mut out);
                }
                if let Some(s) = f.sftp.as_mut() {
                    for (k, v) in [
                        ("sftp.password", s.password.as_mut()),
                        ("sftp.private_key", s.private_key.as_mut()),
                        (
                            "sftp.private_key_passphrase",
                            s.private_key_passphrase.as_mut(),
                        ),
                    ] {
                        if let Some(v) = v {
                            out.push((k.into(), v));
                        }
                    }
                }
            }
            ConnectionKind::Delta(d) => {
                if let Some(s) = d.storage.as_mut() {
                    push_storage("storage", s, &mut out);
                }
            }
            ConnectionKind::Sqlite(_) | ConnectionKind::Duckdb(_) => {}
            ConnectionKind::Clickhouse(c) => {
                if let Some(v) = c.password.as_mut() {
                    out.push(("password".into(), v));
                }
                if let Some(v) = c.dsn.as_mut() {
                    out.push(("dsn".into(), v));
                }
            }
            ConnectionKind::D1(d) => out.push(("api_token".into(), &mut d.api_token)),
            ConnectionKind::Turso(t) => {
                if let Some(v) = t.auth_token.as_mut() {
                    out.push(("auth_token".into(), v));
                }
            }
        }
        out
    }
}

fn push_storage<'a>(
    prefix: &str,
    s: &'a mut super::model::StorageConfig,
    out: &mut Vec<(String, &'a mut SecretValue)>,
) {
    if let Some(v) = s.key_id.as_mut() {
        out.push((format!("{prefix}.key_id"), v));
    }
    if let Some(v) = s.secret.as_mut() {
        out.push((format!("{prefix}.secret"), v));
    }
    if let Some(v) = s.session_token.as_mut() {
        out.push((format!("{prefix}.session_token"), v));
    }
}

/// Parses a connection node (with `environments` already applied or stripped).
pub fn parse_connection(node: &Node) -> Result<(ConnectionCommon, ConnectionKind), ConfigError> {
    if node.as_map().is_none() {
        return Err(
            ConfigError::new(format!("a connection must be a map, found {}", node.kind()))
                .at(node.span.clone()),
        );
    }
    let ty = node
        .get("type")
        .ok_or_else(|| ConfigError::new("connection is missing `type`").at(node.span.clone()))?;
    let ty_name = ty
        .as_str()
        .ok_or_else(|| ConfigError::new("`type` must be a string").at(ty.span.clone()))?;
    let (rest, mut common) = node.take_keys(COMMON_KEYS);
    common.shift_remove("type");
    common.shift_remove("environments");
    let common: ConnectionCommon = from_node(&Node::map(common, node.span.clone()))?;
    let kind = match ty_name {
        "ducklake" => ConnectionKind::Ducklake(from_node::<DuckLakeConn>(&rest)?),
        "postgres" => ConnectionKind::Postgres(from_node::<PostgresConn>(&rest)?),
        "redshift" => ConnectionKind::Redshift(from_node::<RedshiftConn>(&rest)?),
        "rest" => ConnectionKind::Rest(from_node::<RestConn>(&rest)?),
        "files" => ConnectionKind::Files(from_node::<FilesConn>(&rest)?),
        "delta" => ConnectionKind::Delta(from_node::<DeltaConn>(&rest)?),
        "sqlite" => ConnectionKind::Sqlite(from_node::<SqliteConn>(&rest)?),
        "duckdb" => ConnectionKind::Duckdb(from_node::<DuckdbConn>(&rest)?),
        "clickhouse" => ConnectionKind::Clickhouse(from_node::<ClickhouseConn>(&rest)?),
        "d1" => ConnectionKind::D1(from_node::<D1Conn>(&rest)?),
        "turso" => ConnectionKind::Turso(from_node::<TursoConn>(&rest)?),
        other => {
            return Err(ConfigError::new(format!(
                "unknown connection type `{other}` (supported: {})",
                TYPES.join(", ")
            ))
            .at(ty.span.clone()));
        }
    };
    Ok((common, kind))
}

/// `(connection type, property)` objects a dbt target fills (06 §12), so they may be partial.
const DBT_NESTED: &[(&str, &str)] = &[("redshift", "wire")];

/// Inlines each `$ref` in `v` with its definition's `required` removed.
fn relax_refs(v: &mut serde_json::Value, g: &SchemaGenerator) {
    match v {
        serde_json::Value::Object(o) => {
            let def = o
                .get("$ref")
                .and_then(|r| r.as_str())
                .and_then(|r| r.rsplit('/').next())
                .and_then(|n| g.definitions().get(n))
                .cloned();
            if let Some(mut d) = def {
                if let Some(m) = d.as_object_mut() {
                    m.remove("required");
                }
                *v = d;
            } else {
                o.values_mut().for_each(|x| relax_refs(x, g));
            }
        }
        serde_json::Value::Array(a) => a.iter_mut().for_each(|x| relax_refs(x, g)),
        _ => {}
    }
}

fn variant<T: JsonSchema>(
    g: &mut SchemaGenerator,
    name: &str,
    common: &serde_json::Value,
) -> serde_json::Value {
    let mut s = T::json_schema(g).to_value();
    // Nested settings a dbt target can fill: relaxed (no `required`) at the top level, strict
    // in the branch without `dbt` below.
    let mut strict = serde_json::Map::new();
    if let Some(obj) = s.as_object_mut() {
        for key in DBT_NESTED
            .iter()
            .filter(|(t, _)| *t == name)
            .map(|(_, k)| *k)
        {
            let Some(p) = obj
                .get_mut("properties")
                .and_then(|p| p.as_object_mut())
                .and_then(|p| p.get_mut(key))
            else {
                continue;
            };
            strict.insert(key.to_owned(), p.clone());
            relax_refs(p, g);
        }
    }
    if let Some(obj) = s.as_object_mut() {
        let props = obj
            .entry("properties")
            .or_insert_with(|| serde_json::json!({}));
        if let (Some(props), Some(common)) = (
            props.as_object_mut(),
            common.get("properties").and_then(|p| p.as_object()),
        ) {
            for (k, v) in common {
                props.insert(k.clone(), v.clone());
            }
            props.insert("type".into(), serde_json::json!({"const": name}));
            if super::dbt::TYPES.contains(&name) {
                props.insert(
                    "dbt".into(),
                    serde_json::json!({
                        "type": "object",
                        "description": "Fill this connection from a dbt profile target; its own keys merge over the target's",
                        "properties": {
                            "profile": {"type": "string"},
                            "target": {"type": "string", "description": "Default: DBT_TARGET, else the profile's `target`"},
                            "profiles_dir": {"type": "string", "description": "Default: DBT_PROFILES_DIR, the working directory, ~/.dbt"}
                        },
                        "required": ["profile"],
                        "additionalProperties": false
                    }),
                );
            }
            props.insert(
                "environments".into(),
                serde_json::json!({
                    "type": "object",
                    "description": "Per-environment overrides, deep-merged over this definition",
                    "additionalProperties": {"type": "object"}
                }),
            );
        }
        if super::dbt::TYPES.contains(&name) {
            // Either the type's own required fields, or a dbt target that supplies them.
            let own = obj.remove("required");
            if own.is_some() || !strict.is_empty() {
                let mut without = serde_json::Map::new();
                if let Some(own) = own {
                    without.insert("required".into(), own);
                }
                if !strict.is_empty() {
                    without.insert("properties".into(), serde_json::Value::Object(strict));
                }
                obj.insert(
                    "anyOf".into(),
                    serde_json::json!([without, {"required": ["dbt"]}]),
                );
            }
        }
        let req = obj
            .entry("required")
            .or_insert_with(|| serde_json::json!([]));
        if let Some(r) = req.as_array_mut() {
            r.push("type".into());
        }
    }
    s
}

pub fn connection_json_schema(g: &mut SchemaGenerator) -> Schema {
    let common = ConnectionCommon::json_schema(g).to_value();
    let one_of = vec![
        variant::<DuckLakeConn>(g, "ducklake", &common),
        variant::<PostgresConn>(g, "postgres", &common),
        variant::<RedshiftConn>(g, "redshift", &common),
        variant::<RestConn>(g, "rest", &common),
        variant::<FilesConn>(g, "files", &common),
        variant::<DeltaConn>(g, "delta", &common),
        variant::<SqliteConn>(g, "sqlite", &common),
        variant::<DuckdbConn>(g, "duckdb", &common),
        variant::<ClickhouseConn>(g, "clickhouse", &common),
        variant::<D1Conn>(g, "d1", &common),
        variant::<TursoConn>(g, "turso", &common),
    ];
    json_schema!({"oneOf": one_of})
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use super::*;
    use crate::config::node::parse_yaml;

    fn node(s: &str) -> Node {
        parse_yaml(s, Arc::from(Path::new("c.yaml"))).expect("yaml")
    }

    #[test]
    fn parses_by_type() {
        let (c, k) = parse_connection(&node(
            "type: postgres\nhost: h\ndatabase: d\nuser: u\npassword: {secret: env:PW}\nread_only: true\n",
        ))
        .expect("pg");
        assert!(c.read_only);
        let ConnectionKind::Postgres(p) = k else {
            panic!()
        };
        assert_eq!(p.host, "h");
        assert!(matches!(p.password, Some(SecretValue::Ref(_))));
    }

    #[test]
    fn delta_connection() {
        let (_, k) = parse_connection(&node(
            "type: delta\nroot: s3://b/delta/\nstorage: {type: s3, provider: config, key_id: k, secret: {secret: env:S}}\n",
        ))
        .expect("delta");
        let ConnectionKind::Delta(d) = k else {
            panic!()
        };
        assert_eq!(d.root, "s3://b/delta/");
        let e = parse_connection(&node("type: delta\n")).expect_err("root");
        assert!(e.message.contains("root"), "{e}");
    }

    #[test]
    fn files_sftp_secrets() {
        let (common, kind) = parse_connection(&node(
            "type: files\nsftp: {host: h, user: u, password: {secret: env:PW}, private_key: {secret: env:K}, host_key: ['SHA256:x']}\n",
        ))
        .expect("files");
        let mut c = Connection {
            name: "f".into(),
            span: Span::new(Arc::from(Path::new("c.yaml")), 1, 1),
            common,
            kind,
            resolved_from: "base".into(),
        };
        let paths: Vec<String> = c.secrets_mut().into_iter().map(|(p, _)| p).collect();
        assert_eq!(paths, ["sftp.password", "sftp.private_key"]);
        let e = parse_connection(&node("type: files\nsftp: {hots: h}\n")).expect_err("field");
        assert!(e.message.contains("unknown field `hots`"), "{e}");
    }

    #[test]
    fn redshift_secrets() {
        let (common, kind) = parse_connection(&node(
            "type: redshift\ndata_api: {workgroup: w, database: d, key_id: {secret: env:K}, secret: {secret: env:S}, poll: {wait: 5s}}\n\
             unload: {bucket: b, iam_role: r, storage: {type: s3, secret: {secret: env:S3}}}\n",
        ))
        .expect("redshift");
        let mut c = Connection {
            name: "r".into(),
            span: Span::new(Arc::from(Path::new("c.yaml")), 1, 1),
            common,
            kind,
            resolved_from: "base".into(),
        };
        let paths: Vec<String> = c.secrets_mut().into_iter().map(|(p, _)| p).collect();
        assert_eq!(
            paths,
            [
                "data_api.key_id",
                "data_api.secret",
                "unload.storage.secret"
            ]
        );
    }

    #[test]
    fn sqlite_family() {
        let (_, k) = parse_connection(&node("type: sqlite\npath: app.db\nbusy_timeout: 2s\n"))
            .expect("sqlite");
        let ConnectionKind::Sqlite(s) = k else {
            panic!()
        };
        assert_eq!((s.path.as_str(), s.create_tables), ("app.db", None));
        let secrets = |yaml: &str| {
            let (common, kind) = parse_connection(&node(yaml)).expect(yaml);
            let mut c = Connection {
                name: "c".into(),
                span: Span::new(Arc::from(Path::new("c.yaml")), 1, 1),
                common,
                kind,
                resolved_from: "base".into(),
            };
            c.secrets_mut()
                .into_iter()
                .map(|(p, _)| p)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            secrets("type: d1\naccount_id: a\ndatabase: app\napi_token: {secret: env:T}\n"),
            ["api_token"]
        );
        assert_eq!(
            secrets("type: turso\nurl: libsql://x\nauth_token: {secret: env:T}\n"),
            ["auth_token"]
        );
        let e = parse_connection(&node("type: d1\naccount_id: a\n")).expect_err("token");
        assert!(e.message.contains("api_token"), "{e}");
    }

    #[test]
    fn unknown_type_and_fields() {
        let e = parse_connection(&node("type: mongo\n")).expect_err("type");
        assert!(e.message.contains("unknown connection type `mongo`"), "{e}");
        let e = parse_connection(&node(
            "type: postgres\nhost: h\ndatabase: d\nuser: u\nhots: x\n",
        ))
        .expect_err("field");
        assert!(e.message.contains("unknown field `hots`"), "{e}");
        assert_eq!(e.span.map(|s| s.line), Some(5));
        let e = parse_connection(&node("host: h\n")).expect_err("missing type");
        assert!(e.message.contains("missing `type`"), "{e}");
    }

    #[test]
    fn schema_has_each_type() {
        let mut g = SchemaGenerator::default();
        let s = connection_json_schema(&mut g).to_value();
        let types: Vec<_> = s["oneOf"]
            .as_array()
            .expect("oneOf")
            .iter()
            .map(|v| {
                v["properties"]["type"]["const"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned()
            })
            .collect();
        assert_eq!(types, TYPES);
        let duckdb = &s["oneOf"][7];
        assert_eq!(
            duckdb["properties"]["dbt"]["required"],
            serde_json::json!(["profile"])
        );
        assert_eq!(duckdb["required"], serde_json::json!(["type"]));
        assert_eq!(
            duckdb["anyOf"],
            serde_json::json!([{"required": ["path"]}, {"required": ["dbt"]}])
        );
        let redshift = &s["oneOf"][2];
        let wire = &redshift["properties"]["wire"]["anyOf"][0];
        assert!(
            wire.get("required").is_none() && wire["properties"]["host"].is_object(),
            "{wire}"
        );
        assert_eq!(
            redshift["anyOf"][0]["properties"]["wire"]["anyOf"][0]["$ref"], "#/$defs/WireConfig",
            "strict without dbt"
        );
        assert_eq!(
            redshift["anyOf"][1],
            serde_json::json!({"required": ["dbt"]})
        );
        let postgres = &s["oneOf"][1]["anyOf"][0]["required"];
        assert!(
            postgres
                .as_array()
                .is_some_and(|r| r.iter().any(|f| f == "host")),
            "{postgres}"
        );
        assert!(s["oneOf"][3]["properties"].get("dbt").is_none(), "rest");
    }
}
