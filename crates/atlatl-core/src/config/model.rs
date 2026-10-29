//! Typed config. Every struct denies unknown fields. Stream-level structs are all-optional
//! because they're deserialized after the defaults chain is deep-merged (spec 02 §3).

use std::borrow::Cow;
use std::fmt;

use indexmap::IndexMap;
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use super::de::{Raw, Spanned};
use super::types::{Auto, ByteSize, Dur, SecretValue};
use crate::error::ErrorClass;

// ----------------------------------------------------------------------------- project

/// `atlatl.yaml`.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProjectFile {
    pub version: Spanned<u32>,
    #[serde(default)]
    pub environments: Option<Spanned<EnvironmentsConfig>>,
    #[serde(default)]
    pub engine: Option<EngineConfig>,
    #[serde(default)]
    pub state: Option<StateConfig>,
    /// Project-wide stream defaults, the lowest precedence in the merge chain.
    #[serde(default)]
    pub defaults: Option<Raw<StreamConfig>>,
    #[serde(default)]
    pub connections: IndexMap<String, Raw<ConnectionSchema>>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct EnvironmentsConfig {
    pub names: Vec<Spanned<String>>,
    pub default: Option<Spanned<String>>,
    /// Env var holding the environment name; `--env` overrides it.
    pub select_from_env: Option<String>,
    /// The environment that connection base definitions describe; in it, base definitions are
    /// always legal. Elsewhere a connection without an override block must be `read_only` or
    /// listed in `<env>.allow_base`.
    pub base: Option<Spanned<String>>,
    pub per_env: IndexMap<String, Spanned<EnvBlock>>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EnvBlock {
    #[serde(default)]
    pub allow_base: Vec<Spanned<String>>,
}

impl<'de> Deserialize<'de> for EnvironmentsConfig {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = EnvironmentsConfig;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an environments map")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut m: A) -> Result<Self::Value, A::Error> {
                let mut out = EnvironmentsConfig::default();
                let mut saw_names = false;
                while let Some(k) = m.next_key::<String>()? {
                    match k.as_str() {
                        "names" => {
                            out.names = m.next_value()?;
                            saw_names = true;
                        }
                        "default" => out.default = m.next_value()?,
                        "select_from_env" => out.select_from_env = m.next_value()?,
                        "base" => out.base = m.next_value()?,
                        _ => {
                            let block: Spanned<EnvBlock> = m.next_value()?;
                            out.per_env.insert(k, block);
                        }
                    }
                }
                if !saw_names {
                    return Err(de::Error::missing_field("names"));
                }
                Ok(out)
            }
        }
        d.deserialize_map(V)
    }
}

impl JsonSchema for EnvironmentsConfig {
    fn schema_name() -> Cow<'static, str> {
        "EnvironmentsConfig".into()
    }
    fn json_schema(g: &mut SchemaGenerator) -> Schema {
        let block = g.subschema_for::<EnvBlock>();
        json_schema!({
            "type": "object",
            "properties": {
                "names": {"type": "array", "items": {"type": "string"}},
                "default": {"type": "string"},
                "select_from_env": {"type": "string"},
                "base": {"type": "string"}
            },
            "required": ["names"],
            "additionalProperties": block
        })
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EngineConfig {
    /// `auto` = cgroup memory.max (or host RAM) × 0.85.
    pub memory: Option<Auto<ByteSize>>,
    /// `auto` = cgroup cpu.max quota.
    pub threads: Option<Auto<u32>>,
    /// Fraction of `memory` given to DuckDB's memory_limit (default 0.6).
    pub duckdb_share: Option<f64>,
    pub temp_directory: Option<String>,
    pub max_temp_directory_size: Option<ByteSize>,
    /// Total in-flight Arrow/NDJSON bytes across all replications.
    pub channel_bytes: Option<ByteSize>,
    /// Replications run concurrently (default 2).
    pub replication_concurrency: Option<u32>,
    /// Extracts per replication run concurrently (default 4).
    pub extract_concurrency: Option<u32>,
    /// Speed against memory outside DuckDB's `memory_limit` (default `high`; spec 01 §5).
    pub memory_mode: Option<MemoryMode>,
}

/// How hard the engine works to return freed allocator memory (spec 01 §5).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MemoryMode {
    /// Release between chunks and after long calls; the larger reserve.
    #[default]
    High,
    /// Also release periodically during long calls: slower, smaller reserve.
    Low,
}

/// Per-replication engine overrides. Replications sharing an op must agree.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EngineOverrides {
    pub memory: Option<Auto<ByteSize>>,
    pub threads: Option<Auto<u32>>,
    pub duckdb_share: Option<f64>,
    pub temp_directory: Option<String>,
    pub max_temp_directory_size: Option<ByteSize>,
    pub memory_mode: Option<MemoryMode>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StateConfig {
    /// Connection whose lake holds `_atlatl` state tables.
    pub lake: Spanned<String>,
    /// Lock backend: `<conn>` (a postgres connection) or `<conn>.catalog` (a DuckLake catalog).
    pub locks: Option<Spanned<String>>,
    /// Default lock acquisition timeout (default 30m).
    pub lock_timeout: Option<Dur>,
}

// ----------------------------------------------------------------------------- connections

/// Schema-only view of a connection; parsing goes through [`super::connection::parse_connection`].
#[derive(Clone, Debug)]
pub struct ConnectionSchema;

impl JsonSchema for ConnectionSchema {
    fn schema_name() -> Cow<'static, str> {
        "Connection".into()
    }
    fn json_schema(g: &mut SchemaGenerator) -> Schema {
        super::connection::connection_json_schema(g)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConnectionCommon {
    pub max_concurrency: Option<u32>,
    #[serde(default)]
    pub read_only: bool,
    pub retry: Option<RetryConfig>,
    pub timeouts: Option<Timeouts>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Timeouts {
    pub connect: Option<Dur>,
    pub statement: Option<Dur>,
    pub idle: Option<Dur>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum SslMode {
    Disable,
    Require,
    VerifyFull,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DuckLakeConn {
    /// ATTACH alias; defaults to the connection name.
    pub alias: Option<String>,
    pub catalog: CatalogConfig,
    pub data_path: String,
    pub storage: Option<StorageConfig>,
    pub data_inlining_row_limit: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CatalogType {
    Postgres,
    /// A DuckDB-file catalog (`path`), for local development and tests.
    Duckdb,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CatalogConfig {
    #[serde(rename = "type")]
    pub kind: CatalogType,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub user: Option<String>,
    pub password: Option<SecretValue>,
    pub sslmode: Option<SslMode>,
    pub iam_auth: Option<bool>,
    pub path: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StorageType {
    S3,
    Local,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum S3Provider {
    CredentialChain,
    Config,
    AssumeRole,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum UrlStyle {
    Path,
    Vhost,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    #[serde(rename = "type")]
    pub kind: StorageType,
    pub provider: Option<S3Provider>,
    pub region: Option<String>,
    /// `host:port`; a scheme is stripped and sets `use_ssl`.
    pub endpoint: Option<String>,
    pub url_style: Option<UrlStyle>,
    pub use_ssl: Option<bool>,
    pub key_id: Option<SecretValue>,
    pub secret: Option<SecretValue>,
    pub session_token: Option<SecretValue>,
    pub role_arn: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PostgresConn {
    pub host: String,
    pub port: Option<u16>,
    pub database: String,
    pub user: String,
    pub password: Option<SecretValue>,
    pub sslmode: Option<SslMode>,
    pub sslrootcert: Option<String>,
    pub application_name: Option<String>,
    pub statement_timeout: Option<Dur>,
    pub keepalives: Option<Keepalives>,
    /// Create the `atlatl` state schema on first use (default true).
    pub manage_state_schema: Option<bool>,
    /// Logical-replication CDC into the lake (`mode: cdc` from this source; 06 §9, 08 §10).
    pub changes: Option<PgChanges>,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PgChanges {
    /// This definition points at a production server: slots are refused unless the active
    /// environment is `environments.base` (or the project has no environments).
    pub production: Option<bool>,
    pub slots: Option<SlotsConfig>,
    /// pgoutput protocol (default `auto`: v2 with streaming and binary values on PG 14+, v1
    /// text before that).
    pub protocol: Option<PgProtocol>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PgProtocol {
    #[default]
    Auto,
    V1,
    V2,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SlotsConfig {
    /// Create the replication's slot and publication when missing (default false: an existing
    /// slot is used, never created).
    pub allow_create: Option<bool>,
    /// Session-scoped slots, dropped when the run ends; every run resyncs. For tests.
    pub temporary: Option<bool>,
}

impl PgChanges {
    /// The owned slot (and publication) of a replication: `atlatl_<replication>`, lowercased,
    /// non-`[a-z0-9_]` mapped to `_`, and at most 63 bytes (a hash suffix keeps long names unique).
    pub fn slot_name(replication: &str) -> String {
        let clean: String = replication
            .chars()
            .map(|c| match c.to_ascii_lowercase() {
                c @ ('a'..='z' | '0'..='9' | '_') => c,
                _ => '_',
            })
            .collect();
        let name = format!("atlatl_{clean}");
        if name.len() <= 63 {
            return name;
        }
        use sha2::Digest;
        let h = sha2::Sha256::digest(replication.as_bytes());
        let hex: String = h[..4].iter().map(|b| format!("{b:02x}")).collect();
        format!("{}_{hex}", &name[..54])
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Keepalives {
    pub idle: Option<Dur>,
    pub interval: Option<Dur>,
    pub count: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RedshiftAccess {
    DataApi,
    Wire,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RedshiftConn {
    pub access: Option<RedshiftAccess>,
    pub data_api: Option<Box<DataApiConfig>>,
    pub wire: Option<Box<WireConfig>>,
    pub unload: Option<Box<UnloadConfig>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DataApiAuth {
    Iam,
    SecretArn,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DataApiConfig {
    pub workgroup: Option<String>,
    pub cluster_identifier: Option<String>,
    pub database: String,
    pub auth: Option<DataApiAuth>,
    pub secret_arn: Option<String>,
    pub region: Option<String>,
    pub poll: Option<PollConfig>,
    /// Data API endpoint override (`http(s)://host[:port]`), e.g. a VPC endpoint or a local fake.
    pub endpoint: Option<String>,
    /// Static keys for the Data API caller; unset means the environment / IRSA web identity.
    pub key_id: Option<SecretValue>,
    pub secret: Option<SecretValue>,
    pub session_token: Option<SecretValue>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PollConfig {
    /// `DescribeStatement` long-poll wait (`WaitTimeSeconds`, ≤ 30s; `0` disables long polling).
    pub wait: Option<Dur>,
    pub initial: Option<Dur>,
    pub max: Option<Dur>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WireConfig {
    pub host: String,
    pub port: Option<u16>,
    pub database: String,
    pub user: String,
    pub password: Option<SecretValue>,
    pub sslmode: Option<SslMode>,
    pub keepalives: Option<Keepalives>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UnloadConfig {
    pub bucket: String,
    pub prefix: Option<String>,
    pub iam_role: String,
    /// `same`, a role ARN, or `none`.
    pub read_role: Option<String>,
    pub max_file_size: Option<ByteSize>,
    /// S3 settings for the read-back, cleanup and `gc` (endpoint, region, URL style, static
    /// keys). Assumed-role credentials replace the keys when `read_role` is a role.
    pub storage: Option<StorageConfig>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RestConn {
    /// API spec path, relative to the project root.
    pub spec: Spanned<String>,
    #[serde(default)]
    pub secrets: IndexMap<String, SecretValue>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FilesConn {
    pub root: Option<String>,
    pub storage: Option<StorageConfig>,
    /// Server and credentials for `sftp://` URLs.
    pub sftp: Option<SftpConfig>,
    /// Headers sent with every `https://` download, e.g. a `User-Agent` the server requires.
    pub headers: Option<IndexMap<String, String>>,
}

/// SFTP transport (06 §5). `sftp://[user@]host[:port]/path` URLs override `host`, `port` and
/// `user`. The server key is checked against `host_key` fingerprints, else `known_hosts`
/// (default `~/.ssh/known_hosts`).
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SftpConfig {
    pub host: Option<String>,
    /// Default 22.
    pub port: Option<u16>,
    pub user: Option<String>,
    pub password: Option<SecretValue>,
    /// OpenSSH, PEM or PuTTY private key text, or its base64 encoding.
    pub private_key: Option<SecretValue>,
    pub private_key_passphrase: Option<SecretValue>,
    /// Accepted server key fingerprints, `SHA256:…` as `ssh-keygen -lf` prints them.
    pub host_key: Option<Vec<String>>,
    /// OpenSSH known_hosts file, used when `host_key` is unset.
    pub known_hosts: Option<String>,
    /// Accept any server key. Tests only.
    pub insecure_skip_host_key_check: Option<bool>,
    /// TCP connect, handshake and auth; default 30s.
    pub connect_timeout: Option<Dur>,
}

/// Delta tables under `root` (local path, `file://` or `s3://`); `from.table` is a path below it.
/// Always read-only: never a target.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeltaConn {
    pub root: String,
    pub storage: Option<StorageConfig>,
}

/// A local DuckDB database file (06 §10): an ingest target, or a read-only source of
/// `for_each.over.query` and `bind` lists.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DuckdbConn {
    /// Database file; it must exist.
    pub path: String,
    /// ATTACH alias when the run writes it; defaults to the connection name.
    pub alias: Option<String>,
}

/// ClickHouse over its HTTP(S) interface (06 §11): a read-only source.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClickhouseConn {
    /// `https://host:8443`; or set `dsn`.
    pub url: Option<String>,
    pub user: Option<String>,
    pub password: Option<SecretValue>,
    /// Default database (default `default`).
    pub database: Option<String>,
    /// `clickhousedb://user:password@host:port/database` (as `clickhouse_connect` takes it), or
    /// an `http(s)://` URL with credentials; instead of `url`, `user`, `password` and `database`.
    pub dsn: Option<SecretValue>,
    /// Extra query settings sent with every query (`max_threads`, …).
    pub settings: Option<IndexMap<String, String>>,
}

/// A local SQLite file as a CDC serving target (06 §6).
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SqliteConn {
    /// Database file, created if missing.
    pub path: String,
    /// `PRAGMA busy_timeout` (default 5s).
    pub busy_timeout: Option<Dur>,
    /// Create missing served tables from the lake schema (default true).
    pub create_tables: Option<bool>,
}

/// Cloudflare D1 over the REST API as a CDC serving target (06 §7).
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct D1Conn {
    pub account_id: String,
    /// The database uuid; or set `database`.
    pub database_id: Option<String>,
    /// The database name, resolved through the paginated database list.
    pub database: Option<String>,
    pub api_token: SecretValue,
    /// API base (default `https://api.cloudflare.com/client/v4`), e.g. a local fake.
    pub endpoint: Option<String>,
    /// Statements per request, sent as a `batch` body when above 1 (default 1; see spike S11).
    pub statements_per_request: Option<u32>,
    /// Create missing served tables from the lake schema (default true).
    pub create_tables: Option<bool>,
}

/// Turso / libSQL over Hrana as a CDC serving target (06 §8).
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TursoConn {
    /// `libsql://`, `https://` or `http://`.
    pub url: String,
    pub auth_token: Option<SecretValue>,
    /// Windows with more rows than this span several transactions (the idempotent-window
    /// scheme, 08 §6.2); default 10000.
    pub transaction_rows: Option<u64>,
    /// Create missing served tables from the lake schema (default true).
    pub create_tables: Option<bool>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ConnectionKind {
    Ducklake(DuckLakeConn),
    Postgres(PostgresConn),
    Redshift(RedshiftConn),
    Rest(RestConn),
    Files(FilesConn),
    Delta(DeltaConn),
    Sqlite(SqliteConn),
    Duckdb(DuckdbConn),
    Clickhouse(ClickhouseConn),
    D1(D1Conn),
    Turso(TursoConn),
}

impl ConnectionKind {
    pub fn type_name(&self) -> &'static str {
        match self {
            ConnectionKind::Ducklake(_) => "ducklake",
            ConnectionKind::Postgres(_) => "postgres",
            ConnectionKind::Redshift(_) => "redshift",
            ConnectionKind::Rest(_) => "rest",
            ConnectionKind::Files(_) => "files",
            ConnectionKind::Delta(_) => "delta",
            ConnectionKind::Sqlite(_) => "sqlite",
            ConnectionKind::Duckdb(_) => "duckdb",
            ConnectionKind::Clickhouse(_) => "clickhouse",
            ConnectionKind::D1(_) => "d1",
            ConnectionKind::Turso(_) => "turso",
        }
    }
}

// ----------------------------------------------------------------------------- retry

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BackoffKind {
    None,
    Constant,
    Linear,
    Exponential,
    ExponentialJitter,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    /// Total attempts per unit, including the first.
    pub max_attempts: Option<u32>,
    pub backoff: Option<BackoffKind>,
    pub base: Option<Dur>,
    pub max: Option<Dur>,
    pub retry_on: Option<Vec<ErrorClass>>,
    /// Stop retrying once this much wall time has been spent in retries.
    pub budget: Option<Dur>,
}

// ----------------------------------------------------------------------------- replications

/// `replications/**/*.yaml`.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReplicationsFile {
    pub replications: IndexMap<String, Raw<ReplicationConfig>>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReplicationConfig {
    pub source: Spanned<String>,
    pub target: Spanned<String>,
    pub defaults: Option<Raw<StreamConfig>>,
    pub streams: Vec<Raw<StreamConfig>>,
    /// Dagster op; replications sharing one run in one process. Default: the file stem.
    pub op: Option<Spanned<String>>,
    /// Replications that must succeed earlier in the same run.
    #[serde(default)]
    pub after: Vec<Spanned<String>>,
    pub atomic_group: Option<Spanned<String>>,
    pub lock_group: Option<Spanned<String>>,
    pub lock_timeout: Option<Dur>,
    /// Replication-level retry (commit retries), inherited by the streams.
    pub retry: Option<Raw<RetryConfig>>,
    pub replication_retries: Option<u32>,
    pub timeout: Option<Dur>,
    pub engine: Option<EngineOverrides>,
    /// Shorthand for `defaults.asset`, merged beneath it.
    pub asset: Option<Raw<AssetConfig>>,
    /// Multi-hop: hops that run from this replication's target once it commits (fan-out when
    /// several). Each hop commits on its own, upstream first (03 §2).
    pub then: Option<Vec<Raw<HopConfig>>>,
    /// Per-tenant fan-out: one independent, atomic instance per item.
    pub for_each: Option<Spanned<ForEachConfig>>,
    /// Postgres sources: read every member under one exported snapshot (`shared`) instead of one
    /// snapshot per member (06 §3.2).
    pub source_snapshot: Option<Spanned<SourceSnapshot>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceSnapshot {
    #[default]
    PerMember,
    Shared,
}

/// One `then:` entry. Its source is the upstream hop's target. Streams inherit the project
/// defaults, the replication's `asset`/`retry` shorthand, then the hop's own `asset`/`defaults`.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HopConfig {
    pub target: Spanned<String>,
    pub defaults: Option<Raw<StreamConfig>>,
    pub streams: Vec<Raw<StreamConfig>>,
    pub asset: Option<Raw<AssetConfig>>,
    pub then: Option<Vec<Raw<HopConfig>>>,
}

/// Replication-level `for_each` (11 §3 (c)).
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ForEachConfig {
    pub over: ForEachOver,
    /// Template variable the item is bound to (plan phase and REST requests).
    #[serde(rename = "as")]
    pub as_name: String,
    /// Map items: the field that identifies an instance (its state and report name). Scalar
    /// items are their own key.
    pub key: Option<String>,
}

/// A literal list, or `{query, connection}` rows (one column: the value; more: a map).
#[derive(Clone, Debug, PartialEq, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum ForEachOver {
    List(Vec<serde_json::Value>),
    Query(ForEachQuery),
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ForEachQuery {
    /// SQL, never templated.
    pub query: String,
    /// A lake (run in the replication's engine session) or a postgres connection.
    pub connection: String,
}

impl<'de> Deserialize<'de> for ForEachOver {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = ForEachOver;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a list of items or `{query, connection}`")
            }
            fn visit_seq<A: de::SeqAccess<'de>>(self, mut s: A) -> Result<ForEachOver, A::Error> {
                let mut out = Vec::new();
                while let Some(v) = s.next_element::<serde_json::Value>()? {
                    out.push(v);
                }
                Ok(ForEachOver::List(out))
            }
            fn visit_map<A: MapAccess<'de>>(self, m: A) -> Result<ForEachOver, A::Error> {
                ForEachQuery::deserialize(de::value::MapAccessDeserializer::new(m))
                    .map(ForEachOver::Query)
            }
        }
        d.deserialize_any(V)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum Mode {
    FullRefresh,
    Incremental,
    Snapshot,
    Cdc,
}

impl Mode {
    pub fn is_ingest(self) -> bool {
        !matches!(self, Mode::Cdc)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Mode::FullRefresh => "full-refresh",
            Mode::Incremental => "incremental",
            Mode::Snapshot => "snapshot",
            Mode::Cdc => "cdc",
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StreamConfig {
    /// Stable, project-wide unique id (`[a-z0-9_./-]+`).
    pub id: Option<Spanned<String>>,
    pub enabled: Option<bool>,
    pub tags: Option<Vec<String>>,
    pub from: Option<Spanned<FromConfig>>,
    /// Target table, templated with `member.*`, `stream.*`, `run.*`.
    pub object: Option<Spanned<String>>,
    pub mode: Option<Spanned<Mode>>,
    pub primary_key: Option<Spanned<Vec<String>>>,
    pub update_key: Option<Spanned<UpdateKey>>,
    /// Source filter (ingest) or served-row filter (cdc); `where` is an alias.
    #[serde(alias = "where")]
    pub window: Option<Spanned<String>>,
    pub chunk: Option<ChunkConfig>,
    pub columns: Option<ColumnsConfig>,
    pub deletes: Option<Spanned<DeletesConfig>>,
    pub on_decode_error: Option<OnErrorConfig>,
    pub on_cast_error: Option<OnErrorConfig>,
    pub on_empty: Option<Spanned<OnEmpty>>,
    pub guard: Option<Guard>,
    pub infer: Option<Infer>,
    /// Inline source schema; the default location is `schemas/<stream id>.yaml`.
    pub schema: Option<Spanned<SourceSchema>>,
    pub table_drift: Option<TableDrift>,
    pub metadata: Option<Spanned<MetadataConfig>>,
    pub hooks: Option<Hooks>,
    /// Streams in this replication to apply first (FK order).
    pub after: Option<Vec<Spanned<String>>>,
    pub retry: Option<RetryConfig>,
    pub asset: Option<AssetConfig>,
    pub vars: Option<IndexMap<String, serde_json::Value>>,
    pub null_keys: Option<NullKeys>,
    pub duplicates: Option<Duplicates>,
    pub order_by: Option<Vec<String>>,
    pub naming: Option<Naming>,
    pub flatten: Option<u32>,
    pub diff: Option<DiffConfig>,
    pub on_gap: Option<OnGap>,
    pub max_changes: Option<u64>,
    pub max_snapshots: Option<u64>,
    pub max_duration: Option<Dur>,
    /// Postgres `changes` streams: append every change (`_atlatl_op`, `_atlatl_synced_at`)
    /// instead of upserting by key.
    pub history: Option<bool>,
    /// `from.tables` streams: overrides for members matching a name or glob (an exact name wins,
    /// then the first matching glob).
    pub members: Option<IndexMap<String, MemberConfig>>,
    /// Lists listed when the run starts, spliced into `from.query` as `{{ bind.<name> }}`
    /// (clickhouse sources, 04 §12).
    pub bind: Option<IndexMap<String, BindConfig>>,
}

/// One `bind` list: `query`'s rows as SQL literals, a tuple per row when it returns several
/// columns.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BindConfig {
    /// SQL, never templated.
    pub query: String,
    /// Run instead of `query` while the stream's target doesn't exist yet (its first run), so
    /// `query` can read the target. SQL, never templated.
    pub fallback: Option<String>,
    /// A lake, duckdb or postgres connection, as for `for_each.over`.
    pub connection: String,
    /// Split the list into lists of at most this many rows, one source query each.
    pub batch: Option<u64>,
    /// Batches in flight at once (a batched list only). Default 1. The source connection's
    /// `max_concurrency` caps every query on it across the run.
    pub concurrency: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MemberConfig {
    /// This member's key, over the stream's `primary_key` and the one discovered in the source.
    pub primary_key: Option<Vec<String>>,
}

impl StreamConfig {
    /// The `members` override for `member`: an exact name, else the first matching glob.
    pub fn member_config(&self, member: &str) -> Option<&MemberConfig> {
        let m = self.members.as_ref()?;
        m.get(member).or_else(|| {
            m.iter()
                .find(|(g, _)| glob_match(member, g))
                .map(|(_, c)| c)
        })
    }
}

/// `*` and `?` glob over bytes (member names, 06 §3.2).
pub fn glob_match(name: &str, glob: &str) -> bool {
    fn m(n: &[u8], g: &[u8]) -> bool {
        match (g.first(), n.first()) {
            (None, None) => true,
            (Some(b'*'), _) => m(n, &g[1..]) || (!n.is_empty() && m(&n[1..], g)),
            (Some(b'?'), Some(_)) => m(&n[1..], &g[1..]),
            (Some(a), Some(b)) if a == b => m(&n[1..], &g[1..]),
            _ => false,
        }
    }
    m(name.as_bytes(), glob.as_bytes())
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FromConfig {
    pub table: Option<String>,
    /// Glob; members expand in sorted order.
    pub tables: Option<String>,
    pub exclude: Option<Vec<String>>,
    pub include_views: Option<bool>,
    pub on_no_match: Option<OnNoMatch>,
    pub query: Option<String>,
    pub endpoint: Option<String>,
    pub files: Option<FilesSelector>,
    /// Delta sources: read this table version instead of the head.
    pub version: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnNoMatch {
    Error,
    Warn,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FileFormat {
    Csv,
    Parquet,
    Json,
    Jsonl,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FilesSelector {
    pub url: String,
    pub glob: Option<String>,
    pub format: Option<FileFormat>,
    pub archive: Option<ArchiveSelector>,
    pub csv: Option<CsvOptions>,
    pub union_by_name: Option<bool>,
    /// `url` is a Redshift UNLOAD manifest listing the files.
    pub manifest: Option<bool>,
    /// Adds `k=v` path segments as (VARCHAR) columns.
    pub hive_partitioning: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ArchiveSelector {
    pub member: String,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CsvOptions {
    pub header: Option<bool>,
    pub delimiter: Option<String>,
    pub quote: Option<String>,
    pub null_values: Option<Vec<String>>,
    pub encoding: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Nulls {
    Exclude,
    Include,
}

/// `update_key: col` or `{column, lookback, inclusive, nulls}`.
#[derive(Clone, Debug, PartialEq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdateKey {
    pub column: String,
    pub lookback: Option<Dur>,
    pub inclusive: Option<bool>,
    pub nulls: Option<Nulls>,
}

impl<'de> Deserialize<'de> for UpdateKey {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Full {
            column: String,
            lookback: Option<Dur>,
            inclusive: Option<bool>,
            nulls: Option<Nulls>,
        }
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = UpdateKey;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a column name or `{column, lookback, inclusive, nulls}`")
            }
            fn visit_str<E: de::Error>(self, s: &str) -> Result<UpdateKey, E> {
                Ok(UpdateKey {
                    column: s.to_owned(),
                    lookback: None,
                    inclusive: None,
                    nulls: None,
                })
            }
            fn visit_map<A: MapAccess<'de>>(self, m: A) -> Result<UpdateKey, A::Error> {
                let f = Full::deserialize(de::value::MapAccessDeserializer::new(m))?;
                Ok(UpdateKey {
                    column: f.column,
                    lookback: f.lookback,
                    inclusive: f.inclusive,
                    nulls: f.nulls,
                })
            }
        }
        d.deserialize_any(V)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ChunkBy {
    Files,
    Hash,
    Range,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChunkConfig {
    pub by: ChunkBy,
    pub size: Option<ByteSize>,
    pub columns: Option<Vec<String>>,
    pub buckets: Option<u32>,
    pub column: Option<String>,
    pub step: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ColumnConfig {
    /// Lake-side (DuckDB) type for new tables and new columns, and the staging cast target.
    #[serde(rename = "type")]
    pub kind: Option<String>,
    /// DuckDB SQL expression evaluated before apply.
    pub transform: Option<String>,
    /// Native type on a serving target (`$col::<type>` in the apply).
    pub target_type: Option<String>,
    pub source_name: Option<String>,
}

/// `columns:`: `include`/`exclude` lists plus per-column settings (`"*"` for unlisted columns).
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct ColumnsConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude: Option<Vec<String>>,
    #[serde(flatten)]
    pub columns: IndexMap<String, ColumnConfig>,
}

impl<'de> Deserialize<'de> for ColumnsConfig {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = ColumnsConfig;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a columns map")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut m: A) -> Result<ColumnsConfig, A::Error> {
                let mut out = ColumnsConfig::default();
                while let Some(k) = m.next_key::<String>()? {
                    match k.as_str() {
                        "include" => out.include = Some(m.next_value()?),
                        "exclude" => out.exclude = Some(m.next_value()?),
                        _ => {
                            let c: ColumnConfig = m.next_value()?;
                            out.columns.insert(k, c);
                        }
                    }
                }
                Ok(out)
            }
        }
        d.deserialize_map(V)
    }
}

impl JsonSchema for ColumnsConfig {
    fn schema_name() -> Cow<'static, str> {
        "ColumnsConfig".into()
    }
    fn json_schema(g: &mut SchemaGenerator) -> Schema {
        let col = g.subschema_for::<ColumnConfig>();
        json_schema!({
            "type": "object",
            "properties": {
                "include": {"type": "array", "items": {"type": "string"}},
                "exclude": {"type": "array", "items": {"type": "string"}}
            },
            "additionalProperties": col
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeleteMode {
    Soft,
    Hard,
    Ignore,
}

/// `snapshot_time` | `now` | `true` (boolean soft-delete column).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeleteValue {
    SnapshotTime,
    Now,
    True,
}

impl<'de> Deserialize<'de> for DeleteValue {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = DeleteValue;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("`snapshot_time`, `now` or `true`")
            }
            fn visit_bool<E: de::Error>(self, b: bool) -> Result<DeleteValue, E> {
                if b {
                    Ok(DeleteValue::True)
                } else {
                    Err(E::custom("`value: false` is not meaningful; use `true`"))
                }
            }
            fn visit_str<E: de::Error>(self, s: &str) -> Result<DeleteValue, E> {
                match s {
                    "snapshot_time" => Ok(DeleteValue::SnapshotTime),
                    "now" => Ok(DeleteValue::Now),
                    _ => Err(E::unknown_variant(s, &["snapshot_time", "now", "true"])),
                }
            }
        }
        d.deserialize_any(V)
    }
}

impl JsonSchema for DeleteValue {
    fn schema_name() -> Cow<'static, str> {
        "DeleteValue".into()
    }
    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({"enum": ["snapshot_time", "now", true]})
    }
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeletesConfig {
    pub mode: Option<DeleteMode>,
    pub column: Option<String>,
    pub value: Option<DeleteValue>,
    pub restore: Option<bool>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ErrorRetry {
    Request,
    Row,
    None,
}

#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Threshold {
    pub ratio: Option<f64>,
    pub min_count: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OnErrorConfig {
    pub retry: Option<ErrorRetry>,
    pub reject_threshold: Option<Threshold>,
    /// Open rejects older than this (from `first_seen_at`) expire: deleted in the commit (03 §7).
    pub reject_ttl: Option<Dur>,
    /// `on_cast_error` only: a failed cast becomes NULL with a counted warning.
    pub set_null: Option<bool>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnEmpty {
    Mirror,
    Skip,
    Fail,
}

#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Guard {
    pub min_rows: Option<u64>,
    pub max_shrink: Option<f64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Infer {
    Text,
    Safe,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NewColumns {
    Add,
    Ignore,
    Fail,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TypeChanges {
    Fail,
    Widen,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MissingColumns {
    Warn,
    Fail,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TableDrift {
    pub new_columns: Option<NewColumns>,
    pub type_changes: Option<TypeChanges>,
    pub missing_columns: Option<MissingColumns>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LoadedAtType {
    EpochSInt,
    Timestamptz,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LoadedAt {
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub kind: Option<LoadedAtType>,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NamedColumn {
    pub name: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MetadataConfig {
    pub loaded_at: Option<LoadedAt>,
    pub run_id: Option<NamedColumn>,
    pub source: Option<NamedColumn>,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Hooks {
    pub pre_apply: Option<Vec<Spanned<String>>>,
    pub post_apply: Option<Vec<Spanned<String>>>,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AssetConfig {
    /// Slash-separated Dagster asset key. Default: the stream id.
    pub key: Option<Spanned<String>>,
    pub group: Option<String>,
    pub deps: Option<Vec<String>>,
    pub description: Option<String>,
    pub kinds: Option<Vec<String>>,
    pub tags: Option<IndexMap<String, String>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NullKeys {
    Fail,
    Skip,
    NullSafe,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Duplicates {
    Fail,
    Last,
    First,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Naming {
    Source,
    Lower,
    Snake,
    #[serde(alias = "sling_csv")]
    CleanCsv,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DiffConfig {
    pub ignore_columns: Option<Vec<String>>,
    pub chunk_rows: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnGap {
    Fail,
    Resync,
}

// ----------------------------------------------------------------------------- source schemas

/// `schemas/**/*.yaml`.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SchemaFile {
    pub schema: Spanned<SourceSchema>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SchemaMode {
    Enforce,
    Observe,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Policy {
    Ignore,
    Warn,
    Fail,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LogicalType {
    String,
    Integer,
    Decimal,
    Float,
    Boolean,
    Date,
    Timestamp,
    Timestamptz,
    Json,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FieldSpec {
    #[serde(rename = "type")]
    pub kind: LogicalType,
    pub format: Option<String>,
    pub nullable: Option<bool>,
    pub values: Option<Vec<serde_json::Value>>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceSchema {
    pub mode: Option<SchemaMode>,
    pub fields: IndexMap<String, FieldSpec>,
    pub on_new_field: Option<Policy>,
    pub on_missing_field: Option<Policy>,
    pub on_type_violation: Option<Policy>,
    pub on_null_violation: Option<Policy>,
    pub on_domain_violation: Option<Policy>,
    pub tolerance: Option<Threshold>,
}
