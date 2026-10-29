//! ClickHouse over its HTTP(S) interface (06 §11), a read-only source. Every query runs with
//! `readonly=2` (no writes) and `wait_end_of_query=1`, so a failure comes back as an HTTP status
//! rather than mid-body; the Parquet result streams to a local file that the engine reads.

pub mod bind;
pub mod incremental;
pub mod types;

use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use atlatl_core::config::model::ClickhouseConn;
use atlatl_core::config::types::SecretValue;
use atlatl_core::error::{ErrorClass, Result, classify_http_status, wait_hint};
use atlatl_core::retry::{AttemptRecord, RandomJitter, RetryLevel, RetryPolicy, retry};
use atlatl_core::{AtlatlError, redact::redact};
use percent_encoding::percent_decode_str;
use reqwest::Url;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

pub const DEFAULT_STATEMENT_TIMEOUT: Duration = Duration::from_secs(600);
const EXCEPTION_CODE: &str = "x-clickhouse-exception-code";
const PARQUET_MAGIC: &[u8] = b"PAR1";
/// Settings atlatl owns: read-only, whole answers, Parquet, the database and the statement timeout.
const RESERVED: [&str; 5] = [
    "readonly",
    "wait_end_of_query",
    "default_format",
    "database",
    "max_execution_time",
];
/// Exact decimal-to-float parsing. The default parser can be an ulp off for `Float32` (subnormals,
/// large exponents), and a `CAST('…' AS Float32)` watermark or quoted bind value would then sit
/// beside the stored value, not on it. Also atlatl's alone.
const PRECISE_FLOATS: (&str, &str) = ("precise_float_parsing", "1");
/// How types reach Parquet, which [`types`] checks against; also atlatl's alone. The legacy
/// encoder (`use_custom_encoder=0`) writes `DateTime` as epoch seconds whatever the rest say.
/// `output_format_parquet_unsupported_types_as_binary` isn't sent (it's newer than the servers
/// these must work with, and [`types::check`] refuses every type it affects), only refused.
const PARQUET_TYPES: [(&str, &str); 4] = [
    ("output_format_parquet_date_as_uint16", "0"),
    ("output_format_parquet_datetime_as_uint32", "0"),
    ("output_format_parquet_string_as_string", "1"),
    ("output_format_parquet_use_custom_encoder", "1"),
];
/// The `output_format_parquet_*` settings a connection or query may set: file layout only, checked
/// not to change what lands. Any other (a new one included) might, so it's refused.
const PARQUET_TUNING: [&str; 13] = [
    "output_format_parquet_compression_method",
    "output_format_parquet_row_group_size",
    "output_format_parquet_row_group_size_bytes",
    "output_format_parquet_data_page_size",
    "output_format_parquet_batch_size",
    "output_format_parquet_parallel_encoding",
    "output_format_parquet_write_page_index",
    "output_format_parquet_write_bloom_filter",
    "output_format_parquet_bloom_filter_bits_per_value",
    "output_format_parquet_bloom_filter_flush_threshold_bytes",
    "output_format_parquet_max_dictionary_size",
    "output_format_parquet_write_checksums",
    "output_format_parquet_version",
];
const PARQUET_PREFIX: &str = "output_format_parquet_";

/// Where and as whom queries run, from the connection's fields or its `dsn`.
#[derive(Clone, PartialEq, Eq)]
pub struct Endpoint {
    /// `http(s)://host:port`, no path.
    pub base: String,
    pub user: String,
    pub password: String,
    pub database: String,
}

impl std::fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Endpoint")
            .field("base", &self.base)
            .field("user", &self.user)
            .field("database", &self.database)
            .finish_non_exhaustive()
    }
}

impl Endpoint {
    pub fn from_conn(c: &ClickhouseConn) -> Result<Self> {
        match (&c.dsn, &c.url) {
            (Some(dsn), None)
                if c.user.is_none() && c.password.is_none() && c.database.is_none() =>
            {
                parse_dsn(&secret(dsn, "dsn")?)
            }
            (Some(_), _) => Err(AtlatlError::config(
                "a clickhouse connection takes `dsn` or `url`/`user`/`password`/`database`, not both",
            )),
            (None, Some(url)) => {
                let u = Url::parse(url)
                    .map_err(|e| AtlatlError::config(format!("clickhouse `url`: {e}")))?;
                if !matches!(u.scheme(), "http" | "https") {
                    return Err(AtlatlError::config(format!(
                        "clickhouse `url` must be http(s)://, not {}://",
                        u.scheme()
                    )));
                }
                if !u.username().is_empty() || u.password().is_some() {
                    return Err(AtlatlError::config(
                        "clickhouse `url` carries no credentials; set `user` and `password`",
                    ));
                }
                Ok(Self {
                    base: origin(&u)?,
                    user: c.user.clone().unwrap_or_else(|| "default".into()),
                    password: match &c.password {
                        Some(p) => secret(p, "password")?,
                        None => String::new(),
                    },
                    database: c.database.clone().unwrap_or_else(|| "default".into()),
                })
            }
            (None, None) => Err(AtlatlError::config(
                "a clickhouse connection needs `url` or `dsn`",
            )),
        }
    }
}

fn secret(v: &SecretValue, what: &str) -> Result<String> {
    match v {
        SecretValue::Literal(s) => Ok(s.clone()),
        SecretValue::Ref(_) => Err(AtlatlError::internal(format!(
            "clickhouse `{what}` secret was not resolved before connecting"
        ))),
    }
}

/// `clickhousedb://` / `clickhouse://` (TLS on ports 443 and 8443 or with `secure=true`, as
/// `clickhouse_connect` does) or `http(s)://`, with optional credentials and `/database`.
pub fn parse_dsn(dsn: &str) -> Result<Endpoint> {
    // Never echo the DSN: it holds the password.
    let u = Url::parse(dsn).map_err(|e| AtlatlError::config(format!("clickhouse `dsn`: {e}")))?;
    let secure_param = u
        .query_pairs()
        .any(|(k, v)| k == "secure" && matches!(v.as_ref(), "true" | "1"));
    let scheme = match u.scheme() {
        "http" | "https" => u.scheme().to_owned(),
        "clickhousedb" | "clickhouse" => {
            let tls = secure_param || matches!(u.port(), Some(443 | 8443));
            if tls { "https" } else { "http" }.to_owned()
        }
        other => {
            return Err(AtlatlError::config(format!(
                "clickhouse `dsn` scheme must be clickhousedb, clickhouse, http or https, not {other}"
            )));
        }
    };
    let host = u
        .host_str()
        .ok_or_else(|| AtlatlError::config("clickhouse `dsn` has no host"))?;
    // An http(s) URL means its usual port; a clickhouse one, ClickHouse's.
    let port = match u.scheme() {
        "http" | "https" => u.port_or_known_default(),
        _ => u.port(),
    }
    .unwrap_or(if scheme == "https" { 8443 } else { 8123 });
    let decode = |s: &str| percent_decode_str(s).decode_utf8_lossy().into_owned();
    let user = match u.username() {
        "" => "default".to_owned(),
        n => decode(n),
    };
    let database = match u.path().trim_matches('/') {
        "" => "default".to_owned(),
        d => decode(d),
    };
    Ok(Endpoint {
        base: format!("{scheme}://{host}:{port}"),
        user,
        password: u.password().map(decode).unwrap_or_default(),
        database,
    })
}

fn origin(u: &Url) -> Result<String> {
    let host = u
        .host_str()
        .ok_or_else(|| AtlatlError::config("clickhouse `url` has no host"))?;
    let port = u.port_or_known_default().unwrap_or(8123);
    Ok(format!("{}://{host}:{port}", u.scheme()))
}

#[derive(Clone, Debug)]
pub struct Options {
    pub connect_timeout: Duration,
    /// Sent as `max_execution_time`; the HTTP request gets a minute more.
    pub statement_timeout: Duration,
    pub retry: RetryPolicy,
    pub settings: Vec<(String, String)>,
    /// Queries in flight on the connection (`max_concurrency`), one permit each; shared by every
    /// client made for it in a run.
    pub slots: Option<Arc<Semaphore>>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(60),
            statement_timeout: DEFAULT_STATEMENT_TIMEOUT,
            retry: RetryPolicy::default(),
            settings: Vec::new(),
            slots: None,
        }
    }
}

/// A query's columns, from `DESCRIBE`.
#[derive(Debug, Default)]
pub struct Described {
    pub columns: Vec<(String, String)>,
    pub retries: Vec<AttemptRecord>,
}

/// One query's result on disk.
#[derive(Debug, Default)]
pub struct Fetched {
    pub bytes: u64,
    pub retries: Vec<AttemptRecord>,
}

#[derive(Clone)]
pub struct ClickHouse {
    http: reqwest::Client,
    ep: Endpoint,
    opts: Options,
}

impl ClickHouse {
    pub fn new(conn: &ClickhouseConn, mut opts: Options) -> Result<Self> {
        if let Some(s) = &conn.settings {
            opts.settings
                .extend(s.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
        Self::with_endpoint(Endpoint::from_conn(conn)?, opts)
    }

    pub fn with_endpoint(ep: Endpoint, opts: Options) -> Result<Self> {
        for (k, _) in &opts.settings {
            let k = k.to_ascii_lowercase();
            if RESERVED.contains(&k.as_str()) {
                return Err(AtlatlError::config(format!(
                    "clickhouse setting `{k}` is set by atlatl (it keeps queries read-only and whole); use `database` or `timeouts.statement` for those"
                )));
            }
            if k == PRECISE_FLOATS.0 {
                return Err(AtlatlError::config(format!(
                    "clickhouse setting `{k}` is set by atlatl: watermarks and bind values rely on exact float parsing"
                )));
            }
            if k.starts_with(PARQUET_PREFIX) && !PARQUET_TUNING.contains(&k.as_str()) {
                return Err(AtlatlError::config(format!(
                    "clickhouse setting `{k}` could change what types land; only Parquet layout settings (compression, row groups, pages, bloom filters) may be set, so convert columns in `from.query` instead"
                )));
            }
        }
        let http = atlatl_core::tls::http_client()
            .connect_timeout(opts.connect_timeout)
            .timeout(opts.statement_timeout + Duration::from_secs(60))
            .build()
            .map_err(|e| AtlatlError::internal(format!("clickhouse client: {e}")))?;
        Ok(Self { http, ep, opts })
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.ep
    }

    /// Runs `sql` and writes its Parquet result to `path` (replaced on each attempt).
    pub async fn query_to_file(
        &self,
        sql: &str,
        path: &Path,
        cancel: &CancellationToken,
    ) -> Result<Fetched> {
        let (bytes, retries) = self.attempts(cancel, || self.attempt(sql, path)).await?;
        Ok(Fetched { bytes, retries })
    }

    /// `query`'s result columns (name and ClickHouse type), from `DESCRIBE`; nothing runs.
    pub async fn describe(&self, query: &str, cancel: &CancellationToken) -> Result<Described> {
        let sql = format!(
            "DESCRIBE ({}\n) FORMAT JSONCompactEachRow",
            types::trimmed(query)
        );
        let (body, retries) = self
            .attempts(cancel, || async {
                self.post(&sql).await?.text().await.map_err(transport)
            })
            .await?;
        let columns = body
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                let row: Vec<serde_json::Value> = serde_json::from_str(l).map_err(|e| {
                    AtlatlError::new(ErrorClass::Decode, format!("clickhouse DESCRIBE row: {e}"))
                })?;
                match (row.first(), row.get(1)) {
                    (Some(serde_json::Value::String(n)), Some(serde_json::Value::String(t))) => {
                        Ok((n.clone(), t.clone()))
                    }
                    _ => Err(AtlatlError::new(
                        ErrorClass::Decode,
                        format!("clickhouse DESCRIBE row without a name and type: {l}"),
                    )),
                }
            })
            .collect::<Result<_>>()?;
        Ok(Described { columns, retries })
    }

    /// Runs `op` under `Options::retry`, each attempt holding one of the connection's slots and
    /// giving up when `cancel` fires.
    async fn attempts<T, F, Fut>(
        &self,
        cancel: &CancellationToken,
        op: F,
    ) -> Result<(T, Vec<AttemptRecord>)>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut retries = Vec::new();
        let mut jitter = RandomJitter::new();
        let v = retry(
            &self.opts.retry,
            RetryLevel::Query,
            &mut jitter,
            |a| retries.push(a.clone()),
            |_| async {
                tokio::select! {
                    r = async {
                        let _slot = match &self.opts.slots {
                            Some(s) => Some(s.acquire().await.map_err(|_| {
                                AtlatlError::internal("clickhouse connection slots closed")
                            })?),
                            None => None,
                        };
                        op().await
                    } => r,
                    _ = cancel.cancelled() => Err(AtlatlError::new(ErrorClass::Killed, "killed during a clickhouse query")),
                }
            },
        )
        .await
        .map_err(|e| e.error)?;
        Ok((v, retries))
    }

    fn url(&self) -> Result<Url> {
        let mut u = Url::parse(&format!("{}/", self.ep.base))
            .map_err(|e| AtlatlError::config(format!("clickhouse endpoint: {e}")))?;
        {
            let mut q = u.query_pairs_mut();
            q.append_pair("database", &self.ep.database)
                .append_pair("default_format", "Parquet")
                .append_pair("readonly", "2")
                .append_pair("wait_end_of_query", "1")
                .append_pair(
                    "max_execution_time",
                    &self.opts.statement_timeout.as_secs().max(1).to_string(),
                );
            // Server defaults differ (ClickHouse Cloud sends `Date` as days in a UInt16): ask for
            // logical types. A connection's `settings` can't override these (`with_endpoint`).
            for (k, v) in PARQUET_TYPES {
                q.append_pair(k, v);
            }
            q.append_pair(PRECISE_FLOATS.0, PRECISE_FLOATS.1);
            for (k, v) in &self.opts.settings {
                q.append_pair(k, v);
            }
        }
        Ok(u)
    }

    /// Sends `sql`; a non-success answer is a classified error.
    async fn post(&self, sql: &str) -> Result<reqwest::Response> {
        let resp = self
            .http
            .post(self.url()?)
            .header("X-ClickHouse-User", &self.ep.user)
            .header("X-ClickHouse-Key", &self.ep.password)
            .body(sql.to_owned())
            .send()
            .await
            .map_err(transport)?;
        let status = resp.status().as_u16();
        if !resp.status().is_success() {
            let code = resp
                .headers()
                .get(EXCEPTION_CODE)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.trim().parse().ok());
            let wait = wait_hint(
                resp.headers()
                    .iter()
                    .filter_map(|(k, v)| Some((k.as_str(), v.to_str().ok()?))),
                SystemTime::now(),
            );
            let body = resp.text().await.unwrap_or_default();
            return Err(classify(status, code, &body).retry_after(wait));
        }
        Ok(resp)
    }

    async fn attempt(&self, sql: &str, path: &Path) -> Result<u64> {
        let mut resp = self.post(sql).await?;
        let status = resp.status().as_u16();
        let mut f = tokio::fs::File::create(path)
            .await
            .map_err(|e| AtlatlError::internal(format!("create {}: {e}", path.display())))?;
        let mut n = 0u64;
        let mut head = Vec::with_capacity(PARQUET_MAGIC.len());
        while let Some(chunk) = resp.chunk().await.map_err(transport)? {
            if head.len() < PARQUET_MAGIC.len() {
                let take = (PARQUET_MAGIC.len() - head.len()).min(chunk.len());
                head.extend_from_slice(&chunk[..take]);
            }
            f.write_all(&chunk)
                .await
                .map_err(|e| AtlatlError::internal(format!("write {}: {e}", path.display())))?;
            n += chunk.len() as u64;
        }
        f.flush()
            .await
            .map_err(|e| AtlatlError::internal(format!("write {}: {e}", path.display())))?;
        if head != PARQUET_MAGIC {
            return Err(AtlatlError::new(
                ErrorClass::Decode,
                format!(
                    "clickhouse answered {status} without a Parquet body ({n} bytes); does `from.query` set its own FORMAT?"
                ),
            ));
        }
        Ok(n)
    }
}

/// A setting atlatl owns that `query` names (in a `SETTINGS` clause): `precise_float_parsing`, or
/// an `output_format_parquet_*` one other than layout tuning, which could change what types land
/// behind [`types::check`]'s back. A mention in a comment or string counts too.
pub fn sets_owned_setting(query: &str) -> Option<String> {
    let q = query.to_ascii_lowercase();
    if q.contains(PRECISE_FLOATS.0) {
        return Some(PRECISE_FLOATS.0.to_owned());
    }
    q.match_indices(PARQUET_PREFIX)
        .map(|(i, _)| {
            q[i..]
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect::<String>()
        })
        .find(|k| !PARQUET_TUNING.contains(&k.as_str()))
}

/// A backtick-quoted ClickHouse identifier.
pub fn ident(name: &str) -> String {
    format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
}

/// `database.table` as a quoted ClickHouse name. Only the first dot separates (ClickHouse names
/// have two levels), so a table name may contain dots.
pub fn table_name(name: &str) -> String {
    match name.split_once('.') {
        Some((db, t)) => format!("{}.{}", ident(db), ident(t)),
        None => ident(name),
    }
}

fn transport(e: reqwest::Error) -> AtlatlError {
    let class = if e.is_timeout() {
        ErrorClass::Timeout
    } else if e.is_builder() {
        ErrorClass::Config
    } else {
        ErrorClass::Transient
    };
    AtlatlError::new(class, redact(&format!("clickhouse: {e}")).into_owned())
}

/// An error answer: by ClickHouse exception code where it decides, else by HTTP status.
pub fn classify(status: u16, code: Option<i64>, body: &str) -> AtlatlError {
    let code = code.or_else(|| {
        body.trim_start()
            .strip_prefix("Code: ")
            .and_then(|r| r.split(|c: char| !c.is_ascii_digit()).next())
            .and_then(|d| d.parse().ok())
    });
    // ClickHouse answers most query mistakes with HTTP 500, so a coded error is `config` unless
    // its code is a server-side condition worth retrying.
    let class = match code {
        // AUTHENTICATION_FAILED, UNKNOWN_USER, REQUIRED_PASSWORD, ACCESS_DENIED
        Some(516 | 192 | 194 | 497) => ErrorClass::Auth,
        // TIMEOUT_EXCEEDED, SOCKET_TIMEOUT
        Some(159 | 209) => ErrorClass::Timeout,
        // TOO_MANY_SIMULTANEOUS_QUERIES
        Some(202) => ErrorClass::Throttled,
        // UNEXPECTED_END_OF_FILE, ATTEMPT_TO_READ_AFTER_EOF, NETWORK_ERROR, ABORTED,
        // MEMORY_LIMIT_EXCEEDED, ALL_CONNECTION_TRIES_FAILED, TOO_FEW_LIVE_REPLICAS,
        // QUERY_WAS_CANCELLED, SYSTEM_ERROR, CANNOT_SCHEDULE_TASK, KEEPER_EXCEPTION, POCO_EXCEPTION
        Some(3 | 32 | 210 | 236 | 241 | 279 | 285 | 394 | 425 | 439 | 999 | 1000) => {
            ErrorClass::Transient
        }
        Some(_) => ErrorClass::Config,
        None => classify_http_status(status).unwrap_or(ErrorClass::Transient),
    };
    let mut text: String = body.trim().chars().take(2000).collect();
    if text.is_empty() {
        text = format!("HTTP {status}");
    }
    let e = AtlatlError::new(class, redact(&format!("clickhouse: {text}")).into_owned());
    match code {
        Some(c) => e.code(format!("clickhouse_{c}")),
        None => e,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dsn_forms() {
        let e = parse_dsn("clickhousedb://u:p%40ss@h.example:8443/db").expect("dsn");
        assert_eq!(e.base, "https://h.example:8443");
        assert_eq!((e.user.as_str(), e.password.as_str()), ("u", "p@ss"));
        assert_eq!(e.database, "db");
        let e = parse_dsn("clickhouse://h:8123").expect("dsn");
        assert_eq!(e.base, "http://h:8123");
        assert_eq!(
            (e.user.as_str(), e.database.as_str()),
            ("default", "default")
        );
        let e = parse_dsn("clickhouse://h:9440?secure=true").expect("dsn");
        assert_eq!(e.base, "https://h:9440");
        let e = parse_dsn("https://u:p@h").expect("dsn");
        assert_eq!(e.base, "https://h:443");
        let e = parse_dsn("clickhousedb://h").expect("dsn");
        assert_eq!(e.base, "http://h:8123");
        assert!(parse_dsn("postgres://h").is_err());
    }

    #[test]
    fn asks_for_logical_parquet_types() {
        let c = ClickHouse::with_endpoint(
            parse_dsn("clickhouse://h:8123/db").expect("dsn"),
            Options {
                settings: vec![
                    ("max_threads".into(), "2".into()),
                    (
                        "output_format_parquet_compression_method".into(),
                        "lz4".into(),
                    ),
                ],
                ..Options::default()
            },
        )
        .expect("client");
        let q = c.url().expect("url");
        let pairs: Vec<(String, String)> = q.query_pairs().into_owned().collect();
        assert!(
            !pairs
                .iter()
                .any(|p| p.0 == "output_format_parquet_unsupported_types_as_binary"),
            "not sent: older servers don't know it"
        );
        for (k, v) in [
            ("readonly", "2"),
            ("output_format_parquet_date_as_uint16", "0"),
            ("output_format_parquet_datetime_as_uint32", "0"),
            ("output_format_parquet_string_as_string", "1"),
            ("output_format_parquet_use_custom_encoder", "1"),
            ("precise_float_parsing", "1"),
            ("max_threads", "2"),
        ] {
            assert!(pairs.iter().any(|p| p.0 == k && p.1 == v), "{k}={v}");
        }
    }

    #[test]
    fn queries_cant_set_owned_settings() {
        let set = |q: &str| sets_owned_setting(q);
        assert_eq!(
            set("SELECT d FROM t SETTINGS Output_Format_Parquet_Date_As_UInt16 = 1").as_deref(),
            Some("output_format_parquet_date_as_uint16")
        );
        assert_eq!(
            set("SELECT 1 SETTINGS output_format_parquet_compression_method = 'lz4', output_format_parquet_use_custom_encoder=0").as_deref(),
            Some("output_format_parquet_use_custom_encoder")
        );
        assert_eq!(
            set("SELECT 1 SETTINGS PRECISE_FLOAT_PARSING = 0").as_deref(),
            Some("precise_float_parsing")
        );
        assert_eq!(
            set("SELECT 1 SETTINGS output_format_parquet_some_new_thing = 1").as_deref(),
            Some("output_format_parquet_some_new_thing")
        );
        assert_eq!(
            set(
                "SELECT 1 SETTINGS output_format_parquet_compression_method = 'lz4', output_format_parquet_row_group_size = 10"
            ),
            None
        );
    }

    #[test]
    fn table_names() {
        assert_eq!(table_name("default.eod"), "`default`.`eod`");
        assert_eq!(table_name("db.a.b"), "`db`.`a.b`");
        assert_eq!(table_name("t`x"), "`t\\`x`");
    }

    #[test]
    fn reserved_settings_are_refused() {
        for k in [
            "readonly",
            "WAIT_END_OF_QUERY",
            "default_format",
            "database",
            "max_execution_time",
            "output_format_parquet_date_as_uint16",
            "Output_Format_Parquet_String_As_String",
            "output_format_parquet_unsupported_types_as_binary",
            "output_format_parquet_use_custom_encoder",
            "output_format_parquet_fixed_string_as_fixed_byte_array",
            "precise_float_parsing",
        ] {
            let e = ClickHouse::with_endpoint(
                parse_dsn("clickhouse://h").expect("dsn"),
                Options {
                    settings: vec![(k.into(), "0".into())],
                    ..Options::default()
                },
            )
            .err()
            .unwrap_or_else(|| panic!("{k} accepted"));
            assert_eq!(e.class, ErrorClass::Config, "{k}");
        }
    }

    #[test]
    fn dsn_errors_never_echo_the_password() {
        let e = parse_dsn("clickhousedb://u:hunter2@").expect_err("no host");
        assert!(!e.message.contains("hunter2"), "{}", e.message);
    }

    #[test]
    fn classifies_by_code_then_status() {
        let c = |s, code, body: &str| classify(s, code, body).class;
        assert_eq!(c(500, Some(164), ""), ErrorClass::Config);
        assert_eq!(
            c(400, None, "Code: 62. DB::Exception: Syntax error"),
            ErrorClass::Config
        );
        assert_eq!(c(403, Some(516), ""), ErrorClass::Auth);
        assert_eq!(c(500, Some(159), ""), ErrorClass::Timeout);
        assert_eq!(c(500, Some(202), ""), ErrorClass::Throttled);
        assert_eq!(c(500, Some(241), ""), ErrorClass::Transient);
        assert_eq!(
            c(500, Some(184), ""),
            ErrorClass::Config,
            "ILLEGAL_AGGREGATION"
        );
        assert_eq!(c(503, None, ""), ErrorClass::Transient);
        assert_eq!(c(404, None, ""), ErrorClass::Config);
        let e = classify(500, Some(164), "Code: 164. readonly");
        assert_eq!(e.code.as_deref(), Some("clickhouse_164"));
        assert!(e.message.contains("readonly"));
    }
}
