//! Redshift connector (06 §4): statements over the Data API (hand-rolled SigV4, no AWS SDK) or
//! the PG wire protocol, `svv_columns` precision lookup, sorted wildcard discovery, and UNLOAD →
//! Parquet + MANIFEST read back by DuckDB through a prefix-scoped secret.

pub mod aws;
pub mod binary;
pub mod catalog;
pub mod creds;
pub mod dataapi;
pub mod error;
pub mod export;
pub mod gc;
pub mod result;
pub mod s3;
pub mod sigv4;
pub mod unload;
pub mod wire;
mod xml;

use std::sync::Arc;
use std::time::Duration;

use atlatl_core::config::model::{DataApiAuth, RedshiftAccess, RedshiftConn, StorageConfig};
use atlatl_core::config::types::SecretValue;
use atlatl_core::error::{AtlatlError, Result};
use atlatl_core::retry::{RandomJitter, RetryLevel, RetryPolicy, retry};
use tokio_util::sync::CancellationToken;

pub use catalog::{Object, SourceColumn};
pub use creds::{Credentials, Provider, Source};
pub use dataapi::{DataApi, DataApiOptions, Poll, Target};
pub use export::{ExportOptions, ExportRequest, Exported, RawTable, Sink};
pub use result::{Executed, Field, ResultSet};
pub use s3::{S3Client, S3Location};
pub use unload::{Chunk, Unloaded, UnloadedFile};
pub use wire::Wire;

/// Retry and timeout settings; the defaults suit production.
#[derive(Clone, Debug)]
pub struct Options {
    /// Query level: a statement that fails `transient`/`throttled` is resubmitted (an UNLOAD to a
    /// fresh prefix).
    pub query_retry: RetryPolicy,
    /// Request level: each Data API, STS and S3 call.
    pub request_retry: RetryPolicy,
    /// Object level: each unloaded file's read-back ([`Unloaded::stage`]).
    pub object_retry: RetryPolicy,
    pub http_timeout: Duration,
    /// The Data API's own limit is 24 h.
    pub statement_timeout: Duration,
    pub sts_endpoint: Option<String>,
    /// Wire sessions (`max_concurrency`).
    pub max_sessions: usize,
    /// A wire pool shared by every client on the connection, so `max_sessions` caps the run rather
    /// than each client; one is built when unset.
    pub wire: Option<Wire>,
    /// Wire export batching and stall detection.
    pub export: export::ExportOptions,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            query_retry: RetryPolicy {
                max_attempts: 3,
                ..RetryPolicy::default()
            },
            request_retry: dataapi::default_request_retry(),
            object_retry: RetryPolicy {
                max_attempts: 4,
                base: Duration::from_secs(1),
                max: Duration::from_secs(30),
                ..RetryPolicy::default()
            },
            http_timeout: Duration::from_secs(90),
            statement_timeout: Duration::from_secs(24 * 3600),
            sts_endpoint: None,
            max_sessions: 4,
            wire: None,
            export: export::ExportOptions::default(),
        }
    }
}

#[derive(Debug)]
pub enum Access {
    DataApi(Box<DataApi>),
    Wire(Wire),
}

/// What [`Redshift::unload`] needs to know about one member.
#[derive(Clone, Debug)]
pub struct UnloadRequest {
    pub run_id: String,
    pub stream: String,
    pub member: String,
    /// The replication attempt (1-based).
    pub attempt: u32,
    /// Redshift SQL, e.g. from [`unload::select_sql`].
    pub select: String,
    /// From [`Redshift::columns`]; the read-back casts to these.
    pub columns: Vec<SourceColumn>,
}

#[derive(Debug)]
struct UnloadSettings {
    bucket: String,
    base: String,
    iam_role: String,
    max_mb: u64,
}

/// A configured Redshift connection. Cheap to share by reference; holds no socket between
/// statements on the Data API path.
#[derive(Debug)]
pub struct Redshift {
    access: Access,
    unload: Option<UnloadSettings>,
    s3: Option<S3Client>,
    opts: Options,
}

fn resolved(v: Option<&SecretValue>, what: &str) -> Result<Option<String>> {
    match v {
        None => Ok(None),
        Some(SecretValue::Literal(s)) => Ok(Some(s.clone())),
        Some(SecretValue::Ref(_)) => Err(AtlatlError::internal(format!("{what} was not resolved"))),
    }
}

fn static_keys(
    key: Option<&SecretValue>,
    secret: Option<&SecretValue>,
    token: Option<&SecretValue>,
    what: &str,
) -> Result<Option<Credentials>> {
    match (
        resolved(key, &format!("{what}.key_id"))?,
        resolved(secret, &format!("{what}.secret"))?,
    ) {
        (Some(k), Some(s)) => Ok(Some(Credentials::new(
            k,
            s,
            resolved(token, &format!("{what}.session_token"))?,
        ))),
        (None, None) => Ok(None),
        _ => Err(AtlatlError::config(format!(
            "{what}: set both key_id and secret, or neither"
        ))),
    }
}

fn storage_keys(st: Option<&StorageConfig>) -> Result<Option<Credentials>> {
    match st {
        Some(s) => static_keys(
            s.key_id.as_ref(),
            s.secret.as_ref(),
            s.session_token.as_ref(),
            "unload.storage",
        ),
        None => Ok(None),
    }
}

impl Redshift {
    /// Validates the connection and builds clients; no network I/O until the first call.
    /// Secret values must already be resolved.
    pub fn new(conn: &RedshiftConn, opts: Options) -> Result<Self> {
        let http = aws::client(opts.http_timeout)?;
        let d = conn.data_api.as_deref();
        let region = d
            .and_then(|d| d.region.clone())
            .or_else(|| {
                conn.unload
                    .as_ref()
                    .and_then(|u| u.storage.as_ref()?.region.clone())
            })
            .unwrap_or_else(|| "us-east-1".into());
        let mk = |src: Source| {
            Provider::new(
                src,
                &region,
                opts.sts_endpoint.as_deref(),
                http.clone(),
                opts.request_retry.clone(),
            )
            .map(Arc::new)
        };
        let caller = match d {
            Some(d) => match static_keys(
                d.key_id.as_ref(),
                d.secret.as_ref(),
                d.session_token.as_ref(),
                "data_api",
            )? {
                Some(c) => mk(Source::Static(c))?,
                None => mk(Source::Default)?,
            },
            None => mk(Source::Default)?,
        };
        let access = match conn.access.unwrap_or(RedshiftAccess::DataApi) {
            RedshiftAccess::DataApi => {
                let d = d.ok_or_else(|| {
                    AtlatlError::config("redshift: `data_api` is required for access: data_api")
                })?;
                let target = match (&d.workgroup, &d.cluster_identifier) {
                    (Some(w), None) => Target::Workgroup(w.clone()),
                    (None, Some(c)) => Target::Cluster(c.clone()),
                    _ => {
                        return Err(AtlatlError::config(
                            "redshift data_api: set exactly one of workgroup, cluster_identifier",
                        ));
                    }
                };
                let secret_arn = match d.auth.unwrap_or(DataApiAuth::Iam) {
                    DataApiAuth::Iam => None,
                    DataApiAuth::SecretArn => Some(d.secret_arn.clone().ok_or_else(|| {
                        AtlatlError::config(
                            "redshift data_api: auth: secret_arn needs `secret_arn`",
                        )
                    })?),
                };
                let dp = Poll::default();
                let p = d.poll.as_ref();
                let poll = Poll {
                    wait: p.and_then(|p| p.wait).map_or(dp.wait, |w| w.0),
                    initial: p.and_then(|p| p.initial).map_or(dp.initial, |w| w.0),
                    max: p.and_then(|p| p.max).map_or(dp.max, |w| w.0),
                };
                if poll.wait > Duration::from_secs(30) {
                    return Err(AtlatlError::config(
                        "redshift data_api.poll.wait is at most 30s",
                    ));
                }
                Access::DataApi(Box::new(DataApi::new(
                    DataApiOptions {
                        endpoint: d.endpoint.clone(),
                        region: region.clone(),
                        target,
                        database: d.database.clone(),
                        secret_arn,
                        poll,
                        retry: opts.request_retry.clone(),
                        statement_timeout: opts.statement_timeout,
                    },
                    caller.clone(),
                    http.clone(),
                )?))
            }
            RedshiftAccess::Wire => {
                let w = conn.wire.as_deref().ok_or_else(|| {
                    AtlatlError::config("redshift: `wire` is required for access: wire")
                })?;
                Access::Wire(match &opts.wire {
                    Some(shared) => shared.clone(),
                    None => Wire::new(
                        wire::connect_options(w)?,
                        opts.max_sessions,
                        opts.statement_timeout,
                    )?,
                })
            }
        };
        let (unload, s3) = match conn.unload.as_deref() {
            None => (None, None),
            Some(u) => {
                let st = u.storage.as_ref();
                let base = match storage_keys(st)? {
                    Some(c) => mk(Source::Static(c))?,
                    None => caller.clone(),
                };
                let read = match u.read_role.as_deref().unwrap_or("same") {
                    "none" => base,
                    "same" => Arc::new(base.assume(&u.iam_role)),
                    arn => Arc::new(base.assume(arn)),
                };
                let loc = S3Location::from_storage(st, &region);
                (
                    Some(UnloadSettings {
                        bucket: u.bucket.clone(),
                        base: unload::base_prefix(u.prefix.as_deref()),
                        iam_role: u.iam_role.clone(),
                        max_mb: unload::max_file_mb(u.max_file_size.map(|b| b.0)),
                    }),
                    Some(S3Client::new(
                        loc,
                        read,
                        http.clone(),
                        opts.request_retry.clone(),
                    )),
                )
            }
        };
        Ok(Self {
            access,
            unload,
            s3,
            opts,
        })
    }

    pub fn access(&self) -> &Access {
        &self.access
    }

    pub fn options(&self) -> &Options {
        &self.opts
    }

    /// The read-back S3 client (`unload` configured).
    pub fn s3(&self) -> Option<&S3Client> {
        self.s3.as_ref()
    }

    /// `(bucket, base prefix)` of the UNLOAD area, for `atlatl gc`.
    pub fn unload_area(&self) -> Option<(&str, &str)> {
        self.unload
            .as_ref()
            .map(|u| (u.bucket.as_str(), u.base.as_str()))
    }

    /// One statement, run to completion (no retry beyond request level).
    pub async fn execute(
        &self,
        sql: &str,
        name: &str,
        cancel: &CancellationToken,
    ) -> Result<Executed> {
        match &self.access {
            Access::DataApi(d) => d.execute(sql, name, cancel).await,
            Access::Wire(w) => w.execute(sql, cancel).await,
        }
    }

    /// A small result set, retried at query level.
    pub async fn query(
        &self,
        sql: &str,
        name: &str,
        cancel: &CancellationToken,
    ) -> Result<ResultSet> {
        retry(
            &self.opts.query_retry,
            RetryLevel::Query,
            &mut RandomJitter::new(),
            |a| tracing::warn!(attempt = a.attempt, error = %a.message, "retrying Redshift query {name}"),
            |_| async {
                match &self.access {
                    Access::DataApi(d) => d.query(sql, name, cancel).await,
                    Access::Wire(w) => w.query(sql, cancel).await,
                }
            },
        )
        .await
        .map_err(|e| e.error)
    }

    /// Columns of `[database.]schema.table` with precision and scale, in ordinal order.
    pub async fn columns(
        &self,
        object: &str,
        cancel: &CancellationToken,
    ) -> Result<Vec<SourceColumn>> {
        let rs = self
            .query(
                &catalog::columns_sql(&catalog::Object::parse(object)),
                &format!("atlatl:columns:{object}"),
                cancel,
            )
            .await?;
        catalog::parse_columns(&rs, object)
    }

    /// `[database.]schema.table` names matching a `*`/`?` glob, sorted.
    pub async fn discover(
        &self,
        pattern: &str,
        exclude: &[String],
        include_views: bool,
        cancel: &CancellationToken,
    ) -> Result<Vec<String>> {
        let rs = self
            .query(
                &catalog::tables_sql(pattern, include_views)?,
                &format!("atlatl:discover:{pattern}"),
                cancel,
            )
            .await?;
        Ok(catalog::match_tables(&rs, pattern, exclude))
    }

    /// Whether members are extracted by UNLOAD (`unload` set) rather than a wire export.
    pub fn unloads(&self) -> bool {
        self.unload.is_some()
    }

    /// Streams a member over the wire into `sink` (`access: wire` without `unload`). Dropped
    /// reads follow `query_retry`.
    pub async fn export<S: Sink>(
        &self,
        req: &ExportRequest,
        sink: &mut S,
        cancel: &CancellationToken,
    ) -> Result<Exported> {
        match &self.access {
            Access::Wire(w) => {
                w.export(req, &self.opts.export, &self.opts.query_retry, sink, cancel)
                    .await
            }
            Access::DataApi(_) => Err(AtlatlError::config(
                "redshift: access: data_api extracts by UNLOAD; set `unload`, or use access: wire",
            )),
        }
    }

    fn unload_settings(&self) -> Result<(&UnloadSettings, &S3Client)> {
        match (&self.unload, &self.s3) {
            (Some(u), Some(s)) => Ok((u, s)),
            _ => Err(AtlatlError::config(
                "redshift: `unload` is required to extract data",
            )),
        }
    }

    /// Runs the UNLOAD into `<base><run>/<stream>/<member>/a<attempt>[r<try>]/part_` and reads
    /// its manifest. A statement failing `transient`/`throttled` is retried into a new prefix.
    pub async fn unload(
        &self,
        req: &UnloadRequest,
        cancel: &CancellationToken,
    ) -> Result<Unloaded> {
        let (u, s3) = self.unload_settings()?;
        let member = unload::member_prefix(&u.base, &req.run_id, &req.stream, &req.member);
        let name = format!("atlatl:{}:{}:{}", req.run_id, req.stream, req.member);
        let (prefix, statement) = retry(
            &self.opts.query_retry,
            RetryLevel::Query,
            &mut RandomJitter::new(),
            |a| tracing::warn!(attempt = a.attempt, error = %a.message, "retrying UNLOAD {name}"),
            |try_| {
                let prefix = unload::attempt_prefix(&member, req.attempt, try_);
                let to = format!("s3://{}/{prefix}{}", u.bucket, unload::PART);
                let sql = unload::unload_sql(&req.select, &to, &u.iam_role, u.max_mb);
                let name = &name;
                async move { self.execute(&sql, name, cancel).await.map(|e| (prefix, e)) }
            },
        )
        .await
        .map_err(|e| e.error)?;
        let manifest_key = format!("{prefix}{}manifest", unload::PART);
        let (files, expected, found) = match s3.get(&u.bucket, &manifest_key).await? {
            Some(body) => {
                let text = String::from_utf8_lossy(&body);
                let (f, n) =
                    unload::parse_manifest(&text, &format!("s3://{}/{manifest_key}", u.bucket))?;
                (f, n, true)
            }
            None if statement.rows.unwrap_or(0) == 0 => (Vec::new(), Some(0), false),
            None => {
                return Err(AtlatlError::new(
                    atlatl_core::ErrorClass::Decode,
                    format!(
                        "UNLOAD {name} reported {} rows but wrote no manifest at s3://{}/{manifest_key}",
                        statement.rows.unwrap_or_default(),
                        u.bucket
                    ),
                ));
            }
        };
        if let (Some(rows), Some(want)) = (statement.rows, expected)
            && rows as u64 != want
        {
            return Err(AtlatlError::new(
                atlatl_core::ErrorClass::Decode,
                format!("UNLOAD {name} reported {rows} rows but its manifest lists {want}"),
            ));
        }
        Ok(Unloaded {
            bucket: u.bucket.clone(),
            member_prefix: member,
            prefix,
            statement,
            files,
            columns: req.columns.clone(),
            expected_rows: expected,
            manifest_found: found,
            read: unload::ReadAccess {
                loc: s3.location().clone(),
                creds: s3.credentials().await?,
            },
        })
    }

    /// Refreshes the read-back credentials (assumed roles expire); re-run [`Unloaded::prepare`].
    pub async fn refresh(&self, u: &mut Unloaded) -> Result<()> {
        let (_, s3) = self.unload_settings()?;
        u.read.creds = s3.credentials().await?;
        Ok(())
    }

    /// Deletes the member's prefix (every attempt). Call after the commit.
    pub async fn cleanup(&self, u: &Unloaded) -> Result<u64> {
        let (_, s3) = self.unload_settings()?;
        s3.delete_prefix(&u.bucket, &u.member_prefix).await
    }

    /// Deletes everything a run unloaded.
    pub async fn cleanup_run(&self, run_id: &str) -> Result<u64> {
        let (u, s3) = self.unload_settings()?;
        s3.delete_prefix(&u.bucket, &unload::run_prefix(&u.base, run_id))
            .await
    }

    /// Orphaned run prefixes older than `older_than`, skipping `active` runs (`atlatl gc`).
    pub async fn stale_prefixes(
        &self,
        older_than: Duration,
        active: &[String],
    ) -> Result<Vec<gc::StalePrefix>> {
        let (u, s3) = self.unload_settings()?;
        gc::collect(
            s3,
            &u.bucket,
            &u.base,
            older_than,
            std::time::SystemTime::now(),
            active,
        )
        .await
    }

    pub async fn remove_prefixes(&self, stale: &[gc::StalePrefix]) -> Result<u64> {
        let (_, s3) = self.unload_settings()?;
        gc::remove(s3, stale).await
    }
}
