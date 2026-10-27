//! `UNLOAD … FORMAT PARQUET MANIFEST VERBOSE` into a run- and attempt-scoped prefix, and the
//! DuckDB read-back through a prefix-scoped secret, per object with retry (06 §4).

use std::time::Instant;

use atlatl_conn_files::error::duck;
use atlatl_conn_files::manifest;
use atlatl_core::error::{AtlatlError, ErrorClass, Result};
use atlatl_core::retry::{AttemptRecord, GaveUp, Jitter, RandomJitter, RetryLevel, RetryPolicy};
use atlatl_ingest::sql::{ident as duck_ident, literal as duck_literal};
use duckdb::Connection;

use crate::catalog::{Object, SourceColumn, ident, literal};
use crate::creds::Credentials;
use crate::result::Executed;
use crate::s3::S3Location;

pub const DEFAULT_PREFIX: &str = "atlatl_unload/";
pub const DEFAULT_MAX_FILE_MB: u64 = 256;
/// Redshift appends `0000_part_00.parquet` … and `manifest` to this.
pub const PART: &str = "part_";

/// One path segment: `[A-Za-z0-9._-]`, anything else becomes `_`.
pub fn segment(s: &str) -> String {
    let out: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if out.is_empty() || out == "." || out == ".." {
        format!("_{out}")
    } else {
        out
    }
}

/// `<base>` with a trailing `/` (and no leading one).
pub fn base_prefix(p: Option<&str>) -> String {
    let p = p.unwrap_or(DEFAULT_PREFIX).trim_start_matches('/');
    if p.is_empty() {
        DEFAULT_PREFIX.to_owned()
    } else if p.ends_with('/') {
        p.to_owned()
    } else {
        format!("{p}/")
    }
}

pub fn run_prefix(base: &str, run_id: &str) -> String {
    format!("{base}{}/", segment(run_id))
}

/// `<base><run>/<stream>/<member>/`; cleanup deletes this, including failed attempts.
pub fn member_prefix(base: &str, run_id: &str, stream: &str, member: &str) -> String {
    format!(
        "{}{}/{}/",
        run_prefix(base, run_id),
        segment(stream),
        segment(member)
    )
}

/// `<member prefix>a<attempt>/`, or `a<attempt>r<try>/` for query-level retries. UNLOAD refuses
/// to overwrite, so every try gets a fresh prefix.
pub fn attempt_prefix(member: &str, attempt: u32, try_: u32) -> String {
    if try_ <= 1 {
        format!("{member}a{attempt}/")
    } else {
        format!("{member}a{attempt}r{try_}/")
    }
}

/// `MAXFILESIZE` in MB, clamped to Redshift's 5 MB – 6.2 GB.
pub fn max_file_mb(bytes: Option<u64>) -> u64 {
    bytes
        .map(|b| b / 1_000_000)
        .unwrap_or(DEFAULT_MAX_FILE_MB)
        .clamp(5, 6200)
}

/// The UNLOAD statement. `to` is the `s3://…/part_` prefix; `select` is Redshift SQL, embedded
/// as a string literal.
pub fn unload_sql(select: &str, to: &str, iam_role: &str, max_mb: u64) -> String {
    format!(
        "UNLOAD ({}) TO {} IAM_ROLE {} FORMAT PARQUET MANIFEST VERBOSE MAXFILESIZE {max_mb} MB PARALLEL ON",
        literal(select),
        literal(to),
        literal(iam_role)
    )
}

/// `SELECT "c1", … FROM ["db".]"s"."t" [WHERE (f1) AND (f2)]`. An outer `LIMIT` isn't allowed in
/// an UNLOAD, so a limit nests the select in a derived table.
pub fn select_sql(
    object: &Object,
    columns: &[SourceColumn],
    filters: &[String],
    limit: Option<u64>,
) -> String {
    let cols = if columns.is_empty() {
        "*".to_owned()
    } else {
        columns
            .iter()
            .map(|c| ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let mut sql = format!("SELECT {cols} FROM {}", object.sql());
    if !filters.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(
            &filters
                .iter()
                .map(|f| format!("({f})"))
                .collect::<Vec<_>>()
                .join(" AND "),
        );
    }
    match limit {
        Some(n) => format!("SELECT * FROM ({sql} LIMIT {n}) AS atlatl_limited"),
        None => sql,
    }
}

/// One unloaded Parquet object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnloadedFile {
    pub url: String,
    pub size: Option<u64>,
    pub record_count: Option<u64>,
}

/// One `chunk: {by: files}` group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chunk {
    pub files: Vec<UnloadedFile>,
    pub bytes: u64,
    pub expected_rows: Option<u64>,
    /// `SELECT … FROM read_parquet([…])` with the source types; to merge from directly.
    pub select: String,
}

/// Credentials and S3 settings for the DuckDB read-back secret.
#[derive(Clone, Debug)]
pub struct ReadAccess {
    pub loc: S3Location,
    pub creds: Credentials,
}

/// A finished UNLOAD: the files from its manifest and what's needed to read them.
#[derive(Clone, Debug)]
pub struct Unloaded {
    pub bucket: String,
    /// The member prefix (all attempts); cleanup removes it.
    pub member_prefix: String,
    /// The successful attempt's prefix, ending in `/`.
    pub prefix: String,
    pub statement: Executed,
    /// Manifest order. Empty for a zero-row UNLOAD.
    pub files: Vec<UnloadedFile>,
    pub columns: Vec<SourceColumn>,
    /// Manifest `meta.record_count` (or the sum of the entries').
    pub expected_rows: Option<u64>,
    /// False when UNLOAD wrote no manifest (zero rows).
    pub manifest_found: bool,
    pub read: ReadAccess,
}

/// Parses a VERBOSE manifest into files and the total record count.
pub fn parse_manifest(text: &str, origin: &str) -> Result<(Vec<UnloadedFile>, Option<u64>)> {
    let entries = manifest::parse(text, origin)?;
    let files: Vec<UnloadedFile> = entries
        .into_iter()
        .map(|e| UnloadedFile {
            url: e.url,
            size: e.content_length,
            record_count: e.record_count,
        })
        .collect();
    let v: serde_json::Value = serde_json::from_str(text).unwrap_or_default();
    let total = v
        .pointer("/meta/record_count")
        .and_then(serde_json::Value::as_u64)
        .or_else(|| files.iter().map(|f| f.record_count).sum());
    Ok((files, total))
}

impl Unloaded {
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    pub fn url(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.prefix)
    }

    /// DuckDB secret name for this read.
    pub fn secret_name(&self) -> String {
        use sha2::Digest;
        let h = sha2::Sha256::digest(self.prefix.as_bytes());
        format!("atlatl_redshift_{}", crate::sigv4::hex(&h[..6]))
    }

    /// `CREATE OR REPLACE SECRET` scoped to the attempt prefix.
    pub fn secret_sql(&self) -> String {
        let a = &self.read;
        let mut opts = vec![
            "TYPE s3".to_owned(),
            "PROVIDER config".to_owned(),
            format!("KEY_ID {}", duck_literal(&a.creds.key_id)),
            format!("SECRET {}", duck_literal(&a.creds.secret)),
        ];
        if let Some(t) = &a.creds.session_token {
            opts.push(format!("SESSION_TOKEN {}", duck_literal(t)));
        }
        opts.push(format!("REGION {}", duck_literal(&a.loc.region)));
        if let Some(ep) = &a.loc.endpoint {
            opts.push(format!("ENDPOINT {}", duck_literal(ep)));
        }
        opts.push(format!(
            "URL_STYLE {}",
            duck_literal(if a.loc.path_style { "path" } else { "vhost" })
        ));
        opts.push(format!("USE_SSL {}", a.loc.use_ssl));
        opts.push(format!("SCOPE {}", duck_literal(&self.url())));
        format!(
            "CREATE OR REPLACE SECRET {} ({})",
            duck_ident(&self.secret_name()),
            opts.join(", ")
        )
    }

    /// Loads httpfs and (re)creates the scoped secret. Call again after [`crate::Redshift::refresh`].
    pub fn prepare(&self, c: &Connection) -> Result<()> {
        c.execute_batch("LOAD httpfs").map_err(|e| {
            AtlatlError::config(format!(
                "the Redshift read-back needs the `httpfs` extension: {e}"
            ))
        })?;
        c.execute_batch(&self.secret_sql()).map_err(duck)
    }

    pub fn release(&self, c: &Connection) -> Result<()> {
        c.execute_batch(&format!(
            "DROP SECRET IF EXISTS {}",
            duck_ident(&self.secret_name())
        ))
        .map_err(duck)
    }

    /// The typed projection: every column cast to its `svv_columns` type; `timestamptz` (naive
    /// UTC in Parquet) restored as UTC.
    fn projection(&self) -> String {
        if self.columns.is_empty() {
            return "*".into();
        }
        self.columns
            .iter()
            .map(|c| {
                let q = duck_ident(&c.name);
                if c.is_timestamptz() {
                    format!("timezone('UTC', CAST({q} AS TIMESTAMP)) AS {q}")
                } else {
                    format!("CAST({q} AS {}) AS {q}", c.duck_type)
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub fn select_for(&self, files: &[UnloadedFile]) -> Result<String> {
        if files.is_empty() {
            return Err(AtlatlError::internal("no unloaded files to read"));
        }
        let list = files
            .iter()
            .map(|f| duck_literal(&f.url))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!(
            "SELECT {} FROM read_parquet([{list}])",
            self.projection()
        ))
    }

    /// `CREATE TEMP TABLE` with the source columns and types, no rows.
    fn create_empty(&self, c: &Connection, name: &str) -> Result<()> {
        if self.columns.is_empty() {
            return Err(AtlatlError::internal(
                "an unload read-back needs the source columns",
            ));
        }
        let cols = self
            .columns
            .iter()
            .map(|c| format!("{} {}", duck_ident(&c.name), c.duck_type))
            .collect::<Vec<_>>()
            .join(", ");
        c.execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE {} ({cols})",
            duck_ident(name)
        ))
        .map_err(duck)
    }

    /// Stages every file into `CREATE TEMP TABLE <name>`; returns the row count. Must run outside
    /// a transaction: each object is appended in its own, and retried on its own.
    pub fn stage(
        &self,
        c: &Connection,
        name: &str,
        policy: &RetryPolicy,
        on_retry: &mut dyn FnMut(&AttemptRecord),
    ) -> Result<u64> {
        let n = self.stage_files(c, name, &self.files, policy, on_retry)?;
        if let Some(want) = self.expected_rows
            && want != n
        {
            return Err(AtlatlError::new(
                ErrorClass::Decode,
                format!("staged {n} rows but the UNLOAD manifest lists {want}"),
            ));
        }
        Ok(n)
    }

    /// Stages `files` (e.g. one [`Chunk`]) into a fresh temp table.
    pub fn stage_files(
        &self,
        c: &Connection,
        name: &str,
        files: &[UnloadedFile],
        policy: &RetryPolicy,
        on_retry: &mut dyn FnMut(&AttemptRecord),
    ) -> Result<u64> {
        self.create_empty(c, name)?;
        let mut total = 0u64;
        for f in files {
            let sql = format!(
                "INSERT INTO {} {}",
                duck_ident(name),
                self.select_for(std::slice::from_ref(f))?
            );
            total += retry_sync(
                policy,
                RetryLevel::Object,
                &mut RandomJitter::new(),
                on_retry,
                |_| append(c, &sql, f),
            )?;
        }
        Ok(total)
    }

    /// Consecutive groups of about `size` bytes (manifest `content_length`).
    pub fn chunks(&self, size: u64) -> Result<Vec<Chunk>> {
        let sizes: Vec<u64> = self.files.iter().map(|f| f.size.unwrap_or(0)).collect();
        manifest::group_by_size(&sizes, size.max(1))
            .into_iter()
            .map(|r| {
                let files = self.files[r.clone()].to_vec();
                Ok(Chunk {
                    bytes: sizes[r].iter().sum(),
                    expected_rows: files.iter().map(|f| f.record_count).sum(),
                    select: self.select_for(&files)?,
                    files,
                })
            })
            .collect()
    }
}

/// One object in one transaction; a count that differs from the manifest rolls back as `decode`.
fn append(c: &Connection, sql: &str, f: &UnloadedFile) -> Result<u64> {
    c.execute_batch("BEGIN TRANSACTION").map_err(duck)?;
    let r = (|| {
        let n = c.execute(sql, []).map_err(duck)? as u64;
        if let Some(want) = f.record_count
            && want != n
        {
            return Err(AtlatlError::new(
                ErrorClass::Decode,
                format!("{}: read {n} rows but the manifest lists {want}", f.url),
            ));
        }
        c.execute_batch("COMMIT").map_err(duck)?;
        Ok(n)
    })();
    if r.is_err() {
        let _ = c.execute_batch("ROLLBACK");
    }
    r.map_err(|e| AtlatlError {
        message: format!("{} ({})", e.message, f.url),
        ..e
    })
}

/// The retry loop of `atlatl_core::retry`, blocking (DuckDB work runs on a blocking thread).
pub fn retry_sync<T>(
    policy: &RetryPolicy,
    level: RetryLevel,
    jitter: &mut dyn Jitter,
    on_retry: &mut dyn FnMut(&AttemptRecord),
    mut op: impl FnMut(u32) -> Result<T>,
) -> Result<T> {
    let started = Instant::now();
    let mut attempt = 1u32;
    loop {
        let e = match op(attempt) {
            Ok(v) => return Ok(v),
            Err(e) => e,
        };
        let reason = if !policy.retry_on.contains(&e.class) {
            Some(GaveUp::NotRetryable)
        } else if attempt >= policy.max_attempts {
            Some(GaveUp::Exhausted)
        } else {
            None
        };
        let wait = policy.wait_for(&e, attempt, jitter.factor());
        let reason = reason.or_else(|| {
            policy
                .budget
                .is_some_and(|b| started.elapsed() + wait > b)
                .then_some(GaveUp::Budget)
        });
        if let Some(r) = reason {
            return Err(match r {
                GaveUp::NotRetryable => e,
                _ => AtlatlError {
                    message: format!("{} (after {attempt} attempts)", e.message),
                    ..e
                },
            });
        }
        on_retry(&AttemptRecord {
            level,
            attempt,
            class: e.class,
            message: e.redacted_message(),
            wait_ms: wait.as_millis() as u64,
        });
        std::thread::sleep(wait);
        attempt += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::duck_type;

    fn col(name: &str, t: &str, p: Option<i64>, s: Option<i64>) -> SourceColumn {
        SourceColumn {
            name: name.into(),
            data_type: t.into(),
            char_len: None,
            precision: p,
            scale: s,
            duck_type: duck_type(t, p, s).expect("type"),
        }
    }

    #[test]
    fn prefixes() {
        let base = base_prefix(None);
        assert_eq!(base, "atlatl_unload/");
        assert_eq!(base_prefix(Some("/x/y")), "x/y/");
        let m = member_prefix(
            &base,
            "01JRUN",
            "warehouse/est_v2/est_core_consf",
            "est_v2.est_core_cons_af",
        );
        assert_eq!(
            m,
            "atlatl_unload/01JRUN/warehouse_est_v2_est_core_consf/est_v2.est_core_cons_af/"
        );
        assert_eq!(attempt_prefix(&m, 2, 1), format!("{m}a2/"));
        assert_eq!(attempt_prefix(&m, 2, 3), format!("{m}a2r3/"));
        assert_eq!(segment(".."), "_..");
        assert_eq!(max_file_mb(None), 256);
        assert_eq!(max_file_mb(Some(1_000)), 5);
        assert_eq!(max_file_mb(Some(10_000_000_000)), 6200);
    }

    #[test]
    fn statements() {
        let cols = [
            col("id", "bigint", None, None),
            col("n", "numeric", Some(18), Some(4)),
        ];
        let sel = select_sql(
            &Object::parse("est_v2.t"),
            &cols,
            &["\"d\" >= '2024-01-01'".into()],
            None,
        );
        assert_eq!(
            sel,
            r#"SELECT "id", "n" FROM "est_v2"."t" WHERE ("d" >= '2024-01-01')"#
        );
        assert_eq!(
            unload_sql(&sel, "s3://b/p/a1/part_", "arn:aws:iam::1:role/r", 256),
            r#"UNLOAD ('SELECT "id", "n" FROM "est_v2"."t" WHERE ("d" >= ''2024-01-01'')') TO 's3://b/p/a1/part_' IAM_ROLE 'arn:aws:iam::1:role/r' FORMAT PARQUET MANIFEST VERBOSE MAXFILESIZE 256 MB PARALLEL ON"#
        );
        assert_eq!(
            select_sql(&Object::parse("s.t"), &[], &[], Some(5)),
            r#"SELECT * FROM (SELECT * FROM "s"."t" LIMIT 5) AS atlatl_limited"#
        );
        let bs = unload_sql(r"SELECT 'a\b'", "s3://b/p", "r", 5);
        assert!(bs.starts_with(r"UNLOAD ('SELECT ''a\\b''')"), "{bs}");
    }

    #[test]
    fn manifests() {
        let (files, total) = parse_manifest(
            r#"{"entries":[{"url":"s3://b/p/part_0000_part_00.parquet","meta":{"content_length":10,"record_count":3}},
                {"url":"s3://b/p/part_0001_part_00.parquet","meta":{"content_length":12,"record_count":4}}],
                "schema":{"elements":[]},"meta":{"content_length":22,"record_count":7},
                "author":{"name":"Amazon Redshift","version":"1.0.0"}}"#,
            "m",
        )
        .expect("manifest");
        assert_eq!(files.len(), 2);
        assert_eq!(total, Some(7));
        let (files, total) = parse_manifest(r#"{"entries":[]}"#, "m").expect("empty");
        assert!(files.is_empty());
        assert_eq!(total, Some(0));
    }

    #[test]
    fn sync_retry() {
        let p = RetryPolicy {
            max_attempts: 3,
            base: std::time::Duration::from_millis(1),
            max: std::time::Duration::from_millis(2),
            ..RetryPolicy::default()
        };
        let mut seen = Vec::new();
        let mut jitter = atlatl_core::retry::FixedJitter(1.0);
        let r = retry_sync(
            &p,
            RetryLevel::Object,
            &mut jitter,
            &mut |a| seen.push(a.attempt),
            |n| {
                if n < 3 {
                    Err(AtlatlError::transient("503"))
                } else {
                    Ok(n)
                }
            },
        );
        assert_eq!(r.expect("ok"), 3);
        assert_eq!(seen, [1, 2]);
        let e = retry_sync(&p, RetryLevel::Object, &mut jitter, &mut |_| {}, |_| {
            Err::<(), _>(AtlatlError::transient("503"))
        })
        .expect_err("exhausted");
        assert!(e.message.contains("after 3 attempts"));
        let e = retry_sync(
            &p,
            RetryLevel::Object,
            &mut jitter,
            &mut |_| panic!("no retry"),
            |_| Err::<(), _>(AtlatlError::config("404")),
        )
        .expect_err("config");
        assert_eq!(e.class, ErrorClass::Config);
    }
}
