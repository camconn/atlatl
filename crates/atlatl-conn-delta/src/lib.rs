//! `delta` source (06 §9): Delta tables read engine-native through DuckDB's `delta`
//! extension, always attached `READ_ONLY` at a pinned version. Staging, drift and modes are
//! `atlatl-ingest`'s; new lake tables take the Delta-derived DuckDB types unchanged (M2.2).

use atlatl_core::config::model::{
    ChunkConfig, DeltaConn, Nulls, S3Provider, StorageConfig, StorageType, UpdateKey, UrlStyle,
};
use atlatl_core::config::types::SecretValue;
use atlatl_core::error::{AtlatlError, ErrorClass, Result};
use atlatl_ingest::sql::{db, ident, literal};
use atlatl_ingest::staging::Column;
use duckdb::Connection;

fn is_s3(uri: &str) -> bool {
    uri.starts_with("s3://")
}

fn storage(conn: &DeltaConn) -> Option<&StorageConfig> {
    conn.storage.as_ref().filter(|s| s.kind == StorageType::S3)
}

fn uses_s3(conn: &DeltaConn) -> bool {
    is_s3(&conn.root) || storage(conn).is_some()
}

/// `LOAD delta`, plus `httpfs` (S3 secrets) and `aws` (`credential_chain`) for S3 roots.
pub fn load(c: &Connection, conn: &DeltaConn) -> Result<()> {
    let mut sql = String::from("LOAD delta;");
    if uses_s3(conn) {
        sql.push_str(" LOAD httpfs;");
        if storage(conn)
            .and_then(|s| s.provider)
            .unwrap_or(S3Provider::CredentialChain)
            == S3Provider::CredentialChain
        {
            sql.push_str(" LOAD aws;");
        }
    }
    c.execute_batch(&sql).map_err(db)
}

/// `root` joined with a relative `from.table` path.
pub fn table_uri(conn: &DeltaConn, table: &str) -> Result<String> {
    let t = table.trim().trim_matches('/');
    if t.is_empty() {
        return Err(AtlatlError::config("`from.table` is empty"));
    }
    if t.contains("://") || table.trim_start().starts_with('/') {
        return Err(AtlatlError::config(format!(
            "`from.table` `{table}` must be a path relative to the delta root `{}`",
            conn.root
        )));
    }
    if t.split('/').any(|s| s == ".." || s == "." || s.is_empty()) {
        return Err(AtlatlError::config(format!(
            "`from.table` `{table}` must not contain empty, `.` or `..` segments"
        )));
    }
    let root = conn.root.trim_end_matches('/');
    let root = root.strip_prefix("file://").unwrap_or(root);
    Ok(format!("{root}/{t}"))
}

fn secret(v: Option<&SecretValue>, what: &str) -> Result<Option<String>> {
    match v {
        None => Ok(None),
        Some(SecretValue::Literal(s)) => Ok(Some(s.clone())),
        Some(SecretValue::Ref(_)) => Err(AtlatlError::internal(format!(
            "{what} was not resolved before use"
        ))),
    }
}

/// `CREATE OR REPLACE SECRET` scoped to the root's bucket; `None` for local roots. An `s3://`
/// root without `storage` uses `credential_chain`. Holds credentials: never log it.
pub fn secret_sql(name: &str, conn: &DeltaConn) -> Result<Option<String>> {
    if !uses_s3(conn) {
        return Ok(None);
    }
    let bucket = conn
        .root
        .strip_prefix("s3://")
        .and_then(|r| r.split('/').next())
        .filter(|b| !b.is_empty())
        .ok_or_else(|| {
            AtlatlError::config(format!(
                "storage.type is s3 but root `{}` is not an s3://bucket/… path",
                conn.root
            ))
        })?;
    let st = storage(conn);
    let mut opts = vec!["TYPE s3".to_owned()];
    match (st, st.and_then(|s| s.provider)) {
        (_, None | Some(S3Provider::CredentialChain)) => {
            opts.push("PROVIDER credential_chain".into())
        }
        (Some(st), Some(S3Provider::Config)) => {
            let key = secret(st.key_id.as_ref(), "storage.key_id")?.ok_or_else(|| {
                AtlatlError::config("storage.key_id is required for provider: config")
            })?;
            let sec = secret(st.secret.as_ref(), "storage.secret")?.ok_or_else(|| {
                AtlatlError::config("storage.secret is required for provider: config")
            })?;
            atlatl_core::redact::register(&sec);
            opts.push("PROVIDER config".into());
            opts.push(format!("KEY_ID {}", literal(&key)));
            opts.push(format!("SECRET {}", literal(&sec)));
            if let Some(t) = secret(st.session_token.as_ref(), "storage.session_token")? {
                atlatl_core::redact::register(&t);
                opts.push(format!("SESSION_TOKEN {}", literal(&t)));
            }
        }
        (_, Some(_)) => {
            return Err(AtlatlError::config(
                "storage.provider: assume_role is not supported yet; use credential_chain or config",
            ));
        }
    }
    if let Some(st) = st {
        if let Some(r) = st.region.as_deref().filter(|r| !r.is_empty()) {
            opts.push(format!("REGION {}", literal(r)));
        }
        let mut use_ssl = st.use_ssl;
        if let Some(ep) = st.endpoint.as_deref().filter(|e| !e.is_empty()) {
            let (host, scheme) = match ep.split_once("://") {
                Some(("https", h)) => (h, Some(true)),
                Some(("http", h)) => (h, Some(false)),
                Some(_) => {
                    return Err(AtlatlError::config(format!(
                        "storage.endpoint `{ep}`: only http:// and https:// are supported"
                    )));
                }
                None => (ep, None),
            };
            let host = host.trim_end_matches('/');
            if host.is_empty() || host.contains('/') {
                return Err(AtlatlError::config(format!(
                    "storage.endpoint `{ep}` must be host[:port] (no path)"
                )));
            }
            if let (Some(a), Some(b)) = (use_ssl, scheme)
                && a != b
            {
                return Err(AtlatlError::config(format!(
                    "storage.endpoint `{ep}` implies use_ssl: {b}, but use_ssl is {a}"
                )));
            }
            use_ssl = use_ssl.or(scheme);
            opts.push(format!("ENDPOINT {}", literal(host)));
        }
        if let Some(u) = st.url_style {
            let u = match u {
                UrlStyle::Path => "path",
                UrlStyle::Vhost => "vhost",
            };
            opts.push(format!("URL_STYLE {}", literal(u)));
        }
        if let Some(s) = use_ssl {
            opts.push(format!("USE_SSL {s}"));
        }
    }
    opts.push(format!("SCOPE {}", literal(&format!("s3://{bucket}/"))));
    Ok(Some(format!(
        "CREATE OR REPLACE SECRET {} ({})",
        ident(name),
        opts.join(", ")
    )))
}

/// Maps extension errors: a missing table or version is `config`, object-store I/O `transient`.
fn classify(e: AtlatlError, uri: &str) -> AtlatlError {
    let m = e.message.as_str();
    let (class, code, what) = if m.contains("InvalidTableLocation")
        || m.contains("Path does not exist")
        || m.contains("No files in log segment")
    {
        (ErrorClass::Config, "delta_not_found", "no Delta table")
    } else if m.contains("LogSegment end version") || m.contains("MissingVersion") {
        (ErrorClass::Config, "delta_version", "version not available")
    } else if m.contains("ObjectStore") {
        (ErrorClass::Transient, "delta_io", "object store error")
    } else {
        return e;
    };
    AtlatlError::new(class, format!("{what} at `{uri}`: {m}")).code(code)
}

/// `describe` after a delta ATTACH: since DuckDB v1.5.6 the attach of a path with no table
/// succeeds, and reading it fails with a catalog error naming the alias.
fn classify_described(e: AtlatlError, uri: &str, alias: &str) -> AtlatlError {
    if e.message
        .contains(&format!("Table with name {alias} does not exist"))
    {
        AtlatlError::config(format!("no Delta table at `{uri}`: {}", e.message))
            .code("delta_not_found")
    } else {
        classify(e, uri)
    }
}

/// The latest committed version, from the `_delta_log` listing (commits and checkpoints). The
/// extension exposes no function for it (S7 §3).
pub fn head_version(c: &Connection, uri: &str) -> Result<u64> {
    let v: Option<i64> = c
        .query_row(
            &format!(
                r"SELECT max(NULLIF(regexp_extract(file, '(\d{{20}})\.(json|checkpoint(\.[^/]*)?\.(parquet|json))$', 1), '')::BIGINT)
                  FROM glob({})",
                literal(&format!("{uri}/_delta_log/*"))
            ),
            [],
            |r| r.get(0),
        )
        .map_err(|e| classify(db(e), uri))?;
    v.map(|v| v as u64).ok_or_else(|| {
        AtlatlError::config(format!("no Delta table at `{uri}` (no _delta_log commits)"))
            .code("delta_not_found")
    })
}

/// A resolved table and its head version at discovery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Table {
    pub uri: String,
    pub head: u64,
}

pub fn discover(c: &Connection, conn: &DeltaConn, table: &str) -> Result<Table> {
    let uri = table_uri(conn, table)?;
    let head = head_version(c, &uri)?;
    Ok(Table { uri, head })
}

/// `atlatl_delta_<stream>_<member>`, the attach alias for one member's pin.
pub fn pin_alias(stream_seq: usize, member_seq: usize) -> String {
    format!("atlatl_delta_{stream_seq}_{member_seq}")
}

/// The only attach atlatl issues: `READ_ONLY` always (a read-write attach plus `INSERT`
/// segfaults the process, S7 §3).
pub fn attach_sql(uri: &str, version: u64, alias: &str) -> String {
    format!(
        "ATTACH {} AS {} (TYPE delta, READ_ONLY, VERSION {version})",
        literal(uri),
        ident(alias)
    )
}

/// A table attached at one version: every read in the run sees the same data, so a commit
/// retry re-reads it identically.
#[derive(Clone, Debug)]
pub struct Pinned {
    pub alias: String,
    pub uri: String,
    pub version: u64,
    /// The Delta schema at `version` as DuckDB types (S7 type map); new lake tables use them.
    pub columns: Vec<Column>,
}

fn attached(c: &Connection, alias: &str) -> Result<Option<(String, bool)>> {
    let mut s = c
        .prepare("SELECT type, readonly FROM duckdb_databases() WHERE database_name = ?")
        .map_err(db)?;
    let mut rows = s
        .query_map([alias], |r| Ok((r.get(0)?, r.get(1)?)))
        .map_err(db)?;
    rows.next().transpose().map_err(db)
}

/// Attaches `uri` at `version` as `alias` (replacing an earlier delta pin with that alias).
pub fn pin(c: &Connection, uri: &str, version: u64, alias: &str) -> Result<Pinned> {
    match attached(c, alias)? {
        Some((ty, _)) if ty != "delta" => {
            return Err(AtlatlError::config(format!(
                "attach alias `{alias}` is already used by a {ty} database"
            )));
        }
        Some(_) => c
            .execute_batch(&format!("DETACH DATABASE {}", ident(alias)))
            .map_err(db)?,
        None => {}
    }
    c.execute_batch(&attach_sql(uri, version, alias))
        .map_err(|e| classify(db(e), uri))?;
    let fail = |e: AtlatlError| {
        let _ = c.execute_batch(&format!("DETACH DATABASE IF EXISTS {}", ident(alias)));
        e
    };
    if attached(c, alias)?.is_none_or(|(_, ro)| !ro) {
        return Err(fail(AtlatlError::internal(format!(
            "delta attach `{alias}` is not read-only"
        ))));
    }
    let columns = atlatl_ingest::staging::describe(c, &ident(alias))
        .map_err(|e| fail(classify_described(e, uri, alias)))?;
    Ok(Pinned {
        alias: alias.to_owned(),
        uri: uri.to_owned(),
        version,
        columns,
    })
}

const TEMPORAL: &[&str] = &[
    "DATE",
    "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE",
    "TIMESTAMP_S",
    "TIMESTAMP_MS",
    "TIMESTAMP_NS",
];

impl Pinned {
    pub fn relation(&self) -> String {
        ident(&self.alias)
    }

    /// The staging query: `SELECT * FROM <pin> [WHERE filter]`. Partition-value and file-stat
    /// predicates skip files; `IN` lists and computed expressions don't (S7 §3).
    pub fn select(&self, filter: Option<&str>) -> String {
        match filter {
            Some(f) => format!("SELECT * FROM {} WHERE {f}", self.relation()),
            None => format!("SELECT * FROM {}", self.relation()),
        }
    }

    /// The filtered pin as a relation, for [`Pinned::chunks`] or a direct apply.
    pub fn source(&self, filter: Option<&str>) -> String {
        match filter {
            Some(_) => format!("({})", self.select(filter)),
            None => self.relation(),
        }
    }

    fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name).or_else(|| {
            self.columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(name))
        })
    }

    /// The source predicate (04 §4.3–4.4): `(window) AND <update_key bound>`. The stored value is
    /// cast to the column's Delta type so stats pushdown applies; `lookback` needs a date or
    /// timestamp column. `None` means a full read.
    pub fn filter(
        &self,
        window: Option<&str>,
        update_key: Option<&UpdateKey>,
        stored: Option<&str>,
    ) -> Result<Option<String>> {
        let mut parts: Vec<String> = window
            .map(str::trim)
            .filter(|w| !w.is_empty())
            .map(|w| format!("({w})"))
            .into_iter()
            .collect();
        if let Some(k) = update_key {
            let col = self.column(&k.column).ok_or_else(|| {
                AtlatlError::config(format!(
                    "update_key column `{}` is not in Delta table `{}`",
                    k.column, self.uri
                ))
                .code("missing_column")
            })?;
            let (name, ty) = (ident(&col.name), col.kind.as_str());
            let lookback = k.lookback.map(|d| d.0).filter(|d| !d.is_zero());
            if lookback.is_some() && !TEMPORAL.contains(&ty) {
                return Err(AtlatlError::config(format!(
                    "update_key `{}` is {ty}; `lookback` needs a date or timestamp column",
                    k.column
                )));
            }
            let bound = stored.map(|v| {
                let v = format!("CAST({} AS {ty})", literal(v));
                let v = match lookback {
                    Some(d) => format!(
                        "CAST({v} - INTERVAL '{} microseconds' AS {ty})",
                        d.as_micros()
                    ),
                    None => v,
                };
                let op = if k.inclusive.unwrap_or(true) {
                    ">="
                } else {
                    ">"
                };
                format!("{name} {op} {v}")
            });
            let nulls = k.nulls == Some(Nulls::Include);
            match (bound, nulls) {
                (Some(b), true) => parts.push(format!("({b} OR {name} IS NULL)")),
                (Some(b), false) => parts.push(b),
                (None, true) => {}
                (None, false) => parts.push(format!("{name} IS NOT NULL")),
            }
        }
        Ok((!parts.is_empty()).then(|| parts.join(" AND ")))
    }

    /// Chunk relations over the filtered pin (04 §7), merged straight from the attach inside
    /// the commit.
    pub fn chunks(
        &self,
        c: &Connection,
        filter: Option<&str>,
        cfg: &ChunkConfig,
    ) -> Result<Vec<String>> {
        let src = self.source(filter);
        Ok(atlatl_ingest::chunk::predicates(c, &src, cfg)?
            .iter()
            .map(|p| atlatl_ingest::chunk::relation(&src, p))
            .collect())
    }

    pub fn detach(&self, c: &Connection) -> Result<()> {
        c.execute_batch(&format!("DETACH DATABASE IF EXISTS {}", self.relation()))
            .map_err(db)
    }
}

#[cfg(test)]
mod tests {
    use atlatl_core::config::types::{Dur, SecretRef};

    use super::*;

    fn conn(root: &str, storage: Option<StorageConfig>) -> DeltaConn {
        DeltaConn {
            root: root.into(),
            storage,
        }
    }

    fn s3(provider: Option<S3Provider>) -> StorageConfig {
        StorageConfig {
            kind: StorageType::S3,
            provider,
            region: Some("us-east-1".into()),
            endpoint: Some("http://localhost:9000".into()),
            url_style: Some(UrlStyle::Path),
            use_ssl: None,
            key_id: Some(SecretValue::Literal("AK".into())),
            secret: Some(SecretValue::Literal("s'cret".into())),
            session_token: None,
            role_arn: None,
        }
    }

    fn pinned(cols: &[(&str, &str)]) -> Pinned {
        Pinned {
            alias: "atlatl_delta_0_0".into(),
            uri: "/d/t".into(),
            version: 3,
            columns: cols
                .iter()
                .map(|(n, k)| Column {
                    name: (*n).into(),
                    kind: (*k).into(),
                })
                .collect(),
        }
    }

    fn key(column: &str) -> UpdateKey {
        UpdateKey {
            column: column.into(),
            lookback: None,
            inclusive: None,
            nulls: None,
        }
    }

    #[test]
    fn table_uris() {
        let c = conn("s3://b/br/", None);
        assert_eq!(
            table_uri(&c, "monthly/cal").expect("uri"),
            "s3://b/br/monthly/cal"
        );
        assert_eq!(
            table_uri(&c, "monthly/cal/").expect("uri"),
            "s3://b/br/monthly/cal"
        );
        let c = conn("file:///data/delta", None);
        assert_eq!(table_uri(&c, "t").expect("uri"), "/data/delta/t");
        for bad in ["", "/abs", "s3://x/t", "a/../b", "a//b", "./a"] {
            assert!(table_uri(&c, bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn secrets() {
        assert_eq!(secret_sql("s", &conn("/local", None)).expect("local"), None);
        assert_eq!(
            secret_sql("s", &conn("s3://bkt/root", None)).expect("chain"),
            Some(
                r#"CREATE OR REPLACE SECRET "s" (TYPE s3, PROVIDER credential_chain, SCOPE 's3://bkt/')"#
                    .into()
            )
        );
        assert_eq!(
            secret_sql("s", &conn("s3://bkt/root", Some(s3(Some(S3Provider::Config)))))
                .expect("config"),
            Some(
                r#"CREATE OR REPLACE SECRET "s" (TYPE s3, PROVIDER config, KEY_ID 'AK', SECRET 's''cret', REGION 'us-east-1', ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false, SCOPE 's3://bkt/')"#
                    .into()
            )
        );
        assert!(secret_sql("s", &conn("/local", Some(s3(None)))).is_err());
        let mut unresolved = s3(Some(S3Provider::Config));
        unresolved.secret = Some(SecretValue::Ref(SecretRef::Env("X".into())));
        assert!(secret_sql("s", &conn("s3://b/", Some(unresolved))).is_err());
        let mut bad = s3(Some(S3Provider::Config));
        bad.use_ssl = Some(true);
        assert!(secret_sql("s", &conn("s3://b/", Some(bad))).is_err());
        let mut local = s3(None);
        local.kind = StorageType::Local;
        assert_eq!(
            secret_sql("s", &conn("/local", Some(local))).expect("local"),
            None
        );
    }

    #[test]
    fn attach_is_always_read_only() {
        assert_eq!(
            attach_sql("s3://b/t'x", 7, "atlatl_delta_1_0"),
            r#"ATTACH 's3://b/t''x' AS "atlatl_delta_1_0" (TYPE delta, READ_ONLY, VERSION 7)"#
        );
        assert_eq!(pin_alias(2, 1), "atlatl_delta_2_1");
    }

    #[test]
    fn filters() {
        let p = pinned(&[
            ("id", "BIGINT"),
            ("Updated", "TIMESTAMP WITH TIME ZONE"),
            ("d", "DATE"),
        ]);
        assert_eq!(p.filter(None, None, None).expect("none"), None);
        assert_eq!(
            p.filter(Some("region = 3"), None, None).expect("window"),
            Some("(region = 3)".into())
        );
        assert_eq!(
            p.filter(None, Some(&key("id")), None).expect("first run"),
            Some(r#""id" IS NOT NULL"#.into())
        );
        assert_eq!(
            p.filter(Some("a OR b"), Some(&key("id")), Some("999"))
                .expect("both"),
            Some(r#"(a OR b) AND "id" >= CAST('999' AS BIGINT)"#.into())
        );
        let k = UpdateKey {
            lookback: Some(Dur::parse("2h").expect("dur")),
            inclusive: Some(false),
            nulls: Some(Nulls::Include),
            ..key("updated")
        };
        assert_eq!(
            p.filter(None, Some(&k), Some("2024-03-10 13:00:00+00"))
                .expect("lookback"),
            Some(
                r#"("Updated" > CAST(CAST('2024-03-10 13:00:00+00' AS TIMESTAMP WITH TIME ZONE) - INTERVAL '7200000000 microseconds' AS TIMESTAMP WITH TIME ZONE) OR "Updated" IS NULL)"#
                    .into()
            )
        );
        let k = UpdateKey {
            nulls: Some(Nulls::Include),
            ..key("d")
        };
        assert_eq!(p.filter(None, Some(&k), None).expect("nulls"), None);
        let e = p
            .filter(
                None,
                Some(&UpdateKey {
                    lookback: Some(Dur::parse("1d").expect("dur")),
                    ..key("id")
                }),
                Some("1"),
            )
            .expect_err("numeric lookback");
        assert_eq!(e.class, ErrorClass::Config);
        let e = p
            .filter(None, Some(&key("nope")), None)
            .expect_err("missing");
        assert_eq!(e.code.as_deref(), Some("missing_column"));
    }

    #[test]
    fn classifies_extension_errors() {
        let missing =
            |m: &str| classify_described(AtlatlError::config(m), "/x", "atlatl_delta_0_0").code;
        assert_eq!(
            missing("Catalog Error: Table with name atlatl_delta_0_0 does not exist!").as_deref(),
            Some("delta_not_found")
        );
        assert_eq!(
            missing("Catalog Error: Table with name other does not exist!"),
            None
        );
        let e = classify(
            AtlatlError::internal(
                "IO Error: DeltaKernel InvalidTableLocationError (28): Invalid table location: Path does not exist",
            ),
            "/x",
        );
        assert_eq!(
            (e.class, e.code.as_deref()),
            (ErrorClass::Config, Some("delta_not_found"))
        );
        let e = classify(
            AtlatlError::internal(
                "LogSegment end version 4 not the same as the specified end version 9",
            ),
            "/x",
        );
        assert_eq!(e.code.as_deref(), Some("delta_version"));
        let e = classify(AtlatlError::internal("Binder Error: x"), "/x");
        assert_eq!(e.code, None);
    }
}
