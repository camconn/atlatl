//! Postgres as an ingest source (06 §3.2): catalog discovery and column metadata over
//! tokio-postgres in read-only sessions, and reads through DuckDB's postgres extension attached
//! `READ_ONLY` (every scan runs in a server-side `READ ONLY` transaction).

use std::sync::Arc;

use atlatl_core::config::model::SslMode;
use atlatl_core::error::Result;
use atlatl_core::redact;
use atlatl_core::{AtlatlError, ErrorClass};
use rustls::ClientConfig;
use tokio::sync::Mutex;

use crate::connect::{ConnectOptions, Session, connect_with};
use crate::error::wrap;

/// How a column is read so the staged value is exact and matches the adoption rules.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Read {
    /// The scanner's own type mapping.
    Native,
    /// Enum → VARCHAR (engine-side cast).
    Enum,
    /// Server-side `::text`: numerics wider than DECIMAL(38) or unconstrained (the scanner reads
    /// them as DOUBLE), and geometric types.
    ServerText,
    /// Server-side `to_json(…)::text`: arrays and composites.
    ServerJson,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceColumn {
    pub name: String,
    /// `format_type(atttypid, atttypmod)`.
    pub pg_type: String,
    pub read: Read,
    /// `pg_type.typname`.
    pub typname: String,
}

/// An `update_key` cursor to evaluate in Postgres (06 §3.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cursor {
    pub column: String,
    /// The stored watermark, as text.
    pub stored: Option<String>,
    pub lookback_s: Option<u64>,
    pub inclusive: bool,
    pub include_nulls: bool,
}

/// The type a watermark literal is cast to, for types whose Postgres comparison matches the
/// engine's on the value the scanner reads: integers, floats and numerics, dates and times, and
/// text (compared as `C`, i.e. byte-wise).
fn comparable(typname: &str) -> Option<&'static str> {
    Some(match typname {
        "int2" => "int2",
        "int4" => "int4",
        "int8" => "int8",
        "float4" => "float4",
        "float8" => "float8",
        "numeric" => "numeric",
        "date" => "date",
        "time" => "time",
        "timestamp" => "timestamp",
        "timestamptz" => "timestamptz",
        "text" | "varchar" => "text",
        _ => return None,
    })
}

/// The cursor predicate in Postgres SQL, when it is safe there: the column is read natively
/// (not converted in Postgres) and has a comparable type. The watermark is a quoted literal cast
/// to the column's own type; text compares byte-wise like the engine.
pub fn cursor_predicate(cur: &Cursor, cols: &[SourceColumn]) -> Option<String> {
    let col = cols.iter().find(|c| c.name == cur.column).or_else(|| {
        cols.iter()
            .find(|c| c.name.eq_ignore_ascii_case(&cur.column))
    })?;
    if col.read != Read::Native {
        return None;
    }
    let ty = comparable(&col.typname)?;
    let text = ty == "text";
    if cur.lookback_s.is_some() && !matches!(ty, "date" | "timestamp" | "timestamptz") {
        return None;
    }
    let i = ident(&col.name);
    let lhs = if text {
        format!("{i} COLLATE \"C\"")
    } else {
        i.clone()
    };
    let bound = cur.stored.as_ref().map(|v| {
        let lit = format!("{}::{ty}", literal(v));
        let lit = match cur.lookback_s {
            Some(s) => format!("({lit} - {}::interval)", literal(&format!("{s} seconds"))),
            None => lit,
        };
        format!("{lhs} {} {lit}", if cur.inclusive { ">=" } else { ">" })
    });
    Some(match (bound, cur.include_nulls) {
        (Some(b), true) => format!("({b} OR {i} IS NULL)"),
        (Some(b), false) => b,
        (None, true) => return None,
        (None, false) => format!("{i} IS NOT NULL"),
    })
}

pub fn read_of(typtype: &str, typcategory: &str, typname: &str, typmod: i32) -> Read {
    if typcategory == "A" || typtype == "c" {
        Read::ServerJson
    } else if typtype == "e" {
        Read::Enum
    } else if typcategory == "G" || (typname == "numeric" && numeric_precision(typmod).is_none()) {
        Read::ServerText
    } else {
        Read::Native
    }
}

/// `(precision, scale)` of a `numeric` typmod when DuckDB's DECIMAL holds it exactly.
pub fn numeric_precision(typmod: i32) -> Option<(u8, u8)> {
    if typmod < 4 {
        return None;
    }
    let m = typmod - 4;
    let (p, s) = ((m >> 16) & 0xffff, m & 0xffff);
    ((1..=38).contains(&p) && s <= p).then_some((p as u8, s as u8))
}

pub fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn libpq_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\\' || c == '\'' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('\'');
    out
}

/// A libpq keyword DSN for the DuckDB attach: every value quoted, keepalives and statement
/// timeout carried over. Registers the password (raw, libpq-quoted, and inside a SQL literal).
pub fn libpq_dsn(o: &ConnectOptions) -> String {
    let mut parts: Vec<(&str, String)> = vec![
        ("host", o.host.clone()),
        ("port", o.port.to_string()),
        ("dbname", o.database.clone()),
        ("user", o.user.clone()),
    ];
    if let Some(p) = &o.password {
        let q = libpq_value(p);
        redact::register(p);
        redact::register(&q);
        redact::register(&q[1..q.len() - 1].replace('\'', "''"));
        parts.push(("password", p.clone()));
    }
    parts.push((
        "sslmode",
        match o.sslmode {
            SslMode::Disable => "disable",
            SslMode::Require => "require",
            SslMode::VerifyFull => "verify-full",
        }
        .into(),
    ));
    if let Some(ca) = &o.sslrootcert {
        parts.push(("sslrootcert", ca.to_string_lossy().into_owned()));
    }
    if let Some(t) = o.connect_timeout {
        parts.push(("connect_timeout", t.as_secs().max(1).to_string()));
    }
    parts.push(("application_name", o.application_name.clone()));
    match o.keepalive {
        Some(k) => {
            parts.push(("keepalives", "1".into()));
            parts.push(("keepalives_idle", k.idle.as_secs().max(1).to_string()));
            parts.push((
                "keepalives_interval",
                k.interval.as_secs().max(1).to_string(),
            ));
            parts.push(("keepalives_count", k.count.to_string()));
        }
        None => parts.push(("keepalives", "0".into())),
    }
    if let Some(t) = o.statement_timeout {
        parts.push(("options", format!("-c statement_timeout={}", t.as_millis())));
    }
    parts
        .iter()
        .map(|(k, v)| format!("{k}={}", libpq_value(v)))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Attaches the source as `alias`, read-only.
pub fn attach_sql(alias: &str, o: &ConnectOptions) -> String {
    format!(
        "LOAD postgres_scanner; DETACH DATABASE IF EXISTS {a}; ATTACH {} AS {a} (TYPE postgres, READ_ONLY)",
        literal(&libpq_dsn(o)),
        a = ident(alias)
    )
}

pub fn detach_sql(alias: &str) -> String {
    format!("DETACH DATABASE IF EXISTS {}", ident(alias))
}

/// The engine relation reading `schema.table` through the attach `alias`. Columns the scanner
/// can't read exactly are converted in Postgres (`postgres_query`); filters then apply
/// engine-side instead of being pushed into the scan.
pub fn relation(alias: &str, schema: &str, table: &str, cols: &[SourceColumn]) -> Result<String> {
    relation_where(alias, schema, table, cols, None).map(|(r, _)| r)
}

/// [`relation`], with `server_where` (Postgres SQL, e.g. [`cursor_predicate`]) evaluated in
/// Postgres when the member is read through `postgres_query`. Returns whether it was: a native
/// scan leaves filtering to the engine, which pushes it into the scan itself.
pub fn relation_where(
    alias: &str,
    schema: &str,
    table: &str,
    cols: &[SourceColumn],
    server_where: Option<&str>,
) -> Result<(String, bool)> {
    if cols.is_empty() {
        return Err(AtlatlError::config(format!(
            "postgres source `{schema}.{table}` has no columns to read"
        )));
    }
    if cols
        .iter()
        .all(|c| matches!(c.read, Read::Native | Read::Enum))
    {
        let proj = cols
            .iter()
            .map(|c| {
                let i = ident(&c.name);
                match c.read {
                    Read::Enum => format!("CAST({i} AS VARCHAR) AS {i}"),
                    _ => i,
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        return Ok((
            format!(
                "(SELECT {proj} FROM {}.{}.{})",
                ident(alias),
                ident(schema),
                ident(table)
            ),
            false,
        ));
    }
    let proj = cols
        .iter()
        .map(|c| {
            let i = ident(&c.name);
            match c.read {
                Read::Native => i,
                Read::Enum | Read::ServerText => format!("{i}::text AS {i}"),
                Read::ServerJson => format!("to_json({i})::text AS {i}"),
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!("SELECT {proj} FROM {}.{}", ident(schema), ident(table));
    if let Some(w) = server_where {
        sql.push_str(&format!(" WHERE {w}"));
    }
    Ok((query_relation(alias, &sql), server_where.is_some()))
}

/// Adopts an exported snapshot in the engine's current transaction on `alias`; it must run
/// before anything reads from the attach in that transaction.
pub fn set_snapshot_sql(alias: &str, snapshot: &str) -> String {
    format!(
        "CALL postgres_execute({}, {})",
        literal(alias),
        literal(&format!("SET TRANSACTION SNAPSHOT {}", literal(snapshot)))
    )
}

/// `from.query`, run in Postgres.
pub fn query_relation(alias: &str, query: &str) -> String {
    format!(
        "(SELECT * FROM postgres_query({}, {}))",
        literal(alias),
        literal(query)
    )
}

/// `schema.table` with `*`/`?` globs; no schema means `public`.
pub fn split(pattern: &str) -> (String, String) {
    match pattern.split_once('.') {
        Some((s, t)) => (s.to_owned(), t.to_owned()),
        None => ("public".into(), pattern.to_owned()),
    }
}

fn is_glob(s: &str) -> bool {
    s.contains(['*', '?'])
}

/// `*` and `?` glob, byte-wise.
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

/// Matching `schema.table` names, sorted byte-wise (independent of the server collation). Names
/// containing `.` are skipped; `exclude` takes names or globs.
pub fn match_tables(
    rows: impl IntoIterator<Item = (String, String)>,
    pattern: &str,
    exclude: &[String],
) -> Vec<String> {
    let (ps, pt) = split(pattern);
    let mut out: Vec<String> = rows
        .into_iter()
        .filter(|(s, t)| !s.contains('.') && !t.contains('.'))
        .filter(|(s, t)| glob_match(s, &ps) && glob_match(t, &pt))
        .map(|(s, t)| format!("{s}.{t}"))
        .filter(|n| !exclude.iter().any(|e| n == e || glob_match(n, e)))
        .collect();
    out.sort();
    out.dedup();
    out
}

/// Base tables (plain and partitioned; partitions are read through their parent), plus views and
/// materialized views when asked. System schemas are never listed.
pub fn tables_sql(include_views: bool, one_schema: bool) -> String {
    let kinds = if include_views {
        "'r', 'p', 'v', 'm'"
    } else {
        "'r', 'p'"
    };
    let mut sql = format!(
        "SELECT n.nspname::text, c.relname::text FROM pg_catalog.pg_class c \
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relkind IN ({kinds}) AND NOT c.relispartition \
         AND n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\\_%'"
    );
    if one_schema {
        sql.push_str(" AND n.nspname = $1");
    }
    sql
}

/// The primary key's columns, in key order (none for views and keyless tables).
pub const PRIMARY_KEY_SQL: &str = "SELECT a.attname::text \
     FROM pg_catalog.pg_index i \
     CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) \
     JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum \
     WHERE i.indrelid = $1::text::regclass AND i.indisprimary \
     ORDER BY k.ord";

pub const COLUMNS_SQL: &str = "SELECT a.attname::text, format_type(a.atttypid, a.atttypmod), \
     t.typtype::text, t.typcategory::text, t.typname::text, a.atttypmod \
     FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type t ON t.oid = a.atttypid \
     WHERE a.attrelid = $1::text::regclass AND a.attnum > 0 AND NOT a.attisdropped \
     ORDER BY a.attnum";

/// A source connection: one lazily opened read-only session for catalog queries, and optionally
/// one holding an exported snapshot.
pub struct PgSource {
    opts: ConnectOptions,
    tls: Option<Arc<ClientConfig>>,
    session: Mutex<Option<Session>>,
    snapshot: Mutex<Option<(Session, String)>>,
}

impl PgSource {
    pub fn new(opts: ConnectOptions) -> Result<Self> {
        let tls = opts.tls()?;
        Ok(Self {
            opts,
            tls,
            session: Mutex::new(None),
            snapshot: Mutex::new(None),
        })
    }

    pub fn options(&self) -> &ConnectOptions {
        &self.opts
    }

    async fn session(&self) -> Result<tokio::sync::MutexGuard<'_, Option<Session>>> {
        let mut g = self.session.lock().await;
        if g.as_ref().is_none_or(Session::is_closed) {
            let s = connect_with(&self.opts, self.tls.as_ref()).await?;
            s.client()
                .batch_execute("SET default_transaction_read_only = on")
                .await
                .map_err(wrap)?;
            *g = Some(s);
        }
        Ok(g)
    }

    /// Sorted `schema.table` members matching a `from.tables` glob.
    pub async fn discover(
        &self,
        pattern: &str,
        exclude: &[String],
        include_views: bool,
    ) -> Result<Vec<String>> {
        let (schema, _) = split(pattern);
        let one = !is_glob(&schema);
        let g = self.session().await?;
        let c = g.as_ref().expect("session").client();
        let sql = tables_sql(include_views, one);
        let rows = if one {
            c.query(&sql, &[&schema]).await
        } else {
            c.query(&sql, &[]).await
        }
        .map_err(|e| context(wrap(e), &format!("discovering `{pattern}`")))?;
        Ok(match_tables(
            rows.iter()
                .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1))),
            pattern,
            exclude,
        ))
    }

    /// Columns of `schema.table` in ordinal order, narrowed by `columns.include`/`exclude`
    /// (exact name, else case-insensitive).
    pub async fn columns(
        &self,
        schema: &str,
        table: &str,
        include: Option<&[String]>,
        exclude: &[String],
    ) -> Result<Vec<SourceColumn>> {
        let g = self.session().await?;
        let c = g.as_ref().expect("session").client();
        let rel = format!("{}.{}", ident(schema), ident(table));
        let rows = c.query(COLUMNS_SQL, &[&rel]).await.map_err(|e| {
            context(
                wrap(e),
                &format!("postgres source table `{schema}.{table}`"),
            )
        })?;
        let named = |list: &[String], n: &str| {
            list.iter().any(|x| x == n) || list.iter().any(|x| x.eq_ignore_ascii_case(n))
        };
        Ok(rows
            .iter()
            .map(|r| SourceColumn {
                name: r.get(0),
                pg_type: r.get(1),
                read: read_of(r.get(2), r.get(3), r.get(4), r.get(5)),
                typname: r.get(4),
            })
            .filter(|c| include.is_none_or(|i| named(i, &c.name)))
            .filter(|c| !named(exclude, &c.name))
            .collect())
    }

    /// The primary key of `schema.table` from its constraint; `None` without one.
    pub async fn primary_key(&self, schema: &str, table: &str) -> Result<Option<Vec<String>>> {
        let g = self.session().await?;
        let c = g.as_ref().expect("session").client();
        let rel = format!("{}.{}", ident(schema), ident(table));
        let rows = c.query(PRIMARY_KEY_SQL, &[&rel]).await.map_err(|e| {
            context(
                wrap(e),
                &format!("primary key of postgres source table `{schema}.{table}`"),
            )
        })?;
        let cols: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
        Ok((!cols.is_empty()).then_some(cols))
    }

    /// Opens a `REPEATABLE READ READ ONLY` transaction on its own session and exports its
    /// snapshot; it stays valid (and holds back the server's xmin horizon) until
    /// [`Self::release_snapshot`]. Idempotent.
    pub async fn export_snapshot(&self) -> Result<String> {
        let mut g = self.snapshot.lock().await;
        if let Some((s, id)) = g.as_ref()
            && !s.is_closed()
        {
            return Ok(id.clone());
        }
        let s = connect_with(&self.opts, self.tls.as_ref()).await?;
        s.client()
            .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(wrap)?;
        let id: String = s
            .client()
            .query_one("SELECT pg_export_snapshot()", &[])
            .await
            .map_err(|e| context(wrap(e), "exporting a snapshot"))?
            .get(0);
        *g = Some((s, id.clone()));
        Ok(id)
    }

    pub async fn release_snapshot(&self) {
        if let Some((s, _)) = self.snapshot.lock().await.take() {
            let _ = s.client().batch_execute("COMMIT").await;
            s.abort();
        }
    }

    pub async fn close(&self) {
        self.release_snapshot().await;
        if let Some(s) = self.session.lock().await.take() {
            s.abort();
        }
    }
}

fn context(mut e: AtlatlError, what: &str) -> AtlatlError {
    e.message = format!("{what}: {}", e.message);
    e
}

/// Errors from DuckDB's postgres extension carry libpq/server text but no SQLSTATE; map the
/// known ones (`None`: not recognised).
pub fn classify_message(msg: &str) -> Option<(ErrorClass, &'static str)> {
    use ErrorClass as C;
    let has = |n: &str| msg.contains(n);
    Some(
        if has("password authentication failed") || has("no pg_hba.conf entry") {
            (C::Auth, "28P01")
        } else if has("role \"") && has("does not exist") {
            (C::Auth, "28000")
        } else if has("database \"") && has("does not exist") {
            (C::Config, "3D000")
        } else if has("permission denied") {
            (C::Config, "42501")
        } else if has("in a read-only transaction") {
            (C::Config, "25006")
        } else if has("canceling statement due to statement timeout") {
            (C::Timeout, "57014")
        } else if has("timeout expired") {
            (C::Timeout, "08001")
        } else if has("server does not support SSL") || has("certificate verify failed") {
            (C::Config, "08P01")
        } else if has("relation \"") && has("does not exist") {
            (C::Config, "42P01")
        } else if has("invalid snapshot identifier") {
            (C::Transient, "snapshot_lost")
        } else {
            return None;
        },
    )
}

/// Re-maps an engine error raised while reading a postgres source. Classified errors (data,
/// config, conflict, killed…) are kept.
pub fn reclassify(e: AtlatlError) -> AtlatlError {
    if !matches!(e.class, ErrorClass::Internal | ErrorClass::Transient) {
        return e;
    }
    let (class, code) = match classify_message(&e.message) {
        Some((c, code)) => (c, Some(code)),
        None if e.class == ErrorClass::Internal => {
            atlatl_engine::error::classify_message(&e.message)
        }
        None => return e,
    };
    AtlatlError {
        class,
        code: code.map(str::to_owned).or(e.code.clone()),
        ..e
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, read: Read) -> SourceColumn {
        typed(name, read, "text", "text")
    }

    fn typed(name: &str, read: Read, typname: &str, pg_type: &str) -> SourceColumn {
        SourceColumn {
            name: name.into(),
            pg_type: pg_type.into(),
            read,
            typname: typname.into(),
        }
    }

    #[test]
    fn read_modes() {
        let nm = |p: i32, s: i32| ((p << 16) | s) + 4;
        assert_eq!(read_of("b", "N", "numeric", nm(12, 4)), Read::Native);
        assert_eq!(read_of("b", "N", "numeric", nm(38, 38)), Read::Native);
        assert_eq!(read_of("b", "N", "numeric", nm(39, 2)), Read::ServerText);
        assert_eq!(
            read_of("b", "N", "numeric", -1),
            Read::ServerText,
            "unconstrained"
        );
        assert_eq!(read_of("b", "A", "_text", -1), Read::ServerJson);
        assert_eq!(read_of("c", "C", "pair", -1), Read::ServerJson);
        assert_eq!(read_of("e", "E", "mood", -1), Read::Enum);
        assert_eq!(read_of("b", "G", "point", -1), Read::ServerText);
        assert_eq!(read_of("b", "U", "jsonb", -1), Read::Native);
        assert_eq!(read_of("b", "D", "timestamptz", -1), Read::Native);
        assert_eq!(numeric_precision(nm(12, 4)), Some((12, 4)));
    }

    #[test]
    fn relations() {
        let native = [col("id", Read::Native), col("m", Read::Enum)];
        assert_eq!(
            relation("pg", "appdb", "t", &native).expect("rel"),
            r#"(SELECT "id", CAST("m" AS VARCHAR) AS "m" FROM "pg"."appdb"."t")"#
        );
        let mixed = [
            col("id", Read::Native),
            col("m", Read::Enum),
            col("tags", Read::ServerJson),
            col("it's", Read::ServerText),
        ];
        assert_eq!(
            relation("pg", "appdb", "T", &mixed).expect("rel"),
            r#"(SELECT * FROM postgres_query('pg', 'SELECT "id", "m"::text AS "m", to_json("tags")::text AS "tags", "it''s"::text AS "it''s" FROM "appdb"."T"'))"#
        );
        assert_eq!(
            relation("pg", "a", "t", &[]).expect_err("empty").class,
            ErrorClass::Config
        );
    }

    #[test]
    fn cursor_predicates_push_only_when_safe() {
        let cols = [
            typed("id", Read::Native, "int4", "integer"),
            typed(
                "at",
                Read::Native,
                "timestamptz",
                "timestamp with time zone",
            ),
            typed("name", Read::Native, "varchar", "character varying(20)"),
            typed("tags", Read::ServerJson, "_text", "text[]"),
            typed("big", Read::ServerText, "numeric", "numeric"),
            typed("m", Read::Enum, "mood", "mood"),
            typed("u", Read::Native, "uuid", "uuid"),
        ];
        let cur = |c: &str, v: Option<&str>| Cursor {
            column: c.into(),
            stored: v.map(Into::into),
            lookback_s: None,
            inclusive: true,
            include_nulls: false,
        };
        assert_eq!(
            cursor_predicate(&cur("at", Some("2024-01-01 00:00:00+00")), &cols).as_deref(),
            Some(r#""at" >= '2024-01-01 00:00:00+00'::timestamptz"#)
        );
        assert_eq!(
            cursor_predicate(&cur("AT", None), &cols).as_deref(),
            Some(r#""at" IS NOT NULL"#),
            "no watermark yet: nulls are still excluded"
        );
        assert_eq!(
            cursor_predicate(
                &Cursor {
                    lookback_s: Some(3600),
                    inclusive: false,
                    include_nulls: true,
                    ..cur("at", Some("2024-01-02 00:00:00+00"))
                },
                &cols
            )
            .as_deref(),
            Some(
                r#"("at" > ('2024-01-02 00:00:00+00'::timestamptz - '3600 seconds'::interval) OR "at" IS NULL)"#
            )
        );
        assert_eq!(
            cursor_predicate(
                &Cursor {
                    lookback_s: Some(5),
                    ..cur("id", Some("7"))
                },
                &cols
            ),
            None,
            "a lookback only pushes on dates and timestamps"
        );
        assert_eq!(
            cursor_predicate(&cur("name", Some("it's")), &cols).as_deref(),
            Some(r#""name" COLLATE "C" >= 'it''s'::text"#),
            "text compares byte-wise, as the engine does"
        );
        for c in ["tags", "big", "m", "u", "missing"] {
            assert_eq!(cursor_predicate(&cur(c, Some("x")), &cols), None, "{c}");
        }
        assert_eq!(
            cursor_predicate(
                &Cursor {
                    include_nulls: true,
                    ..cur("at", None)
                },
                &cols
            ),
            None,
            "nothing to filter"
        );

        let (rel, pushed) =
            relation_where("pg", "appdb", "t", &cols[..4], Some(r#""id" >= '1'::int4"#))
                .expect("rel");
        assert!(pushed);
        assert!(
            rel.ends_with(r#"FROM "appdb"."t" WHERE "id" >= ''1''::int4'))"#),
            "{rel}"
        );
        let (rel, pushed) = relation_where("pg", "appdb", "t", &cols[..2], Some("x")).expect("rel");
        assert!(
            !pushed && !rel.contains("WHERE"),
            "native scans filter engine-side: {rel}"
        );
        assert_eq!(
            set_snapshot_sql("pg", "00000003-0000001B-1"),
            "CALL postgres_execute('pg', 'SET TRANSACTION SNAPSHOT ''00000003-0000001B-1''')"
        );
    }

    #[test]
    fn discovery_matching_is_bytewise_sorted() {
        let rows = [
            ("appdb", "b"),
            ("appdb", "B"),
            ("appdb", "a_b"),
            ("appdb", "_x"),
            ("appdb", "aB"),
            ("appdb", "old_users"),
            ("other", "a"),
            ("appdb", "dot.ted"),
        ]
        .map(|(s, t)| (s.to_owned(), t.to_owned()));
        assert_eq!(
            match_tables(rows.clone(), "appdb.*", &["appdb.old_*".into()]),
            ["appdb.B", "appdb._x", "appdb.aB", "appdb.a_b", "appdb.b"]
        );
        assert_eq!(match_tables(rows.clone(), "*.a", &[]), ["other.a"]);
        assert_eq!(match_tables(rows, "appdb.a?b", &[]), ["appdb.a_b"]);
        assert_eq!(split("users_*"), ("public".into(), "users_*".into()));
    }

    #[test]
    fn dsn_quotes_and_redacts() {
        let mut o = ConnectOptions::new("h", 5433, "d b", "u", Some("p'w\\x".into()));
        o.statement_timeout = Some(std::time::Duration::from_secs(5));
        let dsn = libpq_dsn(&o);
        assert!(
            dsn.starts_with(
                "host='h' port='5433' dbname='d b' user='u' password='p\\'w\\\\x' sslmode='disable'"
            ),
            "{dsn}"
        );
        assert!(dsn.contains("keepalives='1' keepalives_idle='30'"), "{dsn}");
        assert!(
            dsn.ends_with("options='-c statement_timeout=5000'"),
            "{dsn}"
        );
        let sql = attach_sql("src", &o);
        assert!(
            sql.ends_with(r#"AS "src" (TYPE postgres, READ_ONLY)"#),
            "{sql}"
        );
        assert!(
            !redact::redact(&sql).contains("w\\\\x"),
            "{}",
            redact::redact(&sql)
        );
    }

    #[test]
    fn scanner_errors() {
        let cases = [
            (
                "IO Error: Unable to connect to Postgres at \"…\": FATAL:  password authentication failed for user \"x\"",
                ErrorClass::Auth,
            ),
            (
                "IO Error: Unable to connect to Postgres at \"…\": FATAL:  database \"nope\" does not exist",
                ErrorClass::Config,
            ),
            (
                "Invalid Error: Failed to prepare COPY \"…\": ERROR:  permission denied for table secret",
                ErrorClass::Config,
            ),
            (
                "IO Error: Failed to fetch header for COPY: ERROR:  canceling statement due to statement timeout",
                ErrorClass::Timeout,
            ),
            (
                "Invalid Error: Failed to execute query \"…\": ERROR:  cannot execute CREATE TABLE in a read-only transaction",
                ErrorClass::Config,
            ),
        ];
        for (m, want) in cases {
            assert_eq!(reclassify(AtlatlError::internal(m)).class, want, "{m}");
        }
        let lost = reclassify(AtlatlError::internal(
            "IO Error: … server closed the connection unexpectedly",
        ));
        assert_eq!(
            (lost.class, lost.code.as_deref()),
            (ErrorClass::Transient, Some("connection_lost"))
        );
        let kept = reclassify(AtlatlError::data(
            "NULL in primary key column `id` (permission denied)",
        ));
        assert_eq!(kept.class, ErrorClass::Data);
    }
}
