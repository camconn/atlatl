//! DuckDB error mapping (spec 03 §5). DuckDB reports every failure as `"<Type> Error: <msg>"`;
//! the type prefix and a few known messages (spikes S1, S3, S4) decide the class.

use std::sync::LazyLock;

use atlatl_core::redact::redact;
use atlatl_core::{AtlatlError, ErrorClass};
use regex::Regex;

pub const COMMIT_CONFLICT: &str = "commit_conflict";
pub const CURSOR_EXPIRED: &str = "cursor_expired";
pub const OUT_OF_MEMORY: &str = "out_of_memory";
/// A Postgres session (DuckLake catalog or source) dropped mid-call.
pub const CONNECTION_LOST: &str = "connection_lost";
/// An interrupted query. Classed `timeout`; the caller knows whether it was a deadline or a kill.
pub const INTERRUPTED: &str = "interrupted";

static HTTP_STATUS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"\bHTTP (?:code )?(\d{3})\b|\b(\d{3}) \((?:Unauthorized|Forbidden|Too Many Requests)\)",
    )
    .expect("regex")
});

pub fn classify_message(msg: &str) -> (ErrorClass, Option<&'static str>) {
    use ErrorClass as C;

    if msg.contains("Transaction conflict") && msg.contains("TransactionContext Error") {
        return (C::Conflict, Some(COMMIT_CONFLICT));
    }
    // Two transactions creating the same schema or table in a plain DuckDB file.
    if msg.contains("Catalog write-write conflict") {
        return (C::Conflict, Some(COMMIT_CONFLICT));
    }
    if msg.contains("Failed to flush changes into DuckLake")
        && msg.contains("ducklake_snapshot_pkey")
    {
        return (C::Conflict, Some(COMMIT_CONFLICT));
    }
    if msg.contains("No snapshot found at version") {
        return (C::Data, Some(CURSOR_EXPIRED));
    }
    if is_missing_extension(msg) {
        return (C::Config, None);
    }
    if is_connection_lost(msg) {
        return (C::Transient, Some(CONNECTION_LOST));
    }

    match error_type(msg) {
        Some("INTERRUPT") => (C::Timeout, Some(INTERRUPTED)),
        Some("Out of Memory") => (C::Internal, Some(OUT_OF_MEMORY)),
        Some("Conversion" | "Constraint" | "Out of Range" | "Invalid Input") => (C::Data, None),
        Some(
            "Catalog"
            | "Binder"
            | "Parser"
            | "Syntax"
            | "Invalid Configuration"
            | "Missing Extension"
            | "Not implemented",
        ) => (C::Config, None),
        Some("IO" | "HTTP") => (network_class(msg), None),
        _ => (C::Internal, None),
    }
}

pub fn classify(e: &duckdb::Error) -> AtlatlError {
    let msg = e.to_string();
    let (class, code) = match e {
        duckdb::Error::DuckDBFailure(..) => classify_message(&msg),
        duckdb::Error::IntegralValueOutOfRange(..) => (ErrorClass::Data, None),
        _ => (ErrorClass::Internal, None),
    };
    let err = AtlatlError::new(class, redact(&msg).into_owned());
    match code {
        Some(c) => err.code(c),
        None => err,
    }
}

/// A DuckDB error message from the C API (the raw streaming reader), classified and redacted.
pub fn from_message(msg: &str) -> AtlatlError {
    let (class, code) = classify_message(msg);
    let err = AtlatlError::new(class, redact(msg).into_owned());
    match code {
        Some(c) => err.code(c),
        None => err,
    }
}

/// `duckdb::Result` → atlatl `Result`, for `?` inside engine closures.
pub trait DbResultExt<T> {
    fn db(self) -> atlatl_core::error::Result<T>;
}

impl<T> DbResultExt<T> for duckdb::Result<T> {
    fn db(self) -> atlatl_core::error::Result<T> {
        self.map_err(classify_owned)
    }
}

/// [`classify`], keeping `e` as the source.
pub fn classify_owned(e: duckdb::Error) -> AtlatlError {
    classify(&e).source(e)
}

fn error_type(msg: &str) -> Option<&str> {
    let (head, _) = msg.split_once(" Error: ")?;
    (!head.contains('\n') && head.len() <= 32).then_some(head.trim())
}

/// libpq and postgres-extension messages for a dead session. A failed `ROLLBACK` during a commit
/// means the catalog connection is gone (the extension reports nothing else).
fn is_connection_lost(msg: &str) -> bool {
    const NEEDLES: [&str; 8] = [
        "server closed the connection unexpectedly",
        "no connection to the server",
        "could not connect to server",
        "Connection refused",
        "Connection reset by peer",
        "SSL SYSCALL error",
        "terminating connection due to administrator command",
        "Failed to execute query \"ROLLBACK\"",
    ];
    !msg.contains("authentication failed") && NEEDLES.iter().any(|n| msg.contains(n))
}

fn is_missing_extension(msg: &str) -> bool {
    msg.starts_with("Missing Extension Error: ")
        || (msg.contains("Extension \"") && msg.contains("not found"))
        || msg.contains("Install it first using")
}

fn network_class(msg: &str) -> ErrorClass {
    let status = HTTP_STATUS
        .captures_iter(msg)
        .filter_map(|c| c.get(1).or(c.get(2)))
        .filter_map(|m| m.as_str().parse::<u16>().ok())
        .find(|s| *s >= 400);
    if msg.contains("password authentication failed") {
        return ErrorClass::Auth;
    }
    status
        .and_then(atlatl_core::error::classify_http_status)
        .unwrap_or(ErrorClass::Transient)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ErrorClass as C;

    #[test]
    fn known_messages() {
        let cases: &[(&str, ErrorClass, Option<&str>)] = &[
            (
                "TransactionContext Error: Failed to commit: Failed to commit DuckLake transaction.\n\
                 Transaction conflict - attempting to insert into table with index \"23\" - but another transaction has deleted from it",
                C::Conflict,
                Some(COMMIT_CONFLICT),
            ),
            (
                "TransactionContext Error: Failed to commit: Failed to commit DuckLake transaction.\n\
                 Transaction conflict - attempting to delete from table with index \"5\" - but another transaction has inserted into it",
                C::Conflict,
                Some(COMMIT_CONFLICT),
            ),
            (
                "TransactionContext Error: Failed to commit: Failed to commit DuckLake transaction.\n\
                 Transaction conflict - attempting to insert into table with index \"7\" - but another transaction has altered it",
                C::Conflict,
                Some(COMMIT_CONFLICT),
            ),
            (
                "TransactionContext Error: Catalog write-write conflict on create with \"br\"",
                C::Conflict,
                Some(COMMIT_CONFLICT),
            ),
            (
                "TransactionContext Error: Failed to commit: Failed to flush changes into DuckLake: \
                 Failed to execute query: ERROR:  duplicate key value violates unique constraint \"ducklake_snapshot_pkey\"",
                C::Conflict,
                Some(COMMIT_CONFLICT),
            ),
            (
                "TransactionContext Error: Failed to commit: Failed to execute query \"ROLLBACK\": ",
                C::Transient,
                Some(CONNECTION_LOST),
            ),
            (
                "IO Error: Unable to connect to Postgres at \"host=127.0.0.1 port=1\": connection to server at \"127.0.0.1\", port 1 failed: Connection refused",
                C::Transient,
                Some(CONNECTION_LOST),
            ),
            (
                "Invalid Error: Failed to execute query \"SELECT 1\": server closed the connection unexpectedly\n\tThis probably means the server terminated abnormally",
                C::Transient,
                Some(CONNECTION_LOST),
            ),
            (
                "IO Error: Unable to connect to Postgres: connection to server at \"h\" failed: FATAL:  password authentication failed for user \"u\"",
                C::Auth,
                None,
            ),
            (
                "TransactionContext Error: cannot commit - transaction is aborted",
                C::Internal,
                None,
            ),
            (
                "Invalid Input Error: No snapshot found at version 3",
                C::Data,
                Some(CURSOR_EXPIRED),
            ),
            (
                "Binder Error: No snapshot found at version 12",
                C::Data,
                Some(CURSOR_EXPIRED),
            ),
            (
                "INTERRUPT Error: Interrupted!",
                C::Timeout,
                Some(INTERRUPTED),
            ),
            (
                "Conversion Error: Could not convert string 'x' to INT32",
                C::Data,
                None,
            ),
            (
                "Constraint Error: Duplicate key \"id: 1\" violates primary key constraint.",
                C::Data,
                None,
            ),
            (
                "Out of Range Error: Type INT32 with value 3000000000 can't be cast because the value is out of range for the destination type INT32",
                C::Data,
                None,
            ),
            ("Invalid Input Error: boom at 5M", C::Data, None),
            (
                "Catalog Error: Table with name nope does not exist!\nDid you mean \"temp.information_schema.tables\"?",
                C::Config,
                None,
            ),
            (
                "Binder Error: Referenced column \"nope\" not found in FROM clause!",
                C::Config,
                None,
            ),
            (
                "Parser Error: syntax error at or near \"SELEC\"",
                C::Config,
                None,
            ),
            (
                "Invalid Configuration Error: Cannot change allow_unsigned_extensions setting while database is running",
                C::Config,
                None,
            ),
            (
                "IO Error: Extension \"/x/v1.5.6/linux_amd64/ducklake.duckdb_extension\" not found.\n\
                 Extension \"ducklake\" is an existing extension.\n\nInstall it first using \"INSTALL ducklake\".",
                C::Config,
                None,
            ),
            (
                "Catalog Error: Table Function with name \"ducklake_snapshots\" is not in the catalog, but it exists in the ducklake extension.\n\n\
                 Please try installing and loading the ducklake extension by running:\nINSTALL ducklake;\nLOAD ducklake;",
                C::Config,
                None,
            ),
            (
                "Missing Extension Error: File s3://b/k.parquet requires the extension httpfs to be loaded",
                C::Config,
                None,
            ),
            (
                "IO Error: Unable to connect to Postgres at host=localhost port=5432: connection refused",
                C::Transient,
                None,
            ),
            (
                "IO Error: Unable to connect to Postgres at host=db: FATAL:  password authentication failed for user \"u\"",
                C::Auth,
                None,
            ),
            (
                "HTTP Error: HTTP GET error on 'https://b.s3.amazonaws.com/k' (HTTP 503)",
                C::Transient,
                None,
            ),
            (
                "HTTP Error: HTTP GET error on 'https://b.s3.amazonaws.com/k' (HTTP 403)",
                C::Auth,
                None,
            ),
            (
                "HTTP Error: Unable to connect to URL \"https://h/x\": 401 (Unauthorized).",
                C::Auth,
                None,
            ),
            (
                "HTTP Error: HTTP GET error on 'https://h/x' (HTTP 429)",
                C::Throttled,
                None,
            ),
            (
                "IO Error: Could not read 'https://h/403/x' (HTTP 500)",
                C::Transient,
                None,
            ),
            (
                "HTTP Error: HTTP GET error on 'https://b.s3.amazonaws.com/missing.parquet' (HTTP 404)",
                C::Config,
                None,
            ),
            (
                "HTTP Error: Unable to connect to URL s3://b/x.parquet: Insufficient Storage (HTTP code 507)",
                C::Transient,
                None,
            ),
            (
                "Out of Memory Error: failed to allocate data of size 4.0 MiB (4.3 MiB/7.6 MiB used)\n\
                 Database is launched in in-memory mode and no temporary directory is specified.",
                C::Internal,
                Some(OUT_OF_MEMORY),
            ),
            (
                "INTERNAL Error: Attempted to access index 3 within vector of size 3",
                C::Internal,
                None,
            ),
            ("something unexpected", C::Internal, None),
            ("", C::Internal, None),
        ];
        for (msg, class, code) in cases {
            assert_eq!(classify_message(msg), (*class, *code), "{msg}");
        }
    }

    #[test]
    fn type_prefix_must_lead() {
        assert_eq!(
            classify_message("Failed: see Catalog Error: x"),
            (C::Internal, None)
        );
        assert_eq!(
            classify_message("line one\nParser Error: x"),
            (C::Internal, None)
        );
    }

    #[test]
    fn classify_redacts_and_codes() {
        atlatl_core::redact::register("hunter2-engine-test");
        let e = duckdb::Error::DuckDBFailure(
            duckdb::ffi::Error::new(1),
            Some(
                "IO Error: Unable to connect to Postgres at password=hunter2-engine-test host=x"
                    .into(),
            ),
        );
        let a = classify(&e);
        assert_eq!(a.class, C::Transient);
        assert!(!a.message.contains("hunter2"), "{}", a.message);

        let e = duckdb::Error::DuckDBFailure(
            duckdb::ffi::Error::new(1),
            Some("INTERRUPT Error: Interrupted!".into()),
        );
        let a = classify_owned(e);
        assert_eq!(
            (a.class, a.code.as_deref()),
            (C::Timeout, Some(INTERRUPTED))
        );
        assert!(std::error::Error::source(&a).is_some());
    }

    #[test]
    fn non_duckdb_variants() {
        assert_eq!(
            classify(&duckdb::Error::IntegralValueOutOfRange(0, 1 << 40)).class,
            C::Data
        );
        assert_eq!(
            classify(&duckdb::Error::QueryReturnedNoRows).class,
            C::Internal
        );
        let e = duckdb::Error::InvalidColumnName("Catalog Error: x".into());
        assert_eq!(classify(&e).class, C::Internal);
    }
}
