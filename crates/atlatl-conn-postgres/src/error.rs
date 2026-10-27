//! Postgres → error taxonomy (spec 03 §5).

use std::error::Error as _;

use atlatl_core::error::classify_io;
use atlatl_core::redact::redact;
use atlatl_core::{AtlatlError, ErrorClass};

pub fn classify_sqlstate(code: &str) -> ErrorClass {
    use ErrorClass as C;
    match code {
        // serialization_failure, deadlock_detected, lock_not_available
        "40001" | "40P01" | "55P03" => C::Conflict,
        // query_canceled (statement_timeout, pg_cancel_backend)
        "57014" => C::Timeout,
        // admin/crash shutdown, cannot_connect_now, idle_session_timeout, too_many_connections,
        // idle_in_transaction_session_timeout, read_only_sql_transaction (writer failed over)
        "57P01" | "57P02" | "57P03" | "57P05" | "53300" | "25P03" | "25006" => C::Transient,
        _ => match code.get(..2) {
            Some("08") => C::Transient,
            Some("28") => C::Auth,
            Some("22" | "23") => C::Data,
            Some("42" | "3D" | "3F" | "0A") => C::Config,
            _ => C::Internal,
        },
    }
}

pub fn classify(e: &tokio_postgres::Error) -> AtlatlError {
    if let Some(db) = e.as_db_error() {
        let code = db.code().code();
        let mut m = format!("{}: {}", db.severity(), db.message());
        if let Some(d) = db.detail() {
            m.push_str(&format!("; detail: {d}"));
        }
        if let Some(c) = db.constraint() {
            m.push_str(&format!("; constraint: {c}"));
        }
        if let Some(h) = db.hint() {
            m.push_str(&format!("; hint: {h}"));
        }
        return AtlatlError::new(classify_sqlstate(code), redact(&m).into_owned()).code(code);
    }
    let kind = e.to_string();
    let class = if e.is_closed() {
        ErrorClass::Transient
    } else if let Some(c) = tls_class(e) {
        c
    } else if let Some(io) = io_source(e) {
        match classify_io(io) {
            ErrorClass::Internal => ErrorClass::Transient,
            c => c,
        }
    } else {
        match kind.as_str() {
            "timeout waiting for server" => ErrorClass::Timeout,
            "error performing TLS handshake" => ErrorClass::Transient,
            "error parsing response from server" => ErrorClass::Decode,
            "authentication error" => ErrorClass::Auth,
            "invalid configuration" | "invalid connection string" => ErrorClass::Config,
            _ => ErrorClass::Internal,
        }
    };
    let mut m = kind;
    let mut src = e.source();
    while let Some(s) = src {
        m.push_str(&format!(": {s}"));
        src = s.source();
    }
    AtlatlError::new(class, redact(&m).into_owned())
}

/// [`classify`], keeping `e` as the source.
pub fn wrap(e: tokio_postgres::Error) -> AtlatlError {
    classify(&e).source(e)
}

/// Certificate and negotiation failures won't fix themselves on retry.
fn tls_class(e: &tokio_postgres::Error) -> Option<ErrorClass> {
    if e.to_string() != "error performing TLS handshake" {
        return None;
    }
    let src = e.source()?;
    let rustls_err = src
        .downcast_ref::<std::io::Error>()
        .and_then(|io| io.get_ref())
        .and_then(|i| i.downcast_ref::<rustls::Error>());
    use rustls::Error as R;
    Some(match rustls_err {
        Some(
            R::InvalidCertificate(_)
            | R::NoCertificatesPresented
            | R::UnsupportedNameType
            | R::PeerIncompatible(_)
            | R::InvalidCertRevocationList(_),
        ) => ErrorClass::Config,
        Some(_) => ErrorClass::Transient,
        None => match src.to_string().as_str() {
            "server does not support TLS" | "no hostname provided for TLS handshake" => {
                ErrorClass::Config
            }
            _ => match src.downcast_ref::<std::io::Error>().map(|io| io.kind()) {
                Some(std::io::ErrorKind::InvalidInput) => ErrorClass::Config,
                _ => ErrorClass::Transient,
            },
        },
    })
}

fn io_source(e: &tokio_postgres::Error) -> Option<&std::io::Error> {
    let mut src = e.source();
    while let Some(s) = src {
        if let Some(io) = s.downcast_ref::<std::io::Error>() {
            return Some(io);
        }
        src = s.source();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use ErrorClass as C;

    #[test]
    fn sqlstate_table() {
        let cases = [
            ("40001", C::Conflict),
            ("40P01", C::Conflict),
            ("55P03", C::Conflict),
            ("57014", C::Timeout),
            ("08000", C::Transient),
            ("08006", C::Transient),
            ("08P01", C::Transient),
            ("57P01", C::Transient),
            ("57P02", C::Transient),
            ("57P03", C::Transient),
            ("57P05", C::Transient),
            ("53300", C::Transient),
            ("25P03", C::Transient),
            ("25006", C::Transient),
            ("28000", C::Auth),
            ("28P01", C::Auth),
            ("22001", C::Data),
            ("22P02", C::Data),
            ("22003", C::Data),
            ("23505", C::Data),
            ("23502", C::Data),
            ("23503", C::Data),
            ("42601", C::Config),
            ("42P01", C::Config),
            ("42703", C::Config),
            ("42501", C::Config),
            ("3D000", C::Config),
            ("3F000", C::Config),
            ("0A000", C::Config),
            ("40002", C::Internal),
            ("40003", C::Internal),
            ("53100", C::Internal),
            ("55000", C::Internal),
            ("57P04", C::Internal),
            ("58030", C::Internal),
            ("XX000", C::Internal),
            ("P0001", C::Internal),
            ("", C::Internal),
            ("4", C::Internal),
        ];
        for (code, class) in cases {
            assert_eq!(classify_sqlstate(code), class, "{code}");
        }
    }

    #[test]
    fn client_timeout() {
        let e = classify(&tokio_postgres::Error::__private_api_timeout());
        assert_eq!(e.class, C::Timeout);
        assert_eq!(e.code, None);
    }
}
