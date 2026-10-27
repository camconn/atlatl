//! Redshift Data API and statement errors → the error taxonomy (06 §4, spike S8 §1).

use atlatl_core::error::{AtlatlError, ErrorClass, classify_http_status};
use atlatl_core::redact::redact;

pub const UNLOAD_NOT_EMPTY: &str = "unload_prefix_not_empty";
pub const ACTIVE_WAITING: &str = "ActiveWaitingRequestsExceededException";

/// Class of a Data API exception (`__type`, namespace stripped) at an HTTP status.
pub fn classify_exception(kind: &str, status: u16) -> ErrorClass {
    use ErrorClass as C;
    match kind {
        "ThrottlingException"
        | "ActiveStatementsExceededException"
        | "ActiveSessionsExceededException"
        | "ActiveWaitingRequestsExceededException"
        | "TooManyRequestsException" => C::Throttled,
        "ExecuteStatementException"
        | "BatchExecuteStatementException"
        | "InternalServerException"
        | "DatabaseConnectionException"
        | "QueryTimeoutException"
        | "ServiceUnavailableException" => C::Transient,
        "ValidationException" => C::Config,
        "ResourceNotFoundException" => C::Internal,
        "ExpiredTokenException"
        | "ExpiredToken"
        | "UnrecognizedClientException"
        | "InvalidSignatureException"
        | "IncompleteSignatureException"
        | "AccessDeniedException"
        | "MissingAuthenticationTokenException" => C::Auth,
        _ => classify_http_status(status).unwrap_or(C::Internal),
    }
}

/// `{"__type": "com.amazonaws…#ThrottlingException", "message": …}`, or `x-amzn-ErrorType`.
pub fn api_error(op: &str, status: u16, error_type: Option<&str>, body: &str) -> AtlatlError {
    let v: serde_json::Value = serde_json::from_str(body).unwrap_or_default();
    let raw = v
        .get("__type")
        .and_then(|t| t.as_str())
        .or(error_type)
        .unwrap_or("");
    let kind = raw
        .rsplit('#')
        .next()
        .unwrap_or(raw)
        .split(':')
        .next()
        .unwrap_or("")
        .to_owned();
    let msg = v
        .get("message")
        .or_else(|| v.get("Message"))
        .and_then(|m| m.as_str())
        .map(str::to_owned)
        .unwrap_or_else(|| body.chars().take(300).collect());
    let class = classify_exception(&kind, status);
    let e = AtlatlError::new(
        class,
        redact(&format!(
            "Redshift Data API {op}: HTTP {status} {kind}: {msg}"
        ))
        .into_owned(),
    );
    if kind.is_empty() { e } else { e.code(kind) }
}

/// Class and code of a `FAILED` statement's `Error` text (or a wire error message).
/// The texts are best guesses until spike S8 §3 #11 is run.
pub fn classify_failed(text: &str) -> (ErrorClass, Option<&'static str>) {
    use ErrorClass as C;
    let t = text.to_ascii_lowercase();
    let has = |xs: &[&str]| xs.iter().any(|x| t.contains(x));
    if has(&[
        "unload destination on s3 is not empty",
        "not empty. consider using a different",
    ]) {
        return (C::Internal, Some(UNLOAD_NOT_EMPTY));
    }
    if has(&["slow down", "slowdown", "rate exceeded", "throttl"]) {
        return (C::Throttled, None);
    }
    if has(&[
        "insufficient capacity",
        "not enough capacity",
        "capacity",
        "could not connect",
        "connection reset",
        "connection refused",
        "connection timed out",
        "server closed the connection",
        "terminating connection",
        "ssl syscall",
        "try again",
        "internal error",
        "service unavailable",
        "is being resumed",
        "is not available",
        "transient",
        "serializable isolation violation",
    ]) {
        return (C::Transient, None);
    }
    if has(&[
        "password authentication failed",
        "expired",
        "not authorized to assume",
        "is not authorized",
    ]) {
        return (C::Auth, None);
    }
    if has(&[
        "division by zero",
        "invalid input syntax",
        "overflow",
        "value too long",
        "out of range",
        "invalid digit",
        "violates",
    ]) {
        return (C::Data, None);
    }
    if has(&[
        "permission denied",
        "does not exist",
        "syntax error",
        "not supported",
        "unsupported",
        "invalid operation",
        "cannot be",
        "is ambiguous",
        "access denied",
        "accessdenied",
        "iam role",
        "different region",
        "specified endpoint",
    ]) {
        return (C::Config, None);
    }
    (C::Internal, None)
}

pub fn failed(stmt: &str, text: &str) -> AtlatlError {
    let (class, code) = classify_failed(text);
    let e = AtlatlError::new(
        class,
        redact(&format!("Redshift statement {stmt} failed: {text}")).into_owned(),
    );
    match code {
        Some(c) => e.code(c),
        None => e,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exceptions() {
        let e = api_error(
            "ExecuteStatement",
            400,
            None,
            r#"{"__type":"com.amazonaws.redshiftdata#ThrottlingException","message":"Rate exceeded"}"#,
        );
        assert_eq!(e.class, ErrorClass::Throttled);
        assert_eq!(e.code.as_deref(), Some("ThrottlingException"));
        let e = api_error("X", 500, Some("InternalServerException:http://x"), "{}");
        assert_eq!(
            (e.class, e.code.as_deref()),
            (ErrorClass::Transient, Some("InternalServerException"))
        );
        let c = |k| classify_exception(k, 400);
        assert_eq!(c("ValidationException"), ErrorClass::Config);
        assert_eq!(
            c("ActiveWaitingRequestsExceededException"),
            ErrorClass::Throttled
        );
        assert_eq!(c("ResourceNotFoundException"), ErrorClass::Internal);
        assert_eq!(c("ExpiredTokenException"), ErrorClass::Auth);
        assert_eq!(c("DatabaseConnectionException"), ErrorClass::Transient);
        assert_eq!(classify_exception("", 503), ErrorClass::Transient);
        assert_eq!(
            api_error("X", 502, None, "<html>").class,
            ErrorClass::Transient
        );
    }

    #[test]
    fn failed_texts() {
        let c = |t| classify_failed(t);
        assert_eq!(
            c(
                "ERROR: Specified unload destination on S3 is not empty. Consider using a different bucket / prefix, manually removing the target files in S3, or using the ALLOWOVERWRITE option."
            ),
            (ErrorClass::Internal, Some(UNLOAD_NOT_EMPTY))
        );
        assert_eq!(
            c("ERROR: relation \"est_v2.nope\" does not exist").0,
            ErrorClass::Config
        );
        assert_eq!(
            c("ERROR: permission denied for relation t").0,
            ErrorClass::Config
        );
        assert_eq!(
            c("ERROR: syntax error at or near \"FORM\"").0,
            ErrorClass::Config
        );
        assert_eq!(
            c("ERROR: Insufficient capacity to run the query").0,
            ErrorClass::Transient
        );
        assert_eq!(
            c("ERROR: S3ServiceException:Please reduce your request rate. Slow Down").0,
            ErrorClass::Throttled
        );
        assert_eq!(c("ERROR: division by zero").0, ErrorClass::Data);
        assert_eq!(c("ERROR: something odd").0, ErrorClass::Internal);
    }
}
