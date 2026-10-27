//! Error taxonomy (spec 03 §5). Connectors map native errors into these classes; anything
//! unmapped is `internal`.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ErrorClass {
    Transient,
    Throttled,
    Decode,
    Conflict,
    Timeout,
    Auth,
    Data,
    Config,
    Killed,
    Internal,
}

impl ErrorClass {
    /// Classes a retry policy may name in `retry_on`; also the default set.
    pub const RETRYABLE: [ErrorClass; 5] = [
        ErrorClass::Transient,
        ErrorClass::Throttled,
        ErrorClass::Decode,
        ErrorClass::Conflict,
        ErrorClass::Timeout,
    ];

    pub fn is_retryable(self) -> bool {
        Self::RETRYABLE.contains(&self)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ErrorClass::Transient => "transient",
            ErrorClass::Throttled => "throttled",
            ErrorClass::Decode => "decode",
            ErrorClass::Conflict => "conflict",
            ErrorClass::Timeout => "timeout",
            ErrorClass::Auth => "auth",
            ErrorClass::Data => "data",
            ErrorClass::Config => "config",
            ErrorClass::Killed => "killed",
            ErrorClass::Internal => "internal",
        }
    }
}

impl fmt::Display for ErrorClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A classified error. `Display` redacts registered secrets.
#[derive(Clone, Debug, Serialize)]
pub struct AtlatlError {
    pub class: ErrorClass,
    /// Stable machine code, e.g. `cursor_expired`, `schema_violation`, a SQLSTATE.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    pub message: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub samples: Vec<serde_json::Value>,
    /// Server-provided wait (Retry-After and friends); overrides computed backoff for `throttled`.
    #[serde(skip)]
    pub retry_after: Option<Duration>,
    #[serde(skip)]
    pub source: Option<Arc<dyn std::error::Error + Send + Sync>>,
}

impl AtlatlError {
    pub fn new(class: ErrorClass, message: impl Into<String>) -> Self {
        Self {
            class,
            code: None,
            message: message.into(),
            samples: Vec::new(),
            retry_after: None,
            source: None,
        }
    }

    pub fn code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }

    pub fn retry_after(mut self, d: Option<Duration>) -> Self {
        self.retry_after = d;
        self
    }

    pub fn source(mut self, e: impl std::error::Error + Send + Sync + 'static) -> Self {
        self.source = Some(Arc::new(e));
        self
    }

    pub fn samples(mut self, s: Vec<serde_json::Value>) -> Self {
        self.samples = s;
        self
    }

    pub fn transient(m: impl Into<String>) -> Self {
        Self::new(ErrorClass::Transient, m)
    }
    pub fn config(m: impl Into<String>) -> Self {
        Self::new(ErrorClass::Config, m)
    }
    pub fn data(m: impl Into<String>) -> Self {
        Self::new(ErrorClass::Data, m)
    }
    pub fn conflict(m: impl Into<String>) -> Self {
        Self::new(ErrorClass::Conflict, m)
    }
    pub fn internal(m: impl Into<String>) -> Self {
        Self::new(ErrorClass::Internal, m)
    }

    pub fn redacted_message(&self) -> String {
        crate::redact::redact(&self.message).into_owned()
    }
}

impl fmt::Display for AtlatlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.class, self.redacted_message())?;
        if let Some(c) = &self.code {
            write!(f, " [{c}]")?;
        }
        Ok(())
    }
}

impl std::error::Error for AtlatlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|e| e as &(dyn std::error::Error + 'static))
    }
}

pub type Result<T, E = AtlatlError> = std::result::Result<T, E>;

/// Maps an I/O error from a network read or write.
pub fn classify_io(e: &std::io::Error) -> ErrorClass {
    use std::io::ErrorKind as K;
    match e.kind() {
        K::TimedOut => ErrorClass::Timeout,
        K::ConnectionReset
        | K::ConnectionAborted
        | K::ConnectionRefused
        | K::NotConnected
        | K::BrokenPipe
        | K::UnexpectedEof
        | K::Interrupted
        | K::WouldBlock
        | K::HostUnreachable
        | K::NetworkUnreachable
        | K::NetworkDown
        | K::AddrNotAvailable => ErrorClass::Transient,
        K::InvalidData => ErrorClass::Decode,
        K::PermissionDenied => ErrorClass::Auth,
        _ => ErrorClass::Internal,
    }
}

impl From<std::io::Error> for AtlatlError {
    fn from(e: std::io::Error) -> Self {
        AtlatlError::new(classify_io(&e), e.to_string()).source(e)
    }
}

/// Default class for an HTTP status; `None` for success and redirects. Vendor-specific throttles
/// (a 409 used as a rate limit) are rules, not defaults.
pub fn classify_http_status(status: u16) -> Option<ErrorClass> {
    match status {
        100..=399 => None,
        429 => Some(ErrorClass::Throttled),
        408 | 500..=599 => Some(ErrorClass::Transient),
        401 | 403 => Some(ErrorClass::Auth),
        _ => Some(ErrorClass::Config),
    }
}

/// The server's requested wait from `Retry-After` (seconds or HTTP date), `RateLimit-Reset`
/// (seconds) or `X-RateLimit-Reset` (seconds, or an epoch in s/ms). First header wins, in that order.
pub fn wait_hint<'a>(
    headers: impl IntoIterator<Item = (&'a str, &'a str)>,
    now: SystemTime,
) -> Option<Duration> {
    let headers: Vec<(String, &str)> = headers
        .into_iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.trim()))
        .collect();
    let get = |n: &str| headers.iter().find(|(k, _)| k == n).map(|(_, v)| *v);
    if let Some(v) = get("retry-after") {
        if let Ok(s) = v.parse::<f64>() {
            return (s >= 0.0).then(|| Duration::from_secs_f64(s));
        }
        if let Ok(t) = httpdate::parse_http_date(v) {
            return Some(t.duration_since(now).unwrap_or(Duration::ZERO));
        }
    }
    if let Some(s) = get("ratelimit-reset").and_then(|v| v.parse::<f64>().ok()) {
        return (s >= 0.0).then(|| Duration::from_secs_f64(s));
    }
    if let Some(n) = get("x-ratelimit-reset").and_then(|v| v.parse::<f64>().ok()) {
        let epoch = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        let secs = if n > 1e12 {
            n / 1000.0 - epoch
        } else if n > 1e9 {
            n - epoch
        } else {
            n
        };
        return Some(Duration::from_secs_f64(secs.max(0.0)));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_mapping() {
        use std::io::{Error, ErrorKind};
        assert_eq!(
            classify_io(&Error::from(ErrorKind::ConnectionReset)),
            ErrorClass::Transient
        );
        assert_eq!(
            classify_io(&Error::from(ErrorKind::UnexpectedEof)),
            ErrorClass::Transient
        );
        assert_eq!(
            classify_io(&Error::from(ErrorKind::TimedOut)),
            ErrorClass::Timeout
        );
        assert_eq!(
            classify_io(&Error::from(ErrorKind::InvalidData)),
            ErrorClass::Decode
        );
        assert_eq!(
            classify_io(&Error::from(ErrorKind::NotFound)),
            ErrorClass::Internal
        );
    }

    #[test]
    fn http_mapping() {
        assert_eq!(classify_http_status(200), None);
        assert_eq!(classify_http_status(429), Some(ErrorClass::Throttled));
        assert_eq!(classify_http_status(503), Some(ErrorClass::Transient));
        assert_eq!(classify_http_status(408), Some(ErrorClass::Transient));
        assert_eq!(classify_http_status(401), Some(ErrorClass::Auth));
        assert_eq!(classify_http_status(404), Some(ErrorClass::Config));
    }

    #[test]
    fn wait_hints() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        assert_eq!(
            wait_hint([("Retry-After", "7")], now),
            Some(Duration::from_secs(7))
        );
        let date = httpdate::fmt_http_date(now + Duration::from_secs(30));
        assert_eq!(
            wait_hint([("retry-after", date.as_str())], now),
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            wait_hint([("RateLimit-Reset", "2.5")], now),
            Some(Duration::from_millis(2500))
        );
        assert_eq!(
            wait_hint([("X-RateLimit-Reset", "1700000012")], now),
            Some(Duration::from_secs(12))
        );
        assert_eq!(
            wait_hint([("X-RateLimit-Reset", "1700000003000")], now),
            Some(Duration::from_secs(3))
        );
        assert_eq!(
            wait_hint([("X-RateLimit-Reset", "4")], now),
            Some(Duration::from_secs(4))
        );
        assert_eq!(
            wait_hint([("X-RateLimit-Reset", "1699999990")], now),
            Some(Duration::ZERO)
        );
        assert_eq!(
            wait_hint([("Retry-After", "9"), ("X-RateLimit-Reset", "1")], now),
            Some(Duration::from_secs(9))
        );
        assert_eq!(wait_hint([("Content-Type", "x")], now), None);
    }

    #[test]
    fn display_redacts() {
        crate::redact::register("sekrit-in-error-test");
        let e =
            AtlatlError::transient("connect failed: password=sekrit-in-error-test").code("08006");
        assert_eq!(
            e.to_string(),
            "transient: connect failed: password=**** [08006]"
        );
    }
}
