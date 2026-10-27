//! Observability (spec 10): logs, the NDJSON event stream and the run report.

pub mod events;
pub mod log;
pub mod report;
pub mod status;

#[cfg(test)]
mod schema_check;
#[cfg(test)]
mod tests;

use serde::Serialize;
use serde_json::Value;

pub use events::{EventBody, EventSink, EventTarget};
pub use log::LogFormat;
pub use report::{ReplicationStatus, ReportHandle, RunReport, RunStatus, StreamStatus};

pub const ATLATL_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Masks secrets in every string (and object key) of `v`, in place.
pub fn redact_value(v: &mut Value) {
    match v {
        Value::String(s) => {
            if let std::borrow::Cow::Owned(r) = crate::redact::redact(s) {
                *s = r;
            }
        }
        Value::Array(a) => a.iter_mut().for_each(redact_value),
        Value::Object(m) => {
            let dirty = m
                .keys()
                .any(|k| matches!(crate::redact::redact(k), std::borrow::Cow::Owned(_)));
            if dirty {
                *m = std::mem::take(m)
                    .into_iter()
                    .map(|(k, v)| (crate::redact::redact(&k).into_owned(), v))
                    .collect();
            }
            m.values_mut().for_each(redact_value);
        }
        _ => {}
    }
}

/// `t` as JSON with every string redacted.
pub fn redacted_json<T: Serialize + ?Sized>(t: &T) -> serde_json::Result<Value> {
    let mut v = serde_json::to_value(t)?;
    redact_value(&mut v);
    Ok(v)
}
