//! Cloudflare D1 through the REST API (06 §7). Every request auto-commits; see 08 §6.3.

use std::time::{Duration, SystemTime};

use atlatl_core::AtlatlError;
use atlatl_core::error::{ErrorClass, Result, classify_http_status, wait_hint};
use atlatl_core::retry::{RandomJitter, RetryLevel, RetryPolicy, retry};
use serde_json::{Value as J, json};

use crate::conn::{Outcome, Stmt};
use crate::types::Value;

pub const DEFAULT_ENDPOINT: &str = "https://api.cloudflare.com/client/v4";
/// Bound parameters per statement.
pub const MAX_PARAMS: usize = 100;
/// SQL text per statement.
pub const MAX_STATEMENT_BYTES: usize = 100_000;
pub const MAX_ROW_BYTES: usize = 2_000_000;
/// JavaScript's safe integer range; larger integers are sent as text (INTEGER affinity
/// converts them back exactly).
const SAFE_INT: i64 = (1 << 53) - 1;

#[derive(Clone)]
pub struct D1 {
    http: reqwest::Client,
    base: String,
    token: String,
    pub database_id: String,
    pub statements_per_request: usize,
    pub in_flight: usize,
    policy: RetryPolicy,
}

#[derive(Clone, Debug)]
pub struct D1Options {
    pub endpoint: String,
    pub account_id: String,
    pub database_id: Option<String>,
    pub database: Option<String>,
    pub token: String,
    pub statements_per_request: usize,
    pub in_flight: usize,
    pub timeout: Duration,
    pub policy: RetryPolicy,
}

impl D1 {
    pub async fn open(o: D1Options) -> Result<Self> {
        let http = atlatl_core::tls::http_client()
            .connect_timeout(Duration::from_secs(10))
            .timeout(o.timeout)
            .build()
            .map_err(|e| AtlatlError::internal(format!("d1 client: {e}")))?;
        let base = format!(
            "{}/accounts/{}/d1/database",
            o.endpoint.trim_end_matches('/'),
            o.account_id
        );
        let mut d = Self {
            http,
            base,
            token: o.token,
            database_id: String::new(),
            statements_per_request: o.statements_per_request.max(1),
            in_flight: o.in_flight.max(1),
            policy: o.policy,
        };
        d.database_id = match (o.database_id, o.database) {
            (Some(id), None) => id,
            (None, Some(name)) => d.resolve(&name).await?,
            _ => {
                return Err(AtlatlError::config(
                    "a d1 connection needs exactly one of `database_id` and `database`",
                ));
            }
        };
        Ok(d)
    }

    /// Finds `name` in the paginated database list.
    async fn resolve(&self, name: &str) -> Result<String> {
        let mut page = 1u64;
        loop {
            let url = format!(
                "{}?name={}&page={page}&per_page=100",
                self.base,
                percent(name)
            );
            let body = self.send(self.http.get(&url)).await?;
            let result = body
                .get("result")
                .and_then(J::as_array)
                .ok_or_else(|| decode("database list without `result`"))?;
            if let Some(db) = result
                .iter()
                .find(|d| d.get("name").and_then(J::as_str) == Some(name))
            {
                return db
                    .get("uuid")
                    .and_then(J::as_str)
                    .map(str::to_owned)
                    .ok_or_else(|| decode("database entry without `uuid`"));
            }
            let info = body.get("result_info");
            let total_pages = info.and_then(|i| i.get("total_pages")).and_then(J::as_u64);
            let total = info.and_then(|i| i.get("total_count")).and_then(J::as_u64);
            let per = info
                .and_then(|i| i.get("per_page"))
                .and_then(J::as_u64)
                .unwrap_or(100);
            let more = match (total_pages, total) {
                (Some(t), _) => page < t,
                (None, Some(t)) => page * per < t,
                (None, None) => !result.is_empty(),
            };
            if !more || result.is_empty() {
                return Err(AtlatlError::config(format!(
                    "no D1 database named `{name}` in the account"
                )));
            }
            page += 1;
        }
    }

    /// Runs `stmts` in order, `statements_per_request` per request.
    pub async fn run(&self, stmts: Vec<Stmt>) -> Result<Vec<Outcome>> {
        let mut out = Vec::with_capacity(stmts.len());
        for req in stmts.chunks(self.statements_per_request) {
            out.extend(self.request(req).await?);
        }
        Ok(out)
    }

    async fn request(&self, stmts: &[Stmt]) -> Result<Vec<Outcome>> {
        let enc = |s: &Stmt| -> Result<J> {
            if s.sql.len() > MAX_STATEMENT_BYTES {
                return Err(AtlatlError::data(format!(
                    "d1: a statement of {} bytes exceeds the 100 KB limit",
                    s.sql.len()
                )));
            }
            if s.params.len() > MAX_PARAMS {
                return Err(AtlatlError::internal(format!(
                    "d1: {} bound parameters exceed the limit of {MAX_PARAMS}",
                    s.params.len()
                )));
            }
            Ok(
                json!({"sql": s.sql, "params": s.params.iter().map(param).collect::<Result<Vec<_>>>()?}),
            )
        };
        let body = match stmts {
            [one] => enc(one)?,
            many => json!({"batch": many.iter().map(enc).collect::<Result<Vec<_>>>()?}),
        };
        let url = format!("{}/{}/raw", self.base, self.database_id);
        let body = body.to_string();
        let r = self
            .send(
                self.http
                    .post(&url)
                    .header("content-type", "application/json")
                    .body(body),
            )
            .await?;
        let results = r
            .get("result")
            .and_then(J::as_array)
            .ok_or_else(|| decode("query response without `result`"))?;
        if results.len() != stmts.len() {
            return Err(decode(&format!(
                "{} results for {} statements",
                results.len(),
                stmts.len()
            )));
        }
        results.iter().map(outcome).collect()
    }

    /// Sends with retries (429 → throttled honouring `Retry-After`; 5xx and network → transient;
    /// non-JSON → decode) and returns the successful envelope.
    async fn send(&self, rb: reqwest::RequestBuilder) -> Result<J> {
        let mut jitter = RandomJitter::new();
        retry(
            &self.policy,
            RetryLevel::Request,
            &mut jitter,
            |a| tracing::warn!(attempt = a.attempt, class = %a.class, "d1: {}", a.message),
            |_| {
                let rb = rb.try_clone();
                async move {
                    let rb =
                        rb.ok_or_else(|| AtlatlError::internal("d1: request is not cloneable"))?;
                    let resp = rb.bearer_auth(&self.token).send().await.map_err(net)?;
                    let status = resp.status().as_u16();
                    let headers: Vec<(String, String)> = resp
                        .headers()
                        .iter()
                        .map(|(k, v)| {
                            (
                                k.as_str().to_owned(),
                                v.to_str().unwrap_or_default().to_owned(),
                            )
                        })
                        .collect();
                    let text = resp.text().await.map_err(net)?;
                    let body: Option<J> = serde_json::from_str(&text).ok();
                    if let Some(class) = classify_http_status(status) {
                        let msg = body
                            .as_ref()
                            .map(errors)
                            .unwrap_or_else(|| text.chars().take(200).collect());
                        let class = if class == ErrorClass::Config {
                            sql_class(&msg)
                        } else {
                            class
                        };
                        let hint = wait_hint(
                            headers.iter().map(|(k, v)| (k.as_str(), v.as_str())),
                            SystemTime::now(),
                        );
                        return Err(AtlatlError::new(class, format!("d1: HTTP {status}: {msg}"))
                            .code(format!("http_{status}"))
                            .retry_after(hint));
                    }
                    let body = body.ok_or_else(|| {
                        decode(&format!(
                            "non-JSON response: {}",
                            text.chars().take(200).collect::<String>()
                        ))
                    })?;
                    if body.get("success").and_then(J::as_bool) != Some(true) {
                        let msg = errors(&body);
                        return Err(AtlatlError::new(sql_class(&msg), format!("d1: {msg}")));
                    }
                    Ok(body)
                }
            },
        )
        .await
        .map_err(|e| e.error)
    }
}

fn percent(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

fn decode(m: &str) -> AtlatlError {
    AtlatlError::new(ErrorClass::Decode, format!("d1: {m}"))
}

fn net(e: reqwest::Error) -> AtlatlError {
    let class = if e.is_timeout() {
        ErrorClass::Timeout
    } else if e.is_decode() {
        ErrorClass::Decode
    } else {
        ErrorClass::Transient
    };
    AtlatlError::new(class, format!("d1: {e}"))
}

fn errors(body: &J) -> String {
    let e: Vec<String> = body
        .get("errors")
        .and_then(J::as_array)
        .map(|a| {
            a.iter()
                .map(|e| {
                    e.get("message")
                        .and_then(J::as_str)
                        .map_or_else(|| e.to_string(), str::to_owned)
                })
                .collect()
        })
        .unwrap_or_default();
    if e.is_empty() {
        body.to_string().chars().take(200).collect()
    } else {
        e.join("; ")
    }
}

/// SQL failures reported as 4xx: constraint and type failures are `data`, the rest `config`.
fn sql_class(msg: &str) -> ErrorClass {
    let m = msg.to_ascii_uppercase();
    if m.contains("CONSTRAINT")
        || m.contains("SQLITE_MISMATCH")
        || m.contains("DATATYPE MISMATCH")
        || m.contains("SQLITE_TOOBIG")
    {
        ErrorClass::Data
    } else if m.contains("SQLITE_BUSY") || m.contains("DATABASE IS LOCKED") {
        ErrorClass::Conflict
    } else {
        ErrorClass::Config
    }
}

fn param(v: &Value) -> Result<J> {
    Ok(match v {
        Value::Null => J::Null,
        Value::Integer(i) if (-SAFE_INT..=SAFE_INT).contains(i) => (*i).into(),
        Value::Integer(i) => i.to_string().into(),
        Value::Real(f) => serde_json::Number::from_f64(*f).map_or(J::Null, J::Number),
        Value::Text(s) => s.clone().into(),
        Value::Blob(_) => {
            return Err(AtlatlError::config(
                "d1: the REST API cannot bind BLOBs; store binary columns as text (a `transform` to hex or base64)",
            ));
        }
    })
}

fn outcome(r: &J) -> Result<Outcome> {
    if r.get("success").and_then(J::as_bool) == Some(false) {
        return Err(AtlatlError::new(
            ErrorClass::Config,
            format!("d1: statement failed: {r}"),
        ));
    }
    let changes = r.pointer("/meta/changes").and_then(J::as_u64).unwrap_or(0);
    let rows = r
        .pointer("/results/rows")
        .and_then(J::as_array)
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    row.as_array()
                        .map(|cells| cells.iter().map(cell).collect())
                        .ok_or_else(|| decode("a row is not an array"))
                })
                .collect::<Result<Vec<Vec<Value>>>>()
        })
        .transpose()?
        .unwrap_or_default();
    Ok(Outcome { changes, rows })
}

fn cell(v: &J) -> Value {
    match v {
        J::Null => Value::Null,
        J::Bool(b) => Value::Integer(i64::from(*b)),
        J::Number(n) => n.as_i64().map_or_else(
            || Value::Real(n.as_f64().unwrap_or(f64::NAN)),
            Value::Integer,
        ),
        J::String(s) => Value::Text(s.clone()),
        other => Value::Text(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params_and_cells() {
        assert_eq!(param(&Value::Integer(5)).expect("p"), json!(5));
        assert_eq!(
            param(&Value::Integer(i64::MAX)).expect("p"),
            json!(i64::MAX.to_string())
        );
        assert_eq!(param(&Value::Real(f64::INFINITY)).expect("p"), J::Null);
        assert!(param(&Value::Blob(vec![1])).is_err());
        assert_eq!(cell(&json!(3)), Value::Integer(3));
        assert_eq!(cell(&json!(1.5)), Value::Real(1.5));
        assert_eq!(cell(&json!("x")), Value::Text("x".into()));
        assert_eq!(percent("a b/é"), "a%20b%2F%C3%A9");
        assert_eq!(
            sql_class("UNIQUE constraint failed: t.id: SQLITE_CONSTRAINT"),
            ErrorClass::Data
        );
        assert_eq!(
            sql_class("no such table: t: SQLITE_ERROR"),
            ErrorClass::Config
        );
    }
}
