//! Redshift Data API client (awsJson1.1 over SigV4): execute, poll, fetch, cancel (06 §4).

use std::sync::Arc;
use std::time::Duration;

use atlatl_core::error::{AtlatlError, ErrorClass, Result};
use atlatl_core::retry::{RandomJitter, RetryLevel, RetryPolicy, retry};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::aws::{self, Endpoint, Signed};
use crate::creds::Provider;
use crate::error::{ACTIVE_WAITING, api_error, failed};
use crate::result::{Executed, Field, ResultSet};

pub const SERVICE: &str = "redshift-data";
/// The Data API rejects SQL over 200 KB.
pub const MAX_SQL_BYTES: usize = 200 * 1000;
const MAX_NAME: usize = 500;
/// Account-wide `CancelStatement` quota is 3 TPS.
const CANCEL_SPACING: Duration = Duration::from_millis(334);
const CANCEL_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Target {
    Workgroup(String),
    Cluster(String),
}

/// `DescribeStatement` polling: long polls of `wait` (≤ 30 s; zero disables them), and capped
/// backoff from `initial` to `max` when long polling is off or refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Poll {
    pub wait: Duration,
    pub initial: Duration,
    pub max: Duration,
}

impl Default for Poll {
    fn default() -> Self {
        Self {
            wait: Duration::from_secs(20),
            initial: Duration::from_millis(500),
            max: Duration::from_secs(10),
        }
    }
}

#[derive(Clone, Debug)]
pub struct DataApiOptions {
    /// Defaults to `https://redshift-data.<region>.amazonaws.com`.
    pub endpoint: Option<String>,
    pub region: String,
    pub target: Target,
    pub database: String,
    pub secret_arn: Option<String>,
    pub poll: Poll,
    /// Request-level retry of each API call.
    pub retry: RetryPolicy,
    /// Cancel and fail with `timeout` past this (the Data API's own limit is 24 h).
    pub statement_timeout: Duration,
}

pub fn default_request_retry() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 6,
        base: Duration::from_millis(250),
        max: Duration::from_secs(20),
        ..RetryPolicy::default()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Description {
    pub id: String,
    pub status: String,
    pub error: Option<String>,
    pub duration: Option<i64>,
    pub result_rows: Option<i64>,
    #[serde(default)]
    pub has_result_set: bool,
    pub redshift_query_id: Option<i64>,
}

impl Description {
    pub fn is_terminal(&self) -> bool {
        matches!(self.status.as_str(), "FINISHED" | "FAILED" | "ABORTED")
    }

    fn executed(&self) -> Executed {
        Executed {
            id: self.id.clone(),
            rows: self.result_rows.filter(|n| *n >= 0),
            has_result_set: self.has_result_set,
            redshift_query_id: self.redshift_query_id,
            duration_ns: self.duration,
        }
    }
}

#[derive(Clone)]
pub struct DataApi {
    http: reqwest::Client,
    endpoint: Endpoint,
    creds: Arc<Provider>,
    o: DataApiOptions,
}

impl std::fmt::Debug for DataApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataApi")
            .field("endpoint", &self.endpoint)
            .field("target", &self.o.target)
            .field("database", &self.o.database)
            .finish()
    }
}

/// Cancels the statement from a spawned task if the waiting future is dropped.
struct CancelOnDrop {
    api: Option<DataApi>,
    id: String,
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        let Some(api) = self.api.take() else { return };
        let Ok(h) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let id = std::mem::take(&mut self.id);
        h.spawn(async move {
            let r = tokio::time::timeout(CANCEL_TIMEOUT, api.cancel(&id)).await;
            tracing::info!(id, ?r, "cancelled abandoned Redshift statement");
        });
    }
}

struct CancelAfterSubmit {
    task: Option<tokio::task::JoinHandle<Result<String>>>,
    api: DataApi,
}

impl Drop for CancelAfterSubmit {
    fn drop(&mut self) {
        let Some(task) = self.task.take() else { return };
        let Ok(h) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let api = self.api.clone();
        h.spawn(async move {
            if let Ok(Ok(id)) = task.await {
                let r = tokio::time::timeout(CANCEL_TIMEOUT, api.cancel(&id)).await;
                tracing::info!(
                    id,
                    ?r,
                    "cancelled Redshift statement submitted by an abandoned run"
                );
            }
        });
    }
}

static CANCEL_NEXT: std::sync::Mutex<Option<tokio::time::Instant>> = std::sync::Mutex::new(None);

/// Reserves the next process-wide `CancelStatement` slot.
fn cancel_slot(now: tokio::time::Instant) -> tokio::time::Instant {
    let mut next = CANCEL_NEXT.lock().unwrap_or_else(|e| e.into_inner());
    let slot = next.map_or(now, |n| n.max(now));
    *next = Some(slot + CANCEL_SPACING);
    slot
}

fn client_token() -> String {
    use rand::RngExt;
    let n: u128 = rand::rng().random();
    format!("atlatl-{n:032x}")
}

impl DataApi {
    pub fn new(o: DataApiOptions, creds: Arc<Provider>, http: reqwest::Client) -> Result<Self> {
        let ep = o
            .endpoint
            .clone()
            .unwrap_or_else(|| format!("https://{SERVICE}.{}.amazonaws.com", o.region));
        Ok(Self {
            http,
            endpoint: Endpoint::parse(&ep)?,
            creds,
            o,
        })
    }

    pub fn options(&self) -> &DataApiOptions {
        &self.o
    }

    async fn call_once(&self, op: &str, body: &Value) -> Result<Value> {
        let creds = self.creds.get().await?;
        let headers = [
            (
                "content-type".to_owned(),
                "application/x-amz-json-1.1".to_owned(),
            ),
            ("x-amz-target".to_owned(), format!("RedshiftData.{op}")),
        ];
        let r = aws::send(
            &self.http,
            &creds,
            Signed {
                service: SERVICE,
                region: &self.o.region,
                method: "POST",
                endpoint: &self.endpoint,
                path: "/",
                query: &[],
                headers: &headers,
                body: serde_json::to_vec(body).map_err(|e| AtlatlError::internal(e.to_string()))?,
                content_sha: false,
            },
        )
        .await?;
        if r.status != 200 {
            return Err(api_error(
                op,
                r.status,
                r.header("x-amzn-errortype"),
                &r.text(),
            ));
        }
        serde_json::from_slice(&r.body).map_err(|e| {
            AtlatlError::new(
                ErrorClass::Decode,
                format!("Redshift Data API {op}: bad JSON: {e}"),
            )
        })
    }

    async fn call_with(&self, policy: &RetryPolicy, op: &str, body: Value) -> Result<Value> {
        retry(
            policy,
            RetryLevel::Request,
            &mut RandomJitter::new(),
            |a| tracing::warn!(attempt = a.attempt, error = %a.message, "retrying Redshift {op}"),
            |_| self.call_once(op, &body),
        )
        .await
        .map_err(|e| e.error)
    }

    async fn call(&self, op: &str, body: Value) -> Result<Value> {
        self.call_with(&self.o.retry, op, body).await
    }

    /// `ExecuteStatement`; the `ClientToken` is reused across retries, so a retried submit never
    /// runs the statement twice.
    pub async fn submit(&self, sql: &str, name: &str) -> Result<String> {
        if sql.len() > MAX_SQL_BYTES {
            return Err(AtlatlError::config(format!(
                "statement {name} is {} bytes; the Data API allows {MAX_SQL_BYTES}",
                sql.len()
            )));
        }
        let mut body = json!({
            "Database": self.o.database,
            "Sql": sql,
            "StatementName": name.chars().take(MAX_NAME).collect::<String>(),
            "WithEvent": false,
            "ClientToken": client_token(),
        });
        match &self.o.target {
            Target::Workgroup(w) => body["WorkgroupName"] = json!(w),
            Target::Cluster(c) => body["ClusterIdentifier"] = json!(c),
        }
        if let Some(s) = &self.o.secret_arn {
            body["SecretArn"] = json!(s);
        }
        let v = self.call("ExecuteStatement", body).await?;
        v.get("Id")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| AtlatlError::new(ErrorClass::Decode, "ExecuteStatement returned no Id"))
    }

    /// [`Self::submit`] on its own task: if this future is dropped mid-request (a kill), the
    /// statement still gets its id and a `CancelStatement` once the request returns.
    async fn submit_detached(&self, sql: &str, name: &str) -> Result<String> {
        let (api, sql, name) = (self.clone(), sql.to_owned(), name.to_owned());
        let mut g = CancelAfterSubmit {
            task: Some(tokio::spawn(async move { api.submit(&sql, &name).await })),
            api: self.clone(),
        };
        let r = match g.task.as_mut() {
            Some(t) => t.await,
            None => return Err(AtlatlError::internal("submit task missing")),
        };
        g.task = None;
        r.map_err(|e| AtlatlError::internal(format!("ExecuteStatement task: {e}")))?
    }

    /// `DescribeStatement`, optionally long-polling. `None` when the long poll is refused
    /// (`ActiveWaitingRequestsExceededException`).
    pub async fn describe(&self, id: &str, wait: Option<u32>) -> Result<Option<Description>> {
        let mut body = json!({"Id": id});
        if let Some(w) = wait {
            body["WaitTimeSeconds"] = json!(w.clamp(1, 30));
        }
        let r = retry(
            &self.o.retry,
            RetryLevel::Request,
            &mut RandomJitter::new(),
            |a| tracing::warn!(attempt = a.attempt, error = %a.message, "retrying Redshift DescribeStatement"),
            |_| async {
                match self.call_once("DescribeStatement", &body).await {
                    Err(e) if wait.is_some() && e.code.as_deref() == Some(ACTIVE_WAITING) => Ok(None),
                    r => r.map(Some),
                }
            },
        )
        .await
        .map_err(|e| e.error)?;
        r.map(|v| {
            serde_json::from_value(v).map_err(|e| {
                AtlatlError::new(ErrorClass::Decode, format!("DescribeStatement: {e}"))
            })
        })
        .transpose()
    }

    /// Polls until `id` is terminal. `FAILED` and `ABORTED` are errors; a fired `cancel` or the
    /// statement timeout sends a best-effort `CancelStatement`, and so does dropping this future
    /// mid-wait (a runner that abandons the member).
    pub async fn wait(&self, id: &str, cancel: &CancellationToken) -> Result<Description> {
        let mut guard = CancelOnDrop {
            api: Some(self.clone()),
            id: id.to_owned(),
        };
        let r = self.wait_inner(id, cancel).await;
        guard.api = None;
        r
    }

    async fn wait_inner(&self, id: &str, cancel: &CancellationToken) -> Result<Description> {
        let deadline = tokio::time::Instant::now() + self.o.statement_timeout;
        let mut long = self.o.poll.wait >= Duration::from_secs(1);
        let mut delay = self.o.poll.initial;
        loop {
            let wait = long.then(|| self.o.poll.wait.as_secs().min(30) as u32);
            let step = async {
                let d = self.describe(id, wait).await?;
                if d.is_none() {
                    tracing::debug!(id, "long poll refused; falling back to polling");
                }
                Ok::<_, AtlatlError>(d)
            };
            let d = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(self.abandon(id, ErrorClass::Killed, "cancelled").await),
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(self.abandon(id, ErrorClass::Timeout, "statement timeout").await)
                }
                d = step => match d {
                    Ok(d) => d,
                    // We lost track of it (describe errors outlasted retries): don't leave it running.
                    Err(e) => {
                        let why = e.message.clone();
                        let a = self.abandon(id, e.class, &format!("status unknown: {why}")).await;
                        return Err(AtlatlError { code: e.code.clone(), ..a });
                    }
                },
            };
            match d {
                None => long = false,
                Some(d) if d.is_terminal() => {
                    return match d.status.as_str() {
                        "FINISHED" => Ok(d),
                        "FAILED" => {
                            Err(failed(id, d.error.as_deref().unwrap_or("(no error text)")))
                        }
                        _ => Err(AtlatlError::transient(format!(
                            "Redshift statement {id} was aborted{}",
                            d.error.map(|e| format!(": {e}")).unwrap_or_default()
                        ))
                        .code("statement_aborted")),
                    };
                }
                Some(_) if long => {}
                Some(_) => {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return Err(self.abandon(id, ErrorClass::Killed, "cancelled").await),
                        _ = tokio::time::sleep_until(deadline) => {
                            return Err(self.abandon(id, ErrorClass::Timeout, "statement timeout").await)
                        }
                        _ = tokio::time::sleep(delay) => {}
                    }
                    delay = (delay * 2).min(self.o.poll.max);
                }
            }
        }
    }

    async fn abandon(&self, id: &str, class: ErrorClass, why: &str) -> AtlatlError {
        let cancelled = match tokio::time::timeout(CANCEL_TIMEOUT, self.cancel(id)).await {
            Ok(Ok(b)) => b.to_string(),
            Ok(Err(e)) => format!("failed: {e}"),
            Err(_) => "timed out".into(),
        };
        AtlatlError::new(
            class,
            format!("Redshift statement {id}: {why} (CancelStatement: {cancelled})"),
        )
    }

    /// `CancelStatement` through the process-wide 3 TPS limiter, retried at most once.
    pub async fn cancel(&self, id: &str) -> Result<bool> {
        tokio::time::sleep_until(cancel_slot(tokio::time::Instant::now())).await;
        let once = RetryPolicy {
            max_attempts: 2,
            ..self.o.retry.clone()
        };
        let v = self
            .call_with(&once, "CancelStatement", json!({"Id": id}))
            .await?;
        Ok(v.get("Status").and_then(Value::as_bool).unwrap_or(false))
    }

    /// Every page of `GetStatementResult`.
    pub async fn results(&self, id: &str) -> Result<ResultSet> {
        #[derive(Deserialize)]
        #[serde(rename_all = "PascalCase")]
        struct Page {
            #[serde(default)]
            column_metadata: Vec<Col>,
            #[serde(default)]
            records: Vec<Vec<Field>>,
            next_token: Option<String>,
        }
        #[derive(Deserialize)]
        struct Col {
            name: String,
        }
        let mut out = ResultSet::default();
        let mut token: Option<String> = None;
        loop {
            let mut body = json!({"Id": id});
            if let Some(t) = &token {
                body["NextToken"] = json!(t);
            }
            let v = self.call("GetStatementResult", body).await?;
            let p: Page = serde_json::from_value(v).map_err(|e| {
                AtlatlError::new(ErrorClass::Decode, format!("GetStatementResult: {e}"))
            })?;
            if out.columns.is_empty() {
                out.columns = p.column_metadata.into_iter().map(|c| c.name).collect();
            }
            out.rows.extend(p.records);
            match p.next_token.filter(|t| !t.is_empty()) {
                Some(t) => token = Some(t),
                None => return Ok(out),
            }
        }
    }

    pub async fn execute(
        &self,
        sql: &str,
        name: &str,
        cancel: &CancellationToken,
    ) -> Result<Executed> {
        let id = self.submit_detached(sql, name).await?;
        tracing::debug!(id, name, "submitted Redshift statement");
        Ok(self.wait(&id, cancel).await?.executed())
    }

    pub async fn query(
        &self,
        sql: &str,
        name: &str,
        cancel: &CancellationToken,
    ) -> Result<ResultSet> {
        let id = self.submit_detached(sql, name).await?;
        let d = self.wait(&id, cancel).await?;
        if !d.has_result_set {
            return Ok(ResultSet::default());
        }
        self.results(&id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn cancel_slots_are_spaced() {
        let now = tokio::time::Instant::now() + Duration::from_secs(1000);
        let a = cancel_slot(now);
        let b = cancel_slot(now);
        let c = cancel_slot(now);
        assert!(b - a >= CANCEL_SPACING && c - b >= CANCEL_SPACING);
    }

    #[test]
    fn describe_parsing() {
        let d: Description = serde_json::from_value(json!({
            "Id": "x", "Status": "FINISHED", "ResultRows": -1, "HasResultSet": false,
            "Duration": 12, "RedshiftQueryId": 7, "UpdatedAt": 1.7e9
        }))
        .expect("d");
        assert!(d.is_terminal());
        assert_eq!(d.executed().rows, None);
        assert_eq!(d.executed().redshift_query_id, Some(7));
    }
}
