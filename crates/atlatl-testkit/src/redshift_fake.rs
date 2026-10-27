//! A local fake of the Redshift Data API (awsJson1.1, SigV4-verified) and STS, backed by DuckDB,
//! whose UNLOAD writes Redshift-style Parquet parts and a VERBOSE manifest into RustFS. Plus a
//! fault-injecting TCP proxy in front of RustFS.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use duckdb::types::ValueRef;
use hmac::{Hmac, KeyInit, Mac};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

pub const KEY: &str = "AKIDFAKE";
pub const SECRET: &str = "fake-secret";
pub const REGION: &str = "us-east-1";

#[derive(Clone, Debug)]
pub struct Call {
    pub op: String,
    pub body: Value,
}

struct Stmt {
    /// Statuses still to report before `done`.
    pending: VecDeque<&'static str>,
    done: &'static str,
    error: Option<String>,
    rows: i64,
    result: Option<(Vec<String>, Vec<Vec<Value>>)>,
    hang: bool,
}

pub struct State {
    pub db: duckdb::Connection,
    pub calls: Vec<Call>,
    /// (op, exception, http status) to return, in order, before handling the op.
    pub inject: VecDeque<(String, String, u16)>,
    /// Non-terminal describes (`SUBMITTED`, `PICKED`, `STARTED`, …) before the terminal one.
    pub describes: usize,
    /// Refuse long polls with `ActiveWaitingRequestsExceededException`.
    pub refuse_long_poll: bool,
    /// Statements stay `STARTED` until cancelled.
    pub hang: bool,
    /// Only UNLOADs hang (after writing their files, like a cancelled UNLOAD's partial output).
    pub hang_unload: bool,
    /// `FAILED` texts for the next statements.
    pub fail: VecDeque<String>,
    pub page_size: usize,
    pub slices: usize,
    /// Zero-row UNLOAD writes a manifest with `entries: []` (else no manifest at all).
    pub empty_manifest: bool,
    /// Add this to every manifest `record_count`.
    pub lie_record_count: u64,
    /// STS `AssumeRole` answers with these keys (RustFS's).
    pub sts_keys: Option<(String, String)>,
    stmts: HashMap<String, Stmt>,
    tokens: HashMap<String, String>,
    next: u64,
}

#[derive(Clone)]
pub struct Fake {
    pub endpoint: String,
    state: Arc<Mutex<State>>,
}

impl Fake {
    pub async fn start() -> Self {
        let s3 = crate::rustfs();
        let db = crate::lake::open_engine().expect("redshift fake");
        db.execute_batch("LOAD httpfs").expect("redshift fake");
        db.execute_batch(&s3.duckdb_secret("fake_w", ""))
            .expect("redshift fake");
        db.execute_batch(SVV).expect("redshift fake");
        let state = Arc::new(Mutex::new(State {
            db,
            calls: Vec::new(),
            inject: VecDeque::new(),
            describes: 0,
            refuse_long_poll: false,
            hang: false,
            hang_unload: false,
            fail: VecDeque::new(),
            page_size: 2,
            slices: 2,
            empty_manifest: false,
            lie_record_count: 0,
            sts_keys: None,
            stmts: HashMap::new(),
            tokens: HashMap::new(),
            next: 0,
        }));
        let l = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("redshift fake");
        let endpoint = format!("http://{}", l.local_addr().expect("redshift fake"));
        let st = state.clone();
        tokio::spawn(async move {
            loop {
                let Ok((sock, _)) = l.accept().await else {
                    return;
                };
                let st = st.clone();
                tokio::spawn(async move {
                    let _ = serve(sock, st).await;
                });
            }
        });
        Self { endpoint, state }
    }

    pub fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn sql(&self, sql: &str) {
        self.state().db.execute_batch(sql).expect("redshift fake");
    }

    pub fn calls(&self, op: &str) -> Vec<Call> {
        self.state()
            .calls
            .iter()
            .filter(|c| c.op == op)
            .cloned()
            .collect()
    }

    pub fn last_id(&self) -> String {
        format!("stmt-{:04}", self.state().next)
    }

    pub fn statuses(&self, id: &str) -> Option<&'static str> {
        self.state().stmts.get(id).map(|s| s.done)
    }
}

// ----------------------------------------------------------------------------- SigV4 (independent of the client's)

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn sha256_hex(b: &[u8]) -> String {
    hex(&Sha256::digest(b))
}

fn hmac(key: &[u8], msg: &str) -> Vec<u8> {
    let mut m = <Hmac<Sha256>>::new_from_slice(key).expect("hmac key");
    m.update(msg.as_bytes());
    m.finalize().into_bytes().to_vec()
}

struct Authorization {
    key_id: String,
    region: String,
    service: String,
    signed_headers: Vec<String>,
    signature: String,
}

impl Authorization {
    fn parse(h: &str) -> Option<Self> {
        let rest = h.strip_prefix("AWS4-HMAC-SHA256 ")?;
        let (mut cred, mut signed, mut sig) = (None, None, None);
        for part in rest.split(',') {
            let (k, v) = part.trim().split_once('=')?;
            match k {
                "Credential" => cred = Some(v),
                "SignedHeaders" => signed = Some(v),
                "Signature" => sig = Some(v),
                _ => {}
            }
        }
        let c: Vec<&str> = cred?.split('/').collect();
        let [key_id, _date, region, service, "aws4_request"] = c.as_slice() else {
            return None;
        };
        Some(Self {
            key_id: (*key_id).to_owned(),
            region: (*region).to_owned(),
            service: (*service).to_owned(),
            signed_headers: signed?.split(';').map(str::to_owned).collect(),
            signature: sig?.to_owned(),
        })
    }
}

/// Signature of a `POST /` request with no query string.
fn signature(
    secret: &str,
    service: &str,
    amz_date: &str,
    method: &str,
    path: &str,
    headers: &[(String, String)],
    payload_hash: &str,
) -> String {
    let mut h: Vec<(String, String)> = headers
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                v.split_whitespace().collect::<Vec<_>>().join(" "),
            )
        })
        .collect();
    h.sort();
    let canon_headers: String = h.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
    let names = h
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical = format!("{method}\n{path}\n\n{canon_headers}\n{names}\n{payload_hash}");
    let date = &amz_date[..8.min(amz_date.len())];
    let scope = format!("{date}/{REGION}/{service}/aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical.as_bytes())
    );
    let k = hmac(
        &hmac(
            &hmac(&hmac(format!("AWS4{secret}").as_bytes(), date), REGION),
            service,
        ),
        "aws4_request",
    );
    hex(&hmac(&k, &to_sign))
}

/// Redshift-style catalog views over DuckDB's information_schema.
const SVV: &str = r#"
CREATE VIEW svv_columns AS SELECT table_schema, table_name, column_name, ordinal_position,
  CASE WHEN data_type LIKE 'DECIMAL%' THEN 'numeric'
       WHEN data_type = 'INTEGER' THEN 'integer' WHEN data_type = 'BIGINT' THEN 'bigint'
       WHEN data_type = 'SMALLINT' THEN 'smallint' WHEN data_type = 'VARCHAR' THEN 'character varying'
       WHEN data_type = 'BOOLEAN' THEN 'boolean' WHEN data_type = 'DATE' THEN 'date'
       WHEN data_type = 'TIMESTAMP' THEN 'timestamp without time zone'
       WHEN data_type = 'TIMESTAMP WITH TIME ZONE' THEN 'timestamp with time zone'
       WHEN data_type = 'DOUBLE' THEN 'double precision' WHEN data_type = 'FLOAT' THEN 'real'
       ELSE lower(data_type) END AS data_type,
  character_maximum_length,
  CASE WHEN data_type LIKE 'DECIMAL%' THEN numeric_precision END AS numeric_precision,
  CASE WHEN data_type LIKE 'DECIMAL%' THEN numeric_scale END AS numeric_scale
FROM information_schema.columns WHERE table_schema NOT IN ('main', 'information_schema', 'pg_catalog');
CREATE VIEW svv_tables AS SELECT table_catalog, table_schema, table_name, table_type
FROM information_schema.tables WHERE table_schema NOT IN ('main', 'information_schema', 'pg_catalog');
"#;

async fn serve(sock: TcpStream, st: Arc<Mutex<State>>) -> std::io::Result<()> {
    let (r, mut w) = sock.into_split();
    let mut r = BufReader::new(r);
    loop {
        let mut line = String::new();
        if r.read_line(&mut line).await? == 0 {
            return Ok(());
        }
        let mut parts = line.split_whitespace();
        let method = parts.next().unwrap_or("").to_owned();
        let path = parts.next().unwrap_or("/").to_owned();
        let mut headers: Vec<(String, String)> = Vec::new();
        loop {
            let mut h = String::new();
            r.read_line(&mut h).await?;
            let h = h.trim_end();
            if h.is_empty() {
                break;
            }
            if let Some((k, v)) = h.split_once(':') {
                headers.push((k.trim().to_ascii_lowercase(), v.trim().to_owned()));
            }
        }
        let len: usize = header(&headers, "content-length")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let mut body = vec![0u8; len];
        r.read_exact(&mut body).await?;
        let st2 = st.clone();
        let (status, ctype, resp) =
            tokio::task::spawn_blocking(move || handle(&st2, &method, &path, &headers, &body))
                .await
                .expect("redshift fake");
        if status == 0 {
            // Long poll on a hanging statement.
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let status = if status == 0 { 200 } else { status };
        let out = format!(
            "HTTP/1.1 {status} X\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\n\r\n",
            resp.len()
        );
        w.write_all(out.as_bytes()).await?;
        w.write_all(resp.as_bytes()).await?;
    }
}

fn header<'a>(h: &'a [(String, String)], k: &str) -> Option<&'a str> {
    h.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str())
}

fn err(kind: &str, status: u16, msg: &str) -> (u16, &'static str, String) {
    (
        status,
        "application/x-amz-json-1.1",
        json!({"__type": format!("com.amazonaws.redshiftdata#{kind}"), "message": msg}).to_string(),
    )
}

/// Checks the SigV4 header against the fake's secret; `Err` is the error response.
fn verify(
    method: &str,
    path: &str,
    headers: &[(String, String)],
    body: &[u8],
    service: &str,
) -> Result<(), (u16, &'static str, String)> {
    let auth = header(headers, "authorization")
        .and_then(Authorization::parse)
        .ok_or_else(|| {
            err(
                "MissingAuthenticationTokenException",
                403,
                "no SigV4 Authorization",
            )
        })?;
    if auth.key_id != KEY {
        return Err(err(
            "UnrecognizedClientException",
            400,
            "The security token included in the request is invalid.",
        ));
    }
    if auth.service != service || auth.region != REGION {
        return Err(err(
            "InvalidSignatureException",
            403,
            "wrong credential scope",
        ));
    }
    for need in ["host", "x-amz-date"] {
        if !auth.signed_headers.iter().any(|h| h == need) {
            return Err(err(
                "InvalidSignatureException",
                403,
                &format!("{need} is not signed"),
            ));
        }
    }
    let date = header(headers, "x-amz-date").unwrap_or_default().to_owned();
    let signed: Vec<(String, String)> = auth
        .signed_headers
        .iter()
        .map(|h| (h.clone(), header(headers, h).unwrap_or_default().to_owned()))
        .collect();
    let payload = sha256_hex(body);
    let sig = signature(SECRET, service, &date, method, path, &signed, &payload);
    if sig != auth.signature {
        return Err(err(
            "InvalidSignatureException",
            403,
            "The request signature we calculated does not match",
        ));
    }
    Ok(())
}

fn handle(
    st: &Mutex<State>,
    method: &str,
    path: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> (u16, &'static str, String) {
    let mut s = st.lock().unwrap_or_else(|e| e.into_inner());
    if header(headers, "content-type")
        .is_some_and(|c| c.starts_with("application/x-www-form-urlencoded"))
    {
        return sts(&mut s, method, path, headers, body);
    }
    if let Err(e) = verify(method, path, headers, body, "redshift-data") {
        return e;
    }
    let Some(op) = header(headers, "x-amz-target").and_then(|t| t.strip_prefix("RedshiftData."))
    else {
        return err("UnknownOperationException", 400, "no X-Amz-Target");
    };
    let op = op.to_owned();
    let v: Value = serde_json::from_slice(body).unwrap_or_default();
    s.calls.push(Call {
        op: op.clone(),
        body: v.clone(),
    });
    if let Some(i) = s.inject.iter().position(|(o, _, _)| *o == op) {
        let (_, kind, status) = s.inject.remove(i).expect("redshift fake");
        return err(&kind, status, "injected");
    }
    let ok = |v: Value| (200u16, "application/x-amz-json-1.1", v.to_string());
    match op.as_str() {
        "ExecuteStatement" => {
            let token = v["ClientToken"].as_str().unwrap_or_default().to_owned();
            if let Some(id) = s.tokens.get(&token) {
                return ok(json!({"Id": id}));
            }
            if v["WorkgroupName"].is_null() == v["ClusterIdentifier"].is_null() {
                return err(
                    "ValidationException",
                    400,
                    "need WorkgroupName or ClusterIdentifier",
                );
            }
            s.next += 1;
            let id = format!("stmt-{:04}", s.next);
            let sql = v["Sql"].as_str().unwrap_or_default().to_owned();
            let stmt = execute(&mut s, &sql);
            s.tokens.insert(token, id.clone());
            s.stmts.insert(id.clone(), stmt);
            ok(json!({"Id": id, "Database": v["Database"]}))
        }
        "DescribeStatement" => {
            let id = v["Id"].as_str().unwrap_or_default().to_owned();
            let long = !v["WaitTimeSeconds"].is_null();
            if long && s.refuse_long_poll {
                return err(
                    "ActiveWaitingRequestsExceededException",
                    400,
                    "too many waiting requests",
                );
            }
            let Some(stmt) = s.stmts.get_mut(&id) else {
                return err("ResourceNotFoundException", 400, "Query does not exist.");
            };
            let status = if stmt.hang {
                "STARTED"
            } else {
                stmt.pending.pop_front().unwrap_or(stmt.done)
            };
            let mut d = json!({
                "Id": id, "Status": status, "HasResultSet": stmt.result.is_some(),
                "ResultRows": if status == "FINISHED" { stmt.rows } else { -1 },
                "Duration": 1000, "RedshiftQueryId": 42,
            });
            if status == "FAILED" {
                d["Error"] = json!(stmt.error);
            }
            let hang = stmt.hang && long;
            let r = ok(d);
            if hang { (0, r.1, r.2) } else { r }
        }
        "GetStatementResult" => {
            let id = v["Id"].as_str().unwrap_or_default();
            let page = s.page_size;
            let Some((cols, rows)) = s.stmts.get(id).and_then(|x| x.result.clone()) else {
                return err("ResourceNotFoundException", 400, "no result");
            };
            let start: usize = v["NextToken"]
                .as_str()
                .and_then(|t| t.parse().ok())
                .unwrap_or(0);
            let end = (start + page).min(rows.len());
            let mut r = json!({
                "ColumnMetadata": cols.iter().map(|c| json!({"name": c, "label": c})).collect::<Vec<_>>(),
                "Records": rows[start..end].to_vec(),
                "TotalNumRows": rows.len(),
            });
            if end < rows.len() {
                r["NextToken"] = json!(end.to_string());
            }
            ok(r)
        }
        "CancelStatement" => {
            let id = v["Id"].as_str().unwrap_or_default();
            match s.stmts.get_mut(id) {
                Some(x) if x.hang || !x.pending.is_empty() => {
                    x.hang = false;
                    x.pending.clear();
                    x.done = "ABORTED";
                    ok(json!({"Status": true}))
                }
                Some(_) => ok(json!({"Status": false})),
                None => err("ResourceNotFoundException", 400, "Query does not exist."),
            }
        }
        _ => err("UnknownOperationException", 400, &op),
    }
}

fn sts(
    s: &mut State,
    method: &str,
    path: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> (u16, &'static str, String) {
    if let Err(e) = verify(method, path, headers, body, "sts") {
        return e;
    }
    let form: HashMap<String, String> = String::from_utf8_lossy(body)
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| (k.to_owned(), percent_decode(v)))
        .collect();
    s.calls.push(Call {
        op: format!("sts:{}", form.get("Action").cloned().unwrap_or_default()),
        body: json!(form),
    });
    let Some((k, sec)) = s.sts_keys.clone() else {
        return (403, "text/xml", "<ErrorResponse><Error><Code>AccessDenied</Code><Message>no</Message></Error></ErrorResponse>".into());
    };
    (
        200,
        "text/xml",
        format!(
            "<AssumeRoleResponse><AssumeRoleResult><Credentials><AccessKeyId>{k}</AccessKeyId>\
             <SecretAccessKey>{sec}</SecretAccessKey><SessionToken></SessionToken>\
             <Expiration>2099-01-01T00:00:00Z</Expiration></Credentials></AssumeRoleResult></AssumeRoleResponse>"
        ),
    )
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap_or(b'?'));
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn new_stmt(s: &State) -> Stmt {
    let mut pending: VecDeque<&'static str> = VecDeque::new();
    let seq = ["SUBMITTED", "PICKED", "STARTED"];
    for i in 0..s.describes {
        pending.push_back(seq[i.min(2)]);
    }
    Stmt {
        pending,
        done: "FINISHED",
        error: None,
        rows: 0,
        result: None,
        hang: s.hang,
    }
}

fn execute(s: &mut State, sql: &str) -> Stmt {
    let mut stmt = new_stmt(s);
    if let Some(text) = s.fail.pop_front() {
        stmt.done = "FAILED";
        stmt.error = Some(text);
        return stmt;
    }
    let is_unload = sql.trim_start().to_ascii_uppercase().starts_with("UNLOAD");
    stmt.hang = s.hang || (s.hang_unload && is_unload);
    let r = if is_unload {
        unload(s, sql).map(|n| {
            stmt.rows = n;
        })
    } else {
        query(&s.db, sql).map(|(cols, rows)| {
            stmt.rows = rows.len() as i64;
            stmt.result = Some((cols, rows));
        })
    };
    if let Err(e) = r {
        stmt.done = "FAILED";
        stmt.error = Some(format!("ERROR: {e}"));
    }
    stmt
}

fn query(db: &duckdb::Connection, sql: &str) -> Result<(Vec<String>, Vec<Vec<Value>>), String> {
    let mut d = db
        .prepare(&format!("DESCRIBE {sql}"))
        .map_err(|e| e.to_string())?;
    let desc: Vec<(String, String)> = d
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .map_err(|e| e.to_string())?
        .collect::<Result<_, _>>()
        .map_err(|e| e.to_string())?;
    let proj: Vec<String> = desc
        .iter()
        .map(|(n, t)| {
            let q = format!("\"{}\"", n.replace('"', "\"\""));
            if [
                "BIGINT", "INTEGER", "SMALLINT", "BOOLEAN", "DOUBLE", "FLOAT",
            ]
            .contains(&t.as_str())
            {
                q
            } else {
                format!("CAST({q} AS VARCHAR) AS {q}")
            }
        })
        .collect();
    let mut st = db
        .prepare(&format!("SELECT {} FROM ({sql}) q", proj.join(", ")))
        .map_err(|e| e.to_string())?;
    let mut rows = st.query([]).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    while let Some(r) = rows.next().map_err(|e| e.to_string())? {
        let mut row = Vec::new();
        for i in 0..desc.len() {
            row.push(match r.get_ref(i).map_err(|e| e.to_string())? {
                ValueRef::Null => json!({"isNull": true}),
                ValueRef::Boolean(b) => json!({"booleanValue": b}),
                ValueRef::SmallInt(n) => json!({"longValue": n}),
                ValueRef::Int(n) => json!({"longValue": n}),
                ValueRef::BigInt(n) => json!({"longValue": n}),
                ValueRef::Float(f) => json!({"doubleValue": f}),
                ValueRef::Double(f) => json!({"doubleValue": f}),
                ValueRef::Text(t) => json!({"stringValue": String::from_utf8_lossy(t)}),
                other => json!({"stringValue": format!("{other:?}")}),
            });
        }
        out.push(row);
    }
    Ok((desc.into_iter().map(|(n, _)| n).collect(), out))
}

/// Reads a Redshift string literal starting at `s[0] == '\''`; returns (value, rest).
fn take_literal(s: &str) -> Option<(String, &str)> {
    let b = s.strip_prefix('\'')?;
    let mut out = String::new();
    let mut it = b.char_indices().peekable();
    while let Some((i, c)) = it.next() {
        match c {
            '\'' if it.peek().map(|x| x.1) == Some('\'') => {
                it.next();
                out.push('\'');
            }
            '\'' => return Some((out, &b[i + 1..])),
            '\\' if it.peek().map(|x| x.1) == Some('\\') => {
                it.next();
                out.push('\\');
            }
            c => out.push(c),
        }
    }
    None
}

fn unload(s: &mut State, sql: &str) -> Result<i64, String> {
    let rest = sql.trim_start()[6..].trim_start();
    let rest = rest
        .strip_prefix('(')
        .ok_or("syntax error at or near UNLOAD")?;
    let (inner, rest) = take_literal(rest.trim_start()).ok_or("syntax error in UNLOAD select")?;
    let rest = rest
        .trim_start()
        .strip_prefix(')')
        .ok_or("syntax error: )")?
        .trim_start();
    let rest = rest.strip_prefix("TO ").ok_or("syntax error: TO")?;
    let (to, opts) = take_literal(rest.trim_start()).ok_or("syntax error: TO literal")?;
    for need in [
        "IAM_ROLE",
        "FORMAT PARQUET",
        "MANIFEST VERBOSE",
        "MAXFILESIZE",
        "PARALLEL ON",
    ] {
        if !opts.contains(need) {
            return Err(format!("fake UNLOAD expects {need}"));
        }
    }
    let db = &s.db;
    let existing: i64 = db
        .query_row(&format!("SELECT count(*) FROM glob('{to}*')"), [], |r| {
            r.get(0)
        })
        .map_err(|e| e.to_string())?;
    if existing > 0 {
        return Err("Specified unload destination on S3 is not empty. Consider using a different bucket / prefix, manually removing the target files in S3, or using the ALLOWOVERWRITE option.".into());
    }
    db.execute_batch(&format!("CREATE OR REPLACE TEMP TABLE __unload AS {inner}"))
        .map_err(|e| e.to_string())?;
    let mut d = db.prepare("DESCRIBE __unload").map_err(|e| e.to_string())?;
    let desc: Vec<(String, String)> = d
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .map_err(|e| e.to_string())?
        .collect::<Result<_, _>>()
        .map_err(|e| e.to_string())?;
    // Parquet keeps no zone: timestamptz is written as naive UTC.
    let proj: Vec<String> = desc
        .iter()
        .map(|(n, t)| {
            let q = format!("\"{n}\"");
            if t == "TIMESTAMP WITH TIME ZONE" {
                format!("CAST({q} AS TIMESTAMP) AS {q}")
            } else {
                q
            }
        })
        .collect();
    let total: i64 = db
        .query_row("SELECT count(*) FROM __unload", [], |r| r.get(0))
        .map_err(|e| e.to_string())?;
    let (bucket, key) = to
        .strip_prefix("s3://")
        .and_then(|r| r.split_once('/'))
        .ok_or("bad TO")?;
    let mut entries = Vec::new();
    let mut bytes = 0i64;
    let k = s.slices.max(1);
    for i in 0..k {
        let filter = format!("rowid % {k} = {i}");
        let n: i64 = db
            .query_row(
                &format!("SELECT count(*) FROM __unload WHERE {filter}"),
                [],
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())?;
        if n == 0 {
            continue;
        }
        let url = format!("{to}{i:04}_part_00.parquet");
        db.execute_batch(&format!(
            "COPY (SELECT {} FROM __unload WHERE {filter}) TO '{url}' (FORMAT parquet)",
            proj.join(", ")
        ))
        .map_err(|e| e.to_string())?;
        let size: i64 = db
            .query_row(&format!("SELECT size FROM read_blob('{url}')"), [], |r| {
                r.get(0)
            })
            .map_err(|e| e.to_string())?;
        bytes += size;
        entries.push(json!({"url": url, "meta": {"content_length": size, "record_count": n as u64 + s.lie_record_count}}));
    }
    if total > 0 || s.empty_manifest {
        let m = json!({
            "entries": entries,
            "schema": {"elements": desc.iter().map(|(n, t)| json!({"name": n, "type": {"base": t.to_lowercase()}})).collect::<Vec<_>>()},
            "meta": {"content_length": bytes, "record_count": total as u64 + s.lie_record_count * entries.len() as u64},
            "author": {"name": "Amazon Redshift", "version": "1.0.0"}
        });
        let (st, resp) = crate::rustfs()
            .request("PUT", &format!("/{bucket}/{key}manifest"), &m.to_string())
            .map_err(|e| e.to_string())?;
        if st != 200 {
            return Err(format!("manifest PUT: {st} {resp}"));
        }
    }
    db.execute_batch("DROP TABLE __unload")
        .map_err(|e| e.to_string())?;
    Ok(total)
}

/// A TCP proxy to `upstream` that answers the first `fail` requests whose request line contains
/// `needle` with `503 Slow Down`. Clients must not reuse connections (`http_keep_alive=false`).
pub struct Faulty {
    pub addr: String,
    pub remaining: Arc<Mutex<u32>>,
    pub hits: Arc<Mutex<u32>>,
}

impl Faulty {
    pub async fn start(upstream: String, needle: String, fail: u32) -> Self {
        let l = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("redshift fake");
        let addr = l.local_addr().expect("redshift fake").to_string();
        let remaining = Arc::new(Mutex::new(fail));
        let hits = Arc::new(Mutex::new(0));
        let (rem, h) = (remaining.clone(), hits.clone());
        tokio::spawn(async move {
            loop {
                let Ok((mut c, _)) = l.accept().await else {
                    return;
                };
                let (rem, h, up, needle) =
                    (rem.clone(), h.clone(), upstream.clone(), needle.clone());
                tokio::spawn(async move {
                    let mut head = Vec::new();
                    let mut buf = [0u8; 4096];
                    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                        match c.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => head.extend_from_slice(&buf[..n]),
                        }
                    }
                    let first = String::from_utf8_lossy(&head)
                        .lines()
                        .next()
                        .unwrap_or("")
                        .to_owned();
                    let fail = first.contains(&needle) && {
                        let mut r = rem.lock().expect("redshift fake");
                        let f = *r > 0;
                        if f {
                            *r -= 1;
                        }
                        f
                    };
                    if fail {
                        *h.lock().expect("redshift fake") += 1;
                        let body = "<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>";
                        let _ = c
                            .write_all(format!("HTTP/1.1 503 Slow Down\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes())
                            .await;
                        return;
                    }
                    let Ok(mut u) = TcpStream::connect(&up).await else {
                        return;
                    };
                    if u.write_all(&head).await.is_err() {
                        return;
                    }
                    let _ = tokio::io::copy_bidirectional(&mut c, &mut u).await;
                });
            }
        });
        Self {
            addr,
            remaining,
            hits,
        }
    }
}
