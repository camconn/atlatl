//! A local fake of the Cloudflare D1 REST API (`/query`, `/raw`, the paginated database list)
//! backed by SQLite, with D1's per-statement limits and fault injection.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};

use rusqlite::types::{Value as SqlValue, ValueRef};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

pub const ACCOUNT: &str = "acct-fake";
pub const TOKEN: &str = "d1-fake-token";
pub const DATABASE_ID: &str = "00000000-0000-4000-8000-00000000d1d1";
pub const DATABASE_NAME: &str = "app";
const MAX_PARAMS: usize = 100;
const MAX_SQL: usize = 100_000;

/// Status, body and extra headers.
pub type Resp = (u16, String, Vec<(String, String)>);

#[derive(Clone, Debug)]
pub struct Call {
    pub method: String,
    pub path: String,
    pub body: Value,
}

pub struct State {
    pub db: rusqlite::Connection,
    pub calls: Vec<Call>,
    /// Responses (status, body, headers) returned, in order, instead of handling requests.
    pub inject: VecDeque<Resp>,
    /// Write statements still allowed; once 0, every write fails with a 500 until reset.
    pub writes_left: Option<usize>,
    /// Requests to apply and then answer with a 502, as if the response were lost.
    pub lose_responses: usize,
    /// A `batch` body runs in one transaction (else statement by statement; S11 is open).
    pub atomic_batches: bool,
    /// Other databases listed before ours, to exercise pagination.
    pub other_databases: usize,
    pub page_size: usize,
}

#[derive(Clone)]
pub struct Fake {
    /// The API base, to use as the connection's `endpoint`.
    pub endpoint: String,
    state: Arc<Mutex<State>>,
}

impl Fake {
    pub async fn start() -> Self {
        let db = rusqlite::Connection::open_in_memory().expect("d1 fake");
        db.execute_batch("PRAGMA foreign_keys = ON")
            .expect("d1 fake");
        let state = Arc::new(Mutex::new(State {
            db,
            calls: Vec::new(),
            inject: VecDeque::new(),
            writes_left: None,
            lose_responses: 0,
            atomic_batches: false,
            other_databases: 0,
            page_size: 20,
        }));
        let l = TcpListener::bind("127.0.0.1:0").await.expect("d1 fake");
        let endpoint = format!("http://{}/client/v4", l.local_addr().expect("d1 fake"));
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
        self.state().db.execute_batch(sql).expect("d1 fake");
    }

    /// The first column of every row of `sql`, as text.
    pub fn rows(&self, sql: &str) -> Vec<String> {
        let s = self.state();
        let mut st = s.db.prepare(sql).expect(sql);
        st.query_map([], |r| {
            Ok(match r.get_ref(0)? {
                ValueRef::Null => "NULL".to_owned(),
                ValueRef::Integer(i) => i.to_string(),
                ValueRef::Real(f) => f.to_string(),
                ValueRef::Text(t) => String::from_utf8_lossy(t).into_owned(),
                ValueRef::Blob(b) => format!("{b:?}"),
            })
        })
        .expect(sql)
        .collect::<Result<_, _>>()
        .expect(sql)
    }

    pub fn calls(&self) -> Vec<Call> {
        self.state().calls.clone()
    }
}

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
        let target = parts.next().unwrap_or("/").to_owned();
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
        let len: usize = headers
            .iter()
            .find(|(k, _)| k == "content-length")
            .and_then(|(_, v)| v.parse().ok())
            .unwrap_or(0);
        let mut body = vec![0u8; len];
        r.read_exact(&mut body).await?;
        let st2 = st.clone();
        let (status, resp, extra) = tokio::task::spawn_blocking(move || {
            let mut s = st2.lock().unwrap_or_else(|e| e.into_inner());
            handle(&mut s, &method, &target, &headers, &body)
        })
        .await
        .expect("d1 fake");
        let mut out = format!(
            "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
            resp.len()
        );
        for (k, v) in extra {
            out.push_str(&format!("{k}: {v}\r\n"));
        }
        out.push_str("\r\n");
        w.write_all(out.as_bytes()).await?;
        w.write_all(resp.as_bytes()).await?;
    }
}

fn envelope_err(status: u16, code: u32, msg: &str) -> Resp {
    (status, json!({"result": null, "success": false, "errors": [{"code": code, "message": msg}], "messages": []}).to_string(), vec![])
}

fn handle(
    s: &mut State,
    method: &str,
    target: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> Resp {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let parsed: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    s.calls.push(Call {
        method: method.to_owned(),
        path: path.to_owned(),
        body: parsed.clone(),
    });
    if let Some(r) = s.inject.pop_front() {
        return r;
    }
    let auth = headers
        .iter()
        .find(|(k, _)| k == "authorization")
        .map(|(_, v)| v.as_str());
    if auth != Some(&format!("Bearer {TOKEN}")) {
        return envelope_err(401, 10000, "Authentication error");
    }
    let base = format!("/client/v4/accounts/{ACCOUNT}/d1/database");
    if method == "GET" && path == base {
        return list(s, query);
    }
    let Some(rest) = path.strip_prefix(&format!("{base}/{DATABASE_ID}/")) else {
        return envelope_err(404, 7404, "database not found");
    };
    let raw = match (method, rest) {
        ("POST", "raw") => true,
        ("POST", "query") => false,
        _ => return envelope_err(404, 7000, "no route"),
    };
    let stmts: Vec<Value> = match parsed.get("batch") {
        Some(Value::Array(b)) => b.clone(),
        _ if parsed.get("sql").is_some() => vec![parsed.clone()],
        _ => return envelope_err(400, 7400, "body must have `sql` or `batch`"),
    };
    let atomic = stmts.len() > 1 && s.atomic_batches;
    if atomic {
        let _ = s.db.execute_batch("SAVEPOINT d1_batch");
    }
    let mut results = Vec::new();
    for st in &stmts {
        match run(s, st, raw) {
            Ok(r) => results.push(r),
            Err((status, msg)) => {
                if atomic {
                    let _ = s.db.execute_batch("ROLLBACK TO d1_batch; RELEASE d1_batch");
                }
                return envelope_err(status, 7500, &msg);
            }
        }
    }
    if atomic {
        let _ = s.db.execute_batch("RELEASE d1_batch");
    }
    if s.lose_responses > 0 {
        s.lose_responses -= 1;
        return (502, "<html>bad gateway</html>".into(), vec![]);
    }
    (
        200,
        json!({"result": results, "success": true, "errors": [], "messages": []}).to_string(),
        vec![],
    )
}

fn list(s: &State, query: &str) -> Resp {
    let q = |k: &str| {
        query.split('&').find_map(|kv| {
            kv.split_once('=')
                .filter(|(a, _)| *a == k)
                .map(|(_, v)| v.to_owned())
        })
    };
    let page: usize = q("page").and_then(|p| p.parse().ok()).unwrap_or(1).max(1);
    let per: usize = q("per_page")
        .and_then(|p| p.parse().ok())
        .unwrap_or(s.page_size)
        .min(s.page_size)
        .max(1);
    let mut all: Vec<Value> = (0..s.other_databases)
        .map(|i| json!({"uuid": format!("other-{i}"), "name": format!("{DATABASE_NAME}-{i}")}))
        .collect();
    all.push(json!({"uuid": DATABASE_ID, "name": DATABASE_NAME}));
    let name = q("name");
    let all: Vec<Value> = all
        .into_iter()
        .filter(|d| {
            name.as_ref()
                .is_none_or(|n| d["name"].as_str().is_some_and(|x| x.contains(n.as_str())))
        })
        .collect();
    let items: Vec<Value> = all
        .iter()
        .skip((page - 1) * per)
        .take(per)
        .cloned()
        .collect();
    let body = json!({
        "result": items,
        "result_info": {"page": page, "per_page": per, "count": items.len(), "total_count": all.len()},
        "success": true, "errors": [], "messages": []
    });
    (200, body.to_string(), vec![])
}

fn bind(v: &Value) -> SqlValue {
    match v {
        Value::Null => SqlValue::Null,
        Value::Bool(b) => SqlValue::Integer(i64::from(*b)),
        Value::Number(n) => n.as_i64().map_or_else(
            || SqlValue::Real(n.as_f64().unwrap_or(0.0)),
            SqlValue::Integer,
        ),
        Value::String(s) => SqlValue::Text(s.clone()),
        other => SqlValue::Text(other.to_string()),
    }
}

fn cell(v: ValueRef<'_>) -> Value {
    match v {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => i.into(),
        ValueRef::Real(f) => serde_json::Number::from_f64(f).map_or(Value::Null, Value::Number),
        ValueRef::Text(t) => String::from_utf8_lossy(t).into_owned().into(),
        ValueRef::Blob(b) => b.iter().map(|x| Value::from(*x)).collect::<Vec<_>>().into(),
    }
}

fn run(s: &mut State, st: &Value, raw: bool) -> Result<Value, (u16, String)> {
    let sql = st.get("sql").and_then(Value::as_str).unwrap_or_default();
    let params: Vec<SqlValue> = st
        .get("params")
        .and_then(Value::as_array)
        .map(|p| p.iter().map(bind).collect())
        .unwrap_or_default();
    if sql.len() > MAX_SQL {
        return Err((400, "statement too long: SQLITE_TOOBIG".into()));
    }
    if params.len() > MAX_PARAMS {
        return Err((
            400,
            "too many SQL variables at offset 0: SQLITE_ERROR".into(),
        ));
    }
    let head = sql.trim_start().to_ascii_uppercase();
    if ["BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "END"]
        .iter()
        .any(|k| head.starts_with(k))
    {
        return Err((400, "To execute a transaction, please use the state.storage.transaction() APIs instead of the SQL BEGIN TRANSACTION or SAVEPOINT statements.".into()));
    }
    let err = |e: rusqlite::Error| {
        let code = match &e {
            rusqlite::Error::SqliteFailure(f, _) => format!("{:?}", f.code),
            _ => "SQLITE_ERROR".into(),
        };
        let code = match code.as_str() {
            "ConstraintViolation" => "SQLITE_CONSTRAINT",
            _ => "SQLITE_ERROR",
        };
        (400, format!("{e}: {code}"))
    };
    let mut stmt = s.db.prepare(sql).map_err(err)?;
    let write = !stmt.readonly();
    if write {
        match &mut s.writes_left {
            Some(0) => {
                return Err((
                    500,
                    "Internal error in D1 DB storage caused object to be reset.".into(),
                ));
            }
            Some(n) => *n -= 1,
            None => {}
        }
    }
    let cols: Vec<String> = stmt.column_names().map(str::to_owned).collect();
    let mut rows = Vec::new();
    {
        let mut q = stmt
            .query(rusqlite::params_from_iter(params))
            .map_err(err)?;
        while let Some(r) = q.next().map_err(err)? {
            rows.push(
                (0..cols.len())
                    .map(|i| r.get_ref(i).map(cell))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(err)?,
            );
        }
    }
    drop(stmt);
    let changes = if write && cols.is_empty() {
        s.db.changes()
    } else {
        0
    };
    let meta = json!({"changes": changes, "changed_db": changes > 0, "duration": 0.1, "last_row_id": s.db.last_insert_rowid(), "rows_read": rows.len(), "rows_written": changes});
    let results = if raw {
        json!({"columns": cols, "rows": rows})
    } else {
        Value::Array(
            rows.into_iter()
                .map(|r| Value::Object(cols.iter().cloned().zip(r).collect()))
                .collect(),
        )
    };
    Ok(json!({"results": results, "success": true, "meta": meta}))
}
