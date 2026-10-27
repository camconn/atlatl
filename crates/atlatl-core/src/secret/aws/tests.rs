use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use super::*;
use crate::template::MapEnv;

#[derive(Clone, Debug)]
struct Seen {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Seen {
    fn header(&self, k: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(k))
            .map(|(_, v)| v.as_str())
    }
}

type Handler = dyn Fn(&Seen, usize) -> (u16, String) + Send + Sync;

struct Fake {
    url: String,
    log: Arc<Mutex<Vec<Seen>>>,
}

impl Fake {
    fn start(h: impl Fn(&Seen, usize) -> (u16, String) + Send + Sync + 'static) -> Self {
        let l = TcpListener::bind("127.0.0.1:0").expect("bind");
        let url = format!("http://{}", l.local_addr().expect("addr"));
        let log: Arc<Mutex<Vec<Seen>>> = Arc::default();
        let (lg, h): (_, Arc<Handler>) = (log.clone(), Arc::new(h));
        std::thread::spawn(move || {
            for sock in l.incoming() {
                let Ok(mut sock) = sock else { return };
                let mut r = BufReader::new(sock.try_clone().expect("clone"));
                let mut line = String::new();
                if r.read_line(&mut line).is_err() {
                    continue;
                }
                let mut parts = line.split_whitespace();
                let method = parts.next().unwrap_or_default().to_owned();
                let target = parts.next().unwrap_or_default().to_owned();
                let mut headers = Vec::new();
                loop {
                    let mut h = String::new();
                    if r.read_line(&mut h).is_err() {
                        break;
                    }
                    let h = h.trim_end();
                    if h.is_empty() {
                        break;
                    }
                    if let Some((k, v)) = h.split_once(':') {
                        headers.push((k.trim().to_ascii_lowercase(), v.trim().to_owned()));
                    }
                }
                let n = headers
                    .iter()
                    .find(|(k, _)| k == "content-length")
                    .and_then(|(_, v)| v.parse().ok())
                    .unwrap_or(0usize);
                let mut body = vec![0; n];
                let _ = r.read_exact(&mut body);
                let seen = Seen {
                    method,
                    target,
                    headers,
                    body,
                };
                let i = {
                    let mut l = lg.lock().expect("log");
                    l.push(seen.clone());
                    l.len() - 1
                };
                let (status, body) = h(&seen, i);
                let _ = write!(
                    sock,
                    "HTTP/1.1 {status} X\r\nContent-Type: application/x-amz-json-1.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        Self { url, log }
    }

    fn requests(&self) -> Vec<Seen> {
        self.log.lock().expect("log").clone()
    }
}

const AKID: &str = "AKIDEXAMPLE";
const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

/// Recomputes the signature from what arrived on the wire.
fn verify(s: &Seen, secret: &str) -> Result<(), String> {
    let auth = s.header("authorization").ok_or("no authorization")?;
    let re = regex::Regex::new(
        r"^AWS4-HMAC-SHA256 Credential=([A-Z0-9]+)/(\d{8})/([a-z0-9-]+)/([a-z]+)/aws4_request, SignedHeaders=([a-z0-9;-]+), Signature=([0-9a-f]{64})$",
    )
    .expect("re");
    let c = re
        .captures(auth)
        .ok_or_else(|| format!("bad shape: {auth}"))?;
    let (day, region, service, signed, sig) = (&c[2], &c[3], &c[4], &c[5], &c[6]);
    let date = s.header("x-amz-date").ok_or("no x-amz-date")?;
    if !date.starts_with(day) {
        return Err("scope date mismatch".into());
    }
    let mut ch = String::new();
    for h in signed.split(';') {
        let v = s
            .header(h)
            .ok_or_else(|| format!("signed header {h} missing"))?;
        ch.push_str(&format!("{h}:{v}\n"));
    }
    let canonical = format!(
        "{}\n/\n\n{ch}\n{signed}\n{}",
        s.method,
        hex(&Sha256::digest(&s.body))
    );
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{date}\n{day}/{region}/{service}/aws4_request\n{}",
        hex(&Sha256::digest(canonical.as_bytes()))
    );
    let mut k = hmac(format!("AWS4{secret}").as_bytes(), day);
    for p in [region, service, "aws4_request"] {
        k = hmac(&k, p);
    }
    if hex(&hmac(&k, &to_sign)) != sig {
        return Err("signature mismatch".into());
    }
    Ok(())
}

fn sm_ok(secret_string: &str) -> impl Fn(&Seen, usize) -> (u16, String) + Send + Sync + 'static {
    let body = serde_json::json!({"Name": "x", "SecretString": secret_string}).to_string();
    move |s, _| {
        if s.header("x-amz-target") != Some(TARGET)
            || s.header("content-type") != Some(CONTENT_TYPE)
        {
            return (400, r#"{"__type":"UnknownOperationException"}"#.into());
        }
        match verify(s, SECRET) {
            Ok(()) => (200, body.clone()),
            Err(e) => (
                403,
                format!(r#"{{"__type":"InvalidSignatureException","message":"{e}"}}"#),
            ),
        }
    }
}

fn env(url: &str, extra: &[(&str, &str)]) -> MapEnv {
    let mut v = vec![
        ("AWS_ACCESS_KEY_ID", AKID),
        ("AWS_SECRET_ACCESS_KEY", SECRET),
        ("AWS_REGION", "us-east-1"),
        ("AWS_ENDPOINT_URL_SECRETS_MANAGER", url),
    ];
    v.extend_from_slice(extra);
    MapEnv::new(v)
}

#[test]
fn sigv4_matches_the_aws_test_suite() {
    // aws-sig-v4-test-suite `get-vanilla`.
    let t = UNIX_EPOCH + Duration::from_secs(1_440_938_160);
    assert_eq!(amz_date(t), "20150830T123600Z");
    let h = sign(
        &Request {
            method: "GET",
            host: "example.amazonaws.com",
            path: "/",
            query: "",
            headers: &[],
            body: b"",
        },
        &Credentials {
            access_key_id: AKID.into(),
            secret_access_key: SECRET.into(),
            session_token: None,
        },
        "us-east-1",
        "service",
        t,
    );
    let auth = &h
        .iter()
        .find(|(k, _)| k == "authorization")
        .expect("auth")
        .1;
    assert_eq!(
        auth,
        "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
         SignedHeaders=host;x-amz-date, \
         Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
    );
}

#[test]
fn get_secret_value_signed_cached_and_keyed() {
    let f = Fake::start(sm_ok(
        r#"{"user": "svc", "password": "s3cr3t-pw", "port": 5432}"#,
    ));
    let e = env(&f.url, &[]);
    let v = resolve("prod/db", Some("password"), &e).expect("password");
    assert_eq!(v, "s3cr3t-pw");
    assert_eq!(resolve("prod/db", Some("port"), &e).expect("port"), "5432");
    let whole = resolve("prod/db", None, &e).expect("whole");
    assert!(whole.contains("\"user\""));
    let reqs = f.requests();
    assert_eq!(reqs.len(), 1, "cached per secret id");
    assert_eq!(reqs[0].method, "POST");
    assert_eq!(reqs[0].target, "/");
    let body: serde_json::Value = serde_json::from_slice(&reqs[0].body).expect("json");
    assert_eq!(body, serde_json::json!({"SecretId": "prod/db"}));
    let auth = reqs[0].header("authorization").expect("auth");
    assert!(
        auth.contains("/us-east-1/secretsmanager/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target,"),
        "{auth}"
    );
    assert!(crate::redact::redact(&format!("k={SECRET}")).contains("****"));
    let err = resolve("prod/db", Some("nope"), &e).expect_err("key");
    assert!(
        err.0.contains("no key `nope`") && err.0.contains("password"),
        "{err}"
    );
    assert!(!err.0.contains("s3cr3t-pw"));
}

#[test]
fn arn_region_session_token_and_errors() {
    let f = Fake::start(|s, _| {
        if s.header("x-amz-security-token") != Some("sess-tok") {
            return (400, "{}".into());
        }
        let auth = s.header("authorization").unwrap_or_default();
        if !auth.contains("/eu-west-2/") || !auth.contains("x-amz-security-token") {
            return (400, "{}".into());
        }
        if verify(s, SECRET).is_err() {
            return (403, "{}".into());
        }
        let id = String::from_utf8_lossy(&s.body).into_owned();
        if id.contains("missing") {
            return (
                400,
                r#"{"__type":"com.amazonaws.secretsmanager#ResourceNotFoundException","message":"Secrets Manager can't find the specified secret."}"#.into(),
            );
        }
        (200, r#"{"SecretBinary": "aGVsbG8="}"#.into())
    });
    let e = env(
        &f.url,
        &[("AWS_SESSION_TOKEN", "sess-tok"), ("AWS_REGION", "")],
    );
    let arn = "arn:aws:secretsmanager:eu-west-2:123456789012:secret:bin-AbCdEf";
    assert_eq!(resolve(arn, None, &e).expect("binary"), "hello");
    let err = resolve(
        "arn:aws:secretsmanager:eu-west-2:123456789012:secret:missing",
        None,
        &e,
    )
    .expect_err("missing");
    assert!(
        err.0.contains("HTTP 400 ResourceNotFoundException"),
        "{err}"
    );
    assert!(
        err.0.starts_with("secret `aws-sm:arn:aws:secretsmanager"),
        "{err}"
    );
}

#[test]
fn retries_server_errors() {
    let f = Fake::start(|s, i| {
        if i == 0 {
            return (500, r#"{"__type":"InternalServiceError"}"#.into());
        }
        sm_ok("v")(s, i)
    });
    assert_eq!(
        resolve("retry/me", None, &env(&f.url, &[])).expect("v"),
        "v"
    );
    assert_eq!(f.requests().len(), 2);
}

#[test]
fn container_credentials_endpoint() {
    let sm = Fake::start(|s, i| {
        if s.header("x-amz-security-token") != Some("ctok") {
            return (400, "{}".into());
        }
        let auth = s.header("authorization").unwrap_or_default();
        if !auth.contains("Credential=ASIACONTAINER/") {
            return (400, "{}".into());
        }
        let _ = i;
        match verify(s, "container-secret") {
            Ok(()) => (200, r#"{"SecretString": "from-container"}"#.into()),
            Err(e) => (403, format!(r#"{{"message":"{e}"}}"#)),
        }
    });
    let creds = Fake::start(|s, _| {
        if s.header("authorization") != Some("Bearer ecs-auth") {
            return (401, "{}".into());
        }
        (
            200,
            r#"{"AccessKeyId":"ASIACONTAINER","SecretAccessKey":"container-secret","Token":"ctok","Expiration":"2099-01-01T00:00:00Z"}"#.into(),
        )
    });
    let full = format!("{}/v2/credentials/abc", creds.url);
    let e = MapEnv::new([
        ("AWS_REGION", "us-east-1"),
        ("AWS_ENDPOINT_URL", sm.url.as_str()),
        ("AWS_CONTAINER_CREDENTIALS_FULL_URI", full.as_str()),
        ("AWS_CONTAINER_AUTHORIZATION_TOKEN", "Bearer ecs-auth"),
    ]);
    assert_eq!(resolve("c/1", None, &e).expect("secret"), "from-container");
    assert_eq!(creds.requests()[0].target, "/v2/credentials/abc");
}

#[test]
fn web_identity_via_sts() {
    let dir = tempfile::tempdir().expect("tmp");
    let tok = dir.path().join("token");
    std::fs::write(&tok, "jwt-token\n").expect("write");
    let sts = Fake::start(|s, _| {
        let body = String::from_utf8_lossy(&s.body);
        if !body.contains("Action=AssumeRoleWithWebIdentity")
            || !body.contains("WebIdentityToken=jwt%2Dtoken")
            || !body.contains("RoleArn=arn%3Aaws%3Aiam%3A%3A1%3Arole%2Fr")
        {
            return (400, "<ErrorResponse><Error><Code>Bad</Code><Message>no</Message></Error></ErrorResponse>".into());
        }
        (
            200,
            "<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials>\
             <AccessKeyId>ASIAWEB</AccessKeyId><SecretAccessKey>web-secret</SecretAccessKey>\
             <SessionToken>web-tok</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration>\
             </Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>"
                .into(),
        )
    });
    let sm = Fake::start(|s, _| match verify(s, "web-secret") {
        Ok(()) if s.header("x-amz-security-token") == Some("web-tok") => {
            (200, r#"{"SecretString": "from-web"}"#.into())
        }
        _ => (403, "{}".into()),
    });
    let tok = tok.display().to_string();
    let e = MapEnv::new([
        ("AWS_REGION", "us-east-1"),
        ("AWS_ENDPOINT_URL_SECRETS_MANAGER", sm.url.as_str()),
        ("AWS_ENDPOINT_URL_STS", sts.url.as_str()),
        ("AWS_WEB_IDENTITY_TOKEN_FILE", tok.as_str()),
        ("AWS_ROLE_ARN", "arn:aws:iam::1:role/r"),
    ]);
    assert_eq!(resolve("w/1", None, &e).expect("secret"), "from-web");
}

#[test]
fn profile_file() {
    let text = "[default]\naws_access_key_id = A1\naws_secret_access_key = S1\n\n[profile ci]\n# c\naws_access_key_id=A2\naws_secret_access_key=S2\naws_session_token=T2\n[sso]\nsso_start_url=x\n";
    let c = profile_credentials(text, "default").expect("default");
    assert_eq!((c.access_key_id.as_str(), c.session_token), ("A1", None));
    let c = profile_credentials(text, "ci").expect("ci");
    assert_eq!(c.session_token.as_deref(), Some("T2"));
    assert!(profile_credentials(text, "sso").is_none());

    let dir = tempfile::tempdir().expect("tmp");
    let p = dir.path().join("creds");
    std::fs::write(&p, text).expect("write");
    let p = p.display().to_string();
    let e = MapEnv::new([
        ("AWS_SHARED_CREDENTIALS_FILE", p.as_str()),
        ("AWS_PROFILE", "ci"),
    ]);
    assert_eq!(
        credentials(&e, "us-east-1").expect("creds").access_key_id,
        "A2"
    );
    let e = MapEnv::new([("AWS_PROFILE", "none"), ("HOME", "/nonexistent")]);
    assert!(
        credentials(&e, "us-east-1")
            .expect_err("none")
            .contains("no AWS credentials")
    );
}

#[test]
fn works_inside_runtimes() {
    let f = Fake::start(sm_ok("rt"));
    let e = Arc::new(env(&f.url, &[]));
    let mt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("rt");
    let e2 = e.clone();
    let v = mt.block_on(async move {
        tokio::spawn(async move { resolve("rt/mt", None, &*e2) })
            .await
            .expect("join")
    });
    assert_eq!(v.expect("mt"), "rt");
    let ct = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");
    assert_eq!(
        ct.block_on(async { resolve("rt/ct", None, &*e) })
            .expect("ct"),
        "rt"
    );
}

#[test]
fn resolve_in_place_registers_the_value() {
    let f = Fake::start(sm_ok(r#"{"token": "tok-from-sm-123"}"#));
    let e = env(&f.url, &[]);
    let r = crate::redact::Redactor::new();
    let mut v = crate::config::types::SecretValue::Ref(
        crate::config::types::SecretRef::parse("aws-sm:api/keys#token").expect("ref"),
    );
    super::super::resolve_in_place(&mut v, false, &e, std::path::Path::new("."), &r).expect("ok");
    assert_eq!(
        v,
        crate::config::types::SecretValue::Literal("tok-from-sm-123".into())
    );
    assert_eq!(r.redact("Bearer tok-from-sm-123"), "Bearer ****");
}
