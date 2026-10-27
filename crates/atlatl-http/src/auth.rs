//! Authentication (07 §3). Every resolved secret or token is registered with the global redactor.
//!
//! [`Auth::prepare`] may do network I/O (OAuth2 token requests, which draw from the connection's
//! limiter) and runs before a limiter slot is taken; [`Auth::apply`] only sets headers, query
//! parameters and signatures, right before sending.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use atlatl_core::retry::{Jitter, RetryPolicy};
use atlatl_core::{AtlatlError, ErrorClass};
use minijinja::Value;
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::expr::{Engine, Scope, is_template, json_value};
use crate::funcs;
use crate::limiter::Limiter;
use crate::request::{Prepared, PreparedBody, build};
use crate::spec::{
    AuthSpec, ClientAuth, Expr, HmacSpec, OAuth2Spec, OAuthFlow, Placement, RequestSpec,
    SigEncoding,
};
use crate::transport::Transport;

/// Refresh this long before `expires_in` runs out.
pub const REFRESH_MARGIN: Duration = Duration::from_secs(60);

#[derive(Clone, Debug)]
struct Token {
    access: String,
    expires_at: Option<Instant>,
    refresh: Option<String>,
}

#[derive(Debug, Default)]
struct TokenState {
    token: Option<Token>,
    refresh: Option<String>,
    generation: u64,
}

/// Credentials in use for one request; pass back to [`Auth::invalidate`] on a 401.
#[derive(Clone, Debug, Default)]
pub struct Ticket {
    identity: String,
    generation: u64,
    access: Option<String>,
}

/// What token requests need from the client.
pub struct AuthEnv<'a> {
    pub transport: &'a Transport,
    pub limiter: &'a Limiter,
    pub engine: &'a Engine,
    pub scope: &'a Scope,
    pub default_request: &'a RequestSpec,
    pub default_timeout: Option<Duration>,
    pub retry: &'a RetryPolicy,
    pub jitter: &'a std::sync::Mutex<Box<dyn Jitter>>,
    pub max_body: u64,
    /// Incremented once per token request sent.
    pub requests: &'a AtomicU64,
}

/// OAuth2 tokens are cached per rendered credential set, so per-item credentials
/// (`secrets.item.*`) each get their own token on a shared connection.
#[derive(Debug)]
pub struct Auth {
    spec: AuthSpec,
    state: Mutex<HashMap<String, TokenState>>,
}

fn render_secret(engine: &Engine, src: &str, scope: &Scope) -> Result<String, AtlatlError> {
    let v = engine.render(src, scope)?;
    if is_template(src) {
        atlatl_core::redact::register(&v);
    }
    Ok(v)
}

impl Auth {
    pub fn new(spec: AuthSpec) -> Self {
        Self {
            spec,
            state: Mutex::new(HashMap::new()),
        }
    }

    pub fn spec(&self) -> &AuthSpec {
        &self.spec
    }

    pub fn reauth_when(&self) -> Option<&Expr> {
        match &self.spec {
            AuthSpec::OAuth2(o) => o.reauth_when.as_ref(),
            _ => None,
        }
    }

    pub async fn prepare(&self, env: &AuthEnv<'_>) -> Result<Ticket, AtlatlError> {
        let AuthSpec::OAuth2(o) = &self.spec else {
            return Ok(Ticket::default());
        };
        let identity = identity(o, env.engine, env.scope)?;
        let mut tokens = self.state.lock().await;
        let st = tokens.entry(identity.clone()).or_default();
        if let Some(t) = &st.token
            && t.expires_at.is_none_or(|e| Instant::now() < e)
        {
            return Ok(Ticket {
                identity,
                generation: st.generation,
                access: Some(t.access.clone()),
            });
        }
        let configured = match (o.flow, &o.refresh_token) {
            (OAuthFlow::RefreshToken, Some(r)) => Some(render_secret(env.engine, r, env.scope)?),
            _ => None,
        };
        let refresh = st.refresh.clone().or(configured);
        let token = match refresh {
            Some(r) => match self.fetch(o, env, Some(&r)).await {
                Ok(t) => t,
                Err(e) if o.flow != OAuthFlow::RefreshToken && e.class == ErrorClass::Auth => {
                    self.fetch(o, env, None).await?
                }
                Err(e) => return Err(e),
            },
            None => self.fetch(o, env, None).await?,
        };
        st.generation += 1;
        if token.refresh.is_some() {
            st.refresh = token.refresh.clone();
        }
        let access = token.access.clone();
        st.token = Some(token);
        Ok(Ticket {
            identity,
            generation: st.generation,
            access: Some(access),
        })
    }

    /// Drops the cached token if `t` still names it (a concurrent refresh may have replaced it).
    pub async fn invalidate(&self, t: &Ticket) {
        let mut tokens = self.state.lock().await;
        if let Some(st) = tokens.get_mut(&t.identity)
            && st.generation == t.generation
        {
            st.token = None;
        }
    }

    async fn fetch(
        &self,
        o: &OAuth2Spec,
        env: &AuthEnv<'_>,
        refresh: Option<&str>,
    ) -> Result<Token, AtlatlError> {
        let e = env.engine;
        let s = env.scope;
        let token_req = RequestSpec {
            url: Some(Expr::new(o.token_url.clone(), Default::default())),
            method: crate::spec::Method::Post,
            path: None,
            body: None,
            ..env.default_request.clone()
        };
        let mut req = build(e, None, &token_req, s, env.default_timeout)?;
        let mut form: Vec<(String, String)> = Vec::new();
        let grant = match (refresh, o.flow) {
            (Some(_), _) => "refresh_token",
            (None, OAuthFlow::ClientCredentials) => "client_credentials",
            (None, OAuthFlow::Password) => "password",
            (None, OAuthFlow::RefreshToken) => "refresh_token",
        };
        form.push(("grant_type".into(), grant.into()));
        if let Some(r) = refresh {
            form.push(("refresh_token".into(), r.to_owned()));
        } else if o.flow == OAuthFlow::Password {
            let u = o.username.as_deref().unwrap_or_default();
            form.push(("username".into(), render_secret(e, u, s)?));
            let p = o.password.as_deref().unwrap_or_default();
            form.push(("password".into(), render_secret(e, p, s)?));
        }
        if !o.scopes.is_empty() {
            form.push(("scope".into(), o.scopes.join(" ")));
        }
        let id = o
            .client_id
            .as_deref()
            .map(|x| render_secret(e, x, s))
            .transpose()?;
        let secret = o
            .client_secret
            .as_deref()
            .map(|x| render_secret(e, x, s))
            .transpose()?;
        match (o.client_auth, &id) {
            (ClientAuth::Basic, Some(id)) => {
                let raw = format!(
                    "{}:{}",
                    funcs::urlencode_str(id),
                    funcs::urlencode_str(secret.as_deref().unwrap_or_default())
                );
                let v = format!("Basic {}", funcs::base64(raw.as_bytes()));
                atlatl_core::redact::register(&v[6..]);
                req.set_header("Authorization", v);
            }
            (ClientAuth::Body, Some(id)) => {
                form.push(("client_id".into(), id.clone()));
                if let Some(sec) = &secret {
                    form.push(("client_secret".into(), sec.clone()));
                }
            }
            (_, None) => {}
        }
        for (k, v) in &o.extra {
            form.push((k.clone(), e.render(v, s)?));
        }
        req.body = Some(PreparedBody {
            bytes: form
                .iter()
                .map(|(k, v)| format!("{}={}", funcs::urlencode_str(k), funcs::urlencode_str(v)))
                .collect::<Vec<_>>()
                .join("&")
                .into_bytes(),
            content_type: "application/x-www-form-urlencoded",
            json: None,
        });
        req.set_header("Accept", "application/json".into());

        let mut attempt = 1u32;
        loop {
            let permit = env.limiter.acquire().await?;
            let res = env.transport.send(&req, env.max_body).await;
            drop(permit);
            env.requests.fetch_add(1, Ordering::Relaxed);
            let err = match res {
                Ok(cap) if (200..300).contains(&cap.status) => return parse_token(&cap.body),
                Ok(cap) => {
                    let class = match cap.status {
                        429 => ErrorClass::Throttled,
                        408 | 500..=599 => ErrorClass::Transient,
                        _ => {
                            return Err(AtlatlError::new(
                                ErrorClass::Auth,
                                format!(
                                    "OAuth2 token request to {} failed: HTTP {}: {}",
                                    cap.url,
                                    cap.status,
                                    cap.snippet()
                                ),
                            ));
                        }
                    };
                    let hint = atlatl_core::error::wait_hint(
                        cap.headers_iter(),
                        std::time::SystemTime::now(),
                    );
                    AtlatlError::new(class, format!("OAuth2 token request: HTTP {}", cap.status))
                        .retry_after(hint)
                }
                Err(e) => e,
            };
            if !err.class.is_retryable() || attempt >= env.retry.max_attempts {
                return Err(err);
            }
            let j = env
                .jitter
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .factor();
            let wait = env.retry.wait_for(&err, attempt, j);
            if err.class == ErrorClass::Throttled {
                env.limiter.throttled(wait);
            } else {
                env.limiter.sleep(wait).await;
            }
            attempt += 1;
        }
    }

    /// Adds credentials to `req`. `scope` is the request scope (with `secrets`).
    pub fn apply(
        &self,
        ticket: &Ticket,
        req: &mut Prepared,
        engine: &Engine,
        scope: &Scope,
    ) -> Result<(), AtlatlError> {
        match &self.spec {
            AuthSpec::None => {}
            AuthSpec::Headers(h) => {
                for (k, v) in h {
                    let v = render_secret(engine, v, scope)?;
                    req.set_header(k, v);
                }
            }
            AuthSpec::Query(q) => {
                for (k, v) in q {
                    let v = render_secret(engine, v, scope)?;
                    req.set_param(k, Some(&v));
                }
            }
            AuthSpec::Bearer { token } => {
                let t = render_secret(engine, token, scope)?;
                req.set_header("Authorization", format!("Bearer {t}"));
            }
            AuthSpec::Basic { username, password } => {
                let u = render_secret(engine, username, scope)?;
                let p = render_secret(engine, password, scope)?;
                let b = funcs::base64(format!("{u}:{p}").as_bytes());
                atlatl_core::redact::register(&b);
                req.set_header("Authorization", format!("Basic {b}"));
            }
            AuthSpec::OAuth2(_) => {
                let t = ticket.access.as_deref().ok_or_else(|| {
                    AtlatlError::internal("OAuth2 credentials applied without a token")
                })?;
                req.set_header("Authorization", format!("Bearer {t}"));
            }
            AuthSpec::Hmac(h) => sign(h, req, engine, scope)?,
        }
        Ok(())
    }
}

/// Digest of every rendered input that selects whose token a request gets.
fn identity(o: &OAuth2Spec, e: &Engine, s: &Scope) -> Result<String, AtlatlError> {
    let mut parts = vec![e.render(&o.token_url, s)?];
    for v in [
        &o.client_id,
        &o.client_secret,
        &o.username,
        &o.password,
        &o.refresh_token,
    ] {
        parts.push(match v {
            Some(v) => render_secret(e, v, s)?,
            None => String::new(),
        });
    }
    for (k, v) in &o.extra {
        parts.push(format!("{k}={}", e.render(v, s)?));
    }
    Ok(funcs::hex(&funcs::sha256(parts.join("\0").as_bytes())))
}

fn parse_token(body: &[u8]) -> Result<Token, AtlatlError> {
    let bad = |m: &str| AtlatlError::new(ErrorClass::Auth, format!("OAuth2 token response {m}"));
    let v = crate::json::parse(body).map_err(|e| bad(&format!("is not JSON: {e}")))?;
    let access = v
        .get("access_token")
        .and_then(|x| x.as_str())
        .ok_or_else(|| bad("has no `access_token`"))?
        .to_owned();
    atlatl_core::redact::register(&access);
    let refresh = v
        .get("refresh_token")
        .and_then(|x| x.as_str())
        .map(str::to_owned);
    if let Some(r) = &refresh {
        atlatl_core::redact::register(r);
    }
    let expires_in = v.get("expires_in").and_then(|x| match x {
        crate::json::JValue::Number(n) => Some(n.as_f64()),
        crate::json::JValue::String(s) => s.parse().ok(),
        _ => None,
    });
    let expires_at = expires_in.filter(|s| *s > 0.0).map(|s| {
        let life = Duration::from_secs_f64(s);
        Instant::now() + life.saturating_sub(REFRESH_MARGIN).max(life / 2)
    });
    Ok(Token {
        access,
        expires_at,
        refresh,
    })
}

fn sign(
    h: &HmacSpec,
    req: &mut Prepared,
    engine: &Engine,
    scope: &Scope,
) -> Result<(), AtlatlError> {
    let key = render_secret(engine, &h.key, scope)?;
    let mut s = scope.clone();
    s.set("timestamp", Value::from(engine.now().timestamp()));
    for (k, v) in &h.params {
        let v = if v.contains("secrets") {
            render_secret(engine, v, &s)?
        } else {
            engine.render(v, &s)?
        };
        req.set_param(k, Some(&v));
    }
    for (k, v) in &h.headers {
        let v = render_secret(engine, v, &s)?;
        req.set_header(k, v);
    }
    s.set("method", Value::from(req.method.as_str()))
        .set("path", Value::from(req.url.path()))
        .set("query", Value::from(req.url.query().unwrap_or_default()))
        .set("url", Value::from(req.url.as_str()))
        .set(
            "body",
            req.body
                .as_ref()
                .map(|b| Value::from(String::from_utf8_lossy(&b.bytes).into_owned()))
                .unwrap_or_default(),
        )
        .set(
            "body_json",
            req.body
                .as_ref()
                .and_then(|b| b.json.clone())
                .map(|j| json_value(std::sync::Arc::new(j)))
                .unwrap_or_default(),
        );
    let canonical = engine.render(&h.canonical, &s)?;
    let alg = match h.algorithm {
        crate::spec::HmacAlg::Sha256 => "sha256",
        crate::spec::HmacAlg::Sha512 => "sha512",
    };
    let mac =
        funcs::hmac(alg, key.as_bytes(), canonical.as_bytes()).map_err(AtlatlError::config)?;
    let sig = match h.encoding {
        SigEncoding::Base64 => funcs::base64(&mac),
        SigEncoding::Hex => funcs::hex(&mac),
    };
    match &h.placement {
        Placement::Header(n) => req.set_header(n, sig),
        Placement::Param(n) => req.set_param(n, Some(&sig)),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::json::JValue;

    macro_rules! json {
        ($($t:tt)+) => { JValue::from(serde_json::json!($($t)+)) };
    }

    use super::*;
    use crate::spec::HmacAlg;

    fn prepared(url: &str, body: Option<JValue>) -> Prepared {
        Prepared {
            method: crate::spec::Method::Post,
            url: url.parse().expect("url"),
            headers: Vec::new(),
            body: body.map(|j| PreparedBody {
                bytes: j.to_string().into_bytes(),
                content_type: "application/json",
                json: Some(j),
            }),
            timeout: None,
        }
    }

    fn scope() -> Scope {
        let mut s = Scope::new();
        s.set_json(
            "secrets",
            Arc::new(json!({"consumer_key": "ck-auth-test", "client_id": "CID", "user": "u1", "pw": "pw-auth-test"})),
        );
        s
    }

    #[test]
    fn sorted_json_hmac_signature() {
        let e = Engine::new(funcs::fixed_clock("2026-09-28T00:00:00Z"));
        let h = HmacSpec {
            algorithm: HmacAlg::Sha256,
            key: "{{ secrets.consumer_key }}".into(),
            canonical: r#"{{ {"content": body_json, "path": path, "query": query} | tojson(sort_keys=true) }}"#.into(),
            encoding: SigEncoding::Base64,
            placement: Placement::Header("Signature".into()),
            params: [("clientId".to_owned(), "{{ secrets.client_id }}".to_owned()), ("timestamp".to_owned(), "{{ timestamp }}".to_owned())]
                .into_iter()
                .collect(),
            headers: Default::default(),
        };
        let auth = Auth::new(AuthSpec::Hmac(Box::new(h)));
        let mut req = prepared(
            "https://api.signed.example/api/v1/accounts?userId=u1",
            Some(crate::json::parse_str(r#"{"b": 1.50, "a": "x"}"#).expect("json")),
        );
        auth.apply(&Ticket::default(), &mut req, &e, &scope())
            .expect("sign");
        assert_eq!(
            req.url.query(),
            Some("userId=u1&clientId=CID&timestamp=1790553600")
        );
        let canonical = r#"{"content":{"a":"x","b":1.50},"path":"/api/v1/accounts","query":"userId=u1&clientId=CID&timestamp=1790553600"}"#;
        let expected = funcs::base64(
            &funcs::hmac("sha256", b"ck-auth-test", canonical.as_bytes()).expect("mac"),
        );
        assert_eq!(req.headers, [("Signature".to_owned(), expected)]);
        assert_eq!(atlatl_core::redact::redact("ck-auth-test"), "****");
    }

    #[test]
    fn static_schemes() {
        let e = Engine::default();
        let mut req = prepared("https://x/y?a=1", None);
        Auth::new(AuthSpec::Query(
            [("api_key".to_owned(), "{{ secrets.pw }}".to_owned())]
                .into_iter()
                .collect(),
        ))
        .apply(&Ticket::default(), &mut req, &e, &scope())
        .expect("query");
        assert_eq!(req.url.as_str(), "https://x/y?a=1&api_key=pw-auth-test");
        Auth::new(AuthSpec::Basic {
            username: "{{ secrets.user }}".into(),
            password: "{{ secrets.pw }}".into(),
        })
        .apply(&Ticket::default(), &mut req, &e, &scope())
        .expect("basic");
        assert_eq!(
            req.headers[0].1,
            format!("Basic {}", funcs::base64(b"u1:pw-auth-test"))
        );
        Auth::new(AuthSpec::Bearer {
            token: "tok".into(),
        })
        .apply(&Ticket::default(), &mut req, &e, &scope())
        .expect("bearer");
        assert_eq!(
            req.headers,
            [("Authorization".to_owned(), "Bearer tok".to_owned())]
        );
        Auth::new(AuthSpec::Headers(
            [("api_key".to_owned(), "{{ secrets.pw }}".to_owned())]
                .into_iter()
                .collect(),
        ))
        .apply(&Ticket::default(), &mut req, &e, &scope())
        .expect("headers");
        assert_eq!(
            req.headers[1],
            ("api_key".to_owned(), "pw-auth-test".to_owned())
        );
        assert_eq!(atlatl_core::redact::redact("x pw-auth-test"), "x ****");
    }

    #[test]
    fn token_parsing() {
        let t = parse_token(br#"{"access_token": "at-auth-test", "expires_in": "3600", "refresh_token": "rt-auth-test"}"#).expect("t");
        assert_eq!(t.refresh.as_deref(), Some("rt-auth-test"));
        assert!(t.expires_at.is_some());
        assert_eq!(
            atlatl_core::redact::redact("at-auth-test rt-auth-test"),
            "**** ****"
        );
        let e = parse_token(b"{}").expect_err("missing");
        assert_eq!(e.class, ErrorClass::Auth);
        assert!(
            parse_token(br#"{"access_token": "at2-auth-test"}"#)
                .expect("no expiry")
                .expires_at
                .is_none()
        );
        // The redactor is process-wide: secrets registered here must not mask other tests' text.
        assert_eq!(
            atlatl_core::redact::redact(r#"GET https://x/a {"a":1}"#),
            r#"GET https://x/a {"a":1}"#
        );
    }
}
