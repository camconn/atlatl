//! Response rules (07 §10): first match wins, endpoint → spec → built-in defaults.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

use atlatl_core::error::classify_http_status;
use atlatl_core::{AtlatlError, ErrorClass};
use regex::Regex;
use tokio::time::Instant;

use crate::expr::{Engine, Scope};
use crate::response::ResponseView;
use crate::spec::{Action, BackoffSpec, DecodeThrottle, Expr, RuleSpec};

pub const THROTTLE_STATUSES: [u16; 3] = [409, 429, 503];
pub const BUILTIN_ATTEMPTS: u32 = 5;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Builtin {
    TooManyRequests,
    Transient,
    Unauthorized,
    DecodeFailure,
    ClientError,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    Auth,
    Endpoint,
    Spec,
    Builtin,
}

#[derive(Clone, Debug)]
pub enum When {
    Expr(Expr),
    Builtin(Builtin),
}

#[derive(Clone, Debug)]
pub struct Rule {
    pub when: When,
    pub action: Action,
    pub class: Option<ErrorClass>,
    pub backoff: Option<BackoffSpec>,
    pub max_attempts: Option<u32>,
    pub message: Option<String>,
    pub origin: Origin,
}

impl Rule {
    pub fn from_spec(r: &RuleSpec, origin: Origin) -> Self {
        Self {
            when: When::Expr(r.when.clone()),
            action: r.action,
            class: r.class,
            backoff: r.backoff.clone(),
            max_attempts: r.max_attempts,
            message: r.message.clone(),
            origin,
        }
    }

    fn builtin(b: Builtin, action: Action, class: Option<ErrorClass>) -> Self {
        Self {
            when: When::Builtin(b),
            action,
            class,
            backoff: None,
            max_attempts: (action == Action::Retry).then_some(BUILTIN_ATTEMPTS),
            message: None,
            origin: Origin::Builtin,
        }
    }

    /// One line for `atlatl api explain`.
    pub fn describe(&self) -> String {
        let when = match &self.when {
            When::Expr(e) => e.value.clone(),
            When::Builtin(Builtin::TooManyRequests) => {
                "status 429 (honours rate-limit headers)".into()
            }
            When::Builtin(Builtin::Transient) => "status 408 or 5xx".into(),
            When::Builtin(Builtin::Unauthorized) => "status 401 (once, then fail with auth)".into(),
            When::Builtin(Builtin::DecodeFailure) => {
                "decode failure on status < 400, 409, 429 or 503".into()
            }
            When::Builtin(Builtin::ClientError) => "status >= 400".into(),
        };
        let mut s = format!("{:?}: when {when} -> {:?}", self.origin, self.action).to_lowercase();
        if let Some(c) = self.class {
            s.push_str(&format!(" ({c})"));
        }
        if let Some(n) = self.max_attempts {
            s.push_str(&format!(", {n} attempts"));
        }
        s
    }
}

pub fn decode_rule() -> Rule {
    Rule::builtin(Builtin::DecodeFailure, Action::Retry, None)
}

pub fn builtin_rules() -> Vec<Rule> {
    vec![
        Rule::builtin(
            Builtin::TooManyRequests,
            Action::Retry,
            Some(ErrorClass::Throttled),
        ),
        Rule::builtin(
            Builtin::Transient,
            Action::Retry,
            Some(ErrorClass::Transient),
        ),
        Rule::builtin(
            Builtin::Unauthorized,
            Action::Reauth,
            Some(ErrorClass::Auth),
        ),
        decode_rule(),
        Rule::builtin(Builtin::ClientError, Action::Fail, None),
    ]
}

/// Connection-wide inputs to decode classification.
#[derive(Debug)]
pub struct Classifier {
    patterns: Vec<Regex>,
    window: DecodeThrottle,
    failures: Mutex<VecDeque<Instant>>,
}

impl Classifier {
    pub fn new(patterns: &[String], window: DecodeThrottle) -> Result<Self, AtlatlError> {
        let patterns = patterns
            .iter()
            .map(|p| {
                Regex::new(&format!("(?i){}", regex::escape(p)))
                    .map_err(|e| AtlatlError::config(format!("throttle pattern `{p}`: {e}")))
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            patterns,
            window,
            failures: Mutex::new(VecDeque::new()),
        })
    }

    pub fn throttle_text(&self, v: &ResponseView) -> bool {
        if self.patterns.is_empty() {
            return false;
        }
        let t = v.cap.text();
        self.patterns.iter().any(|p| p.is_match(&t))
    }

    /// Classifies a decode failure, counting it towards `decode_failures_as_throttle`.
    pub fn decode_class(&self, v: &ResponseView) -> ErrorClass {
        let now = Instant::now();
        let over = {
            let mut f = self.failures.lock().unwrap_or_else(|e| e.into_inner());
            f.push_back(now);
            while f
                .front()
                .is_some_and(|t| now.saturating_duration_since(*t) > self.window.window)
            {
                f.pop_front();
            }
            f.len() > self.window.count as usize
        };
        if over || THROTTLE_STATUSES.contains(&v.status()) || self.throttle_text(v) {
            ErrorClass::Throttled
        } else {
            ErrorClass::Decode
        }
    }

    /// Class for a `retry`/`fail` rule that doesn't name one.
    pub fn default_class(&self, v: &ResponseView, action: Action) -> ErrorClass {
        let s = v.status();
        if action == Action::Retry && (THROTTLE_STATUSES.contains(&s) || self.throttle_text(v)) {
            return ErrorClass::Throttled;
        }
        match (classify_http_status(s), action) {
            (Some(ErrorClass::Config), Action::Retry) | (None, Action::Retry) => {
                if v.decode_error().is_some() {
                    ErrorClass::Decode
                } else {
                    ErrorClass::Transient
                }
            }
            (Some(c), _) => c,
            (None, _) => ErrorClass::Data,
        }
    }
}

pub struct Matched<'a> {
    pub rule: &'a Rule,
    pub class: ErrorClass,
}

fn builtin_matches(b: Builtin, v: &ResponseView, c: &Classifier) -> bool {
    let s = v.status();
    match b {
        Builtin::TooManyRequests => s == 429,
        Builtin::Transient => s == 408 || s >= 500,
        Builtin::Unauthorized => s == 401,
        Builtin::DecodeFailure => {
            (s < 400 || THROTTLE_STATUSES.contains(&s) || c.throttle_text(v))
                && v.decode_error().is_some()
        }
        Builtin::ClientError => s >= 400,
    }
}

pub fn evaluate<'a>(
    rules: &'a [Rule],
    engine: &Engine,
    scope: &Scope,
    v: &ResponseView,
    c: &Classifier,
) -> Result<Option<Matched<'a>>, AtlatlError> {
    for r in rules {
        let hit = match &r.when {
            When::Expr(e) => engine.eval_bool(e, scope).map_err(|err| {
                AtlatlError::config(format!("rule at {}: {}", e.span, err.message))
            })?,
            When::Builtin(b) => builtin_matches(*b, v, c),
        };
        if hit {
            let class = match (r.class, &r.when) {
                (Some(cl), _) => cl,
                (None, When::Builtin(Builtin::DecodeFailure)) => c.decode_class(v),
                (None, _)
                    if r.action == Action::Retry
                        && v.decode_error().is_some()
                        && v.status() < 400 =>
                {
                    c.decode_class(v)
                }
                (None, _) => c.default_class(v, r.action),
            };
            return Ok(Some(Matched { rule: r, class }));
        }
    }
    Ok(None)
}

pub fn backoff_policy(
    base: &atlatl_core::retry::RetryPolicy,
    b: Option<&BackoffSpec>,
) -> atlatl_core::retry::RetryPolicy {
    let mut p = base.clone();
    if let Some(b) = b {
        p.backoff = b.kind;
        if let Some(x) = b.base {
            p.base = x.0;
        }
        if let Some(x) = b.max {
            p.max = x.0;
        }
        if p.base > p.max {
            p.max = p.base;
        }
    }
    p
}

pub fn wait(policy: &atlatl_core::retry::RetryPolicy, attempt: u32, jitter: f64) -> Duration {
    policy.computed_wait(attempt, jitter)
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use atlatl_core::config::node::Span;
    use minijinja::Value;

    use super::*;
    use crate::response::Captured;
    use crate::spec::DecodeSpec;

    fn view(status: u16, ct: Option<&str>, body: &str) -> Arc<ResponseView> {
        ResponseView::new(
            Captured {
                status,
                headers: ct
                    .map(|c| vec![("content-type".into(), c.into())])
                    .unwrap_or_default(),
                body: body.as_bytes().to_vec(),
                elapsed: Duration::ZERO,
                url: "u".into(),
            },
            DecodeSpec::default(),
        )
    }

    fn user(when: &str, action: Action, class: Option<ErrorClass>) -> Rule {
        Rule {
            when: When::Expr(crate::spec::Expr::new(
                when.into(),
                Span::new(Arc::from(Path::new("t.yaml")), 3, 5),
            )),
            action,
            class,
            backoff: None,
            max_attempts: None,
            message: None,
            origin: Origin::Endpoint,
        }
    }

    fn run(
        rules: &[Rule],
        v: &Arc<ResponseView>,
        c: &Classifier,
    ) -> Option<(Action, ErrorClass, Origin)> {
        let mut s = Scope::new();
        s.set("response", Value::from_dyn_object(v.clone()));
        evaluate(rules, &Engine::default(), &s, v, c)
            .expect("eval")
            .map(|m| (m.rule.action, m.class, m.rule.origin))
    }

    fn classifier(p: &[&str]) -> Classifier {
        Classifier::new(
            &p.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            DecodeThrottle::default(),
        )
        .expect("c")
    }

    #[tokio::test(start_paused = true)]
    async fn builtin_decisions() {
        let rules = builtin_rules();
        let c = classifier(&[]);
        use Action::*;
        use ErrorClass as E;
        assert_eq!(run(&rules, &view(200, None, "[]"), &c), None);
        assert_eq!(
            run(&rules, &view(429, None, ""), &c).map(|x| (x.0, x.1)),
            Some((Retry, E::Throttled))
        );
        assert_eq!(
            run(&rules, &view(503, Some("text/html"), "<html>"), &c).map(|x| (x.0, x.1)),
            Some((Retry, E::Transient))
        );
        assert_eq!(
            run(&rules, &view(401, None, "{}"), &c).map(|x| (x.0, x.1)),
            Some((Reauth, E::Auth))
        );
        assert_eq!(
            run(&rules, &view(403, None, "{}"), &c).map(|x| (x.0, x.1)),
            Some((Fail, E::Auth))
        );
        assert_eq!(
            run(&rules, &view(404, Some("text/html"), "<html>"), &c).map(|x| (x.0, x.1)),
            Some((Fail, E::Config))
        );
        // Plain-text 409, no content type, no user rule: throttled, not a crash.
        assert_eq!(
            run(&rules, &view(409, None, "slow down"), &c).map(|x| (x.0, x.1)),
            Some((Retry, E::Throttled))
        );
        assert_eq!(
            run(&rules, &view(200, Some("text/html"), "<!doctype html>"), &c).map(|x| (x.0, x.1)),
            Some((Retry, E::Decode))
        );
        assert_eq!(
            run(&rules, &view(200, None, r#"{"a": [1"#), &c).map(|x| (x.0, x.1)),
            Some((Retry, E::Decode))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_decode_failures_become_throttling() {
        let rules = builtin_rules();
        let c = classifier(&[]);
        let html = view(200, Some("text/html"), "<html>please slow down</html>");
        let classes: Vec<_> = (0..5)
            .map(|_| run(&rules, &html, &c).expect("m").1)
            .collect();
        assert_eq!(
            classes,
            [
                ErrorClass::Decode,
                ErrorClass::Decode,
                ErrorClass::Decode,
                ErrorClass::Throttled,
                ErrorClass::Throttled
            ]
        );
        tokio::time::advance(Duration::from_secs(61)).await;
        assert_eq!(
            run(&rules, &html, &c).expect("m").1,
            ErrorClass::Decode,
            "window expired"
        );
        let p = classifier(&["Slow Down"]);
        assert_eq!(
            run(&rules, &html, &p).expect("m").1,
            ErrorClass::Throttled,
            "pattern"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn user_rules_first_and_default_classes() {
        let c = classifier(&["slow down"]);
        let mut rules = vec![
            user("response.status == 404", Action::Skip, None),
            user(
                r#"response.status == 409 or "slow down" in response.text"#,
                Action::Retry,
                None,
            ),
            user("response.status == 418", Action::Retry, None),
        ];
        rules.extend(builtin_rules());
        assert_eq!(
            run(&rules, &view(404, None, "<html>"), &c).map(|x| (x.0, x.2)),
            Some((Action::Skip, Origin::Endpoint))
        );
        assert_eq!(
            run(&rules, &view(409, None, "slow down"), &c).map(|x| x.1),
            Some(ErrorClass::Throttled)
        );
        assert_eq!(
            run(&rules, &view(418, None, "{}"), &c).map(|x| x.1),
            Some(ErrorClass::Transient)
        );
        let bad = vec![user("response.status ==", Action::Skip, None)];
        let v = view(200, None, "{}");
        let mut s = Scope::new();
        s.set("response", Value::from_dyn_object(v.clone()));
        let e = evaluate(&bad, &Engine::default(), &s, &v, &c)
            .err()
            .expect("syntax");
        assert!(e.message.contains("t.yaml:3:5"), "{e}");
    }

    #[test]
    fn describe_lists_builtins() {
        let d: Vec<String> = builtin_rules().iter().map(Rule::describe).collect();
        assert_eq!(
            d[0],
            "builtin: when status 429 (honours rate-limit headers) -> retry (throttled), 5 attempts"
        );
        assert_eq!(d[4], "builtin: when status >= 400 -> fail");
    }
}
