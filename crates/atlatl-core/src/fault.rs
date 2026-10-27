//! Fault injection for tests (12 §3). **Test-only behaviour**: inert unless `ATLATL_FAULT` is set
//! (or [`set`] is called), in which case named points in the runner panic, sleep, fail with an
//! error class, or SIGKILL the process. It ships in release builds so the binary is tested as
//! built; when unarmed a hit costs a `OnceLock` check and a relaxed atomic load.
//!
//! `ATLATL_FAULT=<rule>[,<rule>…]` where `<rule> = <point>[@<scope>]:<action>[:once]` and
//! `<action>` is `panic`, `sleep:<ms>`, `error:<class>` or `kill`. The scope is the replication
//! name (the first member's, for an atomic group); a rule without one matches every scope. `once`
//! disarms the rule after it fires. The first matching rule wins.
//!
//! Points: `exec.replication` (start of each attempt, inside the panic boundary),
//! `lake.after_stage` (members staged, before table locks), `lake.commit` (inside the commit
//! transaction, after every apply and state write, before `COMMIT`; every commit attempt; its
//! sleep blocks an engine thread and ignores the engine's kill), `lake.after_commit` (committed,
//! before the report is updated), `hop.start` (before each `then:` hop runs; scope
//! `<replication>#<hop>`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::Duration;

use crate::error::{AtlatlError, ErrorClass, Result};

pub const ENV: &str = "ATLATL_FAULT";
pub const CODE: &str = "fault_injected";

#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    Panic,
    Sleep(Duration),
    Error(ErrorClass),
    Kill,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Rule {
    pub point: String,
    pub scope: Option<String>,
    pub action: Action,
    pub once: bool,
}

#[derive(Default)]
struct State {
    rules: Vec<Rule>,
    fired: HashMap<String, u64>,
}

static ARMED: AtomicBool = AtomicBool::new(false);
static STATE: Mutex<Option<State>> = Mutex::new(None);
static INIT: OnceLock<std::result::Result<(), String>> = OnceLock::new();

pub fn parse(spec: &str) -> std::result::Result<Vec<Rule>, String> {
    spec.split(',')
        .map(str::trim)
        .filter(|r| !r.is_empty())
        .map(parse_rule)
        .collect()
}

fn parse_rule(r: &str) -> std::result::Result<Rule, String> {
    let mut parts: Vec<&str> = r.split(':').collect();
    let once = parts.last() == Some(&"once");
    if once {
        parts.pop();
    }
    let (target, action) = match parts.as_slice() {
        [t, "panic"] => (*t, Action::Panic),
        [t, "kill"] => (*t, Action::Kill),
        [t, "sleep", ms] => (
            *t,
            Action::Sleep(Duration::from_millis(
                ms.parse()
                    .map_err(|_| format!("`{r}`: bad sleep millis `{ms}`"))?,
            )),
        ),
        [t, "error", c] => {
            let class: ErrorClass =
                serde_json::from_value(serde_json::Value::String((*c).to_owned()))
                    .map_err(|_| format!("`{r}`: unknown error class `{c}`"))?;
            (*t, Action::Error(class))
        }
        _ => {
            return Err(format!(
                "`{r}`: expected <point>[@<scope>]:(panic|kill|sleep:<ms>|error:<class>)[:once]"
            ));
        }
    };
    let (point, scope) = match target.split_once('@') {
        Some((p, s)) => (p, Some(s.to_owned())),
        None => (target, None),
    };
    if point.is_empty() {
        return Err(format!("`{r}`: empty point"));
    }
    Ok(Rule {
        point: point.to_owned(),
        scope,
        action,
        once,
    })
}

fn install(rules: Vec<Rule>) {
    let mut g = STATE.lock().unwrap_or_else(PoisonError::into_inner);
    ARMED.store(!rules.is_empty(), Ordering::SeqCst);
    *g = Some(State {
        rules,
        fired: HashMap::new(),
    });
}

/// Reads `ATLATL_FAULT` once. Later calls return the first result; hits call this implicitly.
pub fn init() -> std::result::Result<(), String> {
    INIT.get_or_init(|| match std::env::var(ENV) {
        Ok(spec) => parse(&spec).map(|rules| {
            tracing::warn!(spec, "{ENV} is set: fault injection armed (test-only)");
            install(rules);
        }),
        Err(_) => Ok(()),
    })
    .clone()
}

/// Replaces the rules (tests in one process). Overrides `ATLATL_FAULT`.
pub fn set(spec: &str) -> std::result::Result<(), String> {
    let _ = init();
    install(parse(spec)?);
    Ok(())
}

pub fn clear() {
    let _ = init();
    install(Vec::new());
}

/// How many times rules at `point` have fired since the last [`set`]/[`clear`].
pub fn fired(point: &str) -> u64 {
    STATE
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .as_ref()
        .and_then(|s| s.fired.get(point).copied())
        .unwrap_or(0)
}

fn take(point: &str, scope: &str) -> Option<Action> {
    if INIT.get().is_none() {
        let _ = init();
    }
    if !ARMED.load(Ordering::Relaxed) {
        return None;
    }
    let mut g = STATE.lock().unwrap_or_else(PoisonError::into_inner);
    let s = g.as_mut()?;
    let i = s
        .rules
        .iter()
        .position(|r| r.point == point && r.scope.as_deref().is_none_or(|x| x == scope))?;
    let action = if s.rules[i].once {
        s.rules.remove(i).action
    } else {
        s.rules[i].action.clone()
    };
    *s.fired.entry(point.to_owned()).or_default() += 1;
    drop(g);
    tracing::warn!(point, scope, ?action, "fault injected");
    Some(action)
}

fn fire(point: &str, a: Action) -> Result<()> {
    match a {
        Action::Sleep(_) => Ok(()),
        Action::Panic => panic!("fault injected at {point}"),
        Action::Error(c) => {
            Err(AtlatlError::new(c, format!("fault injected at {point}")).code(CODE))
        }
        Action::Kill => {
            let _ = std::process::Command::new("kill")
                .args(["-KILL", &std::process::id().to_string()])
                .status();
            loop {
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    }
}

/// A fault point in blocking code (sleeps block the thread).
pub fn hit(point: &str, scope: &str) -> Result<()> {
    match take(point, scope) {
        None => Ok(()),
        Some(Action::Sleep(d)) => {
            std::thread::sleep(d);
            Ok(())
        }
        Some(a) => fire(point, a),
    }
}

/// A fault point in async code (sleeps yield, so cancellation drops them).
pub async fn hit_async(point: &str, scope: &str) -> Result<()> {
    match take(point, scope) {
        None => Ok(()),
        Some(Action::Sleep(d)) => {
            tokio::time::sleep(d).await;
            Ok(())
        }
        Some(a) => fire(point, a),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_rules() {
        let r = parse(
            "lake.commit:sleep:250:once, exec.replication@boom:panic,x:error:conflict,y:kill",
        )
        .expect("parse");
        assert_eq!(
            r[0],
            Rule {
                point: "lake.commit".into(),
                scope: None,
                action: Action::Sleep(Duration::from_millis(250)),
                once: true
            }
        );
        assert_eq!(
            r[1],
            Rule {
                point: "exec.replication".into(),
                scope: Some("boom".into()),
                action: Action::Panic,
                once: false
            }
        );
        assert_eq!(r[2].action, Action::Error(ErrorClass::Conflict));
        assert_eq!(r[3].action, Action::Kill);
        assert!(parse("").expect("empty").is_empty());
        for bad in [
            "x",
            "x:sleep",
            "x:sleep:abc",
            "x:error:nope",
            ":panic",
            "x:explode",
        ] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }

    // One test owns the global rules, so nothing else in this binary races it.
    #[tokio::test]
    async fn fires_by_scope_and_once() {
        assert!(hit("t.point", "a").is_ok());
        set("t.point@a:error:transient:once,t.point@b:error:data,t.sleep:sleep:1").expect("set");
        assert!(hit("t.point", "zzz").is_ok());
        assert_eq!(
            hit("t.point", "a").map_err(|e| (e.class, e.code)),
            Err((ErrorClass::Transient, Some(CODE.into())))
        );
        assert!(hit("t.point", "a").is_ok(), "once");
        assert_eq!(
            hit_async("t.point", "b").await.map_err(|e| e.class),
            Err(ErrorClass::Data)
        );
        assert_eq!(
            hit_async("t.point", "b").await.map_err(|e| e.class),
            Err(ErrorClass::Data)
        );
        assert!(hit_async("t.sleep", "").await.is_ok());
        assert_eq!(fired("t.point"), 3);
        set("t.panic:panic").expect("set");
        let p = std::panic::catch_unwind(|| hit("t.panic", "")).expect_err("panics");
        assert_eq!(
            p.downcast_ref::<String>().map(String::as_str),
            Some("fault injected at t.panic")
        );
        clear();
        assert!(hit("t.point", "b").is_ok());
        assert_eq!(fired("t.point"), 0);
    }
}
