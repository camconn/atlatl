//! Process-wide secret redaction (spec 10 §5).

use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::{LazyLock, RwLock};

use aho_corasick::{AhoCorasick, AhoCorasickKind, MatchKind};
use regex::Regex;

pub const MASK: &str = "****";

pub const DEFAULT_QUERY_PARAMS: &[&str] = &[
    "api_key",
    "apikey",
    "token",
    "c",
    "key",
    "secret",
    "password",
    "signature",
];
pub const SENSITIVE_HEADERS: &[&str] = &[
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "x-api-key",
];

static DSN_PASSWORD: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)(\bpassword\s*=\s*)('(?:[^'\\]|\\.)*'|[^\s'"]+)"#).expect("regex")
});
static URI_USERINFO: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"([a-zA-Z][a-zA-Z0-9+.-]*://[^:/@\s]+:)[^@\s/]+@").expect("regex")
});

pub struct Redactor {
    secrets: RwLock<Secrets>,
    query: RwLock<Regex>,
}

/// Registered secrets as automata of doubling size, merged like a binary counter: per-item
/// secrets arrive one at a time for the whole run, and each secret is rebuilt into O(log N)
/// automata over the run while a redaction scans O(log N) of them. Every occurrence of every
/// secret is found (overlapping search) and the union of them is masked, so no byte of a
/// secret is left in clear however secrets overlap.
#[derive(Default)]
struct Secrets {
    all: HashSet<String>,
    /// `None`: the automaton couldn't be built; the level is scanned directly instead.
    levels: Vec<(Vec<String>, Option<AhoCorasick>)>,
}

impl Secrets {
    fn insert(&mut self, secret: &str) {
        if !self.all.insert(secret.to_owned()) {
            return;
        }
        let mut level = vec![secret.to_owned()];
        while self
            .levels
            .last()
            .is_some_and(|(l, _)| l.len() <= level.len())
        {
            if let Some((l, _)) = self.levels.pop() {
                level.extend(l);
            }
        }
        let a = AhoCorasick::builder()
            .match_kind(MatchKind::Standard)
            .kind(Some(AhoCorasickKind::ContiguousNFA))
            .build(&level)
            .ok();
        self.levels.push((level, a));
    }

    fn redact<'a>(&self, out: Cow<'a, str>) -> Cow<'a, str> {
        let text = out.as_ref();
        let mut spans: Vec<(usize, usize)> = Vec::new();
        for (secrets, a) in &self.levels {
            match a {
                Some(a) => {
                    spans.extend(a.find_overlapping_iter(text).map(|m| (m.start(), m.end())))
                }
                None => {
                    for p in secrets {
                        spans.extend(
                            text.match_indices(p.as_str())
                                .map(|(i, m)| (i, i + m.len())),
                        );
                    }
                }
            }
        }
        if spans.is_empty() {
            return out;
        }
        spans.sort_unstable();
        let mut res = String::with_capacity(text.len());
        let (mut at, mut open): (usize, Option<(usize, usize)>) = (0, None);
        for (s, e) in spans {
            match open {
                Some((os, oe)) if s <= oe => open = Some((os, oe.max(e))),
                _ => {
                    if let Some((os, oe)) = open {
                        res.push_str(&text[at..os]);
                        res.push_str(MASK);
                        at = oe;
                    }
                    open = Some((s, e));
                }
            }
        }
        if let Some((os, oe)) = open {
            res.push_str(&text[at..os]);
            res.push_str(MASK);
            at = oe;
        }
        res.push_str(&text[at..]);
        Cow::Owned(res)
    }
}

static GLOBAL: LazyLock<Redactor> = LazyLock::new(Redactor::new);

impl Default for Redactor {
    fn default() -> Self {
        Self::new()
    }
}

impl Redactor {
    pub fn new() -> Self {
        Self {
            secrets: RwLock::new(Secrets::default()),
            query: RwLock::new(query_regex(DEFAULT_QUERY_PARAMS)),
        }
    }

    pub fn global() -> &'static Redactor {
        &GLOBAL
    }

    /// Registers a resolved secret. Empty values are ignored.
    pub fn register(&self, secret: &str) {
        if secret.is_empty() {
            return;
        }
        self.secrets
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(secret);
    }

    pub fn set_query_params(&self, names: &[&str]) {
        *self.query.write().unwrap_or_else(|e| e.into_inner()) = query_regex(names);
    }

    /// Masks registered secrets, sensitive URL query parameters, DSN passwords and URI userinfo.
    pub fn redact<'a>(&self, s: &'a str) -> Cow<'a, str> {
        let mut out = self
            .secrets
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .redact(Cow::Borrowed(s));
        let q = self.query.read().unwrap_or_else(|e| e.into_inner());
        for re in [&*q, &*DSN_PASSWORD] {
            if re.is_match(&out) {
                out = Cow::Owned(re.replace_all(&out, format!("${{1}}{MASK}")).into_owned());
            }
        }
        if URI_USERINFO.is_match(&out) {
            out = Cow::Owned(
                URI_USERINFO
                    .replace_all(&out, format!("${{1}}{MASK}@"))
                    .into_owned(),
            );
        }
        out
    }

    pub fn redact_header<'a>(&self, name: &str, value: &'a str) -> Cow<'a, str> {
        if SENSITIVE_HEADERS
            .iter()
            .any(|h| h.eq_ignore_ascii_case(name))
        {
            Cow::Borrowed(MASK)
        } else {
            self.redact(value)
        }
    }
}

fn query_regex(names: &[&str]) -> Regex {
    let alt = names
        .iter()
        .map(|n| regex::escape(n))
        .collect::<Vec<_>>()
        .join("|");
    // Values stop at quotes too, so URLs inside JSON or quoted log text keep their delimiters.
    Regex::new(&format!(r#"(?i)([?&](?:{alt})=)[^&#\s"'<>]*"#)).expect("query regex")
}

pub fn redact(s: &str) -> Cow<'_, str> {
    Redactor::global().redact(s)
}

pub fn register(secret: &str) {
    Redactor::global().register(secret)
}

#[cfg(test)]
mod tests {

    #[test]
    fn overlapping_secrets_across_levels_never_leak() {
        let r = Redactor::new();
        for x in ["fg", "cdef", "abcde"] {
            r.register(x);
        }
        assert_eq!(r.redact("abcdefg"), "****");
        assert_eq!(r.redact("x fg y cdef z abcde"), "x **** y **** z ****");
        let r = Redactor::new();
        r.register("abcd");
        r.register("cdef");
        assert_eq!(
            r.redact("abcdef!"),
            "****!",
            "partial overlaps are masked as one span"
        );
    }

    #[test]
    fn many_item_secrets_stay_cheap_and_whole() {
        let r = Redactor::new();
        let t = std::time::Instant::now();
        for i in 0..20_000 {
            let s = format!("user-secret-{i:06}-x");
            r.register(&s);
            assert_eq!(
                r.redact(&format!("GET /a?userSecret={s} ok")),
                "GET /a?userSecret=**** ok"
            );
        }
        assert!(
            t.elapsed() < std::time::Duration::from_secs(5),
            "{:?}",
            t.elapsed()
        );
        // A new secret containing older ones (in any level) is masked whole.
        r.register("abc");
        r.register("xxabcxx");
        assert_eq!(r.redact("1 xxabcxx 2 abc"), "1 **** 2 ****");
        r.register("zz-user-secret-000001-x-zz");
        assert_eq!(r.redact("<zz-user-secret-000001-x-zz>"), "<****>");
    }

    #[test]
    fn json_text_keeps_its_structure() {
        let r = Redactor::new();
        let out = r.redact(
            r#"{"u": "https://x/?token=abc&x=1", "d": "host=h password=pw1 user=u", "n": 1}"#,
        );
        assert_eq!(
            out,
            r#"{"u": "https://x/?token=****&x=1", "d": "host=h password=**** user=u", "n": 1}"#
        );
        let out = r.redact(r#"{"u": "https://x/?api_key=abc"}"#);
        assert_eq!(out, r#"{"u": "https://x/?api_key=****"}"#);
        assert!(serde_json::from_str::<serde_json::Value>(&out).is_ok());
    }

    use super::*;

    #[test]
    fn registered_secrets_longest_first() {
        let r = Redactor::new();
        r.register("abc");
        r.register("abcdef");
        r.register("");
        assert_eq!(r.redact("x abcdef y abc"), "x **** y ****");
        assert_eq!(r.redact("nothing"), Cow::Borrowed("nothing"));
    }

    #[test]
    fn query_params() {
        let r = Redactor::new();
        assert_eq!(
            r.redact("https://api.x.com/v1?symbol=AAPL&api_key=s3cr3t&c=guest:guest#frag"),
            "https://api.x.com/v1?symbol=AAPL&api_key=****&c=****#frag"
        );
        assert_eq!(r.redact("https://x/y?Token=abc"), "https://x/y?Token=****");
        assert_eq!(r.redact("https://x/y?country=us"), "https://x/y?country=us");
        r.set_query_params(&["sig"]);
        assert_eq!(
            r.redact("https://x/y?sig=1&api_key=2"),
            "https://x/y?sig=****&api_key=2"
        );
    }

    #[test]
    fn dsn_and_uri_passwords() {
        let r = Redactor::new();
        assert_eq!(
            r.redact("ducklake:postgres:host=h dbname=d user=u password='p w\\'x' sslmode=require"),
            "ducklake:postgres:host=h dbname=d user=u password=**** sslmode=require"
        );
        assert_eq!(r.redact("host=h password=hunter2"), "host=h password=****");
        assert_eq!(
            r.redact("postgres://dagster:hunter2@db:5432/x"),
            "postgres://dagster:****@db:5432/x"
        );
    }

    #[test]
    fn headers() {
        let r = Redactor::new();
        assert_eq!(r.redact_header("Authorization", "Bearer x"), "****");
        assert_eq!(r.redact_header("X-API-KEY", "k"), "****");
        assert_eq!(
            r.redact_header("Accept", "application/json"),
            "application/json"
        );
    }
}
