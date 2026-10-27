//! `atlatl_json_canon(VARCHAR) -> VARCHAR`: JSON with object keys sorted (last duplicate wins, as
//! in jsonb), no whitespace, strings re-escaped minimally and numbers kept as written. Two
//! documents compare equal after it iff they are equal as jsonb (bar number spelling such as
//! `1e2` vs `100`). Text it can't canonicalise (invalid, or nested past [`MAX_DEPTH`]) maps to
//! [`RAW`] + the text itself, never NULL, so such documents still compare unequal to anything
//! different, including NULL.

use std::collections::BTreeMap;
use std::fmt::Write;

use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeId};
use duckdb::ffi::duckdb_string_t;
use duckdb::types::DuckString;
use duckdb::vscalar::{ScalarFunctionSignature, VScalar};
use duckdb::vtab::arrow::WritableVector;

pub const FUNCTION: &str = "atlatl_json_canon";

enum J<'a> {
    Null,
    Bool(bool),
    Num(&'a str),
    Str(String),
    Arr(Vec<J<'a>>),
    Obj(BTreeMap<String, J<'a>>),
}

/// Deeper documents aren't parsed (the worker thread's stack); they compare by raw text.
pub const MAX_DEPTH: usize = 512;

/// Prefix of the raw-text fallback; canonical JSON never starts with it.
pub const RAW: char = '\u{1}';

/// What the SQL function returns for non-NULL input.
pub fn canon_or_raw(s: &str) -> String {
    canon(s).unwrap_or_else(|| format!("{RAW}{s}"))
}

struct P<'a> {
    s: &'a str,
    i: usize,
    depth: usize,
}

impl<'a> P<'a> {
    fn ws(&mut self) {
        while self
            .s
            .as_bytes()
            .get(self.i)
            .is_some_and(|b| b.is_ascii_whitespace())
        {
            self.i += 1;
        }
    }

    fn eat(&mut self, lit: &str) -> Option<()> {
        self.s[self.i..]
            .starts_with(lit)
            .then(|| self.i += lit.len())
    }

    fn value(&mut self) -> Option<J<'a>> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return None;
        }
        let v = self.value_inner();
        self.depth -= 1;
        v
    }

    fn value_inner(&mut self) -> Option<J<'a>> {
        self.ws();
        let v =
            match *self.s.as_bytes().get(self.i)? {
                b'n' => self.eat("null").map(|_| J::Null)?,
                b't' => self.eat("true").map(|_| J::Bool(true))?,
                b'f' => self.eat("false").map(|_| J::Bool(false))?,
                b'"' => J::Str(self.string()?),
                b'[' => {
                    self.i += 1;
                    let mut out = Vec::new();
                    self.ws();
                    if self.eat("]").is_none() {
                        loop {
                            out.push(self.value()?);
                            self.ws();
                            if self.eat(",").is_some() {
                                continue;
                            }
                            self.eat("]")?;
                            break;
                        }
                    }
                    J::Arr(out)
                }
                b'{' => {
                    self.i += 1;
                    let mut out = BTreeMap::new();
                    self.ws();
                    if self.eat("}").is_none() {
                        loop {
                            self.ws();
                            let k = self.string()?;
                            self.ws();
                            self.eat(":")?;
                            out.insert(k, self.value()?);
                            self.ws();
                            if self.eat(",").is_some() {
                                continue;
                            }
                            self.eat("}")?;
                            break;
                        }
                    }
                    J::Obj(out)
                }
                _ => {
                    let start = self.i;
                    while self.s.as_bytes().get(self.i).is_some_and(|b| {
                        matches!(b, b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9')
                    }) {
                        self.i += 1;
                    }
                    (self.i > start).then(|| J::Num(&self.s[start..self.i]))?
                }
            };
        Some(v)
    }

    fn string(&mut self) -> Option<String> {
        self.eat("\"")?;
        let mut out = String::new();
        let mut chars = self.s[self.i..].char_indices();
        while let Some((off, ch)) = chars.next() {
            match ch {
                '"' => {
                    self.i += off + 1;
                    return Some(out);
                }
                '\\' => {
                    let (_, e) = chars.next()?;
                    match e {
                        'n' => out.push('\n'),
                        't' => out.push('\t'),
                        'r' => out.push('\r'),
                        'b' => out.push('\u{8}'),
                        'f' => out.push('\u{c}'),
                        'u' => {
                            let hex = |c: &mut std::str::CharIndices| -> Option<u32> {
                                let h: String = (0..4)
                                    .map(|_| c.next().map(|(_, x)| x))
                                    .collect::<Option<_>>()?;
                                u32::from_str_radix(&h, 16).ok()
                            };
                            let hi = hex(&mut chars)?;
                            let cp = if (0xD800..0xDC00).contains(&hi) {
                                let rest = chars.as_str();
                                if !rest.starts_with("\\u") {
                                    return None;
                                }
                                chars.next();
                                chars.next();
                                let lo = hex(&mut chars)?;
                                0x10000 + ((hi - 0xD800) << 10) + (lo.checked_sub(0xDC00)?)
                            } else {
                                hi
                            };
                            out.push(char::from_u32(cp)?);
                        }
                        other => out.push(other),
                    }
                }
                c => out.push(c),
            }
        }
        None
    }
}

fn write_str(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn write(out: &mut String, v: &J) {
    match v {
        J::Null => out.push_str("null"),
        J::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        J::Num(n) => out.push_str(n),
        J::Str(s) => write_str(out, s),
        J::Arr(a) => {
            out.push('[');
            for (i, x) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write(out, x);
            }
            out.push(']');
        }
        J::Obj(o) => {
            out.push('{');
            for (i, (k, x)) in o.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_str(out, k);
                out.push(':');
                write(out, x);
            }
            out.push('}');
        }
    }
}

/// `None` for text that isn't a single JSON document.
pub fn canon(s: &str) -> Option<String> {
    let mut p = P { s, i: 0, depth: 0 };
    let v = p.value()?;
    p.ws();
    if p.i != s.len() {
        return None;
    }
    let mut out = String::with_capacity(s.len());
    write(&mut out, &v);
    Some(out)
}

pub struct JsonCanon;

impl VScalar for JsonCanon {
    type State = ();

    fn invoke(
        _: &(),
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let n = input.len();
        let col = input.flat_vector(0);
        // SAFETY: column 0 is VARCHAR per the signature and holds `n` rows.
        let values = unsafe { col.as_slice_with_len::<duckdb_string_t>(n) };
        let mut out = output.flat_vector();
        for (row, v) in values.iter().enumerate() {
            if col.row_is_null(row as u64) {
                out.set_null(row);
                continue;
            }
            let text = DuckString::new(&mut { *v }).as_str().to_string();
            out.insert(row, canon_or_raw(&text).as_str());
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into()],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

#[cfg(test)]
mod tests {
    use super::canon;

    #[test]
    fn canonical_forms() {
        let a = canon(r#"{"src":"x","n":12345678901234567890.50}"#);
        let b = canon(r#"{"n": 12345678901234567890.50, "src": "x"}"#);
        assert_eq!(a, b);
        assert_eq!(
            a.as_deref(),
            Some(r#"{"n":12345678901234567890.50,"src":"x"}"#)
        );
        assert_eq!(
            canon(r#"{"a":1,"a":2}"#).as_deref(),
            Some(r#"{"a":2}"#),
            "last duplicate wins, like jsonb"
        );
        assert_eq!(
            canon(r#"["é\n", {"b":[], "a":{}}]"#).as_deref(),
            Some("[\"é\\n\",{\"a\":{},\"b\":[]}]")
        );
        assert_eq!(canon(r#""😀""#).as_deref(), Some("\"😀\""));
        assert_ne!(canon(r#"[1,2]"#), canon(r#"[2,1]"#));
        assert_eq!(canon("{"), None);
        assert_eq!(canon("1 2"), None);
    }

    #[test]
    fn deep_nesting_is_bounded() {
        let at = |n: usize| format!("{}{}", "[".repeat(n), "]".repeat(n));
        assert_eq!(
            canon(&at(super::MAX_DEPTH)).map(|c| c.len()),
            Some(2 * super::MAX_DEPTH)
        );
        assert_eq!(canon(&at(super::MAX_DEPTH + 1)), None);
        assert_eq!(canon(&at(200_000)), None, "no stack overflow");
        assert_eq!(canon(&"{\"a\":".repeat(100_000)), None);
        // In SQL the fallback is the raw text, so deep documents still compare by content.
        let (a, b) = (
            format!("{}1{}", "[".repeat(600), "]".repeat(600)),
            format!("{}2{}", "[".repeat(600), "]".repeat(600)),
        );
        assert_ne!(super::canon_or_raw(&a), super::canon_or_raw(&b));
        assert_eq!(super::canon_or_raw(&a), super::canon_or_raw(&a));
        assert!(super::canon_or_raw(&a).starts_with(super::RAW));
        assert_eq!(super::canon_or_raw("{"), "\u{1}{");
    }
}
