//! JSON with exact numbers. Numbers keep their source text, so a vendor's `1.10` or a 30-digit id
//! reaches staging unchanged. (`serde_json/arbitrary_precision` would do this too, but features
//! unify across the workspace and it breaks serde's internally tagged enums elsewhere.)

use std::fmt::{self, Write as _};

use indexmap::IndexMap;
use serde::Serialize;

pub type Map = IndexMap<String, JValue>;

/// A JSON number as written. Always valid JSON number syntax.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Num(Box<str>);

impl Num {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_i64(&self) -> Option<i64> {
        self.0.parse().ok()
    }

    pub fn as_u64(&self) -> Option<u64> {
        self.0.parse().ok()
    }

    pub fn as_i128(&self) -> Option<i128> {
        self.0.parse().ok()
    }

    pub fn as_f64(&self) -> f64 {
        self.0.parse().unwrap_or(f64::NAN)
    }

    /// `None` for NaN and infinities, which JSON can't represent.
    pub fn from_f64(f: f64) -> Option<Self> {
        serde_json::Number::from_f64(f).map(|n| Num(n.to_string().into()))
    }

    /// Validates `s` as a JSON number.
    pub fn parse(s: &str) -> Option<Self> {
        let b = s.as_bytes();
        let mut p = Parser { b, i: 0, depth: 0 };
        (p.number().is_ok() && p.i == b.len()).then(|| Num(s.into()))
    }
}

impl From<i64> for Num {
    fn from(v: i64) -> Self {
        Num(v.to_string().into())
    }
}

impl From<u64> for Num {
    fn from(v: u64) -> Self {
        Num(v.to_string().into())
    }
}

impl From<i128> for Num {
    fn from(v: i128) -> Self {
        Num(v.to_string().into())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub enum JValue {
    #[default]
    Null,
    Bool(bool),
    Number(Num),
    String(String),
    Array(Vec<JValue>),
    Object(Map),
}

impl JValue {
    pub fn is_null(&self) -> bool {
        matches!(self, JValue::Null)
    }

    pub fn is_object(&self) -> bool {
        matches!(self, JValue::Object(_))
    }

    pub fn is_array(&self) -> bool {
        matches!(self, JValue::Array(_))
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            JValue::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&Vec<JValue>> {
        match self {
            JValue::Array(a) => Some(a),
            _ => None,
        }
    }

    pub fn as_object(&self) -> Option<&Map> {
        match self {
            JValue::Object(m) => Some(m),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            JValue::Number(n) => n.as_u64(),
            _ => None,
        }
    }

    pub fn get(&self, key: &str) -> Option<&JValue> {
        self.as_object()?.get(key)
    }

    pub fn kind(&self) -> &'static str {
        match self {
            JValue::Null => "null",
            JValue::Bool(_) => "boolean",
            JValue::Number(_) => "number",
            JValue::String(_) => "string",
            JValue::Array(_) => "array",
            JValue::Object(_) => "object",
        }
    }

    pub fn object(pairs: impl IntoIterator<Item = (String, JValue)>) -> Self {
        JValue::Object(pairs.into_iter().collect())
    }

    /// A `serde_json` copy. A number stays one when `serde_json` holds its value exactly (`1.50`
    /// as 1.5, `1E2` as 100.0); one it can't (past 64 bits, more digits than an f64 keeps, `1e400`)
    /// becomes a string of the exact text rather than a rounded or `null` value.
    pub fn to_serde(&self) -> serde_json::Value {
        match self {
            JValue::Null => serde_json::Value::Null,
            JValue::Bool(b) => (*b).into(),
            JValue::Number(n) => match exact_number(n, false) {
                Some(v) => serde_json::Value::Number(v),
                None => serde_json::Value::String(n.as_str().to_owned()),
            },
            JValue::String(s) => s.clone().into(),
            JValue::Array(a) => serde_json::Value::Array(a.iter().map(JValue::to_serde).collect()),
            JValue::Object(m) => serde_json::Value::Object(
                m.iter().map(|(k, v)| (k.clone(), v.to_serde())).collect(),
            ),
        }
    }

    /// A `serde_json` copy to persist and read back with [`JValue::from_serde_exact`]: a number
    /// `serde_json` wouldn't write back digit for digit is kept as `{"$number": "<text>"}`, so it
    /// returns as the same number with the same text, never a rounded one or a string. Keys are
    /// written as is; only a real object shaped like a marker (one key `"$number"` holding a
    /// numeric string, or one key `"$object"` holding an object) is wrapped as
    /// `{"$object": <object>}`.
    pub fn to_serde_exact(&self) -> serde_json::Value {
        match self {
            JValue::Number(n) => match exact_number(n, true) {
                Some(v) => serde_json::Value::Number(v),
                None => serde_json::json!({ EXACT_NUMBER: n.as_str() }),
            },
            JValue::Array(a) => {
                serde_json::Value::Array(a.iter().map(JValue::to_serde_exact).collect())
            }
            JValue::Object(m) => {
                let out: serde_json::Map<_, _> = m
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_serde_exact()))
                    .collect();
                if numeric_tag(&out).is_some() || literal_object(&out).is_some() {
                    serde_json::json!({ EXACT_OBJECT: out })
                } else {
                    serde_json::Value::Object(out)
                }
            }
            other => other.to_serde(),
        }
    }

    /// What [`JValue::to_serde_exact`] wrote; anything else as [`From`] reads it. A one-key
    /// `{"$number": <numeric string>}` is a number, and a one-key `{"$object": {...}}` is its
    /// inner object taken literally (keys as is, values read on). Rows saved before this encoding
    /// read back unchanged, except such a lone `$number` (a number) or `$object` (its inner
    /// object).
    pub fn from_serde_exact(v: &serde_json::Value) -> Self {
        match v {
            serde_json::Value::Object(m) => {
                if let Some(t) = numeric_tag(m) {
                    return JValue::Number(Num(t.into()));
                }
                JValue::Object(
                    literal_object(m)
                        .unwrap_or(m)
                        .iter()
                        .map(|(k, v)| (k.clone(), JValue::from_serde_exact(v)))
                        .collect(),
                )
            }
            serde_json::Value::Array(a) => {
                JValue::Array(a.iter().map(JValue::from_serde_exact).collect())
            }
            other => other.into(),
        }
    }

    pub fn sort_keys(self) -> Self {
        match self {
            JValue::Object(m) => {
                let mut e: Vec<_> = m.into_iter().collect();
                e.sort_by(|a, b| a.0.cmp(&b.0));
                JValue::Object(e.into_iter().map(|(k, v)| (k, v.sort_keys())).collect())
            }
            JValue::Array(a) => JValue::Array(a.into_iter().map(JValue::sort_keys).collect()),
            other => other,
        }
    }

    pub fn to_pretty(&self) -> String {
        let mut s = String::new();
        self.write(&mut s, Some(0)).expect("string write");
        s
    }

    fn write(&self, out: &mut String, indent: Option<usize>) -> fmt::Result {
        let nl = |out: &mut String, d: usize| {
            if indent.is_some() {
                out.push('\n');
                out.push_str(&"  ".repeat(d));
            }
        };
        match self {
            JValue::Null => out.push_str("null"),
            JValue::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            JValue::Number(n) => out.push_str(n.as_str()),
            JValue::String(s) => write_str(out, s),
            JValue::Array(a) => {
                out.push('[');
                for (i, v) in a.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    nl(out, indent.map_or(0, |d| d + 1));
                    v.write(out, indent.map(|d| d + 1))?;
                }
                if !a.is_empty() {
                    nl(out, indent.unwrap_or(0));
                }
                out.push(']');
            }
            JValue::Object(m) => {
                out.push('{');
                for (i, (k, v)) in m.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    nl(out, indent.map_or(0, |d| d + 1));
                    write_str(out, k);
                    out.push(':');
                    if indent.is_some() {
                        out.push(' ');
                    }
                    v.write(out, indent.map(|d| d + 1))?;
                }
                if !m.is_empty() {
                    nl(out, indent.unwrap_or(0));
                }
                out.push('}');
            }
        }
        Ok(())
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
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

static NULL: JValue = JValue::Null;

/// `v["key"]`; `null` when absent or not an object.
impl std::ops::Index<&str> for JValue {
    type Output = JValue;
    fn index(&self, k: &str) -> &JValue {
        self.get(k).unwrap_or(&NULL)
    }
}

/// Compact JSON.
impl fmt::Display for JValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::new();
        self.write(&mut s, None)?;
        f.write_str(&s)
    }
}

/// Serializes exactly with `serde_json` (numbers go through `RawValue`).
impl Serialize for JValue {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{Error, SerializeMap, SerializeSeq};
        match self {
            JValue::Null => s.serialize_unit(),
            JValue::Bool(b) => s.serialize_bool(*b),
            JValue::Number(n) => serde_json::value::RawValue::from_string(n.as_str().to_owned())
                .map_err(S::Error::custom)?
                .serialize(s),
            JValue::String(x) => s.serialize_str(x),
            JValue::Array(a) => {
                let mut q = s.serialize_seq(Some(a.len()))?;
                for v in a {
                    q.serialize_element(v)?;
                }
                q.end()
            }
            JValue::Object(m) => {
                let mut q = s.serialize_map(Some(m.len()))?;
                for (k, v) in m {
                    q.serialize_entry(k, v)?;
                }
                q.end()
            }
        }
    }
}

impl From<&serde_json::Value> for JValue {
    fn from(v: &serde_json::Value) -> Self {
        match v {
            serde_json::Value::Null => JValue::Null,
            serde_json::Value::Bool(b) => JValue::Bool(*b),
            serde_json::Value::Number(n) => JValue::Number(Num(n.to_string().into())),
            serde_json::Value::String(s) => JValue::String(s.clone()),
            serde_json::Value::Array(a) => JValue::Array(a.iter().map(Into::into).collect()),
            serde_json::Value::Object(m) => {
                JValue::Object(m.iter().map(|(k, v)| (k.clone(), v.into())).collect())
            }
        }
    }
}

impl From<serde_json::Value> for JValue {
    fn from(v: serde_json::Value) -> Self {
        (&v).into()
    }
}

impl From<&str> for JValue {
    fn from(s: &str) -> Self {
        JValue::String(s.to_owned())
    }
}

impl From<String> for JValue {
    fn from(s: String) -> Self {
        JValue::String(s)
    }
}

impl From<u64> for JValue {
    fn from(v: u64) -> Self {
        JValue::Number(v.into())
    }
}

impl From<i64> for JValue {
    fn from(v: i64) -> Self {
        JValue::Number(v.into())
    }
}

impl From<u16> for JValue {
    fn from(v: u16) -> Self {
        JValue::Number(u64::from(v).into())
    }
}

impl From<bool> for JValue {
    fn from(v: bool) -> Self {
        JValue::Bool(v)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParseError {
    /// `syntax`, `eof` or `utf8`.
    pub kind: &'static str,
    pub message: String,
    pub offset: usize,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at byte {}", self.message, self.offset)
    }
}

const MAX_DEPTH: usize = 512;

struct Parser<'a> {
    b: &'a [u8],
    i: usize,
    depth: usize,
}

impl Parser<'_> {
    fn err<T>(&self, message: impl Into<String>) -> Result<T, ParseError> {
        Err(ParseError {
            kind: if self.i >= self.b.len() {
                "eof"
            } else {
                "syntax"
            },
            message: message.into(),
            offset: self.i,
        })
    }

    fn ws(&mut self) {
        while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.b.get(self.i).copied()
    }

    fn expect_lit(&mut self, lit: &[u8], v: JValue) -> Result<JValue, ParseError> {
        if self.b[self.i..].starts_with(lit) {
            self.i += lit.len();
            Ok(v)
        } else if lit.starts_with(&self.b[self.i..]) {
            self.i = self.b.len();
            self.err("unexpected end of input")
        } else {
            self.err("invalid literal")
        }
    }

    fn value(&mut self) -> Result<JValue, ParseError> {
        self.ws();
        match self.peek() {
            None => self.err("unexpected end of input"),
            Some(b'n') => self.expect_lit(b"null", JValue::Null),
            Some(b't') => self.expect_lit(b"true", JValue::Bool(true)),
            Some(b'f') => self.expect_lit(b"false", JValue::Bool(false)),
            Some(b'"') => self.string().map(JValue::String),
            Some(b'-' | b'0'..=b'9') => {
                let start = self.i;
                self.number()?;
                let text = std::str::from_utf8(&self.b[start..self.i]).expect("ascii");
                Ok(JValue::Number(Num(text.into())))
            }
            Some(b'[') => {
                self.nest()?;
                self.i += 1;
                let mut a = Vec::new();
                self.ws();
                if self.peek() == Some(b']') {
                    self.i += 1;
                    self.depth -= 1;
                    return Ok(JValue::Array(a));
                }
                loop {
                    a.push(self.value()?);
                    self.ws();
                    match self.peek() {
                        Some(b',') => self.i += 1,
                        Some(b']') => {
                            self.i += 1;
                            self.depth -= 1;
                            return Ok(JValue::Array(a));
                        }
                        None => return self.err("unexpected end of input in array"),
                        _ => return self.err("expected `,` or `]`"),
                    }
                }
            }
            Some(b'{') => {
                self.nest()?;
                self.i += 1;
                let mut m = Map::new();
                self.ws();
                if self.peek() == Some(b'}') {
                    self.i += 1;
                    self.depth -= 1;
                    return Ok(JValue::Object(m));
                }
                loop {
                    self.ws();
                    if self.peek() != Some(b'"') {
                        return self.err("expected a string key");
                    }
                    let k = self.string()?;
                    self.ws();
                    if self.peek() != Some(b':') {
                        return self.err("expected `:`");
                    }
                    self.i += 1;
                    let v = self.value()?;
                    m.insert(k, v);
                    self.ws();
                    match self.peek() {
                        Some(b',') => self.i += 1,
                        Some(b'}') => {
                            self.i += 1;
                            self.depth -= 1;
                            return Ok(JValue::Object(m));
                        }
                        None => return self.err("unexpected end of input in object"),
                        _ => return self.err("expected `,` or `}`"),
                    }
                }
            }
            Some(_) => self.err("expected a JSON value"),
        }
    }

    fn nest(&mut self) -> Result<(), ParseError> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return self.err("nesting too deep");
        }
        Ok(())
    }

    fn digits(&mut self) -> usize {
        let s = self.i;
        while self.peek().is_some_and(|c| c.is_ascii_digit()) {
            self.i += 1;
        }
        self.i - s
    }

    fn number(&mut self) -> Result<(), ParseError> {
        if self.peek() == Some(b'-') {
            self.i += 1;
        }
        match self.peek() {
            Some(b'0') => self.i += 1,
            Some(b'1'..=b'9') => {
                self.digits();
            }
            _ => return self.err("invalid number"),
        }
        if self.peek() == Some(b'.') {
            self.i += 1;
            if self.digits() == 0 {
                return self.err("invalid number: expected digits after `.`");
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.i += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.i += 1;
            }
            if self.digits() == 0 {
                return self.err("invalid number: expected exponent digits");
            }
        }
        Ok(())
    }

    fn hex4(&mut self) -> Result<u32, ParseError> {
        let Some(h) = self.b.get(self.i..self.i + 4) else {
            self.i = self.b.len();
            return self.err("unexpected end of input in escape");
        };
        let v = std::str::from_utf8(h)
            .ok()
            .and_then(|h| u32::from_str_radix(h, 16).ok());
        match v {
            Some(v) => {
                self.i += 4;
                Ok(v)
            }
            None => self.err("invalid \\u escape"),
        }
    }

    fn string(&mut self) -> Result<String, ParseError> {
        self.i += 1;
        let mut out = String::new();
        loop {
            let start = self.i;
            while self
                .peek()
                .is_some_and(|c| c != b'"' && c != b'\\' && c >= 0x20)
            {
                self.i += 1;
            }
            out.push_str(std::str::from_utf8(&self.b[start..self.i]).expect("validated utf-8"));
            match self.peek() {
                None => return self.err("unexpected end of input in string"),
                Some(b'"') => {
                    self.i += 1;
                    return Ok(out);
                }
                Some(b'\\') => {
                    self.i += 1;
                    let Some(c) = self.peek() else {
                        return self.err("unexpected end of input in string");
                    };
                    self.i += 1;
                    match c {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{08}'),
                        b'f' => out.push('\u{0c}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let hi = self.hex4()?;
                            let cp = if (0xD800..0xDC00).contains(&hi)
                                && self.b[self.i..].starts_with(b"\\u")
                            {
                                self.i += 2;
                                let lo = self.hex4()?;
                                if !(0xDC00..0xE000).contains(&lo) {
                                    return self.err("invalid surrogate pair");
                                }
                                0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00)
                            } else {
                                hi
                            };
                            out.push(char::from_u32(cp).unwrap_or('\u{FFFD}'));
                        }
                        _ => {
                            self.i -= 1;
                            return self.err("invalid escape");
                        }
                    }
                }
                Some(_) => return self.err("control character in string"),
            }
        }
    }
}

/// Parses one JSON document (a leading BOM and surrounding whitespace are allowed).
pub fn parse(bytes: &[u8]) -> Result<JValue, ParseError> {
    let b = bytes.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(bytes);
    if let Err(e) = std::str::from_utf8(b) {
        return Err(ParseError {
            kind: "utf8",
            message: "invalid UTF-8".into(),
            offset: e.valid_up_to(),
        });
    }
    let mut p = Parser { b, i: 0, depth: 0 };
    let v = p.value()?;
    p.ws();
    if p.i < b.len() {
        return p.err("trailing characters after the JSON value");
    }
    Ok(v)
}

const EXACT_NUMBER: &str = "$number";
const EXACT_OBJECT: &str = "$object";

/// The text of a one-key `{"$number": <numeric string>}`.
fn numeric_tag(m: &serde_json::Map<String, serde_json::Value>) -> Option<&str> {
    match m.get(EXACT_NUMBER) {
        Some(serde_json::Value::String(t)) if m.len() == 1 && decimal_digits(t).is_some() => {
            Some(t)
        }
        _ => None,
    }
}

/// The inner object of a one-key `{"$object": {...}}`.
fn literal_object(
    m: &serde_json::Map<String, serde_json::Value>,
) -> Option<&serde_json::Map<String, serde_json::Value>> {
    match m.get(EXACT_OBJECT) {
        Some(serde_json::Value::Object(o)) if m.len() == 1 => Some(o),
        _ => None,
    }
}

/// `n` as a `serde_json` number when it holds the same value, or with `digits` the same digits
/// too (`1.50` isn't `1.5`).
fn exact_number(n: &Num, digits: bool) -> Option<serde_json::Number> {
    let text = n.as_str();
    n.as_i64()
        .map(serde_json::Number::from)
        .or_else(|| n.as_u64().map(serde_json::Number::from))
        .or_else(|| serde_json::Number::from_f64(n.as_f64()))
        .filter(|v| {
            let back = v.to_string();
            if digits {
                decimal_digits(&back) == decimal_digits(text)
            } else {
                cmp_numbers(&back, text) == std::cmp::Ordering::Equal
            }
        })
}

/// A JSON number's sign, digits (trailing zeros kept, so `1.10` isn't `1.1`) and the power of ten
/// they're scaled by: how its text reads, whatever the exponent's spelling. `None` for text that
/// isn't a number.
fn decimal_digits(text: &str) -> Option<(bool, String, i64)> {
    let (neg, t) = match text.strip_prefix('-') {
        Some(t) => (true, t),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (mantissa, exp) = match t.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.strip_prefix('+').unwrap_or(e).parse::<i64>().ok()?),
        None => (t, 0),
    };
    let (int, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if int.is_empty() && frac.is_empty()
        || !int.chars().chain(frac.chars()).all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{int}{frac}");
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Some((false, "0".into(), 0));
    }
    Some((neg, digits.to_owned(), exp.checked_sub(frac.len() as i64)?))
}

/// Two JSON numbers in exact numeric order, from their text (`1.0` and `1` are equal); text that
/// isn't a number sorts as text.
pub fn cmp_numbers(a: &str, b: &str) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let (Some(x), Some(y)) = (decimal_digits(a), decimal_digits(b)) else {
        return a.cmp(b);
    };
    // Trailing zeros moved into the exponent, so equal values have equal digits.
    let norm = |(neg, d, e): (bool, String, i64)| {
        let t = d.trim_end_matches('0');
        if t.is_empty() {
            return (0i8, String::new(), 0i128);
        }
        let e = e as i128 + (d.len() - t.len()) as i128;
        (if neg { -1 } else { 1 }, t.to_owned(), e + t.len() as i128)
    };
    let ((sx, dx, mx), (sy, dy, my)) = (norm(x), norm(y));
    let mag = || mx.cmp(&my).then_with(|| dx.cmp(&dy));
    match sx.cmp(&sy) {
        Ordering::Equal if sx == 0 => Ordering::Equal,
        Ordering::Equal if sx > 0 => mag(),
        Ordering::Equal => mag().reverse(),
        o => o,
    }
}

pub fn parse_str(s: &str) -> Result<JValue, ParseError> {
    parse(s.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt(s: &str) -> String {
        parse_str(s).expect(s).to_string()
    }

    #[test]
    fn serde_copies_keep_number_values() {
        let v = parse_str(
            r#"[1, -9223372036854775808, 18446744073709551615, 1.5, 0.1, 1e+300, 1.0, -0.0, 1.50, 1E2, 1700000000.100,
                170141183460469231731687303715884105727, 1.10, 12345678901234567890.123, 1e400, 2E-7]"#,
        )
        .expect("parse");
        assert_eq!(
            v.to_serde(),
            serde_json::json!([
                1,
                -9223372036854775808i64,
                18446744073709551615u64,
                1.5,
                0.1,
                1e300,
                1.0,
                -0.0,
                1.5,
                100.0,
                1700000000.1,
                "170141183460469231731687303715884105727",
                1.1,
                "12345678901234567890.123",
                "1e400",
                2e-7
            ])
        );
    }

    fn exact_round_trip(v: &JValue) -> JValue {
        let text = serde_json::to_string(&v.to_serde_exact()).expect("text");
        let back = JValue::from_serde_exact(&serde_json::from_str(&text).expect("json"));
        assert_eq!(&back, v);
        assert_eq!(back.to_string(), v.to_string());
        back
    }

    #[test]
    fn exact_serde_copies_round_trip() {
        let v = parse_str(
            r#"{"cursor": 9007199254740995.0, "id": 170141183460469231731687303715884105727, "n": 5, "s": "9007199254740995.0", "l": [1.10, {"$number": "x"}]}"#,
        )
        .expect("parse");
        assert_eq!(
            v.to_serde_exact(),
            serde_json::json!({
                "cursor": {"$number": "9007199254740995.0"},
                "id": {"$number": "170141183460469231731687303715884105727"},
                "n": 5,
                "s": "9007199254740995.0",
                "l": [{"$number": "1.10"}, {"$number": "x"}]
            })
        );
        exact_round_trip(&v);
    }

    #[test]
    fn exact_serde_wraps_only_marker_shaped_objects() {
        for (src, want) in [
            (
                r#"{"$number": "123"}"#,
                serde_json::json!({"$object": {"$number": "123"}}),
            ),
            (
                r#"{"$number": "-1.5e3"}"#,
                serde_json::json!({"$object": {"$number": "-1.5e3"}}),
            ),
            (
                r#"{"$object": {"a": 1}}"#,
                serde_json::json!({"$object": {"$object": {"a": 1}}}),
            ),
            (
                r#"{"$object": {"$object": {}}}"#,
                serde_json::json!({"$object": {"$object": {"$object": {"$object": {}}}}}),
            ),
            (
                r#"{"$object": {"$number": "1"}}"#,
                serde_json::json!({"$object": {"$object": {"$object": {"$number": "1"}}}}),
            ),
            (
                r#"{"$number": "7", "$object": true}"#,
                serde_json::json!({"$number": "7", "$object": true}),
            ),
            (
                r#"{"$number": "7", "$object": false}"#,
                serde_json::json!({"$number": "7", "$object": false}),
            ),
            (r#"{"$object": 1}"#, serde_json::json!({"$object": 1})),
            (
                r#"{"$object": {}, "k": 1}"#,
                serde_json::json!({"$object": {}, "k": 1}),
            ),
            (r#"{"$number": 5}"#, serde_json::json!({"$number": 5})),
            (r#"{"$number": "x"}"#, serde_json::json!({"$number": "x"})),
            (
                r#"{"$number": "1", "k": 2}"#,
                serde_json::json!({"$number": "1", "k": 2}),
            ),
            (
                r#"{"$number": 9007199254740995.0}"#,
                serde_json::json!({"$number": {"$number": "9007199254740995.0"}}),
            ),
            (
                r#"{"$x": 1, "$$y": [{"$number": "1"}]}"#,
                serde_json::json!({"$x": 1, "$$y": [{"$object": {"$number": "1"}}]}),
            ),
            (
                r#"{"$$ref": "/a", "$": "$$"}"#,
                serde_json::json!({"$$ref": "/a", "$": "$$"}),
            ),
            (
                r#"[[{"$number": "9"}, 9007199254740995.0, {"$$ref": 1}, {"$object": {"k": {"$number": "2"}}}]]"#,
                serde_json::json!([[
                    {"$object": {"$number": "9"}},
                    {"$number": "9007199254740995.0"},
                    {"$$ref": 1},
                    {"$object": {"$object": {"k": {"$object": {"$number": "2"}}}}}
                ]]),
            ),
        ] {
            let v = parse_str(src).expect(src);
            assert_eq!(v.to_serde_exact(), want, "{src}");
            exact_round_trip(&v);
        }
    }

    #[test]
    fn exact_serde_reads_legacy_rows() {
        let old = serde_json::json!({
            "$x": 1,
            "$$ref": "/defs",
            "a": [{"$number": "x"}, {"$y": {"$number": "12"}}, {"$$ref": {"$$$z": 2}}],
            "o": [{"$object": 1}, {"$object": {"$number": "3"}}, {"$object": {}, "k": 1}]
        });
        assert_eq!(
            JValue::from_serde_exact(&old),
            parse_str(
                r#"{"$x": 1, "$$ref": "/defs", "a": [{"$number": "x"}, {"$y": 12}, {"$$ref": {"$$$z": 2}}],
                    "o": [{"$object": 1}, {"$number": "3"}, {"$object": {}, "k": 1}]}"#
            )
            .expect("parse")
        );
    }

    #[test]
    fn numbers_compare_exactly() {
        use std::cmp::Ordering::{Equal, Greater, Less};
        for (a, b, want) in [
            ("9007199254740993.0", "9007199254740992.0", Greater),
            ("9007199254740993", "9007199254740992", Greater),
            (
                "340282366920938463463374607431768211457",
                "340282366920938463463374607431768211456",
                Greater,
            ),
            ("1.0", "1", Equal),
            ("1e3", "1000.00", Equal),
            ("-0.0", "0", Equal),
            ("0.1", "0.10000000000000001", Less),
            ("-2", "-10", Greater),
            ("-1e-400", "0", Less),
            ("1e400", "9e399", Greater),
            ("12.5", "1.25e1", Equal),
        ] {
            assert_eq!(cmp_numbers(a, b), want, "{a} vs {b}");
            assert_eq!(cmp_numbers(b, a), want.reverse(), "{b} vs {a}");
        }
    }

    #[test]
    fn round_trips_exactly() {
        assert_eq!(
            rt(
                r#" {"a": 1.10, "b": [123456789012345678901234567890, -0.0, 1e400, 2E-7], "c": null} "#
            ),
            r#"{"a":1.10,"b":[123456789012345678901234567890,-0.0,1e400,2E-7],"c":null}"#
        );
        assert_eq!(
            rt(r#""tab\t \"q\" é 😀 \/""#),
            "\"tab\\t \\\"q\\\" é 😀 /\""
        );
        assert_eq!(rt("\u{feff}[true, false]"), "[true,false]");
        assert_eq!(rt(r#"{"z": 1, "a": 2, "z": 3}"#), r#"{"z":3,"a":2}"#);
        let v = parse_str(r#"{"k": [1, {"x": "y"}]}"#).expect("v");
        assert_eq!(
            v.to_pretty(),
            "{\n  \"k\": [\n    1,\n    {\n      \"x\": \"y\"\n    }\n  ]\n}"
        );
        assert_eq!(serde_json::to_string(&v).expect("ser"), v.to_string());
        let big = parse_str("[1.230, 99999999999999999999]").expect("big");
        assert_eq!(
            serde_json::to_string(&big).expect("ser"),
            "[1.230,99999999999999999999]"
        );
    }

    #[test]
    fn errors_have_kinds_and_offsets() {
        let e = |s: &str| {
            let e = parse_str(s).expect_err(s);
            (e.kind, e.offset)
        };
        assert_eq!(e(r#"{"a": [1, 2"#), ("eof", 11));
        assert_eq!(e("slow down"), ("syntax", 0));
        assert_eq!(e(r#"{"a": 01}"#), ("syntax", 7));
        assert_eq!(e("[1,]"), ("syntax", 3));
        assert_eq!(e(r#"{"a" 1}"#), ("syntax", 5));
        assert_eq!(e("tru"), ("eof", 3));
        assert_eq!(e("1 2"), ("syntax", 2));
        assert_eq!(e("\"a\u{1}\""), ("syntax", 2));
        assert_eq!(e(""), ("eof", 0));
        assert_eq!(parse(b"[\"\xff\"]").expect_err("utf8").kind, "utf8");
        assert_eq!(e(&"[".repeat(600)), ("syntax", 512));
        assert!(
            Num::parse("1.5e3").is_some()
                && Num::parse("1.").is_none()
                && Num::parse("+1").is_none()
        );
    }
}
