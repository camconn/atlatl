//! Capture-first responses and lazy decoding (07 §10, 03 §5). Decoding never throws out of the
//! pipeline: a failure yields `response.json = none` and `response.decode_error`.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use minijinja::Value;
use minijinja::value::{Enumerator, Object, ObjectRepr};
use serde::Serialize;

use crate::expr::json_value;
use crate::json::JValue;
use crate::spec::{ContentTypeMode, DecodeSpec, Format};

#[derive(Clone, Debug)]
pub struct Captured {
    pub status: u16,
    /// Lowercased names, in arrival order.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub elapsed: Duration,
    /// Redacted.
    pub url: String,
}

impl Captured {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    pub fn text(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.body)
    }

    pub fn headers_iter(&self) -> impl Iterator<Item = (&str, &str)> + Clone {
        self.headers.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// A short, redacted excerpt of the body for error messages.
    pub fn snippet(&self) -> String {
        let t = self.text();
        let t = t.trim();
        let cut: String = t.chars().take(200).collect();
        let cut = if cut.len() < t.len() {
            format!("{cut}…")
        } else {
            cut
        };
        atlatl_core::redact::redact(&cut).into_owned()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DecodeError {
    /// `empty`, `syntax`, `eof`, `utf8`, `csv`, `content_type`.
    pub kind: &'static str,
    pub message: String,
    pub offset: Option<u64>,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} error: {}", self.kind, self.message)?;
        if let Some(o) = self.offset {
            write!(f, " at byte {o}")?;
        }
        Ok(())
    }
}

fn strip_bom(b: &[u8]) -> &[u8] {
    b.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(b)
}

fn json_err(e: crate::json::ParseError, base: u64) -> DecodeError {
    DecodeError {
        kind: e.kind,
        message: e.message,
        offset: Some(base + e.offset as u64),
    }
}

fn decode_json(body: &[u8]) -> Result<JValue, DecodeError> {
    let b = strip_bom(body);
    if b.iter().all(u8::is_ascii_whitespace) {
        return Err(DecodeError {
            kind: "empty",
            message: "empty body".into(),
            offset: Some(0),
        });
    }
    crate::json::parse(b).map_err(|e| json_err(e, 0))
}

fn decode_jsonl(body: &[u8]) -> Result<JValue, DecodeError> {
    let b = strip_bom(body);
    let mut out = Vec::new();
    let mut off = 0u64;
    for line in b.split(|c| *c == b'\n') {
        let l = line.strip_suffix(b"\r").unwrap_or(line);
        if !l.iter().all(u8::is_ascii_whitespace) {
            out.push(crate::json::parse(l).map_err(|e| json_err(e, off))?);
        }
        off += line.len() as u64 + 1;
    }
    Ok(JValue::Array(out))
}

pub(crate) fn csv_field(v: &str, nulls: &[String]) -> JValue {
    if nulls.iter().any(|n| n == v) {
        JValue::Null
    } else {
        JValue::String(v.to_owned())
    }
}

fn decode_csv(body: &[u8], nulls: &[String]) -> Result<JValue, DecodeError> {
    let mut r = csv::ReaderBuilder::new().from_reader(strip_bom(body));
    let err = |e: csv::Error| DecodeError {
        kind: if matches!(e.kind(), csv::ErrorKind::Utf8 { .. }) {
            "utf8"
        } else {
            "csv"
        },
        offset: e.position().map(|p| p.byte()),
        message: e.to_string(),
    };
    let headers = r.headers().map_err(err)?.clone();
    let mut out = Vec::new();
    for rec in r.records() {
        let rec = rec.map_err(err)?;
        out.push(JValue::Object(
            headers
                .iter()
                .zip(rec.iter())
                .map(|(k, v)| (k.to_owned(), csv_field(v, nulls)))
                .collect(),
        ));
    }
    Ok(JValue::Array(out))
}

fn decode_text(body: &[u8]) -> Result<JValue, DecodeError> {
    std::str::from_utf8(body)
        .map(|s| JValue::String(s.to_owned()))
        .map_err(|e| DecodeError {
            kind: "utf8",
            message: e.to_string(),
            offset: Some(e.valid_up_to() as u64),
        })
}

fn format_for_content_type(ct: &str) -> Option<Format> {
    let ct = ct
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    Some(match ct.as_str() {
        "application/json" | "text/json" => Format::Json,
        c if c.ends_with("+json") => Format::Json,
        "application/x-ndjson"
        | "application/jsonl"
        | "application/x-jsonlines"
        | "application/jsonlines" => Format::Jsonl,
        "text/csv" | "application/csv" => Format::Csv,
        "text/plain" => Format::Text,
        "application/octet-stream" => Format::Bytes,
        _ => return None,
    })
}

fn decode_as(f: Format, body: &[u8], spec: &DecodeSpec) -> Result<JValue, DecodeError> {
    match f {
        Format::Json => decode_json(body),
        Format::Jsonl => decode_jsonl(body),
        Format::Csv => decode_csv(body, spec.csv_nulls()),
        Format::Text => decode_text(body),
        Format::Bytes => Ok(JValue::String(crate::funcs::base64(body))),
        Format::Xml => Err(DecodeError {
            kind: "content_type",
            message: "xml decoding is not supported yet".into(),
            offset: None,
        }),
    }
}

pub fn decode(cap: &Captured, spec: &DecodeSpec) -> Result<JValue, DecodeError> {
    let declared = spec.format;
    let ct = cap.header("content-type");
    match spec.content_type {
        ContentTypeMode::Ignore => decode_as(declared, &cap.body, spec),
        ContentTypeMode::Trust => match ct.map(|c| (c, format_for_content_type(c))) {
            Some((_, Some(f))) => decode_as(f, &cap.body, spec),
            Some((c, None)) => Err(DecodeError {
                kind: "content_type",
                message: format!("unexpected content-type `{c}`"),
                offset: None,
            }),
            None => decode_as(declared, &cap.body, spec),
        },
        ContentTypeMode::Sniff => {
            let actual = ct.and_then(format_for_content_type);
            if declared == Format::Json && actual == Some(Format::Jsonl) {
                return decode_jsonl(&cap.body);
            }
            decode_as(declared, &cap.body, spec).map_err(|mut e| {
                let lead = strip_bom(&cap.body)
                    .iter()
                    .skip_while(|b| b.is_ascii_whitespace())
                    .take(15)
                    .copied()
                    .collect::<Vec<u8>>()
                    .to_ascii_lowercase();
                if lead.starts_with(b"<!doctype html") || lead.starts_with(b"<html") {
                    e.message = format!("{} (the body is HTML)", e.message);
                } else if let Some(c) = ct
                    && actual != Some(declared)
                {
                    e.message = format!("{} (content-type `{c}`)", e.message);
                } else if ct.is_none() {
                    e.message = format!("{} (no content-type)", e.message);
                }
                e
            })
        }
    }
}

/// `response` in rule conditions and field expressions.
#[derive(Debug)]
pub struct ResponseView {
    pub cap: Captured,
    spec: DecodeSpec,
    decoded: OnceLock<Result<Arc<JValue>, DecodeError>>,
    text: OnceLock<Value>,
}

impl ResponseView {
    pub fn new(cap: Captured, spec: DecodeSpec) -> Arc<Self> {
        Arc::new(Self {
            cap,
            spec,
            decoded: OnceLock::new(),
            text: OnceLock::new(),
        })
    }

    pub fn decoded(&self) -> &Result<Arc<JValue>, DecodeError> {
        self.decoded
            .get_or_init(|| decode(&self.cap, &self.spec).map(Arc::new))
    }

    pub fn decode_error(&self) -> Option<&DecodeError> {
        self.decoded().as_ref().err()
    }

    pub fn status(&self) -> u16 {
        self.cap.status
    }
}

const KEYS: &[&str] = &[
    "status",
    "headers",
    "text",
    "json",
    "decode_error",
    "elapsed_ms",
    "url",
];

impl Object for ResponseView {
    fn repr(self: &Arc<Self>) -> ObjectRepr {
        ObjectRepr::Map
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        Some(match key.as_str()? {
            "status" => Value::from(self.cap.status),
            "headers" => {
                let mut m: BTreeMap<String, String> = BTreeMap::new();
                for (k, v) in &self.cap.headers {
                    m.entry(k.clone())
                        .and_modify(|x| {
                            x.push_str(", ");
                            x.push_str(v);
                        })
                        .or_insert_with(|| v.clone());
                }
                Value::from(m)
            }
            "text" => self
                .text
                .get_or_init(|| Value::from(self.cap.text().into_owned()))
                .clone(),
            "json" => match self.decoded() {
                Ok(v) => json_value(v.clone()),
                Err(_) => Value::from(()),
            },
            "decode_error" => match self.decoded() {
                Ok(_) => Value::from(()),
                Err(e) => Value::from_pairs([
                    ("kind", Value::from(e.kind)),
                    ("message", Value::from(e.message.as_str())),
                    ("offset", e.offset.map(Value::from).unwrap_or_default()),
                ]),
            },
            "elapsed_ms" => Value::from(self.cap.elapsed.as_millis() as u64),
            "url" => Value::from(self.cap.url.as_str()),
            _ => return None,
        })
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::Str(KEYS)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! jv {
        ($($t:tt)+) => { JValue::from(serde_json::json!($($t)+)) };
    }

    pub(crate) fn cap(status: u16, ct: Option<&str>, body: &str) -> Captured {
        Captured {
            status,
            headers: ct
                .map(|c| vec![("content-type".to_owned(), c.to_owned())])
                .unwrap_or_default(),
            body: body.as_bytes().to_vec(),
            elapsed: Duration::from_millis(3),
            url: "https://x/y".into(),
        }
    }

    fn spec(format: Format, content_type: ContentTypeMode) -> DecodeSpec {
        DecodeSpec {
            format,
            content_type,
            ..Default::default()
        }
    }

    #[test]
    fn csv_null_values() {
        let body = "a,b,c\n1,,.\n\"\",\".\",x\n";
        let csv = |n: Option<&[&str]>| {
            let s = DecodeSpec {
                null_values: n.map(|n| n.iter().map(|v| (*v).to_owned()).collect()),
                ..spec(Format::Csv, ContentTypeMode::Sniff)
            };
            decode(&cap(200, None, body), &s).expect("csv")
        };
        assert_eq!(
            csv(None),
            jv!([{"a": "1", "b": null, "c": "."}, {"a": null, "b": ".", "c": "x"}])
        );
        assert_eq!(
            csv(Some(&[])),
            jv!([{"a": "1", "b": "", "c": "."}, {"a": "", "b": ".", "c": "x"}])
        );
        assert_eq!(
            csv(Some(&["", "."])),
            jv!([{"a": "1", "b": null, "c": null}, {"a": null, "b": null, "c": "x"}])
        );
    }

    #[test]
    fn json_exact_and_errors() {
        let d = |c: &Captured| decode(c, &DecodeSpec::default());
        let v = d(&cap(200, None, r#"{"a": 12345678901234567890123.4500}"#)).expect("ok");
        assert_eq!(v.to_string(), r#"{"a":12345678901234567890123.4500}"#);
        let e = d(&cap(409, None, "slow down")).expect_err("text");
        assert_eq!(e.kind, "syntax");
        assert!(e.message.contains("no content-type"), "{e}");
        let e = d(&cap(200, Some("text/html"), "<!DOCTYPE html><p>busy</p>")).expect_err("html");
        assert!(e.message.contains("HTML"), "{e}");
        let e = d(&cap(200, Some("application/json"), r#"{"a": [1, 2"#)).expect_err("eof");
        assert_eq!((e.kind, e.offset), ("eof", Some(11)));
        let e = d(&cap(200, None, "  \n")).expect_err("empty");
        assert_eq!(e.kind, "empty");
        let e = d(&cap(200, None, "{\"a\": 1}\n{\"b\": x}")).expect_err("offset");
        assert_eq!(e.offset, Some(9));
    }

    #[test]
    fn other_formats() {
        let v = decode(
            &cap(200, None, "{\"a\":1}\n\n{\"a\":2.50}\r\n"),
            &spec(Format::Jsonl, ContentTypeMode::Sniff),
        )
        .expect("jsonl");
        assert_eq!(v.to_string(), r#"[{"a":1},{"a":2.50}]"#);
        let v = decode(
            &cap(200, None, "a,b\n1,x\n2,\"y,z\"\n"),
            &spec(Format::Csv, ContentTypeMode::Sniff),
        )
        .expect("csv");
        assert_eq!(v, jv!([{"a": "1", "b": "x"}, {"a": "2", "b": "y,z"}]));
        let e = decode(
            &cap(200, None, "a,b\n1\n"),
            &spec(Format::Csv, ContentTypeMode::Sniff),
        )
        .expect_err("ragged");
        assert_eq!(e.kind, "csv");
        let v = decode(
            &cap(200, Some("application/x-ndjson"), "{\"a\":1}\n{\"a\":2}"),
            &DecodeSpec::default(),
        )
        .expect("sniff ndjson");
        assert_eq!(v.as_array().map(Vec::len), Some(2));
        let e = decode(
            &cap(200, Some("text/html"), "{}"),
            &spec(Format::Json, ContentTypeMode::Trust),
        )
        .expect_err("trust");
        assert_eq!(e.kind, "content_type");
        let v = decode(
            &cap(200, Some("text/csv"), "a\n1\n"),
            &spec(Format::Json, ContentTypeMode::Trust),
        )
        .expect("trust csv");
        assert_eq!(v, jv!([{"a": "1"}]));
        let v = decode(
            &cap(200, Some("text/html"), "{}"),
            &spec(Format::Json, ContentTypeMode::Ignore),
        )
        .expect("ignore");
        assert_eq!(v, jv!({}));
        let e = decode(
            &Captured {
                body: vec![0xff, 0xfe],
                ..cap(200, None, "")
            },
            &spec(Format::Text, ContentTypeMode::Sniff),
        )
        .expect_err("utf8");
        assert_eq!(e.kind, "utf8");
    }

    #[test]
    fn view_exposes_lazy_fields() {
        let e = crate::expr::Engine::default();
        let mut s = crate::expr::Scope::new();
        let mut c = cap(409, None, "Slow down, please");
        c.headers.push(("x-a".into(), "1".into()));
        c.headers.push(("x-a".into(), "2".into()));
        s.set(
            "response",
            Value::from_dyn_object(ResponseView::new(c, DecodeSpec::default())),
        );
        let j = |x| e.eval_json(x, &s).expect(x);
        assert_eq!(j("response.status"), jv!(409));
        assert_eq!(j("response.json"), JValue::Null);
        assert_eq!(j("response.decode_error.kind"), jv!("syntax"));
        assert_eq!(j("'Slow down' in response.text"), jv!(true));
        assert_eq!(j("response.headers['x-a']"), jv!("1, 2"));
        assert_eq!(j("response.json.a.b"), JValue::Null);
    }
}
