//! Row-level decode failures and replayable request descriptors (03 §6.1, §7).
//!
//! A descriptor is enough to re-issue a request on a later run through the same auth, limiter and
//! rules. Secret values that templating put into the URL, headers or body are sealed as
//! `{{secrets.<name>}}` markers, so a descriptor never carries a credential.

use atlatl_core::AtlatlError;
use serde::Serialize;

use crate::json::{JValue, Map};
use crate::request::{Prepared, PreparedBody};
use crate::response::Captured;
use crate::spec::{ContentTypeMode, DecodeSpec, Format, Method};

/// Longest `raw` excerpt kept with a reject.
pub const RAW_EXCERPT: usize = 2000;

/// A row (or whole response) that still failed to decode after its refetches.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RowReject {
    /// Replayable, secret-free request descriptor (see [`descriptor`]); `row` is set within it.
    pub request: JValue,
    /// The row index (line for JSONL, record for CSV); `None` for a whole-body failure.
    pub row: Option<u64>,
    /// Redacted excerpt of the failing row or body.
    pub raw: String,
    pub error: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BadRow {
    pub index: Option<u64>,
    pub raw: String,
    pub error: String,
}

/// A body decoded row by row where the format allows it.
#[derive(Clone, Debug, PartialEq)]
pub enum Rows {
    /// JSONL / CSV: good rows by index, and the rows that failed.
    Rows {
        good: Vec<(u64, JValue)>,
        bad: Vec<BadRow>,
    },
    /// JSON and the other whole-body formats.
    Whole(Result<JValue, BadRow>),
}

fn excerpt(s: &str) -> String {
    let cut: String = s.chars().take(RAW_EXCERPT).collect();
    atlatl_core::redact::redact(&cut).into_owned()
}

fn strip_bom(b: &[u8]) -> &[u8] {
    b.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(b)
}

fn row_format(cap: &Captured, spec: &DecodeSpec) -> Format {
    let ct = cap.header("content-type").map(|c| {
        c.split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase()
    });
    let jsonl_ct = matches!(
        ct.as_deref(),
        Some(
            "application/x-ndjson"
                | "application/jsonl"
                | "application/x-jsonlines"
                | "application/jsonlines"
        )
    );
    match (spec.format, spec.content_type) {
        (Format::Json, ContentTypeMode::Sniff | ContentTypeMode::Trust) if jsonl_ct => {
            Format::Jsonl
        }
        (f, _) => f,
    }
}

/// Decodes a captured body leniently: JSONL lines and CSV records fail one by one.
pub fn decode_rows(cap: &Captured, spec: &DecodeSpec) -> Rows {
    match row_format(cap, spec) {
        Format::Jsonl => {
            let (mut good, mut bad) = (Vec::new(), Vec::new());
            for (i, line) in strip_bom(&cap.body).split(|c| *c == b'\n').enumerate() {
                let l = line.strip_suffix(b"\r").unwrap_or(line);
                if l.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                match crate::json::parse(l) {
                    Ok(v) => good.push((i as u64, v)),
                    Err(e) => bad.push(BadRow {
                        index: Some(i as u64),
                        raw: excerpt(&String::from_utf8_lossy(l)),
                        error: format!("{}: {}", e.kind, e.message),
                    }),
                }
            }
            Rows::Rows { good, bad }
        }
        Format::Csv => {
            let (mut good, mut bad) = (Vec::new(), Vec::new());
            let nulls = spec.csv_nulls();
            let mut r = csv::ReaderBuilder::new()
                .flexible(false)
                .from_reader(strip_bom(&cap.body));
            let headers = match r.byte_headers() {
                Ok(h) => h.clone(),
                Err(e) => {
                    return Rows::Whole(Err(BadRow {
                        index: None,
                        raw: excerpt(&cap.text()),
                        error: format!("csv: {e}"),
                    }));
                }
            };
            let heads: Vec<String> = headers
                .iter()
                .map(|h| String::from_utf8_lossy(h).into_owned())
                .collect();
            for (i, rec) in r.byte_records().enumerate() {
                let i = i as u64;
                let rec = match rec {
                    Ok(r) => r,
                    Err(e) => {
                        bad.push(BadRow {
                            index: Some(i),
                            raw: String::new(),
                            error: format!("csv: {e}"),
                        });
                        continue;
                    }
                };
                let mut m = Map::new();
                let mut err = None;
                for (k, v) in heads.iter().zip(rec.iter()) {
                    match std::str::from_utf8(v) {
                        Ok(s) => {
                            m.insert(k.clone(), crate::response::csv_field(s, nulls));
                        }
                        Err(e) => err = Some(format!("utf8: {e}")),
                    }
                }
                match err {
                    None => good.push((i, JValue::Object(m))),
                    Some(error) => bad.push(BadRow {
                        index: Some(i),
                        raw: excerpt(&String::from_utf8_lossy(rec.as_slice())),
                        error,
                    }),
                }
            }
            Rows::Rows { good, bad }
        }
        _ => Rows::Whole(crate::response::decode(cap, spec).map_err(|e| BadRow {
            index: None,
            raw: excerpt(&cap.text()),
            error: e.to_string(),
        })),
    }
}

/// Replaces secret values (and their URL-encoded forms) with `{{secrets.<name>}}`.
pub fn seal(s: &str, secrets: &[(String, String)]) -> String {
    let mut out = s.to_owned();
    let mut sorted: Vec<&(String, String)> =
        secrets.iter().filter(|(_, v)| !v.is_empty()).collect();
    sorted.sort_by_key(|(_, v)| std::cmp::Reverse(v.len()));
    for (k, v) in sorted {
        let marker = format!("{{{{secrets.{k}}}}}");
        let enc = crate::funcs::urlencode_str(v);
        out = out.replace(v.as_str(), &marker);
        if enc != *v {
            out = out.replace(&enc, &marker);
        }
    }
    out
}

/// Reverses [`seal`]. Markers go back URL-encoded inside a URL.
pub fn unseal(s: &str, secrets: &[(String, String)], url: bool) -> Result<String, AtlatlError> {
    let mut out = s.to_owned();
    for (k, v) in secrets {
        let marker = format!("{{{{secrets.{k}}}}}");
        let val = if url {
            crate::funcs::urlencode_str(v)
        } else {
            v.clone()
        };
        out = out.replace(&marker, &val);
    }
    if out.contains("{{secrets.") {
        return Err(AtlatlError::config(format!(
            "a stored reject request refers to a secret the connection no longer defines: {}",
            atlatl_core::redact::redact(s)
        )));
    }
    Ok(out)
}

/// A replayable descriptor for a prepared request (before auth is applied).
pub fn descriptor(endpoint: &str, req: &Prepared, secrets: &[(String, String)]) -> JValue {
    let mut m = Map::new();
    m.insert("kind".into(), "decode".into());
    m.insert("endpoint".into(), endpoint.into());
    m.insert("method".into(), req.method.as_str().into());
    m.insert("url".into(), seal(req.url.as_str(), secrets).into());
    m.insert(
        "headers".into(),
        JValue::Array(
            req.headers
                .iter()
                .map(|(k, v)| JValue::Array(vec![k.as_str().into(), seal(v, secrets).into()]))
                .collect(),
        ),
    );
    if let Some(b) = &req.body {
        m.insert(
            "body".into(),
            seal(&String::from_utf8_lossy(&b.bytes), secrets).into(),
        );
        m.insert("content_type".into(), b.content_type.into());
    }
    JValue::Object(m)
}

fn method(s: &str) -> Result<Method, AtlatlError> {
    Ok(match s {
        "GET" => Method::Get,
        "POST" => Method::Post,
        "PUT" => Method::Put,
        "PATCH" => Method::Patch,
        "DELETE" => Method::Delete,
        m => {
            return Err(AtlatlError::config(format!(
                "stored reject: unknown method `{m}`"
            )));
        }
    })
}

fn content_type(s: &str) -> &'static str {
    match s {
        "application/json" => "application/json",
        "application/x-www-form-urlencoded" => "application/x-www-form-urlencoded",
        "text/plain; charset=utf-8" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

/// Rebuilds the request a descriptor describes.
pub fn prepared_from(
    desc: &JValue,
    secrets: &[(String, String)],
    timeout: Option<std::time::Duration>,
) -> Result<Prepared, AtlatlError> {
    let bad = || AtlatlError::config("a stored reject has an invalid request descriptor");
    let url = desc.get("url").and_then(JValue::as_str).ok_or_else(bad)?;
    let url = reqwest::Url::parse(&unseal(url, secrets, true)?).map_err(|_| bad())?;
    let mut headers = Vec::new();
    for h in desc
        .get("headers")
        .and_then(JValue::as_array)
        .into_iter()
        .flatten()
    {
        let (Some(k), Some(v)) = (
            h.as_array()
                .and_then(|a| a.first())
                .and_then(JValue::as_str),
            h.as_array().and_then(|a| a.get(1)).and_then(JValue::as_str),
        ) else {
            return Err(bad());
        };
        headers.push((k.to_owned(), unseal(v, secrets, false)?));
    }
    let body = match desc.get("body").and_then(JValue::as_str) {
        None => None,
        Some(b) => {
            let bytes = unseal(b, secrets, false)?.into_bytes();
            let ct = content_type(
                desc.get("content_type")
                    .and_then(JValue::as_str)
                    .unwrap_or_default(),
            );
            let json = (ct == "application/json")
                .then(|| crate::json::parse(&bytes).ok())
                .flatten();
            Some(PreparedBody {
                bytes,
                content_type: ct,
                json,
            })
        }
    };
    Ok(Prepared {
        method: method(desc.get("method").and_then(JValue::as_str).unwrap_or("GET"))?,
        url,
        headers,
        body,
        timeout,
    })
}

/// A stable reject identity: 16 hex chars of sha256 over the descriptor (sorted keys) and row.
pub fn reject_key(request: &JValue, row: Option<u64>) -> String {
    let text = format!(
        "{}#{}",
        request.clone().sort_keys(),
        row.map(|r| r.to_string()).unwrap_or_default()
    );
    crate::funcs::hex(&crate::funcs::sha256(text.as_bytes()))[..16].to_owned()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn cap(ct: Option<&str>, body: &str) -> Captured {
        Captured {
            status: 200,
            headers: ct
                .map(|c| vec![("content-type".to_owned(), c.to_owned())])
                .unwrap_or_default(),
            body: body.as_bytes().to_vec(),
            elapsed: Duration::ZERO,
            url: "http://x/".into(),
        }
    }

    fn spec(format: Format) -> DecodeSpec {
        DecodeSpec {
            format,
            ..Default::default()
        }
    }

    #[test]
    fn jsonl_rows_fail_one_by_one() {
        let r = decode_rows(
            &cap(None, "{\"a\":1.10}\n{\"a\":\n\n{\"a\":3}\n"),
            &spec(Format::Jsonl),
        );
        let Rows::Rows { good, bad } = r else {
            panic!("rows")
        };
        assert_eq!(good.len(), 2);
        assert_eq!(good[0].1.to_string(), r#"{"a":1.10}"#);
        assert_eq!(good[1].0, 3);
        assert_eq!(bad.len(), 1);
        assert_eq!(bad[0].index, Some(1));
        assert_eq!(bad[0].raw, "{\"a\":");
    }

    #[test]
    fn json_with_jsonl_content_type_is_row_decoded() {
        let r = decode_rows(
            &cap(Some("application/x-ndjson"), "{\"a\":1}\nnope\n"),
            &spec(Format::Json),
        );
        assert!(matches!(r, Rows::Rows { ref bad, .. } if bad.len() == 1));
    }

    #[test]
    fn csv_rows() {
        let r = decode_rows(&cap(None, "a,b\n1,2\n3\n4,5\n"), &spec(Format::Csv));
        let Rows::Rows { good, bad } = r else {
            panic!("rows")
        };
        assert_eq!(good.len(), 2);
        assert_eq!(bad.len(), 1);
        assert_eq!(bad[0].index, Some(1));
    }

    #[test]
    fn csv_rows_null_values() {
        let body = "a,b\n,x\nNA,\n";
        let rows = |s: &DecodeSpec| {
            let Rows::Rows { good, bad } = decode_rows(&cap(None, body), s) else {
                panic!("rows")
            };
            assert!(bad.is_empty());
            good.into_iter()
                .map(|(_, v)| v.to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            rows(&spec(Format::Csv)),
            [r#"{"a":null,"b":"x"}"#, r#"{"a":"NA","b":null}"#]
        );
        let s = DecodeSpec {
            null_values: Some(vec!["NA".into()]),
            ..spec(Format::Csv)
        };
        assert_eq!(rows(&s), [r#"{"a":"","b":"x"}"#, r#"{"a":null,"b":""}"#]);
    }

    #[test]
    fn whole_body_failure() {
        let r = decode_rows(&cap(None, "<html>busy</html>"), &spec(Format::Json));
        assert!(matches!(r, Rows::Whole(Err(BadRow { index: None, .. }))));
    }

    #[test]
    fn seal_roundtrip_hides_secrets() {
        let secrets = vec![("api_key".to_owned(), "s3cr3t/+=".to_owned())];
        let url = format!(
            "https://x/y?c={}&d=1",
            crate::funcs::urlencode_str("s3cr3t/+=")
        );
        let req = Prepared {
            method: Method::Post,
            url: reqwest::Url::parse(&url).expect("url"),
            headers: vec![("X-Key".into(), "s3cr3t/+=".into())],
            body: Some(PreparedBody {
                bytes: br#"{"k":"s3cr3t/+="}"#.to_vec(),
                content_type: "application/json",
                json: None,
            }),
            timeout: None,
        };
        let d = descriptor("ep", &req, &secrets);
        assert!(!d.to_string().contains("s3cr3t"), "{d}");
        let back = prepared_from(&d, &secrets, None).expect("back");
        assert_eq!(back.url, req.url);
        assert_eq!(back.headers, req.headers);
        assert_eq!(
            back.body.as_ref().map(|b| b.bytes.clone()),
            Some(req.body.as_ref().expect("b").bytes.clone())
        );
        assert!(prepared_from(&d, &[], None).is_err());
        assert_eq!(reject_key(&d, Some(1)), reject_key(&d.clone(), Some(1)));
        assert_ne!(reject_key(&d, Some(1)), reject_key(&d, Some(2)));
    }
}
