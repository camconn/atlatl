//! AWS Signature Version 4, header form.

use chrono::{DateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use sha2::{Digest, Sha256};

use crate::creds::Credentials;

/// RFC 3986 unreserved characters stay; everything else is percent-encoded.
const UNRESERVED: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

pub fn encode(s: &str) -> String {
    utf8_percent_encode(s, UNRESERVED).to_string()
}

/// Encodes each path segment, keeping `/`.
pub fn encode_path(p: &str) -> String {
    p.split('/').map(encode).collect::<Vec<_>>().join("/")
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

pub fn sha256_hex(b: &[u8]) -> String {
    hex(&Sha256::digest(b))
}

fn hmac(key: &[u8], msg: &[u8]) -> Vec<u8> {
    #[allow(clippy::expect_used)]
    let mut m = <Hmac<Sha256>>::new_from_slice(key).expect("hmac accepts any key length");
    m.update(msg);
    m.finalize().into_bytes().to_vec()
}

pub fn amz_date(t: DateTime<Utc>) -> String {
    t.format("%Y%m%dT%H%M%SZ").to_string()
}

/// The parts of a request that are signed. `path` is already encoded; `query` pairs are raw.
#[derive(Clone, Debug)]
pub struct Canonical<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub query: &'a [(String, String)],
    /// Lowercase names; `host` and `x-amz-date` must be among them.
    pub headers: &'a [(String, String)],
    pub payload_hash: &'a str,
}

pub fn canonical_query(q: &[(String, String)]) -> String {
    let mut v: Vec<(String, String)> = q.iter().map(|(k, v)| (encode(k), encode(v))).collect();
    v.sort();
    v.into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn norm(v: &str) -> String {
    v.split_whitespace().collect::<Vec<_>>().join(" ")
}

impl Canonical<'_> {
    pub fn signed_headers(&self) -> String {
        let mut names: Vec<&str> = self.headers.iter().map(|(k, _)| k.as_str()).collect();
        names.sort_unstable();
        names.dedup();
        names.join(";")
    }

    pub fn request(&self) -> String {
        let mut h: Vec<(&str, String)> = self
            .headers
            .iter()
            .map(|(k, v)| (k.as_str(), norm(v)))
            .collect();
        h.sort();
        let headers: String = h.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
        format!(
            "{}\n{}\n{}\n{headers}\n{}\n{}",
            self.method,
            self.path,
            canonical_query(self.query),
            self.signed_headers(),
            self.payload_hash
        )
    }
}

/// `(scope, signature)` for a canonical request at `amz_date` (`YYYYMMDDTHHMMSSZ`).
pub fn signature(
    secret: &str,
    region: &str,
    service: &str,
    amz_date: &str,
    c: &Canonical<'_>,
) -> (String, String) {
    let date = &amz_date[..8.min(amz_date.len())];
    let scope = format!("{date}/{region}/{service}/aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(c.request().as_bytes())
    );
    let k = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k = hmac(&k, region.as_bytes());
    let k = hmac(&k, service.as_bytes());
    let k = hmac(&k, b"aws4_request");
    (scope, hex(&hmac(&k, to_sign.as_bytes())))
}

/// Headers to add to a request signed at `t`: `x-amz-date`, `x-amz-security-token` (if any) and
/// `authorization`. `req.headers` are the other signed headers, including `host`.
pub fn sign(
    creds: &Credentials,
    region: &str,
    service: &str,
    t: DateTime<Utc>,
    req: &Canonical<'_>,
) -> Vec<(String, String)> {
    let stamp = amz_date(t);
    let mut all: Vec<(String, String)> = req.headers.to_vec();
    let mut add = vec![("x-amz-date".to_owned(), stamp.clone())];
    if let Some(tok) = &creds.session_token {
        add.push(("x-amz-security-token".to_owned(), tok.clone()));
    }
    all.extend(add.iter().cloned());
    let c = Canonical {
        headers: &all,
        ..req.clone()
    };
    let (scope, sig) = signature(&creds.secret, region, service, &stamp, &c);
    add.push((
        "authorization".to_owned(),
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={}, Signature={sig}",
            creds.key_id,
            c.signed_headers()
        ),
    ));
    add
}

/// A parsed `Authorization: AWS4-HMAC-SHA256 …` header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Authorization {
    pub key_id: String,
    pub date: String,
    pub region: String,
    pub service: String,
    pub signed_headers: Vec<String>,
    pub signature: String,
}

impl Authorization {
    pub fn parse(h: &str) -> Option<Self> {
        let rest = h.strip_prefix("AWS4-HMAC-SHA256 ")?;
        let mut cred = None;
        let mut signed = None;
        let mut sig = None;
        for part in rest.split(',') {
            let (k, v) = part.trim().split_once('=')?;
            match k {
                "Credential" => cred = Some(v),
                "SignedHeaders" => signed = Some(v),
                "Signature" => sig = Some(v),
                _ => {}
            }
        }
        let c: Vec<&str> = cred?.split('/').collect();
        let [key_id, date, region, service, "aws4_request"] = c.as_slice() else {
            return None;
        };
        Some(Self {
            key_id: (*key_id).to_owned(),
            date: (*date).to_owned(),
            region: (*region).to_owned(),
            service: (*service).to_owned(),
            signed_headers: signed?.split(';').map(str::to_owned).collect(),
            signature: sig?.to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[(&str, &str)]) -> Vec<(String, String)> {
        v.iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect()
    }

    // aws-sig-v4-test-suite: get-vanilla and get-vanilla-query-order-key-case.
    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

    #[test]
    fn test_suite_vectors() {
        let h = s(&[
            ("host", "example.amazonaws.com"),
            ("x-amz-date", "20150830T123600Z"),
        ]);
        let empty = sha256_hex(b"");
        let c = Canonical {
            method: "GET",
            path: "/",
            query: &[],
            headers: &h,
            payload_hash: &empty,
        };
        let (scope, sig) = signature(SECRET, "us-east-1", "service", "20150830T123600Z", &c);
        assert_eq!(scope, "20150830/us-east-1/service/aws4_request");
        assert_eq!(
            sig,
            "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
        let q = s(&[("Param2", "value2"), ("Param1", "value1")]);
        let c = Canonical { query: &q, ..c };
        assert_eq!(
            signature(SECRET, "us-east-1", "service", "20150830T123600Z", &c).1,
            "b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500"
        );
    }

    #[test]
    fn sign_and_parse_roundtrip() {
        let creds = Credentials::new("AKID", "sek", Some("tok".into()));
        let t = DateTime::parse_from_rfc3339("2026-01-02T03:04:05Z")
            .expect("t")
            .with_timezone(&Utc);
        let h = s(&[
            ("host", "localhost:1"),
            ("x-amz-target", "RedshiftData.ExecuteStatement"),
        ]);
        let req = Canonical {
            method: "POST",
            path: "/",
            query: &[],
            headers: &h,
            payload_hash: "abc",
        };
        let add = sign(&creds, "r", "redshift-data", t, &req);
        let auth = &add.iter().find(|(k, _)| k == "authorization").expect("a").1;
        let a = Authorization::parse(auth).expect("parse");
        assert_eq!(
            (a.key_id.as_str(), a.date.as_str(), a.service.as_str()),
            ("AKID", "20260102", "redshift-data")
        );
        assert_eq!(
            a.signed_headers,
            ["host", "x-amz-date", "x-amz-security-token", "x-amz-target"]
        );
        assert!(add.contains(&("x-amz-security-token".into(), "tok".into())));
        assert!(add.contains(&("x-amz-date".into(), "20260102T030405Z".into())));
    }

    #[test]
    fn encoding() {
        assert_eq!(encode("a b/c~d_e.f-g"), "a%20b%2Fc~d_e.f-g");
        assert_eq!(encode_path("/b/k=1/x y.parquet"), "/b/k%3D1/x%20y.parquet");
        assert_eq!(
            canonical_query(&s(&[("prefix", "a/b"), ("list-type", "2")])),
            "list-type=2&prefix=a%2Fb"
        );
    }
}
