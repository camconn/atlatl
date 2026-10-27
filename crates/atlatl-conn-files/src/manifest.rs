//! Redshift `UNLOAD … MANIFEST [VERBOSE]` manifests.

use atlatl_core::error::{AtlatlError, ErrorClass, Result};
use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestEntry {
    pub url: String,
    pub content_length: Option<u64>,
    pub record_count: Option<u64>,
}

#[derive(Deserialize)]
struct Raw {
    entries: Vec<RawEntry>,
}

#[derive(Deserialize)]
struct RawEntry {
    url: String,
    #[serde(default)]
    meta: Option<RawMeta>,
}

#[derive(Deserialize)]
struct RawMeta {
    content_length: Option<u64>,
    record_count: Option<u64>,
}

pub fn parse(text: &str, origin: &str) -> Result<Vec<ManifestEntry>> {
    let raw: Raw = serde_json::from_str(text).map_err(|e| {
        AtlatlError::new(
            ErrorClass::Decode,
            format!("manifest {origin}: not an UNLOAD manifest: {e}"),
        )
    })?;
    Ok(raw
        .entries
        .into_iter()
        .map(|e| ManifestEntry {
            url: e.url,
            content_length: e.meta.as_ref().and_then(|m| m.content_length),
            record_count: e.meta.as_ref().and_then(|m| m.record_count),
        })
        .collect())
}

/// Greedy consecutive groups of about `size` bytes; a group always has at least one file.
pub fn group_by_size(sizes: &[u64], size: u64) -> Vec<std::ops::Range<usize>> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut acc = 0u64;
    for (i, &s) in sizes.iter().enumerate() {
        if i > start && acc + s > size {
            out.push(start..i);
            start = i;
            acc = 0;
        }
        acc += s;
    }
    if start < sizes.len() {
        out.push(start..sizes.len());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_verbose() {
        let m = parse(
            r#"{"entries":[
                {"url":"s3://b/p/0000_part_00.parquet","meta":{"content_length":10,"record_count":3}},
                {"url":"s3://b/p/0001_part_00.parquet"}
            ],"schema":{"elements":[]},"meta":{"content_length":10,"record_count":3}}"#,
            "m",
        )
        .expect("manifest");
        assert_eq!(m.len(), 2);
        assert_eq!(m[0].content_length, Some(10));
        assert_eq!(m[0].record_count, Some(3));
        assert_eq!(m[1].content_length, None);
        let e = parse("{}", "m").expect_err("bad");
        assert_eq!(e.class, ErrorClass::Decode);
    }

    #[test]
    fn groups() {
        assert_eq!(group_by_size(&[5, 5, 5, 5], 10), [0..2, 2..4]);
        assert_eq!(group_by_size(&[20, 1, 1], 10), [0..1, 1..3]);
        assert_eq!(group_by_size(&[3, 3, 3], 100), vec![0..3]);
        assert_eq!(group_by_size(&[], 10), Vec::<std::ops::Range<usize>>::new());
        assert_eq!(group_by_size(&[4, 7, 2], 10), [0..1, 1..3]);
    }
}
