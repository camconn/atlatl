//! `atlatl_json_exact(doc VARCHAR[, key VARCHAR]) -> VARCHAR`: `doc`, or its top-level member
//! `key` (the first, if repeated), as compact JSON with number literals kept as written. DuckDB's
//! own extraction (`json_extract*`, `->`, `->>`) re-serializes fractional numbers through a
//! double. Scanning is as lenient as DuckDB's reader (`NaN`, trailing commas); NULL for a missing
//! member or text it can't scan.

use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeId};
use duckdb::ffi::duckdb_string_t;
use duckdb::types::DuckString;
use duckdb::vscalar::{ScalarFunctionSignature, VScalar};
use duckdb::vtab::arrow::WritableVector;

pub const FUNCTION: &str = "atlatl_json_exact";

fn ws(b: &[u8], mut i: usize) -> usize {
    while b.get(i).is_some_and(u8::is_ascii_whitespace) {
        i += 1;
    }
    i
}

fn string_end(b: &[u8], mut i: usize) -> Option<usize> {
    i += 1;
    loop {
        match *b.get(i)? {
            b'"' => return Some(i + 1),
            b'\\' => i += 2,
            _ => i += 1,
        }
    }
}

fn value_end(b: &[u8], mut i: usize) -> Option<usize> {
    match *b.get(i)? {
        b'"' => string_end(b, i),
        b'[' | b'{' => {
            let mut depth = 0usize;
            loop {
                match *b.get(i)? {
                    b'"' => {
                        i = string_end(b, i)?;
                        continue;
                    }
                    b'[' | b'{' => depth += 1,
                    b']' | b'}' => {
                        depth -= 1;
                        if depth == 0 {
                            return Some(i + 1);
                        }
                    }
                    _ => {}
                }
                i += 1;
            }
        }
        _ => {
            let n = b[i..]
                .iter()
                .take_while(|c| {
                    !c.is_ascii_whitespace()
                        && !matches!(c, b',' | b':' | b'"' | b'[' | b']' | b'{' | b'}')
                })
                .count();
            (n > 0).then_some(i + n)
        }
    }
}

fn compact(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let (mut quoted, mut escaped) = (false, false);
    for c in s.chars() {
        if quoted {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                quoted = false;
            }
        } else if c == '"' {
            quoted = true;
        } else if c.is_ascii_whitespace() {
            continue;
        }
        out.push(c);
    }
    out
}

pub fn exact(doc: &str) -> Option<String> {
    let b = doc.as_bytes();
    let i = ws(b, 0);
    let end = value_end(b, i)?;
    (ws(b, end) == b.len()).then(|| compact(&doc[i..end]))
}

pub fn member(doc: &str, key: &str) -> Option<String> {
    let b = doc.as_bytes();
    let mut i = ws(b, 0);
    if b.get(i) != Some(&b'{') {
        return None;
    }
    i += 1;
    loop {
        i = ws(b, i);
        if *b.get(i)? != b'"' {
            return None;
        }
        let k_end = string_end(b, i)?;
        let k = &doc[i..k_end];
        i = ws(b, k_end);
        if *b.get(i)? != b':' {
            return None;
        }
        let v = ws(b, i + 1);
        let v_end = value_end(b, v)?;
        let hit = match k[1..k.len() - 1].contains('\\') {
            true => crate::json_canon::unescape(k).is_some_and(|k| k == key),
            false => &k[1..k.len() - 1] == key,
        };
        if hit {
            return Some(compact(&doc[v..v_end]));
        }
        i = ws(b, v_end);
        if *b.get(i)? != b',' {
            return None;
        }
        i += 1;
    }
}

pub struct JsonExact;

impl VScalar for JsonExact {
    type State = ();

    fn invoke(
        _: &(),
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let n = input.len();
        let cols: Vec<_> = (0..input.num_columns())
            .map(|c| input.flat_vector(c))
            .collect();
        // SAFETY: every column is VARCHAR per the signatures and holds `n` rows.
        let values: Vec<_> = cols
            .iter()
            .map(|c| unsafe { c.as_slice_with_len::<duckdb_string_t>(n) })
            .collect();
        let mut out = output.flat_vector();
        for row in 0..n {
            if cols.iter().any(|c| c.row_is_null(row as u64)) {
                out.set_null(row);
                continue;
            }
            let text = |c: usize| {
                DuckString::new(&mut { values[c][row] })
                    .as_str()
                    .to_string()
            };
            let r = match cols.len() {
                1 => exact(&text(0)),
                _ => member(&text(0), &text(1)),
            };
            match r {
                Some(s) => out.insert(row, s.as_str()),
                None => out.set_null(row),
            }
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        let v = || LogicalTypeId::Varchar.into();
        vec![
            ScalarFunctionSignature::exact(vec![v()], v()),
            ScalarFunctionSignature::exact(vec![v(), v()], v()),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::{exact, member};

    #[test]
    fn members_keep_number_text() {
        let doc = r#"{ "a" : 0.12345678901234567891, "b":12345678901234567.891,
            "c": {"x": [1.10, "y\"}"]}, "d!": 1e400, "a": 2, "n": null, "t": NaN, }"#;
        assert_eq!(member(doc, "a").as_deref(), Some("0.12345678901234567891"));
        assert_eq!(member(doc, "b").as_deref(), Some("12345678901234567.891"));
        assert_eq!(member(doc, "c").as_deref(), Some(r#"{"x":[1.10,"y\"}"]}"#));
        assert_eq!(member(doc, "d!").as_deref(), Some("1e400"));
        assert_eq!(member(doc, "n").as_deref(), Some("null"));
        assert_eq!(member(doc, "t").as_deref(), Some("NaN"));
        assert_eq!(member(doc, "z"), None);
        assert_eq!(member("[1]", "a"), None);
        assert_eq!(member(r#"{"a":"x y"}"#, "a").as_deref(), Some(r#""x y""#));
        assert_eq!(member(r#"{"a":"#, "a"), None);
        assert_eq!(member(r#"{"a":[1,"#, "a"), None);
    }

    #[test]
    fn documents_compact() {
        assert_eq!(
            exact(" [ 0.12345678901234567891 , {\"k\" : \"a b\"} ] ").as_deref(),
            Some(r#"[0.12345678901234567891,{"k":"a b"}]"#)
        );
        assert_eq!(exact("1.10").as_deref(), Some("1.10"));
        assert_eq!(exact("1 2"), None);
        assert_eq!(exact(r#""\"#), None);
    }
}
