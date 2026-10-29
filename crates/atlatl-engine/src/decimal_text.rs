//! `atlatl_decimal_text(text VARCHAR) -> VARCHAR`: a decimal or exponent numeral as exact plain
//! decimal text (`"15e-1"` → `"1.5"`), no leading or trailing zeros, NULL if `text` isn't one or
//! its plain form would exceed [`MAX_LEN`]. DuckDB won't cast exponent forms with several mantissa
//! integer digits to a high-scale DECIMAL.

use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeId};
use duckdb::ffi::duckdb_string_t;
use duckdb::types::DuckString;
use duckdb::vscalar::{ScalarFunctionSignature, VScalar};
use duckdb::vtab::arrow::WritableVector;

pub const FUNCTION: &str = "atlatl_decimal_text";

pub const MAX_LEN: usize = 400;

/// Exact decimal text as `(negative, digits, exp)`, value = ±digits × 10^exp, digits without
/// leading or trailing zeros (`"0"` for zero).
pub fn parts(s: &str) -> Option<(bool, String, i64)> {
    let s = s.trim();
    let (neg, s) = match s.as_bytes().first()? {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    let (mant, exp) = match s.find(['e', 'E']) {
        Some(i) => (&s[..i], s[i + 1..].parse::<i64>().ok()?),
        None => (s, 0),
    };
    let (int, frac) = mant.split_once('.').unwrap_or((mant, ""));
    if (int.is_empty() && frac.is_empty())
        || !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let all = format!("{int}{frac}");
    let lead = all.trim_start_matches('0');
    let digits = lead.trim_end_matches('0');
    if digits.is_empty() {
        return Some((false, "0".into(), 0));
    }
    let exp = exp
        .checked_sub(i64::try_from(frac.len()).ok()?)?
        .checked_add(i64::try_from(lead.len() - digits.len()).ok()?)?;
    Some((neg, digits.to_owned(), exp))
}

/// Length of [`plain`]'s output, saturating.
pub fn plain_len(neg: bool, digits: &str, exp: i64) -> usize {
    let n = digits.len();
    let shift = usize::try_from(exp.unsigned_abs()).unwrap_or(usize::MAX);
    let body = if exp >= 0 {
        n.saturating_add(shift)
    } else if n > shift {
        n + 1
    } else {
        shift.saturating_add(2)
    };
    body.saturating_add(usize::from(neg))
}

pub fn plain(neg: bool, digits: &str, exp: i64) -> String {
    let sign = if neg { "-" } else { "" };
    let body = if exp >= 0 {
        format!("{digits}{}", "0".repeat(exp as usize))
    } else {
        let scale = exp.unsigned_abs() as usize;
        if digits.len() > scale {
            let (i, f) = digits.split_at(digits.len() - scale);
            format!("{i}.{f}")
        } else {
            format!("0.{}{digits}", "0".repeat(scale - digits.len()))
        }
    };
    format!("{sign}{body}")
}

pub fn canonical(s: &str) -> Option<String> {
    let (n, d, e) = parts(s)?;
    (plain_len(n, &d, e) <= MAX_LEN).then(|| plain(n, &d, e))
}

pub struct DecimalText;

impl VScalar for DecimalText {
    type State = ();

    fn invoke(
        _: &(),
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let n = input.len();
        let col = input.flat_vector(0);
        // SAFETY: the column is VARCHAR per the signature and holds `n` rows.
        let values = unsafe { col.as_slice_with_len::<duckdb_string_t>(n) };
        let mut out = output.flat_vector();
        for (row, v) in values.iter().enumerate() {
            let r = match col.row_is_null(row as u64) {
                true => None,
                false => canonical(DuckString::new(&mut { *v }).as_str().as_ref()),
            };
            match r {
                Some(s) => out.insert(row, s.as_str()),
                None => out.set_null(row),
            }
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
    use super::{MAX_LEN, canonical, parts, plain};

    #[test]
    fn plain_text() {
        let p = |s: &str| parts(s).map(|(n, d, e)| plain(n, &d, e));
        assert_eq!(p("1.50").as_deref(), Some("1.5"));
        assert_eq!(p("1e-3").as_deref(), Some("0.001"));
        assert_eq!(p("-12.5E2").as_deref(), Some("-1250"));
        assert_eq!(p(".05").as_deref(), Some("0.05"));
        assert_eq!(p("-0.00").as_deref(), Some("0"));
        assert_eq!(p("00120").as_deref(), Some("120"));
        for bad in [
            "",
            ".",
            "1.2.3",
            "abc",
            "1e",
            "NaN",
            "inf",
            "1_0",
            "1e99999999999999999999",
        ] {
            assert_eq!(parts(bad), None, "{bad}");
        }
    }

    #[test]
    fn canonical_spellings() {
        for s in [
            "15e-1", "0.15E+1", "1.5e0", "150e-2", "1.50", "+1.5", ".15e1", "1.5",
        ] {
            assert_eq!(canonical(s).as_deref(), Some("1.5"), "{s}");
        }
        assert_eq!(canonical("5.").as_deref(), Some("5"));
        let tiny = format!("0.{}1", "0".repeat(37));
        for s in ["1e-38", "10e-39", "0.1e-37", tiny.as_str()] {
            assert_eq!(canonical(s).as_deref(), Some(tiny.as_str()), "{s}");
        }
        assert_eq!(canonical("-1E+2").as_deref(), Some("-100"));
        assert_eq!(canonical("-0e5").as_deref(), Some("0"));
        assert_eq!(canonical("1x"), None);
    }

    #[test]
    fn bounds_plain_length() {
        for s in ["1e999999999", "1e-999999999", "-1e9223372036854775807"] {
            assert_eq!(canonical(s), None, "{s}");
        }
        let edge = format!("1e{}", MAX_LEN - 1);
        assert_eq!(canonical(&edge).map(|s| s.len()), Some(MAX_LEN));
        assert_eq!(canonical(&format!("1e{MAX_LEN}")), None);
        assert_eq!(canonical("0e999999999").as_deref(), Some("0"));
    }
}
