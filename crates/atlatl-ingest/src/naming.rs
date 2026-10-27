//! Column naming policies (04 §5.3).

use std::collections::HashMap;

use unicode_normalization::UnicodeNormalization;

/// Legacy CSV header cleaning, for adopting tables loaded that way: NFD-strip accents, runs of
/// `[^\p{L}\p{N}_]` → `_`, trim `_`, `_` prefix before a leading digit, dedupe as `name1`,
/// `name2`, …, lowercase.
pub fn clean_csv(headers: &[&str]) -> Vec<String> {
    let mut seen: HashMap<String, usize> = HashMap::new();
    headers
        .iter()
        .map(|h| {
            let stripped: String = h
                .nfd()
                .filter(|c| !unicode_normalization::char::is_combining_mark(*c))
                .collect();
            let mut out = String::new();
            let mut pending = false;
            for c in stripped.chars() {
                if c.is_alphanumeric() || c == '_' {
                    if pending && !out.is_empty() {
                        out.push('_');
                    }
                    pending = false;
                    out.push(c);
                } else {
                    pending = true;
                }
            }
            let mut name = out.trim_matches('_').to_owned();
            if name.chars().next().is_some_and(|c| c.is_ascii_digit()) {
                name.insert(0, '_');
            }
            let name = name.to_lowercase();
            let n = seen.entry(name.clone()).or_insert(0);
            *n += 1;
            if *n == 1 {
                name
            } else {
                format!("{name}{}", *n - 1)
            }
        })
        .collect()
}

pub fn snake(name: &str) -> String {
    let mut out = String::new();
    let chars: Vec<char> = name.chars().collect();
    for (i, &c) in chars.iter().enumerate() {
        if c.is_uppercase() {
            let prev_lower =
                i > 0 && (chars[i - 1].is_lowercase() || chars[i - 1].is_ascii_digit());
            let next_lower = chars.get(i + 1).is_some_and(|n| n.is_lowercase());
            if i > 0
                && (prev_lower || (next_lower && chars[i - 1].is_uppercase()))
                && !out.ends_with('_')
            {
                out.push('_');
            }
            out.extend(c.to_lowercase());
        } else if c.is_alphanumeric() {
            out.push(c);
        } else if !out.ends_with('_') && !out.is_empty() {
            out.push('_');
        }
    }
    out.trim_end_matches('_').to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_csv_cleaning() {
        let got = clean_csv(&[
            "Ticker Symbol",
            "Fiscal Year (FY)",
            "Café Réel",
            "2020 Value",
            "ticker symbol",
            "ticker_symbol",
            "__x__",
            "a--b..c",
        ]);
        assert_eq!(
            got,
            [
                "ticker_symbol",
                "fiscal_year_fy",
                "cafe_reel",
                "_2020_value",
                "ticker_symbol1",
                "ticker_symbol2",
                "x",
                "a_b_c"
            ]
        );
    }

    #[test]
    fn snake_case() {
        assert_eq!(snake("dealID"), "deal_id");
        assert_eq!(snake("FiscalTag"), "fiscal_tag");
        assert_eq!(snake("HTTPStatusCode"), "http_status_code");
        assert_eq!(snake("proposedTickerSymbol"), "proposed_ticker_symbol");
        assert_eq!(snake("already_snake"), "already_snake");
        assert_eq!(snake("v2Id"), "v2_id");
    }
}
