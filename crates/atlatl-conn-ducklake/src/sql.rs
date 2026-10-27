//! Quoting for SQL literals, identifiers and libpq keyword values.

use atlatl_core::AtlatlError;
use atlatl_core::error::Result;

/// A DuckDB string literal (standard SQL: only `'` is special).
pub fn literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

pub fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// A libpq connection-string keyword value, always single-quoted with `\` and `'` escaped.
pub fn libpq_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\\' || c == '\'' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('\'');
    out
}

/// ATTACH aliases are plain identifiers: they name the lake in SQL, secrets and metadata.
pub fn check_alias(alias: &str) -> Result<()> {
    let mut chars = alias.chars();
    let ok = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if ok {
        Ok(())
    } else {
        Err(AtlatlError::config(format!(
            "ducklake alias `{alias}` must match [A-Za-z_][A-Za-z0-9_]*"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literals() {
        assert_eq!(literal("a"), "'a'");
        assert_eq!(literal("it's"), "'it''s'");
        assert_eq!(literal(r"back\slash"), r"'back\slash'");
        assert_eq!(literal(""), "''");
    }

    #[test]
    fn idents() {
        assert_eq!(ident("lake"), "\"lake\"");
        assert_eq!(ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn libpq_values() {
        assert_eq!(libpq_value("plain"), "'plain'");
        assert_eq!(libpq_value(""), "''");
        assert_eq!(libpq_value("a b"), "'a b'");
        assert_eq!(libpq_value("it's"), r"'it\'s'");
        assert_eq!(libpq_value(r"c:\x"), r"'c:\\x'");
        assert_eq!(libpq_value(r"\'"), r"'\\\''");
        assert_eq!(libpq_value("p=q x"), "'p=q x'");
    }

    #[test]
    fn aliases() {
        for ok in ["lake", "catalog", "_x1", "A_b"] {
            check_alias(ok).expect(ok);
        }
        for bad in ["", "1x", "a-b", "a b", "a\"b", "lake;"] {
            assert!(check_alias(bad).is_err(), "{bad}");
        }
    }
}
