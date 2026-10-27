use atlatl_core::error::{AtlatlError, ErrorClass};

pub fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

pub fn literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// `catalog.schema.table` from a dotted name, each part quoted.
pub fn qualified(parts: &[&str]) -> String {
    parts.iter().map(|p| ident(p)).collect::<Vec<_>>().join(".")
}

pub fn db(e: duckdb::Error) -> AtlatlError {
    let msg = atlatl_core::redact::redact(&e.to_string()).into_owned();
    let class = if msg.starts_with("Conversion Error") || msg.starts_with("Constraint Error") {
        ErrorClass::Data
    } else if msg.starts_with("Catalog Error")
        || msg.starts_with("Binder Error")
        || msg.starts_with("Parser Error")
    {
        ErrorClass::Config
    } else {
        match atlatl_engine::error::classify_message(&msg) {
            (ErrorClass::Conflict, code) => {
                let e = AtlatlError::new(ErrorClass::Conflict, msg);
                return match code {
                    Some(c) => e.code(c),
                    None => e,
                };
            }
            _ => ErrorClass::Internal,
        }
    };
    AtlatlError::new(class, msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_conflicts_retry() {
        let c = duckdb::Connection::open_in_memory().expect("db");
        let a = c.try_clone().expect("a");
        let b = c.try_clone().expect("b");
        a.execute_batch("BEGIN; CREATE SCHEMA s").expect("a");
        b.execute_batch("BEGIN").expect("b");
        let e = b
            .execute_batch("CREATE SCHEMA s")
            .map_err(db)
            .expect_err("conflict");
        assert_eq!(e.class, ErrorClass::Conflict, "{e:?}");
    }

    #[test]
    fn quoting() {
        assert_eq!(ident("a\"b"), "\"a\"\"b\"");
        assert_eq!(literal("it's"), "'it''s'");
        assert_eq!(
            qualified(&["catalog", "br", "T x"]),
            "\"catalog\".\"br\".\"T x\""
        );
    }
}
