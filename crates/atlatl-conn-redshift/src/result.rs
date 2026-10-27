//! Small result sets (discovery, precision lookup) from either access path.

use serde::Deserialize;

/// A `GetStatementResult` field; the wire path yields only `String` and `Null`.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub enum Field {
    #[serde(rename = "isNull")]
    Null(bool),
    #[serde(rename = "booleanValue")]
    Bool(bool),
    #[serde(rename = "longValue")]
    Long(i64),
    #[serde(rename = "doubleValue")]
    Double(f64),
    #[serde(rename = "stringValue")]
    String(String),
    /// Base64, as sent.
    #[serde(rename = "blobValue")]
    Blob(String),
}

impl Field {
    pub fn is_null(&self) -> bool {
        matches!(self, Field::Null(_))
    }

    pub fn text(&self) -> Option<String> {
        match self {
            Field::Null(_) => None,
            Field::Bool(b) => Some(b.to_string()),
            Field::Long(n) => Some(n.to_string()),
            Field::Double(d) => Some(d.to_string()),
            Field::String(s) | Field::Blob(s) => Some(s.clone()),
        }
    }

    pub fn int(&self) -> Option<i64> {
        match self {
            Field::Long(n) => Some(*n),
            Field::Double(d) => Some(*d as i64),
            Field::String(s) => s.trim().parse().ok(),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Field>>,
}

impl ResultSet {
    pub fn col(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.eq_ignore_ascii_case(name))
    }

    fn field(&self, row: usize, name: &str) -> Option<&Field> {
        self.rows.get(row)?.get(self.col(name)?)
    }

    pub fn text(&self, row: usize, name: &str) -> Option<String> {
        self.field(row, name).and_then(Field::text)
    }

    pub fn int(&self, row: usize, name: &str) -> Option<i64> {
        self.field(row, name).and_then(Field::int)
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// A finished statement.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Executed {
    pub id: String,
    /// Rows affected or unloaded, if reported.
    pub rows: Option<i64>,
    pub has_result_set: bool,
    pub redshift_query_id: Option<i64>,
    pub duration_ns: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fields() {
        let f: Vec<Field> = serde_json::from_str(
            r#"[{"longValue":3},{"stringValue":"18"},{"isNull":true},{"doubleValue":1.5},{"booleanValue":false}]"#,
        )
        .expect("fields");
        assert_eq!(f[0].int(), Some(3));
        assert_eq!(f[1].int(), Some(18));
        assert!(f[2].is_null() && f[2].text().is_none());
        assert_eq!(f[3].text().as_deref(), Some("1.5"));
        assert_eq!(f[4].text().as_deref(), Some("false"));
        let rs = ResultSet {
            columns: vec!["Column_Name".into()],
            rows: vec![vec![Field::String("x".into())]],
        };
        assert_eq!(rs.text(0, "column_name").as_deref(), Some("x"));
        assert_eq!(rs.text(1, "column_name"), None);
    }
}
