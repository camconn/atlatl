//! `update_key` on a clickhouse source: `{incremental_where_cond}` and `{incremental_value}` in
//! `from.query` become the stored watermark's condition and value (`1=1` and `null` before the
//! first one).

use atlatl_core::template::{INCREMENTAL_VALUE, INCREMENTAL_WHERE_COND};

use crate::ident;

/// A watermark: the update key's column and, once there is one, its value as a ClickHouse
/// literal (lookback already applied).
#[derive(Clone, Debug)]
pub struct Watermark {
    pub column: String,
    pub value: Option<String>,
    pub inclusive: bool,
    pub include_nulls: bool,
}

impl Watermark {
    pub fn condition(&self) -> String {
        let col = ident(&self.column);
        let Some(v) = &self.value else {
            return "1=1".into();
        };
        let b = format!("{col} {} {v}", if self.inclusive { ">=" } else { ">" });
        if self.include_nulls {
            format!("({b} OR {col} IS NULL)")
        } else {
            b
        }
    }

    pub fn render(&self, query: &str) -> String {
        query
            .replace(INCREMENTAL_WHERE_COND, &self.condition())
            .replace(INCREMENTAL_VALUE, self.value.as_deref().unwrap_or("null"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wm(value: Option<&str>) -> Watermark {
        Watermark {
            column: "date".into(),
            value: value.map(str::to_owned),
            inclusive: true,
            include_nulls: false,
        }
    }

    #[test]
    fn first_run_reads_everything() {
        let q = "SELECT * FROM t WHERE {incremental_where_cond} AND d >= coalesce({incremental_value}, '2000-01-01')";
        assert_eq!(
            wm(None).render(q),
            "SELECT * FROM t WHERE 1=1 AND d >= coalesce(null, '2000-01-01')"
        );
    }

    #[test]
    fn later_runs_bound_by_the_watermark() {
        let q = "SELECT * FROM t WHERE {incremental_where_cond} OR x > {incremental_value}";
        assert_eq!(
            wm(Some("'2024-01-01'")).render(q),
            "SELECT * FROM t WHERE `date` >= '2024-01-01' OR x > '2024-01-01'"
        );
        let w = Watermark {
            inclusive: false,
            include_nulls: true,
            ..wm(Some("5"))
        };
        assert_eq!(w.condition(), "(`date` > 5 OR `date` IS NULL)");
    }
}
