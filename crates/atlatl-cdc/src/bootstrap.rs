//! `atlatl cdc bootstrap` (08 §9): diff lake@S against the serving table through DuckDB's
//! read-only Postgres attach. Results use the same TEMP-table shape as [`crate::net_changes`].

use atlatl_core::error::{AtlatlError, Result};
use duckdb::{Connection, OptionalExt};
use serde::Serialize;

use crate::{NetChanges, Source, db, ident};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// Apply only differences (existing populated tables).
    Adopt,
    /// Upsert every lake row; delete target rows missing from the lake. Never truncates.
    Reload,
}

/// The serving table as DuckDB sees it through `ATTACH … (TYPE postgres, READ_ONLY)`.
#[derive(Clone, Debug)]
pub struct Target {
    pub attach: String,
    pub schema: String,
    pub table: String,
    /// The soft-delete column (served or not): rows with it set count as already deleted.
    pub soft_delete_column: Option<String>,
    /// `deletes.value: true`: the column is boolean and deleted means `true`.
    pub soft_delete_bool: bool,
}

impl Target {
    fn fq(&self) -> String {
        format!(
            "{}.{}.{}",
            ident(&self.attach),
            ident(&self.schema),
            ident(&self.table)
        )
    }
}

fn lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Target columns with the scanner's DuckDB type and Postgres's own `format_type` / `typname`
/// of the base type (a domain resolved to the type it's over).
fn pg_columns(c: &Connection, t: &Target) -> Result<Vec<(String, String, String, String)>> {
    let scanned = types(c, &t.fq())?;
    let rel = lit(&format!("{}.{}", ident(&t.schema), ident(&t.table)));
    let q = format!(
        "WITH RECURSIVE b(attnum, oid, typmod) AS ( \
           SELECT a.attnum, a.atttypid, a.atttypmod FROM pg_attribute a \
           WHERE a.attrelid = {rel}::regclass AND a.attnum > 0 AND NOT a.attisdropped \
           UNION ALL \
           SELECT b.attnum, ty.typbasetype, ty.typtypmod FROM b JOIN pg_type ty ON ty.oid = b.oid WHERE ty.typtype = 'd') \
         SELECT a.attname::text, format_type(b.oid, b.typmod), ty.typname::text FROM b \
         JOIN pg_type ty ON ty.oid = b.oid \
         JOIN pg_attribute a ON a.attrelid = {rel}::regclass AND a.attnum = b.attnum \
         WHERE ty.typtype <> 'd'"
    );
    let mut st = c
        .prepare(&format!(
            "SELECT * FROM postgres_query({}, {})",
            lit(&t.attach),
            lit(&q)
        ))
        .map_err(db)?;
    let pg: Vec<(String, String, String)> = st
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .map_err(db)?
        .collect::<std::result::Result<_, _>>()
        .map_err(db)?;
    Ok(scanned
        .into_iter()
        .map(|(n, duck)| {
            let (f, tn) = pg
                .iter()
                .find(|(p, _, _)| *p == n)
                .map(|(_, f, t)| (f.clone(), t.clone()))
                .unwrap_or_default();
            (n, duck, f, tn)
        })
        .collect())
}

/// How one served column is carried and compared.
enum ColKind {
    /// Cast the lake value to the scanner type; compare as is.
    Plain(String),
    /// json/jsonb: compare canonical text (key order, spacing).
    Json,
    /// A `numeric` the scanner doesn't read as DECIMAL (DOUBLE when unconstrained or over 38
    /// digits, VARCHAR through a domain): keep lake values exact, read the target as text and
    /// compare in the lake's type.
    Numeric,
    /// `timestamp(p)` / `time(p)` with p < 6: Postgres rounds on write, so compare lake values
    /// rounded to p digits.
    Timestamp(String, u32),
}

fn col_kind(duck: &str, format: &str, typname: &str) -> ColKind {
    if matches!(typname, "json" | "jsonb") {
        return ColKind::Json;
    }
    if typname == "numeric" && !duck.starts_with("DECIMAL") {
        return ColKind::Numeric;
    }
    if (format.starts_with("timestamp(") || format.starts_with("time("))
        && let Some(p) = format
            .split(['(', ')'])
            .nth(1)
            .and_then(|p| p.parse::<u32>().ok())
        && p < 6
        && (duck.starts_with("TIMESTAMP") || duck == "TIME")
    {
        return ColKind::Timestamp(duck.to_owned(), p);
    }
    ColKind::Plain(duck.to_owned())
}

/// DuckDB's last timestamp (UTC). Postgres's run to 294276 AD; the scanner lands the next
/// microsecond as `infinity`, wraps later ones to BC or fails unnamed.
const LAST_TIMESTAMP: &str = "294247-01-10 04:00:54.775806";

/// A target value the scanner reads wrong, looked for in Postgres: the predicate on column `i`
/// and the error's tail after `holds <value>`.
fn unlandable(duck: &str, i: &str) -> Option<(String, String)> {
    let (last, t) = match duck {
        "TIMESTAMP" => (LAST_TIMESTAMP.to_owned(), "timestamp"),
        "TIMESTAMP WITH TIME ZONE" => (format!("{LAST_TIMESTAMP}+00"), "timestamptz"),
        d if d.starts_with("DECIMAL") => {
            return Some((
                format!("{i} = 'NaN'"),
                ", which a decimal can't hold".to_owned(),
            ));
        }
        _ => return None,
    };
    Some((
        format!("{i} > '{last}'::{t} AND {i} < 'infinity'"),
        format!(", past the last timestamp the lake holds ({last})"),
    ))
}

/// Numeric text without trailing fractional zeros, so `1.50` (Postgres) matches `1.5000`.
fn numeric_text(e: &str) -> String {
    format!("CASE WHEN contains({e}, '.') THEN rtrim(rtrim({e}, '0'), '.') ELSE {e} END")
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct DryRun {
    pub upserts: u64,
    pub deletes: u64,
    pub samples: Vec<serde_json::Value>,
}

fn types(c: &Connection, relation: &str) -> Result<Vec<(String, String)>> {
    let mut s = c
        .prepare(&format!("DESCRIBE SELECT * FROM {relation}"))
        .map_err(db)?;
    let rows = s
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
        .map_err(db)?;
    rows.map(|r| r.map_err(db)).collect()
}

/// Computes the bootstrap diff at snapshot `at`. Lake values are cast to the target's column
/// types (as the postgres scanner reports them) before comparing.
pub fn diff(
    c: &Connection,
    s: &Source,
    at: i64,
    t: &Target,
    mode: Mode,
    prefix: &str,
) -> Result<NetChanges> {
    let tn = |n: &str| ident(&format!("{prefix}_{n}"));
    let tgt = pg_columns(c, t)?;
    let kind = |col: &str| {
        tgt.iter()
            .find(|(n, ..)| n == col)
            .map(|(_, d, f, tn)| col_kind(d, f, tn))
            .ok_or_else(|| {
                AtlatlError::config(format!(
                    "served column `{col}` is not in {}.{}",
                    t.schema, t.table
                ))
            })
    };
    let kinds = s
        .columns
        .iter()
        .map(|(n, _)| kind(n).map(|k| (n.as_str(), k)))
        .collect::<Result<Vec<_>>>()?;
    let fq_lake = format!(
        "{}.{}.{}",
        ident(&s.catalog),
        ident(&s.schema),
        ident(&s.table)
    );
    let expr_types = types(
        c,
        &format!(
            "(SELECT {} FROM (SELECT * FROM {fq_lake} AT (VERSION => {at})) x LIMIT 0)",
            s.columns
                .iter()
                .map(|(n, e)| format!("{e} AS {}", ident(n)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    )?;
    let lake_cols = s
        .columns
        .iter()
        .zip(&kinds)
        .map(|((n, e), (_, k))| match k {
            ColKind::Plain(ty) | ColKind::Timestamp(ty, _) => {
                format!("CAST({e} AS {ty}) AS {}", ident(n))
            }
            // Nested lake values (STRUCT, MAP, LIST) become JSON, not DuckDB's `{'a': 1}` text.
            ColKind::Json => match expr_types
                .iter()
                .find(|(c, _)| c == n)
                .map(|(_, t)| t.as_str())
            {
                Some("JSON" | "VARCHAR") | None => format!("{e} AS {}", ident(n)),
                Some(_) => format!("to_json({e}) AS {}", ident(n)),
            },
            ColKind::Numeric => format!("{e} AS {}", ident(n)),
        })
        .collect::<Vec<_>>()
        .join(", ");
    // Unlandable values are nulled in Postgres; those that would sway the diff are flagged and
    // fail it. Reload compares nothing but the key.
    let is_pk = |n: &str| s.primary_key.iter().any(|k| k == n);
    let mut flagged = Vec::new();
    let pg_cols = kinds
        .iter()
        .map(|(n, k)| {
            let i = ident(n);
            let duck = match k {
                ColKind::Json | ColKind::Numeric => return format!("{i}::text AS {i}"),
                ColKind::Plain(d) | ColKind::Timestamp(d, _) => d,
            };
            let Some((bad, why)) = unlandable(duck, &i) else {
                return i;
            };
            // A `CASE` drops the typmod: numeric(p,s) would scan as DOUBLE.
            let base = tgt
                .iter()
                .find(|(p, ..)| p == n)
                .map(|(_, _, f, _)| f.as_str())
                .filter(|f| !f.is_empty())
                .map(|f| format!("::{f}"))
                .unwrap_or_default();
            let col = format!("(CASE WHEN {bad} THEN NULL ELSE {i} END){base} AS {i}");
            let sways = is_pk(n) || (mode == Mode::Adopt && !s.ignore.iter().any(|x| x == n));
            if !sways {
                return col;
            }
            let flag = ident(&format!("atlatl_bad_{}", flagged.len()));
            let fix = if is_pk(n) {
                "correct it in the target"
            } else {
                "correct it in the target, or bootstrap with `--mode reload`, which overwrites it"
            };
            flagged.push((
                flag.clone(),
                format!("column `{n}` of `{}.{}` holds ", t.schema, t.table),
                format!("{why}; {fix}"),
            ));
            format!("{col}, CASE WHEN {bad} THEN ({i})::text END AS {flag}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let alive = match (&t.soft_delete_column, t.soft_delete_bool) {
        (Some(c), false) => format!("({} IS NULL)", ident(c)),
        (Some(c), true) => format!("({} IS NOT TRUE)", ident(c)),
        (None, _) => "true".to_owned(),
    };
    let tgt_sql = format!(
        "SELECT {pg_cols}, {alive} AS _alive FROM {}.{}",
        ident(&t.schema),
        ident(&t.table)
    );
    let filter = s
        .filter
        .as_ref()
        .map(|f| format!("WHERE {f}"))
        .unwrap_or_default();
    c.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE {lake} AS SELECT {lake_cols} FROM (SELECT * FROM {fq_lake} AT (VERSION => {at})) x {filter};
         CREATE OR REPLACE TEMP TABLE {tgtt} AS SELECT * FROM postgres_query({att}, {q});",
        lake = tn("lake"),
        tgtt = tn("target"),
        att = lit(&t.attach),
        q = lit(&tgt_sql),
    ))
    .map_err(db)?;
    if !flagged.is_empty() {
        let which = flagged
            .iter()
            .enumerate()
            .map(|(k, (f, ..))| format!("WHEN {f} IS NOT NULL THEN {k}"))
            .collect::<Vec<_>>()
            .join(" ");
        let vals = flagged
            .iter()
            .map(|(f, ..)| f.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let hit: Option<(i64, String)> = c
            .query_row(
                &format!(
                    "SELECT CASE {which} END, coalesce({vals}) FROM {} WHERE coalesce({vals}) IS NOT NULL LIMIT 1",
                    tn("target")
                ),
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(db)?;
        if let Some((k, v)) = hit {
            let (_, head, tail) = &flagged[k as usize];
            return Err(AtlatlError::data(format!("{head}{v}{tail}")));
        }
        for (f, ..) in &flagged {
            c.execute_batch(&format!("ALTER TABLE {} DROP COLUMN {f}", tn("target")))
                .map_err(db)?;
        }
    }
    let on = s
        .primary_key
        .iter()
        .map(|k| format!("a.{k} = b.{k}", k = ident(k)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let lake_types = types(c, &tn("lake"))?;
    let cmp: Vec<(String, String)> = kinds
        .iter()
        .filter(|(n, _)| !s.primary_key.iter().any(|k| k == n) && !s.ignore.iter().any(|i| i == n))
        .map(|(n, k)| {
            let (a, b) = (format!("a.{}", ident(n)), format!("b.{}", ident(n)));
            match k {
                ColKind::Plain(_) => (a, b),
                ColKind::Json => (
                    format!("atlatl_json_canon(CAST({a} AS VARCHAR))"),
                    format!("atlatl_json_canon(CAST({b} AS VARCHAR))"),
                ),
                ColKind::Numeric => {
                    let ty = lake_types.iter().find(|(c, _)| c == n).map(|(_, t)| t.clone()).unwrap_or_else(|| "VARCHAR".into());
                    let exact = ty.starts_with("DECIMAL")
                        || matches!(ty.trim_start_matches('U'), "TINYINT" | "SMALLINT" | "INTEGER" | "BIGINT" | "HUGEINT");
                    if exact {
                        (numeric_text(&format!("CAST({a} AS VARCHAR)")), numeric_text(&b))
                    } else {
                        (a, format!("TRY_CAST({b} AS {ty})"))
                    }
                }
                ColKind::Timestamp(ty, p) if ty == "TIME" => {
                    let unit = 10i64.pow(6 - p);
                    let us = format!("((hour({a}) * 3600 + minute({a}) * 60)::BIGINT * 1000000 + microsecond({a}))");
                    let r = format!("CAST(round({us} / {unit}) * {unit} AS BIGINT)");
                    // Postgres doesn't wrap: time(0) stores 23:59:59.6 as 24:00:00.
                    (format!("CASE WHEN {r} >= 86400000000 THEN TIME '24:00:00' ELSE CAST(TIME '00:00:00' + to_microseconds({r}) AS TIME) END"), b)
                }
                ColKind::Timestamp(ty, p) => {
                    let unit = 10i64.pow(6 - p);
                    (format!("CAST(make_timestamp(CAST(round(epoch_us({a}) / {unit}) * {unit} AS BIGINT)) AS {ty})"), b)
                }
            }
        })
        .collect();
    let soft_served = t
        .soft_delete_column
        .as_ref()
        .is_some_and(|c| s.columns.iter().any(|(n, _)| n == c));
    let restore = if t.soft_delete_column.is_some() && !soft_served {
        " OR NOT b._alive"
    } else {
        ""
    };
    let changed = if mode == Mode::Reload {
        "true".to_owned()
    } else if cmp.is_empty() {
        format!("false{restore}")
    } else {
        let a = cmp
            .iter()
            .map(|(a, _)| a.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let b = cmp
            .iter()
            .map(|(_, b)| b.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        format!("({a}) IS DISTINCT FROM ({b}){restore}")
    };
    let served = s
        .columns
        .iter()
        .map(|(n, _)| format!("a.{}", ident(n)))
        .collect::<Vec<_>>()
        .join(", ");
    let pk_b = s
        .primary_key
        .iter()
        .map(|k| format!("b.{}", ident(k)))
        .collect::<Vec<_>>()
        .join(", ");
    let pk0 = ident(&s.primary_key[0]);
    let cat = lit(&s.catalog);
    c.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE {up} AS
           SELECT {served} FROM {lake} a LEFT JOIN {tgt} b ON {on} WHERE b.{pk0} IS NULL OR {changed};
         CREATE OR REPLACE TEMP TABLE {del} AS
           SELECT {pk_b}, {at}::BIGINT AS _snapshot, (SELECT snapshot_time FROM ducklake_snapshots({cat}) WHERE snapshot_id = {at}) AS _ts
           FROM {tgt} b ANTI JOIN {lake} a ON {on} WHERE b._alive;",
        up = tn("up"),
        del = tn("del"),
        lake = tn("lake"),
        tgt = tn("target"),
    ))
    .map_err(db)?;
    let n = |x: &str| -> Result<u64> {
        let v: i64 = c
            .query_row(&format!("SELECT count(*) FROM {x}"), [], |r| r.get(0))
            .map_err(db)?;
        Ok(v as u64)
    };
    Ok(NetChanges {
        upserts: format!("{prefix}_up"),
        deletes: format!("{prefix}_del"),
        touched: n(&tn("lake"))?,
        upsert_rows: n(&tn("up"))?,
        delete_rows: n(&tn("del"))?,
    })
}

/// Counts plus up to `limit` sample differences (lake vs target values), for `--dry-run`.
pub fn dry_run(
    c: &Connection,
    s: &Source,
    nc: &NetChanges,
    prefix: &str,
    limit: usize,
) -> Result<DryRun> {
    let on = s
        .primary_key
        .iter()
        .map(|k| format!("u.{k} = t.{k}", k = ident(k)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        "SELECT json_object('op', CASE WHEN t.{pk0} IS NULL THEN 'insert' ELSE 'update' END, 'lake', to_json(u), 'target', to_json(t))::VARCHAR
         FROM {up} u LEFT JOIN {tgt} t ON {on}
         UNION ALL
         SELECT json_object('op', 'delete', 'target', to_json(d))::VARCHAR FROM {del} d
         LIMIT {limit}",
        pk0 = ident(&s.primary_key[0]),
        up = ident(&nc.upserts),
        del = ident(&nc.deletes),
        tgt = ident(&format!("{prefix}_target")),
    );
    let mut st = c.prepare(&sql).map_err(db)?;
    let samples = st
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(db)?
        .map(|r| {
            r.map_err(db)
                .map(|t| serde_json::from_str(&t).unwrap_or(serde_json::Value::String(t)))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(DryRun {
        upserts: nc.upsert_rows,
        deletes: nc.delete_rows,
        samples,
    })
}

#[cfg(test)]
mod tests {
    use atlatl_core::error::ErrorClass;
    use atlatl_testkit::{LocalLake, Postgres, pg12, pg16};

    use super::*;
    use crate::head;

    fn exec_pg(dsn: &str, sql: &str) {
        let (dsn, sql) = (dsn.to_owned(), sql.to_owned());
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt")
            .block_on(async move {
                let (c, conn) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                    .await
                    .expect("connect");
                tokio::spawn(conn);
                c.batch_execute(&sql).await.expect("pg");
            });
    }

    #[test]
    fn adopt_and_reload_against_postgres() {
        let pg = pg16();
        let db_name = pg.fresh_db().expect("db");
        let dsn = pg.dsn(&db_name);
        exec_pg(
            &dsn,
            "CREATE SCHEMA dagster;
             CREATE TYPE dagster.kind AS ENUM ('ipo', 'earnings');
             CREATE TABLE dagster.e (id bigint PRIMARY KEY, kind dagster.kind, amount numeric(18,4), tags text[], deleted_at timestamptz);
             INSERT INTO dagster.e VALUES
               (1, 'ipo', 1.5, '{a}', NULL),          -- same as lake
               (2, 'ipo', 2.0, '{a}', NULL),          -- lake says earnings
               (3, 'earnings', 3.0, '{}', NULL),      -- not in lake → delete
               (4, 'ipo', 4.0, '{a}', now());         -- soft-deleted; lake has it → re-insert",
        );
        let lake = LocalLake::file().expect("lake");
        let c = lake.open("lake").expect("open");
        c.execute_batch(&format!(
            "CREATE SCHEMA lake.au;
             CREATE TABLE lake.au.e (id BIGINT, kind VARCHAR, amount DECIMAL(18,4), tags VARCHAR[]);
             INSERT INTO lake.au.e VALUES (1, 'ipo', 1.5, ['a']), (2, 'earnings', 2.0, ['a']), (4, 'ipo', 4.0, ['a']), (5, 'ipo', 5.25, ['b', 'c']);
             ATTACH '{dsn}' AS app (TYPE postgres, READ_ONLY);"
        ))
        .expect("lake");
        let at = head(&c, "lake").expect("head");
        let s = Source {
            catalog: "lake".into(),
            schema: "au".into(),
            table: "e".into(),
            primary_key: vec!["id".into()],
            columns: ["id", "kind", "amount", "tags"]
                .iter()
                .map(|c| ((*c).to_owned(), format!("\"{c}\"")))
                .collect(),
            ignore: vec![],
            filter: None,
        };
        let t = Target {
            attach: "app".into(),
            schema: "dagster".into(),
            table: "e".into(),
            soft_delete_column: Some("deleted_at".into()),
            soft_delete_bool: false,
        };
        let nc = diff(&c, &s, at, &t, Mode::Adopt, "b1").expect("adopt");
        let mut st = c.prepare("SELECT id FROM b1_up ORDER BY id").expect("q");
        let up: Vec<i64> = st
            .query_map([], |r| r.get(0))
            .expect("rows")
            .collect::<Result<_, _>>()
            .expect("rows");
        assert_eq!(up, [2, 4, 5], "changed, soft-deleted-but-present, new");
        let del: i64 = c
            .query_row("SELECT id FROM b1_del", [], |r| r.get(0))
            .expect("del");
        assert_eq!((del, nc.delete_rows), (3, 1));
        let dr = dry_run(&c, &s, &nc, "b1", 10).expect("dry");
        assert_eq!((dr.upserts, dr.deletes, dr.samples.len()), (3, 1, 4));
        assert!(dr.samples.iter().any(|x| x["op"] == "update"
            && x["lake"]["kind"] == "earnings"
            && x["target"]["kind"] == "ipo"));

        let nc = diff(&c, &s, at, &t, Mode::Reload, "b2").expect("reload");
        assert_eq!(
            (nc.upsert_rows, nc.delete_rows),
            (4, 1),
            "reload upserts every lake row"
        );

        let bad = Source {
            columns: vec![
                ("id".into(), "\"id\"".into()),
                ("nope".into(), "\"kind\"".into()),
            ],
            ..s
        };
        assert!(diff(&c, &bad, at, &t, Mode::Adopt, "b3").is_err());
    }

    fn ids(c: &Connection, nc: &NetChanges) -> Vec<i64> {
        let mut st = c
            .prepare(&format!(
                "SELECT id FROM {} ORDER BY id",
                ident(&nc.upserts)
            ))
            .expect("q");
        st.query_map([], |r| r.get(0))
            .expect("rows")
            .collect::<Result<_, _>>()
            .expect("rows")
    }

    fn unreadable_target_values_fail_loudly(pg: &Postgres) {
        let db_name = pg.fresh_db().expect("db");
        let dsn = pg.dsn(&db_name);
        exec_pg(
            &dsn,
            "CREATE SCHEMA ap;
             CREATE TABLE ap.e (id bigint PRIMARY KEY, ts timestamp, tz timestamptz, n numeric(5,2), u numeric);
             INSERT INTO ap.e VALUES
               (1, '294247-01-10 04:00:54.775806', '294247-01-10 04:00:54.775806+00', 1.5, 1.50),
               (2, 'infinity', '-infinity', NULL, NULL),
               (3, '4714-11-24 00:00:00 BC', '4714-11-24 00:00:00+00 BC', NULL, NULL),
               (4, NULL, NULL, NULL, 'NaN'),
               (5, NULL, NULL, NULL, 1.23456);",
        );
        let lake = LocalLake::file().expect("lake");
        let c = lake.open("lake").expect("open");
        c.execute_batch(&format!(
            "SET TimeZone = 'UTC';
             CREATE SCHEMA lake.au;
             CREATE TABLE lake.au.e (id BIGINT, ts TIMESTAMP, tz TIMESTAMPTZ, n DECIMAL(5,2), u DECIMAL(18,4));
             INSERT INTO lake.au.e VALUES
               (1, '294247-01-10 04:00:54.775806', '294247-01-10 04:00:54.775806+00', 1.5, 1.5),
               (2, 'infinity', '-infinity', NULL, NULL),
               (3, make_timestamp(-210866803200000000), make_timestamp(-210866803200000000)::TIMESTAMPTZ, NULL, NULL),
               (4, NULL, NULL, NULL, NULL),
               (5, NULL, NULL, NULL, 1.2346);
             ATTACH '{dsn}' AS app (TYPE postgres, READ_ONLY);"
        ))
        .expect("lake");
        let at = head(&c, "lake").expect("head");
        let s = Source {
            catalog: "lake".into(),
            schema: "au".into(),
            table: "e".into(),
            primary_key: vec!["id".into()],
            columns: ["id", "ts", "tz", "n", "u"]
                .iter()
                .map(|c| ((*c).to_owned(), format!("\"{c}\"")))
                .collect(),
            ignore: vec![],
            filter: None,
        };
        let t = Target {
            attach: "app".into(),
            schema: "ap".into(),
            table: "e".into(),
            soft_delete_column: None,
            soft_delete_bool: false,
        };
        let nc = diff(&c, &s, at, &t, Mode::Adopt, "x0").expect("adopt");
        assert_eq!(
            ids(&c, &nc),
            [4, 5],
            "edges land exactly; NaN and digits past the lake's scale differ"
        );

        let reset = "UPDATE ap.e SET ts = 'infinity', tz = '-infinity', n = NULL WHERE id = 2;";
        let fix =
            "; correct it in the target, or bootstrap with `--mode reload`, which overwrites it";
        let cases = [
            (
                "ts = '294247-01-10 04:00:54.775807'",
                format!(
                    "column `ts` of `ap.e` holds 294247-01-10 04:00:54.775807, past the last timestamp the lake holds (294247-01-10 04:00:54.775806){fix}"
                ),
            ),
            (
                "ts = '294247-01-10 04:00:54.775808'",
                format!(
                    "column `ts` of `ap.e` holds 294247-01-10 04:00:54.775808, past the last timestamp the lake holds (294247-01-10 04:00:54.775806){fix}"
                ),
            ),
            (
                "ts = '294276-12-31 23:59:59.999999'",
                format!(
                    "column `ts` of `ap.e` holds 294276-12-31 23:59:59.999999, past the last timestamp the lake holds (294247-01-10 04:00:54.775806){fix}"
                ),
            ),
            (
                "tz = '294247-01-10 04:00:54.775807+00'",
                format!(
                    "column `tz` of `ap.e` holds 294247-01-10 04:00:54.775807+00, past the last timestamp the lake holds (294247-01-10 04:00:54.775806+00){fix}"
                ),
            ),
            (
                "n = 'NaN'",
                format!("column `n` of `ap.e` holds NaN, which a decimal can't hold{fix}"),
            ),
        ];
        for (k, (set, want)) in cases.iter().enumerate() {
            exec_pg(
                &dsn,
                &format!("{reset} UPDATE ap.e SET {set} WHERE id = 2;"),
            );
            let e = diff(&c, &s, at, &t, Mode::Adopt, &format!("x{k}")).expect_err(set);
            assert_eq!(
                (e.class, e.message.as_str()),
                (ErrorClass::Data, want.as_str()),
                "{set}"
            );
            let nc = diff(&c, &s, at, &t, Mode::Reload, &format!("r{k}")).expect(set);
            assert_eq!(nc.upsert_rows, 5, "reload overwrites {set}");
            let col = set.split(' ').next().expect("col");
            let ig = Source {
                ignore: vec![col.into()],
                ..s.clone()
            };
            let nc = diff(&c, &ig, at, &t, Mode::Adopt, &format!("i{k}")).expect(set);
            assert_eq!(ids(&c, &nc), [4, 5], "ignored {col} doesn't sway the diff");
        }

        exec_pg(
            &dsn,
            &format!(
                "{reset} CREATE TABLE ap.k (id timestamp PRIMARY KEY);
                 INSERT INTO ap.k VALUES ('294250-01-01 00:00:00');"
            ),
        );
        let k = Source {
            columns: vec![("id".into(), "\"ts\"".into())],
            ..s.clone()
        };
        let e = diff(
            &c,
            &k,
            at,
            &Target {
                table: "k".into(),
                ..t.clone()
            },
            Mode::Reload,
            "k0",
        )
        .expect_err("key");
        assert_eq!(
            e.message,
            "column `id` of `ap.k` holds 294250-01-01 00:00:00, past the last timestamp the lake holds (294247-01-10 04:00:54.775806); correct it in the target"
        );
    }

    /// The scanner reads `numeric(p>38)` as DOUBLE and a domain as VARCHAR: values differing
    /// past a float's precision still differ, and `1.50` still equals `1.5`.
    fn wide_numerics_compare_exactly(pg: &Postgres) {
        let db_name = pg.fresh_db().expect("db");
        let dsn = pg.dsn(&db_name);
        exec_pg(
            &dsn,
            "CREATE SCHEMA ap;
             CREATE DOMAIN ap.amount AS numeric;
             CREATE DOMAIN ap.wide AS numeric(50,2);
             CREATE TABLE ap.e (id bigint PRIMARY KEY, w numeric(50,2), d ap.amount, x ap.wide);
             INSERT INTO ap.e VALUES
               (1, 1234567890123456789.01, 1234567890123456789.01, 1.5),
               (2, 1234567890123456789.02, 1.5, 1.5),
               (3, 1.5, 1234567890123456789.02, 1.5),
               (4, 1.5, 1.5, 1234567890123456789.02);",
        );
        let lake = LocalLake::file().expect("lake");
        let c = lake.open("lake").expect("open");
        c.execute_batch(&format!(
            "CREATE SCHEMA lake.au;
             CREATE TABLE lake.au.e (id BIGINT, w DECIMAL(38,2), d DECIMAL(38,2), x DECIMAL(38,2));
             INSERT INTO lake.au.e VALUES
               (1, 1234567890123456789.01, 1234567890123456789.01, 1.5),
               (2, 1234567890123456789.01, 1.5, 1.5),
               (3, 1.5, 1234567890123456789.01, 1.5),
               (4, 1.5, 1.5, 1234567890123456789.01);
             ATTACH '{dsn}' AS app (TYPE postgres, READ_ONLY);"
        ))
        .expect("lake");
        let mut scanned = types(&c, "app.ap.e").expect("describe");
        scanned.sort();
        assert_eq!(
            scanned.iter().map(|(_, t)| t.as_str()).collect::<Vec<_>>(),
            ["VARCHAR", "BIGINT", "DOUBLE", "VARCHAR"],
            "{scanned:?}"
        );
        let at = head(&c, "lake").expect("head");
        let s = Source {
            catalog: "lake".into(),
            schema: "au".into(),
            table: "e".into(),
            primary_key: vec!["id".into()],
            columns: ["id", "w", "d", "x"]
                .iter()
                .map(|c| ((*c).to_owned(), format!("\"{c}\"")))
                .collect(),
            ignore: vec![],
            filter: None,
        };
        let t = Target {
            attach: "app".into(),
            schema: "ap".into(),
            table: "e".into(),
            soft_delete_column: None,
            soft_delete_bool: false,
        };
        let nc = diff(&c, &s, at, &t, Mode::Adopt, "w0").expect("adopt");
        assert_eq!(ids(&c, &nc), [2, 3, 4]);
    }

    #[test]
    fn wide_numerics_compare_exactly_pg12() {
        wide_numerics_compare_exactly(pg12());
    }

    #[test]
    fn wide_numerics_compare_exactly_pg16() {
        wide_numerics_compare_exactly(pg16());
    }

    #[test]
    fn unreadable_target_values_fail_loudly_pg12() {
        unreadable_target_values_fail_loudly(pg12());
    }

    #[test]
    fn unreadable_target_values_fail_loudly_pg16() {
        unreadable_target_values_fail_loudly(pg16());
    }
}
