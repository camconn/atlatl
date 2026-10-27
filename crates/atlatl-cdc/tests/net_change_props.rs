//! M1.8: for random histories, applying diff(C, E) to the table as of C yields the table as of E
//! (projected to served columns), whatever the writer did: updates, delete+insert rewrites of
//! unchanged rows, full rewrites, compaction.

use atlatl_cdc::{Source, Window, head, ident, net_changes};
use atlatl_testkit::LocalLake;
use duckdb::Connection;
use proptest::prelude::*;

#[derive(Clone, Debug)]
enum Op {
    Insert(i64, u8),
    Update(i64, u8),
    Delete(i64),
    /// dbt-style: delete the key and re-insert it, maybe unchanged.
    Rewrite(i64, Option<u8>),
    /// `DELETE` everything and re-insert the same rows (a `table` materialisation).
    RewriteAll,
    /// Touch a non-served column only.
    Churn(i64),
    Compact,
}

fn op() -> impl Strategy<Value = Op> {
    let k = 0i64..12;
    prop_oneof![
        (k.clone(), any::<u8>()).prop_map(|(k, v)| Op::Insert(k, v)),
        (k.clone(), any::<u8>()).prop_map(|(k, v)| Op::Update(k, v)),
        k.clone().prop_map(Op::Delete),
        (k.clone(), proptest::option::of(any::<u8>())).prop_map(|(k, v)| Op::Rewrite(k, v)),
        Just(Op::RewriteAll),
        k.prop_map(Op::Churn),
        Just(Op::Compact),
    ]
}

fn apply(c: &Connection, o: &Op) {
    let sql = match o {
        Op::Insert(k, v) => format!(
            "INSERT INTO lake.p.t SELECT {k}, {v}, 0 WHERE NOT EXISTS (SELECT 1 FROM lake.p.t WHERE k = {k})"
        ),
        Op::Update(k, v) => format!("UPDATE lake.p.t SET v = {v} WHERE k = {k}"),
        Op::Delete(k) => format!("DELETE FROM lake.p.t WHERE k = {k}"),
        Op::Rewrite(k, v) => {
            let val = v
                .map(|v| v.to_string())
                .unwrap_or_else(|| format!("(SELECT v FROM lake.p.t WHERE k = {k})"));
            format!(
                "BEGIN; CREATE OR REPLACE TEMP TABLE r AS SELECT k, {val} AS v, n FROM lake.p.t WHERE k = {k};
                 DELETE FROM lake.p.t WHERE k = {k}; INSERT INTO lake.p.t SELECT * FROM r; COMMIT;"
            )
        }
        Op::RewriteAll => "BEGIN; CREATE OR REPLACE TEMP TABLE r AS SELECT * FROM lake.p.t;
             DELETE FROM lake.p.t; INSERT INTO lake.p.t SELECT * FROM r; COMMIT;"
            .into(),
        Op::Churn(k) => format!("UPDATE lake.p.t SET n = n + 1 WHERE k = {k}"),
        Op::Compact => {
            "CALL ducklake_rewrite_data_files('lake', 't', schema => 'p', delete_threshold => 0.0)"
                .into()
        }
    };
    c.execute_batch(&sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn state(c: &Connection, sql: &str) -> Vec<(i64, i64)> {
    let mut s = c.prepare(sql).expect("prepare");
    s.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .expect("rows")
        .collect::<Result<_, _>>()
        .expect("rows")
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 40, ..ProptestConfig::default() })]

    #[test]
    fn apply_diff_to_c_gives_e(before in proptest::collection::vec(op(), 0..8), window in proptest::collection::vec(op(), 1..10)) {
        let lake = LocalLake::file().expect("lake");
        let c = lake.open("lake").expect("open");
        c.execute_batch("CREATE SCHEMA lake.p; CREATE TABLE lake.p.t (k BIGINT, v INTEGER, n INTEGER);").expect("t");
        for o in &before {
            apply(&c, o);
        }
        let from = head(&c, "lake").expect("head");
        for o in &window {
            apply(&c, o);
        }
        let to = head(&c, "lake").expect("head");
        let s = Source {
            catalog: "lake".into(),
            schema: "p".into(),
            table: "t".into(),
            primary_key: vec!["k".into()],
            columns: vec![("k".into(), "\"k\"".into()), ("v".into(), "\"v\"".into())],
            ignore: vec![],
            filter: None,
        };
        if from == to {
            return Ok(());
        }
        let nc = net_changes(&c, &s, Window { from, to }, "p").expect("diff");
        c.execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE target AS SELECT k, v FROM (SELECT * FROM lake.p.t AT (VERSION => {from}));
             DELETE FROM target WHERE k IN (SELECT k FROM {del});
             DELETE FROM target WHERE k IN (SELECT k FROM {up});
             INSERT INTO target SELECT k, v FROM {up};",
            del = ident(&nc.deletes),
            up = ident(&nc.upserts),
        ))
        .expect("apply diff");
        let got = state(&c, "SELECT k, v FROM target ORDER BY k");
        let want = state(&c, &format!("SELECT k, v FROM (SELECT * FROM lake.p.t AT (VERSION => {to})) ORDER BY k"));
        prop_assert_eq!(got, want);
        // Only real changes are sent: every upsert differs from C, every delete existed at C.
        let wasted: i64 = c.query_row(
            &format!(
                "SELECT count(*) FROM {up} u JOIN (SELECT * FROM lake.p.t AT (VERSION => {from})) b USING (k) WHERE u.v IS NOT DISTINCT FROM b.v",
                up = ident(&nc.upserts)
            ),
            [],
            |r| r.get(0),
        ).expect("wasted");
        prop_assert_eq!(wasted, 0);
    }
}
