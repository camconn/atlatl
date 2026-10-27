//! Postgres logical-replication CDC into the lake (08 §10, 06 §9): owned slots and
//! publications, our own pgoutput client and decoder (v1, or v2 with streaming and
//! binary values on PG 14+), bounded windows staged as text, applied to the lake by primary
//! key in one transaction with the LSN cursor, and the slot confirmed only after that commit.

pub mod apply;
mod binary;
pub mod config;
pub mod lsn;
mod pgoutput;
pub mod slot;
pub mod source;
mod stage;
pub mod types;
mod wire;

pub use apply::{Applied, StreamOutcome, Window};
pub use lsn::Lsn;
pub use slot::{Decommissioned, SlotPolicy, SlotStats, decommission, slot_name, slot_stats};
pub use source::{
    Limits, PgChangeSource, ReplicationSpec, RunReport, StopReason, StreamSpec, WindowSummary,
    run_once,
};

#[cfg(test)]
mod send {
    fn is_send<T: Send>(_: &T) {}

    #[allow(dead_code)]
    async fn futures_are_send(src: &mut crate::PgChangeSource, duck: duckdb::Connection) {
        let f = src.changes(duck, "lake", "run");
        is_send(&f);
        let _ = f.await.map(|w| is_send(&w));
        is_send(&src.confirm(crate::Lsn::ZERO));
    }
}
