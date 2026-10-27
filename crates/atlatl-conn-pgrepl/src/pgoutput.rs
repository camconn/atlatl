//! pgoutput messages, protocol versions 1 and 2 with text or binary tuple values
//! (https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html).
//! Logical messages and two-phase commit are not requested, so they are rejected.

use atlatl_core::AtlatlError;
use atlatl_core::error::Result;
use bytes::Bytes;

use crate::lsn::Lsn;
use crate::wire::{cstr, get_i16, get_i32, get_u8, get_u32, get_u64, take};

#[derive(Debug, Clone, PartialEq)]
pub enum Col {
    Null,
    /// An unchanged TOASTed value: the old value is not sent.
    Unchanged,
    Text(Bytes),
    /// The type's binary send format (`binary 'true'`).
    Binary(Bytes),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub key: bool,
    pub name: String,
    pub type_oid: u32,
    pub typmod: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Relation {
    pub oid: u32,
    pub namespace: String,
    pub name: String,
    /// `d` default, `n` nothing, `f` full, `i` index.
    pub replica_identity: u8,
    pub columns: Vec<Column>,
}

#[derive(Debug, PartialEq)]
pub enum Msg {
    Begin {
        final_lsn: Lsn,
        xid: u32,
    },
    Commit {
        commit_lsn: Lsn,
        end_lsn: Lsn,
    },
    Origin,
    Relation(Relation),
    Type,
    Insert {
        rel: u32,
        new: Vec<Col>,
    },
    Update {
        rel: u32,
        /// `K` (key columns only) or `O` (whole old row, REPLICA IDENTITY FULL).
        old: Option<(u8, Vec<Col>)>,
        new: Vec<Col>,
    },
    Delete {
        rel: u32,
        old: (u8, Vec<Col>),
    },
    Truncate {
        rels: Vec<u32>,
    },
    /// A block of an in-progress transaction (`streaming 'on'`); its changes carry an xid.
    StreamStart {
        xid: u32,
        first: bool,
    },
    StreamStop,
    StreamCommit {
        xid: u32,
        commit_lsn: Lsn,
        end_lsn: Lsn,
    },
    /// `subxid == xid` aborts the whole transaction, otherwise just that subtransaction.
    StreamAbort {
        xid: u32,
        subxid: u32,
    },
}

fn bad(m: impl Into<String>) -> AtlatlError {
    AtlatlError::internal(format!("pgoutput: {}", m.into())).code("pgoutput")
}

fn tuple(b: &mut Bytes) -> Result<Vec<Col>> {
    let n = get_i16(b)?;
    let mut cols = Vec::with_capacity(n.max(0) as usize);
    for _ in 0..n {
        cols.push(match get_u8(b)? {
            b'n' => Col::Null,
            b'u' => Col::Unchanged,
            b't' => {
                let len = get_i32(b)?;
                if len < 0 {
                    return Err(bad("negative value length"));
                }
                Col::Text(take(b, len as usize)?)
            }
            b'b' => {
                let len = get_i32(b)?;
                if len < 0 {
                    return Err(bad("negative value length"));
                }
                Col::Binary(take(b, len as usize)?)
            }
            k => return Err(bad(format!("unknown tuple column kind {k}"))),
        });
    }
    Ok(cols)
}

/// Decodes one message. Inside a stream block (`in_stream`), changes, relations and types
/// start with the xid of the (sub)transaction they belong to, which is returned with them.
pub fn decode(mut b: Bytes, in_stream: bool) -> Result<(Option<u32>, Msg)> {
    let tag = get_u8(&mut b)?;
    let xid = if in_stream && matches!(tag, b'R' | b'Y' | b'I' | b'U' | b'D' | b'T') {
        Some(get_u32(&mut b)?)
    } else {
        None
    };
    let msg = match tag {
        b'B' => {
            let final_lsn = Lsn(get_u64(&mut b)?);
            let _ts = get_u64(&mut b)?;
            Msg::Begin {
                final_lsn,
                xid: get_u32(&mut b)?,
            }
        }
        b'C' => {
            let _flags = get_u8(&mut b)?;
            let commit_lsn = Lsn(get_u64(&mut b)?);
            let end_lsn = Lsn(get_u64(&mut b)?);
            Msg::Commit {
                commit_lsn,
                end_lsn,
            }
        }
        b'O' => Msg::Origin,
        b'R' => {
            let oid = get_u32(&mut b)?;
            let namespace = cstr(&mut b)?;
            let name = cstr(&mut b)?;
            let replica_identity = get_u8(&mut b)?;
            let n = get_i16(&mut b)?;
            let mut columns = Vec::with_capacity(n.max(0) as usize);
            for _ in 0..n {
                columns.push(Column {
                    key: get_u8(&mut b)? & 1 == 1,
                    name: cstr(&mut b)?,
                    type_oid: get_u32(&mut b)?,
                    typmod: get_i32(&mut b)?,
                });
            }
            Msg::Relation(Relation {
                oid,
                namespace,
                name,
                replica_identity,
                columns,
            })
        }
        b'Y' => Msg::Type,
        b'I' => {
            let rel = get_u32(&mut b)?;
            if get_u8(&mut b)? != b'N' {
                return Err(bad("insert without N"));
            }
            Msg::Insert {
                rel,
                new: tuple(&mut b)?,
            }
        }
        b'U' => {
            let rel = get_u32(&mut b)?;
            let mut kind = get_u8(&mut b)?;
            let old = if kind == b'K' || kind == b'O' {
                let t = tuple(&mut b)?;
                let o = Some((kind, t));
                kind = get_u8(&mut b)?;
                o
            } else {
                None
            };
            if kind != b'N' {
                return Err(bad("update without N"));
            }
            Msg::Update {
                rel,
                old,
                new: tuple(&mut b)?,
            }
        }
        b'D' => {
            let rel = get_u32(&mut b)?;
            let kind = get_u8(&mut b)?;
            if kind != b'K' && kind != b'O' {
                return Err(bad("delete without K or O"));
            }
            Msg::Delete {
                rel,
                old: (kind, tuple(&mut b)?),
            }
        }
        b'T' => {
            let n = get_u32(&mut b)?;
            let _options = get_u8(&mut b)?;
            let mut rels = Vec::with_capacity(n.min(1024) as usize);
            for _ in 0..n {
                rels.push(get_u32(&mut b)?);
            }
            Msg::Truncate { rels }
        }
        b'S' => Msg::StreamStart {
            xid: get_u32(&mut b)?,
            first: get_u8(&mut b)? == 1,
        },
        b'E' => Msg::StreamStop,
        b'c' => {
            let xid = get_u32(&mut b)?;
            let _flags = get_u8(&mut b)?;
            let commit_lsn = Lsn(get_u64(&mut b)?);
            let end_lsn = Lsn(get_u64(&mut b)?);
            let _ts = get_u64(&mut b)?;
            Msg::StreamCommit {
                xid,
                commit_lsn,
                end_lsn,
            }
        }
        b'A' => Msg::StreamAbort {
            xid: get_u32(&mut b)?,
            subxid: get_u32(&mut b)?,
        },
        b'M' | b'P' | b'K' | b'r' | b'p' => {
            return Err(bad(format!(
                "message {:?} needs a protocol option atlatl does not request",
                tag as char
            )));
        }
        t => return Err(bad(format!("unknown message {:?}", t as char))),
    };
    Ok((xid, msg))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    fn relation() -> Bytes {
        let mut r = BytesMut::new();
        r.put_u8(b'R');
        r.put_u32(16384);
        r.put_slice(b"public\0t\0");
        r.put_u8(b'd');
        r.put_i16(2);
        r.put_u8(1);
        r.put_slice(b"id\0");
        r.put_u32(23);
        r.put_i32(-1);
        r.put_u8(0);
        r.put_slice(b"v\0");
        r.put_u32(25);
        r.put_i32(-1);
        r.freeze()
    }

    #[test]
    fn relation_and_changes() {
        let (_, Msg::Relation(rel)) = decode(relation(), false).expect("relation") else {
            panic!("not a relation")
        };
        assert_eq!((rel.namespace.as_str(), rel.name.as_str()), ("public", "t"));
        assert!(rel.columns[0].key && !rel.columns[1].key);
        assert_eq!(rel.columns[1].type_oid, 25);

        let mut i = BytesMut::new();
        i.put_u8(b'I');
        i.put_u32(16384);
        i.put_u8(b'N');
        i.put_i16(2);
        i.put_u8(b't');
        i.put_i32(1);
        i.put_slice(b"7");
        i.put_u8(b'n');
        assert_eq!(
            decode(i.freeze(), false).expect("insert").1,
            Msg::Insert {
                rel: 16384,
                new: vec![Col::Text(Bytes::from_static(b"7")), Col::Null]
            }
        );

        let mut u = BytesMut::new();
        u.put_u8(b'U');
        u.put_u32(16384);
        u.put_u8(b'K');
        u.put_i16(2);
        u.put_u8(b't');
        u.put_i32(1);
        u.put_slice(b"2");
        u.put_u8(b'n');
        u.put_u8(b'N');
        u.put_i16(2);
        u.put_u8(b't');
        u.put_i32(2);
        u.put_slice(b"10");
        u.put_u8(b'u');
        let (_, Msg::Update { old, new, .. }) = decode(u.freeze(), false).expect("update") else {
            panic!("not an update")
        };
        assert_eq!(old.expect("old").0, b'K');
        assert_eq!(new[1], Col::Unchanged);

        let mut c = BytesMut::new();
        c.put_u8(b'C');
        c.put_u8(0);
        c.put_u64(0x100);
        c.put_u64(0x180);
        c.put_u64(0);
        assert_eq!(
            decode(c.freeze(), false).expect("commit").1,
            Msg::Commit {
                commit_lsn: Lsn(0x100),
                end_lsn: Lsn(0x180)
            }
        );

        let mut t = BytesMut::new();
        t.put_u8(b'T');
        t.put_u32(2);
        t.put_u8(0);
        t.put_u32(1);
        t.put_u32(2);
        assert_eq!(
            decode(t.freeze(), false).expect("truncate").1,
            Msg::Truncate { rels: vec![1, 2] }
        );
    }

    #[test]
    fn malformed_messages_are_errors() {
        let r = relation();
        for cut in 1..r.len() {
            assert!(decode(r.slice(..cut), false).is_err(), "cut at {cut}");
        }
        let mut i = BytesMut::new();
        i.put_u8(b'I');
        i.put_u32(1);
        i.put_u8(b'N');
        i.put_i16(1);
        i.put_u8(b't');
        i.put_i32(10);
        i.put_slice(b"abc");
        assert!(decode(i.freeze(), false).is_err());
        assert!(decode(Bytes::from_static(b"M\x01"), false).is_err());
        assert!(decode(Bytes::from_static(b"Z"), false).is_err());
        assert!(decode(Bytes::from_static(b"S\0\0\0"), false).is_err());
    }

    #[test]
    fn streamed_changes_carry_their_xid() {
        assert_eq!(
            decode(Bytes::from_static(b"S\0\0\x02\x00\x01"), false)
                .expect("start")
                .1,
            Msg::StreamStart {
                xid: 512,
                first: true
            }
        );
        let mut i = BytesMut::new();
        i.put_u8(b'I');
        i.put_u32(513);
        i.put_u32(16384);
        i.put_u8(b'N');
        i.put_i16(2);
        i.put_u8(b'b');
        i.put_i32(4);
        i.put_i32(7);
        i.put_u8(b'u');
        let i = i.freeze();
        assert_eq!(
            decode(i.clone(), true).expect("insert"),
            (
                Some(513),
                Msg::Insert {
                    rel: 16384,
                    new: vec![
                        Col::Binary(Bytes::from_static(&[0, 0, 0, 7])),
                        Col::Unchanged
                    ]
                }
            )
        );
        assert!(decode(i, false).is_err(), "xid read as the relation");

        let mut r = BytesMut::new();
        r.put_u8(b'R');
        r.put_u32(513);
        r.put_slice(&relation()[1..]);
        let (xid, Msg::Relation(rel)) = decode(r.freeze(), true).expect("relation") else {
            panic!("not a relation")
        };
        assert_eq!((xid, rel.oid), (Some(513), 16384));

        let mut c = BytesMut::new();
        c.put_u8(b'c');
        c.put_u32(512);
        c.put_u8(0);
        c.put_u64(0x100);
        c.put_u64(0x180);
        c.put_u64(0);
        assert_eq!(
            decode(c.freeze(), false).expect("commit").1,
            Msg::StreamCommit {
                xid: 512,
                commit_lsn: Lsn(0x100),
                end_lsn: Lsn(0x180)
            }
        );
        assert_eq!(
            decode(Bytes::from_static(b"A\0\0\x02\0\0\0\x02\x05"), false)
                .expect("abort")
                .1,
            Msg::StreamAbort {
                xid: 512,
                subxid: 517
            }
        );
        assert_eq!(
            decode(Bytes::from_static(b"E"), true).expect("stop").1,
            Msg::StreamStop
        );
    }
}
