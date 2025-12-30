use bytes::{Buf, Bytes};

use crate::error::{PgWireError, Result};
use crate::lsn::Lsn;

#[derive(Debug, Clone)]
pub enum ReplicationCopyData {
    XLogData {
        wal_start: Lsn,
        wal_end: Lsn,
        server_time_micros: i64,
        data: Bytes,
    },
    KeepAlive {
        wal_end: Lsn,
        server_time_micros: i64,
        reply_requested: bool,
    },
}

pub fn parse_copy_data(payload: Bytes) -> Result<ReplicationCopyData> {
    if payload.is_empty() {
        return Err(PgWireError::Protocol("empty CopyData payload".into()));
    }
    let mut b = payload.clone();
    let kind = b.get_u8();
    match kind {
        b'w' => {
            if b.remaining() < 8 + 8 + 8 {
                return Err(PgWireError::Protocol("XLogData payload too short".into()));
            }
            let wal_start = Lsn(b.get_i64() as u64);
            let wal_end = Lsn(b.get_i64() as u64);
            let server_time_micros = b.get_i64();
            let data = b.copy_to_bytes(b.remaining());
            Ok(ReplicationCopyData::XLogData {
                wal_start,
                wal_end,
                server_time_micros,
                data,
            })
        }
        b'k' => {
            if b.remaining() < 8 + 8 + 1 {
                return Err(PgWireError::Protocol("KeepAlive payload too short".into()));
            }
            let wal_end = Lsn(b.get_i64() as u64);
            let server_time_micros = b.get_i64();
            let reply_requested = b.get_u8() == 1;
            Ok(ReplicationCopyData::KeepAlive {
                wal_end,
                server_time_micros,
                reply_requested,
            })
        }
        _ => Err(PgWireError::Protocol(format!(
            "unknown CopyData kind: {kind}"
        ))),
    }
}

pub fn encode_standby_status_update(
    applied: Lsn,
    client_time_micros: i64,
    reply_requested: bool,
) -> Vec<u8> {
    // 'r' + write + flush + apply + client_time + reply_requested
    let mut out = Vec::with_capacity(1 + 8 * 4 + 1);
    out.push(b'r');
    out.extend_from_slice(&(applied.0 as i64).to_be_bytes());
    out.extend_from_slice(&(applied.0 as i64).to_be_bytes());
    out.extend_from_slice(&(applied.0 as i64).to_be_bytes());
    out.extend_from_slice(&client_time_micros.to_be_bytes());
    out.push(if reply_requested { 1 } else { 0 });
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn parse_xlogdata() {
        let mut v = Vec::new();
        v.push(b'w');
        v.extend_from_slice(&(1i64).to_be_bytes());
        v.extend_from_slice(&(2i64).to_be_bytes());
        v.extend_from_slice(&(3i64).to_be_bytes());
        v.extend_from_slice(b"abc");
        let msg = parse_copy_data(Bytes::from(v)).unwrap();
        match msg {
            ReplicationCopyData::XLogData {
                wal_start,
                wal_end,
                server_time_micros,
                data,
            } => {
                assert_eq!(wal_start.0, 1);
                assert_eq!(wal_end.0, 2);
                assert_eq!(server_time_micros, 3);
                assert_eq!(&data[..], b"abc");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn encode_status_update_has_expected_length() {
        let p = encode_standby_status_update(Lsn(42), 7, true);
        assert_eq!(p.len(), 1 + 8 * 4 + 1);
        assert_eq!(p[0], b'r');
        assert_eq!(p[p.len() - 1], 1);
    }
}
