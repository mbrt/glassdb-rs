//! The unified transaction object: the canonical `_t/<txid>` body (ADR-019).
//!
//! Values live *only* in this object. While **pending** it is small (lease +
//! lock intentions); a single CAS flips it to **committed**, attaching the full
//! value map — the commit point. **Aborted** is the wound/self-abort terminal.
//! Unlike v1, the status and timestamp are authoritative in the *body* (there
//! are no tags), and the timestamp doubles as the lease while pending (ADR-021).
//!
//! The in-memory representation reuses [`TxLog`] (id, status, timestamp, the
//! value map as `writes`, and the lock intentions as `locks`); this module is
//! just the v2 body codec, sharing the canonical `TransactionLog` encoding with
//! [`crate::tlogger`].

use crate::error::StorageError;
use crate::tlogger::{TxLog, TxWrite, decode_tx_log, marshal_log};

/// Encodes a transaction object to its canonical protobuf body (the CAS unit).
///
/// The timestamp must be set: it is the commit time once committed and the lease
/// anchor while pending (ADR-021), so it is never defaulted from a hidden clock
/// here — the engine sets it explicitly to keep encoding deterministic.
pub fn encode(obj: &TxLog) -> Result<Vec<u8>, StorageError> {
    let ts = obj
        .timestamp
        .ok_or_else(|| StorageError::other("transaction object has no timestamp/lease"))?;
    marshal_log(obj, ts)
}

/// Decodes a transaction object from its protobuf body. The status and timestamp
/// are read from the body, not tags (ADR-019).
pub fn decode(id: &glassdb_data::TxId, buf: &[u8]) -> Result<TxLog, StorageError> {
    decode_tx_log(id, buf)
}

/// Returns the write the transaction recorded for the full key `path`, or `None`
/// if it wrote nothing there. This is how a reader materializes a key's value
/// from the committed writer's object.
pub fn find_write<'a>(obj: &'a TxLog, path: &str) -> Option<&'a TxWrite> {
    obj.writes.iter().find(|w| w.path == path)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, UNIX_EPOCH};

    use glassdb_data::{TxId, paths};

    use super::*;
    use crate::lock::LockType;
    use crate::tlogger::{PathLock, TxCommitStatus, TxWrite};

    fn committed_object() -> TxLog {
        let key_path = paths::from_key("db/c", b"hello");
        TxLog {
            id: TxId::from_bytes(vec![1, 2, 3, 4]),
            timestamp: Some(UNIX_EPOCH + Duration::from_secs(1_700_000_000)),
            status: TxCommitStatus::Ok,
            writes: vec![TxWrite {
                path: key_path,
                value: Arc::from(&b"world"[..]),
                deleted: false,
                prev_writer: TxId::default(),
            }],
            locks: Vec::new(),
        }
    }

    #[test]
    fn committed_round_trip() {
        let obj = committed_object();
        let decoded = decode(&obj.id, &encode(&obj).unwrap()).unwrap();
        assert_eq!(decoded.status, TxCommitStatus::Ok);
        assert_eq!(decoded.writes, obj.writes);
        assert_eq!(decoded.timestamp, obj.timestamp);
    }

    #[test]
    fn pending_round_trip_carries_lease_and_locks() {
        let key_path = paths::from_key("db/c", b"hello");
        let obj = TxLog {
            id: TxId::from_bytes(vec![9]),
            timestamp: Some(UNIX_EPOCH + Duration::from_secs(42)),
            status: TxCommitStatus::Pending,
            writes: Vec::new(),
            locks: vec![PathLock {
                path: key_path,
                typ: LockType::Write,
            }],
        };
        let decoded = decode(&obj.id, &encode(&obj).unwrap()).unwrap();
        assert_eq!(decoded.status, TxCommitStatus::Pending);
        // The lease (timestamp) survives the round trip with sub-second loss
        // tolerated by the body encoding (full nanos are preserved here).
        assert_eq!(decoded.timestamp, obj.timestamp);
        assert_eq!(decoded.locks, obj.locks);
    }

    #[test]
    fn aborted_round_trip() {
        let obj = TxLog {
            id: TxId::from_bytes(vec![7]),
            timestamp: Some(UNIX_EPOCH + Duration::from_secs(1)),
            status: TxCommitStatus::Aborted,
            writes: Vec::new(),
            locks: Vec::new(),
        };
        let decoded = decode(&obj.id, &encode(&obj).unwrap()).unwrap();
        assert_eq!(decoded.status, TxCommitStatus::Aborted);
    }

    #[test]
    fn encode_requires_timestamp() {
        let mut obj = committed_object();
        obj.timestamp = None;
        assert!(encode(&obj).is_err());
    }

    #[test]
    fn find_write_locates_key() {
        let obj = committed_object();
        let key_path = paths::from_key("db/c", b"hello");
        assert_eq!(
            find_write(&obj, &key_path).unwrap().value.as_ref(),
            b"world"
        );
        assert!(find_write(&obj, "db/c/_k/missing").is_none());
    }

    // Golden vector: a fixed committed object must always encode to these exact
    // bytes. Changing the on-disk format must break this test.
    #[test]
    fn golden_encoding() {
        let got = encode(&committed_object()).unwrap();
        let want = [
            0x0a, 0x06, 0x08, 0x80, 0xe2, 0xcf, 0xaa, 0x06, 0x10, 0x01, 0x1a, 0x1d, 0x0a, 0x04,
            0x64, 0x62, 0x2f, 0x63, 0x12, 0x13, 0x0a, 0x0a, 0x5f, 0x6b, 0x2f, 0x50, 0x36, 0x4b,
            0x67, 0x51, 0x36, 0x77, 0x12, 0x05, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0x1a, 0x00,
        ];
        assert_eq!(got, want, "tx-object encoding drifted: {got:02x?}");
    }
}
