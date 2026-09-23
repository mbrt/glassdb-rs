//! The unified transaction record for a transaction identity.
//! (ADR-019/035).
//!
//! Values live *only* in this record. While **pending** it is small (lease +
//! lock intentions); a single CAS flips it to **committed**, attaching the full
//! value map — the commit point. **Aborted** is the wound/self-abort terminal.
//! Unlike v1, the status and timestamp are authoritative in the *body* (there
//! are no tags), and the timestamp doubles as the lease while pending (ADR-021).
//!
//! The in-memory representation reuses [`TxRecord`] (id, status, timestamp, the
//! value map as `writes`, and the lock intentions as `locks`); this module is
//! the public body facade over the canonical transaction-record codec.

use crate::error::StorageError;
use glassdb_data::LogicalKey;

use crate::transaction::{TxCommitStatus, TxRecord, TxRecordCodec, TxWrite};

/// Encodes a transaction record to its canonical protobuf body (the CAS unit).
///
/// The timestamp must be set: it is the commit time once committed and the lease
/// anchor while pending (ADR-021), so it is never defaulted from a hidden clock
/// here — the engine sets it explicitly to keep encoding deterministic.
pub fn encode(record: &TxRecord) -> Result<Vec<u8>, StorageError> {
    record
        .timestamp
        .ok_or_else(|| StorageError::other("transaction record has no timestamp/lease"))?;
    TxRecordCodec::encode(record)
}

/// Decodes a transaction record from its protobuf body. The status and timestamp
/// are read from the body, not tags (ADR-019).
pub fn decode(
    db_prefix: &str,
    id: &glassdb_data::TxId,
    buf: &[u8],
) -> Result<TxRecord, StorageError> {
    TxRecordCodec::decode(db_prefix, id, buf)
}

/// Decodes only the transaction status without requiring relocation context.
pub fn status(buf: &[u8]) -> Result<TxCommitStatus, StorageError> {
    TxRecordCodec::decode_status(buf)
}

/// Returns the write the transaction recorded for `key`, or `None`
/// if it wrote nothing there. This is how a reader materializes a key's value
/// from the committed writer's record.
pub fn find_write<'a>(record: &'a TxRecord, key: &LogicalKey) -> Option<&'a TxWrite> {
    record.writes.iter().find(|write| &write.key == key)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, UNIX_EPOCH};

    use glassdb_data::{CollectionAddress, LogicalKey, TxId};

    use super::*;
    use crate::lock::LockType;
    use crate::transaction::{TxCommitStatus, TxLock, TxWrite};

    fn key(key: &[u8]) -> LogicalKey {
        LogicalKey::new(CollectionAddress::root("db"), key)
    }

    fn committed_record() -> TxRecord {
        TxRecord {
            id: TxId::from_bytes(vec![1, 2, 3, 4]),
            timestamp: Some(UNIX_EPOCH + Duration::from_secs(1_700_000_000)),
            status: TxCommitStatus::Ok,
            writes: vec![TxWrite {
                key: key(b"hello"),
                value: Arc::from(&b"world"[..]),
                deleted: false,
                prev_writer: TxId::default(),
            }],
            locks: Vec::new(),
            collection_changes: Vec::new(),
            prepared_collections: Vec::new(),
        }
    }

    #[test]
    fn committed_round_trip() {
        let record = committed_record();
        let decoded = decode("db", &record.id, &encode(&record).unwrap()).unwrap();
        assert_eq!(decoded.status, TxCommitStatus::Ok);
        assert_eq!(decoded.writes, record.writes);
        assert_eq!(decoded.timestamp, record.timestamp);
    }

    #[test]
    fn pending_round_trip_carries_lease_and_locks() {
        let record = TxRecord {
            id: TxId::from_bytes(vec![9]),
            timestamp: Some(UNIX_EPOCH + Duration::from_secs(42)),
            status: TxCommitStatus::Pending,
            writes: Vec::new(),
            locks: vec![TxLock::Key {
                key: key(b"hello"),
                typ: LockType::Write,
            }],
            collection_changes: Vec::new(),
            prepared_collections: Vec::new(),
        };
        let decoded = decode("db", &record.id, &encode(&record).unwrap()).unwrap();
        assert_eq!(decoded.status, TxCommitStatus::Pending);
        // The lease (timestamp) survives the round trip with sub-second loss
        // tolerated by the body encoding (full nanos are preserved here).
        assert_eq!(decoded.timestamp, record.timestamp);
        assert_eq!(decoded.locks, record.locks);
    }

    #[test]
    fn aborted_round_trip() {
        let record = TxRecord {
            id: TxId::from_bytes(vec![7]),
            timestamp: Some(UNIX_EPOCH + Duration::from_secs(1)),
            status: TxCommitStatus::Aborted,
            writes: Vec::new(),
            locks: Vec::new(),
            collection_changes: Vec::new(),
            prepared_collections: Vec::new(),
        };
        let decoded = decode("db", &record.id, &encode(&record).unwrap()).unwrap();
        assert_eq!(decoded.status, TxCommitStatus::Aborted);
    }

    #[test]
    fn wounded_round_trip() {
        let record = TxRecord {
            id: TxId::from_bytes(vec![8]),
            timestamp: Some(UNIX_EPOCH + Duration::from_secs(2)),
            status: TxCommitStatus::Wounded,
            writes: Vec::new(),
            locks: Vec::new(),
            collection_changes: Vec::new(),
            prepared_collections: Vec::new(),
        };
        let decoded = decode("db", &record.id, &encode(&record).unwrap()).unwrap();
        assert_eq!(decoded.status, TxCommitStatus::Wounded);
    }

    #[test]
    fn encode_requires_timestamp() {
        let mut record = committed_record();
        record.timestamp = None;
        assert!(encode(&record).is_err());
    }

    #[test]
    fn find_write_locates_key() {
        let record = committed_record();
        assert_eq!(
            find_write(&record, &key(b"hello")).unwrap().value.as_ref(),
            b"world"
        );
        assert!(find_write(&record, &key(b"missing")).is_none());
    }

    // Golden vector: a fixed committed record must always encode to these exact
    // bytes. Changing the on-disk format must break this test.
    #[test]
    fn golden_encoding() {
        let got = encode(&committed_record()).unwrap();
        let want = [
            0x0a, 0x06, 0x08, 0x80, 0xe2, 0xcf, 0xaa, 0x06, 0x10, 0x01, 0x1a, 0x24, 0x0a, 0x10,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x12, 0x0e, 0x0a, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x12, 0x05, 0x77,
            0x6f, 0x72, 0x6c, 0x64, 0x1a, 0x00,
        ];
        assert_eq!(got, want, "tx-record encoding drifted: {got:02x?}");
    }
}
