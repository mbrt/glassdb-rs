//! Protobuf definitions for persistent database records. Ported from the Go
//! `internal/proto` package. The Rust bindings in `generated.rs` are
//! pre-generated from `proto/transaction.proto` with `prost-build` and
//! checked into the repo, so building the crate does not require `protoc`.
//! Run `hack/regen-proto.sh` after editing the `.proto` file.

mod generated;
pub use generated::*;

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn transaction_record_round_trip() {
        let record = TransactionRecord {
            timestamp: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 123_000_000,
            }),
            status: transaction_record::Status::Committed as i32,
            writes: vec![CollectionWrites {
                collection_id: vec![1; 16],
                writes: vec![
                    Write {
                        key: b"Hello".to_vec(),
                        val_delete: Some(write::ValDelete::Value(b"world!".to_vec())),
                    },
                    Write {
                        key: b"other".to_vec(),
                        val_delete: Some(write::ValDelete::Deleted(true)),
                    },
                ],
                locks: Some(CollectionLocks {
                    key_locks: vec![KeyLock {
                        key: b"Hello".to_vec(),
                        lock_type: lock::LockType::Write as i32,
                    }],
                    membership_locks: vec![MembershipLock {
                        target: Some(membership_lock::Target::Root(true)),
                        lock_type: lock::LockType::Read as i32,
                    }],
                    directory_lock: 0,
                    topology_participant: false,
                }),
            }],
            collection_changes: vec![],
            prepared_collection_ids: vec![],
        };

        let bytes = record.encode_to_vec();
        let decoded = TransactionRecord::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded, record);
        assert_eq!(decoded.status, transaction_record::Status::Committed as i32);
        match &decoded.writes[0].writes[0].val_delete {
            Some(write::ValDelete::Value(v)) => assert_eq!(v, b"world!"),
            other => panic!("unexpected val_delete: {other:?}"),
        }
    }
}
