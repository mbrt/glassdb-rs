//! Protobuf definitions for transaction logs. Ported from the Go
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
    fn transaction_log_round_trip() {
        let log = TransactionLog {
            timestamp: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 123_000_000,
            }),
            status: transaction_log::Status::Committed as i32,
            writes: vec![CollectionWrites {
                prefix: "db/root".to_string(),
                writes: vec![
                    Write {
                        suffix: "_k/H6KgQ6w".to_string(),
                        prev_tid: vec![1, 2, 3, 4],
                        val_delete: Some(write::ValDelete::Value(b"world!".to_vec())),
                    },
                    Write {
                        suffix: "_k/other".to_string(),
                        prev_tid: vec![],
                        val_delete: Some(write::ValDelete::Deleted(true)),
                    },
                ],
                locks: Some(CollectionLocks {
                    collection_lock: lock::LockType::Write as i32,
                    locks: vec![Lock {
                        suffix: "_k/H6KgQ6w".to_string(),
                        lock_type: lock::LockType::Write as i32,
                        scope: lock::Scope::Entry as i32,
                    }],
                }),
            }],
            structural_splits: Vec::new(),
        };

        let bytes = log.encode_to_vec();
        let decoded = TransactionLog::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded, log);
        assert_eq!(decoded.status, transaction_log::Status::Committed as i32);
        match &decoded.writes[0].writes[0].val_delete {
            Some(write::ValDelete::Value(v)) => assert_eq!(v, b"world!"),
            other => panic!("unexpected val_delete: {other:?}"),
        }
    }
}
