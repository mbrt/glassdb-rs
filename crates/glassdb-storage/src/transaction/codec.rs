use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use glassdb_data::{
    CollectionAddress, CollectionId, KeyRef, LeafRef, MAX_COLLECTION_NAME_BYTES, NodeToken,
    ObjectPath, TxId,
};
use glassdb_proto as pb;
use prost::Message;

use super::{TxCollectionChange, TxCollectionOp, TxCommitStatus, TxLock, TxLog, TxWrite};
use crate::cached_store::Codec;
use crate::error::StorageError;
use crate::lock::LockType;

/// Canonical protobuf codec for transaction-log objects.
pub(crate) struct TxLogCodec;

impl TxLogCodec {
    /// Encodes a transaction log as its canonical persisted body.
    pub(crate) fn encode(log: &TxLog) -> Result<Vec<u8>, StorageError> {
        let timestamp = log
            .timestamp
            .ok_or_else(|| StorageError::other("transaction log has no persisted timestamp"))?;
        if log.id.is_unset() {
            return Err(StorageError::other("empty transaction ID"));
        }
        validate_single_database(log)?;

        let mut collection_writes: BTreeMap<CollectionAddress, pb::CollectionWrites> =
            BTreeMap::new();
        for write in &log.writes {
            append_write(&mut collection_writes, write);
        }
        for lock in &log.locks {
            append_lock(&mut collection_writes, lock);
        }

        let collection_changes = log
            .collection_changes
            .iter()
            .map(|change| pb::CollectionChange {
                parent_collection_id: change.parent.id().as_bytes().to_vec(),
                name: change.name.clone(),
                collection_id: change.collection.id().as_bytes().to_vec(),
                operation: match change.op {
                    TxCollectionOp::Create => pb::collection_change::Operation::Create as i32,
                    TxCollectionOp::Drop => pb::collection_change::Operation::Drop as i32,
                },
            })
            .collect();

        let status = encode_status(log.status)?;
        let encoded = pb::TransactionLog {
            timestamp: Some(system_to_proto_ts(timestamp)),
            status: status as i32,
            writes: collection_writes.into_values().collect(),
            collection_changes,
            prepared_collection_ids: log
                .prepared_collections
                .iter()
                .map(|collection| collection.id().as_bytes().to_vec())
                .collect(),
        };
        Ok(encoded.encode_to_vec())
    }

    /// Decodes a transaction-log body using the database root from its object path.
    pub(crate) fn decode(db_root: &str, id: &TxId, bytes: &[u8]) -> Result<TxLog, StorageError> {
        let encoded = parse_log(bytes)?;
        let status = decode_status(encoded.status())?;
        let (writes, locks) = decode_collection_writes(db_root, &encoded.writes)?;
        let collection_changes = decode_collection_changes(db_root, &encoded.collection_changes)?;
        let prepared_collections =
            decode_prepared_collections(db_root, &encoded.prepared_collection_ids)?;

        Ok(TxLog {
            id: id.clone(),
            timestamp: encoded.timestamp.map(proto_ts_to_system),
            status,
            writes,
            locks,
            collection_changes,
            prepared_collections,
        })
    }

    /// Decodes only the commit status from a transaction-log body.
    pub(crate) fn decode_status(bytes: &[u8]) -> Result<TxCommitStatus, StorageError> {
        decode_status(parse_log(bytes)?.status())
    }
}

impl Codec for TxLogCodec {
    type Value = TxLog;

    fn decode(path: &str, body: &[u8]) -> Result<Self::Value, StorageError> {
        let ObjectPath::Transaction { db_root, id } = ObjectPath::try_from(path)
            .map_err(|error| StorageError::with_source("parsing transaction path", error))?
        else {
            return Err(StorageError::other(
                "transaction log has a non-transaction path",
            ));
        };
        TxLogCodec::decode(db_root.as_str(), &id, body)
    }

    fn encode(log: &Self::Value) -> Result<Vec<u8>, StorageError> {
        TxLogCodec::encode(log)
    }

    fn size(log: &Self::Value) -> usize {
        log.writes
            .iter()
            .map(|write| {
                write.key.key().len() + write.value.len() + write.prev_writer.as_bytes().len()
            })
            .sum::<usize>()
            + log
                .locks
                .iter()
                .map(|lock| match lock {
                    TxLock::Entry { key, .. } => key.key().len(),
                    TxLock::Membership { leaf, .. } => {
                        leaf.node_token().map_or(0, |token| token.as_str().len())
                    }
                    TxLock::Directory { .. } | TxLock::Topology { .. } => 0,
                })
                .sum::<usize>()
            + log
                .collection_changes
                .iter()
                .map(|change| change.name.len() + 32)
                .sum::<usize>()
            + log.prepared_collections.len() * 16
            + std::mem::size_of::<TxLog>()
    }

    fn valid_path(path: &str) -> bool {
        matches!(
            ObjectPath::try_from(path),
            Ok(ObjectPath::Transaction { .. })
        )
    }

    fn name() -> &'static str {
        "transaction log"
    }
}

fn parse_log(bytes: &[u8]) -> Result<pb::TransactionLog, StorageError> {
    pb::TransactionLog::decode(bytes)
        .map_err(|error| StorageError::with_source("unmarshalling transaction log", error))
}

fn encode_status(status: TxCommitStatus) -> Result<pb::transaction_log::Status, StorageError> {
    match status {
        TxCommitStatus::Ok => Ok(pb::transaction_log::Status::Committed),
        TxCommitStatus::Aborted => Ok(pb::transaction_log::Status::Aborted),
        TxCommitStatus::Pending => Ok(pb::transaction_log::Status::Pending),
        TxCommitStatus::Wounded => Ok(pb::transaction_log::Status::Wounded),
        TxCommitStatus::Unknown => Err(StorageError::other("unsupported commit status")),
    }
}

fn decode_status(status: pb::transaction_log::Status) -> Result<TxCommitStatus, StorageError> {
    match status {
        pb::transaction_log::Status::Committed => Ok(TxCommitStatus::Ok),
        pb::transaction_log::Status::Aborted => Ok(TxCommitStatus::Aborted),
        pb::transaction_log::Status::Pending => Ok(TxCommitStatus::Pending),
        pb::transaction_log::Status::Wounded => Ok(TxCommitStatus::Wounded),
        pb::transaction_log::Status::Default => Err(StorageError::other("unknown commit status")),
    }
}

fn decode_collection_writes(
    db_root: &str,
    encoded: &[pb::CollectionWrites],
) -> Result<(Vec<TxWrite>, Vec<TxLock>), StorageError> {
    let mut writes = Vec::new();
    let mut locks = Vec::new();
    for group in encoded {
        let collection = decode_collection_id(db_root, &group.collection_id)?;
        writes.extend(decode_writes(&collection, &group.writes));
        if let Some(group_locks) = &group.locks {
            locks.extend(decode_entry_locks(&collection, &group_locks.entry_locks));
            locks.extend(decode_membership_locks(
                &collection,
                &group_locks.membership_locks,
            )?);
            let typ = parse_lock_type(group_locks.directory_lock);
            if !matches!(typ, LockType::None | LockType::Unknown) {
                locks.push(TxLock::Directory {
                    collection: collection.clone(),
                    typ,
                });
            }
            if group_locks.topology_lock {
                locks.push(TxLock::Topology {
                    collection: collection.clone(),
                });
            }
        }
    }
    Ok((writes, locks))
}

fn decode_writes(collection: &CollectionAddress, encoded: &[pb::Write]) -> Vec<TxWrite> {
    encoded
        .iter()
        .map(|write| TxWrite {
            key: KeyRef::new(collection.clone(), &write.key),
            value: write_value(write),
            deleted: write_deleted(write),
            prev_writer: TxId::from_bytes(write.prev_tid.clone()),
        })
        .collect()
}

fn decode_entry_locks(collection: &CollectionAddress, encoded: &[pb::EntryLock]) -> Vec<TxLock> {
    encoded
        .iter()
        .map(|lock| TxLock::Entry {
            key: KeyRef::new(collection.clone(), &lock.key),
            typ: parse_lock_type(lock.lock_type),
        })
        .collect()
}

fn decode_membership_locks(
    collection: &CollectionAddress,
    encoded: &[pb::MembershipLock],
) -> Result<Vec<TxLock>, StorageError> {
    encoded
        .iter()
        .map(|lock| {
            let leaf = match lock.target.as_ref() {
                Some(pb::membership_lock::Target::Root(true)) => LeafRef::root(collection.clone()),
                Some(pb::membership_lock::Target::Node(token)) if !token.is_empty() => {
                    let token = NodeToken::try_from(token.as_str()).map_err(|error| {
                        StorageError::with_source("parsing membership-lock node token", error)
                    })?;
                    LeafRef::node(collection.clone(), token)
                }
                _ => {
                    return Err(StorageError::other(
                        "transaction log has invalid membership lock",
                    ));
                }
            };
            Ok(TxLock::Membership {
                leaf,
                typ: parse_lock_type(lock.lock_type),
            })
        })
        .collect()
}

fn decode_collection_changes(
    db_root: &str,
    encoded: &[pb::CollectionChange],
) -> Result<Vec<TxCollectionChange>, StorageError> {
    encoded
        .iter()
        .map(|change| {
            if change.name.is_empty() || change.name.len() > MAX_COLLECTION_NAME_BYTES {
                return Err(StorageError::other(
                    "transaction log has an invalid collection name",
                ));
            }
            let parent = decode_collection_id(db_root, &change.parent_collection_id)?;
            let collection = decode_collection_id(db_root, &change.collection_id)?;
            if collection.id().is_root() {
                return Err(StorageError::other(
                    "transaction log changes the permanent root collection",
                ));
            }
            let op = match change.operation() {
                pb::collection_change::Operation::Create => TxCollectionOp::Create,
                pb::collection_change::Operation::Drop => TxCollectionOp::Drop,
                pb::collection_change::Operation::Unknown => {
                    return Err(StorageError::other(
                        "transaction log has an unknown collection operation",
                    ));
                }
            };
            Ok(TxCollectionChange {
                parent,
                name: change.name.clone(),
                collection,
                op,
            })
        })
        .collect()
}

fn decode_prepared_collections(
    db_root: &str,
    encoded: &[Vec<u8>],
) -> Result<Vec<CollectionAddress>, StorageError> {
    encoded
        .iter()
        .map(|id| {
            let collection = decode_collection_id(db_root, id)?;
            if collection.id().is_root() {
                return Err(StorageError::other(
                    "transaction log prepares the permanent root collection",
                ));
            }
            Ok(collection)
        })
        .collect()
}

fn write_value(write: &pb::Write) -> Arc<[u8]> {
    match &write.val_delete {
        Some(pb::write::ValDelete::Value(value)) => Arc::from(value.as_slice()),
        _ => Arc::from(&[] as &[u8]),
    }
}

fn write_deleted(write: &pb::Write) -> bool {
    matches!(&write.val_delete, Some(pb::write::ValDelete::Deleted(true)))
}

fn append_write(
    collection_writes: &mut BTreeMap<CollectionAddress, pb::CollectionWrites>,
    write: &TxWrite,
) {
    let val_delete = if write.deleted {
        pb::write::ValDelete::Deleted(true)
    } else {
        pb::write::ValDelete::Value(write.value.to_vec())
    };
    let encoded = pb::Write {
        key: write.key.key().to_vec(),
        prev_tid: write.prev_writer.as_bytes().to_vec(),
        val_delete: Some(val_delete),
    };
    let collection = write.key.collection();
    let group = collection_writes
        .entry(collection.clone())
        .or_insert_with(|| pb::CollectionWrites {
            collection_id: collection.id().as_bytes().to_vec(),
            writes: Vec::new(),
            locks: Some(pb::CollectionLocks::default()),
        });
    group.writes.push(encoded);
}

fn append_lock(
    collection_writes: &mut BTreeMap<CollectionAddress, pb::CollectionWrites>,
    lock: &TxLock,
) {
    let collection = match lock {
        TxLock::Entry { key, .. } => key.collection(),
        TxLock::Membership { leaf, .. } => leaf.collection(),
        TxLock::Directory { collection, .. } | TxLock::Topology { collection } => collection,
    };
    let group = collection_writes
        .entry(collection.clone())
        .or_insert_with(|| pb::CollectionWrites {
            collection_id: collection.id().as_bytes().to_vec(),
            writes: Vec::new(),
            locks: Some(pb::CollectionLocks::default()),
        });
    let locks = group.locks.get_or_insert_with(pb::CollectionLocks::default);

    match lock {
        TxLock::Entry { key, typ } => locks.entry_locks.push(pb::EntryLock {
            key: key.key().to_vec(),
            lock_type: lock_type_to_proto(*typ) as i32,
        }),
        TxLock::Membership { leaf, typ } => {
            let target = match leaf.node_token() {
                Some(token) => pb::membership_lock::Target::Node(token.to_string()),
                None => pb::membership_lock::Target::Root(true),
            };
            locks.membership_locks.push(pb::MembershipLock {
                target: Some(target),
                lock_type: lock_type_to_proto(*typ) as i32,
            });
        }
        TxLock::Directory { typ, .. } => {
            locks.directory_lock = lock_type_to_proto(*typ) as i32;
        }
        TxLock::Topology { .. } => {
            locks.topology_lock = true;
        }
    }
}

fn decode_collection_id(
    db_root: &str,
    collection_id: &[u8],
) -> Result<CollectionAddress, StorageError> {
    let id = CollectionId::from_slice(collection_id)
        .ok_or_else(|| StorageError::other("transaction log has an invalid collection ID"))?;
    Ok(CollectionAddress::new(db_root, id))
}

fn validate_single_database(log: &TxLog) -> Result<(), StorageError> {
    let mut db_root: Option<String> = None;
    let mut check = |collection: &CollectionAddress| -> Result<(), StorageError> {
        match db_root.as_deref() {
            Some(root) if root != collection.db_root() => Err(StorageError::other(
                "transaction log spans multiple database roots",
            )),
            Some(_) => Ok(()),
            None => {
                db_root = Some(collection.db_root().to_string());
                Ok(())
            }
        }
    };
    for write in &log.writes {
        check(write.key.collection())?;
    }
    for lock in &log.locks {
        match lock {
            TxLock::Entry { key, .. } => check(key.collection())?,
            TxLock::Membership { leaf, .. } => check(leaf.collection())?,
            TxLock::Directory { collection, .. } | TxLock::Topology { collection } => {
                check(collection)?
            }
        }
    }
    for change in &log.collection_changes {
        check(&change.parent)?;
        check(&change.collection)?;
    }
    for collection in &log.prepared_collections {
        check(collection)?;
    }
    Ok(())
}

fn lock_type_to_proto(typ: LockType) -> pb::lock::LockType {
    match typ {
        LockType::None => pb::lock::LockType::None,
        LockType::Read => pb::lock::LockType::Read,
        LockType::Write => pb::lock::LockType::Write,
        LockType::Create => pb::lock::LockType::Create,
        LockType::Unknown => pb::lock::LockType::Unknown,
    }
}

fn parse_lock_type(typ: i32) -> LockType {
    match pb::lock::LockType::try_from(typ) {
        Ok(pb::lock::LockType::None) => LockType::None,
        Ok(pb::lock::LockType::Read) => LockType::Read,
        Ok(pb::lock::LockType::Write) => LockType::Write,
        Ok(pb::lock::LockType::Create) => LockType::Create,
        _ => LockType::Unknown,
    }
}

fn system_to_proto_ts(timestamp: SystemTime) -> prost_types::Timestamp {
    match timestamp.duration_since(UNIX_EPOCH) {
        Ok(duration) => prost_types::Timestamp {
            seconds: duration.as_secs() as i64,
            nanos: duration.subsec_nanos() as i32,
        },
        Err(error) => {
            let duration = error.duration();
            prost_types::Timestamp {
                seconds: -(duration.as_secs() as i64),
                nanos: -(duration.subsec_nanos() as i32),
            }
        }
    }
}

fn proto_ts_to_system(timestamp: prost_types::Timestamp) -> SystemTime {
    if timestamp.seconds >= 0 {
        UNIX_EPOCH + Duration::new(timestamp.seconds as u64, timestamp.nanos.max(0) as u32)
    } else {
        UNIX_EPOCH - Duration::new((-timestamp.seconds) as u64, timestamp.nanos.unsigned_abs())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collection(db_root: &str, byte: u8) -> CollectionAddress {
        CollectionAddress::new(db_root, CollectionId::from_slice(&[byte; 16]).unwrap())
    }

    fn log_with_status(status: TxCommitStatus) -> TxLog {
        let mut log = TxLog::new(TxId::from_bytes(vec![1, 2, 3, 4]), status);
        log.timestamp = Some(UNIX_EPOCH + Duration::from_secs(1_700_000_000));
        log
    }

    fn complete_log(db_root: &str) -> TxLog {
        let parent = collection(db_root, 1);
        let created = collection(db_root, 2);
        let dropped = collection(db_root, 3);
        TxLog {
            id: TxId::from_bytes(vec![1, 2, 3, 4]),
            timestamp: Some(UNIX_EPOCH + Duration::from_secs(42)),
            status: TxCommitStatus::Pending,
            writes: vec![
                TxWrite {
                    key: KeyRef::new(parent.clone(), b"value"),
                    value: Arc::from(&b"contents"[..]),
                    deleted: false,
                    prev_writer: TxId::from_bytes(vec![9]),
                },
                TxWrite {
                    key: KeyRef::new(parent.clone(), b"deleted"),
                    value: Arc::from(&[][..]),
                    deleted: true,
                    prev_writer: TxId::from_bytes(vec![8]),
                },
            ],
            locks: vec![
                TxLock::Entry {
                    key: KeyRef::new(parent.clone(), b"entry"),
                    typ: LockType::Write,
                },
                TxLock::Membership {
                    leaf: LeafRef::root(parent.clone()),
                    typ: LockType::Read,
                },
                TxLock::Membership {
                    leaf: LeafRef::node(parent.clone(), NodeToken::from_bytes([7; 16])),
                    typ: LockType::Create,
                },
                TxLock::Directory {
                    collection: parent.clone(),
                    typ: LockType::Write,
                },
                TxLock::Topology {
                    collection: created.clone(),
                },
            ],
            collection_changes: vec![
                TxCollectionChange {
                    parent: parent.clone(),
                    name: b"created".to_vec(),
                    collection: created.clone(),
                    op: TxCollectionOp::Create,
                },
                TxCollectionChange {
                    parent,
                    name: b"dropped".to_vec(),
                    collection: dropped.clone(),
                    op: TxCollectionOp::Drop,
                },
            ],
            prepared_collections: vec![created, dropped],
        }
    }

    fn encoded_log() -> pb::TransactionLog {
        pb::TransactionLog {
            timestamp: Some(prost_types::Timestamp {
                seconds: 42,
                nanos: 0,
            }),
            status: pb::transaction_log::Status::Committed as i32,
            writes: Vec::new(),
            collection_changes: Vec::new(),
            prepared_collection_ids: Vec::new(),
        }
    }

    fn encoded_change() -> pb::CollectionChange {
        pb::CollectionChange {
            parent_collection_id: vec![1; 16],
            name: b"child".to_vec(),
            collection_id: vec![2; 16],
            operation: pb::collection_change::Operation::Create as i32,
        }
    }

    fn assert_rejected(encoded: pb::TransactionLog) {
        let bytes = encoded.encode_to_vec();
        assert!(
            TxLogCodec::decode("db", &TxId::from_bytes(vec![1]), &bytes).is_err(),
            "malformed transaction log unexpectedly decoded"
        );
    }

    #[test]
    fn every_status_round_trips_through_full_and_status_only_decode() {
        for status in [
            TxCommitStatus::Ok,
            TxCommitStatus::Aborted,
            TxCommitStatus::Pending,
            TxCommitStatus::Wounded,
        ] {
            let log = log_with_status(status);
            let bytes = TxLogCodec::encode(&log).unwrap();
            assert_eq!(TxLogCodec::decode_status(&bytes).unwrap(), status);
            assert_eq!(
                TxLogCodec::decode("db", &log.id, &bytes).unwrap().status,
                status
            );
        }

        assert!(TxLogCodec::encode(&log_with_status(TxCommitStatus::Unknown)).is_err());
        for status in [pb::transaction_log::Status::Default as i32, 99] {
            let mut encoded = encoded_log();
            encoded.status = status;
            let bytes = encoded.encode_to_vec();
            assert!(TxLogCodec::decode_status(&bytes).is_err());
            assert!(TxLogCodec::decode("db", &TxId::from_bytes(vec![1]), &bytes).is_err());
        }
    }

    #[test]
    fn repeated_fields_and_every_lock_shape_round_trip() {
        let log = complete_log("db");
        let bytes = TxLogCodec::encode(&log).unwrap();
        let decoded = TxLogCodec::decode("db", &log.id, &bytes).unwrap();

        assert_eq!(decoded.id, log.id);
        assert_eq!(decoded.timestamp, log.timestamp);
        assert_eq!(decoded.status, log.status);
        assert_eq!(decoded.writes, log.writes);
        assert_eq!(decoded.locks, log.locks);
        assert_eq!(decoded.collection_changes, log.collection_changes);
        assert_eq!(decoded.prepared_collections, log.prepared_collections);
    }

    #[test]
    fn every_collection_address_is_relocated_without_changing_bytes() {
        let log = complete_log("original");
        let bytes = TxLogCodec::encode(&log).unwrap();
        let relocated = TxLogCodec::decode("moved", &log.id, &bytes).unwrap();

        assert!(
            relocated
                .writes
                .iter()
                .all(|write| write.key.collection().db_root() == "moved")
        );
        assert!(relocated.locks.iter().all(|lock| match lock {
            TxLock::Entry { key, .. } => key.collection().db_root() == "moved",
            TxLock::Membership { leaf, .. } => leaf.collection().db_root() == "moved",
            TxLock::Directory { collection, .. } | TxLock::Topology { collection } => {
                collection.db_root() == "moved"
            }
        }));
        assert!(relocated.collection_changes.iter().all(|change| {
            change.parent.db_root() == "moved" && change.collection.db_root() == "moved"
        }));
        assert!(
            relocated
                .prepared_collections
                .iter()
                .all(|collection| collection.db_root() == "moved")
        );
        assert_eq!(TxLogCodec::encode(&relocated).unwrap(), bytes);
    }

    #[test]
    fn one_transaction_cannot_span_database_roots() {
        let mut log = log_with_status(TxCommitStatus::Ok);
        log.writes = vec![
            TxWrite {
                key: KeyRef::new(collection("first", 1), b"a"),
                value: Arc::from(&b"a"[..]),
                deleted: false,
                prev_writer: TxId::default(),
            },
            TxWrite {
                key: KeyRef::new(collection("second", 1), b"b"),
                value: Arc::from(&b"b"[..]),
                deleted: false,
                prev_writer: TxId::default(),
            },
        ];

        assert!(TxLogCodec::encode(&log).is_err());
    }

    #[test]
    fn malformed_protobuf_and_status_are_rejected() {
        assert!(TxLogCodec::decode_status(&[0xff]).is_err());
        assert!(TxLogCodec::decode("db", &TxId::from_bytes(vec![1]), &[0xff]).is_err());

        let mut encoded = encoded_log();
        encoded.status = pb::transaction_log::Status::Default as i32;
        assert_rejected(encoded);
    }

    #[test]
    fn status_only_decode_ignores_unrelated_semantic_body_errors() {
        let mut encoded = encoded_log();
        encoded.writes.push(pb::CollectionWrites {
            collection_id: vec![1],
            writes: Vec::new(),
            locks: None,
        });
        let bytes = encoded.encode_to_vec();

        assert_eq!(
            TxLogCodec::decode_status(&bytes).unwrap(),
            TxCommitStatus::Ok
        );
        assert!(TxLogCodec::decode("db", &TxId::from_bytes(vec![1]), &bytes).is_err());
    }

    #[test]
    fn malformed_collection_ids_are_rejected_in_every_repeated_field() {
        let mut group = encoded_log();
        group.writes.push(pb::CollectionWrites {
            collection_id: vec![1; 15],
            writes: Vec::new(),
            locks: None,
        });
        assert_rejected(group);

        let mut parent = encoded_log();
        let mut change = encoded_change();
        change.parent_collection_id = vec![1; 15];
        parent.collection_changes.push(change);
        assert_rejected(parent);

        let mut child = encoded_log();
        let mut change = encoded_change();
        change.collection_id = vec![2; 15];
        child.collection_changes.push(change);
        assert_rejected(child);

        let mut prepared = encoded_log();
        prepared.prepared_collection_ids.push(vec![3; 15]);
        assert_rejected(prepared);
    }

    #[test]
    fn malformed_membership_lock_targets_are_rejected() {
        for target in [
            None,
            Some(pb::membership_lock::Target::Root(false)),
            Some(pb::membership_lock::Target::Node(String::new())),
        ] {
            let mut encoded = encoded_log();
            encoded.writes.push(pb::CollectionWrites {
                collection_id: vec![1; 16],
                writes: Vec::new(),
                locks: Some(pb::CollectionLocks {
                    membership_locks: vec![pb::MembershipLock {
                        target,
                        lock_type: pb::lock::LockType::Read as i32,
                    }],
                    ..pb::CollectionLocks::default()
                }),
            });
            assert_rejected(encoded);
        }
    }

    #[test]
    fn malformed_collection_changes_are_rejected() {
        for name in [Vec::new(), vec![b'x'; MAX_COLLECTION_NAME_BYTES + 1]] {
            let mut encoded = encoded_log();
            let mut change = encoded_change();
            change.name = name;
            encoded.collection_changes.push(change);
            assert_rejected(encoded);
        }

        for operation in [pb::collection_change::Operation::Unknown as i32, 99] {
            let mut encoded = encoded_log();
            let mut change = encoded_change();
            change.operation = operation;
            encoded.collection_changes.push(change);
            assert_rejected(encoded);
        }

        let mut encoded = encoded_log();
        let mut change = encoded_change();
        change.collection_id = vec![0; 16];
        encoded.collection_changes.push(change);
        assert_rejected(encoded);
    }

    #[test]
    fn preparing_the_permanent_root_is_rejected() {
        let mut encoded = encoded_log();
        encoded.prepared_collection_ids.push(vec![0; 16]);
        assert_rejected(encoded);
    }
}
