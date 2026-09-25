use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use glassdb_data::{
    CollectionAddress, CollectionId, ID_BYTES, LeafRef, LogicalKey, MAX_COLLECTION_NAME_BYTES,
    NodeId, ObjectPath, TxId,
};
use glassdb_proto as pb;
use prost::Message;

use super::{TxCollectionChange, TxCollectionOp, TxCommitStatus, TxLock, TxRecord, TxWrite};
use crate::cached_store::Codec;
use crate::error::StorageError;
use crate::lock::{LockType, lock_type_from_proto as parse_lock_type, lock_type_to_proto};

/// Canonical protobuf codec for transaction-record objects.
pub(crate) struct TxRecordCodec;

impl TxRecordCodec {
    /// Encodes a transaction record as its canonical persisted body.
    pub(crate) fn encode(record: &TxRecord) -> Result<Vec<u8>, StorageError> {
        Self::encode_for_database(record, None)
    }

    fn encode_for_database(
        record: &TxRecord,
        expected_db_prefix: Option<&str>,
    ) -> Result<Vec<u8>, StorageError> {
        let timestamp = record
            .timestamp
            .ok_or_else(|| StorageError::other("transaction record has no persisted timestamp"))?;
        if record.id.is_unset() {
            return Err(StorageError::other("empty transaction identity"));
        }
        validate_database_membership(record, expected_db_prefix)?;

        let mut collection_writes: BTreeMap<CollectionAddress, pb::CollectionWrites> =
            BTreeMap::new();
        for write in &record.writes {
            append_write(&mut collection_writes, write);
        }
        for lock in &record.locks {
            append_lock(&mut collection_writes, lock);
        }

        let collection_changes = record
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

        let status = encode_status(record.status)?;
        let encoded = pb::TransactionRecord {
            timestamp: Some(system_to_proto_ts(timestamp)),
            status: status as i32,
            writes: collection_writes.into_values().collect(),
            collection_changes,
            prepared_collection_ids: record
                .prepared_collections
                .iter()
                .map(|collection| collection.id().as_bytes().to_vec())
                .collect(),
        };
        Ok(encoded.encode_to_vec())
    }

    /// Decodes a transaction-record body using the database prefix from its object path.
    pub(crate) fn decode(
        db_prefix: &str,
        id: &TxId,
        bytes: &[u8],
    ) -> Result<TxRecord, StorageError> {
        let encoded = parse_record(bytes)?;
        let status = decode_status(encoded.status())?;
        let (writes, locks) = decode_collection_writes(db_prefix, &encoded.writes)?;
        let collection_changes = decode_collection_changes(db_prefix, &encoded.collection_changes)?;
        let prepared_collections =
            decode_prepared_collections(db_prefix, &encoded.prepared_collection_ids)?;

        Ok(TxRecord {
            id: id.clone(),
            timestamp: encoded.timestamp.map(proto_ts_to_system),
            status,
            writes,
            locks,
            collection_changes,
            prepared_collections,
        })
    }

    /// Decodes only the commit status from a transaction-record body.
    pub(crate) fn decode_status(bytes: &[u8]) -> Result<TxCommitStatus, StorageError> {
        decode_status(parse_record(bytes)?.status())
    }
}

impl Codec for TxRecordCodec {
    type Value = TxRecord;

    fn decode(path: &ObjectPath, body: &[u8]) -> Result<Self::Value, StorageError> {
        let ObjectPath::Transaction { db_prefix, id } = path else {
            return Err(StorageError::other(
                "transaction record has a non-transaction path",
            ));
        };
        TxRecordCodec::decode(db_prefix.as_str(), id, body)
    }

    fn encode(path: &ObjectPath, record: &Self::Value) -> Result<Vec<u8>, StorageError> {
        let ObjectPath::Transaction { db_prefix, id } = path else {
            return Err(StorageError::other(
                "transaction record has a non-transaction path",
            ));
        };
        if id != &record.id {
            return Err(StorageError::other(
                "transaction-record path does not match its ID",
            ));
        }
        TxRecordCodec::encode_for_database(record, Some(db_prefix.as_str()))
    }

    fn size(record: &Self::Value) -> usize {
        record
            .writes
            .iter()
            .map(|write| {
                write.key.key().len() + write.value.len() + write.prev_writer.as_bytes().len()
            })
            .sum::<usize>()
            + record
                .locks
                .iter()
                .map(|lock| match lock {
                    TxLock::Key { key, .. } => key.key().len(),
                    TxLock::Membership { leaf, .. } => leaf.node_id().map_or(0, |_| ID_BYTES),
                    TxLock::Directory { .. } | TxLock::TopologyParticipant { .. } => 0,
                })
                .sum::<usize>()
            + record
                .collection_changes
                .iter()
                .map(|change| change.name.len() + 32)
                .sum::<usize>()
            + record.prepared_collections.len() * 16
            + std::mem::size_of::<TxRecord>()
    }

    fn accepts(path: &ObjectPath) -> bool {
        matches!(path, ObjectPath::Transaction { .. })
    }

    fn name() -> &'static str {
        "transaction record"
    }
}

fn parse_record(bytes: &[u8]) -> Result<pb::TransactionRecord, StorageError> {
    pb::TransactionRecord::decode(bytes)
        .map_err(|error| StorageError::with_source("unmarshalling transaction record", error))
}

fn encode_status(status: TxCommitStatus) -> Result<pb::transaction_record::Status, StorageError> {
    match status {
        TxCommitStatus::Committed => Ok(pb::transaction_record::Status::Committed),
        TxCommitStatus::Aborted => Ok(pb::transaction_record::Status::Aborted),
        TxCommitStatus::Pending => Ok(pb::transaction_record::Status::Pending),
        TxCommitStatus::Wounded => Ok(pb::transaction_record::Status::Wounded),
        TxCommitStatus::Unknown => Err(StorageError::other("unsupported commit status")),
    }
}

fn decode_status(status: pb::transaction_record::Status) -> Result<TxCommitStatus, StorageError> {
    match status {
        pb::transaction_record::Status::Committed => Ok(TxCommitStatus::Committed),
        pb::transaction_record::Status::Aborted => Ok(TxCommitStatus::Aborted),
        pb::transaction_record::Status::Pending => Ok(TxCommitStatus::Pending),
        pb::transaction_record::Status::Wounded => Ok(TxCommitStatus::Wounded),
        pb::transaction_record::Status::Default => {
            Err(StorageError::other("unknown commit status"))
        }
    }
}

fn decode_collection_writes(
    db_prefix: &str,
    encoded: &[pb::CollectionWrites],
) -> Result<(Vec<TxWrite>, Vec<TxLock>), StorageError> {
    let mut writes = Vec::new();
    let mut locks = Vec::new();
    for group in encoded {
        let collection = decode_collection_id(db_prefix, &group.collection_id)?;
        writes.extend(decode_writes(&collection, &group.writes));
        if let Some(group_locks) = &group.locks {
            locks.extend(decode_key_locks(&collection, &group_locks.key_locks));
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
            if group_locks.topology_participant {
                locks.push(TxLock::TopologyParticipant {
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
            key: LogicalKey::new(collection.clone(), &write.key),
            value: write_value(write),
            deleted: write_deleted(write),
            prev_writer: TxId::from_bytes(write.prev_tid.clone()),
        })
        .collect()
}

fn decode_key_locks(collection: &CollectionAddress, encoded: &[pb::KeyLock]) -> Vec<TxLock> {
    encoded
        .iter()
        .map(|lock| TxLock::Key {
            key: LogicalKey::new(collection.clone(), &lock.key),
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
                Some(pb::membership_lock::Target::Node(id)) => {
                    let id = NodeId::from_slice(id).ok_or_else(|| {
                        StorageError::other(
                            "transaction record has an invalid membership-lock node ID",
                        )
                    })?;
                    LeafRef::node(collection.clone(), id)
                }
                _ => {
                    return Err(StorageError::other(
                        "transaction record has invalid membership lock",
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
    db_prefix: &str,
    encoded: &[pb::CollectionChange],
) -> Result<Vec<TxCollectionChange>, StorageError> {
    encoded
        .iter()
        .map(|change| {
            if change.name.is_empty() || change.name.len() > MAX_COLLECTION_NAME_BYTES {
                return Err(StorageError::other(
                    "transaction record has an invalid collection name",
                ));
            }
            let parent = decode_collection_id(db_prefix, &change.parent_collection_id)?;
            let collection = decode_collection_id(db_prefix, &change.collection_id)?;
            if collection.id().is_root() {
                return Err(StorageError::other(
                    "transaction record changes the permanent root collection",
                ));
            }
            let op = match change.operation() {
                pb::collection_change::Operation::Create => TxCollectionOp::Create,
                pb::collection_change::Operation::Drop => TxCollectionOp::Drop,
                pb::collection_change::Operation::Unknown => {
                    return Err(StorageError::other(
                        "transaction record has an unknown collection operation",
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
    db_prefix: &str,
    encoded: &[Vec<u8>],
) -> Result<Vec<CollectionAddress>, StorageError> {
    encoded
        .iter()
        .map(|id| {
            let collection = decode_collection_id(db_prefix, id)?;
            if collection.id().is_root() {
                return Err(StorageError::other(
                    "transaction record prepares the permanent root collection",
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
        TxLock::Key { key, .. } => key.collection(),
        TxLock::Membership { leaf, .. } => leaf.collection(),
        TxLock::Directory { collection, .. } | TxLock::TopologyParticipant { collection } => {
            collection
        }
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
        TxLock::Key { key, typ } => locks.key_locks.push(pb::KeyLock {
            key: key.key().to_vec(),
            lock_type: lock_type_to_proto(*typ) as i32,
        }),
        TxLock::Membership { leaf, typ } => {
            let target = match leaf.node_id() {
                Some(id) => pb::membership_lock::Target::Node(id.as_bytes().to_vec()),
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
        TxLock::TopologyParticipant { .. } => {
            locks.topology_participant = true;
        }
    }
}

fn decode_collection_id(
    db_prefix: &str,
    collection_id: &[u8],
) -> Result<CollectionAddress, StorageError> {
    let id = CollectionId::from_slice(collection_id)
        .ok_or_else(|| StorageError::other("transaction record has an invalid collection ID"))?;
    Ok(CollectionAddress::new(db_prefix, id))
}

fn validate_database_membership(
    record: &TxRecord,
    expected_db_prefix: Option<&str>,
) -> Result<(), StorageError> {
    let mut db_prefix: Option<String> = None;
    let mut check = |collection: &CollectionAddress| -> Result<(), StorageError> {
        if expected_db_prefix.is_some_and(|expected| expected != collection.db_prefix()) {
            return Err(StorageError::other(
                "transaction-record path does not match its database prefix",
            ));
        }
        match db_prefix.as_deref() {
            Some(root) if root != collection.db_prefix() => Err(StorageError::other(
                "transaction record spans multiple database prefixes",
            )),
            Some(_) => Ok(()),
            None => {
                db_prefix = Some(collection.db_prefix().to_string());
                Ok(())
            }
        }
    };
    for write in &record.writes {
        check(write.key.collection())?;
    }
    for lock in &record.locks {
        match lock {
            TxLock::Key { key, .. } => check(key.collection())?,
            TxLock::Membership { leaf, .. } => check(leaf.collection())?,
            TxLock::Directory { collection, .. } | TxLock::TopologyParticipant { collection } => {
                check(collection)?
            }
        }
    }
    for change in &record.collection_changes {
        check(&change.parent)?;
        check(&change.collection)?;
    }
    for collection in &record.prepared_collections {
        check(collection)?;
    }
    Ok(())
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

    fn collection(db_prefix: &str, byte: u8) -> CollectionAddress {
        CollectionAddress::new(db_prefix, CollectionId::from_slice(&[byte; 16]).unwrap())
    }

    fn record_with_status(status: TxCommitStatus) -> TxRecord {
        let mut record = TxRecord::new(TxId::from_bytes(vec![1, 2, 3, 4]), status);
        record.timestamp = Some(UNIX_EPOCH + Duration::from_secs(1_700_000_000));
        record
    }

    fn complete_record(db_prefix: &str) -> TxRecord {
        let parent = collection(db_prefix, 1);
        let created = collection(db_prefix, 2);
        let dropped = collection(db_prefix, 3);
        TxRecord {
            id: TxId::from_bytes(vec![1, 2, 3, 4]),
            timestamp: Some(UNIX_EPOCH + Duration::from_secs(42)),
            status: TxCommitStatus::Pending,
            writes: vec![
                TxWrite {
                    key: LogicalKey::new(parent.clone(), b"value"),
                    value: Arc::from(&b"contents"[..]),
                    deleted: false,
                    prev_writer: TxId::from_bytes(vec![9]),
                },
                TxWrite {
                    key: LogicalKey::new(parent.clone(), b"deleted"),
                    value: Arc::from(&[][..]),
                    deleted: true,
                    prev_writer: TxId::from_bytes(vec![8]),
                },
            ],
            locks: vec![
                TxLock::Key {
                    key: LogicalKey::new(parent.clone(), b"entry"),
                    typ: LockType::Write,
                },
                TxLock::Membership {
                    leaf: LeafRef::root(parent.clone()),
                    typ: LockType::Read,
                },
                TxLock::Membership {
                    leaf: LeafRef::node(parent.clone(), NodeId::from_bytes([7; 16])),
                    typ: LockType::Create,
                },
                TxLock::Directory {
                    collection: parent.clone(),
                    typ: LockType::Write,
                },
                TxLock::TopologyParticipant {
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

    fn encoded_record() -> pb::TransactionRecord {
        pb::TransactionRecord {
            timestamp: Some(prost_types::Timestamp {
                seconds: 42,
                nanos: 0,
            }),
            status: pb::transaction_record::Status::Committed as i32,
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

    fn assert_rejected(encoded: pb::TransactionRecord) {
        let bytes = encoded.encode_to_vec();
        assert!(
            TxRecordCodec::decode("db", &TxId::from_bytes(vec![1]), &bytes).is_err(),
            "malformed transaction record unexpectedly decoded"
        );
    }

    #[test]
    fn every_status_round_trips_through_full_and_status_only_decode() {
        for status in [
            TxCommitStatus::Committed,
            TxCommitStatus::Aborted,
            TxCommitStatus::Pending,
            TxCommitStatus::Wounded,
        ] {
            let record = record_with_status(status);
            let bytes = TxRecordCodec::encode(&record).unwrap();
            assert_eq!(TxRecordCodec::decode_status(&bytes).unwrap(), status);
            assert_eq!(
                TxRecordCodec::decode("db", &record.id, &bytes)
                    .unwrap()
                    .status,
                status
            );
        }

        assert!(TxRecordCodec::encode(&record_with_status(TxCommitStatus::Unknown)).is_err());
        for status in [pb::transaction_record::Status::Default as i32, 99] {
            let mut encoded = encoded_record();
            encoded.status = status;
            let bytes = encoded.encode_to_vec();
            assert!(TxRecordCodec::decode_status(&bytes).is_err());
            assert!(TxRecordCodec::decode("db", &TxId::from_bytes(vec![1]), &bytes).is_err());
        }
    }

    #[test]
    fn repeated_fields_and_every_lock_shape_round_trip() {
        let record = complete_record("db");
        let bytes = TxRecordCodec::encode(&record).unwrap();
        let decoded = TxRecordCodec::decode("db", &record.id, &bytes).unwrap();

        assert_eq!(decoded.id, record.id);
        assert_eq!(decoded.timestamp, record.timestamp);
        assert_eq!(decoded.status, record.status);
        assert_eq!(decoded.writes, record.writes);
        assert_eq!(decoded.locks, record.locks);
        assert_eq!(decoded.collection_changes, record.collection_changes);
        assert_eq!(decoded.prepared_collections, record.prepared_collections);
    }

    #[test]
    fn every_collection_address_is_relocated_without_changing_bytes() {
        let record = complete_record("original");
        let bytes = TxRecordCodec::encode(&record).unwrap();
        let relocated = TxRecordCodec::decode("moved", &record.id, &bytes).unwrap();

        assert!(
            relocated
                .writes
                .iter()
                .all(|write| write.key.collection().db_prefix() == "moved")
        );
        assert!(relocated.locks.iter().all(|lock| match lock {
            TxLock::Key { key, .. } => key.collection().db_prefix() == "moved",
            TxLock::Membership { leaf, .. } => leaf.collection().db_prefix() == "moved",
            TxLock::Directory { collection, .. } | TxLock::TopologyParticipant { collection } => {
                collection.db_prefix() == "moved"
            }
        }));
        assert!(relocated.collection_changes.iter().all(|change| {
            change.parent.db_prefix() == "moved" && change.collection.db_prefix() == "moved"
        }));
        assert!(
            relocated
                .prepared_collections
                .iter()
                .all(|collection| collection.db_prefix() == "moved")
        );
        assert_eq!(TxRecordCodec::encode(&relocated).unwrap(), bytes);
    }

    #[test]
    fn one_transaction_cannot_span_database_roots() {
        let mut record = record_with_status(TxCommitStatus::Committed);
        record.writes = vec![
            TxWrite {
                key: LogicalKey::new(collection("first", 1), b"a"),
                value: Arc::from(&b"a"[..]),
                deleted: false,
                prev_writer: TxId::default(),
            },
            TxWrite {
                key: LogicalKey::new(collection("second", 1), b"b"),
                value: Arc::from(&b"b"[..]),
                deleted: false,
                prev_writer: TxId::default(),
            },
        ];

        assert!(TxRecordCodec::encode(&record).is_err());
    }

    #[test]
    fn malformed_protobuf_and_status_are_rejected() {
        assert!(TxRecordCodec::decode_status(&[0xff]).is_err());
        assert!(TxRecordCodec::decode("db", &TxId::from_bytes(vec![1]), &[0xff]).is_err());

        let mut encoded = encoded_record();
        encoded.status = pb::transaction_record::Status::Default as i32;
        assert_rejected(encoded);
    }

    #[test]
    fn status_only_decode_ignores_unrelated_semantic_payload_errors() {
        let mut encoded = encoded_record();
        encoded.writes.push(pb::CollectionWrites {
            collection_id: vec![1],
            writes: Vec::new(),
            locks: None,
        });
        let bytes = encoded.encode_to_vec();

        assert_eq!(
            TxRecordCodec::decode_status(&bytes).unwrap(),
            TxCommitStatus::Committed
        );
        assert!(TxRecordCodec::decode("db", &TxId::from_bytes(vec![1]), &bytes).is_err());
    }

    #[test]
    fn malformed_collection_ids_are_rejected_in_every_repeated_field() {
        let mut group = encoded_record();
        group.writes.push(pb::CollectionWrites {
            collection_id: vec![1; 15],
            writes: Vec::new(),
            locks: None,
        });
        assert_rejected(group);

        let mut parent = encoded_record();
        let mut change = encoded_change();
        change.parent_collection_id = vec![1; 15];
        parent.collection_changes.push(change);
        assert_rejected(parent);

        let mut child = encoded_record();
        let mut change = encoded_change();
        change.collection_id = vec![2; 15];
        child.collection_changes.push(change);
        assert_rejected(child);

        let mut prepared = encoded_record();
        prepared.prepared_collection_ids.push(vec![3; 15]);
        assert_rejected(prepared);
    }

    #[test]
    fn malformed_membership_lock_targets_are_rejected() {
        for target in [
            None,
            Some(pb::membership_lock::Target::Root(false)),
            Some(pb::membership_lock::Target::Node(Vec::new())),
            Some(pb::membership_lock::Target::Node(vec![7; 15])),
            Some(pb::membership_lock::Target::Node(
                b"0000000000000000000000".to_vec(),
            )),
        ] {
            let mut encoded = encoded_record();
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
            let mut encoded = encoded_record();
            let mut change = encoded_change();
            change.name = name;
            encoded.collection_changes.push(change);
            assert_rejected(encoded);
        }

        for operation in [pb::collection_change::Operation::Unknown as i32, 99] {
            let mut encoded = encoded_record();
            let mut change = encoded_change();
            change.operation = operation;
            encoded.collection_changes.push(change);
            assert_rejected(encoded);
        }

        let mut encoded = encoded_record();
        let mut change = encoded_change();
        change.collection_id = vec![0; 16];
        encoded.collection_changes.push(change);
        assert_rejected(encoded);
    }

    #[test]
    fn preparing_the_permanent_root_is_rejected() {
        let mut encoded = encoded_record();
        encoded.prepared_collection_ids.push(vec![0; 16]);
        assert_rejected(encoded);
    }
}
