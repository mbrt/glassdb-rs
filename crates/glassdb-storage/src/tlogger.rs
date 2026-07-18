//! Transaction-log persistence. Ported from the Go
//! `internal/storage/tlogger.go`. Logs are protobuf bodies; the commit status
//! and timestamp live in the body itself (ADR-019/ADR-023), not in object tags.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use glassdb_backend as backend;
use glassdb_concurr::rt;
use glassdb_data::{CollectionPath, KeyRef, LeafRef, TxId, paths};
use glassdb_proto as pb;
use prost::Message;

use crate::cached_store::{CachedStore, CasResult, Codec, Observation, Requirement};
use crate::error::StorageError;
use crate::lock::LockType;

/// The commit state of a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TxCommitStatus {
    #[default]
    Unknown,
    Ok,
    Aborted,
    Pending,
}

impl TxCommitStatus {
    /// Reports whether the status is terminal (committed or aborted).
    pub fn is_final(self) -> bool {
        matches!(self, TxCommitStatus::Ok | TxCommitStatus::Aborted)
    }
}

/// The full contents of a transaction log entry.
#[derive(Debug, Clone)]
pub struct TxLog {
    pub id: TxId,
    /// `None` means "use the current time when persisting".
    pub timestamp: Option<SystemTime>,
    pub status: TxCommitStatus,
    pub writes: Vec<TxWrite>,
    pub locks: Vec<TxLock>,
}

impl TxLog {
    /// Creates an empty log for the given transaction.
    pub fn new(id: TxId, status: TxCommitStatus) -> Self {
        TxLog {
            id,
            timestamp: None,
            status,
            writes: Vec::new(),
            locks: Vec::new(),
        }
    }
}

/// A single write within a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxWrite {
    pub key: KeyRef,
    pub value: Arc<[u8]>,
    pub deleted: bool,
    pub prev_writer: TxId,
}

/// A value written by a transaction, including whether it was a deletion or was
/// not written at all.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TValue {
    pub value: Arc<[u8]>,
    pub deleted: bool,
    /// True when the transaction committed but did not write this value (e.g.
    /// read-only lock).
    pub not_written: bool,
}

/// The commit status of a transaction along with its timestamp and version.
#[derive(Debug, Clone)]
pub struct TxStatus {
    pub status: TxCommitStatus,
    pub last_update: SystemTime,
    pub observation: Observation<TxLog>,
}

/// One backend page of transaction IDs from a deterministic log shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxListPage {
    pub ids: Vec<TxId>,
    pub next: Option<backend::ListCursor>,
}

/// A transaction lock backreference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxLock {
    Entry { key: KeyRef, typ: LockType },
    Structure { leaf: LeafRef, typ: LockType },
    Membership { leaf: LeafRef, typ: LockType },
}

impl TxLock {
    /// Returns the lock type recorded for this backreference.
    pub fn typ(&self) -> LockType {
        match self {
            TxLock::Entry { typ, .. }
            | TxLock::Structure { typ, .. }
            | TxLock::Membership { typ, .. } => *typ,
        }
    }
}

/// Reads and writes transaction logs under a path prefix.
#[derive(Clone)]
pub struct TLogger {
    prefix: String,
    logs: crate::cached_store::TypedCachedStore<TxLog>,
}

impl Codec for TxLog {
    type Value = TxLog;

    fn decode(path: &str, body: &[u8]) -> Result<Self::Value, StorageError> {
        let id = paths::transaction_id_of(path)
            .map_err(|error| StorageError::with_source("parsing transaction path", error))?;
        decode_tx_log(paths::db_root_of(path), &id, body)
    }

    fn encode(log: &Self::Value) -> Result<Vec<u8>, StorageError> {
        let timestamp = log
            .timestamp
            .ok_or_else(|| StorageError::other("transaction log has no persisted timestamp"))?;
        marshal_log(log, timestamp)
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
                    TxLock::Structure { leaf, .. } | TxLock::Membership { leaf, .. } => {
                        leaf.node_token().map_or(0, str::len)
                    }
                })
                .sum::<usize>()
            + std::mem::size_of::<TxLog>()
    }

    fn valid_path(path: &str) -> bool {
        paths::transaction_id_of(path).is_ok()
    }

    fn name() -> &'static str {
        "transaction log"
    }
}

impl TLogger {
    /// Creates a logger storing logs under `prefix`.
    pub fn new(objects: CachedStore, prefix: impl Into<String>) -> Self {
        TLogger {
            prefix: prefix.into(),
            logs: objects.typed(),
        }
    }

    /// Returns transaction status with an explicit generic requirement bound.
    pub async fn commit_status_at(
        &self,
        id: &TxId,
        requirement: Requirement,
    ) -> Result<TxStatus, StorageError> {
        let path = paths::from_transaction(&self.prefix, id);
        let observation = match self.cached_final(&path)? {
            Some(observation) => observation,
            None => self.logs.read(&path, requirement).await?,
        };
        let (status, last_update) = match observation.value() {
            Some(log) => (log.status, log.timestamp.unwrap_or(UNIX_EPOCH)),
            None => (TxCommitStatus::Unknown, UNIX_EPOCH),
        };
        Ok(TxStatus {
            status,
            last_update,
            observation,
        })
    }

    /// Reads the full transaction object with an explicit requirement bound.
    pub async fn get_at(
        &self,
        id: &TxId,
        requirement: Requirement,
    ) -> Result<Observation<TxLog>, StorageError> {
        let path = paths::from_transaction(&self.prefix, id);
        let observation = match self.cached_final(&path)? {
            Some(observation) => observation,
            None => self.logs.read(&path, requirement).await?,
        };
        if observation.is_absent() {
            Err(StorageError::NotFound)
        } else {
            Ok(observation)
        }
    }

    /// Creates a new transaction log entry, failing if one already exists.
    pub async fn set(&self, l: &TxLog) -> Result<Observation<TxLog>, StorageError> {
        let ts = l.timestamp.unwrap_or_else(rt::system_now);
        let mut persisted = l.clone();
        persisted.timestamp = Some(ts);
        match self
            .logs
            .create(
                &paths::from_transaction(&self.prefix, &l.id),
                None,
                Arc::new(persisted),
            )
            .await?
        {
            CasResult::Committed(observed) => Ok(observed),
            CasResult::Conflict => Err(StorageError::Precondition),
        }
    }

    /// Updates the log only if its current version matches `expected`.
    pub async fn set_if(
        &self,
        l: &TxLog,
        expected: &Observation<TxLog>,
    ) -> Result<Observation<TxLog>, StorageError> {
        let ts = l.timestamp.unwrap_or_else(rt::system_now);
        let mut persisted = l.clone();
        persisted.timestamp = Some(ts);
        match self
            .logs
            .compare_and_swap(expected, Arc::new(persisted))
            .await?
        {
            CasResult::Committed(observed) => Ok(observed),
            CasResult::Conflict => Err(StorageError::Precondition),
        }
    }

    /// Lists one page of transaction IDs from `shard`.
    pub async fn list_transaction_ids(
        &self,
        shard: usize,
        cursor: Option<&backend::ListCursor>,
        limit: backend::ListLimit,
    ) -> Result<TxListPage, StorageError> {
        let prefix = paths::transaction_shard_prefix(&self.prefix, shard);
        let page = self.logs.list(&prefix, cursor, limit).await?;
        let ids = page
            .objects
            .iter()
            .filter_map(|path| paths::transaction_id_of(path).ok())
            .collect();
        Ok(TxListPage {
            ids,
            next: page.next,
        })
    }

    /// Removes the log for `id`, ignoring not-found errors.
    pub async fn delete(&self, id: &TxId) -> Result<(), StorageError> {
        self.logs
            .delete(&paths::from_transaction(&self.prefix, id))
            .await?;
        Ok(())
    }

    fn cached_final(&self, path: &str) -> Result<Option<Observation<TxLog>>, StorageError> {
        Ok(self
            .logs
            .peek(path)?
            .filter(|observation| observation.value().is_some_and(|log| log.status.is_final())))
    }
}

fn write_value(w: &pb::Write) -> Arc<[u8]> {
    match &w.val_delete {
        Some(pb::write::ValDelete::Value(v)) => Arc::from(v.as_slice()),
        _ => Arc::from(&[] as &[u8]),
    }
}

fn write_deleted(w: &pb::Write) -> bool {
    matches!(&w.val_delete, Some(pb::write::ValDelete::Deleted(true)))
}

fn parse_log(buf: &[u8]) -> Result<pb::TransactionLog, StorageError> {
    pb::TransactionLog::decode(buf)
        .map_err(|e| StorageError::with_source("unmarshalling transaction log", e))
}

pub(crate) fn decode_tx_status(buf: &[u8]) -> Result<TxCommitStatus, StorageError> {
    let log = parse_log(buf)?;
    match log.status() {
        pb::transaction_log::Status::Committed => Ok(TxCommitStatus::Ok),
        pb::transaction_log::Status::Aborted => Ok(TxCommitStatus::Aborted),
        pb::transaction_log::Status::Pending => Ok(TxCommitStatus::Pending),
        pb::transaction_log::Status::Default => Err(StorageError::other("unknown commit status")),
    }
}

/// Decodes a transaction-log protobuf body into a [`TxLog`]. The status and
/// timestamp are taken from the body (not tags), which is what the v2 unified
/// transaction object relies on (ADR-019). Shared by [`TLogger::get`] and the
/// v2 [`crate::txobject`] codec.
pub(crate) fn decode_tx_log(db_root: &str, id: &TxId, buf: &[u8]) -> Result<TxLog, StorageError> {
    decode_tx_log_from_proto(db_root, id, &parse_log(buf)?)
}

fn decode_tx_log_from_proto(
    db_root: &str,
    id: &TxId,
    tr: &pb::TransactionLog,
) -> Result<TxLog, StorageError> {
    let status = match tr.status() {
        pb::transaction_log::Status::Committed => TxCommitStatus::Ok,
        pb::transaction_log::Status::Aborted => TxCommitStatus::Aborted,
        pb::transaction_log::Status::Pending => TxCommitStatus::Pending,
        pb::transaction_log::Status::Default => {
            return Err(StorageError::other("unknown commit status"));
        }
    };
    let mut res = TxLog {
        id: id.clone(),
        timestamp: tr.timestamp.map(proto_ts_to_system),
        status,
        writes: Vec::new(),
        locks: Vec::new(),
    };

    for cw in &tr.writes {
        let collection = decode_collection_path(db_root, cw.collection.as_ref())?;
        for w in &cw.writes {
            res.writes.push(TxWrite {
                key: KeyRef::new(collection.clone(), &w.key),
                value: write_value(w),
                deleted: write_deleted(w),
                prev_writer: TxId::from_bytes(w.prev_tid.clone()),
            });
        }
        if let Some(locks) = &cw.locks {
            for lock in &locks.entry_locks {
                res.locks.push(TxLock::Entry {
                    key: KeyRef::new(collection.clone(), &lock.key),
                    typ: parse_lock_type(lock.lock_type),
                });
            }
            for lock in &locks.leaf_locks {
                let leaf = match lock.target.as_ref() {
                    Some(pb::leaf_lock::Target::Root(true)) => LeafRef::root(collection.clone()),
                    Some(pb::leaf_lock::Target::Node(token)) if !token.is_empty() => {
                        LeafRef::node(collection.clone(), token.as_str())
                    }
                    _ => return Err(StorageError::other("transaction log has invalid leaf lock")),
                };
                let typ = parse_lock_type(lock.lock_type);
                let lock = match pb::lock::Scope::try_from(lock.scope) {
                    Ok(pb::lock::Scope::Structure) => TxLock::Structure { leaf, typ },
                    Ok(pb::lock::Scope::Membership) => TxLock::Membership { leaf, typ },
                    _ => {
                        return Err(StorageError::other(
                            "transaction log leaf lock has invalid scope",
                        ));
                    }
                };
                res.locks.push(lock);
            }
        }
    }
    Ok(res)
}

pub(crate) fn marshal_log(l: &TxLog, ts: SystemTime) -> Result<Vec<u8>, StorageError> {
    if l.id.is_unset() {
        return Err(StorageError::other("empty transaction ID"));
    }
    validate_single_database(l)?;
    let mut coll_writes: BTreeMap<CollectionPath, pb::CollectionWrites> = BTreeMap::new();

    for e in &l.writes {
        marshal_write(&mut coll_writes, e)?;
    }
    for e in &l.locks {
        marshal_lock(&mut coll_writes, e)?;
    }

    let status = match l.status {
        TxCommitStatus::Ok => pb::transaction_log::Status::Committed,
        TxCommitStatus::Aborted => pb::transaction_log::Status::Aborted,
        TxCommitStatus::Pending => pb::transaction_log::Status::Pending,
        TxCommitStatus::Unknown => {
            return Err(StorageError::other("unsupported commit status"));
        }
    };

    let tr = pb::TransactionLog {
        timestamp: Some(system_to_proto_ts(ts)),
        status: status as i32,
        writes: coll_writes.into_values().collect(),
    };
    Ok(tr.encode_to_vec())
}

fn marshal_write(
    coll_writes: &mut BTreeMap<CollectionPath, pb::CollectionWrites>,
    e: &TxWrite,
) -> Result<(), StorageError> {
    let val_delete = if e.deleted {
        pb::write::ValDelete::Deleted(true)
    } else {
        pb::write::ValDelete::Value(e.value.to_vec())
    };
    let write = pb::Write {
        key: e.key.key().to_vec(),
        prev_tid: e.prev_writer.as_bytes().to_vec(),
        val_delete: Some(val_delete),
    };
    let collection = e.key.collection();
    let coll = coll_writes
        .entry(collection.clone())
        .or_insert_with(|| pb::CollectionWrites {
            collection: Some(encode_collection_path(collection)),
            writes: Vec::new(),
            locks: Some(pb::CollectionLocks::default()),
        });
    coll.writes.push(write);
    Ok(())
}

fn marshal_lock(
    coll_writes: &mut BTreeMap<CollectionPath, pb::CollectionWrites>,
    lock: &TxLock,
) -> Result<(), StorageError> {
    let collection = match lock {
        TxLock::Entry { key, .. } => key.collection(),
        TxLock::Structure { leaf, .. } | TxLock::Membership { leaf, .. } => leaf.collection(),
    };
    let coll = coll_writes
        .entry(collection.clone())
        .or_insert_with(|| pb::CollectionWrites {
            collection: Some(encode_collection_path(collection)),
            writes: Vec::new(),
            locks: Some(pb::CollectionLocks::default()),
        });
    let clocks = coll.locks.get_or_insert_with(pb::CollectionLocks::default);

    match lock {
        TxLock::Entry { key, typ } => clocks.entry_locks.push(pb::EntryLock {
            key: key.key().to_vec(),
            lock_type: lock_type_to_proto(*typ) as i32,
        }),
        TxLock::Structure { leaf, typ } | TxLock::Membership { leaf, typ } => {
            let target = match leaf.node_token() {
                Some(token) => pb::leaf_lock::Target::Node(token.to_string()),
                None => pb::leaf_lock::Target::Root(true),
            };
            let scope = match lock {
                TxLock::Structure { .. } => pb::lock::Scope::Structure,
                TxLock::Membership { .. } => pb::lock::Scope::Membership,
                TxLock::Entry { .. } => unreachable!(),
            };
            clocks.leaf_locks.push(pb::LeafLock {
                target: Some(target),
                lock_type: lock_type_to_proto(*typ) as i32,
                scope: scope as i32,
            });
        }
    }
    Ok(())
}

fn encode_collection_path(collection: &CollectionPath) -> pb::CollectionPath {
    pb::CollectionPath {
        segments: collection.segments().map(<[u8]>::to_vec).collect(),
    }
}

fn decode_collection_path(
    db_root: &str,
    collection: Option<&pb::CollectionPath>,
) -> Result<CollectionPath, StorageError> {
    let collection = collection
        .filter(|collection| !collection.segments.is_empty())
        .ok_or_else(|| StorageError::other("transaction log has no collection path"))?;
    Ok(CollectionPath::from_segments(
        db_root,
        collection.segments.iter(),
    ))
}

fn validate_single_database(log: &TxLog) -> Result<(), StorageError> {
    let mut db_root: Option<String> = None;
    let mut check = |collection: &CollectionPath| -> Result<(), StorageError> {
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
            TxLock::Structure { leaf, .. } | TxLock::Membership { leaf, .. } => {
                check(leaf.collection())?
            }
        }
    }
    Ok(())
}

fn lock_type_to_proto(t: LockType) -> pb::lock::LockType {
    match t {
        LockType::None => pb::lock::LockType::None,
        LockType::Read => pb::lock::LockType::Read,
        LockType::Write => pb::lock::LockType::Write,
        LockType::Create => pb::lock::LockType::Create,
        LockType::Unknown => pb::lock::LockType::Unknown,
    }
}

fn parse_lock_type(t: i32) -> LockType {
    match pb::lock::LockType::try_from(t) {
        Ok(pb::lock::LockType::None) => LockType::None,
        Ok(pb::lock::LockType::Read) => LockType::Read,
        Ok(pb::lock::LockType::Write) => LockType::Write,
        Ok(pb::lock::LockType::Create) => LockType::Create,
        _ => LockType::Unknown,
    }
}

fn system_to_proto_ts(t: SystemTime) -> prost_types::Timestamp {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => prost_types::Timestamp {
            seconds: d.as_secs() as i64,
            nanos: d.subsec_nanos() as i32,
        },
        Err(e) => {
            let d = e.duration();
            prost_types::Timestamp {
                seconds: -(d.as_secs() as i64),
                nanos: -(d.subsec_nanos() as i32),
            }
        }
    }
}

fn proto_ts_to_system(ts: prost_types::Timestamp) -> SystemTime {
    if ts.seconds >= 0 {
        UNIX_EPOCH + Duration::new(ts.seconds as u64, ts.nanos.max(0) as u32)
    } else {
        UNIX_EPOCH - Duration::new((-ts.seconds) as u64, ts.nanos.unsigned_abs())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Timeline;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::RecordingBackend;

    fn new_tlogger() -> TLogger {
        let backend = Arc::new(MemoryBackend::new());
        let timeline = Timeline::new();
        let objects = CachedStore::new(backend, 1 << 20, timeline.clone());
        TLogger::new(objects, "db")
    }

    #[tokio::test]
    async fn round_trip() {
        let t = new_tlogger();
        let id = TxId::from_bytes(vec![1, 2, 3, 4]);
        let collection = CollectionPath::new("db", b"root");
        let key = KeyRef::new(collection.clone(), b"hello");
        let log = TxLog {
            id: id.clone(),
            timestamp: Some(UNIX_EPOCH + Duration::from_millis(1_700_000_000_000)),
            status: TxCommitStatus::Ok,
            writes: vec![TxWrite {
                key: key.clone(),
                value: Arc::from(&b"world"[..]),
                deleted: false,
                prev_writer: TxId::from_bytes(vec![9]),
            }],
            locks: vec![
                TxLock::Structure {
                    leaf: LeafRef::root(collection),
                    typ: LockType::Read,
                },
                TxLock::Entry {
                    key: key.clone(),
                    typ: LockType::Write,
                },
            ],
        };
        t.set(&log).await.unwrap();

        let got = t.get_at(&id, Requirement::Any).await.unwrap();
        let got = got.value().unwrap();
        assert_eq!(got.status, TxCommitStatus::Ok);
        assert_eq!(got.writes, log.writes);
        // Locks include the collection lock and the key lock.
        assert!(got.locks.contains(&TxLock::Structure {
            leaf: LeafRef::root(CollectionPath::new("db", b"root")),
            typ: LockType::Read,
        }));
        assert!(got.locks.contains(&TxLock::Entry {
            key,
            typ: LockType::Write,
        }));

        let status = t.commit_status_at(&id, Requirement::Any).await.unwrap();
        assert_eq!(status.status, TxCommitStatus::Ok);
    }

    #[tokio::test]
    async fn commit_status_unknown_when_absent() {
        let t = new_tlogger();
        let status = t
            .commit_status_at(&TxId::from_bytes(vec![7]), Requirement::Any)
            .await
            .unwrap();
        assert_eq!(status.status, TxCommitStatus::Unknown);
    }

    #[tokio::test]
    async fn get_returns_log_and_version() {
        let t = new_tlogger();
        let id = TxId::from_bytes(vec![1, 2, 3, 4]);
        let key = KeyRef::new(CollectionPath::new("db", b"root"), b"hello");
        let mut log = TxLog::new(id.clone(), TxCommitStatus::Ok);
        log.timestamp = Some(UNIX_EPOCH + Duration::from_secs(1_700_000_000));
        log.writes = vec![TxWrite {
            key,
            value: Arc::from(&b"world"[..]),
            deleted: false,
            prev_writer: TxId::default(),
        }];
        let stored_v = t.set(&log).await.unwrap();

        let got = t.get_at(&id, Requirement::Any).await.unwrap();
        let version = got.revision().cloned();
        let got = got.value().unwrap();
        assert_eq!(got.status, TxCommitStatus::Ok);
        assert_eq!(got.writes, log.writes);
        assert_eq!(got.timestamp, log.timestamp);
        assert_eq!(version.as_ref(), stored_v.revision());
    }

    #[test]
    fn encoded_collection_paths_are_relocatable() {
        let id = TxId::from_bytes(vec![1]);
        let mut log = TxLog::new(id.clone(), TxCommitStatus::Ok);
        log.timestamp = Some(UNIX_EPOCH + Duration::from_secs(42));
        log.writes.push(TxWrite {
            key: KeyRef::new(
                CollectionPath::new("original", b"parent").child(b"child"),
                b"key",
            ),
            value: Arc::from(&b"value"[..]),
            deleted: false,
            prev_writer: TxId::default(),
        });

        let encoded = marshal_log(&log, log.timestamp.unwrap()).unwrap();
        let relocated = decode_tx_log("moved", &id, &encoded).unwrap();

        assert_eq!(relocated.writes[0].key.collection().db_root(), "moved");
        assert_eq!(
            relocated.writes[0]
                .key
                .collection()
                .segments()
                .collect::<Vec<_>>(),
            vec![b"parent".as_slice(), b"child".as_slice()]
        );
        assert_eq!(
            marshal_log(&relocated, relocated.timestamp.unwrap()).unwrap(),
            encoded,
            "the database root must not be encoded in the transaction body"
        );
    }

    #[test]
    fn one_transaction_cannot_span_database_roots() {
        let mut log = TxLog::new(TxId::from_bytes(vec![1]), TxCommitStatus::Ok);
        log.timestamp = Some(UNIX_EPOCH);
        log.writes = vec![
            TxWrite {
                key: KeyRef::new(CollectionPath::new("first", b"c"), b"a"),
                value: Arc::from(&b"a"[..]),
                deleted: false,
                prev_writer: TxId::default(),
            },
            TxWrite {
                key: KeyRef::new(CollectionPath::new("second", b"c"), b"b"),
                value: Arc::from(&b"b"[..]),
                deleted: false,
                prev_writer: TxId::default(),
            },
        ];

        assert!(marshal_log(&log, UNIX_EPOCH).is_err());
    }

    #[tokio::test]
    async fn finalized_logs_are_served_from_the_typed_cache() {
        let backend = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let operations = backend.log();
        let timeline = Timeline::new();
        let objects = CachedStore::new(Arc::new(backend), 1 << 20, timeline.clone());
        let logger = TLogger::new(objects, "db");
        let id = TxId::from_bytes(vec![4, 3, 2, 1]);
        logger
            .set(&TxLog::new(id.clone(), TxCommitStatus::Aborted))
            .await
            .unwrap();
        operations.lock().unwrap().clear();

        logger.get_at(&id, Requirement::Any).await.unwrap();
        logger.get_at(&id, Requirement::Any).await.unwrap();

        let conditional_reads = operations
            .lock()
            .unwrap()
            .iter()
            .filter(|operation| operation.op == "read_if_modified")
            .count();
        assert_eq!(conditional_reads, 0);
    }

    #[tokio::test]
    async fn pending_logs_still_obey_generic_freshness() {
        let backend = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let operations = backend.log();
        let timeline = Timeline::new();
        let objects = CachedStore::new(Arc::new(backend), 1 << 20, timeline.clone());
        let logger = TLogger::new(objects, "db");
        let id = TxId::from_bytes(vec![4, 3, 2, 2]);
        logger
            .set(&TxLog::new(id.clone(), TxCommitStatus::Pending))
            .await
            .unwrap();
        operations.lock().unwrap().clear();

        logger
            .get_at(&id, Requirement::AtLeast(timeline.now()))
            .await
            .unwrap();
        logger
            .get_at(&id, Requirement::AtLeast(timeline.now()))
            .await
            .unwrap();

        let conditional_reads = operations
            .lock()
            .unwrap()
            .iter()
            .filter(|operation| operation.op == "read_if_modified")
            .count();
        assert_eq!(conditional_reads, 2);
    }

    #[tokio::test]
    async fn list_transaction_ids_pages_one_shard() {
        let t = new_tlogger();
        let ids = [
            TxId::from_bytes(vec![1, 2]),
            TxId::from_bytes(vec![1, 3]),
            TxId::from_bytes(vec![1, 4]),
        ];
        for id in &ids {
            t.set(&TxLog::new(id.clone(), TxCommitStatus::Aborted))
                .await
                .unwrap();
        }
        let shard = paths::transaction_shard(&ids[0]);
        assert!(ids.iter().all(|id| paths::transaction_shard(id) == shard));
        let limit = backend::ListLimit::new(2).unwrap();
        let first = t.list_transaction_ids(shard, None, limit).await.unwrap();
        assert_eq!(first.ids.len(), 2);
        let second = t
            .list_transaction_ids(shard, first.next.as_ref(), limit)
            .await
            .unwrap();
        assert!(second.next.is_none());
        let mut listed = first.ids;
        listed.extend(second.ids);
        listed.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        let mut expected: Vec<TxId> = ids.to_vec();
        expected.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        assert_eq!(listed, expected);
    }
}
