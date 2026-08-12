//! Transaction-log persistence. Ported from the Go
//! `internal/storage/tlogger.go`. Logs are protobuf bodies; the commit status
//! and timestamp live in the body itself (ADR-019/ADR-023), not in object tags.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use glassdb_backend as backend;
use glassdb_concurr::rt;
use glassdb_data::{DbRoot, ObjectPath, TxId};

use crate::cached_store::{CachedStore, CasResult, ObjectKey, Observation, Requirement};
use crate::error::StorageError;
use crate::transaction::{TxCommitStatus, TxLifecycleRelation, TxLog, TxLogCodec, TxRecordState};

const TRANSACTION_SHARD_COUNT: usize = 64 * 64;

/// The commit status of a transaction along with its timestamp and version.
#[derive(Debug, Clone)]
pub struct TxStatus {
    pub status: TxCommitStatus,
    pub last_update: SystemTime,
    pub observation: Observation<TxLog>,
}

impl TxStatus {
    /// Creates a transaction status view from an observed log object.
    pub fn from_observation(observation: Observation<TxLog>) -> Self {
        let (status, last_update) = match observation.value() {
            Some(log) => (log.status, log.timestamp.unwrap_or(UNIX_EPOCH)),
            None => (TxCommitStatus::Unknown, UNIX_EPOCH),
        };
        Self {
            status,
            last_update,
            observation,
        }
    }
}

/// One backend page of transaction IDs from a deterministic log shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxListPage {
    pub ids: Vec<TxId>,
    pub next: Option<backend::ListCursor>,
}

/// Reads and writes transaction logs under a path prefix.
#[derive(Clone)]
pub struct TLogger {
    db_root: DbRoot,
    logs: crate::cached_store::TypedCachedStore<TxLogCodec>,
}

impl TLogger {
    /// Creates a logger storing logs under `db_root`.
    pub fn new(objects: CachedStore, db_root: DbRoot) -> Self {
        TLogger {
            db_root,
            logs: objects.typed(),
        }
    }

    /// Returns transaction status with an explicit generic requirement bound.
    pub async fn commit_status_at(
        &self,
        id: &TxId,
        requirement: Requirement,
    ) -> Result<TxStatus, StorageError> {
        let path = ObjectKey::from(ObjectPath::Transaction {
            db_root: self.db_root.clone(),
            id: id.clone(),
        });
        let observation = match self.cached_final(&path)? {
            Some(observation) => observation,
            None => self.logs.read(path, requirement).await?,
        };
        Ok(TxStatus::from_observation(observation))
    }

    /// Reads the full transaction object with an explicit requirement bound.
    pub async fn get_at(
        &self,
        id: &TxId,
        requirement: Requirement,
    ) -> Result<Observation<TxLog>, StorageError> {
        let path = ObjectKey::from(ObjectPath::Transaction {
            db_root: self.db_root.clone(),
            id: id.clone(),
        });
        let observation = match self.cached_final(&path)? {
            Some(observation) => observation,
            None => self.logs.read(path, requirement).await?,
        };
        if observation.is_absent() {
            Err(StorageError::NotFound)
        } else {
            Ok(observation)
        }
    }

    /// Creates a transaction's initial log, failing if one already exists.
    pub async fn set(&self, l: &TxLog) -> Result<Observation<TxLog>, StorageError> {
        validate_lifecycle_transition(None, Some(l.status))?;
        let ts = l.timestamp.unwrap_or_else(rt::system_now);
        let mut persisted = l.clone();
        persisted.timestamp = Some(ts);
        let path = ObjectPath::Transaction {
            db_root: self.db_root.clone(),
            id: l.id.clone(),
        };
        match self.logs.create(path, None, Arc::new(persisted)).await? {
            CasResult::Committed(observed) => Ok(observed),
            CasResult::Conflict => Err(StorageError::Precondition),
        }
    }

    /// Transitions a mutable log if its current version matches `expected`.
    ///
    /// Immutable logs are not replaceable. A wounded log has one permitted
    /// transition to aborted when its owner acknowledges retirement.
    pub async fn set_if(
        &self,
        l: &TxLog,
        expected: &Observation<TxLog>,
    ) -> Result<Observation<TxLog>, StorageError> {
        let current = expected
            .value()
            .ok_or_else(|| StorageError::other("transaction log CAS requires a present value"))?;
        validate_lifecycle_transition(Some(current.status), Some(l.status))?;
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

    /// Returns every physical transaction-log shard.
    pub fn transaction_shards(&self) -> impl Iterator<Item = usize> {
        0..TRANSACTION_SHARD_COUNT
    }

    /// Returns the physical shard containing `id`.
    pub fn transaction_shard(&self, id: &TxId) -> usize {
        ObjectPath::transaction_shard(id)
    }

    /// Lists one page of transaction IDs from `shard`.
    pub async fn list_transaction_ids(
        &self,
        shard: usize,
        cursor: Option<&backend::ListCursor>,
        limit: backend::ListLimit,
    ) -> Result<TxListPage, StorageError> {
        let prefix = ObjectPath::transaction_shard_prefix(&self.db_root, shard);
        let page = self.logs.list(&prefix, cursor, limit).await?;
        let ids = page
            .objects
            .iter()
            .filter_map(|path| match path.object_path() {
                ObjectPath::Transaction { db_root, id } if db_root == &self.db_root => {
                    Some(id.clone())
                }
                _ => None,
            })
            .collect();
        Ok(TxListPage {
            ids,
            next: page.next,
        })
    }

    /// Removes an exact immutable log during GC, converging if it is missing.
    ///
    /// Pending and wounded logs must first transition to aborted; deleting one
    /// directly fails locally with [`StorageError::Precondition`].
    pub async fn delete(&self, expected: &Observation<TxLog>) -> Result<(), StorageError> {
        let current = expected.value().ok_or_else(|| {
            StorageError::other("transaction log deletion requires a present value")
        })?;
        validate_lifecycle_transition(Some(current.status), None)?;
        self.logs.delete(expected).await?;
        Ok(())
    }

    fn cached_final(&self, path: &ObjectKey) -> Result<Option<Observation<TxLog>>, StorageError> {
        Ok(self.logs.peek(path)?.filter(|observation| {
            observation
                .value()
                .is_some_and(|log| log.status.is_immutable())
        }))
    }
}

/// Validates the durable transaction lifecycle before any backend operation.
///
/// Immutable objects are cached indefinitely, so replacing one would invalidate
/// knowledge held by every database instance that has observed it. `Wounded`
/// is terminal for transaction semantics but remains mutable until its owner
/// acknowledges it as `Aborted`.
fn validate_lifecycle_transition(
    current: Option<TxCommitStatus>,
    next: Option<TxCommitStatus>,
) -> Result<(), StorageError> {
    let current = TxRecordState::try_from_status(current)?;
    let next = TxRecordState::try_from_status(next)?;
    if current == TxRecordState::Missing && next == TxRecordState::Missing {
        return Err(StorageError::other(
            "transaction lifecycle transition has no source or destination",
        ));
    }

    match current.relation_to(next) {
        TxLifecycleRelation::CanAdvance => Ok(()),
        TxLifecycleRelation::Same
            if current == TxRecordState::Pending && next == TxRecordState::Pending =>
        {
            Ok(())
        }
        TxLifecycleRelation::Same | TxLifecycleRelation::Blocks => Err(StorageError::Precondition),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use crate::Timeline;
    use crate::lock::LockType;
    use crate::transaction::{TxCollectionChange, TxCollectionOp, TxLock, TxWrite};
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{
        BackendOp, HookBackend, HookFuture, OpLog, RecordingBackend,
    };
    use glassdb_data::{CollectionAddress, CollectionId, KeyRef, LeafRef};
    use tokio::sync::Notify;

    fn db_root() -> DbRoot {
        DbRoot::try_from("db").unwrap()
    }

    fn new_tlogger() -> TLogger {
        let backend = Arc::new(MemoryBackend::new());
        let timeline = Timeline::new();
        let objects = CachedStore::new(backend, 1 << 20, timeline.clone(), None);
        TLogger::new(objects, db_root())
    }

    fn test_collection(db_root: &str, byte: u8) -> CollectionAddress {
        CollectionAddress::new(db_root, CollectionId::from_slice(&[byte; 16]).unwrap())
    }

    fn new_recording_tlogger() -> (TLogger, OpLog) {
        let backend = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let operations = backend.log();
        let objects = CachedStore::new(Arc::new(backend), 1 << 20, Timeline::new(), None);
        (TLogger::new(objects, db_root()), operations)
    }

    fn assert_operations(operations: &OpLog, expected: &[&str]) {
        let mut operations = operations.lock().unwrap();
        let actual: Vec<_> = operations.iter().map(|operation| operation.op).collect();
        assert_eq!(actual, expected);
        operations.clear();
    }

    #[tokio::test]
    async fn mutations_reject_transaction_identity_mismatches_before_backend_io() {
        let (logger, operations) = new_recording_tlogger();
        let id = TxId::from_bytes(vec![1, 2, 3, 4]);
        let observed = logger
            .set(&TxLog::new(id, TxCommitStatus::Pending))
            .await
            .unwrap();
        assert_operations(&operations, &["write_if_not_exists"]);

        let different_id = TxId::from_bytes(vec![4, 3, 2, 1]);
        assert!(
            logger
                .set_if(
                    &TxLog::new(different_id, TxCommitStatus::Pending),
                    &observed,
                )
                .await
                .is_err()
        );
        assert_operations(&operations, &[]);

        let mut wrong_database =
            TxLog::new(TxId::from_bytes(vec![5, 6, 7, 8]), TxCommitStatus::Pending);
        wrong_database.writes.push(TxWrite {
            key: KeyRef::new(test_collection("other", 1), b"key"),
            value: Arc::from(&b"value"[..]),
            deleted: false,
            prev_writer: TxId::default(),
        });
        assert!(logger.set(&wrong_database).await.is_err());
        assert_operations(&operations, &[]);
    }

    #[tokio::test]
    async fn allowed_lifecycle_transitions_add_no_backend_reads() {
        let (logger, operations) = new_recording_tlogger();

        for (suffix, status) in [
            (1, TxCommitStatus::Pending),
            (2, TxCommitStatus::Ok),
            (3, TxCommitStatus::Aborted),
            (7, TxCommitStatus::Wounded),
        ] {
            let id = TxId::from_bytes(vec![9, suffix]);
            let observed = logger.set(&TxLog::new(id, status)).await.unwrap();
            assert_operations(&operations, &["write_if_not_exists"]);
            if status.is_immutable() {
                logger.delete(&observed).await.unwrap();
                assert_operations(&operations, &["delete_if"]);
            }
        }

        let committed_id = TxId::from_bytes(vec![9, 4]);
        let pending = logger
            .set(&TxLog::new(committed_id.clone(), TxCommitStatus::Pending))
            .await
            .unwrap();
        assert_operations(&operations, &["write_if_not_exists"]);
        let refreshed = logger
            .set_if(
                &TxLog::new(committed_id.clone(), TxCommitStatus::Pending),
                &pending,
            )
            .await
            .unwrap();
        assert_operations(&operations, &["write_if"]);
        let committed = logger
            .set_if(&TxLog::new(committed_id, TxCommitStatus::Ok), &refreshed)
            .await
            .unwrap();
        assert_operations(&operations, &["write_if"]);
        logger.delete(&committed).await.unwrap();
        assert_operations(&operations, &["delete_if"]);

        let aborted_id = TxId::from_bytes(vec![9, 5]);
        let pending = logger
            .set(&TxLog::new(aborted_id.clone(), TxCommitStatus::Pending))
            .await
            .unwrap();
        assert_operations(&operations, &["write_if_not_exists"]);
        logger
            .set_if(&TxLog::new(aborted_id, TxCommitStatus::Aborted), &pending)
            .await
            .unwrap();
        assert_operations(&operations, &["write_if"]);

        let wounded_id = TxId::from_bytes(vec![9, 6]);
        let pending = logger
            .set(&TxLog::new(wounded_id.clone(), TxCommitStatus::Pending))
            .await
            .unwrap();
        assert_operations(&operations, &["write_if_not_exists"]);
        let wounded = logger
            .set_if(
                &TxLog::new(wounded_id.clone(), TxCommitStatus::Wounded),
                &pending,
            )
            .await
            .unwrap();
        assert_operations(&operations, &["write_if"]);
        let aborted = logger
            .set_if(&TxLog::new(wounded_id, TxCommitStatus::Aborted), &wounded)
            .await
            .unwrap();
        assert_operations(&operations, &["write_if"]);
        logger.delete(&aborted).await.unwrap();
        assert_operations(&operations, &["delete_if"]);
    }

    #[tokio::test]
    async fn rejected_lifecycle_transitions_issue_no_backend_operations() {
        let (logger, operations) = new_recording_tlogger();

        for (suffix, current) in [(1, TxCommitStatus::Ok), (2, TxCommitStatus::Aborted)] {
            let id = TxId::from_bytes(vec![10, suffix]);
            let observed = logger.set(&TxLog::new(id.clone(), current)).await.unwrap();
            assert_operations(&operations, &["write_if_not_exists"]);
            for next in [
                TxCommitStatus::Pending,
                TxCommitStatus::Ok,
                TxCommitStatus::Aborted,
                TxCommitStatus::Wounded,
            ] {
                assert!(matches!(
                    logger
                        .set_if(&TxLog::new(id.clone(), next), &observed)
                        .await,
                    Err(StorageError::Precondition)
                ));
                assert_operations(&operations, &[]);
            }
            assert_eq!(
                logger
                    .commit_status_at(&id, Requirement::Any)
                    .await
                    .unwrap()
                    .status,
                current
            );
            assert_operations(&operations, &[]);
        }

        let pending_id = TxId::from_bytes(vec![10, 3]);
        let pending = logger
            .set(&TxLog::new(pending_id.clone(), TxCommitStatus::Pending))
            .await
            .unwrap();
        assert_operations(&operations, &["write_if_not_exists"]);
        assert!(matches!(
            logger.delete(&pending).await,
            Err(StorageError::Precondition)
        ));
        assert_operations(&operations, &[]);

        let wounded_id = TxId::from_bytes(vec![10, 4]);
        let wounded = logger
            .set(&TxLog::new(wounded_id.clone(), TxCommitStatus::Wounded))
            .await
            .unwrap();
        assert_operations(&operations, &["write_if_not_exists"]);
        assert!(matches!(
            logger.delete(&wounded).await,
            Err(StorageError::Precondition)
        ));
        assert_operations(&operations, &[]);
        for next in [
            TxCommitStatus::Pending,
            TxCommitStatus::Ok,
            TxCommitStatus::Wounded,
        ] {
            assert!(matches!(
                logger
                    .set_if(&TxLog::new(wounded_id.clone(), next), &wounded)
                    .await,
                Err(StorageError::Precondition)
            ));
            assert_operations(&operations, &[]);
        }
        assert_eq!(
            logger
                .commit_status_at(&pending_id, Requirement::Any)
                .await
                .unwrap()
                .status,
            TxCommitStatus::Pending
        );
        assert_operations(&operations, &[]);

        assert!(matches!(
            logger
                .set_if(&TxLog::new(pending_id, TxCommitStatus::Unknown), &pending,)
                .await,
            Err(StorageError::Other { .. })
        ));
        assert_operations(&operations, &[]);

        assert!(matches!(
            logger
                .set(&TxLog::new(
                    TxId::from_bytes(vec![10, 4]),
                    TxCommitStatus::Unknown,
                ))
                .await,
            Err(StorageError::Other { .. })
        ));
        assert_operations(&operations, &[]);
    }

    #[tokio::test]
    async fn round_trip() {
        let t = new_tlogger();
        let id = TxId::from_bytes(vec![1, 2, 3, 4]);
        let collection = test_collection("db", 1);
        let child = test_collection("db", 2);
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
                TxLock::Membership {
                    leaf: LeafRef::root(collection),
                    typ: LockType::Read,
                },
                TxLock::Entry {
                    key: key.clone(),
                    typ: LockType::Write,
                },
                TxLock::Directory {
                    collection: test_collection("db", 1),
                    typ: LockType::Write,
                },
                TxLock::Topology {
                    collection: test_collection("db", 1),
                },
            ],
            collection_changes: vec![TxCollectionChange {
                parent: test_collection("db", 1),
                name: b"child".to_vec(),
                collection: child.clone(),
                op: TxCollectionOp::Create,
            }],
            prepared_collections: vec![child],
        };
        t.set(&log).await.unwrap();

        let got = t.get_at(&id, Requirement::Any).await.unwrap();
        let got = got.value().unwrap();
        assert_eq!(got.status, TxCommitStatus::Ok);
        assert_eq!(got.writes, log.writes);
        assert!(got.locks.contains(&TxLock::Membership {
            leaf: LeafRef::root(test_collection("db", 1)),
            typ: LockType::Read,
        }));
        assert!(got.locks.contains(&TxLock::Entry {
            key,
            typ: LockType::Write,
        }));
        assert!(got.locks.contains(&TxLock::Directory {
            collection: test_collection("db", 1),
            typ: LockType::Write,
        }));
        assert!(got.locks.contains(&TxLock::Topology {
            collection: test_collection("db", 1),
        }));
        assert_eq!(got.collection_changes, log.collection_changes);
        assert_eq!(got.prepared_collections, log.prepared_collections);

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

    // Regression for the false-NotFound race: once a transaction-log create
    // owns its path lane, a same-path status read must wait instead of reaching
    // the backend and observing absence before the create linearizes. After the
    // create completes, the reader rechecks and reuses the published object.
    #[tokio::test]
    async fn commit_status_waits_for_in_flight_create() {
        let id = TxId::from_bytes(vec![1, 2, 3, 4]);
        let transaction_path = ObjectPath::Transaction {
            db_root: db_root(),
            id: id.clone(),
        }
        .to_string();
        let create_started = Arc::new(Notify::new());
        let release_create = Arc::new(Notify::new());
        let reads = Arc::new(AtomicUsize::new(0));
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        backend.set_before({
            let transaction_path = transaction_path.clone();
            let create_started = create_started.clone();
            let release_create = release_create.clone();
            let reads = reads.clone();
            move |operation| {
                let is_target = operation.path() == transaction_path;
                let is_read = is_target
                    && matches!(
                        operation,
                        BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                    );
                if is_read {
                    reads.fetch_add(1, Ordering::SeqCst);
                }
                let gate_create =
                    is_target && matches!(operation, BackendOp::WriteIfNotExists { .. });
                let create_started = create_started.clone();
                let release_create = release_create.clone();
                let future: HookFuture = Box::pin(async move {
                    if gate_create {
                        create_started.notify_one();
                        release_create.notified().await;
                    }
                    Ok(())
                });
                future
            }
        });

        let objects = CachedStore::new(backend, 1 << 20, Timeline::new(), None);
        let logger = TLogger::new(objects, db_root());
        let log = TxLog::new(id.clone(), TxCommitStatus::Ok);

        let creating = tokio::spawn({
            let logger = logger.clone();
            async move { logger.set(&log).await }
        });
        create_started.notified().await;

        let read_started = Arc::new(Notify::new());
        let reading = tokio::spawn({
            let logger = logger.clone();
            let read_started = read_started.clone();
            async move {
                read_started.notify_one();
                logger.commit_status_at(&id, Requirement::Any).await
            }
        });
        read_started.notified().await;

        assert!(
            !reading.is_finished(),
            "the status read must wait for the path lane"
        );
        assert_eq!(reads.load(Ordering::SeqCst), 0, "no backend read is issued");

        release_create.notify_one();
        creating.await.unwrap().unwrap();
        let status = reading.await.unwrap().unwrap();
        assert_eq!(status.status, TxCommitStatus::Ok);
        assert_eq!(
            reads.load(Ordering::SeqCst),
            0,
            "the queued read must reuse the create's published state"
        );
    }

    #[tokio::test]
    async fn get_returns_log_and_version() {
        let t = new_tlogger();
        let id = TxId::from_bytes(vec![1, 2, 3, 4]);
        let key = KeyRef::new(test_collection("db", 1), b"hello");
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

    #[tokio::test]
    async fn finalized_logs_are_served_from_the_typed_cache() {
        let backend = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let operations = backend.log();
        let timeline = Timeline::new();
        let objects = CachedStore::new(Arc::new(backend), 1 << 20, timeline.clone(), None);
        let logger = TLogger::new(objects, db_root());
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
        let objects = CachedStore::new(Arc::new(backend), 1 << 20, timeline.clone(), None);
        let logger = TLogger::new(objects, db_root());
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
    async fn wounded_logs_are_not_cached_as_immutable() {
        let backend = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let operations = backend.log();
        let timeline = Timeline::new();
        let objects = CachedStore::new(Arc::new(backend), 1 << 20, timeline.clone(), None);
        let logger = TLogger::new(objects, db_root());
        let id = TxId::from_bytes(vec![4, 3, 2, 3]);
        logger
            .set(&TxLog::new(id.clone(), TxCommitStatus::Wounded))
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
        let shard = t.transaction_shard(&ids[0]);
        assert!(ids.iter().all(|id| t.transaction_shard(id) == shard));
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
