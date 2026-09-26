//! Transaction-record persistence. Ported from the Go
//! `internal/storage/tlogger.go`. Records are protobuf bodies; the commit status
//! and timestamp live in the body itself (ADR-019/ADR-023), not in object tags.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use glassdb_backend as backend;
use glassdb_concurr::rt;
use glassdb_data::{DbPrefix, ObjectPath, TxId};

use crate::cached_store::{CachedStore, CasResult, ObjectKey, Observation, Requirement};
use crate::error::StorageError;
use crate::transaction::{
    TxCommitStatus, TxLifecycleRelation, TxRecord, TxRecordCodec, TxRecordState,
};

const TRANSACTION_PREFIX_COUNT: usize = 64 * 64;

/// The commit status of a transaction along with its timestamp and revision.
#[derive(Debug, Clone)]
pub struct TxStatus {
    pub status: TxCommitStatus,
    pub last_update: SystemTime,
    pub observation: Observation<TxRecord>,
}

impl TxStatus {
    /// Creates a transaction status view from an observed transaction record.
    pub fn from_observation(observation: Observation<TxRecord>) -> Self {
        let (status, last_update) = match observation.value() {
            Some(record) => (record.status, record.timestamp.unwrap_or(UNIX_EPOCH)),
            None => (TxCommitStatus::Unknown, UNIX_EPOCH),
        };
        Self {
            status,
            last_update,
            observation,
        }
    }
}

/// One backend page of transaction identities from a scan prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxListPage {
    pub ids: Vec<TxId>,
    pub next: Option<backend::ListCursor>,
}

/// Reads and writes transaction records under a path prefix.
#[derive(Clone)]
pub struct TxRecordStore {
    db_prefix: DbPrefix,
    records: crate::cached_store::TypedCachedStore<TxRecordCodec>,
}

impl TxRecordStore {
    /// Creates a store for transaction records under `db_prefix`.
    pub fn new(objects: CachedStore, db_prefix: DbPrefix) -> Self {
        TxRecordStore {
            db_prefix,
            records: objects.typed(),
        }
    }

    /// Returns transaction status, revalidating mutable state at `requirement`.
    ///
    /// Cached committed and aborted statuses remain valid after object deletion.
    /// Their observations can predate `requirement`.
    pub async fn commit_status_at(
        &self,
        id: &TxId,
        requirement: Requirement,
    ) -> Result<TxStatus, StorageError> {
        let path = ObjectKey::from(ObjectPath::Transaction {
            db_prefix: self.db_prefix.clone(),
            id: *id,
        });
        let observation = match self.cached_final(&path)? {
            Some(observation) => observation,
            None => self.records.read(path, requirement).await?,
        };
        Ok(TxStatus::from_observation(observation))
    }

    /// Reads transaction contents, revalidating mutable state at `requirement`.
    ///
    /// Cached committed and aborted contents remain valid after object deletion.
    /// Their observations can predate `requirement` and do not prove presence.
    pub async fn get_at(
        &self,
        id: &TxId,
        requirement: Requirement,
    ) -> Result<Observation<TxRecord>, StorageError> {
        let path = ObjectKey::from(ObjectPath::Transaction {
            db_prefix: self.db_prefix.clone(),
            id: *id,
        });
        let observation = match self.cached_final(&path)? {
            Some(observation) => observation,
            None => self.records.read(path, requirement).await?,
        };
        if observation.is_absent() {
            Err(StorageError::NotFound)
        } else {
            Ok(observation)
        }
    }

    /// Creates a transaction's initial record, failing if one already exists.
    pub async fn set(&self, l: &TxRecord) -> Result<Observation<TxRecord>, StorageError> {
        validate_lifecycle_transition(None, Some(l.status))?;
        let ts = l.timestamp.unwrap_or_else(rt::system_now);
        let mut persisted = l.clone();
        persisted.timestamp = Some(ts);
        let path = ObjectPath::Transaction {
            db_prefix: self.db_prefix.clone(),
            id: l.id,
        };
        match self.records.create(path, None, Arc::new(persisted)).await? {
            CasResult::Applied(receipt) => Ok(receipt.into_installed()),
            CasResult::Rejected => Err(StorageError::Precondition),
        }
    }

    /// Transitions a mutable record if its current revision matches `expected`.
    ///
    /// Immutable records are not replaceable. A wounded record has one permitted
    /// transition to aborted when its owner acknowledges retirement.
    pub async fn set_if(
        &self,
        l: &TxRecord,
        expected: &Observation<TxRecord>,
    ) -> Result<Observation<TxRecord>, StorageError> {
        let current = expected.value().ok_or_else(|| {
            StorageError::other("transaction record replace requires a present value")
        })?;
        validate_lifecycle_transition(Some(current.status), Some(l.status))?;
        let ts = l.timestamp.unwrap_or_else(rt::system_now);
        let mut persisted = l.clone();
        persisted.timestamp = Some(ts);
        match self.records.replace(expected, Arc::new(persisted)).await? {
            CasResult::Applied(receipt) => Ok(receipt.into_installed()),
            CasResult::Rejected => Err(StorageError::Precondition),
        }
    }

    /// Returns the index of every transaction prefix.
    pub fn transaction_prefix_indexes(&self) -> impl Iterator<Item = usize> {
        0..TRANSACTION_PREFIX_COUNT
    }

    /// Returns the index of the transaction prefix that contains `id`.
    pub fn transaction_prefix_index(&self, id: &TxId) -> usize {
        ObjectPath::transaction_prefix_index(id)
    }

    /// Lists one page of transaction identities from the transaction prefix at
    /// `index`.
    pub async fn list_transaction_ids(
        &self,
        index: usize,
        cursor: Option<&backend::ListCursor>,
        limit: backend::ListLimit,
    ) -> Result<TxListPage, StorageError> {
        self.scan_transaction_ids(2, index, cursor, limit).await
    }

    /// Lists transaction identities at the requested prefix depth.
    pub async fn scan_transaction_ids(
        &self,
        depth: u8,
        index: usize,
        cursor: Option<&backend::ListCursor>,
        limit: backend::ListLimit,
    ) -> Result<TxListPage, StorageError> {
        let prefix = ObjectPath::transaction_scan_prefix(&self.db_prefix, depth, index)
            .map_err(|error| StorageError::with_source("transaction scan prefix", error))?;
        let page = self.records.list(&prefix, cursor, limit).await?;
        let ids = page
            .objects
            .iter()
            .filter_map(|path| match path.object_path() {
                ObjectPath::Transaction { db_prefix, id } if db_prefix == &self.db_prefix => {
                    Some(*id)
                }
                _ => None,
            })
            .collect();
        Ok(TxListPage {
            ids,
            next: page.next,
        })
    }

    /// Removes an exact immutable record during GC, converging if it is missing.
    ///
    /// Pending and wounded records must first transition to aborted; deleting one
    /// directly fails locally with [`StorageError::Precondition`].
    pub async fn delete(&self, expected: &Observation<TxRecord>) -> Result<(), StorageError> {
        let current = expected.value().ok_or_else(|| {
            StorageError::other("transaction record deletion requires a present value")
        })?;
        validate_lifecycle_transition(Some(current.status), None)?;
        self.records.delete(expected).await?;
        Ok(())
    }

    fn cached_final(
        &self,
        path: &ObjectKey,
    ) -> Result<Option<Observation<TxRecord>>, StorageError> {
        Ok(self.records.peek(path)?.filter(|observation| {
            observation
                .value()
                .is_some_and(|record| record.status.is_immutable())
        }))
    }
}

/// Validates the durable transaction lifecycle before any backend operation.
///
/// Immutable objects are cached indefinitely, so replacing one would invalidate
/// knowledge held by every database instance that has observed it. `Wounded`
/// is a final status for transaction semantics but remains mutable until its owner
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
    use glassdb_data::{CollectionAddress, CollectionId, LeafRef, LogicalKey};
    use tokio::sync::Notify;

    fn tx_id(prefix: &[u8]) -> TxId {
        TxId::with_priority(0, prefix)
    }

    fn db_prefix() -> DbPrefix {
        DbPrefix::try_from("db").unwrap()
    }

    fn new_tx_record_store() -> TxRecordStore {
        let backend = Arc::new(MemoryBackend::new());
        let timeline = Timeline::new();
        let objects = CachedStore::new(backend, 1 << 20, timeline.clone(), None);
        TxRecordStore::new(objects, db_prefix())
    }

    fn test_collection(db_prefix: &str, byte: u8) -> CollectionAddress {
        CollectionAddress::new(db_prefix, CollectionId::from_bytes([byte; 16]))
    }

    fn new_recording_tx_record_store() -> (TxRecordStore, OpLog) {
        let backend = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let operations = backend.log();
        let objects = CachedStore::new(Arc::new(backend), 1 << 20, Timeline::new(), None);
        (TxRecordStore::new(objects, db_prefix()), operations)
    }

    fn assert_operations(operations: &OpLog, expected: &[&str]) {
        let mut operations = operations.lock().unwrap();
        let actual: Vec<_> = operations.iter().map(|operation| operation.op).collect();
        assert_eq!(actual, expected);
        operations.clear();
    }

    #[tokio::test]
    async fn mutations_reject_transaction_identity_mismatches_before_backend_io() {
        let (logger, operations) = new_recording_tx_record_store();
        let id = tx_id(&[1, 2, 3, 4]);
        let observed = logger
            .set(&TxRecord::new(id, TxCommitStatus::Pending))
            .await
            .unwrap();
        assert_operations(&operations, &["write_if_not_exists"]);

        let different_id = tx_id(&[4, 3, 2, 1]);
        assert!(
            logger
                .set_if(
                    &TxRecord::new(different_id, TxCommitStatus::Pending),
                    &observed,
                )
                .await
                .is_err()
        );
        assert_operations(&operations, &[]);

        let mut wrong_database = TxRecord::new(tx_id(&[5, 6, 7, 8]), TxCommitStatus::Pending);
        wrong_database.writes.push(TxWrite {
            key: LogicalKey::new(test_collection("other", 1), b"key"),
            value: Arc::from(&b"value"[..]),
            deleted: false,
        });
        assert!(logger.set(&wrong_database).await.is_err());
        assert_operations(&operations, &[]);
    }

    #[tokio::test]
    async fn allowed_lifecycle_transitions_add_no_backend_reads() {
        let (logger, operations) = new_recording_tx_record_store();

        for (suffix, status) in [
            (1, TxCommitStatus::Pending),
            (2, TxCommitStatus::Committed),
            (3, TxCommitStatus::Aborted),
            (7, TxCommitStatus::Wounded),
        ] {
            let id = tx_id(&[9, suffix]);
            let observed = logger.set(&TxRecord::new(id, status)).await.unwrap();
            assert_operations(&operations, &["write_if_not_exists"]);
            if status.is_immutable() {
                logger.delete(&observed).await.unwrap();
                assert_operations(&operations, &["delete_if"]);
            }
        }

        let committed_id = tx_id(&[9, 4]);
        let pending = logger
            .set(&TxRecord::new(committed_id, TxCommitStatus::Pending))
            .await
            .unwrap();
        assert_operations(&operations, &["write_if_not_exists"]);
        let refreshed = logger
            .set_if(
                &TxRecord::new(committed_id, TxCommitStatus::Pending),
                &pending,
            )
            .await
            .unwrap();
        assert_operations(&operations, &["write_if"]);
        let committed = logger
            .set_if(
                &TxRecord::new(committed_id, TxCommitStatus::Committed),
                &refreshed,
            )
            .await
            .unwrap();
        assert_operations(&operations, &["write_if"]);
        logger.delete(&committed).await.unwrap();
        assert_operations(&operations, &["delete_if"]);

        let aborted_id = tx_id(&[9, 5]);
        let pending = logger
            .set(&TxRecord::new(aborted_id, TxCommitStatus::Pending))
            .await
            .unwrap();
        assert_operations(&operations, &["write_if_not_exists"]);
        logger
            .set_if(
                &TxRecord::new(aborted_id, TxCommitStatus::Aborted),
                &pending,
            )
            .await
            .unwrap();
        assert_operations(&operations, &["write_if"]);

        let wounded_id = tx_id(&[9, 6]);
        let pending = logger
            .set(&TxRecord::new(wounded_id, TxCommitStatus::Pending))
            .await
            .unwrap();
        assert_operations(&operations, &["write_if_not_exists"]);
        let wounded = logger
            .set_if(
                &TxRecord::new(wounded_id, TxCommitStatus::Wounded),
                &pending,
            )
            .await
            .unwrap();
        assert_operations(&operations, &["write_if"]);
        let aborted = logger
            .set_if(
                &TxRecord::new(wounded_id, TxCommitStatus::Aborted),
                &wounded,
            )
            .await
            .unwrap();
        assert_operations(&operations, &["write_if"]);
        logger.delete(&aborted).await.unwrap();
        assert_operations(&operations, &["delete_if"]);
    }

    #[tokio::test]
    async fn rejected_lifecycle_transitions_issue_no_backend_operations() {
        let (logger, operations) = new_recording_tx_record_store();

        for (suffix, current) in [(1, TxCommitStatus::Committed), (2, TxCommitStatus::Aborted)] {
            let id = tx_id(&[10, suffix]);
            let observed = logger.set(&TxRecord::new(id, current)).await.unwrap();
            assert_operations(&operations, &["write_if_not_exists"]);
            for next in [
                TxCommitStatus::Pending,
                TxCommitStatus::Committed,
                TxCommitStatus::Aborted,
                TxCommitStatus::Wounded,
            ] {
                assert!(matches!(
                    logger.set_if(&TxRecord::new(id, next), &observed).await,
                    Err(StorageError::Precondition)
                ));
                assert_operations(&operations, &[]);
            }
            assert_eq!(
                logger
                    .commit_status_at(&id, Requirement::ANY)
                    .await
                    .unwrap()
                    .status,
                current
            );
            assert_operations(&operations, &[]);
        }

        let pending_id = tx_id(&[10, 3]);
        let pending = logger
            .set(&TxRecord::new(pending_id, TxCommitStatus::Pending))
            .await
            .unwrap();
        assert_operations(&operations, &["write_if_not_exists"]);
        assert!(matches!(
            logger.delete(&pending).await,
            Err(StorageError::Precondition)
        ));
        assert_operations(&operations, &[]);

        let wounded_id = tx_id(&[10, 4]);
        let wounded = logger
            .set(&TxRecord::new(wounded_id, TxCommitStatus::Wounded))
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
            TxCommitStatus::Committed,
            TxCommitStatus::Wounded,
        ] {
            assert!(matches!(
                logger
                    .set_if(&TxRecord::new(wounded_id, next), &wounded)
                    .await,
                Err(StorageError::Precondition)
            ));
            assert_operations(&operations, &[]);
        }
        assert_eq!(
            logger
                .commit_status_at(&pending_id, Requirement::ANY)
                .await
                .unwrap()
                .status,
            TxCommitStatus::Pending
        );
        assert_operations(&operations, &[]);

        assert!(matches!(
            logger
                .set_if(
                    &TxRecord::new(pending_id, TxCommitStatus::Unknown),
                    &pending,
                )
                .await,
            Err(StorageError::Other { .. })
        ));
        assert_operations(&operations, &[]);

        assert!(matches!(
            logger
                .set(&TxRecord::new(tx_id(&[10, 4]), TxCommitStatus::Unknown,))
                .await,
            Err(StorageError::Other { .. })
        ));
        assert_operations(&operations, &[]);
    }

    #[tokio::test]
    async fn round_trip() {
        let t = new_tx_record_store();
        let id = tx_id(&[1, 2, 3, 4]);
        let collection = test_collection("db", 1);
        let child = test_collection("db", 2);
        let key = LogicalKey::new(collection.clone(), b"hello");
        let record = TxRecord {
            id,
            timestamp: Some(UNIX_EPOCH + Duration::from_millis(1_700_000_000_000)),
            status: TxCommitStatus::Committed,
            writes: vec![TxWrite {
                key: key.clone(),
                value: Arc::from(&b"world"[..]),
                deleted: false,
            }],
            locks: vec![
                TxLock::Membership {
                    leaf: LeafRef::root(collection),
                    typ: LockType::Read,
                },
                TxLock::Key {
                    key: key.clone(),
                    typ: LockType::Write,
                },
                TxLock::Directory {
                    collection: test_collection("db", 1),
                    typ: LockType::Write,
                },
                TxLock::TopologyParticipant {
                    collection: test_collection("db", 1),
                },
            ],
            collection_changes: vec![TxCollectionChange {
                parent: test_collection("db", 1),
                name: glassdb_data::CollectionName::new("child").unwrap(),
                collection: child.clone(),
                op: TxCollectionOp::Create,
            }],
            prepared_collections: vec![child],
        };
        t.set(&record).await.unwrap();

        let got = t.get_at(&id, Requirement::ANY).await.unwrap();
        let got = got.value().unwrap();
        assert_eq!(got.status, TxCommitStatus::Committed);
        assert_eq!(got.writes, record.writes);
        assert!(got.locks.contains(&TxLock::Membership {
            leaf: LeafRef::root(test_collection("db", 1)),
            typ: LockType::Read,
        }));
        assert!(got.locks.contains(&TxLock::Key {
            key,
            typ: LockType::Write,
        }));
        assert!(got.locks.contains(&TxLock::Directory {
            collection: test_collection("db", 1),
            typ: LockType::Write,
        }));
        assert!(got.locks.contains(&TxLock::TopologyParticipant {
            collection: test_collection("db", 1),
        }));
        assert_eq!(got.collection_changes, record.collection_changes);
        assert_eq!(got.prepared_collections, record.prepared_collections);

        let status = t.commit_status_at(&id, Requirement::ANY).await.unwrap();
        assert_eq!(status.status, TxCommitStatus::Committed);
    }

    #[tokio::test]
    async fn commit_status_unknown_when_absent() {
        let t = new_tx_record_store();
        let status = t
            .commit_status_at(&tx_id(&[7]), Requirement::ANY)
            .await
            .unwrap();
        assert_eq!(status.status, TxCommitStatus::Unknown);
    }

    // Regression for the false-NotFound race: once a transaction-record create
    // owns its path lane, a same-path status read must wait instead of reaching
    // the backend and observing absence before the create linearizes. After the
    // create completes, the reader rechecks and reuses the published object.
    #[tokio::test]
    async fn commit_status_waits_for_in_flight_create() {
        let id = tx_id(&[1, 2, 3, 4]);
        let transaction_path = ObjectPath::Transaction {
            db_prefix: db_prefix(),
            id,
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
        let logger = TxRecordStore::new(objects, db_prefix());
        let record = TxRecord::new(id, TxCommitStatus::Committed);

        let creating = tokio::spawn({
            let logger = logger.clone();
            async move { logger.set(&record).await }
        });
        create_started.notified().await;

        let read_started = Arc::new(Notify::new());
        let reading = tokio::spawn({
            let logger = logger.clone();
            let read_started = read_started.clone();
            async move {
                read_started.notify_one();
                logger.commit_status_at(&id, Requirement::ANY).await
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
        assert_eq!(status.status, TxCommitStatus::Committed);
        assert_eq!(
            reads.load(Ordering::SeqCst),
            0,
            "the queued read must reuse the create's published state"
        );
    }

    #[tokio::test]
    async fn get_returns_record_and_revision() {
        let t = new_tx_record_store();
        let id = tx_id(&[1, 2, 3, 4]);
        let key = LogicalKey::new(test_collection("db", 1), b"hello");
        let mut record = TxRecord::new(id, TxCommitStatus::Committed);
        record.timestamp = Some(UNIX_EPOCH + Duration::from_secs(1_700_000_000));
        record.writes = vec![TxWrite {
            key,
            value: Arc::from(&b"world"[..]),
            deleted: false,
        }];
        let stored_v = t.set(&record).await.unwrap();

        let got = t.get_at(&id, Requirement::ANY).await.unwrap();
        let revision = got.revision().cloned();
        let got = got.value().unwrap();
        assert_eq!(got.status, TxCommitStatus::Committed);
        assert_eq!(got.writes, record.writes);
        assert_eq!(got.timestamp, record.timestamp);
        assert_eq!(revision.as_ref(), stored_v.revision());
    }

    #[tokio::test]
    async fn final_contents_remain_cached_after_peer_deletion() {
        for status in [TxCommitStatus::Committed, TxCommitStatus::Aborted] {
            let backend = RecordingBackend::new(Arc::new(MemoryBackend::new()));
            let operations = backend.log();
            let backend = Arc::new(backend);
            let timeline = Timeline::new();
            let logger = TxRecordStore::new(
                CachedStore::new(backend.clone(), 1 << 20, timeline.clone(), None),
                db_prefix(),
            );
            let peer = TxRecordStore::new(
                CachedStore::new(backend, 1 << 20, Timeline::new(), None),
                db_prefix(),
            );
            let id = tx_id(&[4, 3, 2, 4]);
            logger.set(&TxRecord::new(id, status)).await.unwrap();
            let observed = peer.get_at(&id, Requirement::ANY).await.unwrap();
            peer.delete(&observed).await.unwrap();
            operations.lock().unwrap().clear();

            let bound = Requirement::after(timeline.currentness_barrier());
            let observed = logger.get_at(&id, bound).await.unwrap();
            assert_eq!(observed.value().unwrap().status, status);
            assert!(!observed.satisfies(bound));
            assert_eq!(
                logger.commit_status_at(&id, bound).await.unwrap().status,
                status,
            );
            assert!(
                operations.lock().unwrap().is_empty(),
                "immutable contents and status need no presence check"
            );
        }
    }

    #[tokio::test]
    async fn pending_records_still_obey_generic_freshness() {
        let backend = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let operations = backend.log();
        let timeline = Timeline::new();
        let objects = CachedStore::new(Arc::new(backend), 1 << 20, timeline.clone(), None);
        let logger = TxRecordStore::new(objects, db_prefix());
        let id = tx_id(&[4, 3, 2, 2]);
        logger
            .set(&TxRecord::new(id, TxCommitStatus::Pending))
            .await
            .unwrap();
        operations.lock().unwrap().clear();

        logger
            .get_at(&id, Requirement::after(timeline.currentness_barrier()))
            .await
            .unwrap();
        logger
            .get_at(&id, Requirement::after(timeline.currentness_barrier()))
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
    async fn wounded_records_are_not_cached_as_immutable() {
        let backend = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let operations = backend.log();
        let timeline = Timeline::new();
        let objects = CachedStore::new(Arc::new(backend), 1 << 20, timeline.clone(), None);
        let logger = TxRecordStore::new(objects, db_prefix());
        let id = tx_id(&[4, 3, 2, 3]);
        logger
            .set(&TxRecord::new(id, TxCommitStatus::Wounded))
            .await
            .unwrap();
        operations.lock().unwrap().clear();

        logger
            .get_at(&id, Requirement::after(timeline.currentness_barrier()))
            .await
            .unwrap();
        logger
            .get_at(&id, Requirement::after(timeline.currentness_barrier()))
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
    async fn list_transaction_ids_pages_one_transaction_prefix() {
        let t = new_tx_record_store();
        let ids = [tx_id(&[1, 2]), tx_id(&[1, 3]), tx_id(&[1, 4])];
        for id in &ids {
            t.set(&TxRecord::new(*id, TxCommitStatus::Aborted))
                .await
                .unwrap();
        }
        let prefix_index = t.transaction_prefix_index(&ids[0]);
        assert!(
            ids.iter()
                .all(|id| t.transaction_prefix_index(id) == prefix_index)
        );
        let limit = backend::ListLimit::new(2).unwrap();
        let first = t
            .list_transaction_ids(prefix_index, None, limit)
            .await
            .unwrap();
        assert_eq!(first.ids.len(), 2);
        let second = t
            .list_transaction_ids(prefix_index, first.next.as_ref(), limit)
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

    #[tokio::test]
    async fn recursive_scans_page_only_the_selected_scope() {
        let t = new_tx_record_store();
        let ids = [
            tx_id(&[1, 2]),
            tx_id(&[1, 3]),
            tx_id(&[2, 0]),
            tx_id(&[80, 3]),
        ];
        for id in &ids {
            t.set(&TxRecord::new(*id, TxCommitStatus::Aborted))
                .await
                .unwrap();
        }
        let prefix_index = t.transaction_prefix_index(&ids[0]);
        for (depth, index) in [(0, 0), (1, prefix_index / 64), (2, prefix_index)] {
            let mut cursor = None;
            let mut found = Vec::new();
            loop {
                let page = t
                    .scan_transaction_ids(
                        depth,
                        index,
                        cursor.as_ref(),
                        backend::ListLimit::new(1).unwrap(),
                    )
                    .await
                    .unwrap();
                found.extend(page.ids);
                match page.next {
                    Some(next) => cursor = Some(next),
                    None => break,
                }
            }
            let expected: Vec<_> = ids
                .iter()
                .filter(|id| match depth {
                    0 => true,
                    1 => t.transaction_prefix_index(id) / 64 == index,
                    _ => t.transaction_prefix_index(id) == index,
                })
                .cloned()
                .collect();
            assert_eq!(found, expected);
        }
    }
}
