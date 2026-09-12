//! Transactional state resolution, locking, and write-back for collection records.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use glassdb_concurr::{RetryConfig, map_all_bounded, rt};
use glassdb_data::{CollectionAddress, TxId};
use glassdb_storage::transaction::{
    TLogger, TxCollectionChange, TxCollectionOp, TxCommitStatus, TxLock,
};
use glassdb_storage::{
    CollectionRecord, CollectionStore, LockType, Observation, Requirement, StorageError, Timeline,
};

use crate::collections::{CollectionChange, CollectionOp, DirectoryRead};
use crate::error::TransError;
use crate::monitor::{Monitor, TxFinalStatus};
use crate::wound_wait::resolve_tx_conflict;

/// Durable directory locks acquired for one transaction.
pub(crate) struct LockedDirectories {
    locks: Vec<TxLock>,
}

impl LockedDirectories {
    /// Consumes the receipt into durable lock records.
    pub(crate) fn into_durable_locks(self) -> Vec<TxLock> {
        self.locks
    }
}

/// Resolves transaction-dependent state in collection records.
#[derive(Clone)]
pub(crate) struct CollectionStateResolver {
    records: CollectionStore,
    transactions: TLogger,
    timeline: Timeline,
    monitor: Monitor,
    retry: RetryConfig,
}

/// Coordinates collection locks and their committed effects.
#[derive(Clone)]
pub(crate) struct CollectionLocker {
    state: CollectionStateResolver,
    records: CollectionStore,
    monitor: Monitor,
    retry: RetryConfig,
    parallelism: NonZeroUsize,
}

impl CollectionLocker {
    /// Creates collection locking over shared collection-state resolution.
    pub(crate) fn new(state: CollectionStateResolver, parallelism: NonZeroUsize) -> Self {
        Self {
            records: state.records.clone(),
            monitor: state.monitor.clone(),
            retry: state.retry,
            state,
            parallelism,
        }
    }

    /// Acquires directory locks in stable physical-address order.
    pub(crate) async fn lock(
        &self,
        id: &TxId,
        reads: &[DirectoryRead],
        changes: &[CollectionChange],
    ) -> Result<LockedDirectories, TransError> {
        let mut desired = BTreeMap::<CollectionAddress, LockType>::new();
        for read in reads {
            desired.entry(read.parent.clone()).or_insert(LockType::Read);
        }
        for change in changes {
            desired.insert(change.parent.clone(), LockType::Write);
        }

        let mut locks = Vec::with_capacity(desired.len());
        for (parent, typ) in desired {
            self.acquire(&parent, id, typ).await?;
            locks.push(TxLock::Directory {
                collection: parent,
                typ,
            });
        }
        Ok(LockedDirectories { locks })
    }

    /// Applies committed directory effects and releases their locks.
    pub(crate) async fn write_back(
        &self,
        id: &TxId,
        changes: &[CollectionChange],
        locks: &[TxLock],
    ) -> Result<bool, TransError> {
        let results = map_all_bounded(
            Self::locked_collections(locks),
            self.parallelism,
            |parent| async move {
                self.state
                    .apply_committed_write_back(&parent, id, changes)
                    .await
            },
        )
        .await;
        results
            .into_iter()
            .try_fold(false, |changed, result| Ok(changed | result?))
    }

    /// Recovers committed directory effects from durable metadata.
    pub(crate) async fn recover_write_back(
        &self,
        id: &TxId,
        changes: &[TxCollectionChange],
        locks: &[TxLock],
    ) -> Result<bool, TransError> {
        let changes = CollectionStateResolver::recover_changes(changes);
        self.write_back(id, &changes, locks).await
    }

    /// Releases every recorded directory lock held by `id`.
    pub(crate) async fn release(&self, id: &TxId, locks: &[TxLock]) -> Result<bool, TransError> {
        let results = map_all_bounded(
            Self::locked_collections(locks),
            self.parallelism,
            |parent| async move { self.release_directory(&parent, id).await },
        )
        .await;
        results
            .into_iter()
            .try_fold(false, |changed, result| Ok(changed | result?))
    }

    /// Reports whether any recorded directory still refers to `id`.
    pub(crate) async fn is_referenced(
        &self,
        id: &TxId,
        locks: &[TxLock],
        requirement: Requirement,
    ) -> Result<bool, TransError> {
        let results = map_all_bounded(
            Self::locked_collections(locks),
            self.parallelism,
            |parent| async move {
                match self.records.load_record(&parent, requirement).await {
                    Ok((record, _)) => Ok(record.directory_lock().contains(id)),
                    Err(StorageError::NotFound) => Ok(false),
                    Err(error) => Err(TransError::from(error)),
                }
            },
        )
        .await;
        results
            .into_iter()
            .try_fold(false, |referenced, result| Ok(referenced | result?))
    }

    /// Removes a settled structural operation from collection topology.
    pub(crate) async fn release_topology_participant(
        &self,
        collection: &CollectionAddress,
        id: &TxId,
    ) -> Result<bool, TransError> {
        loop {
            let (mut record, observed) =
                match self.records.load_record(collection, Requirement::Any).await {
                    Ok(record) => record,
                    Err(StorageError::NotFound) => return Ok(false),
                    Err(error) => return Err(error.into()),
                };
            if !record.remove_topology_participant(id) {
                return Ok(false);
            }
            if self.records.store_record(&record, &observed).await? {
                return Ok(true);
            }
        }
    }

    pub(crate) async fn acquire(
        &self,
        parent: &CollectionAddress,
        id: &TxId,
        desired: LockType,
    ) -> Result<(), TransError> {
        let mut backoff = self.retry.backoff();
        loop {
            let (mut record, observed) = self
                .state
                .resolve_observed(parent, Some(id), Requirement::Any)
                .await?;
            let lock = record.directory_lock();
            let already_held = lock.contains(id)
                && (desired == LockType::Read || lock.lock_type() == LockType::Write);
            if already_held {
                return Ok(());
            }
            let conflicts = match desired {
                LockType::Read => lock.lock_type() == LockType::Write,
                LockType::Write => !lock.holders().is_empty(),
                _ => false,
            };
            if conflicts
                && let Some(holder) = lock.holders().iter().find(|holder| *holder != id).cloned()
            {
                resolve_tx_conflict(&self.monitor, id, &holder).await?;
                rt::sleep(backoff.next_delay()).await;
                continue;
            }
            match desired {
                LockType::Read => record.add_directory_reader(id.clone()),
                LockType::Write => record.set_directory_writer(id.clone()),
                _ => {
                    return Err(TransError::other(
                        "invalid collection-directory lock request",
                    ));
                }
            }
            if self.records.store_record(&record, &observed).await? {
                return Ok(());
            }
            rt::sleep(backoff.next_delay()).await;
        }
    }

    async fn release_directory(
        &self,
        parent: &CollectionAddress,
        id: &TxId,
    ) -> Result<bool, TransError> {
        loop {
            let (mut record, observed) =
                match self.records.load_record(parent, Requirement::Any).await {
                    Ok(record) => record,
                    Err(StorageError::NotFound) => return Ok(false),
                    Err(error) => return Err(error.into()),
                };
            if !record.remove_directory_holder(id) {
                return Ok(false);
            }
            if self.records.store_record(&record, &observed).await? {
                return Ok(true);
            }
        }
    }

    fn locked_collections(locks: &[TxLock]) -> BTreeSet<CollectionAddress> {
        locks
            .iter()
            .filter_map(|lock| match lock {
                TxLock::Directory { collection, .. } => Some(collection.clone()),
                _ => None,
            })
            .collect()
    }
}

impl CollectionStateResolver {
    /// Creates collection-state resolution over record and transaction stores.
    pub(crate) fn new(
        records: CollectionStore,
        transactions: TLogger,
        timeline: Timeline,
        monitor: Monitor,
        retry: RetryConfig,
    ) -> Self {
        Self {
            records,
            transactions,
            timeline,
            monitor,
            retry,
        }
    }

    /// Loads a collection record after resolving foreign lifecycle and
    /// directory holders.
    pub(crate) async fn resolve(
        &self,
        collection: &CollectionAddress,
        own: Option<&TxId>,
        requirement: Requirement,
    ) -> Result<CollectionRecord, TransError> {
        self.resolve_observed(collection, own, requirement)
            .await
            .map(|(record, _)| record)
    }

    /// Loads a record and its CAS observation after resolving foreign holders.
    async fn resolve_observed(
        &self,
        parent: &CollectionAddress,
        own: Option<&TxId>,
        requirement: Requirement,
    ) -> Result<(CollectionRecord, Observation<CollectionRecord>), TransError> {
        let mut backoff = self.retry.backoff();
        let mut requirement = requirement;
        loop {
            let (mut record, observed) = self
                .records
                .load_record(parent, requirement)
                .await
                .map_err(|error| error.classify_collection_absence(parent))?;
            if let Some(holder) = record.topology_freeze().cloned()
                && Some(&holder) != own
            {
                match self.monitor.await_tx_final(&holder).await? {
                    TxFinalStatus::Committed => return Err(TransError::StaleCollection),
                    TxFinalStatus::Aborted => {
                        record.remove_topology_freeze(&holder);
                        if self.records.store_record(&record, &observed).await? {
                            continue;
                        }
                    }
                }
                rt::sleep(backoff.next_delay()).await;
                continue;
            }

            let holder = record
                .directory_lock()
                .holders()
                .iter()
                .find(|holder| Some(*holder) != own)
                .cloned();
            let Some(holder) = holder else {
                return Ok((record, observed));
            };
            match self.monitor.await_tx_final(&holder).await? {
                TxFinalStatus::Committed => {
                    match self.help_committed_write_back(parent, &holder).await {
                        Ok(()) => {}
                        Err(TransError::Storage(StorageError::NotFound)) => {
                            // GC can reclaim a log after another instance removes
                            // its directory holder. A cached record must reload.
                            requirement = Requirement::AtLeast(self.timeline.now());
                            rt::sleep(backoff.next_delay()).await;
                        }
                        Err(error) => return Err(error),
                    }
                }
                TxFinalStatus::Aborted => {
                    record.remove_directory_holder(&holder);
                    if !self.records.store_record(&record, &observed).await? {
                        rt::sleep(backoff.next_delay()).await;
                    }
                }
            }
        }
    }

    async fn apply_committed_write_back(
        &self,
        parent: &CollectionAddress,
        id: &TxId,
        changes: &[CollectionChange],
    ) -> Result<bool, TransError> {
        let mut backoff = self.retry.backoff();
        loop {
            let (mut record, observed) =
                match self.records.load_record(parent, Requirement::Any).await {
                    Ok(record) => record,
                    Err(StorageError::NotFound) => return Ok(false),
                    Err(error) => return Err(error.into()),
                };
            if !record.directory_lock().contains(id) {
                return Ok(false);
            }
            let mut changed = false;
            for change in changes.iter().filter(|change| &change.parent == parent) {
                match change.op {
                    CollectionOp::Create => match record.child(&change.name) {
                        None => {
                            record.add_child(change.name.clone(), change.collection.id())?;
                            changed = true;
                        }
                        Some(found) if found == change.collection.id() => {}
                        Some(_) => {
                            return Err(TransError::other(
                                "committed collection create conflicts with a newer binding",
                            ));
                        }
                    },
                    CollectionOp::Drop => match record.child(&change.name) {
                        Some(found) if found == change.collection.id() => {
                            record.remove_child(&change.name);
                            changed = true;
                        }
                        None => {}
                        Some(_) => {
                            return Err(TransError::other(
                                "committed collection drop found a replacement binding",
                            ));
                        }
                    },
                }
            }
            if changed {
                record.advance_directory_version();
            }
            record.remove_directory_holder(id);
            if self.records.store_record(&record, &observed).await? {
                return Ok(true);
            }
            rt::sleep(backoff.next_delay()).await;
        }
    }

    async fn help_committed_write_back(
        &self,
        parent: &CollectionAddress,
        id: &TxId,
    ) -> Result<(), TransError> {
        let observed = self
            .transactions
            .get_at(id, Requirement::Any)
            .await
            .map_err(|error| {
                TransError::Storage(error.context(format!("loading committed transaction {id}")))
            })?;
        let log = observed.value().ok_or_else(|| {
            TransError::other(format!("committed transaction log disappeared for {id}"))
        })?;
        if log.status != TxCommitStatus::Ok {
            return Err(TransError::other(format!(
                "transaction {id} finalized as committed but its log has status {:?}",
                log.status
            )));
        }
        let changes = Self::recover_changes(&log.collection_changes);
        self.apply_committed_write_back(parent, id, &changes)
            .await
            .map(|_| ())
    }

    fn recover_changes(changes: &[TxCollectionChange]) -> Vec<CollectionChange> {
        changes
            .iter()
            .map(|change| CollectionChange {
                parent: change.parent.clone(),
                name: change.name.clone(),
                collection: change.collection.clone(),
                expected: None,
                op: match change.op {
                    TxCollectionOp::Create => CollectionOp::Create,
                    TxCollectionOp::Drop => CollectionOp::Drop,
                },
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use glassdb_backend::memory::MemoryBackend;
    use glassdb_concurr::Background;
    use glassdb_data::DbRoot;
    use glassdb_storage::{CachedStore, Timeline};

    use super::*;
    use crate::monitor::ProtocolTiming;

    fn new_locker() -> (CollectionLocker, CollectionStore, Arc<Background>) {
        let timeline = Timeline::new();
        let objects = CachedStore::new(
            Arc::new(MemoryBackend::new()),
            1 << 20,
            timeline.clone(),
            None,
        );
        let records = CollectionStore::new(objects.clone());
        let transactions = TLogger::new(objects, DbRoot::try_from("db").unwrap());
        let background = Arc::new(Background::new());
        let monitor = Monitor::with_config(
            transactions.clone(),
            timeline.clone(),
            Arc::downgrade(&background),
            RetryConfig::default(),
            ProtocolTiming::default(),
        );
        let state = CollectionStateResolver::new(
            records.clone(),
            transactions,
            timeline,
            monitor,
            RetryConfig::default(),
        );
        (
            CollectionLocker::new(state, NonZeroUsize::MIN),
            records,
            background,
        )
    }

    #[tokio::test]
    async fn sole_reader_can_upgrade_to_writer() {
        let (locker, records, _background) = new_locker();
        let parent = CollectionAddress::root("db");
        assert!(
            records
                .create_record(&parent, &CollectionRecord::new())
                .await
                .unwrap()
        );
        let id = TxId::from_bytes(vec![1]);

        locker.acquire(&parent, &id, LockType::Read).await.unwrap();
        locker.acquire(&parent, &id, LockType::Write).await.unwrap();

        let (record, _) = records
            .load_record(&parent, Requirement::Any)
            .await
            .unwrap();
        assert_eq!(record.directory_lock().lock_type(), LockType::Write);
        assert_eq!(record.directory_lock().holders(), std::slice::from_ref(&id));
    }

    #[tokio::test]
    async fn reclaimed_log_refreshes_a_cached_directory_holder() {
        use crate::engine::{AssemblyFixture, EngineConfig};
        use glassdb_data::CollectionId;
        use glassdb_storage::transaction::TxLog;

        let backend = Arc::new(MemoryBackend::new());
        let local = AssemblyFixture::new(
            backend.clone(),
            DbRoot::try_from("db").unwrap(),
            &EngineConfig::default(),
        );
        let peer = AssemblyFixture::new(
            backend,
            DbRoot::try_from("db").unwrap(),
            &EngineConfig::default(),
        );
        let parent = CollectionAddress::root("db");
        let old = TxId::from_bytes(vec![1]);
        let local_log = local
            .tlogger
            .set(&TxLog::new(old.clone(), TxCommitStatus::Ok))
            .await
            .unwrap();
        assert_eq!(
            local.monitor.tx_status(&old).await.unwrap(),
            TxCommitStatus::Ok
        );
        let mut record = CollectionRecord::new();
        record.set_directory_writer(old.clone());
        local.records.create_record(&parent, &record).await.unwrap();
        let (mut record, observed) = peer
            .records
            .load_record(&parent, Requirement::Any)
            .await
            .unwrap();
        record.remove_directory_holder(&old);
        let child = CollectionId::from_slice(&[1; 16]).unwrap();
        record.add_child(b"child".to_vec(), child).unwrap();
        peer.records.store_record(&record, &observed).await.unwrap();
        let observed = peer.tlogger.get_at(&old, Requirement::Any).await.unwrap();
        peer.tlogger.delete(&observed).await.unwrap();
        local.tlogger.delete(&local_log).await.unwrap();
        assert!(matches!(
            local.tlogger.get_at(&old, Requirement::Any).await,
            Err(StorageError::NotFound)
        ));
        let resolver = CollectionStateResolver::new(
            local.records.clone(),
            local.tlogger.clone(),
            local.timeline.clone(),
            local.monitor.clone(),
            RetryConfig::default(),
        );
        let resolved = resolver
            .resolve(&parent, None, Requirement::Any)
            .await
            .unwrap();
        assert_eq!(resolved.child(b"child"), Some(child));
        assert!(!resolved.directory_lock().contains(&old));
    }
}
