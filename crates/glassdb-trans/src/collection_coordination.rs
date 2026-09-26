//! Transactional state resolution, locking, and write-back for collection records.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use glassdb_concurr::{RetryConfig, map_all_bounded, rt};
use glassdb_data::{CollectionAddress, TxId};
use glassdb_storage::transaction::{
    TxCollectionChange, TxCollectionOp, TxCommitStatus, TxLock, TxRecordStore,
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
    /// Converts the acquired directory locks into durable lock records.
    pub(crate) fn into_durable_locks(self) -> Vec<TxLock> {
        self.locks
    }
}

/// Resolves transaction-dependent state in collection records.
#[derive(Clone)]
pub(crate) struct CollectionStateResolver {
    records: CollectionStore,
    transactions: TxRecordStore,
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
    ///
    /// Returns only directories whose holder-removal CAS applied for `id`.
    /// A present no-holder observation must satisfy `requirement`. With `ANY`,
    /// callers must share acquisition's cache knowledge or treat no-ops as
    /// speculative. Neither no-holder nor missing-record completion reports
    /// an applied removal.
    pub(crate) async fn write_back(
        &self,
        id: &TxId,
        changes: &[CollectionChange],
        locks: &[TxLock],
        requirement: Requirement,
    ) -> Result<BTreeSet<CollectionAddress>, TransError> {
        let results = map_all_bounded(
            Self::locked_collections(locks),
            self.parallelism,
            |parent| async move {
                let removed = self
                    .state
                    .apply_committed_write_back(&parent, id, changes, requirement)
                    .await?;
                Ok(removed.then_some(parent))
            },
        )
        .await;
        results.into_iter().filter_map(Result::transpose).collect()
    }

    /// Recovers committed directory effects from durable metadata.
    ///
    /// Returns the same per-directory removal proof as [`Self::write_back`].
    /// With `Requirement::after` using a barrier captured after finalization,
    /// success also proves every submitted directory complete, even if the
    /// returned set is empty.
    pub(crate) async fn recover_write_back(
        &self,
        id: &TxId,
        changes: &[TxCollectionChange],
        locks: &[TxLock],
        requirement: Requirement,
    ) -> Result<BTreeSet<CollectionAddress>, TransError> {
        let changes = CollectionStateResolver::recover_changes(changes);
        self.write_back(id, &changes, locks, requirement).await
    }

    /// Releases every recorded directory lock held by `id`.
    ///
    /// A present record with no holder must satisfy `requirement`. Owner
    /// cleanup can use `ANY` because it shares cache knowledge with acquisition.
    pub(crate) async fn release(
        &self,
        id: &TxId,
        locks: &[TxLock],
        requirement: Requirement,
    ) -> Result<bool, TransError> {
        let results = map_all_bounded(
            Self::locked_collections(locks),
            self.parallelism,
            |parent| async move { self.release_directory(&parent, id, requirement).await },
        )
        .await;
        results
            .into_iter()
            .try_fold(false, |changed, result| Ok(changed | result?))
    }

    /// Removes a settled participant from collection topology.
    ///
    /// The caller must establish that its structural intents have settled.
    /// A present record without the participant must satisfy `requirement`.
    pub(crate) async fn release_topology_participant(
        &self,
        collection: &CollectionAddress,
        id: &TxId,
        requirement: Requirement,
    ) -> Result<bool, TransError> {
        let mut read_requirement = Requirement::ANY;
        loop {
            let (mut record, observed) =
                match self.records.load_record(collection, read_requirement).await {
                    Ok(record) => record,
                    // Publication, shared preparation cache, and GC eligibility
                    // exclude a pre-creation cached absence for recorded collections.
                    Err(StorageError::NotFound) => return Ok(false),
                    Err(error) => return Err(error.into()),
                };
            if !record.remove_topology_participant(id) {
                if observed.satisfies(requirement) {
                    return Ok(false);
                }
                // Intent settlement does not refresh the collection record.
                // Require the caller's bound only when no removal CAS applies.
                read_requirement = requirement;
                continue;
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
                .resolve_observed(parent, Some(id), Requirement::ANY)
                .await?;
            let lock = record.directory_lock();
            let already_held = lock.contains(id)
                && (desired == LockType::Read || lock.lock_type() == LockType::Write);
            if already_held {
                // This active identity's releases update the same cache;
                // foreign cleanup requires finalization. Retained ownership
                // permits this return. Directory contents are validated later.
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
                LockType::Read => record.add_directory_reader(*id),
                LockType::Write => record.set_directory_writer(*id),
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
        requirement: Requirement,
    ) -> Result<bool, TransError> {
        let mut read_requirement = Requirement::ANY;
        loop {
            let (mut record, observed) =
                match self.records.load_record(parent, read_requirement).await {
                    Ok(record) => record,
                    // Other instances access published collections after record creation.
                    // Local preparation and cleanup share a cache. GC waits for commit
                    // or acknowledged abort before inspecting prepared resources, so
                    // cached absence cannot hide a later record creation here.
                    Err(StorageError::NotFound) => return Ok(false),
                    Err(error) => return Err(error.into()),
                };
            if !record.remove_directory_holder(id) {
                if observed.satisfies(requirement) {
                    return Ok(false);
                }
                // A present cached record can predate the holder. Use the
                // caller's bound only when no CAS proves that removal is done.
                read_requirement = requirement;
                continue;
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
        transactions: TxRecordStore,
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
                            requirement = Requirement::after(self.timeline.currentness_barrier());
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
        requirement: Requirement,
    ) -> Result<bool, TransError> {
        let mut backoff = self.retry.backoff();
        let mut read_requirement = Requirement::ANY;
        loop {
            let (mut record, observed) =
                match self.records.load_record(parent, read_requirement).await {
                    Ok(record) => record,
                    // Published records predate access by other instances.
                    // Preparation shares its owner's cache, and recovery runs
                    // after commit. A cached absence cannot hide later creation
                    // of a record this transaction can still hold.
                    Err(StorageError::NotFound) => return Ok(false),
                    Err(error) => return Err(error.into()),
                };
            if !record.directory_lock().contains(id) {
                if observed.satisfies(requirement) {
                    return Ok(false);
                }
                // A stale no-holder record can predate acquisition. Reuse the
                // completion bound and apply any holder found by this check.
                read_requirement = requirement;
                continue;
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
                record.advance_directory_generation();
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
        // Final-status resolution shares this store and has observed immutable
        // committed contents. ANY cannot restore an older Pending body; it
        // still does not prove that the transaction record remains present.
        let observed = self
            .transactions
            .get_at(id, Requirement::ANY)
            .await
            .map_err(|error| {
                TransError::Storage(error.context(format!("loading committed transaction {id}")))
            })?;
        let record = observed.value().ok_or_else(|| {
            TransError::other(format!("committed transaction record disappeared for {id}"))
        })?;
        if record.status != TxCommitStatus::Committed {
            return Err(TransError::other(format!(
                "transaction {id} resolved as committed but its record has status {:?}",
                record.status
            )));
        }
        let changes = Self::recover_changes(&record.collection_changes);
        // Resolution observed this holder through the same record cache. A
        // later no-holder state proves removal; a retained holder seeds CAS.
        self.apply_committed_write_back(parent, id, &changes, Requirement::ANY)
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
    use glassdb_data::DbPrefix;
    use glassdb_storage::{CachedStore, Timeline};

    use super::*;
    use crate::monitor::ProtocolTiming;

    fn tx_id(prefix: &[u8]) -> TxId {
        TxId::with_priority(0, prefix)
    }

    fn new_locker() -> (CollectionLocker, CollectionStore, Arc<Background>) {
        let timeline = Timeline::new();
        let objects = CachedStore::new(
            Arc::new(MemoryBackend::new()),
            1 << 20,
            timeline.clone(),
            None,
        );
        let records = CollectionStore::new(objects.clone());
        let transactions = TxRecordStore::new(objects, DbPrefix::try_from("db").unwrap());
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
        let id = tx_id(&[1]);

        locker.acquire(&parent, &id, LockType::Read).await.unwrap();
        locker.acquire(&parent, &id, LockType::Write).await.unwrap();

        let (record, _) = records
            .load_record(&parent, Requirement::ANY)
            .await
            .unwrap();
        assert_eq!(record.directory_lock().lock_type(), LockType::Write);
        assert_eq!(record.directory_lock().holders(), std::slice::from_ref(&id));
    }

    #[tokio::test]
    async fn reclaimed_record_refreshes_a_cached_directory_holder() {
        use crate::engine::{AssemblyFixture, EngineConfig};
        use glassdb_data::{CollectionId, CollectionName};
        use glassdb_storage::transaction::TxRecord;

        let backend = Arc::new(MemoryBackend::new());
        let local = AssemblyFixture::new(
            backend.clone(),
            DbPrefix::try_from("db").unwrap(),
            &EngineConfig::default(),
        );
        let peer = AssemblyFixture::new(
            backend,
            DbPrefix::try_from("db").unwrap(),
            &EngineConfig::default(),
        );
        let parent = CollectionAddress::root("db");
        let old = tx_id(&[1]);
        let local_record = local
            .tx_records
            .set(&TxRecord::new(old, TxCommitStatus::Committed))
            .await
            .unwrap();
        assert_eq!(
            local.monitor.tx_status(&old).await.unwrap(),
            TxCommitStatus::Committed
        );
        let mut record = CollectionRecord::new();
        record.set_directory_writer(old);
        local.records.create_record(&parent, &record).await.unwrap();
        let (mut record, observed) = peer
            .records
            .load_record(&parent, Requirement::ANY)
            .await
            .unwrap();
        record.remove_directory_holder(&old);
        let child = CollectionId::from_bytes([1; 16]);
        let name = CollectionName::new("child").unwrap();
        record.add_child(name.clone(), child).unwrap();
        peer.records.store_record(&record, &observed).await.unwrap();
        let observed = peer
            .tx_records
            .get_at(&old, Requirement::ANY)
            .await
            .unwrap();
        peer.tx_records.delete(&observed).await.unwrap();
        local.tx_records.delete(&local_record).await.unwrap();
        assert!(matches!(
            local.tx_records.get_at(&old, Requirement::ANY).await,
            Err(StorageError::NotFound)
        ));
        let resolver = CollectionStateResolver::new(
            local.records.clone(),
            local.tx_records.clone(),
            local.timeline.clone(),
            local.monitor.clone(),
            RetryConfig::default(),
        );
        let resolved = resolver
            .resolve(&parent, None, Requirement::ANY)
            .await
            .unwrap();
        assert_eq!(resolved.child(&name), Some(child));
        assert!(!resolved.directory_lock().contains(&old));
    }
}
