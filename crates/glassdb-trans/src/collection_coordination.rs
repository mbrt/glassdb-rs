//! Transactional state resolution, locking, and write-back for collection records.

use std::collections::{BTreeMap, BTreeSet};
use std::future::ready;
use std::num::NonZeroUsize;

use glassdb_concurr::{RetrySchedule, map_all_bounded};
use glassdb_data::{CollectionAddress, TxId};
use glassdb_storage::transaction::{
    TxCollectionChange, TxCollectionOp, TxCommitStatus, TxLock, TxRecordStore,
};
use glassdb_storage::{
    CasRetries, CollectionRecord, CollectionStore, Converged, Decision, Fenced, LockType,
    Observation, Requirement, StorageError, Timeline,
};

use crate::collections::{CollectionChange, CollectionOp, DirectoryRead};
use crate::error::TransError;
use crate::monitor::HolderWait;
use crate::monitor::{Monitor, TxFinalStatus};
use crate::wound_wait::resolve_tx_conflict;

/// The directory locks of a transaction identity, with their lock types.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DirectoryLocks {
    locks: BTreeMap<CollectionAddress, LockType>,
}

impl DirectoryLocks {
    /// Returns the directory locks that `reads` and `changes` need: a write
    /// lock on each changed directory, and a read lock on each other read
    /// directory.
    pub(crate) fn needed(reads: &[DirectoryRead], changes: &[CollectionChange]) -> Self {
        let mut locks = BTreeMap::new();
        for read in reads {
            locks.entry(read.parent.clone()).or_insert(LockType::Read);
        }
        for change in changes {
            locks.insert(change.parent.clone(), LockType::Write);
        }
        Self { locks }
    }

    /// Reports whether these locks include every lock of `other`. A write lock
    /// includes a read lock.
    pub(crate) fn covers(&self, other: &Self) -> bool {
        other
            .locks
            .iter()
            .all(|(parent, typ)| match self.locks.get(parent) {
                Some(LockType::Write) => true,
                Some(held) => held == typ,
                None => false,
            })
    }

    /// Returns the durable lock records of these locks.
    pub(crate) fn durable_locks(&self) -> Vec<TxLock> {
        self.locks
            .iter()
            .map(|(parent, typ)| TxLock::Directory {
                collection: parent.clone(),
                typ: *typ,
            })
            .collect()
    }
}

/// What a load of a collection record does with a foreign holder, or a
/// topology freeze, that has no final status yet.
#[derive(Clone, Copy)]
pub(crate) enum PendingHolders {
    /// Waits for the final status.
    Wait,
    /// Returns the record with the pending holder still in it.
    Skip,
}

/// What the help with the write-back of a committed directory holder found.
enum HelpedWriteBack {
    /// The collection record has no lock of the holder now.
    Applied,
    /// GC reclaimed the committed record of the holder. The path of the record
    /// is empty, or holds a blind wound (ADR-104). GC reclaims it only after
    /// no current collection record names the holder (ADR-079).
    Reclaimed,
}

/// Resolves transaction-dependent state in collection records.
#[derive(Clone)]
pub(crate) struct CollectionStateResolver {
    records: CollectionStore,
    transactions: TxRecordStore,
    timeline: Timeline,
    monitor: Monitor,
    retry: RetrySchedule,
}

/// Coordinates collection locks and their committed effects.
#[derive(Clone)]
pub(crate) struct CollectionLocker {
    state: CollectionStateResolver,
    records: CollectionStore,
    monitor: Monitor,
    retry: RetrySchedule,
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

    /// Acquires `locks` in stable physical-address order.
    ///
    /// The caller must hold no key lock, and no directory lock at a higher
    /// address. Equal-priority transactions wait for each other under
    /// wound-wait, and directory locks have no deadlock timeout or serial
    /// fallback. So only an order of all directory locks before all key locks,
    /// each in address order, prevents a wait cycle.
    pub(crate) async fn lock(&self, id: &TxId, locks: &DirectoryLocks) -> Result<(), TransError> {
        for (parent, typ) in &locks.locks {
            self.lock_directory(parent, id, *typ).await?;
        }
        Ok(())
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
    /// A present record with no holder must satisfy `requirement`.
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

    /// Acquires the lock of one directory, or upgrades a read lock to a write
    /// lock.
    async fn lock_directory(
        &self,
        parent: &CollectionAddress,
        id: &TxId,
        desired: LockType,
    ) -> Result<(), TransError> {
        let wait = self.monitor.holder_wait();
        let wait = &wait;
        let decide = |record: CollectionRecord, _| async move {
            let mut record = match self
                .state
                .resolve_record(parent, record, Some(id), PendingHolders::Wait, wait)
                .await?
            {
                Ok(record) => record,
                Err(next) => return Ok(next),
            };
            let lock = record.directory_lock();
            let already_held = lock.contains(id)
                && (desired == LockType::Read || lock.lock_type() == LockType::Write);
            if already_held {
                // Proof: shared local knowledge (same cache, final
                // before foreign removal) for the held lock, and later
                // validation for the directory contents. This active
                // identity releases its locks through the same cache.
                return Ok(Decision::Done(()));
            }
            let conflicts = match desired {
                LockType::Read => lock.lock_type() == LockType::Write,
                LockType::Write => !lock.holders().is_empty(),
                _ => false,
            };
            if conflicts
                && let Some(holder) = lock.holders().iter().find(|holder| *holder != id).cloned()
            {
                return match resolve_tx_conflict(wait, id, &holder).await? {
                    // The holder can have left the record already (ADR-024).
                    None => Ok(Decision::Reload(Requirement::after(
                        self.state.timeline.currentness_barrier(),
                    ))),
                    Some(TxFinalStatus::Committed) => {
                        match self
                            .state
                            .help_committed_write_back(parent, &holder)
                            .await?
                        {
                            HelpedWriteBack::Applied => Ok(Decision::Wait(Requirement::ANY)),
                            // As in `load_resolved`: GC can reclaim the
                            // log after another instance removed the
                            // holder.
                            HelpedWriteBack::Reclaimed => Ok(Decision::Wait(Requirement::after(
                                self.state.timeline.currentness_barrier(),
                            ))),
                        }
                    }
                    Some(TxFinalStatus::Aborted) => {
                        record.remove_directory_holder(&holder);
                        Ok(Decision::Advance(record))
                    }
                };
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
            Ok::<_, TransError>(Decision::Write(record, ()))
        };
        let converge = |proof| async move {
            // A lock CAS adds a holder, so the owner must know whether it can
            // still land (ADR-106).
            match self
                .records
                .converge(parent, proof, CasRetries::Unbounded(self.retry), &decide)
                .await?
            {
                Fenced::Converged(converged) => Ok(converged),
                Fenced::InDoubt => Err(TransError::MutationInDoubt(format!(
                    "directory lock of {id} on {parent:?}"
                ))),
            }
        };
        let mut locked = converge(Requirement::ANY).await?;
        if matches!(locked, Converged::Missing) {
            // A missing record can be a cached absence. Read it again after a
            // new barrier (missing-object retries), so that only a bounded
            // absence reports a dropped collection.
            locked = converge(Requirement::after(
                self.state.timeline.currentness_barrier(),
            ))
            .await?;
        }
        match locked {
            Converged::Written { .. } | Converged::Done(()) => Ok(()),
            Converged::Missing => Err(StorageError::NotFound
                .classify_collection_absence(parent)
                .into()),
            Converged::Stop(()) | Converged::Exhausted => Err(TransError::Retry),
        }
    }

    async fn release_directory(
        &self,
        parent: &CollectionAddress,
        id: &TxId,
        requirement: Requirement,
    ) -> Result<bool, TransError> {
        // A present cached record can predate the holder, so a record without
        // it ends the release only at the caller's bound.
        let released = self
            .records
            .converge(
                parent,
                requirement,
                CasRetries::Unbounded(self.retry),
                |mut record, _| {
                    ready(Ok::<_, TransError>(if record.remove_directory_holder(id) {
                        Decision::Write(record, ())
                    } else {
                        Decision::Done(())
                    }))
                },
            )
            .await?
            .or_unavailable()?;
        match released {
            Converged::Written { .. } => Ok(true),
            Converged::Done(()) | Converged::Missing => Ok(false),
            Converged::Stop(()) | Converged::Exhausted => Err(TransError::Retry),
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
        retry: RetrySchedule,
    ) -> Self {
        Self {
            records,
            transactions,
            timeline,
            monitor,
            retry,
        }
    }

    /// Loads a collection record and its CAS observation, and removes the
    /// foreign holders and the topology freeze that have a final status.
    pub(crate) async fn load_resolved(
        &self,
        parent: &CollectionAddress,
        own: Option<&TxId>,
        requirement: Requirement,
        pending: PendingHolders,
    ) -> Result<(CollectionRecord, Observation<CollectionRecord>), TransError> {
        let wait = self.monitor.holder_wait();
        let wait = &wait;
        let resolved = self
            .records
            .converge(
                parent,
                requirement,
                CasRetries::Unbounded(self.retry),
                |record, observed| async move {
                    // Resolve only states at `requirement`, as a load at it
                    // would give.
                    if !observed.satisfies(requirement) {
                        return Ok(Decision::Reload(requirement));
                    }
                    Ok::<_, TransError>(
                        match self
                            .resolve_record(parent, record, own, pending, wait)
                            .await?
                        {
                            Ok(record) => Decision::Done((record, observed)),
                            Err(next) => next,
                        },
                    )
                },
            )
            .await?
            .or_unavailable()?;
        match resolved {
            Converged::Done(resolved) => Ok(resolved),
            Converged::Missing => Err(StorageError::NotFound
                .classify_collection_absence(parent)
                .into()),
            Converged::Written { .. } | Converged::Stop(_) | Converged::Exhausted => {
                Err(TransError::Retry)
            }
        }
    }

    /// Resolves one state of the record of `parent`: the foreign topology
    /// freeze and the foreign directory writer, when they have a final status.
    /// Returns the resolved record, or the decision that the step of the
    /// caller makes first.
    /// Pending readers cannot change the bindings or the generation, so they
    /// stay. A writer that needs the lock resolves them in
    /// `CollectionLocker::lock_directory` (ADR-089).
    async fn resolve_record<T>(
        &self,
        parent: &CollectionAddress,
        mut record: CollectionRecord,
        own: Option<&TxId>,
        pending: PendingHolders,
        wait: &HolderWait<'_>,
    ) -> Result<Result<CollectionRecord, Decision<CollectionRecord, T>>, TransError> {
        if let Some(holder) = record.topology_freeze().cloned()
            && Some(&holder) != own
        {
            let Some(status) = self.holder_status(&holder, pending, wait).await? else {
                return Ok(self.unresolved(record, pending));
            };
            return match status {
                TxFinalStatus::Committed => Err(TransError::StaleCollection),
                TxFinalStatus::Aborted => {
                    record.remove_topology_freeze(&holder);
                    Ok(Err(Decision::Advance(record)))
                }
            };
        }
        if record.directory_lock().lock_type() == LockType::Read {
            return Ok(Ok(record));
        }
        let holder = record
            .directory_lock()
            .holders()
            .iter()
            .find(|holder| Some(*holder) != own)
            .cloned();
        let Some(holder) = holder else {
            return Ok(Ok(record));
        };
        let Some(status) = self.holder_status(&holder, pending, wait).await? else {
            return Ok(self.unresolved(record, pending));
        };
        match status {
            TxFinalStatus::Committed => {
                match self.help_committed_write_back(parent, &holder).await? {
                    HelpedWriteBack::Applied => Ok(Err(Decision::Reload(Requirement::ANY))),
                    // GC can reclaim a log after another instance removes its
                    // directory holder. A cached record must reload.
                    HelpedWriteBack::Reclaimed => Ok(Err(Decision::Wait(Requirement::after(
                        self.timeline.currentness_barrier(),
                    )))),
                }
            }
            TxFinalStatus::Aborted => {
                record.remove_directory_holder(&holder);
                Ok(Err(Decision::Advance(record)))
            }
        }
    }

    /// Returns what a resolution does with a holder that has no final status
    /// yet. A skip keeps the record. A wait reads the record again at a fresh
    /// barrier, because the holder can have left it already (ADR-024).
    fn unresolved<T>(
        &self,
        record: CollectionRecord,
        pending: PendingHolders,
    ) -> Result<CollectionRecord, Decision<CollectionRecord, T>> {
        match pending {
            PendingHolders::Skip => Ok(record),
            PendingHolders::Wait => Err(Decision::Reload(Requirement::after(
                self.timeline.currentness_barrier(),
            ))),
        }
    }

    /// Returns the final status of `holder`, or `None` if it has none yet. A
    /// wait waits for at most one step of `wait`.
    async fn holder_status(
        &self,
        holder: &TxId,
        pending: PendingHolders,
        wait: &HolderWait<'_>,
    ) -> Result<Option<TxFinalStatus>, TransError> {
        match pending {
            PendingHolders::Wait => wait.final_status(holder).await,
            PendingHolders::Skip => Ok(match self.monitor.tx_status(holder).await? {
                TxCommitStatus::Committed => Some(TxFinalStatus::Committed),
                // The CAS on the collection record checks that the lock is
                // current, so a blind wound holds no committed effect here.
                TxCommitStatus::Aborted
                | TxCommitStatus::Wounded
                | TxCommitStatus::BlindWounded => Some(TxFinalStatus::Aborted),
                TxCommitStatus::Pending | TxCommitStatus::Unknown => None,
            }),
        }
    }

    async fn apply_committed_write_back(
        &self,
        parent: &CollectionAddress,
        id: &TxId,
        changes: &[CollectionChange],
        requirement: Requirement,
    ) -> Result<bool, TransError> {
        // A stale record without the holder can predate acquisition, so it
        // ends the write-back only at the completion bound.
        let applied = self
            .records
            .converge(
                parent,
                requirement,
                CasRetries::Unbounded(self.retry),
                |mut record, _| {
                    ready(
                        Self::write_back_record(&mut record, id, parent, changes).map(|applies| {
                            if applies {
                                Decision::Write(record, ())
                            } else {
                                Decision::Done(())
                            }
                        }),
                    )
                },
            )
            .await?
            .or_unavailable()?;
        match applied {
            Converged::Written { .. } => Ok(true),
            Converged::Done(()) | Converged::Missing => Ok(false),
            Converged::Stop(()) | Converged::Exhausted => Err(TransError::Retry),
        }
    }

    /// Applies the committed changes of `id` below `parent` to `record`, and
    /// removes its directory lock. Returns `false` when `record` has no lock
    /// of `id`, so there is nothing to apply.
    fn write_back_record(
        record: &mut CollectionRecord,
        id: &TxId,
        parent: &CollectionAddress,
        changes: &[CollectionChange],
    ) -> Result<bool, TransError> {
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
            record.advance_directory_generation();
        }
        record.remove_directory_holder(id);
        Ok(true)
    }

    /// Helps the write-back below `parent` of `id`, which this instance knows
    /// as committed.
    async fn help_committed_write_back(
        &self,
        parent: &CollectionAddress,
        id: &TxId,
    ) -> Result<HelpedWriteBack, TransError> {
        // Proof: stable fact (final committed contents). Final-status
        // resolution observed the committed record through this store, so ANY
        // cannot return an older pending body. This read does not prove that
        // the record is still present. A stale absence or a stale blind wound
        // only delays work: `Reclaimed` makes the caller load the collection
        // record again at a fresh barrier.
        let observed = match self.transactions.get_at(id, Requirement::ANY).await {
            Ok(observed) => observed,
            Err(StorageError::NotFound) => return Ok(HelpedWriteBack::Reclaimed),
            Err(error) => {
                return Err(TransError::Storage(
                    error.context(format!("loading committed transaction {id}")),
                ));
            }
        };
        let record = observed.value().ok_or_else(|| {
            TransError::other(format!("committed transaction record disappeared for {id}"))
        })?;
        // ADR-104: a blind wound of a committed identity can land only after
        // GC reclaimed its committed record.
        if record.status == TxCommitStatus::BlindWounded {
            return Ok(HelpedWriteBack::Reclaimed);
        }
        if record.status != TxCommitStatus::Committed {
            return Err(TransError::other(format!(
                "transaction {id} resolved as committed but its record has status {:?}",
                record.status
            )));
        }
        let changes = Self::recover_changes(&record.collection_changes);
        // Proof: shared local knowledge (same cache, one acquisition).
        // Resolution observed this
        // holder through the same record cache, so a later state without the
        // holder proves its removal. A state with the holder starts a
        // conditional mutation.
        self.apply_committed_write_back(parent, id, &changes, Requirement::ANY)
            .await?;
        Ok(HelpedWriteBack::Applied)
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
    use glassdb_data::{CollectionId, CollectionName, DbPrefix};
    use glassdb_storage::{CachedStore, Timeline};

    use std::time::Duration;

    use glassdb_storage::transaction::TxRecord;

    use super::*;
    use crate::collections::{DirectoryEvidence, DirectoryReadKind};
    use crate::monitor::{OwnerAbortOutcome, ProtocolTiming};
    use crate::retry_timing::RetryTiming;

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
        let records = CollectionStore::new(objects.clone(), std::num::NonZeroUsize::MIN);
        let transactions = TxRecordStore::new(objects, DbPrefix::try_from("db").unwrap());
        let background = Arc::new(Background::new());
        let monitor = Monitor::with_config(
            transactions.clone(),
            timeline.clone(),
            Arc::downgrade(&background),
            RetrySchedule::default(),
            RetrySchedule::default(),
            ProtocolTiming::default(),
            crate::gc::GcHints::default(),
        );
        let state = CollectionStateResolver::new(
            records.clone(),
            transactions,
            timeline,
            monitor,
            RetrySchedule::default(),
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

        let read = DirectoryRead {
            parent: parent.clone(),
            kind: DirectoryReadKind::Listing { generation: 0 },
            evidence: DirectoryEvidence::default(),
        };
        let change = CollectionChange {
            parent: parent.clone(),
            name: CollectionName::new("child").unwrap(),
            collection: CollectionAddress::new("db", CollectionId::from_bytes([2; 16])),
            expected: None,
            op: CollectionOp::Create,
        };
        locker
            .lock(&id, &DirectoryLocks::needed(&[read], &[]))
            .await
            .unwrap();
        locker
            .lock(&id, &DirectoryLocks::needed(&[], &[change]))
            .await
            .unwrap();

        let (record, _) = records
            .load_record(&parent, Requirement::ANY)
            .await
            .unwrap();
        assert_eq!(record.directory_lock().lock_type(), LockType::Write);
        assert_eq!(record.directory_lock().holders(), std::slice::from_ref(&id));
    }

    #[tokio::test]
    async fn reclaimed_record_refreshes_a_cached_directory_holder() {
        assert_reclaim_refreshes_a_cached_directory_holder(false).await;
    }

    // ADR-104: after GC reclaims the committed record of a holder, a blind
    // wound can land at the empty path. It proves the same as the absence.
    #[tokio::test]
    async fn blind_wound_after_reclaim_refreshes_a_cached_directory_holder() {
        assert_reclaim_refreshes_a_cached_directory_holder(true).await;
    }

    /// Resolves a cached directory writer whose committed record GC reclaimed
    /// after a peer removed the writer. With `blind_wound`, a blind wound then
    /// fills the empty path of the record.
    async fn assert_reclaim_refreshes_a_cached_directory_holder(blind_wound: bool) {
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
        if blind_wound {
            local
                .tx_records
                .set(&TxRecord::new(old, TxCommitStatus::BlindWounded))
                .await
                .unwrap();
        }
        let resolver = CollectionStateResolver::new(
            local.records.clone(),
            local.tx_records.clone(),
            local.timeline.clone(),
            local.monitor.clone(),
            RetrySchedule::default(),
        );
        let resolved = resolver
            .load_resolved(&parent, None, Requirement::ANY, PendingHolders::Wait)
            .await
            .unwrap()
            .0;
        assert_eq!(resolved.child(&name), Some(child));
        assert!(!resolved.directory_lock().contains(&old));
    }

    fn write_lock(parent: &CollectionAddress) -> DirectoryLocks {
        DirectoryLocks::needed(
            &[],
            &[CollectionChange {
                parent: parent.clone(),
                name: CollectionName::new("child").unwrap(),
                collection: CollectionAddress::new("db", CollectionId::from_bytes([2; 16])),
                expected: None,
                op: CollectionOp::Create,
            }],
        )
    }

    /// Creates the root record with `reader` as its pending directory reader.
    async fn root_with_reader(
        locker: &CollectionLocker,
        records: &CollectionStore,
        reader: &TxId,
    ) -> CollectionAddress {
        let parent = CollectionAddress::root("db");
        let mut record = CollectionRecord::new();
        record.add_directory_reader(*reader);
        assert!(records.create_record(&parent, &record).await.unwrap());
        locker.monitor.begin_tx(reader);
        parent
    }

    async fn holders(records: &CollectionStore, parent: &CollectionAddress) -> Vec<TxId> {
        let (record, _) = records.load_record(parent, Requirement::ANY).await.unwrap();
        record.directory_lock().holders().to_vec()
    }

    // ADR-089: a reader cannot change the bindings, so a load does not wait.
    #[tokio::test]
    async fn a_load_does_not_wait_for_a_pending_reader() {
        let (locker, records, _background) = new_locker();
        let reader = tx_id(&[1]);
        let parent = root_with_reader(&locker, &records, &reader).await;

        let record = tokio::time::timeout(
            Duration::from_secs(5),
            locker
                .state
                .load_resolved(&parent, None, Requirement::ANY, PendingHolders::Wait),
        )
        .await
        .expect("the load waited for the pending reader")
        .unwrap()
        .0;

        assert!(record.directory_lock().contains(&reader));
    }

    #[tokio::test]
    async fn an_older_writer_wounds_a_younger_reader() {
        let (locker, records, _background) = new_locker();
        let reader = TxId::with_priority(2_000_000_000, b"younger");
        let writer = TxId::with_priority(1_000_000_000, b"older");
        let parent = root_with_reader(&locker, &records, &reader).await;

        tokio::time::timeout(
            Duration::from_secs(5),
            locker.lock(&writer, &write_lock(&parent)),
        )
        .await
        .expect("the older writer waited for the younger reader")
        .unwrap();

        assert_eq!(holders(&records, &parent).await, vec![writer]);
        assert!(matches!(
            locker.monitor.tx_status(&reader).await.unwrap(),
            TxCommitStatus::Aborted | TxCommitStatus::Wounded | TxCommitStatus::BlindWounded
        ));
    }

    #[tokio::test]
    async fn a_younger_writer_waits_for_an_older_reader() {
        let (locker, records, _background) = new_locker();
        let reader = TxId::with_priority(1_000_000_000, b"older");
        let writer = TxId::with_priority(2_000_000_000, b"younger");
        let parent = root_with_reader(&locker, &records, &reader).await;
        let locks = write_lock(&parent);
        let lock = locker.lock(&writer, &locks);
        tokio::pin!(lock);

        assert!(
            tokio::time::timeout(Duration::from_millis(200), &mut lock)
                .await
                .is_err(),
            "the younger writer did not wait"
        );
        assert_eq!(
            locker.monitor.abort_owned_tx(&reader).await.unwrap(),
            OwnerAbortOutcome::Acknowledged
        );
        lock.await.unwrap();

        assert_eq!(holders(&records, &parent).await, vec![writer]);
    }

    #[tokio::test]
    async fn a_writer_removes_a_committed_reader() {
        let (locker, records, _background) = new_locker();
        let reader = tx_id(&[1]);
        let writer = tx_id(&[2]);
        let parent = root_with_reader(&locker, &records, &reader).await;
        let mut record = TxRecord::new(reader, TxCommitStatus::Committed);
        record.locks.push(TxLock::Directory {
            collection: parent.clone(),
            typ: LockType::Read,
        });
        locker.monitor.commit_tx(record).await.unwrap();

        locker.lock(&writer, &write_lock(&parent)).await.unwrap();

        assert_eq!(holders(&records, &parent).await, vec![writer]);
    }

    #[tokio::test]
    async fn a_writer_removes_an_aborted_reader() {
        let (locker, records, _background) = new_locker();
        let reader = tx_id(&[1]);
        let writer = tx_id(&[2]);
        let parent = root_with_reader(&locker, &records, &reader).await;
        locker.monitor.abort_owned_tx(&reader).await.unwrap();

        locker.lock(&writer, &write_lock(&parent)).await.unwrap();

        assert_eq!(holders(&records, &parent).await, vec![writer]);
    }

    // GC can reclaim the record of a committed reader after a peer removed its
    // hold. A writer whose cache still shows the hold must load the record again.
    #[tokio::test]
    async fn a_writer_reloads_a_cached_reader_whose_record_was_reclaimed() {
        assert_a_writer_reloads_a_reclaimed_cached_reader(false).await;
    }

    // ADR-104: after GC reclaims the committed record of a reader, a blind
    // wound can land at the empty path. It proves the same as the absence.
    #[tokio::test]
    async fn a_writer_reloads_a_cached_reader_with_a_blind_wound_after_reclaim() {
        assert_a_writer_reloads_a_reclaimed_cached_reader(true).await;
    }

    /// Locks a directory whose cached record names a committed reader that a
    /// peer removed and whose record GC reclaimed. With `blind_wound`, a blind
    /// wound then fills the empty path of the record.
    async fn assert_a_writer_reloads_a_reclaimed_cached_reader(blind_wound: bool) {
        use crate::engine::{AssemblyFixture, EngineConfig};

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
        let reader = tx_id(&[1]);
        let writer = tx_id(&[2]);
        let local_record = local
            .tx_records
            .set(&TxRecord::new(reader, TxCommitStatus::Committed))
            .await
            .unwrap();
        assert_eq!(
            local.monitor.tx_status(&reader).await.unwrap(),
            TxCommitStatus::Committed
        );
        let mut record = CollectionRecord::new();
        record.add_directory_reader(reader);
        local.records.create_record(&parent, &record).await.unwrap();
        let (mut record, observed) = peer
            .records
            .load_record(&parent, Requirement::ANY)
            .await
            .unwrap();
        record.remove_directory_holder(&reader);
        peer.records.store_record(&record, &observed).await.unwrap();
        local.tx_records.delete(&local_record).await.unwrap();
        if blind_wound {
            local
                .tx_records
                .set(&TxRecord::new(reader, TxCommitStatus::BlindWounded))
                .await
                .unwrap();
        }
        let locker = CollectionLocker::new(
            CollectionStateResolver::new(
                local.records.clone(),
                local.tx_records.clone(),
                local.timeline.clone(),
                local.monitor.clone(),
                RetrySchedule::default(),
            ),
            NonZeroUsize::MIN,
        );

        locker.lock(&writer, &write_lock(&parent)).await.unwrap();

        assert_eq!(holders(&local.records, &parent).await, vec![writer]);
    }

    /// One database instance over `backend`, with its own cache.
    struct Instance {
        locker: CollectionLocker,
        state: CollectionStateResolver,
        records: CollectionStore,
        monitor: Monitor,
        timeline: Timeline,
        _background: Arc<Background>,
    }

    fn instance(backend: Arc<dyn glassdb_backend::Backend>) -> Instance {
        let timeline = Timeline::new();
        let objects = CachedStore::new(backend, 1 << 20, timeline.clone(), None);
        let records = CollectionStore::new(objects.clone(), NonZeroUsize::MIN);
        let transactions = TxRecordStore::new(objects, DbPrefix::try_from("db").unwrap());
        let background = Arc::new(Background::new());
        let monitor = Monitor::with_config(
            transactions.clone(),
            timeline.clone(),
            Arc::downgrade(&background),
            RetrySchedule::default(),
            RetryTiming::default().holder_poll,
            ProtocolTiming::default(),
            crate::gc::GcHints::default(),
        );
        let state = CollectionStateResolver::new(
            records.clone(),
            transactions,
            timeline.clone(),
            monitor.clone(),
            RetrySchedule::default(),
        );
        Instance {
            locker: CollectionLocker::new(state.clone(), NonZeroUsize::MIN),
            state,
            records,
            monitor,
            timeline,
            _background: background,
        }
    }

    // ADR-024: a waiter for a directory holder reads the record again after
    // each step of its wait. Here the holder leaves the record, and GC deletes
    // its transaction record, so the status of the holder stays unknown to the
    // waiter.
    #[tokio::test(start_paused = true)]
    async fn a_writer_sees_a_holder_leave_the_record_before_its_final_status() {
        let backend: Arc<dyn glassdb_backend::Backend> = Arc::new(MemoryBackend::new());
        let owner = instance(backend.clone());
        let waiter = instance(backend.clone());
        let transactions = TxRecordStore::new(
            CachedStore::new(backend, 1 << 20, Timeline::new(), None),
            DbPrefix::try_from("db").unwrap(),
        );
        let parent = CollectionAddress::root("db");
        let reader = TxId::with_priority(1_000_000_000, b"older");
        let writer = TxId::with_priority(2_000_000_000, b"younger");
        let pending = transactions
            .set(&TxRecord::new(reader, TxCommitStatus::Pending))
            .await
            .unwrap();
        let mut record = CollectionRecord::new();
        record.add_directory_reader(reader);
        assert!(owner.records.create_record(&parent, &record).await.unwrap());

        let locks = write_lock(&parent);
        let lock = waiter.locker.lock(&writer, &locks);
        tokio::pin!(lock);
        // The steps of the wait grow to the cap before the holder leaves.
        assert!(
            tokio::time::timeout(Duration::from_secs(10), &mut lock)
                .await
                .is_err(),
            "the younger writer did not wait"
        );
        let (mut record, observed) = owner
            .records
            .load_record(&parent, Requirement::ANY)
            .await
            .unwrap();
        record.remove_directory_holder(&reader);
        assert!(
            owner
                .records
                .store_record(&record, &observed)
                .await
                .unwrap()
        );
        // The reader commits, and GC deletes its record at once, before the
        // next poll of the waiter.
        let committed = transactions
            .set_if(&TxRecord::new(reader, TxCommitStatus::Committed), &pending)
            .await
            .unwrap();
        transactions.delete(&committed).await.unwrap();

        // One step of the wait is at most the cap of the holder poll.
        let cap = RetryTiming::default().holder_poll.max_interval;
        tokio::time::timeout(cap + Duration::from_millis(10), lock)
            .await
            .expect("the writer did not read the record again")
            .unwrap();
        assert_eq!(holders(&waiter.records, &parent).await, vec![writer]);
    }

    // Instance `b` caches the absence of two records, and instance `a` then
    // creates them. A directory lock and a directory snapshot read past the
    // cached absence, so they do not report a dropped collection.
    #[tokio::test]
    async fn transaction_path_reads_past_a_cached_absence() {
        let backend: Arc<dyn glassdb_backend::Backend> = Arc::new(MemoryBackend::new());
        let a = instance(backend.clone());
        let b = instance(backend);
        let parent = |byte| CollectionAddress::new("db", CollectionId::from_bytes([byte; 16]));
        let (locked, listed) = (parent(7), parent(8));
        for collection in [&locked, &listed] {
            assert!(matches!(
                b.records.load_record(collection, Requirement::ANY).await,
                Err(StorageError::NotFound)
            ));
            let record = CollectionRecord::new();
            assert!(a.records.create_record(collection, &record).await.unwrap());
        }
        let id = tx_id(&[1]);

        let locks = DirectoryLocks {
            locks: BTreeMap::from([(locked.clone(), LockType::Read)]),
        };
        b.locker.lock(&id, &locks).await.unwrap();
        let (record, _) = a
            .records
            .load_record(
                &locked,
                Requirement::after(a.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        assert!(record.directory_lock().contains(&id));

        let snapshot =
            crate::collection_catalog::CollectionCatalog::new(b.state.clone(), b.timeline.clone())
                .snapshot(&listed, &id)
                .await
                .unwrap();
        assert!(snapshot.children.is_empty());
    }

    // Instance `b` caches the absence of three records, and instance `a` then
    // creates them with a directory holder or a topology participant. A cached
    // absence ends a cleanup only at the caller's bound, so cleanup at a bound
    // after the creation finds the holder and the participant.
    #[tokio::test]
    async fn bounded_cleanup_reads_past_a_cached_absence() {
        let backend: Arc<dyn glassdb_backend::Backend> = Arc::new(MemoryBackend::new());
        let a = instance(backend.clone());
        let b = instance(backend);
        let parent = |byte| CollectionAddress::new("db", CollectionId::from_bytes([byte; 16]));
        let (released, written_back, left) = (parent(7), parent(8), parent(9));
        let holder = tx_id(&[1]);
        let participant = tx_id(&[2]);
        for collection in [&released, &written_back, &left] {
            assert!(matches!(
                b.records.load_record(collection, Requirement::ANY).await,
                Err(StorageError::NotFound)
            ));
            let mut record = CollectionRecord::new();
            if collection == &left {
                assert!(record.add_topology_participant(participant));
            } else {
                record.set_directory_writer(holder);
            }
            assert!(a.records.create_record(collection, &record).await.unwrap());
        }
        let lock = |collection: &CollectionAddress| {
            vec![TxLock::Directory {
                collection: collection.clone(),
                typ: LockType::Write,
            }]
        };
        let child = CollectionName::new("child").unwrap();
        let changes = vec![TxCollectionChange {
            parent: written_back.clone(),
            name: child.clone(),
            collection: CollectionAddress::new("db", CollectionId::from_bytes([10; 16])),
            op: TxCollectionOp::Create,
        }];
        let topology = crate::structural::TopologyMembership::new(
            b.records.clone(),
            b.monitor.clone(),
            b.timeline.clone(),
            RetrySchedule::default(),
        );

        let bound = Requirement::after(b.timeline.currentness_barrier());
        assert!(
            b.locker
                .release(&holder, &lock(&released), bound)
                .await
                .unwrap()
        );
        let applied = b
            .locker
            .recover_write_back(&holder, &changes, &lock(&written_back), bound)
            .await
            .unwrap();
        assert!(applied.contains(&written_back));
        assert!(
            topology
                .leave(&left, std::slice::from_ref(&participant), bound)
                .await
                .unwrap()
        );

        let fresh = Requirement::after(a.timeline.currentness_barrier());
        for collection in [&released, &written_back] {
            let (record, _) = a.records.load_record(collection, fresh).await.unwrap();
            assert!(record.directory_lock().holders().is_empty());
        }
        let (record, _) = a.records.load_record(&written_back, fresh).await.unwrap();
        assert!(record.child(&child).is_some());
        let (record, _) = a.records.load_record(&left, fresh).await.unwrap();
        assert_eq!(record.topology_participants().count(), 0);
    }
}
