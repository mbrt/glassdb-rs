//! Transactional locking and write-back for collection records.

use std::collections::BTreeMap;

use glassdb_concurr::{RetryConfig, rt};
use glassdb_data::{CollectionAddress, TxId};
use glassdb_storage::{
    CollectionRecord, CollectionStore, LockType, Observation, Requirement, StorageError, TLogger,
    TxCollectionChange, TxCollectionOp, TxCommitStatus, TxLock,
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

/// Coordinates locks and committed effects on collection records.
#[derive(Clone)]
pub(crate) struct DirectoryLocker {
    records: CollectionStore,
    transactions: TLogger,
    monitor: Monitor,
    retry: RetryConfig,
}

impl DirectoryLocker {
    /// Creates directory locking over collection and transaction stores.
    pub(crate) fn new(
        records: CollectionStore,
        transactions: TLogger,
        monitor: Monitor,
        retry: RetryConfig,
    ) -> Self {
        DirectoryLocker {
            records,
            transactions,
            monitor,
            retry,
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

    /// Loads a record after resolving lifecycle and directory holders.
    pub(crate) async fn load_resolved_record(
        &self,
        parent: &CollectionAddress,
        own: Option<&TxId>,
        requirement: Requirement,
    ) -> Result<(CollectionRecord, Observation<CollectionRecord>), TransError> {
        let prefix = parent.physical_prefix();
        let mut backoff = self.retry.backoff();
        loop {
            let (mut record, observed) = self
                .records
                .load_record(&prefix, requirement)
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
                    self.help_committed_write_back(parent, &holder).await?;
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

    /// Applies committed directory effects and releases their locks.
    pub(crate) async fn write_back(
        &self,
        id: &TxId,
        changes: &[CollectionChange],
        locks: &[TxLock],
    ) -> Result<(), TransError> {
        for parent in directory_collections(locks) {
            self.write_back_one(parent, id, changes).await?;
        }
        Ok(())
    }

    /// Recovers committed directory effects from durable metadata.
    pub(crate) async fn recover_write_back(
        &self,
        id: &TxId,
        changes: &[TxCollectionChange],
        locks: &[TxLock],
    ) -> Result<(), TransError> {
        let changes = recover_collection_changes(changes);
        self.write_back(id, &changes, locks).await
    }

    /// Releases every recorded directory lock held by `id`.
    pub(crate) async fn release(&self, id: &TxId, locks: &[TxLock]) -> Result<(), TransError> {
        for parent in directory_collections(locks) {
            let prefix = parent.physical_prefix();
            loop {
                let (mut record, observed) =
                    match self.records.load_record(&prefix, Requirement::Any).await {
                        Ok(record) => record,
                        Err(StorageError::NotFound) => break,
                        Err(error) => return Err(error.into()),
                    };
                if !record.remove_directory_holder(id) {
                    break;
                }
                if self.records.store_record(&record, &observed).await? {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Reports whether any recorded directory still refers to `id`.
    pub(crate) async fn is_referenced(
        &self,
        id: &TxId,
        locks: &[TxLock],
        requirement: Requirement,
    ) -> Result<bool, TransError> {
        for collection in directory_collections(locks) {
            if let Ok((record, _)) = self
                .records
                .load_record(&collection.physical_prefix(), requirement)
                .await
                && record.directory_lock().contains(id)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Removes a settled structural operation from collection topology.
    pub(crate) async fn release_topology_participant(
        &self,
        collection: &CollectionAddress,
        id: &TxId,
    ) -> Result<(), TransError> {
        let prefix = collection.physical_prefix();
        loop {
            let (mut record, observed) =
                match self.records.load_record(&prefix, Requirement::Any).await {
                    Ok(record) => record,
                    Err(StorageError::NotFound) => return Ok(()),
                    Err(error) => return Err(error.into()),
                };
            if !record.remove_topology_participant(id) {
                return Ok(());
            }
            if self.records.store_record(&record, &observed).await? {
                return Ok(());
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
                .load_resolved_record(parent, Some(id), Requirement::Any)
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

    async fn write_back_one(
        &self,
        parent: &CollectionAddress,
        id: &TxId,
        changes: &[CollectionChange],
    ) -> Result<(), TransError> {
        let prefix = parent.physical_prefix();
        let mut backoff = self.retry.backoff();
        loop {
            let (mut record, observed) =
                match self.records.load_record(&prefix, Requirement::Any).await {
                    Ok(record) => record,
                    Err(StorageError::NotFound) => return Ok(()),
                    Err(error) => return Err(error.into()),
                };
            if !record.directory_lock().contains(id) {
                return Ok(());
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
                return Ok(());
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
        let changes = recover_collection_changes(&log.collection_changes);
        self.write_back_one(parent, id, &changes).await
    }
}

fn directory_collections(locks: &[TxLock]) -> impl Iterator<Item = &CollectionAddress> {
    locks.iter().filter_map(|lock| match lock {
        TxLock::Directory { collection, .. } => Some(collection),
        _ => None,
    })
}

fn recover_collection_changes(changes: &[TxCollectionChange]) -> Vec<CollectionChange> {
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
