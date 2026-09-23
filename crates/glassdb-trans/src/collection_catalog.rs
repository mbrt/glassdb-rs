//! Logical snapshots and validation for collection name-to-ID directories.

use std::collections::{BTreeMap, BTreeSet};

use glassdb_data::{CollectionAddress, TxId};
use glassdb_storage::{
    CollectionRecord, CurrentnessBarrier, Requirement, SplitPolicy, StorageError,
};

use crate::collection_coordination::CollectionStateResolver;
use crate::collections::{
    CollectionChange, CollectionOp, DirectoryRead, DirectoryReadKind, DirectorySnapshot,
};
use crate::error::TransError;

/// Builds and validates semantic views of transactional collection directories.
#[derive(Clone)]
pub(crate) struct CollectionCatalog {
    state: CollectionStateResolver,
}

impl CollectionCatalog {
    /// Creates access to transactional collection directories.
    pub(crate) fn new(state: CollectionStateResolver) -> Self {
        CollectionCatalog { state }
    }

    /// Loads a direct-child directory after resolving finalized transactions.
    pub(crate) async fn snapshot(
        &self,
        parent: &CollectionAddress,
    ) -> Result<DirectorySnapshot, TransError> {
        // Callers retain directory reads and mutation preconditions as semantic
        // dependencies. Validation checks them after the body before accepting
        // its outcome, so this snapshot can use cached contents.
        let record = self.state.resolve(parent, None, Requirement::ANY).await?;
        Ok(DirectorySnapshot {
            children: record
                .children()
                .map(|(name, id)| (name.to_vec(), id))
                .collect(),
            generation: record.directory_generation(),
        })
    }

    /// Validates logical directory reads and mutation preconditions.
    pub(crate) async fn validate(
        &self,
        id: Option<&TxId>,
        reads: &[DirectoryRead],
        changes: &[CollectionChange],
        barrier: CurrentnessBarrier,
        split_policy: &SplitPolicy,
    ) -> Result<bool, TransError> {
        let mut records = BTreeMap::<CollectionAddress, CollectionRecord>::new();
        let mut dropped = BTreeSet::new();
        for parent in reads
            .iter()
            .map(|read| &read.parent)
            .chain(changes.iter().map(|change| &change.parent))
        {
            if !records.contains_key(parent) && !dropped.contains(parent) {
                let record = match self
                    .state
                    .resolve(parent, id, Requirement::after(barrier))
                    .await
                {
                    Ok(record) => record,
                    // A removed name can invalidate this body even if a later
                    // directory read refers to the dropped collection. Check
                    // the surviving directories before returning that error.
                    Err(
                        TransError::StaleCollection
                        | TransError::Storage(StorageError::StaleCollection),
                    ) => {
                        dropped.insert(parent.clone());
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                records.insert(parent.clone(), record);
            }
        }

        for read in reads {
            let Some(record) = records.get(&read.parent) else {
                continue;
            };
            let valid = match &read.kind {
                DirectoryReadKind::Entry { name, collection } => record.child(name) == *collection,
                DirectoryReadKind::Listing { generation } => {
                    record.directory_generation() == *generation
                }
            };
            if !valid {
                return Ok(false);
            }
        }
        for change in changes {
            if let Some(record) = records.get(&change.parent)
                && record.child(&change.name) != change.expected
            {
                return Ok(false);
            }
        }
        if !dropped.is_empty() {
            return Err(TransError::StaleCollection);
        }
        for change in changes {
            let record = records
                .get_mut(&change.parent)
                .expect("every changed directory was loaded above");
            match change.op {
                CollectionOp::Create => {
                    if !record.add_child(change.name.clone(), change.collection.id())? {
                        return Ok(false);
                    }
                }
                CollectionOp::Drop => {
                    if record.remove_child(&change.name) != Some(change.collection.id()) {
                        return Ok(false);
                    }
                }
            }
        }
        for parent in changes.iter().map(|change| &change.parent) {
            if !Self::directory_fits(&records[parent], split_policy) {
                return Err(TransError::InvalidInput(
                    "subcollection directory exceeds the collection-record size limit".into(),
                ));
            }
        }
        Ok(true)
    }

    fn directory_fits(record: &CollectionRecord, policy: &SplitPolicy) -> bool {
        record.content_encoded_len() <= policy.content_limit()
            && record.encoded_len() <= policy.node_max_bytes()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use glassdb_backend::memory::MemoryBackend;
    use glassdb_concurr::{Background, RetryConfig};
    use glassdb_data::{CollectionId, DbPrefix};
    use glassdb_storage::transaction::{
        TxCollectionChange, TxCollectionOp, TxCommitStatus, TxLock, TxRecord, TxRecordStore,
    };
    use glassdb_storage::{CachedStore, CollectionStore, LockType, Timeline};

    use super::*;
    use crate::monitor::Monitor;

    fn new_catalog() -> (CollectionCatalog, CollectionStore, Monitor, Arc<Background>) {
        let timeline = Timeline::new();
        let objects = CachedStore::new(
            Arc::new(MemoryBackend::new()),
            1 << 20,
            timeline.clone(),
            None,
        );
        let records = CollectionStore::new(objects.clone());
        let background = Arc::new(Background::new());
        let transactions = TxRecordStore::new(objects, DbPrefix::try_from("db").unwrap());
        let monitor = Monitor::with_config(
            transactions.clone(),
            timeline.clone(),
            Arc::downgrade(&background),
            RetryConfig::default(),
            crate::monitor::ProtocolTiming::default(),
        );
        let state = CollectionStateResolver::new(
            records.clone(),
            transactions,
            timeline,
            monitor.clone(),
            RetryConfig::default(),
        );
        let catalog = CollectionCatalog::new(state);
        (catalog, records, monitor, background)
    }

    #[tokio::test]
    async fn snapshot_helps_a_committed_directory_holder() {
        let (catalog, records, monitor, _background) = new_catalog();
        let parent = CollectionAddress::root("db");
        let child = CollectionAddress::new(
            "db",
            CollectionId::from_slice(&[1; 16]).expect("fixed ID has the required width"),
        );
        let id = TxId::from_bytes(vec![1]);
        let mut record = CollectionRecord::new();
        record.set_directory_writer(id.clone());
        assert!(records.create_record(&parent, &record).await.unwrap());
        monitor.begin_tx(&id);
        let mut tx_record = TxRecord::new(id.clone(), TxCommitStatus::Ok);
        tx_record.locks.push(TxLock::Directory {
            collection: parent.clone(),
            typ: LockType::Write,
        });
        tx_record.collection_changes.push(TxCollectionChange {
            parent: parent.clone(),
            name: b"child".to_vec(),
            collection: child.clone(),
            op: TxCollectionOp::Create,
        });
        monitor.commit_tx(tx_record).await.unwrap();

        let snapshot = catalog.snapshot(&parent).await.unwrap();

        assert_eq!(snapshot.children, vec![(b"child".to_vec(), child.id())]);
        let (record, _) = records
            .load_record(&parent, Requirement::ANY)
            .await
            .unwrap();
        assert!(!record.directory_lock().contains(&id));
    }
}
