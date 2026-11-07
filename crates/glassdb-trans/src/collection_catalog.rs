//! Logical snapshots and validation for collection name-to-ID directories.

use std::collections::{BTreeMap, BTreeSet};

use glassdb_data::{CollectionAddress, CollectionId, TxId};
use glassdb_storage::{
    CollectionRecord, CurrentnessBarrier, NodeSizePolicy, Requirement, StorageError,
};

use crate::collection_coordination::{CollectionStateResolver, PendingHolders};
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

    /// Loads a direct-child directory for a transaction body of the identity
    /// `own`.
    ///
    /// The load does not wait for a pending holder or a pending topology
    /// freeze, and it ignores the holds of `own`. An identity keeps its
    /// directory locks for a body replay, so a wait there could close a wait
    /// cycle (ADR-090). A pending writer has not changed the record yet, so the
    /// snapshot shows the bindings from before its change. A pending drop has
    /// not removed the collection yet. The pending changes of `own` are not in
    /// the record either.
    pub(crate) async fn snapshot(
        &self,
        parent: &CollectionAddress,
        own: &TxId,
    ) -> Result<DirectorySnapshot, TransError> {
        // Callers retain directory reads and mutation preconditions as semantic
        // dependencies. Validation checks them after the body before accepting
        // its outcome, so this snapshot can use cached contents.
        let (record, _) = self
            .state
            .load_resolved(parent, Some(own), Requirement::ANY, PendingHolders::Skip)
            .await?;
        Ok(DirectorySnapshot {
            children: record
                .children()
                .map(|(name, id)| (name.clone(), id))
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
        node_size_policy: &NodeSizePolicy,
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
                    .load_resolved(
                        parent,
                        id,
                        Requirement::after(barrier),
                        PendingHolders::Wait,
                    )
                    .await
                {
                    Ok((record, _)) => record,
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
            // A committed drop removes the binding of its collection and
            // advances the generation of its parent. The records do not
            // resolve atomically, so a parent's record can predate a drop that
            // the resolution of the child found. Such a record cannot prove
            // the reads of the bindings that the drop removed.
            let is_dropped = |child: CollectionId| {
                dropped.contains(&CollectionAddress::from_db_prefix(
                    read.parent.db_prefix_component().clone(),
                    child,
                ))
            };
            let record = records.get(&read.parent);
            let valid = match &read.kind {
                DirectoryReadKind::Entry { name, collection } => {
                    !collection.is_some_and(is_dropped)
                        && record.is_none_or(|record| record.child(name) == *collection)
                }
                DirectoryReadKind::Listing { generation } => record.is_none_or(|record| {
                    record.directory_generation() == *generation
                        && !record.children().any(|(_, child)| is_dropped(child))
                }),
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
            if !Self::directory_fits(&records[parent], node_size_policy) {
                return Err(TransError::InvalidInput(
                    "subcollection directory exceeds the collection-record size limit".into(),
                ));
            }
        }
        Ok(true)
    }

    fn directory_fits(record: &CollectionRecord, policy: &NodeSizePolicy) -> bool {
        record.content_encoded_len() <= policy.content_limit()
            && record.encoded_len() <= policy.node_max_bytes()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use glassdb_backend::memory::MemoryBackend;
    use glassdb_concurr::{Background, RetrySchedule};
    use glassdb_data::{CollectionName, DbPrefix};
    use glassdb_storage::transaction::{
        TxCollectionChange, TxCollectionOp, TxCommitStatus, TxLock, TxRecord, TxRecordStore,
    };
    use glassdb_storage::{CachedStore, CollectionStore, LockType, Timeline};

    use super::*;
    use crate::monitor::Monitor;

    fn new_catalog() -> (
        CollectionCatalog,
        CollectionStore,
        Monitor,
        Timeline,
        Arc<Background>,
    ) {
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
            RetrySchedule::default(),
            crate::monitor::ProtocolTiming::default(),
        );
        let state = CollectionStateResolver::new(
            records.clone(),
            transactions,
            timeline.clone(),
            monitor.clone(),
            RetrySchedule::default(),
        );
        let catalog = CollectionCatalog::new(state);
        (catalog, records, monitor, timeline, background)
    }

    #[tokio::test]
    async fn snapshot_helps_a_committed_directory_holder() {
        let (catalog, records, monitor, _timeline, _background) = new_catalog();
        let parent = CollectionAddress::root("db");
        let child = CollectionAddress::new("db", CollectionId::from_bytes([1; 16]));
        let id = TxId::with_priority(0, &[1]);
        let mut record = CollectionRecord::new();
        record.set_directory_writer(id);
        assert!(records.create_record(&parent, &record).await.unwrap());
        monitor.begin_tx(&id);
        let mut tx_record = TxRecord::new(id, TxCommitStatus::Committed);
        tx_record.locks.push(TxLock::Directory {
            collection: parent.clone(),
            typ: LockType::Write,
        });
        let name = CollectionName::new("child").unwrap();
        tx_record.collection_changes.push(TxCollectionChange {
            parent: parent.clone(),
            name: name.clone(),
            collection: child.clone(),
            op: TxCollectionOp::Create,
        });
        monitor.commit_tx(tx_record).await.unwrap();

        let snapshot = catalog
            .snapshot(&parent, &TxId::with_priority(0, b"reader"))
            .await
            .unwrap();

        assert_eq!(snapshot.children, vec![(name, child.id())]);
        let (record, _) = records
            .load_record(&parent, Requirement::ANY)
            .await
            .unwrap();
        assert!(!record.directory_lock().contains(&id));
    }

    // A body load does not wait for a pending writer (ADR-090). The body can
    // replay while its identity keeps locks, so a wait here could close a wait
    // cycle with a writer that waits for those locks.
    #[tokio::test]
    async fn snapshot_does_not_wait_for_a_pending_directory_writer() {
        let (catalog, records, monitor, _timeline, _background) = new_catalog();
        let parent = CollectionAddress::root("db");
        let child = CollectionAddress::new("db", CollectionId::from_bytes([1; 16]));
        let name = CollectionName::new("child").unwrap();
        let writer = TxId::with_priority(0, &[1]);
        let mut record = CollectionRecord::new();
        assert!(record.add_child(name.clone(), child.id()).unwrap());
        record.set_directory_writer(writer);
        assert!(records.create_record(&parent, &record).await.unwrap());
        monitor.begin_tx(&writer);

        let snapshot = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            catalog.snapshot(&parent, &TxId::with_priority(0, b"reader")),
        )
        .await
        .expect("the snapshot waited for the pending writer")
        .unwrap();

        assert_eq!(snapshot.children, vec![(name, child.id())]);
    }

    // A body load does not wait for a pending drop either (ADR-090).
    // Validation waits for it, and finds the drop if it commits.
    #[tokio::test]
    async fn snapshot_does_not_wait_for_a_pending_topology_freeze() {
        let (catalog, records, monitor, _timeline, _background) = new_catalog();
        let parent = CollectionAddress::new("db", CollectionId::from_bytes([3; 16]));
        let child = CollectionAddress::new("db", CollectionId::from_bytes([1; 16]));
        let name = CollectionName::new("child").unwrap();
        let dropper = TxId::with_priority(0, &[1]);
        let mut record = CollectionRecord::new();
        assert!(record.add_child(name.clone(), child.id()).unwrap());
        assert!(record.set_topology_freeze(dropper));
        assert!(records.create_record(&parent, &record).await.unwrap());
        monitor.begin_tx(&dropper);

        let snapshot = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            catalog.snapshot(&parent, &TxId::with_priority(0, b"reader")),
        )
        .await
        .expect("the snapshot waited for the pending drop")
        .unwrap();

        assert_eq!(snapshot.children, vec![(name, child.id())]);
    }

    /// Stores a parent whose record still binds `name` to `child` after a
    /// committed transaction dropped `child`. A validation sees this state when
    /// it resolves the parent before the drop and the child after it.
    async fn bind_dropped_child(
        records: &CollectionStore,
        monitor: &Monitor,
        parent: &CollectionAddress,
        name: &CollectionName,
        child: &CollectionAddress,
    ) -> u64 {
        let mut parent_record = CollectionRecord::new();
        assert!(parent_record.add_child(name.clone(), child.id()).unwrap());
        assert!(records.create_record(parent, &parent_record).await.unwrap());
        let dropper = TxId::with_priority(0, &[2]);
        let mut child_record = CollectionRecord::new();
        assert!(child_record.set_topology_freeze(dropper));
        assert!(records.create_record(child, &child_record).await.unwrap());
        monitor.begin_tx(&dropper);
        let mut tx_record = TxRecord::new(dropper, TxCommitStatus::Committed);
        tx_record.collection_changes.push(TxCollectionChange {
            parent: parent.clone(),
            name: name.clone(),
            collection: child.clone(),
            op: TxCollectionOp::Drop,
        });
        monitor.commit_tx(tx_record).await.unwrap();
        parent_record.directory_generation()
    }

    async fn validate_reads(reads: impl FnOnce(u64) -> Vec<DirectoryRead>) -> bool {
        let (catalog, records, monitor, timeline, _background) = new_catalog();
        let parent = CollectionAddress::root("db");
        let child = CollectionAddress::new("db", CollectionId::from_bytes([1; 16]));
        let name = CollectionName::new("child").unwrap();
        let generation = bind_dropped_child(&records, &monitor, &parent, &name, &child).await;
        let mut reads = reads(generation);
        // The body used the child, so its directory is a dependency too.
        reads.push(DirectoryRead {
            parent: child,
            kind: DirectoryReadKind::Listing { generation: 0 },
        });
        catalog
            .validate(
                None,
                &reads,
                &[],
                timeline.currentness_barrier(),
                &NodeSizePolicy::default(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn an_entry_of_a_dropped_collection_is_invalid() {
        let valid = validate_reads(|_| {
            vec![DirectoryRead {
                parent: CollectionAddress::root("db"),
                kind: DirectoryReadKind::Entry {
                    name: CollectionName::new("child").unwrap(),
                    collection: Some(CollectionId::from_bytes([1; 16])),
                },
            }]
        })
        .await;

        assert!(!valid);
    }

    #[tokio::test]
    async fn a_listing_that_binds_a_dropped_collection_is_invalid() {
        let valid = validate_reads(|generation| {
            vec![DirectoryRead {
                parent: CollectionAddress::root("db"),
                kind: DirectoryReadKind::Listing { generation },
            }]
        })
        .await;

        assert!(!valid);
    }
}
