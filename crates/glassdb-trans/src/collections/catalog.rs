//! Transactional coordination for collection name-to-ID directories.

use std::collections::BTreeMap;

use glassdb_concurr::{RetryConfig, rt};
use glassdb_data::{CollectionAddress, TxId};
use glassdb_storage::{
    CollectionRoot, IndexNode, LeafObservation, LockType, Node, Requirement, ShardStore,
    SplitPolicy, StorageError, TLogger, TxCollectionChange, TxCollectionOp, TxCommitStatus, TxLock,
};

use super::{CollectionChange, CollectionOp, DirectoryRead, DirectoryReadKind, DirectorySnapshot};
use crate::error::TransError;
use crate::monitor::{Monitor, TxFinalStatus};
use crate::wound_wait::resolve_tx_conflict;

/// Accesses and coordinates transactional collection directories.
#[derive(Clone)]
pub struct CollectionCatalog {
    shards: ShardStore,
    transactions: TLogger,
    monitor: Monitor,
    retry: RetryConfig,
}

fn directory_fits(root: &CollectionRoot, policy: &SplitPolicy) -> bool {
    let content_limit = policy
        .node_max_bytes
        .saturating_sub(policy.split_headroom_bytes);
    if root.content_encoded_len() > content_limit || root.encoded_len() > policy.node_max_bytes {
        return false;
    }
    let mut index_root = root.clone();
    index_root.set_node(Node::index(IndexNode::from_children([
        (Vec::new(), "x".repeat(24)),
        (vec![0], "y".repeat(24)),
    ])));
    index_root.content_encoded_len() <= content_limit
        && index_root.encoded_len() <= policy.node_max_bytes
}

impl CollectionCatalog {
    /// Creates access to transactional collection directories.
    pub fn new(
        shards: ShardStore,
        transactions: TLogger,
        monitor: Monitor,
        retry: RetryConfig,
    ) -> Self {
        Self {
            shards,
            transactions,
            monitor,
            retry,
        }
    }

    /// Loads a direct-child directory after resolving finalized transactions.
    pub async fn snapshot(
        &self,
        parent: &CollectionAddress,
    ) -> Result<DirectorySnapshot, TransError> {
        let (root, _) = self
            .load_resolved_root(parent, None, Requirement::Any)
            .await?;
        Ok(DirectorySnapshot {
            children: root
                .children()
                .map(|(name, id)| (name.to_vec(), id))
                .collect(),
            version: root.directory_version(),
        })
    }

    /// Acquires root-wide directory locks in physical path order.
    pub(crate) async fn lock_directories(
        &self,
        id: &TxId,
        reads: &[DirectoryRead],
        changes: &[CollectionChange],
    ) -> Result<Vec<TxLock>, TransError> {
        let mut desired = BTreeMap::<CollectionAddress, LockType>::new();
        for read in reads {
            desired.entry(read.parent.clone()).or_insert(LockType::Read);
        }
        for change in changes {
            desired.insert(change.parent.clone(), LockType::Write);
        }

        let mut locks = Vec::with_capacity(desired.len());
        for (parent, typ) in desired {
            self.acquire_directory_lock(&parent, id, typ).await?;
            locks.push(TxLock::Directory {
                collection: parent,
                typ,
            });
        }
        Ok(locks)
    }

    /// Validates logical directory reads and mutation preconditions.
    pub(crate) async fn validate(
        &self,
        id: Option<&TxId>,
        reads: &[DirectoryRead],
        changes: &[CollectionChange],
        requirement: Requirement,
        split_policy: &SplitPolicy,
    ) -> Result<bool, TransError> {
        let mut roots = BTreeMap::<CollectionAddress, CollectionRoot>::new();
        for parent in reads
            .iter()
            .map(|read| &read.parent)
            .chain(changes.iter().map(|change| &change.parent))
        {
            if !roots.contains_key(parent) {
                let (root, _) = self.load_resolved_root(parent, id, requirement).await?;
                roots.insert(parent.clone(), root);
            }
        }

        for read in reads {
            let root = &roots[&read.parent];
            let valid = match &read.kind {
                DirectoryReadKind::Entry { name, collection } => root.child(name) == *collection,
                DirectoryReadKind::Listing { version } => root.directory_version() == *version,
            };
            if !valid {
                return Ok(false);
            }
        }
        for change in changes {
            if roots[&change.parent].child(&change.name) != change.expected {
                return Ok(false);
            }
        }
        for change in changes {
            let root = roots
                .get_mut(&change.parent)
                .expect("every changed directory was loaded above");
            match change.op {
                CollectionOp::Create => {
                    if !root.add_child(change.name.clone(), change.collection.id())? {
                        return Ok(false);
                    }
                }
                CollectionOp::Drop => {
                    if root.remove_child(&change.name) != Some(change.collection.id()) {
                        return Ok(false);
                    }
                }
            }
        }
        for parent in changes.iter().map(|change| &change.parent) {
            if !directory_fits(&roots[parent], split_policy) {
                return Err(TransError::InvalidInput(
                    "subcollection directory exceeds the node size limit".into(),
                ));
            }
        }
        Ok(true)
    }

    /// Applies committed directory effects and releases their root locks.
    pub(crate) async fn write_back(
        &self,
        id: &TxId,
        changes: &[CollectionChange],
        locks: &[TxLock],
    ) -> Result<(), TransError> {
        for parent in locks.iter().filter_map(|lock| match lock {
            TxLock::Directory { collection, .. } => Some(collection),
            _ => None,
        }) {
            self.write_back_one(parent, id, changes).await?;
        }
        Ok(())
    }

    /// Finishes committed directory effects from durable transaction metadata.
    pub(crate) async fn recover_write_back(
        &self,
        id: &TxId,
        changes: &[TxCollectionChange],
        locks: &[TxLock],
    ) -> Result<(), TransError> {
        let changes = recover_collection_changes(changes);
        for parent in locks.iter().filter_map(|lock| match lock {
            TxLock::Directory { collection, .. } => Some(collection),
            _ => None,
        }) {
            self.write_back_one(parent, id, &changes).await?;
        }
        Ok(())
    }

    /// Releases directory locks before a body-level validation retry.
    pub(crate) async fn release_locks(&self, id: &TxId, locks: &[TxLock]) {
        for parent in locks.iter().filter_map(|lock| match lock {
            TxLock::Directory { collection, .. } => Some(collection),
            _ => None,
        }) {
            let prefix = parent.physical_prefix();
            loop {
                let Ok((mut root, observed)) =
                    self.shards.load_root(&prefix, Requirement::Any).await
                else {
                    break;
                };
                if !root.remove_directory_holder(id) {
                    break;
                }
                match self.shards.store_root(&prefix, &root, &observed).await {
                    Ok(true) => break,
                    Ok(false) => {}
                    Err(_) => break,
                }
            }
        }
    }

    async fn load_resolved_root(
        &self,
        parent: &CollectionAddress,
        own: Option<&TxId>,
        requirement: Requirement,
    ) -> Result<(CollectionRoot, LeafObservation), TransError> {
        let prefix = parent.physical_prefix();
        let mut backoff = self.retry.backoff();
        loop {
            let (mut root, observed) = self
                .shards
                .load_root(&prefix, requirement)
                .await
                .map_err(|error| error.classify_collection_absence(parent))?;
            if let Some(holder) = root.node().collection_delete_intent().cloned()
                && Some(&holder) != own
            {
                match self.monitor.await_tx_final(&holder).await? {
                    TxFinalStatus::Committed => return Err(TransError::StaleCollection),
                    TxFinalStatus::Aborted => {
                        root.node_locks_mut().remove_delete_intent(&holder);
                        if self.shards.store_root(&prefix, &root, &observed).await? {
                            continue;
                        }
                    }
                }
                rt::sleep(backoff.next_delay()).await;
                continue;
            }

            let holder = root
                .directory_lock()
                .holders()
                .iter()
                .find(|holder| Some(*holder) != own)
                .cloned();
            let Some(holder) = holder else {
                return Ok((root, observed));
            };
            match self.monitor.await_tx_final(&holder).await? {
                TxFinalStatus::Committed => {
                    self.help_committed_write_back(parent, &holder).await?;
                }
                TxFinalStatus::Aborted => {
                    root.remove_directory_holder(&holder);
                    if !self.shards.store_root(&prefix, &root, &observed).await? {
                        rt::sleep(backoff.next_delay()).await;
                    }
                }
            }
        }
    }

    async fn acquire_directory_lock(
        &self,
        parent: &CollectionAddress,
        id: &TxId,
        desired: LockType,
    ) -> Result<(), TransError> {
        let prefix = parent.physical_prefix();
        let mut backoff = self.retry.backoff();
        loop {
            let (mut root, observed) = self
                .load_resolved_root(parent, Some(id), Requirement::Any)
                .await?;
            let lock = root.directory_lock();
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
            // If the request still conflicts here, wound-wait has removed every
            // foreign reader, so our shared lock can be replaced atomically.
            match desired {
                LockType::Read => root.add_directory_reader(id.clone()),
                LockType::Write => root.set_directory_writer(id.clone()),
                _ => {
                    return Err(TransError::other(
                        "invalid collection-directory lock request",
                    ));
                }
            }
            if self.shards.store_root(&prefix, &root, &observed).await? {
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
            let (mut root, observed) = match self.shards.load_root(&prefix, Requirement::Any).await
            {
                Ok(root) => root,
                // A prior cleanup pass may already have reclaimed a parent
                // dropped by the same committed transaction.
                Err(StorageError::NotFound) => return Ok(()),
                Err(error) => return Err(error.into()),
            };
            if !root.directory_lock().contains(id) {
                return Ok(());
            }
            let mut changed = false;
            for change in changes.iter().filter(|change| &change.parent == parent) {
                match change.op {
                    CollectionOp::Create => match root.child(&change.name) {
                        None => {
                            root.add_child(change.name.clone(), change.collection.id())?;
                            changed = true;
                        }
                        Some(id) if id == change.collection.id() => {}
                        Some(_) => {
                            return Err(TransError::other(
                                "committed collection create conflicts with a newer binding",
                            ));
                        }
                    },
                    CollectionOp::Drop => match root.child(&change.name) {
                        Some(id) if id == change.collection.id() => {
                            root.remove_child(&change.name);
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
                root.advance_directory_version();
            }
            root.remove_directory_holder(id);
            if self.shards.store_root(&prefix, &root, &observed).await? {
                return Ok(());
            }
            rt::sleep(backoff.next_delay()).await;
        }
    }

    /// Finishes a committed peer's directory effects from its durable log.
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use glassdb_backend::memory::MemoryBackend;
    use glassdb_concurr::Background;
    use glassdb_data::CollectionId;
    use glassdb_storage::{CachedStore, TLogger, Timeline, TxLog};

    use super::*;

    fn new_catalog() -> (CollectionCatalog, ShardStore, Monitor, Arc<Background>) {
        let timeline = Timeline::new();
        let objects = CachedStore::new(
            Arc::new(MemoryBackend::new()),
            1 << 20,
            timeline.clone(),
            None,
        );
        let shards = ShardStore::new(objects.clone());
        let background = Arc::new(Background::new());
        let transactions = TLogger::new(objects, "db");
        let monitor = Monitor::new(transactions.clone(), timeline, Arc::downgrade(&background));
        let catalog = CollectionCatalog::new(
            shards.clone(),
            transactions,
            monitor.clone(),
            RetryConfig::default(),
        );
        (catalog, shards, monitor, background)
    }

    #[tokio::test]
    async fn sole_directory_reader_can_upgrade_to_writer() {
        let (catalog, shards, _monitor, _background) = new_catalog();
        let parent = CollectionAddress::root("db");
        let prefix = parent.physical_prefix();
        assert!(
            shards
                .create_root(&prefix, &CollectionRoot::new())
                .await
                .unwrap()
        );
        let id = TxId::from_bytes(vec![1]);

        catalog
            .acquire_directory_lock(&parent, &id, LockType::Read)
            .await
            .unwrap();
        catalog
            .acquire_directory_lock(&parent, &id, LockType::Write)
            .await
            .unwrap();

        let (root, _) = shards.load_root(&prefix, Requirement::Any).await.unwrap();
        assert_eq!(root.directory_lock().lock_type(), LockType::Write);
        assert_eq!(root.directory_lock().holders(), std::slice::from_ref(&id));
    }

    #[tokio::test]
    async fn snapshot_helps_a_committed_directory_holder() {
        let (catalog, shards, monitor, _background) = new_catalog();
        let parent = CollectionAddress::root("db");
        let child = CollectionAddress::new(
            "db",
            CollectionId::from_slice(&[1; 16]).expect("fixed ID has the required width"),
        );
        let id = TxId::from_bytes(vec![1]);
        let mut root = CollectionRoot::new();
        root.set_directory_writer(id.clone());
        assert!(
            shards
                .create_root(&parent.physical_prefix(), &root)
                .await
                .unwrap()
        );
        monitor.begin_tx(&id);
        let mut log = TxLog::new(id.clone(), TxCommitStatus::Ok);
        log.locks.push(TxLock::Directory {
            collection: parent.clone(),
            typ: LockType::Write,
        });
        log.collection_changes.push(TxCollectionChange {
            parent: parent.clone(),
            name: b"child".to_vec(),
            collection: child.clone(),
            op: TxCollectionOp::Create,
        });
        monitor.commit_tx(log).await.unwrap();

        let snapshot = catalog.snapshot(&parent).await.unwrap();

        assert_eq!(snapshot.children, vec![(b"child".to_vec(), child.id())]);
        let (root, _) = shards
            .load_root(&parent.physical_prefix(), Requirement::Any)
            .await
            .unwrap();
        assert!(!root.directory_lock().contains(&id));
    }
}
