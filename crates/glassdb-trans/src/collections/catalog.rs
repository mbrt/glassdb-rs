//! Transactional coordination for collection name-to-ID directories.

use std::collections::BTreeMap;

use glassdb_concurr::{RetryConfig, rt};
use glassdb_data::{CollectionAddress, TxId};
use glassdb_storage::{
    CollectionRoot, IndexNode, LeafObservation, LockType, Node, Requirement, ShardStore,
    SplitPolicy, StorageError, TxCollectionOp, TxLock,
};

use super::{CollectionChange, CollectionOp, DirectoryRead, DirectoryReadKind, DirectorySnapshot};
use crate::error::TransError;
use crate::monitor::{Monitor, TxFinalStatus};
use crate::wound_wait::resolve_tx_conflict;

/// Accesses and coordinates transactional collection directories.
#[derive(Clone)]
pub struct CollectionCatalog {
    shards: ShardStore,
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
    pub fn new(shards: ShardStore, monitor: Monitor, retry: RetryConfig) -> Self {
        Self {
            shards,
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
                DirectoryReadKind::Listing { children } => root
                    .children()
                    .map(|(name, id)| (name.to_vec(), id))
                    .eq(children.iter().cloned()),
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
            self.write_back_one(parent, id, Some(changes)).await?;
        }
        Ok(())
    }

    /// Finishes committed directory effects from their durable transaction log.
    pub(crate) async fn recover_write_back(
        &self,
        id: &TxId,
        locks: &[TxLock],
    ) -> Result<(), TransError> {
        for parent in locks.iter().filter_map(|lock| match lock {
            TxLock::Directory { collection, .. } => Some(collection),
            _ => None,
        }) {
            self.write_back_one(parent, id, None).await?;
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
            let (mut root, observed) = match self.shards.load_root(&prefix, requirement).await {
                Ok(root) => root,
                Err(StorageError::NotFound) if !parent.id().is_root() => {
                    return Err(TransError::StaleCollection);
                }
                Err(error) => return Err(error.into()),
            };
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
                    self.write_back_one(parent, &holder, None).await?;
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
            if conflicts {
                let holder = lock
                    .holders()
                    .iter()
                    .find(|holder| *holder != id)
                    .cloned()
                    .ok_or_else(|| TransError::other("directory lock cannot be upgraded"))?;
                resolve_tx_conflict(&self.monitor, id, &holder).await?;
                rt::sleep(backoff.next_delay()).await;
                continue;
            }
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
        local_changes: Option<&[CollectionChange]>,
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
            let owned_changes;
            let changes = match local_changes {
                Some(changes) => changes,
                None => {
                    let log = self
                        .monitor
                        .transaction_log_at(id, Requirement::Any)
                        .await?;
                    owned_changes = log
                        .collection_changes
                        .into_iter()
                        .filter(|change| &change.parent == parent)
                        .map(|change| CollectionChange {
                            parent: change.parent,
                            name: change.name,
                            collection: change.collection,
                            expected: None,
                            op: match change.op {
                                TxCollectionOp::Create => CollectionOp::Create,
                                TxCollectionOp::Drop => CollectionOp::Drop,
                            },
                        })
                        .collect::<Vec<_>>();
                    &owned_changes
                }
            };
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
}
