//! Transactional coordination for collection directories and lifecycle fences.

use std::collections::{BTreeMap, BTreeSet};

use glassdb_concurr::{RetryConfig, rt};
use glassdb_data::{CollectionAddress, CollectionId, TxId};
use glassdb_storage::{
    CollectionRoot, IndexNode, LeafObservation, LockType, Node, Requirement, ShardStore,
    SplitPolicy, StorageError, TxCollectionOp, TxCommitStatus, TxLock,
};

use crate::error::TransError;
use crate::monitor::Monitor;
use crate::node_locking::{Reclaim, try_reclaim};
use crate::split::Splitter;

/// One directory dependency observed by a transaction body.
#[derive(Debug, Clone)]
pub struct DirectoryRead {
    pub parent: CollectionAddress,
    pub kind: DirectoryReadKind,
}

/// The logical portion of a child directory that was observed.
#[derive(Debug, Clone)]
pub enum DirectoryReadKind {
    Entry {
        name: Vec<u8>,
        collection: Option<CollectionId>,
    },
    Listing {
        children: Vec<(Vec<u8>, CollectionId)>,
    },
}

/// One staged direct-child binding mutation.
#[derive(Debug, Clone)]
pub struct CollectionChange {
    pub parent: CollectionAddress,
    pub name: Vec<u8>,
    pub collection: CollectionAddress,
    pub expected: Option<CollectionId>,
    pub op: CollectionOp,
}

/// The staged effect on a direct-child binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectionOp {
    Create,
    Drop,
}

/// Collection-management accesses carried beside ordinary key accesses.
#[derive(Debug, Clone, Default)]
pub struct CollectionData {
    pub reads: Vec<DirectoryRead>,
    pub changes: Vec<CollectionChange>,
}

impl CollectionData {
    /// Reports whether the transaction changes a collection binding.
    pub fn has_writes(&self) -> bool {
        !self.changes.is_empty()
    }

    /// Retains only directory observations used to validate a user error.
    pub fn into_read_only(mut self) -> Self {
        self.changes.clear();
        self
    }
}

/// A resolved, transactionally clean view of one direct-child directory.
#[derive(Debug, Clone)]
pub struct DirectorySnapshot {
    pub children: Vec<(Vec<u8>, CollectionId)>,
}

/// Coordinates collection roots with ordinary transaction status objects.
#[derive(Clone)]
pub struct CollectionManager {
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

impl CollectionManager {
    /// Creates collection lifecycle coordination over the shared stores.
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

    /// Creates every fresh, still-undiscoverable collection root.
    pub(crate) async fn prepare_roots(
        &self,
        changes: &[CollectionChange],
    ) -> Result<(), TransError> {
        for change in changes
            .iter()
            .filter(|change| change.op == CollectionOp::Create)
        {
            let prefix = change.collection.physical_prefix();
            if !self
                .shards
                .create_root(&prefix, &CollectionRoot::new())
                .await?
            {
                self.shards
                    .load_root(&prefix, Requirement::Any)
                    .await
                    .map_err(TransError::from)?;
            }
        }
        Ok(())
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

    /// Installs delete intents on all nodes of every staged drop.
    pub(crate) async fn fence_drops(
        &self,
        id: &TxId,
        changes: &[CollectionChange],
        splitter: &Splitter,
    ) -> Result<(), TransError> {
        for collection in changes
            .iter()
            .filter(|change| change.op == CollectionOp::Drop)
            .map(|change| &change.collection)
        {
            self.freeze_topology(collection, id, splitter).await?;
            let prefix = collection.physical_prefix();
            let nodes = self.shards.list_nodes(&prefix, Requirement::Any).await?;
            for (token, _) in nodes {
                self.fence_node(&prefix, &token, id).await?;
            }
            self.fence_root(collection, id).await?;
        }
        Ok(())
    }

    /// Clears an abandoned transaction's delete preparation.
    pub(crate) async fn clear_aborted_drops(
        &self,
        id: &TxId,
        collections: &[CollectionAddress],
    ) -> Result<(), TransError> {
        for collection in collections {
            let prefix = collection.physical_prefix();
            let nodes = self.shards.list_nodes(&prefix, Requirement::Any).await?;
            for (token, _) in nodes {
                self.clear_node_fence(&prefix, &token, id).await?;
            }
            self.clear_root_fence(collection, id).await?;
        }
        Ok(())
    }

    /// Reclaims physical objects for collections no longer discoverable.
    pub(crate) async fn reclaim(
        &self,
        collections: &[CollectionAddress],
    ) -> Result<(), TransError> {
        for collection in collections {
            let prefix = collection.physical_prefix();
            let nodes = self.shards.list_nodes(&prefix, Requirement::Any).await?;
            for (_, observed) in nodes {
                self.shards.delete_node(&observed).await?;
            }
            let LeafObservation::Root(observed) = self
                .shards
                .load_root_state(&prefix, Requirement::Any)
                .await?
            else {
                return Err(TransError::other(
                    "collection root lookup returned a standalone node",
                ));
            };
            if observed.exists() {
                self.shards.delete_root(&observed).await?;
            }
        }
        Ok(())
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
                match self.monitor.tx_status(&holder).await? {
                    TxCommitStatus::Ok => return Err(TransError::StaleCollection),
                    TxCommitStatus::Aborted => {
                        root.node_locks_mut().remove_delete_intent(&holder);
                        if self.shards.store_root(&prefix, &root, &observed).await? {
                            continue;
                        }
                    }
                    TxCommitStatus::Pending | TxCommitStatus::Unknown => {
                        self.monitor.wait_for_tx(&holder).await;
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
            match self.monitor.tx_status(&holder).await? {
                TxCommitStatus::Pending | TxCommitStatus::Unknown => {
                    self.monitor.wait_for_tx(&holder).await;
                    rt::sleep(backoff.next_delay()).await;
                }
                TxCommitStatus::Ok => {
                    self.write_back_one(parent, &holder, None).await?;
                }
                TxCommitStatus::Aborted => {
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
                match self.monitor.tx_status(&holder).await? {
                    TxCommitStatus::Pending => {
                        if matches!(
                            try_reclaim(&self.monitor, id, &holder).await?,
                            Reclaim::Wait
                        ) {
                            self.monitor.wait_for_tx(&holder).await;
                        }
                    }
                    TxCommitStatus::Unknown => {
                        self.monitor.wait_for_tx(&holder).await;
                    }
                    TxCommitStatus::Ok | TxCommitStatus::Aborted => {}
                }
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

    async fn freeze_topology(
        &self,
        collection: &CollectionAddress,
        id: &TxId,
        splitter: &Splitter,
    ) -> Result<(), TransError> {
        let prefix = collection.physical_prefix();
        let mut backoff = self.retry.backoff();
        loop {
            let (mut root, observed) = self.shards.load_root(&prefix, Requirement::Any).await?;
            if root.topology_freeze() != Some(id)
                && let Some(holder) = root.topology_freeze().cloned()
            {
                match self.monitor.tx_status(&holder).await? {
                    TxCommitStatus::Ok => return Err(TransError::StaleCollection),
                    TxCommitStatus::Aborted => {
                        root.remove_topology_freeze(&holder);
                    }
                    TxCommitStatus::Pending => {
                        if matches!(
                            try_reclaim(&self.monitor, id, &holder).await?,
                            Reclaim::Wait
                        ) {
                            self.monitor.wait_for_tx(&holder).await;
                        }
                        continue;
                    }
                    TxCommitStatus::Unknown => {
                        self.monitor.wait_for_tx(&holder).await;
                        continue;
                    }
                }
            }
            if root.topology_freeze().is_none() {
                assert!(root.set_topology_freeze(id.clone()));
                if self.shards.store_root(&prefix, &root, &observed).await? {
                    continue;
                }
                rt::sleep(backoff.next_delay()).await;
                continue;
            }
            if root.topology_freeze() != Some(id) {
                continue;
            }
            if root.topology_participants().next().is_none() {
                return Ok(());
            }

            let participant = root
                .topology_participants()
                .next()
                .cloned()
                .expect("participant presence was checked above");
            match self.monitor.tx_status(&participant).await? {
                TxCommitStatus::Pending => {
                    if matches!(
                        try_reclaim(&self.monitor, id, &participant).await?,
                        Reclaim::Wait
                    ) {
                        self.monitor.wait_for_tx(&participant).await;
                    }
                }
                TxCommitStatus::Unknown => {
                    self.monitor.wait_for_tx(&participant).await;
                }
                TxCommitStatus::Ok | TxCommitStatus::Aborted => {
                    splitter
                        .settle_topology_participant(collection, &participant)
                        .await?;
                }
            }
            rt::sleep(backoff.next_delay()).await;
        }
    }

    async fn fence_node(&self, prefix: &str, token: &str, id: &TxId) -> Result<(), TransError> {
        let mut backoff = self.retry.backoff();
        loop {
            let (mut node, observed) =
                match self.shards.load_node(prefix, token, Requirement::Any).await {
                    Ok(node) => node,
                    Err(StorageError::NotFound) => return Ok(()),
                    Err(error) => return Err(error.into()),
                };
            if node.collection_delete_intent() == Some(id) {
                return Ok(());
            }
            if let Some(holder) = node.collection_delete_intent().cloned() {
                self.resolve_delete_holder(&holder, id).await?;
                continue;
            }
            if let Some(holder) = self.pending_node_holder(&node, id).await? {
                self.resolve_pending_holder(&holder, id).await?;
                continue;
            }
            node.set_collection_delete_intent(id.clone());
            if self
                .shards
                .store_node(prefix, token, &node, Some(&observed))
                .await?
            {
                return Ok(());
            }
            rt::sleep(backoff.next_delay()).await;
        }
    }

    async fn fence_root(
        &self,
        collection: &CollectionAddress,
        id: &TxId,
    ) -> Result<(), TransError> {
        let prefix = collection.physical_prefix();
        let mut backoff = self.retry.backoff();
        loop {
            let (mut root, observed) = self.shards.load_root(&prefix, Requirement::Any).await?;
            if root.node().collection_delete_intent() == Some(id) {
                return Ok(());
            }
            if let Some(holder) = root.node().collection_delete_intent().cloned() {
                self.resolve_delete_holder(&holder, id).await?;
                continue;
            }
            if let Some(holder) = self.pending_node_holder(root.node(), id).await? {
                self.resolve_pending_holder(&holder, id).await?;
                continue;
            }
            root.node_locks_mut().set_delete_intent(id.clone());
            if self.shards.store_root(&prefix, &root, &observed).await? {
                return Ok(());
            }
            rt::sleep(backoff.next_delay()).await;
        }
    }

    async fn pending_node_holder(
        &self,
        node: &Node,
        own: &TxId,
    ) -> Result<Option<TxId>, TransError> {
        let mut holders = BTreeSet::new();
        holders.extend(node.structural_gate().holders().iter().cloned());
        holders.extend(node.membership_lock().holders().iter().cloned());
        if let Some(leaf) = node.as_leaf() {
            for entry in leaf.entries() {
                holders.extend(entry.locked_by.iter().cloned());
            }
        }
        holders.remove(own);
        for holder in holders {
            if matches!(
                self.monitor.tx_status(&holder).await?,
                TxCommitStatus::Pending | TxCommitStatus::Unknown
            ) {
                return Ok(Some(holder));
            }
        }
        Ok(None)
    }

    async fn resolve_pending_holder(&self, holder: &TxId, id: &TxId) -> Result<(), TransError> {
        if matches!(try_reclaim(&self.monitor, id, holder).await?, Reclaim::Wait) {
            self.monitor.wait_for_tx(holder).await;
        }
        Ok(())
    }

    async fn resolve_delete_holder(&self, holder: &TxId, id: &TxId) -> Result<(), TransError> {
        match self.monitor.tx_status(holder).await? {
            TxCommitStatus::Ok => Err(TransError::StaleCollection),
            TxCommitStatus::Aborted => Ok(()),
            TxCommitStatus::Pending => self.resolve_pending_holder(holder, id).await,
            TxCommitStatus::Unknown => {
                self.monitor.wait_for_tx(holder).await;
                Ok(())
            }
        }
    }

    async fn clear_node_fence(
        &self,
        prefix: &str,
        token: &str,
        id: &TxId,
    ) -> Result<(), TransError> {
        loop {
            let (mut node, observed) = self
                .shards
                .load_node(prefix, token, Requirement::Any)
                .await?;
            if !node.remove_collection_delete_intent(id) {
                return Ok(());
            }
            if self
                .shards
                .store_node(prefix, token, &node, Some(&observed))
                .await?
            {
                return Ok(());
            }
        }
    }

    async fn clear_root_fence(
        &self,
        collection: &CollectionAddress,
        id: &TxId,
    ) -> Result<(), TransError> {
        let prefix = collection.physical_prefix();
        loop {
            let (mut root, observed) = match self.shards.load_root(&prefix, Requirement::Any).await
            {
                Ok(root) => root,
                Err(StorageError::NotFound) => return Ok(()),
                Err(error) => return Err(error.into()),
            };
            let changed =
                root.node_locks_mut().remove_delete_intent(id) | root.remove_topology_freeze(id);
            if !changed {
                return Ok(());
            }
            if self.shards.store_root(&prefix, &root, &observed).await? {
                return Ok(());
            }
        }
    }
}
