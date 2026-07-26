//! Physical preparation, fencing, and reclamation of collection incarnations.

use std::collections::BTreeSet;

use glassdb_concurr::{RetryConfig, rt};
use glassdb_data::{CollectionAddress, TxId};
use glassdb_storage::{
    CollectionRoot, LeafObservation, Node, Requirement, ShardStore, StorageError, TxCommitStatus,
};

use super::{CollectionChange, CollectionOp};
use crate::error::TransError;
use crate::monitor::{Monitor, TxFinalStatus};
use crate::split::Splitter;
use crate::wound_wait::{Reclaim, resolve_tx_conflict, try_reclaim};

/// Drives collection incarnations through preparation, deletion, and cleanup.
#[derive(Clone)]
pub(crate) struct CollectionLifecycle {
    shards: ShardStore,
    monitor: Monitor,
    retry: RetryConfig,
}

impl CollectionLifecycle {
    /// Creates collection lifecycle access over the shared stores.
    pub(crate) fn new(shards: ShardStore, monitor: Monitor, retry: RetryConfig) -> Self {
        Self {
            shards,
            monitor,
            retry,
        }
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
                match resolve_tx_conflict(&self.monitor, id, &holder).await? {
                    TxFinalStatus::Committed => return Err(TransError::StaleCollection),
                    TxFinalStatus::Aborted => {
                        root.remove_topology_freeze(&holder);
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
            resolve_tx_conflict(&self.monitor, id, &participant).await?;
            splitter
                .settle_topology_participant(collection, &participant)
                .await?;
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
            self.monitor.await_tx_final(holder).await?;
        }
        Ok(())
    }

    async fn resolve_delete_holder(&self, holder: &TxId, id: &TxId) -> Result<(), TransError> {
        match resolve_tx_conflict(&self.monitor, id, holder).await? {
            TxFinalStatus::Committed => Err(TransError::StaleCollection),
            TxFinalStatus::Aborted => Ok(()),
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
