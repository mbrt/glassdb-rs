//! Physical preparation, fencing, and reclamation of collection incarnations.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use glassdb_concurr::{RetryConfig, rt};
use glassdb_data::{CollectionAddress, TxId};
use glassdb_storage::{
    CollectionRecord, CollectionStore, Node, Requirement, Shard, ShardStore, StorageError,
    TxCommitStatus,
};

use super::{CollectionChange, CollectionOp};
use crate::error::TransError;
use crate::monitor::{Monitor, TxFinalStatus};
use crate::wound_wait::{Reclaim, resolve_tx_conflict, try_reclaim};

/// Completes the structural recovery a finalized topology participant left
/// behind, so a drop can freeze the topology without waiting for the background
/// sweep. The [`Splitter`](crate::split::Splitter) supplies the implementation.
#[async_trait]
pub trait TopologySettler: Send + Sync {
    /// Finishes and releases `id`'s structural work on `collection`. Returns
    /// [`TransError::Retry`] while `id` is not yet final.
    async fn settle_topology_participant(
        &self,
        collection: &CollectionAddress,
        id: &TxId,
    ) -> Result<(), TransError>;
}

/// Drives collection incarnations through preparation, deletion, and cleanup.
#[derive(Clone)]
pub struct CollectionLifecycle {
    records: CollectionStore,
    shards: ShardStore,
    monitor: Monitor,
    retry: RetryConfig,
    // A drop must outlive every pre-existing topology participant before its
    // commit point, so fencing settles them rather than racing them.
    topology: Arc<dyn TopologySettler>,
}

impl CollectionLifecycle {
    /// Creates collection lifecycle access over the shared stores.
    pub fn new(
        records: CollectionStore,
        shards: ShardStore,
        monitor: Monitor,
        retry: RetryConfig,
        topology: Arc<dyn TopologySettler>,
    ) -> Self {
        Self {
            records,
            shards,
            monitor,
            retry,
            topology,
        }
    }

    /// Creates both objects of every fresh, still-undiscoverable collection.
    pub(crate) async fn prepare_collections(
        &self,
        changes: &[CollectionChange],
    ) -> Result<(), TransError> {
        for change in changes
            .iter()
            .filter(|change| change.op == CollectionOp::Create)
        {
            let prefix = change.collection.physical_prefix();
            if !self
                .records
                .create_record(&prefix, &CollectionRecord::new())
                .await?
            {
                self.records
                    .load_record(&prefix, Requirement::Any)
                    .await
                    .map_err(TransError::from)?;
            }
            if !self
                .shards
                .create_root(&prefix, &Node::leaf(Shard::new()))
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
    ) -> Result<(), TransError> {
        for collection in changes
            .iter()
            .filter(|change| change.op == CollectionOp::Drop)
            .map(|change| &change.collection)
        {
            self.freeze_topology(collection, id).await?;
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
            let observed = self
                .shards
                .load_root_state(&prefix, Requirement::Any)
                .await?;
            if observed.exists() {
                self.shards.delete_root(&observed).await?;
            }
            let observed = self
                .records
                .load_record_state(&prefix, Requirement::Any)
                .await?;
            if observed.exists() {
                self.records.delete_record(&observed).await?;
            }
        }
        Ok(())
    }

    async fn freeze_topology(
        &self,
        collection: &CollectionAddress,
        id: &TxId,
    ) -> Result<(), TransError> {
        let prefix = collection.physical_prefix();
        let mut backoff = self.retry.backoff();
        loop {
            let (mut record, observed) =
                self.records.load_record(&prefix, Requirement::Any).await?;
            if record.topology_freeze() != Some(id)
                && let Some(holder) = record.topology_freeze().cloned()
            {
                match resolve_tx_conflict(&self.monitor, id, &holder).await? {
                    TxFinalStatus::Committed => return Err(TransError::StaleCollection),
                    TxFinalStatus::Aborted => {
                        record.remove_topology_freeze(&holder);
                    }
                }
            }
            if record.topology_freeze().is_none() {
                assert!(record.set_topology_freeze(id.clone()));
                if self.records.store_record(&record, &observed).await? {
                    continue;
                }
                rt::sleep(backoff.next_delay()).await;
                continue;
            }
            if record.topology_freeze() != Some(id) {
                continue;
            }
            if record.topology_participants().next().is_none() {
                return Ok(());
            }

            let participant = record
                .topology_participants()
                .next()
                .cloned()
                .expect("participant presence was checked above");
            resolve_tx_conflict(&self.monitor, id, &participant).await?;
            self.topology
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
            // The topology freeze has drained structural participants. This
            // exact-revision rewrite fuses the remaining one-shot structural
            // exclusion with intent installation: a late node CAS either lands
            // first and makes us retry, or loses and then observes the intent.
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
            if root.collection_delete_intent() == Some(id) {
                return Ok(());
            }
            if let Some(holder) = root.collection_delete_intent().cloned() {
                self.resolve_delete_holder(&holder, id).await?;
                continue;
            }
            if let Some(holder) = self.pending_node_holder(&root, id).await? {
                self.resolve_pending_holder(&holder, id).await?;
                continue;
            }
            // As for standalone nodes, the exact-revision rewrite closes the
            // final race without leaving a separate gate to recover on abort.
            root.set_collection_delete_intent(id.clone());
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
            if !root.remove_collection_delete_intent(id) {
                break;
            }
            if self.shards.store_root(&prefix, &root, &observed).await? {
                break;
            }
        }
        loop {
            let (mut record, observed) =
                match self.records.load_record(&prefix, Requirement::Any).await {
                    Ok(record) => record,
                    Err(StorageError::NotFound) => return Ok(()),
                    Err(error) => return Err(error.into()),
                };
            if !record.remove_topology_freeze(id) {
                return Ok(());
            }
            if self.records.store_record(&record, &observed).await? {
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use glassdb_backend::middleware::{BackendOp, HookBackend, HookFuture};
    use glassdb_backend::{Backend, memory::MemoryBackend};
    use glassdb_concurr::Background;
    use glassdb_data::paths;
    use glassdb_storage::{CachedStore, CurrentState, Shard, ShardEntry, TLogger, Timeline};
    use tokio::sync::Notify;

    use super::*;

    const COLLECTION: &str = "db/_c/0000000000000000000000";
    const SOURCE_TOKEN: &str = "L";

    struct UnexpectedTopologySettler;

    #[async_trait]
    impl TopologySettler for UnexpectedTopologySettler {
        async fn settle_topology_participant(
            &self,
            _collection: &CollectionAddress,
            _id: &TxId,
        ) -> Result<(), TransError> {
            panic!("the subject under test must not settle topology participants")
        }
    }

    struct FirstSourceWriteGate {
        armed: AtomicBool,
        entered: Notify,
        release: Notify,
    }

    impl FirstSourceWriteGate {
        fn wrap(inner: Arc<dyn Backend>) -> (Arc<HookBackend>, Arc<Self>) {
            let source_path = paths::from_node(COLLECTION, SOURCE_TOKEN);
            let gate = Arc::new(Self {
                armed: AtomicBool::new(false),
                entered: Notify::new(),
                release: Notify::new(),
            });
            let backend = HookBackend::new(inner);
            backend.set_before({
                let gate = gate.clone();
                move |op| {
                    let wait = matches!(
                        op,
                        BackendOp::WriteIf { path, .. }
                            if path == &source_path
                                && gate.armed.swap(false, Ordering::SeqCst)
                    );
                    let gate = gate.clone();
                    let future: HookFuture = Box::pin(async move {
                        if wait {
                            gate.entered.notify_one();
                            gate.release.notified().await;
                        }
                        Ok(())
                    });
                    future
                }
            });
            (backend, gate)
        }

        fn arm(&self) {
            self.armed.store(true, Ordering::SeqCst);
        }

        async fn wait_until_entered(&self) {
            self.entered.notified().await;
        }

        fn release(&self) {
            self.release.notify_one();
        }
    }

    struct TestStore {
        records: CollectionStore,
        shards: ShardStore,
        objects: CachedStore,
        timeline: Timeline,
    }

    fn store(backend: Arc<dyn Backend>) -> TestStore {
        let timeline = Timeline::new();
        let objects = CachedStore::new(backend, 1 << 20, timeline.clone(), None);
        TestStore {
            records: CollectionStore::new(objects.clone()),
            shards: ShardStore::new(objects.clone()),
            objects,
            timeline,
        }
    }

    fn live_entry(key: &[u8]) -> ShardEntry {
        ShardEntry {
            current: CurrentState::External {
                writer: TxId::from_bytes(vec![9]),
            },
            ..ShardEntry::new(key)
        }
    }

    async fn run_fence_shrink_race(fence_waits: bool) {
        let (backend, gate) = FirstSourceWriteGate::wrap(Arc::new(MemoryBackend::new()));
        let backend: Arc<dyn Backend> = backend;
        let primary = store(backend.clone());
        let peer = store(backend.clone());
        let background = Arc::new(Background::new());
        let monitor = Monitor::with_config(
            TLogger::new(primary.objects.clone(), "db"),
            primary.timeline.clone(),
            Arc::downgrade(&background),
            glassdb_concurr::Clock::real(),
            RetryConfig::default(),
            crate::monitor::ProtocolTiming::default(),
        );
        let retry = RetryConfig {
            initial_interval: Duration::ZERO,
            max_interval: Duration::ZERO,
        };
        let primary_lifecycle = CollectionLifecycle::new(
            primary.records.clone(),
            primary.shards.clone(),
            monitor.clone(),
            retry,
            Arc::new(UnexpectedTopologySettler),
        );
        let peer_lifecycle = CollectionLifecycle::new(
            peer.records.clone(),
            peer.shards.clone(),
            monitor.clone(),
            retry,
            Arc::new(UnexpectedTopologySettler),
        );
        let split_id = TxId::from_bytes(vec![2]);
        let drop_id = TxId::from_bytes(vec![1]);

        let mut source = Node::leaf(Shard::from_entries([live_entry(b"a"), live_entry(b"z")]));
        source.set_structural_gate(split_id.clone());
        assert!(
            primary
                .shards
                .store_node(COLLECTION, SOURCE_TOKEN, &source, None)
                .await
                .unwrap()
        );
        let (mut shrunk, source_version) = primary
            .shards
            .load_node(COLLECTION, SOURCE_TOKEN, Requirement::Any)
            .await
            .unwrap();
        let (right, _) = shrunk.split("R").unwrap();
        shrunk.remove_structural_gate(&split_id);
        assert!(
            primary
                .shards
                .store_node(COLLECTION, "R", &right, None)
                .await
                .unwrap()
        );
        monitor.begin_tx(&split_id);
        monitor.abort_tx(&split_id).await.unwrap();

        gate.arm();
        let shrink_landed = if fence_waits {
            let fencing = tokio::spawn({
                let lifecycle = primary_lifecycle.clone();
                let drop_id = drop_id.clone();
                async move {
                    lifecycle
                        .fence_node(COLLECTION, SOURCE_TOKEN, &drop_id)
                        .await
                }
            });
            gate.wait_until_entered().await;
            let shrink_landed = peer
                .shards
                .store_node(COLLECTION, SOURCE_TOKEN, &shrunk, Some(&source_version))
                .await
                .unwrap();
            gate.release();
            fencing.await.unwrap().unwrap();
            shrink_landed
        } else {
            let shrinking = tokio::spawn({
                let shards = primary.shards.clone();
                let shrunk = shrunk.clone();
                let source_version = source_version.clone();
                async move {
                    shards
                        .store_node(COLLECTION, SOURCE_TOKEN, &shrunk, Some(&source_version))
                        .await
                }
            });
            gate.wait_until_entered().await;
            let fence_result = peer_lifecycle
                .fence_node(COLLECTION, SOURCE_TOKEN, &drop_id)
                .await;
            gate.release();
            let shrink_landed = shrinking.await.unwrap().unwrap();
            fence_result.unwrap();
            shrink_landed
        };
        assert_eq!(shrink_landed, fence_waits);

        let verifier = store(backend);
        let (final_source, _) = verifier
            .shards
            .load_node(COLLECTION, SOURCE_TOKEN, Requirement::Any)
            .await
            .unwrap();
        assert_eq!(final_source.collection_delete_intent(), Some(&drop_id));
        assert_eq!(final_source.right_sibling(), shrink_landed.then_some("R"));
    }

    #[tokio::test]
    async fn collection_fence_retries_after_an_in_flight_shrink_lands() {
        run_fence_shrink_race(true).await;
    }

    #[tokio::test]
    async fn collection_fence_prevents_a_late_in_flight_shrink() {
        run_fence_shrink_race(false).await;
    }
}
