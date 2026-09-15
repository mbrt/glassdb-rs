//! Physical preparation, fencing, and reclamation of collection incarnations.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use glassdb_concurr::{RetryConfig, rt};
use glassdb_data::{CollectionAddress, NodeToken, TxId};
use glassdb_storage::transaction::TxCommitStatus;
use glassdb_storage::{
    CollectionRecord, CollectionStore, LeafBody, Node, NodeStore, Requirement, StorageError,
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
    nodes: NodeStore,
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
        nodes: NodeStore,
        monitor: Monitor,
        retry: RetryConfig,
        topology: Arc<dyn TopologySettler>,
    ) -> Self {
        Self {
            records,
            nodes,
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
            if !self
                .records
                .create_record(&change.collection, &CollectionRecord::new())
                .await?
            {
                self.records
                    .load_record(&change.collection, Requirement::ANY)
                    .await
                    .map_err(TransError::from)?;
            }
            if !self
                .nodes
                .create_root(&change.collection, &Node::leaf(LeafBody::new()))
                .await?
            {
                self.nodes
                    .load_root(&change.collection, Requirement::ANY)
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
            let nodes = self.nodes.list_nodes(collection, Requirement::ANY).await?;
            for (token, _) in nodes {
                self.fence_node(collection, &token, id).await?;
            }
            self.fence_root(collection, id).await?;
        }
        Ok(())
    }

    /// Clears delete preparation from a discarded body execution.
    pub(crate) async fn clear_aborted_drops(
        &self,
        id: &TxId,
        collections: &[CollectionAddress],
    ) -> Result<bool, TransError> {
        let mut changed = false;
        for collection in collections {
            let mut cursor = None;
            loop {
                let page = self
                    .nodes
                    .scan_nodes(collection, cursor.as_ref(), Requirement::ANY)
                    .await?;
                for (token, _) in page.nodes {
                    changed |= self.clear_node_fence(collection, &token, id).await?;
                }
                match page.next {
                    Some(next) => cursor = Some(next),
                    None => break,
                }
            }
            changed |= self.clear_root_fence(collection, id).await?;
        }
        Ok(changed)
    }

    /// Reclaims physical objects for collections no longer discoverable.
    pub(crate) async fn reclaim(
        &self,
        collections: &[CollectionAddress],
    ) -> Result<bool, TransError> {
        let mut changed = false;
        for collection in collections {
            let mut cursor = None;
            loop {
                let page = self
                    .nodes
                    .scan_nodes(collection, cursor.as_ref(), Requirement::ANY)
                    .await?;
                for (_, observed) in page.nodes {
                    self.nodes.delete_node(&observed).await?;
                    changed = true;
                }
                match page.next {
                    Some(next) => cursor = Some(next),
                    None => break,
                }
            }
            let observed = self
                .nodes
                .load_root_state(collection, Requirement::ANY)
                .await?;
            if observed.exists() {
                self.nodes.delete_root(&observed).await?;
                changed = true;
            }
            let observed = self
                .records
                .load_record_state(collection, Requirement::ANY)
                .await?;
            if observed.exists() {
                self.records.delete_record(&observed).await?;
                changed = true;
            }
        }
        Ok(changed)
    }

    async fn freeze_topology(
        &self,
        collection: &CollectionAddress,
        id: &TxId,
    ) -> Result<(), TransError> {
        let mut backoff = self.retry.backoff();
        loop {
            let (mut record, observed) = self
                .records
                .load_record(collection, Requirement::ANY)
                .await?;
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

    async fn fence_node(
        &self,
        collection: &CollectionAddress,
        token: &NodeToken,
        id: &TxId,
    ) -> Result<(), TransError> {
        let mut backoff = self.retry.backoff();
        loop {
            let (mut node, observed) = match self
                .nodes
                .load_node(collection, token, Requirement::ANY)
                .await
            {
                Ok(node) => node,
                Err(StorageError::NotFound) => return Ok(()),
                Err(error) => return Err(error.into()),
            };
            if node.collection_delete_intent() == Some(id) {
                return Ok(());
            }
            if let Some(holder) = node.collection_delete_intent().cloned() {
                self.resolve_delete_holder(&holder, id).await?;
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
                .nodes
                .store_node(collection, token, &node, Some(&observed))
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
        let mut backoff = self.retry.backoff();
        loop {
            let (mut root, observed) = self.nodes.load_root(collection, Requirement::ANY).await?;
            if root.collection_delete_intent() == Some(id) {
                return Ok(());
            }
            if let Some(holder) = root.collection_delete_intent().cloned() {
                self.resolve_delete_holder(&holder, id).await?;
            }
            if let Some(holder) = self.pending_node_holder(&root, id).await? {
                self.resolve_pending_holder(&holder, id).await?;
                continue;
            }
            // As for standalone nodes, the exact-revision rewrite closes the
            // final race without leaving a separate gate to recover on abort.
            root.set_collection_delete_intent(id.clone());
            if self.nodes.store_root(collection, &root, &observed).await? {
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
                holders.extend(entry.lock_holders().iter().cloned());
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

    /// Establishes that a foreign delete intent can be replaced.
    async fn resolve_delete_holder(&self, holder: &TxId, id: &TxId) -> Result<(), TransError> {
        match resolve_tx_conflict(&self.monitor, id, holder).await? {
            TxFinalStatus::Committed => Err(TransError::StaleCollection),
            TxFinalStatus::Aborted => Ok(()),
        }
    }

    async fn clear_node_fence(
        &self,
        collection: &CollectionAddress,
        token: &NodeToken,
        id: &TxId,
    ) -> Result<bool, TransError> {
        loop {
            let (mut node, observed) = self
                .nodes
                .load_node(collection, token, Requirement::ANY)
                .await?;
            if !node.remove_collection_delete_intent(id) {
                return Ok(false);
            }
            if self
                .nodes
                .store_node(collection, token, &node, Some(&observed))
                .await?
            {
                return Ok(true);
            }
        }
    }

    async fn clear_root_fence(
        &self,
        collection: &CollectionAddress,
        id: &TxId,
    ) -> Result<bool, TransError> {
        let mut changed = false;
        loop {
            let (mut root, observed) =
                match self.nodes.load_root(collection, Requirement::ANY).await {
                    Ok(root) => root,
                    Err(StorageError::NotFound) => return Ok(changed),
                    Err(error) => return Err(error.into()),
                };
            if !root.remove_collection_delete_intent(id) {
                break;
            }
            if self.nodes.store_root(collection, &root, &observed).await? {
                changed = true;
                break;
            }
        }
        loop {
            let (mut record, observed) =
                match self.records.load_record(collection, Requirement::ANY).await {
                    Ok(record) => record,
                    Err(StorageError::NotFound) => return Ok(changed),
                    Err(error) => return Err(error.into()),
                };
            if !record.remove_topology_freeze(id) {
                return Ok(changed);
            }
            if self.records.store_record(&record, &observed).await? {
                return Ok(true);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use glassdb_backend::middleware::{BackendOp, HookBackend, HookFuture, RecordingBackend};
    use glassdb_backend::{Backend, BackendError, memory::MemoryBackend};
    use glassdb_concurr::Background;
    use glassdb_data::{CollectionId, DbRoot, NodeToken, ObjectPath};
    use glassdb_storage::transaction::{TLogger, TxCollectionChange, TxCollectionOp, TxLog};
    use glassdb_storage::{CachedStore, CurrentState, IndexNode, LeafBody, LeafEntry, Timeline};
    use tokio::sync::Notify;

    use super::*;
    use crate::engine::{AssemblyFixture, EngineConfig};
    use crate::monitor::TxRecoveryManifest;

    const COLLECTION: &str = "db/_c/0000000000000000000000";
    const SOURCE_TOKEN: &str = "0000000000000000000000";
    const RIGHT_TOKEN: &str = "0F410F410F410F410F410F";

    fn collection() -> CollectionAddress {
        CollectionAddress::from_physical_prefix(COLLECTION).unwrap()
    }

    fn node_token(value: &str) -> NodeToken {
        NodeToken::try_from(value).unwrap()
    }

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
            let source_path = ObjectPath::Node {
                collection: collection(),
                token: node_token(SOURCE_TOKEN),
            }
            .to_string();
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
        nodes: NodeStore,
        objects: CachedStore,
        timeline: Timeline,
    }

    fn store(backend: Arc<dyn Backend>) -> TestStore {
        let timeline = Timeline::new();
        let objects = CachedStore::new(backend, 1 << 20, timeline.clone(), None);
        TestStore {
            records: CollectionStore::new(objects.clone()),
            nodes: NodeStore::new(objects.clone(), std::num::NonZeroUsize::MIN),
            objects,
            timeline,
        }
    }

    fn live_entry(key: &[u8]) -> LeafEntry {
        LeafEntry::new(key).with_current(CurrentState::External {
            writer: TxId::from_bytes(vec![9]),
        })
    }

    fn lifecycle(fixture: &AssemblyFixture) -> CollectionLifecycle {
        CollectionLifecycle::new(
            fixture.records.clone(),
            fixture.nodes.clone(),
            fixture.monitor.clone(),
            RetryConfig::default(),
            Arc::new(UnexpectedTopologySettler),
        )
    }

    async fn refence_terminal_drop(status: TxCommitStatus, with_child: bool, cleanup_races: bool) {
        let hooks = HookBackend::new(Arc::new(MemoryBackend::new()));
        let recorder = RecordingBackend::new(hooks.clone());
        let operations = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        let owner = AssemblyFixture::new(
            hooks.clone(),
            DbRoot::try_from("db").unwrap(),
            &EngineConfig::default(),
        );
        let peer = AssemblyFixture::new(
            backend.clone(),
            DbRoot::try_from("db").unwrap(),
            &EngineConfig::default(),
        );
        let owner_lifecycle = lifecycle(&owner);
        let peer_lifecycle = lifecycle(&peer);
        let collection = CollectionAddress::new("db", CollectionId::from_slice(&[17; 16]).unwrap());
        let mut change = CollectionChange {
            parent: CollectionAddress::root("db"),
            name: b"child".to_vec(),
            collection: collection.clone(),
            expected: None,
            op: CollectionOp::Create,
        };
        owner_lifecycle
            .prepare_collections(std::slice::from_ref(&change))
            .await
            .unwrap();
        let mut parent = CollectionRecord::new();
        parent
            .add_child(change.name.clone(), collection.id())
            .unwrap();
        assert!(
            owner
                .records
                .create_record(&change.parent, &parent)
                .await
                .unwrap()
        );
        assert!(
            owner
                .nodes
                .create_root(&change.parent, &Node::leaf(LeafBody::new()))
                .await
                .unwrap()
        );
        let mut paths = vec![ObjectPath::TreeRoot {
            collection: collection.clone(),
        }];
        if with_child {
            let token = node_token(SOURCE_TOKEN);
            assert!(
                owner
                    .nodes
                    .store_node(&collection, &token, &Node::leaf(LeafBody::new()), None)
                    .await
                    .unwrap()
            );
            let (_, observed) = owner
                .nodes
                .load_root(&collection, Requirement::ANY)
                .await
                .unwrap();
            let root = Node::index(IndexNode::from_children([(
                Vec::new(),
                SOURCE_TOKEN.to_owned(),
            )]));
            assert!(
                owner
                    .nodes
                    .store_root(&collection, &root, &observed)
                    .await
                    .unwrap()
            );
            paths.push(ObjectPath::Node {
                collection: collection.clone(),
                token,
            });
        }
        change.expected = Some(collection.id());
        change.op = CollectionOp::Drop;
        let first = TxId::from_bytes(vec![1]);
        let second = TxId::from_bytes(vec![2]);
        let manifest = TxRecoveryManifest {
            collection_changes: vec![TxCollectionChange {
                parent: change.parent.clone(),
                name: change.name.clone(),
                collection: collection.clone(),
                op: TxCollectionOp::Drop,
            }],
            ..Default::default()
        };
        owner
            .monitor
            .begin_persisted_tx(&first, manifest.clone())
            .await
            .unwrap();
        owner_lifecycle
            .fence_drops(&first, std::slice::from_ref(&change))
            .await
            .unwrap();
        match status {
            TxCommitStatus::Wounded => {
                assert_eq!(
                    peer.monitor.preempt_tx(&first).await.unwrap(),
                    TxFinalStatus::Aborted
                );
            }
            TxCommitStatus::Ok => {
                let mut log = TxLog::new(first.clone(), TxCommitStatus::Ok);
                log.collection_changes = manifest.collection_changes.clone();
                owner.monitor.commit_tx(log).await.unwrap();
            }
            _ => panic!("the first drop must be terminal"),
        }
        peer.monitor
            .begin_persisted_tx(&second, manifest)
            .await
            .unwrap();

        // Wounded and acknowledged-aborted holders take the same abort-side
        // branch. Start wounded so repeated status reads can bound a broken
        // loop; cached immutable Aborted status could otherwise hide it.
        let status_path = ObjectPath::Transaction {
            db_root: DbRoot::try_from("db").unwrap(),
            id: first.clone(),
        }
        .to_string();
        let status_reads = AtomicUsize::new(0);
        let cleanup_armed = AtomicBool::new(cleanup_races);
        let contested_path = paths.last().unwrap().to_string();
        hooks.set_before({
            let owner_lifecycle = owner_lifecycle.clone();
            let owner_monitor = owner.monitor.clone();
            let collection = collection.clone();
            let first = first.clone();
            let contested_path = contested_path.clone();
            move |op| {
                let status_read = matches!(
                    op,
                    BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                ) && op.path() == status_path;
                let repeated = status_read && status_reads.fetch_add(1, Ordering::SeqCst) >= 7;
                let cleanup = matches!(op, BackendOp::WriteIf { .. })
                    && op.path() == contested_path
                    && cleanup_armed.swap(false, Ordering::SeqCst);
                let owner_lifecycle = owner_lifecycle.clone();
                let owner_monitor = owner_monitor.clone();
                let collection = collection.clone();
                let first = first.clone();
                Box::pin(async move {
                    if repeated {
                        return Err(BackendError::other(
                            "delete-intent resolution made no progress",
                        ));
                    }
                    if cleanup {
                        // Acknowledged owner cleanup wins after the new drop
                        // selected its revision, so replacement must retry.
                        owner_monitor
                            .abort_owned_tx(&first)
                            .await
                            .map_err(|error| {
                                BackendError::with_source("acknowledging the prior drop", error)
                            })?;
                        owner_lifecycle
                            .clear_aborted_drops(&first, &[collection])
                            .await
                            .map_err(|error| {
                                BackendError::with_source("clearing the prior drop", error)
                            })?;
                    }
                    Ok(())
                })
            }
        });
        operations.lock().unwrap().clear();
        let result = peer_lifecycle
            .fence_drops(&second, std::slice::from_ref(&change))
            .await;
        hooks.clear_before();
        let recorded = std::mem::take(&mut *operations.lock().unwrap());
        let expected = if status == TxCommitStatus::Ok {
            assert!(matches!(result, Err(TransError::StaleCollection)));
            &first
        } else {
            result.unwrap();
            &second
        };
        for path in &paths {
            let path = path.to_string();
            let calls: Vec<_> = recorded
                .iter()
                .filter(|op| op.path == path)
                .map(|op| op.op)
                .collect();
            let expected_calls: &[&str] = if status == TxCommitStatus::Ok {
                &[]
            } else if cleanup_races && path == contested_path {
                &["read", "write_if", "read", "write_if"]
            } else {
                &["read", "write_if"]
            };
            assert_eq!(calls, expected_calls, "unexpected node I/O for {path}");
        }
        if status != TxCommitStatus::Ok {
            peer_lifecycle
                .fence_drops(&second, std::slice::from_ref(&change))
                .await
                .unwrap();
            let replay = std::mem::take(&mut *operations.lock().unwrap());
            assert!(
                replay
                    .iter()
                    .all(|op| paths.iter().all(|path| op.path != path.to_string()))
            );
        }
        let verifier = store(backend);
        for path in paths {
            let observed = verifier
                .nodes
                .load_node_at_state(&path, Requirement::ANY)
                .await
                .unwrap();
            assert_eq!(
                observed.value().unwrap().collection_delete_intent(),
                Some(expected)
            );
        }
    }

    #[tokio::test]
    async fn drop_replaces_a_wounded_root_intent() {
        refence_terminal_drop(TxCommitStatus::Wounded, false, false).await;
    }

    #[tokio::test]
    async fn drop_replaces_wounded_node_intents() {
        refence_terminal_drop(TxCommitStatus::Wounded, true, false).await;
    }

    #[tokio::test]
    async fn drop_retries_if_aborted_owner_clears_the_intent() {
        for with_child in [false, true] {
            refence_terminal_drop(TxCommitStatus::Wounded, with_child, true).await;
        }
    }

    #[tokio::test]
    async fn drop_preserves_committed_intents() {
        for with_child in [false, true] {
            refence_terminal_drop(TxCommitStatus::Ok, with_child, false).await;
        }
    }

    async fn run_fence_shrink_race(fence_waits: bool) {
        let (backend, gate) = FirstSourceWriteGate::wrap(Arc::new(MemoryBackend::new()));
        let backend: Arc<dyn Backend> = backend;
        let primary = store(backend.clone());
        let peer = store(backend.clone());
        let background = Arc::new(Background::new());
        let monitor = Monitor::with_config(
            TLogger::new(primary.objects.clone(), DbRoot::try_from("db").unwrap()),
            primary.timeline.clone(),
            Arc::downgrade(&background),
            RetryConfig::default(),
            crate::monitor::ProtocolTiming::default(),
        );
        let retry = RetryConfig {
            initial_interval: Duration::ZERO,
            max_interval: Duration::ZERO,
        };
        let primary_lifecycle = CollectionLifecycle::new(
            primary.records.clone(),
            primary.nodes.clone(),
            monitor.clone(),
            retry,
            Arc::new(UnexpectedTopologySettler),
        );
        let peer_lifecycle = CollectionLifecycle::new(
            peer.records.clone(),
            peer.nodes.clone(),
            monitor.clone(),
            retry,
            Arc::new(UnexpectedTopologySettler),
        );
        let split_id = TxId::from_bytes(vec![2]);
        let drop_id = TxId::from_bytes(vec![1]);

        let mut source = Node::leaf(LeafBody::from_entries([live_entry(b"a"), live_entry(b"z")]));
        source.set_structural_gate(split_id.clone());
        assert!(
            primary
                .nodes
                .store_node(&collection(), &node_token(SOURCE_TOKEN), &source, None,)
                .await
                .unwrap()
        );
        let (mut shrunk, source_version) = primary
            .nodes
            .load_node(&collection(), &node_token(SOURCE_TOKEN), Requirement::ANY)
            .await
            .unwrap();
        let (right, _) = shrunk.split(RIGHT_TOKEN).unwrap();
        shrunk.remove_structural_gate(&split_id);
        assert!(
            primary
                .nodes
                .store_node(&collection(), &node_token(RIGHT_TOKEN), &right, None)
                .await
                .unwrap()
        );
        monitor.begin_tx(&split_id);
        monitor.abort_owned_tx(&split_id).await.unwrap();

        gate.arm();
        let shrink_landed = if fence_waits {
            let fencing = tokio::spawn({
                let lifecycle = primary_lifecycle.clone();
                let drop_id = drop_id.clone();
                async move {
                    lifecycle
                        .fence_node(&collection(), &node_token(SOURCE_TOKEN), &drop_id)
                        .await
                }
            });
            gate.wait_until_entered().await;
            let shrink_landed = peer
                .nodes
                .store_node(
                    &collection(),
                    &node_token(SOURCE_TOKEN),
                    &shrunk,
                    Some(&source_version),
                )
                .await
                .unwrap();
            gate.release();
            fencing.await.unwrap().unwrap();
            shrink_landed
        } else {
            let shrinking = tokio::spawn({
                let nodes = primary.nodes.clone();
                let shrunk = shrunk.clone();
                let source_version = source_version.clone();
                async move {
                    nodes
                        .store_node(
                            &collection(),
                            &node_token(SOURCE_TOKEN),
                            &shrunk,
                            Some(&source_version),
                        )
                        .await
                }
            });
            gate.wait_until_entered().await;
            let fence_result = peer_lifecycle
                .fence_node(&collection(), &node_token(SOURCE_TOKEN), &drop_id)
                .await;
            gate.release();
            let shrink_landed = shrinking.await.unwrap().unwrap();
            fence_result.unwrap();
            shrink_landed
        };
        assert_eq!(shrink_landed, fence_waits);

        let verifier = store(backend);
        let (final_source, _) = verifier
            .nodes
            .load_node(&collection(), &node_token(SOURCE_TOKEN), Requirement::ANY)
            .await
            .unwrap();
        assert_eq!(final_source.collection_delete_intent(), Some(&drop_id));
        assert_eq!(
            final_source.right_sibling(),
            shrink_landed.then_some(RIGHT_TOKEN)
        );
    }

    #[tokio::test]
    async fn collection_fence_retries_after_an_in_flight_shrink_lands() {
        run_fence_shrink_race(true).await;
    }

    #[tokio::test]
    async fn collection_fence_prevents_a_late_in_flight_shrink() {
        run_fence_shrink_race(false).await;
    }

    #[tokio::test]
    async fn reclamation_deletes_each_page_before_listing_more_nodes() {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let primary = store(backend.clone());
        let background = Arc::new(Background::new());
        let monitor = Monitor::with_config(
            TLogger::new(primary.objects.clone(), DbRoot::try_from("db").unwrap()),
            primary.timeline.clone(),
            Arc::downgrade(&background),
            RetryConfig::default(),
            crate::monitor::ProtocolTiming::default(),
        );
        let lifecycle = CollectionLifecycle::new(
            primary.records.clone(),
            primary.nodes.clone(),
            monitor,
            RetryConfig::default(),
            Arc::new(UnexpectedTopologySettler),
        );
        let collection = collection();
        primary
            .records
            .create_record(&collection, &CollectionRecord::new())
            .await
            .unwrap();
        primary
            .nodes
            .create_root(&collection, &Node::leaf(LeafBody::new()))
            .await
            .unwrap();
        for i in 0u16..257 {
            let mut bytes = [0; 16];
            bytes[..2].copy_from_slice(&i.to_be_bytes());
            primary
                .nodes
                .store_node(
                    &collection,
                    &NodeToken::from_bytes(bytes),
                    &Node::leaf(LeafBody::new()),
                    None,
                )
                .await
                .unwrap();
        }
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        backend.set_before({
            let events = events.clone();
            move |op| {
                match op {
                    BackendOp::List { path, .. } => {
                        events.lock().unwrap().push(("list", path.to_string()))
                    }
                    BackendOp::DeleteIf { path, .. } => {
                        events.lock().unwrap().push(("delete", path.to_string()))
                    }
                    _ => {}
                }
                Box::pin(async { Ok(()) })
            }
        });
        assert!(
            lifecycle
                .reclaim(std::slice::from_ref(&collection))
                .await
                .unwrap()
        );
        let events = events.lock().unwrap();
        let first_delete = events.iter().position(|(op, _)| *op == "delete").unwrap();
        let second_list = events
            .iter()
            .enumerate()
            .filter(|(_, (op, _))| *op == "list")
            .nth(1)
            .unwrap()
            .0;
        assert!(
            first_delete < second_list,
            "cleanup must not retain the whole collection"
        );
        let deleted: Vec<_> = events
            .iter()
            .filter(|(op, _)| *op == "delete")
            .map(|(_, path)| path)
            .collect();
        assert_eq!(deleted.len(), 259);
        assert!(deleted[257].ends_with("/_r"));
        assert!(deleted[258].ends_with("/_i"));
    }
}
