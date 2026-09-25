use super::*;

use super::candidates::SWEEP_INTERVAL;
use super::change::{ChangeAttemptResult, reclaim_holder_free_tombstones};
use super::merge::merge_candidate;
use super::reconcile::{
    DEFERRED_RECONCILIATION_CAP, ParentReconciler, PendingReconciliation, ReconciliationOutcome,
};
use super::recovery::ChangeKind;
use super::split::{SplitNeed, SplitReason, SplitTarget, split_need, split_path};
use crate::engine::{AssemblyFixture, EngineConfig};
use crate::leaf_coord::StructuralHinter;
use crate::monitor::TxFinalStatus;
use crate::node_locking::GateAcquisition;
use glassdb_backend::Backend;
use glassdb_backend::memory::MemoryBackend;
use glassdb_backend::middleware::{BackendOp, HookBackend, HookFuture, RecordingBackend};
use glassdb_data::{LogicalKey, NodeToken, ObjectPath, StructuralIntentId, TxId};
use glassdb_storage::transaction::{TxCommitStatus, TxLock, TxRecord, TxWrite};
use glassdb_storage::{
    CachedStore, CollectionRecord, CollectionStore, CurrentState, IndexNode, LeafBody, LeafEntry,
    LeafObservation, LockType, MergeTarget, Node, Observation, Requirement, StorageError,
    StructuralChange, StructuralIntent, StructuralIntentPhase, TreeRouter,
};

mod merge_tests;
mod reconcile_tests;
mod recovery_tests;
mod split_tests;

const COLL: &str = "db/_c/0000000000000000000000";

struct NoStructuralHints;

impl StructuralHinter for NoStructuralHints {
    fn observe_leaf(&self, _path: &ObjectPath, _leaf: &LeafBody) {}

    fn capacity_rejected(&self, _path: &ObjectPath) {}
}

fn collection() -> CollectionAddress {
    CollectionAddress::root("db")
}

fn collection_at(prefix: &str) -> CollectionAddress {
    CollectionAddress::from_physical_prefix(prefix).unwrap()
}

fn db_prefix(value: &str) -> DbPrefix {
    DbPrefix::try_from(value).unwrap()
}

fn test_token(value: &str) -> NodeToken {
    if let Ok(token) = NodeToken::try_from(value) {
        return token;
    }
    let mut bytes = [0_u8; 16];
    for (index, byte) in value.bytes().enumerate() {
        let slot = index % bytes.len();
        bytes[slot] = bytes[slot].wrapping_mul(31).wrapping_add(byte);
    }
    bytes[15] ^= value.len() as u8;
    NodeToken::from_bytes(bytes)
}

fn canonical_node(node: &Node) -> Node {
    let mut canonical = match (node.as_leaf(), node.as_index()) {
        (Some(leaf), None) => Node::leaf(leaf.clone()),
        (None, Some(index)) => Node::index(IndexNode::from_children(
            index
                .children()
                .map(|(key, token)| (key.to_vec(), test_token(token).to_string())),
        )),
        _ => unreachable!("a node has exactly one body"),
    }
    .with_low_key(node.low_key().to_vec())
    .with_high_key(node.high_key().map(<[u8]>::to_vec))
    .with_right_sibling(
        node.right_sibling()
            .map(|token| test_token(token).to_string()),
    );
    canonical.set_locks(node.locks().clone());
    if node.is_drained() {
        let target = canonical
            .right_sibling()
            .expect("a drained node links its merge target")
            .to_string();
        canonical.drain(&target);
    }
    canonical
}

fn canonical_intent(intent: &StructuralIntent) -> StructuralIntent {
    intent.clone()
}

fn root_path() -> ObjectPath {
    ObjectPath::TreeRoot {
        collection: collection(),
    }
}

fn node_path(token: &str) -> ObjectPath {
    ObjectPath::Node {
        collection: collection(),
        token: test_token(token),
    }
}

// A soft cap so tight a two-entry leaf is at the cap and a third overflows it,
// and any three-child index overflows — so splits are driven by a handful of
// keys instead of hundreds.
fn tiny() -> NodeSizePolicy {
    NodeSizePolicy::builder()
        .leaf_max_entries(2)
        .node_soft_max_bytes(1 << 20)
        .index_max_children(2)
        .build()
        .unwrap()
}

#[derive(Clone)]
struct TestStore {
    records: CollectionStore,
    nodes: NodeStore,
    intent_store: StructuralIntentStore,
    objects: CachedStore,
    timeline: Timeline,
    foundation: AssemblyFixture,
}

impl std::ops::Deref for TestStore {
    type Target = NodeStore;

    fn deref(&self) -> &Self::Target {
        &self.nodes
    }
}

impl TestStore {
    async fn create_root(&self, prefix: &str, node: &Node) -> Result<bool, StorageError> {
        let collection = collection_at(prefix);
        self.records
            .create_record(&collection, &CollectionRecord::new())
            .await?;
        self.nodes
            .create_root(&collection, &canonical_node(node))
            .await
    }

    async fn load_root_node(
        &self,
        prefix: &str,
        requirement: Requirement,
    ) -> Result<Option<(Node, LeafObservation)>, StorageError> {
        self.nodes
            .load_root_node(&collection_at(prefix), requirement)
            .await
    }

    async fn load_root(
        &self,
        prefix: &str,
        requirement: Requirement,
    ) -> Result<(Node, LeafObservation), StorageError> {
        self.nodes
            .load_root(&collection_at(prefix), requirement)
            .await
    }

    async fn store_root(
        &self,
        prefix: &str,
        node: &Node,
        expected: &LeafObservation,
    ) -> Result<bool, StorageError> {
        self.nodes
            .store_root(&collection_at(prefix), &canonical_node(node), expected)
            .await
    }

    async fn load_node(
        &self,
        prefix: &str,
        token: &str,
        requirement: Requirement,
    ) -> Result<(Node, LeafObservation), StorageError> {
        self.nodes
            .load_node(&collection_at(prefix), &test_token(token), requirement)
            .await
    }

    async fn store_node(
        &self,
        prefix: &str,
        token: &str,
        node: &Node,
        expected: Option<&LeafObservation>,
    ) -> Result<bool, StorageError> {
        self.nodes
            .store_node(
                &collection_at(prefix),
                &test_token(token),
                &canonical_node(node),
                expected,
            )
            .await
    }

    async fn list_nodes(
        &self,
        prefix: &str,
        requirement: Requirement,
    ) -> Result<Vec<(NodeToken, Observation<Node>)>, StorageError> {
        self.nodes
            .list_nodes(&collection_at(prefix), requirement)
            .await
    }

    async fn write_structural_intent(
        &self,
        intent_id: &str,
        intent: &StructuralIntent,
    ) -> Result<Observation<StructuralIntent>, StorageError> {
        self.intent_store
            .write(
                &db_prefix("db"),
                &StructuralIntentId::from(test_token(intent_id)),
                &canonical_intent(intent),
            )
            .await
    }

    async fn discover_structural_intents(
        &self,
        root: &str,
        requirement: Requirement,
    ) -> Result<Vec<(StructuralIntentId, Observation<StructuralIntent>)>, StorageError> {
        self.intent_store
            .discover(&db_prefix(root), requirement)
            .await
    }
}

fn store() -> TestStore {
    store_with_backend(Arc::new(MemoryBackend::new()))
}

fn store_with_backend(backend: Arc<dyn Backend>) -> TestStore {
    let mut config = EngineConfig::default();
    config.set_cache_size(1 << 20);
    let foundation = AssemblyFixture::new(backend, db_prefix("db"), &config);
    TestStore {
        records: foundation.records.clone(),
        nodes: foundation.nodes.clone(),
        intent_store: foundation.structural_intents.clone(),
        objects: foundation.objects.clone(),
        timeline: foundation.timeline.clone(),
        foundation,
    }
}

// A committed live key, so it counts as existing under a descent lookup.
fn live(key: &[u8]) -> LeafEntry {
    LeafEntry::new(key).with_current(CurrentState::External {
        writer: TxId::from_bytes(vec![1]),
    })
}

fn inline_live(key: &[u8], value: &[u8]) -> LeafEntry {
    LeafEntry::new(key).with_current(CurrentState::Inline {
        writer: TxId::from_bytes(vec![1]),
        value: Arc::from(value),
    })
}

fn tombstone(key: &[u8], writer: TxId) -> LeafEntry {
    LeafEntry::new(key).with_current(CurrentState::Tombstone { writer })
}

fn pressure_inline() -> InlinePolicy {
    InlinePolicy {
        max_value_bytes: 8,
        max_leaf_bytes: 8,
    }
}

fn leaf_node(keys: &[&[u8]], high: Option<&[u8]>, right: Option<&str>) -> Node {
    Node::leaf(LeafBody::from_entries(keys.iter().map(|k| live(k))))
        .with_high_key(high.map(<[u8]>::to_vec))
        .with_right_sibling(right.map(|token| test_token(token).to_string()))
}

fn restructurer(store: &TestStore, bg: &Arc<Background>, policy: NodeSizePolicy) -> Restructurer {
    restructurer_with_candidates(store, bg, MaintenanceCandidates::with_policy(policy))
}

fn restructurer_with_candidates(
    store: &TestStore,
    bg: &Arc<Background>,
    candidates: MaintenanceCandidates,
) -> Restructurer {
    restructurer_with_candidates_and_hints(store, bg, candidates, GcHints::default())
}

fn restructurer_with_candidates_and_hints(
    store: &TestStore,
    bg: &Arc<Background>,
    candidates: MaintenanceCandidates,
    gc_hints: GcHints,
) -> Restructurer {
    let mon = store.foundation.monitor_for(
        bg,
        RetryConfig::default(),
        crate::monitor::ProtocolTiming::default(),
    );
    restructurer_with_monitor_and_hints(store, bg, mon, candidates, gc_hints)
}

fn restructurer_with_monitor(
    store: &TestStore,
    bg: &Arc<Background>,
    mon: Monitor,
    candidates: MaintenanceCandidates,
) -> Restructurer {
    restructurer_with_monitor_and_hints(store, bg, mon, candidates, GcHints::default())
}

fn restructurer_with_monitor_and_hints(
    store: &TestStore,
    bg: &Arc<Background>,
    mon: Monitor,
    candidates: MaintenanceCandidates,
    gc_hints: GcHints,
) -> Restructurer {
    let key_state = KeyStateResolver::new(mon.clone());
    let coord = LeafCoordinator::with_hinter(
        store.nodes.clone(),
        key_state.clone(),
        mon.clone(),
        RetryConfig::default(),
        *candidates.policy(),
        Arc::new(candidates.clone()),
    );
    Restructurer::with_candidates(
        Arc::downgrade(bg),
        store.records.clone(),
        store.nodes.clone(),
        store.intent_store.clone(),
        store.timeline.clone(),
        mon,
        key_state,
        db_prefix("db"),
        coord,
        candidates,
        RetryConfig::default(),
        gc_hints,
    )
}

fn restructurer_and_monitor(
    store: &TestStore,
    bg: &Arc<Background>,
    policy: NodeSizePolicy,
) -> (Restructurer, Monitor) {
    let mon = store.foundation.monitor_for(
        bg,
        RetryConfig::default(),
        crate::monitor::ProtocolTiming::default(),
    );
    let candidates = MaintenanceCandidates::with_policy(policy);
    let restructurer = restructurer_with_monitor(store, bg, mon.clone(), candidates);
    (restructurer, mon)
}

fn leaf_with_membership_reader(keys: &[&[u8]], holder: &TxId) -> Node {
    let mut node = leaf_node(keys, None, None);
    node.add_membership_reader(holder.clone());
    node
}

fn leaf_with_locked_entry(keys: &[&[u8]], holder: &TxId) -> Node {
    let mut entries: Vec<_> = keys.iter().map(|key| live(key)).collect();
    entries[0].replace_write_lock(holder.clone());
    Node::leaf(LeafBody::from_entries(entries))
}

/// A recorded source revision that no stored node carries, so recovery
/// reads the worker that recorded it as unable to publish.
fn superseded_source_revision() -> String {
    "superseded-source-revision".to_string()
}

fn nonroot_intent(source: &str, right: &str, split_key: &[u8]) -> StructuralIntent {
    StructuralIntent {
        collection: collection(),
        source_token: Some(test_token(source)),
        source_revision: superseded_source_revision(),
        change: StructuralChange::Split {
            created_tokens: vec![test_token(right)],
            split_key: split_key.to_vec(),
        },
        participant_id: TxId::from_bytes(b"structural-participant".to_vec()),
        phase: StructuralIntentPhase::Ready,
    }
}

fn test_index(children: &[(&[u8], &str)]) -> IndexNode {
    IndexNode::from_children(
        children
            .iter()
            .map(|(separator, child)| (separator.to_vec(), test_token(child).to_string())),
    )
}
