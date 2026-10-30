use super::*;

use super::candidates::SWEEP_INTERVAL;
use super::change::ChangeAttemptResult;
use super::merge::MergeReason;
use super::reclamation::reclaim_holder_free_tombstones;
use super::reconcile::{
    DEFERRED_RECONCILIATION_CAP, ParentReconciler, PendingReconciliation, ReconciliationOutcome,
};
use super::recovery::ChangeKind;
use super::rule::RuleRequest;
use super::split::{SplitNeed, SplitReason, SplitTarget};
use crate::engine::{AssemblyFixture, EngineConfig};
use crate::leaf_coord::StructuralHinter;
use crate::monitor::TxFinalStatus;
use crate::node_locking::GateAcquisition;
use crate::retry_timing::RetryTiming;
use glassdb_backend::Backend;
use glassdb_backend::memory::MemoryBackend;
use glassdb_backend::middleware::{BackendOp, HookBackend, HookFuture, RecordingBackend};
use glassdb_concurr::RetrySchedule;
use glassdb_data::{LogicalKey, NodeId, ObjectPath, StructuralIntentId, TxId};
use glassdb_storage::transaction::{TxCommitStatus, TxLock, TxRecord, TxWrite};
use glassdb_storage::{
    CachedStore, CollectionRecord, CollectionStore, CurrentState, IndexNode, LeafBody, LeafEntry,
    LeafObservation, LockType, MergeTarget, Node, Observation, Requirement, StorageError,
    StructuralChange, StructuralIntent, StructuralIntentPhase, TreeRouter,
};

mod contention_tests;
mod merge_tests;
mod policy_tests;
mod reconcile_tests;
mod recovery_tests;
mod split_tests;
mod topology_tests;

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

/// Maps a readable test name to a stable 128-bit ID.
fn test_id_bytes(name: &str) -> [u8; 16] {
    let mut bytes = [0_u8; 16];
    for (index, byte) in name.bytes().enumerate() {
        let slot = index % bytes.len();
        bytes[slot] = bytes[slot].wrapping_mul(31).wrapping_add(byte);
    }
    bytes[15] ^= name.len() as u8;
    bytes
}

fn test_node_id(name: &str) -> NodeId {
    NodeId::from_bytes(test_id_bytes(name))
}

fn test_intent_id(name: &str) -> StructuralIntentId {
    StructuralIntentId::from_bytes(test_id_bytes(name))
}

fn tx_id(prefix: &[u8]) -> TxId {
    TxId::with_priority(0, prefix)
}

fn root_path() -> ObjectPath {
    ObjectPath::TreeRoot {
        collection: collection(),
    }
}

fn node_path(name: &str) -> ObjectPath {
    ObjectPath::Node {
        collection: collection(),
        id: test_node_id(name),
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
        self.nodes.create_root(&collection, node).await
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
            .store_root(&collection_at(prefix), node, expected)
            .await
    }

    async fn load_node(
        &self,
        prefix: &str,
        name: &str,
        requirement: Requirement,
    ) -> Result<(Node, LeafObservation), StorageError> {
        self.nodes
            .load_node(&collection_at(prefix), &test_node_id(name), requirement)
            .await
    }

    async fn store_node(
        &self,
        prefix: &str,
        name: &str,
        node: &Node,
        expected: Option<&LeafObservation>,
    ) -> Result<bool, StorageError> {
        self.nodes
            .store_node(&collection_at(prefix), &test_node_id(name), node, expected)
            .await
    }

    async fn list_nodes(
        &self,
        prefix: &str,
        requirement: Requirement,
    ) -> Result<Vec<(NodeId, Observation<Node>)>, StorageError> {
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
            .write(&db_prefix("db"), &test_intent_id(intent_id), intent)
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
        writer: tx_id(&[1]),
    })
}

fn inline_live(key: &[u8], value: &[u8]) -> LeafEntry {
    LeafEntry::new(key).with_current(CurrentState::Inline {
        writer: tx_id(&[1]),
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
        .with_right_sibling(right.map(test_node_id))
}

/// The topology policy, the node sizes, and the inline policy of a test
/// restructurer.
#[derive(Clone, Copy)]
struct TestTopology {
    policy: TopologyPolicy,
    sizes: NodeSizePolicy,
    inline: InlinePolicy,
}

impl TestTopology {
    fn size_causes(sizes: NodeSizePolicy) -> Self {
        Self {
            policy: TopologyPolicy::SizeCauses,
            sizes,
            inline: InlinePolicy::default(),
        }
    }

    fn avoidable_time(sizes: NodeSizePolicy) -> Self {
        Self {
            policy: TopologyPolicy::AvoidableTime,
            ..Self::size_causes(sizes)
        }
    }

    fn with_inline(self, inline: InlinePolicy) -> Self {
        Self { inline, ..self }
    }
}

fn restructurer(store: &TestStore, bg: &Arc<Background>, policy: NodeSizePolicy) -> Restructurer {
    restructurer_with_topology(store, bg, TestTopology::size_causes(policy))
}

fn restructurer_with_topology(
    store: &TestStore,
    bg: &Arc<Background>,
    topology: TestTopology,
) -> Restructurer {
    restructurer_with_topology_and_hints(store, bg, topology, GcHints::default())
}

fn restructurer_with_topology_and_hints(
    store: &TestStore,
    bg: &Arc<Background>,
    topology: TestTopology,
    gc_hints: GcHints,
) -> Restructurer {
    let mon = store.foundation.monitor_for(
        bg,
        RetrySchedule::default(),
        crate::monitor::ProtocolTiming::default(),
    );
    restructurer_with_monitor_and_hints(store, bg, mon, topology, gc_hints)
}

fn restructurer_with_monitor(
    store: &TestStore,
    bg: &Arc<Background>,
    mon: Monitor,
    topology: TestTopology,
) -> Restructurer {
    restructurer_with_monitor_and_hints(store, bg, mon, topology, GcHints::default())
}

fn restructurer_with_monitor_and_hints(
    store: &TestStore,
    bg: &Arc<Background>,
    mon: Monitor,
    topology: TestTopology,
    gc_hints: GcHints,
) -> Restructurer {
    let parts = TopologyParts::new(
        topology.policy,
        topology.sizes,
        topology.inline,
        mon.clone(),
    );
    restructurer_with_parts(store, bg, mon, parts, gc_hints)
}

fn restructurer_with_parts(
    store: &TestStore,
    bg: &Arc<Background>,
    mon: Monitor,
    parts: TopologyParts,
    gc_hints: GcHints,
) -> Restructurer {
    let key_state = KeyStateResolver::new(mon.clone());
    let coord = LeafCoordinator::with_hinter(
        store.nodes.clone(),
        key_state.clone(),
        mon.clone(),
        RetryTiming::default(),
        parts.sizes,
        Arc::new(parts.hints.clone()),
    );
    Restructurer::with_parts(
        Arc::downgrade(bg),
        TopologyMembership::new(store.records.clone(), mon.clone(), RetrySchedule::default()),
        store.nodes.clone(),
        store.intent_store.clone(),
        store.timeline.clone(),
        mon,
        key_state,
        db_prefix("db"),
        coord,
        parts,
        gc_hints,
    )
}

// A restructurer with the avoidable time policy over leaves of `sizes`, and
// the policy, whose measurements a test reads.
fn avoidable_time_restructurer(
    store: &TestStore,
    bg: &Arc<Background>,
    sizes: NodeSizePolicy,
) -> (Restructurer, Arc<AvoidableTimePolicy>) {
    let mon = store.foundation.monitor_for(
        bg,
        RetrySchedule::default(),
        crate::monitor::ProtocolTiming::default(),
    );
    let queue = MaintenanceCandidates::new();
    let settings = TopologySettings {
        sizes,
        inline: InlinePolicy::default(),
    };
    let policy = Arc::new(AvoidableTimePolicy::new(settings, queue.clone()));
    let parts = TopologyParts::of(queue, sizes, policy.clone());
    let restructurer = restructurer_with_parts(store, bg, mon, parts, GcHints::default());
    (restructurer, policy)
}

fn restructurer_and_monitor(
    store: &TestStore,
    bg: &Arc<Background>,
    policy: NodeSizePolicy,
) -> (Restructurer, Monitor) {
    let mon = store.foundation.monitor_for(
        bg,
        RetrySchedule::default(),
        crate::monitor::ProtocolTiming::default(),
    );
    let restructurer =
        restructurer_with_monitor(store, bg, mon.clone(), TestTopology::size_causes(policy));
    (restructurer, mon)
}

/// Splits the node at `path` with its own topology participant, as a sweep
/// does after it located the node.
async fn split_path(
    sp: &Restructurer,
    path: &ObjectPath,
    reason: &SplitReason,
) -> Result<(), TransError> {
    sp.changes
        .split_path(path, reason, StructuralTopology::Owned)
        .await
}

fn leaf_with_membership_reader(keys: &[&[u8]], holder: &TxId) -> Node {
    let mut node = leaf_node(keys, None, None);
    node.add_membership_reader(*holder);
    node
}

fn leaf_with_locked_entry(keys: &[&[u8]], holder: &TxId) -> Node {
    let mut entries: Vec<_> = keys.iter().map(|key| live(key)).collect();
    entries[0].replace_write_lock(*holder);
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
        source_node_id: Some(test_node_id(source)),
        source_revision: superseded_source_revision(),
        change: StructuralChange::Split {
            created_node_ids: vec![test_node_id(right)],
            split_key: split_key.to_vec(),
        },
        participant_id: tx_id(b"structural-participant"),
        phase: StructuralIntentPhase::Ready,
    }
}

fn test_index(children: &[(&[u8], &str)]) -> IndexNode {
    IndexNode::from_children(
        children
            .iter()
            .map(|(separator, child)| (separator.to_vec(), test_node_id(child))),
    )
}

// A committed key that a recent transaction `writer` wrote, so that it is part
// of a write span with the other keys of `writer` (ADR-084).
fn written_by(key: &[u8], writer: &TxId) -> LeafEntry {
    LeafEntry::new(key).with_current(CurrentState::External { writer: *writer })
}

// Queues one change that a topology rule asked for, as the avoidable time
// policy does at the end of a window.
fn push_request(queue: &MaintenanceCandidates, request: RuleRequest) {
    let (path, cause) = crate::structural::policy::request_candidate(request);
    queue.push_expiring(&path, cause, crate::structural::policy::DEMAND_MAX_AGE);
}

// A recent transaction identity.
fn recent_writer() -> TxId {
    TxId::new_at(rt::system_now())
}

// A recent transaction identity that opened an owner operation in the
// database instance of `mon`, so that it is a local writer there (ADR-085).
fn local_writer(mon: &Monitor) -> TxId {
    let writer = recent_writer();
    mon.begin_owner_operation(&writer).unwrap().complete();
    writer
}
