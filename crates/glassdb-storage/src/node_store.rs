//! Typed persistence for B-link tree nodes.
//!
//! Tree roots (`_r`) and standalone nodes (`_n/<node-id>`) are the coordination
//! units. Mutations use create-if-absent, revision-conditional replace,
//! or exact-revision deletion (ADR-023/ADR-031/ADR-042), all through the decoded
//! [`CachedStore`].

use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;

use glassdb_backend as backend;
use glassdb_data::{CollectionAddress, NodeId, ObjectPath, TxId};

use crate::cached_store::{
    CachedStore, CasResult, CasRetries, Codec, Converged, CutKey, CutSearch, CutTarget, Decision,
    Observation, ObservationCheck, Requirement,
};
use crate::error::StorageError;
use crate::leaf::LeafBody;
use crate::lock::LockType;
use crate::node::{Node, NodeLocks};
use crate::stale_view::ViewRead;

const NODE_LIST_PAGE_SIZE: usize = 128;

/// Reads and compare-and-swaps B-link nodes.
#[derive(Clone)]
pub struct NodeStore {
    nodes: crate::cached_store::TypedCachedStore<Node>,
    parallelism: NonZeroUsize,
    reads: NodeReads,
}

/// Reads B-link nodes, and never writes them. Routing reads through it.
///
/// For a stale read-only transaction of a stale view, it reads each leaf at
/// the floor of the view when the decoded cache has a state that covers the
/// floor, and marks each leaf that it reads, so that the cache keeps the
/// earlier states of the leaf (ADR-103). It reads index nodes at their latest
/// state.
#[derive(Clone)]
pub struct NodeReads {
    nodes: crate::cached_store::TypedCachedStore<Node>,
    view: Option<ViewRead>,
}

/// One bounded page of standalone node observations.
pub struct NodePage {
    pub nodes: Vec<(NodeId, Observation<Node>)>,
    pub next: Option<backend::ListCursor>,
}

/// A B-link leaf loaded for one coordination round.
pub struct LoadedLeaf {
    edit: LeafEdit,
}

/// A leaf mutation bound to the exact node state it will compare-and-swap.
pub struct LeafEdit {
    observation: LeafObservation,
    node: Node,
}

/// The exact node state from which a leaf was decoded.
pub type LeafObservation = Observation<Node>;

/// The outcome of checking a retained leaf observation.
pub type LeafObservationCheck = ObservationCheck<Node>;

impl LoadedLeaf {
    /// Returns the object path of this loaded leaf.
    pub fn path(&self) -> &ObjectPath {
        self.edit.path()
    }

    /// Returns the complete node carrying this leaf's entries and coordination.
    pub fn node(&self) -> &Node {
        self.edit.node()
    }

    /// Returns the loaded leaf entries.
    pub fn entries(&self) -> &LeafBody {
        self.edit.entries()
    }

    /// Returns the loaded node-level coordination state.
    pub fn locks(&self) -> &NodeLocks {
        self.edit.locks()
    }

    /// Returns the exact observation from which this leaf was loaded.
    pub fn observation(&self) -> &LeafObservation {
        self.edit.observation()
    }

    /// Reports whether this loaded leaf still covers `key` below its high-key.
    /// A `false` result means a split moved `key` to a right
    /// sibling after the key was routed here, so a caller must re-descend.
    pub fn covers(&self, key: &[u8]) -> bool {
        self.edit.covers(key)
    }

    /// Converts this loaded leaf into an observation-bound mutation.
    pub fn into_edit(self) -> LeafEdit {
        self.edit
    }
}

impl LeafEdit {
    /// Returns the immutable object path to which this edit is bound.
    pub fn path(&self) -> &ObjectPath {
        self.observation.path()
    }

    /// Returns the complete node carrying this edit's topology and contents.
    pub fn node(&self) -> &Node {
        &self.node
    }

    /// Returns the staged leaf entries.
    pub fn entries(&self) -> &LeafBody {
        self.node
            .as_leaf()
            .expect("LeafEdit is always created from a leaf node")
    }

    /// Replaces the staged leaf entries without changing topology.
    pub fn set_entries(&mut self, entries: LeafBody) {
        self.node
            .set_leaf(entries)
            .expect("LeafEdit is always created from a leaf node");
    }

    /// Returns the staged node-level coordination state.
    pub fn locks(&self) -> &NodeLocks {
        self.node.locks()
    }

    /// Replaces the staged node-level coordination state without changing topology.
    pub fn set_locks(&mut self, locks: NodeLocks) {
        self.node.set_locks(locks);
    }

    /// Returns the exact observation against which this edit will commit.
    pub fn observation(&self) -> &LeafObservation {
        &self.observation
    }

    /// Reports whether this edited leaf still covers `key`.
    pub fn covers(&self, key: &[u8]) -> bool {
        self.node.covers(key)
    }
}

impl Codec for Node {
    type Value = Node;

    fn decode(_path: &ObjectPath, body: &[u8]) -> Result<Self::Value, StorageError> {
        Node::decode(body)
    }

    fn encode(_path: &ObjectPath, node: &Self::Value) -> Result<Vec<u8>, StorageError> {
        Ok(node.encode())
    }

    fn size(node: &Self::Value) -> usize {
        node.encoded_len()
    }

    fn accepts(path: &ObjectPath) -> bool {
        matches!(path, ObjectPath::TreeRoot { .. } | ObjectPath::Node { .. })
    }

    fn name() -> &'static str {
        "node"
    }
}

impl NodeReads {
    /// Returns the reads of one stale read-only transaction of a stale view
    /// over the nodes of `store` (ADR-103).
    pub fn for_view(store: &NodeStore, view: ViewRead) -> Self {
        Self {
            nodes: store.nodes.clone(),
            view: Some(view),
        }
    }

    /// Captures a currentness barrier after the node observations made so far.
    pub(crate) fn currentness_barrier(&self) -> crate::CurrentnessBarrier {
        self.nodes.currentness_barrier()
    }

    /// Loads the fixed tree root's exact observation, including absence.
    pub async fn load_root_state(
        &self,
        collection: &CollectionAddress,
        requirement: Requirement,
    ) -> Result<LeafObservation, StorageError> {
        self.load_node_at_state(
            &ObjectPath::TreeRoot {
                collection: collection.clone(),
            },
            requirement,
        )
        .await
    }

    /// Loads an exact `_r` or `_n/<node-id>` node observation, including absence.
    pub async fn load_node_at_state(
        &self,
        path: &ObjectPath,
        requirement: Requirement,
    ) -> Result<LeafObservation, StorageError> {
        validate_node_path(path)?;
        let Some(view) = self.view else {
            return self.nodes.read(path, requirement).await;
        };
        let observed = match view.point() {
            Some(point) => self.nodes.read_at(path, requirement, point).await?,
            None => self.nodes.read(path, requirement).await?,
        };
        match observed.value() {
            Some(node) if node.as_leaf().is_some() => self.nodes.keep_earlier_states(path),
            // A split turns the tree root into an index node in place. Its
            // index states need no earlier states.
            Some(_) if matches!(path, ObjectPath::TreeRoot { .. }) => {
                self.nodes.drop_earlier_states(path)
            }
            _ => {}
        }
        Ok(observed)
    }

    /// Loads the non-root node's exact observation.
    pub async fn load_node_state(
        &self,
        collection: &CollectionAddress,
        id: &NodeId,
        requirement: Requirement,
    ) -> Result<LeafObservation, StorageError> {
        let path = ObjectPath::Node {
            collection: collection.clone(),
            id: *id,
        };
        let observed = self.load_node_at_state(&path, requirement).await?;
        if observed.is_absent() {
            return Err(StorageError::NotFound);
        }
        Ok(observed)
    }
}

impl From<NodeStore> for NodeReads {
    fn from(store: NodeStore) -> Self {
        store.reads
    }
}

impl NodeStore {
    /// Creates a node store that reads and compare-and-swaps through `objects`.
    pub fn new(objects: CachedStore, parallelism: NonZeroUsize) -> Self {
        let nodes = objects.typed();
        Self {
            reads: NodeReads {
                nodes: nodes.clone(),
                view: None,
            },
            nodes,
            parallelism,
        }
    }

    /// Checks whether a retained leaf observation satisfies `requirement`.
    pub async fn check_leaf_current(
        &self,
        observed: &LeafObservation,
        requirement: Requirement,
    ) -> Result<LeafObservationCheck, StorageError> {
        self.nodes.check_current(observed, requirement).await
    }

    /// Checks retained leaf observations against `requirement` with bounded
    /// work on distinct paths.
    pub async fn check_leaves_current(
        &self,
        observations: &[LeafObservation],
        requirement: Requirement,
    ) -> Vec<Result<LeafObservationCheck, StorageError>> {
        self.nodes
            .check_many_current(observations, requirement, self.parallelism)
            .await
    }

    /// Starts a cut search on the timeline of the node cache (ADR-097).
    pub fn cut_search(&self) -> CutSearch {
        self.nodes.cut_search()
    }

    /// Adds the interval of `key` to a cut search, from the leaf states of the
    /// path of `observed` that show the key unchanged (ADR-103).
    pub fn add_key_to_cut(
        &self,
        search: &mut CutSearch,
        observed: &LeafObservation,
        key: &[u8],
    ) -> Result<CutKey, StorageError> {
        self.nodes
            .add_part_to_cut(search, observed, |node| key_state(node, key))
    }

    /// Checks the leaf states of `keys`, which do not reach `target`. Reports
    /// whether every key is unchanged after the barrier of the target: its
    /// leaf state is still current, or the current leaf state shows the key
    /// unchanged (ADR-103).
    pub async fn refresh_keys_for_cut(
        &self,
        keys: &[(LeafObservation, Vec<u8>)],
        target: &CutTarget,
    ) -> Result<bool, StorageError> {
        let observations: Vec<_> = keys.iter().map(|(observed, _)| observed.clone()).collect();
        let checks = self
            .check_leaves_current(&observations, Requirement::after(target.barrier()))
            .await;
        for ((observed, key), check) in keys.iter().zip(checks) {
            if let LeafObservationCheck::Changed(current) = check? {
                let before = observed.value().and_then(|node| key_state(node, key));
                let after = current.value().and_then(|node| key_state(node, key));
                if before.is_none() || before != after {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    /// Adds retained leaf observations to a cut search (ADR-097).
    pub fn add_leaves_to_cut(
        &self,
        search: &mut CutSearch,
        observations: &[LeafObservation],
    ) -> Result<(), StorageError> {
        self.nodes.add_to_cut(search, observations)
    }

    /// Checks the leaf observations that do not reach a cut target.
    pub async fn refresh_leaves_for_cut(
        &self,
        observations: &[LeafObservation],
        target: &CutTarget,
    ) -> Result<bool, StorageError> {
        self.nodes
            .refresh_for_cut(observations, target, self.parallelism)
            .await
    }

    /// Loads the fixed tree root's exact observation, including absence.
    pub async fn load_root_state(
        &self,
        collection: &CollectionAddress,
        requirement: Requirement,
    ) -> Result<LeafObservation, StorageError> {
        self.reads.load_root_state(collection, requirement).await
    }

    /// Loads an exact `_r` or `_n/<node-id>` node observation, including absence.
    pub async fn load_node_at_state(
        &self,
        path: &ObjectPath,
        requirement: Requirement,
    ) -> Result<LeafObservation, StorageError> {
        self.reads.load_node_at_state(path, requirement).await
    }

    /// Loads an existing node at an exact `_r` or `_n/<node-id>` path.
    pub async fn load_node_at(
        &self,
        path: &ObjectPath,
        requirement: Requirement,
    ) -> Result<(Node, LeafObservation), StorageError> {
        let observed = self.load_node_at_state(path, requirement).await?;
        let node = observed
            .value()
            .map(|node| node.as_ref().clone())
            .ok_or(StorageError::NotFound)?;
        Ok((node, observed))
    }

    /// Loads the non-root node's exact observation.
    pub async fn load_node_state(
        &self,
        collection: &CollectionAddress,
        id: &NodeId,
        requirement: Requirement,
    ) -> Result<LeafObservation, StorageError> {
        self.reads
            .load_node_state(collection, id, requirement)
            .await
    }

    /// Loads the non-root node named `id` (`{prefix}/_n/<node-id>`, ADR-031). A
    /// [`StorageError::NotFound`] means the node is missing — a dangling child or
    /// right-sibling reference, which a descent surfaces rather than silently
    /// skips.
    pub async fn load_node(
        &self,
        collection: &CollectionAddress,
        id: &NodeId,
        requirement: Requirement,
    ) -> Result<(Node, LeafObservation), StorageError> {
        let observation = self.load_node_state(collection, id, requirement).await?;
        let node = observation
            .value()
            .expect("load_node_state rejects absence")
            .as_ref()
            .clone();
        Ok((node, observation))
    }

    /// Compare-and-swaps the non-root node named `id`. `expected = None` means
    /// create-if-absent (a freshly split-out sibling). Returns `false` on a
    /// precondition miss, `true` on success.
    pub async fn store_node(
        &self,
        collection: &CollectionAddress,
        id: &NodeId,
        node: &Node,
        expected: Option<&LeafObservation>,
    ) -> Result<bool, StorageError> {
        let path = ObjectPath::Node {
            collection: collection.clone(),
            id: *id,
        };
        let res = match expected {
            Some(observed) if observed.path() == &path => {
                debug_assert!(no_key_lock_outlives_membership(observed, node));
                let node = following(observed, node.clone())?;
                self.nodes.replace(observed, Arc::new(node)).await
            }
            Some(_) => return Err(StorageError::other("node observation path changed")),
            None => {
                self.nodes
                    .create(path, None, Arc::new(node.clone().created()))
                    .await
            }
        };
        match res {
            Ok(CasResult::Applied(_)) => Ok(true),
            Ok(CasResult::Rejected) | Err(StorageError::NotFound) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Compare-and-swaps an exact `_r` or `_n/<node-id>` node path, reporting the
    /// observation of the state it installed, or `None` on a lost CAS.
    ///
    /// Callers that go on to mutate the node need that observation rather than a
    /// re-read: it is the only state that pairs the body they wrote with the
    /// revision they wrote it at.
    pub async fn store_node_at(
        &self,
        path: &ObjectPath,
        node: &Node,
        expected: &LeafObservation,
    ) -> Result<Option<LeafObservation>, StorageError> {
        validate_node_path(path)?;
        if expected.path() != path {
            return Err(StorageError::other("node observation path changed"));
        }
        debug_assert!(no_key_lock_outlives_membership(expected, node));
        let node = following(expected, node.clone())?;
        match self.nodes.replace(expected, Arc::new(node)).await {
            Ok(CasResult::Applied(receipt)) => Ok(Some(receipt.into_installed())),
            Ok(CasResult::Rejected) | Err(StorageError::NotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Changes the `_r` or `_n/<node-id>` node at `path` with one writer until
    /// `decide` gives a result: the step loads the node, `decide` changes a
    /// copy of it, and a [`Decision::Write`] lands with a CAS on the loaded
    /// state.
    ///
    /// The first load is at [`Requirement::ANY`], so a cached state costs no
    /// backend call, and so is each load after a CAS. A CAS that loses loads
    /// the node again. A [`Decision::Done`] and a missing node need a state
    /// that satisfies `proof`, so the step loads again at `proof` when the
    /// loaded state does not satisfy it. [`Decision::Reload`] and
    /// [`Decision::Wait`] set the requirement of the next load. A landed CAS proves its own premise, because the CAS
    /// rejects a stale state. Give `proof` from the observation or the event
    /// that the decision depends on, for example a barrier captured after the
    /// read that shows the premise. Use [`Requirement::ANY`] only when this
    /// cache already holds a state that is as new as the premise, for example
    /// because this caller wrote it.
    ///
    /// An in-doubt CAS is an error.
    pub async fn converge<T, E, F, Fut>(
        &self,
        path: &ObjectPath,
        proof: Requirement,
        retries: CasRetries,
        mut decide: F,
    ) -> Result<Converged<Node, T>, E>
    where
        E: From<StorageError>,
        F: FnMut(Node, LeafObservation) -> Fut,
        Fut: Future<Output = Result<Decision<Node, T>, E>>,
    {
        validate_node_path(path)?;
        self.nodes
            .converge(path.clone(), proof, retries, |node, observation| {
                let decided = decide(node, observation.clone());
                async move {
                    let decision = decided.await?;
                    Ok(match decision {
                        Decision::Write(node, value) => {
                            debug_assert!(no_key_lock_outlives_membership(&observation, &node));
                            Decision::Write(following(&observation, node)?, value)
                        }
                        Decision::Advance(node) => {
                            debug_assert!(no_key_lock_outlives_membership(&observation, &node));
                            Decision::Advance(following(&observation, node)?)
                        }
                        decision => decision,
                    })
                }
            })
            .await
    }

    /// Deletes the exact observed standalone node, converging if it is missing.
    pub async fn delete_node(&self, expected: &Observation<Node>) -> Result<(), StorageError> {
        self.nodes.delete(expected).await?;
        Ok(())
    }

    /// Lists observed standalone nodes under one collection-ID prefix
    /// prefix, including temporarily unreachable structural nodes.
    ///
    /// Uses the same body requirements and absence rules as [`Self::scan_nodes`].
    pub async fn list_nodes(
        &self,
        collection: &CollectionAddress,
        requirement: Requirement,
    ) -> Result<Vec<(NodeId, Observation<Node>)>, StorageError> {
        let mut cursor = None;
        let mut nodes = Vec::new();
        loop {
            let page = self
                .scan_nodes(collection, cursor.as_ref(), requirement)
                .await?;
            nodes.extend(page.nodes);
            match page.next {
                Some(next) => cursor = Some(next),
                None => return Ok(nodes),
            }
        }
    }

    /// Reads one page of standalone nodes, including unreachable structural nodes.
    ///
    /// Listing names does not validate their bodies. Each body read, including
    /// an absent result omitted from the page, uses `requirement`. With `ANY`,
    /// cached absence can therefore omit a listed node. Callers must justify
    /// that omission from publication and lifecycle rules. Pages are not a snapshot.
    pub async fn scan_nodes(
        &self,
        collection: &CollectionAddress,
        cursor: Option<&backend::ListCursor>,
        requirement: Requirement,
    ) -> Result<NodePage, StorageError> {
        let list_prefix = ObjectPath::nodes_prefix(collection);
        let limit = backend::ListLimit::new(NODE_LIST_PAGE_SIZE).unwrap();
        let page = self.nodes.list(&list_prefix, cursor, limit).await?;
        let mut nodes = Vec::new();
        for path in page.objects {
            let ObjectPath::Node {
                collection: listed_collection,
                id,
            } = path.object_path()
            else {
                return Err(StorageError::other("node listing returned a non-node path"));
            };
            if listed_collection != collection {
                return Err(StorageError::other(
                    "node listing returned a different collection",
                ));
            }
            let id = *id;
            let observed = self.nodes.read(path, requirement).await?;
            if observed.exists() {
                nodes.push((id, observed));
            }
        }
        Ok(NodePage {
            nodes,
            next: page.next,
        })
    }

    /// Loads the leaf node at `_r` or `_n/<node-id>`.
    pub async fn load_leaf(
        &self,
        path: &ObjectPath,
        requirement: Requirement,
    ) -> Result<LoadedLeaf, StorageError> {
        validate_node_path(path)?;
        let observed = self.nodes.read(path, requirement).await?;
        match observed.value() {
            Some(node) => {
                let node = node.as_ref().clone();
                node.as_leaf().ok_or(StorageError::Precondition)?;
                Ok(LoadedLeaf {
                    edit: LeafEdit {
                        observation: observed,
                        node,
                    },
                })
            }
            None => Err(StorageError::NotFound),
        }
    }

    /// Compare-and-swaps a leaf edit, retaining proof of the successful mutation.
    pub async fn commit_leaf(&self, edit: LeafEdit) -> Result<CasResult<Node>, StorageError> {
        let LeafEdit { observation, node } = edit;
        debug_assert!(no_key_lock_outlives_membership(&observation, &node));
        let node = following(&observation, node)?;
        let result = self.nodes.replace(&observation, Arc::new(node)).await;
        match result {
            Ok(result) => Ok(result),
            Err(StorageError::NotFound) => Ok(CasResult::Rejected),
            Err(error) => Err(error),
        }
    }

    /// Loads the fixed tree root under `prefix`.
    pub async fn load_root(
        &self,
        collection: &CollectionAddress,
        requirement: Requirement,
    ) -> Result<(Node, LeafObservation), StorageError> {
        self.load_node_at(
            &ObjectPath::TreeRoot {
                collection: collection.clone(),
            },
            requirement,
        )
        .await
    }

    /// Compare-and-swaps the fixed tree root.
    pub async fn store_root(
        &self,
        collection: &CollectionAddress,
        root: &Node,
        expected: &LeafObservation,
    ) -> Result<bool, StorageError> {
        Ok(self
            .store_node_at(
                &ObjectPath::TreeRoot {
                    collection: collection.clone(),
                },
                root,
                expected,
            )
            .await?
            .is_some())
    }

    /// Creates the fixed tree root if absent.
    pub async fn create_root(
        &self,
        collection: &CollectionAddress,
        root: &Node,
    ) -> Result<bool, StorageError> {
        let path = ObjectPath::TreeRoot {
            collection: collection.clone(),
        };
        match self
            .nodes
            .create(path, None, Arc::new(root.clone().created()))
            .await
        {
            Ok(CasResult::Applied(_)) => Ok(true),
            Ok(CasResult::Rejected) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Creates a fixed tree root if absent and returns its exact installed
    /// observation. `None` means another object already occupies the path.
    pub async fn create_root_observed(
        &self,
        collection: &CollectionAddress,
        root: &Node,
    ) -> Result<Option<Observation<Node>>, StorageError> {
        let path = ObjectPath::TreeRoot {
            collection: collection.clone(),
        };
        match self
            .nodes
            .create(path, None, Arc::new(root.clone().created()))
            .await?
        {
            CasResult::Applied(receipt) => Ok(Some(receipt.into_installed())),
            CasResult::Rejected => Ok(None),
        }
    }

    /// Deletes the exact observed fixed tree root.
    pub async fn delete_root(&self, expected: &Observation<Node>) -> Result<(), StorageError> {
        self.delete_node(expected).await
    }
}

/// Returns `node` as the change that follows the observed state, so that the
/// replace never restores an earlier state of the node (ADR-105).
fn following(expected: &LeafObservation, node: Node) -> Result<Node, StorageError> {
    let current = expected
        .value()
        .ok_or_else(|| StorageError::other("a node replace needs an observed node"))?;
    Ok(node.following(current))
}

/// Tells whether no holder left the membership write lock of the replaced node
/// while it still holds a write lock or create lock in the new leaf.
///
/// Range scans decide the key membership from the leaf for a key holder that
/// holds no membership lock (ADR-077), so such a write would hide a committed
/// create or delete from them. Only an engine bug can break the rule, so callers
/// run the check inside `debug_assert!`, which the tests use.
fn no_key_lock_outlives_membership(expected: &LeafObservation, node: &Node) -> bool {
    let (Some(old), Some(leaf)) = (expected.value(), node.as_leaf()) else {
        return true;
    };
    let before = old.membership_lock();
    if before.lock_type() != LockType::Write {
        return true;
    }
    let after = node.membership_lock();
    let left: Vec<&TxId> = before
        .holders()
        .iter()
        .filter(|holder| !(after.lock_type() == LockType::Write && after.contains(holder)))
        .collect();
    !leaf.entries().any(|entry| {
        matches!(entry.lock_type(), LockType::Write | LockType::Create)
            && left.iter().any(|holder| entry.is_locked_by(holder))
    })
}

fn validate_node_path(path: &ObjectPath) -> Result<(), StorageError> {
    match path {
        ObjectPath::TreeRoot { .. } | ObjectPath::Node { .. } => Ok(()),
        _ => Err(StorageError::other("expected a tree-node object path")),
    }
}

/// The state of `key` in one leaf state, for the key interval of a read
/// (ADR-103): its committed writer, or the membership generation of the leaf
/// for a key with no writer. A writer does not come back after the key
/// changes, and the generation only grows, so two leaf states with the same
/// key state show the key unchanged between them. It is `None` when the leaf
/// state does not hold the key, or when an exclusive holder can change the key
/// without a new leaf state.
fn key_state(node: &Node, key: &[u8]) -> Option<(Option<TxId>, Option<u64>)> {
    let leaf = node.as_leaf()?;
    if node.is_below_range(key) || !node.covers(key) {
        return None;
    }
    let entry = leaf.lookup(key);
    if entry.is_some_and(|entry| {
        matches!(entry.lock_type(), LockType::Write | LockType::Create)
            && !entry.lock_holders().is_empty()
    }) {
        return None;
    }
    match entry.and_then(|entry| entry.current.writer()) {
        Some(writer) => Some((Some(*writer), None)),
        None => Some((None, Some(node.membership_generation()))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cached_store::Revision;
    use crate::node::IndexNode;
    use crate::stale_view::MonotonicFloor;
    use crate::timeline::StalenessCutoff;
    use crate::{CurrentState, LeafEntry, Timeline};

    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{OpLog, RecordingBackend};
    use glassdb_data::TxId;

    struct TestStore {
        nodes: NodeStore,
        timeline: Timeline,
    }

    impl std::ops::Deref for TestStore {
        type Target = NodeStore;

        fn deref(&self) -> &Self::Target {
            &self.nodes
        }
    }

    fn store_over(backend: Arc<dyn Backend>) -> TestStore {
        let timeline = Timeline::new();
        let objects = CachedStore::new(backend, 1 << 20, timeline.clone(), None);
        let nodes = NodeStore::new(objects, std::num::NonZeroUsize::MIN);
        TestStore { nodes, timeline }
    }

    fn count(log: &OpLog, op: &str) -> usize {
        log.lock().unwrap().iter().filter(|r| r.op == op).count()
    }

    fn collection() -> CollectionAddress {
        CollectionAddress::root("coll")
    }

    fn node_id(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 16])
    }

    fn node_path(byte: u8) -> ObjectPath {
        ObjectPath::Node {
            collection: collection(),
            id: node_id(byte),
        }
    }

    // Use a separate store so the reader begins cold. Creating directly avoids
    // a seeding read, keeping the operation log limited to reader traffic.
    async fn seed_empty_leaf(backend: &Arc<dyn Backend>, id: &NodeId) {
        let store = store_over(backend.clone());
        assert!(
            store
                .store_node(&collection(), id, &Node::leaf(LeafBody::new()), None)
                .await
                .unwrap()
        );
    }

    #[test]
    fn key_state_is_the_writer_or_the_generation_of_an_absent_key() {
        let writer = TxId::with_priority(1, b"writer");
        let holder = TxId::with_priority(2, b"holder");
        let leaf = |entries: Vec<LeafEntry>| {
            Node::leaf(LeafBody::from_entries(entries)).with_high_key(Some(b"m".to_vec()))
        };
        let written = LeafEntry::new(b"a".to_vec()).with_current(CurrentState::External { writer });
        let node = leaf(vec![written.clone()]);
        assert_eq!(key_state(&node, b"a"), Some((Some(writer), None)));
        // An absent key has the generation of the leaf, which changes with
        // its key membership.
        let generation = node.membership_generation();
        assert_eq!(key_state(&node, b"b"), Some((None, Some(generation))));
        let mut grown = node.clone();
        let mut locks = grown.locks().clone();
        locks.advance_membership_generation();
        grown.set_locks(locks);
        assert_ne!(key_state(&grown, b"b"), key_state(&node, b"b"));
        // The leaf state does not hold a key at or after its high key.
        assert_eq!(key_state(&node, b"z"), None);

        // A read lock does not change the key, and a write lock can.
        let mut read_locked = written.clone();
        read_locked.acquire_read_lock(holder);
        assert_eq!(
            key_state(&leaf(vec![read_locked]), b"a"),
            Some((Some(writer), None))
        );
        let mut write_locked = written;
        write_locked.replace_write_lock(holder);
        assert_eq!(key_state(&leaf(vec![write_locked]), b"a"), None);
    }

    /// Writes a leaf, observes it twice, raises a view floor between the two
    /// observations, and replaces the leaf. When `view_read` is set, the first
    /// observation is a read through the view. Returns the first revision, the
    /// reads of the view at the floor, the store, and the floor, which must
    /// stay live while the test reads.
    async fn replaced_after_a_floor(
        view_read: bool,
    ) -> (Option<Revision>, NodeReads, NodeStore, MonotonicFloor) {
        let timeline = Timeline::new();
        let objects = CachedStore::new(
            Arc::new(MemoryBackend::new()),
            1 << 20,
            timeline.clone(),
            None,
        );
        let floor = objects.view_floors().new_floor();
        let store = NodeStore::new(objects, std::num::NonZeroUsize::MIN);
        let id = node_id(1);
        let empty = Node::leaf(LeafBody::new());
        assert!(
            store
                .store_node(&collection(), &id, &empty, None)
                .await
                .unwrap()
        );
        let fresh = Requirement::after(timeline.currentness_barrier());
        if view_read {
            NodeReads::for_view(&store, floor.view_read(StalenessCutoff::ANY))
                .load_node_state(&collection(), &id, fresh)
                .await
                .unwrap();
        } else {
            store
                .load_node_state(&collection(), &id, fresh)
                .await
                .unwrap();
        }
        // A point between two observations is in the interval of the state.
        let mid = timeline.now();
        let observed = store
            .load_node_state(
                &collection(),
                &id,
                Requirement::after(timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        floor.raise(mid);
        let written = Node::leaf(LeafBody::from_entries(vec![LeafEntry::new(b"a".to_vec())]));
        assert!(
            store
                .store_node(&collection(), &id, &written, Some(&observed))
                .await
                .unwrap()
        );
        let reads = NodeReads::for_view(&store, floor.view_read(StalenessCutoff::ANY));
        (observed.revision().cloned(), reads, store, floor)
    }

    #[tokio::test]
    async fn a_view_reads_a_leaf_that_it_read_before_at_its_floor() {
        let (first, reads, store, _floor) = replaced_after_a_floor(true).await;
        let by_id = reads
            .load_node_state(&collection(), &node_id(1), Requirement::ANY)
            .await
            .unwrap();
        let by_path = reads
            .load_node_at_state(&node_path(1), Requirement::ANY)
            .await
            .unwrap();
        assert_eq!(by_id.revision(), first.as_ref());
        assert_eq!(by_path.revision(), first.as_ref());
        let latest = store
            .load_node_state(&collection(), &node_id(1), Requirement::ANY)
            .await
            .unwrap();
        assert_ne!(latest.revision(), first.as_ref());
    }

    #[tokio::test]
    async fn the_cache_keeps_no_earlier_state_of_a_leaf_that_no_view_read() {
        let (first, reads, _store, _floor) = replaced_after_a_floor(false).await;
        let read = reads
            .load_node_state(&collection(), &node_id(1), Requirement::ANY)
            .await
            .unwrap();
        assert_ne!(read.revision(), first.as_ref());
    }

    #[tokio::test]
    async fn a_tree_root_that_became_an_index_keeps_no_earlier_states() {
        let timeline = Timeline::new();
        let objects = CachedStore::new(
            Arc::new(MemoryBackend::new()),
            1 << 20,
            timeline.clone(),
            None,
        );
        let floor = objects.view_floors().new_floor();
        let store = NodeStore::new(objects, std::num::NonZeroUsize::MIN);
        let c = collection();
        let fresh = || Requirement::after(timeline.currentness_barrier());
        let view = || NodeReads::for_view(&store, floor.view_read(StalenessCutoff::ANY));
        assert!(
            store
                .create_root(&c, &Node::leaf(LeafBody::new()))
                .await
                .unwrap()
        );
        let leaf = view().load_root_state(&c, fresh()).await.unwrap();
        // A split turns the root into an index node in place.
        let index = |byte| Node::index(IndexNode::from_children([(b"".to_vec(), node_id(byte))]));
        assert!(store.store_root(&c, &index(1), &leaf).await.unwrap());
        view().load_root_state(&c, fresh()).await.unwrap();
        let mid = timeline.now();
        let first = store.load_root_state(&c, fresh()).await.unwrap();
        floor.raise(mid);
        assert!(store.store_root(&c, &index(2), &first).await.unwrap());

        let read = view().load_root_state(&c, Requirement::ANY).await.unwrap();
        assert_ne!(read.revision(), first.revision());
    }

    // ADR-109: a root that is created again by another create request does not
    // repeat the state of the first create, so a late CAS that expected that
    // state cannot land.
    #[tokio::test]
    async fn a_root_created_again_does_not_repeat_its_first_state() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let c = collection();
        assert!(
            store
                .create_root(&c, &Node::leaf(LeafBody::new()))
                .await
                .unwrap()
        );
        let (_, first) = store.load_root(&c, Requirement::ANY).await.unwrap();
        store.delete_root(&first).await.unwrap();
        assert!(
            store
                .create_root(&c, &Node::leaf(LeafBody::new()))
                .await
                .unwrap()
        );

        let index = Node::index(IndexNode::from_children([(b"".to_vec(), node_id(1))]));
        assert!(!store.store_root(&c, &index, &first).await.unwrap());
    }

    #[tokio::test]
    async fn hot_reload_checks_current_without_full_read() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        let path = node_path(1);
        seed_empty_leaf(&backend, &node_id(1)).await;

        let reader = store_over(backend);
        let first = reader
            .load_leaf(
                &path,
                Requirement::after(reader.timeline.currentness_barrier()),
            )
            .await
            .unwrap()
            .observation()
            .clone();
        assert_eq!(count(&log, "read"), 1, "cold load full-reads");
        assert_eq!(count(&log, "read_if_modified"), 0);

        let second = reader
            .load_leaf(
                &path,
                Requirement::after(reader.timeline.currentness_barrier()),
            )
            .await
            .unwrap()
            .observation()
            .clone();
        assert_eq!(count(&log, "read"), 1, "hot load must not full-read");
        assert_eq!(
            count(&log, "read_if_modified"),
            1,
            "hot load checks conditionally"
        );
        assert_eq!(first.revision(), second.revision());
    }

    #[tokio::test]
    async fn any_serves_cached_without_backend_op() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        let path = node_path(1);
        seed_empty_leaf(&backend, &node_id(1)).await;

        let reader = store_over(backend);
        reader
            .load_leaf(
                &path,
                Requirement::after(reader.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        assert_eq!(count(&log, "read"), 1);

        reader.load_leaf(&path, Requirement::ANY).await.unwrap();
        assert_eq!(count(&log, "read"), 1, "cached Any must not read");
        assert_eq!(
            count(&log, "read_if_modified"),
            0,
            "cached Any must not check the backend"
        );

        assert!(matches!(
            reader.load_leaf(&node_path(2), Requirement::ANY).await,
            Err(StorageError::NotFound)
        ));
        assert_eq!(count(&log, "read"), 2, "uncached Any falls through");
    }

    #[tokio::test]
    async fn batch_currentness_combines_exact_present_states_and_advances_all_evidence() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        seed_empty_leaf(&backend, &node_id(7)).await;

        let timeline = Timeline::new();
        // Separate caches model retained observations before and after
        // eviction, without mixing database-local timelines.
        let first_store = NodeStore::new(
            CachedStore::new(backend.clone(), 1 << 20, timeline.clone(), None),
            NonZeroUsize::MIN,
        );
        let second_store = NodeStore::new(
            CachedStore::new(backend, 1 << 20, timeline.clone(), None),
            NonZeroUsize::MIN,
        );
        let first = first_store
            .load_node_state(&collection(), &node_id(7), Requirement::ANY)
            .await
            .unwrap();
        let second = second_store
            .load_node_state(&collection(), &node_id(7), Requirement::ANY)
            .await
            .unwrap();
        assert_eq!(count(&log, "read"), 2);
        log.lock().unwrap().clear();

        assert!(first.same_state(&second));
        let bound = timeline.currentness_barrier();
        let checks = first_store
            .check_leaves_current(&[first.clone(), second.clone()], Requirement::after(bound))
            .await;

        assert!(
            checks
                .iter()
                .all(|check| matches!(check, Ok(LeafObservationCheck::Current)))
        );
        assert!(first.is_current_after(bound));
        assert!(second.is_current_after(bound));
        assert_eq!(count(&log, "read_if_modified"), 1);
    }

    #[tokio::test]
    async fn batch_currentness_does_not_advance_a_changed_revision() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        seed_empty_leaf(&backend, &node_id(7)).await;
        let reader = store_over(backend.clone());
        let old = reader
            .load_node_state(&collection(), &node_id(7), Requirement::ANY)
            .await
            .unwrap();

        let peer = store_over(backend);
        let mut edit = peer
            .load_leaf(&node_path(7), Requirement::ANY)
            .await
            .unwrap()
            .into_edit();
        edit.set_entries(LeafBody::from_entries([LeafEntry::new(b"new".as_slice())]));
        assert!(peer.commit_leaf(edit).await.unwrap().is_applied());

        let requirement = Requirement::after(reader.timeline.currentness_barrier());
        let current = reader
            .load_node_state(&collection(), &node_id(7), requirement)
            .await
            .unwrap();
        assert!(!old.satisfies(requirement));

        let checks = reader
            .check_leaves_current(&[current.clone(), old.clone(), old.clone()], requirement)
            .await;
        assert!(matches!(checks[0], Ok(LeafObservationCheck::Current)));
        for result in &checks[1..] {
            let Ok(LeafObservationCheck::Changed(changed)) = result else {
                panic!("the old revision must be reported as changed");
            };
            assert!(changed.same_state(&current));
            assert!(changed.satisfies(requirement));
        }
        assert!(!old.satisfies(requirement));
    }

    #[tokio::test]
    async fn batch_currentness_checks_independent_absences_separately() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        let timeline = Timeline::new();
        let first_store = NodeStore::new(
            CachedStore::new(backend.clone(), 0, timeline.clone(), None),
            NonZeroUsize::MIN,
        );
        let second_store = NodeStore::new(
            CachedStore::new(backend.clone(), 0, timeline.clone(), None),
            NonZeroUsize::MIN,
        );
        let validator = NodeStore::new(
            CachedStore::new(backend, 1 << 20, timeline.clone(), None),
            NonZeroUsize::new(16).unwrap(),
        );
        let first = first_store
            .load_node_at_state(&node_path(8), Requirement::ANY)
            .await
            .unwrap();
        let second = second_store
            .load_node_at_state(&node_path(8), Requirement::ANY)
            .await
            .unwrap();
        assert!(!first.same_state(&second));

        log.lock().unwrap().clear();
        let checks = validator
            .check_leaves_current(
                &[first, second],
                Requirement::after(timeline.currentness_barrier()),
            )
            .await;

        assert!(
            checks
                .iter()
                .all(|check| matches!(check, Ok(LeafObservationCheck::Current)))
        );
        assert_eq!(
            count(&log, "read"),
            1,
            "the second distinct check can use evidence from the first"
        );
    }

    #[tokio::test]
    async fn committed_edit_is_visible_to_the_next_cached_load() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let path = node_path(1);
        assert!(
            store
                .store_node(
                    &collection(),
                    &node_id(1),
                    &Node::leaf(LeafBody::new()),
                    None,
                )
                .await
                .unwrap()
        );

        let loaded = store.load_leaf(&path, Requirement::ANY).await.unwrap();
        let previous_revision = loaded.observation().revision().cloned();
        let mut edit = loaded.into_edit();
        edit.set_entries(LeafBody::from_entries([LeafEntry::new(b"new".as_slice())]));
        let installed = store
            .commit_leaf(edit)
            .await
            .unwrap()
            .into_receipt()
            .unwrap()
            .into_installed();

        let committed = store.load_leaf(&path, Requirement::ANY).await.unwrap();
        assert!(installed.same_state(committed.observation()));
        assert!(
            installed
                .value()
                .unwrap()
                .as_leaf()
                .unwrap()
                .lookup(b"new")
                .is_some()
        );
        assert!(committed.entries().lookup(b"new").is_some());
        assert_ne!(
            committed.observation().revision(),
            previous_revision.as_ref()
        );
    }

    #[tokio::test]
    async fn leaf_edit_commits_bounded_changes_without_changing_topology() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let path = node_path(1);
        let original = Node::leaf(LeafBody::new())
            .with_high_key(Some(b"m".to_vec()))
            .with_right_sibling(Some(node_id(9)));
        assert!(
            store
                .store_node(&collection(), &node_id(1), &original, None)
                .await
                .unwrap()
        );

        let loaded = store.load_leaf(&path, Requirement::ANY).await.unwrap();
        let entries = LeafBody::from_entries([LeafEntry::new(b"key".as_slice())]);
        let mut locks = NodeLocks::default();
        let holder = TxId::with_priority(0, b"holder");
        locks.set_membership_writer(holder);

        let mut edit = loaded.into_edit();
        assert_eq!(edit.path(), &path);
        edit.set_entries(entries.clone());
        edit.set_locks(locks.clone());
        assert!(store.commit_leaf(edit).await.unwrap().is_applied());

        let committed = store.load_leaf(&path, Requirement::ANY).await.unwrap();
        assert_eq!(committed.entries(), &entries);
        assert_eq!(committed.locks(), &locks);
        assert_eq!(committed.node().high_key(), Some(b"m".as_slice()));
        assert_eq!(committed.node().right_sibling(), Some(node_id(9)));
        assert_eq!(committed.node().membership_lock().holders(), &[holder]);
    }

    #[tokio::test]
    async fn leaf_edit_is_bound_to_its_observed_path() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let left_path = node_path(1);
        let right_path = node_path(2);
        for id in [node_id(1), node_id(2)] {
            assert!(
                store
                    .store_node(&collection(), &id, &Node::leaf(LeafBody::new()), None)
                    .await
                    .unwrap()
            );
        }

        let mut edit = store
            .load_leaf(&left_path, Requirement::ANY)
            .await
            .unwrap()
            .into_edit();
        edit.set_entries(LeafBody::from_entries([LeafEntry::new(
            b"left-key".as_slice(),
        )]));
        assert_eq!(edit.path(), &left_path);
        assert!(store.commit_leaf(edit).await.unwrap().is_applied());

        let left = store.load_leaf(&left_path, Requirement::ANY).await.unwrap();
        let right = store
            .load_leaf(&right_path, Requirement::ANY)
            .await
            .unwrap();
        assert!(left.entries().lookup(b"left-key").is_some());
        assert!(right.entries().is_empty());
    }

    #[tokio::test]
    async fn stale_leaf_edit_conflicts() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let path = node_path(1);
        assert!(
            store
                .store_node(
                    &collection(),
                    &node_id(1),
                    &Node::leaf(LeafBody::new()),
                    None,
                )
                .await
                .unwrap()
        );

        let mut winner = store
            .load_leaf(&path, Requirement::ANY)
            .await
            .unwrap()
            .into_edit();
        let mut stale = store
            .load_leaf(&path, Requirement::ANY)
            .await
            .unwrap()
            .into_edit();
        winner.set_entries(LeafBody::from_entries([LeafEntry::new(
            b"winner".as_slice(),
        )]));
        stale.set_entries(LeafBody::from_entries([LeafEntry::new(
            b"stale".as_slice(),
        )]));

        assert!(store.commit_leaf(winner).await.unwrap().is_applied());
        assert!(!store.commit_leaf(stale).await.unwrap().is_applied());

        let committed = store.load_leaf(&path, Requirement::ANY).await.unwrap();
        assert!(committed.entries().lookup(b"winner").is_some());
        assert!(committed.entries().lookup(b"stale").is_none());
    }

    #[tokio::test]
    async fn node_listing_drains_backend_pages() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let store = store_over(Arc::new(recorder));
        let expected: Vec<_> = (0..=NODE_LIST_PAGE_SIZE)
            .map(|index| node_id(index as u8))
            .collect();
        for id in &expected {
            assert!(
                store
                    .store_node(&collection(), id, &Node::leaf(LeafBody::new()), None)
                    .await
                    .unwrap()
            );
        }

        let listed = store
            .list_nodes(&collection(), Requirement::ANY)
            .await
            .unwrap();

        assert_eq!(count(&log, "list"), 2);
        assert_eq!(listed.len(), expected.len());
        for id in expected {
            assert!(listed.iter().any(|(listed, _)| listed == &id));
        }
    }

    // A leaf whose only membership writer holds a write lock on its key.
    async fn leaf_with_membership_writer(store: &TestStore, writer: TxId) -> LeafEntry {
        let mut held = LeafEntry::new(b"deleted".to_vec());
        held.replace_write_lock(writer);
        let mut node = Node::leaf(LeafBody::from_entries([held.clone()]));
        node.set_membership_writer(writer);
        assert!(
            store
                .store_node(&collection(), &node_id(1), &node, None)
                .await
                .unwrap()
        );
        held
    }

    // ADR-077: a holder leaves the membership write lock of a leaf only with
    // its write locks and create locks in that leaf, because scans decide the
    // key membership from the leaf for the other key holders.
    #[tokio::test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "no_key_lock_outlives_membership")]
    async fn a_membership_writer_cannot_leave_without_its_key_locks() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let writer = TxId::with_priority(1, b"writer");
        leaf_with_membership_writer(&store, writer).await;
        let mut edit = store
            .load_leaf(&node_path(1), Requirement::ANY)
            .await
            .unwrap()
            .into_edit();
        let mut locks = edit.locks().clone();
        locks.remove_membership_holder(&writer);
        edit.set_locks(locks);

        let _ = store.commit_leaf(edit).await;
    }

    #[tokio::test]
    async fn a_membership_writer_leaves_with_its_key_locks() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let writer = TxId::with_priority(1, b"writer");
        let mut released = leaf_with_membership_writer(&store, writer).await;
        let mut edit = store
            .load_leaf(&node_path(1), Requirement::ANY)
            .await
            .unwrap()
            .into_edit();
        let mut locks = edit.locks().clone();
        locks.remove_membership_holder(&writer);
        released.release_lock(&writer);
        edit.set_entries(LeafBody::from_entries([released]));
        edit.set_locks(locks);

        assert!(store.commit_leaf(edit).await.unwrap().is_applied());
    }

    /// The result of the drop-intent removal by `nodes` at `proof`.
    async fn remove_intent(
        nodes: &NodeStore,
        owner: &TxId,
        proof: Requirement,
    ) -> Converged<Node, ()> {
        nodes
            .converge(
                &node_path(1),
                proof,
                CasRetries::Bounded(8),
                |mut node, _| {
                    std::future::ready(Ok::<_, StorageError>(if node.remove_drop_intent(owner) {
                        Decision::Write(node, ())
                    } else {
                        Decision::Done(())
                    }))
                },
            )
            .await
            .unwrap()
    }

    async fn drop_intent(store: &TestStore) -> Option<TxId> {
        let requirement = Requirement::after(store.timeline.currentness_barrier());
        let (node, _) = store
            .load_node(&collection(), &node_id(1), requirement)
            .await
            .unwrap();
        node.drop_intent().copied()
    }

    /// Two stores with their own caches over one backend, as two database
    /// instances. `local` has a cached state of the node without a drop
    /// intent, and `peer` then writes the drop intent of `owner`.
    async fn stale_without_intent(owner: &TxId) -> (TestStore, TestStore) {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let local = store_over(backend.clone());
        let peer = store_over(backend);
        let empty = Node::leaf(LeafBody::new());
        assert!(
            local
                .store_node(&collection(), &node_id(1), &empty, None)
                .await
                .unwrap()
        );
        let (mut node, observed) = peer
            .load_node(&collection(), &node_id(1), Requirement::ANY)
            .await
            .unwrap();
        node.set_drop_intent(*owner);
        assert!(
            peer.store_node(&collection(), &node_id(1), &node, Some(&observed))
                .await
                .unwrap()
        );
        (local, peer)
    }

    // The cached state has no intent to remove, so the change writes nothing
    // and no CAS rejects the stale state. Only the freshness requirement `proof` makes the
    // step see the intent.
    #[tokio::test]
    async fn converge_proves_a_change_that_writes_nothing() {
        let owner = TxId::with_priority(0, b"owner");
        let (local, peer) = stale_without_intent(&owner).await;

        let stale = remove_intent(&local, &owner, Requirement::ANY).await;
        assert!(matches!(stale, Converged::Done(())));
        assert_eq!(drop_intent(&peer).await, Some(owner));

        let proof = Requirement::after(local.timeline.currentness_barrier());
        let proven = remove_intent(&local, &owner, proof).await;
        assert!(matches!(proven, Converged::Written { .. }));
        assert_eq!(drop_intent(&peer).await, None);
    }

    // A cached absence is a result without a write too. Only the proof
    // requirement makes the step see the node that a peer created.
    #[tokio::test]
    async fn converge_proves_a_missing_node() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let local = store_over(backend.clone());
        let peer = store_over(backend);
        let absent = local
            .load_node_at_state(&node_path(1), Requirement::ANY)
            .await
            .unwrap();
        assert!(absent.is_absent());
        let empty = Node::leaf(LeafBody::new());
        assert!(
            peer.store_node(&collection(), &node_id(1), &empty, None)
                .await
                .unwrap()
        );
        let owner = TxId::with_priority(0, b"owner");
        let fence = async |proof| {
            local
                .converge(
                    &node_path(1),
                    proof,
                    CasRetries::Bounded(8),
                    |mut node, _| {
                        node.set_drop_intent(owner);
                        std::future::ready(Ok::<_, StorageError>(Decision::Write(node, ())))
                    },
                )
                .await
                .unwrap()
        };

        assert!(matches!(fence(Requirement::ANY).await, Converged::Missing));
        let proof = Requirement::after(local.timeline.currentness_barrier());
        assert!(matches!(fence(proof).await, Converged::Written { .. }));
        assert_eq!(drop_intent(&peer).await, Some(owner));
    }

    // A change that writes needs no proof: the CAS on the stale state loses,
    // and the step decides again on the state that the lost CAS loads.
    #[tokio::test]
    async fn converge_decides_again_after_a_lost_cas() {
        let owner = TxId::with_priority(0, b"owner");
        let (local, peer) = stale_without_intent(&owner).await;
        let other = TxId::with_priority(0, b"other");
        let mut decisions = 0;

        let result = local
            .converge(
                &node_path(1),
                Requirement::ANY,
                CasRetries::Bounded(8),
                |mut node, _| {
                    decisions += 1;
                    let decision = if node.drop_intent().is_some() {
                        Decision::Stop(())
                    } else {
                        node.set_drop_intent(other);
                        Decision::Write(node, ())
                    };
                    std::future::ready(Ok::<_, StorageError>(decision))
                },
            )
            .await
            .unwrap();

        assert!(matches!(result, Converged::Stop(())));
        assert_eq!(decisions, 2);
        assert_eq!(drop_intent(&peer).await, Some(owner));
    }

    // An advance writes and then decides on the state that its CAS installed,
    // without a backend read in between.
    #[tokio::test]
    async fn converge_advances_on_the_installed_state() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let store = store_over(Arc::new(recorder));
        let empty = Node::leaf(LeafBody::new());
        assert!(
            store
                .store_node(&collection(), &node_id(1), &empty, None)
                .await
                .unwrap()
        );
        let reads = count(&log, "read") + count(&log, "read_if_modified");
        let first = TxId::with_priority(0, b"first");
        let mut seen = Vec::new();

        let result = store
            .converge(
                &node_path(1),
                Requirement::ANY,
                CasRetries::Bounded(8),
                |mut node, _| {
                    seen.push(node.drop_intent().copied());
                    let decision = match node.drop_intent() {
                        None => {
                            node.set_drop_intent(first);
                            Decision::Advance(node)
                        }
                        Some(_) => Decision::Done(()),
                    };
                    std::future::ready(Ok::<_, StorageError>(decision))
                },
            )
            .await
            .unwrap();

        assert!(matches!(result, Converged::Done(())));
        assert_eq!(seen, [None, Some(first)]);
        assert_eq!(
            count(&log, "read") + count(&log, "read_if_modified"),
            reads,
            "the second decision uses the installed state"
        );
    }

    // A reload at a requirement replaces a stale cached state with one that
    // satisfies it.
    #[tokio::test]
    async fn converge_reloads_at_the_requirement_of_the_decision() {
        let owner = TxId::with_priority(0, b"owner");
        let (local, _peer) = stale_without_intent(&owner).await;
        let fresh = Requirement::after(local.timeline.currentness_barrier());
        let mut seen = Vec::new();

        let result = local
            .converge(
                &node_path(1),
                Requirement::ANY,
                CasRetries::Bounded(8),
                |node, observed| {
                    seen.push(node.drop_intent().copied());
                    let decision = if observed.satisfies(fresh) {
                        Decision::Done(())
                    } else {
                        Decision::Reload(fresh)
                    };
                    std::future::ready(Ok::<_, StorageError>(decision))
                },
            )
            .await
            .unwrap();

        assert!(matches!(result, Converged::Done(())));
        assert_eq!(seen, [None, Some(owner)]);
    }

    /// Decides `Wait(ANY)` once and then `Done`, and returns how long the step
    /// took in paused time.
    async fn wait_once(retries: CasRetries) -> std::time::Duration {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let empty = Node::leaf(LeafBody::new());
        assert!(
            store
                .store_node(&collection(), &node_id(1), &empty, None)
                .await
                .unwrap()
        );
        let mut waited = false;
        let started = tokio::time::Instant::now();
        let result = store
            .converge(&node_path(1), Requirement::ANY, retries, |_, _| {
                let decision = if waited {
                    Decision::Done(())
                } else {
                    waited = true;
                    Decision::Wait(Requirement::ANY)
                };
                std::future::ready(Ok::<_, StorageError>(decision))
            })
            .await
            .unwrap();
        assert!(matches!(result, Converged::Done(())));
        started.elapsed()
    }

    // A wait uses the backoff of an unbounded step. A bounded step has none
    // and loads again at once.
    #[tokio::test(start_paused = true)]
    async fn converge_waits_with_the_backoff_of_the_step() {
        let schedule = glassdb_concurr::RetrySchedule {
            initial_interval: std::time::Duration::from_secs(10),
            max_interval: std::time::Duration::from_secs(20),
        };
        // The first delay is the initial interval with a jitter of at most
        // one half.
        assert!(wait_once(CasRetries::Unbounded(schedule)).await >= schedule.initial_interval / 2);
        assert!(wait_once(CasRetries::Bounded(8)).await < schedule.initial_interval / 2);
    }

    // A lost advance counts toward the bound like a lost write.
    #[tokio::test]
    async fn converge_counts_a_lost_advance() {
        let owner = TxId::with_priority(0, b"owner");
        let (local, _peer) = stale_without_intent(&owner).await;
        let other = TxId::with_priority(0, b"other");
        let mut decisions = 0;

        let result = local
            .converge(
                &node_path(1),
                Requirement::ANY,
                CasRetries::Bounded(1),
                |mut node, _| {
                    decisions += 1;
                    node.set_drop_intent(other);
                    std::future::ready(Ok::<_, StorageError>(Decision::<Node, ()>::Advance(node)))
                },
            )
            .await
            .unwrap();

        assert!(matches!(result, Converged::Exhausted));
        assert_eq!(decisions, 1);
    }
}
