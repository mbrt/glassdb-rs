//! Background growth of the B-link coordination tree by leaf and node splits
//! (ADR-031).
//!
//! Coordination objects are grow-only: a leaf that crosses its soft cap is
//! halved so no single object becomes a scalability or contention bottleneck.
//! Splitting runs off the hot path in a periodic background task, fed candidates
//! from stored over-cap leaves and direct-commit inline admission misses —
//! never a key-space enumeration.
//!
//! Every split is a sequence of independent, idempotent compare-and-swaps under
//! a one-node structure-write lock. Before joining collection topology in
//! `_i`, it writes a `Preparing` intent below its topology participant's `_s`
//! prefix.
//! After taking the source gate it conditionally advances that intent to
//! `Ready`; only then may it create nodes. A lifecycle freeze can therefore
//! find exactly one participant's work and cancel an unadvanced intent without
//! racing late node creation:
//!
//! 0. Advance the structural intent with the source observation and split key; its
//!    created-node tokens were reserved while `Preparing`.
//! 1. Create the right sibling (`write_if_not_exists`) holding the upper half
//!    and inheriting the source's former high-key and right-sibling.
//! 2. **Shrink the source in one CAS** — drop the upper half, set high-key to the
//!    split key, link to the sibling. This is the linearization point: descent
//!    now finds the moved keys by stepping right, and a concurrent locker that
//!    retained the pre-shrink observation loses its CAS and re-routes (ADR-031
//!    coverage re-check).
//! 3. Insert the separator into the parent so future descents skip the
//!    right-link hop; recurse when the parent itself overflows. Purely an
//!    optimization — correctness never depends on it landing.
//!
//! A leaf split, including a root-leaf split, acquires structure-write through
//! the shared [`LeafCoordinator`], in the same folded CAS stream as data
//! mutations on that leaf. Interior indexes use direct structural CASes.
//! The source shrink (or root rewrite) releases structure-write inline, so no
//! unlocked post-split state is exposed before a separate release CAS.
//! Once a leaf is quiescent behind that gate, holder-free tombstones are
//! removed before the final reason check. The compacted leaf either cancels the
//! split in one CAS or supplies the ordinary recoverable split outputs
//! (ADR-062).
//!
//! The collection root `_r` cannot move (its address is fixed), so when it
//! overflows it splits **in place**: two children are created and the root is
//! rewritten into a two-entry index over them, growing the tree's height while
//! leaving the independent collection record untouched.

mod recovery;

use std::collections::{BTreeMap, VecDeque};
use std::ops::{AddAssign, Sub};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use async_trait::async_trait;
use glassdb_concurr::{Background, RetryConfig, rt};
use glassdb_data::{CollectionAddress, DbRoot, NodeToken, ObjectPath, TxId};
use glassdb_storage::transaction::{TxCommitStatus, TxLock, TxLog};
use glassdb_storage::{
    CollectionStore, IndexNode, InlinePolicy, LeafBody, LeafEntry, LeafObservation, LockType, Node,
    NodeStore, Requirement, SplitPolicy, StorageError, StructuralIntentStore, Timeline, TreeRouter,
};
use tokio::sync::Notify;

use crate::collections::TopologySettler;
use crate::error::TransError;
use crate::gc::TxCleanupHints;
use crate::key_state_resolver::KeyStateResolver;
use crate::leaf_coord::{LeafCoordinator, SplitHinter};
use crate::monitor::{Monitor, TxRecoveryManifest};
use crate::node_locking::{
    NodeLockReconciler, QuiescedEntries, StructuralGateOperation, StructuralGateOutcome,
};

use recovery::{
    PreparedIntent, PreparedIntentCleanup, ReadyIntent, ReadyIntentCompletion,
    ReadyIntentTransition, RecoveryAction, RecoveryStep, StructuralRecovery,
};

/// How often the splitter drains its candidate queue. A split is a handful of
/// CAS round-trips, so a tight cadence keeps overflowing leaves short-lived.
const SPLIT_INTERVAL: Duration = Duration::from_secs(1);

/// Back off empty structural-intent listings independently of split candidates.
const STRUCTURAL_RECOVERY_IDLE_INTERVAL: Duration = Duration::from_secs(60);

/// Upper bound on the buffered split-candidate queue. Candidates are only hints:
/// the splitter reloads and re-checks each one, so dropping the oldest when full
/// merely delays a split, never causes an unsafe one.
const CANDIDATE_QUEUE_CAP: usize = 4096;

/// Bounded attempts to insert a separator into a contended parent before
/// re-queuing it for a later sweep. Descent works meanwhile through right-links.
const PARENT_RETRIES: usize = 8;

/// Safety bound on the leaf right-link hops walked while reconciling
/// separators, so a malformed or concurrently-mutated chain can never spin the
/// splitter. A well-formed chain up to a split key is far shorter than this.
const MAX_RECONCILE_HOPS: usize = 4096;

/// A right-link edge that a parent index does not name yet: the separator that
/// bounds the child's range, and the child it routes to.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MissingSeparator {
    separator: Vec<u8>,
    child: NodeToken,
}

/// A leaf separator a split could not publish into its parent index on the
/// first try (a lost CAS): re-driven by a later [`Splitter`] sweep so the
/// directory does not stay reliant on a right-link walk (ADR-031). Re-driving
/// reconciles the whole chain, so `split_key -> new_token` names only the
/// rightmost edge to publish.
#[derive(Clone)]
pub(crate) struct PendingSeparator {
    collection: CollectionAddress,
    split_key: Vec<u8>,
    new_token: NodeToken,
}

/// Shares structural-node mutation primitives between split coordination and
/// separator publication.
#[derive(Clone)]
struct StructuralNodeAccess {
    nodes: NodeStore,
    mon: Monitor,
    key_state: KeyStateResolver,
    coord: LeafCoordinator,
}

impl StructuralNodeAccess {
    fn new(
        nodes: NodeStore,
        mon: Monitor,
        key_state: KeyStateResolver,
        coord: LeafCoordinator,
    ) -> Self {
        Self {
            nodes,
            mon,
            key_state,
            coord,
        }
    }

    /// Registers a new split identity and acquires one node's structural gate
    /// under wound-wait.
    ///
    /// `None` reports that the gate was not taken — contention, a wait, or a
    /// lost CAS — and that the identity is already retired, so the caller can
    /// retry without cleanup of its own. An error retires it likewise.
    async fn begin_gated_split(
        &self,
        collection: &CollectionAddress,
        token: Option<&NodeToken>,
    ) -> Result<Option<(TxId, Node, LeafObservation)>, TransError> {
        let id = TxId::new_at(rt::system_now());
        self.mon.begin_tx(&id);
        match self.acquire_structural_gate(collection, token, &id).await {
            Ok(Some((node, observation))) => Ok(Some((id, node, observation))),
            Ok(None) => {
                self.finalize_split(&id).await;
                Ok(None)
            }
            Err(error) => {
                self.finalize_split(&id).await;
                Err(error)
            }
        }
    }

    /// Acquires a source node's structural gate under wound-wait.
    async fn acquire_structural_gate(
        &self,
        collection: &CollectionAddress,
        token: Option<&NodeToken>,
        id: &TxId,
    ) -> Result<Option<(Node, LeafObservation)>, TransError> {
        let path = match token {
            Some(token) => ObjectPath::Node {
                collection: collection.clone(),
                token: token.clone(),
            },
            None => ObjectPath::TreeRoot {
                collection: collection.clone(),
            },
        };
        let (node, _) = match self.nodes.load_node_at(&path, Requirement::Any).await {
            Ok(loaded) => loaded,
            Err(StorageError::NotFound) if token.is_none() => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if node.as_leaf().is_some() {
            return self.acquire_leaf_structural_gate(&path, id).await;
        }
        self.acquire_structural_gate_direct(collection, token, id)
            .await
    }

    async fn acquire_leaf_structural_gate(
        &self,
        path: &ObjectPath,
        id: &TxId,
    ) -> Result<Option<(Node, LeafObservation)>, TransError> {
        let outcome = self
            .coord
            .coordinate(StructuralGateOperation::new(id.clone(), path.clone()))
            .await?;
        let StructuralGateOutcome::Acquired(requirement) = outcome else {
            return Ok(None);
        };
        let (node, observation) = self.nodes.load_node_at(path, requirement).await?;
        if node.structural_gate().lock_type() == LockType::Write
            && node.structural_gate().contains(id)
        {
            Ok(Some((node, observation)))
        } else {
            Ok(None)
        }
    }

    async fn acquire_structural_gate_direct(
        &self,
        collection: &CollectionAddress,
        token: Option<&NodeToken>,
        id: &TxId,
    ) -> Result<Option<(Node, LeafObservation)>, TransError> {
        for _ in 0..PARENT_RETRIES {
            let (mut node, observation) = match token {
                Some(token) => {
                    self.nodes
                        .load_node(collection, token, Requirement::Any)
                        .await?
                }
                None => match self.nodes.load_root(collection, Requirement::Any).await {
                    Ok((root, observation)) => (root, observation),
                    Err(StorageError::NotFound) => return Ok(None),
                    Err(error) => return Err(error.into()),
                },
            };
            if node.structural_gate().lock_type() == LockType::Write
                && node.structural_gate().contains(id)
            {
                return Ok(Some((node, observation)));
            }

            let entries: BTreeMap<Vec<u8>, _> = node
                .as_leaf()
                .into_iter()
                .flat_map(LeafBody::entries)
                .cloned()
                .map(|entry| (entry.key.clone(), entry))
                .collect();
            let reconciler = NodeLockReconciler::new(&self.key_state, &self.mon, id);
            let entries = match reconciler
                .quiesce_entries(collection, &entries, Requirement::Any)
                .await?
            {
                QuiescedEntries::Ready(entries) => entries,
                QuiescedEntries::Wait(_) => return Ok(None),
            };
            let mut locks = node.locks().clone();
            if reconciler
                .acquire_structural_gate(&mut locks)
                .await?
                .is_some()
            {
                return Ok(None);
            }

            if node.as_leaf().is_some() {
                node.set_leaf(LeafBody::from_entries(entries.into_values()))?;
            }
            node.set_locks(locks);
            if self
                .store_structural_node(collection, token, &node, &observation)
                .await?
            {
                let (_, locked_observation) = match token {
                    Some(token) => {
                        self.nodes
                            .load_node(
                                collection,
                                token,
                                Requirement::AtLeast(observation.current_after()),
                            )
                            .await?
                    }
                    None => {
                        let (root, observation) = self
                            .nodes
                            .load_root(
                                collection,
                                Requirement::AtLeast(observation.current_after()),
                            )
                            .await?;
                        (root, observation)
                    }
                };
                return Ok(Some((node, locked_observation)));
            }
        }
        Ok(None)
    }

    async fn release_structural_gate(
        &self,
        collection: &CollectionAddress,
        token: Option<&NodeToken>,
        id: &TxId,
    ) -> Result<(), TransError> {
        for _ in 0..PARENT_RETRIES {
            let (mut node, observation) = match token {
                Some(token) => {
                    self.nodes
                        .load_node(collection, token, Requirement::Any)
                        .await?
                }
                None => {
                    let (root, observation) =
                        self.nodes.load_root(collection, Requirement::Any).await?;
                    (root, observation)
                }
            };
            if !node.remove_structural_gate(id) {
                return Ok(());
            }
            if self
                .store_structural_node(collection, token, &node, &observation)
                .await?
            {
                return Ok(());
            }
        }
        Err(TransError::Retry)
    }

    async fn store_structural_node(
        &self,
        collection: &CollectionAddress,
        token: Option<&NodeToken>,
        node: &Node,
        observation: &LeafObservation,
    ) -> Result<bool, TransError> {
        match token {
            Some(token) => Ok(self
                .nodes
                .store_node(collection, token, node, Some(observation))
                .await?),
            None => Ok(self.nodes.store_root(collection, node, observation).await?),
        }
    }

    async fn finalize_split(&self, id: &TxId) {
        if let Err(error) = self
            .mon
            .commit_tx(TxLog::new(id.clone(), TxCommitStatus::Ok))
            .await
        {
            tracing::debug!(
                target: "glassdb::splitter",
                error = %error,
                "finalizing split transaction failed"
            );
        }
    }

    async fn finish_without_split(
        &self,
        collection: &CollectionAddress,
        token: Option<&NodeToken>,
        id: &TxId,
    ) -> Result<(), TransError> {
        let release = self.release_structural_gate(collection, token, id).await;
        self.finalize_split(id).await;
        release
    }
}
struct SeparatorPublication {
    separator: PendingSeparator,
    start: Requirement,
    retries_remaining: usize,
}

enum SeparatorPublicationOutcome {
    Published,
    ParentRequiresSplit(ParentRequiresSplit),
}

struct ParentRequiresSplit {
    path: ObjectPath,
    continuation: ParentSplitContinuation,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ParentSplitContinuation {
    ResumePublication,
    CompletePublication,
}

/// Publishes leaf-chain separators and owns their deferred retry queue.
#[derive(Clone)]
struct SeparatorPublisher {
    structure: StructuralNodeAccess,
    router: TreeRouter,
    timeline: Timeline,
    policy: SplitPolicy,
    pending: Arc<Mutex<VecDeque<PendingSeparator>>>,
}

impl SeparatorPublisher {
    fn new(
        structure: StructuralNodeAccess,
        router: TreeRouter,
        timeline: Timeline,
        policy: SplitPolicy,
    ) -> Self {
        Self {
            structure,
            router,
            timeline,
            policy,
            pending: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Queues a separator whose parent insert must be re-driven by a later
    /// sweep. The oldest is dropped when full because descent remains correct
    /// through right-links.
    fn defer(&self, separator: PendingSeparator) {
        let mut pending = self.pending.lock().unwrap();
        if pending.len() >= CANDIDATE_QUEUE_CAP {
            pending.pop_front();
        }
        pending.push_back(separator);
    }

    /// Drains the deferred separators for one publication sweep.
    fn drain_pending(&self) -> Vec<PendingSeparator> {
        self.pending.lock().unwrap().drain(..).collect()
    }

    fn begin_publication(
        &self,
        collection: &CollectionAddress,
        split_key: &[u8],
        new_token: &NodeToken,
    ) -> SeparatorPublication {
        SeparatorPublication {
            separator: PendingSeparator {
                collection: collection.clone(),
                split_key: split_key.to_vec(),
                new_token: new_token.clone(),
            },
            start: Requirement::AtLeast(self.timeline.now()),
            retries_remaining: PARENT_RETRIES,
        }
    }

    /// Publishes every missing edge through the target separator or requests
    /// the parent split needed to continue safely.
    async fn publish(
        &self,
        publication: &mut SeparatorPublication,
    ) -> Result<SeparatorPublicationOutcome, TransError> {
        while publication.retries_remaining > 0 {
            publication.retries_remaining -= 1;
            let separator = &publication.separator;
            let Some(parent) = self
                .router
                .parent_index_for(
                    &separator.collection,
                    &separator.split_key,
                    publication.start,
                )
                .await?
            else {
                return Ok(SeparatorPublicationOutcome::Published);
            };
            let parent_token = match &parent.path {
                ObjectPath::TreeRoot { .. } => None,
                ObjectPath::Node { token, .. } => Some(token.clone()),
                _ => return Err(TransError::other("router returned a non-node parent path")),
            };
            let Some((lock_id, locked_parent, locked_version)) = self
                .structure
                .begin_gated_split(&separator.collection, parent_token.as_ref())
                .await?
            else {
                continue;
            };
            let published = self
                .publish_into_gated_parent(
                    separator,
                    &parent.path,
                    parent_token.as_ref(),
                    &locked_parent,
                    &locked_version,
                    &lock_id,
                )
                .await;
            let released = self
                .structure
                .finish_without_split(&separator.collection, parent_token.as_ref(), &lock_id)
                .await;
            match published? {
                Some(outcome) => {
                    released?;
                    return Ok(outcome);
                }
                None => {
                    let _ = released;
                    continue;
                }
            }
        }

        self.defer(publication.separator.clone());
        Err(TransError::Retry)
    }

    /// Merges every unindexed edge up to `separator` into the gated parent.
    ///
    /// `None` reports a lost CAS, which the caller retries. The gate stays
    /// installed on every path, because the caller releases it once.
    async fn publish_into_gated_parent(
        &self,
        separator: &PendingSeparator,
        parent_path: &ObjectPath,
        parent_token: Option<&NodeToken>,
        parent: &Node,
        version: &LeafObservation,
        lock_id: &TxId,
    ) -> Result<Option<SeparatorPublicationOutcome>, TransError> {
        let Some(index) = parent.as_index() else {
            return Ok(Some(SeparatorPublicationOutcome::Published));
        };
        if index.child_for(&separator.split_key) == Some(separator.new_token.as_str()) {
            return Ok(Some(SeparatorPublicationOutcome::Published));
        }
        let missing = self
            .missing_separators(
                &separator.collection,
                parent,
                &separator.split_key,
                Requirement::AtLeast(version.current_after()),
            )
            .await?;
        if missing.is_empty() {
            return Ok(Some(SeparatorPublicationOutcome::Published));
        }
        let mut new_index = index.clone();
        for edge in &missing {
            new_index.insert_child(edge.separator.clone(), edge.child.to_string());
        }
        let mut updated = parent.clone();
        updated.set_index(new_index)?;
        let content_limit = self.policy.content_limit();
        if updated.content_encoded_len() > content_limit
            || updated.encoded_len() > self.policy.node_max_bytes()
        {
            if parent.over_soft_cap(&self.policy) {
                return Ok(Some(SeparatorPublicationOutcome::ParentRequiresSplit(
                    ParentRequiresSplit {
                        path: parent_path.clone(),
                        continuation: ParentSplitContinuation::ResumePublication,
                    },
                )));
            }
            return Err(TransError::InvalidInput(
                "separator exceeds the coordination node size limit".into(),
            ));
        }

        updated.remove_structural_gate(lock_id);
        if !self
            .structure
            .store_structural_node(&separator.collection, parent_token, &updated, version)
            .await?
        {
            return Ok(None);
        }
        if updated.over_soft_cap(&self.policy) {
            return Ok(Some(SeparatorPublicationOutcome::ParentRequiresSplit(
                ParentRequiresSplit {
                    path: parent_path.clone(),
                    continuation: ParentSplitContinuation::CompletePublication,
                },
            )));
        }
        Ok(Some(SeparatorPublicationOutcome::Published))
    }

    /// Returns the right-link edges through `split_key` that `parent` does not
    /// name yet, in chain order.
    ///
    /// A split publishes its separator into the parent as a follow-on step, so
    /// the index can lag behind the leaf chain until a later sweep reconciles it
    /// (ADR-031). `parent` is the caller's own observed index, so the result
    /// reconciles against the version the caller goes on to write.
    async fn missing_separators(
        &self,
        collection: &CollectionAddress,
        parent: &Node,
        split_key: &[u8],
        requirement: Requirement,
    ) -> Result<Vec<MissingSeparator>, TransError> {
        let Some(index) = parent.as_index() else {
            return Ok(Vec::new());
        };
        let Some(start) = index.child_for(split_key) else {
            return Ok(Vec::new());
        };
        let start = node_token(start)?;
        let mut current = self.router.leaf_at(collection, &start, requirement).await?;
        let mut missing = Vec::new();
        for _ in 0..MAX_RECONCILE_HOPS {
            let Some(node) = current.node() else {
                return Err(StorageError::NotFound.into());
            };
            let (Some(right), Some(separator)) = (node.right_sibling(), node.high_key()) else {
                return Ok(missing);
            };
            if separator > split_key {
                return Ok(missing);
            }
            let right = right.to_string();
            let separator = separator.to_vec();
            if index.child_for(&separator) != Some(right.as_str()) {
                missing.push(MissingSeparator {
                    separator: separator.clone(),
                    child: node_token(&right)?,
                });
            }
            if separator == split_key {
                return Ok(missing);
            }
            let Some(next) = self
                .router
                .next_leaf(collection, &current, requirement)
                .await?
            else {
                return Ok(missing);
            };
            current = next;
        }
        Err(TransError::other(
            "separator reconciliation exceeded the right-link hop bound",
        ))
    }
}

/// The feed of leaves that may need splitting (ADR-031), owned by the
/// [`Splitter`]. The coordinator observes stored leaf size through
/// [`SplitHinter`], while direct-commit admission reports inline pressure
/// through [`SplitHintSink`]. The splitter drains and re-checks both causes.
/// Cloneable so the producers and splitter share one queue and policy.
#[derive(Clone)]
pub(crate) struct SplitCandidates {
    policy: SplitPolicy,
    inline: InlinePolicy,
    queue: Arc<Mutex<VecDeque<SplitCandidate>>>,
}

/// Lightweight producer handle for split hints decided outside the leaf
/// coordinator. Opaque to its holders: they report pressure, never inspect or
/// drive the splitter's queue.
#[derive(Clone)]
pub struct SplitHintSink {
    candidates: SplitCandidates,
}

impl SplitHintSink {
    /// Records recoverable aggregate inline pressure for authoritative
    /// revalidation by the splitter.
    pub(crate) fn observe_inline_pressure(&self, path: &ObjectPath, key: &[u8], value_len: usize) {
        if !self.candidates.inline.admits_value(value_len) {
            return;
        }
        self.candidates.push(SplitCandidate {
            path: path.clone(),
            priority: self.candidates.new_id(),
            reason: SplitReason::InlinePressure {
                key: key.to_vec(),
                value_len,
            },
        });
    }

    #[cfg(test)]
    pub(crate) fn pending_inline_pressure(&self) -> usize {
        self.candidates
            .queue
            .lock()
            .unwrap()
            .iter()
            .filter(|candidate| candidate.reason.is_inline_pressure())
            .count()
    }
}

#[derive(Clone)]
struct SplitCandidate {
    path: ObjectPath,
    priority: TxId,
    reason: SplitReason,
}

#[derive(Clone)]
enum SplitReason {
    SoftCap,
    InlinePressure { key: Vec<u8>, value_len: usize },
}

impl SplitReason {
    fn class(&self) -> u8 {
        match self {
            SplitReason::SoftCap => 0,
            SplitReason::InlinePressure { .. } => 1,
        }
    }

    fn is_inline_pressure(&self) -> bool {
        matches!(self, SplitReason::InlinePressure { .. })
    }
}

enum SplitNeed {
    Split,
    NotActionable,
    Reroute,
}

/// Removes durable absence entries that no transaction still holds.
fn reclaim_holder_free_tombstones(node: &mut Node) -> Vec<TxId> {
    let Some(leaf) = node.as_leaf() else {
        return Vec::new();
    };
    let mut reclaimed = Vec::new();
    let retained = leaf.entries().filter_map(|entry| {
        if entry.lock_holders().is_empty() && entry.current.is_tombstone() {
            reclaimed.push(
                entry
                    .current
                    .writer()
                    .expect("a tombstone always names its writer")
                    .clone(),
            );
            None
        } else {
            Some(entry.clone())
        }
    });
    let compacted = LeafBody::from_entries(retained);
    if !reclaimed.is_empty() {
        node.set_leaf(compacted)
            .expect("tombstone reclamation only rewrites leaves");
    }
    reclaimed
}

/// A source node that is quiescent behind its structural gate and still needs
/// the requested split after tombstone reclamation.
struct QuiescedSplitSource {
    node: Node,
    observation: LeafObservation,
    reclaimed: Vec<TxId>,
}

/// The objects planned for an in-place root split.
struct RootSplitPlan {
    left_token: NodeToken,
    right_token: NodeToken,
    left: Node,
    right: Node,
    index: Node,
    split_key: Vec<u8>,
}

/// Whether a coordinated split finished, can discard Preparing, or needs recovery.
enum SplitAttemptResult {
    Completed,
    RetryCleanly,
    // Keep the common split outcomes small through a Box while retaining
    // recovery authority.
    RecoveryRequired(Box<ReadyIntent>),
}

/// Preserves the operation result independently from structural cleanup state.
struct SplitAttemptOutcome {
    result: Result<(), TransError>,
    state: SplitAttemptResult,
}

impl SplitAttemptOutcome {
    fn completed() -> Self {
        Self {
            result: Ok(()),
            state: SplitAttemptResult::Completed,
        }
    }

    fn retry_cleanly(result: Result<(), TransError>) -> Self {
        Self {
            result,
            state: SplitAttemptResult::RetryCleanly,
        }
    }

    fn recovery_required(ready: ReadyIntent, error: TransError) -> Self {
        Self {
            result: Err(error),
            state: SplitAttemptResult::RecoveryRequired(Box::new(ready)),
        }
    }
}

#[derive(Clone, Copy)]
enum StructuralSplitTarget<'a> {
    Root,
    NonRoot(&'a NodeToken),
}

impl<'a> StructuralSplitTarget<'a> {
    fn source_token(self) -> Option<&'a NodeToken> {
        match self {
            Self::Root => None,
            Self::NonRoot(token) => Some(token),
        }
    }
}

#[derive(Clone, Copy)]
enum StructuralSplitTopology<'a> {
    Owned,
    Joined(&'a TxId),
}

/// One root or non-root split with a single outer lifecycle.
struct StructuralSplitAttempt<'a> {
    splitter: &'a Splitter,
    collection: &'a CollectionAddress,
    target: StructuralSplitTarget<'a>,
    worker: TxId,
    reason: &'a SplitReason,
}

impl<'a> StructuralSplitAttempt<'a> {
    fn new(
        splitter: &'a Splitter,
        collection: &'a CollectionAddress,
        target: StructuralSplitTarget<'a>,
        worker: TxId,
        reason: &'a SplitReason,
    ) -> Self {
        Self {
            splitter,
            collection,
            target,
            worker,
            reason,
        }
    }

    async fn run(self, topology: StructuralSplitTopology<'_>) -> Result<(), TransError> {
        let result = match topology {
            StructuralSplitTopology::Owned => {
                match self
                    .splitter
                    .begin_topology_tx(self.collection, &self.worker)
                    .await
                {
                    Ok(()) => self.run_prepared(topology).await,
                    Err(error) => Err(error),
                }
            }
            StructuralSplitTopology::Joined(_) => {
                self.splitter.mon.begin_tx(&self.worker);
                self.run_prepared(topology).await
            }
        };

        match topology {
            StructuralSplitTopology::Owned => {
                self.splitter
                    .finalize_topology_split(self.collection, &self.worker)
                    .await;
            }
            StructuralSplitTopology::Joined(_) => {
                self.splitter.finalize_split(&self.worker).await;
            }
        }
        if result.is_err() {
            self.splitter.recovery_wake.notify_one();
        }
        result
    }

    async fn run_prepared(&self, topology: StructuralSplitTopology<'_>) -> Result<(), TransError> {
        let participant = match topology {
            StructuralSplitTopology::Owned => &self.worker,
            StructuralSplitTopology::Joined(participant) => participant,
        };
        let prepared = self
            .splitter
            .recovery
            .prepare_intent(self.collection, self.target.source_token(), participant)
            .await?;
        let cleanup = prepared.cleanup_witness();
        let outcome = match topology {
            StructuralSplitTopology::Owned => {
                match self
                    .splitter
                    .join_topology(self.collection, &self.worker)
                    .await
                {
                    Ok(()) => self.coordinate(prepared).await,
                    Err(error) => SplitAttemptOutcome::retry_cleanly(Err(error)),
                }
            }
            StructuralSplitTopology::Joined(_) => self.coordinate(prepared).await,
        };
        self.finish(outcome, &cleanup, topology).await
    }

    async fn coordinate(&self, prepared: PreparedIntent) -> SplitAttemptOutcome {
        match self.target {
            StructuralSplitTarget::Root => {
                self.splitter
                    .coordinate_root_split(self.collection, &self.worker, self.reason, prepared)
                    .await
            }
            StructuralSplitTarget::NonRoot(token) => {
                self.splitter
                    .coordinate_nonroot_split(
                        self.collection,
                        token,
                        &self.worker,
                        self.reason,
                        prepared,
                    )
                    .await
            }
        }
    }

    async fn finish(
        &self,
        outcome: SplitAttemptOutcome,
        prepared: &PreparedIntentCleanup,
        topology: StructuralSplitTopology<'_>,
    ) -> Result<(), TransError> {
        let SplitAttemptOutcome { result, state } = outcome;
        match state {
            SplitAttemptResult::Completed => match topology {
                StructuralSplitTopology::Owned => result.and(
                    self.splitter
                        .leave_topology(self.collection, &self.worker)
                        .await,
                ),
                StructuralSplitTopology::Joined(_) => result,
            },
            SplitAttemptResult::RetryCleanly => {
                let cleanup = match self.splitter.recovery.discard_prepared(prepared).await {
                    Ok(()) => match topology {
                        StructuralSplitTopology::Owned => {
                            self.splitter
                                .leave_topology(self.collection, &self.worker)
                                .await
                        }
                        StructuralSplitTopology::Joined(_) => Ok(()),
                    },
                    Err(error) => Err(error),
                };
                result.and(cleanup)
            }
            SplitAttemptResult::RecoveryRequired(_ready) => result,
        }
    }
}

#[derive(Default)]
struct Stats {
    candidates: AtomicU64,
    completed: AtomicU64,
    deferred: AtomicU64,
    tombstones_reclaimed: AtomicU64,
    splits_avoided: AtomicU64,
    inline_pressure_candidates: AtomicU64,
    inline_pressure_completed: AtomicU64,
    inline_pressure_deferred: AtomicU64,
    inline_pressure_discarded: AtomicU64,
}

/// Background split activity for one snapshot or accumulated interval.
///
/// `completed` counts locally observed source/root linearizations. A split may
/// also be `deferred` if a later publication or cleanup step needs another
/// sweep, so the fields are not mutually exclusive outcomes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SplitterStats {
    /// Deduplicated candidates processed for any split cause.
    pub candidates: u64,
    /// Locally observed source/root split linearizations for any cause.
    pub completed: u64,
    /// Retryable candidate attempts requeued for any cause.
    pub deferred: u64,
    /// Holder-free tombstone entries removed by acknowledged leaf rewrites.
    pub tombstones_reclaimed: u64,
    /// Actionable splits cancelled after tombstone reclamation removed the need.
    pub splits_avoided: u64,
    /// Activity attributable specifically to aggregate inline pressure.
    pub inline_pressure: InlinePressureStats,
}

/// Split activity attributable to aggregate inline pressure.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InlinePressureStats {
    /// Processed candidates.
    pub candidates: u64,
    /// Locally observed leaf splits.
    pub completed: u64,
    /// Retryable candidate attempts requeued.
    pub deferred: u64,
    /// Candidates discarded after authoritative revalidation.
    pub discarded: u64,
}

impl AddAssign for InlinePressureStats {
    fn add_assign(&mut self, rhs: Self) {
        self.candidates += rhs.candidates;
        self.completed += rhs.completed;
        self.deferred += rhs.deferred;
        self.discarded += rhs.discarded;
    }
}

impl Sub for InlinePressureStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            candidates: self.candidates.saturating_sub(rhs.candidates),
            completed: self.completed.saturating_sub(rhs.completed),
            deferred: self.deferred.saturating_sub(rhs.deferred),
            discarded: self.discarded.saturating_sub(rhs.discarded),
        }
    }
}

impl AddAssign for SplitterStats {
    fn add_assign(&mut self, rhs: Self) {
        self.candidates += rhs.candidates;
        self.completed += rhs.completed;
        self.deferred += rhs.deferred;
        self.tombstones_reclaimed += rhs.tombstones_reclaimed;
        self.splits_avoided += rhs.splits_avoided;
        self.inline_pressure += rhs.inline_pressure;
    }
}

impl Sub for SplitterStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            candidates: self.candidates.saturating_sub(rhs.candidates),
            completed: self.completed.saturating_sub(rhs.completed),
            deferred: self.deferred.saturating_sub(rhs.deferred),
            tombstones_reclaimed: self
                .tombstones_reclaimed
                .saturating_sub(rhs.tombstones_reclaimed),
            splits_avoided: self.splits_avoided.saturating_sub(rhs.splits_avoided),
            inline_pressure: self.inline_pressure - rhs.inline_pressure,
        }
    }
}

impl SplitCandidates {
    /// Creates an empty candidate feed with the supplied split policy.
    #[cfg(test)]
    fn with_policy(policy: SplitPolicy) -> Self {
        Self::with_policies(policy, InlinePolicy::default())
    }

    /// Creates an empty candidate feed with co-wired split and inline policies.
    fn with_policies(policy: SplitPolicy, inline: InlinePolicy) -> Self {
        SplitCandidates {
            policy,
            inline,
            queue: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// The soft-cap policy shared by the feed and the splitter.
    pub(crate) fn policy(&self) -> &SplitPolicy {
        &self.policy
    }

    fn hint_sink(&self) -> SplitHintSink {
        SplitHintSink {
            candidates: self.clone(),
        }
    }

    /// Drains every queued candidate, de-duplicated by path and cause, for one
    /// sweep cycle.
    fn drain(&self) -> Vec<SplitCandidate> {
        let mut q = self.queue.lock().unwrap();
        let mut by_path = std::collections::BTreeMap::<(ObjectPath, u8), SplitCandidate>::new();
        while let Some(candidate) = q.pop_front() {
            let key = (candidate.path.clone(), candidate.reason.class());
            match by_path.get_mut(&key) {
                Some(current) => current.merge(candidate),
                None => {
                    by_path.insert(key, candidate);
                }
            }
        }
        by_path.into_values().collect()
    }

    /// Requeues a deferred split without changing its wound-wait priority.
    fn requeue(&self, candidate: SplitCandidate) {
        self.push(candidate);
    }

    /// Adds one volatile candidate while keeping the best-effort feed bounded.
    fn push(&self, candidate: SplitCandidate) {
        let mut q = self.queue.lock().unwrap();
        if q.len() >= CANDIDATE_QUEUE_CAP {
            q.pop_front();
        }
        q.push_back(candidate);
    }

    /// Mints an operation id at normal transaction priority.
    fn new_id(&self) -> TxId {
        TxId::new_at(rt::system_now())
    }
}

impl SplitCandidate {
    /// Coalesces same-path, same-cause observations without sacrificing the
    /// oldest structural priority or the largest requested headroom.
    fn merge(&mut self, other: SplitCandidate) {
        if other.priority.older(&self.priority) {
            self.priority = other.priority.clone();
        }
        if let (
            SplitReason::InlinePressure { key, value_len },
            SplitReason::InlinePressure {
                key: other_key,
                value_len: other_len,
            },
        ) = (&mut self.reason, other.reason)
            && other_len > *value_len
        {
            *key = other_key;
            *value_len = other_len;
        }
    }
}

impl SplitHinter for SplitCandidates {
    /// Records that `path`'s leaf, now holding `entries`, may be a split
    /// candidate: over either the entry-count or the encoded-byte soft cap. A
    /// node needs at least two entries to be divisible, so a single hot key is
    /// never enqueued however large. The byte size is a hint the splitter
    /// re-checks authoritatively against the full node (which adds a little
    /// framing), so this need not account for it. The oldest hint is dropped
    /// when the queue is full.
    fn observe_leaf(&self, path: &ObjectPath, entries: &LeafBody) {
        let over_cap = entries.len() >= 2
            && (entries.len() > self.policy.leaf_max_entries()
                || entries.encoded_len() > self.policy.node_soft_max_bytes());
        if !over_cap {
            return;
        }
        self.push(SplitCandidate {
            path: path.clone(),
            priority: self.new_id(),
            reason: SplitReason::SoftCap,
        });
    }
}

/// Background executor that halves over-full B-link nodes (ADR-031). Holds no
/// per-transaction state: every split is a pure structural compare-and-swap
/// through the node and structural-intent stores, recovered idempotently like any
/// in-doubt CAS.
#[derive(Clone)]
pub struct Splitter {
    // Weak so a clone captured in the spawned loop does not keep the executor
    // alive across shutdown; `Engine` is the single strong owner.
    bg: Weak<Background>,
    records: CollectionStore,
    nodes: NodeStore,
    router: TreeRouter,
    mon: Monitor,
    structural_nodes: StructuralNodeAccess,
    timeline: Timeline,
    // The candidate feed this splitter drains. The coordinator receives a
    // clone for stored-leaf capacity; direct resolvers receive lightweight hint
    // sinks for inline-pressure observations.
    candidates: SplitCandidates,
    publisher: SeparatorPublisher,
    recovery: StructuralRecovery,
    // Wakes the independent recovery loop when a local split leaves `_s` work.
    recovery_wake: Arc<Notify>,
    // Paces collection-record and node CAS retries. Transaction-status polling remains
    // entirely owned by Monitor.
    retry: RetryConfig,
    cleanup_hints: TxCleanupHints,
    stats: Arc<Stats>,
}

impl Splitter {
    /// Builds a splitter and coordinator that share one timeline and
    /// split-candidate feed.
    #[allow(clippy::too_many_arguments)]
    pub fn with_coordinator(
        bg: Weak<Background>,
        records: CollectionStore,
        nodes: NodeStore,
        intent_store: StructuralIntentStore,
        timeline: Timeline,
        mon: Monitor,
        key_state: KeyStateResolver,
        retry: RetryConfig,
        db_root: DbRoot,
        policy: SplitPolicy,
        inline: InlinePolicy,
        cleanup_hints: TxCleanupHints,
    ) -> (LeafCoordinator, Self) {
        let candidates = SplitCandidates::with_policies(policy, inline);
        let coord = LeafCoordinator::with_hinter(
            nodes.clone(),
            key_state.clone(),
            mon.clone(),
            retry,
            policy,
            Arc::new(candidates.clone()),
        );
        let splitter = Splitter::with_candidates(
            bg,
            records,
            nodes,
            intent_store,
            timeline,
            mon,
            key_state,
            db_root,
            coord.clone(),
            candidates,
            retry,
            cleanup_hints,
        );
        (coord, splitter)
    }

    /// Returns a producer handle for split hints decided outside the leaf
    /// coordinator.
    pub fn hint_sink(&self) -> SplitHintSink {
        self.candidates.hint_sink()
    }

    /// Returns and resets background split activity counters.
    pub fn stats_and_reset(&self) -> SplitterStats {
        SplitterStats {
            candidates: self.stats.candidates.swap(0, Ordering::Relaxed),
            completed: self.stats.completed.swap(0, Ordering::Relaxed),
            deferred: self.stats.deferred.swap(0, Ordering::Relaxed),
            tombstones_reclaimed: self.stats.tombstones_reclaimed.swap(0, Ordering::Relaxed),
            splits_avoided: self.stats.splits_avoided.swap(0, Ordering::Relaxed),
            inline_pressure: InlinePressureStats {
                candidates: self
                    .stats
                    .inline_pressure_candidates
                    .swap(0, Ordering::Relaxed),
                completed: self
                    .stats
                    .inline_pressure_completed
                    .swap(0, Ordering::Relaxed),
                deferred: self
                    .stats
                    .inline_pressure_deferred
                    .swap(0, Ordering::Relaxed),
                discarded: self
                    .stats
                    .inline_pressure_discarded
                    .swap(0, Ordering::Relaxed),
            },
        }
    }

    /// Creates a splitter over an explicitly co-wired coordinator and feed.
    #[allow(clippy::too_many_arguments)]
    fn with_candidates(
        bg: Weak<Background>,
        records: CollectionStore,
        nodes: NodeStore,
        intent_store: StructuralIntentStore,
        timeline: Timeline,
        mon: Monitor,
        key_state: KeyStateResolver,
        db_root: DbRoot,
        coord: LeafCoordinator,
        candidates: SplitCandidates,
        retry: RetryConfig,
        cleanup_hints: TxCleanupHints,
    ) -> Self {
        let router = TreeRouter::new(nodes.clone(), std::num::NonZeroUsize::MIN);
        let structural_nodes =
            StructuralNodeAccess::new(nodes.clone(), mon.clone(), key_state, coord);
        let publisher = SeparatorPublisher::new(
            structural_nodes.clone(),
            router.clone(),
            timeline.clone(),
            *candidates.policy(),
        );
        let recovery = StructuralRecovery::new(
            records.clone(),
            nodes.clone(),
            intent_store.clone(),
            router.clone(),
            mon.clone(),
            structural_nodes.clone(),
            publisher.clone(),
            timeline.clone(),
            db_root,
            retry,
        );
        Splitter {
            bg,
            records,
            nodes,
            router,
            mon,
            structural_nodes,
            timeline,
            candidates,
            publisher,
            recovery,
            recovery_wake: Arc::new(Notify::new()),
            retry,
            cleanup_hints,
            stats: Arc::new(Stats::default()),
        }
    }

    /// Starts independent split-candidate and structural-recovery loops.
    pub fn start(&self) {
        let Some(bg) = self.bg.upgrade() else {
            return;
        };
        let splitter = self.clone();
        bg.spawn(async move {
            loop {
                rt::sleep(SPLIT_INTERVAL).await;
                splitter.run_once().await;
            }
        });
        let recovery = self.clone();
        bg.spawn(async move {
            loop {
                let active = recovery.recover_structural_intents().await;
                let delay = if active {
                    recovery.mon.protocol_timing().pending_timeout()
                } else {
                    STRUCTURAL_RECOVERY_IDLE_INTERVAL
                };
                tokio::select! {
                    _ = rt::sleep(delay) => {}
                    _ = recovery.recovery_wake.notified() => {}
                }
            }
        });
    }

    /// Runs one sweep: split every queued candidate. Best-effort — a transient
    /// error on one candidate only defers its split to a later cycle, so it is
    /// logged and the sweep continues.
    async fn run_once(&self) {
        for candidate in self.candidates.drain() {
            self.stats.candidates.fetch_add(1, Ordering::Relaxed);
            let pressure = candidate.reason.is_inline_pressure();
            if pressure {
                self.stats
                    .inline_pressure_candidates
                    .fetch_add(1, Ordering::Relaxed);
            }
            if let Err(e) = self.process_candidate(&candidate).await {
                tracing::debug!(
                    target: "glassdb::splitter",
                    path = %candidate.path,
                    error = %e,
                    "split candidate deferred"
                );
                let discard = pressure
                    && matches!(e, TransError::InvalidInput(_) | TransError::StaleCollection);
                if discard {
                    self.stats
                        .inline_pressure_discarded
                        .fetch_add(1, Ordering::Relaxed);
                } else if !matches!(e, TransError::InvalidInput(_)) {
                    self.stats.deferred.fetch_add(1, Ordering::Relaxed);
                    if pressure {
                        self.stats
                            .inline_pressure_deferred
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    self.candidates.requeue(candidate);
                }
            }
        }
        // Re-drive separators a previous cycle could not publish, so the parent
        // index eventually learns them and descent stops relying on right-links.
        for sep in self.publisher.drain_pending() {
            if let Err(e) = self
                .publish_separators(&sep.collection, &sep.split_key, &sep.new_token, None)
                .await
            {
                tracing::debug!(
                    target: "glassdb::splitter",
                    error = %e,
                    "separator publication deferred"
                );
            }
        }
    }

    /// Dispatches one candidate through its cause-specific validation path.
    async fn process_candidate(&self, candidate: &SplitCandidate) -> Result<(), TransError> {
        match &candidate.reason {
            SplitReason::SoftCap => {
                self.split_path_with_id(
                    &candidate.path,
                    candidate.priority.renew(),
                    &candidate.reason,
                )
                .await
            }
            SplitReason::InlinePressure { key, value_len } => {
                self.split_inline_pressure(
                    &candidate.path,
                    key,
                    *value_len,
                    candidate.priority.renew(),
                )
                .await
            }
        }
    }

    /// Reroutes and revalidates one pressure observation before splitting.
    async fn split_inline_pressure(
        &self,
        observed_path: &ObjectPath,
        key: &[u8],
        value_len: usize,
        id: TxId,
    ) -> Result<(), TransError> {
        let collection = split_collection(observed_path)?;
        let located = match self
            .router
            .route_key(collection, key, Requirement::AtLeast(self.timeline.now()))
            .await
        {
            Ok(located) => located,
            Err(StorageError::NotFound) => {
                self.stats
                    .inline_pressure_discarded
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        let reason = SplitReason::InlinePressure {
            key: key.to_vec(),
            value_len,
        };
        match located
            .node()
            .map(|node| self.split_need(node, &reason))
            .unwrap_or(SplitNeed::NotActionable)
        {
            SplitNeed::Split => self.split_path_with_id(&located.path, id, &reason).await,
            SplitNeed::NotActionable => {
                self.stats
                    .inline_pressure_discarded
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            SplitNeed::Reroute => Err(TransError::Retry),
        }
    }

    /// Classifies whether `node` still needs the split represented by `reason`.
    fn split_need(&self, node: &Node, reason: &SplitReason) -> SplitNeed {
        match reason {
            SplitReason::SoftCap => {
                if node.over_soft_cap(self.candidates.policy()) {
                    SplitNeed::Split
                } else {
                    SplitNeed::NotActionable
                }
            }
            SplitReason::InlinePressure { key, value_len } => {
                let Some(leaf) = node.as_leaf() else {
                    return SplitNeed::Reroute;
                };
                if !node.covers(key) {
                    return SplitNeed::Reroute;
                }
                if !self.candidates.inline.admits_value(*value_len)
                    || leaf.len() < 2
                    || !leaf.lookup(key).is_some_and(LeafEntry::exists)
                {
                    return SplitNeed::NotActionable;
                }
                let other_inline_bytes = leaf
                    .entries()
                    .filter(|entry| entry.key.as_slice() != key.as_slice())
                    .map(|entry| entry.current.inline_len())
                    .sum();
                if self
                    .candidates
                    .inline
                    .admits(other_inline_bytes, *value_len)
                {
                    SplitNeed::NotActionable
                } else {
                    SplitNeed::Split
                }
            }
        }
    }

    /// Carries an oversized split output into a later sweep so one hint can
    /// drive the whole split cascade.
    fn enqueue_if_over_soft_cap(
        &self,
        collection: &CollectionAddress,
        token: &NodeToken,
        node: &Node,
    ) {
        if !node.over_soft_cap(self.candidates.policy()) {
            return;
        }
        self.candidates.push(SplitCandidate {
            path: ObjectPath::Node {
                collection: collection.clone(),
                token: token.clone(),
            },
            priority: self.candidates.new_id(),
            reason: SplitReason::SoftCap,
        });
    }

    /// Splits the leaf at object `path` if it is still over the soft cap: an
    /// in-place root split when `path` is the collection root `_r`, else a
    /// standalone node half-split.
    async fn split_path(&self, path: &ObjectPath) -> Result<(), TransError> {
        let reason = SplitReason::SoftCap;
        self.split_path_with_id(path, self.candidates.new_id(), &reason)
            .await
    }

    /// Splits `path` using an already-aged wound-wait priority.
    async fn split_path_with_id(
        &self,
        path: &ObjectPath,
        id: TxId,
        reason: &SplitReason,
    ) -> Result<(), TransError> {
        let (collection, target) = match path {
            ObjectPath::TreeRoot { collection } => (collection, StructuralSplitTarget::Root),
            ObjectPath::Node { collection, token } => {
                (collection, StructuralSplitTarget::NonRoot(token))
            }
            _ => return Err(TransError::other("split candidate is not a tree node")),
        };
        StructuralSplitAttempt::new(self, collection, target, id, reason)
            .run(StructuralSplitTopology::Owned)
            .await
    }

    async fn begin_topology_tx(
        &self,
        collection: &CollectionAddress,
        id: &TxId,
    ) -> Result<(), TransError> {
        self.mon
            .begin_persisted_tx(
                id,
                TxRecoveryManifest {
                    locks: vec![TxLock::Topology {
                        collection: collection.clone(),
                    }],
                    ..TxRecoveryManifest::default()
                },
            )
            .await
    }

    /// Splits `path` beneath an existing topology participant.
    async fn split_path_joined(
        &self,
        path: &ObjectPath,
        topology_participant: &TxId,
    ) -> Result<(), TransError> {
        let (collection, target) = match path {
            ObjectPath::TreeRoot { collection } => (collection, StructuralSplitTarget::Root),
            ObjectPath::Node { collection, token } => {
                (collection, StructuralSplitTarget::NonRoot(token))
            }
            _ => return Err(TransError::other("split candidate is not a tree node")),
        };
        // Recovery can publish separators after the topology participant has
        // finalized. A fresh structural identity prevents ordinary lock
        // helping from mistaking this in-flight recursive split for stale work.
        let worker = self.candidates.new_id();
        let reason = SplitReason::SoftCap;
        StructuralSplitAttempt::new(self, collection, target, worker, &reason)
            .run(StructuralSplitTopology::Joined(topology_participant))
            .await
    }

    /// Acquires a source node's structural gate under wound-wait. A leaf,
    /// including the fixed root while it is a leaf, joins the shared coordinator
    /// round. An index uses the direct structural CAS path.
    async fn acquire_structural_gate(
        &self,
        collection: &CollectionAddress,
        token: Option<&NodeToken>,
        id: &TxId,
    ) -> Result<Option<(Node, LeafObservation)>, TransError> {
        self.structural_nodes
            .acquire_structural_gate(collection, token, id)
            .await
    }

    /// Releases a structure-write holder after its node mutation has landed.
    async fn release_structural_gate(
        &self,
        collection: &CollectionAddress,
        token: Option<&NodeToken>,
        id: &TxId,
    ) -> Result<(), TransError> {
        self.structural_nodes
            .release_structural_gate(collection, token, id)
            .await
    }

    /// Stores a complete root or non-root node against an expected observation.
    async fn store_structural_node(
        &self,
        collection: &CollectionAddress,
        token: Option<&NodeToken>,
        node: &Node,
        observation: &LeafObservation,
    ) -> Result<bool, TransError> {
        self.structural_nodes
            .store_structural_node(collection, token, node, observation)
            .await
    }

    /// Releases the structural gate while the intent is still safe to discard.
    async fn cancel_preparing_split(
        &self,
        collection: &CollectionAddress,
        target: StructuralSplitTarget<'_>,
        worker: &TxId,
        result: Result<(), TransError>,
    ) -> SplitAttemptOutcome {
        let release = self
            .release_structural_gate(collection, target.source_token(), worker)
            .await;
        SplitAttemptOutcome::retry_cleanly(release.and(result))
    }

    /// Finishes an authoritative reason check that no longer calls for this
    /// source to split.
    async fn finish_without_split(
        &self,
        collection: &CollectionAddress,
        target: StructuralSplitTarget<'_>,
        worker: &TxId,
        reason: &SplitReason,
        need: SplitNeed,
    ) -> SplitAttemptOutcome {
        let result = match need {
            SplitNeed::NotActionable => {
                if reason.is_inline_pressure() {
                    self.stats
                        .inline_pressure_discarded
                        .fetch_add(1, Ordering::Relaxed);
                }
                Ok(())
            }
            SplitNeed::Reroute => Err(TransError::Retry),
            SplitNeed::Split => unreachable!("a required split must remain in coordination"),
        };
        self.cancel_preparing_split(collection, target, worker, result)
            .await
    }

    fn record_reclamation(&self, writers: &[TxId], split_avoided: bool) {
        if writers.is_empty() {
            return;
        }
        let count = u64::try_from(writers.len()).unwrap_or(u64::MAX);
        self.stats
            .tombstones_reclaimed
            .fetch_add(count, Ordering::Relaxed);
        if split_avoided {
            self.stats.splits_avoided.fetch_add(1, Ordering::Relaxed);
        }
        self.cleanup_hints.schedule_all(writers.iter().cloned());
    }

    /// Persists compaction and opens the gate in the same CAS when it made the
    /// candidate non-actionable. The split intent is still Preparing and can be
    /// discarded through the ordinary clean-cancellation path.
    async fn finish_reclamation_without_split(
        &self,
        collection: &CollectionAddress,
        target: StructuralSplitTarget<'_>,
        worker: &TxId,
        mut node: Node,
        observation: &LeafObservation,
        writers: &[TxId],
    ) -> SplitAttemptOutcome {
        node.remove_structural_gate(worker);
        match self
            .store_structural_node(collection, target.source_token(), &node, observation)
            .await
        {
            Ok(true) => {
                self.record_reclamation(writers, true);
                SplitAttemptOutcome::retry_cleanly(Ok(()))
            }
            Ok(false) => {
                self.cancel_preparing_split(collection, target, worker, Err(TransError::Retry))
                    .await
            }
            Err(error) => {
                let _ = self
                    .release_structural_gate(collection, target.source_token(), worker)
                    .await;
                SplitAttemptOutcome::retry_cleanly(Err(error))
            }
        }
    }

    /// Acquires, revalidates, and compacts one source before any split intent
    /// becomes recoverable.
    async fn prepare_split_source(
        &self,
        collection: &CollectionAddress,
        target: StructuralSplitTarget<'_>,
        worker: &TxId,
        reason: &SplitReason,
    ) -> Result<QuiescedSplitSource, SplitAttemptOutcome> {
        let (mut node, observation) = match self
            .acquire_structural_gate(collection, target.source_token(), worker)
            .await
        {
            Ok(Some(acquired)) => acquired,
            Ok(None) => {
                return Err(SplitAttemptOutcome::retry_cleanly(Err(TransError::Retry)));
            }
            Err(error) => return Err(SplitAttemptOutcome::retry_cleanly(Err(error))),
        };
        match self.split_need(&node, reason) {
            SplitNeed::Split => {}
            need => {
                return Err(self
                    .finish_without_split(collection, target, worker, reason, need)
                    .await);
            }
        }

        let reclaimed = reclaim_holder_free_tombstones(&mut node);
        match self.split_need(&node, reason) {
            SplitNeed::Split => Ok(QuiescedSplitSource {
                node,
                observation,
                reclaimed,
            }),
            SplitNeed::NotActionable if !reclaimed.is_empty() => Err(self
                .finish_reclamation_without_split(
                    collection,
                    target,
                    worker,
                    node,
                    &observation,
                    &reclaimed,
                )
                .await),
            need => Err(self
                .finish_without_split(collection, target, worker, reason, need)
                .await),
        }
    }

    /// Transitions the structural intent to Ready while its source gate is
    /// still held.
    async fn mark_split_ready(
        &self,
        worker: &TxId,
        prepared: PreparedIntent,
        observation: &LeafObservation,
        split_key: Vec<u8>,
    ) -> Result<ReadyIntent, SplitAttemptOutcome> {
        match self
            .recovery
            .mark_ready(prepared, worker, observation, split_key)
            .await
        {
            ReadyIntentTransition::Ready(ready) => Ok(ready),
            ReadyIntentTransition::RetryCleanly(error) => {
                Err(SplitAttemptOutcome::retry_cleanly(Err(error)))
            }
            ReadyIntentTransition::RecoveryRequired(ready, error) => {
                Err(SplitAttemptOutcome::recovery_required(ready, error))
            }
        }
    }

    /// Creates one immutable child reserved by a structural split.
    async fn create_split_node(
        &self,
        collection: &CollectionAddress,
        token: &NodeToken,
        node: &Node,
    ) -> Result<(), TransError> {
        if self.nodes.store_node(collection, token, node, None).await? {
            Ok(())
        } else {
            Err(TransError::Retry)
        }
    }

    /// Shrinks a non-root source against the observation in its Ready intent.
    async fn store_nonroot_split_source(
        &self,
        collection: &CollectionAddress,
        token: &NodeToken,
        node: &Node,
        observation: &LeafObservation,
    ) -> Result<(), TransError> {
        if self
            .nodes
            .store_node(collection, token, node, Some(observation))
            .await?
        {
            Ok(())
        } else {
            Err(TransError::Retry)
        }
    }

    /// Rewrites the fixed collection root against the observation in its Ready
    /// intent.
    async fn store_split_root(
        &self,
        collection: &CollectionAddress,
        index: &Node,
        observation: &LeafObservation,
    ) -> Result<(), TransError> {
        if self
            .store_structural_node(collection, None, index, observation)
            .await?
        {
            Ok(())
        } else {
            Err(TransError::Retry)
        }
    }

    /// Builds both root children and the replacement root index before the
    /// structural intent becomes recoverable.
    fn plan_root_split(
        &self,
        prepared: &PreparedIntent,
        node: &Node,
        worker: &TxId,
    ) -> Result<RootSplitPlan, TransError> {
        let (left_token, right_token) = prepared
            .root_children()
            .expect("a prepared root intent always reserves two children");
        let left_token = left_token.clone();
        let right_token = right_token.clone();
        let (left, right, split_key) = split_into_children(node, right_token.as_str(), worker);
        let index = Node::index(IndexNode::from_children([
            (Vec::new(), left_token.to_string()),
            (split_key.clone(), right_token.to_string()),
        ]));
        let policy = self.candidates.policy();
        if index.content_encoded_len() > policy.content_limit()
            || index.encoded_len() > policy.node_max_bytes()
        {
            return Err(TransError::InvalidInput(
                "root index exceeds the coordination node size limit".into(),
            ));
        }
        Ok(RootSplitPlan {
            left_token,
            right_token,
            left,
            right,
            index,
            split_key,
        })
    }

    /// Publishes statistics, follow-up candidates, and cleanup hints after the
    /// source/root linearization is acknowledged.
    fn record_completed_split(
        &self,
        collection: &CollectionAddress,
        reason: &SplitReason,
        reclaimed: &[TxId],
        outputs: [(&NodeToken, &Node); 2],
    ) {
        self.record_reclamation(reclaimed, false);
        self.stats.completed.fetch_add(1, Ordering::Relaxed);
        for (token, node) in outputs {
            self.enqueue_if_over_soft_cap(collection, token, node);
        }
        if reason.is_inline_pressure() {
            self.stats
                .inline_pressure_completed
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Deletes an acknowledged Ready intent or leaves it for recovery.
    async fn finish_ready_split(&self, ready: ReadyIntent) -> SplitAttemptOutcome {
        match self.recovery.complete_ready(ready).await {
            ReadyIntentCompletion::Completed => SplitAttemptOutcome::completed(),
            ReadyIntentCompletion::RecoveryRequired(ready, error) => SplitAttemptOutcome {
                result: Err(error),
                state: SplitAttemptResult::RecoveryRequired(ready),
            },
        }
    }

    /// Performs the write-ahead, sibling creation, shrink, and publication.
    async fn coordinate_nonroot_split(
        &self,
        collection: &CollectionAddress,
        token: &NodeToken,
        worker: &TxId,
        reason: &SplitReason,
        prepared: PreparedIntent,
    ) -> SplitAttemptOutcome {
        debug_assert!(prepared.targets(collection, Some(token)));
        let target = StructuralSplitTarget::NonRoot(token);
        let right_token = prepared
            .nonroot_sibling()
            .expect("a prepared non-root intent always reserves one sibling")
            .clone();
        let QuiescedSplitSource {
            mut node,
            observation,
            reclaimed,
        } = match self
            .prepare_split_source(collection, target, worker, reason)
            .await
        {
            Ok(source) => source,
            Err(outcome) => return outcome,
        };

        let Some((right, split_key)) = node.split(right_token.as_str()) else {
            return self
                .cancel_preparing_split(collection, target, worker, Ok(()))
                .await;
        };
        node.remove_structural_gate(worker);
        let ready = match self
            .mark_split_ready(worker, prepared, &observation, split_key.clone())
            .await
        {
            Ok(ready) => ready,
            Err(outcome) => return outcome,
        };
        if let Err(error) = self
            .create_split_node(collection, &right_token, &right)
            .await
        {
            return SplitAttemptOutcome::recovery_required(ready, error);
        }
        if let Err(error) = self
            .store_nonroot_split_source(collection, token, &node, &observation)
            .await
        {
            return SplitAttemptOutcome::recovery_required(ready, error);
        }
        self.record_completed_split(
            collection,
            reason,
            &reclaimed,
            [(token, &node), (&right_token, &right)],
        );
        if let Err(error) = self
            .publish_separators(
                collection,
                &split_key,
                &right_token,
                Some(ready.participant()),
            )
            .await
        {
            return SplitAttemptOutcome::recovery_required(ready, error);
        }
        self.finish_ready_split(ready).await
    }

    async fn join_topology(
        &self,
        collection: &CollectionAddress,
        id: &TxId,
    ) -> Result<(), TransError> {
        let mut backoff = self.retry.backoff();
        loop {
            let (mut record, observed) =
                match self.records.load_record(collection, Requirement::Any).await {
                    Ok(record) => record,
                    Err(StorageError::NotFound) => return Err(TransError::StaleCollection),
                    Err(error) => return Err(error.into()),
                };
            if record
                .topology_participants()
                .any(|participant| participant == id)
            {
                return Ok(());
            }
            if let Some(holder) = record.topology_freeze() {
                return match self.mon.tx_status(holder).await? {
                    TxCommitStatus::Aborted | TxCommitStatus::Wounded => {
                        let holder = holder.clone();
                        record.remove_topology_freeze(&holder);
                        if self.records.store_record(&record, &observed).await? {
                            continue;
                        }
                        rt::sleep(backoff.next_delay()).await;
                        continue;
                    }
                    TxCommitStatus::Ok => Err(TransError::StaleCollection),
                    TxCommitStatus::Pending | TxCommitStatus::Unknown => Err(TransError::Retry),
                };
            }
            if !record.add_topology_participant(id.clone()) {
                return Err(TransError::Retry);
            }
            if self.records.store_record(&record, &observed).await? {
                return Ok(());
            }
            rt::sleep(backoff.next_delay()).await;
        }
    }

    async fn leave_topology(
        &self,
        collection: &CollectionAddress,
        id: &TxId,
    ) -> Result<(), TransError> {
        self.recovery.leave_topology(collection, id).await
    }

    /// Performs the write-ahead, child creation, and root rewrite.
    async fn coordinate_root_split(
        &self,
        collection: &CollectionAddress,
        worker: &TxId,
        reason: &SplitReason,
        prepared: PreparedIntent,
    ) -> SplitAttemptOutcome {
        debug_assert!(prepared.targets(collection, None));
        let target = StructuralSplitTarget::Root;
        let QuiescedSplitSource {
            node,
            observation,
            reclaimed,
        } = match self
            .prepare_split_source(collection, target, worker, reason)
            .await
        {
            Ok(source) => source,
            Err(outcome) => return outcome,
        };
        let RootSplitPlan {
            left_token,
            right_token,
            left,
            right,
            index,
            split_key,
        } = match self.plan_root_split(&prepared, &node, worker) {
            Ok(plan) => plan,
            Err(error) => {
                return self
                    .cancel_preparing_split(collection, target, worker, Err(error))
                    .await;
            }
        };
        let ready = match self
            .mark_split_ready(worker, prepared, &observation, split_key)
            .await
        {
            Ok(ready) => ready,
            Err(outcome) => return outcome,
        };
        if let Err(error) = self.create_split_node(collection, &left_token, &left).await {
            return SplitAttemptOutcome::recovery_required(ready, error);
        }
        if let Err(error) = self
            .create_split_node(collection, &right_token, &right)
            .await
        {
            return SplitAttemptOutcome::recovery_required(ready, error);
        }
        if let Err(error) = self
            .store_split_root(collection, &index, &observation)
            .await
        {
            return SplitAttemptOutcome::recovery_required(ready, error);
        }
        self.record_completed_split(
            collection,
            reason,
            &reclaimed,
            [(&left_token, &left), (&right_token, &right)],
        );
        self.finish_ready_split(ready).await
    }

    /// Finalizes the split's ephemeral wound-wait identity without creating a
    /// transaction object. Structural state, not transaction status, records
    /// the split's durable outcome.
    async fn finalize_split(&self, id: &TxId) {
        self.structural_nodes.finalize_split(id).await;
    }

    async fn finalize_topology_split(&self, collection: &CollectionAddress, id: &TxId) {
        let mut log = TxLog::new(id.clone(), TxCommitStatus::Ok);
        log.locks.push(TxLock::Topology {
            collection: collection.clone(),
        });
        if let Err(e) = self.mon.commit_tx(log).await {
            tracing::debug!(
                target: "glassdb::splitter",
                error = %e,
                "finalizing topology participant failed"
            );
        }
    }

    /// Runs one durable structural-recovery sweep.
    async fn recover_structural_intents(&self) -> bool {
        let action = self.recovery.begin_sweep();
        match self.drive_recovery_action(action).await {
            Ok(active) => active,
            Err(error) => {
                tracing::debug!(
                    target: "glassdb::splitter",
                    error = %error,
                    "structural recovery action failed"
                );
                true
            }
        }
    }

    /// Executes recursive split requests for one opaque recovery action.
    async fn drive_recovery_action(&self, mut action: RecoveryAction) -> Result<bool, TransError> {
        loop {
            match self.recovery.advance(&mut action).await? {
                RecoveryStep::Completed { active } => return Ok(active),
                RecoveryStep::SplitParent { path, participant } => {
                    let result = Box::pin(self.split_path_joined(&path, &participant)).await;
                    action.resume_parent_split(result);
                }
            }
        }
    }

    /// Publishes the leaf separator(s) a split produced into the parent index so
    /// later descents route directly instead of walking right-links (ADR-031).
    ///
    /// Reconciles the leaf right-link chain against the parent: starting from
    /// the child the parent currently routes `split_key` to, it publishes every
    /// separator up to and including `split_key -> new_token` that the parent is
    /// missing. This heals a cascade — splitting a sibling whose own separator
    /// was never published still lands every intermediate separator — so the
    /// directory never grows unboundedly reliant on right-link walks. Idempotent
    /// (an already-published chain is a no-op) and re-drivable: a lost CAS is
    /// re-queued for a later sweep. On a successful insert that overflows the
    /// parent, recurses to split it.
    async fn publish_separators(
        &self,
        collection: &CollectionAddress,
        split_key: &[u8],
        new_token: &NodeToken,
        topology_participant: Option<&TxId>,
    ) -> Result<(), TransError> {
        let mut publication = self
            .publisher
            .begin_publication(collection, split_key, new_token);
        loop {
            match self.publisher.publish(&mut publication).await? {
                SeparatorPublicationOutcome::Published => return Ok(()),
                SeparatorPublicationOutcome::ParentRequiresSplit(action) => {
                    match topology_participant {
                        Some(id) => Box::pin(self.split_path_joined(&action.path, id)).await?,
                        None => Box::pin(self.split_path(&action.path)).await?,
                    }
                    if action.continuation == ParentSplitContinuation::CompletePublication {
                        return Ok(());
                    }
                }
            }
        }
    }
}

#[async_trait]
impl TopologySettler for Splitter {
    /// Completes structural recovery before releasing one finalized topology participant.
    async fn settle_topology_participant(
        &self,
        collection: &CollectionAddress,
        id: &TxId,
    ) -> Result<(), TransError> {
        let action = self.recovery.begin_participant_settlement(collection, id);
        self.drive_recovery_action(action).await.map(|_| ())
    }
}

/// Splits `node` (a root leaf or root index) into a lower and an upper child for
/// an in-place root split, returning `(left, right, split_key)`. `left` links to
/// `right_token`; `right` inherits `node`'s former bounds.
fn split_into_children(
    node: &Node,
    right_token: &str,
    structure_holder: &TxId,
) -> (Node, Node, Vec<u8>) {
    let mut source = node.clone();
    let (right, split_key) = source
        .split(right_token)
        .expect("root over the soft cap has at least two entries/children");
    source.remove_structural_gate(structure_holder);
    (source, right, split_key)
}

fn node_token(token: &str) -> Result<NodeToken, TransError> {
    NodeToken::try_from(token).map_err(|error| TransError::with_source("parsing node token", error))
}

fn split_collection(path: &ObjectPath) -> Result<&CollectionAddress, TransError> {
    match path {
        ObjectPath::TreeRoot { collection } | ObjectPath::Node { collection, .. } => Ok(collection),
        _ => Err(TransError::other("split candidate is not a tree node")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::engine::{AssemblyFixture, EngineConfig};
    use crate::monitor::TxFinalStatus;
    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{BackendOp, HookBackend, HookFuture, RecordingBackend};
    use glassdb_data::{LogicalKey, StructuralIntentId, TxId};
    use glassdb_storage::transaction::TxWrite;
    use glassdb_storage::{
        CachedStore, CollectionRecord, CollectionStore, CurrentState, LeafEntry, LockType,
        Observation, StructuralIntent, StructuralIntentPhase,
    };

    const COLL: &str = "db/_c/0000000000000000000000";

    struct NoSplitHints;

    impl SplitHinter for NoSplitHints {
        fn observe_leaf(&self, _path: &ObjectPath, _leaf: &LeafBody) {}
    }

    fn collection() -> CollectionAddress {
        CollectionAddress::root("db")
    }

    fn collection_at(prefix: &str) -> CollectionAddress {
        CollectionAddress::from_physical_prefix(prefix).unwrap()
    }

    fn db_root(value: &str) -> DbRoot {
        DbRoot::try_from(value).unwrap()
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
        .with_high_key(node.high_key().map(<[u8]>::to_vec))
        .with_right_sibling(
            node.right_sibling()
                .map(|token| test_token(token).to_string()),
        );
        canonical.set_locks(node.locks().clone());
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
    fn tiny() -> SplitPolicy {
        SplitPolicy::builder()
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
                    &db_root("db"),
                    &StructuralIntentId::from(test_token(intent_id)),
                    &canonical_intent(intent),
                )
                .await
        }

        async fn list_structural_intents(
            &self,
            root: &str,
            requirement: Requirement,
        ) -> Result<Vec<(StructuralIntentId, Observation<StructuralIntent>)>, StorageError>
        {
            self.intent_store.list(&db_root(root), requirement).await
        }
    }

    fn store() -> TestStore {
        store_with_backend(Arc::new(MemoryBackend::new()))
    }

    fn store_with_backend(backend: Arc<dyn Backend>) -> TestStore {
        let mut config = EngineConfig::default();
        config.set_cache_size(1 << 20);
        let foundation = AssemblyFixture::new(backend, db_root("db"), &config);
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

    fn splitter(store: &TestStore, bg: &Arc<Background>, policy: SplitPolicy) -> Splitter {
        splitter_with_candidates(store, bg, SplitCandidates::with_policy(policy))
    }

    fn splitter_with_candidates(
        store: &TestStore,
        bg: &Arc<Background>,
        candidates: SplitCandidates,
    ) -> Splitter {
        splitter_with_candidates_and_hints(store, bg, candidates, TxCleanupHints::default())
    }

    fn splitter_with_candidates_and_hints(
        store: &TestStore,
        bg: &Arc<Background>,
        candidates: SplitCandidates,
        cleanup_hints: TxCleanupHints,
    ) -> Splitter {
        let mon = store.foundation.monitor_for(
            bg,
            RetryConfig::default(),
            crate::monitor::ProtocolTiming::default(),
        );
        splitter_with_monitor_and_hints(store, bg, mon, candidates, cleanup_hints)
    }

    fn splitter_with_monitor(
        store: &TestStore,
        bg: &Arc<Background>,
        mon: Monitor,
        candidates: SplitCandidates,
    ) -> Splitter {
        splitter_with_monitor_and_hints(store, bg, mon, candidates, TxCleanupHints::default())
    }

    fn splitter_with_monitor_and_hints(
        store: &TestStore,
        bg: &Arc<Background>,
        mon: Monitor,
        candidates: SplitCandidates,
        cleanup_hints: TxCleanupHints,
    ) -> Splitter {
        let key_state = KeyStateResolver::new(mon.clone());
        let coord = LeafCoordinator::with_hinter(
            store.nodes.clone(),
            key_state.clone(),
            mon.clone(),
            RetryConfig::default(),
            *candidates.policy(),
            Arc::new(candidates.clone()),
        );
        Splitter::with_candidates(
            Arc::downgrade(bg),
            store.records.clone(),
            store.nodes.clone(),
            store.intent_store.clone(),
            store.timeline.clone(),
            mon,
            key_state,
            db_root("db"),
            coord,
            candidates,
            RetryConfig::default(),
            cleanup_hints,
        )
    }

    fn splitter_and_monitor(
        store: &TestStore,
        bg: &Arc<Background>,
        policy: SplitPolicy,
    ) -> (Splitter, Monitor) {
        let mon = store.foundation.monitor_for(
            bg,
            RetryConfig::default(),
            crate::monitor::ProtocolTiming::default(),
        );
        let candidates = SplitCandidates::with_policy(policy);
        let splitter = splitter_with_monitor(store, bg, mon.clone(), candidates);
        (splitter, mon)
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

    fn nonroot_intent(source: &str, right: &str, split_key: &[u8]) -> StructuralIntent {
        StructuralIntent {
            collection: collection(),
            source_token: Some(test_token(source)),
            source_version: String::new(),
            created_tokens: vec![test_token(right)],
            split_key: split_key.to_vec(),
            participant_id: TxId::from_bytes(b"structural-participant".to_vec()),
            phase: StructuralIntentPhase::Ready,
        }
    }

    #[path = "recovery.rs"]
    mod recovery_tests;

    #[test]
    fn separator_queue_is_bounded_and_drops_the_oldest() {
        let s = store();
        let bg = Arc::new(Background::new());
        let publisher = splitter(&s, &bg, tiny()).publisher;
        for ordinal in 0..=CANDIDATE_QUEUE_CAP {
            publisher.defer(PendingSeparator {
                collection: collection(),
                split_key: ordinal.to_be_bytes().to_vec(),
                new_token: NodeToken::from_bytes((ordinal as u128).to_be_bytes()),
            });
        }

        let pending = publisher.drain_pending();
        assert_eq!(pending.len(), CANDIDATE_QUEUE_CAP);
        assert_eq!(pending[0].split_key, 1usize.to_be_bytes());
    }

    #[test]
    fn reclamation_removes_only_holder_free_tombstones() {
        let reclaimed_writer = TxId::with_priority(1, b"reclaimed");
        let retained_writer = TxId::with_priority(2, b"retained");
        let holder = TxId::with_priority(3, b"holder");
        let mut retained = tombstone(b"locked", retained_writer.clone());
        retained.acquire_read_lock(holder);
        let mut node = Node::leaf(LeafBody::from_entries([
            live(b"live"),
            tombstone(b"reclaimed", reclaimed_writer.clone()),
            retained,
        ]));
        let mut locks = node.locks().clone();
        locks.advance_membership_version();
        node.set_locks(locks);

        assert_eq!(
            reclaim_holder_free_tombstones(&mut node),
            vec![reclaimed_writer]
        );
        let leaf = node.as_leaf().unwrap();
        assert!(leaf.lookup(b"reclaimed").is_none());
        assert!(leaf.lookup(b"live").unwrap().exists());
        assert_eq!(
            leaf.lookup(b"locked").unwrap().current.writer(),
            Some(&retained_writer)
        );
        assert_eq!(node.membership_version(), 1);
    }

    #[tokio::test]
    async fn root_reclamation_can_avoid_an_actionable_split() {
        let s = store();
        let first = TxId::with_priority(2, b"first");
        let second = TxId::with_priority(3, b"second");
        let mut root = Node::leaf(LeafBody::from_entries([
            live(b"a"),
            tombstone(b"b", first.clone()),
            tombstone(b"c", second.clone()),
        ]));
        let mut locks = root.locks().clone();
        locks.advance_membership_version();
        locks.advance_membership_version();
        root.set_locks(locks);
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());
        let candidates = SplitCandidates::with_policy(tiny());
        candidates.observe_leaf(&root_path(), root.as_leaf().unwrap());
        let cleanup_hints = TxCleanupHints::default();
        let sp = splitter_with_candidates_and_hints(&s, &bg, candidates, cleanup_hints.clone());

        sp.run_once().await;

        let (root, _) = s
            .load_root(COLL, Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        let leaf = root.as_leaf().expect("compaction avoided height growth");
        assert_eq!(leaf.len(), 1);
        assert!(leaf.lookup(b"a").unwrap().exists());
        assert_eq!(root.membership_version(), 2);
        assert!(
            s.list_nodes(COLL, Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            sp.stats_and_reset(),
            SplitterStats {
                candidates: 1,
                tombstones_reclaimed: 2,
                splits_avoided: 1,
                ..SplitterStats::default()
            }
        );
        assert_eq!(cleanup_hints.pending(), vec![first, second]);
    }

    #[tokio::test]
    async fn root_leaf_gate_acquisition_uses_one_coordinator_round() {
        let recorder = Arc::new(RecordingBackend::new(Arc::new(MemoryBackend::new())));
        let operations = recorder.log();
        let seed = store_with_backend(recorder.clone());
        seed.create_root(COLL, &leaf_node(&[b"a"], None, None))
            .await
            .unwrap();

        // Use a new cache so the operation count includes the root load that
        // classifies the node before structural-gate acquisition.
        let s = store_with_backend(recorder);
        let bg = Arc::new(Background::new());
        let sp = splitter(&s, &bg, tiny());
        operations.lock().unwrap().clear();

        let worker = TxId::with_priority(1, b"root-gate");
        let (node, observation) = sp
            .structural_nodes
            .acquire_structural_gate(&collection(), None, &worker)
            .await
            .unwrap()
            .expect("the root leaf can acquire its structural gate");

        assert!(node.structural_gate().contains(&worker));
        assert_eq!(observation.path(), &root_path());
        assert_eq!(
            sp.structural_nodes.coord.stats_and_reset(),
            crate::leaf_coord::LeafCoordinatorStats {
                submissions: 1,
                rounds: 1,
                cas_retries: 0,
            }
        );
        let root_path = root_path().to_string();
        let root_operations: Vec<_> = operations
            .lock()
            .unwrap()
            .iter()
            .filter(|operation| operation.path == root_path)
            .map(|operation| operation.op)
            .collect();
        assert_eq!(root_operations, ["read", "write_if"]);

        sp.structural_nodes
            .release_structural_gate(&collection(), None, &worker)
            .await
            .unwrap();
        let (root, _) = s
            .load_root(COLL, Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert!(root.structural_gate().holders().is_empty());
    }

    #[tokio::test]
    async fn failed_compaction_does_not_publish_reclamation_outcomes() {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let s = store_with_backend(backend.clone());
        let root = Node::leaf(LeafBody::from_entries([
            live(b"a"),
            tombstone(b"b", TxId::with_priority(2, b"deleted")),
            tombstone(b"c", TxId::with_priority(3, b"other")),
        ]));
        s.create_root(COLL, &root).await.unwrap();
        let failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        backend.set_before({
            let failed = failed.clone();
            move |operation| {
                let reject = matches!(
                    operation,
                    BackendOp::WriteIf { path, value, .. }
                        if path.ends_with("/_r")
                            && Node::decode(value).is_ok_and(|node| {
                                node.as_leaf().is_some_and(|leaf| leaf.len() == 1)
                                    && node.structural_gate().holders().is_empty()
                            })
                ) && !failed.swap(true, std::sync::atomic::Ordering::SeqCst);
                let result = if reject {
                    Err(glassdb_backend::BackendError::Precondition)
                } else {
                    Ok(())
                };
                let future: HookFuture = Box::pin(async move { result });
                future
            }
        });
        let bg = Arc::new(Background::new());
        let candidates = SplitCandidates::with_policy(tiny());
        candidates.observe_leaf(&root_path(), root.as_leaf().unwrap());
        let cleanup_hints = TxCleanupHints::default();
        let sp = splitter_with_candidates_and_hints(&s, &bg, candidates, cleanup_hints.clone());

        sp.run_once().await;

        assert!(failed.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            sp.stats_and_reset(),
            SplitterStats {
                candidates: 1,
                deferred: 1,
                ..SplitterStats::default()
            }
        );
        assert!(cleanup_hints.pending().is_empty());
        let (root, _) = s
            .load_root(COLL, Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert_eq!(root.as_leaf().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn nonroot_split_partitions_the_compacted_leaf() {
        let s = store();
        let writer = TxId::with_priority(2, b"deleted");
        let mut source = Node::leaf(LeafBody::from_entries([
            live(b"a"),
            live(b"b"),
            live(b"c"),
            tombstone(b"d", writer.clone()),
        ]));
        let mut locks = source.locks().clone();
        locks.advance_membership_version();
        locks.advance_membership_version();
        source.set_locks(locks);
        s.store_node(COLL, "L", &source, None).await.unwrap();
        s.create_root(
            COLL,
            &Node::index(IndexNode::from_children([(Vec::new(), "L".to_string())])),
        )
        .await
        .unwrap();
        let bg = Arc::new(Background::new());
        let candidates = SplitCandidates::with_policy(tiny());
        candidates.observe_leaf(&node_path("L"), source.as_leaf().unwrap());
        let cleanup_hints = TxCleanupHints::default();
        let sp = splitter_with_candidates_and_hints(&s, &bg, candidates, cleanup_hints.clone());

        sp.run_once().await;

        let leaves = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN)
            .leaves(&collection(), Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert_eq!(leaves.len(), 2);
        assert!(leaves.iter().all(|leaf| {
            let node = leaf.node().unwrap();
            node.membership_version() == 2
                && node
                    .as_leaf()
                    .unwrap()
                    .entries()
                    .all(|entry| !entry.current.is_tombstone())
        }));
        assert_eq!(
            sp.stats_and_reset(),
            SplitterStats {
                candidates: 1,
                completed: 1,
                tombstones_reclaimed: 1,
                ..SplitterStats::default()
            }
        );
        assert_eq!(cleanup_hints.pending(), vec![writer]);
    }

    // ADR-051: an inline value may be a key's only copy, so a split has to move
    // it to the new leaf verbatim.
    #[tokio::test]
    async fn a_split_carries_inline_values_to_the_new_leaf() {
        let s = store();
        let keys: [&[u8]; 4] = [b"a", b"b", b"c", b"d"];
        let inlined = |key: &[u8]| {
            LeafEntry::new(key).with_current(CurrentState::Inline {
                writer: TxId::from_bytes(vec![1]),
                value: Arc::from(key),
            })
        };
        s.create_root(COLL, &Node::leaf(LeafBody::from_entries(keys.map(inlined))))
            .await
            .unwrap();
        let bg = Arc::new(Background::new());

        splitter(&s, &bg, tiny())
            .split_path(&root_path())
            .await
            .unwrap();

        let router = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN);
        assert_eq!(
            router
                .leaves(&collection(), Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .len(),
            2,
            "one leaf became two"
        );
        for key in keys {
            let loc = router
                .route_key(&collection(), key, Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap();
            let entry = loc.node().unwrap().as_leaf().unwrap().lookup(key).cloned();
            assert_eq!(entry, Some(inlined(key)), "key {key:?} lost its value");
        }
    }

    // A small collection whose single leaf lives in the root `_r`; when it grows
    // past the cap the root splits in place into a two-child index, raising the
    // height, and every key stays reachable in key order.
    #[tokio::test]
    async fn root_leaf_splits_in_place_into_an_index() {
        let s = store();
        let root = Node::leaf(LeafBody::from_entries(
            [b"a".as_slice(), b"b", b"c", b"d"].iter().map(|k| live(k)),
        ));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());

        splitter(&s, &bg, tiny())
            .split_path(&root_path())
            .await
            .unwrap();

        // The root is now an index (height grew from 1 to 2).
        let (node, _) = s
            .load_root_node(COLL, Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap()
            .unwrap();
        assert!(node.as_index().is_some(), "root became an index");

        let router = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN);
        let leaves = router
            .leaves(&collection(), Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert_eq!(leaves.len(), 2, "one leaf became two");
        // The lower leaf is bounded by the split key and links to the upper one.
        assert!(leaves[0].node().unwrap().right_sibling().is_some());
        assert_eq!(
            leaves[0].node().unwrap().high_key(),
            Some(
                leaves[1]
                    .node()
                    .unwrap()
                    .as_leaf()
                    .unwrap()
                    .entries()
                    .next()
                    .unwrap()
                    .key
                    .as_slice()
            ),
        );
        // Every key remains reachable by descent, in order.
        for k in [b"a".as_slice(), b"b", b"c", b"d"] {
            let loc = router
                .route_key(&collection(), k, Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap();
            assert!(
                loc.node().unwrap().as_leaf().unwrap().exists(k),
                "key {k:?} lost"
            );
        }
        assert!(
            s.list_structural_intents("db", Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .is_empty()
        );
        let transaction_prefix = format!("{}/_t/", db_root("db"));
        assert_eq!(
            s.objects
                .list(
                    &transaction_prefix,
                    None,
                    glassdb_backend::ListLimit::new(2).unwrap(),
                )
                .await
                .unwrap()
                .objects
                .len(),
            1,
            "the topology participant remains recoverable until GC"
        );
    }

    // A standalone leaf over the cap half-splits: the upper half moves to a fresh
    // sibling, the source shrinks and links to it, and the parent index learns
    // the separator so later descents skip the right-link hop.
    #[tokio::test]
    async fn nonroot_leaf_half_splits_and_parent_learns_the_separator() {
        let s = store();
        s.store_node(
            COLL,
            "L",
            &leaf_node(&[b"a", b"b", b"c", b"d"], None, None),
            None,
        )
        .await
        .unwrap();
        let root = Node::index(IndexNode::from_children([(Vec::new(), "L".to_string())]));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());

        splitter(&s, &bg, tiny())
            .split_path(&node_path("L"))
            .await
            .unwrap();

        let router = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN);
        let leaves = router
            .leaves(&collection(), Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert_eq!(leaves.len(), 2, "leaf L split into two");
        // The parent index now routes the moved keys directly to the sibling, not
        // via a right-link walk: its child for the split key differs from L.
        let (root_node, _) = s
            .load_root_node(COLL, Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap()
            .unwrap();
        let index = root_node.as_index().unwrap();
        assert_eq!(index.len(), 2, "parent gained the separator");
        for k in [b"a".as_slice(), b"b", b"c", b"d"] {
            let loc = router
                .route_key(&collection(), k, Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap();
            assert!(
                loc.node().unwrap().as_leaf().unwrap().exists(k),
                "key {k:?} lost"
            );
        }
        assert!(
            s.list_structural_intents("db", Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .is_empty()
        );
    }

    // A leaf separator is published before its newly-over-cap parent is split.
    // The parent split must therefore carry that separator into the next level.
    #[tokio::test]
    async fn separator_publication_cascades_after_the_parent_insert() {
        let s = store();
        s.store_node(
            COLL,
            "L0",
            &leaf_node(&[b"a"], Some(b"m"), Some("L1")),
            None,
        )
        .await
        .unwrap();
        s.store_node(
            COLL,
            "L1",
            &leaf_node(&[b"m", b"n", b"o"], None, None),
            None,
        )
        .await
        .unwrap();
        s.create_root(
            COLL,
            &Node::index(IndexNode::from_children([
                (Vec::new(), "L0".to_string()),
                (b"m".to_vec(), "L1".to_string()),
            ])),
        )
        .await
        .unwrap();
        let bg = Arc::new(Background::new());

        splitter(&s, &bg, tiny())
            .split_path(&node_path("L1"))
            .await
            .unwrap();

        let (root, _) = s
            .load_root_node(COLL, Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap()
            .unwrap();
        let child_tokens: Vec<_> = root
            .as_index()
            .unwrap()
            .children()
            .map(|(_, token)| token.to_string())
            .collect();
        assert_eq!(child_tokens.len(), 2, "the root grew by one level");
        for token in child_tokens {
            let (child, _) = s
                .load_node(COLL, &token, Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap();
            assert!(child.as_index().is_some(), "root children are indexes");
        }

        let router = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN);
        assert_eq!(
            router
                .leaves(&collection(), Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .len(),
            3,
            "the published sibling remains reachable after the parent split"
        );
        for key in [b"a".as_slice(), b"m", b"n", b"o"] {
            let leaf = router
                .route_key(&collection(), key, Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap();
            assert!(leaf.node().unwrap().as_leaf().unwrap().exists(key));
        }
    }

    // An index root that overflows its fan-out splits in place: two index children
    // are created and the root is rewritten over them, so all original children
    // remain reachable one level deeper.
    #[tokio::test]
    async fn root_index_splits_in_place_growing_height() {
        let s = store();
        // Three leaves under a three-child index root (over a two-child cap).
        for (tok, keys, high, right) in [
            (
                "L0",
                vec![b"a".as_slice()],
                Some(b"m".as_slice()),
                Some("L1"),
            ),
            (
                "L1",
                vec![b"m".as_slice()],
                Some(b"t".as_slice()),
                Some("L2"),
            ),
            ("L2", vec![b"t".as_slice()], None, None),
        ] {
            s.store_node(COLL, tok, &leaf_node(&keys, high, right), None)
                .await
                .unwrap();
        }
        let root = Node::index(IndexNode::from_children([
            (Vec::new(), "L0".to_string()),
            (b"m".to_vec(), "L1".to_string()),
            (b"t".to_vec(), "L2".to_string()),
        ]));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());

        splitter(&s, &bg, tiny())
            .split_path(&root_path())
            .await
            .unwrap();

        let (node, _) = s
            .load_root_node(COLL, Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            node.as_index().unwrap().len(),
            2,
            "root now has two index children"
        );
        // Every original leaf is still reached in order (now via one more hop).
        let router = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN);
        for k in [b"a".as_slice(), b"m", b"t"] {
            let loc = router
                .route_key(&collection(), k, Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap();
            assert!(
                loc.node().unwrap().as_leaf().unwrap().exists(k),
                "key {k:?} lost"
            );
        }
    }

    // Re-running a split on a node already back under the cap is a no-op: the
    // splitter reloads, sees it is not over the cap, and leaves the tree alone.
    #[tokio::test]
    async fn re_split_of_a_settled_node_is_a_noop() {
        let s = store();
        let root = Node::leaf(LeafBody::from_entries(
            [b"a".as_slice(), b"b", b"c", b"d"].iter().map(|k| live(k)),
        ));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());
        let sp = splitter(&s, &bg, tiny());

        sp.split_path(&root_path()).await.unwrap();
        let after_first = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN)
            .leaves(&collection(), Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        // Re-run: each resulting leaf holds two keys, which is at (not over) the
        // cap, so nothing changes.
        for leaf in &after_first {
            sp.split_path(&leaf.path).await.unwrap();
        }
        sp.split_path(&root_path()).await.unwrap();

        let after_second = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN)
            .leaves(&collection(), Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert_eq!(
            after_first.len(),
            after_second.len(),
            "a settled tree does not keep splitting"
        );
        assert!(
            s.list_structural_intents("db", Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .is_empty(),
            "a no-op split cleans its Preparing intent"
        );
        let (record, _) = s
            .records
            .load_record(&collection(), Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert_eq!(record.topology_participants().count(), 0);
    }

    // The candidate feed drives run_once end to end: a leaf pushed over the cap is
    // drained and split; the byte/entry gate keeps under-cap leaves out.
    #[tokio::test]
    async fn feed_drives_run_once() {
        let s = store();
        let root = Node::leaf(LeafBody::from_entries(
            [b"a".as_slice(), b"b", b"c", b"d"].iter().map(|k| live(k)),
        ));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());

        let candidates = SplitCandidates::with_policy(tiny());
        // Under the cap: not enqueued.
        candidates.observe_leaf(
            &root_path(),
            &LeafBody::from_entries([live(b"a"), live(b"b")]),
        );
        assert!(
            candidates.drain().is_empty(),
            "at-cap leaf is not a candidate"
        );
        // Over the cap: enqueued and split by a sweep.
        candidates.observe_leaf(
            &root_path(),
            &LeafBody::from_entries([live(b"a"), live(b"b"), live(b"c"), live(b"d")]),
        );
        let sp = splitter_with_candidates(&s, &bg, candidates);
        sp.run_once().await;

        let leaves = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN)
            .leaves(&collection(), Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert_eq!(leaves.len(), 2, "the fed candidate was split");
        assert_eq!(
            sp.stats_and_reset(),
            SplitterStats {
                candidates: 1,
                completed: 1,
                deferred: 0,
                ..SplitterStats::default()
            }
        );
        assert_eq!(sp.stats_and_reset(), SplitterStats::default());
    }

    // One large mutation can overshoot the soft cap by enough that halving the
    // leaf once leaves both outputs oversized. The outputs must feed the next
    // sweep themselves; no later mutation should be needed to finish the tree.
    #[tokio::test]
    async fn one_hint_cascades_until_every_leaf_is_under_cap() {
        let s = store();
        let keys: [&[u8]; 9] = [b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h", b"i"];
        let root = Node::leaf(LeafBody::from_entries(keys.iter().map(|key| live(key))));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());
        let policy = SplitPolicy::builder()
            .leaf_max_entries(2)
            .node_soft_max_bytes(1 << 20)
            .index_max_children(100)
            .build()
            .unwrap();
        let candidates = SplitCandidates::with_policy(policy);
        candidates.observe_leaf(&root_path(), root.as_leaf().unwrap());
        let sp = splitter_with_candidates(&s, &bg, candidates);

        // 9 -> 4+5 -> 2+2+2+3 -> 2+2+2+1+2.
        for _ in 0..3 {
            sp.run_once().await;
        }

        let router = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN);
        let leaves = router
            .leaves(&collection(), Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert_eq!(leaves.len(), 5);
        assert!(leaves.iter().all(|leaf| {
            leaf.node().unwrap().as_leaf().unwrap().len() <= policy.leaf_max_entries()
        }));
        for key in keys {
            let located = router
                .route_key(&collection(), key, Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap();
            assert!(located.node().unwrap().as_leaf().unwrap().exists(key));
        }
    }

    #[tokio::test]
    async fn repeated_inline_pressure_performs_one_rerouted_median_split_each() {
        let s = store();
        let root = Node::leaf(LeafBody::from_entries([
            live(b"a"),
            live(b"b"),
            live(b"c"),
            live(b"d"),
            live(b"e"),
            inline_live(b"f", b"12345678"),
            live(b"g"),
            live(b"h"),
        ]));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());
        let candidates = SplitCandidates::with_policies(SplitPolicy::default(), pressure_inline());
        let sp = splitter_with_candidates(&s, &bg, candidates.clone());
        let root_path = root_path();

        candidates
            .hint_sink()
            .observe_inline_pressure(&root_path, b"h", 8);
        sp.run_once().await;

        let router = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN);
        assert_eq!(
            router
                .leaves(&collection(), Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .len(),
            2,
            "one request performs only the root's median split"
        );
        assert_eq!(
            sp.stats_and_reset(),
            SplitterStats {
                candidates: 1,
                completed: 1,
                inline_pressure: InlinePressureStats {
                    candidates: 1,
                    completed: 1,
                    ..InlinePressureStats::default()
                },
                ..SplitterStats::default()
            }
        );

        let target = router
            .route_key(&collection(), b"h", Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        let reason = SplitReason::InlinePressure {
            key: b"h".to_vec(),
            value_len: 8,
        };
        assert!(
            matches!(
                target.node().map(|node| sp.split_need(node, &reason)),
                Some(SplitNeed::Split)
            ),
            "the first median left real pressure for a future observation"
        );

        // The old root path is deliberately stale now. Key-directed
        // revalidation must find and split the currently routed child.
        candidates
            .hint_sink()
            .observe_inline_pressure(&root_path, b"h", 8);
        sp.run_once().await;

        assert_eq!(
            router
                .leaves(&collection(), Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .len(),
            3,
            "a second real observation drives exactly one more split"
        );
        assert_eq!(
            sp.stats_and_reset(),
            SplitterStats {
                candidates: 1,
                completed: 1,
                inline_pressure: InlinePressureStats {
                    candidates: 1,
                    completed: 1,
                    ..InlinePressureStats::default()
                },
                ..SplitterStats::default()
            }
        );
        let target = router
            .route_key(&collection(), b"h", Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert!(matches!(
            target.node().map(|node| sp.split_need(node, &reason)),
            Some(SplitNeed::NotActionable)
        ));
    }

    #[tokio::test]
    async fn inline_pressure_is_discarded_after_authoritative_revalidation() {
        let s = store();
        let root = Node::leaf(LeafBody::from_entries([live(b"a"), live(b"b")]));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());
        let candidates = SplitCandidates::with_policies(SplitPolicy::default(), pressure_inline());
        let sp = splitter_with_candidates(&s, &bg, candidates.clone());
        let root_path = root_path();

        candidates
            .hint_sink()
            .observe_inline_pressure(&root_path, b"b", 8);
        sp.run_once().await;
        assert_eq!(
            sp.stats_and_reset(),
            SplitterStats {
                candidates: 1,
                inline_pressure: InlinePressureStats {
                    candidates: 1,
                    discarded: 1,
                    ..InlinePressureStats::default()
                },
                ..SplitterStats::default()
            },
            "a value that now fits does not reshape the tree"
        );

        candidates
            .hint_sink()
            .observe_inline_pressure(&root_path, b"missing", 8);
        sp.run_once().await;
        assert_eq!(
            sp.stats_and_reset(),
            SplitterStats {
                candidates: 1,
                inline_pressure: InlinePressureStats {
                    candidates: 1,
                    discarded: 1,
                    ..InlinePressureStats::default()
                },
                ..SplitterStats::default()
            },
            "a key that disappeared does not reshape the tree"
        );
        assert!(
            s.load_root_node(COLL, Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .unwrap()
                .0
                .as_leaf()
                .is_some()
        );
    }

    #[tokio::test]
    async fn contended_candidate_is_requeued() {
        let s = store();
        let holder = TxId::with_priority(0, b"holder");
        let mut node = Node::leaf(LeafBody::from_entries(
            [b"a".as_slice(), b"b", b"c", b"d"].iter().map(|k| live(k)),
        ));
        node.add_membership_reader(holder.clone());
        let root = node;
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());
        let candidates = SplitCandidates::with_policy(tiny());
        candidates.observe_leaf(
            &root_path(),
            &LeafBody::from_entries([live(b"a"), live(b"b"), live(b"c"), live(b"d")]),
        );
        let sp = splitter_with_candidates(&s, &bg, candidates);

        sp.run_once().await;
        assert_eq!(
            sp.stats_and_reset(),
            SplitterStats {
                candidates: 1,
                completed: 0,
                deferred: 1,
                ..SplitterStats::default()
            }
        );
        assert!(
            s.load_root_node(COLL, Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .unwrap()
                .0
                .as_leaf()
                .is_some(),
            "an older holder defers the split"
        );

        let (mut root, observation) = s
            .load_root(COLL, Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        root.remove_membership_holder(&holder);
        assert!(s.store_root(COLL, &root, &observation).await.unwrap());

        sp.run_once().await;
        assert_eq!(
            sp.stats_and_reset(),
            SplitterStats {
                candidates: 1,
                completed: 1,
                deferred: 0,
                ..SplitterStats::default()
            }
        );
        assert!(
            s.load_root_node(COLL, Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .unwrap()
                .0
                .as_index()
                .is_some(),
            "the retained candidate splits after the holder leaves"
        );
    }

    #[tokio::test]
    async fn split_wounds_a_younger_entry_holder_and_lands() {
        let s = store();
        let bg = Arc::new(Background::new());
        let (sp, mon) = splitter_and_monitor(&s, &bg, tiny());
        let younger = TxId::new_at(rt::system_now() + Duration::from_secs(1));
        mon.begin_tx(&younger);
        s.store_node(
            COLL,
            "L",
            &leaf_with_locked_entry(&[b"a", b"b", b"c", b"d"], &younger),
            None,
        )
        .await
        .unwrap();
        let root = Node::index(IndexNode::from_children([(Vec::new(), "L".to_string())]));
        s.create_root(COLL, &root).await.unwrap();

        sp.split_path(&node_path("L")).await.unwrap();

        assert_eq!(
            mon.tx_status(&younger).await.unwrap(),
            TxCommitStatus::Wounded
        );
        let leaves = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN)
            .leaves(&collection(), Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert_eq!(leaves.len(), 2);
        for leaf in leaves {
            let node = leaf.node().unwrap();
            assert!(node.structural_gate().holders().is_empty());
            assert!(
                node.as_leaf()
                    .unwrap()
                    .entries()
                    .all(|entry| !entry.is_locked_by(&younger))
            );
        }
    }

    #[tokio::test]
    async fn split_help_forwards_a_committed_entry_holder_before_moving_its_entry() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let s = store_with_backend(backend.clone());
        let other = store_with_backend(backend);
        let bg = Arc::new(Background::new());
        let (sp, mon) = splitter_and_monitor(&s, &bg, tiny());
        let holder = TxId::with_priority(1, b"committed");
        mon.begin_tx(&holder);
        let mut log = TxLog::new(holder.clone(), TxCommitStatus::Ok);
        log.writes.push(TxWrite {
            key: LogicalKey::new(collection(), b"d"),
            value: Arc::from(b"new-d".as_slice()),
            deleted: false,
            prev_writer: TxId::from_bytes(vec![1]),
        });
        mon.commit_tx(log).await.unwrap();

        let mut entries: Vec<_> = [b"a".as_slice(), b"b", b"c", b"d"]
            .iter()
            .map(|key| live(key))
            .collect();
        let upper = entries.last_mut().unwrap();
        upper.replace_write_lock(holder.clone());
        let node = Node::leaf(LeafBody::from_entries(entries));
        s.store_node(COLL, "L", &node, None).await.unwrap();
        let root = Node::index(IndexNode::from_children([(Vec::new(), "L".to_string())]));
        s.create_root(COLL, &root).await.unwrap();

        sp.split_path(&node_path("L")).await.unwrap();

        let leaf = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN)
            .route_key(&collection(), b"d", Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert!(leaf.node().unwrap().structural_gate().holders().is_empty());
        let entry = leaf
            .node()
            .unwrap()
            .as_leaf()
            .unwrap()
            .entries()
            .find(|entry| entry.key == b"d")
            .unwrap();
        assert_eq!(
            entry.current,
            CurrentState::External {
                writer: holder.clone()
            }
        );
        assert!(entry.lock_holders().is_empty());
        assert_eq!(entry.lock_type(), LockType::None);

        // A different instance still targeting the pre-split source must
        // re-descend and converge without recreating the removed holder.
        let other_bg = Arc::new(Background::new());
        let other_transactions = other.foundation.tlogger.clone();
        let other_mon = other.foundation.monitor_for(
            &other_bg,
            RetryConfig::default(),
            crate::monitor::ProtocolTiming::default(),
        );
        let other_key_state = KeyStateResolver::new(other_mon.clone());
        let other_coord = LeafCoordinator::with_hinter(
            other.nodes.clone(),
            other_key_state,
            other_mon.clone(),
            RetryConfig::default(),
            SplitPolicy::default(),
            Arc::new(NoSplitHints),
        );
        let other_locker = crate::tlocker::Locker::new(
            other_coord,
            TreeRouter::new(other.nodes.clone(), std::num::NonZeroUsize::MIN),
            crate::collection_coordination::CollectionStateResolver::new(
                other.records.clone(),
                other_transactions,
                other_mon.clone(),
                RetryConfig::default(),
            ),
            other_mon,
            RetryConfig::default(),
            std::num::NonZeroUsize::MIN,
        );
        other_locker
            .keys()
            .write_back_one_put(
                &holder,
                &node_path("L"),
                b"d",
                &LogicalKey::new(collection(), b"d"),
            )
            .await;
        let current = TreeRouter::new(other.nodes.clone(), std::num::NonZeroUsize::MIN)
            .route_key(&collection(), b"d", Requirement::Any)
            .await
            .unwrap();
        let current = current
            .node()
            .unwrap()
            .as_leaf()
            .unwrap()
            .lookup(b"d")
            .unwrap();
        assert_eq!(current.current, CurrentState::External { writer: holder });
        assert!(current.lock_holders().is_empty());
    }

    #[tokio::test]
    async fn split_defers_to_an_older_membership_reader_then_lands() {
        let s = store();
        let bg = Arc::new(Background::new());
        let (sp, mon) = splitter_and_monitor(&s, &bg, tiny());
        let older = TxId::new_at(rt::system_now() - Duration::from_secs(1));
        mon.begin_tx(&older);
        s.store_node(
            COLL,
            "L",
            &leaf_with_membership_reader(&[b"a", b"b", b"c", b"d"], &older),
            None,
        )
        .await
        .unwrap();
        let root = Node::index(IndexNode::from_children([(Vec::new(), "L".to_string())]));
        s.create_root(COLL, &root).await.unwrap();

        sp.candidates.observe_leaf(
            &node_path("L"),
            &LeafBody::from_entries([live(b"a"), live(b"b"), live(b"c"), live(b"d")]),
        );
        sp.run_once().await;
        assert_eq!(
            TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN)
                .leaves(&collection(), Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .len(),
            1
        );

        mon.abort_owned_tx(&older).await.unwrap();
        sp.run_once().await;
        assert_eq!(
            TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN)
                .leaves(&collection(), Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .len(),
            2
        );
    }

    // ADR-031 byte cap: a leaf well under the entry cap but over the encoded
    // byte cap is still fed and split. Regression for the byte cap having no
    // producer (only the entry-count crossing used to enqueue).
    #[tokio::test]
    async fn byte_cap_enqueues_and_splits_below_entry_cap() {
        let s = store();
        let root = Node::leaf(LeafBody::from_entries(
            [b"a".as_slice(), b"b", b"c", b"d"].iter().map(|k| live(k)),
        ));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());

        // A generous entry cap but a tiny byte cap: the four-entry leaf is far
        // under the entry cap yet over the byte cap.
        let policy = SplitPolicy::builder()
            .leaf_max_entries(1000)
            .node_soft_max_bytes(8)
            .index_max_children(1000)
            .build()
            .unwrap();
        let candidates = SplitCandidates::with_policy(policy);
        candidates.observe_leaf(
            &root_path(),
            &LeafBody::from_entries([live(b"a"), live(b"b"), live(b"c"), live(b"d")]),
        );

        let sp = splitter_with_candidates(&s, &bg, candidates);
        sp.run_once().await;

        // The only cap crossed is the byte cap, so a split here proves the byte
        // cap now has a producer.
        let leaves = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN)
            .leaves(&collection(), Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert_eq!(leaves.len(), 2, "byte-cap overflow triggered a split");
    }

    // ADR-031 cascade healing: splitting a sibling whose own separator was never
    // published still lands every separator. The parent knows P0 -> M, while
    // the leaf chain already extends M -> S without S's separator. When S
    // splits, publication starts at the last published edge and lands both the
    // missing `S` separator and the new one.
    #[tokio::test]
    async fn splitting_an_unpublished_sibling_reconciles_the_chain() {
        let s = store();
        s.store_node(
            COLL,
            "P0",
            &leaf_node(&[b"a", b"b"], Some(b"g"), Some("M")),
            None,
        )
        .await
        .unwrap();
        s.store_node(
            COLL,
            "M",
            &leaf_node(&[b"g", b"h"], Some(b"m"), Some("S")),
            None,
        )
        .await
        .unwrap();
        s.store_node(COLL, "S", &leaf_node(&[b"m", b"n", b"o"], None, None), None)
            .await
            .unwrap();
        let root = Node::index(IndexNode::from_children([
            (Vec::new(), "P0".to_string()),
            (b"g".to_vec(), "M".to_string()),
        ]));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());

        // Tiny leaf cap so S splits, but a wide fan-out so the parent index does
        // not itself overflow — keeping the assertion on its separators direct.
        let policy = SplitPolicy::builder()
            .leaf_max_entries(2)
            .node_soft_max_bytes(1 << 20)
            .index_max_children(100)
            .build()
            .unwrap();
        splitter(&s, &bg, policy)
            .split_path(&node_path("S"))
            .await
            .unwrap();

        // The existing `g -> M` edge is retained, and the parent learns both
        // the previously missing `m -> S` edge and S's new `n` edge.
        let (root_node, _) = s
            .load_root_node(COLL, Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap()
            .unwrap();
        let seps: Vec<Vec<u8>> = root_node
            .as_index()
            .unwrap()
            .children()
            .map(|(sep, _)| sep.to_vec())
            .collect();
        assert_eq!(
            seps,
            vec![b"".to_vec(), b"g".to_vec(), b"m".to_vec(), b"n".to_vec()],
            "the whole chain's separators are published"
        );

        // Every key is still reachable in order.
        let router = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN);
        for k in [b"a".as_slice(), b"b", b"g", b"h", b"m", b"n", b"o"] {
            let loc = router
                .route_key(&collection(), k, Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap();
            assert!(
                loc.node().unwrap().as_leaf().unwrap().exists(k),
                "key {k:?} lost"
            );
        }
    }

    // ADR-032 retry path: a separator whose parent CAS keeps losing leaves its
    // structural intent in progress and is re-queued for a later sweep. A backend that blocks writes to
    // the root `_r` forces the publication to give up; healing it lets the
    // re-driven publication land.
    #[tokio::test]
    async fn lost_parent_cas_is_republished_by_a_later_sweep() {
        let (backend, blocker) = RootWriteBlocker::wrap(Arc::new(MemoryBackend::new()));
        let s = store_with_backend(backend.clone() as Arc<dyn Backend>);

        // A root index over a single leaf L[a,b,c] (over the cap).
        s.store_node(COLL, "L", &leaf_node(&[b"a", b"b", b"c"], None, None), None)
            .await
            .unwrap();
        let root = Node::index(IndexNode::from_children([(Vec::new(), "L".to_string())]));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());
        let sp = splitter(&s, &bg, tiny());

        // Block the parent `_r` CAS: the split lands (L shrinks, a sibling is
        // created) but the separator publication cannot, so it is re-queued.
        blocker.block(true);
        assert!(matches!(
            sp.split_path(&node_path("L")).await,
            Err(TransError::Retry)
        ));
        let (blocked_root, _) = s
            .load_root_node(COLL, Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            blocked_root.as_index().unwrap().len(),
            1,
            "separator is not published while the parent CAS is blocked"
        );
        assert!(
            blocked_root.structural_gate().holders().is_empty(),
            "a publication that gives up releases the parent gate"
        );
        let (blocked_coordination, _) = s
            .records
            .load_record(&collection(), Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert_eq!(
            blocked_coordination.topology_participants().count(),
            1,
            "the participant stays registered while structural recovery is pending"
        );
        assert_eq!(
            TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN)
                .leaves(&collection(), Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .len(),
            2,
            "the leaves still split; only the parent separator is missing"
        );

        // Heal and sweep: the re-queued separator is published.
        blocker.block(false);
        sp.run_once().await;
        let (healed_root, _) = s
            .load_root_node(COLL, Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            healed_root.as_index().unwrap().len(),
            2,
            "the deferred separator is republished by a later sweep"
        );
        assert!(sp.recover_structural_intents().await);
        assert!(
            s.list_structural_intents("db", Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .is_empty()
        );
        let (recovered_coordination, _) = s
            .records
            .load_record(&collection(), Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert_eq!(recovered_coordination.topology_participants().count(), 0);
    }

    /// Controls a hook that rejects conditional writes to the collection root.
    struct RootWriteBlocker {
        blocked: std::sync::atomic::AtomicBool,
    }

    impl RootWriteBlocker {
        fn wrap(inner: Arc<dyn Backend>) -> (Arc<HookBackend>, Arc<Self>) {
            let blocker = Arc::new(Self {
                blocked: std::sync::atomic::AtomicBool::new(false),
            });
            let backend = HookBackend::new(inner);
            backend.set_before({
                let blocker = blocker.clone();
                move |op| {
                    let blocked = blocker.blocked.load(std::sync::atomic::Ordering::SeqCst)
                        && match op {
                            BackendOp::WriteIf { path, value, .. }
                            | BackendOp::WriteIfNotExists { path, value }
                                if path.ends_with("/_r") =>
                            {
                                Node::decode(value)
                                    .ok()
                                    .and_then(|root| root.as_index().map(|index| index.len() > 1))
                                    .unwrap_or(false)
                            }
                            _ => false,
                        };
                    let result = if blocked {
                        Err(glassdb_backend::BackendError::Precondition)
                    } else {
                        Ok(())
                    };
                    let future: HookFuture = Box::pin(async move { result });
                    future
                }
            });
            (backend, blocker)
        }

        fn block(&self, on: bool) {
            self.blocked.store(on, std::sync::atomic::Ordering::SeqCst);
        }
    }

    // A stale parent over a three-leaf chain: the root names only L0, while the
    // right-links have moved keys at and above "m" to L1 and "t" to L4.
    async fn seed_unpublished_leaf_chain(s: &TestStore, children: &[(&[u8], &str)]) -> Node {
        s.store_node(
            COLL,
            "L0",
            &leaf_node(&[b"apple"], Some(b"m"), Some("L1")),
            None,
        )
        .await
        .unwrap();
        s.store_node(
            COLL,
            "L1",
            &leaf_node(&[b"mango"], Some(b"t"), Some("L4")),
            None,
        )
        .await
        .unwrap();
        s.store_node(COLL, "L4", &leaf_node(&[b"zebra"], None, None), None)
            .await
            .unwrap();
        let root =
            Node::index(IndexNode::from_children(children.iter().map(
                |(separator, child)| (separator.to_vec(), test_token(child).to_string()),
            )));
        s.create_root(COLL, &root).await.unwrap();
        root
    }

    async fn publisher(s: &TestStore, bg: &Arc<Background>) -> SeparatorPublisher {
        splitter(s, bg, SplitPolicy::default()).publisher
    }

    #[tokio::test]
    async fn missing_separators_reports_every_unindexed_edge_in_chain_order() {
        let s = store();
        let bg = Arc::new(Background::new());
        let parent = seed_unpublished_leaf_chain(&s, &[(b"", "L0")]).await;

        assert_eq!(
            publisher(&s, &bg)
                .await
                .missing_separators(&collection(), &parent, b"t", Requirement::Any)
                .await
                .unwrap(),
            [
                MissingSeparator {
                    separator: b"m".to_vec(),
                    child: test_token("L1"),
                },
                MissingSeparator {
                    separator: b"t".to_vec(),
                    child: test_token("L4"),
                },
            ]
        );
    }

    #[tokio::test]
    async fn missing_separators_stops_at_the_split_key() {
        let s = store();
        let bg = Arc::new(Background::new());
        let parent = seed_unpublished_leaf_chain(&s, &[(b"", "L0")]).await;

        assert_eq!(
            publisher(&s, &bg)
                .await
                .missing_separators(&collection(), &parent, b"m", Requirement::Any)
                .await
                .unwrap(),
            [MissingSeparator {
                separator: b"m".to_vec(),
                child: test_token("L1"),
            }],
            "an edge past the split key belongs to a later publication"
        );
    }

    #[tokio::test]
    async fn missing_separators_is_empty_when_the_parent_names_every_edge() {
        let s = store();
        let bg = Arc::new(Background::new());
        let parent =
            seed_unpublished_leaf_chain(&s, &[(b"", "L0"), (b"m", "L1"), (b"t", "L4")]).await;
        let publisher = publisher(&s, &bg).await;

        for split_key in [b"m".as_slice(), b"t".as_slice()] {
            assert!(
                publisher
                    .missing_separators(&collection(), &parent, split_key, Requirement::Any)
                    .await
                    .unwrap()
                    .is_empty(),
                "a current index has nothing to publish for {split_key:?}"
            );
        }
    }

    #[tokio::test]
    async fn separator_reconciliation_fails_on_a_right_link_cycle() {
        let s = store();
        let bg = Arc::new(Background::new());
        // L0 and L1 point at each other and share a high-key, so neither ever
        // reaches the split key and the walk would never terminate.
        s.store_node(
            COLL,
            "L0",
            &leaf_node(&[b"apple"], Some(b"m"), Some("L1")),
            None,
        )
        .await
        .unwrap();
        s.store_node(
            COLL,
            "L1",
            &leaf_node(&[b"cat"], Some(b"m"), Some("L0")),
            None,
        )
        .await
        .unwrap();
        let parent = Node::index(IndexNode::from_children([(
            Vec::new(),
            test_token("L0").to_string(),
        )]));
        s.create_root(COLL, &parent).await.unwrap();

        let error = publisher(&s, &bg)
            .await
            .missing_separators(&collection(), &parent, b"t", Requirement::Any)
            .await
            .expect_err("a cycle must not reconcile");
        assert!(
            error.to_string().contains("right-link hop bound"),
            "unexpected reconciliation error: {error}"
        );
    }
}
