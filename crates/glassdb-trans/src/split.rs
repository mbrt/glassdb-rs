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
//! 0. Advance the structural intent with the source version and split key; its
//!    created-node tokens were reserved while `Preparing`.
//! 1. Create the right sibling (`write_if_not_exists`) holding the upper half
//!    and inheriting the source's former high-key and right-sibling.
//! 2. **Shrink the source in one CAS** — drop the upper half, set high-key to the
//!    split key, link to the sibling. This is the linearization point: descent
//!    now finds the moved keys by stepping right, and a concurrent locker that
//!    loaded the pre-shrink version loses its CAS and re-routes (ADR-031
//!    ownership re-check).
//! 3. Insert the separator into the parent so future descents skip the
//!    right-link hop; recurse when the parent itself overflows. Purely an
//!    optimization — correctness never depends on it landing.
//!
//! A leaf split acquires structure-write through the shared
//! [`ShardCoordinator`], in the same folded CAS stream as data mutations on
//! that leaf. Interior indexes and roots still use direct structural CASes.
//! The source shrink (or root rewrite) releases structure-write inline, so no
//! unlocked post-split state is exposed before a separate release CAS.
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
use glassdb_data::{CollectionAddress, DbRoot, NodeToken, ObjectPath, StructuralRecordId, TxId};
use glassdb_storage::transaction::{TxCommitStatus, TxLock, TxLog};
use glassdb_storage::{
    CollectionStore, IndexNode, InlinePolicy, LeafObservation, LockType, Node, Observation,
    Requirement, Shard, ShardEntry, ShardStore, SplitPolicy, StorageError, StructuralLog,
    StructuralLogPhase, StructuralLogStore, Timeline, TreeRouter,
};
use tokio::sync::Notify;

use crate::collections::TopologySettler;
use crate::error::TransError;
use crate::key_state_resolver::KeyStateResolver;
use crate::monitor::{Monitor, TxRecoveryManifest};
use crate::node_locking::{NodeLockReconciler, QuiescedEntries, StructuralGateResolver};
use crate::shard_coord::{FoldOutcome, ShardCoordinator, SplitHinter};

use recovery::{ParticipantSettlementStep, RecordRecoveryStep, StructuralRecovery};

/// How often the splitter drains its candidate queue. A split is a handful of
/// CAS round-trips, so a tight cadence keeps overflowing leaves short-lived.
const SPLIT_INTERVAL: Duration = Duration::from_secs(1);

/// Back off empty structural-log listings independently of split candidates.
const STRUCTURAL_RECOVERY_IDLE_INTERVAL: Duration = Duration::from_secs(60);

/// Upper bound on the buffered split-candidate queue. Candidates are only hints:
/// the splitter reloads and re-checks each one, so dropping the oldest when full
/// merely delays a split, never causes an unsafe one.
const CANDIDATE_QUEUE_CAP: usize = 4096;

/// Bounded attempts to insert a separator into a contended parent before
/// re-queuing it for a later sweep. Descent works meanwhile through right-links.
const PARENT_RETRIES: usize = 8;

/// Safety bound on the leaf right-link hops walked while reconciling separators,
/// so a malformed or concurrently-mutated chain can never spin the splitter. A
/// well-formed chain up to a split key is far shorter than this.
const MAX_RECONCILE_HOPS: usize = 4096;

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
    shards: ShardStore,
    mon: Monitor,
    key_state: KeyStateResolver,
    coord: ShardCoordinator,
}

impl StructuralNodeAccess {
    fn new(
        shards: ShardStore,
        mon: Monitor,
        key_state: KeyStateResolver,
        coord: ShardCoordinator,
    ) -> Self {
        Self {
            shards,
            mon,
            key_state,
            coord,
        }
    }

    /// Acquires a source node's structure-write lock under wound-wait.
    async fn acquire_structural_gate(
        &self,
        collection: &CollectionAddress,
        token: Option<&NodeToken>,
        id: &TxId,
    ) -> Result<Option<(Node, LeafObservation)>, TransError> {
        if let Some(token) = token {
            let (node, _) = self
                .shards
                .load_node(collection, token, Requirement::Any)
                .await?;
            if node.as_leaf().is_some() {
                return self
                    .acquire_leaf_structural_gate(collection, token, id)
                    .await;
            }
        }
        self.acquire_structural_gate_direct(collection, token, id)
            .await
    }

    async fn acquire_leaf_structural_gate(
        &self,
        collection: &CollectionAddress,
        token: &NodeToken,
        id: &TxId,
    ) -> Result<Option<(Node, LeafObservation)>, TransError> {
        let path = ObjectPath::Node {
            collection: collection.clone(),
            token: token.clone(),
        };
        let outcome = self
            .coord
            .submit_shard(
                &path,
                id,
                Arc::new(StructuralGateResolver::new(id.clone(), path.clone())),
                Requirement::Any,
            )
            .await?;
        if !matches!(
            outcome.as_ref().map(|coordinated| &coordinated.outcome),
            Some(FoldOutcome::Locked {
                typ: LockType::Write,
                ..
            })
        ) {
            return Ok(None);
        }

        let requirement = outcome
            .and_then(|coordinated| coordinated.cas_precondition)
            .map(|observation| Requirement::AtLeast(observation.current_after()))
            .unwrap_or(Requirement::Any);
        let (node, version) = self
            .shards
            .load_node(collection, token, requirement)
            .await?;
        if node.structural_gate().lock_type() == LockType::Write
            && node.structural_gate().contains(id)
        {
            Ok(Some((node, version)))
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
            let (mut node, version) = match token {
                Some(token) => {
                    self.shards
                        .load_node(collection, token, Requirement::Any)
                        .await?
                }
                None => match self.shards.load_root(collection, Requirement::Any).await {
                    Ok((root, version)) => (root, version),
                    Err(StorageError::NotFound) => return Ok(None),
                    Err(error) => return Err(error.into()),
                },
            };
            if node.structural_gate().lock_type() == LockType::Write
                && node.structural_gate().contains(id)
            {
                return Ok(Some((node, version)));
            }

            let entries: BTreeMap<Vec<u8>, _> = node
                .as_leaf()
                .into_iter()
                .flat_map(Shard::entries)
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
                node.set_leaf(Shard::from_entries(entries.into_values()))?;
            }
            node.set_locks(locks);
            if self
                .store_structural_node(collection, token, &node, &version)
                .await?
            {
                let (_, locked_version) = match token {
                    Some(token) => {
                        self.shards
                            .load_node(
                                collection,
                                token,
                                Requirement::AtLeast(version.current_after()),
                            )
                            .await?
                    }
                    None => {
                        let (root, version) = self
                            .shards
                            .load_root(collection, Requirement::AtLeast(version.current_after()))
                            .await?;
                        (root, version)
                    }
                };
                return Ok(Some((node, locked_version)));
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
            let (mut node, version) = match token {
                Some(token) => {
                    self.shards
                        .load_node(collection, token, Requirement::Any)
                        .await?
                }
                None => {
                    let (root, version) =
                        self.shards.load_root(collection, Requirement::Any).await?;
                    (root, version)
                }
            };
            if !node.remove_structural_gate(id) {
                return Ok(());
            }
            if self
                .store_structural_node(collection, token, &node, &version)
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
                .shards
                .store_node(collection, token, node, Some(observation))
                .await?),
            None => Ok(self
                .shards
                .store_root(collection, node, observation)
                .await?),
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
    nodes: StructuralNodeAccess,
    router: TreeRouter,
    timeline: Timeline,
    policy: SplitPolicy,
    pending: Arc<Mutex<VecDeque<PendingSeparator>>>,
}

impl SeparatorPublisher {
    fn new(
        nodes: StructuralNodeAccess,
        router: TreeRouter,
        timeline: Timeline,
        policy: SplitPolicy,
    ) -> Self {
        Self {
            nodes,
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
            let lock_id = TxId::new_at(rt::system_now());
            self.nodes.mon.begin_tx(&lock_id);
            let acquired = match self
                .nodes
                .acquire_structural_gate(&separator.collection, parent_token.as_ref(), &lock_id)
                .await
            {
                Ok(acquired) => acquired,
                Err(error) => {
                    self.nodes.finalize_split(&lock_id).await;
                    return Err(error);
                }
            };
            let Some((locked_parent, locked_version)) = acquired else {
                self.nodes.finalize_split(&lock_id).await;
                continue;
            };
            let Some(index) = locked_parent.as_index() else {
                self.nodes
                    .finish_without_split(&separator.collection, parent_token.as_ref(), &lock_id)
                    .await?;
                return Ok(SeparatorPublicationOutcome::Published);
            };
            if index.child_for(&separator.split_key) == Some(separator.new_token.as_str()) {
                self.nodes
                    .finish_without_split(&separator.collection, parent_token.as_ref(), &lock_id)
                    .await?;
                return Ok(SeparatorPublicationOutcome::Published);
            }
            let missing = match self
                .missing_separators(
                    &separator.collection,
                    &locked_parent,
                    &separator.split_key,
                    Requirement::AtLeast(locked_version.current_after()),
                )
                .await
            {
                Ok(missing) => missing,
                Err(error) => {
                    let _ = self
                        .nodes
                        .finish_without_split(
                            &separator.collection,
                            parent_token.as_ref(),
                            &lock_id,
                        )
                        .await;
                    return Err(error);
                }
            };
            if missing.is_empty() {
                self.nodes
                    .finish_without_split(&separator.collection, parent_token.as_ref(), &lock_id)
                    .await?;
                return Ok(SeparatorPublicationOutcome::Published);
            }
            let mut new_index = index.clone();
            for (split_key, token) in &missing {
                new_index.insert_child(split_key.clone(), token.to_string());
            }
            let mut updated = locked_parent.clone();
            updated.set_index(new_index)?;
            let content_limit = self.policy.content_limit();
            if updated.content_encoded_len() > content_limit
                || updated.encoded_len() > self.policy.node_max_bytes
            {
                self.nodes
                    .finish_without_split(&separator.collection, parent_token.as_ref(), &lock_id)
                    .await?;
                if locked_parent.over_soft_cap(&self.policy) {
                    return Ok(SeparatorPublicationOutcome::ParentRequiresSplit(
                        ParentRequiresSplit {
                            path: parent.path,
                            continuation: ParentSplitContinuation::ResumePublication,
                        },
                    ));
                }
                return Err(TransError::InvalidInput(
                    "separator exceeds the coordination node size limit".into(),
                ));
            }

            updated.remove_structural_gate(&lock_id);
            let stored = match self
                .nodes
                .store_structural_node(
                    &separator.collection,
                    parent_token.as_ref(),
                    &updated,
                    &locked_version,
                )
                .await
            {
                Ok(stored) => stored,
                Err(error) => {
                    let _ = self
                        .nodes
                        .finish_without_split(
                            &separator.collection,
                            parent_token.as_ref(),
                            &lock_id,
                        )
                        .await;
                    return Err(error);
                }
            };
            if stored {
                self.nodes
                    .finish_without_split(&separator.collection, parent_token.as_ref(), &lock_id)
                    .await?;
                if updated.over_soft_cap(&self.policy) {
                    return Ok(SeparatorPublicationOutcome::ParentRequiresSplit(
                        ParentRequiresSplit {
                            path: parent.path,
                            continuation: ParentSplitContinuation::CompletePublication,
                        },
                    ));
                }
                return Ok(SeparatorPublicationOutcome::Published);
            }
            let _ = self
                .nodes
                .release_structural_gate(&separator.collection, parent_token.as_ref(), &lock_id)
                .await;
            self.nodes.finalize_split(&lock_id).await;
        }

        self.defer(publication.separator.clone());
        Err(TransError::Retry)
    }

    /// Returns the unpublished right-link edges through `split_key` in chain order.
    async fn missing_separators(
        &self,
        collection: &CollectionAddress,
        parent: &Node,
        split_key: &[u8],
        requirement: Requirement,
    ) -> Result<Vec<(Vec<u8>, NodeToken)>, TransError> {
        let Some(index) = parent.as_index() else {
            return Ok(Vec::new());
        };
        let Some(start) = index.child_for(split_key) else {
            return Ok(Vec::new());
        };
        let start = node_token(start)?;
        let mut missing = Vec::new();
        let (mut cur, _) = self
            .nodes
            .shards
            .load_node(collection, &start, requirement)
            .await?;
        for _ in 0..MAX_RECONCILE_HOPS {
            let (Some(right), Some(boundary)) = (cur.right_sibling(), cur.high_key()) else {
                break;
            };
            if boundary > split_key {
                break;
            }
            let right = right.to_string();
            let right_token = node_token(&right)?;
            let boundary = boundary.to_vec();
            if index.child_for(&boundary) != Some(right.as_str()) {
                missing.push((boundary.clone(), right_token.clone()));
            }
            let reached_target = boundary.as_slice() == split_key;
            let (next, _) = self
                .nodes
                .shards
                .load_node(collection, &right_token, requirement)
                .await?;
            cur = next;
            if reached_target {
                break;
            }
        }
        Ok(missing)
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

/// Lightweight producer handle for split hints decided outside the shard
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

/// An exact structural intent that is still safe to cancel.
struct PreparedSplit {
    observed: Observation<StructuralLog>,
    record: StructuralLog,
}

impl PreparedSplit {
    fn from_observation(observed: Observation<StructuralLog>) -> Result<Self, TransError> {
        let record = observed
            .value()
            .filter(|record| {
                record.phase == StructuralLogPhase::Preparing
                    && if record.is_root() {
                        record.created_tokens.len() == 2
                    } else {
                        record.source_token.is_some() && record.created_tokens.len() == 1
                    }
            })
            .ok_or_else(|| TransError::other("invalid prepared split intent"))?
            .as_ref()
            .clone();
        Ok(Self { observed, record })
    }

    /// Marks the point after which a lost acknowledgement requires recovery.
    fn into_ready(self, source_version: String, split_key: Vec<u8>) -> ReadySplit {
        let mut record = self.record;
        record.source_version = source_version;
        record.split_key = split_key;
        record.phase = StructuralLogPhase::Ready;
        ReadySplit {
            state: Box::new(ReadySplitState {
                expected: self.observed,
                record,
                observed: None,
            }),
        }
    }
}

/// A witness that the durable Ready transition may have landed.
///
/// `observed` is populated only when the transition was acknowledged. An
/// unacknowledged witness is retained solely to prevent unsafe cleanup; node
/// creation is never attempted from it.
struct ReadySplit {
    state: Box<ReadySplitState>,
}

struct ReadySplitState {
    expected: Observation<StructuralLog>,
    record: StructuralLog,
    observed: Option<Observation<StructuralLog>>,
}

impl ReadySplit {
    fn expected(&self) -> &Observation<StructuralLog> {
        &self.state.expected
    }

    fn record(&self) -> &StructuralLog {
        &self.state.record
    }

    fn confirm(&mut self, observed: Observation<StructuralLog>) -> Result<(), TransError> {
        let matches = observed
            .value()
            .is_some_and(|record| record.as_ref() == &self.state.record);
        if !matches {
            return Err(TransError::other(
                "Ready transition returned an unexpected structural intent",
            ));
        }
        self.state.observed = Some(observed);
        Ok(())
    }

    fn observation(&self) -> &Observation<StructuralLog> {
        self.state
            .observed
            .as_ref()
            .expect("only an acknowledged Ready split may create nodes")
    }
}

/// Whether a coordinated split finished, can discard Preparing, or needs recovery.
enum SplitAttemptResult {
    Completed,
    RetryCleanly,
    RecoveryRequired(ReadySplit),
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

    fn recovery_required(ready: ReadySplit, error: TransError) -> Self {
        Self {
            result: Err(error),
            state: SplitAttemptResult::RecoveryRequired(ready),
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
            .prepare_structural_intent(self.collection, self.target.source_token(), participant)
            .await?;
        let observed = prepared.observed.clone();
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
        self.finish(outcome, &observed, topology).await
    }

    async fn coordinate(&self, prepared: PreparedSplit) -> SplitAttemptOutcome {
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
        prepared: &Observation<StructuralLog>,
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
                let cleanup = match self
                    .splitter
                    .structural_logs
                    .delete_structural_log(prepared)
                    .await
                {
                    Ok(()) => match topology {
                        StructuralSplitTopology::Owned => {
                            self.splitter
                                .leave_topology(self.collection, &self.worker)
                                .await
                        }
                        StructuralSplitTopology::Joined(_) => Ok(()),
                    },
                    Err(error) => Err(error.into()),
                };
                result.and(cleanup)
            }
            SplitAttemptResult::RecoveryRequired(ready) => {
                debug_assert_eq!(ready.record().phase, StructuralLogPhase::Ready);
                result
            }
        }
    }
}

#[derive(Default)]
struct Stats {
    candidates: AtomicU64,
    completed: AtomicU64,
    deferred: AtomicU64,
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
    fn observe_leaf(&self, path: &ObjectPath, entries: &Shard) {
        let over_cap = entries.len() >= 2
            && (entries.len() > self.policy.leaf_max_entries
                || entries.encoded_len() > self.policy.leaf_max_bytes);
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
/// through the node and structural-log stores, recovered idempotently like any
/// in-doubt CAS.
#[derive(Clone)]
pub struct Splitter {
    // Weak so a clone captured in the spawned loop does not keep the executor
    // alive across shutdown; the single strong owner is `DbInner::background`.
    bg: Weak<Background>,
    records: CollectionStore,
    shards: ShardStore,
    structural_logs: StructuralLogStore,
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
    stats: Arc<Stats>,
}

impl Splitter {
    /// Builds a splitter and coordinator that share one timeline and
    /// split-candidate feed.
    #[allow(clippy::too_many_arguments)]
    pub fn with_coordinator(
        bg: Weak<Background>,
        records: CollectionStore,
        shards: ShardStore,
        structural_logs: StructuralLogStore,
        timeline: Timeline,
        mon: Monitor,
        key_state: KeyStateResolver,
        retry: RetryConfig,
        db_root: DbRoot,
        policy: SplitPolicy,
        inline: InlinePolicy,
    ) -> (ShardCoordinator, Self) {
        let candidates = SplitCandidates::with_policies(policy, inline);
        let coord = ShardCoordinator::with_hinter(
            shards.clone(),
            key_state.clone(),
            mon.clone(),
            retry,
            policy,
            Arc::new(candidates.clone()),
        );
        let splitter = Splitter::with_candidates(
            bg,
            records,
            shards,
            structural_logs,
            timeline,
            mon,
            key_state,
            db_root,
            coord.clone(),
            candidates,
            retry,
        );
        (coord, splitter)
    }

    /// Returns a producer handle for split hints decided outside the shard
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
        shards: ShardStore,
        structural_logs: StructuralLogStore,
        timeline: Timeline,
        mon: Monitor,
        key_state: KeyStateResolver,
        db_root: DbRoot,
        coord: ShardCoordinator,
        candidates: SplitCandidates,
        retry: RetryConfig,
    ) -> Self {
        let router = TreeRouter::new(shards.nodes().clone());
        let structural_nodes =
            StructuralNodeAccess::new(shards.clone(), mon.clone(), key_state, coord);
        let publisher = SeparatorPublisher::new(
            structural_nodes.clone(),
            router.clone(),
            timeline.clone(),
            *candidates.policy(),
        );
        let recovery = StructuralRecovery::new(
            records.clone(),
            shards.clone(),
            structural_logs.clone(),
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
            shards,
            structural_logs,
            router,
            mon,
            structural_nodes,
            timeline,
            candidates,
            publisher,
            recovery,
            recovery_wake: Arc::new(Notify::new()),
            retry,
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
                let active = recovery.recover_structural_logs().await;
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
            .leaf_for(collection, key, Requirement::AtLeast(self.timeline.now()))
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
                if !node.owns(key) {
                    return SplitNeed::Reroute;
                }
                if !self.candidates.inline.admits_value(*value_len)
                    || leaf.len() < 2
                    || !leaf.lookup(key).is_some_and(ShardEntry::exists)
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

    /// Persists the participant-owned intent that makes a future root join
    /// recoverable before any source gate or node creation can happen.
    async fn prepare_structural_intent(
        &self,
        collection: &CollectionAddress,
        source_token: Option<&NodeToken>,
        participant: &TxId,
    ) -> Result<PreparedSplit, TransError> {
        let is_root = source_token.is_none();
        let created_tokens = if is_root {
            vec![NodeToken::new_random(), NodeToken::new_random()]
        } else {
            vec![NodeToken::new_random()]
        };
        let record_id = StructuralRecordId::from(
            created_tokens
                .last()
                .expect("a split always reserves at least one token"),
        );
        let observed = self
            .structural_logs
            .write_structural_log(
                collection.db_root_component(),
                &record_id,
                &StructuralLog {
                    collection: collection.clone(),
                    source_token: source_token.cloned(),
                    source_version: String::new(),
                    created_tokens,
                    split_key: Vec::new(),
                    participant_id: participant.clone(),
                    phase: StructuralLogPhase::Preparing,
                },
            )
            .await?;
        PreparedSplit::from_observation(observed)
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

    /// Acquires a source node's structure-write lock under wound-wait. A leaf
    /// joins the shared coordinator round; roots and interior indexes use the
    /// direct structural CAS path because they carry no data-mutation traffic.
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

    /// Stores a complete root or non-root node at an expected version.
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

    /// Performs the write-ahead, sibling creation, shrink, and publication.
    async fn coordinate_nonroot_split(
        &self,
        collection: &CollectionAddress,
        token: &NodeToken,
        worker: &TxId,
        reason: &SplitReason,
        prepared: PreparedSplit,
    ) -> SplitAttemptOutcome {
        debug_assert!(!prepared.record.is_root());
        debug_assert_eq!(&prepared.record.collection, collection);
        debug_assert_eq!(prepared.record.source_token.as_ref(), Some(token));
        let right_token = prepared.record.created_tokens[0].clone();
        let (mut node, version) = match self
            .acquire_structural_gate(collection, Some(token), worker)
            .await
        {
            Ok(Some(acquired)) => acquired,
            Ok(None) => return SplitAttemptOutcome::retry_cleanly(Err(TransError::Retry)),
            Err(error) => return SplitAttemptOutcome::retry_cleanly(Err(error)),
        };
        match self.split_need(&node, reason) {
            SplitNeed::Split => {}
            SplitNeed::NotActionable => {
                if reason.is_inline_pressure() {
                    self.stats
                        .inline_pressure_discarded
                        .fetch_add(1, Ordering::Relaxed);
                }
                let result = self
                    .release_structural_gate(collection, Some(token), worker)
                    .await;
                return SplitAttemptOutcome::retry_cleanly(result);
            }
            SplitNeed::Reroute => {
                let result = match self
                    .release_structural_gate(collection, Some(token), worker)
                    .await
                {
                    Ok(()) => Err(TransError::Retry),
                    Err(error) => Err(error),
                };
                return SplitAttemptOutcome::retry_cleanly(result);
            }
        }

        let Some((right, split_key)) = node.split(right_token.as_str()) else {
            let result = self
                .release_structural_gate(collection, Some(token), worker)
                .await;
            return SplitAttemptOutcome::retry_cleanly(result);
        };
        node.remove_structural_gate(worker);

        let source_version = match version.revision() {
            Some(revision) => revision.serialize().to_string(),
            None => {
                return SplitAttemptOutcome::retry_cleanly(Err(TransError::other(
                    "split source is absent",
                )));
            }
        };
        let mut ready = prepared.into_ready(source_version, split_key.clone());
        let transition = self
            .structural_logs
            .update_structural_log(ready.expected(), ready.record())
            .await;
        let observed = match transition {
            Ok(Some(observed)) => observed,
            Ok(None) => {
                let result = match self
                    .release_structural_gate(collection, Some(token), worker)
                    .await
                {
                    Ok(()) => Err(TransError::Retry),
                    Err(error) => Err(error),
                };
                return SplitAttemptOutcome::retry_cleanly(result);
            }
            Err(error) => {
                return SplitAttemptOutcome::recovery_required(ready, error.into());
            }
        };
        if let Err(error) = ready.confirm(observed) {
            return SplitAttemptOutcome::recovery_required(ready, error);
        }

        match self
            .shards
            .store_node(collection, &right_token, &right, None)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                return SplitAttemptOutcome::recovery_required(ready, TransError::Retry);
            }
            Err(error) => {
                return SplitAttemptOutcome::recovery_required(ready, error.into());
            }
        }
        match self
            .shards
            .store_node(collection, token, &node, Some(&version))
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                return SplitAttemptOutcome::recovery_required(ready, TransError::Retry);
            }
            Err(error) => {
                return SplitAttemptOutcome::recovery_required(ready, error.into());
            }
        }
        self.stats.completed.fetch_add(1, Ordering::Relaxed);
        self.enqueue_if_over_soft_cap(collection, token, &node);
        self.enqueue_if_over_soft_cap(collection, &right_token, &right);
        if reason.is_inline_pressure() {
            self.stats
                .inline_pressure_completed
                .fetch_add(1, Ordering::Relaxed);
        }
        if let Err(error) = self
            .publish_separators(
                collection,
                &split_key,
                &right_token,
                Some(&ready.record().participant_id),
            )
            .await
        {
            return SplitAttemptOutcome::recovery_required(ready, error);
        }
        if let Err(error) = self
            .structural_logs
            .delete_structural_log(ready.observation())
            .await
        {
            return SplitAttemptOutcome::recovery_required(ready, error.into());
        }
        SplitAttemptOutcome::completed()
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
        prepared: PreparedSplit,
    ) -> SplitAttemptOutcome {
        debug_assert!(prepared.record.is_root());
        debug_assert_eq!(&prepared.record.collection, collection);
        let (node, version) = match self.acquire_structural_gate(collection, None, worker).await {
            Ok(Some(acquired)) => acquired,
            Ok(None) => return SplitAttemptOutcome::retry_cleanly(Err(TransError::Retry)),
            Err(error) => return SplitAttemptOutcome::retry_cleanly(Err(error)),
        };
        match self.split_need(&node, reason) {
            SplitNeed::Split => {}
            SplitNeed::NotActionable => {
                if reason.is_inline_pressure() {
                    self.stats
                        .inline_pressure_discarded
                        .fetch_add(1, Ordering::Relaxed);
                }
                let result = self.release_structural_gate(collection, None, worker).await;
                return SplitAttemptOutcome::retry_cleanly(result);
            }
            SplitNeed::Reroute => {
                let result = match self.release_structural_gate(collection, None, worker).await {
                    Ok(()) => Err(TransError::Retry),
                    Err(error) => Err(error),
                };
                return SplitAttemptOutcome::retry_cleanly(result);
            }
        }

        let l_token = prepared.record.created_tokens[0].clone();
        let r_token = prepared.record.created_tokens[1].clone();
        let (left, right, split_key) = split_into_children(&node, r_token.as_str(), worker);
        let root_index = IndexNode::from_children([
            (Vec::new(), l_token.to_string()),
            (split_key.clone(), r_token.to_string()),
        ]);
        let index = Node::index(root_index);
        let sized_root = index.clone();
        let content_limit = self.candidates.policy().content_limit();
        if sized_root.content_encoded_len() > content_limit
            || sized_root.encoded_len() > self.candidates.policy().node_max_bytes
        {
            let result = match self.release_structural_gate(collection, None, worker).await {
                Ok(()) => Err(TransError::InvalidInput(
                    "root index exceeds the coordination node size limit".into(),
                )),
                Err(error) => Err(error),
            };
            return SplitAttemptOutcome::retry_cleanly(result);
        }

        let source_version = match version.revision() {
            Some(revision) => revision.serialize().to_string(),
            None => {
                return SplitAttemptOutcome::retry_cleanly(Err(TransError::other(
                    "split source is absent",
                )));
            }
        };
        let mut ready = prepared.into_ready(source_version, split_key);
        let transition = self
            .structural_logs
            .update_structural_log(ready.expected(), ready.record())
            .await;
        let observed = match transition {
            Ok(Some(observed)) => observed,
            Ok(None) => {
                let result = match self.release_structural_gate(collection, None, worker).await {
                    Ok(()) => Err(TransError::Retry),
                    Err(error) => Err(error),
                };
                return SplitAttemptOutcome::retry_cleanly(result);
            }
            Err(error) => {
                return SplitAttemptOutcome::recovery_required(ready, error.into());
            }
        };
        if let Err(error) = ready.confirm(observed) {
            return SplitAttemptOutcome::recovery_required(ready, error);
        }

        match self
            .shards
            .store_node(collection, &l_token, &left, None)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                return SplitAttemptOutcome::recovery_required(ready, TransError::Retry);
            }
            Err(error) => {
                return SplitAttemptOutcome::recovery_required(ready, error.into());
            }
        }
        match self
            .shards
            .store_node(collection, &r_token, &right, None)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                return SplitAttemptOutcome::recovery_required(ready, TransError::Retry);
            }
            Err(error) => {
                return SplitAttemptOutcome::recovery_required(ready, error.into());
            }
        }
        match self
            .store_structural_node(collection, None, &index, &version)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                return SplitAttemptOutcome::recovery_required(ready, TransError::Retry);
            }
            Err(error) => {
                return SplitAttemptOutcome::recovery_required(ready, error);
            }
        }
        self.stats.completed.fetch_add(1, Ordering::Relaxed);
        self.enqueue_if_over_soft_cap(collection, &l_token, &left);
        self.enqueue_if_over_soft_cap(collection, &r_token, &right);
        if reason.is_inline_pressure() {
            self.stats
                .inline_pressure_completed
                .fetch_add(1, Ordering::Relaxed);
        }
        if let Err(error) = self
            .structural_logs
            .delete_structural_log(ready.observation())
            .await
        {
            return SplitAttemptOutcome::recovery_required(ready, error.into());
        }
        SplitAttemptOutcome::completed()
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

    /// Recovers every unresolved structural record in this database.
    async fn recover_structural_logs(&self) -> bool {
        let sweep = match self.recovery.scan().await {
            Ok(sweep) => sweep,
            Err(e) => {
                tracing::debug!(
                    target: "glassdb::splitter",
                    error = %e,
                    "listing structural records failed"
                );
                return true;
            }
        };
        let active = sweep.active;
        for (record_id, record) in sweep.records {
            if let Err(e) = self.recover_record(&record).await {
                tracing::debug!(
                    target: "glassdb::splitter",
                    record = %record_id,
                    error = %e,
                    "structural recovery deferred"
                );
            }
        }
        for (collection, participant) in sweep.participants {
            let final_status = match self.recovery.participant_is_final(&participant).await {
                Ok(final_status) => final_status,
                Err(error) => {
                    tracing::debug!(
                        target: "glassdb::splitter",
                        error = %error,
                        participant = %participant,
                        "checking topology participant status failed"
                    );
                    continue;
                }
            };
            if !final_status {
                continue;
            }
            if let Err(error) = self
                .settle_topology_participant(&collection, &participant)
                .await
            {
                tracing::debug!(
                    target: "glassdb::splitter",
                    error = %error,
                    participant = %participant,
                    "settling topology participant failed"
                );
            }
        }
        active
    }

    /// Resolves one structural record from fenced tree reachability.
    async fn recover_record(
        &self,
        observed: &Observation<StructuralLog>,
    ) -> Result<(), TransError> {
        let mut recovery = self.recovery.begin_record(observed.clone());
        loop {
            match self.recovery.advance_record(&mut recovery).await? {
                RecordRecoveryStep::Completed => return Ok(()),
                RecordRecoveryStep::SplitParent { path, participant } => {
                    Box::pin(self.split_path_joined(&path, &participant)).await?;
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
        let mut settlement = self.recovery.begin_participant_settlement(collection, id);
        loop {
            match self
                .recovery
                .advance_participant_settlement(&mut settlement)
                .await?
            {
                ParticipantSettlementStep::Completed => return Ok(()),
                ParticipantSettlementStep::Recover(observed) => {
                    self.recover_record(&observed).await?;
                }
            }
        }
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

    use crate::monitor::TxFinalStatus;
    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{BackendOp, HookBackend, HookFuture, RecordingBackend};
    use glassdb_data::{KeyRef, TxId};
    use glassdb_storage::transaction::{TLogger, TxWrite};
    use glassdb_storage::{
        CachedStore, CollectionRecord, CollectionStore, CurrentState, LockType, ShardEntry,
    };

    const COLL: &str = "db/_c/0000000000000000000000";

    struct NoSplitHints;

    impl SplitHinter for NoSplitHints {
        fn observe_leaf(&self, _path: &ObjectPath, _shard: &Shard) {}
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
            (Some(shard), None) => Node::leaf(shard.clone()),
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

    fn canonical_record(record: &StructuralLog) -> StructuralLog {
        record.clone()
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
        SplitPolicy {
            leaf_max_entries: 2,
            leaf_max_bytes: 1 << 20,
            index_max_children: 2,
            ..SplitPolicy::default()
        }
    }

    #[derive(Clone)]
    struct TestStore {
        records: CollectionStore,
        shards: ShardStore,
        structural_logs: StructuralLogStore,
        objects: CachedStore,
        timeline: Timeline,
    }

    impl std::ops::Deref for TestStore {
        type Target = ShardStore;

        fn deref(&self) -> &Self::Target {
            &self.shards
        }
    }

    impl TestStore {
        async fn create_root(&self, prefix: &str, node: &Node) -> Result<bool, StorageError> {
            let collection = collection_at(prefix);
            self.records
                .create_record(&collection, &CollectionRecord::new())
                .await?;
            self.shards
                .create_root(&collection, &canonical_node(node))
                .await
        }

        async fn load_root_node(
            &self,
            prefix: &str,
            requirement: Requirement,
        ) -> Result<Option<(Node, LeafObservation)>, StorageError> {
            self.shards
                .load_root_node(&collection_at(prefix), requirement)
                .await
        }

        async fn load_root(
            &self,
            prefix: &str,
            requirement: Requirement,
        ) -> Result<(Node, LeafObservation), StorageError> {
            self.shards
                .load_root(&collection_at(prefix), requirement)
                .await
        }

        async fn store_root(
            &self,
            prefix: &str,
            node: &Node,
            expected: &LeafObservation,
        ) -> Result<bool, StorageError> {
            self.shards
                .store_root(&collection_at(prefix), &canonical_node(node), expected)
                .await
        }

        async fn load_node(
            &self,
            prefix: &str,
            token: &str,
            requirement: Requirement,
        ) -> Result<(Node, LeafObservation), StorageError> {
            self.shards
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
            self.shards
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
            self.shards
                .list_nodes(&collection_at(prefix), requirement)
                .await
        }

        async fn write_structural_log(
            &self,
            record_id: &str,
            record: &StructuralLog,
        ) -> Result<Observation<StructuralLog>, StorageError> {
            self.structural_logs
                .write_structural_log(
                    &db_root("db"),
                    &StructuralRecordId::from(test_token(record_id)),
                    &canonical_record(record),
                )
                .await
        }

        async fn list_structural_logs(
            &self,
            root: &str,
            requirement: Requirement,
        ) -> Result<Vec<(StructuralRecordId, Observation<StructuralLog>)>, StorageError> {
            self.structural_logs
                .list_structural_logs(&db_root(root), requirement)
                .await
        }
    }

    fn store() -> TestStore {
        store_with_backend(Arc::new(MemoryBackend::new()))
    }

    fn store_with_backend(backend: Arc<dyn Backend>) -> TestStore {
        let timeline = Timeline::new();
        let objects = CachedStore::new(backend, 1 << 20, timeline.clone(), None);
        TestStore {
            records: CollectionStore::new(objects.clone()),
            shards: ShardStore::new(objects.clone()),
            structural_logs: StructuralLogStore::new(objects.clone()),
            objects,
            timeline,
        }
    }

    // A committed live key, so it counts as existing under a descent lookup.
    fn live(key: &[u8]) -> ShardEntry {
        ShardEntry::new(key).with_current(CurrentState::External {
            writer: TxId::from_bytes(vec![1]),
        })
    }

    fn inline_live(key: &[u8], value: &[u8]) -> ShardEntry {
        ShardEntry::new(key).with_current(CurrentState::Inline {
            writer: TxId::from_bytes(vec![1]),
            value: Arc::from(value),
        })
    }

    fn pressure_inline() -> InlinePolicy {
        InlinePolicy {
            max_value_bytes: 8,
            max_leaf_bytes: 8,
        }
    }

    fn leaf_node(keys: &[&[u8]], high: Option<&[u8]>, right: Option<&str>) -> Node {
        Node::leaf(Shard::from_entries(keys.iter().map(|k| live(k))))
            .with_high_key(high.map(<[u8]>::to_vec))
            .with_right_sibling(right.map(|token| test_token(token).to_string()))
    }

    fn splitter(shards: &TestStore, bg: &Arc<Background>, policy: SplitPolicy) -> Splitter {
        splitter_with_candidates(shards, bg, SplitCandidates::with_policy(policy))
    }

    fn splitter_with_candidates(
        shards: &TestStore,
        bg: &Arc<Background>,
        candidates: SplitCandidates,
    ) -> Splitter {
        let tl = TLogger::new(shards.objects.clone(), db_root("db"));
        let mon = Monitor::with_config(
            tl.clone(),
            shards.timeline.clone(),
            Arc::downgrade(bg),
            RetryConfig::default(),
            crate::monitor::ProtocolTiming::default(),
        );
        splitter_with_monitor(shards, bg, mon, candidates)
    }

    fn splitter_with_monitor(
        shards: &TestStore,
        bg: &Arc<Background>,
        mon: Monitor,
        candidates: SplitCandidates,
    ) -> Splitter {
        let key_state = KeyStateResolver::new(mon.clone());
        let coord = ShardCoordinator::with_hinter(
            shards.shards.clone(),
            key_state.clone(),
            mon.clone(),
            RetryConfig::default(),
            *candidates.policy(),
            Arc::new(candidates.clone()),
        );
        Splitter::with_candidates(
            Arc::downgrade(bg),
            shards.records.clone(),
            shards.shards.clone(),
            shards.structural_logs.clone(),
            shards.timeline.clone(),
            mon,
            key_state,
            db_root("db"),
            coord,
            candidates,
            RetryConfig::default(),
        )
    }

    fn splitter_and_monitor(
        shards: &TestStore,
        bg: &Arc<Background>,
        policy: SplitPolicy,
    ) -> (Splitter, Monitor) {
        let tl = TLogger::new(shards.objects.clone(), db_root("db"));
        let mon = Monitor::with_config(
            tl.clone(),
            shards.timeline.clone(),
            Arc::downgrade(bg),
            RetryConfig::default(),
            crate::monitor::ProtocolTiming::default(),
        );
        let candidates = SplitCandidates::with_policy(policy);
        let splitter = splitter_with_monitor(shards, bg, mon.clone(), candidates);
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
        Node::leaf(Shard::from_entries(entries))
    }

    fn nonroot_record(source: &str, right: &str, split_key: &[u8]) -> StructuralLog {
        StructuralLog {
            collection: collection(),
            source_token: Some(test_token(source)),
            source_version: String::new(),
            created_tokens: vec![test_token(right)],
            split_key: split_key.to_vec(),
            participant_id: TxId::from_bytes(b"structural-participant".to_vec()),
            phase: StructuralLogPhase::Ready,
        }
    }

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

    // ADR-051: an inline value may be a key's only copy, so a split has to move
    // it to the new leaf verbatim.
    #[tokio::test]
    async fn a_split_carries_inline_values_to_the_new_leaf() {
        let s = store();
        let keys: [&[u8]; 4] = [b"a", b"b", b"c", b"d"];
        let inlined = |key: &[u8]| {
            ShardEntry::new(key).with_current(CurrentState::Inline {
                writer: TxId::from_bytes(vec![1]),
                value: Arc::from(key),
            })
        };
        s.create_root(COLL, &Node::leaf(Shard::from_entries(keys.map(inlined))))
            .await
            .unwrap();
        let bg = Arc::new(Background::new());

        splitter(&s, &bg, tiny())
            .split_path(&root_path())
            .await
            .unwrap();

        let router = TreeRouter::new(s.shards.nodes().clone());
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
                .leaf_for(&collection(), key, Requirement::AtLeast(s.timeline.now()))
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
        let root = Node::leaf(Shard::from_entries(
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

        let router = TreeRouter::new(s.shards.nodes().clone());
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
                .leaf_for(&collection(), k, Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap();
            assert!(
                loc.node().unwrap().as_leaf().unwrap().exists(k),
                "key {k:?} lost"
            );
        }
        assert!(
            s.list_structural_logs("db", Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .is_empty()
        );
        let transaction_prefix = format!("{}/_t/", db_root("db"));
        let request = glassdb_backend::ListRequest::new(
            &transaction_prefix,
            None,
            glassdb_backend::ListLimit::new(2).unwrap(),
        )
        .unwrap();
        assert_eq!(
            s.objects.list_request(request).await.unwrap().objects.len(),
            1,
            "the topology participant remains recoverable until GC"
        );
    }

    #[tokio::test]
    async fn settlement_cancels_a_prepared_split_before_node_creation() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let operations = recorder.log();
        let s = store_with_backend(Arc::new(recorder));
        let root = Node::leaf(Shard::from_entries(
            [b"a".as_slice(), b"b", b"c", b"d"]
                .iter()
                .map(|key| live(key)),
        ));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());
        let sp = splitter(&s, &bg, tiny());
        let participant = TxId::with_priority(1, b"participant");

        sp.begin_topology_tx(&collection(), &participant)
            .await
            .unwrap();
        let intent = sp
            .prepare_structural_intent(&collection(), None, &participant)
            .await
            .unwrap();
        sp.join_topology(&collection(), &participant).await.unwrap();
        sp.mon.abort_owned_tx(&participant).await.unwrap();

        operations.lock().unwrap().clear();
        sp.settle_topology_participant(&collection(), &participant)
            .await
            .unwrap();
        let expected_listing =
            ObjectPath::participant_structural_records_prefix(&db_root("db"), &participant);
        let listings: Vec<_> = operations
            .lock()
            .unwrap()
            .iter()
            .filter(|operation| operation.op == "list")
            .map(|operation| operation.path.clone())
            .collect();
        assert!(!listings.is_empty());
        assert!(
            listings.iter().all(|path| path == &expected_listing),
            "settlement must list only the participant-owned intent prefix"
        );

        let worker = TxId::with_priority(2, b"worker");
        sp.mon.begin_tx(&worker);
        let reason = SplitReason::SoftCap;
        let attempt = sp
            .coordinate_root_split(&collection(), &worker, &reason, intent)
            .await;
        assert!(matches!(attempt.result, Err(TransError::Retry)));
        assert!(matches!(attempt.state, SplitAttemptResult::RetryCleanly));
        sp.finalize_split(&worker).await;
        assert!(
            s.list_nodes(COLL, Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .is_empty(),
            "a cancelled Preparing intent cannot create its reserved nodes"
        );
        let (root, _) = s
            .load_root(COLL, Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert!(root.as_leaf().is_some());
        let (record, _) = s
            .records
            .load_record(&collection(), Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert_eq!(record.topology_participants().count(), 0);
    }

    // Failures before the Ready CAS are cleanly cancellable. Once that CAS may
    // have landed, every later failure must retain the Ready record and its
    // topology participant for structural recovery.
    #[tokio::test]
    async fn structural_split_failure_transition_table() {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        enum FailurePoint {
            StructuralGate,
            ReadyPrecondition,
            ReadyLostAck,
            ChildCreate,
            RootRewrite,
            LogDelete,
        }

        for (point, retains_ready) in [
            (FailurePoint::StructuralGate, false),
            (FailurePoint::ReadyPrecondition, false),
            (FailurePoint::ReadyLostAck, true),
            (FailurePoint::ChildCreate, true),
            (FailurePoint::RootRewrite, true),
            (FailurePoint::LogDelete, true),
        ] {
            let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
            let s = store_with_backend(backend.clone());
            let root = Node::leaf(Shard::from_entries(
                [b"a".as_slice(), b"b", b"c", b"d"]
                    .iter()
                    .map(|key| live(key)),
            ));
            s.create_root(COLL, &root).await.unwrap();
            let bg = Arc::new(Background::new());
            let sp = splitter(&s, &bg, tiny());

            let root_path = root_path().to_string();
            let nodes_prefix = ObjectPath::nodes_prefix(&collection());
            let structural_prefix = ObjectPath::structural_records_prefix(&db_root("db"));
            let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
            backend.set_before({
                let fired = fired.clone();
                let root_path = root_path.clone();
                let nodes_prefix = nodes_prefix.clone();
                let structural_prefix = structural_prefix.clone();
                move |operation| {
                    let should_fail = match operation {
                        BackendOp::WriteIf { path, value, .. } => match point {
                            FailurePoint::StructuralGate => path == &root_path,
                            FailurePoint::ReadyPrecondition => {
                                path.starts_with(&structural_prefix)
                                    && StructuralLog::decode(value).is_ok_and(|record| {
                                        record.phase == StructuralLogPhase::Ready
                                    })
                            }
                            FailurePoint::RootRewrite => {
                                path == &root_path
                                    && Node::decode(value)
                                        .is_ok_and(|node| node.as_index().is_some())
                            }
                            FailurePoint::ReadyLostAck
                            | FailurePoint::ChildCreate
                            | FailurePoint::LogDelete => false,
                        },
                        BackendOp::WriteIfNotExists { path, .. } => {
                            point == FailurePoint::ChildCreate && path.starts_with(&nodes_prefix)
                        }
                        BackendOp::DeleteIf { path, .. } => {
                            point == FailurePoint::LogDelete && path.starts_with(&structural_prefix)
                        }
                        _ => false,
                    };
                    let result =
                        if should_fail && !fired.swap(true, std::sync::atomic::Ordering::SeqCst) {
                            match point {
                                FailurePoint::ReadyPrecondition | FailurePoint::RootRewrite => {
                                    Err(glassdb_backend::BackendError::Precondition)
                                }
                                FailurePoint::StructuralGate
                                | FailurePoint::ChildCreate
                                | FailurePoint::LogDelete => {
                                    Err(glassdb_backend::BackendError::other("injected failure"))
                                }
                                FailurePoint::ReadyLostAck => unreachable!(),
                            }
                        } else {
                            Ok(())
                        };
                    let future: HookFuture = Box::pin(async move { result });
                    future
                }
            });
            backend.set_after({
                let fired = fired.clone();
                let structural_prefix = structural_prefix.clone();
                move |operation, outcome| {
                    let should_fail = point == FailurePoint::ReadyLostAck
                        && outcome.is_success()
                        && matches!(
                            operation,
                            BackendOp::WriteIf { path, value, .. }
                                if path.starts_with(&structural_prefix)
                                    && StructuralLog::decode(value).is_ok_and(|record| {
                                        record.phase == StructuralLogPhase::Ready
                                    })
                        );
                    let result =
                        if should_fail && !fired.swap(true, std::sync::atomic::Ordering::SeqCst) {
                            Err(glassdb_backend::BackendError::Unavailable(
                                "injected lost acknowledgement".into(),
                            ))
                        } else {
                            Ok(())
                        };
                    let future: HookFuture = Box::pin(async move { result });
                    future
                }
            });

            assert!(
                sp.split_path(&ObjectPath::TreeRoot {
                    collection: collection(),
                })
                .await
                .is_err(),
                "case {point:?}"
            );
            assert!(
                fired.load(std::sync::atomic::Ordering::SeqCst),
                "case {point:?} did not reach its failure point"
            );

            let logs = s
                .list_structural_logs("db", Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap();
            if retains_ready {
                assert_eq!(logs.len(), 1, "case {point:?}");
                assert_eq!(
                    logs[0].1.value().unwrap().phase,
                    StructuralLogPhase::Ready,
                    "case {point:?}"
                );
            } else {
                assert!(logs.is_empty(), "case {point:?}");
            }

            let (record, _) = s
                .records
                .load_record(&collection(), Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap();
            assert_eq!(
                record.topology_participants().count(),
                usize::from(retains_ready),
                "case {point:?}"
            );
        }
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

        let router = TreeRouter::new(s.shards.nodes().clone());
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
                .leaf_for(&collection(), k, Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap();
            assert!(
                loc.node().unwrap().as_leaf().unwrap().exists(k),
                "key {k:?} lost"
            );
        }
        assert!(
            s.list_structural_logs("db", Requirement::AtLeast(s.timeline.now()))
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

        let router = TreeRouter::new(s.shards.nodes().clone());
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
                .leaf_for(&collection(), key, Requirement::AtLeast(s.timeline.now()))
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
        let router = TreeRouter::new(s.shards.nodes().clone());
        for k in [b"a".as_slice(), b"m", b"t"] {
            let loc = router
                .leaf_for(&collection(), k, Requirement::AtLeast(s.timeline.now()))
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
        let root = Node::leaf(Shard::from_entries(
            [b"a".as_slice(), b"b", b"c", b"d"].iter().map(|k| live(k)),
        ));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());
        let sp = splitter(&s, &bg, tiny());

        sp.split_path(&root_path()).await.unwrap();
        let after_first = TreeRouter::new(s.shards.nodes().clone())
            .leaves(&collection(), Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        // Re-run: each resulting leaf holds two keys, which is at (not over) the
        // cap, so nothing changes.
        for leaf in &after_first {
            sp.split_path(&leaf.path).await.unwrap();
        }
        sp.split_path(&root_path()).await.unwrap();

        let after_second = TreeRouter::new(s.shards.nodes().clone())
            .leaves(&collection(), Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert_eq!(
            after_first.len(),
            after_second.len(),
            "a settled tree does not keep splitting"
        );
        assert!(
            s.list_structural_logs("db", Requirement::AtLeast(s.timeline.now()))
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
        let root = Node::leaf(Shard::from_entries(
            [b"a".as_slice(), b"b", b"c", b"d"].iter().map(|k| live(k)),
        ));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());

        let candidates = SplitCandidates::with_policy(tiny());
        // Under the cap: not enqueued.
        candidates.observe_leaf(&root_path(), &Shard::from_entries([live(b"a"), live(b"b")]));
        assert!(
            candidates.drain().is_empty(),
            "at-cap leaf is not a candidate"
        );
        // Over the cap: enqueued and split by a sweep.
        candidates.observe_leaf(
            &root_path(),
            &Shard::from_entries([live(b"a"), live(b"b"), live(b"c"), live(b"d")]),
        );
        let sp = splitter_with_candidates(&s, &bg, candidates);
        sp.run_once().await;

        let leaves = TreeRouter::new(s.shards.nodes().clone())
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
        let root = Node::leaf(Shard::from_entries(keys.iter().map(|key| live(key))));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());
        let policy = SplitPolicy {
            leaf_max_entries: 2,
            leaf_max_bytes: 1 << 20,
            index_max_children: 100,
            ..SplitPolicy::default()
        };
        let candidates = SplitCandidates::with_policy(policy);
        candidates.observe_leaf(&root_path(), root.as_leaf().unwrap());
        let sp = splitter_with_candidates(&s, &bg, candidates);

        // 9 -> 4+5 -> 2+2+2+3 -> 2+2+2+1+2.
        for _ in 0..3 {
            sp.run_once().await;
        }

        let router = TreeRouter::new(s.shards.nodes().clone());
        let leaves = router
            .leaves(&collection(), Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert_eq!(leaves.len(), 5);
        assert!(leaves.iter().all(|leaf| {
            leaf.node().unwrap().as_leaf().unwrap().len() <= policy.leaf_max_entries
        }));
        for key in keys {
            let located = router
                .leaf_for(&collection(), key, Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap();
            assert!(located.node().unwrap().as_leaf().unwrap().exists(key));
        }
    }

    #[tokio::test]
    async fn repeated_inline_pressure_performs_one_rerouted_median_split_each() {
        let s = store();
        let root = Node::leaf(Shard::from_entries([
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

        let router = TreeRouter::new(s.shards.nodes().clone());
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
            .leaf_for(&collection(), b"h", Requirement::AtLeast(s.timeline.now()))
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
        // revalidation must find and split the current owning child.
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
            .leaf_for(&collection(), b"h", Requirement::AtLeast(s.timeline.now()))
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
        let root = Node::leaf(Shard::from_entries([live(b"a"), live(b"b")]));
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
        let mut node = Node::leaf(Shard::from_entries(
            [b"a".as_slice(), b"b", b"c", b"d"].iter().map(|k| live(k)),
        ));
        node.add_membership_reader(holder.clone());
        let root = node;
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());
        let candidates = SplitCandidates::with_policy(tiny());
        candidates.observe_leaf(
            &root_path(),
            &Shard::from_entries([live(b"a"), live(b"b"), live(b"c"), live(b"d")]),
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

        let (mut root, version) = s
            .load_root(COLL, Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        root.remove_membership_holder(&holder);
        assert!(s.store_root(COLL, &root, &version).await.unwrap());

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
        let leaves = TreeRouter::new(s.shards.nodes().clone())
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
            key: KeyRef::new(collection(), b"d"),
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
        let node = Node::leaf(Shard::from_entries(entries));
        s.store_node(COLL, "L", &node, None).await.unwrap();
        let root = Node::index(IndexNode::from_children([(Vec::new(), "L".to_string())]));
        s.create_root(COLL, &root).await.unwrap();

        sp.split_path(&node_path("L")).await.unwrap();

        let leaf = TreeRouter::new(s.shards.nodes().clone())
            .leaf_for(&collection(), b"d", Requirement::AtLeast(s.timeline.now()))
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
        let other_transactions = TLogger::new(other.objects.clone(), db_root("db"));
        let other_mon = Monitor::with_config(
            other_transactions.clone(),
            other.timeline.clone(),
            Arc::downgrade(&other_bg),
            RetryConfig::default(),
            crate::monitor::ProtocolTiming::default(),
        );
        let other_key_state = KeyStateResolver::new(other_mon.clone());
        let other_coord = ShardCoordinator::with_hinter(
            other.shards.clone(),
            other_key_state,
            other_mon.clone(),
            RetryConfig::default(),
            SplitPolicy::default(),
            Arc::new(NoSplitHints),
        );
        let other_locker = crate::tlocker::Locker::new(
            other_coord,
            TreeRouter::new(other.shards.nodes().clone()),
            crate::collection_coordination::CollectionStateResolver::new(
                other.records.clone(),
                other_transactions,
                other_mon.clone(),
                RetryConfig::default(),
            ),
            other_mon,
            RetryConfig::default(),
        );
        other_locker
            .keys()
            .write_back_one_put(
                &holder,
                &node_path("L"),
                b"d",
                &KeyRef::new(collection(), b"d"),
            )
            .await;
        let current = TreeRouter::new(other.shards.nodes().clone())
            .leaf_for(&collection(), b"d", Requirement::Any)
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
            &Shard::from_entries([live(b"a"), live(b"b"), live(b"c"), live(b"d")]),
        );
        sp.run_once().await;
        assert_eq!(
            TreeRouter::new(s.shards.nodes().clone())
                .leaves(&collection(), Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .len(),
            1
        );

        mon.abort_owned_tx(&older).await.unwrap();
        sp.run_once().await;
        assert_eq!(
            TreeRouter::new(s.shards.nodes().clone())
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
        let root = Node::leaf(Shard::from_entries(
            [b"a".as_slice(), b"b", b"c", b"d"].iter().map(|k| live(k)),
        ));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());

        // A generous entry cap but a tiny byte cap: the four-entry leaf is far
        // under the entry cap yet over the byte cap.
        let policy = SplitPolicy {
            leaf_max_entries: 1000,
            leaf_max_bytes: 8,
            index_max_children: 1000,
            ..SplitPolicy::default()
        };
        let candidates = SplitCandidates::with_policy(policy);
        candidates.observe_leaf(
            &root_path(),
            &Shard::from_entries([live(b"a"), live(b"b"), live(b"c"), live(b"d")]),
        );

        let sp = splitter_with_candidates(&s, &bg, candidates);
        sp.run_once().await;

        // The only cap crossed is the byte cap, so a split here proves the byte
        // cap now has a producer.
        let leaves = TreeRouter::new(s.shards.nodes().clone())
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
        let policy = SplitPolicy {
            leaf_max_entries: 2,
            leaf_max_bytes: 1 << 20,
            index_max_children: 100,
            ..SplitPolicy::default()
        };
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
        let router = TreeRouter::new(s.shards.nodes().clone());
        for k in [b"a".as_slice(), b"b", b"g", b"h", b"m", b"n", b"o"] {
            let loc = router
                .leaf_for(&collection(), k, Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap();
            assert!(
                loc.node().unwrap().as_leaf().unwrap().exists(k),
                "key {k:?} lost"
            );
        }
    }

    // ADR-032 retry path: a separator whose parent CAS keeps losing leaves its
    // structural record in progress and is re-queued for a later sweep. A backend that blocks writes to
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
            TreeRouter::new(s.shards.nodes().clone())
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
        assert!(sp.recover_structural_logs().await);
        assert!(
            s.list_structural_logs("db", Requirement::AtLeast(s.timeline.now()))
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

    #[tokio::test]
    async fn startup_structural_recovery_reclaims_an_orphan_after_restart() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let first = store_with_backend(backend.clone());
        first
            .store_node(COLL, "L", &leaf_node(&[b"a", b"b"], None, None), None)
            .await
            .unwrap();
        first
            .store_node(COLL, "R", &leaf_node(&[b"m", b"n"], None, None), None)
            .await
            .unwrap();
        let root = Node::index(IndexNode::from_children([(Vec::new(), "L".to_string())]));
        first.create_root(COLL, &root).await.unwrap();
        first
            .write_structural_log("R", &nonroot_record("L", "R", b"m"))
            .await
            .unwrap();
        drop(first);

        let second = store_with_backend(backend);
        let bg = Arc::new(Background::new());
        let splitter = splitter(&second, &bg, tiny());
        splitter.start();
        for _ in 0..20 {
            if matches!(
                second
                    .load_node(COLL, "R", Requirement::AtLeast(second.timeline.now()))
                    .await,
                Err(StorageError::NotFound)
            ) {
                break;
            }
            rt::yield_now().await;
        }

        assert!(matches!(
            second
                .load_node(COLL, "R", Requirement::AtLeast(second.timeline.now()))
                .await,
            Err(StorageError::NotFound)
        ));
        assert!(
            second
                .load_node(COLL, "L", Requirement::AtLeast(second.timeline.now()))
                .await
                .is_ok()
        );
        assert!(
            second
                .list_structural_logs("db", Requirement::AtLeast(second.timeline.now()))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn structural_recovery_defers_while_the_source_writer_is_live() {
        let s = store();
        let bg = Arc::new(Background::new());
        let sp = splitter(&s, &bg, tiny());
        let id = TxId::with_priority(1, b"live-split");
        sp.mon.begin_tx(&id);

        let mut source = leaf_node(&[b"a", b"b"], None, None);
        source.set_structural_gate(id.clone());
        s.store_node(COLL, "L", &source, None).await.unwrap();
        s.store_node(COLL, "R", &leaf_node(&[b"m", b"n"], None, None), None)
            .await
            .unwrap();
        let root = Node::index(IndexNode::from_children([(Vec::new(), "L".to_string())]));
        s.create_root(COLL, &root).await.unwrap();
        let record = nonroot_record("L", "R", b"m");
        let observed = s.write_structural_log("R", &record).await.unwrap();

        assert!(matches!(
            sp.recover_record(&observed).await,
            Err(TransError::Retry)
        ));
        assert!(
            s.load_node(COLL, "R", Requirement::AtLeast(s.timeline.now()))
                .await
                .is_ok()
        );
        assert_eq!(
            s.list_structural_logs("db", Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .len(),
            1
        );

        sp.mon.abort_owned_tx(&id).await.unwrap();
        sp.recover_record(&observed).await.unwrap();
        assert!(matches!(
            s.load_node(COLL, "R", Requirement::AtLeast(s.timeline.now()))
                .await,
            Err(StorageError::NotFound)
        ));
        assert!(
            s.list_structural_logs("db", Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .is_empty()
        );
    }

    /// Regression: structural recovery must fence an in-flight split by reading
    /// the source *freshly*, not from a snapshot it cached before the split took
    /// the gate.
    ///
    /// A split acquires its source structural gate before writing its structural
    /// record, so the record's watermark is at least as fresh as that gate.
    /// Recovery once fenced (and tested reachability) at a single sweep-start
    /// epoch, which a pre-split cached snapshot — no gate, no sibling — could
    /// satisfy; recovery then judged the live split unapplied and deleted its
    /// freshly created, now-live child, breaking the leaf right-link chain.
    /// Pinning the reads to the record's own watermark forces recovery past the
    /// gate write.
    ///
    /// Here `s` (recovery) caches the pre-gate source, a peer sharing the backend
    /// then takes the gate and creates the child, and recovery must defer instead
    /// of reclaiming the child. Reading the source from the stale cache (as the
    /// buggy sweep epoch allowed) reclaims `R`; the fresh read observes the live
    /// holder and defers.
    #[tokio::test]
    async fn recovery_reads_a_live_split_freshly_and_keeps_its_child() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let s = store_with_backend(backend.clone());
        let peer = store_with_backend(backend);
        let bg = Arc::new(Background::new());
        let sp = splitter(&s, &bg, tiny());
        let id = TxId::with_priority(1, b"inflight-split");
        sp.mon.begin_tx(&id);

        // Initial tree, written by the peer: a root index over a single leaf L
        // that carries no structural gate.
        peer.store_node(COLL, "L", &leaf_node(&[b"a", b"b"], None, None), None)
            .await
            .unwrap();
        let root = Node::index(IndexNode::from_children([(Vec::new(), "L".to_string())]));
        peer.create_root(COLL, &root).await.unwrap();

        // Recovery reads L first, caching the pre-gate snapshot (no gate). A weak
        // freshness bound would later be satisfied by exactly this stale entry.
        s.load_node(COLL, "L", Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();

        // The in-flight split (peer, sharing the backend): take the source gate
        // and create the sibling. `s`'s cache is unaware of both writes.
        let (mut gated, version) = peer
            .load_node(COLL, "L", Requirement::AtLeast(peer.timeline.now()))
            .await
            .unwrap();
        gated.set_structural_gate(id.clone());
        assert!(
            peer.store_node(COLL, "L", &gated, Some(&version))
                .await
                .unwrap()
        );
        peer.store_node(COLL, "R", &leaf_node(&[b"m", b"n"], None, None), None)
            .await
            .unwrap();

        // The record is written after the gate, so its watermark is at least as
        // fresh; recovery reading at that watermark must observe the live gate.
        let record = nonroot_record("L", "R", b"m");
        let observed = s.write_structural_log("R", &record).await.unwrap();

        assert!(
            matches!(sp.recover_record(&observed).await, Err(TransError::Retry)),
            "recovery must defer to the live split rather than reclaim its child"
        );
        assert!(
            s.load_node(COLL, "R", Requirement::AtLeast(s.timeline.now()))
                .await
                .is_ok(),
            "the live split's child must survive recovery"
        );
        assert_eq!(
            s.list_structural_logs("db", Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .len(),
            1,
            "the in-flight split's record is left for a later sweep"
        );
    }

    #[tokio::test]
    async fn recovery_rolls_forward_a_landed_nonroot_split() {
        let s = store();
        s.store_node(
            COLL,
            "P0",
            &leaf_node(&[b"a", b"b"], Some(b"m"), Some("L")),
            None,
        )
        .await
        .unwrap();
        s.store_node(
            COLL,
            "L",
            &leaf_node(&[b"m", b"n"], Some(b"t"), Some("R")),
            None,
        )
        .await
        .unwrap();
        s.store_node(COLL, "R", &leaf_node(&[b"t", b"u"], None, None), None)
            .await
            .unwrap();
        let root = Node::index(IndexNode::from_children([
            (Vec::new(), "P0".to_string()),
            (b"m".to_vec(), "L".to_string()),
        ]));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());
        let sp = splitter(&s, &bg, tiny());

        let record = StructuralLog {
            collection: collection(),
            source_token: Some(test_token("L")),
            source_version: String::new(),
            created_tokens: vec![test_token("R")],
            split_key: b"t".to_vec(),
            participant_id: TxId::from_bytes(b"structural-participant".to_vec()),
            phase: StructuralLogPhase::Ready,
        };
        let observed = s.write_structural_log("R", &record).await.unwrap();

        sp.recover_record(&observed).await.unwrap();

        let (root_node, _) = s
            .load_root_node(COLL, Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap()
            .unwrap();
        assert!(
            !root_node.over_soft_cap(&tiny()),
            "recovery completes the parent split requested by publication"
        );
        let router = TreeRouter::new(s.shards.nodes().clone());
        assert_eq!(
            router
                .leaves(&collection(), Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .len(),
            3,
            "the recovered separator keeps every leaf reachable after the parent split"
        );
        for key in [b"a".as_slice(), b"m", b"t"] {
            let leaf = router
                .leaf_for(&collection(), key, Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap();
            assert!(leaf.node().unwrap().as_leaf().unwrap().exists(key));
        }
        assert!(
            s.list_structural_logs("db", Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .is_empty()
        );
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

    struct FirstSourceWriteGate {
        armed: std::sync::atomic::AtomicBool,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    impl FirstSourceWriteGate {
        fn wrap(inner: Arc<dyn Backend>, source_path: String) -> (Arc<HookBackend>, Arc<Self>) {
            let gate = Arc::new(Self {
                armed: std::sync::atomic::AtomicBool::new(false),
                entered: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
            });
            let backend = HookBackend::new(inner);
            backend.set_before({
                let gate = gate.clone();
                move |op| {
                    let wait = matches!(
                        op,
                        BackendOp::WriteIf { path, .. }
                            if path == &source_path
                                && gate
                                    .armed
                                    .swap(false, std::sync::atomic::Ordering::SeqCst)
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
            self.armed.store(true, std::sync::atomic::Ordering::SeqCst);
        }

        async fn wait_until_entered(&self) {
            self.entered.notified().await;
        }

        fn release(&self) {
            self.release.notify_one();
        }
    }

    #[tokio::test]
    async fn recovery_fences_an_aborted_writer_before_reclaiming_its_sibling() {
        let source_path = node_path("L").to_string();
        let (backend, gate) =
            FirstSourceWriteGate::wrap(Arc::new(MemoryBackend::new()), source_path.clone());
        let backend: Arc<dyn Backend> = backend;
        let s = store_with_backend(backend.clone());
        // This writer models a separately opened database, so it owns a
        // distinct database-local path coordinator over the shared backend.
        let peer = store_with_backend(backend);
        let bg = Arc::new(Background::new());
        let sp = splitter(&s, &bg, tiny());
        let id = TxId::with_priority(1, b"racing-split");

        let mut original = leaf_node(&[b"a", b"b", b"m", b"n"], None, None);
        original.set_structural_gate(id.clone());
        s.store_node(COLL, "L", &original, None).await.unwrap();
        let (mut shrunk, source_version) = s
            .load_node(COLL, "L", Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        let (right, split_key) = shrunk.split("R").unwrap();
        shrunk.remove_structural_gate(&id);
        s.store_node(COLL, "R", &right, None).await.unwrap();
        let root = Node::index(IndexNode::from_children([(Vec::new(), "L".to_string())]));
        s.create_root(COLL, &root).await.unwrap();

        let record = StructuralLog {
            collection: collection(),
            source_token: Some(test_token("L")),
            source_version: source_version.revision().unwrap().serialize().to_string(),
            created_tokens: vec![test_token("R")],
            split_key,
            participant_id: TxId::from_bytes(b"structural-participant".to_vec()),
            phase: StructuralLogPhase::Ready,
        };
        let observed = s.write_structural_log("R", &record).await.unwrap();
        sp.mon.begin_tx(&id);
        assert_eq!(
            sp.mon.preempt_tx(&id).await.unwrap(),
            TxFinalStatus::Aborted
        );

        gate.arm();
        let recovering = {
            let sp = sp.clone();
            tokio::spawn(async move { sp.recover_record(&observed).await })
        };
        gate.wait_until_entered().await;

        assert!(
            peer.store_node(COLL, "L", &shrunk, Some(&source_version))
                .await
                .unwrap()
        );
        gate.release();
        recovering.await.unwrap().unwrap();

        assert!(
            s.load_node(COLL, "R", Requirement::AtLeast(s.timeline.now()))
                .await
                .is_ok()
        );
        let (root_node, _) = s
            .load_root_node(COLL, Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            root_node.as_index().unwrap().child_for(b"m"),
            Some(test_token("R").as_str())
        );
    }
}
