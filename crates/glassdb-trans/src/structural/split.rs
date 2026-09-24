//! Background growth of the B-link coordination tree by leaf and node splits
//! (ADR-031).
//!
//! A leaf that crosses its soft cap is halved so no single object becomes a
//! scalability or contention bottleneck. Splitting runs off the hot path in a
//! periodic background task, fed candidates from stored over-cap leaves,
//! capacity rejections, and inline admission misses — never a key-space
//! enumeration.
//!
//! Every split is a sequence of independent, idempotent compare-and-swaps under
//! a one-node structural gate. Before joining collection topology in `_i`, it
//! writes a `Preparing` intent below its topology participant's `_s` prefix.
//! After taking the source gate it conditionally advances that intent to
//! `Ready`; only then may it create nodes. A lifecycle freeze can therefore
//! find exactly one participant's work and cancel an unadvanced intent without
//! racing late node creation:
//!
//! 0. Advance the structural intent with the source observation and split key;
//!    its created-node tokens were reserved while `Preparing`.
//! 1. Create the right sibling (`write_if_not_exists`) holding the upper half
//!    and inheriting the source's former high-key and right-sibling.
//! 2. **Shrink the source in one CAS** — drop the upper half, set high-key to
//!    the split key, link to the sibling. This is the linearization point:
//!    descent now finds the moved keys by stepping right, and a concurrent
//!    locker that retained the pre-shrink observation loses its CAS and
//!    re-routes (ADR-031 coverage re-check).
//! 3. Reconcile the parent with the right-link chain of its children so
//!    future descents skip the right-link hop (ADR-073); recurse when the
//!    parent itself overflows. Purely an optimization — correctness never
//!    depends on it landing.
//!
//! A leaf split, including a root-leaf split, acquires a structural gate
//! through the shared [`LeafCoordinator`](crate::leaf_coord::LeafCoordinator),
//! in the same batched CAS stream as data mutations on that leaf. Interior
//! indexes use direct structural CASes. The source shrink (or root rewrite)
//! releases the structural gate inline, so no unlocked post-split state is
//! exposed before a separate release CAS. Once a leaf is quiescent behind that
//! gate, holder-free tombstones are removed before the final reason check. The
//! compacted leaf either cancels the split in one CAS or supplies the ordinary
//! recoverable split outputs (ADR-062).
//!
//! The tree root `_r` cannot move (its address is fixed), so when it
//! overflows it splits **in place**: two children are created and the root is
//! rewritten into a two-entry index over them, growing the tree's height while
//! leaving the independent collection record untouched.

use std::sync::atomic::Ordering;

use glassdb_data::{CollectionAddress, NodeToken, ObjectPath, TxId};
use glassdb_storage::{IndexNode, LeafEntry, LeafObservation, Node, Requirement, StorageError};

use crate::error::TransError;
use crate::node_locking::GateAcquisition;

use super::candidates::{CandidateCause, MaintenanceCandidate};
use super::change::{
    ChangeAttemptOutcome, ChangeContext, PlannedChange, StructuralChangeAttempt,
    StructuralTopology, reclaim_holder_free_tombstones,
};
use super::recovery::{PreparedIntent, ReadyChange};

/// Why a node may need a split.
#[derive(Clone)]
pub(super) enum SplitReason {
    SoftCap,
    Capacity,
    InlinePressure { key: Vec<u8>, value_len: usize },
}

/// Whether a node still needs the split that its reason asked for.
pub(super) enum SplitNeed {
    Split,
    NotActionable,
    Reroute,
}

/// The node that a split divides.
#[derive(Clone, Copy)]
pub(super) enum SplitTarget<'a> {
    Root,
    NonRoot(&'a NodeToken),
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

impl SplitReason {
    pub(super) fn class(&self) -> u8 {
        match self {
            SplitReason::SoftCap => 0,
            SplitReason::InlinePressure { .. } => 1,
            SplitReason::Capacity => 2,
        }
    }

    pub(super) fn is_inline_pressure(&self) -> bool {
        matches!(self, SplitReason::InlinePressure { .. })
    }
}

impl<'a> SplitTarget<'a> {
    pub(super) fn source_token(self) -> Option<&'a NodeToken> {
        match self {
            Self::Root => None,
            Self::NonRoot(token) => Some(token),
        }
    }
}

/// Splits the node of one candidate when `reason` still calls for it, using
/// the candidate's already-aged wound-wait priority.
pub(super) async fn split_candidate(
    ctx: &ChangeContext,
    path: &ObjectPath,
    reason: &SplitReason,
    id: TxId,
) -> Result<(), TransError> {
    match reason {
        SplitReason::SoftCap | SplitReason::Capacity => {
            split_path_with_id(ctx, path, id, reason).await
        }
        SplitReason::InlinePressure { key, value_len } => {
            split_inline_pressure(ctx, path, key, *value_len, id).await
        }
    }
}

/// Splits a node when the given reason is still actionable.
pub(super) async fn split_path(
    ctx: &ChangeContext,
    path: &ObjectPath,
    reason: &SplitReason,
) -> Result<(), TransError> {
    split_path_with_id(ctx, path, ctx.candidates.new_id(), reason).await
}

/// Splits `path` beneath an existing topology participant.
pub(super) async fn split_path_joined(
    ctx: &ChangeContext,
    path: &ObjectPath,
    topology_participant: &TxId,
    reason: &SplitReason,
) -> Result<(), TransError> {
    let (collection, target) = split_target(path)?;
    // Recovery can reconcile parents after the topology participant has a
    // final status. A fresh structural identity prevents help-forward from
    // mistaking this in-flight recursive split for stale work.
    let worker = ctx.candidates.new_id();
    StructuralChangeAttempt::new(
        ctx,
        collection,
        PlannedChange::Split { target, reason },
        worker,
    )
    .run(StructuralTopology::Joined(topology_participant))
    .await
}

/// Coordinates one split after its intent is prepared.
pub(super) async fn coordinate(
    ctx: &ChangeContext,
    collection: &CollectionAddress,
    target: SplitTarget<'_>,
    worker: &TxId,
    reason: &SplitReason,
    prepared: PreparedIntent,
) -> ChangeAttemptOutcome {
    match target {
        SplitTarget::Root => coordinate_root_split(ctx, collection, worker, reason, prepared).await,
        SplitTarget::NonRoot(token) => {
            coordinate_nonroot_split(ctx, collection, token, worker, reason, prepared).await
        }
    }
}

/// Classifies whether `node` still needs the split represented by `reason`.
pub(super) fn split_need(ctx: &ChangeContext, node: &Node, reason: &SplitReason) -> SplitNeed {
    match reason {
        SplitReason::Capacity => {
            // The rejected operation is retried by its owner. A hint only
            // asks for one split of a divisible node, even below soft caps.
            if node.as_leaf().is_some_and(|leaf| leaf.len() >= 2)
                || node.as_index().is_some_and(|index| index.len() >= 2)
            {
                SplitNeed::Split
            } else {
                SplitNeed::NotActionable
            }
        }
        SplitReason::SoftCap => {
            if node.over_soft_cap(ctx.candidates.policy()) {
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
            let inline = ctx.candidates.inline();
            if !inline.admits_value(*value_len)
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
            if inline.admits(other_inline_bytes, *value_len) {
                SplitNeed::NotActionable
            } else {
                SplitNeed::Split
            }
        }
    }
}

/// Reroutes and revalidates one pressure observation before splitting.
async fn split_inline_pressure(
    ctx: &ChangeContext,
    observed_path: &ObjectPath,
    key: &[u8],
    value_len: usize,
    id: TxId,
) -> Result<(), TransError> {
    let (collection, _) = split_target(observed_path)?;
    let located = match ctx
        .router
        .route_key(
            collection,
            key,
            Requirement::after(ctx.timeline.currentness_barrier()),
        )
        .await
    {
        Ok(located) => located,
        Err(StorageError::NotFound) => {
            ctx.stats
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
        .map(|node| split_need(ctx, node, &reason))
        .unwrap_or(SplitNeed::NotActionable)
    {
        SplitNeed::Split => split_path_with_id(ctx, &located.path, id, &reason).await,
        SplitNeed::NotActionable => {
            ctx.stats
                .inline_pressure_discarded
                .fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        SplitNeed::Reroute => Err(TransError::Retry),
    }
}

/// Splits `path` using an already-aged wound-wait priority.
async fn split_path_with_id(
    ctx: &ChangeContext,
    path: &ObjectPath,
    id: TxId,
    reason: &SplitReason,
) -> Result<(), TransError> {
    let (collection, target) = split_target(path)?;
    StructuralChangeAttempt::new(ctx, collection, PlannedChange::Split { target, reason }, id)
        .run(StructuralTopology::Owned)
        .await
}

/// Performs the write-ahead, sibling creation, shrink, and parent
/// reconciliation.
async fn coordinate_nonroot_split(
    ctx: &ChangeContext,
    collection: &CollectionAddress,
    token: &NodeToken,
    worker: &TxId,
    reason: &SplitReason,
    prepared: PreparedIntent,
) -> ChangeAttemptOutcome {
    debug_assert!(prepared.targets(collection, Some(token)));
    let target = SplitTarget::NonRoot(token);
    let right_token = prepared
        .nonroot_sibling()
        .expect("a prepared non-root intent always reserves one sibling")
        .clone();
    let QuiescedSplitSource {
        mut node,
        observation,
        reclaimed,
    } = match prepare_split_source(ctx, collection, target, worker, reason).await {
        Ok(source) => source,
        Err(outcome) => return outcome,
    };

    let Some((right, split_key)) = node.split(right_token.as_str()) else {
        return ctx
            .cancel_preparing_change(collection, Some(token), worker, Ok(()))
            .await;
    };
    node.remove_structural_gate(worker);
    let ready = match ctx
        .mark_ready(
            worker,
            prepared,
            &observation,
            ReadyChange::Split {
                split_key: split_key.clone(),
            },
        )
        .await
    {
        Ok(ready) => ready,
        Err(outcome) => return outcome,
    };
    if let Err(error) = create_split_node(ctx, collection, &right_token, &right).await {
        return ChangeAttemptOutcome::recovery_required(ready, error);
    }
    if let Err(error) =
        store_nonroot_split_source(ctx, collection, token, &node, &observation).await
    {
        return ChangeAttemptOutcome::recovery_required(ready, error);
    }
    record_completed_split(
        ctx,
        collection,
        reason,
        &reclaimed,
        [(token, &node), (&right_token, &right)],
    );
    if let Err(error) = ctx
        .reconcile_parent(
            collection,
            &split_key,
            &right_token,
            Some(ready.participant()),
        )
        .await
    {
        return ChangeAttemptOutcome::recovery_required(ready, error);
    }
    ctx.finish_ready_change(ready).await
}

/// Performs the write-ahead, child creation, and root rewrite.
async fn coordinate_root_split(
    ctx: &ChangeContext,
    collection: &CollectionAddress,
    worker: &TxId,
    reason: &SplitReason,
    prepared: PreparedIntent,
) -> ChangeAttemptOutcome {
    debug_assert!(prepared.targets(collection, None));
    let QuiescedSplitSource {
        node,
        observation,
        reclaimed,
    } = match prepare_split_source(ctx, collection, SplitTarget::Root, worker, reason).await {
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
    } = match plan_root_split(ctx, &prepared, &node, worker) {
        Ok(plan) => plan,
        Err(error) => {
            return ctx
                .cancel_preparing_change(collection, None, worker, Err(error))
                .await;
        }
    };
    let ready = match ctx
        .mark_ready(
            worker,
            prepared,
            &observation,
            ReadyChange::Split { split_key },
        )
        .await
    {
        Ok(ready) => ready,
        Err(outcome) => return outcome,
    };
    if let Err(error) = create_split_node(ctx, collection, &left_token, &left).await {
        return ChangeAttemptOutcome::recovery_required(ready, error);
    }
    if let Err(error) = create_split_node(ctx, collection, &right_token, &right).await {
        return ChangeAttemptOutcome::recovery_required(ready, error);
    }
    if let Err(error) = store_split_root(ctx, &index, &observation).await {
        return ChangeAttemptOutcome::recovery_required(ready, error);
    }
    record_completed_split(
        ctx,
        collection,
        reason,
        &reclaimed,
        [(&left_token, &left), (&right_token, &right)],
    );
    ctx.finish_ready_change(ready).await
}

/// Acquires, revalidates, and compacts one source before any split intent
/// becomes recoverable.
async fn prepare_split_source(
    ctx: &ChangeContext,
    collection: &CollectionAddress,
    target: SplitTarget<'_>,
    worker: &TxId,
    reason: &SplitReason,
) -> Result<QuiescedSplitSource, ChangeAttemptOutcome> {
    let (mut node, observation) = match ctx
        .structural_nodes
        .acquire_structural_gate(
            collection,
            target.source_token(),
            worker,
            GateAcquisition::WoundWait,
        )
        .await
    {
        Ok(Some(acquired)) => acquired,
        Ok(None) => {
            return Err(ChangeAttemptOutcome::retry_cleanly(Err(TransError::Retry)));
        }
        Err(error) => return Err(ChangeAttemptOutcome::retry_cleanly(Err(error))),
    };
    match split_need(ctx, &node, reason) {
        SplitNeed::Split => {}
        need => {
            return Err(finish_without_split(ctx, collection, target, worker, reason, need).await);
        }
    }

    let reclaimed = reclaim_holder_free_tombstones(&mut node);
    match split_need(ctx, &node, reason) {
        SplitNeed::Split => Ok(QuiescedSplitSource {
            node,
            observation,
            reclaimed,
        }),
        SplitNeed::NotActionable if !reclaimed.is_empty() => Err(ctx
            .finish_reclamation_without_change(
                collection,
                target.source_token(),
                worker,
                node,
                &observation,
                &reclaimed,
                true,
            )
            .await),
        need => Err(finish_without_split(ctx, collection, target, worker, reason, need).await),
    }
}

/// Finishes an authoritative reason check that no longer calls for this
/// source to split.
async fn finish_without_split(
    ctx: &ChangeContext,
    collection: &CollectionAddress,
    target: SplitTarget<'_>,
    worker: &TxId,
    reason: &SplitReason,
    need: SplitNeed,
) -> ChangeAttemptOutcome {
    let result = match need {
        SplitNeed::NotActionable => {
            if reason.is_inline_pressure() {
                ctx.stats
                    .inline_pressure_discarded
                    .fetch_add(1, Ordering::Relaxed);
            }
            Ok(())
        }
        SplitNeed::Reroute => Err(TransError::Retry),
        SplitNeed::Split => unreachable!("a required split must remain in coordination"),
    };
    ctx.cancel_preparing_change(collection, target.source_token(), worker, result)
        .await
}

/// Builds both root children and the replacement root index before the
/// structural intent becomes recoverable.
fn plan_root_split(
    ctx: &ChangeContext,
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
    let policy = ctx.candidates.policy();
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

/// Creates one immutable child reserved by a structural split.
async fn create_split_node(
    ctx: &ChangeContext,
    collection: &CollectionAddress,
    token: &NodeToken,
    node: &Node,
) -> Result<(), TransError> {
    if ctx.nodes.store_node(collection, token, node, None).await? {
        Ok(())
    } else {
        Err(TransError::Retry)
    }
}

/// Shrinks a non-root source against the observation in its Ready intent.
async fn store_nonroot_split_source(
    ctx: &ChangeContext,
    collection: &CollectionAddress,
    token: &NodeToken,
    node: &Node,
    observation: &LeafObservation,
) -> Result<(), TransError> {
    if ctx
        .nodes
        .store_node(collection, token, node, Some(observation))
        .await?
    {
        Ok(())
    } else {
        Err(TransError::Retry)
    }
}

/// Rewrites the fixed tree root against the observation in its Ready
/// intent.
async fn store_split_root(
    ctx: &ChangeContext,
    index: &Node,
    observation: &LeafObservation,
) -> Result<(), TransError> {
    if ctx
        .structural_nodes
        .store_structural_node(index, observation)
        .await?
        .is_some()
    {
        Ok(())
    } else {
        Err(TransError::Retry)
    }
}

/// Publishes statistics, follow-up candidates, and GC hints after the
/// source/root linearization is acknowledged.
fn record_completed_split(
    ctx: &ChangeContext,
    collection: &CollectionAddress,
    reason: &SplitReason,
    reclaimed: &[TxId],
    outputs: [(&NodeToken, &Node); 2],
) {
    ctx.record_reclamation(reclaimed, false);
    ctx.stats.splits.fetch_add(1, Ordering::Relaxed);
    for (token, node) in outputs {
        enqueue_if_over_soft_cap(ctx, collection, token, node);
    }
    if reason.is_inline_pressure() {
        ctx.stats
            .inline_pressure_completed
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Carries an oversized split output into a later sweep so one hint can
/// drive the whole split cascade.
fn enqueue_if_over_soft_cap(
    ctx: &ChangeContext,
    collection: &CollectionAddress,
    token: &NodeToken,
    node: &Node,
) {
    if !node.over_soft_cap(ctx.candidates.policy()) {
        return;
    }
    ctx.candidates.push(MaintenanceCandidate {
        path: ObjectPath::Node {
            collection: collection.clone(),
            token: token.clone(),
        },
        priority: ctx.candidates.new_id(),
        cause: CandidateCause::Split(SplitReason::SoftCap),
    });
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
        .expect("a split source has at least two entries/children");
    source.remove_structural_gate(structure_holder);
    (source, right, split_key)
}

/// Returns the collection and the node that a split of `path` divides.
fn split_target(path: &ObjectPath) -> Result<(&CollectionAddress, SplitTarget<'_>), TransError> {
    match path {
        ObjectPath::TreeRoot { collection } => Ok((collection, SplitTarget::Root)),
        ObjectPath::Node { collection, token } => Ok((collection, SplitTarget::NonRoot(token))),
        _ => Err(TransError::other("split candidate is not a tree node")),
    }
}
