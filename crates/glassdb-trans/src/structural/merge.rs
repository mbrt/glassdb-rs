//! Background merges of underfull nodes into their right sibling (ADR-073).
//!
//! A merge uses the same outer lifecycle as a non-root split. After the Ready
//! transition, one CAS on the target R absorbs the entries of the gated source
//! L and installs a merge reservation. One CAS on L then drains it, which is
//! the linearization point. If the drain cannot land, one CAS on R abandons
//! the merge.

use std::sync::atomic::Ordering;

use glassdb_data::{CollectionAddress, NodeToken, ObjectPath, StructuralIntentId, TxId};
use glassdb_storage::{
    LeafBody, LeafObservation, MergeTarget, Node, NodeBody, Requirement, StorageError,
};

use crate::error::TransError;
use crate::node_locking::GateAcquisition;

use super::NODE_CAS_ATTEMPTS;
use super::change::{
    ChangeAttemptOutcome, ChangeContext, PlannedChange, StructuralChangeAttempt,
    StructuralTopology, reclaim_holder_free_tombstones,
};
use super::nodes::node_token;
use super::recovery::{PreparedIntent, ReadyChange, ReadyIntent};

/// Safety bound on the right-link hops that the search for a merge target
/// walks past drained nodes, so a malformed chain can never spin the
/// restructurer.
const MAX_TARGET_SEARCH_HOPS: usize = 4096;

/// A merge target and the state that the merge decision used.
struct TargetState {
    token: NodeToken,
    node: Node,
    observation: LeafObservation,
}

/// Merges the node at `path` into its right sibling when cached state
/// shows that the merge is actionable.
pub(super) async fn merge_candidate(
    ctx: &ChangeContext,
    path: &ObjectPath,
    id: TxId,
) -> Result<(), TransError> {
    let ObjectPath::Node { collection, token } = path else {
        return Ok(());
    };
    if !merge_planned(ctx, collection, token).await? {
        return Ok(());
    }
    StructuralChangeAttempt::new(ctx, collection, PlannedChange::Merge { source: token }, id)
        .run(StructuralTopology::Owned)
        .await
}

/// Performs the polite gate, Ready, absorb, drain, reservation removal, and
/// parent reconciliation of one merge.
pub(super) async fn coordinate(
    ctx: &ChangeContext,
    collection: &CollectionAddress,
    source: &NodeToken,
    worker: &TxId,
    prepared: PreparedIntent,
) -> ChangeAttemptOutcome {
    debug_assert!(prepared.targets(collection, Some(source)));
    let (mut left, observation) = match ctx
        .structural_nodes
        .acquire_structural_gate(collection, Some(source), worker, GateAcquisition::Polite)
        .await
    {
        Ok(Some(acquired)) => acquired,
        Ok(None) => return ChangeAttemptOutcome::retry_cleanly(Err(TransError::Retry)),
        Err(error) => return ChangeAttemptOutcome::retry_cleanly(Err(error)),
    };
    let reclaimed = reclaim_holder_free_tombstones(&mut left);
    let target = match checked_merge_target(ctx, collection, &left).await {
        Ok(Some(target)) => target,
        Ok(None) if !reclaimed.is_empty() => {
            return ctx
                .finish_reclamation_without_change(
                    collection,
                    Some(source),
                    worker,
                    left,
                    &observation,
                    &reclaimed,
                    false,
                )
                .await;
        }
        Ok(None) => {
            return ctx
                .cancel_preparing_change(collection, Some(source), worker, Ok(()))
                .await;
        }
        Err(error) => {
            return ctx
                .cancel_preparing_change(collection, Some(source), worker, Err(error))
                .await;
        }
    };

    let merge = MergeTarget {
        token: target.token.clone(),
        boundary: left
            .high_key()
            .expect("a merge source has a right sibling")
            .to_vec(),
        generation: target.node.membership_generation(),
    };
    let ready = match ctx
        .mark_ready(
            worker,
            prepared,
            &observation,
            ReadyChange::Merge(merge.clone()),
        )
        .await
    {
        Ok(ready) => ready,
        Err(outcome) => return outcome,
    };
    match absorb(ctx, collection, &merge, &left, ready.id(), target).await {
        Ok(Some(target_reclaimed)) => ctx.record_reclamation(&target_reclaimed, false),
        Ok(None) => {
            return stop_ready_merge(ctx, collection, source, worker, ready).await;
        }
        Err(error) => return ChangeAttemptOutcome::recovery_required(ready, error),
    }

    let mut drained = left;
    drained.drain(merge.token.as_str());
    match ctx
        .nodes
        .store_node(collection, source, &drained, Some(&observation))
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            if let Err(error) = ctx
                .structural_nodes
                .abandon_merge(collection, &merge, ready.id(), Requirement::ANY)
                .await
            {
                return ChangeAttemptOutcome::recovery_required(ready, error);
            }
            return stop_ready_merge(ctx, collection, source, worker, ready).await;
        }
        Err(error) => return ChangeAttemptOutcome::recovery_required(ready, error.into()),
    }
    ctx.record_reclamation(&reclaimed, false);
    ctx.stats.merges.fetch_add(1, Ordering::Relaxed);
    if let Err(error) = ctx
        .structural_nodes
        .remove_merge_reservation(collection, &merge.token, ready.id(), Requirement::ANY)
        .await
    {
        return ChangeAttemptOutcome::recovery_required(ready, error);
    }
    if let Err(error) = ctx
        .reconcile_parent(
            collection,
            &merge.boundary,
            &merge.token,
            Some(ready.participant()),
        )
        .await
    {
        return ChangeAttemptOutcome::recovery_required(ready, error);
    }
    ctx.finish_ready_change(ready).await
}

/// Reports whether cached state shows node `token` as an actionable merge
/// source. Busy nodes defer the merge, so that it does not write an intent
/// that the polite gate acquisition would then cancel.
async fn merge_planned(
    ctx: &ChangeContext,
    collection: &CollectionAddress,
    token: &NodeToken,
) -> Result<bool, TransError> {
    let left = match ctx
        .nodes
        .load_node(collection, token, Requirement::ANY)
        .await
    {
        Ok((left, _)) => left,
        Err(StorageError::NotFound) => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let Some(boundary) = left.high_key().filter(|_| !left.is_drained()) else {
        return Ok(false);
    };
    let Some(target) = merge_target(ctx, collection, &left, Requirement::ANY).await? else {
        return Ok(false);
    };
    if !merge_need(ctx, &left, &target.node) {
        return Ok(false);
    }
    // Merging the children of two parents would leave the left parent
    // routing through the drained node (ADR-073).
    let parent = ctx
        .router
        .parent_of(collection, left.low_key(), token, Requirement::ANY)
        .await?;
    if !parent
        .as_ref()
        .and_then(|parent| parent.node())
        .is_some_and(|parent| parent.covers(boundary))
    {
        return Ok(false);
    }
    if left.locks().merge_reservation().is_some()
        || target.node.locks().merge_reservation().is_some()
        || has_live_holder(ctx, &left).await?
        || has_live_holder(ctx, &target.node).await?
    {
        return Err(TransError::Retry);
    }
    Ok(true)
}

/// Finds the merge target of the gated `left` in current state and checks
/// the merge decision again. `None` cancels the merge.
async fn checked_merge_target(
    ctx: &ChangeContext,
    collection: &CollectionAddress,
    left: &Node,
) -> Result<Option<TargetState>, TransError> {
    if left.is_drained() || left.high_key().is_none() {
        return Ok(None);
    }
    let requirement = Requirement::after(ctx.timeline.currentness_barrier());
    let Some(target) = merge_target(ctx, collection, left, requirement).await? else {
        return Ok(None);
    };
    if !merge_need(ctx, left, &target.node) {
        return Ok(None);
    }
    if target.node.locks().merge_reservation().is_some()
        || has_live_holder(ctx, &target.node).await?
    {
        return Err(TransError::Retry);
    }
    Ok(Some(target))
}

/// Finds the node that receives a merge of `left`: the first node on its
/// right-link chain that is not drained.
async fn merge_target(
    ctx: &ChangeContext,
    collection: &CollectionAddress,
    left: &Node,
    requirement: Requirement,
) -> Result<Option<TargetState>, TransError> {
    let mut next = left.right_sibling().map(node_token).transpose()?;
    for _ in 0..MAX_TARGET_SEARCH_HOPS {
        let Some(token) = next else {
            return Ok(None);
        };
        let (node, observation) = ctx.nodes.load_node(collection, &token, requirement).await?;
        if !node.is_drained() {
            return Ok(Some(TargetState {
                token,
                node,
                observation,
            }));
        }
        next = node.right_sibling().map(node_token).transpose()?;
    }
    Err(TransError::other(
        "merge target search exceeded the right-link hop bound",
    ))
}

/// Reports whether `left` is underfull and whether its merge with `right`
/// stays at or below half of the threshold of every split cause, so that
/// the merged node does not split again soon (ADR-073). The merge compacts
/// both nodes, so their holder-free tombstones do not count.
fn merge_need(ctx: &ChangeContext, left: &Node, right: &Node) -> bool {
    let (mut left, mut right) = (left.clone(), right.clone());
    reclaim_holder_free_tombstones(&mut left);
    reclaim_holder_free_tombstones(&mut right);
    let policy = ctx.candidates.policy();
    let fits_bytes = left.content_encoded_len() + right.content_encoded_len()
        <= policy.node_soft_max_bytes().min(policy.content_limit()) / 2;
    match (left.body(), right.body()) {
        (NodeBody::Leaf(left), NodeBody::Leaf(right)) => {
            let inline_len =
                |leaf: &LeafBody| -> usize { leaf.entries().map(|e| e.current.inline_len()).sum() };
            left.entries().filter(|entry| entry.exists()).count() < policy.leaf_min_entries()
                && left.len() + right.len() <= policy.leaf_max_entries() / 2
                && inline_len(left) + inline_len(right)
                    <= ctx.candidates.inline().max_leaf_bytes / 2
                && fits_bytes
        }
        (NodeBody::Index(left), NodeBody::Index(right)) => {
            left.len() < policy.index_min_children()
                && left.len() + right.len() <= policy.index_max_children() / 2
                && fits_bytes
        }
        _ => false,
    }
}

/// Reports whether a holder of any claim on `node` can still be live.
async fn has_live_holder(ctx: &ChangeContext, node: &Node) -> Result<bool, TransError> {
    let entry_holders = node
        .as_leaf()
        .into_iter()
        .flat_map(LeafBody::entries)
        .flat_map(|entry| entry.lock_holders());
    let holders = node
        .structural_gate()
        .holders()
        .iter()
        .chain(node.membership_lock().holders())
        .chain(node.drop_intent())
        .chain(entry_holders);
    for holder in holders {
        if !ctx.mon.tx_status(holder).await?.is_final() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Compacts the merge target, adds the entries of the gated `left` to it,
/// and installs the merge reservation of `intent`. Returns the writers of
/// the reclaimed tombstones, or `None` when the merge must stop, with no
/// absorb CAS that can still land.
async fn absorb(
    ctx: &ChangeContext,
    collection: &CollectionAddress,
    merge: &MergeTarget,
    left: &Node,
    intent: &StructuralIntentId,
    target: TargetState,
) -> Result<Option<Vec<TxId>>, TransError> {
    let policy = ctx.candidates.policy();
    let mut current = (target.node, target.observation);
    for attempt in 0..NODE_CAS_ATTEMPTS {
        if attempt > 0 {
            current = ctx
                .nodes
                .load_node(collection, &merge.token, Requirement::ANY)
                .await?;
        }
        let (mut right, observation) = current.clone();
        // Recovery advances the generation to fence this absorb after it
        // deleted the intent.
        if right.is_drained()
            || right.membership_generation() != merge.generation
            || right.drop_intent().is_some()
            || right.locks().merge_reservation().is_some()
        {
            return Ok(None);
        }
        if let Some(holder) = right.structural_gate().holders().first().cloned() {
            if !ctx.mon.tx_status(&holder).await?.is_final() {
                return Ok(None);
            }
            right.remove_structural_gate(&holder);
        }
        // The CAS expects this exact state of R, so it removes only
        // tombstones that have no holder when it lands. R's gate is not
        // necessary for that (ADR-073).
        let reclaimed = reclaim_holder_free_tombstones(&mut right);
        right.absorb(left, intent.clone())?;
        if right.content_encoded_len() > policy.content_limit()
            || right.encoded_len() > policy.node_max_bytes()
        {
            return Ok(None);
        }
        if ctx
            .structural_nodes
            .store_structural_node(&right, &observation)
            .await?
            .is_some()
        {
            return Ok(Some(reclaimed));
        }
    }
    Ok(None)
}

/// Releases the source gate of a Ready merge that did not take effect, and
/// deletes its intent.
async fn stop_ready_merge(
    ctx: &ChangeContext,
    collection: &CollectionAddress,
    source: &NodeToken,
    worker: &TxId,
    ready: ReadyIntent,
) -> ChangeAttemptOutcome {
    if let Err(error) = ctx
        .structural_nodes
        .release_structural_gate(collection, Some(source), worker)
        .await
    {
        return ChangeAttemptOutcome::recovery_required(ready, error);
    }
    finish_ready_without_merge(ctx, ready).await
}

/// Deletes a Ready merge intent whose merge did not take effect, and asks
/// for a later attempt.
async fn finish_ready_without_merge(
    ctx: &ChangeContext,
    ready: ReadyIntent,
) -> ChangeAttemptOutcome {
    let mut outcome = ctx.finish_ready_change(ready).await;
    outcome.result = outcome.result.and(Err(TransError::Retry));
    outcome
}
