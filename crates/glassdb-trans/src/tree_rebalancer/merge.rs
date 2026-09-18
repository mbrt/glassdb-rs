//! Recoverable merging of adjacent leaves into the retained left leaf.

use glassdb_data::{CollectionAddress, NodeToken, ObjectPath, StructuralIntentId, TxId};
use glassdb_storage::transaction::TxCommitStatus;
use glassdb_storage::{
    LeafBody, LeafObservation, LockType, MergeIntent, MergeIntentPhase, Node, NodeLocks, NodeStore,
    Observation, Requirement, SplitPolicy, StorageError, StructuralIntent, StructuralIntentStore,
    Timeline,
};

use crate::error::TransError;

use super::{MAX_RECONCILE_HOPS, PARENT_RETRIES, StructuralNodeAccess};

/// Owns merge publication and recovery of interrupted merges.
#[derive(Clone)]
pub(super) struct MergeProtocol {
    nodes: NodeStore,
    intents: StructuralIntentStore,
    structure: StructuralNodeAccess,
    timeline: Timeline,
    policy: SplitPolicy,
}

impl MergeProtocol {
    pub(super) fn new(
        nodes: NodeStore,
        intents: StructuralIntentStore,
        structure: StructuralNodeAccess,
        timeline: Timeline,
        policy: SplitPolicy,
    ) -> Self {
        Self {
            nodes,
            intents,
            structure,
            timeline,
            policy,
        }
    }

    /// Records a merge before its participant joins collection topology.
    pub(super) async fn prepare(
        &self,
        collection: &CollectionAddress,
        left: &NodeToken,
        right: &NodeToken,
        participant: &TxId,
    ) -> Result<Observation<StructuralIntent>, TransError> {
        if left == right {
            return Err(TransError::other("merge sources are the same leaf"));
        }
        let intent_id = StructuralIntentId::from(NodeToken::new_random());
        Ok(self
            .intents
            .write(
                collection.db_root_component(),
                &intent_id,
                &StructuralIntent::Merge(MergeIntent {
                    collection: collection.clone(),
                    participant_id: participant.clone(),
                    left_token: left.clone(),
                    left_version: String::new(),
                    right_token: right.clone(),
                    right_version: String::new(),
                    phase: MergeIntentPhase::Preparing,
                }),
            )
            .await?)
    }

    /// Records the source snapshots eligible for a merge.
    pub(super) async fn ready(
        &self,
        prepared: &Observation<StructuralIntent>,
        left: &LeafObservation,
        right: &LeafObservation,
    ) -> Result<Option<Observation<StructuralIntent>>, TransError> {
        let (intent, id) = merge_intent(prepared)?;
        let mut intent = intent.clone();
        if intent.phase != MergeIntentPhase::Preparing {
            return Err(TransError::other("merge is not Preparing"));
        }
        if left.path() != &source_path(&intent, &intent.left_token)
            || right.path() != &source_path(&intent, &intent.right_token)
        {
            return Err(TransError::other("merge source observation path changed"));
        }
        let (Some(left_node), Some(right_node)) = (left.value(), right.value()) else {
            return Ok(None);
        };
        if left_node.structural_gate().holder().is_some()
            || right_node.structural_gate().intent().is_some()
            || !has_owned_gate(right_node, &intent.participant_id)
            || !self
                .sources_are_adjacent(&intent.collection, left_node, &intent.right_token)
                .await?
        {
            return Ok(None);
        }
        let Some(merged) = self.merged_node(&intent, id, left_node, right_node)? else {
            return Ok(None);
        };
        let claimed_right = bind_gate(right_node, &intent, id)?;
        if merged
            .as_leaf()
            .is_none_or(|leaf| leaf.len() >= self.policy.leaf_max_entries())
            || merged.content_encoded_len() >= self.policy.node_soft_max_bytes()
            || merged.content_encoded_len() >= self.policy.content_limit()
            || merged.encoded_len() > self.policy.node_max_bytes()
            || claimed_right.encoded_len() > self.policy.node_max_bytes()
        {
            return Ok(None);
        }
        let (Some(left_revision), Some(right_revision)) = (left.revision(), right.revision())
        else {
            return Ok(None);
        };
        intent.left_version = left_revision.serialize().to_string();
        intent.right_version = right_revision.serialize().to_string();
        intent.phase = MergeIntentPhase::Ready;
        Ok(self
            .intents
            .update(prepared, &StructuralIntent::Merge(intent))
            .await?)
    }

    /// Completes or cancels one merge without replacing later node revisions.
    pub(super) async fn recover(
        &self,
        discovered: &Observation<StructuralIntent>,
    ) -> Result<(), TransError> {
        // Callers can retain a completed intent across later changes to the tree.
        let mut observed = self.load_intent(discovered.path()).await?;
        for _ in 0..PARENT_RETRIES {
            if observed.is_absent() {
                return Ok(());
            }
            let (intent, id) = merge_intent(&observed)?;
            let intent = intent.clone();
            let id = id.clone();
            match intent.phase {
                MergeIntentPhase::Preparing => {
                    if self.structure.mon.tx_status(&intent.participant_id).await?
                        == TxCommitStatus::Pending
                    {
                        return Err(TransError::Retry);
                    }
                    // Deleting this exact revision prevents Ready. Preparing
                    // workers can install only ordinary, reclaimable gates.
                    return self.delete_completed(&observed).await;
                }
                MergeIntentPhase::Ready => {
                    let mut next = intent.clone();
                    next.phase = if self.publish_left(&intent, &id).await? {
                        MergeIntentPhase::Applying
                    } else {
                        MergeIntentPhase::Aborting
                    };
                    if let Some(next) = self
                        .intents
                        .update(&observed, &StructuralIntent::Merge(next))
                        .await?
                    {
                        observed = next;
                        continue;
                    }
                }
                MergeIntentPhase::Aborting => {
                    self.release_right(&intent, &id).await?;
                    return self.delete_completed(&observed).await;
                }
                MergeIntentPhase::Applying => {
                    self.redirect_right(&intent, &id).await?;
                    self.release_left(&intent, &id).await?;
                    return self.delete_completed(&observed).await;
                }
            }
            observed = self.load_intent(observed.path()).await?;
        }
        Err(TransError::Retry)
    }

    /// Checks adjacency through permanent retired-node forwards.
    pub(super) async fn sources_are_adjacent(
        &self,
        collection: &CollectionAddress,
        left: &Node,
        right: &NodeToken,
    ) -> Result<bool, TransError> {
        let Some(next) = left.right_sibling() else {
            return Ok(false);
        };
        let mut next = super::node_token(next)?;
        let requirement = self.current_requirement();
        for _ in 0..MAX_RECONCILE_HOPS {
            if &next == right {
                return Ok(true);
            }
            let observed = self
                .nodes
                .load_node_at_state(
                    &ObjectPath::Node {
                        collection: collection.clone(),
                        token: next,
                    },
                    requirement,
                )
                .await?;
            let Some(target) = observed.value().and_then(|node| node.forwarding_target()) else {
                return Ok(false);
            };
            // Retired identities are permanent, so these links cannot change
            // the adjacency proof for these source snapshots.
            next = super::node_token(target)?;
        }
        Ok(false)
    }

    async fn load_intent(
        &self,
        path: &ObjectPath,
    ) -> Result<Observation<StructuralIntent>, TransError> {
        Ok(self.intents.load(path, self.current_requirement()).await?)
    }

    async fn delete_completed(
        &self,
        observed: &Observation<StructuralIntent>,
    ) -> Result<(), TransError> {
        match self.intents.delete(observed).await {
            Ok(()) => Ok(()),
            Err(StorageError::Precondition) => Err(TransError::Retry),
            Err(error) => Err(error.into()),
        }
    }

    /// Publishes the union, or fences its recorded revision before cancellation.
    async fn publish_left(
        &self,
        intent: &MergeIntent,
        id: &StructuralIntentId,
    ) -> Result<bool, TransError> {
        let requirement = self.current_requirement();
        let left_path = source_path(intent, &intent.left_token);
        let right_path = source_path(intent, &intent.right_token);
        let (left, right) = tokio::try_join!(
            self.nodes.load_node_at_state(&left_path, requirement),
            self.nodes.load_node_at_state(&right_path, requirement),
        )?;
        // The publication CAS changes the revision and installs this gate in
        // one step. Test its proof before classifying a changed revision.
        if left
            .value()
            .is_some_and(|node| has_intent_gate(node, intent, id))
        {
            return Ok(true);
        }
        if !has_revision(&left, &intent.left_version) {
            return self.fence_left(intent, id).await;
        }
        let Some(right_node) = right.value() else {
            return self.fence_left(intent, id).await;
        };
        let claimed = if has_intent_gate(right_node, intent, id) {
            right_node.as_ref().clone()
        } else if has_revision(&right, &intent.right_version) {
            let claimed = bind_gate(right_node, intent, id)?;
            if self
                .structure
                .store_structural_node(&claimed, &right)
                .await?
                .is_none()
            {
                return self.fence_left(intent, id).await;
            }
            claimed
        } else {
            return self.fence_left(intent, id).await;
        };
        let merged = self
            .merged_node(intent, id, left.value().ok_or(TransError::Retry)?, &claimed)?
            .ok_or_else(|| TransError::other("Ready merge has invalid sources"))?;
        // Neither claim nor publication can rebase onto a later source revision.
        if self
            .structure
            .store_structural_node(&merged, &left)
            .await?
            .is_some()
        {
            return Ok(true);
        }
        self.fence_left(intent, id).await
    }

    async fn fence_left(
        &self,
        intent: &MergeIntent,
        id: &StructuralIntentId,
    ) -> Result<bool, TransError> {
        for _ in 0..PARENT_RETRIES {
            let observed = self
                .nodes
                .load_node_at_state(
                    &source_path(intent, &intent.left_token),
                    self.current_requirement(),
                )
                .await?;
            let Some(node) = observed.value() else {
                return Ok(false);
            };
            if has_intent_gate(node, intent, id) {
                return Ok(true);
            }
            // Ready recorded an ungated leaf. A later gate advances its
            // generation permanently; retired node identities are permanent.
            if node.structural_gate().holder().is_some() || node.forwarding_target().is_some() {
                return Ok(false);
            }
            let mut fenced = node.as_ref().clone();
            let mut locks = fenced.locks().clone();
            locks.advance_membership_version();
            fenced.set_locks(locks);
            // Temporary holders can change and then restore a content-based
            // revision. Preserve them, but advance the generation so a delayed
            // union CAS cannot succeed after recovery reopens the right leaf.
            if self
                .structure
                .store_structural_node(&fenced, &observed)
                .await?
                .is_some()
            {
                return Ok(false);
            }
        }
        Err(TransError::Retry)
    }

    async fn release_right(
        &self,
        intent: &MergeIntent,
        id: &StructuralIntentId,
    ) -> Result<(), TransError> {
        for _ in 0..PARENT_RETRIES {
            let observed = self
                .nodes
                .load_node_at_state(
                    &source_path(intent, &intent.right_token),
                    self.current_requirement(),
                )
                .await?;
            let Some(node) = observed.value() else {
                return Ok(());
            };
            if !has_intent_gate(node, intent, id) && !has_revision(&observed, &intent.right_version)
            {
                return Ok(());
            }
            let mut released = node.as_ref().clone();
            let mut locks = released.locks().clone();
            locks.complete_structural_intent(&intent.participant_id, id);
            locks.remove_structural_gate(&intent.participant_id);
            released.set_locks(locks);
            // Fence even an unclaimed recorded revision: a delayed Ready
            // worker may already have sent its right-gate promotion.
            if self
                .structure
                .store_structural_node(&released, &observed)
                .await?
                .is_some()
            {
                return Ok(());
            }
        }
        Err(TransError::Retry)
    }

    async fn redirect_right(
        &self,
        intent: &MergeIntent,
        id: &StructuralIntentId,
    ) -> Result<(), TransError> {
        for _ in 0..PARENT_RETRIES {
            let observed = self
                .nodes
                .load_node_at_state(
                    &source_path(intent, &intent.right_token),
                    self.current_requirement(),
                )
                .await?;
            let node = observed.value().ok_or(TransError::Retry)?;
            if node.forwarding_target() == Some(intent.left_token.as_str()) {
                return Ok(());
            }
            if !has_intent_gate(node, intent, id) {
                return Err(TransError::Retry);
            }
            if self
                .structure
                .store_structural_node(&Node::forward(intent.left_token.to_string()), &observed)
                .await?
                .is_some()
            {
                return Ok(());
            }
        }
        Err(TransError::Retry)
    }

    async fn release_left(
        &self,
        intent: &MergeIntent,
        id: &StructuralIntentId,
    ) -> Result<(), TransError> {
        for _ in 0..PARENT_RETRIES {
            let observed = self
                .nodes
                .load_node_at_state(
                    &source_path(intent, &intent.left_token),
                    self.current_requirement(),
                )
                .await?;
            let Some(node) = observed.value() else {
                return Ok(());
            };
            let mut locks = node.locks().clone();
            if !locks.complete_structural_intent(&intent.participant_id, id) {
                return Ok(());
            }
            let mut released = node.as_ref().clone();
            released.set_locks(locks);
            // Applying and the right redirect are durable before this release.
            // A stale helper must preserve any later left writes or splits.
            if self
                .structure
                .store_structural_node(&released, &observed)
                .await?
                .is_some()
            {
                return Ok(());
            }
        }
        Err(TransError::Retry)
    }

    fn merged_node(
        &self,
        intent: &MergeIntent,
        id: &StructuralIntentId,
        left: &Node,
        right: &Node,
    ) -> Result<Option<Node>, TransError> {
        let (Some(left_leaf), Some(right_leaf)) = (left.as_leaf(), right.as_leaf()) else {
            return Ok(None);
        };
        let Some(boundary) = left.high_key() else {
            return Ok(None);
        };
        if right.high_key().is_some_and(|high| high <= boundary)
            || !source_is_quiescent(left)
            || !source_is_quiescent(right)
            || left_leaf
                .entries()
                .any(|entry| entry.key.as_slice() >= boundary)
            || right_leaf
                .entries()
                .any(|entry| entry.key.as_slice() < boundary || !right.covers(&entry.key))
            || left.membership_version().max(right.membership_version()) == u64::MAX
        {
            return Ok(None);
        }
        let mut merged = Node::leaf(LeafBody::from_entries(
            left_leaf.entries().chain(right_leaf.entries()).cloned(),
        ))
        .with_high_key(right.high_key().map(<[u8]>::to_vec))
        .with_right_sibling(right.right_sibling().map(str::to_owned));
        let mut locks = NodeLocks::default();
        locks.set_structural_gate(intent.participant_id.clone());
        locks
            .set_merged_membership_version(left.membership_version(), right.membership_version())?;
        locks.bind_structural_intent(&intent.participant_id, id.clone())?;
        merged.set_locks(locks);
        Ok(Some(merged))
    }

    fn current_requirement(&self) -> Requirement {
        Requirement::after(self.timeline.currentness_barrier())
    }
}

fn merge_intent(
    observed: &Observation<StructuralIntent>,
) -> Result<(&MergeIntent, &StructuralIntentId), TransError> {
    let StructuralIntent::Merge(intent) = observed.value().ok_or(TransError::Retry)?.as_ref()
    else {
        return Err(TransError::other("expected a merge intent"));
    };
    let ObjectPath::StructuralIntent { intent_id, .. } = observed.path() else {
        return Err(TransError::other("invalid merge intent path"));
    };
    Ok((intent, intent_id))
}

fn source_path(intent: &MergeIntent, token: &NodeToken) -> ObjectPath {
    ObjectPath::Node {
        collection: intent.collection.clone(),
        token: token.clone(),
    }
}

fn has_revision(observed: &LeafObservation, version: &str) -> bool {
    observed
        .revision()
        .is_some_and(|revision| revision.serialize() == version)
}

fn source_is_quiescent(node: &Node) -> bool {
    node.membership_lock().holders().is_empty()
        && node.collection_delete_intent().is_none()
        && node
            .as_leaf()
            .is_some_and(|leaf| leaf.entries().all(|entry| entry.lock_holders().is_empty()))
}

fn has_owned_gate(node: &Node, owner: &TxId) -> bool {
    node.structural_gate().lock_type() == LockType::Write && node.structural_gate().contains(owner)
}

fn has_intent_gate(node: &Node, intent: &MergeIntent, id: &StructuralIntentId) -> bool {
    has_owned_gate(node, &intent.participant_id) && node.structural_gate().intent() == Some(id)
}

fn bind_gate(
    node: &Node,
    intent: &MergeIntent,
    id: &StructuralIntentId,
) -> Result<Node, TransError> {
    let mut claimed = node.clone();
    let mut locks = claimed.locks().clone();
    locks.bind_structural_intent(&intent.participant_id, id.clone())?;
    claimed.set_locks(locks);
    Ok(claimed)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{BackendOp, HookBackend, HookFuture};
    use glassdb_backend::{Backend, BackendError};
    use glassdb_concurr::RetryConfig;
    use glassdb_data::DbRoot;
    use glassdb_storage::{CurrentState, IndexNode, LeafEntry};

    use crate::engine::{AssemblyFixture, EngineConfig};
    use crate::key_state_resolver::KeyStateResolver;
    use crate::leaf_coord::{LeafCoordinator, SplitHinter};

    use super::*;

    struct NoHints;

    impl SplitHinter for NoHints {
        fn observe_leaf(&self, _path: &ObjectPath, _leaf: &LeafBody) {}
    }

    struct Fixture {
        base: AssemblyFixture,
        protocol: MergeProtocol,
    }

    impl Fixture {
        fn new(backend: Arc<dyn Backend>, policy: SplitPolicy) -> Self {
            let base = AssemblyFixture::new(
                backend,
                DbRoot::try_from("db").unwrap(),
                &EngineConfig::default(),
            );
            let key_state = KeyStateResolver::new(base.monitor.clone());
            let gate_retry = crate::node_locking::StructuralGateRetry::new(
                base.timeline.clone(),
                Arc::default(),
            );
            let coord = LeafCoordinator::with_hinter(
                base.nodes.clone(),
                key_state.clone(),
                base.monitor.clone(),
                gate_retry.clone(),
                RetryConfig::default(),
                policy,
                Arc::new(NoHints),
            );
            let structure = StructuralNodeAccess::new(
                base.nodes.clone(),
                base.monitor.clone(),
                key_state,
                gate_retry,
                coord,
            );
            let protocol = MergeProtocol::new(
                base.nodes.clone(),
                base.structural_intents.clone(),
                structure,
                base.timeline.clone(),
                policy,
            );
            Self { base, protocol }
        }

        async fn seed(&self) -> (LeafObservation, LeafObservation) {
            let writer = TxId::with_priority(1, b"committed");
            let left = Node::leaf(LeafBody::from_entries([
                LeafEntry::new(b"a").with_current(CurrentState::Inline {
                    writer: writer.clone(),
                    value: Arc::from(b"left value".as_slice()),
                }),
                LeafEntry::new(b"b").with_current(CurrentState::Tombstone {
                    writer: writer.clone(),
                }),
            ]))
            .with_high_key(Some(b"m".to_vec()))
            .with_right_sibling(Some(token(2).to_string()));
            let mut right = Node::leaf(LeafBody::from_entries([LeafEntry::new(b"z")
                .with_current(CurrentState::Inline {
                    writer,
                    value: Arc::from(b"right value".as_slice()),
                })]));
            right.set_structural_gate(owner());
            let mut locks = right.locks().clone();
            locks.advance_membership_version();
            locks.advance_membership_version();
            right.set_locks(locks);
            for (token, node) in [(token(1), left), (token(2), right)] {
                assert!(
                    self.base
                        .nodes
                        .store_node(&collection(), &token, &node, None)
                        .await
                        .unwrap()
                );
            }
            let root = Node::index(IndexNode::from_children([
                (Vec::new(), token(1).to_string()),
                (b"m".to_vec(), token(2).to_string()),
            ]));
            assert!(
                self.base
                    .nodes
                    .create_root(&collection(), &root)
                    .await
                    .unwrap()
            );
            (self.source(&token(1)).await, self.source(&token(2)).await)
        }

        async fn ready(&self) -> Observation<StructuralIntent> {
            let (left, right) = self.seed().await;
            let preparing = self
                .protocol
                .prepare(&collection(), &token(1), &token(2), &owner())
                .await
                .unwrap();
            self.protocol
                .ready(&preparing, &left, &right)
                .await
                .unwrap()
                .unwrap()
        }

        async fn source(&self, token: &NodeToken) -> LeafObservation {
            self.base
                .nodes
                .load_node_state(&collection(), token, self.protocol.current_requirement())
                .await
                .unwrap()
        }
    }

    fn collection() -> CollectionAddress {
        CollectionAddress::root("db")
    }

    fn token(value: u8) -> NodeToken {
        NodeToken::from_bytes([value; 16])
    }

    fn owner() -> TxId {
        TxId::with_priority(2, b"merge owner")
    }

    fn merge_step(operation: &BackendOp<'_>) -> Option<usize> {
        match operation {
            BackendOp::WriteIf { path, value, .. } if path.contains("/_s/") => {
                StructuralIntent::decode(value)
                    .ok()
                    .filter(|intent| {
                        matches!(
                            intent,
                            StructuralIntent::Merge(MergeIntent {
                                phase: MergeIntentPhase::Applying,
                                ..
                            })
                        )
                    })
                    .map(|_| 2)
            }
            BackendOp::WriteIf { path, value, .. } if path.contains("/_n/") => {
                let node = Node::decode(value).ok()?;
                if path.ends_with(token(2).as_str()) {
                    if node.forwarding_target().is_some() {
                        Some(3)
                    } else if node.structural_gate().intent().is_some() {
                        Some(0)
                    } else {
                        None
                    }
                } else if path.ends_with(token(1).as_str()) {
                    if node.structural_gate().intent().is_some() {
                        Some(1)
                    } else {
                        Some(4)
                    }
                } else {
                    None
                }
            }
            BackendOp::DeleteIf { path, .. } if path.contains("/_s/") => Some(5),
            _ => None,
        }
    }

    async fn assert_merged(fixture: &Fixture) {
        let merged = fixture.source(&token(1)).await;
        let node = merged.value().unwrap();
        let leaf = node.as_leaf().unwrap();
        assert_eq!(leaf.len(), 3);
        assert_eq!(
            leaf.lookup(b"a")
                .unwrap()
                .current
                .inline()
                .unwrap()
                .as_ref(),
            b"left value"
        );
        assert_eq!(
            leaf.lookup(b"z")
                .unwrap()
                .current
                .inline()
                .unwrap()
                .as_ref(),
            b"right value"
        );
        assert!(leaf.lookup(b"b").unwrap().current.is_tombstone());
        assert_eq!(node.membership_version(), 4);
        assert!(node.structural_gate().holder().is_none());
        assert_eq!(
            fixture
                .source(&token(2))
                .await
                .value()
                .unwrap()
                .forwarding_target(),
            Some(token(1).as_str())
        );
        assert_eq!(
            fixture
                .base
                .nodes
                .list_nodes(&collection(), fixture.protocol.current_requirement())
                .await
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn recovery_preserves_values_after_each_failed_or_unacknowledged_step() {
        for stop in 0..6 {
            for landed in [false, true] {
                let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
                let fixture = Fixture::new(backend.clone(), SplitPolicy::default());
                let ready = fixture.ready().await;
                if landed {
                    backend.set_after(move |operation, outcome| {
                        let fail = outcome.is_success() && merge_step(operation) == Some(stop);
                        let future: HookFuture = Box::pin(async move {
                            if fail {
                                Err(BackendError::other("merge reply lost"))
                            } else {
                                Ok(())
                            }
                        });
                        future
                    });
                } else {
                    backend.set_before(move |operation| {
                        let fail = merge_step(operation) == Some(stop);
                        let future: HookFuture = Box::pin(async move {
                            if fail {
                                Err(BackendError::other("merge interrupted"))
                            } else {
                                Ok(())
                            }
                        });
                        future
                    });
                }
                assert!(
                    fixture.protocol.recover(&ready).await.is_err(),
                    "step {stop}, landed {landed}"
                );
                backend.clear_before();
                backend.clear_after();

                // A recorded decision does not depend on a helper's local policy.
                let restrictive = SplitPolicy::builder().leaf_max_entries(1).build().unwrap();
                let peer = Fixture::new(backend, restrictive);
                peer.protocol.recover(&ready).await.unwrap();
                assert_merged(&peer).await;
                assert!(
                    peer.base
                        .structural_intents
                        .load(ready.path(), peer.protocol.current_requirement())
                        .await
                        .unwrap()
                        .is_absent()
                );

                // A retained intent cannot recreate a leaf after later reclamation.
                let merged = peer.source(&token(1)).await;
                peer.base.nodes.delete_node(&merged).await.unwrap();
                fixture.protocol.recover(&ready).await.unwrap();
                assert!(matches!(
                    peer.base
                        .nodes
                        .load_node_state(
                            &collection(),
                            &token(1),
                            peer.protocol.current_requirement()
                        )
                        .await,
                    Err(StorageError::NotFound)
                ));
            }
        }
    }

    #[tokio::test]
    async fn cancellation_fences_delayed_right_claim_and_left_publication() {
        use std::sync::atomic::{AtomicBool, Ordering};
        for stop in [0, 1] {
            let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
            let fixture = Fixture::new(backend.clone(), SplitPolicy::default());
            let ready = fixture.ready().await;
            let entered = Arc::new(tokio::sync::Notify::new());
            let resume = Arc::new(tokio::sync::Notify::new());
            let paused = Arc::new(AtomicBool::new(false));
            backend.set_before({
                let entered = entered.clone();
                let resume = resume.clone();
                move |operation| {
                    let pause =
                        merge_step(operation) == Some(stop) && !paused.swap(true, Ordering::SeqCst);
                    let entered = entered.clone();
                    let resume = resume.clone();
                    let future: HookFuture = Box::pin(async move {
                        if pause {
                            entered.notify_one();
                            resume.notified().await;
                        }
                        Ok(())
                    });
                    future
                }
            });
            let worker = tokio::spawn({
                let protocol = fixture.protocol.clone();
                let ready = ready.clone();
                async move { protocol.recover(&ready).await }
            });
            tokio::time::timeout(Duration::from_secs(5), entered.notified())
                .await
                .expect("merge must reach its delayed write");

            let peer = Fixture::new(backend.clone(), SplitPolicy::default());
            let left = peer.source(&token(1)).await;
            let mut changed = left.value().unwrap().as_ref().clone();
            assert!(changed.structural_gate().holder().is_none());
            let replacement = CurrentState::Inline {
                writer: TxId::with_priority(3, b"concurrent writer"),
                value: Arc::from(b"updated left".as_slice()),
            };
            changed
                .set_leaf(LeafBody::from_entries([
                    LeafEntry::new(b"a").with_current(replacement.clone()),
                    changed.as_leaf().unwrap().lookup(b"b").unwrap().clone(),
                ]))
                .unwrap();
            assert!(
                peer.base
                    .nodes
                    .store_node_at(left.path(), &changed, &left)
                    .await
                    .unwrap()
                    .is_some()
            );
            peer.protocol.recover(&ready).await.unwrap();
            resume.notify_one();
            tokio::time::timeout(Duration::from_secs(5), worker)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            backend.clear_before();
            assert_eq!(
                peer.source(&token(1))
                    .await
                    .value()
                    .unwrap()
                    .as_leaf()
                    .unwrap()
                    .lookup(b"a")
                    .unwrap()
                    .current,
                replacement
            );

            for (token, keys) in [
                (token(1), vec![b"a".as_slice(), b"b"]),
                (token(2), vec![b"z".as_slice()]),
            ] {
                let current = peer.source(&token).await;
                let node = current.value().unwrap();
                assert_eq!(
                    node.as_leaf()
                        .unwrap()
                        .entries()
                        .map(|entry| entry.key.as_slice())
                        .collect::<Vec<_>>(),
                    keys
                );
                assert!(node.structural_gate().holder().is_none());
            }
            assert!(
                peer.base
                    .structural_intents
                    .load(ready.path(), peer.protocol.current_requirement())
                    .await
                    .unwrap()
                    .is_absent()
            );
        }
    }

    #[tokio::test]
    async fn cancellation_retries_a_failed_or_unacknowledged_fence_without_removing_holders() {
        for landed in [false, true] {
            let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
            let fixture = Fixture::new(backend.clone(), SplitPolicy::default());
            let ready = fixture.ready().await;
            let left = fixture.source(&token(1)).await;
            let reader = TxId::with_priority(3, b"concurrent scan");
            let mut held = left.value().unwrap().as_ref().clone();
            held.add_membership_reader(reader.clone());
            assert!(
                fixture
                    .base
                    .nodes
                    .store_node_at(left.path(), &held, &left)
                    .await
                    .unwrap()
                    .is_some()
            );
            if landed {
                backend.set_after(|operation, outcome| {
                    let fail = outcome.is_success() && merge_step(operation) == Some(4);
                    let future: HookFuture = Box::pin(async move {
                        if fail {
                            Err(BackendError::other("cancellation fence reply lost"))
                        } else {
                            Ok(())
                        }
                    });
                    future
                });
            } else {
                backend.set_before(|operation| {
                    let fail = merge_step(operation) == Some(4);
                    let future: HookFuture = Box::pin(async move {
                        if fail {
                            Err(BackendError::other("cancellation fence interrupted"))
                        } else {
                            Ok(())
                        }
                    });
                    future
                });
            }
            assert!(fixture.protocol.recover(&ready).await.is_err());
            assert!(
                fixture
                    .source(&token(2))
                    .await
                    .value()
                    .unwrap()
                    .structural_gate()
                    .contains(&owner())
            );
            let current = fixture
                .base
                .structural_intents
                .load(ready.path(), fixture.protocol.current_requirement())
                .await
                .unwrap();
            assert!(matches!(
                current.value().unwrap().as_ref(),
                StructuralIntent::Merge(MergeIntent {
                    phase: MergeIntentPhase::Ready,
                    ..
                })
            ));
            backend.clear_before();
            backend.clear_after();

            let peer = Fixture::new(backend, SplitPolicy::default());
            peer.protocol.recover(&ready).await.unwrap();
            let left = peer.source(&token(1)).await;
            let left = left.value().unwrap();
            assert_eq!(left.as_leaf(), held.as_leaf());
            assert_eq!(left.membership_lock().holders(), &[reader]);
            assert!(left.structural_gate().holder().is_none());
            assert!(
                peer.source(&token(2))
                    .await
                    .value()
                    .unwrap()
                    .structural_gate()
                    .holder()
                    .is_none()
            );
            assert!(
                peer.base
                    .structural_intents
                    .load(ready.path(), peer.protocol.current_requirement())
                    .await
                    .unwrap()
                    .is_absent()
            );
        }
    }

    #[tokio::test]
    async fn published_left_survives_ordinary_gate_cleanup_and_owner_finalization() {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let fixture = Fixture::new(backend.clone(), SplitPolicy::default());
        let ready = fixture.ready().await;
        backend.set_before(|operation| {
            let fail = merge_step(operation) == Some(2);
            let future: HookFuture = Box::pin(async move {
                if fail {
                    Err(BackendError::other("intent phase write interrupted"))
                } else {
                    Ok(())
                }
            });
            future
        });
        assert!(fixture.protocol.recover(&ready).await.is_err());
        fixture.base.monitor.begin_tx(&owner());
        fixture
            .base
            .monitor
            .commit_tx(glassdb_storage::transaction::TxLog::new(
                owner(),
                TxCommitStatus::Aborted,
            ))
            .await
            .unwrap();
        for source in [token(1), token(2)] {
            fixture
                .protocol
                .structure
                .release_structural_gate(&collection(), Some(&source), &owner())
                .await
                .unwrap();
            assert!(
                fixture
                    .source(&source)
                    .await
                    .value()
                    .unwrap()
                    .structural_gate()
                    .intent()
                    .is_some()
            );
        }
        // The union is readable while its gate prevents independent mutation.
        assert_eq!(
            fixture
                .source(&token(1))
                .await
                .value()
                .unwrap()
                .as_leaf()
                .unwrap()
                .len(),
            3
        );
        backend.clear_before();
        let peer = Fixture::new(backend, SplitPolicy::default());
        peer.protocol.recover(&ready).await.unwrap();
        assert_merged(&peer).await;
    }

    #[tokio::test]
    async fn stale_applying_helper_preserves_a_later_left_write() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let fixture = Fixture::new(backend.clone(), SplitPolicy::default());
        let ready = fixture.ready().await;
        let entered = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let paused = Arc::new(AtomicBool::new(false));
        backend.set_before({
            let entered = entered.clone();
            let resume = resume.clone();
            move |operation| {
                let pause =
                    merge_step(operation) == Some(4) && !paused.swap(true, Ordering::SeqCst);
                let entered = entered.clone();
                let resume = resume.clone();
                let future: HookFuture = Box::pin(async move {
                    if pause {
                        entered.notify_one();
                        resume.notified().await;
                    }
                    Ok(())
                });
                future
            }
        });
        let worker = tokio::spawn({
            let protocol = fixture.protocol.clone();
            let ready = ready.clone();
            async move { protocol.recover(&ready).await }
        });
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("merge must reach its delayed release");
        let peer = Fixture::new(backend.clone(), SplitPolicy::default());
        peer.protocol.recover(&ready).await.unwrap();
        let left = peer.source(&token(1)).await;
        let mut changed = left.value().unwrap().as_ref().clone();
        let later = LeafEntry::new(b"a").with_current(CurrentState::Inline {
            writer: TxId::with_priority(3, b"later"),
            value: b"later value".as_slice().into(),
        });
        changed
            .set_leaf(LeafBody::from_entries(
                changed
                    .as_leaf()
                    .unwrap()
                    .entries()
                    .filter(|entry| entry.key.as_slice() != b"a")
                    .cloned()
                    .chain([later.clone()]),
            ))
            .unwrap();
        assert!(
            peer.base
                .nodes
                .store_node_at(left.path(), &changed, &left)
                .await
                .unwrap()
                .is_some()
        );
        resume.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_ok() || matches!(result, Err(TransError::Retry)));
        backend.clear_before();
        assert_eq!(
            peer.source(&token(1))
                .await
                .value()
                .unwrap()
                .as_leaf()
                .unwrap()
                .lookup(b"a"),
            Some(&later)
        );
    }

    #[tokio::test]
    async fn changed_source_cancels_without_rebasing_or_losing_its_write() {
        let fixture = Fixture::new(Arc::new(MemoryBackend::new()), SplitPolicy::default());
        let ready = fixture.ready().await;
        let right = fixture.source(&token(2)).await;
        let mut changed = right.value().unwrap().as_ref().clone();
        changed.remove_structural_gate(&owner());
        let replacement = CurrentState::Inline {
            writer: TxId::with_priority(3, b"later writer"),
            value: Arc::from(b"new value".as_slice()),
        };
        changed
            .set_leaf(LeafBody::from_entries([
                LeafEntry::new(b"z").with_current(replacement.clone())
            ]))
            .unwrap();
        assert!(
            fixture
                .base
                .nodes
                .store_node_at(right.path(), &changed, &right)
                .await
                .unwrap()
                .is_some()
        );
        fixture.protocol.recover(&ready).await.unwrap();
        assert_eq!(
            fixture
                .source(&token(2))
                .await
                .value()
                .unwrap()
                .as_leaf()
                .unwrap()
                .lookup(b"z")
                .unwrap()
                .current,
            replacement
        );
        for source in [token(1), token(2)] {
            let current = fixture.source(&source).await;
            assert!(
                current
                    .value()
                    .unwrap()
                    .structural_gate()
                    .holder()
                    .is_none()
            );
            assert!(current.value().unwrap().as_leaf().is_some());
        }
    }

    #[tokio::test]
    async fn ready_rejects_a_union_at_the_ordinary_split_limit() {
        let policy = SplitPolicy::builder().leaf_max_entries(3).build().unwrap();
        let fixture = Fixture::new(Arc::new(MemoryBackend::new()), policy);
        let (left, right) = fixture.seed().await;
        let preparing = fixture
            .protocol
            .prepare(&collection(), &token(1), &token(2), &owner())
            .await
            .unwrap();

        assert!(
            fixture
                .protocol
                .ready(&preparing, &left, &right)
                .await
                .unwrap()
                .is_none()
        );

        let current = fixture
            .base
            .structural_intents
            .load(preparing.path(), fixture.protocol.current_requirement())
            .await
            .unwrap();
        assert!(matches!(
            current.value().unwrap().as_ref(),
            StructuralIntent::Merge(MergeIntent {
                phase: MergeIntentPhase::Preparing,
                ..
            })
        ));
        for source in [token(1), token(2)] {
            assert!(
                fixture
                    .source(&source)
                    .await
                    .value()
                    .unwrap()
                    .structural_gate()
                    .intent()
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn ready_accepts_permanent_aliases_but_rejects_overlapping_keys() {
        for overlap in [false, true] {
            let fixture = Fixture::new(Arc::new(MemoryBackend::new()), SplitPolicy::default());
            let (left, right) = fixture.seed().await;
            let alias = token(3);
            assert!(
                fixture
                    .base
                    .nodes
                    .store_node(
                        &collection(),
                        &alias,
                        &Node::forward(token(2).to_string()),
                        None
                    )
                    .await
                    .unwrap()
            );
            let mut aliased = left
                .value()
                .unwrap()
                .as_ref()
                .clone()
                .with_right_sibling(Some(alias.to_string()));
            if overlap {
                aliased
                    .set_leaf(LeafBody::from_entries([LeafEntry::new(b"z")]))
                    .unwrap();
            }
            let left = fixture
                .base
                .nodes
                .store_node_at(left.path(), &aliased, &left)
                .await
                .unwrap()
                .unwrap();
            let preparing = fixture
                .protocol
                .prepare(&collection(), &token(1), &token(2), &owner())
                .await
                .unwrap();
            let ready = fixture
                .protocol
                .ready(&preparing, &left, &right)
                .await
                .unwrap();
            assert_eq!(ready.is_some(), !overlap);
        }
    }
}
