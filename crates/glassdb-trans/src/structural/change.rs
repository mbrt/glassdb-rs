//! The lifecycle and the steps that every structural change shares.
//!
//! A change first writes a `Preparing` structural intent below its topology
//! participant and joins the collection topology. The lifecycle then gates the
//! source, and the split or merge module checks the change again and plans its
//! node writes. After the lifecycle marks the intent `Ready`, the module makes
//! the writes, and the lifecycle reconciles the parent. The lifecycle deletes
//! the intent when the change completes, discards it when the change stops
//! before `Ready`, and leaves it to recovery otherwise.

use std::sync::Arc;

use glassdb_concurr::rt;
use glassdb_data::{CollectionAddress, NodeId, ObjectPath, TxId};
use glassdb_storage::{LeafObservation, Node, Requirement};
use tokio::sync::Notify;

use crate::error::TransError;
use crate::node_locking::GateAcquisition;

use super::merge::Merger;
use super::nodes::StructuralNodeAccess;
use super::reclamation::ReclamationReporter;
use super::reconcile::{ParentReconciler, ParentSplitContinuation, ReconciliationOutcome};
use super::recovery::{
    ChangeKind, PreparedIntent, PreparedIntentCancellation, ReadyChange, ReadyIntent,
    ReadyIntentCompletion, ReadyIntentTransition, StructuralRecovery,
};
use super::split::{SplitReason, SplitTarget, Splitter};
use super::topology::TopologyMembership;

/// Runs splits and merges through the structural-intent lifecycle, and
/// reconciles the parents that they change.
#[derive(Clone)]
pub(super) struct ChangeLifecycle {
    recovery: StructuralRecovery,
    pub(super) topology: TopologyMembership,
    pub(super) structural_nodes: StructuralNodeAccess,
    reconciler: ParentReconciler,
    reclamation: ReclamationReporter,
    // Wakes the independent recovery loop when a local change leaves `_s` work.
    recovery_wake: Arc<Notify>,
    splitter: Splitter,
    merger: Merger,
}

/// The structural change that one attempt makes.
#[derive(Clone, Copy)]
pub(super) enum PlannedChange<'a> {
    Split {
        collection: &'a CollectionAddress,
        target: SplitTarget<'a>,
        reason: &'a SplitReason,
    },
    Merge {
        collection: &'a CollectionAddress,
        source: &'a NodeId,
    },
}

/// Whether a change owns its topology participant, or runs beneath the
/// participant of another change.
#[derive(Clone, Copy)]
pub(super) enum StructuralTopology<'a> {
    Owned,
    Joined(&'a TxId),
}

/// The result of checking a change again while its source is gated.
pub(super) enum Prepared<P> {
    /// The change is not necessary, or the check failed.
    Cancel(Result<(), TransError>),
    /// Only the removal of holder-free tombstones is necessary. `node` is the
    /// compacted source, which still holds the gate.
    ReclaimOnly { node: Node, reclaimed: Vec<TxId> },
    /// The change is necessary. `change` goes into the Ready intent, and
    /// `plan` holds the node writes.
    Ready { change: ReadyChange, plan: P },
}

/// The result of the node writes of a change whose intent is Ready.
pub(super) enum Applied {
    /// The change took effect. A change below a parent gives the route that
    /// the parent must hold.
    Landed(Option<ParentRoute>),
    /// The change did not take effect, and none of its writes can still land.
    Stopped,
}

/// A route from a parent to its child `target` for `key`, which a change
/// created in the right-link chain (ADR-073).
pub(super) struct ParentRoute {
    pub(super) key: Vec<u8>,
    pub(super) target: NodeId,
}

/// Preserves the operation result independently from structural cleanup state.
pub(super) struct ChangeAttemptOutcome {
    pub(super) result: Result<(), TransError>,
    pub(super) state: ChangeAttemptResult,
}

/// Whether a coordinated change finished, can discard Preparing, or needs
/// recovery.
pub(super) enum ChangeAttemptResult {
    Completed,
    RetryCleanly,
    // Keep the common change outcomes small through a Box while retaining
    // recovery authority.
    RecoveryRequired(Box<ReadyIntent>),
}

/// One split or merge with a single outer lifecycle.
struct StructuralChangeAttempt<'a> {
    lifecycle: &'a ChangeLifecycle,
    change: PlannedChange<'a>,
    worker: &'a TxId,
}

impl ChangeLifecycle {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        recovery: StructuralRecovery,
        topology: TopologyMembership,
        structural_nodes: StructuralNodeAccess,
        reconciler: ParentReconciler,
        reclamation: ReclamationReporter,
        recovery_wake: Arc<Notify>,
        splitter: Splitter,
        merger: Merger,
    ) -> Self {
        Self {
            recovery,
            topology,
            structural_nodes,
            reconciler,
            reclamation,
            recovery_wake,
            splitter,
            merger,
        }
    }

    /// Makes `change` under the structural identity `worker`. The change
    /// completes, or leaves its Ready intent to recovery.
    pub(super) async fn run(
        &self,
        change: PlannedChange<'_>,
        worker: TxId,
        topology: StructuralTopology<'_>,
    ) -> Result<(), TransError> {
        StructuralChangeAttempt {
            lifecycle: self,
            change,
            worker: &worker,
        }
        .run(topology)
        .await
    }

    /// Splits the node at `path` when `reason` still calls for it.
    pub(super) async fn split_path(
        &self,
        path: &ObjectPath,
        reason: &SplitReason,
        topology: StructuralTopology<'_>,
    ) -> Result<(), TransError> {
        // A joined split also gets its own structural identity. Recovery can
        // reconcile parents after the topology participant has a final
        // status, and a fresh identity prevents help-forward from mistaking
        // this in-flight split for stale work.
        let worker = TxId::new_at(rt::system_now());
        self.run(PlannedChange::split(path, reason)?, worker, topology)
            .await
    }

    /// Makes the parent that routes `key` to `target` agree with the right-link
    /// chain of its children through `key`, so later descents route directly
    /// instead of walking right-links (ADR-073).
    ///
    /// This also heals a cascade: splitting a sibling whose own separator was
    /// never added still adds every intermediate separator, so the parent never
    /// grows unboundedly reliant on right-link walks. Idempotent and
    /// re-drivable: a lost CAS is re-queued for a later sweep. On a successful
    /// change that overflows the parent, recurses to split it.
    pub(super) async fn reconcile_parent(
        &self,
        collection: &CollectionAddress,
        key: &[u8],
        target: &NodeId,
        topology_participant: Option<&TxId>,
    ) -> Result<(), TransError> {
        let topology = match topology_participant {
            Some(id) => StructuralTopology::Joined(id),
            None => StructuralTopology::Owned,
        };
        let mut reconciliation = self.reconciler.begin(collection, key, target);
        loop {
            match self.reconciler.reconcile(&mut reconciliation).await? {
                ReconciliationOutcome::Reconciled => return Ok(()),
                ReconciliationOutcome::ParentRequiresSplit(action) => {
                    Box::pin(self.split_path(&action.path, &action.reason, topology)).await?;
                    if action.continuation == ParentSplitContinuation::CompleteReconciliation {
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Coordinates `change` after its intent `prepared` is written and its
    /// topology participant is admitted.
    #[cfg(test)]
    pub(super) async fn coordinate(
        &self,
        change: PlannedChange<'_>,
        worker: &TxId,
        prepared: PreparedIntent,
    ) -> ChangeAttemptOutcome {
        StructuralChangeAttempt {
            lifecycle: self,
            change,
            worker,
        }
        .coordinate(prepared)
        .await
    }
}

impl<'a> PlannedChange<'a> {
    /// Plans a split of the tree node at `path`.
    pub(super) fn split(path: &'a ObjectPath, reason: &'a SplitReason) -> Result<Self, TransError> {
        let (collection, target) = match path {
            ObjectPath::TreeRoot { collection } => (collection, SplitTarget::Root),
            ObjectPath::Node { collection, id } => (collection, SplitTarget::NonRoot(id)),
            _ => return Err(TransError::other("split candidate is not a tree node")),
        };
        Ok(Self::Split {
            collection,
            target,
            reason,
        })
    }

    fn collection(self) -> &'a CollectionAddress {
        match self {
            Self::Split { collection, .. } | Self::Merge { collection, .. } => collection,
        }
    }

    fn source_node_id(self) -> Option<&'a NodeId> {
        match self {
            Self::Split { target, .. } => target.source_node_id(),
            Self::Merge { source, .. } => Some(source),
        }
    }

    fn kind(self) -> ChangeKind {
        match self {
            Self::Split { .. } => ChangeKind::Split,
            Self::Merge { .. } => ChangeKind::Merge,
        }
    }
}

impl ChangeAttemptOutcome {
    pub(super) fn completed() -> Self {
        Self {
            result: Ok(()),
            state: ChangeAttemptResult::Completed,
        }
    }

    pub(super) fn retry_cleanly(result: Result<(), TransError>) -> Self {
        Self {
            result,
            state: ChangeAttemptResult::RetryCleanly,
        }
    }

    pub(super) fn recovery_required(ready: ReadyIntent, error: TransError) -> Self {
        Self {
            result: Err(error),
            state: ChangeAttemptResult::RecoveryRequired(Box::new(ready)),
        }
    }
}

impl StructuralChangeAttempt<'_> {
    async fn run(&self, topology: StructuralTopology<'_>) -> Result<(), TransError> {
        let lifecycle = self.lifecycle;
        let collection = self.change.collection();
        let result = match topology {
            StructuralTopology::Owned => {
                match lifecycle.topology.begin(collection, self.worker).await {
                    Ok(()) => self.run_prepared(topology).await,
                    Err(error) => Err(error),
                }
            }
            StructuralTopology::Joined(_) => {
                lifecycle.structural_nodes.begin_worker(self.worker);
                self.run_prepared(topology).await
            }
        };

        match topology {
            StructuralTopology::Owned => {
                lifecycle.topology.finalize(collection, self.worker).await;
            }
            StructuralTopology::Joined(_) => {
                lifecycle
                    .structural_nodes
                    .finalize_worker(self.worker)
                    .await;
            }
        }
        if result.is_err() {
            lifecycle.recovery_wake.notify_one();
        }
        result
    }

    async fn run_prepared(&self, topology: StructuralTopology<'_>) -> Result<(), TransError> {
        let participant = match topology {
            StructuralTopology::Owned => self.worker,
            StructuralTopology::Joined(participant) => participant,
        };
        let prepared = self
            .lifecycle
            .recovery
            .prepare_intent(
                self.change.collection(),
                self.change.source_node_id(),
                self.change.kind(),
                participant,
            )
            .await?;
        let cancellation = prepared.cancellation_witness();
        let outcome = match topology {
            StructuralTopology::Owned => {
                match self
                    .lifecycle
                    .topology
                    .join(self.change.collection(), self.worker)
                    .await
                {
                    Ok(()) => self.coordinate(prepared).await,
                    Err(error) => ChangeAttemptOutcome::retry_cleanly(Err(error)),
                }
            }
            StructuralTopology::Joined(_) => self.coordinate(prepared).await,
        };
        self.finish(outcome, &cancellation, topology).await
    }

    async fn coordinate(&self, prepared: PreparedIntent) -> ChangeAttemptOutcome {
        let lifecycle = self.lifecycle;
        let collection = self.change.collection();
        debug_assert!(prepared.targets(collection, self.change.source_node_id()));
        let acquisition = match self.change {
            PlannedChange::Split { .. } => GateAcquisition::WoundWait,
            // A merge is optional maintenance and must not abort user
            // transactions (ADR-073).
            PlannedChange::Merge { .. } => GateAcquisition::Polite,
        };
        let (node, observation) = match lifecycle
            .structural_nodes
            .acquire_structural_gate(
                collection,
                self.change.source_node_id(),
                self.worker,
                acquisition,
            )
            .await
        {
            Ok(Some(acquired)) => acquired,
            Ok(None) => return ChangeAttemptOutcome::retry_cleanly(Err(TransError::Retry)),
            Err(error) => return ChangeAttemptOutcome::retry_cleanly(Err(error)),
        };
        match self.change {
            PlannedChange::Split { target, reason, .. } => {
                let planned =
                    lifecycle
                        .splitter
                        .prepare(target, reason, self.worker, node, &prepared);
                let (ready, plan) = match self.mark_ready(planned, prepared, &observation).await {
                    Ok(ready) => ready,
                    Err(outcome) => return outcome,
                };
                let applied = lifecycle
                    .splitter
                    .apply(collection, reason, &observation, plan)
                    .await;
                self.complete(ready, applied).await
            }
            PlannedChange::Merge { source, .. } => {
                let planned = lifecycle.merger.prepare(collection, node).await;
                let (ready, plan) = match self.mark_ready(planned, prepared, &observation).await {
                    Ok(ready) => ready,
                    Err(outcome) => return outcome,
                };
                let applied = lifecycle
                    .merger
                    .apply(collection, source, &observation, ready.id(), plan)
                    .await;
                self.complete(ready, applied).await
            }
        }
    }

    /// Transitions the structural intent to Ready while its source gate is
    /// still held, or ends a change that is not necessary while the intent
    /// is still Preparing.
    async fn mark_ready<P>(
        &self,
        planned: Prepared<P>,
        prepared: PreparedIntent,
        observation: &LeafObservation,
    ) -> Result<(ReadyIntent, P), ChangeAttemptOutcome> {
        let (change, plan) = match planned {
            Prepared::Cancel(result) => return Err(self.cancel_preparing(result).await),
            Prepared::ReclaimOnly { node, reclaimed } => {
                return Err(self
                    .finish_reclamation_only(node, observation, &reclaimed)
                    .await);
            }
            Prepared::Ready { change, plan } => (change, plan),
        };
        match self
            .lifecycle
            .recovery
            .mark_ready(prepared, self.worker, observation, change)
            .await
        {
            ReadyIntentTransition::Ready(ready) => Ok((ready, plan)),
            ReadyIntentTransition::RetryCleanly(error) => {
                Err(ChangeAttemptOutcome::retry_cleanly(Err(error)))
            }
            ReadyIntentTransition::RecoveryRequired(ready, error) => {
                Err(ChangeAttemptOutcome::recovery_required(ready, error))
            }
        }
    }

    /// Reconciles the parent of a change that took effect, and deletes its
    /// Ready intent.
    async fn complete(
        &self,
        ready: ReadyIntent,
        applied: Result<Applied, TransError>,
    ) -> ChangeAttemptOutcome {
        match applied {
            Ok(Applied::Landed(route)) => {
                if let Some(ParentRoute { key, target }) = route
                    && let Err(error) = self
                        .lifecycle
                        .reconcile_parent(
                            self.change.collection(),
                            &key,
                            &target,
                            Some(ready.participant()),
                        )
                        .await
                {
                    return ChangeAttemptOutcome::recovery_required(ready, error);
                }
                self.finish_ready(ready).await
            }
            Ok(Applied::Stopped) => self.stop_ready(ready).await,
            Err(error) => ChangeAttemptOutcome::recovery_required(ready, error),
        }
    }

    /// Releases the source gate of a Ready change that did not take effect,
    /// and deletes its intent.
    async fn stop_ready(&self, ready: ReadyIntent) -> ChangeAttemptOutcome {
        if let Err(error) = self.release_gate().await {
            return ChangeAttemptOutcome::recovery_required(ready, error);
        }
        let mut outcome = self.finish_ready(ready).await;
        outcome.result = outcome.result.and(Err(TransError::Retry));
        outcome
    }

    /// Deletes an acknowledged Ready intent or leaves it for recovery.
    async fn finish_ready(&self, ready: ReadyIntent) -> ChangeAttemptOutcome {
        match self.lifecycle.recovery.complete_ready(ready).await {
            ReadyIntentCompletion::Completed => ChangeAttemptOutcome::completed(),
            ReadyIntentCompletion::RecoveryRequired(ready, error) => ChangeAttemptOutcome {
                result: Err(error),
                state: ChangeAttemptResult::RecoveryRequired(ready),
            },
        }
    }

    /// Releases the structural gate while the intent is still safe to discard.
    async fn cancel_preparing(&self, result: Result<(), TransError>) -> ChangeAttemptOutcome {
        let release = self.release_gate().await;
        ChangeAttemptOutcome::retry_cleanly(release.and(result))
    }

    /// Persists compaction and opens the gate in the same CAS when the change
    /// is not necessary. The intent is still Preparing and can be discarded
    /// through the ordinary clean-cancellation path.
    async fn finish_reclamation_only(
        &self,
        mut node: Node,
        observation: &LeafObservation,
        reclaimed: &[TxId],
    ) -> ChangeAttemptOutcome {
        node.remove_structural_gate(self.worker);
        match self
            .lifecycle
            .structural_nodes
            .store_structural_node(&node, observation)
            .await
        {
            Ok(Some(_)) => {
                let split_avoided = matches!(self.change, PlannedChange::Split { .. });
                self.lifecycle.reclamation.record(reclaimed, split_avoided);
                ChangeAttemptOutcome::retry_cleanly(Ok(()))
            }
            Ok(None) => self.cancel_preparing(Err(TransError::Retry)).await,
            Err(error) => {
                let _ = self.release_gate().await;
                ChangeAttemptOutcome::retry_cleanly(Err(error))
            }
        }
    }

    async fn release_gate(&self) -> Result<(), TransError> {
        self.lifecycle
            .structural_nodes
            .release_structural_gate(
                self.change.collection(),
                self.change.source_node_id(),
                self.worker,
            )
            .await
    }

    async fn finish(
        &self,
        outcome: ChangeAttemptOutcome,
        prepared: &PreparedIntentCancellation,
        topology: StructuralTopology<'_>,
    ) -> Result<(), TransError> {
        let ChangeAttemptOutcome { result, state } = outcome;
        match state {
            ChangeAttemptResult::Completed => match topology {
                StructuralTopology::Owned => result.and(self.leave_topology().await),
                StructuralTopology::Joined(_) => result,
            },
            ChangeAttemptResult::RetryCleanly => {
                let cleanup = match self.lifecycle.recovery.discard_prepared(prepared).await {
                    Ok(()) => match topology {
                        StructuralTopology::Owned => self.leave_topology().await,
                        StructuralTopology::Joined(_) => Ok(()),
                    },
                    Err(error) => Err(error),
                };
                result.and(cleanup)
            }
            ChangeAttemptResult::RecoveryRequired(_ready) => result,
        }
    }

    /// Removes the owned topology participant, which this database instance
    /// admitted.
    async fn leave_topology(&self) -> Result<(), TransError> {
        // The local admission permits any record observation.
        self.lifecycle
            .topology
            .leave(self.change.collection(), self.worker, Requirement::ANY)
            .await
    }
}
