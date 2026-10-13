//! The lifecycle and the steps that every structural change shares.
//!
//! A change first writes a `Preparing` structural intent below its topology
//! participant and joins the collection topology. The lifecycle then gates the
//! source, and the split or merge module checks the change again and plans its
//! node writes. After the lifecycle marks the intent `Ready`, the module makes
//! the writes, and the lifecycle reconciles the parent. The lifecycle deletes
//! the intent when the change completes, discards it when the change stops
//! before `Ready`, and leaves it to recovery otherwise.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use glassdb_concurr::{join_all_bounded, rt};
use glassdb_data::{CollectionAddress, NodeId, ObjectPath, TxId};
use glassdb_storage::{LeafObservation, Node, Requirement};
use tokio::sync::Notify;

use crate::error::TransError;
use crate::node_locking::GateAcquisition;

use super::candidates::MaintenanceCandidates;
use super::measurements;
use super::merge::{MergeReason, Merger};
use super::nodes::StructuralNodeAccess;
use super::reclamation::ReclamationReporter;
use super::reconcile::{ParentReconciler, ParentReconciliation, ReconciliationOutcome};
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
    // Receives the time of each leaf change that lands.
    candidates: MaintenanceCandidates,
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
        reason: MergeReason,
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
// One value exists for each structural change, and only while it runs, so the
// size of its largest variant costs nothing.
#[allow(clippy::large_enum_variant)]
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

/// The parents that a reconciliation could not make agree with their routes.
pub(super) struct UnreconciledParents {
    pub(super) error: TransError,
    /// The keys of the routes whose parents can disagree with them. `None`
    /// reports that a parent split failed beneath the participant of another
    /// change, which then can have an intent left.
    pub(super) keys: Option<BTreeSet<Vec<u8>>>,
}

/// The change of an owned participant after its node writes, with the
/// witness that can still discard its Preparing intent.
struct Written {
    coordinated: Coordinated,
    cancellation: PreparedIntentCancellation,
}

/// The state of a change after its node writes.
// One value exists for each change of a batch, and only while it runs, so
// the size of its largest variant costs nothing.
#[allow(clippy::large_enum_variant)]
enum Coordinated {
    /// The change ended, or left its Ready intent to recovery.
    Ended(ChangeAttemptOutcome),
    /// The node writes of the change landed. Its parent must agree with its
    /// route before its Ready intent completes.
    Landed(LandedChange),
}

impl Coordinated {
    /// Returns the route that the parent of a change must hold, after its
    /// node writes landed.
    fn route(&self) -> Option<&ParentRoute> {
        match self {
            Coordinated::Landed(landed) => landed.route.as_ref(),
            Coordinated::Ended(_) => None,
        }
    }
}

impl UnreconciledParents {
    /// Reports whether the parent of the route for `key` can disagree with
    /// it.
    fn includes(&self, key: &[u8]) -> bool {
        self.keys.as_ref().is_none_or(|keys| keys.contains(key))
    }
}

/// A change whose node writes landed.
struct LandedChange {
    ready: ReadyIntent,
    route: Option<ParentRoute>,
    /// The time that a change of a leaf held the gate.
    gate_time: Option<Duration>,
}

/// One change of a sweep, which owns its topology participant `worker`.
pub(super) struct SweepChange<'a> {
    pub(super) change: PlannedChange<'a>,
    pub(super) worker: TxId,
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
        candidates: MaintenanceCandidates,
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
            candidates,
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
        match topology {
            StructuralTopology::Owned => self
                .run_sweep(vec![&SweepChange { change, worker }], NonZeroUsize::MIN)
                .await
                .pop()
                .expect("a sweep returns one result for each change"),
            StructuralTopology::Joined(participant) => {
                StructuralChangeAttempt {
                    lifecycle: self,
                    change,
                    worker: &worker,
                }
                .run_joined(participant)
                .await
            }
        }
    }

    /// Makes the `changes` of one sweep, at most `parallelism` at a time,
    /// and returns the result of each one. Each change completes, or leaves
    /// its Ready intent to recovery.
    ///
    /// Each change has its own topology participant, but the participants of
    /// one collection join and leave its record with one CAS. Concurrent
    /// changes of one collection otherwise lose most of their record CASes to
    /// each other, and back off for longer than the changes take.
    pub(super) async fn run_sweep(
        &self,
        changes: Vec<&SweepChange<'_>>,
        parallelism: NonZeroUsize,
    ) -> Vec<Result<(), TransError>> {
        // The futures are collected before they run, because an iterator of
        // closures over borrowed state held across an await is not `Send`.
        let attempts: Vec<_> = changes
            .into_iter()
            .map(|change| StructuralChangeAttempt {
                lifecycle: self,
                change: change.change,
                worker: &change.worker,
            })
            .collect();
        let preparing: Vec<_> = attempts
            .iter()
            .map(|attempt| attempt.prepare_owned())
            .collect();
        let prepared = join_all_bounded(preparing, parallelism).await;

        let mut joining = Participants::default();
        for (attempt, prepared) in attempts.iter().zip(&prepared) {
            if prepared.is_ok() {
                joining.add(attempt);
            }
        }
        let joined = joining.join(&self.topology, parallelism).await;

        let writing: Vec<_> = attempts
            .iter()
            .zip(prepared)
            .map(|(attempt, prepared)| attempt.write_admitted(prepared, &joined))
            .collect();
        let written = join_all_bounded(writing, parallelism).await;

        // The sibling changes of one batch have the same parent, so their
        // routes reconcile together. They otherwise lose the gate of the
        // parent to each other, and end in recovery.
        let mut routes = BTreeMap::<&CollectionAddress, Vec<(Vec<u8>, NodeId)>>::new();
        for (attempt, written) in attempts.iter().zip(&written) {
            if let Some(route) = written.as_ref().ok().and_then(|w| w.coordinated.route()) {
                routes
                    .entry(attempt.change.collection())
                    .or_default()
                    .push((route.key.clone(), route.target));
            }
        }
        let reconciling: Vec<_> = routes
            .iter()
            .map(|(collection, routes)| self.reconcile_parents(collection, routes.clone(), None))
            .collect();
        let reconciled = join_all_bounded(reconciling, parallelism).await;
        let reconciled: BTreeMap<_, _> = routes.keys().copied().zip(reconciled).collect();

        let settling: Vec<_> = attempts
            .iter()
            .zip(written)
            .map(|(attempt, written)| {
                attempt.settle_written(written, reconciled.get(attempt.change.collection()))
            })
            .collect();
        let settled = join_all_bounded(settling, parallelism).await;

        let mut leaving = Participants::default();
        for (attempt, (_, departs)) in attempts.iter().zip(&settled) {
            if *departs {
                leaving.add(attempt);
            }
        }
        let left = leaving.leave(&self.topology, parallelism).await;

        let finalizing: Vec<_> = attempts
            .iter()
            .zip(settled)
            .map(|(attempt, (result, departs))| {
                let departed = departs.then(|| &left[attempt.change.collection()]);
                attempt.finalize_owned(result, departed)
            })
            .collect();
        join_all_bounded(finalizing, parallelism).await
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

    /// Makes the parents that route the keys of `routes` to their nodes agree
    /// with the right-link chains of their children through the keys, so
    /// later descents route directly instead of walking right-links
    /// (ADR-073).
    ///
    /// This also heals a cascade: splitting a sibling whose own separator was
    /// never added still adds every intermediate separator, so the parent never
    /// grows unboundedly reliant on right-link walks. Idempotent and
    /// re-drivable: a lost CAS is re-queued for a later sweep. A parent that
    /// overflows splits beneath `topology_participant`, or beneath its own
    /// participant without one.
    pub(super) async fn reconcile_parents(
        &self,
        collection: &CollectionAddress,
        routes: Vec<(Vec<u8>, NodeId)>,
        topology_participant: Option<&TxId>,
    ) -> Result<(), UnreconciledParents> {
        let topology = match topology_participant {
            Some(id) => StructuralTopology::Joined(id),
            None => StructuralTopology::Owned,
        };
        let mut reconciliation = self.reconciler.begin(collection, routes);
        loop {
            match self.reconciler.reconcile(&mut reconciliation).await {
                Ok(ReconciliationOutcome::Reconciled) => return Ok(()),
                Ok(ReconciliationOutcome::ParentRequiresSplit(action)) => {
                    let split = Box::pin(self.split_path(&action.path, &action.reason, topology));
                    if let Err(error) = split.await {
                        // A split beneath the participant of another change
                        // can leave its intent to that participant.
                        let keys = match topology {
                            StructuralTopology::Joined(_) => None,
                            StructuralTopology::Owned => Some(unresolved(&reconciliation)),
                        };
                        return Err(UnreconciledParents { error, keys });
                    }
                }
                Err(error) => {
                    return Err(UnreconciledParents {
                        error,
                        keys: Some(unresolved(&reconciliation)),
                    });
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

    /// Returns the kind of the change for its typical time.
    fn measured_kind(self) -> measurements::ChangeKind {
        match self {
            Self::Split { .. } => measurements::ChangeKind::Split,
            Self::Merge { .. } => measurements::ChangeKind::Merge,
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
    /// Makes the change beneath the topology participant of another change.
    async fn run_joined(&self, participant: &TxId) -> Result<(), TransError> {
        let lifecycle = self.lifecycle;
        lifecycle.structural_nodes.begin_worker(self.worker);
        let result = match self.prepare_intent(participant).await {
            Ok(prepared) => {
                let cancellation = prepared.cancellation_witness();
                let outcome = self.coordinate(prepared).await;
                self.settle(outcome, &cancellation).await.0
            }
            Err(error) => Err(error),
        };
        lifecycle
            .structural_nodes
            .finalize_worker(self.worker)
            .await;
        if result.is_err() {
            lifecycle.recovery_wake.notify_one();
        }
        result
    }

    /// Persists the recovery manifest of the owned topology participant, and
    /// writes the Preparing intent of the change beneath it.
    async fn prepare_owned(
        &self,
    ) -> Result<(PreparedIntent, PreparedIntentCancellation), TransError> {
        self.lifecycle
            .topology
            .begin(self.change.collection(), self.worker)
            .await?;
        let prepared = self.prepare_intent(self.worker).await?;
        let cancellation = prepared.cancellation_witness();
        Ok((prepared, cancellation))
    }

    /// Makes the node writes of the change of an owned participant, after
    /// its collection admission in `joined`.
    async fn write_admitted(
        &self,
        prepared: Result<(PreparedIntent, PreparedIntentCancellation), TransError>,
        joined: &RecordResults<'_>,
    ) -> Result<Written, TransError> {
        let (prepared, cancellation) = prepared?;
        let coordinated = match &joined[self.change.collection()] {
            Ok(()) => self.write_nodes(prepared).await,
            Err(error) => {
                Coordinated::Ended(ChangeAttemptOutcome::retry_cleanly(Err(error.clone())))
            }
        };
        Ok(Written {
            coordinated,
            cancellation,
        })
    }

    /// Completes the change of an owned participant after `reconciled`, the
    /// reconciliation of the parents of its batch, and settles its intent.
    /// Returns the result of the change, and whether its participant can
    /// leave the topology.
    async fn settle_written(
        &self,
        written: Result<Written, TransError>,
        reconciled: Option<&Result<(), UnreconciledParents>>,
    ) -> (Result<(), TransError>, bool) {
        let Written {
            coordinated,
            cancellation,
        } = match written {
            Ok(written) => written,
            Err(error) => return (Err(error), false),
        };
        let outcome = match coordinated {
            Coordinated::Ended(outcome) => outcome,
            Coordinated::Landed(landed) => {
                let reconciled = match (reconciled, &landed.route) {
                    (Some(Err(failed)), Some(route)) if failed.includes(&route.key) => {
                        Err(failed.error.clone())
                    }
                    _ => Ok(()),
                };
                self.complete(landed, reconciled).await
            }
        };
        self.settle(outcome, &cancellation).await
    }

    /// Gives the owned participant its final status, after its departure
    /// `departed`, if it could depart.
    async fn finalize_owned(
        &self,
        result: Result<(), TransError>,
        departed: Option<&Result<(), TransError>>,
    ) -> Result<(), TransError> {
        let lifecycle = self.lifecycle;
        // One departure CAS removes all the participants of a collection, so
        // its error does not fail the changes that landed. Garbage collection
        // removes a participant with a final status and no intents.
        let departure_failed = departed.is_some_and(|departed| departed.is_err());
        lifecycle
            .topology
            .finalize(self.change.collection(), self.worker)
            .await;
        if result.is_err() || departure_failed {
            lifecycle.recovery_wake.notify_one();
        }
        result
    }

    async fn prepare_intent(&self, participant: &TxId) -> Result<PreparedIntent, TransError> {
        self.lifecycle
            .recovery
            .prepare_intent(
                self.change.collection(),
                self.change.source_node_id(),
                self.change.kind(),
                participant,
            )
            .await
    }

    /// Makes the change alone: its node writes, the reconciliation of its
    /// parent, and the completion of its intent.
    async fn coordinate(&self, prepared: PreparedIntent) -> ChangeAttemptOutcome {
        match self.write_nodes(prepared).await {
            Coordinated::Ended(outcome) => outcome,
            Coordinated::Landed(landed) => {
                let reconciled = match &landed.route {
                    Some(ParentRoute { key, target }) => {
                        self.lifecycle
                            .reconcile_parents(
                                self.change.collection(),
                                vec![(key.clone(), *target)],
                                Some(landed.ready.participant()),
                            )
                            .await
                    }
                    None => Ok(()),
                };
                self.complete(landed, reconciled.map_err(|failed| failed.error))
                    .await
            }
        }
    }

    /// Gates the source of the change and makes its node writes.
    async fn write_nodes(&self, prepared: PreparedIntent) -> Coordinated {
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
            Ok(None) => {
                return Coordinated::Ended(ChangeAttemptOutcome::retry_cleanly(Err(
                    TransError::Retry,
                )));
            }
            Err(error) => {
                return Coordinated::Ended(ChangeAttemptOutcome::retry_cleanly(Err(error)));
            }
        };
        // The time of a leaf change is the time that it holds the gate, until
        // the node writes land. Only then do the transactions of the leaf
        // wait for the change. The wait for the gate depends on the writers
        // of the leaf instead.
        let gated = rt::Instant::now();
        let leaf = node.as_leaf().is_some();
        let (ready, applied) = match self.change {
            PlannedChange::Split { target, reason, .. } => {
                let planned =
                    lifecycle
                        .splitter
                        .prepare(target, reason, self.worker, node, &prepared);
                let (ready, plan) = match self.mark_ready(planned, prepared, &observation).await {
                    Ok(ready) => ready,
                    Err(outcome) => return Coordinated::Ended(outcome),
                };
                let applied = lifecycle
                    .splitter
                    .apply(collection, reason, &observation, plan)
                    .await;
                (ready, applied)
            }
            PlannedChange::Merge { source, reason, .. } => {
                let planned = lifecycle.merger.prepare(collection, node, reason).await;
                let (ready, plan) = match self.mark_ready(planned, prepared, &observation).await {
                    Ok(ready) => ready,
                    Err(outcome) => return Coordinated::Ended(outcome),
                };
                let applied = lifecycle
                    .merger
                    .apply(collection, source, &observation, ready.id(), plan)
                    .await;
                (ready, applied)
            }
        };
        let gate_time = leaf.then(|| gated.elapsed());
        match applied {
            Ok(Applied::Landed(route)) => Coordinated::Landed(LandedChange {
                ready,
                route,
                gate_time,
            }),
            Ok(Applied::Stopped) => Coordinated::Ended(self.stop_ready(ready).await),
            Err(error) => Coordinated::Ended(ChangeAttemptOutcome::recovery_required(ready, error)),
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

    /// Deletes the Ready intent of a change whose node writes landed, once
    /// `reconciled` reports that its parent agrees with its route.
    async fn complete(
        &self,
        landed: LandedChange,
        reconciled: Result<(), TransError>,
    ) -> ChangeAttemptOutcome {
        let LandedChange {
            ready, gate_time, ..
        } = landed;
        if let Err(error) = reconciled {
            return ChangeAttemptOutcome::recovery_required(ready, error);
        }
        let outcome = self.finish_ready(ready).await;
        if let Some(time) = gate_time
            && outcome.result.is_ok()
        {
            self.lifecycle
                .candidates
                .observe_change_time(self.change.measured_kind(), time);
        }
        outcome
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

    /// Discards the Preparing intent of a change that stopped cleanly.
    /// Returns the result of the change, and whether its participant can
    /// leave the topology, because the participant has no intent left.
    async fn settle(
        &self,
        outcome: ChangeAttemptOutcome,
        prepared: &PreparedIntentCancellation,
    ) -> (Result<(), TransError>, bool) {
        let ChangeAttemptOutcome { result, state } = outcome;
        match state {
            ChangeAttemptResult::Completed => (result, true),
            ChangeAttemptResult::RetryCleanly => {
                match self.lifecycle.recovery.discard_prepared(prepared).await {
                    Ok(()) => (result, true),
                    Err(error) => (result.and(Err(error)), false),
                }
            }
            ChangeAttemptResult::RecoveryRequired(_ready) => (result, false),
        }
    }
}

/// The owned topology participants of one sweep, grouped by collection.
#[derive(Default)]
struct Participants<'a> {
    by_collection: BTreeMap<&'a CollectionAddress, Vec<TxId>>,
}

/// The result of one CAS on the record of each collection.
type RecordResults<'a> = BTreeMap<&'a CollectionAddress, Result<(), TransError>>;

impl<'a> Participants<'a> {
    fn add(&mut self, attempt: &StructuralChangeAttempt<'a>) {
        self.by_collection
            .entry(attempt.change.collection())
            .or_default()
            .push(*attempt.worker);
    }

    /// Admits the participants of each collection with one CAS.
    async fn join(
        self,
        topology: &TopologyMembership,
        parallelism: NonZeroUsize,
    ) -> RecordResults<'a> {
        let joins: Vec<_> = self
            .by_collection
            .iter()
            .map(|(collection, ids)| topology.join(collection, ids))
            .collect();
        let results = join_all_bounded(joins, parallelism).await;
        self.by_collection.into_keys().zip(results).collect()
    }

    /// Removes the participants of each collection with one CAS.
    async fn leave(
        self,
        topology: &TopologyMembership,
        parallelism: NonZeroUsize,
    ) -> RecordResults<'a> {
        // The local admission permits any record observation.
        let leaves: Vec<_> = self
            .by_collection
            .iter()
            .map(|(collection, ids)| topology.leave(collection, ids, Requirement::ANY))
            .collect();
        let results = join_all_bounded(leaves, parallelism).await;
        self.by_collection
            .into_keys()
            .zip(results.into_iter().map(|left| left.map(|_| ())))
            .collect()
    }
}

/// Returns the keys of the routes of `reconciliation` whose parents do not
/// agree with them yet.
fn unresolved(reconciliation: &ParentReconciliation) -> BTreeSet<Vec<u8>> {
    reconciliation
        .unresolved_keys()
        .map(<[u8]>::to_vec)
        .collect()
}
