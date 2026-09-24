//! The lifecycle and the steps that every structural change shares.
//!
//! A change first writes a `Preparing` structural intent below its topology
//! participant and joins the collection topology. The split or merge module
//! then gates its source, marks the intent `Ready`, and makes its node writes.
//! The lifecycle deletes the intent when the change completes, discards it when
//! the change stops before `Ready`, and leaves it to recovery otherwise.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use glassdb_concurr::{RetryConfig, rt};
use glassdb_data::{CollectionAddress, DbPrefix, NodeToken, TxId};
use glassdb_storage::transaction::{TxCommitStatus, TxLock, TxRecord};
use glassdb_storage::{
    CollectionStore, LeafBody, LeafObservation, Node, NodeStore, Requirement, StorageError,
    StructuralIntentStore, Timeline, TreeRouter,
};
use tokio::sync::Notify;

use crate::error::TransError;
use crate::gc::GcHints;
use crate::key_state_resolver::KeyStateResolver;
use crate::leaf_coord::LeafCoordinator;
use crate::monitor::{Monitor, TxRecoveryManifest};

use super::candidates::MaintenanceCandidates;
use super::nodes::StructuralNodeAccess;
use super::reconcile::{ParentReconciler, ParentSplitContinuation, ReconciliationOutcome};
use super::recovery::{
    ChangeKind, PreparedIntent, PreparedIntentCancellation, ReadyChange, ReadyIntent,
    ReadyIntentCompletion, ReadyIntentTransition, StructuralRecovery,
};
use super::split::{SplitReason, SplitTarget};
use super::stats::Stats;
use super::{merge, split};

/// The stores, services, and shared state that every structural change of one
/// database instance uses. Cloneable so the restructurer's loops share it.
#[derive(Clone)]
pub(super) struct ChangeContext {
    records: CollectionStore,
    pub(super) nodes: NodeStore,
    pub(super) router: TreeRouter,
    pub(super) mon: Monitor,
    pub(super) structural_nodes: StructuralNodeAccess,
    pub(super) timeline: Timeline,
    // The candidate feed that the restructurer drains. The coordinator
    // receives a clone for stored-leaf capacity; direct-commit policies receive
    // lightweight hint sinks for inline-pressure observations.
    pub(super) candidates: MaintenanceCandidates,
    pub(super) reconciler: ParentReconciler,
    pub(super) recovery: StructuralRecovery,
    // Wakes the independent recovery loop when a local change leaves `_s` work.
    pub(super) recovery_wake: Arc<Notify>,
    // Paces collection-record and node CAS retries. Transaction-status polling
    // remains entirely owned by Monitor.
    retry: RetryConfig,
    gc_hints: GcHints,
    pub(super) stats: Arc<Stats>,
}

/// The structural change that one attempt makes.
#[derive(Clone, Copy)]
pub(super) enum PlannedChange<'a> {
    Split {
        target: SplitTarget<'a>,
        reason: &'a SplitReason,
    },
    Merge {
        source: &'a NodeToken,
    },
}

/// Whether a change owns its topology participant, or runs beneath the
/// participant of another change.
#[derive(Clone, Copy)]
pub(super) enum StructuralTopology<'a> {
    Owned,
    Joined(&'a TxId),
}

/// Preserves the operation result independently from structural cleanup state.
pub(super) struct ChangeAttemptOutcome {
    pub(super) result: Result<(), TransError>,
    pub(super) state: ChangeAttemptResult,
}

/// One split or merge with a single outer lifecycle.
pub(super) struct StructuralChangeAttempt<'a> {
    ctx: &'a ChangeContext,
    collection: &'a CollectionAddress,
    change: PlannedChange<'a>,
    worker: TxId,
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

impl ChangeContext {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        records: CollectionStore,
        nodes: NodeStore,
        intent_store: StructuralIntentStore,
        timeline: Timeline,
        mon: Monitor,
        key_state: KeyStateResolver,
        db_prefix: DbPrefix,
        coord: LeafCoordinator,
        candidates: MaintenanceCandidates,
        retry: RetryConfig,
        gc_hints: GcHints,
    ) -> Self {
        let router = TreeRouter::new(nodes.clone(), std::num::NonZeroUsize::MIN);
        let structural_nodes =
            StructuralNodeAccess::new(nodes.clone(), mon.clone(), key_state, coord);
        let reconciler = ParentReconciler::new(
            structural_nodes.clone(),
            router.clone(),
            timeline.clone(),
            candidates.clone(),
        );
        let recovery = StructuralRecovery::new(
            records.clone(),
            nodes.clone(),
            intent_store,
            router.clone(),
            mon.clone(),
            structural_nodes.clone(),
            reconciler.clone(),
            timeline.clone(),
            db_prefix,
            retry,
        );
        Self {
            records,
            nodes,
            router,
            mon,
            structural_nodes,
            timeline,
            candidates,
            reconciler,
            recovery,
            recovery_wake: Arc::new(Notify::new()),
            retry,
            gc_hints,
            stats: Arc::new(Stats::default()),
        }
    }

    /// Releases the structural gate while the intent is still safe to discard.
    pub(super) async fn cancel_preparing_change(
        &self,
        collection: &CollectionAddress,
        source: Option<&NodeToken>,
        worker: &TxId,
        result: Result<(), TransError>,
    ) -> ChangeAttemptOutcome {
        let release = self
            .structural_nodes
            .release_structural_gate(collection, source, worker)
            .await;
        ChangeAttemptOutcome::retry_cleanly(release.and(result))
    }

    /// Persists compaction and opens the gate in the same CAS when the
    /// candidate is not actionable. The intent is still Preparing and can be
    /// discarded through the ordinary clean-cancellation path.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn finish_reclamation_without_change(
        &self,
        collection: &CollectionAddress,
        source: Option<&NodeToken>,
        worker: &TxId,
        mut node: Node,
        observation: &LeafObservation,
        writers: &[TxId],
        split_avoided: bool,
    ) -> ChangeAttemptOutcome {
        node.remove_structural_gate(worker);
        match self
            .structural_nodes
            .store_structural_node(&node, observation)
            .await
        {
            Ok(Some(_)) => {
                self.record_reclamation(writers, split_avoided);
                ChangeAttemptOutcome::retry_cleanly(Ok(()))
            }
            Ok(None) => {
                self.cancel_preparing_change(collection, source, worker, Err(TransError::Retry))
                    .await
            }
            Err(error) => {
                let _ = self
                    .structural_nodes
                    .release_structural_gate(collection, source, worker)
                    .await;
                ChangeAttemptOutcome::retry_cleanly(Err(error))
            }
        }
    }

    /// Publishes the statistics and GC hints of acknowledged tombstone
    /// reclamation.
    pub(super) fn record_reclamation(&self, writers: &[TxId], split_avoided: bool) {
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
        self.gc_hints.schedule_all(writers.iter().cloned());
    }

    /// Transitions the structural intent to Ready while its source gate is
    /// still held.
    pub(super) async fn mark_ready(
        &self,
        worker: &TxId,
        prepared: PreparedIntent,
        observation: &LeafObservation,
        change: ReadyChange,
    ) -> Result<ReadyIntent, ChangeAttemptOutcome> {
        match self
            .recovery
            .mark_ready(prepared, worker, observation, change)
            .await
        {
            ReadyIntentTransition::Ready(ready) => Ok(ready),
            ReadyIntentTransition::RetryCleanly(error) => {
                Err(ChangeAttemptOutcome::retry_cleanly(Err(error)))
            }
            ReadyIntentTransition::RecoveryRequired(ready, error) => {
                Err(ChangeAttemptOutcome::recovery_required(ready, error))
            }
        }
    }

    /// Deletes an acknowledged Ready intent or leaves it for recovery.
    pub(super) async fn finish_ready_change(&self, ready: ReadyIntent) -> ChangeAttemptOutcome {
        match self.recovery.complete_ready(ready).await {
            ReadyIntentCompletion::Completed => ChangeAttemptOutcome::completed(),
            ReadyIntentCompletion::RecoveryRequired(ready, error) => ChangeAttemptOutcome {
                result: Err(error),
                state: ChangeAttemptResult::RecoveryRequired(ready),
            },
        }
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
        target: &NodeToken,
        topology_participant: Option<&TxId>,
    ) -> Result<(), TransError> {
        let mut reconciliation = self.reconciler.begin(collection, key, target);
        loop {
            match self.reconciler.reconcile(&mut reconciliation).await? {
                ReconciliationOutcome::Reconciled => return Ok(()),
                ReconciliationOutcome::ParentRequiresSplit(action) => {
                    match topology_participant {
                        Some(id) => {
                            Box::pin(split::split_path_joined(
                                self,
                                &action.path,
                                id,
                                &action.reason,
                            ))
                            .await?
                        }
                        None => {
                            Box::pin(split::split_path(self, &action.path, &action.reason)).await?
                        }
                    }
                    if action.continuation == ParentSplitContinuation::CompleteReconciliation {
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Persists the recovery manifest of a topology participant.
    pub(super) async fn begin_topology_tx(
        &self,
        collection: &CollectionAddress,
        id: &TxId,
    ) -> Result<(), TransError> {
        self.mon
            .begin_persisted_tx(
                id,
                TxRecoveryManifest {
                    locks: vec![TxLock::TopologyParticipant {
                        collection: collection.clone(),
                    }],
                    ..TxRecoveryManifest::default()
                },
            )
            .await
    }

    /// Admits `id` as a topology participant of `collection`.
    pub(super) async fn join_topology(
        &self,
        collection: &CollectionAddress,
        id: &TxId,
    ) -> Result<(), TransError> {
        let mut backoff = self.retry.backoff();
        loop {
            let (mut record, observed) =
                match self.records.load_record(collection, Requirement::ANY).await {
                    Ok(record) => record,
                    Err(StorageError::NotFound) => return Err(TransError::StaleCollection),
                    Err(error) => return Err(error.into()),
                };
            if record
                .topology_participants()
                .any(|participant| participant == id)
            {
                // This is the same change's admission; its identity is not
                // reused after departure. New admission requires the CAS below.
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
                    TxCommitStatus::Committed => Err(TransError::StaleCollection),
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

    /// Removes a participant admitted by this database instance.
    pub(super) async fn leave_topology(
        &self,
        collection: &CollectionAddress,
        id: &TxId,
    ) -> Result<(), TransError> {
        self.recovery
            .leave_topology(collection, id, Requirement::ANY)
            .await
    }

    async fn finalize_topology_worker(&self, collection: &CollectionAddress, id: &TxId) {
        let mut record = TxRecord::new(id.clone(), TxCommitStatus::Committed);
        record.locks.push(TxLock::TopologyParticipant {
            collection: collection.clone(),
        });
        if let Err(e) = self.mon.commit_tx(record).await {
            tracing::debug!(
                target: "glassdb::restructurer",
                error = %e,
                "finalizing topology participant failed"
            );
        }
    }
}

impl<'a> PlannedChange<'a> {
    fn source_token(self) -> Option<&'a NodeToken> {
        match self {
            Self::Split { target, .. } => target.source_token(),
            Self::Merge { source } => Some(source),
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

impl<'a> StructuralChangeAttempt<'a> {
    pub(super) fn new(
        ctx: &'a ChangeContext,
        collection: &'a CollectionAddress,
        change: PlannedChange<'a>,
        worker: TxId,
    ) -> Self {
        Self {
            ctx,
            collection,
            change,
            worker,
        }
    }

    pub(super) async fn run(self, topology: StructuralTopology<'_>) -> Result<(), TransError> {
        let result = match topology {
            StructuralTopology::Owned => {
                match self
                    .ctx
                    .begin_topology_tx(self.collection, &self.worker)
                    .await
                {
                    Ok(()) => self.run_prepared(topology).await,
                    Err(error) => Err(error),
                }
            }
            StructuralTopology::Joined(_) => {
                self.ctx.mon.begin_tx(&self.worker);
                self.run_prepared(topology).await
            }
        };

        match topology {
            StructuralTopology::Owned => {
                self.ctx
                    .finalize_topology_worker(self.collection, &self.worker)
                    .await;
            }
            StructuralTopology::Joined(_) => {
                self.ctx
                    .structural_nodes
                    .finalize_worker(&self.worker)
                    .await;
            }
        }
        if result.is_err() {
            self.ctx.recovery_wake.notify_one();
        }
        result
    }

    async fn run_prepared(&self, topology: StructuralTopology<'_>) -> Result<(), TransError> {
        let participant = match topology {
            StructuralTopology::Owned => &self.worker,
            StructuralTopology::Joined(participant) => participant,
        };
        let prepared = self
            .ctx
            .recovery
            .prepare_intent(
                self.collection,
                self.change.source_token(),
                self.change.kind(),
                participant,
            )
            .await?;
        let cancellation = prepared.cancellation_witness();
        let outcome = match topology {
            StructuralTopology::Owned => {
                match self.ctx.join_topology(self.collection, &self.worker).await {
                    Ok(()) => self.coordinate(prepared).await,
                    Err(error) => ChangeAttemptOutcome::retry_cleanly(Err(error)),
                }
            }
            StructuralTopology::Joined(_) => self.coordinate(prepared).await,
        };
        self.finish(outcome, &cancellation, topology).await
    }

    async fn coordinate(&self, prepared: PreparedIntent) -> ChangeAttemptOutcome {
        match self.change {
            PlannedChange::Split { target, reason } => {
                split::coordinate(
                    self.ctx,
                    self.collection,
                    target,
                    &self.worker,
                    reason,
                    prepared,
                )
                .await
            }
            PlannedChange::Merge { source } => {
                merge::coordinate(self.ctx, self.collection, source, &self.worker, prepared).await
            }
        }
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
                StructuralTopology::Owned => {
                    result.and(self.ctx.leave_topology(self.collection, &self.worker).await)
                }
                StructuralTopology::Joined(_) => result,
            },
            ChangeAttemptResult::RetryCleanly => {
                let cleanup = match self.ctx.recovery.discard_prepared(prepared).await {
                    Ok(()) => match topology {
                        StructuralTopology::Owned => {
                            self.ctx.leave_topology(self.collection, &self.worker).await
                        }
                        StructuralTopology::Joined(_) => Ok(()),
                    },
                    Err(error) => Err(error),
                };
                result.and(cleanup)
            }
            ChangeAttemptResult::RecoveryRequired(_ready) => result,
        }
    }
}

/// Removes durable absence entries that no transaction still holds.
pub(super) fn reclaim_holder_free_tombstones(node: &mut Node) -> Vec<TxId> {
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
