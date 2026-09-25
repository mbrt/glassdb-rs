//! Background structural changes of the B-link coordination tree: splits of
//! over-full nodes (ADR-031) and merges of underfull nodes into their right
//! sibling (ADR-073).
//!
//! The [`Restructurer`] schedules the work. It drains the candidate feed, and
//! gives each candidate to the split or the merge module, which re-checks it
//! against cached state. Both kinds of change then run through one lifecycle:
//! a structural intent written ahead of the change, a one-node structural
//! gate, and parent reconciliation after the change. An independent loop
//! completes the changes that a crash or an error interrupted.

mod candidates;
mod change;
mod merge;
mod nodes;
mod reclamation;
mod reconcile;
mod recovery;
mod split;
mod stats;
mod topology;

use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use glassdb_concurr::{Background, RetryConfig, ScanCadence, rt};
use glassdb_data::{CollectionAddress, DbPrefix, ObjectPath, TxId};
use glassdb_storage::{
    CollectionStore, InlinePolicy, NodeSizePolicy, NodeStore, StructuralIntentStore, Timeline,
    TreeRouter,
};
use tokio::sync::Notify;

use crate::collections::TopologySettler;
use crate::error::TransError;
use crate::gc::GcHints;
use crate::key_state_resolver::KeyStateResolver;
use crate::leaf_coord::LeafCoordinator;
use crate::monitor::Monitor;

use candidates::{CandidateCause, MaintenanceCandidate, MaintenanceCandidates};
use change::{ChangeLifecycle, PlannedChange, StructuralTopology};
use merge::Merger;
use nodes::StructuralNodeAccess;
use reclamation::ReclamationReporter;
use reconcile::ParentReconciler;
use recovery::{RecoveryAction, RecoveryStep, StructuralRecovery};
use split::Splitter;
use stats::Stats;
use topology::TopologyMembership;

pub use candidates::StructuralHintSink;
pub use stats::{InlinePressureStats, RestructurerStats};

/// Back off empty structural-intent listings independently of candidates.
const STRUCTURAL_RECOVERY_IDLE_INTERVAL: Duration = Duration::from_secs(600);

/// Bounded compare-and-swap attempts on one contended structural node. When
/// they run out, the caller stops its step, and a later sweep or recovery
/// attempt tries again.
const NODE_CAS_ATTEMPTS: usize = 8;

/// Background executor that halves over-full B-link nodes (ADR-031) and merges
/// underfull nodes into their right sibling (ADR-073). Holds no
/// per-transaction state: every structural change is a sequence of structural
/// compare-and-swaps through the node and structural-intent stores, recovered
/// idempotently like any in-doubt CAS.
#[derive(Clone)]
pub struct Restructurer {
    // Weak so a clone captured in the spawned loop does not keep the executor
    // alive across shutdown; `Engine` is the single strong owner.
    bg: Weak<Background>,
    mon: Monitor,
    // The candidate feed that the restructurer drains. The coordinator
    // receives a clone for stored-leaf capacity; direct-commit policies receive
    // lightweight hint sinks for inline-pressure observations.
    candidates: MaintenanceCandidates,
    stats: Arc<Stats>,
    splitter: Splitter,
    merger: Merger,
    changes: ChangeLifecycle,
    reconciler: ParentReconciler,
    recovery: StructuralRecovery,
    recovery_wake: Arc<Notify>,
}

impl Restructurer {
    /// Builds a restructurer and coordinator that share one timeline and
    /// candidate feed.
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
        db_prefix: DbPrefix,
        policy: NodeSizePolicy,
        inline: InlinePolicy,
        gc_hints: GcHints,
    ) -> (LeafCoordinator, Self) {
        let candidates = MaintenanceCandidates::with_policies(policy, inline);
        let coord = LeafCoordinator::with_hinter(
            nodes.clone(),
            key_state.clone(),
            mon.clone(),
            retry,
            policy,
            Arc::new(candidates.clone()),
        );
        let restructurer = Restructurer::with_candidates(
            bg,
            records,
            nodes,
            intent_store,
            timeline,
            mon,
            key_state,
            db_prefix,
            coord.clone(),
            candidates,
            retry,
            gc_hints,
        );
        (coord, restructurer)
    }

    /// Returns a producer handle for structural hints decided outside the leaf
    /// coordinator.
    pub fn hint_sink(&self) -> StructuralHintSink {
        self.candidates.hint_sink()
    }

    /// Returns and resets background split and merge activity counters.
    pub fn stats_and_reset(&self) -> RestructurerStats {
        self.stats.take()
    }

    /// Starts independent candidate and structural-recovery loops.
    pub fn start(&self) {
        let Some(bg) = self.bg.upgrade() else {
            return;
        };
        let restructurer = self.clone();
        bg.spawn(async move {
            loop {
                restructurer.candidates.next_sweep().await;
                restructurer.run_once().await;
            }
        });
        let recovery = self.clone();
        bg.spawn(async move {
            let minimum = recovery.mon.protocol_timing().pending_timeout();
            let mut cadence = ScanCadence::new(minimum, STRUCTURAL_RECOVERY_IDLE_INTERVAL);
            loop {
                let delay = match recovery.recover_structural_intents().await {
                    Ok(progress) => {
                        cadence.observe(progress);
                        if recovery.recovery.has_continuation() {
                            cadence.delay().min(minimum)
                        } else {
                            cadence.delay()
                        }
                    }
                    Err(_) => minimum,
                };
                tokio::select! {
                    _ = rt::sleep(delay) => {}
                    _ = recovery.recovery_wake.notified() => {}
                }
            }
        });
    }

    /// Creates a restructurer over an explicitly co-wired coordinator and feed.
    #[allow(clippy::too_many_arguments)]
    fn with_candidates(
        bg: Weak<Background>,
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
        let stats = Arc::new(Stats::default());
        let router = TreeRouter::new(nodes.clone(), NonZeroUsize::MIN);
        let structural_nodes =
            StructuralNodeAccess::new(nodes.clone(), mon.clone(), key_state, coord);
        let reconciler = ParentReconciler::new(
            structural_nodes.clone(),
            router.clone(),
            timeline.clone(),
            candidates.clone(),
        );
        let topology = TopologyMembership::new(records, mon.clone(), retry);
        let recovery = StructuralRecovery::new(
            topology.clone(),
            nodes.clone(),
            intent_store,
            router.clone(),
            mon.clone(),
            structural_nodes.clone(),
            reconciler.clone(),
            timeline.clone(),
            db_prefix,
        );
        let reclamation = ReclamationReporter::new(stats.clone(), gc_hints);
        let splitter = Splitter::new(
            nodes.clone(),
            router.clone(),
            timeline.clone(),
            candidates.clone(),
            stats.clone(),
            reclamation.clone(),
        );
        let merger = Merger::new(
            nodes,
            structural_nodes.clone(),
            router,
            timeline,
            mon.clone(),
            *candidates.policy(),
            *candidates.inline(),
            stats.clone(),
            reclamation.clone(),
        );
        let recovery_wake = Arc::new(Notify::new());
        let changes = ChangeLifecycle::new(
            recovery.clone(),
            topology,
            structural_nodes,
            reconciler.clone(),
            reclamation,
            recovery_wake.clone(),
            splitter.clone(),
            merger.clone(),
        );
        Restructurer {
            bg,
            mon,
            candidates,
            stats,
            splitter,
            merger,
            changes,
            reconciler,
            recovery,
            recovery_wake,
        }
    }

    /// Runs one sweep: split or merge every queued candidate. Best-effort — a
    /// transient error on one candidate only defers its change to a later
    /// cycle, so it is logged and the sweep continues.
    async fn run_once(&self) {
        let stats = &self.stats;
        for candidate in self.candidates.drain() {
            stats.candidates.fetch_add(1, Ordering::Relaxed);
            let pressure = candidate.cause.is_inline_pressure();
            if pressure {
                stats
                    .inline_pressure_candidates
                    .fetch_add(1, Ordering::Relaxed);
            }
            if let Err(e) = self.process_candidate(&candidate).await {
                tracing::debug!(
                    target: "glassdb::restructurer",
                    path = %candidate.path,
                    error = %e,
                    "structural candidate deferred"
                );
                let discard = pressure
                    && matches!(e, TransError::InvalidInput(_) | TransError::StaleCollection);
                if discard {
                    stats
                        .inline_pressure_discarded
                        .fetch_add(1, Ordering::Relaxed);
                } else if !matches!(e, TransError::InvalidInput(_)) {
                    stats.deferred.fetch_add(1, Ordering::Relaxed);
                    if pressure {
                        stats
                            .inline_pressure_deferred
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    self.candidates.requeue(candidate);
                }
            }
        }
        // Re-drive reconciliations a previous cycle could not land, so parents
        // eventually agree with their children and descent stops relying on
        // right-links.
        for pending in self.reconciler.drain_pending() {
            if let Err(e) = self
                .changes
                .reconcile_parent(&pending.collection, &pending.key, &pending.target, None)
                .await
            {
                tracing::debug!(
                    target: "glassdb::restructurer",
                    error = %e,
                    "parent reconciliation deferred"
                );
            }
        }
    }

    /// Dispatches one candidate to the change that its cause calls for.
    async fn process_candidate(&self, candidate: &MaintenanceCandidate) -> Result<(), TransError> {
        // The candidate's already-aged wound-wait priority becomes the
        // identity of the change.
        let id = candidate.priority.renew();
        match &candidate.cause {
            CandidateCause::Split(reason) => {
                let Some(path) = self.splitter.locate(&candidate.path, reason).await? else {
                    return Ok(());
                };
                let change = PlannedChange::split(&path, reason)?;
                self.changes
                    .run(change, id, StructuralTopology::Owned)
                    .await
            }
            CandidateCause::Underfull => {
                let ObjectPath::Node { collection, token } = &candidate.path else {
                    return Ok(());
                };
                if !self.merger.is_actionable(collection, token).await? {
                    return Ok(());
                }
                let change = PlannedChange::Merge {
                    collection,
                    source: token,
                };
                self.changes
                    .run(change, id, StructuralTopology::Owned)
                    .await
            }
        }
    }

    /// Runs one durable structural-recovery sweep.
    async fn recover_structural_intents(&self) -> Result<bool, TransError> {
        let action = self.recovery.begin_sweep();
        match self.drive_recovery_action(action).await {
            Ok(active) => Ok(active),
            Err(error) => {
                tracing::debug!(
                    target: "glassdb::restructurer",
                    error = %error,
                    "structural recovery action failed"
                );
                Err(error)
            }
        }
    }

    /// Drives one opaque recovery action, and executes the parent splits that
    /// it requests.
    async fn drive_recovery_action(&self, mut action: RecoveryAction) -> Result<bool, TransError> {
        loop {
            match self.recovery.advance(&mut action).await? {
                RecoveryStep::Completed { active, failed } => {
                    return if failed {
                        Err(TransError::Retry)
                    } else {
                        Ok(active)
                    };
                }
                RecoveryStep::SplitParent {
                    path,
                    participant,
                    reason,
                } => {
                    let result = Box::pin(self.changes.split_path(
                        &path,
                        &reason,
                        StructuralTopology::Joined(&participant),
                    ))
                    .await;
                    action.resume_parent_split(result);
                }
            }
        }
    }
}

#[async_trait]
impl TopologySettler for Restructurer {
    /// Completes structural recovery before releasing one topology participant with a final status.
    async fn settle_topology_participant(
        &self,
        collection: &CollectionAddress,
        id: &TxId,
    ) -> Result<(), TransError> {
        let action = self.recovery.begin_participant_settlement(collection, id);
        self.drive_recovery_action(action).await.map(|_| ())
    }
}

#[cfg(test)]
mod tests;
