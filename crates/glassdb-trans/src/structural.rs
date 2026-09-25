//! Background structural changes of the B-link coordination tree: splits of
//! over-full nodes (ADR-031) and merges of underfull nodes into their right
//! sibling (ADR-073).
//!
//! The [`Restructurer`] schedules the work. It drains the candidate feed, and
//! gives each candidate to the split or the merge module, which re-checks it
//! against current state. Both kinds of change share one lifecycle and one
//! context: a structural intent written ahead of the change, a one-node
//! structural gate, and parent reconciliation after the change. An independent
//! loop completes the changes that a crash or an error interrupted.

mod candidates;
mod change;
mod merge;
mod nodes;
mod reconcile;
mod recovery;
mod split;
mod stats;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use glassdb_concurr::{Background, RetryConfig, ScanCadence, rt};
use glassdb_data::{CollectionAddress, DbPrefix, TxId};
use glassdb_storage::{
    CollectionStore, InlinePolicy, NodeSizePolicy, NodeStore, StructuralIntentStore, Timeline,
};

use crate::collections::TopologySettler;
use crate::error::TransError;
use crate::gc::GcHints;
use crate::key_state_resolver::KeyStateResolver;
use crate::leaf_coord::LeafCoordinator;
use crate::monitor::Monitor;

use candidates::{CandidateCause, MaintenanceCandidate, MaintenanceCandidates};
use change::ChangeContext;
use recovery::{RecoveryAction, RecoveryStep};

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
    ctx: ChangeContext,
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
        self.ctx.candidates.hint_sink()
    }

    /// Returns and resets background split and merge activity counters.
    pub fn stats_and_reset(&self) -> RestructurerStats {
        self.ctx.stats.take()
    }

    /// Starts independent candidate and structural-recovery loops.
    pub fn start(&self) {
        let Some(bg) = self.bg.upgrade() else {
            return;
        };
        let restructurer = self.clone();
        bg.spawn(async move {
            loop {
                restructurer.ctx.candidates.next_sweep().await;
                restructurer.run_once().await;
            }
        });
        let recovery = self.clone();
        bg.spawn(async move {
            let minimum = recovery.ctx.mon.protocol_timing().pending_timeout();
            let mut cadence = ScanCadence::new(minimum, STRUCTURAL_RECOVERY_IDLE_INTERVAL);
            loop {
                let delay = match recovery.recover_structural_intents().await {
                    Ok(progress) => {
                        cadence.observe(progress);
                        if recovery.ctx.recovery.has_continuation() {
                            cadence.delay().min(minimum)
                        } else {
                            cadence.delay()
                        }
                    }
                    Err(_) => minimum,
                };
                tokio::select! {
                    _ = rt::sleep(delay) => {}
                    _ = recovery.ctx.recovery_wake.notified() => {}
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
        Restructurer {
            bg,
            ctx: ChangeContext::new(
                records,
                nodes,
                intent_store,
                timeline,
                mon,
                key_state,
                db_prefix,
                coord,
                candidates,
                retry,
                gc_hints,
            ),
        }
    }

    /// Runs one sweep: split or merge every queued candidate. Best-effort — a
    /// transient error on one candidate only defers its change to a later
    /// cycle, so it is logged and the sweep continues.
    async fn run_once(&self) {
        let stats = &self.ctx.stats;
        for candidate in self.ctx.candidates.drain() {
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
                    self.ctx.candidates.requeue(candidate);
                }
            }
        }
        // Re-drive reconciliations a previous cycle could not land, so parents
        // eventually agree with their children and descent stops relying on
        // right-links.
        for pending in self.ctx.reconciler.drain_pending() {
            if let Err(e) = self
                .ctx
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
        let id = candidate.priority.renew();
        match &candidate.cause {
            CandidateCause::Split(reason) => {
                split::split_candidate(&self.ctx, &candidate.path, reason, id).await
            }
            CandidateCause::Underfull => {
                merge::merge_candidate(&self.ctx, &candidate.path, id).await
            }
        }
    }

    /// Runs one durable structural-recovery sweep.
    async fn recover_structural_intents(&self) -> Result<bool, TransError> {
        let action = self.ctx.recovery.begin_sweep();
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
            match self.ctx.recovery.advance(&mut action).await? {
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
                    let result = Box::pin(split::split_path_joined(
                        &self.ctx,
                        &path,
                        &participant,
                        &reason,
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
        let action = self
            .ctx
            .recovery
            .begin_participant_settlement(collection, id);
        self.drive_recovery_action(action).await.map(|_| ())
    }
}

#[cfg(test)]
mod tests;
