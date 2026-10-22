//! Background structural changes of the B-link coordination tree: splits of
//! over-full nodes (ADR-031) and merges of underfull nodes into their right
//! sibling (ADR-073).
//!
//! The [`Restructurer`] schedules the work. It drains the candidate feed, and
//! gives each candidate to the split or the merge module, which re-checks it
//! against cached state. Both kinds of change then run through one lifecycle:
//! a structural intent written ahead of the change, a one-node structural
//! gate, and parent reconciliation after the change. An independent loop
//! completes the changes that a crash or an error interrupted. When the
//! [`TopologyPolicy`] has a rule, as the default policy does, a third loop
//! gives the measurements of each window to the rule, and queues the leaf
//! changes that the rule asks for (ADR-074).

mod candidates;
mod change;
mod measurements;
mod merge;
mod nodes;
mod paid_rate;
mod reclamation;
mod reconcile;
mod recovery;
mod rule;
mod split;
mod stats;
mod topology;

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use glassdb_concurr::{Background, ScanCadence, join_all_bounded, rt};
use glassdb_data::{CollectionAddress, DbPrefix, NodeId, ObjectPath, TxId};
use glassdb_storage::{
    InlinePolicy, NodeSizePolicy, NodeStore, StructuralIntentStore, Timeline, TreeRouter,
};
use tokio::sync::Notify;

use crate::collections::TopologySettler;
use crate::error::TransError;
use crate::gc::GcHints;
use crate::key_state_resolver::KeyStateResolver;
use crate::leaf_coord::LeafCoordinator;
use crate::monitor::Monitor;
use crate::retry_timing::RetryTiming;

use candidates::{CandidateCause, MaintenanceCandidate, MaintenanceCandidates};
use change::{ChangeLifecycle, PlannedChange, StructuralTopology, SweepChange};
use merge::MergeReason;
use merge::Merger;
use nodes::StructuralNodeAccess;
use reclamation::ReclamationReporter;
use reconcile::ParentReconciler;
use recovery::{RecoveryAction, RecoveryStep, StructuralRecovery};
use rule::{TOPOLOGY_WINDOW, TopologyRule};
use split::{SplitReason, Splitter};
use stats::Stats;

pub use candidates::StructuralHintSink;
pub(crate) use measurements::TypicalTime;
pub use rule::TopologyPolicy;
pub use stats::{DemandMergeStats, InlinePressureStats, RestructurerStats, TopologyTimeStats};
pub(crate) use topology::TopologyMembership;

/// Back off empty structural-intent listings independently of candidates.
const STRUCTURAL_RECOVERY_IDLE_INTERVAL: Duration = Duration::from_secs(600);

/// Bounded compare-and-swap attempts on one contended structural node. When
/// they run out, the caller stops its step, and a later sweep or recovery
/// attempt tries again.
const NODE_CAS_ATTEMPTS: usize = 8;

/// The maximum number of splits of different nodes that one sweep makes at
/// the same time.
const STRUCTURAL_CHANGE_PARALLELISM: NonZeroUsize = NonZeroUsize::new(16).unwrap();

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
    topology_policy: TopologyPolicy,
}

/// A split candidate and the node that its split divides.
struct LocatedSplit {
    candidate: MaintenanceCandidate,
    reason: SplitReason,
    path: ObjectPath,
}

impl Restructurer {
    /// Builds a restructurer and coordinator that share one timeline and
    /// candidate feed.
    #[allow(clippy::too_many_arguments)]
    pub fn with_coordinator(
        bg: Weak<Background>,
        membership: TopologyMembership,
        nodes: NodeStore,
        intent_store: StructuralIntentStore,
        timeline: Timeline,
        mon: Monitor,
        key_state: KeyStateResolver,
        retry: RetryTiming,
        db_prefix: DbPrefix,
        policy: NodeSizePolicy,
        inline: InlinePolicy,
        topology: TopologyPolicy,
        gc_hints: GcHints,
    ) -> (LeafCoordinator, Self) {
        let candidates = if topology.rule().is_some() {
            MaintenanceCandidates::for_topology_rule(policy, inline)
        } else {
            MaintenanceCandidates::with_policies(policy, inline)
        };
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
            membership,
            nodes,
            intent_store,
            timeline,
            mon,
            key_state,
            db_prefix,
            coord.clone(),
            candidates,
            gc_hints,
            topology,
        );
        (coord, restructurer)
    }

    /// Returns a producer handle for structural hints decided outside the leaf
    /// coordinator.
    pub fn hint_sink(&self) -> StructuralHintSink {
        self.candidates.hint_sink()
    }

    /// Returns and resets the counters of background split and merge
    /// activity, and of the time that transactions reported to the topology
    /// rule.
    pub fn stats_and_reset(&self) -> RestructurerStats {
        let mut stats = self.stats.take();
        if let Some(measurements) = self.candidates.measurements() {
            stats.topology_time = measurements.take_totals();
        }
        stats
    }

    /// Starts independent candidate, structural-recovery, and topology rule
    /// loops.
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
        if let Some(mut rule) = self.topology_policy.rule() {
            let restructurer = self.clone();
            bg.spawn(async move {
                loop {
                    rt::sleep(TOPOLOGY_WINDOW).await;
                    restructurer.decide_leaf_changes(rule.as_mut());
                }
            });
        }
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
        topology: TopologyMembership,
        nodes: NodeStore,
        intent_store: StructuralIntentStore,
        timeline: Timeline,
        mon: Monitor,
        key_state: KeyStateResolver,
        db_prefix: DbPrefix,
        coord: LeafCoordinator,
        candidates: MaintenanceCandidates,
        gc_hints: GcHints,
        topology_policy: TopologyPolicy,
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
            candidates.clone(),
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
            candidates.clone(),
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
            topology_policy,
        }
    }

    /// Gives the measurements of the window that ends now to `rule`, and
    /// queues the leaf changes that it asks for.
    fn decide_leaf_changes(&self, rule: &mut dyn TopologyRule) {
        let Some(measurements) = self.candidates.measurements() else {
            return;
        };
        for request in rule.decide(&measurements.take_window()) {
            self.candidates.push_request(request);
        }
    }

    /// Runs one sweep: split or merge every queued candidate. Best-effort — a
    /// transient error on one candidate only defers its change to a later
    /// cycle, so it is logged and the sweep continues.
    async fn run_once(&self) {
        let mut splits = Vec::new();
        let mut merges = Vec::new();
        let (candidates, stale) = self.candidates.drain();
        for candidate in &stale {
            self.count_stale(candidate);
        }
        for candidate in candidates {
            self.count_candidate(&candidate);
            match &candidate.cause {
                CandidateCause::Split(reason) => {
                    let reason = reason.clone();
                    splits.push((candidate, reason));
                }
                CandidateCause::Merge(reason) => {
                    let reason = *reason;
                    merges.push((candidate, reason));
                }
            }
        }
        self.split_all(splits).await;
        // A merge changes two nodes and reserves its target (ADR-073), so the
        // merges run in sequence after the splits.
        for (candidate, reason) in merges {
            if self.drop_if_stale(&candidate) {
                continue;
            }
            let result = self.merge(&candidate, reason).await;
            self.settle_candidate(candidate, result);
        }
        // Re-drive reconciliations a previous cycle could not land, so parents
        // eventually agree with their children and descent stops relying on
        // right-links.
        let mut pending = BTreeMap::<CollectionAddress, Vec<(Vec<u8>, NodeId)>>::new();
        for route in self.reconciler.drain_pending() {
            pending
                .entry(route.collection)
                .or_default()
                .push((route.key, route.target));
        }
        for (collection, routes) in pending {
            if let Err(failed) = self
                .changes
                .reconcile_parents(&collection, routes, None)
                .await
            {
                tracing::debug!(
                    target: "glassdb::restructurer",
                    error = %failed.error,
                    "parent reconciliation deferred"
                );
            }
        }
    }

    /// Makes the splits of `candidates` that are still necessary, many at a
    /// time. The candidates that locate the same node split it one after the
    /// other, in the order of the candidates.
    async fn split_all(&self, candidates: Vec<(MaintenanceCandidate, SplitReason)>) {
        let locating: Vec<_> = candidates
            .iter()
            .map(|(candidate, reason)| self.splitter.locate(&candidate.path, reason))
            .collect();
        let located = join_all_bounded(locating, STRUCTURAL_CHANGE_PARALLELISM).await;
        // Splits of the same node compete for its gate, so each round splits
        // a node at most once.
        let mut rounds = Vec::<Vec<LocatedSplit>>::new();
        let mut rounds_of_node = BTreeMap::<ObjectPath, usize>::new();
        for ((candidate, reason), located) in candidates.into_iter().zip(located) {
            match located {
                Ok(Some(path)) => {
                    let round = rounds_of_node.entry(path.clone()).or_default();
                    if *round == rounds.len() {
                        rounds.push(Vec::new());
                    }
                    rounds[*round].push(LocatedSplit {
                        candidate,
                        reason,
                        path,
                    });
                    *round += 1;
                }
                Ok(None) => {}
                Err(error) => self.settle_candidate(candidate, Err(error)),
            }
        }
        for round in rounds {
            // A participant stays in the collection record until the changes
            // of its batch end, so a batch is not larger than the changes
            // that run at the same time.
            let mut round = round.into_iter().peekable();
            while round.peek().is_some() {
                // One sweep can take longer than the age of a stale request,
                // so each batch checks the age again.
                let batch: Vec<_> = round
                    .by_ref()
                    .filter(|split| !self.drop_if_stale(&split.candidate))
                    .take(STRUCTURAL_CHANGE_PARALLELISM.get())
                    .collect();
                self.split_batch(batch).await;
            }
        }
    }

    /// Makes the splits of `batch`, each of a different node, at the same
    /// time.
    async fn split_batch(&self, batch: Vec<LocatedSplit>) {
        let changes: Vec<_> = batch
            .iter()
            .map(|split| {
                PlannedChange::split(&split.path, &split.reason).map(|change| SweepChange {
                    change,
                    // The candidate's already-aged wound-wait priority
                    // becomes the identity of the change.
                    worker: split.candidate.priority.renew(),
                })
            })
            .collect();
        let planned: Vec<_> = changes.iter().filter_map(|c| c.as_ref().ok()).collect();
        let mut results = self
            .changes
            .run_sweep(planned, STRUCTURAL_CHANGE_PARALLELISM)
            .await
            .into_iter();
        let results: Vec<_> = changes
            .iter()
            .map(|change| match change {
                Ok(_) => results.next().expect("one result for each change"),
                Err(error) => Err(error.clone()),
            })
            .collect();
        for (split, result) in batch.into_iter().zip(results) {
            self.settle_candidate(split.candidate, result);
        }
    }

    /// Merges the node of `candidate` into its right sibling, if `reason`
    /// still calls for it.
    async fn merge(
        &self,
        candidate: &MaintenanceCandidate,
        reason: MergeReason,
    ) -> Result<(), TransError> {
        let ObjectPath::Node {
            collection,
            id: source,
        } = &candidate.path
        else {
            return Ok(());
        };
        if !self
            .merger
            .is_actionable(collection, source, reason)
            .await?
        {
            return Ok(());
        }
        let change = PlannedChange::Merge {
            collection,
            source,
            reason,
        };
        // The candidate's already-aged wound-wait priority becomes the
        // identity of the change.
        self.changes
            .run(
                change,
                candidate.priority.renew(),
                StructuralTopology::Owned,
            )
            .await
    }

    /// Counts one candidate that a sweep processes.
    fn count_candidate(&self, candidate: &MaintenanceCandidate) {
        self.stats.candidates.fetch_add(1, Ordering::Relaxed);
        if candidate.cause.is_inline_pressure() {
            self.stats
                .inline_pressure_candidates
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Counts `candidate` as dropped if it is stale (ADR-083), and tells if it
    /// is.
    fn drop_if_stale(&self, candidate: &MaintenanceCandidate) -> bool {
        let stale = candidate.is_stale();
        if stale {
            self.count_stale(candidate);
        }
        stale
    }

    /// Counts one stale demand candidate that a sweep dropped.
    fn count_stale(&self, candidate: &MaintenanceCandidate) {
        self.stats.stale_demand.fetch_add(1, Ordering::Relaxed);
        if matches!(
            candidate.cause,
            CandidateCause::Merge(MergeReason::Demand { .. })
        ) {
            self.stats
                .demand_merge_stale
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Requeues `candidate` when a transient error deferred its change.
    fn settle_candidate(&self, candidate: MaintenanceCandidate, result: Result<(), TransError>) {
        let Err(e) = result else {
            return;
        };
        let stats = &self.stats;
        let pressure = candidate.cause.is_inline_pressure();
        tracing::debug!(
            target: "glassdb::restructurer",
            path = %candidate.path,
            error = %e,
            "structural candidate deferred"
        );
        let discard =
            pressure && matches!(e, TransError::InvalidInput(_) | TransError::StaleCollection);
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
