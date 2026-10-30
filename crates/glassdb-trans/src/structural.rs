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
//! [`TopologyPolicy`] has a rule, as the avoidable time policy does, a third loop
//! gives the measurements of each window to the rule, and queues the leaf
//! changes that the rule asks for (ADR-074).

mod candidates;
mod change;
mod contention;
mod measurements;
mod merge;
mod nodes;
mod paid_rate;
mod policy;
mod reclamation;
mod reconcile;
mod recovery;
mod rule;
mod sink;
mod split;
mod stats;
mod topology;
mod write_spans;

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
use policy::{
    AvoidableTimePolicy, SizeCausesPolicy, TopologyDecisions, TopologySettings, TopologySignals,
};
use reclamation::ReclamationReporter;
use reconcile::ParentReconciler;
use recovery::{RecoveryAction, RecoveryStep, StructuralRecovery};
use split::{SplitReason, Splitter};
use stats::Stats;

pub(crate) use measurements::TypicalTime;
pub(crate) use policy::SignalKind;
pub use rule::TopologyPolicy;
pub use sink::StructuralHintSink;
pub use stats::{DemandMergeStats, InlinePressureStats, RestructurerStats, TopologyTimeStats};
pub(crate) use topology::TopologyMembership;

/// Back off empty structural-intent listings independently of candidates.
const STRUCTURAL_RECOVERY_IDLE_INTERVAL: Duration = Duration::from_secs(600);

/// Bounded compare-and-swap attempts on one contended structural node. When
/// they run out, the caller stops its step, and a later sweep or recovery
/// attempt tries again.
const NODE_CAS_ATTEMPTS: usize = 8;

/// The maximum number of splits, or of merges, of different nodes that one
/// sweep makes at the same time.
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
    // The candidate queue that the restructurer drains.
    candidates: MaintenanceCandidates,
    // The intake of the topology signals, which the coordinator and the other
    // producers receive.
    hints: StructuralHintSink,
    signals: Arc<dyn TopologySignals>,
    stats: Arc<Stats>,
    splitter: Splitter,
    merger: Merger,
    changes: ChangeLifecycle,
    reconciler: ParentReconciler,
    recovery: StructuralRecovery,
    recovery_wake: Arc<Notify>,
}

/// What one topology policy gives the restructurer: the queue that the policy
/// and the engine rules fill, the intake of the topology signals, and the policy
/// behind its two traits.
pub(super) struct TopologyParts {
    sizes: NodeSizePolicy,
    queue: MaintenanceCandidates,
    hints: StructuralHintSink,
    signals: Arc<dyn TopologySignals>,
    decisions: Arc<dyn TopologyDecisions>,
}

/// A split candidate and the node that its split divides.
struct LocatedSplit {
    candidate: MaintenanceCandidate,
    reason: SplitReason,
    path: ObjectPath,
}

/// A merge candidate and the nodes that its merge changes.
struct LocatedMerge {
    candidate: MaintenanceCandidate,
    reason: MergeReason,
    collection: CollectionAddress,
    source: NodeId,
    target: NodeId,
}

/// A candidate and the nodes that its change writes.
enum LocatedChange {
    Split(LocatedSplit),
    Merge(LocatedMerge),
}

/// What the check of one candidate in cached state found.
enum Location {
    Found(LocatedChange),
    /// The change is no longer necessary.
    NotNeeded,
    /// The check failed, so the change defers.
    Failed(MaintenanceCandidate, TransError),
}

/// The source that a merge drains, and the target that receives its entries.
struct MergeNodes {
    collection: CollectionAddress,
    source: NodeId,
    target: NodeId,
}

impl TopologyParts {
    /// The parts of `policy`, over nodes of `sizes` and values of `inline`,
    /// for the database instance of `mon`.
    pub(super) fn new(
        policy: TopologyPolicy,
        sizes: NodeSizePolicy,
        inline: InlinePolicy,
        mon: Monitor,
    ) -> Self {
        let queue = MaintenanceCandidates::new();
        let settings = TopologySettings { sizes, inline };
        match policy {
            TopologyPolicy::SizeCauses => {
                let policy = SizeCausesPolicy::new(settings, mon, queue.clone());
                Self::of(queue, sizes, Arc::new(policy))
            }
            TopologyPolicy::AvoidableTime => {
                let policy = AvoidableTimePolicy::new(settings, queue.clone());
                Self::of(queue, sizes, Arc::new(policy))
            }
        }
    }

    fn of<P>(queue: MaintenanceCandidates, sizes: NodeSizePolicy, policy: Arc<P>) -> Self
    where
        P: TopologySignals + TopologyDecisions + 'static,
    {
        Self {
            sizes,
            hints: StructuralHintSink::new(queue.clone(), sizes, policy.clone()),
            queue,
            signals: policy.clone(),
            decisions: policy,
        }
    }
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
        let parts = TopologyParts::new(topology, policy, inline, mon.clone());
        let coord = LeafCoordinator::with_hinter(
            nodes.clone(),
            key_state.clone(),
            mon.clone(),
            retry,
            policy,
            Arc::new(parts.hints.clone()),
        );
        let restructurer = Restructurer::with_parts(
            bg,
            membership,
            nodes,
            intent_store,
            timeline,
            mon,
            key_state,
            db_prefix,
            coord.clone(),
            parts,
            gc_hints,
        );
        (coord, restructurer)
    }

    /// Returns a producer handle for structural hints decided outside the leaf
    /// coordinator.
    pub fn hint_sink(&self) -> StructuralHintSink {
        self.hints.clone()
    }

    /// Returns and resets the counters of background split and merge
    /// activity, and of the time that transactions reported to the topology
    /// rule.
    pub fn stats_and_reset(&self) -> RestructurerStats {
        let mut stats = self.stats.take();
        stats.topology_time = self.signals.take_topology_time();
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
        if let Some(window) = self.signals.window() {
            let signals = self.signals.clone();
            bg.spawn(async move {
                loop {
                    rt::sleep(window).await;
                    signals.end_window();
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

    /// Creates a restructurer over an explicitly co-wired coordinator and
    /// topology policy.
    #[allow(clippy::too_many_arguments)]
    fn with_parts(
        bg: Weak<Background>,
        topology: TopologyMembership,
        nodes: NodeStore,
        intent_store: StructuralIntentStore,
        timeline: Timeline,
        mon: Monitor,
        key_state: KeyStateResolver,
        db_prefix: DbPrefix,
        coord: LeafCoordinator,
        parts: TopologyParts,
        gc_hints: GcHints,
    ) -> Self {
        let TopologyParts {
            sizes,
            queue: candidates,
            hints,
            signals,
            decisions,
        } = parts;
        let stats = Arc::new(Stats::default());
        let router = TreeRouter::new(nodes.clone(), NonZeroUsize::MIN);
        let structural_nodes =
            StructuralNodeAccess::new(nodes.clone(), mon.clone(), key_state, coord);
        let reconciler = ParentReconciler::new(
            structural_nodes.clone(),
            router.clone(),
            timeline.clone(),
            sizes,
            hints.clone(),
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
            sizes,
            decisions.clone(),
            hints.clone(),
            stats.clone(),
            reclamation.clone(),
        );
        let merger = Merger::new(
            nodes,
            structural_nodes.clone(),
            router,
            timeline,
            mon.clone(),
            sizes,
            decisions,
            hints.clone(),
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
            hints.clone(),
        );
        Restructurer {
            bg,
            mon,
            candidates,
            hints,
            signals,
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
        let (candidates, stale) = self.candidates.drain();
        for candidate in &stale {
            self.count_stale(candidate);
        }
        for candidate in &candidates {
            self.count_candidate(candidate);
        }
        self.change_all(candidates).await;
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

    /// Makes the changes of `candidates` that are still necessary, many at a
    /// time, in the order of the candidates. The changes of the same node run
    /// one after the other, and a merge of a node runs after its splits.
    async fn change_all(&self, candidates: Vec<MaintenanceCandidate>) {
        let changes = self.locate_all(candidates).await;
        // A split gates its node, and a merge gates its source and reserves
        // its target (ADR-073). The gate or the reservation of one change
        // stops another change of the same node. The nodes come from cached
        // state, and a change of a later round can locate other nodes, so a
        // change of a round can still meet another one, and defer. A change
        // of a later round waits for all of the earlier round, so it can get
        // stale while it waits.
        for (index, round) in rounds_of_changes(changes).into_iter().enumerate() {
            for batch in self.batches(round) {
                // The changes of an earlier round changed the nodes of this
                // one. A merge of a drained or split source then stops, or
                // pays again for the new leaves, before it writes.
                let batch = if index == 0 {
                    batch
                } else {
                    self.locate_again(batch).await
                };
                self.change_batch(batch).await;
            }
        }
    }

    /// Returns the changes of `candidates` that cached state shows as
    /// actionable, in the order of the candidates. Settles the candidates
    /// whose check failed.
    async fn locate_all(&self, candidates: Vec<MaintenanceCandidate>) -> Vec<LocatedChange> {
        let locating: Vec<_> = candidates
            .into_iter()
            .map(|candidate| self.locate(candidate))
            .collect();
        let mut changes = Vec::new();
        for located in join_all_bounded(locating, STRUCTURAL_CHANGE_PARALLELISM).await {
            match located {
                Location::Found(change) => changes.push(change),
                Location::NotNeeded => {}
                Location::Failed(candidate, error) => self.settle_candidate(candidate, Err(error)),
            }
        }
        changes
    }

    /// Finds the nodes that the change of `candidate` writes, or returns the
    /// candidate with the error that stopped the check.
    async fn locate(&self, candidate: MaintenanceCandidate) -> Location {
        match candidate.cause.clone() {
            CandidateCause::Split(reason) => {
                match self.splitter.locate(&candidate.path, &reason).await {
                    Ok(Some(path)) => Location::Found(LocatedChange::Split(LocatedSplit {
                        candidate,
                        reason,
                        path,
                    })),
                    Ok(None) => Location::NotNeeded,
                    Err(error) => Location::Failed(candidate, error),
                }
            }
            CandidateCause::Merge(reason) => {
                match self.locate_merge(&candidate.path, reason).await {
                    Ok(Some(nodes)) => Location::Found(LocatedChange::Merge(LocatedMerge {
                        candidate,
                        reason,
                        collection: nodes.collection,
                        source: nodes.source,
                        target: nodes.target,
                    })),
                    Ok(None) => Location::NotNeeded,
                    Err(error) => Location::Failed(candidate, error),
                }
            }
        }
    }

    /// Checks the changes of `batch` again in cached state. The changes of an
    /// earlier round can have moved their nodes.
    async fn locate_again(&self, batch: Vec<LocatedChange>) -> Vec<LocatedChange> {
        let candidates = batch
            .into_iter()
            .map(LocatedChange::into_candidate)
            .collect();
        self.locate_all(candidates).await
    }

    /// Returns the nodes that the merge of the node at `path` changes, if
    /// `reason` still calls for it.
    async fn locate_merge(
        &self,
        path: &ObjectPath,
        reason: MergeReason,
    ) -> Result<Option<MergeNodes>, TransError> {
        let ObjectPath::Node {
            collection,
            id: source,
        } = path
        else {
            return Ok(None);
        };
        let target = self
            .merger
            .actionable_target(collection, source, reason)
            .await?;
        Ok(target.map(|target| MergeNodes {
            collection: collection.clone(),
            source: *source,
            target,
        }))
    }

    /// Makes the changes of `batch`, which write different nodes, at the same
    /// time.
    async fn change_batch(&self, batch: Vec<LocatedChange>) {
        let changes: Vec<_> = batch
            .iter()
            .map(|change| {
                let (change, candidate) = match change {
                    LocatedChange::Split(split) => (
                        PlannedChange::split(&split.path, &split.reason)?,
                        &split.candidate,
                    ),
                    LocatedChange::Merge(merge) => (
                        PlannedChange::Merge {
                            collection: &merge.collection,
                            source: &merge.source,
                            reason: merge.reason,
                        },
                        &merge.candidate,
                    ),
                };
                Ok(SweepChange {
                    change,
                    // The candidate's already-aged wound-wait priority
                    // becomes the identity of the change.
                    worker: candidate.priority.renew(),
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
            .map(
                |change: &Result<SweepChange<'_>, TransError>| match change {
                    Ok(_) => results.next().expect("one result for each change"),
                    Err(error) => Err(error.clone()),
                },
            )
            .collect();
        for (change, result) in batch.into_iter().zip(results) {
            self.settle_candidate(change.into_candidate(), result);
        }
    }

    /// Divides the changes of one round into batches, and drops the stale
    /// candidates (ADR-083) just before each batch runs.
    fn batches(&self, round: Vec<LocatedChange>) -> impl Iterator<Item = Vec<LocatedChange>> {
        // A participant stays in the collection record until the changes of
        // its batch end, so a batch is not larger than the changes that run
        // at the same time. One sweep can take longer than the age of a stale
        // request, so each batch checks the age again.
        let mut round = round.into_iter().peekable();
        std::iter::from_fn(move || {
            round.peek()?;
            Some(
                round
                    .by_ref()
                    .filter(|change| !self.drop_if_stale(change.candidate()))
                    .take(STRUCTURAL_CHANGE_PARALLELISM.get())
                    .collect(),
            )
        })
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

/// Puts each of `changes` in the first round after the rounds of the earlier
/// changes of its nodes, so that a round changes a node at most once. The
/// changes of one node keep their order, except that a merge runs after the
/// splits of its nodes. Each round keeps the order of `changes`.
fn rounds_of_changes(changes: Vec<LocatedChange>) -> Vec<Vec<LocatedChange>> {
    // A merge of a leaf that also needs a split waits for the split. The
    // split removes the cause, and the merge then pays again for the leaves
    // that the split made (ADR-076).
    let mut changes: Vec<_> = changes.into_iter().enumerate().collect();
    changes.sort_by_key(|(_, change)| matches!(change, LocatedChange::Merge(_)));
    let mut rounds = Vec::<Vec<(usize, LocatedChange)>>::new();
    let mut next_round_of_node = BTreeMap::<ObjectPath, usize>::new();
    for (order, change) in changes {
        let nodes = change.nodes();
        let round = nodes
            .iter()
            .filter_map(|node| next_round_of_node.get(node).copied())
            .max()
            .unwrap_or_default();
        for node in nodes {
            next_round_of_node.insert(node, round + 1);
        }
        if round == rounds.len() {
            rounds.push(Vec::new());
        }
        rounds[round].push((order, change));
    }
    rounds
        .into_iter()
        .map(|mut round| {
            round.sort_by_key(|(order, _)| *order);
            round.into_iter().map(|(_, change)| change).collect()
        })
        .collect()
}

impl LocatedChange {
    fn candidate(&self) -> &MaintenanceCandidate {
        match self {
            Self::Split(split) => &split.candidate,
            Self::Merge(merge) => &merge.candidate,
        }
    }

    fn into_candidate(self) -> MaintenanceCandidate {
        match self {
            Self::Split(split) => split.candidate,
            Self::Merge(merge) => merge.candidate,
        }
    }

    /// Returns the nodes that the change gates or reserves.
    fn nodes(&self) -> Vec<ObjectPath> {
        match self {
            Self::Split(split) => vec![split.path.clone()],
            Self::Merge(merge) => [merge.source, merge.target]
                .map(|id| ObjectPath::Node {
                    collection: merge.collection.clone(),
                    id,
                })
                .to_vec(),
        }
    }
}

#[cfg(test)]
mod tests;
