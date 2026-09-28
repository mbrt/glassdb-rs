//! The feed of nodes that may need a structural change.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use glassdb_concurr::rt;
use glassdb_data::{CollectionAddress, NodeId, ObjectPath, TxId};
use glassdb_storage::{InlinePolicy, LeafBody, Node, NodeSizePolicy};
use tokio::sync::Notify;

use crate::access::AccessSet;
use crate::leaf_coord::StructuralHinter;

use super::avoidable::{AvoidableTime, ChangeKind};
use super::merge::MergeReason;
use super::rule::ChangeRequest;
use super::split::SplitReason;

/// Interval of the sweep when no new candidate arrives. Such a sweep retries
/// requeued candidates.
pub(super) const SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// Delay between a new candidate and its sweep. Writes queue candidates in
/// bursts, so that one sweep takes a burst, and continuous writes cause at most
/// one sweep for each delay.
const CANDIDATE_COALESCING_DELAY: Duration = Duration::from_millis(50);

/// Upper bound on the buffered candidate queue. Candidates are only hints: the
/// restructurer reloads and re-checks each one, so dropping the oldest when
/// full merely delays a structural change, never causes an unsafe one.
const CANDIDATE_QUEUE_CAP: usize = 4096;

/// The feed of nodes that may need a split (ADR-031) or a merge (ADR-073),
/// owned by the [`Restructurer`](super::Restructurer). The coordinator observes
/// stored leaves through [`StructuralHinter`], direct-commit admission reports
/// inline pressure through [`StructuralHintSink`], and parent reconciliation
/// reports underfull parents. The restructurer drains and re-checks every
/// cause. Cloneable so the producers and restructurer share one queue and
/// policy.
#[derive(Clone)]
pub(super) struct MaintenanceCandidates {
    policy: NodeSizePolicy,
    inline: InlinePolicy,
    queue: Arc<Mutex<VecDeque<MaintenanceCandidate>>>,
    queued: Arc<Notify>,
    // Present only when a topology rule decides the leaf changes, because
    // nothing else takes its windows.
    avoidable: Option<Arc<AvoidableTime>>,
}

/// Lightweight producer handle for structural hints decided outside the leaf
/// coordinator. Opaque to its holders: they report pressure, never inspect or
/// drive the restructurer's queue.
#[derive(Clone)]
pub struct StructuralHintSink {
    candidates: MaintenanceCandidates,
}

/// One node and the cause for which it may need a structural change.
#[derive(Clone)]
pub(super) struct MaintenanceCandidate {
    pub(super) path: ObjectPath,
    pub(super) priority: TxId,
    pub(super) cause: CandidateCause,
}

/// Why a node is a candidate for a structural change.
#[derive(Clone)]
pub(super) enum CandidateCause {
    Split(SplitReason),
    /// The node can merge into its right sibling (ADR-073).
    Merge(MergeReason),
}

impl StructuralHintSink {
    /// Records recoverable aggregate inline pressure for authoritative
    /// revalidation by the restructurer. A topology rule decides with the
    /// time of the pressure instead.
    pub(crate) fn observe_inline_pressure(&self, path: &ObjectPath, key: &[u8], value_len: usize) {
        if self.candidates.avoidable.is_some() || !self.candidates.inline.admits_value(value_len) {
            return;
        }
        self.candidates.push(MaintenanceCandidate {
            path: path.clone(),
            priority: self.candidates.new_id(),
            cause: CandidateCause::Split(SplitReason::InlinePressure {
                key: key.to_vec(),
                value_len,
            }),
        });
    }

    /// Notes that a direct commit candidate of `path` used `time` more than a
    /// direct commit, because the leaf could not carry its value inline.
    pub(crate) fn inline_pressure_time(&self, path: &ObjectPath, time: Duration) {
        if let Some(avoidable) = &self.candidates.avoidable {
            avoidable.add_inline_pressure(path, time);
        }
    }

    /// Notes that a direct commit candidate with keys in the adjacent leaves
    /// `left` and `right` used `time` more than a direct commit.
    pub(crate) fn adjacent_miss_time(&self, left: &ObjectPath, right: &ObjectPath, time: Duration) {
        if let Some(avoidable) = &self.candidates.avoidable {
            avoidable.add_merge_time(left, right, time);
        }
    }

    /// Tells if a topology rule decides on avoidable time. Only then are the
    /// commit passes of transactions measured.
    pub(crate) fn measures_avoidable_time(&self) -> bool {
        self.candidates.avoidable.is_some()
    }

    /// Notes that a conflict ended a commit pass of a transaction with
    /// `accesses` without a commit, after `time` with the body run before it.
    pub(crate) fn observe_conflict_pass(&self, accesses: &AccessSet, time: Duration) {
        if let Some(avoidable) = &self.candidates.avoidable {
            avoidable.add_conflict_pass(accesses, time);
        }
    }

    /// Notes that a scan that continued from `left` used `time` to read the
    /// adjacent leaf `right`.
    pub(crate) fn scan_crossing_time(&self, left: &ObjectPath, right: &ObjectPath, time: Duration) {
        if let Some(avoidable) = &self.candidates.avoidable {
            avoidable.add_merge_time(left, right, time);
        }
    }

    /// Returns a sink that no restructurer reads, for a component that a test
    /// runs without one.
    #[cfg(test)]
    pub(crate) fn detached() -> Self {
        MaintenanceCandidates::with_policy(NodeSizePolicy::default()).hint_sink()
    }

    #[cfg(test)]
    pub(crate) fn pending_inline_pressure(&self) -> usize {
        self.candidates
            .queue
            .lock()
            .unwrap()
            .iter()
            .filter(|candidate| candidate.cause.is_inline_pressure())
            .count()
    }
}

impl CandidateCause {
    pub(super) fn is_inline_pressure(&self) -> bool {
        matches!(
            self,
            CandidateCause::Split(SplitReason::InlinePressure { .. })
        )
    }

    fn class(&self) -> u8 {
        match self {
            CandidateCause::Split(reason) => reason.class(),
            CandidateCause::Merge(reason) => reason.class(),
        }
    }
}

impl MaintenanceCandidates {
    /// Creates an empty candidate feed with the supplied node size policy.
    #[cfg(test)]
    pub(super) fn with_policy(policy: NodeSizePolicy) -> Self {
        Self::with_policies(policy, InlinePolicy::default())
    }

    /// Creates an empty candidate feed with co-wired node size and inline
    /// policies, where sizes and inline pressure decide leaf changes.
    pub(super) fn with_policies(policy: NodeSizePolicy, inline: InlinePolicy) -> Self {
        Self::new(policy, inline, None)
    }

    /// Creates an empty candidate feed with co-wired node size and inline
    /// policies, where a topology rule decides the leaf changes that sizes do
    /// not force.
    pub(super) fn for_topology_rule(policy: NodeSizePolicy, inline: InlinePolicy) -> Self {
        Self::new(policy, inline, Some(Arc::new(AvoidableTime::new())))
    }

    /// The avoidable time that the producers of the feed report, when a
    /// topology rule decides the leaf changes.
    pub(super) fn avoidable(&self) -> Option<&AvoidableTime> {
        self.avoidable.as_deref()
    }

    /// The node size policy shared by the feed and the restructurer.
    pub(super) fn policy(&self) -> &NodeSizePolicy {
        &self.policy
    }

    /// The inline policy shared by the feed and the restructurer.
    pub(super) fn inline(&self) -> &InlinePolicy {
        &self.inline
    }

    pub(super) fn hint_sink(&self) -> StructuralHintSink {
        StructuralHintSink {
            candidates: self.clone(),
        }
    }

    /// Waits until the next sweep is due: soon after a new candidate, and at
    /// the latest after the sweep interval.
    pub(super) async fn next_sweep(&self) {
        tokio::select! {
            _ = rt::sleep(SWEEP_INTERVAL) => {}
            _ = self.queued.notified() => rt::sleep(CANDIDATE_COALESCING_DELAY).await,
        }
    }

    /// Drains every queued candidate, de-duplicated by path and cause, for one
    /// sweep cycle. Splits come before merges (ADR-073).
    pub(super) fn drain(&self) -> Vec<MaintenanceCandidate> {
        let mut q = self.queue.lock().unwrap();
        let mut by_path = BTreeMap::<(ObjectPath, u8), MaintenanceCandidate>::new();
        while let Some(candidate) = q.pop_front() {
            let key = (candidate.path.clone(), candidate.cause.class());
            match by_path.get_mut(&key) {
                Some(current) => current.coalesce(candidate),
                None => {
                    by_path.insert(key, candidate);
                }
            }
        }
        let mut candidates: Vec<_> = by_path.into_values().collect();
        candidates.sort_by_key(|candidate| matches!(candidate.cause, CandidateCause::Merge(_)));
        candidates
    }

    /// Records that the index node at `path`, now `node`, may be a merge
    /// candidate. The tree root is never merged.
    pub(super) fn observe_index(&self, path: &ObjectPath, node: &Node) {
        let underfull = node
            .as_index()
            .is_some_and(|index| index.len() < self.policy.index_min_children());
        if matches!(path, ObjectPath::Node { .. }) && underfull {
            self.push(MaintenanceCandidate {
                path: path.clone(),
                priority: self.new_id(),
                cause: CandidateCause::Merge(MergeReason::Underfull),
            });
        }
    }

    /// Records that a split or merge of `kind` that landed wrote the leaf `id`.
    pub(super) fn observe_leaf_change(
        &self,
        collection: &CollectionAddress,
        id: NodeId,
        kind: ChangeKind,
    ) {
        if let Some(avoidable) = &self.avoidable {
            let path = ObjectPath::Node {
                collection: collection.clone(),
                id,
            };
            avoidable.record_changed(path, kind);
        }
    }

    /// Returns the number of live entries under which a non-root leaf is
    /// underfull. A topology rule decides the merges for time, but no
    /// transaction may use a leaf with no live entries again, and then the
    /// rule never gets time to merge it.
    pub(super) fn leaf_min_live_entries(&self) -> usize {
        if self.avoidable.is_some() {
            1
        } else {
            self.policy.leaf_min_entries()
        }
    }

    /// Queues one leaf change that a topology rule asked for.
    pub(super) fn push_request(&self, request: ChangeRequest) {
        let (leaf, cause) = match request {
            ChangeRequest::Split(leaf) => (
                leaf,
                CandidateCause::Split(SplitReason::Demand { at: None }),
            ),
            ChangeRequest::SplitAt(leaf, key) => (
                leaf,
                CandidateCause::Split(SplitReason::Demand { at: Some(key) }),
            ),
            ChangeRequest::Merge(leaf) => (leaf, CandidateCause::Merge(MergeReason::Demand)),
        };
        self.push(MaintenanceCandidate {
            path: leaf.into_path(),
            priority: self.new_id(),
            cause,
        });
    }

    /// Requeues a deferred candidate without changing its wound-wait priority.
    /// It does not wake the sweep loop, so that a candidate that fails again
    /// and again does not retry in a tight loop.
    pub(super) fn requeue(&self, candidate: MaintenanceCandidate) {
        self.enqueue(candidate);
    }

    /// Adds one volatile candidate and wakes the sweep loop.
    pub(super) fn push(&self, candidate: MaintenanceCandidate) {
        self.enqueue(candidate);
        self.queued.notify_one();
    }

    /// Mints an operation id at normal transaction priority.
    pub(super) fn new_id(&self) -> TxId {
        TxId::new_at(rt::system_now())
    }

    fn new(
        policy: NodeSizePolicy,
        inline: InlinePolicy,
        avoidable: Option<Arc<AvoidableTime>>,
    ) -> Self {
        MaintenanceCandidates {
            policy,
            inline,
            queue: Arc::new(Mutex::new(VecDeque::new())),
            queued: Arc::new(Notify::new()),
            avoidable,
        }
    }

    /// Adds one candidate while keeping the best-effort feed bounded.
    fn enqueue(&self, candidate: MaintenanceCandidate) {
        let mut q = self.queue.lock().unwrap();
        if q.len() >= CANDIDATE_QUEUE_CAP {
            q.pop_front();
        }
        q.push_back(candidate);
    }
}

impl MaintenanceCandidate {
    /// Coalesces same-path, same-cause observations of `other`, which is newer,
    /// without sacrificing the oldest structural priority or the largest
    /// requested headroom. A demand split keeps the newest split key, because
    /// the leaf changes after each measurement.
    fn coalesce(&mut self, other: MaintenanceCandidate) {
        if other.priority.older(&self.priority) {
            self.priority = other.priority;
        }
        match (&mut self.cause, other.cause) {
            (
                CandidateCause::Split(SplitReason::InlinePressure { key, value_len }),
                CandidateCause::Split(SplitReason::InlinePressure {
                    key: other_key,
                    value_len: other_len,
                }),
            ) if other_len > *value_len => {
                *key = other_key;
                *value_len = other_len;
            }
            (
                CandidateCause::Split(SplitReason::Demand { at }),
                CandidateCause::Split(SplitReason::Demand { at: Some(other_at) }),
            ) => *at = Some(other_at),
            _ => {}
        }
    }
}

impl StructuralHinter for MaintenanceCandidates {
    /// Records that `path`'s leaf, now holding `entries`, may be a split
    /// candidate: over either the entry-count or the encoded-byte soft cap. A
    /// node needs at least two entries to be divisible, so a single hot key is
    /// never enqueued however large. The byte size is a hint the restructurer
    /// re-checks authoritatively against the full node (which adds a little
    /// framing), so this need not account for it. An underfull non-root leaf
    /// is a merge candidate instead. The oldest hint is dropped when the queue
    /// is full.
    fn observe_leaf(&self, path: &ObjectPath, entries: &LeafBody) {
        let over_cap = entries.len() >= 2
            && (entries.len() > self.policy.leaf_max_entries()
                || entries.encoded_len() > self.policy.node_soft_max_bytes());
        let cause = if over_cap {
            CandidateCause::Split(SplitReason::SoftCap)
        } else if matches!(path, ObjectPath::Node { .. })
            && entries.entries().filter(|entry| entry.exists()).count()
                < self.leaf_min_live_entries()
        {
            CandidateCause::Merge(MergeReason::Underfull)
        } else {
            return;
        };
        self.push(MaintenanceCandidate {
            path: path.clone(),
            priority: self.new_id(),
            cause,
        });
    }

    fn capacity_rejected(&self, path: &ObjectPath) {
        self.push(MaintenanceCandidate {
            path: path.clone(),
            priority: self.new_id(),
            cause: CandidateCause::Split(SplitReason::Capacity),
        });
    }

    fn leaf_delay(&self, path: &ObjectPath, time: Duration, split_key: &[u8]) {
        if let Some(avoidable) = &self.avoidable {
            avoidable.add_leaf_delay(path, time, split_key);
        }
    }

    fn measures_leaf_delays(&self) -> bool {
        self.avoidable.is_some()
    }
}
