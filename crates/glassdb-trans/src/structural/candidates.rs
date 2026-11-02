//! The queue of nodes that may need a structural change.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use glassdb_concurr::rt;
use glassdb_data::{ObjectPath, TxId};
use tokio::sync::Notify;

use super::merge::MergeReason;
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

/// The queue of nodes that may need a split (ADR-031) or a merge (ADR-073),
/// which the [`Restructurer`](super::Restructurer) drains. The topology policy
/// and the engine rules of the
/// [`StructuralHintSink`](super::StructuralHintSink) queue candidates, and the
/// restructurer checks each one again before its change. Cloneable so the
/// producers and the restructurer share one queue.
#[derive(Clone)]
pub(super) struct MaintenanceCandidates {
    queue: Arc<Mutex<VecDeque<MaintenanceCandidate>>>,
    queued: Arc<Notify>,
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
    /// Creates an empty queue.
    pub(super) fn new() -> Self {
        MaintenanceCandidates {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            queued: Arc::new(Notify::new()),
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
    /// sweep cycle. The splits come first, because a full leaf can stop
    /// writes, and the merges last.
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

    /// Queues the candidate of `path` for `cause`.
    pub(super) fn push_cause(&self, path: &ObjectPath, cause: CandidateCause) {
        self.push(MaintenanceCandidate {
            path: path.clone(),
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

    /// The number of queued inline pressure candidates.
    #[cfg(test)]
    pub(super) fn pending_inline_pressure(&self) -> usize {
        self.queue
            .lock()
            .unwrap()
            .iter()
            .filter(|candidate| candidate.cause.is_inline_pressure())
            .count()
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
    /// requested headroom.
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
                CandidateCause::Split(SplitReason::Contention { key }),
                CandidateCause::Split(SplitReason::Contention { key: other_key }),
            ) => *key = other_key,
            _ => {}
        }
    }
}
