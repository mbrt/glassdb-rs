//! The feed of nodes that may need a structural change.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use glassdb_concurr::rt;
use glassdb_data::{ObjectPath, TxId};
use glassdb_storage::{InlinePolicy, LeafBody, Node, NodeSizePolicy};
use tokio::sync::Notify;

use crate::leaf_coord::StructuralHinter;

use super::split::SplitReason;
use super::{CANDIDATE_COALESCING_DELAY, CANDIDATE_QUEUE_CAP, SWEEP_INTERVAL};

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
    /// The node is below an underfull threshold, so it can merge into its
    /// right sibling (ADR-073).
    Underfull,
}

impl StructuralHintSink {
    /// Records recoverable aggregate inline pressure for authoritative
    /// revalidation by the restructurer.
    pub(crate) fn observe_inline_pressure(&self, path: &ObjectPath, key: &[u8], value_len: usize) {
        if !self.candidates.inline.admits_value(value_len) {
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
            CandidateCause::Underfull => 3,
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
    /// policies.
    pub(super) fn with_policies(policy: NodeSizePolicy, inline: InlinePolicy) -> Self {
        MaintenanceCandidates {
            policy,
            inline,
            queue: Arc::new(Mutex::new(VecDeque::new())),
            queued: Arc::new(Notify::new()),
        }
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
        candidates.sort_by_key(|candidate| matches!(candidate.cause, CandidateCause::Underfull));
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
                cause: CandidateCause::Underfull,
            });
        }
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
    /// Coalesces same-path, same-cause observations without sacrificing the
    /// oldest structural priority or the largest requested headroom.
    fn coalesce(&mut self, other: MaintenanceCandidate) {
        if other.priority.older(&self.priority) {
            self.priority = other.priority.clone();
        }
        if let (
            CandidateCause::Split(SplitReason::InlinePressure { key, value_len }),
            CandidateCause::Split(SplitReason::InlinePressure {
                key: other_key,
                value_len: other_len,
            }),
        ) = (&mut self.cause, other.cause)
            && other_len > *value_len
        {
            *key = other_key;
            *value_len = other_len;
        }
    }
}

impl StructuralHinter for MaintenanceCandidates {
    /// Records that `path`'s leaf, now holding `entries`, may be a split
    /// candidate: over either the entry-count or the encoded-byte soft cap. A
    /// node needs at least two entries to be divisible, so a single hot key is
    /// never enqueued however large. The byte size is a hint the restructurer
    /// re-checks authoritatively against the full node (which adds a little
    /// framing), so this need not account for it. A non-root leaf with few
    /// live entries is a merge candidate instead. The oldest hint is dropped
    /// when the queue is full.
    fn observe_leaf(&self, path: &ObjectPath, entries: &LeafBody) {
        let over_cap = entries.len() >= 2
            && (entries.len() > self.policy.leaf_max_entries()
                || entries.encoded_len() > self.policy.node_soft_max_bytes());
        let cause = if over_cap {
            CandidateCause::Split(SplitReason::SoftCap)
        } else if matches!(path, ObjectPath::Node { .. })
            && entries.entries().filter(|entry| entry.exists()).count()
                < self.policy.leaf_min_entries()
        {
            CandidateCause::Underfull
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
}
