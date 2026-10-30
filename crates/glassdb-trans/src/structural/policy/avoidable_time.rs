//! The avoidable time policy (ADR-074 to ADR-076).

use std::sync::Mutex;
use std::time::Duration;

use glassdb_data::{CollectionAddress, NodeId, ObjectPath, StructuralIntentId};
use glassdb_storage::{LeafBody, Node, PaidChange};

use crate::access::AccessSet;
use crate::leaf_coord::LeafDelayKind;
use crate::structural::candidates::{CandidateCause, MaintenanceCandidates};
use crate::structural::measurements::{ChangeKind, TopologyMeasurements};
use crate::structural::merge::{MergeReason, MergeVeto};
use crate::structural::paid_rate::PaidRates;
use crate::structural::rule::{
    AvoidableTimeRule, ChangeRequest, RuleRequest, TOPOLOGY_WINDOW, TopologyRule,
};
use crate::structural::split::{SplitReason, SplitTarget};
use crate::structural::stats::TopologyTimeStats;

use super::{
    Interests, MergeDecision, SignalKind, SizeLimitsPolicy, SplitDecision, TopologyDecisions,
    TopologySettings, TopologySignals,
};

/// The age after which a sweep drops a change that the rule asked for
/// (ADR-083). An older request can be for a load that stopped, or for a leaf
/// that changed since. The rule asks again when its time pays for the change
/// again.
pub(crate) const DEMAND_MAX_AGE: Duration = TOPOLOGY_WINDOW.saturating_mul(3);

/// The avoidable time of the transactions of the database instance decides
/// the splits and merges that the sizes do not force (ADR-074). Leaves over a
/// soft cap still split. A topology rule decides only from the time of a
/// leaf, and no transaction may use a leaf with no live entries again, so such
/// a leaf merges as underfull.
pub(crate) struct AvoidableTimePolicy {
    base: SizeLimitsPolicy,
    measurements: TopologyMeasurements,
    paid_rates: PaidRates,
    rule: Mutex<AvoidableTimeRule>,
    queue: MaintenanceCandidates,
}

impl AvoidableTimePolicy {
    /// The avoidable time policy over `settings`, which queues its candidates
    /// in `queue`.
    pub(crate) fn new(settings: TopologySettings, queue: MaintenanceCandidates) -> Self {
        Self {
            base: SizeLimitsPolicy::new(settings, 1, queue.clone()),
            measurements: TopologyMeasurements::new(),
            paid_rates: PaidRates::default(),
            rule: Mutex::default(),
            queue,
        }
    }

    /// The measurements of the current window.
    #[cfg(test)]
    pub(crate) fn measurements(&self) -> &TopologyMeasurements {
        &self.measurements
    }

    /// Reports whether a merge of `left` into `right` that pays `rate` pays
    /// more than the paid rates of a split of the two leaves.
    fn merge_pays(&self, left: &Node, right: &Node, rate: Duration) -> bool {
        self.paid_rates.pays_more(
            PaidChange::Merge,
            rate,
            [left.paid_rate(), right.paid_rate()],
        )
    }

    /// Decides the split that a topology rule asked for, at the key of its
    /// measurement, or at the median without one. It must pay more than the
    /// paid rate of the last merge of the leaf, and its leaves keep its rate
    /// as their paid rate (ADR-076).
    fn demand_split(&self, node: &Node, at: Option<&[u8]>, rate: Duration) -> SplitDecision {
        let divisible = node.as_leaf().is_some_and(|leaf| match at {
            Some(key) => leaf.divides_at(key),
            None => leaf.len() >= 2,
        });
        let pays = self
            .paid_rates
            .pays_more(PaidChange::Split, rate, [node.paid_rate()]);
        if divisible && pays {
            SplitDecision::Split {
                at: at.map(<[u8]>::to_vec),
                paid_rate: Some(rate),
            }
        } else {
            SplitDecision::NotNeeded
        }
    }
}

impl TopologySignals for AvoidableTimePolicy {
    fn interests(&self) -> Interests {
        Interests::none().with(SignalKind::LeafDelays)
    }

    fn observe_leaf(&self, path: &ObjectPath, leaf: &LeafBody) {
        self.base.observe_leaf(path, leaf);
    }

    fn leaf_delay(&self, path: &ObjectPath, time: Duration, split_key: &[u8], kind: LeafDelayKind) {
        self.measurements
            .add_leaf_delay(path, time, split_key, kind);
    }

    fn inline_pressure_time(&self, path: &ObjectPath, time: Duration) {
        self.measurements.add_inline_pressure(path, time);
    }

    fn adjacent_miss_time(&self, left: &ObjectPath, right: &ObjectPath, time: Duration) {
        self.measurements.add_merge_time(left, right, time);
    }

    fn conflict_pass(&self, accesses: &AccessSet, time: Duration) {
        self.measurements.add_conflict_pass(accesses, time);
    }

    fn scan_crossing_time(&self, left: &ObjectPath, right: &ObjectPath, time: Duration) {
        self.measurements.add_merge_time(left, right, time);
    }

    fn window(&self) -> Option<Duration> {
        Some(TOPOLOGY_WINDOW)
    }

    /// Gives the measurements of the window that ends now to the rule, and
    /// queues the leaf changes that it asks for.
    fn end_window(&self) {
        let window = self.measurements.take_window();
        let requests = self.rule.lock().unwrap().decide(&window);
        for request in requests {
            let (path, cause) = request_candidate(request);
            self.queue.push_expiring(&path, cause, DEMAND_MAX_AGE);
        }
    }

    fn take_topology_time(&self) -> TopologyTimeStats {
        self.measurements.take_totals()
    }

    fn leaves_changed(
        &self,
        collection: &CollectionAddress,
        leaves: &[NodeId],
        kind: ChangeKind,
        paid: Option<&StructuralIntentId>,
    ) {
        if let Some(intent) = paid {
            self.paid_rates.landed(intent);
        }
        for id in leaves {
            let path = ObjectPath::Node {
                collection: collection.clone(),
                id: *id,
            };
            self.measurements.record_changed(path, kind);
        }
    }

    fn change_timed(&self, kind: ChangeKind, took: Duration) {
        self.measurements.record_change(kind, took);
    }
}

impl TopologyDecisions for AvoidableTimePolicy {
    fn split(&self, target: SplitTarget<'_>, node: &Node, reason: &SplitReason) -> SplitDecision {
        match reason {
            SplitReason::Demand { at, rate } => self.demand_split(node, at.as_deref(), *rate),
            _ => self.base.split(target, node, reason),
        }
    }

    /// A merge that a topology rule asked for must pay more than the paid
    /// rates of a split of the two leaves (ADR-076), unless the leaf has no
    /// live entries. Such a merge did not pay for itself, so the merged leaf
    /// keeps no paid rate.
    fn merge(&self, left: &Node, right: &Node, reason: MergeReason) -> MergeDecision {
        match reason {
            MergeReason::Demand { rate } => {
                let pays = self.merge_pays(left, right, rate);
                let underfull = left.as_leaf().is_some_and(|leaf| self.base.underfull(leaf));
                if !(pays || underfull) {
                    return MergeDecision::Veto(MergeVeto::Unpaid);
                }
                match self.base.half() {
                    MergeDecision::Within {
                        max_entries,
                        max_inline_bytes,
                        ..
                    } => MergeDecision::Within {
                        max_entries,
                        max_inline_bytes,
                        paid_rate: pays.then_some(rate),
                    },
                    veto => veto,
                }
            }
            _ => self.base.merge(left, right, reason),
        }
    }
}

/// Returns the leaf and the cause of the candidate for one change that the
/// rule asked for.
pub(crate) fn request_candidate(request: RuleRequest) -> (ObjectPath, CandidateCause) {
    let rate = request.rate;
    let (leaf, cause) = match request.change {
        ChangeRequest::Split(leaf) => (
            leaf,
            CandidateCause::Split(SplitReason::Demand { at: None, rate }),
        ),
        ChangeRequest::SplitAt(leaf, key) => (
            leaf,
            CandidateCause::Split(SplitReason::Demand {
                at: Some(key),
                rate,
            }),
        ),
        ChangeRequest::Merge(leaf) => (leaf, CandidateCause::Merge(MergeReason::Demand { rate })),
    };
    (leaf.into_path(), cause)
}
