//! Removal of holder-free tombstones during structural changes (ADR-062).

use std::sync::Arc;
use std::sync::atomic::Ordering;

use glassdb_data::TxId;
use glassdb_storage::{LeafBody, Node};

use crate::gc::GcHints;

use super::stats::Stats;

/// Publishes the statistics and GC hints of acknowledged tombstone
/// reclamation.
#[derive(Clone)]
pub(super) struct ReclamationReporter {
    stats: Arc<Stats>,
    gc_hints: GcHints,
}

impl ReclamationReporter {
    pub(super) fn new(stats: Arc<Stats>, gc_hints: GcHints) -> Self {
        Self { stats, gc_hints }
    }

    /// Records the writers of tombstones that an acknowledged node write
    /// removed. `split_avoided` tells that the reclamation made a split
    /// unnecessary.
    pub(super) fn record(&self, writers: &[TxId], split_avoided: bool) {
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
                *entry
                    .current
                    .writer()
                    .expect("a tombstone always names its writer"),
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
