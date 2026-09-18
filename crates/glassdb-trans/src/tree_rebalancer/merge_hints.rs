//! Bounded evidence of direct commits missed across a pair of leaves.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use glassdb_concurr::rt;
use glassdb_data::{ObjectPath, TxId};
use glassdb_storage::{InlinePolicy, Node, SplitPolicy};

const MAX_PAIRS: usize = 128;
const MIN_MISSES: usize = 32;
const WAIT: Duration = Duration::from_secs(30);
const WINDOW: Duration = Duration::from_secs(60);

#[derive(Clone, Default)]
pub(super) struct MergeHints(Arc<State>);

#[derive(Default)]
struct State {
    candidates: Mutex<BTreeMap<[ObjectPath; 2], MergeCandidate>>,
    observations: AtomicU64,
}

pub(super) struct MergeCandidate {
    pub(super) paths: [ObjectPath; 2],
    identities: BTreeSet<TxId>,
    first_seen: rt::Instant,
    output_bytes: usize,
}

impl MergeHints {
    /// Records a possible direct commit without delaying its logged fallback.
    pub(super) fn observe(
        &self,
        id: &TxId,
        paths: [&ObjectPath; 2],
        nodes: [&Node; 2],
        output_bytes: usize,
    ) {
        let [
            ObjectPath::Node { collection: a, .. },
            ObjectPath::Node { collection: b, .. },
        ] = paths
        else {
            return;
        };
        if a != b || paths[0] == paths[1] || nodes.iter().any(|node| node.as_leaf().is_none()) {
            return;
        }
        let Ok(mut candidates) = self.0.candidates.try_lock() else {
            return;
        };
        let now = rt::Instant::now();
        candidates.retain(|_, candidate| now.duration_since(candidate.first_seen) < WINDOW);
        let mut paths = paths.map(Clone::clone);
        paths.sort();
        if !candidates.contains_key(&paths) && candidates.len() >= MAX_PAIRS {
            return;
        }
        let candidate = candidates
            .entry(paths.clone())
            .or_insert_with(|| MergeCandidate {
                paths,
                identities: BTreeSet::new(),
                first_seen: now,
                output_bytes: 0,
            });
        candidate.output_bytes = candidate.output_bytes.max(output_bytes);
        if candidate.identities.len() < MIN_MISSES && candidate.identities.insert(id.clone()) {
            self.0.observations.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Removes one mature candidate so every attempt requires new evidence.
    pub(super) fn take(&self) -> Option<MergeCandidate> {
        let mut candidates = self.0.candidates.lock().unwrap();
        let now = rt::Instant::now();
        candidates.retain(|_, candidate| now.duration_since(candidate.first_seen) < WINDOW);
        let paths = candidates
            .values()
            .find(|candidate| {
                candidate.identities.len() >= MIN_MISSES
                    && now.duration_since(candidate.first_seen) >= WAIT
            })?
            .paths
            .clone();
        candidates.remove(&paths)
    }

    pub(super) fn observations_and_reset(&self) -> u64 {
        self.0.observations.swap(0, Ordering::Relaxed)
    }
}

impl MergeCandidate {
    /// Checks current spare capacity for a hinted merge.
    pub(super) fn permits(
        &self,
        nodes: [&Node; 2],
        split: SplitPolicy,
        inline: InlinePolicy,
    ) -> bool {
        if !inline.admits_value(0) {
            return false;
        }
        let mut entries = 0usize;
        let mut content_bytes = 0usize;
        let mut inline_bytes = self.output_bytes;
        for node in nodes {
            let Some(leaf) = node.as_leaf() else {
                return false;
            };
            if node.structural_gate().intent().is_some() {
                return false;
            }
            entries = entries.saturating_add(leaf.len());
            content_bytes = content_bytes.saturating_add(node.content_encoded_len());
            for entry in leaf.entries() {
                inline_bytes = inline_bytes.saturating_add(entry.current.inline_len());
            }
        }
        // No replacement credit: the existing inline values must survive the
        // merge, and later pressure still has its ordinary split path.
        entries <= spare_limit(split.leaf_max_entries())
            && content_bytes <= spare_limit(split.node_soft_max_bytes().min(split.content_limit()))
            && inline_bytes <= spare_limit(inline.max_leaf_bytes)
    }
}

fn spare_limit(limit: usize) -> usize {
    limit - limit.div_ceil(4)
}

#[cfg(test)]
mod tests {
    use super::*;
    use glassdb_data::{CollectionAddress, NodeToken};
    use glassdb_storage::{CurrentState, LeafBody, LeafEntry};

    fn paths(pair: u8) -> [ObjectPath; 2] {
        [1, 2].map(|side| ObjectPath::Node {
            collection: CollectionAddress::root("hints"),
            token: NodeToken::from_bytes([pair, side, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
        })
    }

    fn nodes(bytes: usize) -> [Node; 2] {
        [b"a", b"m"].map(|key| {
            Node::leaf(LeafBody::from_entries([LeafEntry::new(key).with_current(
                CurrentState::Inline {
                    writer: TxId::with_priority(1, b"seed"),
                    value: Arc::from(vec![0; bytes]),
                },
            )]))
        })
    }

    fn report(hints: &MergeHints, paths: &[ObjectPath; 2], nodes: &[Node; 2], id: usize) {
        hints.observe(
            &TxId::with_priority(id as u64, b"attempt"),
            [&paths[0], &paths[1]],
            [&nodes[0], &nodes[1]],
            16,
        );
    }

    #[tokio::test(start_paused = true)]
    async fn distinct_misses_and_wait_are_required_for_each_attempt() {
        let hints = MergeHints::default();
        let paths = paths(0);
        let nodes = nodes(8);
        for _ in 0..64 {
            report(&hints, &paths, &nodes, 0);
        }
        rt::sleep(WAIT).await;
        assert!(
            hints.take().is_none(),
            "one repeated identity is not demand"
        );
        for id in 1..MIN_MISSES {
            report(&hints, &paths, &nodes, id);
        }
        assert!(hints.take().is_some());
        assert!(hints.take().is_none(), "an attempt consumes its evidence");
        for id in 0..MIN_MISSES {
            report(&hints, &paths, &nodes, id);
        }
        assert!(hints.take().is_none(), "new evidence needs a new wait");
        rt::sleep(WAIT).await;
        assert!(hints.take().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn hints_expire_and_the_number_of_pairs_is_bounded() {
        let hints = MergeHints::default();
        let nodes = nodes(8);
        for id in 0..MIN_MISSES {
            report(&hints, &paths(0), &nodes, id);
        }
        rt::sleep(WINDOW).await;
        assert!(hints.take().is_none());
        for pair in 0..=MAX_PAIRS {
            for id in 0..MIN_MISSES {
                report(&hints, &paths(pair as u8), &nodes, id);
            }
        }
        rt::sleep(WAIT).await;
        for _ in 0..MAX_PAIRS {
            assert!(hints.take().is_some());
        }
        assert!(hints.take().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn current_values_and_the_largest_output_must_leave_spare_capacity() {
        let hints = MergeHints::default();
        let paths = paths(0);
        let nodes = nodes(8);
        for id in 0..MIN_MISSES {
            report(&hints, &paths, &nodes, id);
        }
        rt::sleep(WAIT).await;
        let selected = hints.take().unwrap();
        let inline = InlinePolicy {
            max_value_bytes: 100,
            max_leaf_bytes: 40,
        };
        assert!(!selected.permits([&nodes[0], &nodes[1]], SplitPolicy::default(), inline));
        assert!(selected.permits(
            [&nodes[0], &nodes[1]],
            SplitPolicy::default(),
            InlinePolicy::default()
        ));
        let split = SplitPolicy::builder().leaf_max_entries(2).build().unwrap();
        assert!(!selected.permits([&nodes[0], &nodes[1]], split, InlinePolicy::default()));
    }
}
