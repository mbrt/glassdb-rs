//! A topology policy outside the engine that remembers what paid for the last
//! change of each leaf.

use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;
use std::time::Duration;

use glassdb::{LeafId, TopologyChange, TopologyPolicy, TopologyWindow};

/// The time after which a remembered payment counts half.
const HALF_LIFE: Duration = Duration::from_secs(30);

/// Decides like [`glassdb::AvoidableTimePolicy`], but a change must also pay
/// back the avoidable time that paid for the opposite change at the same leaf
/// boundary. A split and a merge measure different topologies: after a split,
/// the time that the split removed is not measured any more, so without this
/// memory a merge can undo the split for less time than the split saves.
pub(crate) struct MemoryPolicy {
    split_threshold: f64,
    merge_threshold: f64,
    paid: Mutex<Paid>,
}

#[derive(Default)]
struct Paid {
    /// The split-side time that paid for the last split of each leaf. The
    /// split made the boundary between the leaf and its right sibling.
    splits: HashMap<LeafId, Duration>,
    /// The merge-side time that paid for the last merge into each leaf. The
    /// merge removed a boundary inside the leaf.
    merges: HashMap<LeafId, Duration>,
}

impl MemoryPolicy {
    pub(crate) fn new(split_threshold: f64, merge_threshold: f64) -> Self {
        Self {
            split_threshold,
            merge_threshold,
            paid: Mutex::new(Paid::default()),
        }
    }
}

impl TopologyPolicy for MemoryPolicy {
    fn decide(&self, window: &TopologyWindow) -> Vec<TopologyChange> {
        let mut paid = self.paid.lock().unwrap();
        paid.decay(window.elapsed);
        let split_time = window.split_time.mul_f64(self.split_threshold);
        let merge_time = window.merge_time.mul_f64(self.merge_threshold);
        let mut held: BTreeSet<&LeafId> = window
            .leaves
            .iter()
            .filter(|(_, leaf)| leaf.split_recently)
            .map(|(id, _)| id)
            .collect();
        let mut changes = Vec::new();
        for (id, leaf) in &window.leaves {
            let over_cap = leaf
                .size
                .is_some_and(|size| size.over_soft_cap(&window.node_size));
            let split_side = leaf.avoidable.total();
            let keep = split_time + paid.merge(id);
            let pays = !leaf.merged_recently && split_side > keep;
            let at_key = pays && leaf.avoidable.leaf_delays() > keep;
            let change = match &leaf.split_key {
                Some(key) if at_key => TopologyChange::SplitAt(id.clone(), key.clone()),
                _ if pays || over_cap => TopologyChange::Split(id.clone()),
                _ => continue,
            };
            if pays {
                paid.merges.remove(id);
                paid.splits.insert(id.clone(), split_side);
            }
            changes.push(change);
            held.insert(id);
        }
        let split_side = |id: &LeafId| {
            window
                .leaves
                .get(id)
                .map_or(Duration::ZERO, |leaf| leaf.avoidable.total())
        };
        for pair in &window.pairs {
            if held.contains(&pair.left) || held.contains(&pair.right) {
                continue;
            }
            let keep = merge_time
                + split_side(&pair.left)
                + split_side(&pair.right)
                + paid.split(&pair.left);
            let merge_side = pair.avoidable.total();
            if merge_side > keep {
                paid.splits.remove(&pair.left);
                paid.merges.insert(pair.right.clone(), merge_side);
                changes.push(TopologyChange::Merge(pair.left.clone()));
                held.insert(&pair.left);
                held.insert(&pair.right);
            }
        }
        changes
    }
}

impl Paid {
    fn split(&self, id: &LeafId) -> Duration {
        self.splits.get(id).copied().unwrap_or_default()
    }

    fn merge(&self, id: &LeafId) -> Duration {
        self.merges.get(id).copied().unwrap_or_default()
    }

    fn decay(&mut self, elapsed: Duration) {
        let factor = 0.5_f64.powf(elapsed.as_secs_f64() / HALF_LIFE.as_secs_f64());
        for times in [&mut self.splits, &mut self.merges] {
            times.retain(|_, time| {
                *time = time.mul_f64(factor);
                *time >= Duration::from_millis(1)
            });
        }
    }
}
