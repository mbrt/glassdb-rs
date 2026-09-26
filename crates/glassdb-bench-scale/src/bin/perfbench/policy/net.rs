//! A topology policy outside the engine that subtracts the time that a split
//! adds from the avoidable time of a leaf, and decides on a moving average.

use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;
use std::time::Duration;

use glassdb::{LeafId, LeafWindow, TopologyChange, TopologyPolicy, TopologyWindow};

/// Decides like [`glassdb::AvoidableTimePolicy`], with two differences for
/// splits. A split adds time to the transactions whose keys it puts in both
/// halves of the leaf, so the policy subtracts their count, times the mean
/// latency of the transactions of the leaf, times a weight. And the moving
/// average of this net time over the windows must pay for the split, not the
/// time of one window: the avoidable time of one window is noisy, and one
/// window can pay for a split that the load does not keep paying for.
pub(crate) struct NetTimePolicy {
    split_threshold: f64,
    merge_threshold: f64,
    divided_weight: f64,
    half_life: Duration,
    leaves: Mutex<HashMap<LeafId, NetTime>>,
}

/// The moving averages of the net split-side time of one leaf in one window,
/// in seconds. The net time can be negative.
#[derive(Debug, Default)]
struct NetTime {
    /// All split causes, minus the time that a split adds.
    total: f64,
    /// The leaf delays alone, minus the time that a split adds.
    delays: f64,
    /// The split key of the last leaf delay since the leaf changed, because
    /// the delays of the average come from all these windows.
    split_key: Option<Vec<u8>>,
}

impl NetTimePolicy {
    pub(crate) fn new(
        split_threshold: f64,
        merge_threshold: f64,
        divided_weight: f64,
        half_life: Duration,
    ) -> Self {
        Self {
            split_threshold,
            merge_threshold,
            divided_weight,
            half_life,
            leaves: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the time in seconds that a split of the leaf would have added
    /// to the transactions of the window.
    fn added_time(&self, leaf: &LeafWindow) -> f64 {
        let committed = leaf.committed.total();
        if committed == 0 {
            return 0.0;
        }
        let mean_latency = leaf.latency.as_secs_f64() / committed as f64;
        self.divided_weight * leaf.divided.total() as f64 * mean_latency
    }
}

impl TopologyPolicy for NetTimePolicy {
    fn decide(&self, window: &TopologyWindow) -> Vec<TopologyChange> {
        let decay = 0.5_f64.powf(window.elapsed.as_secs_f64() / self.half_life.as_secs_f64());
        let split_time = window
            .split_time
            .mul_f64(self.split_threshold)
            .as_secs_f64();
        let merge_time = window.merge_time.mul_f64(self.merge_threshold);
        let mut leaves = self.leaves.lock().unwrap();
        leaves.retain(|id, net| {
            net.decay(decay);
            window.leaves.contains_key(id) || !net.is_negligible()
        });
        let mut held: BTreeSet<&LeafId> = window
            .leaves
            .iter()
            .filter(|(_, leaf)| leaf.split_recently)
            .map(|(id, _)| id)
            .collect();
        let mut changes = Vec::new();
        for (id, leaf) in &window.leaves {
            // The measurements start again at each change of the leaf.
            if leaf.split_recently || leaf.merged_recently {
                leaves.remove(id);
            }
            let net = leaves.entry(id.clone()).or_default();
            net.add(leaf, self.added_time(leaf), 1.0 - decay);
            let over_cap = leaf
                .size
                .is_some_and(|size| size.over_soft_cap(&window.node_size));
            let pays = !leaf.merged_recently && net.total > split_time;
            let at_key = pays && net.delays > split_time;
            let change = match &net.split_key {
                Some(key) if at_key => TopologyChange::SplitAt(id.clone(), key.clone()),
                _ if pays || over_cap => TopologyChange::Split(id.clone()),
                _ => continue,
            };
            // A split can take more than one window to land. If the average
            // stays, the next window asks for a second split of the same
            // leaf. The engine retries a deferred split itself.
            leaves.remove(id);
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
            let keep = merge_time + split_side(&pair.left) + split_side(&pair.right);
            if pair.avoidable.total() > keep {
                changes.push(TopologyChange::Merge(pair.left.clone()));
                held.insert(&pair.left);
                held.insert(&pair.right);
            }
        }
        changes
    }
}

impl NetTime {
    /// Adds the measurements of `leaf` in one window with `weight`, less
    /// `added` seconds that a split would have added.
    fn add(&mut self, leaf: &LeafWindow, added: f64, weight: f64) {
        self.total += weight * (leaf.avoidable.total().as_secs_f64() - added);
        self.delays += weight * (leaf.avoidable.leaf_delays().as_secs_f64() - added);
        if leaf.split_key.is_some() {
            self.split_key.clone_from(&leaf.split_key);
        }
    }

    fn decay(&mut self, factor: f64) {
        self.total *= factor;
        self.delays *= factor;
    }

    fn is_negligible(&self) -> bool {
        self.total.abs() < 1e-6 && self.delays.abs() < 1e-6
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use glassdb::memory::MemoryBackend;
    use glassdb::{Backend, Database, SplitTime};

    use super::*;

    /// Keeps the last window that has a leaf, and changes nothing.
    struct Capture(Arc<Mutex<Option<TopologyWindow>>>);

    impl TopologyPolicy for Capture {
        fn decide(&self, window: &TopologyWindow) -> Vec<TopologyChange> {
            if !window.leaves.is_empty() {
                *self.0.lock().unwrap() = Some(window.clone());
            }
            Vec::new()
        }
    }

    // Returns a window of one second of a real database instance with one
    // leaf, whose measurements the tests then set.
    async fn one_leaf_window() -> (TopologyWindow, LeafId) {
        let captured = Arc::new(Mutex::new(None));
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let db = Database::builder("netpolicytest", backend)
            .topology_policy(Capture(captured.clone()))
            .open()
            .await
            .unwrap();
        let collection = db
            .root_collection()
            .create_collection_if_absent(b"c")
            .await
            .unwrap();
        collection.write(b"key", b"value").await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        db.shutdown().await;
        let mut window = captured.lock().unwrap().take().unwrap();
        window.elapsed = Duration::from_secs(1);
        let id = window.leaves.keys().next().unwrap().clone();
        window.leaves.retain(|leaf, _| *leaf == id);
        window.pairs.clear();
        (window, id)
    }

    fn policy() -> NetTimePolicy {
        NetTimePolicy::new(0.25, 0.1, 0.25, Duration::from_secs(10))
    }

    fn set(window: &mut TopologyWindow, id: &LeafId, leaf: LeafWindow) {
        window.leaves.insert(id.clone(), leaf);
    }

    fn lost_cas(millis: u64) -> LeafWindow {
        let mut leaf = LeafWindow::default();
        leaf.avoidable = SplitTime {
            lost_cas: Duration::from_millis(millis),
            ..SplitTime::default()
        };
        leaf.split_key = Some(b"key".to_vec());
        leaf
    }

    // Returns the index of the first of `windows` whose decision changes the
    // leaf, if any.
    fn first_change(
        policy: &NetTimePolicy,
        window: &mut TopologyWindow,
        id: &LeafId,
        leaves: impl IntoIterator<Item = LeafWindow>,
    ) -> Option<(usize, Vec<TopologyChange>)> {
        leaves.into_iter().enumerate().find_map(|(index, leaf)| {
            set(window, id, leaf);
            let changes = policy.decide(window);
            (!changes.is_empty()).then_some((index, changes))
        })
    }

    #[tokio::test(start_paused = true)]
    async fn a_leaf_splits_at_its_split_key_when_its_net_time_keeps_paying() {
        let (mut window, id) = one_leaf_window().await;
        let policy = policy();

        let change = first_change(&policy, &mut window, &id, (0..10).map(|_| lost_cas(1000)));

        // The average of one second in each window pays for a quarter of the
        // default split time of half a second in the second window.
        assert_eq!(
            change,
            Some((1, vec![TopologyChange::SplitAt(id, b"key".to_vec())]))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_leaf_does_not_split_again_before_its_split_lands() {
        let (mut window, id) = one_leaf_window().await;
        let policy = policy();
        first_change(&policy, &mut window, &id, (0..2).map(|_| lost_cas(1000))).unwrap();

        let change = first_change(&policy, &mut window, &id, [lost_cas(1000)]);

        assert_eq!(change, None);
    }

    #[tokio::test(start_paused = true)]
    async fn one_window_does_not_pay_for_a_split() {
        let (mut window, id) = one_leaf_window().await;
        let policy = policy();

        let leaves = std::iter::once(lost_cas(1500)).chain((0..30).map(|_| LeafWindow::default()));
        let change = first_change(&policy, &mut window, &id, leaves);

        assert_eq!(change, None);
    }

    #[tokio::test(start_paused = true)]
    async fn transactions_that_a_split_divides_keep_a_leaf_from_splitting() {
        let (mut window, id) = one_leaf_window().await;
        let policy = policy();
        let divided = || {
            let mut leaf = lost_cas(1000);
            leaf.committed.direct = 10;
            leaf.divided.direct = 5;
            leaf.latency = Duration::from_secs(10);
            leaf
        };

        // The 5 divided transactions of one second each add 1.25 seconds.
        let change = first_change(&policy, &mut window, &id, (0..30).map(|_| divided()));

        assert_eq!(change, None);
    }
}
