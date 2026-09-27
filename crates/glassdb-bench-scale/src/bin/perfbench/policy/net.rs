//! A topology policy outside the engine that subtracts the time that a split
//! adds from the avoidable time of a leaf, and decides on a moving average.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;
use std::time::Duration;

use glassdb::{LeafId, LeafWindow, TopologyChange, TopologyPolicy, TopologyWindow};

/// Decides like [`glassdb::AvoidableTimePolicy`], with two differences for
/// splits. A split adds time to the transactions whose keys it puts in both
/// halves of the leaf, so the policy subtracts an estimate of this time, times
/// a weight. And the moving average of this net time over the windows must pay
/// for the split, not the time of one window: the avoidable time of one window
/// is noisy, and one window can pay for a split that the load does not keep
/// paying for.
///
/// With a crossing weight, the merge side of two adjacent leaves also counts
/// the time of the commit passes over the two leaves that a conflict ended,
/// times the weight. This time shows the cost of a split after it, which an
/// estimate before it can miss. It is a moving average, because these passes
/// end in few windows. The other merge-side time is of one window, because a
/// moving average of scan crossings stays below the time of the windows that
/// have them, and the leaves that scans cross do not merge (ADR-074).
pub(crate) struct NetTimePolicy {
    added: AddedTime,
    split_threshold: f64,
    merge_threshold: f64,
    divided_weight: f64,
    crossing_weight: Option<f64>,
    half_life: Duration,
    averages: Mutex<Averages>,
}

#[derive(Debug, Default)]
struct Averages {
    leaves: HashMap<LeafId, NetTime>,
    /// The moving averages of the crossing conflict time of two adjacent
    /// leaves in one window, in seconds.
    crossing: BTreeMap<(LeafId, LeafId), f64>,
}

/// What estimates the time that a split adds to the transactions of a leaf.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum AddedTime {
    /// The divided commits, times the mean latency of the commits of the
    /// leaf.
    Commits,
    /// The time of the divided commit passes, also of the passes that did not
    /// commit.
    Passes,
    /// The time of the divided commit passes that a conflict ended without a
    /// commit.
    Conflicts,
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
        added: AddedTime,
        split_threshold: f64,
        merge_threshold: f64,
        divided_weight: f64,
        crossing_weight: Option<f64>,
        half_life: Duration,
    ) -> Self {
        Self {
            added,
            split_threshold,
            merge_threshold,
            divided_weight,
            crossing_weight,
            half_life,
            averages: Mutex::new(Averages::default()),
        }
    }

    /// Returns the time in seconds that a split of the leaf would have added
    /// to the transactions of the window.
    fn added_time(&self, leaf: &LeafWindow) -> f64 {
        let divided = match self.added {
            AddedTime::Commits => {
                let committed = leaf.committed.total();
                if committed == 0 {
                    return 0.0;
                }
                let mean_latency = leaf.latency.as_secs_f64() / committed as f64;
                leaf.divided.total() as f64 * mean_latency
            }
            AddedTime::Passes => leaf.divided_time.as_secs_f64(),
            AddedTime::Conflicts => leaf.divided_conflict_time.as_secs_f64(),
        };
        self.divided_weight * divided
    }

    /// Returns the splits of the leaves whose net split-side time pays for a
    /// split, and holds these leaves.
    fn splits(
        &self,
        window: &TopologyWindow,
        leaves: &mut HashMap<LeafId, NetTime>,
        decay: f64,
        held: &mut BTreeSet<LeafId>,
    ) -> Vec<TopologyChange> {
        let split_time = window
            .split_time
            .mul_f64(self.split_threshold)
            .as_secs_f64();
        leaves.retain(|id, net| {
            net.decay(decay);
            window.leaves.contains_key(id) || !net.is_negligible()
        });
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
            held.insert(id.clone());
        }
        changes
    }

    /// Returns the merges of the adjacent leaves whose merge-side time pays
    /// for a merge, except of held leaves, and holds the merged leaves.
    fn merges(
        &self,
        window: &TopologyWindow,
        crossing: &mut BTreeMap<(LeafId, LeafId), f64>,
        decay: f64,
        held: &mut BTreeSet<LeafId>,
    ) -> Vec<TopologyChange> {
        let merge_time = window.merge_time.mul_f64(self.merge_threshold);
        let changed = |id: &LeafId| {
            window
                .leaves
                .get(id)
                .is_some_and(|leaf| leaf.split_recently || leaf.merged_recently)
        };
        crossing.retain(|(left, right), time| {
            *time *= decay;
            // The measurements start again at each change of the leaves.
            !changed(left) && !changed(right) && *time >= 1e-6
        });
        let split_side = |id: &LeafId| {
            window
                .leaves
                .get(id)
                .map_or(Duration::ZERO, |leaf| leaf.avoidable.total())
        };
        let mut merge_sides: BTreeMap<(LeafId, LeafId), Duration> = window
            .pairs
            .iter()
            .map(|pair| {
                (
                    (pair.left.clone(), pair.right.clone()),
                    pair.avoidable.total(),
                )
            })
            .collect();
        if let Some(weight) = self.crossing_weight {
            for pair in &window.pairs {
                *crossing
                    .entry((pair.left.clone(), pair.right.clone()))
                    .or_default() += (1.0 - decay) * pair.crossing_conflict_time.as_secs_f64();
            }
            // The average also decides for the pairs of earlier windows. When
            // another instance splits a leaf of such a pair, its transactions
            // are over three leaves, and no pair has their crossing time. The
            // merge of the left leaf with its right sibling of now then takes
            // one of these leaves away.
            for (ids, average) in crossing.iter() {
                *merge_sides.entry(ids.clone()).or_default() +=
                    Duration::from_secs_f64(weight * average);
            }
        }
        let mut changes = Vec::new();
        for ((left, right), merge_side) in merge_sides {
            if held.contains(&left) || held.contains(&right) {
                continue;
            }
            if merge_side <= merge_time + split_side(&left) + split_side(&right) {
                continue;
            }
            changes.push(TopologyChange::Merge(left.clone()));
            held.insert(left.clone());
            held.insert(right.clone());
            // A merge can take more than one window to land, and after it the
            // left leaf is gone. So the average must not ask again.
            crossing.remove(&(left, right));
        }
        changes
    }
}

impl TopologyPolicy for NetTimePolicy {
    fn decide(&self, window: &TopologyWindow) -> Vec<TopologyChange> {
        let decay = 0.5_f64.powf(window.elapsed.as_secs_f64() / self.half_life.as_secs_f64());
        let mut averages = self.averages.lock().unwrap();
        let Averages { leaves, crossing } = &mut *averages;
        let mut held: BTreeSet<LeafId> = window
            .leaves
            .iter()
            .filter(|(_, leaf)| leaf.split_recently)
            .map(|(id, _)| id.clone())
            .collect();
        let mut changes = self.splits(window, leaves, decay, &mut held);
        changes.extend(self.merges(window, crossing, decay, &mut held));
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
    use glassdb::{Backend, Database, NodeSizePolicy, SplitTime};

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
        policy_with(AddedTime::Commits)
    }

    fn policy_with(added: AddedTime) -> NetTimePolicy {
        NetTimePolicy::new(added, 0.25, 0.1, 0.25, None, Duration::from_secs(10))
    }

    fn crossing_policy(weight: Option<f64>) -> NetTimePolicy {
        NetTimePolicy::new(
            AddedTime::Conflicts,
            0.25,
            0.1,
            4.0,
            weight,
            Duration::from_secs(10),
        )
    }

    /// Splits the leaves over a soft cap, and keeps the last window with two
    /// adjacent leaves.
    struct CapturePair(Arc<Mutex<Option<TopologyWindow>>>);

    impl TopologyPolicy for CapturePair {
        fn decide(&self, window: &TopologyWindow) -> Vec<TopologyChange> {
            if !window.pairs.is_empty() {
                *self.0.lock().unwrap() = Some(window.clone());
            }
            window
                .leaves
                .iter()
                .filter(|(_, leaf)| {
                    leaf.size
                        .is_some_and(|size| size.over_soft_cap(&window.node_size))
                })
                .map(|(id, _)| TopologyChange::Split(id.clone()))
                .collect()
        }
    }

    // Returns a window of one second of a real database instance with two
    // adjacent leaves and no other leaf, whose measurements the tests then
    // set.
    async fn pair_window() -> (TopologyWindow, LeafId, LeafId) {
        let captured = Arc::new(Mutex::new(None));
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let sizes = NodeSizePolicy::builder()
            .leaf_max_entries(2)
            .leaf_min_entries(0)
            .index_min_children(0)
            .build()
            .unwrap();
        let db = Database::builder("netpolicytest", backend)
            .node_size_policy(sizes)
            .topology_policy(CapturePair(captured.clone()))
            .open()
            .await
            .unwrap();
        let collection = db
            .root_collection()
            .create_collection_if_absent(b"c")
            .await
            .unwrap();
        for key in [b"a", b"b", b"c"] {
            collection.write(key, b"value").await.unwrap();
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
        let collection = &collection;
        db.tx(|tx| async move {
            tx.read(collection, b"a").await?;
            tx.read(collection, b"c").await?;
            Ok(())
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        db.shutdown().await;
        let mut window = captured.lock().unwrap().take().unwrap();
        window.elapsed = Duration::from_secs(1);
        window.pairs.truncate(1);
        let (left, right) = (window.pairs[0].left.clone(), window.pairs[0].right.clone());
        window.leaves.clear();
        for id in [&left, &right] {
            set(&mut window, id, LeafWindow::default());
        }
        (window, left, right)
    }

    // Returns the index of the first window whose decision changes a leaf,
    // with the crossing conflict time of the pair of `window` in each window,
    // and `leaf` as the measurements of both leaves of the pair.
    fn first_merge(
        policy: &NetTimePolicy,
        window: &mut TopologyWindow,
        crossing_conflicts: impl IntoIterator<Item = u64>,
        leaf: &LeafWindow,
    ) -> Option<(usize, Vec<TopologyChange>)> {
        let ids = [window.pairs[0].left.clone(), window.pairs[0].right.clone()];
        crossing_conflicts
            .into_iter()
            .enumerate()
            .find_map(|(index, millis)| {
                window.pairs[0].crossing_conflict_time = Duration::from_millis(millis);
                for id in &ids {
                    set(window, id, leaf.clone());
                }
                let changes = policy.decide(window);
                (!changes.is_empty()).then_some((index, changes))
            })
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

    #[tokio::test(start_paused = true)]
    async fn divided_commit_passes_that_do_not_commit_keep_a_leaf_from_splitting() {
        let (mut window, id) = one_leaf_window().await;
        let starving = || {
            let mut leaf = lost_cas(1000);
            leaf.divided_time = Duration::from_secs(5);
            leaf
        };

        // The divided commit passes of 5 seconds add 1.25 seconds, but none
        // of them commits.
        let by_commits = first_change(
            &policy_with(AddedTime::Commits),
            &mut window,
            &id,
            (0..30).map(|_| starving()),
        );
        let by_passes = first_change(
            &policy_with(AddedTime::Passes),
            &mut window,
            &id,
            (0..30).map(|_| starving()),
        );

        assert!(by_commits.is_some());
        assert_eq!(by_passes, None);
    }

    #[tokio::test(start_paused = true)]
    async fn two_leaves_merge_when_the_transactions_over_them_keep_conflicting() {
        let (mut window, left, _) = pair_window().await;
        let quiet = LeafWindow::default();

        let with_weight = first_merge(&crossing_policy(Some(1.0)), &mut window, [1000; 30], &quiet);
        let without_weight = first_merge(&crossing_policy(None), &mut window, [1000; 30], &quiet);

        // The average of one second in each window pays for a tenth of the
        // default merge time of half a second in the first window.
        assert_eq!(with_weight, Some((0, vec![TopologyChange::Merge(left)])));
        assert_eq!(without_weight, None);
    }

    #[tokio::test(start_paused = true)]
    async fn the_split_side_time_of_two_leaves_keeps_them_apart() {
        let (mut window, _, _) = pair_window().await;
        let mut contended = lost_cas(600);
        // The divided transactions keep each leaf from splitting.
        contended.divided_conflict_time = Duration::from_millis(200);

        let change = first_merge(
            &crossing_policy(Some(1.0)),
            &mut window,
            [1000; 30],
            &contended,
        );

        assert_eq!(change, None);
    }

    #[tokio::test(start_paused = true)]
    async fn the_crossing_conflicts_of_earlier_windows_merge_two_leaves() {
        let (mut window, left, _) = pair_window().await;
        let policy = crossing_policy(Some(1.0));
        let mut contended = lost_cas(600);
        // The divided transactions keep each leaf from splitting.
        contended.divided_conflict_time = Duration::from_millis(200);
        let with_pair = first_merge(&policy, &mut window, [1000], &contended);
        let mut without_pair = window.clone();
        without_pair.pairs.clear();
        without_pair.leaves.clear();

        // Without the split side of the leaves, the average of the crossing
        // conflict time of the earlier window pays for the merge.
        let change = policy.decide(&without_pair);

        assert_eq!(with_pair, None);
        assert_eq!(change, vec![TopologyChange::Merge(left)]);
    }
}
