//! The avoidable time of the leaves of one database instance (ADR-074).
//!
//! Transactions report the time that one split of a leaf, or one merge of two
//! adjacent leaves, would remove. The restructurer takes these reports one
//! window at a time for its topology rule, and measures the typical time of
//! its own changes.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use glassdb_concurr::rt;
use glassdb_data::{NodeId, ObjectPath};
use glassdb_storage::LeafBody;

use crate::access::AccessSet;
use crate::leaf_coord::upper_half;

use super::rule::{LeafId, LeafWindow, PairWindow, TopologyWindow};

/// The time of one split or merge until this database instance measures one.
const DEFAULT_CHANGE_TIME: Duration = Duration::from_millis(500);

/// The weight of a new measurement in a typical time is one part in this many.
const TYPICAL_TIME_WEIGHT: u32 = 8;

/// Collects the avoidable time of one database instance, one window at a time.
#[derive(Debug)]
pub(super) struct AvoidableTime {
    state: Mutex<State>,
    split_time: TypicalTime,
    merge_time: TypicalTime,
}

/// The kind of a structural change whose time the restructurer measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ChangeKind {
    Split,
    Merge,
}

#[derive(Debug)]
struct State {
    window_started: rt::Instant,
    leaves: BTreeMap<ObjectPath, LeafWindow>,
    pairs: BTreeMap<(ObjectPath, ObjectPath), PairTimes>,
    changes: Vec<(ObjectPath, ChangeKind)>,
    changes_last_window: Vec<(ObjectPath, ChangeKind)>,
}

/// The measurements of two adjacent leaves in the current window.
#[derive(Debug, Default)]
struct PairTimes {
    avoidable_time: Duration,
    crossing_conflict_time: Duration,
}

/// The moving average of the time of one operation.
#[derive(Debug, Default)]
pub(crate) struct TypicalTime {
    average: Mutex<Option<Duration>>,
}

/// The point reads of one transaction in one leaf.
struct LeafReads<'a> {
    /// The median key of the leaf, as the read of the lowest key observed it.
    median: Option<&'a [u8]>,
    /// The right sibling of the leaf, as the same read observed it.
    right_sibling: Option<NodeId>,
    keys: Vec<&'a [u8]>,
}

impl TypicalTime {
    /// Returns the typical time, or none before the first measurement.
    pub(crate) fn get(&self) -> Option<Duration> {
        *self.average.lock().unwrap()
    }

    /// Adds one measurement.
    pub(crate) fn record(&self, took: Duration) {
        let mut average = self.average.lock().unwrap();
        *average = Some(match *average {
            None => took,
            Some(old) if took >= old => old + (took - old) / TYPICAL_TIME_WEIGHT,
            Some(old) => old - (old - took) / TYPICAL_TIME_WEIGHT,
        });
    }
}

impl AvoidableTime {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(State {
                window_started: rt::Instant::now(),
                leaves: BTreeMap::new(),
                pairs: BTreeMap::new(),
                changes: Vec::new(),
                changes_last_window: Vec::new(),
            }),
            split_time: TypicalTime::default(),
            merge_time: TypicalTime::default(),
        }
    }

    /// Adds leaf delay time of the leaf at `path`, which a split at
    /// `split_key` removes.
    pub(super) fn add_leaf_delay(&self, path: &ObjectPath, time: Duration, split_key: &[u8]) {
        if time.is_zero() {
            return;
        }
        let mut state = self.state.lock().unwrap();
        let leaf = state.leaf(path);
        leaf.delay_time += time;
        leaf.split_key = Some(split_key.to_vec());
    }

    /// Adds inline pressure time of the leaf at `path`, which a split at the
    /// median removes.
    pub(super) fn add_inline_pressure(&self, path: &ObjectPath, time: Duration) {
        if !time.is_zero() {
            self.state.lock().unwrap().leaf(path).inline_pressure_time += time;
        }
    }

    /// Adds merge-side time of the adjacent leaves at `left` and `right`.
    pub(super) fn add_merge_time(&self, left: &ObjectPath, right: &ObjectPath, time: Duration) {
        if time.is_zero() {
            return;
        }
        self.state
            .lock()
            .unwrap()
            .pairs
            .entry((left.clone(), right.clone()))
            .or_default()
            .avoidable_time += time;
    }

    /// Records one commit pass of a transaction with `accesses` that a
    /// conflict ended after `time`, in each leaf that a split would divide its
    /// point reads in, and in the two adjacent leaves of its point reads if it
    /// has point reads in no other leaf.
    pub(super) fn add_conflict_pass(&self, accesses: &AccessSet, time: Duration) {
        let reads = LeafReads::of(accesses);
        let pair = LeafReads::adjacent_pair(&reads);
        let mut state = self.state.lock().unwrap();
        for (path, read) in reads {
            if read.divided() {
                state.leaf(path).divided_conflict_time += time;
            }
        }
        if let Some(pair) = pair {
            state.pairs.entry(pair).or_default().crossing_conflict_time += time;
        }
    }

    /// Records that a structural change of `kind` that landed wrote the leaf
    /// at `path`.
    pub(super) fn record_changed(&self, path: ObjectPath, kind: ChangeKind) {
        let mut state = self.state.lock().unwrap();
        // Time from before the change measured a topology that is gone. If it
        // stays, the rule can do the same change again for the same time.
        state.leaves.remove(&path);
        state
            .pairs
            .retain(|(left, right), _| *left != path && *right != path);
        state.changes.push((path, kind));
    }

    /// Records the time of one structural change that landed.
    pub(super) fn record_change(&self, kind: ChangeKind, took: Duration) {
        match kind {
            ChangeKind::Split => self.split_time.record(took),
            ChangeKind::Merge => self.merge_time.record(took),
        }
    }

    /// Returns the measurements since the last call, and starts a new window.
    pub(super) fn take_window(&self) -> TopologyWindow {
        let mut state = self.state.lock().unwrap();
        let now = rt::Instant::now();
        let elapsed = now.saturating_duration_since(state.window_started);
        state.window_started = now;
        let changes = std::mem::take(&mut state.changes);
        let changes_before = std::mem::replace(&mut state.changes_last_window, changes.clone());
        for (path, kind) in changes_before.into_iter().chain(changes) {
            let leaf = state.leaf(&path);
            match kind {
                ChangeKind::Split => leaf.split_recently = true,
                ChangeKind::Merge => leaf.merged_recently = true,
            }
        }
        let leaves = std::mem::take(&mut state.leaves)
            .into_iter()
            .map(|(path, leaf)| (LeafId::new(path), leaf))
            .collect();
        let pairs = std::mem::take(&mut state.pairs)
            .into_iter()
            .map(|((left, right), times)| PairWindow {
                left: LeafId::new(left),
                right: LeafId::new(right),
                avoidable_time: times.avoidable_time,
                crossing_conflict_time: times.crossing_conflict_time,
            })
            .collect();
        drop(state);
        TopologyWindow {
            elapsed,
            split_time: self.split_time.get().unwrap_or(DEFAULT_CHANGE_TIME),
            merge_time: self.merge_time.get().unwrap_or(DEFAULT_CHANGE_TIME),
            leaves,
            pairs,
        }
    }
}

impl State {
    fn leaf(&mut self, path: &ObjectPath) -> &mut LeafWindow {
        self.leaves.entry(path.clone()).or_default()
    }
}

impl<'a> LeafReads<'a> {
    /// Groups the point reads of `accesses` by the leaf that they observed.
    fn of(accesses: &'a AccessSet) -> BTreeMap<&'a ObjectPath, Self> {
        let mut leaves: BTreeMap<&ObjectPath, Self> = BTreeMap::new();
        for read in accesses.point_reads() {
            let observation = read.observation();
            let leaf = leaves.entry(observation.path()).or_insert_with(|| Self {
                median: observation
                    .value()
                    .and_then(|node| node.as_leaf())
                    .and_then(LeafBody::median_key),
                right_sibling: observation.value().and_then(|node| node.right_sibling()),
                keys: Vec::new(),
            });
            leaf.keys.push(read.key().key());
        }
        leaves
    }

    /// Returns the two leaves of `reads`, the left one first, if there are
    /// two and they are adjacent.
    fn adjacent_pair(reads: &BTreeMap<&ObjectPath, Self>) -> Option<(ObjectPath, ObjectPath)> {
        let [(a_path, a), (b_path, b)] =
            <[_; 2]>::try_from(reads.iter().collect::<Vec<_>>()).ok()?;
        if a.links_right_to(a_path, b_path) {
            Some(((*a_path).clone(), (*b_path).clone()))
        } else if b.links_right_to(b_path, a_path) {
            Some(((*b_path).clone(), (*a_path).clone()))
        } else {
            None
        }
    }

    /// Reports whether the leaf at `path` with these reads has the leaf at
    /// `right` as its right sibling.
    fn links_right_to(&self, path: &ObjectPath, right: &ObjectPath) -> bool {
        match (path, right) {
            (
                ObjectPath::Node {
                    collection: left_collection,
                    ..
                },
                ObjectPath::Node { collection, id },
            ) => left_collection == collection && self.right_sibling == Some(*id),
            _ => false,
        }
    }

    /// Reports whether a split at the median puts the keys in both halves.
    fn divided(&self) -> bool {
        self.median
            .is_some_and(|median| upper_half(median, self.keys.iter().copied()).is_none())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use glassdb_data::{CollectionAddress, NodeId, ObjectPath};

    use super::{AvoidableTime, ChangeKind, TypicalTime};
    use crate::structural::rule::LeafId;

    fn node(byte: u8) -> ObjectPath {
        ObjectPath::Node {
            collection: CollectionAddress::root("db"),
            id: NodeId::from_bytes([byte; 16]),
        }
    }

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    #[test]
    fn a_change_drops_the_earlier_time_of_its_leaves_and_holds_them_for_two_windows() {
        let avoidable = AvoidableTime::new();
        avoidable.add_leaf_delay(&node(1), ms(900), b"a");
        avoidable.add_leaf_delay(&node(3), ms(700), b"c");
        avoidable.add_merge_time(&node(1), &node(2), ms(800));
        avoidable.add_merge_time(&node(3), &node(4), ms(600));
        avoidable.record_changed(node(1), ChangeKind::Split);
        avoidable.add_leaf_delay(&node(1), ms(100), b"a");

        let first = avoidable.take_window();
        let second = avoidable.take_window();
        let third = avoidable.take_window();

        let changed = &first.leaves[&LeafId::new(node(1))];
        assert_eq!(changed.delay_time, ms(100));
        assert!(changed.split_recently && !changed.merged_recently);
        assert_eq!(first.leaves[&LeafId::new(node(3))].delay_time, ms(700));
        assert_eq!(first.pairs.len(), 1);
        assert_eq!(first.pairs[0].left, LeafId::new(node(3)));
        assert!(second.leaves[&LeafId::new(node(1))].split_recently);
        assert!(third.leaves.is_empty());
        assert!(third.pairs.is_empty());
    }

    #[test]
    fn a_window_keeps_the_last_split_key_of_each_leaf_since_its_last_change() {
        let avoidable = AvoidableTime::new();
        avoidable.add_leaf_delay(&node(1), ms(100), b"a");
        avoidable.add_leaf_delay(&node(1), ms(100), b"b");
        avoidable.add_inline_pressure(&node(1), ms(100));
        avoidable.add_leaf_delay(&node(2), ms(100), b"c");
        avoidable.record_changed(node(2), ChangeKind::Split);
        avoidable.add_inline_pressure(&node(2), ms(100));

        let window = avoidable.take_window();

        let split_key = |byte| window.leaves[&LeafId::new(node(byte))].split_key.clone();
        assert_eq!(split_key(1), Some(b"b".to_vec()));
        assert_eq!(split_key(2), None);
    }

    #[test]
    fn typical_time_moves_one_eighth_toward_each_measurement() {
        let typical = TypicalTime::default();
        assert_eq!(typical.get(), None);

        typical.record(Duration::from_millis(80));
        typical.record(Duration::from_millis(160));
        assert_eq!(typical.get(), Some(Duration::from_millis(90)));

        typical.record(Duration::from_millis(10));
        assert_eq!(typical.get(), Some(Duration::from_millis(80)));
    }
}
