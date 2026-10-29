use std::time::UNIX_EPOCH;

use glassdb_data::{NodeId, TxId};
use glassdb_storage::{CurrentState, LeafBody, LeafEntry};

use super::*;

// The instant of the decisions: 1,000 s after the epoch.
fn now() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1000)
}

// A writer that started `age` before `now`, told apart from the others by
// `name`.
fn writer(name: u8, age: Duration) -> TxId {
    let started = now() - age;
    let nanos = started.duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
    TxId::with_priority(nanos, &[name])
}

// A recent writer.
fn recent(name: u8) -> TxId {
    writer(name, Duration::from_secs(1))
}

// A writer that is not recent, for the keys that no transaction wrote together
// lately.
fn old() -> TxId {
    writer(0, SPAN_AGE)
}

fn key(index: usize) -> Vec<u8> {
    format!("k{index:02}").into_bytes()
}

fn entry(index: usize, writer: &TxId) -> LeafEntry {
    LeafEntry::new(key(index)).with_current(CurrentState::External { writer: *writer })
}

// A leaf with the keys from `from` to `to`, excluding `to`, that `writers`
// wrote: each item gives a writer and the keys from its first to its last
// index. The other keys have an old writer.
fn leaf(from: usize, to: usize, writers: &[(&TxId, usize, usize)]) -> Node {
    Node::leaf(LeafBody::from_entries((from..to).map(|index| {
        let writer = writers
            .iter()
            .find(|(_, first, last)| (*first..=*last).contains(&index))
            .map_or(old(), |(writer, _, _)| **writer);
        entry(index, &writer)
    })))
}

// Two adjacent leaves: the left one from `from` to `boundary`, and the right
// one from `boundary` to `to`.
fn pair(
    from: usize,
    boundary: usize,
    to: usize,
    writers: &[(&TxId, usize, usize)],
) -> (Node, Node) {
    let left = leaf(from, boundary, writers)
        .with_high_key(Some(key(boundary)))
        .with_right_sibling(Some(NodeId::from_bytes([2; 16])));
    let right = leaf(boundary, to, writers).with_low_key(key(boundary));
    (left, right)
}

fn policy(max_entries: usize) -> NodeSizePolicy {
    NodeSizePolicy::builder()
        .leaf_max_entries(max_entries)
        .leaf_min_entries(1)
        .node_soft_max_bytes(1 << 20)
        .build()
        .unwrap()
}

#[test]
fn a_soft_cap_split_without_recent_write_spans_is_at_the_median() {
    assert_eq!(
        soft_cap_split(&leaf(0, 10, &[]), &policy(8), now()),
        SoftCapSplit::Median
    );
}

// The median k05 divides the write span from k04 to k06. The nearest key that
// divides no write span is k04, because a split there puts all of the span on
// the right.
#[test]
fn a_soft_cap_split_moves_to_the_nearest_key_that_divides_no_write_span() {
    let leaf = leaf(0, 10, &[(&recent(1), 4, 6)]);

    assert_eq!(
        soft_cap_split(&leaf, &policy(8), now()),
        SoftCapSplit::At(key(4))
    );
}

#[test]
fn a_soft_cap_split_waits_while_each_key_of_the_middle_half_divides_a_write_span() {
    let leaf = leaf(0, 10, &[(&recent(1), 1, 9)]);

    assert_eq!(soft_cap_split(&leaf, &policy(8), now()), SoftCapSplit::Wait);
}

// Above twice the soft cap, the split divides the write span of the keys from
// k01 to k06, but not the one from k07 to k09.
#[test]
fn a_leaf_over_twice_the_soft_cap_splits_at_the_key_that_divides_the_fewest_write_spans() {
    let writers = [(&recent(1), 1, 6), (&recent(2), 7, 9), (&recent(3), 10, 15)];
    let leaf = leaf(0, 17, &writers);

    assert_eq!(
        soft_cap_split(&leaf, &policy(8), now()),
        SoftCapSplit::At(key(7))
    );
}

// A transaction that wrote many keys long ago, for example a bulk load, does
// not hold them together.
#[test]
fn the_keys_of_an_old_writer_are_not_a_write_span() {
    let leaf = leaf(0, 10, &[(&old(), 1, 9)]);

    assert_eq!(
        soft_cap_split(&leaf, &policy(8), now()),
        SoftCapSplit::Median
    );
}

#[test]
fn a_write_span_over_the_boundary_pays_for_a_merge() {
    let (left, right) = pair(0, 4, 8, &[(&recent(1), 2, 5)]);

    assert!(pays_for_merge(&left, &right, &policy(8), now()));
}

#[test]
fn a_merge_needs_a_write_span_over_the_boundary() {
    let (left, right) = pair(0, 4, 8, &[(&recent(1), 0, 2), (&recent(2), 5, 7)]);

    assert!(!pays_for_merge(&left, &right, &policy(8), now()));
}

// One write span over the boundary against two others in the leaves: a merge
// for it divides the others at the next split.
#[test]
fn a_merge_does_not_pay_for_a_few_of_the_write_spans_of_the_leaves() {
    let writers = [(&recent(1), 0, 1), (&recent(2), 5, 6), (&recent(3), 10, 11)];
    let (left, right) = pair(0, 6, 12, &writers);

    assert!(!pays_for_merge(&left, &right, &policy(12), now()));
}

// The merged leaf of 8 entries is over the soft cap of 4, so its next split
// must find a key of its middle half, k02 to k05, that divides no write span.
#[test]
fn a_merge_over_the_soft_cap_needs_a_split_key_that_divides_no_write_span() {
    let (wide_left, wide_right) = pair(0, 4, 8, &[(&recent(1), 1, 6)]);
    let (narrow_left, narrow_right) = pair(0, 4, 8, &[(&recent(1), 3, 4)]);

    assert!(!pays_for_merge(&wide_left, &wide_right, &policy(4), now()));
    assert!(pays_for_merge(
        &narrow_left,
        &narrow_right,
        &policy(4),
        now()
    ));
}

// The clock of the instance of a writer can be ahead of this one.
#[test]
fn a_writer_that_started_a_little_after_now_is_recent() {
    let ahead = writer(1, Duration::ZERO);
    let later = |by: Duration| {
        let nanos = (now() + by).duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        TxId::with_priority(nanos, &[1])
    };
    let skewed = leaf(0, 10, &[(&later(Duration::from_secs(5)), 1, 9)]);
    let far = leaf(0, 10, &[(&later(SPAN_AGE), 1, 9)]);

    assert_eq!(ahead.started_at(), now());
    assert_eq!(
        soft_cap_split(&skewed, &policy(8), now()),
        SoftCapSplit::Wait
    );
    assert_eq!(
        soft_cap_split(&far, &policy(8), now()),
        SoftCapSplit::Median
    );
}
