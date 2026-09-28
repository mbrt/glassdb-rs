use std::collections::BTreeMap;
use std::time::Duration;

use glassdb_data::{CollectionAddress, NodeId, ObjectPath};
use glassdb_storage::NodeSizePolicy;

use super::{
    AvoidableTimePolicy, LeafId, LeafSize, LeafWindow, PairWindow, SizePolicy, TopologyChange,
    TopologyPolicy, TopologyWindow,
};
use crate::structural::avoidable::{MergeTime, SplitTime};

const CHANGE_TIME: Duration = Duration::from_millis(500);

fn leaf(byte: u8) -> LeafId {
    LeafId::new(ObjectPath::Node {
        collection: CollectionAddress::root("db"),
        id: NodeId::from_bytes([byte; 16]),
    })
}

fn root() -> LeafId {
    LeafId::new(ObjectPath::TreeRoot {
        collection: CollectionAddress::root("db"),
    })
}

fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

fn lost_cas(millis: u64) -> LeafWindow {
    LeafWindow {
        avoidable: SplitTime {
            lost_cas: ms(millis),
            ..SplitTime::default()
        },
        ..LeafWindow::default()
    }
}

// Lost CAS time of one window that alone pays for a split at one change time.
fn lost_cas_of_one_split() -> LeafWindow {
    lost_cas(8000)
}

// Lost CAS time, and divided transactions that conflict so much that a split
// would add more time than it removes.
fn contended() -> LeafWindow {
    LeafWindow {
        divided_conflict_time: ms(200),
        ..lost_cas(600)
    }
}

fn sized(entries: usize, live_entries: usize) -> LeafWindow {
    LeafWindow {
        size: Some(LeafSize {
            entries,
            live_entries,
            encoded_bytes: 0,
        }),
        ..LeafWindow::default()
    }
}

fn pair(left: u8, right: u8) -> PairWindow {
    PairWindow {
        left: leaf(left),
        right: leaf(right),
        avoidable: MergeTime::default(),
        crossing_conflict_time: Duration::ZERO,
    }
}

fn scan_crossing(left: u8, right: u8, millis: u64) -> PairWindow {
    PairWindow {
        avoidable: MergeTime {
            scan_crossing: ms(millis),
            ..MergeTime::default()
        },
        ..pair(left, right)
    }
}

fn crossing_conflicts(left: u8, right: u8, millis: u64) -> PairWindow {
    PairWindow {
        crossing_conflict_time: ms(millis),
        ..pair(left, right)
    }
}

// The rule with a threshold of one typical change time, which makes the
// boundaries of the tests easy to read.
fn at_one_change_time() -> AvoidableTimePolicy {
    AvoidableTimePolicy::new()
        .split_threshold(1.0)
        .merge_threshold(1.0)
}

fn window(leaves: Vec<(LeafId, LeafWindow)>, pairs: Vec<PairWindow>) -> TopologyWindow {
    TopologyWindow {
        elapsed: Duration::from_secs(1),
        split_time: CHANGE_TIME,
        merge_time: CHANGE_TIME,
        node_size: NodeSizePolicy::builder()
            .leaf_max_entries(8)
            .leaf_min_entries(2)
            .build()
            .unwrap(),
        leaves: leaves.into_iter().collect::<BTreeMap<_, _>>(),
        pairs,
    }
}

// Decides on `window` in `windows` windows of one second, and returns all
// changes. A leaf that a change asks for is gone from the later windows, as
// after the change.
fn decide_over(
    policy: &AvoidableTimePolicy,
    mut window: TopologyWindow,
    windows: usize,
) -> Vec<TopologyChange> {
    let mut changes = Vec::new();
    for _ in 0..windows {
        let decided = policy.decide(&window);
        for change in &decided {
            let (TopologyChange::Split(id)
            | TopologyChange::SplitAt(id, _)
            | TopologyChange::Merge(id)) = change;
            window.leaves.remove(id);
            window
                .pairs
                .retain(|pair| pair.left != *id && pair.right != *id);
        }
        changes.extend(decided);
    }
    changes
}

#[test]
fn a_leaf_splits_when_its_split_side_time_keeps_paying_for_a_split() {
    let window = window(
        vec![(leaf(1), lost_cas(501)), (leaf(2), lost_cas(499))],
        vec![],
    );

    assert_eq!(
        decide_over(&at_one_change_time(), window, 200),
        vec![TopologyChange::Split(leaf(1))]
    );
}

#[test]
fn one_window_does_not_pay_for_a_split_that_the_load_does_not_keep_paying_for() {
    let policy = at_one_change_time();

    let burst = policy.decide(&window(vec![(leaf(1), lost_cas(7000))], vec![]));
    let after = decide_over(
        &policy,
        window(vec![(leaf(1), LeafWindow::default())], vec![]),
        30,
    );

    assert_eq!(burst, vec![]);
    assert_eq!(after, vec![]);
}

#[test]
fn a_leaf_does_not_split_again_before_its_split_lands() {
    let policy = at_one_change_time();
    let loaded = window(vec![(leaf(1), lost_cas(1000))], vec![]);

    let first = (0..30).find_map(|_| Some(policy.decide(&loaded)).filter(|c| !c.is_empty()));
    let next = policy.decide(&loaded);

    assert_eq!(first, Some(vec![TopologyChange::Split(leaf(1))]));
    assert_eq!(next, vec![]);
}

// After a split, the divided transactions that conflict wait for the locks of
// more leaves.
#[test]
fn divided_transactions_that_conflict_keep_a_leaf_from_splitting() {
    let divided = LeafWindow {
        divided_conflict_time: ms(200),
        ..lost_cas(1000)
    };

    let with_conflicts = decide_over(
        &at_one_change_time(),
        window(vec![(leaf(1), divided)], vec![]),
        200,
    );
    let without = decide_over(
        &at_one_change_time(),
        window(vec![(leaf(1), lost_cas(1000))], vec![]),
        200,
    );

    assert_eq!(with_conflicts, vec![]);
    assert_eq!(without, vec![TopologyChange::Split(leaf(1))]);
}

// The split key separates the keys of the leaf delays. Inline pressure and a
// soft cap ask for balanced halves.
#[test]
fn a_split_is_at_the_split_key_only_when_the_leaf_delays_pay_for_it() {
    let keyed = |window: LeafWindow| LeafWindow {
        split_key: Some(b"k".to_vec()),
        ..window
    };
    let mut with_inline_pressure = lost_cas(400);
    with_inline_pressure.avoidable.inline_pressure = ms(400);
    let window = window(
        vec![
            (leaf(1), keyed(lost_cas(1000))),
            (leaf(2), keyed(sized(9, 9))),
            (leaf(3), keyed(with_inline_pressure)),
        ],
        vec![],
    );

    assert_eq!(
        decide_over(&at_one_change_time(), window, 200),
        vec![
            TopologyChange::Split(leaf(2)),
            TopologyChange::SplitAt(leaf(1), b"k".to_vec()),
            TopologyChange::Split(leaf(3)),
        ]
    );
}

#[test]
fn the_split_threshold_scales_the_split_time() {
    let window = window(vec![(leaf(1), lost_cas(900))], vec![]);

    assert_eq!(
        decide_over(&at_one_change_time().split_threshold(2.0), window, 200),
        vec![]
    );
}

// Scans cross the leaves only in some windows, so each of these windows must
// pay for the merge.
#[test]
fn two_leaves_merge_when_merge_side_time_pays_for_the_merge_and_both_split_sides() {
    let leaves = || vec![(leaf(1), lost_cas(200)), (leaf(2), lost_cas(100))];

    let pays = window(leaves(), vec![scan_crossing(1, 2, 801)]);
    let does_not_pay = window(leaves(), vec![scan_crossing(1, 2, 800)]);

    assert_eq!(
        at_one_change_time().decide(&pays),
        vec![TopologyChange::Merge(leaf(1))]
    );
    assert_eq!(at_one_change_time().decide(&does_not_pay), vec![]);
}

#[test]
fn two_leaves_merge_when_the_transactions_over_them_keep_conflicting() {
    let window = window(vec![], vec![crossing_conflicts(1, 2, 1000)]);

    assert_eq!(
        decide_over(&at_one_change_time(), window, 30),
        vec![TopologyChange::Merge(leaf(1))]
    );
}

#[test]
fn the_split_side_time_of_two_leaves_keeps_them_apart() {
    let window = window(
        vec![(leaf(1), contended()), (leaf(2), contended())],
        vec![crossing_conflicts(1, 2, 1000)],
    );

    assert_eq!(decide_over(&at_one_change_time(), window, 200), vec![]);
}

#[test]
fn the_crossing_conflicts_of_earlier_windows_merge_two_leaves() {
    let policy = at_one_change_time();

    let with_pair = policy.decide(&window(
        vec![(leaf(1), contended()), (leaf(2), contended())],
        vec![crossing_conflicts(1, 2, 10_000)],
    ));
    // Another instance can change the leaves, so that no pair of this window
    // has the time of the transactions over them.
    let without_pair = policy.decide(&window(vec![], vec![]));

    assert_eq!(with_pair, vec![]);
    assert_eq!(without_pair, vec![TopologyChange::Merge(leaf(1))]);
}

#[test]
fn a_leaf_that_split_recently_can_split_again_but_not_merge() {
    let split = LeafWindow {
        split_recently: true,
        ..lost_cas_of_one_split()
    };
    let quiet = LeafWindow {
        split_recently: true,
        ..LeafWindow::default()
    };
    let window = window(
        vec![(leaf(1), split), (leaf(3), quiet)],
        vec![scan_crossing(1, 2, 5000), scan_crossing(3, 4, 5000)],
    );

    assert_eq!(
        at_one_change_time().decide(&window),
        vec![TopologyChange::Split(leaf(1))]
    );
}

#[test]
fn a_leaf_that_merged_recently_can_merge_again_but_splits_only_on_a_soft_cap() {
    let merged = |window: LeafWindow| LeafWindow {
        merged_recently: true,
        ..window
    };
    let window = window(
        vec![
            (leaf(1), merged(lost_cas_of_one_split())),
            (leaf(3), merged(sized(9, 9))),
            (leaf(5), merged(LeafWindow::default())),
        ],
        vec![scan_crossing(5, 6, 5000)],
    );

    assert_eq!(
        at_one_change_time().decide(&window),
        vec![
            TopologyChange::Split(leaf(3)),
            TopologyChange::Merge(leaf(5)),
        ]
    );
}

#[test]
fn a_leaf_that_splits_or_merges_does_not_merge_again_in_the_same_window() {
    let window = window(
        vec![(leaf(4), lost_cas_of_one_split())],
        vec![
            scan_crossing(1, 2, 1000),
            scan_crossing(2, 3, 1000),
            scan_crossing(3, 4, 9000),
        ],
    );

    assert_eq!(
        at_one_change_time().decide(&window),
        vec![
            TopologyChange::Split(leaf(4)),
            TopologyChange::Merge(leaf(1)),
        ]
    );
}

#[test]
fn leaves_over_a_soft_cap_split_without_avoidable_time() {
    let window = window(vec![(leaf(1), sized(9, 9)), (leaf(2), sized(8, 8))], vec![]);

    assert_eq!(
        at_one_change_time().decide(&window),
        vec![TopologyChange::Split(leaf(1))]
    );
}

#[test]
fn the_size_policy_decides_on_soft_caps_inline_pressure_and_live_entries() {
    let inline_pressure = LeafWindow {
        avoidable: SplitTime {
            inline_pressure: ms(1),
            ..SplitTime::default()
        },
        ..LeafWindow::default()
    };
    let window = window(
        vec![
            (leaf(1), sized(9, 9)),
            (leaf(2), inline_pressure),
            (leaf(3), sized(3, 1)),
            (leaf(4), lost_cas(5000)),
            (root(), sized(1, 1)),
        ],
        vec![scan_crossing(5, 6, 5000)],
    );

    assert_eq!(
        SizePolicy.decide(&window),
        vec![
            TopologyChange::Split(leaf(1)),
            TopologyChange::Split(leaf(2)),
            TopologyChange::Merge(leaf(3)),
        ]
    );
}
