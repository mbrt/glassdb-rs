use std::collections::BTreeMap;
use std::time::Duration;

use glassdb_data::{CollectionAddress, NodeId, ObjectPath};

use super::{
    AvoidableTimeRule, ChangeRequest, LeafId, LeafWindow, MERGE_THRESHOLD, PairWindow,
    SPLIT_THRESHOLD, TopologyRule, TopologyWindow,
};

const CHANGE_TIME: Duration = Duration::from_millis(500);

fn leaf(byte: u8) -> LeafId {
    LeafId::new(ObjectPath::Node {
        collection: CollectionAddress::root("db"),
        id: NodeId::from_bytes([byte; 16]),
    })
}

fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

// The split-side time of each window that a split needs in the long run.
fn split_pays() -> Duration {
    CHANGE_TIME.mul_f64(SPLIT_THRESHOLD)
}

// The merge-side time of one window that a merge needs, above the split-side
// time of both leaves.
fn merge_pays() -> Duration {
    CHANGE_TIME.mul_f64(MERGE_THRESHOLD)
}

fn delayed(time: Duration) -> LeafWindow {
    LeafWindow {
        delay_time: time,
        ..LeafWindow::default()
    }
}

// Leaf delays of one window that alone pay for a split.
fn delays_of_one_split() -> LeafWindow {
    delayed(ms(400))
}

// Leaf delays, and divided transactions that conflict so much that a split
// would add more time than it removes.
fn contended() -> LeafWindow {
    LeafWindow {
        divided_conflict_time: ms(10),
        ..delayed(ms(30))
    }
}

fn pair(left: u8, right: u8) -> PairWindow {
    PairWindow {
        left: leaf(left),
        right: leaf(right),
        avoidable_time: Duration::ZERO,
        crossing_conflict_time: Duration::ZERO,
    }
}

fn scan_crossing(left: u8, right: u8, time: Duration) -> PairWindow {
    PairWindow {
        avoidable_time: time,
        ..pair(left, right)
    }
}

fn crossing_conflicts(left: u8, right: u8, time: Duration) -> PairWindow {
    PairWindow {
        crossing_conflict_time: time,
        ..pair(left, right)
    }
}

fn window(leaves: Vec<(LeafId, LeafWindow)>, pairs: Vec<PairWindow>) -> TopologyWindow {
    TopologyWindow {
        split_time: CHANGE_TIME,
        merge_time: CHANGE_TIME,
        leaves: leaves.into_iter().collect::<BTreeMap<_, _>>(),
        pairs,
    }
}

// Decides on `window` in `windows` windows of one second, and returns all
// requests. A leaf that a request asks for is gone from the later windows, as
// after the change.
fn decide_over(
    rule: &mut AvoidableTimeRule,
    mut window: TopologyWindow,
    windows: usize,
) -> Vec<ChangeRequest> {
    let mut requests = Vec::new();
    for _ in 0..windows {
        let decided = rule.decide(&window);
        for request in &decided {
            let (ChangeRequest::Split(id)
            | ChangeRequest::SplitAt(id, _)
            | ChangeRequest::Merge(id)) = request;
            window.leaves.remove(id);
            window
                .pairs
                .retain(|pair| pair.left != *id && pair.right != *id);
        }
        requests.extend(decided);
    }
    requests
}

#[test]
fn a_leaf_splits_when_its_split_side_time_keeps_paying_for_a_split() {
    let window = window(
        vec![
            (leaf(1), delayed(split_pays() + ms(1))),
            (leaf(2), delayed(split_pays() - ms(1))),
        ],
        vec![],
    );

    assert_eq!(
        decide_over(&mut AvoidableTimeRule::default(), window, 200),
        vec![ChangeRequest::Split(leaf(1))]
    );
}

#[test]
fn a_slower_split_needs_more_split_side_time() {
    let window = TopologyWindow {
        split_time: CHANGE_TIME * 2,
        ..window(vec![(leaf(1), delayed(split_pays() + ms(1)))], vec![])
    };

    assert_eq!(
        decide_over(&mut AvoidableTimeRule::default(), window, 200),
        vec![]
    );
}

#[test]
fn one_window_does_not_pay_for_a_split_that_the_load_does_not_keep_paying_for() {
    let mut rule = AvoidableTimeRule::default();

    let burst = rule.decide(&window(vec![(leaf(1), delayed(split_pays() * 14))], vec![]));
    let after = decide_over(
        &mut rule,
        window(vec![(leaf(1), LeafWindow::default())], vec![]),
        30,
    );

    assert_eq!(burst, vec![]);
    assert_eq!(after, vec![]);
}

#[test]
fn a_leaf_does_not_split_again_before_its_split_lands() {
    let mut rule = AvoidableTimeRule::default();
    let loaded = window(vec![(leaf(1), delayed(split_pays() * 2))], vec![]);

    let first = (0..30).find_map(|_| Some(rule.decide(&loaded)).filter(|r| !r.is_empty()));
    let next = rule.decide(&loaded);

    assert_eq!(first, Some(vec![ChangeRequest::Split(leaf(1))]));
    assert_eq!(next, vec![]);
}

// After a split, the divided transactions that conflict wait for the locks of
// more leaves.
#[test]
fn divided_transactions_that_conflict_keep_a_leaf_from_splitting() {
    let divided = LeafWindow {
        divided_conflict_time: ms(10),
        ..delayed(ms(50))
    };

    let with_conflicts = decide_over(
        &mut AvoidableTimeRule::default(),
        window(vec![(leaf(1), divided)], vec![]),
        200,
    );
    let without = decide_over(
        &mut AvoidableTimeRule::default(),
        window(vec![(leaf(1), delayed(ms(50)))], vec![]),
        200,
    );

    assert_eq!(with_conflicts, vec![]);
    assert_eq!(without, vec![ChangeRequest::Split(leaf(1))]);
}

// The split key separates the keys of the leaf delays. Inline pressure asks
// for balanced halves.
#[test]
fn a_split_is_at_the_split_key_only_when_the_leaf_delays_pay_for_it() {
    let keyed = |window: LeafWindow| LeafWindow {
        split_key: Some(b"k".to_vec()),
        ..window
    };
    let with_inline_pressure = LeafWindow {
        inline_pressure_time: ms(20),
        ..delayed(ms(20))
    };
    let window = window(
        vec![
            (leaf(1), keyed(delayed(ms(50)))),
            (leaf(2), keyed(with_inline_pressure)),
        ],
        vec![],
    );

    assert_eq!(
        decide_over(&mut AvoidableTimeRule::default(), window, 200),
        vec![
            ChangeRequest::SplitAt(leaf(1), b"k".to_vec()),
            ChangeRequest::Split(leaf(2)),
        ]
    );
}

// Scans cross the leaves only in some windows, so each of these windows must
// pay for the merge.
#[test]
fn two_leaves_merge_when_merge_side_time_pays_for_the_merge_and_both_split_sides() {
    let leaves = || vec![(leaf(1), delayed(ms(10))), (leaf(2), delayed(ms(5)))];
    let both_sides = merge_pays() + ms(15);

    let pays = window(leaves(), vec![scan_crossing(1, 2, both_sides + ms(1))]);
    let does_not_pay = window(leaves(), vec![scan_crossing(1, 2, both_sides)]);

    assert_eq!(
        AvoidableTimeRule::default().decide(&pays),
        vec![ChangeRequest::Merge(leaf(1))]
    );
    assert_eq!(AvoidableTimeRule::default().decide(&does_not_pay), vec![]);
}

#[test]
fn two_leaves_merge_when_the_transactions_over_them_keep_conflicting() {
    let window = window(vec![], vec![crossing_conflicts(1, 2, merge_pays() * 2)]);

    assert_eq!(
        decide_over(&mut AvoidableTimeRule::default(), window, 30),
        vec![ChangeRequest::Merge(leaf(1))]
    );
}

#[test]
fn the_split_side_time_of_two_leaves_keeps_them_apart() {
    let window = window(
        vec![(leaf(1), contended()), (leaf(2), contended())],
        vec![crossing_conflicts(1, 2, ms(100))],
    );

    assert_eq!(
        decide_over(&mut AvoidableTimeRule::default(), window, 200),
        vec![]
    );
}

#[test]
fn the_crossing_conflicts_of_earlier_windows_merge_two_leaves() {
    let mut rule = AvoidableTimeRule::default();

    let with_pair = rule.decide(&window(
        vec![(leaf(1), contended()), (leaf(2), contended())],
        vec![crossing_conflicts(1, 2, ms(1200))],
    ));
    // Another instance can change the leaves, so that no pair of this window
    // has the time of the transactions over them.
    let without_pair = rule.decide(&window(vec![], vec![]));

    assert_eq!(with_pair, vec![]);
    assert_eq!(without_pair, vec![ChangeRequest::Merge(leaf(1))]);
}

#[test]
fn a_leaf_that_split_recently_can_split_again_but_not_merge() {
    let split = LeafWindow {
        split_recently: true,
        ..delays_of_one_split()
    };
    let quiet = LeafWindow {
        split_recently: true,
        ..LeafWindow::default()
    };
    let window = window(
        vec![(leaf(1), split), (leaf(3), quiet)],
        vec![scan_crossing(1, 2, ms(5000)), scan_crossing(3, 4, ms(5000))],
    );

    assert_eq!(
        AvoidableTimeRule::default().decide(&window),
        vec![ChangeRequest::Split(leaf(1))]
    );
}

#[test]
fn a_leaf_that_merged_recently_can_merge_again_but_not_split() {
    let merged = |window: LeafWindow| LeafWindow {
        merged_recently: true,
        ..window
    };
    let window = window(
        vec![
            (leaf(1), merged(delays_of_one_split())),
            (leaf(5), merged(LeafWindow::default())),
        ],
        vec![scan_crossing(5, 6, ms(5000))],
    );

    assert_eq!(
        AvoidableTimeRule::default().decide(&window),
        vec![ChangeRequest::Merge(leaf(5))]
    );
}

#[test]
fn a_leaf_that_splits_or_merges_does_not_merge_again_in_the_same_window() {
    let window = window(
        vec![(leaf(4), delays_of_one_split())],
        vec![
            scan_crossing(1, 2, ms(100)),
            scan_crossing(2, 3, ms(100)),
            scan_crossing(3, 4, ms(900)),
        ],
    );

    assert_eq!(
        AvoidableTimeRule::default().decide(&window),
        vec![ChangeRequest::Split(leaf(4)), ChangeRequest::Merge(leaf(1)),]
    );
}
