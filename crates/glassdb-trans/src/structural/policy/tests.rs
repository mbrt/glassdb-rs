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

fn scan_crossing(left: u8, right: u8, millis: u64) -> PairWindow {
    PairWindow {
        left: leaf(left),
        right: leaf(right),
        avoidable: MergeTime {
            scan_crossing: ms(millis),
            ..MergeTime::default()
        },
    }
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

#[test]
fn a_leaf_splits_when_its_split_side_time_is_more_than_a_split() {
    let window = window(
        vec![(leaf(1), lost_cas(501)), (leaf(2), lost_cas(500))],
        vec![],
    );

    assert_eq!(
        AvoidableTimePolicy::new().decide(&window),
        vec![TopologyChange::Split(leaf(1))]
    );
}

#[test]
fn the_split_threshold_scales_the_split_time() {
    let window = window(vec![(leaf(1), lost_cas(900))], vec![]);

    assert_eq!(
        AvoidableTimePolicy::new()
            .split_threshold(2.0)
            .decide(&window),
        vec![]
    );
}

#[test]
fn two_leaves_merge_when_merge_side_time_pays_for_the_merge_and_both_split_sides() {
    let leaves = || vec![(leaf(1), lost_cas(200)), (leaf(2), lost_cas(100))];
    let policy = AvoidableTimePolicy::new();

    let pays = window(leaves(), vec![scan_crossing(1, 2, 801)]);
    assert_eq!(policy.decide(&pays), vec![TopologyChange::Merge(leaf(1))]);

    let does_not_pay = window(leaves(), vec![scan_crossing(1, 2, 800)]);
    assert_eq!(policy.decide(&does_not_pay), vec![]);
}

#[test]
fn a_leaf_that_split_recently_can_split_again_but_not_merge() {
    let split = LeafWindow {
        split_recently: true,
        ..lost_cas(501)
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
        AvoidableTimePolicy::new().decide(&window),
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
            (leaf(1), merged(lost_cas(5000))),
            (leaf(3), merged(sized(9, 9))),
            (leaf(5), merged(LeafWindow::default())),
        ],
        vec![scan_crossing(5, 6, 5000)],
    );

    assert_eq!(
        AvoidableTimePolicy::new().decide(&window),
        vec![
            TopologyChange::Split(leaf(3)),
            TopologyChange::Merge(leaf(5)),
        ]
    );
}

#[test]
fn a_leaf_that_splits_or_merges_does_not_merge_again_in_the_same_window() {
    let window = window(
        vec![(leaf(4), lost_cas(600))],
        vec![
            scan_crossing(1, 2, 1000),
            scan_crossing(2, 3, 1000),
            scan_crossing(3, 4, 1000),
        ],
    );

    assert_eq!(
        AvoidableTimePolicy::new().decide(&window),
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
        AvoidableTimePolicy::new().decide(&window),
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
