use std::time::Duration;

use glassdb_data::{CollectionAddress, NodeId, ObjectPath};

use super::{AvoidableTime, ChangeKind, LeafChanges, MergeTime, SplitTime, TypicalTime, WINDOW};

fn node(byte: u8) -> ObjectPath {
    ObjectPath::Node {
        collection: CollectionAddress::root("db"),
        id: NodeId::from_bytes([byte; 16]),
    }
}

fn lost_cas(millis: u64) -> SplitTime {
    SplitTime {
        lost_cas: Duration::from_millis(millis),
        ..SplitTime::default()
    }
}

fn scan_crossing(millis: u64) -> MergeTime {
    MergeTime {
        scan_crossing: Duration::from_millis(millis),
        ..MergeTime::default()
    }
}

fn thresholds(split: f64, merge: f64) -> AvoidableTime {
    AvoidableTime::new(LeafChanges::AvoidableTime {
        split_threshold: split,
        merge_threshold: merge,
    })
}

// The rule with a threshold of one typical change time, which makes the
// boundaries of the tests easy to read.
fn at_one_change_time() -> AvoidableTime {
    thresholds(1.0, 1.0)
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

#[tokio::test(start_paused = true)]
async fn a_leaf_pays_for_a_split_once_in_a_window_when_its_time_is_more_than_a_split() {
    let avoidable = at_one_change_time();

    let paid: Vec<_> = [300, 200, 1, 1000]
        .map(|millis| avoidable.add_split_time(&node(1), lost_cas(millis)))
        .into();
    assert_eq!(paid, [false, false, true, false]);

    tokio::time::advance(WINDOW).await;
    assert!(!avoidable.add_split_time(&node(1), lost_cas(500)));
    assert!(avoidable.add_split_time(&node(1), lost_cas(1)));
}

#[tokio::test(start_paused = true)]
async fn the_thresholds_scale_the_typical_change_times() {
    let avoidable = thresholds(2.0, 0.5);
    avoidable.record_change(ChangeKind::Split, Duration::from_millis(100));

    assert!(!avoidable.add_split_time(&node(1), lost_cas(200)));
    assert!(avoidable.add_split_time(&node(1), lost_cas(1)));
    assert!(avoidable.add_merge_time(&node(3), &node(4), scan_crossing(251)));
}

#[tokio::test(start_paused = true)]
async fn two_leaves_pay_for_a_merge_and_for_the_split_side_time_of_both() {
    let avoidable = at_one_change_time();
    avoidable.add_split_time(&node(1), lost_cas(200));
    avoidable.add_split_time(&node(2), lost_cas(100));

    assert!(!avoidable.add_merge_time(&node(1), &node(2), scan_crossing(800)));
    assert!(avoidable.add_merge_time(&node(1), &node(2), scan_crossing(1)));
    assert!(!avoidable.add_merge_time(&node(1), &node(2), scan_crossing(1000)));
}

#[tokio::test(start_paused = true)]
async fn a_leaf_that_pays_for_a_split_does_not_merge_in_the_same_window() {
    let avoidable = at_one_change_time();
    assert!(avoidable.add_split_time(&node(1), lost_cas(501)));

    assert!(!avoidable.add_merge_time(&node(1), &node(2), scan_crossing(5000)));
}

#[tokio::test(start_paused = true)]
async fn a_change_holds_its_leaves_against_the_other_kind_for_two_windows() {
    let avoidable = at_one_change_time();
    avoidable.record_changed(node(1), ChangeKind::Split);
    avoidable.record_changed(node(3), ChangeKind::Merge);

    assert!(!avoidable.add_merge_time(&node(1), &node(2), scan_crossing(5000)));
    assert!(avoidable.add_merge_time(&node(3), &node(4), scan_crossing(501)));
    assert!(!avoidable.add_split_time(&node(3), lost_cas(5000)));
    assert!(avoidable.add_split_time(&node(1), lost_cas(501)));

    tokio::time::advance(WINDOW).await;
    assert!(!avoidable.add_merge_time(&node(1), &node(5), scan_crossing(5000)));
    assert!(!avoidable.add_split_time(&node(3), lost_cas(5000)));

    tokio::time::advance(WINDOW).await;
    assert!(avoidable.add_merge_time(&node(1), &node(5), scan_crossing(501)));
    assert!(avoidable.add_split_time(&node(3), lost_cas(501)));
}

#[tokio::test(start_paused = true)]
async fn a_change_drops_the_earlier_time_of_its_leaves() {
    let avoidable = at_one_change_time();
    avoidable.add_split_time(&node(1), lost_cas(400));
    avoidable.add_merge_time(&node(3), &node(4), scan_crossing(400));

    avoidable.record_changed(node(1), ChangeKind::Split);
    avoidable.record_changed(node(3), ChangeKind::Merge);

    assert!(!avoidable.add_split_time(&node(1), lost_cas(400)));
    assert!(!avoidable.add_merge_time(&node(3), &node(4), scan_crossing(400)));

    let totals = avoidable.take_stats();
    assert_eq!(totals.split, lost_cas(800));
    assert_eq!(totals.merge, scan_crossing(800));
}

#[tokio::test(start_paused = true)]
async fn the_size_rule_keeps_totals_and_decides_nothing() {
    let avoidable = AvoidableTime::new(LeafChanges::Size);

    assert!(!avoidable.decides());
    assert!(!avoidable.add_split_time(&node(1), lost_cas(5000)));
    assert!(!avoidable.add_merge_time(&node(1), &node(2), scan_crossing(5000)));
    let totals = avoidable.take_stats();
    assert_eq!(totals.split, lost_cas(5000));
    assert_eq!(totals.merge, scan_crossing(5000));
}

#[test]
#[should_panic(expected = "a threshold multiple must be finite and not negative, got -1")]
fn negative_thresholds_are_rejected() {
    thresholds(-1.0, 1.0);
}
