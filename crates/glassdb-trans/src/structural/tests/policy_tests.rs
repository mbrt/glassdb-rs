//! The topology signals that each topology policy uses.

use std::collections::BTreeSet;

use super::*;
use crate::leaf_coord::LeafDelayKind;

fn causes(sp: &Restructurer) -> Vec<CandidateCause> {
    sp.candidates
        .drain()
        .0
        .into_iter()
        .map(|candidate| candidate.cause)
        .collect()
}

// Gives `sp` the signals that only one of the two policies uses: an inline
// pressure, lost leaf CASes on one key, and a leaf delay.
fn send_policy_signals(sp: &Restructurer) {
    sp.hints.observe_inline_pressure(&root_path(), b"b", 8);
    let round = BTreeSet::from([b"b".to_vec()]);
    for _ in 0..40 {
        sp.hints.lost_leaf_cas(&root_path(), &round);
    }
    sp.hints.leaf_delay(
        &root_path(),
        Duration::from_secs(1),
        b"b",
        LeafDelayKind::LostCas,
    );
}

// The size causes split for inline pressure and for contended keys, and take
// no measurements of time.
#[tokio::test]
async fn the_size_causes_queue_splits_and_measure_no_time() {
    let s = store();
    let bg = Arc::new(Background::new());
    let topology =
        TestTopology::size_causes(NodeSizePolicy::default()).with_inline(pressure_inline());
    let sp = restructurer_with_topology(&s, &bg, topology);

    send_policy_signals(&sp);

    let causes = causes(&sp);
    assert!(causes.iter().any(|cause| cause.is_inline_pressure()));
    assert!(
        causes
            .iter()
            .any(|cause| matches!(cause, CandidateCause::Split(SplitReason::Contention { .. })))
    );
    assert!(sp.stats_and_reset().topology_time.lost_cas.is_zero());
}

// The avoidable time policy measures the leaf delays, and queues nothing for
// inline pressure or for lost leaf CASes: its rule decides from the time.
#[tokio::test]
async fn the_avoidable_time_policy_measures_time_and_queues_nothing() {
    let s = store();
    let bg = Arc::new(Background::new());
    let topology =
        TestTopology::avoidable_time(NodeSizePolicy::default()).with_inline(pressure_inline());
    let sp = restructurer_with_topology(&s, &bg, topology);

    send_policy_signals(&sp);

    assert!(causes(&sp).is_empty());
    assert_eq!(
        sp.stats_and_reset().topology_time.lost_cas,
        Duration::from_secs(1)
    );
}
