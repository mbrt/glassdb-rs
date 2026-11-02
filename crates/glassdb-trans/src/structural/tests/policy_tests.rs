//! The topology signals that each topology policy uses.

use std::collections::BTreeSet;

use super::*;

fn causes(sp: &Restructurer) -> Vec<CandidateCause> {
    sp.candidates
        .drain()
        .into_iter()
        .map(|candidate| candidate.cause)
        .collect()
}

// Gives `sp` the signals that not every policy uses: an inline pressure, and
// lost leaf CASes on one key.
fn send_policy_signals(sp: &Restructurer) {
    sp.hints.observe_inline_pressure(&root_path(), b"b", 8);
    let round = BTreeSet::from([b"b".to_vec()]);
    for _ in 0..40 {
        sp.hints.lost_leaf_cas(&root_path(), &round);
    }
}

// The writers policy splits for inline pressure and for contended keys.
#[tokio::test]
async fn the_writers_policy_queues_inline_pressure_and_contention_splits() {
    let s = store();
    let bg = Arc::new(Background::new());
    let topology = TestTopology::writers(NodeSizePolicy::default()).with_inline(pressure_inline());
    let sp = restructurer_with_topology(&s, &bg, topology);

    send_policy_signals(&sp);

    let causes = causes(&sp);
    assert!(causes.iter().any(|cause| cause.is_inline_pressure()));
    assert!(
        causes
            .iter()
            .any(|cause| matches!(cause, CandidateCause::Split(SplitReason::Contention { .. })))
    );
}

// The size causes split only for the leaf sizes: inline pressure splits, and
// lost leaf CASes do not.
#[tokio::test]
async fn the_size_causes_queue_only_size_splits() {
    let s = store();
    let bg = Arc::new(Background::new());
    let topology =
        TestTopology::size_causes(NodeSizePolicy::default()).with_inline(pressure_inline());
    let sp = restructurer_with_topology(&s, &bg, topology);

    send_policy_signals(&sp);

    let causes = causes(&sp);
    assert!(causes.iter().any(|cause| cause.is_inline_pressure()));
    assert!(
        !causes
            .iter()
            .any(|cause| matches!(cause, CandidateCause::Split(SplitReason::Contention { .. })))
    );
}
