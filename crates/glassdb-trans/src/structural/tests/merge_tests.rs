use super::*;

use glassdb_backend::middleware::HookOutcome;
use glassdb_storage::{PaidChange, Payment};

use crate::structural::rule::{ChangeRequest, LeafId, RuleRequest};

// Leaves below four live entries are underfull, and a merged leaf may hold at
// most four entries.
fn mergeable() -> NodeSizePolicy {
    NodeSizePolicy::builder()
        .leaf_max_entries(8)
        .leaf_min_entries(4)
        .node_soft_max_bytes(1 << 20)
        .build()
        .unwrap()
}

// A root over two adjacent leaves: L0 covers the keys below "m", and L1 the
// others.
async fn seed_two_leaves(s: &TestStore, left: &[&[u8]], right: &[&[u8]]) {
    seed_leaves(s, leaf_node(left, None, None), leaf_node(right, None, None)).await;
}

async fn seed_leaves(s: &TestStore, left: Node, right: Node) {
    assert!(s.store_node(COLL, "L0", &as_l0(left), None).await.unwrap());
    assert!(
        s.store_node(COLL, "L1", &right.with_low_key(b"m".to_vec()), None)
            .await
            .unwrap()
    );
    s.create_root(COLL, &Node::index(test_index(&[(b"", "L0"), (b"m", "L1")])))
        .await
        .unwrap();
}

fn as_l0(node: Node) -> Node {
    as_l0_of(node, b"m", "L1")
}

// Makes `node` the left neighbor of `right`, which starts at `high`.
fn as_l0_of(node: Node, high: &[u8], right: &str) -> Node {
    node.with_high_key(Some(high.to_vec()))
        .with_right_sibling(Some(test_node_id(right)))
}

async fn current_node(s: &TestStore, name: &str) -> Node {
    s.load_node(
        COLL,
        name,
        Requirement::after(s.timeline.currentness_barrier()),
    )
    .await
    .unwrap()
    .0
}

fn keys(node: &Node) -> Vec<Vec<u8>> {
    node.as_leaf()
        .unwrap()
        .entries()
        .map(|entry| entry.key.clone())
        .collect()
}

async fn assert_no_intents(s: &TestStore) {
    assert!(
        s.discover_structural_intents("db", Requirement::after(s.timeline.currentness_barrier()))
            .await
            .unwrap()
            .is_empty()
    );
}

// The two leaves are as seeded: the merge did not take effect.
async fn assert_not_merged(s: &TestStore, left: &[&[u8]], right: &[&[u8]]) {
    let left_node = current_node(s, "L0").await;
    assert!(!left_node.is_drained());
    assert!(left_node.structural_gate().holders().is_empty());
    assert_eq!(keys(&left_node), left);
    let right_node = current_node(s, "L1").await;
    assert_eq!(right_node.low_key(), b"m");
    assert!(right_node.locks().merge_reservation().is_none());
    assert_eq!(keys(&right_node), right);
    assert_no_intents(s).await;
}

// L0 is drained into L1, which holds all keys, and the root names only L1.
async fn assert_merged(s: &TestStore) {
    let left = current_node(s, "L0").await;
    assert!(left.is_drained());
    assert_eq!(left.right_sibling(), Some(test_node_id("L1")));
    let right = current_node(s, "L1").await;
    assert_eq!(right.low_key(), b"");
    assert_eq!(keys(&right), [b"a", b"b", b"m", b"n"]);
    assert!(right.locks().merge_reservation().is_none());
    let (root, _) = s
        .load_root(COLL, Requirement::after(s.timeline.currentness_barrier()))
        .await
        .unwrap();
    assert_eq!(root.as_index().unwrap(), &test_index(&[(b"", "L1")]));
    assert_no_intents(s).await;
}

async fn merge_l0(sp: &Restructurer) -> Result<(), TransError> {
    merge_now(sp, "L0", MergeReason::Underfull).await
}

// Merges the leaf `name` into its right sibling at once, if `reason` still
// calls for it, and returns the result that a sweep settles.
async fn merge_now(sp: &Restructurer, name: &str, reason: MergeReason) -> Result<(), TransError> {
    let path = node_path(name);
    let Some(nodes) = sp.locate_merge(&path, reason).await? else {
        return Ok(());
    };
    let change = PlannedChange::Merge {
        collection: &nodes.collection,
        source: &nodes.source,
        reason,
    };
    sp.changes
        .run(
            change,
            TxId::new_at(rt::system_now()),
            StructuralTopology::Owned,
        )
        .await
}

// Splits a leaf with `entries` at "m" for a topology rule, with a paid rate of
// 1000 ms in each second, and returns its two halves.
fn split_for_rule(entries: impl IntoIterator<Item = LeafEntry>) -> (Node, Node) {
    let mut left = Node::leaf(LeafBody::from_entries(entries));
    let paid = Payment {
        per_second: Duration::from_secs(1),
        intent: test_intent_id("paid"),
    };
    let (right, _) = left
        .split_leaf_at(test_node_id("L1"), b"m", Some(paid))
        .unwrap();
    (left, right)
}

// L0 and L1 as a split for a topology rule left them.
async fn seed_split_leaves(s: &TestStore) {
    let (left, right) = split_for_rule([live(b"a"), live(b"b"), live(b"m"), live(b"n")]);
    seed_leaves(s, left, right).await;
}

// L0 with no live entries and L1, as a split for a topology rule left them.
async fn seed_split_leaves_with_empty_left(s: &TestStore) {
    let deleted = tombstone(b"a", tx_id(b"deleter"));
    let (left, right) = split_for_rule([deleted, live(b"m"), live(b"n")]);
    seed_leaves(s, left, right).await;
}

// A restructurer whose leaves merge only when a topology rule asks.
fn rule_restructurer(s: &TestStore, bg: &Arc<Background>) -> Restructurer {
    let candidates = MaintenanceCandidates::for_topology_rule(mergeable(), InlinePolicy::default());
    restructurer_with_candidates(s, bg, candidates)
}

async fn demand_merge_l0(sp: &Restructurer, rate: Duration) -> Result<(), TransError> {
    merge_now(sp, "L0", MergeReason::Demand { rate }).await
}

// ADR-076: the leaves of a split that a topology rule asked for keep its paid
// rate. A merge that a rule asks for must pay more than 1.5 times it, and the
// leaf that receives the entries keeps the paid rate of the merge.
#[tokio::test(start_paused = true)]
async fn a_demand_merge_must_pay_more_than_the_paid_rate_of_the_split() {
    let s = store();
    seed_split_leaves(&s).await;
    let bg = Arc::new(Background::new());
    let sp = rule_restructurer(&s, &bg);

    demand_merge_l0(&sp, Duration::from_millis(1500))
        .await
        .unwrap();
    assert_not_merged(&s, &[b"a", b"b"], &[b"m", b"n"]).await;
    assert_eq!(
        sp.stats_and_reset().demand_merges,
        DemandMergeStats {
            unpaid: 1,
            ..DemandMergeStats::default()
        }
    );

    demand_merge_l0(&sp, Duration::from_millis(1600))
        .await
        .unwrap();
    assert_merged(&s).await;
    let kept = current_node(&s, "L1").await.paid_rate().cloned().unwrap();
    assert_eq!(
        (kept.change, kept.per_second),
        (PaidChange::Merge, Duration::from_millis(1600))
    );
}

// ADR-076: a paid rate halves each minute after this database instance first
// compared a request with it, so that a change of the load can change the
// topology again.
#[tokio::test(start_paused = true)]
async fn a_paid_rate_holds_less_as_it_gets_older() {
    let s = store();
    seed_split_leaves(&s).await;
    let bg = Arc::new(Background::new());
    let sp = rule_restructurer(&s, &bg);

    demand_merge_l0(&sp, Duration::from_millis(1000))
        .await
        .unwrap();
    assert_not_merged(&s, &[b"a", b"b"], &[b"m", b"n"]).await;

    tokio::time::advance(Duration::from_secs(60)).await;
    demand_merge_l0(&sp, Duration::from_millis(1000))
        .await
        .unwrap();
    assert_merged(&s).await;
}

// A paid rate holds only the changes that a topology rule asks for. A leaf
// with no live entries still merges, and the merged leaf keeps no paid rate.
#[tokio::test(start_paused = true)]
async fn a_paid_rate_does_not_hold_the_merge_of_an_empty_leaf() {
    let s = store();
    seed_split_leaves_with_empty_left(&s).await;
    let bg = Arc::new(Background::new());
    let sp = rule_restructurer(&s, &bg);

    merge_l0(&sp).await.unwrap();

    let merged = current_node(&s, "L1").await;
    assert_eq!(keys(&merged), [b"m", b"n"]);
    assert_eq!(merged.paid_rate(), None);
}

// Regression: a merge that a topology rule asked for, and that went ahead
// only because its source had no live entries, kept its rate as a paid rate,
// although that rate did not pay for it.
#[tokio::test(start_paused = true)]
async fn a_demand_merge_of_an_empty_leaf_keeps_no_paid_rate() {
    let s = store();
    seed_split_leaves_with_empty_left(&s).await;
    let bg = Arc::new(Background::new());
    let sp = rule_restructurer(&s, &bg);

    demand_merge_l0(&sp, Duration::from_millis(10))
        .await
        .unwrap();

    let merged = current_node(&s, "L1").await;
    assert_eq!(keys(&merged), [b"m", b"n"]);
    assert_eq!(merged.paid_rate(), None);
}

#[tokio::test]
async fn underfull_leaf_merges_into_its_right_sibling() {
    let s = store();
    seed_two_leaves(&s, &[b"a", b"b"], &[b"m", b"n"]).await;
    let bg = Arc::new(Background::new());
    let sp = restructurer(&s, &bg, mergeable());

    sp.candidates.observe_leaf(
        &node_path("L0"),
        &LeafBody::from_entries([live(b"a"), live(b"b")]),
    );
    sp.run_once().await;

    assert_merged(&s).await;
    assert_eq!(
        sp.stats_and_reset(),
        RestructurerStats {
            candidates: 1,
            merges: 1,
            ..RestructurerStats::default()
        }
    );
}

#[tokio::test]
async fn merge_is_vetoed_when_the_merged_leaf_would_split_soon() {
    let s = store();
    seed_two_leaves(&s, &[b"a"], &[b"m", b"n", b"o", b"p"]).await;
    let bg = Arc::new(Background::new());
    let sp = restructurer(&s, &bg, mergeable());

    merge_l0(&sp).await.unwrap();

    assert_not_merged(&s, &[b"a"], &[b"m", b"n", b"o", b"p"]).await;
    assert_eq!(
        sp.stats_and_reset().demand_merges,
        DemandMergeStats::default(),
        "only the merges of a topology rule count"
    );
}

// The size veto of ADR-073 also stops the merges of a topology rule, and the
// stats show it.
#[tokio::test]
async fn a_demand_merge_that_would_split_soon_counts_as_too_large() {
    let s = store();
    seed_two_leaves(&s, &[b"a"], &[b"m", b"n", b"o", b"p"]).await;
    let bg = Arc::new(Background::new());
    let sp = rule_restructurer(&s, &bg);

    demand_merge_l0(&sp, Duration::from_secs(1)).await.unwrap();

    assert_not_merged(&s, &[b"a"], &[b"m", b"n", b"o", b"p"]).await;
    assert_eq!(
        sp.stats_and_reset().demand_merges,
        DemandMergeStats {
            too_large: 1,
            ..DemandMergeStats::default()
        }
    );
}

// ADR-083: a sweep drops a merge that a topology rule asked for more than
// three windows before.
#[tokio::test(start_paused = true)]
async fn a_sweep_drops_a_stale_demand_merge() {
    let s = store();
    seed_two_leaves(&s, &[b"a", b"b"], &[b"m", b"n"]).await;
    let bg = Arc::new(Background::new());
    let sp = rule_restructurer(&s, &bg);

    sp.candidates.push_request(RuleRequest {
        change: ChangeRequest::Merge(LeafId::new(node_path("L0"))),
        rate: Duration::from_secs(1),
    });
    tokio::time::advance(Duration::from_secs(4)).await;
    sp.run_once().await;

    assert_not_merged(&s, &[b"a", b"b"], &[b"m", b"n"]).await;
    let stats = sp.stats_and_reset();
    assert_eq!((stats.stale_demand, stats.demand_merges.stale), (1, 1));
}

// After a bulk delete, the target holds mostly cold tombstones. Counted as
// entries, they would veto the merge, and the last leaf of a level, which is
// never a merge source, would keep them.
#[tokio::test]
async fn merge_compacts_the_cold_tombstones_of_its_target() {
    let s = store();
    let deleted = [
        TxId::with_priority(2, b"o"),
        TxId::with_priority(3, b"p"),
        TxId::with_priority(4, b"q"),
    ];
    let right = LeafBody::from_entries([
        live(b"m"),
        live(b"n"),
        tombstone(b"o", deleted[0]),
        tombstone(b"p", deleted[1]),
        tombstone(b"q", deleted[2]),
    ]);
    seed_leaves(&s, leaf_node(&[b"a", b"b"], None, None), Node::leaf(right)).await;
    let bg = Arc::new(Background::new());
    let gc_hints = GcHints::default();
    let sp = restructurer_with_candidates_and_hints(
        &s,
        &bg,
        MaintenanceCandidates::with_policy(mergeable()),
        gc_hints.clone(),
    );

    merge_l0(&sp).await.unwrap();

    assert_merged(&s).await;
    assert_eq!(
        sp.stats_and_reset(),
        RestructurerStats {
            merges: 1,
            tombstones_reclaimed: 3,
            ..RestructurerStats::default()
        }
    );
    assert_eq!(gc_hints.pending(), deleted);
}

// Without this veto, a merge could undo each capacity split when no soft limit
// applies, and writes to the merged leaf would be rejected again.
#[tokio::test]
async fn merge_is_vetoed_when_the_merged_leaf_would_need_a_capacity_split() {
    let policy = NodeSizePolicy::builder()
        .node_max_bytes(512)
        .split_headroom_bytes(128)
        .leaf_max_entries(usize::MAX)
        .leaf_min_entries(4)
        .node_soft_max_bytes(usize::MAX)
        .build()
        .unwrap();
    let key = |first: u8| [&[first], [b'x'; 59].as_slice()].concat();
    let (a, b, m, n) = (key(b'a'), key(b'b'), key(b'm'), key(b'n'));
    let s = store();
    seed_two_leaves(&s, &[&a, &b], &[&m, &n]).await;
    let merged_len = current_node(&s, "L0").await.content_encoded_len()
        + current_node(&s, "L1").await.content_encoded_len();
    assert!(
        merged_len > policy.content_limit() / 2 && merged_len <= policy.content_limit(),
        "the merged leaf fits the hard cap, but needs a capacity split soon"
    );
    let bg = Arc::new(Background::new());
    let sp = restructurer(&s, &bg, policy);

    merge_l0(&sp).await.unwrap();

    assert_not_merged(&s, &[&a, &b], &[&m, &n]).await;
}

#[tokio::test]
async fn merge_stays_among_the_children_of_one_parent() {
    let s = store();
    assert!(
        s.store_node(COLL, "L0", &as_l0(leaf_node(&[b"a"], None, None)), None)
            .await
            .unwrap()
    );
    assert!(
        s.store_node(
            COLL,
            "L1",
            &leaf_node(&[b"m"], None, None).with_low_key(b"m".to_vec()),
            None,
        )
        .await
        .unwrap()
    );
    s.store_node(
        COLL,
        "P0",
        &Node::index(test_index(&[(b"", "L0")]))
            .with_high_key(Some(b"m".to_vec()))
            .with_right_sibling(Some(test_node_id("P1"))),
        None,
    )
    .await
    .unwrap();
    s.store_node(
        COLL,
        "P1",
        &Node::index(test_index(&[(b"m", "L1")])).with_low_key(b"m".to_vec()),
        None,
    )
    .await
    .unwrap();
    s.create_root(COLL, &Node::index(test_index(&[(b"", "P0"), (b"m", "P1")])))
        .await
        .unwrap();
    let bg = Arc::new(Background::new());
    let sp = restructurer(&s, &bg, mergeable());

    merge_l0(&sp).await.unwrap();

    assert_not_merged(&s, &[b"a"], &[b"m"]).await;
}

// A leaf merge under P0 leaves P0 underfull, so P0 merges into P1. Then the
// leaves of P0 and P1 have one parent and can merge too.
#[tokio::test]
async fn merges_cascade_through_underfull_parents() {
    let s = store();
    let nodes = [
        ("L0", as_l0_of(leaf_node(&[b"a"], None, None), b"f", "L0b")),
        (
            "L0b",
            leaf_node(&[b"f"], Some(b"m"), Some("L1")).with_low_key(b"f".to_vec()),
        ),
        (
            "L1",
            leaf_node(&[b"m"], None, None).with_low_key(b"m".to_vec()),
        ),
        (
            "P0",
            as_l0_of(
                Node::index(test_index(&[(b"", "L0"), (b"f", "L0b")])),
                b"m",
                "P1",
            ),
        ),
        (
            "P1",
            Node::index(test_index(&[(b"m", "L1")])).with_low_key(b"m".to_vec()),
        ),
    ];
    for (name, node) in &nodes {
        assert!(s.store_node(COLL, name, node, None).await.unwrap());
    }
    s.create_root(COLL, &Node::index(test_index(&[(b"", "P0"), (b"m", "P1")])))
        .await
        .unwrap();
    let policy = NodeSizePolicy::builder()
        .leaf_max_entries(8)
        .leaf_min_entries(4)
        .index_max_children(8)
        .index_min_children(3)
        .node_soft_max_bytes(1 << 20)
        .build()
        .unwrap();
    let bg = Arc::new(Background::new());
    let sp = restructurer(&s, &bg, policy);

    sp.candidates
        .observe_leaf(&node_path("L0"), &LeafBody::from_entries([live(b"a")]));
    sp.run_once().await;
    sp.run_once().await;

    let parent = current_node(&s, "P0").await;
    assert!(parent.is_drained());
    assert_eq!(parent.right_sibling(), Some(test_node_id("P1")));
    let (root, _) = s
        .load_root(COLL, Requirement::after(s.timeline.currentness_barrier()))
        .await
        .unwrap();
    assert_eq!(root.as_index().unwrap(), &test_index(&[(b"", "P1")]));

    sp.candidates.observe_leaf(
        &node_path("L0b"),
        &LeafBody::from_entries([live(b"a"), live(b"f")]),
    );
    sp.run_once().await;

    let merged = current_node(&s, "L1").await;
    assert_eq!(merged.low_key(), b"");
    assert_eq!(keys(&merged), [b"a", b"f", b"m"]);
    assert_eq!(
        current_node(&s, "P1").await.as_index().unwrap(),
        &test_index(&[(b"", "L1")])
    );
    let router = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN);
    for key in [b"a".as_slice(), b"f", b"m"] {
        let leaf = router
            .route_key(
                &collection(),
                key,
                Requirement::after(s.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        assert!(leaf.node().unwrap().as_leaf().unwrap().exists(key));
    }
    assert_no_intents(&s).await;
    assert_eq!(sp.stats_and_reset().merges, 3);
}

#[tokio::test(start_paused = true)]
async fn a_new_candidate_does_not_wait_for_the_sweep_interval() {
    let s = store();
    seed_two_leaves(&s, &[b"a", b"b"], &[b"m", b"n"]).await;
    let bg = Arc::new(Background::new());
    let sp = restructurer(&s, &bg, mergeable());
    sp.start();

    sp.candidates.observe_leaf(
        &node_path("L0"),
        &LeafBody::from_entries([live(b"a"), live(b"b")]),
    );
    tokio::time::timeout(SWEEP_INTERVAL, async {
        while !current_node(&s, "L0").await.is_drained() {
            rt::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the merge must land before the sweep interval");

    assert_merged(&s).await;
}

// Without this rule, a merge that defers to a pending holder would retry after
// each coalescing delay until the holder finishes.
#[tokio::test(start_paused = true)]
async fn a_deferred_candidate_waits_for_the_sweep_interval() {
    let s = store();
    let bg = Arc::new(Background::new());
    let (sp, mon) = restructurer_and_monitor(&s, &bg, mergeable());
    let younger = TxId::new_at(rt::system_now() + Duration::from_secs(1));
    mon.begin_tx(&younger);
    seed_leaves(
        &s,
        leaf_with_locked_entry(&[b"a", b"b"], &younger),
        leaf_node(&[b"m"], None, None),
    )
    .await;
    sp.start();
    let deferred_once = RestructurerStats {
        candidates: 1,
        deferred: 1,
        ..RestructurerStats::default()
    };

    sp.candidates.observe_leaf(
        &node_path("L0"),
        &LeafBody::from_entries([live(b"a"), live(b"b")]),
    );
    rt::sleep(SWEEP_INTERVAL / 2).await;
    assert_eq!(sp.stats_and_reset(), deferred_once);

    rt::sleep(SWEEP_INTERVAL).await;
    assert_eq!(sp.stats_and_reset(), deferred_once);
}

// Regression: the typical merge time included the time before the merge held
// its gate, mostly a wait for the writers of the leaf. One slow start then
// raised the merge threshold of the database instance for a long time. The
// test makes the start slow with a slow write of the structural intent,
// which also comes before the gate.
#[tokio::test(start_paused = true)]
async fn the_time_before_a_merge_holds_its_gate_is_not_its_typical_time() {
    const SLOW_START: Duration = Duration::from_secs(20);
    let hook = HookBackend::new(Arc::new(MemoryBackend::new()));
    let s = store_with_backend(hook.clone());
    seed_two_leaves(&s, &[b"a", b"b"], &[b"m", b"n"]).await;
    let bg = Arc::new(Background::new());
    let candidates = MaintenanceCandidates::for_topology_rule(mergeable(), InlinePolicy::default());
    let sp = restructurer_with_candidates(&s, &bg, candidates);
    let prefix = ObjectPath::structural_intents_prefix(&db_prefix("db"));
    // The intent is written before the merge gets its gate.
    hook.set_before(move |op| {
        let intent =
            matches!(op, BackendOp::WriteIfNotExists { .. }) && op.path().starts_with(&prefix);
        Box::pin(async move {
            if intent {
                rt::sleep(SLOW_START).await;
            }
            rt::sleep(Duration::from_millis(1)).await;
            Ok(())
        })
    });

    sp.candidates.push_request(RuleRequest {
        change: ChangeRequest::Merge(LeafId::new(node_path("L0"))),
        rate: Duration::from_secs(1),
    });
    sp.run_once().await;

    assert_merged(&s).await;
    let window = sp.candidates.measurements().unwrap().take_window();
    // Below the default of 500 ms, so the merge recorded its time.
    assert!(
        window.merge_time < Duration::from_millis(400),
        "{:?}",
        window.merge_time
    );
}

// A merge is optional maintenance, so it never aborts a user transaction.
#[tokio::test]
async fn merge_defers_to_a_younger_entry_holder_then_lands() {
    let s = store();
    let bg = Arc::new(Background::new());
    let (sp, mon) = restructurer_and_monitor(&s, &bg, mergeable());
    let younger = TxId::new_at(rt::system_now() + Duration::from_secs(1));
    mon.begin_tx(&younger);
    seed_leaves(
        &s,
        leaf_with_locked_entry(&[b"a", b"b"], &younger),
        leaf_node(&[b"m"], None, None),
    )
    .await;

    sp.candidates.observe_leaf(
        &node_path("L0"),
        &LeafBody::from_entries([live(b"a"), live(b"b")]),
    );
    sp.run_once().await;

    assert_eq!(
        mon.tx_status(&younger).await.unwrap(),
        TxCommitStatus::Pending
    );
    assert!(!current_node(&s, "L0").await.is_drained());

    mon.abort_owned_tx(&younger).await.unwrap();
    sp.run_once().await;

    assert!(current_node(&s, "L0").await.is_drained());
    assert_eq!(keys(&current_node(&s, "L1").await), [b"a", b"b", b"m"]);
}

// A holder can arrive after the planner checked the claims of the source. The
// polite gate acquisition then stops the merge without wounding it.
#[tokio::test]
async fn polite_gate_leaves_a_late_holder_pending() {
    let memory = Arc::new(MemoryBackend::new());
    let peer = store_with_backend(memory.clone());
    seed_two_leaves(&peer, &[b"a", b"b"], &[b"m"]).await;
    let hook = HookBackend::new(memory);
    let local = store_with_backend(hook.clone());
    let bg = Arc::new(Background::new());
    let (sp, mon) = restructurer_and_monitor(&local, &bg, mergeable());
    let younger = TxId::new_at(rt::system_now() + Duration::from_secs(1));
    mon.begin_tx(&younger);
    let prefix = ObjectPath::structural_intents_prefix(&db_prefix("db"));
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(true));
    hook.set_before({
        let peer = peer.clone();
        move |op| {
            let lock = matches!(op, BackendOp::WriteIfNotExists { .. })
                && op.path().starts_with(&prefix)
                && armed.swap(false, Ordering::SeqCst);
            let peer = peer.clone();
            let younger = younger;
            Box::pin(async move {
                if lock {
                    let (_, observation) = peer
                        .load_node(
                            COLL,
                            "L0",
                            Requirement::after(peer.timeline.currentness_barrier()),
                        )
                        .await
                        .unwrap();
                    let locked = as_l0(leaf_with_locked_entry(&[b"a", b"b"], &younger));
                    assert!(
                        peer.store_node(COLL, "L0", &locked, Some(&observation))
                            .await
                            .unwrap()
                    );
                }
                Ok(())
            })
        }
    });

    assert!(matches!(merge_l0(&sp).await, Err(TransError::Retry)));

    hook.clear_before();
    assert_eq!(
        mon.tx_status(&younger).await.unwrap(),
        TxCommitStatus::Pending
    );
    let left = current_node(&local, "L0").await;
    assert!(!left.is_drained());
    assert!(left.structural_gate().holders().is_empty());
    assert!(
        left.as_leaf()
            .unwrap()
            .entries()
            .any(|entry| entry.is_locked_by(&younger))
    );
    assert_no_intents(&local).await;
}

#[derive(Debug, Clone, Copy)]
enum DrainLoss {
    GateRevoked,
    SourceChangedUnderGate,
}

// If the drain cannot land, the absorbed copies in the target must go away, and
// the source must not keep the gate of a merge that has no intent.
#[tokio::test]
async fn a_merge_that_loses_its_drain_abandons_the_absorb() {
    for loss in [DrainLoss::GateRevoked, DrainLoss::SourceChangedUnderGate] {
        let memory = Arc::new(MemoryBackend::new());
        let peer = store_with_backend(memory.clone());
        seed_two_leaves(&peer, &[b"a", b"b"], &[b"m", b"n"]).await;
        let hook = HookBackend::new(memory);
        let local = store_with_backend(hook.clone());
        let bg = Arc::new(Background::new());
        let sp = restructurer(&local, &bg, mergeable());
        let target = node_path("L1").to_string();
        let armed = Arc::new(std::sync::atomic::AtomicBool::new(true));
        hook.set_after({
            let peer = peer.clone();
            move |op, outcome: HookOutcome<'_>| {
                let change_source = matches!(op, BackendOp::WriteIf { .. })
                    && op.path() == target
                    && outcome.is_success()
                    && armed.swap(false, Ordering::SeqCst);
                let peer = peer.clone();
                Box::pin(async move {
                    if change_source {
                        let (mut left, observation) = peer
                            .load_node(
                                COLL,
                                "L0",
                                Requirement::after(peer.timeline.currentness_barrier()),
                            )
                            .await
                            .unwrap();
                        match loss {
                            DrainLoss::GateRevoked => {
                                let worker = left.structural_gate().holders()[0];
                                assert!(left.remove_structural_gate(&worker));
                            }
                            DrainLoss::SourceChangedUnderGate => {
                                let mut locks = left.locks().clone();
                                locks.advance_membership_generation();
                                left.set_locks(locks);
                            }
                        }
                        assert!(
                            peer.store_node(COLL, "L0", &left, Some(&observation))
                                .await
                                .unwrap()
                        );
                    }
                    Ok(())
                })
            }
        });

        assert!(
            matches!(merge_l0(&sp).await, Err(TransError::Retry)),
            "{loss:?}"
        );

        hook.clear_after();
        assert_not_merged(&local, &[b"a", b"b"], &[b"m", b"n"]).await;
        assert_eq!(sp.stats_and_reset().merges, 0, "{loss:?}");
    }
}

// The absorb lands only while the target has the membership generation that
// the Ready intent recorded.
#[tokio::test]
async fn absorb_stops_when_the_target_membership_changes_after_ready() {
    let memory = Arc::new(MemoryBackend::new());
    let peer = store_with_backend(memory.clone());
    seed_two_leaves(&peer, &[b"a", b"b"], &[b"m"]).await;
    let hook = HookBackend::new(memory);
    let local = store_with_backend(hook.clone());
    let bg = Arc::new(Background::new());
    let sp = restructurer(&local, &bg, mergeable());
    let target = node_path("L1").to_string();
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(true));
    hook.set_before({
        let peer = peer.clone();
        move |op| {
            let insert = matches!(op, BackendOp::WriteIf { .. })
                && op.path() == target
                && armed.swap(false, Ordering::SeqCst);
            let peer = peer.clone();
            Box::pin(async move {
                if insert {
                    let (mut right, observation) = peer
                        .load_node(
                            COLL,
                            "L1",
                            Requirement::after(peer.timeline.currentness_barrier()),
                        )
                        .await
                        .unwrap();
                    right
                        .set_leaf(LeafBody::from_entries([live(b"m"), live(b"n")]))
                        .unwrap();
                    let mut locks = right.locks().clone();
                    locks.advance_membership_generation();
                    right.set_locks(locks);
                    assert!(
                        peer.store_node(COLL, "L1", &right, Some(&observation))
                            .await
                            .unwrap()
                    );
                }
                Ok(())
            })
        }
    });

    assert!(matches!(merge_l0(&sp).await, Err(TransError::Retry)));

    hook.clear_before();
    assert_not_merged(&local, &[b"a", b"b"], &[b"m", b"n"]).await;
}

/// The last merge step that a crashed worker completed.
#[derive(Clone, Copy, Debug)]
enum MergeCrash {
    Ready,
    Absorb,
    Drain,
    ReservationRemoval,
}

// Stores the state that a merge worker leaves when it crashes after `crash`,
// with an aborted gate holder on L0. Returns the membership generation that
// the Ready intent records for L1.
async fn seed_interrupted_merge(s: &TestStore, sp: &Restructurer, crash: MergeCrash) -> u64 {
    seed_two_leaves(s, &[b"a", b"b"], &[b"m", b"n"]).await;
    let fresh = || Requirement::after(s.timeline.currentness_barrier());
    let worker = TxId::with_priority(1, b"merge-worker");
    let (mut left, observed) = s.load_node(COLL, "L0", fresh()).await.unwrap();
    left.set_structural_gate(worker);
    assert!(
        s.store_node(COLL, "L0", &left, Some(&observed))
            .await
            .unwrap()
    );
    let (left, gated) = s.load_node(COLL, "L0", fresh()).await.unwrap();
    let (mut right, observed) = s.load_node(COLL, "L1", fresh()).await.unwrap();
    let generation = right.membership_generation();
    let intent_id = test_intent_id("M");
    s.write_structural_intent(
        "M",
        &StructuralIntent {
            collection: collection(),
            source_node_id: Some(test_node_id("L0")),
            source_revision: gated.revision().unwrap().serialize().to_string(),
            change: StructuralChange::Merge {
                target: Some(MergeTarget {
                    node_id: test_node_id("L1"),
                    boundary: b"m".to_vec(),
                    generation,
                }),
            },
            participant_id: worker,
            phase: StructuralIntentPhase::Ready,
        },
    )
    .await
    .unwrap();
    sp.mon.begin_tx(&worker);
    assert_eq!(
        sp.mon.preempt_tx(&worker).await.unwrap(),
        TxFinalStatus::Aborted
    );
    if matches!(crash, MergeCrash::Ready) {
        return generation;
    }

    right.absorb(&left, intent_id, None).unwrap();
    assert!(
        s.store_node(COLL, "L1", &right, Some(&observed))
            .await
            .unwrap()
    );
    if matches!(crash, MergeCrash::Absorb) {
        return generation;
    }

    let mut drained = left;
    drained.drain(test_node_id("L1"));
    assert!(
        s.store_node(COLL, "L0", &drained, Some(&gated))
            .await
            .unwrap()
    );
    if matches!(crash, MergeCrash::Drain) {
        return generation;
    }

    let (mut right, observed) = s.load_node(COLL, "L1", fresh()).await.unwrap();
    assert!(right.remove_merge_reservation(&intent_id));
    assert!(
        s.store_node(COLL, "L1", &right, Some(&observed))
            .await
            .unwrap()
    );
    generation
}

#[tokio::test]
async fn recovery_finishes_a_drained_merge_and_abandons_the_others() {
    for crash in [
        MergeCrash::Ready,
        MergeCrash::Absorb,
        MergeCrash::Drain,
        MergeCrash::ReservationRemoval,
    ] {
        let s = store();
        let bg = Arc::new(Background::new());
        let sp = restructurer(&s, &bg, mergeable());
        let generation = seed_interrupted_merge(&s, &sp, crash).await;

        assert!(sp.recover_structural_intents().await.unwrap(), "{crash:?}");

        match crash {
            MergeCrash::Ready | MergeCrash::Absorb => {
                assert_not_merged(&s, &[b"a", b"b"], &[b"m", b"n"]).await;
                assert!(
                    current_node(&s, "L1").await.membership_generation() > generation,
                    "{crash:?}: an absorb that is still in flight must not land"
                );
            }
            MergeCrash::Drain | MergeCrash::ReservationRemoval => assert_merged(&s).await,
        }
    }
}

// Recovery can fence the source and delete the intent while the worker is
// about to absorb. If the absorb then landed and the worker crashed, nothing
// would remove its merge reservation. So the absorb must not land.
#[tokio::test]
async fn recovery_fences_a_late_absorb() {
    let memory = Arc::new(MemoryBackend::new());
    let peer = store_with_backend(memory.clone());
    seed_two_leaves(&peer, &[b"a", b"b"], &[b"m", b"n"]).await;
    let hook = HookBackend::new(memory);
    let local = store_with_backend(hook.clone());
    let bg = Arc::new(Background::new());
    let sp = restructurer(&local, &bg, mergeable());
    let peer_bg = Arc::new(Background::new());
    let recovering = restructurer(&peer, &peer_bg, mergeable());
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let absorbed = Arc::new(tokio::sync::Notify::new());
    let target = node_path("L1").to_string();
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(true));
    hook.set_before({
        let target = target.clone();
        let entered = entered.clone();
        let release = release.clone();
        move |op| {
            let park = matches!(op, BackendOp::WriteIf { .. })
                && op.path() == target
                && armed.swap(false, Ordering::SeqCst);
            let entered = entered.clone();
            let release = release.clone();
            Box::pin(async move {
                if park {
                    entered.notify_one();
                    release.notified().await;
                }
                Ok(())
            })
        }
    });
    // A landed absorb stops the worker for good, as a crash would.
    hook.set_after({
        let absorbed = absorbed.clone();
        move |op, outcome: HookOutcome<'_>| {
            let landed = matches!(op, BackendOp::WriteIf { .. })
                && op.path() == target
                && outcome.is_success();
            let absorbed = absorbed.clone();
            Box::pin(async move {
                if landed {
                    absorbed.notify_one();
                    std::future::pending::<()>().await;
                }
                Ok(())
            })
        }
    });
    let mut merge = tokio::spawn({
        let sp = sp.clone();
        async move { merge_l0(&sp).await }
    });
    entered.notified().await;

    let left = current_node(&peer, "L0").await;
    let worker = left.structural_gate().holders()[0];
    assert_eq!(
        recovering.mon.preempt_tx(&worker).await.unwrap(),
        TxFinalStatus::Aborted
    );
    assert!(recovering.recover_structural_intents().await.unwrap());
    release.notify_one();

    tokio::select! {
        result = &mut merge => assert!(result.unwrap().is_err()),
        _ = absorbed.notified() => {
            merge.abort();
            panic!("the absorb landed after recovery deleted its intent");
        }
    }
    hook.clear_before();
    hook.clear_after();
    assert_not_merged(&local, &[b"a", b"b"], &[b"m", b"n"]).await;
}

// A root over the leaves L0..L{count-1}, each with one key: "a", "b", and so
// on. Every leaf is underfull.
async fn seed_leaf_chain(s: &TestStore, count: u8) {
    let mut children = Vec::new();
    for leaf in 0..count {
        let name = format!("L{leaf}");
        let key = [b'a' + leaf];
        let low = if leaf == 0 { Vec::new() } else { key.to_vec() };
        let right = (leaf + 1 < count).then(|| format!("L{}", leaf + 1));
        let high = [b'a' + leaf + 1];
        let node = leaf_node(
            &[&key],
            right.as_ref().map(|_| high.as_slice()),
            right.as_deref(),
        )
        .with_low_key(low.clone());
        assert!(s.store_node(COLL, &name, &node, None).await.unwrap());
        children.push((low, test_node_id(&name)));
    }
    s.create_root(COLL, &Node::index(IndexNode::from_children(children)))
        .await
        .unwrap();
}

fn push_underfull_merges(sp: &Restructurer, names: &[&str]) {
    for name in names {
        sp.candidates.push(MaintenanceCandidate {
            path: node_path(name),
            priority: sp.candidates.new_id(),
            requested: rt::Instant::now(),
            cause: CandidateCause::Merge(MergeReason::Underfull),
        });
    }
}

// Counts the writes of the collection record while `sweep` runs.
async fn record_writes_of(backend: &HookBackend, sweep: impl Future<Output = ()>) -> usize {
    let writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    backend.set_before({
        let record = ObjectPath::CollectionRecord {
            collection: collection(),
        }
        .to_string();
        let writes = writes.clone();
        move |op| {
            if matches!(op, BackendOp::WriteIf { .. }) && op.path() == record {
                writes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            let future: HookFuture = Box::pin(async { Ok(()) });
            future
        }
    });
    sweep.await;
    backend.clear_before();
    writes.load(std::sync::atomic::Ordering::SeqCst)
}

async fn root_children(s: &TestStore) -> IndexNode {
    let (root, _) = s
        .load_root(COLL, Requirement::after(s.timeline.currentness_barrier()))
        .await
        .unwrap();
    root.as_index().unwrap().clone()
}

// Under a load that crosses many leaf boundaries, a topology rule asks for
// many more merges than one merge at a time can make. The merges of different
// nodes in one sweep run together, and join and leave the topology with one
// CAS of the collection record.
#[tokio::test]
async fn one_sweep_merges_different_pairs_together() {
    let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
    let s = store_with_backend(backend.clone() as Arc<dyn Backend>);
    seed_leaf_chain(&s, 6).await;
    let bg = Arc::new(Background::new());
    let sp = restructurer(&s, &bg, mergeable());
    push_underfull_merges(&sp, &["L0", "L2", "L4"]);

    let record_writes = record_writes_of(&backend, sp.run_once()).await;

    assert_eq!(sp.stats_and_reset().merges, 3);
    assert_eq!(record_writes, 2, "one CAS to join and one to leave");
    for (left, right, merged_keys) in [
        ("L0", "L1", [b"a", b"b"]),
        ("L2", "L3", [b"c", b"d"]),
        ("L4", "L5", [b"e", b"f"]),
    ] {
        assert!(current_node(&s, left).await.is_drained());
        let right = current_node(&s, right).await;
        assert_eq!(keys(&right), merged_keys);
        assert!(right.locks().merge_reservation().is_none());
    }
    assert_eq!(
        root_children(&s).await,
        test_index(&[(b"", "L1"), (b"c", "L3"), (b"e", "L5")])
    );
    assert_no_intents(&s).await;
}

// The target of one merge is the source of the next. If both ran at the same
// time, the reservation of one would stop the other, so they run one after
// the other in the same sweep.
#[tokio::test]
async fn merges_that_share_a_node_run_one_after_the_other_in_one_sweep() {
    let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
    let s = store_with_backend(backend.clone() as Arc<dyn Backend>);
    seed_leaf_chain(&s, 3).await;
    let bg = Arc::new(Background::new());
    let sp = restructurer(&s, &bg, mergeable());
    push_underfull_merges(&sp, &["L0", "L1"]);

    let record_writes = record_writes_of(&backend, sp.run_once()).await;

    let stats = sp.stats_and_reset();
    assert_eq!((stats.merges, stats.deferred), (2, 0));
    assert_eq!(record_writes, 4, "one join and one leave for each round");
    assert!(current_node(&s, "L0").await.is_drained());
    assert!(current_node(&s, "L1").await.is_drained());
    let merged = current_node(&s, "L2").await;
    assert_eq!(merged.low_key(), b"");
    assert_eq!(keys(&merged), [b"a", b"b", b"c"]);
    assert_eq!(root_children(&s).await, test_index(&[(b"", "L2")]));
    assert_no_intents(&s).await;
}

// A leaf can need a merge for two reasons at the same time. After the first
// merge drains it, the second one must stop before it writes an intent, and
// not defer.
#[tokio::test(start_paused = true)]
async fn a_second_reason_to_merge_a_drained_leaf_stops_in_the_same_sweep() {
    let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
    let s = store_with_backend(backend.clone() as Arc<dyn Backend>);
    seed_leaf_chain(&s, 2).await;
    let bg = Arc::new(Background::new());
    let sp = restructurer(&s, &bg, mergeable());
    push_underfull_merges(&sp, &["L0"]);
    sp.candidates.push_request(RuleRequest {
        change: ChangeRequest::Merge(LeafId::new(node_path("L0"))),
        rate: Duration::from_secs(1),
    });

    let record_writes = record_writes_of(&backend, sp.run_once()).await;

    let stats = sp.stats_and_reset();
    assert_eq!((stats.merges, stats.deferred), (1, 0));
    assert_eq!(record_writes, 2, "only the first merge joins the topology");
    assert!(current_node(&s, "L0").await.is_drained());
    assert_eq!(keys(&current_node(&s, "L1").await), [b"a", b"b"]);
    assert_no_intents(&s).await;
}

fn push_demand(sp: &Restructurer, change: ChangeRequest, millis: u64) {
    sp.candidates.push_request(RuleRequest {
        change,
        rate: Duration::from_millis(millis),
    });
}

// A split adds a separator to the parent and a merge removes one. When they
// land in one batch, one reconciliation of the parent must make both
// changes.
#[tokio::test(start_paused = true)]
async fn one_batch_splits_and_merges_under_one_parent() {
    let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
    let s = store_with_backend(backend.clone() as Arc<dyn Backend>);
    for (name, keys, low, high, right) in [
        ("L0", &[b"a"][..], &b""[..], Some(&b"b"[..]), Some("L1")),
        ("L1", &[b"b"][..], b"b", Some(b"m"), Some("L2")),
        ("L2", &[b"m", b"n", b"o", b"p"][..], b"m", None, None),
    ] {
        let keys: Vec<&[u8]> = keys.iter().map(|key| key.as_slice()).collect();
        let node = leaf_node(&keys, high, right).with_low_key(low.to_vec());
        assert!(s.store_node(COLL, name, &node, None).await.unwrap());
    }
    s.create_root(
        COLL,
        &Node::index(test_index(&[(b"", "L0"), (b"b", "L1"), (b"m", "L2")])),
    )
    .await
    .unwrap();
    let bg = Arc::new(Background::new());
    let sp = rule_restructurer(&s, &bg);
    push_demand(&sp, ChangeRequest::Merge(LeafId::new(node_path("L0"))), 500);
    push_demand(&sp, ChangeRequest::Split(LeafId::new(node_path("L2"))), 100);

    let record_writes = record_writes_of(&backend, sp.run_once()).await;

    let stats = sp.stats_and_reset();
    assert_eq!((stats.splits, stats.merges, stats.deferred), (1, 1, 0));
    assert_eq!(record_writes, 2, "one batch");
    let root = root_children(&s).await;
    let children: Vec<_> = root.children().collect();
    assert_eq!(children.len(), 3, "{children:?}");
    assert_eq!(children[0], (&b""[..], test_node_id("L1")));
    assert_eq!(children[1], (&b"m"[..], test_node_id("L2")));
    assert_eq!(children[2].0, b"o");
    assert_eq!(keys(&current_node(&s, "L1").await), [b"a", b"b"]);
    assert_eq!(keys(&current_node(&s, "L2").await), [b"m", b"n"]);
    assert_no_intents(&s).await;
}

// A merge into a leaf that also needs a split waits for the split, even when
// the merge has the higher rate. If the merge ran first, the paid rate of the
// merged leaf would stop the split.
#[tokio::test(start_paused = true)]
async fn a_merge_into_a_leaf_that_needs_a_split_waits_for_the_split() {
    let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
    let s = store_with_backend(backend.clone() as Arc<dyn Backend>);
    seed_two_leaves(&s, &[b"a"], &[b"m", b"n", b"o"]).await;
    let bg = Arc::new(Background::new());
    let sp = rule_restructurer(&s, &bg);
    push_demand(&sp, ChangeRequest::Merge(LeafId::new(node_path("L0"))), 500);
    push_demand(&sp, ChangeRequest::Split(LeafId::new(node_path("L1"))), 100);

    let record_writes = record_writes_of(&backend, sp.run_once()).await;

    let stats = sp.stats_and_reset();
    assert_eq!((stats.splits, stats.merges, stats.deferred), (1, 1, 0));
    assert_eq!(record_writes, 4, "one batch for the split, then the merge");
    assert!(current_node(&s, "L0").await.is_drained());
    let merged = current_node(&s, "L1").await;
    assert_eq!(merged.low_key(), b"");
    assert_eq!(keys(&merged)[0], b"a");
    assert!(keys(&merged).len() < 4, "the merge went into one half");
    assert_no_intents(&s).await;
}

// ADR-084: a recent transaction wrote "b" and "m" together, over the
// boundary of L0 and L1, so the two leaves merge.
#[tokio::test]
async fn leaves_merge_for_keys_committed_together_over_their_boundary() {
    let s = store();
    let writer = recent_writer();
    let left = Node::leaf(LeafBody::from_entries([
        live(b"a"),
        written_by(b"b", &writer),
    ]));
    let right = Node::leaf(LeafBody::from_entries([
        written_by(b"m", &writer),
        live(b"n"),
    ]));
    seed_leaves(&s, left, right).await;
    let bg = Arc::new(Background::new());
    let sp = restructurer(&s, &bg, mergeable());

    merge_now(&sp, "L0", MergeReason::WriteSpan).await.unwrap();

    assert_merged(&s).await;
}

// ADR-084: without keys committed together over the boundary, a merge for
// write spans does not go ahead.
#[tokio::test]
async fn leaves_do_not_merge_without_keys_committed_together_over_their_boundary() {
    let s = store();
    seed_two_leaves(&s, &[b"a", b"b"], &[b"m", b"n"]).await;
    let bg = Arc::new(Background::new());
    let sp = restructurer(&s, &bg, mergeable());

    merge_now(&sp, "L0", MergeReason::WriteSpan).await.unwrap();

    assert_not_merged(&s, &[b"a", b"b"], &[b"m", b"n"]).await;
}

// ADR-084: a merge for write spans makes a leaf of at most twice the soft cap
// of entries, here 16.
#[tokio::test]
async fn leaves_do_not_merge_for_write_spans_over_twice_the_soft_cap() {
    let s = store();
    let writer = recent_writer();
    let keys = |range: std::ops::RangeInclusive<u8>, written: u8| {
        LeafBody::from_entries(range.map(|key| {
            if key == written {
                written_by(&[key], &writer)
            } else {
                live(&[key])
            }
        }))
    };
    let left = Node::leaf(keys(b'a'..=b'i', b'i'));
    let right = Node::leaf(keys(b'm'..=b'u', b'm'));
    seed_leaves(&s, left, right).await;
    let bg = Arc::new(Background::new());
    let sp = restructurer(&s, &bg, mergeable());

    merge_now(&sp, "L0", MergeReason::WriteSpan).await.unwrap();

    assert!(!current_node(&s, "L0").await.is_drained());
    assert_no_intents(&s).await;
}
