use super::*;

#[test]
fn reclamation_removes_only_holder_free_tombstones() {
    let reclaimed_writer = TxId::with_priority(1, b"reclaimed");
    let retained_writer = TxId::with_priority(2, b"retained");
    let holder = TxId::with_priority(3, b"holder");
    let mut retained = tombstone(b"locked", retained_writer);
    retained.acquire_read_lock(holder);
    let mut node = Node::leaf(LeafBody::from_entries([
        live(b"live"),
        tombstone(b"reclaimed", reclaimed_writer),
        retained,
    ]));
    let mut locks = node.locks().clone();
    locks.advance_membership_generation();
    node.set_locks(locks);

    assert_eq!(
        reclaim_holder_free_tombstones(&mut node),
        vec![reclaimed_writer]
    );
    let leaf = node.as_leaf().unwrap();
    assert!(leaf.lookup(b"reclaimed").is_none());
    assert!(leaf.lookup(b"live").unwrap().exists());
    assert_eq!(
        leaf.lookup(b"locked").unwrap().current.writer(),
        Some(&retained_writer)
    );
    assert_eq!(node.membership_generation(), 1);
}

#[tokio::test]
async fn root_reclamation_can_avoid_an_actionable_split() {
    let s = store();
    let first = TxId::with_priority(2, b"first");
    let second = TxId::with_priority(3, b"second");
    let mut root = Node::leaf(LeafBody::from_entries([
        live(b"a"),
        tombstone(b"b", first),
        tombstone(b"c", second),
    ]));
    let mut locks = root.locks().clone();
    locks.advance_membership_generation();
    locks.advance_membership_generation();
    root.set_locks(locks);
    s.create_root(COLL, &root).await.unwrap();
    let bg = Arc::new(Background::new());
    let candidates = MaintenanceCandidates::with_policy(tiny());
    candidates.observe_leaf(&root_path(), root.as_leaf().unwrap());
    let gc_hints = GcHints::default();
    let sp = restructurer_with_candidates_and_hints(&s, &bg, candidates, gc_hints.clone());

    sp.run_once().await;

    let (root, _) = s
        .load_root(COLL, Requirement::after(s.timeline.currentness_barrier()))
        .await
        .unwrap();
    let leaf = root.as_leaf().expect("compaction avoided height growth");
    assert_eq!(leaf.len(), 1);
    assert!(leaf.lookup(b"a").unwrap().exists());
    // Compaction keeps the generation; only the gate installation advanced it.
    assert_eq!(root.membership_generation(), 3);
    assert!(
        s.list_nodes(COLL, Requirement::after(s.timeline.currentness_barrier()))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        sp.stats_and_reset(),
        RestructurerStats {
            candidates: 1,
            tombstones_reclaimed: 2,
            splits_avoided: 1,
            ..RestructurerStats::default()
        }
    );
    assert_eq!(gc_hints.pending(), vec![first, second]);
}

#[tokio::test]
async fn root_leaf_gate_acquisition_uses_one_coordinator_round() {
    let recorder = Arc::new(RecordingBackend::new(Arc::new(MemoryBackend::new())));
    let operations = recorder.log();
    let seed = store_with_backend(recorder.clone());
    seed.create_root(COLL, &leaf_node(&[b"a"], None, None))
        .await
        .unwrap();

    // Use a new cache so the operation count includes the root load that
    // classifies the node before structural-gate acquisition.
    let s = store_with_backend(recorder);
    let bg = Arc::new(Background::new());
    let sp = restructurer(&s, &bg, tiny());
    operations.lock().unwrap().clear();

    let worker = TxId::with_priority(1, b"root-gate");
    let (node, observation) = sp
        .changes
        .structural_nodes
        .acquire_structural_gate(&collection(), None, &worker, GateAcquisition::WoundWait)
        .await
        .unwrap()
        .expect("the root leaf can acquire its structural gate");

    assert!(node.structural_gate().contains(&worker));
    assert_eq!(observation.path(), &root_path());
    assert_eq!(
        sp.changes.structural_nodes.coord.stats_and_reset(),
        crate::leaf_coord::LeafCoordinatorStats {
            submissions: 1,
            rounds: 1,
            cas_retries: 0,
        }
    );
    let root_path = root_path().to_string();
    let root_operations: Vec<_> = operations
        .lock()
        .unwrap()
        .iter()
        .filter(|operation| operation.path == root_path)
        .map(|operation| operation.op)
        .collect();
    assert_eq!(root_operations, ["read", "write_if"]);

    sp.changes
        .structural_nodes
        .release_structural_gate(&collection(), None, &worker)
        .await
        .unwrap();
    let (root, _) = s
        .load_root(COLL, Requirement::after(s.timeline.currentness_barrier()))
        .await
        .unwrap();
    assert!(root.structural_gate().holders().is_empty());
}

#[tokio::test]
async fn failed_compaction_does_not_publish_reclamation_outcomes() {
    let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
    let s = store_with_backend(backend.clone());
    let root = Node::leaf(LeafBody::from_entries([
        live(b"a"),
        tombstone(b"b", TxId::with_priority(2, b"deleted")),
        tombstone(b"c", TxId::with_priority(3, b"other")),
    ]));
    s.create_root(COLL, &root).await.unwrap();
    let failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    backend.set_before({
        let failed = failed.clone();
        move |operation| {
            let reject = matches!(
                operation,
                BackendOp::WriteIf { path, value, .. }
                    if path.ends_with("/_r")
                        && Node::decode(value).is_ok_and(|node| {
                            node.as_leaf().is_some_and(|leaf| leaf.len() == 1)
                                && node.structural_gate().holders().is_empty()
                        })
            ) && !failed.swap(true, std::sync::atomic::Ordering::SeqCst);
            let result = if reject {
                Err(glassdb_backend::BackendError::Precondition)
            } else {
                Ok(())
            };
            let future: HookFuture = Box::pin(async move { result });
            future
        }
    });
    let bg = Arc::new(Background::new());
    let candidates = MaintenanceCandidates::with_policy(tiny());
    candidates.observe_leaf(&root_path(), root.as_leaf().unwrap());
    let gc_hints = GcHints::default();
    let sp = restructurer_with_candidates_and_hints(&s, &bg, candidates, gc_hints.clone());

    sp.run_once().await;

    assert!(failed.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(
        sp.stats_and_reset(),
        RestructurerStats {
            candidates: 1,
            deferred: 1,
            ..RestructurerStats::default()
        }
    );
    assert!(gc_hints.pending().is_empty());
    let (root, _) = s
        .load_root(COLL, Requirement::after(s.timeline.currentness_barrier()))
        .await
        .unwrap();
    assert_eq!(root.as_leaf().unwrap().len(), 3);
}

#[tokio::test]
async fn nonroot_split_partitions_the_compacted_leaf() {
    let s = store();
    let writer = TxId::with_priority(2, b"deleted");
    let mut source = Node::leaf(LeafBody::from_entries([
        live(b"a"),
        live(b"b"),
        live(b"c"),
        tombstone(b"d", writer),
    ]));
    let mut locks = source.locks().clone();
    locks.advance_membership_generation();
    locks.advance_membership_generation();
    source.set_locks(locks);
    s.store_node(COLL, "L", &source, None).await.unwrap();
    s.create_root(
        COLL,
        &Node::index(IndexNode::from_children([(Vec::new(), test_node_id("L"))])),
    )
    .await
    .unwrap();
    let bg = Arc::new(Background::new());
    let candidates = MaintenanceCandidates::with_policy(tiny());
    candidates.observe_leaf(&node_path("L"), source.as_leaf().unwrap());
    let gc_hints = GcHints::default();
    let sp = restructurer_with_candidates_and_hints(&s, &bg, candidates, gc_hints.clone());

    sp.run_once().await;

    let leaves = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN)
        .leaves(
            &collection(),
            Requirement::after(s.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert_eq!(leaves.len(), 2);
    // The split keeps the generation; only the gate installation advanced it.
    assert!(leaves.iter().all(|leaf| {
        let node = leaf.node().unwrap();
        node.membership_generation() == 3
            && node
                .as_leaf()
                .unwrap()
                .entries()
                .all(|entry| !entry.current.is_tombstone())
    }));
    assert_eq!(
        sp.stats_and_reset(),
        RestructurerStats {
            candidates: 1,
            splits: 1,
            tombstones_reclaimed: 1,
            ..RestructurerStats::default()
        }
    );
    assert_eq!(gc_hints.pending(), vec![writer]);
}

// ADR-051: an inline value may be a key's only copy, so a split has to move
// it to the new leaf verbatim.
#[tokio::test]
async fn a_split_carries_inline_values_to_the_new_leaf() {
    let s = store();
    let keys: [&[u8]; 4] = [b"a", b"b", b"c", b"d"];
    let inlined = |key: &[u8]| {
        LeafEntry::new(key).with_current(CurrentState::Inline {
            writer: tx_id(&[1]),
            value: Arc::from(key),
        })
    };
    s.create_root(COLL, &Node::leaf(LeafBody::from_entries(keys.map(inlined))))
        .await
        .unwrap();
    let bg = Arc::new(Background::new());

    split_path(
        &restructurer(&s, &bg, tiny()),
        &root_path(),
        &SplitReason::SoftCap,
    )
    .await
    .unwrap();

    let router = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN);
    assert_eq!(
        router
            .leaves(
                &collection(),
                Requirement::after(s.timeline.currentness_barrier())
            )
            .await
            .unwrap()
            .len(),
        2,
        "one leaf became two"
    );
    for key in keys {
        let loc = router
            .route_key(
                &collection(),
                key,
                Requirement::after(s.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        let entry = loc.node().unwrap().as_leaf().unwrap().lookup(key).cloned();
        assert_eq!(entry, Some(inlined(key)), "key {key:?} lost its value");
    }
}

// A small collection whose single leaf lives in the root `_r`; when it grows
// past the cap the root splits in place into a two-child index, raising the
// height, and every key stays reachable in key order.
#[tokio::test]
async fn root_leaf_splits_in_place_into_an_index() {
    let s = store();
    let root = Node::leaf(LeafBody::from_entries(
        [b"a".as_slice(), b"b", b"c", b"d"].iter().map(|k| live(k)),
    ));
    s.create_root(COLL, &root).await.unwrap();
    let bg = Arc::new(Background::new());

    split_path(
        &restructurer(&s, &bg, tiny()),
        &root_path(),
        &SplitReason::SoftCap,
    )
    .await
    .unwrap();

    // The root is now an index (height grew from 1 to 2).
    let (node, _) = s
        .load_root_node(COLL, Requirement::after(s.timeline.currentness_barrier()))
        .await
        .unwrap()
        .unwrap();
    assert!(node.as_index().is_some(), "root became an index");

    let router = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN);
    let leaves = router
        .leaves(
            &collection(),
            Requirement::after(s.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert_eq!(leaves.len(), 2, "one leaf became two");
    // The lower leaf is bounded by the split key and links to the upper one.
    assert!(leaves[0].node().unwrap().right_sibling().is_some());
    assert_eq!(
        leaves[0].node().unwrap().high_key(),
        Some(
            leaves[1]
                .node()
                .unwrap()
                .as_leaf()
                .unwrap()
                .entries()
                .next()
                .unwrap()
                .key
                .as_slice()
        ),
    );
    // Every key remains reachable by descent, in order.
    for k in [b"a".as_slice(), b"b", b"c", b"d"] {
        let loc = router
            .route_key(
                &collection(),
                k,
                Requirement::after(s.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        assert!(
            loc.node().unwrap().as_leaf().unwrap().exists(k),
            "key {k:?} lost"
        );
    }
    assert!(
        s.discover_structural_intents("db", Requirement::after(s.timeline.currentness_barrier()))
            .await
            .unwrap()
            .is_empty()
    );
    let transaction_prefix = format!("{}/_t/", db_prefix("db"));
    assert_eq!(
        s.objects
            .list(
                &transaction_prefix,
                None,
                glassdb_backend::ListLimit::new(2).unwrap(),
            )
            .await
            .unwrap()
            .objects
            .len(),
        1,
        "the topology participant remains recoverable until GC"
    );
}

// A standalone leaf over the cap half-splits: the upper half moves to a fresh
// sibling, the source shrinks and links to it, and the parent index learns
// the separator so later descents skip the right-link hop.
#[tokio::test]
async fn nonroot_leaf_half_splits_and_parent_learns_the_separator() {
    let s = store();
    s.store_node(
        COLL,
        "L",
        &leaf_node(&[b"a", b"b", b"c", b"d"], None, None),
        None,
    )
    .await
    .unwrap();
    let root = Node::index(IndexNode::from_children([(Vec::new(), test_node_id("L"))]));
    s.create_root(COLL, &root).await.unwrap();
    let bg = Arc::new(Background::new());

    split_path(
        &restructurer(&s, &bg, tiny()),
        &node_path("L"),
        &SplitReason::SoftCap,
    )
    .await
    .unwrap();

    let router = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN);
    let leaves = router
        .leaves(
            &collection(),
            Requirement::after(s.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert_eq!(leaves.len(), 2, "leaf L split into two");
    // The parent index now routes the moved keys directly to the sibling, not
    // via a right-link walk: its child for the split key differs from L.
    let (root_node, _) = s
        .load_root_node(COLL, Requirement::after(s.timeline.currentness_barrier()))
        .await
        .unwrap()
        .unwrap();
    let index = root_node.as_index().unwrap();
    assert_eq!(index.len(), 2, "parent gained the separator");
    for k in [b"a".as_slice(), b"b", b"c", b"d"] {
        let loc = router
            .route_key(
                &collection(),
                k,
                Requirement::after(s.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        assert!(
            loc.node().unwrap().as_leaf().unwrap().exists(k),
            "key {k:?} lost"
        );
    }
    assert!(
        s.discover_structural_intents("db", Requirement::after(s.timeline.currentness_barrier()))
            .await
            .unwrap()
            .is_empty()
    );
}

// A leaf separator is published before its newly-over-cap parent is split.
// The parent split must therefore carry that separator into the next level.
#[tokio::test]
async fn parent_reconciliation_cascades_after_the_parent_insert() {
    let s = store();
    s.store_node(
        COLL,
        "L0",
        &leaf_node(&[b"a"], Some(b"m"), Some("L1")),
        None,
    )
    .await
    .unwrap();
    s.store_node(
        COLL,
        "L1",
        &leaf_node(&[b"m", b"n", b"o"], None, None),
        None,
    )
    .await
    .unwrap();
    s.create_root(
        COLL,
        &Node::index(IndexNode::from_children([
            (Vec::new(), test_node_id("L0")),
            (b"m".to_vec(), test_node_id("L1")),
        ])),
    )
    .await
    .unwrap();
    let bg = Arc::new(Background::new());

    split_path(
        &restructurer(&s, &bg, tiny()),
        &node_path("L1"),
        &SplitReason::SoftCap,
    )
    .await
    .unwrap();

    let (root, _) = s
        .load_root_node(COLL, Requirement::after(s.timeline.currentness_barrier()))
        .await
        .unwrap()
        .unwrap();
    let child_ids: Vec<_> = root
        .as_index()
        .unwrap()
        .children()
        .map(|(_, id)| id)
        .collect();
    assert_eq!(child_ids.len(), 2, "the root grew by one level");
    for id in child_ids {
        let (child, _) = s
            .nodes
            .load_node(
                &collection_at(COLL),
                &id,
                Requirement::after(s.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        assert!(child.as_index().is_some(), "root children are indexes");
    }

    let router = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN);
    assert_eq!(
        router
            .leaves(
                &collection(),
                Requirement::after(s.timeline.currentness_barrier())
            )
            .await
            .unwrap()
            .len(),
        3,
        "the published sibling remains reachable after the parent split"
    );
    for key in [b"a".as_slice(), b"m", b"n", b"o"] {
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
}

// An index root that overflows its fan-out splits in place: two index children
// are created and the root is rewritten over them, so all original children
// remain reachable one level deeper.
#[tokio::test]
async fn root_index_splits_in_place_growing_height() {
    let s = store();
    // Three leaves under a three-child index root (over a two-child cap).
    for (tok, keys, high, right) in [
        (
            "L0",
            vec![b"a".as_slice()],
            Some(b"m".as_slice()),
            Some("L1"),
        ),
        (
            "L1",
            vec![b"m".as_slice()],
            Some(b"t".as_slice()),
            Some("L2"),
        ),
        ("L2", vec![b"t".as_slice()], None, None),
    ] {
        s.store_node(COLL, tok, &leaf_node(&keys, high, right), None)
            .await
            .unwrap();
    }
    let root = Node::index(IndexNode::from_children([
        (Vec::new(), test_node_id("L0")),
        (b"m".to_vec(), test_node_id("L1")),
        (b"t".to_vec(), test_node_id("L2")),
    ]));
    s.create_root(COLL, &root).await.unwrap();
    let bg = Arc::new(Background::new());

    split_path(
        &restructurer(&s, &bg, tiny()),
        &root_path(),
        &SplitReason::SoftCap,
    )
    .await
    .unwrap();

    let (node, _) = s
        .load_root_node(COLL, Requirement::after(s.timeline.currentness_barrier()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        node.as_index().unwrap().len(),
        2,
        "root now has two index children"
    );
    // Every original leaf is still reached in order (now via one more hop).
    let router = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN);
    for k in [b"a".as_slice(), b"m", b"t"] {
        let loc = router
            .route_key(
                &collection(),
                k,
                Requirement::after(s.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        assert!(
            loc.node().unwrap().as_leaf().unwrap().exists(k),
            "key {k:?} lost"
        );
    }
}

// The parent of an index node is one level above it, not the parent of
// the leaves below the split key.
#[tokio::test]
async fn nonroot_index_split_reconciles_the_index_above_it() {
    let s = store();
    for (tok, keys, high, right) in [
        (
            "L0",
            vec![b"a".as_slice()],
            Some(b"m".as_slice()),
            Some("L1"),
        ),
        (
            "L1",
            vec![b"m".as_slice()],
            Some(b"t".as_slice()),
            Some("L2"),
        ),
        ("L2", vec![b"t".as_slice()], None, None),
    ] {
        s.store_node(COLL, tok, &leaf_node(&keys, high, right), None)
            .await
            .unwrap();
    }
    let index = Node::index(test_index(&[(b"", "L0"), (b"m", "L1"), (b"t", "L2")]));
    s.store_node(COLL, "I0", &index, None).await.unwrap();
    s.create_root(COLL, &Node::index(test_index(&[(b"", "I0")])))
        .await
        .unwrap();
    let bg = Arc::new(Background::new());

    split_path(
        &restructurer(&s, &bg, tiny()),
        &node_path("I0"),
        &SplitReason::SoftCap,
    )
    .await
    .unwrap();

    let (root, _) = s
        .load_root(COLL, Requirement::after(s.timeline.currentness_barrier()))
        .await
        .unwrap();
    let root = root.as_index().unwrap();
    assert_eq!(
        root.children().map(|(key, _)| key).collect::<Vec<_>>(),
        [b"".as_slice(), b"m"]
    );
    let (sibling, _) = s
        .nodes
        .load_node(
            &collection_at(COLL),
            &root.child_for(b"m").unwrap(),
            Requirement::after(s.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert!(sibling.as_index().is_some(), "the root names the new index");
}

#[tokio::test]
async fn separator_capacity_splits_parent_during_reconciliation_and_recovery() {
    for recover in [false, true] {
        let s = store();
        let children = [
            (b"a".as_slice(), "L0"),
            (b"b", "L1"),
            (b"c", "L2"),
            (b"d", "L3"),
            (b"e", "L4"),
            (b"f", "L5"),
            (b"g", "L6"),
            (b"h", "L7"),
        ];
        for (i, (key, name)) in children.iter().enumerate() {
            let next = children.get(i + 1);
            s.store_node(
                COLL,
                name,
                &leaf_node(
                    &[key],
                    next.map(|(key, _)| *key),
                    next.map(|(_, name)| *name),
                ),
                None,
            )
            .await
            .unwrap();
        }
        let mut index =
            IndexNode::from_children(children[..7].iter().enumerate().map(|(i, (key, name))| {
                (
                    if i == 0 { Vec::new() } else { key.to_vec() },
                    test_node_id(name),
                )
            }));
        let root = Node::index(index.clone());
        s.create_root(COLL, &root).await.unwrap();
        index.insert_child(b"h".to_vec(), test_node_id("L7"));
        let required_bytes = Node::index(index).content_encoded_len();
        let policy = NodeSizePolicy::builder()
            .node_max_bytes(required_bytes + 128 - 1)
            .split_headroom_bytes(128)
            .node_soft_max_bytes(usize::MAX)
            .index_max_children(usize::MAX)
            .build()
            .unwrap();
        assert!(policy.key_fits(b"h"));
        assert!(!root.over_soft_cap(&policy));
        let bg = Arc::new(Background::new());
        let sp = restructurer(&s, &bg, policy);

        if recover {
            // This is the durable state after a source shrink and before its
            // parent reconciliation. Recovery must retain the capacity cause.
            let participant = TxId::with_priority(1, b"interrupted-split");
            sp.changes
                .topology
                .begin(&collection(), &participant)
                .await
                .unwrap();
            sp.changes
                .topology
                .join(&collection(), &participant)
                .await
                .unwrap();
            s.write_structural_intent(
                "L7",
                &StructuralIntent {
                    collection: collection(),
                    source_node_id: Some(test_node_id("L6")),
                    source_revision: superseded_source_revision(),
                    change: StructuralChange::Split {
                        created_node_ids: vec![test_node_id("L7")],
                        split_key: b"h".to_vec(),
                    },
                    participant_id: participant,
                    phase: StructuralIntentPhase::Ready,
                },
            )
            .await
            .unwrap();
            sp.mon.abort_owned_tx(&participant).await.unwrap();
            assert!(sp.recover_structural_intents().await.unwrap());
            assert!(
                s.discover_structural_intents(
                    "db",
                    Requirement::after(s.timeline.currentness_barrier())
                )
                .await
                .unwrap()
                .is_empty()
            );
        } else {
            sp.changes
                .reconcile_parent(&collection(), b"h", &test_node_id("L7"), None)
                .await
                .unwrap();
        }

        let (root, _) = s
            .load_root(COLL, Requirement::after(s.timeline.currentness_barrier()))
            .await
            .unwrap();
        let root_index = root.as_index().unwrap();
        assert_eq!(root_index.len(), 2);
        for (_, id) in root_index.children() {
            let (child, _) = s
                .nodes
                .load_node(
                    &collection_at(COLL),
                    &id,
                    Requirement::after(s.timeline.currentness_barrier()),
                )
                .await
                .unwrap();
            assert!(
                child.as_index().is_some(),
                "the parent split must grow the tree height"
            );
        }
        let router = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN);
        for (key, _) in children {
            let located = router
                .route_key(
                    &collection(),
                    key,
                    Requirement::after(s.timeline.currentness_barrier()),
                )
                .await
                .unwrap();
            assert!(located.node().unwrap().as_leaf().unwrap().exists(key));
        }
    }
}

// Re-running a split on a node already back under the cap is a no-op: the
// restructurer reloads, sees it is not over the cap, and leaves the tree alone.
#[tokio::test]
async fn re_split_of_a_settled_node_is_a_noop() {
    let s = store();
    let root = Node::leaf(LeafBody::from_entries(
        [b"a".as_slice(), b"b", b"c", b"d"].iter().map(|k| live(k)),
    ));
    s.create_root(COLL, &root).await.unwrap();
    let bg = Arc::new(Background::new());
    let sp = restructurer(&s, &bg, tiny());

    split_path(&sp, &root_path(), &SplitReason::SoftCap)
        .await
        .unwrap();
    let after_first = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN)
        .leaves(
            &collection(),
            Requirement::after(s.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    // Re-run: each resulting leaf holds two keys, which is at (not over) the
    // cap, so nothing changes.
    for leaf in &after_first {
        split_path(&sp, &leaf.path, &SplitReason::SoftCap)
            .await
            .unwrap();
    }
    split_path(&sp, &root_path(), &SplitReason::SoftCap)
        .await
        .unwrap();

    let after_second = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN)
        .leaves(
            &collection(),
            Requirement::after(s.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert_eq!(
        after_first.len(),
        after_second.len(),
        "a settled tree does not keep splitting"
    );
    assert!(
        s.discover_structural_intents("db", Requirement::after(s.timeline.currentness_barrier()))
            .await
            .unwrap()
            .is_empty(),
        "a no-op split cleans its Preparing intent"
    );
    let (record, _) = s
        .records
        .load_record(
            &collection(),
            Requirement::after(s.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert_eq!(record.topology_participants().count(), 0);
}

// The candidate feed drives run_once end to end: a leaf pushed over the cap is
// drained and split; the byte/entry gate keeps under-cap leaves out.
#[tokio::test]
async fn feed_drives_run_once() {
    let s = store();
    let root = Node::leaf(LeafBody::from_entries(
        [b"a".as_slice(), b"b", b"c", b"d"].iter().map(|k| live(k)),
    ));
    s.create_root(COLL, &root).await.unwrap();
    let bg = Arc::new(Background::new());

    let candidates = MaintenanceCandidates::with_policy(tiny());
    // Under the cap: not enqueued.
    candidates.observe_leaf(
        &root_path(),
        &LeafBody::from_entries([live(b"a"), live(b"b")]),
    );
    assert!(
        candidates.drain().is_empty(),
        "at-cap leaf is not a candidate"
    );
    // Over the cap: enqueued and split by a sweep.
    candidates.observe_leaf(
        &root_path(),
        &LeafBody::from_entries([live(b"a"), live(b"b"), live(b"c"), live(b"d")]),
    );
    let sp = restructurer_with_candidates(&s, &bg, candidates);
    sp.run_once().await;

    let leaves = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN)
        .leaves(
            &collection(),
            Requirement::after(s.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert_eq!(leaves.len(), 2, "the fed candidate was split");
    assert_eq!(
        sp.stats_and_reset(),
        RestructurerStats {
            candidates: 1,
            splits: 1,
            deferred: 0,
            ..RestructurerStats::default()
        }
    );
    assert_eq!(sp.stats_and_reset(), RestructurerStats::default());
}

// One large mutation can overshoot the soft cap by enough that halving the
// leaf once leaves both outputs oversized. The outputs must feed the next
// sweep themselves; no later mutation should be needed to finish the tree.
#[tokio::test]
async fn one_hint_cascades_until_every_leaf_is_under_cap() {
    let s = store();
    let keys: [&[u8]; 9] = [b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h", b"i"];
    let root = Node::leaf(LeafBody::from_entries(keys.iter().map(|key| live(key))));
    s.create_root(COLL, &root).await.unwrap();
    let bg = Arc::new(Background::new());
    let policy = NodeSizePolicy::builder()
        .leaf_max_entries(2)
        .node_soft_max_bytes(1 << 20)
        .index_max_children(100)
        .build()
        .unwrap();
    let candidates = MaintenanceCandidates::with_policy(policy);
    candidates.observe_leaf(&root_path(), root.as_leaf().unwrap());
    let sp = restructurer_with_candidates(&s, &bg, candidates);

    // 9 -> 4+5 -> 2+2+2+3 -> 2+2+2+1+2.
    for _ in 0..3 {
        sp.run_once().await;
    }

    let router = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN);
    let leaves = router
        .leaves(
            &collection(),
            Requirement::after(s.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert_eq!(leaves.len(), 5);
    assert!(leaves.iter().all(|leaf| {
        leaf.node().unwrap().as_leaf().unwrap().len() <= policy.leaf_max_entries()
    }));
    for key in keys {
        let located = router
            .route_key(
                &collection(),
                key,
                Requirement::after(s.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        assert!(located.node().unwrap().as_leaf().unwrap().exists(key));
    }
}

#[tokio::test]
async fn repeated_inline_pressure_performs_one_rerouted_median_split_each() {
    let s = store();
    let root = Node::leaf(LeafBody::from_entries([
        live(b"a"),
        live(b"b"),
        live(b"c"),
        live(b"d"),
        live(b"e"),
        inline_live(b"f", b"12345678"),
        live(b"g"),
        live(b"h"),
    ]));
    s.create_root(COLL, &root).await.unwrap();
    let bg = Arc::new(Background::new());
    let candidates =
        MaintenanceCandidates::with_policies(NodeSizePolicy::default(), pressure_inline());
    let sp = restructurer_with_candidates(&s, &bg, candidates.clone());
    let root_path = root_path();

    candidates
        .hint_sink()
        .observe_inline_pressure(&root_path, b"h", 8);
    sp.run_once().await;

    let router = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN);
    assert_eq!(
        router
            .leaves(
                &collection(),
                Requirement::after(s.timeline.currentness_barrier())
            )
            .await
            .unwrap()
            .len(),
        2,
        "one request performs only the root's median split"
    );
    assert_eq!(
        sp.stats_and_reset(),
        RestructurerStats {
            candidates: 1,
            splits: 1,
            inline_pressure: InlinePressureStats {
                candidates: 1,
                completed: 1,
                ..InlinePressureStats::default()
            },
            ..RestructurerStats::default()
        }
    );

    let target = router
        .route_key(
            &collection(),
            b"h",
            Requirement::after(s.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    let reason = SplitReason::InlinePressure {
        key: b"h".to_vec(),
        value_len: 8,
    };
    assert!(
        matches!(
            target.node().map(|node| sp.splitter.need(node, &reason)),
            Some(SplitNeed::Split)
        ),
        "the first median left real pressure for a future observation"
    );

    // The old root path is deliberately stale now. Key-directed
    // revalidation must find and split the currently routed child.
    candidates
        .hint_sink()
        .observe_inline_pressure(&root_path, b"h", 8);
    sp.run_once().await;

    assert_eq!(
        router
            .leaves(
                &collection(),
                Requirement::after(s.timeline.currentness_barrier())
            )
            .await
            .unwrap()
            .len(),
        3,
        "a second real observation drives exactly one more split"
    );
    assert_eq!(
        sp.stats_and_reset(),
        RestructurerStats {
            candidates: 1,
            splits: 1,
            inline_pressure: InlinePressureStats {
                candidates: 1,
                completed: 1,
                ..InlinePressureStats::default()
            },
            ..RestructurerStats::default()
        }
    );
    let target = router
        .route_key(
            &collection(),
            b"h",
            Requirement::after(s.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert!(matches!(
        target.node().map(|node| sp.splitter.need(node, &reason)),
        Some(SplitNeed::NotActionable)
    ));
}

#[tokio::test]
async fn inline_pressure_is_discarded_after_authoritative_revalidation() {
    let s = store();
    let root = Node::leaf(LeafBody::from_entries([live(b"a"), live(b"b")]));
    s.create_root(COLL, &root).await.unwrap();
    let bg = Arc::new(Background::new());
    let candidates =
        MaintenanceCandidates::with_policies(NodeSizePolicy::default(), pressure_inline());
    let sp = restructurer_with_candidates(&s, &bg, candidates.clone());
    let root_path = root_path();

    candidates
        .hint_sink()
        .observe_inline_pressure(&root_path, b"b", 8);
    sp.run_once().await;
    assert_eq!(
        sp.stats_and_reset(),
        RestructurerStats {
            candidates: 1,
            inline_pressure: InlinePressureStats {
                candidates: 1,
                discarded: 1,
                ..InlinePressureStats::default()
            },
            ..RestructurerStats::default()
        },
        "a value that now fits does not reshape the tree"
    );

    candidates
        .hint_sink()
        .observe_inline_pressure(&root_path, b"missing", 8);
    sp.run_once().await;
    assert_eq!(
        sp.stats_and_reset(),
        RestructurerStats {
            candidates: 1,
            inline_pressure: InlinePressureStats {
                candidates: 1,
                discarded: 1,
                ..InlinePressureStats::default()
            },
            ..RestructurerStats::default()
        },
        "a key that disappeared does not reshape the tree"
    );
    assert!(
        s.load_root_node(COLL, Requirement::after(s.timeline.currentness_barrier()))
            .await
            .unwrap()
            .unwrap()
            .0
            .as_leaf()
            .is_some()
    );
}

#[tokio::test]
async fn contended_candidate_is_requeued() {
    let s = store();
    let holder = TxId::with_priority(0, b"holder");
    let mut node = Node::leaf(LeafBody::from_entries(
        [b"a".as_slice(), b"b", b"c", b"d"].iter().map(|k| live(k)),
    ));
    node.add_membership_reader(holder);
    let root = node;
    s.create_root(COLL, &root).await.unwrap();
    let bg = Arc::new(Background::new());
    let candidates = MaintenanceCandidates::with_policy(tiny());
    candidates.observe_leaf(
        &root_path(),
        &LeafBody::from_entries([live(b"a"), live(b"b"), live(b"c"), live(b"d")]),
    );
    let sp = restructurer_with_candidates(&s, &bg, candidates);

    sp.run_once().await;
    assert_eq!(
        sp.stats_and_reset(),
        RestructurerStats {
            candidates: 1,
            splits: 0,
            deferred: 1,
            ..RestructurerStats::default()
        }
    );
    assert!(
        s.load_root_node(COLL, Requirement::after(s.timeline.currentness_barrier()))
            .await
            .unwrap()
            .unwrap()
            .0
            .as_leaf()
            .is_some(),
        "an older holder defers the split"
    );

    let (mut root, observation) = s
        .load_root(COLL, Requirement::after(s.timeline.currentness_barrier()))
        .await
        .unwrap();
    root.remove_membership_holder(&holder);
    assert!(s.store_root(COLL, &root, &observation).await.unwrap());

    sp.run_once().await;
    assert_eq!(
        sp.stats_and_reset(),
        RestructurerStats {
            candidates: 1,
            splits: 1,
            deferred: 0,
            ..RestructurerStats::default()
        }
    );
    assert!(
        s.load_root_node(COLL, Requirement::after(s.timeline.currentness_barrier()))
            .await
            .unwrap()
            .unwrap()
            .0
            .as_index()
            .is_some(),
        "the retained candidate splits after the holder leaves"
    );
}

#[tokio::test]
async fn split_wounds_a_younger_entry_holder_and_lands() {
    let s = store();
    let bg = Arc::new(Background::new());
    let (sp, mon) = restructurer_and_monitor(&s, &bg, tiny());
    let younger = TxId::new_at(rt::system_now() + Duration::from_secs(1));
    mon.begin_tx(&younger);
    s.store_node(
        COLL,
        "L",
        &leaf_with_locked_entry(&[b"a", b"b", b"c", b"d"], &younger),
        None,
    )
    .await
    .unwrap();
    let root = Node::index(IndexNode::from_children([(Vec::new(), test_node_id("L"))]));
    s.create_root(COLL, &root).await.unwrap();

    split_path(&sp, &node_path("L"), &SplitReason::SoftCap)
        .await
        .unwrap();

    assert_eq!(
        mon.tx_status(&younger).await.unwrap(),
        TxCommitStatus::Wounded
    );
    let leaves = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN)
        .leaves(
            &collection(),
            Requirement::after(s.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert_eq!(leaves.len(), 2);
    for leaf in leaves {
        let node = leaf.node().unwrap();
        assert!(node.structural_gate().holders().is_empty());
        assert!(
            node.as_leaf()
                .unwrap()
                .entries()
                .all(|entry| !entry.is_locked_by(&younger))
        );
    }
}

#[tokio::test]
async fn split_help_forwards_a_committed_entry_holder_before_moving_its_entry() {
    let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let s = store_with_backend(backend.clone());
    let recorder = Arc::new(RecordingBackend::new(backend));
    let operations = recorder.log();
    let other = store_with_backend(recorder);
    let bg = Arc::new(Background::new());
    let (sp, _) = restructurer_and_monitor(&s, &bg, tiny());
    let holder = TxId::with_priority(1, b"committed");
    let other_bg = Arc::new(Background::new());
    let other_transactions = other.foundation.tx_records.clone();
    let other_mon = other.foundation.monitor_for(
        &other_bg,
        RetryConfig::default(),
        crate::monitor::ProtocolTiming::default(),
    );
    let other_key_state = KeyStateResolver::new(other_mon.clone());
    let other_coord = LeafCoordinator::with_hinter(
        other.nodes.clone(),
        other_key_state,
        other_mon.clone(),
        RetryConfig::default(),
        NodeSizePolicy::default(),
        Arc::new(NoStructuralHints),
    );
    let other_locker = crate::tlocker::Locker::new(
        other_coord,
        TreeRouter::new(other.nodes.clone(), std::num::NonZeroUsize::MIN),
        crate::collection_coordination::CollectionStateResolver::new(
            other.records.clone(),
            other_transactions,
            other.timeline.clone(),
            other_mon.clone(),
            RetryConfig::default(),
        ),
        other_mon.clone(),
        RetryConfig::default(),
        std::num::NonZeroUsize::MIN,
    );
    other_mon.begin_tx(&holder);
    let mut record = TxRecord::new(holder, TxCommitStatus::Committed);
    record.writes.push(TxWrite {
        key: LogicalKey::new(collection(), b"d"),
        value: Arc::from(b"new-d".as_slice()),
        deleted: false,
        prev_writer: Some(tx_id(&[1])),
    });

    let entries: Vec<_> = [b"a".as_slice(), b"b", b"c", b"d"]
        .iter()
        .map(|key| live(key))
        .collect();
    let node = Node::leaf(LeafBody::from_entries(entries));
    s.store_node(COLL, "L", &node, None).await.unwrap();
    let root = Node::index(IndexNode::from_children([(Vec::new(), test_node_id("L"))]));
    s.create_root(COLL, &root).await.unwrap();

    let accesses = crate::access::AccessSet::new(
        Vec::new(),
        vec![crate::access::WriteAccess::put(
            LogicalKey::new(collection(), b"d"),
            Arc::from(b"new-d".as_slice()),
        )],
        Vec::new(),
    );
    let crate::tlocker::LockOutcome::Locked(locked) = other_locker
        .keys()
        .lock_at(
            &holder,
            &accesses,
            false,
            Requirement::after(other.timeline.currentness_barrier()),
        )
        .await
        .unwrap()
    else {
        panic!("key lock must be acquired before the split");
    };
    record.locks = locked.locked_paths();
    other_mon.commit_tx(record).await.unwrap();

    let held = other
        .nodes
        .load_leaf(&node_path("L"), Requirement::ANY)
        .await
        .unwrap();
    assert!(held.entries().lookup(b"d").unwrap().is_locked_by(&holder));
    split_path(&sp, &node_path("L"), &SplitReason::SoftCap)
        .await
        .unwrap();

    let leaf = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN)
        .route_key(
            &collection(),
            b"d",
            Requirement::after(s.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert!(leaf.node().unwrap().structural_gate().holders().is_empty());
    let entry = leaf
        .node()
        .unwrap()
        .as_leaf()
        .unwrap()
        .entries()
        .find(|entry| entry.key == b"d")
        .unwrap();
    assert_eq!(entry.current, CurrentState::External { writer: holder });
    assert!(entry.lock_holders().is_empty());
    assert_eq!(entry.lock_type(), LockType::None);

    // A different instance still targeting the pre-split source must
    // re-descend and converge without recreating the removed holder.
    operations.lock().unwrap().clear();
    other_locker.keys().write_back(&holder, &locked).await;
    {
        let operations = operations.lock().unwrap();
        let source = node_path("L").to_string();
        assert_eq!(
            operations
                .iter()
                .find(|operation| operation.path == source)
                .unwrap()
                .op,
            "write_if",
            "the cached hold goes directly to CAS, whose conflict exposes the split"
        );
        let writes: Vec<_> = operations
            .iter()
            .filter(|operation| operation.op == "write_if")
            .map(|operation| operation.path.as_str())
            .collect();
        assert_eq!(
            writes,
            [source.as_str()],
            "the split already published the moved entry"
        );
    }
    let current = TreeRouter::new(other.nodes.clone(), std::num::NonZeroUsize::MIN)
        .route_key(&collection(), b"d", Requirement::ANY)
        .await
        .unwrap();
    let current = current
        .node()
        .unwrap()
        .as_leaf()
        .unwrap()
        .lookup(b"d")
        .unwrap();
    assert_eq!(current.current, CurrentState::External { writer: holder });
    assert!(current.lock_holders().is_empty());
}

#[tokio::test]
async fn split_defers_to_an_older_membership_reader_then_lands() {
    let s = store();
    let bg = Arc::new(Background::new());
    let (sp, mon) = restructurer_and_monitor(&s, &bg, tiny());
    let older = TxId::new_at(rt::system_now() - Duration::from_secs(1));
    mon.begin_tx(&older);
    s.store_node(
        COLL,
        "L",
        &leaf_with_membership_reader(&[b"a", b"b", b"c", b"d"], &older),
        None,
    )
    .await
    .unwrap();
    let root = Node::index(IndexNode::from_children([(Vec::new(), test_node_id("L"))]));
    s.create_root(COLL, &root).await.unwrap();

    sp.candidates.observe_leaf(
        &node_path("L"),
        &LeafBody::from_entries([live(b"a"), live(b"b"), live(b"c"), live(b"d")]),
    );
    sp.run_once().await;
    assert_eq!(
        TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN)
            .leaves(
                &collection(),
                Requirement::after(s.timeline.currentness_barrier())
            )
            .await
            .unwrap()
            .len(),
        1
    );

    mon.abort_owned_tx(&older).await.unwrap();
    sp.run_once().await;
    assert_eq!(
        TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN)
            .leaves(
                &collection(),
                Requirement::after(s.timeline.currentness_barrier())
            )
            .await
            .unwrap()
            .len(),
        2
    );
}

// ADR-031 byte cap: a leaf well under the entry cap but over the encoded
// byte cap is still fed and split. Regression for the byte cap having no
// producer (only the entry-count crossing used to enqueue).
#[tokio::test]
async fn byte_cap_enqueues_and_splits_below_entry_cap() {
    let s = store();
    let root = Node::leaf(LeafBody::from_entries(
        [b"a".as_slice(), b"b", b"c", b"d"].iter().map(|k| live(k)),
    ));
    s.create_root(COLL, &root).await.unwrap();
    let bg = Arc::new(Background::new());

    // A generous entry cap but a tiny byte cap: the four-entry leaf is far
    // under the entry cap yet over the byte cap.
    let policy = NodeSizePolicy::builder()
        .leaf_max_entries(1000)
        .node_soft_max_bytes(8)
        .index_max_children(1000)
        .build()
        .unwrap();
    let candidates = MaintenanceCandidates::with_policy(policy);
    candidates.observe_leaf(
        &root_path(),
        &LeafBody::from_entries([live(b"a"), live(b"b"), live(b"c"), live(b"d")]),
    );

    let sp = restructurer_with_candidates(&s, &bg, candidates);
    sp.run_once().await;

    // The only cap crossed is the byte cap, so a split here proves the byte
    // cap now has a producer.
    let leaves = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN)
        .leaves(
            &collection(),
            Requirement::after(s.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert_eq!(leaves.len(), 2, "byte-cap overflow triggered a split");
}

// ADR-031 cascade healing: splitting a sibling whose own separator was never
// published still lands every separator. The parent knows P0 -> M, while
// the leaf chain already extends M -> S without S's separator. When S
// splits, reconciliation starts at the last published edge and lands both
// the missing `S` separator and the new one.
#[tokio::test]
async fn splitting_an_unpublished_sibling_reconciles_the_chain() {
    let s = store();
    s.store_node(
        COLL,
        "P0",
        &leaf_node(&[b"a", b"b"], Some(b"g"), Some("M")),
        None,
    )
    .await
    .unwrap();
    s.store_node(
        COLL,
        "M",
        &leaf_node(&[b"g", b"h"], Some(b"m"), Some("S")),
        None,
    )
    .await
    .unwrap();
    s.store_node(COLL, "S", &leaf_node(&[b"m", b"n", b"o"], None, None), None)
        .await
        .unwrap();
    let root = Node::index(IndexNode::from_children([
        (Vec::new(), test_node_id("P0")),
        (b"g".to_vec(), test_node_id("M")),
    ]));
    s.create_root(COLL, &root).await.unwrap();
    let bg = Arc::new(Background::new());

    // Tiny leaf cap so S splits, but a wide fan-out so the parent index does
    // not itself overflow — keeping the assertion on its separators direct.
    let policy = NodeSizePolicy::builder()
        .leaf_max_entries(2)
        .node_soft_max_bytes(1 << 20)
        .index_max_children(100)
        .build()
        .unwrap();
    split_path(
        &restructurer(&s, &bg, policy),
        &node_path("S"),
        &SplitReason::SoftCap,
    )
    .await
    .unwrap();

    // The existing `g -> M` edge is retained, and the parent learns both
    // the previously missing `m -> S` edge and S's new `n` edge.
    let (root_node, _) = s
        .load_root_node(COLL, Requirement::after(s.timeline.currentness_barrier()))
        .await
        .unwrap()
        .unwrap();
    let seps: Vec<Vec<u8>> = root_node
        .as_index()
        .unwrap()
        .children()
        .map(|(sep, _)| sep.to_vec())
        .collect();
    assert_eq!(
        seps,
        vec![b"".to_vec(), b"g".to_vec(), b"m".to_vec(), b"n".to_vec()],
        "the whole chain's separators are published"
    );

    // Every key is still reachable in order.
    let router = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN);
    for k in [b"a".as_slice(), b"b", b"g", b"h", b"m", b"n", b"o"] {
        let loc = router
            .route_key(
                &collection(),
                k,
                Requirement::after(s.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        assert!(
            loc.node().unwrap().as_leaf().unwrap().exists(k),
            "key {k:?} lost"
        );
    }
}

// ADR-032 retry path: a reconciliation whose parent CAS keeps losing leaves
// its structural intent in progress and is re-queued for a later sweep. A
// backend that blocks writes to the root `_r` forces the reconciliation to
// give up; healing it lets the re-driven reconciliation land.
#[tokio::test]
async fn lost_parent_cas_is_reconciled_by_a_later_sweep() {
    let (backend, blocker) = RootWriteBlocker::wrap(Arc::new(MemoryBackend::new()));
    let s = store_with_backend(backend.clone() as Arc<dyn Backend>);

    // A root index over a single leaf L[a,b,c] (over the cap).
    s.store_node(COLL, "L", &leaf_node(&[b"a", b"b", b"c"], None, None), None)
        .await
        .unwrap();
    let root = Node::index(IndexNode::from_children([(Vec::new(), test_node_id("L"))]));
    s.create_root(COLL, &root).await.unwrap();
    let bg = Arc::new(Background::new());
    let sp = restructurer(&s, &bg, tiny());

    // Block the parent `_r` CAS: the split lands (L shrinks, a sibling is
    // created) but the parent reconciliation cannot, so it is re-queued.
    blocker.block(true);
    assert!(matches!(
        split_path(&sp, &node_path("L"), &SplitReason::SoftCap).await,
        Err(TransError::Retry)
    ));
    let (blocked_root, _) = s
        .load_root_node(COLL, Requirement::after(s.timeline.currentness_barrier()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        blocked_root.as_index().unwrap().len(),
        1,
        "separator is not published while the parent CAS is blocked"
    );
    assert!(
        blocked_root.structural_gate().holders().is_empty(),
        "a reconciliation that gives up releases the parent gate"
    );
    let (blocked_coordination, _) = s
        .records
        .load_record(
            &collection(),
            Requirement::after(s.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert_eq!(
        blocked_coordination.topology_participants().count(),
        1,
        "the participant stays registered while structural recovery is pending"
    );
    assert_eq!(
        TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN)
            .leaves(
                &collection(),
                Requirement::after(s.timeline.currentness_barrier())
            )
            .await
            .unwrap()
            .len(),
        2,
        "the leaves still split; only the parent separator is missing"
    );

    // Heal and sweep: the re-queued separator is published.
    blocker.block(false);
    sp.run_once().await;
    let (healed_root, _) = s
        .load_root_node(COLL, Requirement::after(s.timeline.currentness_barrier()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        healed_root.as_index().unwrap().len(),
        2,
        "the deferred separator is republished by a later sweep"
    );
    assert!(sp.recover_structural_intents().await.unwrap());
    assert!(
        s.discover_structural_intents("db", Requirement::after(s.timeline.currentness_barrier()))
            .await
            .unwrap()
            .is_empty()
    );
    let (recovered_coordination, _) = s
        .records
        .load_record(
            &collection(),
            Requirement::after(s.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert_eq!(recovered_coordination.topology_participants().count(), 0);
}

/// Controls a hook that rejects CASes of the tree root.
struct RootWriteBlocker {
    blocked: std::sync::atomic::AtomicBool,
}

impl RootWriteBlocker {
    fn wrap(inner: Arc<dyn Backend>) -> (Arc<HookBackend>, Arc<Self>) {
        let blocker = Arc::new(Self {
            blocked: std::sync::atomic::AtomicBool::new(false),
        });
        let backend = HookBackend::new(inner);
        backend.set_before({
            let blocker = blocker.clone();
            move |op| {
                let blocked = blocker.blocked.load(std::sync::atomic::Ordering::SeqCst)
                    && match op {
                        BackendOp::WriteIf { path, value, .. }
                        | BackendOp::WriteIfNotExists { path, value }
                            if path.ends_with("/_r") =>
                        {
                            Node::decode(value)
                                .ok()
                                .and_then(|root| root.as_index().map(|index| index.len() > 1))
                                .unwrap_or(false)
                        }
                        _ => false,
                    };
                let result = if blocked {
                    Err(glassdb_backend::BackendError::Precondition)
                } else {
                    Ok(())
                };
                let future: HookFuture = Box::pin(async move { result });
                future
            }
        });
        (backend, blocker)
    }

    fn block(&self, on: bool) {
        self.blocked.store(on, std::sync::atomic::Ordering::SeqCst);
    }
}
