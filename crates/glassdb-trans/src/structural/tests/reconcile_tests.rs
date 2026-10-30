use super::*;

#[test]
fn reconciliation_queue_is_bounded_and_drops_the_oldest() {
    let s = store();
    let bg = Arc::new(Background::new());
    let reconciler = restructurer(&s, &bg, tiny()).reconciler;
    for ordinal in 0..=DEFERRED_RECONCILIATION_CAP {
        reconciler.defer(PendingReconciliation {
            collection: collection(),
            key: ordinal.to_be_bytes().to_vec(),
            target: NodeId::from_bytes((ordinal as u128).to_be_bytes()),
        });
    }

    let pending = reconciler.drain_pending();
    assert_eq!(pending.len(), DEFERRED_RECONCILIATION_CAP);
    assert_eq!(pending[0].key, 1usize.to_be_bytes());
}

// A stale parent over a three-leaf chain: the root names only L0, while the
// right-links have moved keys at and above "m" to L1 and "t" to L4.
async fn seed_unpublished_leaf_chain(s: &TestStore, children: &[(&[u8], &str)]) -> Node {
    s.store_node(
        COLL,
        "L0",
        &leaf_node(&[b"apple"], Some(b"m"), Some("L1")),
        None,
    )
    .await
    .unwrap();
    s.store_node(
        COLL,
        "L1",
        &leaf_node(&[b"mango"], Some(b"t"), Some("L4")),
        None,
    )
    .await
    .unwrap();
    s.store_node(COLL, "L4", &leaf_node(&[b"zebra"], None, None), None)
        .await
        .unwrap();
    let root =
        Node::index(IndexNode::from_children(children.iter().map(
            |(separator, child)| (separator.to_vec(), test_node_id(child)),
        )));
    s.create_root(COLL, &root).await.unwrap();
    root
}

async fn reconciler(s: &TestStore, bg: &Arc<Background>) -> ParentReconciler {
    restructurer(s, bg, NodeSizePolicy::default()).reconciler
}

// Reconciles the root for `key`, which `target` covers, and returns the
// reconciled root index.
async fn reconciled_root(s: &TestStore, key: &[u8], target: &str) -> IndexNode {
    let bg = Arc::new(Background::new());
    let reconciler = reconciler(s, &bg).await;
    let mut reconciliation =
        reconciler.begin(&collection(), [(key.to_vec(), test_node_id(target))]);
    assert!(matches!(
        reconciler.reconcile(&mut reconciliation).await.unwrap(),
        ReconciliationOutcome::Reconciled
    ));
    let (root, _) = s
        .load_root(COLL, Requirement::after(s.timeline.currentness_barrier()))
        .await
        .unwrap();
    root.as_index().unwrap().clone()
}

// Seeds the leaf chain under a root with `children`, whose gate a live
// reconciliation of another database instance holds. Returns the root.
async fn seed_gated_root(s: &TestStore, sp: &Restructurer, children: &[(&[u8], &str)]) -> Node {
    let mut root = seed_unpublished_leaf_chain(s, children).await;
    let holder = TxId::with_priority(1, b"live-reconciliation");
    sp.mon.begin_tx(&holder);
    root.set_structural_gate(holder);
    let (_, observation) = s
        .load_root(COLL, Requirement::after(s.timeline.currentness_barrier()))
        .await
        .unwrap();
    assert!(s.store_root(COLL, &root, &observation).await.unwrap());
    root
}

async fn current_root(s: &TestStore) -> Node {
    let (root, _) = s
        .load_root(COLL, Requirement::after(s.timeline.currentness_barrier()))
        .await
        .unwrap();
    root
}

/// A parent that already agrees needs no write, so its reconciliation does not
/// wait for the gate. A worker of another database instance can hold that
/// gate for each try, and the reconciliation would end in recovery.
#[tokio::test]
async fn reconciliation_of_an_agreeing_parent_does_not_wait_for_its_gate() {
    let s = store();
    let bg = Arc::new(Background::new());
    let sp = restructurer(&s, &bg, NodeSizePolicy::default());
    let root = seed_gated_root(&s, &sp, &[(b"", "L0"), (b"m", "L1"), (b"t", "L4")]).await;

    let mut reconciliation = sp
        .reconciler
        .begin(&collection(), [(b"m".to_vec(), test_node_id("L1"))]);
    assert!(matches!(
        sp.reconciler.reconcile(&mut reconciliation).await.unwrap(),
        ReconciliationOutcome::Reconciled
    ));
    assert_eq!(
        current_root(&s).await,
        root,
        "the gate of the root stays with its holder"
    );
}

/// A parent that does not agree yet needs a write, so its reconciliation
/// still waits for the gate.
#[tokio::test]
async fn reconciliation_of_a_parent_that_does_not_agree_waits_for_its_gate() {
    let s = store();
    let bg = Arc::new(Background::new());
    let sp = restructurer(&s, &bg, NodeSizePolicy::default());
    let root = seed_gated_root(&s, &sp, &[(b"", "L0")]).await;

    let mut reconciliation = sp
        .reconciler
        .begin(&collection(), [(b"m".to_vec(), test_node_id("L1"))]);
    assert!(matches!(
        sp.reconciler.reconcile(&mut reconciliation).await,
        Err(TransError::Retry)
    ));
    assert_eq!(current_root(&s).await, root);
    assert_eq!(
        sp.reconciler.drain_pending().len(),
        1,
        "the route waits for a later sweep"
    );
}

#[tokio::test]
async fn reconciliation_rechecks_a_child_read_started_before_the_split() {
    let memory = Arc::new(MemoryBackend::new());
    let peer = store_with_backend(memory.clone());
    peer.store_node(
        COLL,
        "L0",
        &leaf_node(&[b"apple", b"mango"], None, None),
        None,
    )
    .await
    .unwrap();
    let parent = Node::index(IndexNode::from_children([(Vec::new(), test_node_id("L0"))]));
    peer.create_root(COLL, &parent).await.unwrap();

    let hook = HookBackend::new(memory);
    let local = store_with_backend(hook.clone());
    let bg = Arc::new(Background::new());
    let reconciler = reconciler(&local, &bg).await;
    let (_, parent_observation) = local.load_root(COLL, Requirement::ANY).await.unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    hook.set_after({
        let path = node_path("L0").to_string();
        let entered = entered.clone();
        let release = release.clone();
        move |operation, outcome| {
            let park = matches!(operation, BackendOp::Read { path: read } if *read == path)
                && outcome.is_success();
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
    let pending = tokio::spawn({
        let nodes = local.nodes.clone();
        async move {
            nodes
                .load_node(&collection(), &test_node_id("L0"), Requirement::ANY)
                .await
        }
    });
    entered.notified().await;

    // The child read started after the parent read, but still returns the
    // state from before this split. Their invocation order cannot prove
    // that parent reconciliation has seen the split.
    let (_, source) = peer.load_node(COLL, "L0", Requirement::ANY).await.unwrap();
    peer.store_node(COLL, "L1", &leaf_node(&[b"mango"], None, None), None)
        .await
        .unwrap();
    peer.store_node(
        COLL,
        "L0",
        &leaf_node(&[b"apple"], Some(b"m"), Some("L1")),
        Some(&source),
    )
    .await
    .unwrap();
    let mut reconciliation = reconciler.begin(&collection(), [(b"m".to_vec(), test_node_id("L1"))]);
    hook.clear_after();
    release.notify_one();
    let (old_child, old_observation) = pending.await.unwrap().unwrap();
    assert!(old_child.right_sibling().is_none());
    assert!(!old_observation.is_current_after(reconciliation.start));
    assert!(!parent_observation.is_current_after(reconciliation.start));

    assert!(matches!(
        reconciler.reconcile(&mut reconciliation).await.unwrap(),
        ReconciliationOutcome::Reconciled
    ));
    let (reconciled, _) = local
        .load_root(
            COLL,
            Requirement::after(local.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert_eq!(
        reconciled.as_index().unwrap().child_for(b"m"),
        Some(test_node_id("L1"))
    );
}

#[tokio::test]
async fn reconciliation_adds_every_missing_separator_in_chain_order() {
    let s = store();
    seed_unpublished_leaf_chain(&s, &[(b"", "L0")]).await;

    assert_eq!(
        reconciled_root(&s, b"t", "L4").await,
        test_index(&[(b"", "L0"), (b"m", "L1"), (b"t", "L4")])
    );
}

#[tokio::test]
async fn reconciliation_stops_at_the_child_that_covers_the_key() {
    let s = store();
    seed_unpublished_leaf_chain(&s, &[(b"", "L0")]).await;

    assert_eq!(
        reconciled_root(&s, b"m", "L1").await,
        test_index(&[(b"", "L0"), (b"m", "L1")]),
        "a separator past the key belongs to a later reconciliation"
    );
}

#[tokio::test]
async fn reconciliation_keeps_a_parent_that_names_every_edge() {
    let s = store();
    let full = [(b"".as_slice(), "L0"), (b"m", "L1"), (b"t", "L4")];
    seed_unpublished_leaf_chain(&s, &full).await;

    for (key, target) in [(b"m".as_slice(), "L1"), (b"t", "L4")] {
        assert_eq!(
            reconciled_root(&s, key, target).await,
            test_index(&full),
            "a current parent has nothing to reconcile for {key:?}"
        );
    }
}

#[tokio::test]
async fn reconciliation_routes_a_drained_child_to_its_merge_target() {
    let s = store();
    s.store_node(
        COLL,
        "L0",
        &leaf_node(&[b"apple"], Some(b"m"), Some("L1")),
        None,
    )
    .await
    .unwrap();
    let mut drained = leaf_node(&[], Some(b"t"), None).with_low_key(b"m".to_vec());
    drained.drain(test_node_id("L4"));
    s.store_node(COLL, "L1", &drained, None).await.unwrap();
    s.store_node(
        COLL,
        "L4",
        &leaf_node(&[b"mango", b"zebra"], None, None).with_low_key(b"m".to_vec()),
        None,
    )
    .await
    .unwrap();
    let full = [(b"".as_slice(), "L0"), (b"m", "L1"), (b"t", "L4")];
    s.create_root(COLL, &Node::index(test_index(&full)))
        .await
        .unwrap();

    assert_eq!(
        reconciled_root(&s, b"t", "L4").await,
        test_index(&[(b"", "L0"), (b"m", "L4")])
    );
}

#[tokio::test]
async fn parent_reconciliation_fails_on_a_right_link_cycle() {
    let s = store();
    let bg = Arc::new(Background::new());
    // L0 and L1 point at each other and share a high-key, so neither ever
    // covers the key and the walk would never terminate.
    s.store_node(
        COLL,
        "L0",
        &leaf_node(&[b"apple"], Some(b"m"), Some("L1")),
        None,
    )
    .await
    .unwrap();
    s.store_node(
        COLL,
        "L1",
        &leaf_node(&[b"cat"], Some(b"m"), Some("L0")),
        None,
    )
    .await
    .unwrap();
    let parent = test_index(&[(b"", "L0")]);
    s.create_root(COLL, &Node::index(parent.clone()))
        .await
        .unwrap();

    let error = reconciler(&s, &bg)
        .await
        .child_chain(
            &collection(),
            &parent,
            b"t",
            s.timeline.currentness_barrier(),
        )
        .await
        .expect_err("a cycle must not reconcile");
    assert!(
        error.to_string().contains("right-link hop bound"),
        "unexpected reconciliation error: {error}"
    );
}

// The sweep, the deferred reconciliations, and structural recovery reconcile
// the same parents. When two of them gate one parent at the same time, one
// loses the gate, and its changes end in recovery. The first gate write here
// waits until the second reconciliation has started.
#[tokio::test]
async fn reconciliations_of_one_collection_take_turns() {
    let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
    let s = store_with_backend(backend.clone() as Arc<dyn Backend>);
    seed_unpublished_leaf_chain(&s, &[(b"", "L0")]).await;
    let bg = Arc::new(Background::new());
    let reconciler = reconciler(&s, &bg).await;
    let second_started = Arc::new(tokio::sync::Notify::new());
    let gate_writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    backend.set_before({
        let root = root_path().to_string();
        let second_started = second_started.clone();
        let gate_writes = gate_writes.clone();
        move |op| {
            let first = matches!(op, BackendOp::WriteIf { .. })
                && op.path() == root
                && gate_writes.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
            let second_started = second_started.clone();
            let future: HookFuture = Box::pin(async move {
                if first {
                    // Leaves time for the second reconciliation to gate the
                    // parent, if it does not wait for its turn.
                    second_started.notified().await;
                    for _ in 0..100 {
                        tokio::task::yield_now().await;
                    }
                }
                Ok(())
            });
            future
        }
    });

    let mut first = reconciler.begin(&collection(), [(b"m".to_vec(), test_node_id("L1"))]);
    let mut second = reconciler.begin(&collection(), [(b"t".to_vec(), test_node_id("L4"))]);
    let (first, second) = tokio::join!(reconciler.reconcile(&mut first), async {
        second_started.notify_one();
        reconciler.reconcile(&mut second).await
    });
    backend.clear_before();

    assert!(matches!(first, Ok(ReconciliationOutcome::Reconciled)));
    assert!(matches!(second, Ok(ReconciliationOutcome::Reconciled)));
    assert!(reconciler.drain_pending().is_empty());
    // Each reconciliation gates the parent once and changes it once.
    assert_eq!(gate_writes.load(std::sync::atomic::Ordering::SeqCst), 4);
}
