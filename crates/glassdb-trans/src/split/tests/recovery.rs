use super::*;

#[tokio::test]
async fn settlement_cancels_a_prepared_split_before_node_creation() {
    let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
    let operations = recorder.log();
    let s = store_with_backend(Arc::new(recorder));
    let root = Node::leaf(LeafBody::from_entries(
        [b"a".as_slice(), b"b", b"c", b"d"]
            .iter()
            .map(|key| live(key)),
    ));
    s.create_root(COLL, &root).await.unwrap();
    let bg = Arc::new(Background::new());
    let sp = splitter(&s, &bg, tiny());
    let participant = TxId::with_priority(1, b"participant");

    sp.begin_topology_tx(&collection(), &participant)
        .await
        .unwrap();
    let intent = sp
        .recovery
        .prepare_intent(&collection(), None, &participant)
        .await
        .unwrap();
    sp.join_topology(&collection(), &participant).await.unwrap();
    sp.mon.abort_owned_tx(&participant).await.unwrap();

    operations.lock().unwrap().clear();
    sp.settle_topology_participant(&collection(), &participant)
        .await
        .unwrap();
    let expected_listing =
        ObjectPath::participant_structural_intents_prefix(&db_root("db"), &participant);
    let listings: Vec<_> = operations
        .lock()
        .unwrap()
        .iter()
        .filter(|operation| operation.op == "list")
        .map(|operation| operation.path.clone())
        .collect();
    assert!(!listings.is_empty());
    assert!(
        listings.iter().all(|path| path == &expected_listing),
        "settlement must list only the participant-owned intent prefix"
    );

    let worker = TxId::with_priority(2, b"worker");
    sp.mon.begin_tx(&worker);
    let reason = SplitReason::SoftCap;
    let attempt = sp
        .coordinate_root_split(&collection(), &worker, &reason, intent)
        .await;
    assert!(matches!(attempt.result, Err(TransError::Retry)));
    assert!(matches!(attempt.state, SplitAttemptResult::RetryCleanly));
    sp.finalize_split(&worker).await;
    assert!(
        s.list_nodes(COLL, Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap()
            .is_empty(),
        "a cancelled Preparing intent cannot create its reserved nodes"
    );
    let (root, _) = s
        .load_root(COLL, Requirement::AtLeast(s.timeline.now()))
        .await
        .unwrap();
    assert!(root.as_leaf().is_some());
    let (record, _) = s
        .records
        .load_record(&collection(), Requirement::AtLeast(s.timeline.now()))
        .await
        .unwrap();
    assert_eq!(record.topology_participants().count(), 0);
}

// Failures before the Ready CAS are cleanly cancellable. Once that CAS may
// have landed, every later failure must retain the Ready intent and its
// topology participant for structural recovery.
#[tokio::test]
async fn structural_split_failure_transition_table() {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum FailurePoint {
        StructuralGate,
        ReadyPrecondition,
        ReadyLostAck,
        ChildCreate,
        RootRewrite,
        LogDelete,
    }

    for (point, retains_ready) in [
        (FailurePoint::StructuralGate, false),
        (FailurePoint::ReadyPrecondition, false),
        (FailurePoint::ReadyLostAck, true),
        (FailurePoint::ChildCreate, true),
        (FailurePoint::RootRewrite, true),
        (FailurePoint::LogDelete, true),
    ] {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let s = store_with_backend(backend.clone());
        let root = Node::leaf(LeafBody::from_entries(
            [b"a".as_slice(), b"b", b"c", b"d"]
                .iter()
                .map(|key| live(key)),
        ));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());
        let sp = splitter(&s, &bg, tiny());

        let root_path = root_path().to_string();
        let nodes_prefix = ObjectPath::nodes_prefix(&collection());
        let structural_prefix = ObjectPath::structural_intents_prefix(&db_root("db"));
        let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        backend.set_before({
            let fired = fired.clone();
            let root_path = root_path.clone();
            let nodes_prefix = nodes_prefix.clone();
            let structural_prefix = structural_prefix.clone();
            move |operation| {
                let should_fail = match operation {
                    BackendOp::WriteIf { path, value, .. } => match point {
                        FailurePoint::StructuralGate => path == &root_path,
                        FailurePoint::ReadyPrecondition => {
                            path.starts_with(&structural_prefix)
                                && StructuralIntent::decode(value).is_ok_and(|intent| {
                                    intent.phase == StructuralIntentPhase::Ready
                                })
                        }
                        FailurePoint::RootRewrite => {
                            path == &root_path
                                && Node::decode(value).is_ok_and(|node| node.as_index().is_some())
                        }
                        FailurePoint::ReadyLostAck
                        | FailurePoint::ChildCreate
                        | FailurePoint::LogDelete => false,
                    },
                    BackendOp::WriteIfNotExists { path, .. } => {
                        point == FailurePoint::ChildCreate && path.starts_with(&nodes_prefix)
                    }
                    BackendOp::DeleteIf { path, .. } => {
                        point == FailurePoint::LogDelete && path.starts_with(&structural_prefix)
                    }
                    _ => false,
                };
                let result =
                    if should_fail && !fired.swap(true, std::sync::atomic::Ordering::SeqCst) {
                        match point {
                            FailurePoint::ReadyPrecondition | FailurePoint::RootRewrite => {
                                Err(glassdb_backend::BackendError::Precondition)
                            }
                            FailurePoint::StructuralGate
                            | FailurePoint::ChildCreate
                            | FailurePoint::LogDelete => {
                                Err(glassdb_backend::BackendError::other("injected failure"))
                            }
                            FailurePoint::ReadyLostAck => unreachable!(),
                        }
                    } else {
                        Ok(())
                    };
                let future: HookFuture = Box::pin(async move { result });
                future
            }
        });
        backend.set_after({
            let fired = fired.clone();
            let structural_prefix = structural_prefix.clone();
            move |operation, outcome| {
                let should_fail = point == FailurePoint::ReadyLostAck
                    && outcome.is_success()
                    && matches!(
                        operation,
                        BackendOp::WriteIf { path, value, .. }
                            if path.starts_with(&structural_prefix)
                                && StructuralIntent::decode(value).is_ok_and(|intent| {
                                    intent.phase == StructuralIntentPhase::Ready
                                })
                    );
                let result =
                    if should_fail && !fired.swap(true, std::sync::atomic::Ordering::SeqCst) {
                        Err(glassdb_backend::BackendError::Unavailable(
                            "injected lost acknowledgement".into(),
                        ))
                    } else {
                        Ok(())
                    };
                let future: HookFuture = Box::pin(async move { result });
                future
            }
        });

        assert!(
            sp.split_path(&ObjectPath::TreeRoot {
                collection: collection(),
            })
            .await
            .is_err(),
            "case {point:?}"
        );
        assert!(
            fired.load(std::sync::atomic::Ordering::SeqCst),
            "case {point:?} did not reach its failure point"
        );

        let logs = s
            .list_structural_intents("db", Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        if retains_ready {
            assert_eq!(logs.len(), 1, "case {point:?}");
            assert_eq!(
                logs[0].1.value().unwrap().phase,
                StructuralIntentPhase::Ready,
                "case {point:?}"
            );
        } else {
            assert!(logs.is_empty(), "case {point:?}");
        }

        let (record, _) = s
            .records
            .load_record(&collection(), Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert_eq!(
            record.topology_participants().count(),
            usize::from(retains_ready),
            "case {point:?}"
        );
    }
}

#[tokio::test]
async fn startup_structural_recovery_reclaims_an_orphan_after_restart() {
    let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let first = store_with_backend(backend.clone());
    first
        .store_node(COLL, "L", &leaf_node(&[b"a", b"b"], None, None), None)
        .await
        .unwrap();
    first
        .store_node(COLL, "R", &leaf_node(&[b"m", b"n"], None, None), None)
        .await
        .unwrap();
    let root = Node::index(IndexNode::from_children([(Vec::new(), "L".to_string())]));
    first.create_root(COLL, &root).await.unwrap();
    first
        .write_structural_intent("R", &nonroot_intent("L", "R", b"m"))
        .await
        .unwrap();
    drop(first);

    let second = store_with_backend(backend);
    let bg = Arc::new(Background::new());
    let splitter = splitter(&second, &bg, tiny());
    splitter.start();
    for _ in 0..20 {
        if matches!(
            second
                .load_node(COLL, "R", Requirement::AtLeast(second.timeline.now()))
                .await,
            Err(StorageError::NotFound)
        ) {
            break;
        }
        rt::yield_now().await;
    }

    assert!(matches!(
        second
            .load_node(COLL, "R", Requirement::AtLeast(second.timeline.now()))
            .await,
        Err(StorageError::NotFound)
    ));
    assert!(
        second
            .load_node(COLL, "L", Requirement::AtLeast(second.timeline.now()))
            .await
            .is_ok()
    );
    assert!(
        second
            .list_structural_intents("db", Requirement::AtLeast(second.timeline.now()))
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn structural_recovery_defers_while_the_source_writer_is_live() {
    let s = store();
    let bg = Arc::new(Background::new());
    let sp = splitter(&s, &bg, tiny());
    let id = TxId::with_priority(1, b"live-split");
    sp.mon.begin_tx(&id);

    let mut source = leaf_node(&[b"a", b"b"], None, None);
    source.set_structural_gate(id.clone());
    s.store_node(COLL, "L", &source, None).await.unwrap();
    s.store_node(COLL, "R", &leaf_node(&[b"m", b"n"], None, None), None)
        .await
        .unwrap();
    let root = Node::index(IndexNode::from_children([(Vec::new(), "L".to_string())]));
    s.create_root(COLL, &root).await.unwrap();
    let intent = nonroot_intent("L", "R", b"m");
    s.write_structural_intent("R", &intent).await.unwrap();

    assert!(sp.recover_structural_intents().await);
    assert!(
        s.load_node(COLL, "R", Requirement::AtLeast(s.timeline.now()))
            .await
            .is_ok()
    );
    assert_eq!(
        s.list_structural_intents("db", Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap()
            .len(),
        1
    );

    sp.mon.abort_owned_tx(&id).await.unwrap();
    assert!(sp.recover_structural_intents().await);
    assert!(matches!(
        s.load_node(COLL, "R", Requirement::AtLeast(s.timeline.now()))
            .await,
        Err(StorageError::NotFound)
    ));
    assert!(
        s.list_structural_intents("db", Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap()
            .is_empty()
    );
}

/// Regression: structural recovery must fence an in-flight split by reading
/// the source *freshly*, not from a snapshot it cached before the split took
/// the gate.
///
/// A split acquires its source structural gate before writing its structural
/// intent, so the intent's watermark is at least as fresh as that gate.
/// Recovery once fenced (and tested reachability) at a single sweep-start
/// epoch, which a pre-split cached snapshot — no gate, no sibling — could
/// satisfy; recovery then judged the live split unapplied and deleted its
/// freshly created, now-live child, breaking the leaf right-link chain.
/// Pinning the reads to the intent's own watermark forces recovery past the
/// gate write.
///
/// Here `s` (recovery) caches the pre-gate source, a peer sharing the backend
/// then takes the gate and creates the child, and recovery must defer instead
/// of reclaiming the child. Reading the source from the stale cache (as the
/// buggy sweep epoch allowed) reclaims `R`; the fresh read observes the live
/// holder and defers.
#[tokio::test]
async fn recovery_reads_a_live_split_freshly_and_keeps_its_child() {
    let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let s = store_with_backend(backend.clone());
    let peer = store_with_backend(backend);
    let bg = Arc::new(Background::new());
    let sp = splitter(&s, &bg, tiny());
    let id = TxId::with_priority(1, b"inflight-split");
    sp.mon.begin_tx(&id);

    // Initial tree, written by the peer: a root index over a single leaf L
    // that carries no structural gate.
    peer.store_node(COLL, "L", &leaf_node(&[b"a", b"b"], None, None), None)
        .await
        .unwrap();
    let root = Node::index(IndexNode::from_children([(Vec::new(), "L".to_string())]));
    peer.create_root(COLL, &root).await.unwrap();

    // Recovery reads L first, caching the pre-gate snapshot (no gate). A weak
    // freshness bound would later be satisfied by exactly this stale entry.
    s.load_node(COLL, "L", Requirement::AtLeast(s.timeline.now()))
        .await
        .unwrap();

    // The in-flight split (peer, sharing the backend): take the source gate
    // and create the sibling. `s`'s cache is unaware of both writes.
    let (mut gated, version) = peer
        .load_node(COLL, "L", Requirement::AtLeast(peer.timeline.now()))
        .await
        .unwrap();
    gated.set_structural_gate(id.clone());
    assert!(
        peer.store_node(COLL, "L", &gated, Some(&version))
            .await
            .unwrap()
    );
    peer.store_node(COLL, "R", &leaf_node(&[b"m", b"n"], None, None), None)
        .await
        .unwrap();

    // The intent is written after the gate, so its watermark is at least as
    // fresh; recovery reading at that watermark must observe the live gate.
    let intent = nonroot_intent("L", "R", b"m");
    s.write_structural_intent("R", &intent).await.unwrap();

    assert!(
        sp.recover_structural_intents().await,
        "recovery must defer to the live split rather than reclaim its child"
    );
    assert!(
        s.load_node(COLL, "R", Requirement::AtLeast(s.timeline.now()))
            .await
            .is_ok(),
        "the live split's child must survive recovery"
    );
    assert_eq!(
        s.list_structural_intents("db", Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap()
            .len(),
        1,
        "the in-flight split's intent is left for a later sweep"
    );
}

#[tokio::test]
async fn recovery_rolls_forward_a_landed_nonroot_split() {
    let s = store();
    s.store_node(
        COLL,
        "P0",
        &leaf_node(&[b"a", b"b"], Some(b"m"), Some("L")),
        None,
    )
    .await
    .unwrap();
    s.store_node(
        COLL,
        "L",
        &leaf_node(&[b"m", b"n"], Some(b"t"), Some("R")),
        None,
    )
    .await
    .unwrap();
    s.store_node(COLL, "R", &leaf_node(&[b"t", b"u"], None, None), None)
        .await
        .unwrap();
    let root = Node::index(IndexNode::from_children([
        (Vec::new(), "P0".to_string()),
        (b"m".to_vec(), "L".to_string()),
    ]));
    s.create_root(COLL, &root).await.unwrap();
    let bg = Arc::new(Background::new());
    let sp = splitter(&s, &bg, tiny());

    let intent = StructuralIntent {
        collection: collection(),
        source_token: Some(test_token("L")),
        source_version: String::new(),
        created_tokens: vec![test_token("R")],
        split_key: b"t".to_vec(),
        participant_id: TxId::from_bytes(b"structural-participant".to_vec()),
        phase: StructuralIntentPhase::Ready,
    };
    s.write_structural_intent("R", &intent).await.unwrap();

    assert!(sp.recover_structural_intents().await);

    let (root_node, _) = s
        .load_root_node(COLL, Requirement::AtLeast(s.timeline.now()))
        .await
        .unwrap()
        .unwrap();
    assert!(
        !root_node.over_soft_cap(&tiny()),
        "recovery completes the parent split requested by publication"
    );
    let router = TreeRouter::new(s.nodes.clone(), std::num::NonZeroUsize::MIN);
    assert_eq!(
        router
            .leaves(&collection(), Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap()
            .len(),
        3,
        "the recovered separator keeps every leaf reachable after the parent split"
    );
    for key in [b"a".as_slice(), b"m", b"t"] {
        let leaf = router
            .route_key(&collection(), key, Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert!(leaf.node().unwrap().as_leaf().unwrap().exists(key));
    }
    assert!(
        s.list_structural_intents("db", Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap()
            .is_empty()
    );
}

struct FirstSourceWriteGate {
    armed: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl FirstSourceWriteGate {
    fn wrap(inner: Arc<dyn Backend>, source_path: String) -> (Arc<HookBackend>, Arc<Self>) {
        let gate = Arc::new(Self {
            armed: std::sync::atomic::AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let backend = HookBackend::new(inner);
        backend.set_before({
            let gate = gate.clone();
            move |op| {
                let wait = matches!(
                    op,
                    BackendOp::WriteIf { path, .. }
                        if path == &source_path
                            && gate
                                .armed
                                .swap(false, std::sync::atomic::Ordering::SeqCst)
                );
                let gate = gate.clone();
                let future: HookFuture = Box::pin(async move {
                    if wait {
                        gate.entered.notify_one();
                        gate.release.notified().await;
                    }
                    Ok(())
                });
                future
            }
        });
        (backend, gate)
    }

    fn arm(&self) {
        self.armed.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    async fn wait_until_entered(&self) {
        self.entered.notified().await;
    }

    fn release(&self) {
        self.release.notify_one();
    }
}

#[tokio::test]
async fn recovery_fences_an_aborted_writer_before_reclaiming_its_sibling() {
    let source_path = node_path("L").to_string();
    let (backend, gate) =
        FirstSourceWriteGate::wrap(Arc::new(MemoryBackend::new()), source_path.clone());
    let backend: Arc<dyn Backend> = backend;
    let s = store_with_backend(backend.clone());
    // This writer models a separately opened database, so it owns a
    // distinct database-local path coordinator over the shared backend.
    let peer = store_with_backend(backend);
    let bg = Arc::new(Background::new());
    let sp = splitter(&s, &bg, tiny());
    let id = TxId::with_priority(1, b"racing-split");

    let mut original = leaf_node(&[b"a", b"b", b"m", b"n"], None, None);
    original.set_structural_gate(id.clone());
    s.store_node(COLL, "L", &original, None).await.unwrap();
    let (mut shrunk, source_version) = s
        .load_node(COLL, "L", Requirement::AtLeast(s.timeline.now()))
        .await
        .unwrap();
    let (right, split_key) = shrunk.split("R").unwrap();
    shrunk.remove_structural_gate(&id);
    s.store_node(COLL, "R", &right, None).await.unwrap();
    let root = Node::index(IndexNode::from_children([(Vec::new(), "L".to_string())]));
    s.create_root(COLL, &root).await.unwrap();

    let intent = StructuralIntent {
        collection: collection(),
        source_token: Some(test_token("L")),
        source_version: source_version.revision().unwrap().serialize().to_string(),
        created_tokens: vec![test_token("R")],
        split_key,
        participant_id: TxId::from_bytes(b"structural-participant".to_vec()),
        phase: StructuralIntentPhase::Ready,
    };
    s.write_structural_intent("R", &intent).await.unwrap();
    sp.mon.begin_tx(&id);
    assert_eq!(
        sp.mon.preempt_tx(&id).await.unwrap(),
        TxFinalStatus::Aborted
    );

    gate.arm();
    let recovering = {
        let sp = sp.clone();
        tokio::spawn(async move { sp.recover_structural_intents().await })
    };
    gate.wait_until_entered().await;

    assert!(
        peer.store_node(COLL, "L", &shrunk, Some(&source_version))
            .await
            .unwrap()
    );
    gate.release();
    assert!(recovering.await.unwrap());

    assert!(
        s.load_node(COLL, "R", Requirement::AtLeast(s.timeline.now()))
            .await
            .is_ok()
    );
    let (root_node, _) = s
        .load_root_node(COLL, Requirement::AtLeast(s.timeline.now()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        root_node.as_index().unwrap().child_for(b"m"),
        Some(test_token("R").as_str())
    );
}

async fn recovery_that_needs_a_parent_split(
    participant: &TxId,
) -> (TestStore, Arc<Background>, Splitter, StructuralIntent) {
    let s = store();
    s.store_node(
        COLL,
        "P0",
        &leaf_node(&[b"a", b"b"], Some(b"m"), Some("L")),
        None,
    )
    .await
    .unwrap();
    s.store_node(
        COLL,
        "L",
        &leaf_node(&[b"m", b"n"], Some(b"t"), Some("R")),
        None,
    )
    .await
    .unwrap();
    s.store_node(COLL, "R", &leaf_node(&[b"t", b"u"], None, None), None)
        .await
        .unwrap();
    s.create_root(
        COLL,
        &Node::index(IndexNode::from_children([
            (Vec::new(), "P0".to_string()),
            (b"m".to_vec(), "L".to_string()),
        ])),
    )
    .await
    .unwrap();
    let bg = Arc::new(Background::new());
    let sp = splitter(&s, &bg, tiny());
    let intent = StructuralIntent {
        collection: collection(),
        source_token: Some(test_token("L")),
        source_version: String::new(),
        created_tokens: vec![test_token("R")],
        split_key: b"t".to_vec(),
        participant_id: participant.clone(),
        phase: StructuralIntentPhase::Ready,
    };
    (s, bg, sp, intent)
}

#[tokio::test]
async fn sweep_defers_one_failed_parent_split_and_continues() {
    let participant = TxId::with_priority(1, b"pending-participant");
    let (s, bg, sp, request_record) = recovery_that_needs_a_parent_split(&participant).await;
    sp.mon.begin_tx(&participant);

    let mut orphan_intent = nonroot_intent("L", "U", b"z");
    orphan_intent.participant_id = participant.clone();
    s.store_node(COLL, "U", &leaf_node(&[b"z"], None, None), None)
        .await
        .unwrap();

    let mut intent_ids = [test_token("request-intent"), test_token("orphan-intent")];
    intent_ids.sort();
    s.intent_store
        .write(
            &db_root("db"),
            &StructuralIntentId::from(intent_ids[0].clone()),
            &request_record,
        )
        .await
        .unwrap();
    s.intent_store
        .write(
            &db_root("db"),
            &StructuralIntentId::from(intent_ids[1].clone()),
            &orphan_intent,
        )
        .await
        .unwrap();

    let mut action = sp.recovery.begin_sweep();
    assert!(matches!(
        sp.recovery.advance(&mut action).await.unwrap(),
        RecoveryStep::SplitParent { .. }
    ));
    action.resume_parent_split(Err(TransError::Retry));
    assert!(matches!(
        sp.recovery.advance(&mut action).await.unwrap(),
        RecoveryStep::Completed { active: true }
    ));

    assert!(matches!(
        s.load_node(COLL, "U", Requirement::AtLeast(s.timeline.now()))
            .await,
        Err(StorageError::NotFound)
    ));
    assert_eq!(
        s.list_structural_intents("db", Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap()
            .len(),
        1,
        "only the intent with the failed parent split stays for another sweep"
    );
    drop(bg);
}

#[tokio::test]
async fn explicit_settlement_returns_a_parent_split_error() {
    let participant = TxId::with_priority(1, b"final-participant");
    let (s, bg, sp, intent) = recovery_that_needs_a_parent_split(&participant).await;
    sp.mon.begin_tx(&participant);
    sp.mon.abort_owned_tx(&participant).await.unwrap();
    s.intent_store
        .write(
            &db_root("db"),
            &StructuralIntentId::from(test_token("request-intent")),
            &intent,
        )
        .await
        .unwrap();

    let mut action = sp
        .recovery
        .begin_participant_settlement(&collection(), &participant);
    assert!(matches!(
        sp.recovery.advance(&mut action).await.unwrap(),
        RecoveryStep::SplitParent { .. }
    ));
    action.resume_parent_split(Err(TransError::Retry));
    assert!(matches!(
        sp.recovery.advance(&mut action).await,
        Err(TransError::Retry)
    ));
    drop(bg);
}
