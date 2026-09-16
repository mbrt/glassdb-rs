use super::*;

#[derive(Clone, Copy)]
enum ParticipantCleanup {
    StaleRecord,
    CachedParticipant,
    OwnerDeparted,
    ReadFailure,
}

async fn recover_peer_participant(committed: bool, case: ParticipantCleanup) {
    let hooks = HookBackend::new(Arc::new(MemoryBackend::new()));
    let recorder = RecordingBackend::new(hooks.clone());
    let operations = recorder.log();
    let backend: Arc<dyn Backend> = Arc::new(recorder);
    let local = store_with_backend(backend.clone());
    let peer = store_with_backend(backend.clone());
    local
        .create_root(COLL, &leaf_node(&[b"a", b"b", b"c", b"d"], None, None))
        .await
        .unwrap();
    let local_bg = Arc::new(Background::new());
    let peer_bg = Arc::new(Background::new());
    let recovering = splitter(&local, &local_bg, tiny());
    let owner = splitter(&peer, &peer_bg, tiny());
    let participant = if committed {
        // The split has completed its tree change, but failed intent deletion
        // leaves both the Ready intent and participant for background recovery.
        hooks.set_before({
            let prefix = ObjectPath::structural_intents_prefix(&db_root("db"));
            move |op| {
                let fail =
                    matches!(op, BackendOp::DeleteIf { path, .. } if path.starts_with(&prefix));
                Box::pin(async move {
                    if fail {
                        Err(glassdb_backend::BackendError::other(
                            "intent cleanup failed",
                        ))
                    } else {
                        Ok(())
                    }
                })
            }
        });
        assert!(owner.split_path(&root_path()).await.is_err());
        hooks.clear_before();
        let intents = peer
            .discover_structural_intents("db", Requirement::ANY)
            .await
            .unwrap();
        assert_eq!(intents.len(), 1);
        let intent = intents[0].1.value().unwrap();
        assert_eq!(intent.phase, StructuralIntentPhase::Ready);
        let id = intent.participant_id.clone();
        assert_eq!(owner.mon.tx_status(&id).await.unwrap(), TxCommitStatus::Ok);
        id
    } else {
        let id = TxId::with_priority(1, b"peer-participant");
        owner.begin_topology_tx(&collection(), &id).await.unwrap();
        owner
            .recovery
            .prepare_intent(&collection(), None, &id)
            .await
            .unwrap();
        owner.join_topology(&collection(), &id).await.unwrap();
        assert_eq!(
            owner.mon.abort_owned_tx(&id).await.unwrap(),
            crate::monitor::OwnerAbortOutcome::Acknowledged
        );
        id
    };
    let record_path = ObjectPath::CollectionRecord {
        collection: collection(),
    }
    .to_string();
    if matches!(case, ParticipantCleanup::CachedParticipant) {
        local
            .records
            .load_record(
                &collection(),
                Requirement::after(local.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
    }
    if matches!(case, ParticipantCleanup::OwnerDeparted) {
        // A peer can complete departure after this sweep deletes the intent
        // and before it checks the collection record.
        hooks.set_before({
            let owner = owner.clone();
            let participant = participant.clone();
            let prefix =
                ObjectPath::participant_structural_intents_prefix(&db_root("db"), &participant);
            move |op| {
                let depart = matches!(op, BackendOp::List { .. }) && op.path() == prefix;
                let owner = owner.clone();
                let participant = participant.clone();
                Box::pin(async move {
                    if depart {
                        owner
                            .leave_topology(&collection(), &participant)
                            .await
                            .map_err(|error| {
                                glassdb_backend::BackendError::with_source("owner departure", error)
                            })?;
                    }
                    Ok(())
                })
            }
        });
    }
    if matches!(case, ParticipantCleanup::ReadFailure) {
        hooks.set_before({
            let path = record_path.clone();
            move |op| {
                let fail = op.path() == path
                    && matches!(
                        op,
                        BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                    );
                Box::pin(async move {
                    if fail {
                        Err(glassdb_backend::BackendError::other(
                            "participant check failed",
                        ))
                    } else {
                        Ok(())
                    }
                })
            }
        });
    }
    operations.lock().unwrap().clear();
    let result = recovering.recover_structural_intents().await;
    let recorded = std::mem::take(&mut *operations.lock().unwrap());
    hooks.clear_before();
    let verifier = store_with_backend(backend);
    assert!(
        verifier
            .discover_structural_intents("db", Requirement::ANY)
            .await
            .unwrap()
            .is_empty()
    );
    let (record, _) = verifier
        .records
        .load_record(&collection(), Requirement::ANY)
        .await
        .unwrap();
    if matches!(case, ParticipantCleanup::ReadFailure) {
        assert!(
            result.is_err(),
            "a failed departure check cannot report a completed sweep"
        );
        assert!(record.topology_participants().any(|id| id == &participant));
        // The transaction object remains available to GC even though intent
        // discovery can no longer find this participant on its next sweep.
        let log = verifier
            .foundation
            .tlogger
            .get_at(&participant, Requirement::ANY)
            .await
            .unwrap();
        assert!(log.value().unwrap().locks.iter().any(|lock| {
            matches!(lock, TxLock::Topology { collection: target } if target == &collection())
        }));
        return;
    }
    assert!(result.unwrap());
    assert!(
        !record.topology_participants().any(|id| id == &participant),
        "structural recovery deleted the intent but left its participant admitted"
    );
    let record_calls: Vec<_> = recorded
        .iter()
        .filter(|op| op.path == record_path)
        .map(|op| op.op)
        .collect();
    let expected: &[&str] = match case {
        ParticipantCleanup::StaleRecord => &["read_if_modified", "write_if"],
        ParticipantCleanup::CachedParticipant => &["write_if"],
        ParticipantCleanup::OwnerDeparted => &["write_if", "read_if_modified"],
        ParticipantCleanup::ReadFailure => unreachable!(),
    };
    assert_eq!(record_calls, expected);
    operations.lock().unwrap().clear();
    assert!(!recovering.recover_structural_intents().await.unwrap());
    assert!(operations.lock().unwrap().iter().all(|op| op.op == "list"));
}

#[tokio::test]
async fn background_recovery_removes_participants_from_stale_records() {
    for committed in [false, true] {
        recover_peer_participant(committed, ParticipantCleanup::StaleRecord).await;
    }
}

#[tokio::test]
async fn background_recovery_removes_cached_participants_without_a_read() {
    for committed in [false, true] {
        recover_peer_participant(committed, ParticipantCleanup::CachedParticipant).await;
    }
}

#[tokio::test]
async fn background_recovery_checks_departure_after_another_instance_removes_the_participant() {
    for committed in [false, true] {
        recover_peer_participant(committed, ParticipantCleanup::OwnerDeparted).await;
    }
}

#[tokio::test]
async fn background_recovery_reports_failed_departure_checks() {
    for committed in [false, true] {
        recover_peer_participant(committed, ParticipantCleanup::ReadFailure).await;
    }
}

#[tokio::test]
async fn recovery_retries_a_cached_preparing_intent_after_the_peer_publishes_ready() {
    let memory = Arc::new(MemoryBackend::new());
    let recorder = Arc::new(RecordingBackend::new(memory.clone()));
    let operations = recorder.log();
    let local = store_with_backend(recorder);
    let hooks = HookBackend::new(memory.clone());
    let peer = store_with_backend(hooks.clone());
    peer.create_root(COLL, &leaf_node(&[b"a", b"b", b"c", b"d"], None, None))
        .await
        .unwrap();
    let local_bg = Arc::new(Background::new());
    let peer_bg = Arc::new(Background::new());
    let recovering = splitter(&local, &local_bg, tiny());
    let owner = splitter(&peer, &peer_bg, tiny());
    let prefix = ObjectPath::structural_intents_prefix(&db_root("db"));
    // The recovering instance discovers Preparing before the owner advances
    // it. Failed owner cleanup then leaves the completed split for recovery.
    hooks.set_after({
        let local = local.clone();
        let prefix = prefix.clone();
        move |op, outcome| {
            let discover = outcome.is_success()
                && matches!(op, BackendOp::WriteIfNotExists { .. })
                && op.path().starts_with(&prefix);
            let local = local.clone();
            Box::pin(async move {
                if discover {
                    let found = local
                        .discover_structural_intents("db", Requirement::ANY)
                        .await
                        .unwrap();
                    assert_eq!(found.len(), 1);
                    assert_eq!(
                        found[0].1.value().unwrap().phase,
                        StructuralIntentPhase::Preparing
                    );
                }
                Ok(())
            })
        }
    });
    hooks.set_before({
        let prefix = prefix.clone();
        move |op| {
            let fail = matches!(op, BackendOp::DeleteIf { .. }) && op.path().starts_with(&prefix);
            Box::pin(async move {
                if fail {
                    Err(glassdb_backend::BackendError::other(
                        "intent cleanup failed",
                    ))
                } else {
                    Ok(())
                }
            })
        }
    });
    assert!(owner.split_path(&root_path()).await.is_err());
    hooks.clear_before();
    hooks.clear_after();
    operations.lock().unwrap().clear();

    assert!(recovering.recover_structural_intents().await.unwrap());
    let calls: Vec<_> = operations
        .lock()
        .unwrap()
        .iter()
        .filter(|op| op.path.starts_with(&prefix) && op.op != "list")
        .map(|op| op.op)
        .collect();
    assert_eq!(calls, ["delete_if", "read", "delete_if"]);
    let verifier = store_with_backend(memory);
    assert!(
        verifier
            .discover_structural_intents("db", Requirement::ANY)
            .await
            .unwrap()
            .is_empty()
    );
    let leaves = TreeRouter::new(verifier.nodes.clone(), std::num::NonZeroUsize::MIN)
        .leaves(&collection(), Requirement::ANY)
        .await
        .unwrap();
    let keys: Vec<_> = leaves
        .iter()
        .flat_map(|leaf| {
            leaf.node()
                .unwrap()
                .as_leaf()
                .unwrap()
                .entries()
                .map(|entry| entry.key.clone())
        })
        .collect();
    assert_eq!(keys, [b"a", b"b", b"c", b"d"]);
    let (record, _) = verifier
        .records
        .load_record(&collection(), Requirement::ANY)
        .await
        .unwrap();
    assert_eq!(record.topology_participants().count(), 0);
}

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
    assert!(
        operations.lock().unwrap().iter().all(|op| {
            !op.path.starts_with(&expected_listing) || !matches!(op.op, "read" | "read_if_modified")
        }),
        "a cached Preparing intent needs only revision-checked deletion"
    );
    let record_path = ObjectPath::CollectionRecord {
        collection: collection(),
    }
    .to_string();
    let record_calls: Vec<_> = operations
        .lock()
        .unwrap()
        .iter()
        .filter(|op| op.path == record_path)
        .map(|op| op.op)
        .collect();
    assert_eq!(record_calls, ["write_if"]);
    operations.lock().unwrap().clear();
    sp.settle_topology_participant(&collection(), &participant)
        .await
        .unwrap();
    assert!(
        operations
            .lock()
            .unwrap()
            .iter()
            .all(|op| op.path != record_path)
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
        s.list_nodes(COLL, Requirement::after(s.timeline.currentness_barrier()))
            .await
            .unwrap()
            .is_empty(),
        "a cancelled Preparing intent cannot create its reserved nodes"
    );
    let (root, _) = s
        .load_root(COLL, Requirement::after(s.timeline.currentness_barrier()))
        .await
        .unwrap();
    assert!(root.as_leaf().is_some());
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
            .discover_structural_intents("db", Requirement::after(s.timeline.currentness_barrier()))
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
            .load_record(
                &collection(),
                Requirement::after(s.timeline.currentness_barrier()),
            )
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
                .load_node(
                    COLL,
                    "R",
                    Requirement::after(second.timeline.currentness_barrier())
                )
                .await,
            Err(StorageError::NotFound)
        ) {
            break;
        }
        rt::yield_now().await;
    }

    assert!(matches!(
        second
            .load_node(
                COLL,
                "R",
                Requirement::after(second.timeline.currentness_barrier())
            )
            .await,
        Err(StorageError::NotFound)
    ));
    assert!(
        second
            .load_node(
                COLL,
                "L",
                Requirement::after(second.timeline.currentness_barrier())
            )
            .await
            .is_ok()
    );
    assert!(
        second
            .discover_structural_intents(
                "db",
                Requirement::after(second.timeline.currentness_barrier())
            )
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
    let mut intent = nonroot_intent("L", "R", b"m");
    intent.source_version = gated_revision(&s, "L").await;
    s.write_structural_intent("R", &intent).await.unwrap();

    assert!(!sp.recover_structural_intents().await.unwrap());
    assert!(
        s.load_node(
            COLL,
            "R",
            Requirement::after(s.timeline.currentness_barrier())
        )
        .await
        .is_ok()
    );
    assert_eq!(
        s.discover_structural_intents("db", Requirement::after(s.timeline.currentness_barrier()))
            .await
            .unwrap()
            .len(),
        1
    );

    sp.mon.abort_owned_tx(&id).await.unwrap();
    assert!(sp.recover_structural_intents().await.unwrap());
    assert!(matches!(
        s.load_node(
            COLL,
            "R",
            Requirement::after(s.timeline.currentness_barrier())
        )
        .await,
        Err(StorageError::NotFound)
    ));
    assert!(
        s.discover_structural_intents("db", Requirement::after(s.timeline.currentness_barrier()))
            .await
            .unwrap()
            .is_empty()
    );
}

/// Structural recovery must fence an in-flight non-root split against the
/// source as it stands, not against a snapshot it cached before the split took
/// the gate.
///
/// Here `s` (recovery) caches the pre-gate source, a peer sharing the backend
/// then takes the gate and creates the child, and recovery must defer instead
/// of reclaiming the child. A bound allocated before any of that is satisfied
/// by the stale entry, and recovery reclaims `R`.
#[tokio::test]
async fn recovery_reads_a_live_split_freshly_and_keeps_its_child() {
    let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let recorder = Arc::new(RecordingBackend::new(backend.clone()));
    let operations = recorder.log();
    let s = store_with_backend(recorder);
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
    s.load_node(
        COLL,
        "L",
        Requirement::after(s.timeline.currentness_barrier()),
    )
    .await
    .unwrap();

    // The in-flight split (peer, sharing the backend): take the source gate
    // and create the sibling. `s`'s cache is unaware of both writes.
    let (mut gated, version) = peer
        .load_node(
            COLL,
            "L",
            Requirement::after(peer.timeline.currentness_barrier()),
        )
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

    // The intent is written after the gate and records the gated revision, so
    // recovery can tell that the split's publish CAS can still land.
    let mut intent = nonroot_intent("L", "R", b"m");
    intent.source_version = gated_revision(&peer, "L").await;
    s.write_structural_intent("R", &intent).await.unwrap();

    operations.lock().unwrap().clear();
    assert!(
        !sp.recover_structural_intents().await.unwrap(),
        "recovery must defer to the live split rather than reclaim its child"
    );
    let recorded = std::mem::take(&mut *operations.lock().unwrap());
    let intent_prefix = ObjectPath::structural_intents_prefix(&db_root("db"));
    assert!(
        recorded.iter().all(|op| {
            !op.path.starts_with(&intent_prefix) || !matches!(op.op, "read" | "read_if_modified")
        }),
        "a cached Ready body needs no currentness read"
    );
    assert!(
        recorded
            .iter()
            .any(|op| op.path == node_path("L").to_string() && op.op == "read_if_modified"),
        "Ready classification must still check the source after discovery"
    );
    assert!(
        s.load_node(
            COLL,
            "R",
            Requirement::after(s.timeline.currentness_barrier())
        )
        .await
        .is_ok(),
        "the live split's child must survive recovery"
    );
    assert_eq!(
        s.discover_structural_intents("db", Requirement::after(s.timeline.currentness_barrier()))
            .await
            .unwrap()
            .len(),
        1,
        "the in-flight split's intent is left for a later sweep"
    );
}

// Stage a non-root split in durable order, leaving its publication to the test.
async fn stage_recovery_split(
    s: &TestStore,
    participant: &TxId,
    worker: &TxId,
    sibling: &str,
) -> (Node, LeafObservation) {
    let mut intent = nonroot_intent("L", sibling, b"");
    intent.participant_id = participant.clone();
    intent.phase = StructuralIntentPhase::Preparing;
    intent.source_version.clear();
    let prepared = s.write_structural_intent(sibling, &intent).await.unwrap();
    let (mut source, observed) = s.load_node(COLL, "L", Requirement::ANY).await.unwrap();
    source.set_structural_gate(worker.clone());
    assert!(
        s.store_node(COLL, "L", &source, Some(&observed))
            .await
            .unwrap()
    );
    let (mut source, gated) = s.load_node(COLL, "L", Requirement::ANY).await.unwrap();
    let (right, split_key) = source.split(sibling).unwrap();
    source.remove_structural_gate(worker);
    intent.source_version = gated.revision().unwrap().serialize().to_string();
    intent.split_key = split_key;
    intent.phase = StructuralIntentPhase::Ready;
    assert!(
        s.intent_store
            .update(&prepared, &canonical_intent(&intent))
            .await
            .unwrap()
            .is_some()
    );
    s.store_node(COLL, sibling, &right, None).await.unwrap();
    (source, gated)
}

async fn check_recovery_batch_reuses_source_reads(explicit: bool) {
    let memory = Arc::new(MemoryBackend::new());
    let recorder = Arc::new(RecordingBackend::new(memory.clone()));
    let operations = recorder.log();
    let s = store_with_backend(recorder);
    s.store_node(
        COLL,
        "L",
        &leaf_node(&[b"a", b"b", b"m", b"n"], None, None),
        None,
    )
    .await
    .unwrap();
    s.create_root(
        COLL,
        &Node::index(IndexNode::from_children([(Vec::new(), "L".to_string())])),
    )
    .await
    .unwrap();
    let bg = Arc::new(Background::new());
    let sp = splitter(&s, &bg, tiny());
    let participant = TxId::with_priority(1, b"batch-participant");
    sp.begin_topology_tx(&collection(), &participant)
        .await
        .unwrap();
    sp.join_topology(&collection(), &participant).await.unwrap();

    // Two failed attempts leave different Ready intents for the same source.
    // Releasing each gate prevents its recorded publication CAS from landing.
    for sibling in ["R", "U"] {
        let worker = TxId::with_priority(1, sibling.as_bytes());
        stage_recovery_split(&s, &participant, &worker, sibling).await;
        let (mut source, observed) = s.load_node(COLL, "L", Requirement::ANY).await.unwrap();
        source.remove_structural_gate(&worker);
        assert!(
            s.store_node(COLL, "L", &source, Some(&observed))
                .await
                .unwrap()
        );
    }
    sp.mon.abort_owned_tx(&participant).await.unwrap();
    operations.lock().unwrap().clear();
    if explicit {
        sp.settle_topology_participant(&collection(), &participant)
            .await
            .unwrap();
    } else {
        assert!(sp.recover_structural_intents().await.unwrap());
    }
    let recorded = std::mem::take(&mut *operations.lock().unwrap());
    let intent_prefix = ObjectPath::structural_intents_prefix(&db_root("db"));
    assert!(
        recorded.iter().all(|op| {
            !op.path.starts_with(&intent_prefix) || !matches!(op.op, "read" | "read_if_modified")
        }),
        "advancing discovery bounds must not recheck present cached intent bodies"
    );
    for path in [node_path("L"), root_path()] {
        assert_eq!(
            recorded
                .iter()
                .filter(|op| {
                    op.path == path.to_string() && matches!(op.op, "read" | "read_if_modified")
                })
                .count(),
            1,
            "one discovery batch needs only one currentness check of {path}"
        );
    }
    let verifier = store_with_backend(memory);
    for sibling in ["R", "U"] {
        assert!(matches!(
            verifier.load_node(COLL, sibling, Requirement::ANY).await,
            Err(StorageError::NotFound)
        ));
    }
    let (source, _) = verifier
        .load_node(COLL, "L", Requirement::ANY)
        .await
        .unwrap();
    let keys: Vec<_> = source
        .as_leaf()
        .unwrap()
        .entries()
        .map(|entry| entry.key.clone())
        .collect();
    assert_eq!(keys, [b"a", b"b", b"m", b"n"]);
    assert!(
        verifier
            .discover_structural_intents("db", Requirement::ANY)
            .await
            .unwrap()
            .is_empty()
    );
    let (record, _) = verifier
        .records
        .load_record(&collection(), Requirement::ANY)
        .await
        .unwrap();
    assert_eq!(record.topology_participants().count(), 0);
}

#[tokio::test]
async fn recovery_sweep_reuses_source_reads_across_a_discovered_batch() {
    check_recovery_batch_reuses_source_reads(false).await;
}

#[tokio::test]
async fn participant_settlement_reuses_source_reads_across_a_discovered_batch() {
    check_recovery_batch_reuses_source_reads(true).await;
}

#[tokio::test]
async fn later_participant_discovery_checks_sources_after_its_own_ready_intents() {
    for explicit in [false, true] {
        let memory = Arc::new(MemoryBackend::new());
        let prefix = ObjectPath::structural_intents_prefix(&db_root("db"));
        let (backend, gate) = OpGate::wrap(
            memory.clone(),
            move |op| matches!(op, BackendOp::DeleteIf { path, .. } if path.starts_with(&prefix)),
        );
        let recorder = Arc::new(RecordingBackend::new(backend));
        let operations = recorder.log();
        let s = store_with_backend(recorder);
        let peer = store_with_backend(memory.clone());
        s.store_node(
            COLL,
            "L",
            &leaf_node(&[b"a", b"b", b"m", b"n"], None, None),
            None,
        )
        .await
        .unwrap();
        s.create_root(
            COLL,
            &Node::index(IndexNode::from_children([(Vec::new(), "L".to_string())])),
        )
        .await
        .unwrap();
        let bg = Arc::new(Background::new());
        let sp = splitter(&s, &bg, tiny());
        let participant = TxId::with_priority(1, b"later-discovery-participant");
        sp.begin_topology_tx(&collection(), &participant)
            .await
            .unwrap();
        sp.join_topology(&collection(), &participant).await.unwrap();
        let first_worker = TxId::with_priority(1, b"first-worker");
        stage_recovery_split(&s, &participant, &first_worker, "R").await;
        let (mut source, observed) = s.load_node(COLL, "L", Requirement::ANY).await.unwrap();
        source.remove_structural_gate(&first_worker);
        assert!(
            s.store_node(COLL, "L", &source, Some(&observed))
                .await
                .unwrap()
        );
        sp.mon.abort_owned_tx(&participant).await.unwrap();

        operations.lock().unwrap().clear();
        gate.arm();
        let recovering = {
            let sp = sp.clone();
            let participant = participant.clone();
            tokio::spawn(async move {
                if explicit {
                    sp.settle_topology_participant(&collection(), &participant)
                        .await
                        .unwrap();
                } else {
                    assert!(sp.recover_structural_intents().await.unwrap());
                }
            })
        };
        gate.wait_until_entered().await;
        // Recovery has checked the unsplit source for the first batch. A peer
        // can create more recovery work under this finalized participant, just
        // as recursive parent recovery can. Its new sibling is now reachable.
        let later_worker = TxId::with_priority(1, b"later-worker");
        let (published, expected) =
            stage_recovery_split(&peer, &participant, &later_worker, "U").await;
        assert!(
            peer.store_node(COLL, "L", &published, Some(&expected))
                .await
                .unwrap()
        );
        gate.release();
        recovering.await.unwrap();

        let prefix = ObjectPath::structural_intents_prefix(&db_root("db"));
        let body_reads: Vec<_> = operations
            .lock()
            .unwrap()
            .iter()
            .filter(|op| {
                op.path.starts_with(&prefix) && matches!(op.op, "read" | "read_if_modified")
            })
            .map(|op| op.op)
            .collect();
        assert_eq!(
            body_reads,
            ["read"],
            "rediscovery reads only the new intent body missing from this cache"
        );
        let verifier = store_with_backend(memory);
        let router = TreeRouter::new(verifier.nodes.clone(), std::num::NonZeroUsize::MIN);
        for key in [b"a".as_slice(), b"b", b"m", b"n"] {
            let leaf = router
                .route_key(&collection(), key, Requirement::ANY)
                .await
                .unwrap();
            assert!(leaf.node().unwrap().as_leaf().unwrap().exists(key));
        }
        assert!(
            verifier
                .load_node(COLL, "U", Requirement::ANY)
                .await
                .is_ok()
        );
        assert!(
            verifier
                .discover_structural_intents("db", Requirement::ANY)
                .await
                .unwrap()
                .is_empty()
        );
        let (record, _) = verifier
            .records
            .load_record(&collection(), Requirement::ANY)
            .await
            .unwrap();
        assert_eq!(record.topology_participants().count(), 0);
    }
}

/// An intent must be fenced against the revision its own worker recorded, not
/// against whatever holds the source gate now. A later split gating the same
/// source cannot keep an abandoned intent's orphan alive, because the abandoned
/// worker's publish CAS died the moment the source moved past that revision.
#[tokio::test]
async fn recovery_reclaims_an_orphan_whose_source_a_later_split_now_gates() {
    let s = store();
    let bg = Arc::new(Background::new());
    let sp = splitter(&s, &bg, tiny());
    let abandoned = TxId::with_priority(1, b"abandoned-split");
    let newcomer = TxId::with_priority(2, b"later-split");
    sp.mon.begin_tx(&newcomer);

    let mut source = leaf_node(&[b"a", b"b"], None, None);
    source.set_structural_gate(abandoned.clone());
    s.store_node(COLL, "L", &source, None).await.unwrap();
    let mut intent = nonroot_intent("L", "R", b"m");
    intent.source_version = gated_revision(&s, "L").await;

    // The abandoned worker loses the source to a later split of the same node.
    let (mut source, version) = s
        .load_node(
            COLL,
            "L",
            Requirement::after(s.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    source.remove_structural_gate(&abandoned);
    source.set_structural_gate(newcomer.clone());
    assert!(
        s.store_node(COLL, "L", &source, Some(&version))
            .await
            .unwrap()
    );

    s.store_node(COLL, "R", &leaf_node(&[b"m", b"n"], None, None), None)
        .await
        .unwrap();
    let root = Node::index(IndexNode::from_children([(Vec::new(), "L".to_string())]));
    s.create_root(COLL, &root).await.unwrap();
    s.write_structural_intent("R", &intent).await.unwrap();

    assert!(
        sp.recover_structural_intents().await.unwrap(),
        "a later split's gate must not shield an abandoned intent from recovery"
    );
    assert!(matches!(
        s.load_node(
            COLL,
            "R",
            Requirement::after(s.timeline.currentness_barrier())
        )
        .await,
        Err(StorageError::NotFound)
    ));
    assert!(
        s.discover_structural_intents("db", Requirement::after(s.timeline.currentness_barrier()))
            .await
            .unwrap()
            .is_empty()
    );
}

/// Regression: recovery must classify a Ready intent under a currentness
/// barrier it allocates after observing that intent, not under the watermark
/// the observation carries.
///
/// A cache entry's watermark is allocated *before* the backend read that fills
/// it, so an entry read concurrently with a split can carry a watermark newer
/// than the intent's while holding source state from before the split gated it.
/// Recovery once classified at the intent's own watermark, accepted exactly
/// such an entry, read a revision the worker never published from, judged the
/// live root split unapplied, and deleted both children - which the splitter
/// then published a root index over, leaving the tree pointing at absent nodes.
///
/// The peer owns the live split and follows the worker's durable order:
/// Preparing intent, gate, Ready intent carrying the gated revision, children.
/// It takes the gate only once the paused sweep has allocated the watermark of
/// the read that discovers the intent, and `s` has cached the ungated root at a
/// newer one.
#[tokio::test]
async fn recovery_defers_to_a_live_root_split_over_a_newer_stale_source() {
    let intents_prefix = ObjectPath::structural_intents_prefix(&db_root("db"));
    let (backend, gate) = OpGate::wrap(
        Arc::new(MemoryBackend::new()),
        move |op| matches!(op, BackendOp::Read { path } if path.starts_with(&intents_prefix)),
    );
    let backend: Arc<dyn Backend> = backend;
    let s = store_with_backend(backend.clone());
    // The live splitter models a separately opened database, so it owns a
    // distinct cache and timeline over the shared backend.
    let peer = store_with_backend(backend);
    let bg = Arc::new(Background::new());
    let sp = splitter(&s, &bg, tiny());
    let worker = TxId::with_priority(1, b"inflight-root-split");
    let participant = TxId::with_priority(1, b"root-split-participant");
    sp.mon.begin_tx(&worker);
    sp.mon.begin_tx(&participant);

    peer.create_root(COLL, &leaf_node(&[b"a", b"b", b"m", b"n"], None, None))
        .await
        .unwrap();
    let mut intent = StructuralIntent {
        collection: collection(),
        source_token: None,
        source_version: String::new(),
        created_tokens: vec![test_token("L"), test_token("R")],
        split_key: b"m".to_vec(),
        participant_id: participant.clone(),
        phase: StructuralIntentPhase::Preparing,
    };
    let prepared = peer.write_structural_intent("R", &intent).await.unwrap();

    gate.arm();
    let recovering = {
        let sp = sp.clone();
        tokio::spawn(async move { sp.recover_structural_intents().await.unwrap() })
    };
    // The sweep is paused inside the read that discovers the intent, so that
    // read's watermark is already allocated while the root is still ungated.
    gate.wait_until_entered().await;
    s.load_root(COLL, Requirement::after(s.timeline.currentness_barrier()))
        .await
        .unwrap();

    let (mut root, version) = peer
        .load_root(
            COLL,
            Requirement::after(peer.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    root.set_structural_gate(worker.clone());
    assert!(peer.store_root(COLL, &root, &version).await.unwrap());
    let (_, gated) = peer
        .load_root_node(
            COLL,
            Requirement::after(peer.timeline.currentness_barrier()),
        )
        .await
        .unwrap()
        .unwrap();
    intent.source_version = gated.revision().unwrap().serialize().to_string();
    intent.phase = StructuralIntentPhase::Ready;
    assert!(
        peer.intent_store
            .update(&prepared, &canonical_intent(&intent))
            .await
            .unwrap()
            .is_some()
    );
    peer.store_node(
        COLL,
        "L",
        &leaf_node(&[b"a", b"b"], Some(b"m"), Some("R")),
        None,
    )
    .await
    .unwrap();
    peer.store_node(COLL, "R", &leaf_node(&[b"m", b"n"], None, None), None)
        .await
        .unwrap();
    gate.release();

    assert!(
        !recovering.await.unwrap(),
        "recovery must defer to the live root split rather than reclaim its children"
    );
    for token in ["L", "R"] {
        assert!(
            s.load_node(
                COLL,
                token,
                Requirement::after(s.timeline.currentness_barrier())
            )
            .await
            .is_ok(),
            "the live root split's child {token} must survive recovery"
        );
    }
    assert_eq!(
        s.discover_structural_intents("db", Requirement::after(s.timeline.currentness_barrier()))
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
        source_version: superseded_source_version(),
        created_tokens: vec![test_token("R")],
        split_key: b"t".to_vec(),
        participant_id: TxId::from_bytes(b"structural-participant".to_vec()),
        phase: StructuralIntentPhase::Ready,
    };
    s.write_structural_intent("R", &intent).await.unwrap();

    assert!(sp.recover_structural_intents().await.unwrap());

    let (root_node, _) = s
        .load_root_node(COLL, Requirement::after(s.timeline.currentness_barrier()))
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
            .leaves(
                &collection(),
                Requirement::after(s.timeline.currentness_barrier())
            )
            .await
            .unwrap()
            .len(),
        3,
        "the recovered separator keeps every leaf reachable after the parent split"
    );
    for key in [b"a".as_slice(), b"m", b"t"] {
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
    assert!(
        s.discover_structural_intents("db", Requirement::after(s.timeline.currentness_barrier()))
            .await
            .unwrap()
            .is_empty()
    );
}

/// Returns the revision a worker holding `token`'s structural gate would record
/// in its Ready intent.
async fn gated_revision(store: &TestStore, token: &str) -> String {
    let (_, observed) = store
        .load_node(
            COLL,
            token,
            Requirement::after(store.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    observed.revision().unwrap().serialize().to_string()
}

/// Pauses the first backend operation a test selects, so the test can
/// interleave concurrent work at exactly that point.
struct OpGate {
    armed: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl OpGate {
    fn wrap(
        inner: Arc<dyn Backend>,
        selects: impl Fn(&BackendOp<'_>) -> bool + Send + Sync + 'static,
    ) -> (Arc<HookBackend>, Arc<Self>) {
        let gate = Arc::new(Self {
            armed: std::sync::atomic::AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let backend = HookBackend::new(inner);
        backend.set_before({
            let gate = gate.clone();
            move |op| {
                let wait =
                    selects(op) && gate.armed.swap(false, std::sync::atomic::Ordering::SeqCst);
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
    let (backend, gate) = OpGate::wrap(
        Arc::new(MemoryBackend::new()),
        move |op| matches!(op, BackendOp::WriteIf { path, .. } if path == &source_path),
    );
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
        .load_node(
            COLL,
            "L",
            Requirement::after(s.timeline.currentness_barrier()),
        )
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
        tokio::spawn(async move { sp.recover_structural_intents().await.unwrap() })
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
        s.load_node(
            COLL,
            "R",
            Requirement::after(s.timeline.currentness_barrier())
        )
        .await
        .is_ok()
    );
    let (root_node, _) = s
        .load_root_node(COLL, Requirement::after(s.timeline.currentness_barrier()))
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
        source_version: superseded_source_version(),
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
        RecoveryStep::Completed { active: true, .. }
    ));

    assert!(matches!(
        s.load_node(
            COLL,
            "U",
            Requirement::after(s.timeline.currentness_barrier())
        )
        .await,
        Err(StorageError::NotFound)
    ));
    assert_eq!(
        s.discover_structural_intents("db", Requirement::after(s.timeline.currentness_barrier()))
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
