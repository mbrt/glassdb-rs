use super::*;
use crate::collection_coordination::CollectionStateResolver;
use crate::collections::TopologySettler;
use crate::engine::{AssemblyFixture, EngineConfig};
use crate::key_state_resolver::KeyStateResolver;
use crate::leaf_coord::{LeafCoordinator, SplitHinter};
use crate::monitor::Monitor;
use crate::tlocker::LockOutcome;
use async_trait::async_trait;
use glassdb_backend as backend;
use glassdb_backend::middleware::{BackendOp, HookBackend, HookFuture, OpLog, RecordingBackend};
use glassdb_backend::{Backend, BackendError, memory::MemoryBackend};
use glassdb_concurr::RetryConfig;
use glassdb_data::{CollectionAddress, CollectionId, DbPrefix, LogicalKey, NodeToken, ObjectPath};
use glassdb_storage::transaction::{
    TxCollectionChange, TxCollectionOp, TxCommitStatus, TxLock, TxRecord, TxRecordStore, TxWrite,
};
use glassdb_storage::{
    CollectionRecord, CollectionStore, CurrentState, IndexNode, LeafBody, LeafEntry, LockType,
    Node, Requirement, Timeline, TreeRouter,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

struct UnexpectedTopologySettler;

struct NoSplitHints;

impl SplitHinter for NoSplitHints {
    fn observe_leaf(&self, _path: &ObjectPath, _leaf: &LeafBody) {}

    fn capacity_rejected(&self, _path: &ObjectPath) {}
}

#[async_trait]
impl TopologySettler for UnexpectedTopologySettler {
    async fn settle_topology_participant(
        &self,
        _collection: &CollectionAddress,
        _id: &TxId,
    ) -> Result<(), TransError> {
        panic!("GC tests must not settle topology participants")
    }
}

fn collection() -> glassdb_data::CollectionAddress {
    glassdb_data::CollectionAddress::root("db")
}

fn root_path() -> ObjectPath {
    ObjectPath::TreeRoot {
        collection: collection(),
    }
}

fn base() -> SystemTime {
    rt::system_now()
}

// Comfortably past the 45s (timeout + skew) safety horizon.
const PAST_HORIZON: Duration = Duration::from_secs(120);

struct Ctx {
    gc: Gc,
    hints: GcHints,
    tx_records: TxRecordStore,
    records: CollectionStore,
    nodes: NodeStore,
    timeline: Timeline,
    coord: LeafCoordinator,
    locker: Locker,
    mon: Monitor,
}

#[tokio::test]
async fn gc_preserves_prepared_collections_until_wounded_owner_retires() {
    use crate::access::{AccessSet, WriteAccess};
    use crate::algo::BodyDecision;
    use crate::collections::{CatalogAccesses, CollectionChange, CollectionOp};
    use crate::engine::engine_fixture;
    use glassdb_backend::middleware::{BackendOp, HookBackend};
    use std::sync::atomic::{AtomicBool, Ordering};

    // Cover the preparation reply, key-lock CAS, and nested directory-lock CAS.
    for phase in 0..3 {
        let backend = Arc::new(MemoryBackend::new());
        let peer = new_ctx_with(backend.clone()).await;
        let hooked = HookBackend::new(backend);
        let config = EngineConfig::default();
        let db_prefix = DbPrefix::try_from("db").unwrap();
        let owner = AssemblyFixture::new(hooked.clone(), db_prefix.clone(), &config);
        let engine = engine_fixture(&owner, db_prefix, config, false);
        let prepared = CollectionAddress::new(
            "db",
            glassdb_data::CollectionId::from_slice(&[7; 16]).unwrap(),
        );
        let mut changes = vec![CollectionChange {
            parent: collection(),
            name: b"new".to_vec(),
            collection: prepared.clone(),
            expected: None,
            op: CollectionOp::Create,
        }];
        if phase == 2 {
            changes.push(CollectionChange {
                parent: prepared.clone(),
                name: b"nested".to_vec(),
                collection: CollectionAddress::new(
                    "db",
                    glassdb_data::CollectionId::from_slice(&[8; 16]).unwrap(),
                ),
                expected: None,
                op: CollectionOp::Create,
            });
        }
        let mut handle = engine.algo.begin(
            AccessSet::new(
                Vec::new(),
                vec![WriteAccess::put(
                    LogicalKey::new(prepared.clone(), b"key"),
                    b"value".to_vec().into(),
                )],
                Vec::new(),
            ),
            CatalogAccesses {
                reads: Vec::new(),
                changes,
            },
        );
        let old_id = handle.id().clone();
        let triggered = Arc::new(AtomicBool::new(false));
        let pause_owner = {
            let triggered = triggered.clone();
            let old_id = old_id.clone();
            let path = if phase == 2 {
                ObjectPath::CollectionRecord {
                    collection: prepared.clone(),
                }
            } else {
                ObjectPath::TreeRoot {
                    collection: prepared.clone(),
                }
            }
            .to_string();
            let owner_monitor = owner.monitor.clone();
            move |operation: &BackendOp<'_>| -> HookFuture {
                let matches_kind = if phase == 0 {
                    matches!(operation, BackendOp::WriteIfNotExists { .. })
                } else {
                    matches!(operation, BackendOp::WriteIf { .. })
                };
                let pause = matches_kind
                    && operation.path() == path
                    && !triggered.swap(true, Ordering::SeqCst);
                let peer_monitor = peer.mon.clone();
                let gc = peer.gc.clone();
                let old_id = old_id.clone();
                let owner_monitor = owner_monitor.clone();
                let records = peer.records.clone();
                let timeline = peer.timeline.clone();
                let prepared = prepared.clone();
                Box::pin(async move {
                    if pause {
                        peer_monitor.preempt_tx(&old_id).await.unwrap();

                        assert_eq!(
                            check_candidate(&gc, &old_id).await.unwrap(),
                            GcOutcome::Retained
                        );
                        records
                            .load_record(
                                &prepared,
                                Requirement::after(timeline.currentness_barrier()),
                            )
                            .await
                            .unwrap();
                        assert_eq!(
                            owner_monitor.tx_status(&old_id).await.unwrap(),
                            TxCommitStatus::Pending
                        );
                    }
                    Ok(())
                })
            }
        };
        if phase == 0 {
            // Preparation warms the root, so routing need not issue a read.
            // Pause its successful create reply to wound the owner while the
            // prepared collection exists but no key lock has been installed.
            hooked.set_after(move |operation, outcome| {
                if outcome.is_success() {
                    pause_owner(operation)
                } else {
                    Box::pin(async { Ok(()) })
                }
            });
        } else {
            hooked.set_before(pause_owner);
        }

        let decision = engine.algo.commit(&mut handle).await.unwrap();
        assert!(triggered.load(Ordering::SeqCst), "phase {phase} must run");
        assert_eq!(
            decision,
            BodyDecision::ReplayBody,
            "phase {phase} must replay after the wound"
        );
        assert_ne!(handle.id(), &old_id);
        assert_eq!(
            owner
                .tx_records
                .commit_status_at(&old_id, Requirement::ANY)
                .await
                .unwrap()
                .status,
            TxCommitStatus::Aborted
        );
        engine.algo.end(&mut handle).await.unwrap();
    }
}

async fn new_ctx() -> Ctx {
    new_ctx_with(Arc::new(MemoryBackend::new())).await
}

async fn new_ctx_with(backend: Arc<dyn Backend>) -> Ctx {
    new_ctx_with_interval(backend, Duration::from_secs(15)).await
}

async fn new_ctx_with_interval(backend: Arc<dyn Backend>, pending_timeout: Duration) -> Ctx {
    let mut config = EngineConfig::default();
    config.set_cache_size(1 << 20);
    config.set_protocol_timing(ProtocolTiming::new(
        pending_timeout,
        ProtocolTiming::default().max_clock_skew(),
    ));
    new_ctx_with_config(backend, &config).await
}

async fn new_ctx_with_config(backend: Arc<dyn Backend>, config: &EngineConfig) -> Ctx {
    let foundation = AssemblyFixture::new(backend, DbPrefix::try_from("db").unwrap(), config);
    let tx_records = foundation.tx_records.clone();
    let records = foundation.records.clone();
    let structural_intents = foundation.structural_intents.clone();
    let nodes = foundation.nodes.clone();
    let timeline = foundation.timeline.clone();
    assert!(
        records
            .create_record(&collection(), &CollectionRecord::new())
            .await
            .unwrap()
    );
    assert!(
        nodes
            .create_root(&collection(), &Node::leaf(LeafBody::new()))
            .await
            .unwrap()
    );
    let mon = foundation.monitor.clone();
    let key_state = KeyStateResolver::new(mon.clone());
    let coord = LeafCoordinator::with_hinter(
        nodes.clone(),
        key_state,
        mon.clone(),
        RetryConfig::default(),
        glassdb_storage::SplitPolicy::default(),
        Arc::new(NoSplitHints),
    );
    let router = TreeRouter::new(nodes.clone(), std::num::NonZeroUsize::MIN);
    let locker = Locker::new(
        coord.clone(),
        router,
        CollectionStateResolver::new(
            records.clone(),
            tx_records.clone(),
            timeline.clone(),
            mon.clone(),
            RetryConfig::default(),
        ),
        mon.clone(),
        RetryConfig::default(),
        std::num::NonZeroUsize::MIN,
    );
    let hints = GcHints::default();
    let gc = Gc::new(
        tx_records.clone(),
        nodes.clone(),
        structural_intents,
        timeline.clone(),
        locker.clone(),
        CollectionLifecycle::new(
            records.clone(),
            nodes.clone(),
            mon.clone(),
            RetryConfig::default(),
            Arc::new(UnexpectedTopologySettler),
        ),
        mon.protocol_timing(),
        hints.clone(),
        DEFAULT_GC_PARALLELISM,
    );
    Ctx {
        gc,
        hints,
        tx_records,
        records,
        nodes,
        timeline,
        coord,
        locker,
        mon,
    }
}

fn tx(n: u8) -> TxId {
    TxId::from_bytes(vec![n])
}

fn key_path(k: &[u8]) -> LogicalKey {
    LogicalKey::new(collection(), k)
}

fn write_lock(k: &[u8]) -> TxLock {
    TxLock::Key {
        key: key_path(k),
        typ: LockType::Write,
    }
}

async fn store_entry(ctx: &Ctx, _key: &[u8], entry: LeafEntry) {
    let path = root_path();
    let loaded = ctx
        .nodes
        .load_leaf(
            &path,
            Requirement::after(ctx.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    let mut entries: BTreeMap<Vec<u8>, LeafEntry> = loaded
        .entries()
        .entries()
        .cloned()
        .map(|e| (e.key.clone(), e))
        .collect();
    entries.insert(entry.key.clone(), entry);
    let leaf = LeafBody::from_entries(entries.into_values());
    let mut edit = loaded.into_edit();
    edit.set_entries(leaf);
    assert!(ctx.nodes.commit_leaf(edit).await.unwrap().is_applied());
}

async fn lookup_entry(ctx: &Ctx, key: &[u8]) -> Option<LeafEntry> {
    let loaded = ctx
        .nodes
        .load_leaf(
            &root_path(),
            Requirement::after(ctx.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    loaded.entries().lookup(key).cloned()
}

fn committed(id: TxId, offset: Duration, writes: &[&[u8]], locks: &[&[u8]]) -> TxRecord {
    TxRecord {
        id,
        timestamp: Some(base() - offset),
        status: TxCommitStatus::Committed,
        writes: writes
            .iter()
            .map(|k| TxWrite {
                key: key_path(k),
                value: Arc::from(&b"v"[..]),
                deleted: false,
                prev_writer: TxId::default(),
            })
            .collect(),
        locks: locks.iter().map(|k| write_lock(k)).collect(),
        collection_changes: Vec::new(),
        prepared_collections: Vec::new(),
    }
}

fn writer_entry(key: &[u8], writer: &TxId) -> LeafEntry {
    LeafEntry::new(key).with_current(CurrentState::External {
        writer: writer.clone(),
    })
}

fn locked_entry(key: &[u8], holder: &TxId) -> LeafEntry {
    let mut entry = LeafEntry::new(key);
    entry.replace_write_lock(holder.clone());
    entry
}

async fn is_gone(tx_records: &TxRecordStore, id: &TxId) -> bool {
    matches!(
        tx_records.get_at(id, Requirement::ANY).await,
        Err(StorageError::NotFound)
    )
}

async fn check_candidate(gc: &Gc, tid: &TxId) -> Result<GcOutcome, TransError> {
    let status = gc
        .filter_candidate(tid, gc.timeline.currentness_barrier())
        .await?;
    let barrier = gc.timeline.currentness_barrier();
    match status {
        GcEligibility::Ready(observation) => gc.try_reclaim(tid, &observation, barrier).await,
        GcEligibility::Deferred(delay) => Ok(GcOutcome::Deferred(delay)),
        GcEligibility::Retained => Ok(GcOutcome::Retained),
    }
}

async fn wait_for_hint_deadline(ctx: &Ctx) {
    for _ in 0..64 {
        rt::yield_now().await;
    }
    tokio::time::advance(ctx.gc.timing.pending_timeout() + ctx.gc.timing.max_clock_skew()).await;
    for _ in 0..64 {
        rt::yield_now().await;
    }
}

async fn check_hints_and_scan_page(ctx: &Ctx) {
    let mut candidates = Vec::new();
    loop {
        let (batch, more) = ctx.hints.drain();
        candidates.extend(batch);
        if !more {
            break;
        }
    }
    let page = ctx
        .tx_records
        .scan_transaction_ids(0, 0, None, backend::ListLimit::new(1000).unwrap())
        .await
        .unwrap();
    candidates.extend(page.ids);
    let candidates: BTreeSet<_> = candidates.into_iter().collect();
    for tid in candidates {
        let _ = check_candidate(&ctx.gc, &tid).await;
    }
}

// A committed record whose written key has since been overwritten by a newer
// writer holds no reference and is swept. Fed via the paged list (no hint),
// so the list feed is exercised too.
#[tokio::test(start_paused = true)]
async fn committed_unreferenced_is_collected() {
    let ctx = new_ctx().await;
    let (old, new) = (tx(1), tx(2));
    ctx.tx_records
        .set(&committed(old.clone(), PAST_HORIZON, &[b"k"], &[b"k"]))
        .await
        .unwrap();
    // The key now points at a newer writer, not `old`.
    store_entry(&ctx, b"k", writer_entry(b"k", &new)).await;

    check_hints_and_scan_page(&ctx).await;

    assert!(is_gone(&ctx.tx_records, &old).await);
    assert_eq!(
        ctx.coord.stats_and_reset().submissions,
        0,
        "an unreferenced committed record needs no key-lock release"
    );
}

// ADR-051: the successor that superseded a committed writer may be a direct
// commit with no transaction record of its own. Liveness is decided by the
// entry's writer id, not by that successor's existence, so the predecessor is
// still swept.
#[tokio::test(start_paused = true)]
async fn committed_superseded_by_a_direct_writer_is_collected() {
    let ctx = new_ctx().await;
    let (old, direct) = (tx(1), tx(2));
    ctx.tx_records
        .set(&committed(old.clone(), PAST_HORIZON, &[b"k"], &[]))
        .await
        .unwrap();
    store_entry(
        &ctx,
        b"k",
        LeafEntry::new(b"k").with_current(CurrentState::Inline {
            writer: direct.clone(),
            value: Arc::from(&b"v2"[..]),
        }),
    )
    .await;

    check_hints_and_scan_page(&ctx).await;

    assert!(is_gone(&ctx.tx_records, &old).await);
    assert!(
        is_gone(&ctx.tx_records, &direct).await,
        "the direct-commit successor never had a record to collect"
    );
}

#[tokio::test(start_paused = true)]
async fn committed_retry_orphan_is_reclaimed_from_the_prepared_manifest() {
    let ctx = new_ctx().await;
    let id = tx(1);
    let prepared = CollectionAddress::new(
        "db",
        CollectionId::from_slice(&[7; 16]).expect("fixed ID has the required width"),
    );
    ctx.records
        .create_record(&prepared, &CollectionRecord::new())
        .await
        .unwrap();
    ctx.nodes
        .create_root(&prepared, &Node::leaf(LeafBody::new()))
        .await
        .unwrap();
    let mut record = committed(id.clone(), PAST_HORIZON, &[], &[]);
    record.prepared_collections.push(prepared.clone());
    ctx.tx_records.set(&record).await.unwrap();

    check_hints_and_scan_page(&ctx).await;

    assert!(is_gone(&ctx.tx_records, &id).await);
    assert!(matches!(
        ctx.nodes.load_root(&prepared, Requirement::ANY).await,
        Err(StorageError::NotFound)
    ));
}

#[tokio::test(start_paused = true)]
async fn aborted_retry_orphan_is_reclaimed_from_the_prepared_manifest() {
    let ctx = new_ctx().await;
    let id = tx(1);
    let prepared = CollectionAddress::new(
        "db",
        CollectionId::from_slice(&[7; 16]).expect("fixed ID has the required width"),
    );
    ctx.records
        .create_record(&prepared, &CollectionRecord::new())
        .await
        .unwrap();
    ctx.nodes
        .create_root(&prepared, &Node::leaf(LeafBody::new()))
        .await
        .unwrap();
    let mut record = committed(id.clone(), PAST_HORIZON, &[], &[]);
    record.status = TxCommitStatus::Aborted;
    record.prepared_collections.push(prepared.clone());
    ctx.tx_records.set(&record).await.unwrap();
    ctx.hints.schedule(id.clone());

    check_hints_and_scan_page(&ctx).await;

    assert!(is_gone(&ctx.tx_records, &id).await);
    assert!(matches!(
        ctx.nodes.load_root(&prepared, Requirement::ANY).await,
        Err(StorageError::NotFound)
    ));
    assert!(matches!(
        ctx.records.load_record(&prepared, Requirement::ANY).await,
        Err(StorageError::NotFound)
    ));
}

#[tokio::test(start_paused = true)]
async fn rejected_collection_reclamation_keeps_the_recovery_manifest() {
    let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
    let ctx = new_ctx_with(backend.clone()).await;
    let id = tx(1);
    let prepared = CollectionAddress::new(
        "db",
        CollectionId::from_slice(&[7; 16]).expect("fixed ID has the required width"),
    );
    ctx.records
        .create_record(&prepared, &CollectionRecord::new())
        .await
        .unwrap();
    ctx.nodes
        .create_root(&prepared, &Node::leaf(LeafBody::new()))
        .await
        .unwrap();
    let mut record = committed(id.clone(), PAST_HORIZON, &[], &[]);
    record.prepared_collections.push(prepared.clone());
    ctx.tx_records.set(&record).await.unwrap();

    let root_path = ObjectPath::CollectionRecord {
        collection: prepared.clone(),
    }
    .to_string();
    let fail_once = Arc::new(AtomicBool::new(true));
    backend.set_before({
        let fail_once = fail_once.clone();
        move |operation| {
            let fail = matches!(
                operation,
                BackendOp::DeleteIf { path, .. }
                    if *path == root_path && fail_once.swap(false, Ordering::SeqCst)
            );
            let future: HookFuture = Box::pin(async move {
                if fail {
                    Err(BackendError::Precondition)
                } else {
                    Ok(())
                }
            });
            future
        }
    });
    ctx.hints.schedule(id.clone());

    check_hints_and_scan_page(&ctx).await;

    assert!(
        !is_gone(&ctx.tx_records, &id).await,
        "rejected reclamation CASes must retain the only durable orphan manifest"
    );
    assert!(
        ctx.records
            .load_record(&prepared, Requirement::ANY)
            .await
            .is_ok()
    );

    backend.clear_before();
    ctx.hints.schedule(id.clone());
    check_hints_and_scan_page(&ctx).await;
    assert!(is_gone(&ctx.tx_records, &id).await);
    assert!(matches!(
        ctx.nodes.load_root(&prepared, Requirement::ANY).await,
        Err(StorageError::NotFound)
    ));
}

#[tokio::test(start_paused = true)]
async fn committed_drop_is_recovered_while_the_record_stores_a_live_value() {
    let ctx = new_ctx().await;
    let id = tx(1);
    let child = CollectionAddress::new(
        "db",
        CollectionId::from_slice(&[8; 16]).expect("fixed ID has the required width"),
    );

    store_entry(&ctx, b"k", writer_entry(b"k", &id)).await;
    let (mut parent_record, parent_observed) = ctx
        .records
        .load_record(
            &collection(),
            Requirement::after(ctx.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert!(
        parent_record
            .add_child(b"child".to_vec(), child.id())
            .unwrap()
    );
    parent_record.set_directory_writer(id.clone());
    assert!(
        ctx.records
            .store_record(&parent_record, &parent_observed)
            .await
            .unwrap()
    );

    let mut child_record = CollectionRecord::new();
    child_record.add_directory_reader(id.clone());
    assert!(child_record.set_topology_freeze(id.clone()));
    assert!(
        ctx.records
            .create_record(&child, &child_record)
            .await
            .unwrap()
    );
    let mut child_node = Node::leaf(LeafBody::new());
    child_node.set_drop_intent(id.clone());
    assert!(ctx.nodes.create_root(&child, &child_node).await.unwrap());

    let mut record = committed(id.clone(), PAST_HORIZON, &[b"k"], &[]);
    record.locks.extend([
        TxLock::Directory {
            collection: collection(),
            typ: LockType::Write,
        },
        TxLock::Directory {
            collection: child.clone(),
            typ: LockType::Read,
        },
    ]);
    record.collection_changes.push(TxCollectionChange {
        parent: collection(),
        name: b"child".to_vec(),
        collection: child.clone(),
        op: TxCollectionOp::Drop,
    });
    ctx.tx_records.set(&record).await.unwrap();
    ctx.hints.schedule(id.clone());

    check_hints_and_scan_page(&ctx).await;

    assert!(
        !is_gone(&ctx.tx_records, &id).await,
        "the live root value still needs its transaction record"
    );
    let (parent_record, _) = ctx
        .records
        .load_record(
            &collection(),
            Requirement::after(ctx.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert_eq!(parent_record.child(b"child"), None);
    assert!(!parent_record.directory_lock().contains(&id));
    assert!(matches!(
        ctx.nodes.load_root(&child, Requirement::ANY).await,
        Err(StorageError::NotFound)
    ));
}

#[tokio::test(start_paused = true)]
async fn committed_references_in_a_reclaimed_collection_are_absent() {
    let ctx = new_ctx().await;
    let id = tx(1);
    let missing = CollectionAddress::new(
        "db",
        CollectionId::from_slice(&[9; 16]).expect("fixed ID has the required width"),
    );
    let key = LogicalKey::new(missing, b"k");
    let mut record = TxRecord::new(id.clone(), TxCommitStatus::Committed);
    record.timestamp = Some(base() - PAST_HORIZON);
    record.writes.push(TxWrite {
        key: key.clone(),
        value: Arc::from(&b"v"[..]),
        deleted: false,
        prev_writer: TxId::default(),
    });
    record.locks.push(TxLock::Key {
        key,
        typ: LockType::Write,
    });
    ctx.tx_records.set(&record).await.unwrap();
    ctx.hints.schedule(id.clone());

    check_hints_and_scan_page(&ctx).await;

    assert!(is_gone(&ctx.tx_records, &id).await);
}

// A committed record still named by its key's `current_writer` is the live
// value: it must never be collected.
#[tokio::test(start_paused = true)]
async fn committed_still_referenced_is_kept() {
    let ctx = new_ctx().await;
    let t = tx(1);
    ctx.tx_records
        .set(&committed(t.clone(), PAST_HORIZON, &[b"k"], &[]))
        .await
        .unwrap();
    store_entry(&ctx, b"k", writer_entry(b"k", &t)).await;
    ctx.hints.schedule(t.clone());

    check_hints_and_scan_page(&ctx).await;

    let record = ctx.tx_records.get_at(&t, Requirement::ANY).await.unwrap();
    let record = record.value().unwrap();
    assert_eq!(record.status, TxCommitStatus::Committed);
}

async fn replace_root(nodes: &NodeStore, node: &Node) {
    let (_, observed) = nodes
        .load_root(&collection(), Requirement::ANY)
        .await
        .unwrap();
    assert!(
        nodes
            .store_root(&collection(), node, &observed)
            .await
            .unwrap()
    );
}

async fn replace_node(nodes: &NodeStore, token: &NodeToken, node: &Node) {
    let observed = nodes
        .load_node_state(&collection(), token, Requirement::ANY)
        .await
        .unwrap();
    assert!(
        nodes
            .store_node(&collection(), token, node, Some(&observed))
            .await
            .unwrap()
    );
}

fn node_path(token: &NodeToken) -> ObjectPath {
    ObjectPath::Node {
        collection: collection(),
        token: token.clone(),
    }
}

fn tree_reads(operations: &OpLog) -> Vec<(&'static str, String)> {
    operations
        .lock()
        .unwrap()
        .iter()
        .filter(|op| {
            matches!(op.op, "read" | "read_if_modified")
                && matches!(
                    ObjectPath::try_from(op.path.as_str()),
                    Ok(ObjectPath::TreeRoot { .. } | ObjectPath::Node { .. })
                )
        })
        .map(|op| (op.op, op.path.clone()))
        .collect()
}

#[tokio::test]
async fn reference_checks_refresh_leaves_without_refreshing_indexes() {
    // Writer-only records use direct descent; the duplicate write/lock key
    // exercises batched descent and must still require only one leaf read.
    for holder in [false, true] {
        let memory = Arc::new(MemoryBackend::new());
        let recorder = Arc::new(RecordingBackend::new(memory.clone()));
        let operations = recorder.log();
        let ctx = new_ctx_with(recorder).await;
        let leaf = NodeToken::from_bytes([1; 16]);
        let index = NodeToken::from_bytes([2; 16]);
        assert!(
            ctx.nodes
                .store_node(&collection(), &leaf, &Node::leaf(LeafBody::new()), None)
                .await
                .unwrap()
        );
        assert!(
            ctx.nodes
                .store_node(
                    &collection(),
                    &index,
                    &Node::index(IndexNode::from_children([(Vec::new(), leaf.to_string())])),
                    None
                )
                .await
                .unwrap()
        );
        replace_root(
            &ctx.nodes,
            &Node::index(IndexNode::from_children([(Vec::new(), index.to_string())])),
        )
        .await;

        // The GC cache predates the reference. An independent instance
        // publishes it, so skipping the terminal-leaf check would lose the record.
        let peer = AssemblyFixture::new(
            memory,
            DbPrefix::try_from("db").unwrap(),
            &EngineConfig::default(),
        );
        let id = tx(1);
        ctx.tx_records
            .set(&committed(
                id.clone(),
                PAST_HORIZON,
                &[b"key"],
                if holder { &[b"key"] } else { &[] },
            ))
            .await
            .unwrap();
        let reference = if holder {
            locked_entry(b"key", &id)
        } else {
            writer_entry(b"key", &id)
        };
        replace_node(
            &peer.nodes,
            &leaf,
            &Node::leaf(LeafBody::from_entries([reference])),
        )
        .await;
        operations.lock().unwrap().clear();

        let outcomes = ctx
            .gc
            .clone()
            .check_batch(vec![id.clone()], NonZeroUsize::MIN)
            .await;
        assert_eq!(outcomes[0].1.as_ref().unwrap(), &GcOutcome::Retained);
        assert!(!is_gone(&peer.tx_records, &id).await);
        assert_eq!(
            tree_reads(&operations),
            [("read_if_modified", node_path(&leaf).to_string())]
        );

        // A later overwrite removes the last reference. GC must refresh the
        // same leaf again before deleting the candidate's exact record revision.
        let replacement = LeafEntry::new(b"key").with_current(CurrentState::Inline {
            writer: tx(2),
            value: Arc::from(b"new-value".as_slice()),
        });
        replace_node(
            &peer.nodes,
            &leaf,
            &Node::leaf(LeafBody::from_entries([replacement])),
        )
        .await;
        operations.lock().unwrap().clear();
        let outcomes = ctx
            .gc
            .clone()
            .check_batch(vec![id.clone()], NonZeroUsize::MIN)
            .await;
        assert_eq!(outcomes[0].1.as_ref().unwrap(), &GcOutcome::Reclaimed);
        assert!(is_gone(&ctx.tx_records, &id).await);
        assert_eq!(
            tree_reads(&operations),
            [("read_if_modified", node_path(&leaf).to_string())]
        );
    }
}

#[tokio::test]
async fn reference_checks_follow_a_split_behind_a_cached_parent() {
    for publish_separator in [false, true] {
        let memory = Arc::new(MemoryBackend::new());
        let recorder = Arc::new(RecordingBackend::new(memory.clone()));
        let operations = recorder.log();
        let ctx = new_ctx_with(recorder).await;
        let left = NodeToken::from_bytes([1; 16]);
        let right = NodeToken::from_bytes([2; 16]);
        assert!(
            ctx.nodes
                .store_node(&collection(), &left, &Node::leaf(LeafBody::new()), None)
                .await
                .unwrap()
        );
        replace_root(
            &ctx.nodes,
            &Node::index(IndexNode::from_children([(Vec::new(), left.to_string())])),
        )
        .await;
        let id = tx(1);
        ctx.tx_records
            .set(&committed(id.clone(), PAST_HORIZON, &[b"pear"], &[b"pear"]))
            .await
            .unwrap();
        let peer = AssemblyFixture::new(
            memory,
            DbPrefix::try_from("db").unwrap(),
            &EngineConfig::default(),
        );
        let body = LeafBody::from_entries([writer_entry(b"pear", &id)]);
        replace_node(&peer.nodes, &left, &Node::leaf(body.clone())).await;

        // Publish the new sibling before shrinking the source. The GC cache
        // keeps the original parent and empty source across both split stages.
        assert!(
            peer.nodes
                .store_node(&collection(), &right, &Node::leaf(body), None)
                .await
                .unwrap()
        );
        replace_node(
            &peer.nodes,
            &left,
            &Node::leaf(LeafBody::new())
                .with_high_key(Some(b"m".to_vec()))
                .with_right_sibling(Some(right.to_string())),
        )
        .await;
        if publish_separator {
            replace_root(
                &peer.nodes,
                &Node::index(IndexNode::from_children([
                    (Vec::new(), left.to_string()),
                    (b"m".to_vec(), right.to_string()),
                ])),
            )
            .await;
        }
        operations.lock().unwrap().clear();

        let outcomes = ctx
            .gc
            .clone()
            .check_batch(vec![id.clone()], NonZeroUsize::MIN)
            .await;
        assert_eq!(outcomes[0].1.as_ref().unwrap(), &GcOutcome::Retained);
        assert!(!is_gone(&peer.tx_records, &id).await);
        assert_eq!(
            tree_reads(&operations),
            [
                ("read_if_modified", node_path(&left).to_string()),
                ("read", node_path(&right).to_string()),
            ]
        );
    }
}

#[tokio::test]
async fn reference_checks_refresh_a_cached_leaf_that_became_an_index() {
    let memory = Arc::new(MemoryBackend::new());
    let recorder = Arc::new(RecordingBackend::new(memory.clone()));
    let operations = recorder.log();
    let ctx = new_ctx_with(recorder).await;
    let peer = AssemblyFixture::new(
        memory,
        DbPrefix::try_from("db").unwrap(),
        &EngineConfig::default(),
    );
    let id = tx(1);
    ctx.tx_records
        .set(&committed(id.clone(), PAST_HORIZON, &[b"pear"], &[]))
        .await
        .unwrap();
    let left = NodeToken::from_bytes([1; 16]);
    let right = NodeToken::from_bytes([2; 16]);
    let body = LeafBody::from_entries([writer_entry(b"pear", &id)]);
    replace_root(&peer.nodes, &Node::leaf(body.clone())).await;
    assert!(
        peer.nodes
            .store_node(
                &collection(),
                &left,
                &Node::leaf(LeafBody::new())
                    .with_high_key(Some(b"m".to_vec()))
                    .with_right_sibling(Some(right.to_string())),
                None
            )
            .await
            .unwrap()
    );
    assert!(
        peer.nodes
            .store_node(&collection(), &right, &Node::leaf(body), None)
            .await
            .unwrap()
    );
    replace_root(
        &peer.nodes,
        &Node::index(IndexNode::from_children([
            (Vec::new(), left.to_string()),
            (b"m".to_vec(), right.to_string()),
        ])),
    )
    .await;
    operations.lock().unwrap().clear();

    let outcomes = ctx
        .gc
        .clone()
        .check_batch(vec![id.clone()], NonZeroUsize::MIN)
        .await;
    assert_eq!(outcomes[0].1.as_ref().unwrap(), &GcOutcome::Retained);
    assert!(!is_gone(&peer.tx_records, &id).await);
    assert_eq!(
        tree_reads(&operations),
        [
            ("read_if_modified", root_path().to_string()),
            ("read", node_path(&right).to_string()),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn committed_key_lock_keeps_the_record_and_lock() {
    let ctx = new_ctx().await;
    let id = tx(1);
    ctx.tx_records
        .set(&committed(id.clone(), PAST_HORIZON, &[b"k"], &[b"k"]))
        .await
        .unwrap();
    store_entry(&ctx, b"k", locked_entry(b"k", &id)).await;

    check_hints_and_scan_page(&ctx).await;

    assert!(!is_gone(&ctx.tx_records, &id).await);
    assert_eq!(
        lookup_entry(&ctx, b"k").await.unwrap().lock_holders(),
        std::slice::from_ref(&id)
    );
    assert_eq!(ctx.coord.stats_and_reset().submissions, 0);
}

#[tokio::test]
async fn aborted_entry_release_refreshes_leaves_without_refreshing_indexes() {
    use crate::access::{AccessSet, ScanRange, WriteAccess};
    use crate::engine::engine_fixture;
    use crate::key_resolver::KeyResolver;
    use crate::monitor::{OwnerAbortOutcome, TxRecoveryManifest};
    use glassdb_data::LeafRef;

    // One key uses direct descent; two keys use batched descent. The scan also
    // holds an empty leaf that entry routing cannot refresh.
    for keys in [
        &[b"apple".as_slice()][..],
        &[b"apple".as_slice(), b"banana"],
    ] {
        let memory = Arc::new(MemoryBackend::new());
        let hooks = HookBackend::new(memory.clone());
        let recorder = Arc::new(RecordingBackend::new(hooks.clone()));
        let operations = recorder.log();
        let ctx = new_ctx_with(recorder).await;
        let leaf = NodeToken::from_bytes([1; 16]);
        let empty = NodeToken::from_bytes([2; 16]);
        let index = NodeToken::from_bytes([3; 16]);
        let current = CurrentState::Inline {
            writer: tx(90),
            value: Arc::from(b"old-value".as_slice()),
        };
        let body = LeafBody::from_entries(
            keys.iter()
                .map(|key| LeafEntry::new(*key).with_current(current.clone())),
        );
        for (token, node) in [
            (&empty, Node::leaf(LeafBody::new())),
            (
                &leaf,
                Node::leaf(body)
                    .with_high_key(Some(b"m".to_vec()))
                    .with_right_sibling(Some(empty.to_string())),
            ),
            (
                &index,
                Node::index(IndexNode::from_children([
                    (Vec::new(), leaf.to_string()),
                    (b"m".to_vec(), empty.to_string()),
                ])),
            ),
        ] {
            assert!(
                ctx.nodes
                    .store_node(&collection(), token, &node, None)
                    .await
                    .unwrap()
            );
        }
        replace_root(
            &ctx.nodes,
            &Node::index(IndexNode::from_children([(Vec::new(), index.to_string())])),
        )
        .await;

        let owner = AssemblyFixture::new(
            memory,
            DbPrefix::try_from("db").unwrap(),
            &EngineConfig::default(),
        );
        let engine = engine_fixture(
            &owner,
            DbPrefix::try_from("db").unwrap(),
            EngineConfig::default(),
            false,
        );
        let id = tx(82);
        let mut locks: Vec<_> = keys.iter().map(|key| write_lock(key)).collect();
        for token in [&leaf, &empty] {
            locks.push(TxLock::Membership {
                leaf: LeafRef::node(collection(), token.clone()),
                typ: LockType::Read,
            });
        }
        owner
            .monitor
            .begin_persisted_tx(
                &id,
                TxRecoveryManifest {
                    locks: locks.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let operation = owner.monitor.begin_owner_operation(&id).unwrap();
        let resolver = KeyResolver::new(
            TreeRouter::new(owner.nodes.clone(), NonZeroUsize::MIN),
            KeyStateResolver::new(owner.monitor.clone()),
            NonZeroUsize::MIN,
        );
        let page = resolver
            .scan_keys(&collection(), &ScanRange::all(), &[], Some(&id), None)
            .await
            .unwrap();
        let accesses = AccessSet::new(
            Vec::new(),
            keys.iter()
                .map(|key| WriteAccess::put(key_path(key), Arc::from(b"discarded".as_slice())))
                .collect(),
            vec![page.into_access(collection(), ScanRange::all(), Vec::new())],
        );
        let LockOutcome::Locked(locked) = engine
            .locker
            .keys()
            .lock_at(
                &id,
                &accesses,
                true,
                Requirement::after(owner.timeline.currentness_barrier()),
            )
            .await
            .unwrap()
        else {
            panic!("entry and scan locks must be acquired");
        };
        assert_eq!(locked.locked_paths(), locks);
        operation.complete();
        assert_eq!(
            owner.monitor.abort_owned_tx(&id).await.unwrap(),
            OwnerAbortOutcome::Acknowledged
        );

        // Enter reclamation after eligibility, retaining GC's pre-acquisition
        // leaf cache. A failed GC check must preserve the recovery record.
        let observed = ctx.tx_records.get_at(&id, Requirement::ANY).await.unwrap();
        let barrier = ctx.timeline.currentness_barrier();
        hooks.set_before({
            let path = node_path(&leaf).to_string();
            move |op| {
                let fail = op.path() == path
                    && matches!(
                        op,
                        BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                    );
                Box::pin(async move {
                    if fail {
                        Err(BackendError::other("GC check failed"))
                    } else {
                        Ok(())
                    }
                })
            }
        });
        assert!(ctx.gc.try_reclaim(&id, &observed, barrier).await.is_err());
        assert!(!is_gone(&ctx.tx_records, &id).await);
        hooks.clear_before();
        // Use a new pass's bound: the failed attempt may have refreshed indexes.
        let barrier = ctx.timeline.currentness_barrier();
        operations.lock().unwrap().clear();
        assert_eq!(
            ctx.gc.try_reclaim(&id, &observed, barrier).await.unwrap(),
            GcOutcome::Reclaimed
        );
        assert!(is_gone(&ctx.tx_records, &id).await);
        assert_eq!(
            tree_reads(&operations),
            [
                ("read_if_modified", node_path(&leaf).to_string()),
                ("read_if_modified", node_path(&empty).to_string()),
            ]
        );
        assert_eq!(
            operations
                .lock()
                .unwrap()
                .iter()
                .filter(|op| op.op == "write_if")
                .count(),
            2
        );

        operations.lock().unwrap().clear();
        assert!(
            !ctx.locker
                .release(&id, &locks, Requirement::after(barrier))
                .await
                .unwrap()
        );
        assert!(operations.lock().unwrap().is_empty());
        for token in [&leaf, &empty] {
            let loaded = owner
                .nodes
                .load_leaf(
                    &node_path(token),
                    Requirement::after(owner.timeline.currentness_barrier()),
                )
                .await
                .unwrap();
            assert!(loaded.node().membership_lock().holders().is_empty());
            for entry in loaded.entries().entries() {
                assert!(entry.lock_holders().is_empty());
                assert_eq!(entry.current, current);
            }
            assert_eq!(
                loaded.entries().entries().count(),
                if token == &leaf { keys.len() } else { 0 }
            );
        }
    }
}

#[tokio::test]
async fn entry_release_follows_splits_without_refreshing_cached_indexes() {
    use crate::engine::engine_fixture;

    for (root_split, publish_separator) in [(false, false), (false, true), (true, true)] {
        let memory = Arc::new(MemoryBackend::new());
        let recorder = Arc::new(RecordingBackend::new(memory.clone()));
        let operations = recorder.log();
        let ctx = new_ctx_with(recorder).await;
        let left = NodeToken::from_bytes([1; 16]);
        let right = NodeToken::from_bytes([2; 16]);
        let id = tx(82);
        let current = CurrentState::Inline {
            writer: tx(90),
            value: Arc::from(b"old-value".as_slice()),
        };
        let held = Node::leaf(LeafBody::from_entries([
            LeafEntry::new(b"apple").with_current(current.clone()),
            locked_entry(b"pear", &id).with_current(current.clone()),
        ]));
        if root_split {
            replace_root(&ctx.nodes, &held).await;
        } else {
            assert!(
                ctx.nodes
                    .store_node(&collection(), &left, &held, None)
                    .await
                    .unwrap()
            );
            replace_root(
                &ctx.nodes,
                &Node::index(IndexNode::from_children([(Vec::new(), left.to_string())])),
            )
            .await;
        }
        let owner = AssemblyFixture::new(
            memory,
            DbPrefix::try_from("db").unwrap(),
            &EngineConfig::default(),
        );
        let engine = engine_fixture(
            &owner,
            DbPrefix::try_from("db").unwrap(),
            EngineConfig::default(),
            false,
        );
        let locks = vec![write_lock(b"pear")];
        let mut record = TxRecord::new(id.clone(), TxCommitStatus::Aborted);
        record.locks = locks.clone();
        owner.tx_records.set(&record).await.unwrap();
        // A split resolves holds before moving entries. Keep the recovering
        // instance's old held leaf while the peer removes the aborted hold.
        assert!(
            engine
                .locker
                .release(&id, &locks, Requirement::ANY)
                .await
                .unwrap()
        );
        let body = LeafBody::from_entries([LeafEntry::new(b"pear").with_current(current.clone())]);
        assert!(
            owner
                .nodes
                .store_node(&collection(), &right, &Node::leaf(body), None)
                .await
                .unwrap()
        );
        let left_node = Node::leaf(LeafBody::from_entries([
            LeafEntry::new(b"apple").with_current(current.clone())
        ]))
        .with_high_key(Some(b"pear".to_vec()))
        .with_right_sibling(Some(right.to_string()));
        if root_split {
            assert!(
                owner
                    .nodes
                    .store_node(&collection(), &left, &left_node, None)
                    .await
                    .unwrap()
            );
        } else {
            replace_node(&owner.nodes, &left, &left_node).await;
        }
        if publish_separator {
            replace_root(
                &owner.nodes,
                &Node::index(IndexNode::from_children([
                    (Vec::new(), left.to_string()),
                    (b"pear".to_vec(), right.to_string()),
                ])),
            )
            .await;
        }
        let barrier = ctx.timeline.currentness_barrier();
        operations.lock().unwrap().clear();
        assert!(
            !ctx.locker
                .release(&id, &locks, Requirement::after(barrier))
                .await
                .unwrap()
        );
        let source = if root_split {
            root_path()
        } else {
            node_path(&left)
        };
        assert_eq!(
            tree_reads(&operations),
            [
                ("read_if_modified", source.to_string()),
                ("read", node_path(&right).to_string()),
            ]
        );
        assert!(
            operations
                .lock()
                .unwrap()
                .iter()
                .all(|op| matches!(op.op, "read" | "read_if_modified"))
        );
        let right_leaf = owner
            .nodes
            .load_leaf(
                &node_path(&right),
                Requirement::after(owner.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        let entry = right_leaf.entries().lookup(b"pear").unwrap();
        assert!(entry.lock_holders().is_empty());
        assert_eq!(entry.current, current);
    }
}

#[tokio::test(start_paused = true)]
async fn committed_membership_lock_is_released_before_deletion() {
    let ctx = new_ctx().await;
    let id = tx(1);
    let mut record = committed(id.clone(), PAST_HORIZON, &[b"k"], &[b"k"]);
    record.locks.push(TxLock::Membership {
        leaf: glassdb_data::LeafRef::root(collection()),
        typ: LockType::Write,
    });
    ctx.tx_records.set(&record).await.unwrap();
    store_entry(&ctx, b"k", writer_entry(b"k", &tx(2))).await;
    let loaded = ctx
        .nodes
        .load_leaf(&root_path(), Requirement::ANY)
        .await
        .unwrap();
    let mut locks = loaded.locks().clone();
    locks.set_membership_writer(id.clone());
    let mut edit = loaded.into_edit();
    edit.set_locks(locks);
    assert!(ctx.nodes.commit_leaf(edit).await.unwrap().is_applied());

    check_hints_and_scan_page(&ctx).await;

    assert!(is_gone(&ctx.tx_records, &id).await);
    let loaded = ctx
        .nodes
        .load_leaf(
            &root_path(),
            Requirement::after(ctx.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert!(loaded.node().membership_lock().holders().is_empty());
    assert_eq!(ctx.coord.stats_and_reset().submissions, 1);
}

async fn reclaim_membership_only(committed: bool, cached_holder: bool) {
    use crate::access::{AccessSet, ScanRange};
    use crate::key_resolver::KeyResolver;
    use crate::monitor::{OwnerAbortOutcome, TxRecoveryManifest};

    let backend = Arc::new(MemoryBackend::new());
    let recorded = RecordingBackend::new(backend.clone());
    let operations = recorded.log();
    // Creating the root leaves GC with a cached leaf without any holders.
    let ctx = new_ctx_with(Arc::new(recorded)).await;
    let owner = AssemblyFixture::new(
        backend,
        DbPrefix::try_from("db").unwrap(),
        &EngineConfig::default(),
    );
    let coord = LeafCoordinator::with_hinter(
        owner.nodes.clone(),
        KeyStateResolver::new(owner.monitor.clone()),
        owner.monitor.clone(),
        RetryConfig::default(),
        glassdb_storage::SplitPolicy::default(),
        Arc::new(NoSplitHints),
    );
    let router = TreeRouter::new(owner.nodes.clone(), std::num::NonZeroUsize::MIN);
    let locker = Locker::new(
        coord,
        router.clone(),
        CollectionStateResolver::new(
            owner.records.clone(),
            owner.tx_records.clone(),
            owner.timeline.clone(),
            owner.monitor.clone(),
            RetryConfig::default(),
        ),
        owner.monitor.clone(),
        RetryConfig::default(),
        std::num::NonZeroUsize::MIN,
    );
    let id = tx(81);
    let locks = vec![TxLock::Membership {
        leaf: glassdb_data::LeafRef::root(collection()),
        typ: LockType::Read,
    }];
    owner
        .monitor
        .begin_persisted_tx(
            &id,
            TxRecoveryManifest {
                locks: locks.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let operation = owner.monitor.begin_owner_operation(&id).unwrap();
    let resolver = KeyResolver::new(
        router,
        KeyStateResolver::new(owner.monitor.clone()),
        std::num::NonZeroUsize::MIN,
    );
    let page = resolver
        .scan_keys(&collection(), &ScanRange::all(), &[], Some(&id), None)
        .await
        .unwrap();
    let accesses = AccessSet::new(
        Vec::new(),
        Vec::new(),
        vec![page.into_access(collection(), ScanRange::all(), Vec::new())],
    );
    let LockOutcome::Locked(locked) = locker
        .keys()
        .lock_at(
            &id,
            &accesses,
            true,
            Requirement::after(owner.timeline.currentness_barrier()),
        )
        .await
        .unwrap()
    else {
        panic!("the empty scan must acquire its membership reader");
    };
    assert_eq!(locked.locked_paths(), locks);
    operation.complete();
    if cached_holder {
        ctx.nodes
            .load_leaf(
                &root_path(),
                Requirement::after(ctx.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
    }
    if committed {
        let mut record = TxRecord::new(id.clone(), TxCommitStatus::Committed);
        record.locks = locks.clone();
        owner.monitor.commit_tx(record).await.unwrap();
    } else {
        assert_eq!(
            owner.monitor.abort_owned_tx(&id).await.unwrap(),
            OwnerAbortOutcome::Acknowledged
        );
    }

    // Exercise reclamation after eligibility. Passing the safety horizon
    // cannot repair either instance's cached leaf.
    let observed = ctx
        .tx_records
        .get_at(&id, Requirement::after(ctx.timeline.currentness_barrier()))
        .await
        .unwrap();
    let barrier = ctx.timeline.currentness_barrier();
    operations.lock().unwrap().clear();
    assert_eq!(
        ctx.gc.try_reclaim(&id, &observed, barrier).await.unwrap(),
        GcOutcome::Reclaimed
    );
    assert!(is_gone(&ctx.tx_records, &id).await);
    let recorded = std::mem::take(&mut *operations.lock().unwrap());
    ctx.coord.stats_and_reset();
    assert!(
        !ctx.locker
            .release(&id, &locks, Requirement::after(barrier))
            .await
            .unwrap()
    );
    assert_eq!(ctx.coord.stats_and_reset().submissions, 1);
    assert!(operations.lock().unwrap().is_empty());
    let leaf = owner
        .nodes
        .load_leaf(
            &root_path(),
            Requirement::after(owner.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert!(
        !leaf.node().membership_lock().contains(&id),
        "GC deleted the transaction record while its membership reader remained"
    );
    let path = root_path().to_string();
    let calls: Vec<_> = recorded
        .iter()
        .filter(|op| op.path == path)
        .map(|op| op.op)
        .collect();
    let expected = if cached_holder {
        vec!["write_if"]
    } else {
        vec!["read_if_modified", "write_if"]
    };
    assert_eq!(calls, expected);
}

#[tokio::test]
async fn committed_membership_only_gc_refreshes_a_cached_no_holder() {
    reclaim_membership_only(true, false).await;
}

#[tokio::test]
async fn aborted_membership_only_gc_refreshes_a_cached_no_holder() {
    reclaim_membership_only(false, false).await;
}

#[tokio::test]
async fn membership_only_gc_reuses_a_cached_holder_for_its_cas() {
    for committed in [false, true] {
        reclaim_membership_only(committed, true).await;
    }
}

#[derive(Clone, Copy)]
enum DirectoryReclamation {
    StaleNoHolder,
    CachedHolder,
    ReadFailure,
    OwnerReleased,
    CommittedReleased,
    CommittedHolder,
}

async fn reclaim_directory(typ: LockType, case: DirectoryReclamation) {
    use crate::collection_coordination::CollectionLocker;
    use crate::monitor::{OwnerAbortOutcome, TxRecoveryManifest};

    let hooks = HookBackend::new(Arc::new(MemoryBackend::new()));
    let recorded = RecordingBackend::new(hooks.clone());
    let operations = recorded.log();
    let backend: Arc<dyn Backend> = Arc::new(recorded);
    // GC caches the record before the independent owner acquires its holder.
    let ctx = new_ctx_with(backend.clone()).await;
    let owner = AssemblyFixture::new(
        backend,
        DbPrefix::try_from("db").unwrap(),
        &EngineConfig::default(),
    );
    let locker = CollectionLocker::new(
        CollectionStateResolver::new(
            owner.records.clone(),
            owner.tx_records.clone(),
            owner.timeline.clone(),
            owner.monitor.clone(),
            RetryConfig::default(),
        ),
        std::num::NonZeroUsize::MIN,
    );
    let id = tx(83);
    let locks = vec![TxLock::Directory {
        collection: collection(),
        typ,
    }];
    owner
        .monitor
        .begin_persisted_tx(
            &id,
            TxRecoveryManifest {
                locks: locks.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let operation = owner.monitor.begin_owner_operation(&id).unwrap();
    locker.acquire(&collection(), &id, typ).await.unwrap();
    operation.complete();
    if matches!(
        case,
        DirectoryReclamation::CommittedReleased | DirectoryReclamation::CommittedHolder
    ) {
        let mut record = TxRecord::new(id.clone(), TxCommitStatus::Committed);
        record.locks = locks.clone();
        owner.monitor.commit_tx(record).await.unwrap();
    } else {
        assert_eq!(
            owner.monitor.abort_owned_tx(&id).await.unwrap(),
            OwnerAbortOutcome::Acknowledged
        );
    }
    if matches!(
        case,
        DirectoryReclamation::CachedHolder | DirectoryReclamation::CommittedHolder
    ) {
        ctx.records
            .load_record(
                &collection(),
                Requirement::after(ctx.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
    }
    let path = ObjectPath::CollectionRecord {
        collection: collection(),
    }
    .to_string();
    if matches!(
        case,
        DirectoryReclamation::OwnerReleased | DirectoryReclamation::CommittedReleased
    ) {
        operations.lock().unwrap().clear();
        assert!(locker.release(&id, &locks, Requirement::ANY).await.unwrap());
        let recorded = std::mem::take(&mut *operations.lock().unwrap());
        let calls: Vec<_> = recorded
            .iter()
            .filter(|op| op.path == path)
            .map(|op| op.op)
            .collect();
        assert_eq!(calls, ["write_if"]);
        assert!(!locker.release(&id, &locks, Requirement::ANY).await.unwrap());
        assert!(operations.lock().unwrap().is_empty());
    }
    // Enter reclamation after eligibility. Waiting out retention cannot
    // change either instance's cached leaf.
    let observed = ctx
        .tx_records
        .get_at(&id, Requirement::after(ctx.timeline.currentness_barrier()))
        .await
        .unwrap();
    let barrier = ctx.timeline.currentness_barrier();
    if matches!(case, DirectoryReclamation::ReadFailure) {
        hooks.set_before({
            let path = path.clone();
            move |op| {
                let fail = op.path() == path
                    && matches!(
                        op,
                        BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                    );
                Box::pin(async move {
                    if fail {
                        Err(BackendError::other("directory check failed"))
                    } else {
                        Ok(())
                    }
                })
            }
        });
        assert!(ctx.gc.try_reclaim(&id, &observed, barrier).await.is_err());
        assert!(!is_gone(&ctx.tx_records, &id).await);
        hooks.clear_before();
    }
    operations.lock().unwrap().clear();
    assert_eq!(
        ctx.gc.try_reclaim(&id, &observed, barrier).await.unwrap(),
        GcOutcome::Reclaimed
    );
    assert!(is_gone(&ctx.tx_records, &id).await);
    let recorded = std::mem::take(&mut *operations.lock().unwrap());
    let (record, _) = owner
        .records
        .load_record(
            &collection(),
            Requirement::after(owner.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert!(
        !record.directory_lock().contains(&id),
        "GC deleted the transaction record while its directory holder remained"
    );
    let calls: Vec<_> = recorded
        .iter()
        .filter(|op| op.path == path)
        .map(|op| op.op)
        .collect();
    let expected: &[&str] = match case {
        DirectoryReclamation::StaleNoHolder | DirectoryReclamation::ReadFailure => {
            &["read_if_modified", "write_if"]
        }
        DirectoryReclamation::CachedHolder | DirectoryReclamation::CommittedHolder => &["write_if"],
        DirectoryReclamation::OwnerReleased | DirectoryReclamation::CommittedReleased => {
            &["read_if_modified"]
        }
    };
    assert_eq!(calls, expected);
}

#[tokio::test]
async fn aborted_directory_reader_gc_refreshes_a_cached_no_holder() {
    reclaim_directory(LockType::Read, DirectoryReclamation::StaleNoHolder).await;
}

#[tokio::test]
async fn aborted_directory_writer_gc_refreshes_a_cached_no_holder() {
    reclaim_directory(LockType::Write, DirectoryReclamation::StaleNoHolder).await;
}

#[tokio::test]
async fn directory_gc_reuses_a_cached_holder_for_its_cas() {
    for typ in [LockType::Read, LockType::Write] {
        reclaim_directory(typ, DirectoryReclamation::CachedHolder).await;
    }
}

#[tokio::test]
async fn committed_directory_gc_reuses_removal_after_cache_eviction() {
    use crate::monitor::TxRecoveryManifest;

    let recorded = RecordingBackend::new(Arc::new(MemoryBackend::new()));
    let operations = recorded.log();
    let mut config = EngineConfig::default();
    config.set_cache_size(0);
    let ctx = new_ctx_with_config(Arc::new(recorded), &config).await;
    let id = tx(86);
    let mut locks = Vec::new();
    let mut parent = CollectionRecord::new();
    // Each shard retains its last entry even with a zero-byte budget. More
    // directories than shards guarantees eviction during write-back without
    // selecting paths from the cache's hash or inspecting its entries.
    for index in 0..=glassdb_concurr::shard::count() {
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&(index as u64 + 1).to_be_bytes());
        let child = CollectionAddress::new("db", CollectionId::from_slice(&bytes).unwrap());
        assert!(
            ctx.records
                .create_record(&child, &CollectionRecord::new())
                .await
                .unwrap()
        );
        assert!(
            ctx.nodes
                .create_root(&child, &Node::leaf(LeafBody::new()))
                .await
                .unwrap()
        );
        parent.add_child(bytes.to_vec(), child.id()).unwrap();
        locks.push(TxLock::Directory {
            collection: child,
            typ: if index % 2 == 0 {
                LockType::Read
            } else {
                LockType::Write
            },
        });
    }
    let (_, observed) = ctx
        .records
        .load_record(&collection(), Requirement::ANY)
        .await
        .unwrap();
    assert!(ctx.records.store_record(&parent, &observed).await.unwrap());
    ctx.mon
        .begin_persisted_tx(
            &id,
            TxRecoveryManifest {
                locks: locks.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let operation = ctx.mon.begin_owner_operation(&id).unwrap();
    for lock in &locks {
        let TxLock::Directory { collection, typ } = lock else {
            unreachable!()
        };
        ctx.locker
            .collections()
            .acquire(collection, &id, *typ)
            .await
            .unwrap();
    }
    operation.complete();
    let mut record = TxRecord::new(id.clone(), TxCommitStatus::Committed);
    record.locks = locks.clone();
    ctx.mon.commit_tx(record).await.unwrap();
    let observed = ctx.tx_records.get_at(&id, Requirement::ANY).await.unwrap();
    let barrier = ctx.timeline.currentness_barrier();
    operations.lock().unwrap().clear();
    assert_eq!(
        ctx.gc.try_reclaim(&id, &observed, barrier).await.unwrap(),
        GcOutcome::Reclaimed
    );
    assert!(is_gone(&ctx.tx_records, &id).await);
    let recorded = std::mem::take(&mut *operations.lock().unwrap());
    for lock in &locks {
        let TxLock::Directory { collection, .. } = lock else {
            unreachable!()
        };
        let path = ObjectPath::CollectionRecord {
            collection: collection.clone(),
        }
        .to_string();
        let calls = recorded
            .iter()
            .filter(|op| op.path == path)
            .map(|op| op.op)
            .collect::<Vec<_>>();
        assert_eq!(calls.iter().filter(|op| **op == "write_if").count(), 1);
        let after_removal = calls
            .iter()
            .skip_while(|op| **op != "write_if")
            .skip(1)
            .copied()
            .collect::<Vec<_>>();
        assert!(
            after_removal.is_empty(),
            "record read again after removal: {calls:?}"
        );
        let (record, _) = ctx
            .records
            .load_record(
                collection,
                Requirement::after(ctx.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        assert!(!record.directory_lock().contains(&id));
    }
}

#[tokio::test]
async fn committed_directory_gc_needs_no_read_after_a_warm_removal() {
    for typ in [LockType::Read, LockType::Write] {
        reclaim_directory(typ, DirectoryReclamation::CommittedHolder).await;
    }
}

#[tokio::test]
async fn committed_directory_removal_does_not_prove_other_records_clear() {
    use crate::collection_coordination::CollectionLocker;
    use crate::monitor::TxRecoveryManifest;

    for fail_check in [false, true] {
        let hooks = HookBackend::new(Arc::new(MemoryBackend::new()));
        let recorded = RecordingBackend::new(hooks.clone());
        let operations = recorded.log();
        let backend: Arc<dyn Backend> = Arc::new(recorded);
        let ctx = new_ctx_with(backend.clone()).await;
        let child = CollectionAddress::new("db", CollectionId::from_slice(&[84; 16]).unwrap());
        assert!(
            ctx.records
                .create_record(&child, &CollectionRecord::new())
                .await
                .unwrap()
        );
        assert!(
            ctx.nodes
                .create_root(&child, &Node::leaf(LeafBody::new()))
                .await
                .unwrap()
        );
        let (mut parent, observed) = ctx
            .records
            .load_record(&collection(), Requirement::ANY)
            .await
            .unwrap();
        parent.add_child(b"child".to_vec(), child.id()).unwrap();
        assert!(ctx.records.store_record(&parent, &observed).await.unwrap());
        let owner = AssemblyFixture::new(
            backend,
            DbPrefix::try_from("db").unwrap(),
            &EngineConfig::default(),
        );
        let locker = CollectionLocker::new(
            CollectionStateResolver::new(
                owner.records.clone(),
                owner.tx_records.clone(),
                owner.timeline.clone(),
                owner.monitor.clone(),
                RetryConfig::default(),
            ),
            std::num::NonZeroUsize::MIN,
        );
        let id = tx(84);
        let locks = vec![
            TxLock::Directory {
                collection: collection(),
                typ: LockType::Read,
            },
            TxLock::Directory {
                collection: child.clone(),
                typ: LockType::Read,
            },
        ];
        owner
            .monitor
            .begin_persisted_tx(
                &id,
                TxRecoveryManifest {
                    locks: locks.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let operation = owner.monitor.begin_owner_operation(&id).unwrap();
        for collection in [collection(), child.clone()] {
            locker
                .acquire(&collection, &id, LockType::Read)
                .await
                .unwrap();
        }
        operation.complete();
        let mut record = TxRecord::new(id.clone(), TxCommitStatus::Committed);
        record.locks = locks;
        owner.monitor.commit_tx(record).await.unwrap();
        // Only the parent's holder is visible to speculative write-back.
        // The child's old no-holder record must still get the bounded check.
        ctx.records
            .load_record(
                &collection(),
                Requirement::after(ctx.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        let observed = ctx.tx_records.get_at(&id, Requirement::ANY).await.unwrap();
        let barrier = ctx.timeline.currentness_barrier();
        let child_path = ObjectPath::CollectionRecord {
            collection: child.clone(),
        }
        .to_string();
        if fail_check {
            hooks.set_before({
                let child_path = child_path.clone();
                move |op| {
                    let fail = op.path() == child_path
                        && matches!(
                            op,
                            BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                        );
                    Box::pin(async move {
                        if fail {
                            Err(BackendError::other("directory check failed"))
                        } else {
                            Ok(())
                        }
                    })
                }
            });
        }
        operations.lock().unwrap().clear();
        let result = ctx.gc.try_reclaim(&id, &observed, barrier).await;
        if fail_check {
            assert!(result.is_err());
        } else {
            assert_eq!(result.unwrap(), GcOutcome::Reclaimed);
        }
        assert_eq!(is_gone(&ctx.tx_records, &id).await, !fail_check);
        let recorded = std::mem::take(&mut *operations.lock().unwrap());
        let expected: &[&str] = if fail_check {
            &["read_if_modified"]
        } else {
            &["read_if_modified", "write_if"]
        };
        assert_eq!(
            recorded
                .iter()
                .filter(|op| op.path == child_path)
                .map(|op| op.op)
                .collect::<Vec<_>>(),
            expected
        );
        hooks.clear_before();
        for (collection, held) in [(collection(), false), (child, fail_check)] {
            let (record, _) = owner
                .records
                .load_record(
                    &collection,
                    Requirement::after(owner.timeline.currentness_barrier()),
                )
                .await
                .unwrap();
            assert_eq!(record.directory_lock().contains(&id), held);
        }
    }
}

#[derive(Clone, Copy)]
enum DirectoryWriteBack {
    Unreferenced,
    LiveValue,
    WriteFailure,
    LostWriteReply,
}

async fn recover_directory_change(op: TxCollectionOp, case: DirectoryWriteBack) {
    use crate::collection_coordination::CollectionLocker;
    use crate::collections::{CollectionChange, CollectionOp};
    use crate::monitor::TxRecoveryManifest;

    let hooks = HookBackend::new(Arc::new(MemoryBackend::new()));
    let recorded = RecordingBackend::new(hooks.clone());
    let operations = recorded.log();
    let backend: Arc<dyn Backend> = Arc::new(recorded);
    let ctx = new_ctx_with(backend.clone()).await;
    let owner = AssemblyFixture::new(
        backend,
        DbPrefix::try_from("db").unwrap(),
        &EngineConfig::default(),
    );
    let locker = CollectionLocker::new(
        CollectionStateResolver::new(
            owner.records.clone(),
            owner.tx_records.clone(),
            owner.timeline.clone(),
            owner.monitor.clone(),
            RetryConfig::default(),
        ),
        std::num::NonZeroUsize::MIN,
    );
    let lifecycle = CollectionLifecycle::new(
        owner.records.clone(),
        owner.nodes.clone(),
        owner.monitor.clone(),
        RetryConfig::default(),
        Arc::new(UnexpectedTopologySettler),
    );
    let id = tx(87);
    let child = CollectionAddress::new("db", CollectionId::from_slice(&[87; 16]).unwrap());
    let mut change = CollectionChange {
        parent: collection(),
        name: b"child".to_vec(),
        collection: child.clone(),
        expected: None,
        op: CollectionOp::Create,
    };
    if op == TxCollectionOp::Drop {
        lifecycle
            .prepare_collections(std::slice::from_ref(&change))
            .await
            .unwrap();
        let (mut record, observed) = ctx
            .records
            .load_record(&collection(), Requirement::ANY)
            .await
            .unwrap();
        record.add_child(change.name.clone(), child.id()).unwrap();
        assert!(ctx.records.store_record(&record, &observed).await.unwrap());
        change.op = CollectionOp::Drop;
        change.expected = Some(child.id());
    }
    // GC retains this no-holder parent while the owner acquires its writer.
    let mut record = TxRecord::new(id.clone(), TxCommitStatus::Committed);
    record.locks = vec![TxLock::Directory {
        collection: collection(),
        typ: LockType::Write,
    }];
    record.collection_changes = vec![TxCollectionChange {
        parent: collection(),
        name: change.name.clone(),
        collection: child.clone(),
        op,
    }];
    if op == TxCollectionOp::Create {
        record.prepared_collections = vec![child.clone()];
    }
    if matches!(case, DirectoryWriteBack::LiveValue) {
        record.writes = committed(id.clone(), PAST_HORIZON, &[b"k"], &[]).writes;
    }
    owner
        .monitor
        .begin_persisted_tx(&id, TxRecoveryManifest::from_record(&record))
        .await
        .unwrap();
    let operation = owner.monitor.begin_owner_operation(&id).unwrap();
    lifecycle
        .prepare_collections(std::slice::from_ref(&change))
        .await
        .unwrap();
    lifecycle
        .install_drop_intents(&id, std::slice::from_ref(&change))
        .await
        .unwrap();
    locker
        .acquire(&collection(), &id, LockType::Write)
        .await
        .unwrap();
    operation.complete();
    owner.monitor.commit_tx(record.clone()).await.unwrap();
    if matches!(case, DirectoryWriteBack::LiveValue) {
        locker
            .write_back(
                &id,
                std::slice::from_ref(&change),
                &record.locks,
                Requirement::ANY,
            )
            .await
            .unwrap();
        store_entry(&ctx, b"k", writer_entry(b"k", &id)).await;
    }
    let path = ObjectPath::CollectionRecord {
        collection: collection(),
    }
    .to_string();
    let observed = ctx.tx_records.get_at(&id, Requirement::ANY).await.unwrap();
    if matches!(
        case,
        DirectoryWriteBack::WriteFailure | DirectoryWriteBack::LostWriteReply
    ) {
        let fail_write = {
            let path = path.clone();
            move |operation: &BackendOp| -> HookFuture {
                let fail =
                    operation.path() == path && matches!(operation, BackendOp::WriteIf { .. });
                Box::pin(async move {
                    if fail {
                        Err(BackendError::Unavailable(
                            "directory write-back unavailable".into(),
                        ))
                    } else {
                        Ok(())
                    }
                })
            }
        };
        let applied = matches!(case, DirectoryWriteBack::LostWriteReply);
        if applied {
            hooks.set_after(move |operation, outcome| {
                if outcome.is_success() {
                    fail_write(operation)
                } else {
                    Box::pin(async { Ok(()) })
                }
            });
        } else {
            hooks.set_before(fail_write);
        }
        assert!(
            ctx.gc
                .try_reclaim(&id, &observed, ctx.timeline.currentness_barrier())
                .await
                .is_err()
        );
        assert!(!is_gone(&ctx.tx_records, &id).await);
        let (record, _) = owner
            .records
            .load_record(
                &collection(),
                Requirement::after(owner.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        assert_eq!(record.directory_lock().contains(&id), !applied);
        assert_eq!(
            record.child(&change.name),
            ((op == TxCollectionOp::Create) == applied).then_some(child.id())
        );
        hooks.clear_before();
        hooks.clear_after();
    }
    operations.lock().unwrap().clear();
    let outcome = ctx
        .gc
        .try_reclaim(&id, &observed, ctx.timeline.currentness_barrier())
        .await
        .unwrap();
    let live_value = matches!(case, DirectoryWriteBack::LiveValue);
    if live_value {
        assert!(!is_gone(&ctx.tx_records, &id).await);
        // Each GC pass captures a new bound. A live value must continue to
        // bypass bounded directory completion after owner write-back.
        ctx.gc
            .try_reclaim(&id, &observed, ctx.timeline.currentness_barrier())
            .await
            .unwrap();
        assert!(!is_gone(&ctx.tx_records, &id).await);
    } else {
        assert_eq!(outcome, GcOutcome::Reclaimed);
        assert!(is_gone(&ctx.tx_records, &id).await);
    }
    let recorded = std::mem::take(&mut *operations.lock().unwrap());
    let calls: Vec<_> = recorded
        .iter()
        .filter(|op| op.path == path)
        .map(|op| op.op)
        .collect();
    let expected: &[&str] = match case {
        DirectoryWriteBack::Unreferenced => &["read_if_modified", "write_if"],
        DirectoryWriteBack::LiveValue => &[],
        DirectoryWriteBack::WriteFailure => &["read", "write_if"],
        DirectoryWriteBack::LostWriteReply => &["read"],
    };
    assert_eq!(calls, expected);
    let (record, _) = owner
        .records
        .load_record(
            &collection(),
            Requirement::after(owner.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert!(!record.directory_lock().contains(&id));
    assert_eq!(
        record.child(&change.name),
        (op == TxCollectionOp::Create).then_some(child.id())
    );
    let root = ctx
        .nodes
        .load_root(
            &child,
            Requirement::after(ctx.timeline.currentness_barrier()),
        )
        .await;
    if op == TxCollectionOp::Create {
        assert!(root.is_ok());
    } else {
        assert!(matches!(root, Err(StorageError::NotFound)));
    }
}

#[tokio::test]
async fn committed_directory_changes_finish_in_one_gc_pass() {
    for op in [TxCollectionOp::Create, TxCollectionOp::Drop] {
        recover_directory_change(op, DirectoryWriteBack::Unreferenced).await;
    }
}

#[tokio::test]
async fn committed_directory_write_back_failure_keeps_the_record() {
    for op in [TxCollectionOp::Create, TxCollectionOp::Drop] {
        for case in [
            DirectoryWriteBack::WriteFailure,
            DirectoryWriteBack::LostWriteReply,
        ] {
            recover_directory_change(op, case).await;
        }
    }
}

#[tokio::test]
async fn live_values_skip_bounded_directory_completion() {
    for op in [TxCollectionOp::Create, TxCollectionOp::Drop] {
        recover_directory_change(op, DirectoryWriteBack::LiveValue).await;
    }
}

#[tokio::test]
async fn aborted_directory_gc_keeps_the_record_if_the_record_check_fails() {
    reclaim_directory(LockType::Read, DirectoryReclamation::ReadFailure).await;
}

#[tokio::test]
async fn directory_gc_checks_owner_release_once() {
    for typ in [LockType::Read, LockType::Write] {
        for case in [
            DirectoryReclamation::OwnerReleased,
            DirectoryReclamation::CommittedReleased,
        ] {
            reclaim_directory(typ, case).await;
        }
    }
}

#[derive(Clone, Copy)]
enum TopologyReclamation {
    StaleNoParticipant,
    CachedParticipant,
    ReadFailure,
    IntentRemaining,
    DirectoryRemoved,
}

async fn reclaim_topology(committed: bool, case: TopologyReclamation) {
    use crate::monitor::{OwnerAbortOutcome, TxRecoveryManifest};
    use glassdb_data::{NodeToken, StructuralIntentId};
    use glassdb_storage::{StructuralIntent, StructuralIntentPhase};

    let hooks = HookBackend::new(Arc::new(MemoryBackend::new()));
    let recorded = RecordingBackend::new(hooks.clone());
    let operations = recorded.log();
    let backend: Arc<dyn Backend> = Arc::new(recorded);
    // GC caches the collection before the independent owner joins topology.
    let ctx = new_ctx_with(backend.clone()).await;
    let owner = AssemblyFixture::new(
        backend,
        DbPrefix::try_from("db").unwrap(),
        &EngineConfig::default(),
    );
    let id = tx(85);
    let mut locks = vec![TxLock::TopologyParticipant {
        collection: collection(),
    }];
    if matches!(case, TopologyReclamation::DirectoryRemoved) {
        locks.push(TxLock::Directory {
            collection: collection(),
            typ: LockType::Read,
        });
    }
    owner
        .monitor
        .begin_persisted_tx(
            &id,
            TxRecoveryManifest {
                locks: locks.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let operation = owner.monitor.begin_owner_operation(&id).unwrap();
    let left = NodeToken::from_bytes([85; 16]);
    let right = NodeToken::from_bytes([86; 16]);
    let prepared = owner
        .structural_intents
        .write(
            collection().db_prefix_component(),
            &StructuralIntentId::from(&right),
            &StructuralIntent {
                collection: collection(),
                source_token: None,
                source_revision: String::new(),
                created_tokens: vec![left, right],
                split_key: Vec::new(),
                participant_id: id.clone(),
                phase: StructuralIntentPhase::Preparing,
            },
        )
        .await
        .unwrap();
    let (mut record, observed) = owner
        .records
        .load_record(&collection(), Requirement::ANY)
        .await
        .unwrap();
    assert!(record.add_topology_participant(id.clone()));
    if matches!(case, TopologyReclamation::DirectoryRemoved) {
        record.add_directory_reader(id.clone());
    }
    assert!(
        owner
            .records
            .store_record(&record, &observed)
            .await
            .unwrap()
    );
    let path = ObjectPath::CollectionRecord {
        collection: collection(),
    }
    .to_string();
    if !matches!(case, TopologyReclamation::IntentRemaining) {
        // A canceled Preparing intent has not created any nodes. Departure
        // can then fail before finalization, leaving only the participant.
        owner.structural_intents.delete(&prepared).await.unwrap();
        let (mut record, observed) = owner
            .records
            .load_record(&collection(), Requirement::ANY)
            .await
            .unwrap();
        assert!(record.remove_topology_participant(&id));
        hooks.set_before({
            let path = path.clone();
            move |op| {
                let fail = op.path() == path && matches!(op, BackendOp::WriteIf { .. });
                Box::pin(async move {
                    if fail {
                        Err(BackendError::other("participant departure failed"))
                    } else {
                        Ok(())
                    }
                })
            }
        });
        assert!(
            owner
                .records
                .store_record(&record, &observed)
                .await
                .is_err()
        );
        hooks.clear_before();
    }
    operation.complete();
    if committed {
        let mut record = TxRecord::new(id.clone(), TxCommitStatus::Committed);
        record.locks = locks;
        owner.monitor.commit_tx(record).await.unwrap();
    } else {
        assert_eq!(
            owner.monitor.abort_owned_tx(&id).await.unwrap(),
            OwnerAbortOutcome::Acknowledged
        );
    }
    if matches!(
        case,
        TopologyReclamation::CachedParticipant | TopologyReclamation::DirectoryRemoved
    ) {
        ctx.records
            .load_record(
                &collection(),
                Requirement::after(ctx.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
    }
    // Enter reclamation after eligibility. Retention cannot refresh the
    // collection record cached before participant registration.
    let observed = ctx
        .tx_records
        .get_at(&id, Requirement::after(ctx.timeline.currentness_barrier()))
        .await
        .unwrap();
    let barrier = ctx.timeline.currentness_barrier();
    if matches!(case, TopologyReclamation::IntentRemaining) {
        operations.lock().unwrap().clear();
        assert_eq!(
            ctx.gc.try_reclaim(&id, &observed, barrier).await.unwrap(),
            GcOutcome::Retained
        );
        assert!(!is_gone(&ctx.tx_records, &id).await);
        assert!(operations.lock().unwrap().iter().all(|op| op.path != path));
        owner.structural_intents.delete(&prepared).await.unwrap();
    }
    if matches!(case, TopologyReclamation::ReadFailure) {
        hooks.set_before({
            let path = path.clone();
            move |op| {
                let fail = op.path() == path
                    && matches!(
                        op,
                        BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                    );
                Box::pin(async move {
                    if fail {
                        Err(BackendError::other("participant check failed"))
                    } else {
                        Ok(())
                    }
                })
            }
        });
        assert!(ctx.gc.try_reclaim(&id, &observed, barrier).await.is_err());
        assert!(!is_gone(&ctx.tx_records, &id).await);
        hooks.clear_before();
    }
    operations.lock().unwrap().clear();
    assert_eq!(
        ctx.gc.try_reclaim(&id, &observed, barrier).await.unwrap(),
        GcOutcome::Reclaimed
    );
    assert!(is_gone(&ctx.tx_records, &id).await);
    let recorded = std::mem::take(&mut *operations.lock().unwrap());
    let (record, _) = owner
        .records
        .load_record(
            &collection(),
            Requirement::after(owner.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert!(
        record
            .topology_participants()
            .all(|participant| participant != &id),
        "GC deleted the transaction record while its topology participant remained"
    );
    assert!(!record.directory_lock().contains(&id));
    let calls: Vec<_> = recorded
        .iter()
        .filter(|op| op.path == path)
        .map(|op| op.op)
        .collect();
    let expected: &[&str] = match case {
        TopologyReclamation::CachedParticipant => &["write_if"],
        TopologyReclamation::DirectoryRemoved => &["write_if", "write_if"],
        _ => &["read_if_modified", "write_if"],
    };
    assert_eq!(calls, expected);
    operations.lock().unwrap().clear();
    assert!(
        !ctx.locker
            .collections()
            .release_topology_participant(&collection(), &id, Requirement::after(barrier))
            .await
            .unwrap()
    );
    assert!(operations.lock().unwrap().is_empty());
}

#[tokio::test]
async fn committed_directory_removal_keeps_topology_reclamation() {
    reclaim_topology(true, TopologyReclamation::DirectoryRemoved).await;
}

#[tokio::test]
async fn committed_topology_gc_refreshes_a_cached_no_participant() {
    reclaim_topology(true, TopologyReclamation::StaleNoParticipant).await;
}

#[tokio::test]
async fn aborted_topology_gc_refreshes_a_cached_no_participant() {
    reclaim_topology(false, TopologyReclamation::StaleNoParticipant).await;
}

#[tokio::test]
async fn topology_gc_reuses_a_cached_participant_for_its_cas() {
    for committed in [false, true] {
        reclaim_topology(committed, TopologyReclamation::CachedParticipant).await;
    }
}

#[tokio::test]
async fn topology_gc_keeps_the_record_if_the_record_check_fails() {
    for committed in [false, true] {
        reclaim_topology(committed, TopologyReclamation::ReadFailure).await;
    }
}

#[tokio::test]
async fn topology_gc_keeps_the_record_and_participant_until_intents_settle() {
    for committed in [false, true] {
        reclaim_topology(committed, TopologyReclamation::IntentRemaining).await;
    }
}

#[derive(Clone, Copy)]
enum DropReclamation {
    StaleFences,
    CachedFences,
    CachedFreeze,
    OwnerCleared,
    ReadFailure,
}

async fn reclaim_aborted_drop(with_child: bool, durable_locks: bool, case: DropReclamation) {
    use crate::collection_coordination::CollectionLocker;
    use crate::collections::{CollectionChange, CollectionOp};
    use crate::monitor::{OwnerAbortOutcome, TxRecoveryManifest};
    use glassdb_data::NodeToken;
    use glassdb_storage::IndexNode;

    let hooks = HookBackend::new(Arc::new(MemoryBackend::new()));
    let recorded = RecordingBackend::new(hooks.clone());
    let operations = recorded.log();
    let backend: Arc<dyn Backend> = Arc::new(recorded);
    let ctx = new_ctx_with(backend.clone()).await;
    let owner = AssemblyFixture::new(
        backend,
        DbPrefix::try_from("db").unwrap(),
        &EngineConfig::default(),
    );
    let lifecycle = CollectionLifecycle::new(
        owner.records.clone(),
        owner.nodes.clone(),
        owner.monitor.clone(),
        RetryConfig::default(),
        Arc::new(UnexpectedTopologySettler),
    );
    let locker = CollectionLocker::new(
        CollectionStateResolver::new(
            owner.records.clone(),
            owner.tx_records.clone(),
            owner.timeline.clone(),
            owner.monitor.clone(),
            RetryConfig::default(),
        ),
        std::num::NonZeroUsize::MIN,
    );
    let target = CollectionAddress::new("db", CollectionId::from_slice(&[37; 16]).unwrap());
    let mut change = CollectionChange {
        parent: collection(),
        name: b"child".to_vec(),
        collection: target.clone(),
        expected: None,
        op: CollectionOp::Create,
    };
    lifecycle
        .prepare_collections(std::slice::from_ref(&change))
        .await
        .unwrap();
    let mut node_paths = vec![ObjectPath::TreeRoot {
        collection: target.clone(),
    }];
    if with_child {
        let token = NodeToken::from_bytes([38; 16]);
        assert!(
            owner
                .nodes
                .store_node(&target, &token, &Node::leaf(LeafBody::new()), None)
                .await
                .unwrap()
        );
        let (_, observed) = owner
            .nodes
            .load_root(&target, Requirement::ANY)
            .await
            .unwrap();
        let root = Node::index(IndexNode::from_children([(Vec::new(), token.to_string())]));
        assert!(
            owner
                .nodes
                .store_root(&target, &root, &observed)
                .await
                .unwrap()
        );
        node_paths.push(ObjectPath::Node {
            collection: target.clone(),
            token,
        });
    }
    let (mut parent, observed) = owner
        .records
        .load_record(&collection(), Requirement::ANY)
        .await
        .unwrap();
    parent.add_child(change.name.clone(), target.id()).unwrap();
    assert!(
        owner
            .records
            .store_record(&parent, &observed)
            .await
            .unwrap()
    );
    let record_path = ObjectPath::CollectionRecord {
        collection: target.clone(),
    }
    .to_string();
    for path in &node_paths {
        ctx.nodes
            .load_node_at_state(path, Requirement::ANY)
            .await
            .unwrap();
    }
    ctx.records
        .load_record(&target, Requirement::ANY)
        .await
        .unwrap();

    change.op = CollectionOp::Drop;
    change.expected = Some(target.id());
    let id = tx(87);
    let locks = vec![
        TxLock::Directory {
            collection: collection(),
            typ: LockType::Write,
        },
        TxLock::Directory {
            collection: target.clone(),
            typ: LockType::Read,
        },
    ];
    owner
        .monitor
        .begin_persisted_tx(
            &id,
            TxRecoveryManifest {
                locks: if durable_locks {
                    locks.clone()
                } else {
                    Vec::new()
                },
                collection_changes: vec![TxCollectionChange {
                    parent: collection(),
                    name: change.name.clone(),
                    collection: target.clone(),
                    op: TxCollectionOp::Drop,
                }],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let operation = owner.monitor.begin_owner_operation(&id).unwrap();
    locker
        .acquire(&collection(), &id, LockType::Write)
        .await
        .unwrap();
    locker.acquire(&target, &id, LockType::Read).await.unwrap();
    owner.monitor.record_tx_locks(&id, locks.clone());
    lifecycle
        .install_drop_intents(&id, std::slice::from_ref(&change))
        .await
        .unwrap();
    operation.complete();
    // Paused time keeps the refresher from copying the local lock list. An
    // acknowledged abort can retain the earlier durable drop-only manifest.
    assert_eq!(
        owner.monitor.abort_owned_tx(&id).await.unwrap(),
        OwnerAbortOutcome::Acknowledged
    );
    let observed = ctx
        .tx_records
        .get_at(&id, Requirement::after(ctx.timeline.currentness_barrier()))
        .await
        .unwrap();
    assert_eq!(
        observed.value().unwrap().locks,
        if durable_locks { locks } else { Vec::new() }
    );
    if matches!(case, DropReclamation::CachedFences) {
        let requirement = Requirement::after(ctx.timeline.currentness_barrier());
        for path in &node_paths {
            ctx.nodes
                .load_node_at_state(path, requirement)
                .await
                .unwrap();
        }
    }
    if matches!(
        case,
        DropReclamation::CachedFences | DropReclamation::CachedFreeze
    ) {
        ctx.records
            .load_record(
                &target,
                Requirement::after(ctx.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
    }
    if matches!(case, DropReclamation::OwnerCleared) {
        operations.lock().unwrap().clear();
        assert!(
            lifecycle
                .clear_aborted_drops(&id, std::slice::from_ref(&target), Requirement::ANY)
                .await
                .unwrap()
        );
        let recorded = std::mem::take(&mut *operations.lock().unwrap());
        for path in node_paths
            .iter()
            .map(ToString::to_string)
            .chain([record_path.clone()])
        {
            let calls: Vec<_> = recorded
                .iter()
                .filter(|op| op.path == path)
                .map(|op| op.op)
                .collect();
            assert_eq!(calls, ["write_if"], "owner release for {path}");
        }
        assert!(
            !lifecycle
                .clear_aborted_drops(&id, std::slice::from_ref(&target), Requirement::ANY)
                .await
                .unwrap()
        );
        assert!(operations.lock().unwrap().iter().all(|op| op.op == "list"));
    }
    let barrier = ctx.timeline.currentness_barrier();
    if matches!(case, DropReclamation::ReadFailure) {
        hooks.set_before({
            let path = node_paths.last().unwrap().to_string();
            move |op| {
                let fail = op.path() == path
                    && matches!(
                        op,
                        BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                    );
                Box::pin(async move {
                    if fail {
                        Err(BackendError::other("drop intent check failed"))
                    } else {
                        Ok(())
                    }
                })
            }
        });
        assert!(ctx.gc.try_reclaim(&id, &observed, barrier).await.is_err());
        assert!(!is_gone(&ctx.tx_records, &id).await);
        hooks.clear_before();
    }
    // Enter reclamation after eligibility. Retention cannot refresh any of
    // these pre-fence cache entries.
    operations.lock().unwrap().clear();
    assert_eq!(
        ctx.gc.try_reclaim(&id, &observed, barrier).await.unwrap(),
        GcOutcome::Reclaimed
    );
    assert!(is_gone(&ctx.tx_records, &id).await);
    let recorded = std::mem::take(&mut *operations.lock().unwrap());
    let mut remaining = Vec::new();
    let verification = Requirement::after(owner.timeline.currentness_barrier());
    for path in &node_paths {
        let node = owner
            .nodes
            .load_node_at_state(path, verification)
            .await
            .unwrap();
        if node.value().unwrap().drop_intent() == Some(&id) {
            remaining.push(path.to_string());
        }
    }
    let (record, _) = owner
        .records
        .load_record(&target, verification)
        .await
        .unwrap();
    if record.topology_freeze() == Some(&id) {
        remaining.push(record_path.clone());
    }
    assert!(
        remaining.is_empty(),
        "GC deleted the record with drop intents still present: {remaining:?}"
    );
    let node_expected: &[&str] = match case {
        DropReclamation::CachedFences => &["write_if"],
        DropReclamation::OwnerCleared => &["read_if_modified"],
        DropReclamation::StaleFences
        | DropReclamation::CachedFreeze
        | DropReclamation::ReadFailure => &["read_if_modified", "write_if"],
    };
    for path in node_paths.iter().map(ToString::to_string) {
        let calls: Vec<_> = recorded
            .iter()
            .filter(|op| op.path == path)
            .map(|op| op.op)
            .collect();
        assert_eq!(calls, node_expected, "GC node reclamation for {path}");
    }
    let record_calls: Vec<_> = recorded
        .iter()
        .filter(|op| op.path == record_path)
        .map(|op| op.op)
        .collect();
    let record_expected: &[&str] = if durable_locks {
        &["read_if_modified", "write_if", "write_if"]
    } else if matches!(case, DropReclamation::CachedFreeze) {
        &["write_if"]
    } else {
        node_expected
    };
    assert_eq!(record_calls, record_expected);
    operations.lock().unwrap().clear();
    assert!(
        !ctx.gc
            .collection_lifecycle
            .clear_aborted_drops(&id, &[target], Requirement::after(barrier))
            .await
            .unwrap()
    );
    assert!(operations.lock().unwrap().iter().all(|op| op.op == "list"));
}

#[tokio::test(start_paused = true)]
async fn aborted_drop_gc_clears_cached_root_fence_and_freeze() {
    reclaim_aborted_drop(false, false, DropReclamation::StaleFences).await;
}

#[tokio::test(start_paused = true)]
async fn aborted_drop_gc_clears_cached_index_and_child_fences() {
    reclaim_aborted_drop(true, false, DropReclamation::StaleFences).await;
}

#[tokio::test(start_paused = true)]
async fn aborted_drop_gc_reuses_directory_release_evidence() {
    reclaim_aborted_drop(true, true, DropReclamation::StaleFences).await;
}

#[tokio::test(start_paused = true)]
async fn aborted_drop_gc_reuses_cached_fences_for_its_cas() {
    reclaim_aborted_drop(true, false, DropReclamation::CachedFences).await;
}

#[tokio::test(start_paused = true)]
async fn aborted_drop_gc_checks_stale_nodes_without_rechecking_a_cached_freeze() {
    reclaim_aborted_drop(true, false, DropReclamation::CachedFreeze).await;
}

#[tokio::test(start_paused = true)]
async fn aborted_drop_owner_release_needs_no_extra_reads() {
    reclaim_aborted_drop(true, false, DropReclamation::OwnerCleared).await;
}

#[tokio::test(start_paused = true)]
async fn aborted_drop_gc_keeps_the_record_if_a_fence_check_fails() {
    reclaim_aborted_drop(true, false, DropReclamation::ReadFailure).await;
}

#[tokio::test(start_paused = true)]
async fn pending_and_wounded_candidates_only_read_their_records() {
    for (status, age) in [TxCommitStatus::Pending, TxCommitStatus::Wounded]
        .into_iter()
        .flat_map(|status| [Duration::ZERO, PAST_HORIZON].map(|age| (status, age)))
    {
        let backend = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let operations = backend.log();
        let ctx = new_ctx_with(Arc::new(backend)).await;
        let id = tx(1);
        let mut record = TxRecord::new(id.clone(), status);
        record.timestamp = Some(base() - age);
        record.locks = vec![write_lock(b"k")];
        ctx.tx_records.set(&record).await.unwrap();
        store_entry(&ctx, b"k", locked_entry(b"k", &id)).await;
        operations.lock().unwrap().clear();

        let background = Background::new();
        ctx.gc.start(&background);
        for _ in 0..2 {
            ctx.hints.schedule(id.clone());
            wait_for_hint_deadline(&ctx).await;
        }
        background.shutdown().await;

        let path = ObjectPath::Transaction {
            db_prefix: DbPrefix::try_from("db").unwrap(),
            id: id.clone(),
        }
        .to_string();
        {
            let operations = operations.lock().unwrap();
            assert!(
                operations.iter().any(|op| op.path == path),
                "GC must check the hinted candidate"
            );
            assert!(
                operations.iter().all(|op| {
                    op.op == "list"
                        || (op.path == path && matches!(op.op, "read" | "read_if_modified"))
                }),
                "pending and wounded candidates must only read their records"
            );
        }
        let stats = ctx.gc.stats_and_reset();
        assert_eq!(stats.progress, 0);
        assert_eq!(stats.failures, 0);
        let diagnostics = ctx.gc.diagnostics();
        assert_eq!(diagnostics.ready, 0);
        assert_eq!(diagnostics.deferred, 0);
        assert_eq!(diagnostics.in_flight, 0);
        let got = ctx.tx_records.get_at(&id, Requirement::ANY).await.unwrap();
        assert_eq!(got.value().unwrap().status, status);
        assert_eq!(
            lookup_entry(&ctx, b"k").await.unwrap().lock_holders(),
            std::slice::from_ref(&id)
        );
    }
}

#[tokio::test(start_paused = true)]
async fn lock_acquisition_resolves_a_wound_that_gc_leaves_alone() {
    use crate::access::{AccessSet, WriteAccess};

    let ctx = new_ctx().await;
    let wounded = tx(1);
    let mut record = TxRecord::new(wounded.clone(), TxCommitStatus::Wounded);
    record.timestamp = Some(base() - PAST_HORIZON);
    record.locks = vec![write_lock(b"k")];
    ctx.tx_records.set(&record).await.unwrap();
    store_entry(&ctx, b"k", locked_entry(b"k", &wounded)).await;
    check_hints_and_scan_page(&ctx).await;
    assert!(
        lookup_entry(&ctx, b"k")
            .await
            .unwrap()
            .is_locked_by(&wounded)
    );

    let contender = tx(2);
    ctx.mon.begin_tx(&contender);
    let accesses = AccessSet::new(
        Vec::new(),
        vec![WriteAccess::put(
            LogicalKey::new(collection(), b"k"),
            b"value".to_vec().into(),
        )],
        Vec::new(),
    );
    let outcome = ctx
        .locker
        .keys()
        .lock_at(
            &contender,
            &accesses,
            false,
            Requirement::after(ctx.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert!(matches!(outcome, LockOutcome::Locked(_)));
    let entry = lookup_entry(&ctx, b"k").await.unwrap();
    assert!(!entry.is_locked_by(&wounded));
    assert!(entry.is_locked_by(&contender));
    check_hints_and_scan_page(&ctx).await;
    let got = ctx
        .tx_records
        .get_at(&wounded, Requirement::ANY)
        .await
        .unwrap();
    assert_eq!(got.value().unwrap().status, TxCommitStatus::Wounded);
}

// An acknowledged aborted record still within its safety horizon keeps its
// locks and remains available to ordinary late observers.
#[tokio::test(start_paused = true)]
async fn recent_aborted_tombstone_is_kept() {
    let ctx = new_ctx().await;
    let t = tx(1);
    let mut record = TxRecord::new(t.clone(), TxCommitStatus::Aborted);
    record.timestamp = Some(base());
    record.locks = vec![write_lock(b"k")];
    ctx.tx_records.set(&record).await.unwrap();
    store_entry(&ctx, b"k", locked_entry(b"k", &t)).await;
    ctx.hints.schedule(t.clone());

    check_hints_and_scan_page(&ctx).await;

    assert!(!is_gone(&ctx.tx_records, &t).await);
    let e = lookup_entry(&ctx, b"k").await.unwrap();
    assert_eq!(e.lock_holders(), std::slice::from_ref(&t));
}

// An aborted record past its tombstone lease has its recorded lock pruned
// (the vestigial entry removed) and is then deleted.
#[tokio::test(start_paused = true)]
async fn expired_aborted_prunes_locks_and_is_deleted() {
    let ctx = new_ctx().await;
    let t = tx(1);
    let mut record = TxRecord::new(t.clone(), TxCommitStatus::Aborted);
    record.timestamp = Some(base() - PAST_HORIZON);
    record.locks = vec![write_lock(b"k")];
    ctx.tx_records.set(&record).await.unwrap();
    store_entry(&ctx, b"k", locked_entry(b"k", &t)).await;
    ctx.hints.schedule(t.clone());

    check_hints_and_scan_page(&ctx).await;

    assert!(is_gone(&ctx.tx_records, &t).await);
    assert!(lookup_entry(&ctx, b"k").await.is_none());
}

// A shared hint for a direct-commit writer has no record and is a harmless no-op.
#[tokio::test(start_paused = true)]
async fn direct_gc_hint_is_a_noop() {
    let ctx = new_ctx().await;
    let t = tx(9);
    ctx.hints.schedule(t.clone());
    check_hints_and_scan_page(&ctx).await;
    assert!(is_gone(&ctx.tx_records, &t).await);
}

// ADR-029: GC's lock reclamation flows through the leaf coordinator
// (via the locker's unlock methods), so a GC release and a live disjoint-key
// acquire contending one leaf batch into a *single* CAS round instead of GC
// racing its own store. The release clears (and prunes) the dead holder's
// entry and the acquire installs its lock, all in one leaf write.
#[tokio::test(start_paused = true)]
async fn gc_release_merges_into_live_acquire_round() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let (backend, gate) = Gate::wrap(mem);
    let rec = Arc::new(RecordingBackend::new(backend));
    let record = rec.log();
    let ctx = new_ctx_with(rec).await;

    let ka = b"key-a".to_vec();
    let kb = same_leaf_sibling(&ka);
    let leaf_path = root_path();
    let leaf_path_string = leaf_path.to_string();

    // Seed both entries in the one shared leaf `_r`: a dead transaction holds
    // A's write lock (no committed writer), so GC's release will clear and
    // prune the now-vestigial entry; B exists committed, so the live
    // overwrite takes a Write lock (not a Create) and needs no membership
    // root lock — the round stays one leaf CAS.
    let dead = tx(1);
    let seed = tx(9);
    let leaf = LeafBody::from_entries([locked_entry(&ka, &dead), writer_entry(&kb, &seed)]);
    let loaded = ctx
        .nodes
        .load_leaf(
            &leaf_path,
            Requirement::after(ctx.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    let mut edit = loaded.into_edit();
    edit.set_entries(leaf);
    assert!(ctx.nodes.commit_leaf(edit).await.unwrap().is_applied());

    let mut dead_record = TxRecord::new(dead.clone(), TxCommitStatus::Aborted);
    dead_record.timestamp = Some(base() - PAST_HORIZON);
    dead_record.locks = vec![write_lock(&ka)];
    ctx.tx_records.set(&dead_record).await.unwrap();

    let live = TxId::with_priority(2_000_000_000, b"live");
    ctx.mon.begin_tx(&live);

    let before = count_stores(&record, &leaf_path_string);
    gate.arm();

    // Drive GC's release and the live acquire concurrently: the first becomes
    // the dedup driver and parks in the gated load; the second queues and
    // merges into its round.
    let gc = ctx.gc.clone();
    let dead2 = dead.clone();
    let release = tokio::spawn(async move { check_candidate(&gc, &dead2).await });
    let locker = ctx.locker.clone();
    let accesses = crate::access::AccessSet::new(
        Vec::new(),
        vec![crate::access::WriteAccess::put(
            key_path(&kb),
            Arc::from(&b"v2"[..]),
        )],
        Vec::new(),
    );
    let live2 = live.clone();
    let lock_requirement = Requirement::after(ctx.timeline.currentness_barrier());
    let acquire = tokio::spawn(async move {
        locker
            .keys()
            .lock_at(&live2, &accesses, false, lock_requirement)
            .await
    });

    // Under paused time this fires only once both tasks are parked (driver in
    // the gated load, the second queued); then release the load.
    rt::sleep(std::time::Duration::from_millis(50)).await;
    gate.release();

    release.await.unwrap().unwrap();
    let outcome = acquire.await.unwrap().unwrap();
    assert!(
        matches!(outcome, LockOutcome::Locked(_)),
        "the live acquire must lock"
    );

    assert_eq!(
        count_stores(&record, &leaf_path_string) - before,
        1,
        "GC release and the live acquire share a single leaf CAS"
    );
    // The dead holder's entry was cleared and, being vestigial, pruned; the
    // live acquirer holds B's lock.
    assert!(
        lookup_entry(&ctx, &ka).await.is_none(),
        "GC released and pruned the dead holder's entry"
    );
    assert_eq!(
        lookup_entry(&ctx, &kb).await.unwrap().lock_holders(),
        std::slice::from_ref(&live)
    );
}

/// Counts the CASes issued against `path`.
fn count_stores(op_log: &glassdb_backend::middleware::OpLog, path: &str) -> usize {
    op_log
        .lock()
        .unwrap()
        .iter()
        .filter(|r| r.path == path && (r.op == "write_if" || r.op == "write_if_not_exists"))
        .count()
}

/// A distinct key that shares the collection's single leaf `_r` with `base`
/// (ADR-031, split deferred), for exercising a GC release and a live acquire
/// contending one leaf object.
fn same_leaf_sibling(base: &[u8]) -> Vec<u8> {
    let sib = b"sibling".to_vec();
    assert_ne!(sib, base, "sibling must differ from the base key");
    sib
}

/// Controls a hook that skips one routing read, then gates the coordinator load.
struct Gate {
    notify: Arc<tokio::sync::Notify>,
    armed: std::sync::atomic::AtomicBool,
    skip: std::sync::atomic::AtomicUsize,
}

impl Gate {
    fn wrap(inner: Arc<dyn Backend>) -> (Arc<HookBackend>, Arc<Self>) {
        let gate = Arc::new(Self {
            notify: Arc::new(tokio::sync::Notify::new()),
            armed: std::sync::atomic::AtomicBool::new(false),
            skip: std::sync::atomic::AtomicUsize::new(0),
        });
        let backend = HookBackend::new(inner);
        backend.set_before({
            let gate = gate.clone();
            move |op| {
                use std::sync::atomic::Ordering::SeqCst;
                let wait = matches!(
                    op,
                    BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                ) && gate.armed.load(SeqCst)
                    && gate
                        .skip
                        .try_update(SeqCst, SeqCst, |n| n.checked_sub(1))
                        .is_err();
                if wait {
                    gate.armed.store(false, SeqCst);
                }
                let notify = gate.notify.clone();
                let future: HookFuture = Box::pin(async move {
                    if wait {
                        notify.notified().await;
                    }
                    Ok(())
                });
                future
            }
        });
        (backend, gate)
    }

    fn arm(&self) {
        self.skip.store(1, std::sync::atomic::Ordering::SeqCst);
        self.armed.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn release(&self) {
        self.notify.notify_one();
    }
}
#[tokio::test(start_paused = true)]
async fn cached_candidate_converges_after_a_peer_deletes_it() {
    use glassdb_backend::middleware::RecordingBackend;
    use glassdb_storage::CachedStore;

    let backend = RecordingBackend::new(Arc::new(MemoryBackend::new()));
    let operations = backend.log();
    let backend = Arc::new(backend);
    let ctx = new_ctx_with(backend.clone()).await;
    let id = tx(14);
    ctx.tx_records
        .set(&committed(id.clone(), PAST_HORIZON, &[], &[]))
        .await
        .unwrap();
    let peer = TxRecordStore::new(
        CachedStore::new(backend, 1 << 20, Timeline::new(), None),
        DbPrefix::try_from("db").unwrap(),
    );
    let observed = peer.get_at(&id, Requirement::ANY).await.unwrap();
    peer.delete(&observed).await.unwrap();
    operations.lock().unwrap().clear();

    assert_eq!(
        check_candidate(&ctx.gc, &id).await.unwrap(),
        GcOutcome::Reclaimed
    );
    assert!(
        operations
            .lock()
            .unwrap()
            .iter()
            .all(|operation| operation.op == "delete_if"),
        "a cached final candidate needs no presence check"
    );
    assert!(is_gone(&ctx.tx_records, &id).await);

    assert_eq!(
        check_candidate(&ctx.gc, &id).await.unwrap(),
        GcOutcome::Retained
    );
}

#[tokio::test(start_paused = true)]
async fn hinted_candidates_wait_without_restarting_the_deadline() {
    let ctx = new_ctx().await;
    let id = tx(11);
    ctx.tx_records
        .set(&committed(id.clone(), PAST_HORIZON, &[], &[]))
        .await
        .unwrap();
    let bg = Arc::new(glassdb_concurr::Background::new());
    ctx.gc.start(&bg);
    ctx.hints.schedule_all([id.clone(), id.clone()]);
    for _ in 0..64 {
        rt::yield_now().await;
    }
    assert!(!is_gone(&ctx.tx_records, &id).await);
    assert_eq!(ctx.gc.diagnostics().deferred, 1);
    let delay = ctx.gc.timing.pending_timeout() + ctx.gc.timing.max_clock_skew();
    tokio::time::advance(delay / 2).await;
    for _ in 0..64 {
        rt::yield_now().await;
    }
    ctx.hints.schedule(id.clone());
    for _ in 0..64 {
        rt::yield_now().await;
    }
    assert!(!is_gone(&ctx.tx_records, &id).await);
    tokio::time::advance(delay / 2).await;
    for _ in 0..64 {
        rt::yield_now().await;
    }
    assert!(is_gone(&ctx.tx_records, &id).await);
    let stats = ctx.gc.stats_and_reset();
    assert_eq!(stats.progress, 1);
    bg.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn concurrent_candidates_share_one_leaf_validation() {
    let backend = RecordingBackend::new(Arc::new(MemoryBackend::new()));
    let operations = backend.log();
    let ctx = new_ctx_with(Arc::new(backend)).await;
    for id in [tx(1), tx(2)] {
        ctx.tx_records
            .set(&committed(id.clone(), PAST_HORIZON, &[b"k"], &[b"k"]))
            .await
            .unwrap();
        ctx.hints.schedule(id);
    }
    store_entry(&ctx, b"k", writer_entry(b"k", &tx(3))).await;
    operations.lock().unwrap().clear();
    let bg = Background::new();
    ctx.gc.start(&bg);
    wait_for_hint_deadline(&ctx).await;
    bg.shutdown().await;

    assert_eq!(ctx.gc.stats_and_reset().progress, 2);
    let root = root_path().to_string();
    let operations = operations.lock().unwrap();
    assert_eq!(
        operations
            .iter()
            .filter(|op| op.path == root && matches!(op.op, "read" | "read_if_modified"))
            .count(),
        1,
        "concurrent checks must reuse the same validated leaf"
    );
}

#[tokio::test(start_paused = true)]
async fn young_final_candidates_complete_without_a_reference_pass() {
    let backend = RecordingBackend::new(Arc::new(MemoryBackend::new()));
    let operations = backend.log();
    let ctx = new_ctx_with(Arc::new(backend)).await;
    let ids = vec![tx(1), tx(2)];
    for (id, status) in ids
        .iter()
        .zip([TxCommitStatus::Committed, TxCommitStatus::Aborted])
    {
        let mut record = committed(id.clone(), Duration::ZERO, &[b"k"], &[b"k"]);
        record.status = status;
        ctx.tx_records.set(&record).await.unwrap();
    }
    operations.lock().unwrap().clear();

    let outcomes = ctx
        .gc
        .clone()
        .check_batch(ids.clone(), NonZeroUsize::new(2).unwrap())
        .now_or_never()
        .expect("ineligible cached candidates must not schedule reference work");

    assert_eq!(outcomes.len(), ids.len());
    for ((id, outcome), expected) in outcomes.into_iter().zip(ids) {
        assert_eq!(id, expected);
        assert!(matches!(outcome, Ok(GcOutcome::Deferred(delay)) if !delay.is_zero()));
    }
    assert!(operations.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn reference_checks_reuse_a_concurrent_writer_observation() {
    let backend = RecordingBackend::new(Arc::new(MemoryBackend::new()));
    let operations = backend.log();
    let ctx = new_ctx_with(Arc::new(backend)).await;
    let id = tx(1);
    ctx.tx_records
        .set(&committed(id.clone(), PAST_HORIZON, &[b"k"], &[b"k"]))
        .await
        .unwrap();
    store_entry(&ctx, b"k", writer_entry(b"k", &tx(2))).await;
    operations.lock().unwrap().clear();

    // Poll GC first so the writer refreshes the leaf after the reference bound.
    let (outcomes, ()) = tokio::join!(
        biased;
        ctx.gc.clone().check_batch(vec![id.clone()], NonZeroUsize::MIN),
        store_entry(&ctx, b"k", writer_entry(b"k", &tx(3))),
    );

    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].1.as_ref().unwrap(), &GcOutcome::Reclaimed);
    assert!(is_gone(&ctx.tx_records, &id).await);
    let root = root_path().to_string();
    assert_eq!(
        operations
            .lock()
            .unwrap()
            .iter()
            .filter(|op| op.path == root && matches!(op.op, "read" | "read_if_modified"))
            .count(),
        1,
        "GC must reuse the writer's observation without another leaf read"
    );
}

#[tokio::test(start_paused = true)]
async fn reference_checks_follow_candidate_filtering() {
    let backend = Arc::new(MemoryBackend::new());
    let hooked = HookBackend::new(backend.clone());
    let ctx = new_ctx_with(hooked.clone()).await;
    let id = tx(2);
    let mut record = committed(id.clone(), Duration::ZERO, &[b"k"], &[b"k"]);
    record.status = TxCommitStatus::Pending;
    ctx.tx_records.set(&record).await.unwrap();
    store_entry(&ctx, b"k", writer_entry(b"k", &tx(3))).await;
    let entered = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    hooked.set_before({
        let path = ObjectPath::Transaction {
            db_prefix: DbPrefix::try_from("db").unwrap(),
            id: id.clone(),
        }
        .to_string();
        let entered = entered.clone();
        let resume = resume.clone();
        move |op| {
            let pause = op.path() == path
                && matches!(
                    op,
                    BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                );
            let entered = entered.clone();
            let resume = resume.clone();
            Box::pin(async move {
                if pause {
                    entered.notify_one();
                    resume.notified().await;
                }
                Ok(())
            })
        }
    });
    ctx.hints.schedule(id.clone());
    let bg = Background::new();
    ctx.gc.start(&bg);
    entered.notified().await;

    // Cache absence after the status bound, before the peer's commit. A
    // GC check using that first bound would wrongly accept this leaf.
    ctx.nodes
        .load_leaf(
            &root_path(),
            Requirement::after(ctx.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    let peer = AssemblyFixture::new(
        backend,
        DbPrefix::try_from("db").unwrap(),
        &EngineConfig::default(),
    );
    let mut edit = peer
        .nodes
        .load_leaf(&root_path(), Requirement::ANY)
        .await
        .unwrap()
        .into_edit();
    edit.set_entries(LeafBody::from_entries([locked_entry(b"k", &id)]));
    assert!(peer.nodes.commit_leaf(edit).await.unwrap().is_applied());
    record.status = TxCommitStatus::Committed;
    // Native paused time does not advance wall time. The old timestamp stands
    // for a filter delayed until after this commit's safety horizon.
    record.timestamp = Some(base() - PAST_HORIZON);
    let pending = peer.tx_records.get_at(&id, Requirement::ANY).await.unwrap();
    peer.tx_records.set_if(&record, &pending).await.unwrap();
    resume.notify_one();
    for _ in 0..64 {
        rt::yield_now().await;
    }
    bg.shutdown().await;

    let stats = ctx.gc.stats_and_reset();
    assert_eq!(stats.progress, 0);
    assert_eq!(stats.failures, 0);
    let diagnostics = ctx.gc.diagnostics();
    assert_eq!(diagnostics.ready, 0);
    assert_eq!(diagnostics.deferred, 0);
    assert_eq!(diagnostics.in_flight, 0);
    assert!(!is_gone(&ctx.tx_records, &id).await);
    assert!(lookup_entry(&ctx, b"k").await.unwrap().is_locked_by(&id));
}

#[tokio::test(start_paused = true)]
async fn a_transient_failure_keeps_the_candidate_until_retry() {
    let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
    let ctx = new_ctx_with_interval(backend.clone(), Duration::from_millis(10)).await;
    let id = tx(12);
    ctx.tx_records
        .set(&committed(id.clone(), PAST_HORIZON, &[], &[]))
        .await
        .unwrap();
    let failed = Arc::new(AtomicBool::new(false));
    backend.set_before(move |op| {
        let fail = matches!(op, BackendOp::DeleteIf { .. }) && !failed.swap(true, Ordering::SeqCst);
        Box::pin(async move {
            if fail {
                Err(BackendError::Unavailable("retry".into()))
            } else {
                Ok(())
            }
        })
    });
    let bg = Arc::new(glassdb_concurr::Background::new());
    ctx.gc.start(&bg);
    ctx.hints.schedule(id.clone());
    wait_for_hint_deadline(&ctx).await;
    assert!(!is_gone(&ctx.tx_records, &id).await);
    assert_eq!(ctx.gc.diagnostics().deferred, 1);
    for _ in 0..20 {
        tokio::time::advance(Duration::from_millis(2)).await;
        for _ in 0..8 {
            rt::yield_now().await;
        }
    }
    assert!(is_gone(&ctx.tx_records, &id).await);
    let stats = ctx.gc.stats_and_reset();
    assert_eq!(stats.failures, 1);
    assert_eq!(stats.progress, 1);
    bg.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_ready_hint_queue_does_not_starve_the_scan() {
    let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
    let ctx = new_ctx_with_interval(backend.clone(), Duration::from_millis(10)).await;
    let id = tx(13);
    ctx.tx_records
        .set(&committed(id.clone(), PAST_HORIZON, &[], &[]))
        .await
        .unwrap();
    let reclaimed_with_ready_hints = Arc::new(AtomicBool::new(false));
    backend.set_before({
        let counters = ctx.hints.counters.clone();
        let reclaimed_with_ready_hints = reclaimed_with_ready_hints.clone();
        move |op| {
            if matches!(op, BackendOp::DeleteIf { .. }) {
                reclaimed_with_ready_hints
                    .store(counters.diagnostics().ready > 0, Ordering::SeqCst);
            }
            Box::pin(async { Ok(()) })
        }
    });
    let bg = Arc::new(glassdb_concurr::Background::new());
    ctx.hints
        .schedule_all((0u64..2000).map(|i| TxId::from_bytes(i.to_be_bytes().to_vec())));
    ctx.gc.start(&bg);
    for _ in 0..64 {
        rt::yield_now().await;
    }
    tokio::time::advance(
        ctx.gc.timing.pending_timeout() + ctx.gc.timing.max_clock_skew() + Duration::from_millis(1),
    )
    .await;
    for _ in 0..64 {
        rt::yield_now().await;
    }
    assert!(ctx.gc.stats_and_reset().lists > 0);
    assert!(is_gone(&ctx.tx_records, &id).await);
    assert!(reclaimed_with_ready_hints.load(Ordering::SeqCst));
    bg.shutdown().await;
}

#[test]
fn hint_producers_share_capacity_and_keep_the_latest_reports() {
    for capacity in [0, 1, 3] {
        let hints = GcHints::new(GcLimits {
            max_pending_hints: capacity,
            max_hint_candidates: 0,
        });
        let clone = hints.clone();
        let ids: Vec<_> = (0..5).map(tx).collect();
        hints.schedule_all(ids[..2].iter().cloned());
        clone.schedule_all(ids[2..].iter().cloned());
        let (reports, more) = hints.drain();
        assert_eq!(reports, ids[ids.len() - capacity..]);
        assert!(!more);
        assert_eq!(
            hints.counters.take().dropped_candidates,
            (ids.len() - capacity) as u64
        );
        assert!(clone.drain().0.is_empty());
    }
}

#[test]
fn candidate_capacity_is_preserved_across_deferrals_and_completion() {
    for capacity in [1, 3, HINT_CAPACITY] {
        let now = rt::Instant::now();
        let counters = Counters::default();
        let mut candidates = Candidates::new(
            Duration::ZERO,
            GcLimits {
                max_hint_candidates: capacity,
                ..GcLimits::default()
            },
        );
        let ids: Vec<_> = (0u64..capacity as u64)
            .map(|i| TxId::from_bytes(i.to_be_bytes().to_vec()))
            .collect();
        for id in &ids {
            candidates.admit(id.clone(), false, now, &counters);
        }
        let overflow = TxId::from_bytes(b"overflow".to_vec());
        candidates.admit(overflow.clone(), false, now, &counters);
        assert_eq!(counters.take().dropped_candidates, 1);
        for _ in 0..capacity {
            let id = candidates.take_ready().unwrap();
            candidates.admit(overflow.clone(), false, now, &counters);
            assert_eq!(counters.take().dropped_candidates, 1);
            let candidate = candidates.complete(&id);
            candidates.defer(id, candidate, now + Duration::from_secs(1));
        }
        candidates.record_backlog(&counters);
        assert_eq!(counters.diagnostics().deferred, capacity as u64);
        assert_eq!(counters.diagnostics().ready, 0);
        candidates.admit(overflow.clone(), false, now, &counters);
        assert_eq!(counters.take().dropped_candidates, 1);

        candidates.promote_due(now + Duration::from_secs(1));
        candidates.record_backlog(&counters);
        let promoted = capacity.min(ADMISSION_BATCH);
        assert_eq!(counters.diagnostics().ready, promoted as u64);
        assert_eq!(
            counters.diagnostics().deferred,
            (capacity - promoted) as u64
        );
        let id = candidates.take_ready().unwrap();
        candidates.complete(&id);
        candidates.admit(overflow, false, now, &counters);
        assert_eq!(counters.take().dropped_candidates, 0);
        assert_eq!(candidates.oldest_ready(), Some(now));
    }
}

#[test]
fn scan_reports_preserve_candidates_in_each_scheduling_state() {
    let now = rt::Instant::now();
    let counters = Counters::default();
    let mut candidates = Candidates::default();
    let hint = tx(1);
    let ready_scan = tx(2);
    let deferred_scan = tx(3);
    candidates.admit(hint.clone(), false, now, &counters);
    candidates.admit(ready_scan.clone(), false, now, &counters);
    candidates.admit(ready_scan.clone(), true, now, &counters);
    assert_eq!(candidates.take_ready(), Some(ready_scan.clone()));
    candidates.complete(&ready_scan);

    let running = candidates.take_ready().unwrap();
    assert_eq!(running, hint);
    candidates.admit(running.clone(), true, now, &counters);
    let candidate = candidates.complete(&running);
    assert!(!candidate.reported_again);
    candidates.defer(running.clone(), candidate, now + Duration::from_secs(1));

    candidates.admit(deferred_scan.clone(), false, now, &counters);
    assert_eq!(candidates.take_ready(), Some(deferred_scan.clone()));
    let candidate = candidates.complete(&deferred_scan);
    candidates.defer(
        deferred_scan.clone(),
        candidate,
        now + Duration::from_secs(2),
    );
    candidates.admit(deferred_scan.clone(), true, now, &counters);
    candidates.admit(deferred_scan.clone(), false, now, &counters);
    candidates.record_backlog(&counters);
    assert_eq!(counters.diagnostics().in_flight, 0);
    assert_eq!(counters.diagnostics().deferred, 2);
    candidates.promote_due(now + Duration::from_secs(1));
    assert_eq!(candidates.take_ready(), Some(running.clone()));
    assert!(!candidates.complete(&running).reported_again);
    assert_eq!(candidates.take_ready(), None);
    candidates.promote_due(now + Duration::from_secs(2));
    assert_eq!(candidates.take_ready(), Some(deferred_scan.clone()));
    candidates.complete(&deferred_scan);
    candidates.record_backlog(&counters);
    assert_eq!(counters.diagnostics(), GcDiagnostics::default());
    assert_eq!(candidates.next_due(), None);
}

#[tokio::test(start_paused = true)]
async fn only_a_hint_during_a_check_requests_a_delayed_follow_up() {
    let ctx = new_ctx().await;
    let mut scheduler = Scheduler::new(&ctx.gc);
    let now = rt::Instant::now();
    let id = tx(20);
    scheduler
        .candidates
        .admit(id.clone(), true, now, &scheduler.counters);
    assert_eq!(scheduler.candidates.take_ready(), Some(id.clone()));
    scheduler
        .candidates
        .admit(id.clone(), true, now, &scheduler.counters);
    scheduler.complete_checks(vec![(id.clone(), Ok(GcOutcome::Retained))]);
    assert_eq!(scheduler.candidates.next_due(), None);

    scheduler
        .candidates
        .admit(id.clone(), true, now, &scheduler.counters);
    assert_eq!(scheduler.candidates.take_ready(), Some(id.clone()));
    scheduler
        .candidates
        .admit(id.clone(), false, now, &scheduler.counters);
    scheduler.complete_checks(vec![(id.clone(), Ok(GcOutcome::Retained))]);
    let delay = ctx.gc.timing.pending_timeout() + ctx.gc.timing.max_clock_skew();
    let due = now + delay;
    assert_eq!(scheduler.candidates.next_due(), Some(due));
    scheduler.candidates.admit(
        id.clone(),
        false,
        now + Duration::from_secs(1),
        &scheduler.counters,
    );
    assert_eq!(scheduler.candidates.next_due(), Some(due));
    scheduler
        .candidates
        .promote_due(now + (delay - Duration::from_nanos(1)));
    assert_eq!(scheduler.candidates.take_ready(), None);
    scheduler.candidates.promote_due(due);
    assert_eq!(scheduler.candidates.take_ready(), Some(id));
}

#[test]
fn scan_admission_does_not_advance_a_hint_deadline() {
    let now = rt::Instant::now();
    let delay = Duration::from_secs(45);
    let mut candidates = Candidates::new(delay, GcLimits::default());
    let counters = Counters::default();
    let id = tx(21);
    candidates.admit(id.clone(), false, now, &counters);
    candidates.admit(id.clone(), true, now + delay / 2, &counters);
    assert_eq!(candidates.next_due(), Some(now + delay));
    assert_eq!(candidates.take_ready(), None);
    candidates.promote_due(now + delay);
    assert_eq!(candidates.take_ready(), Some(id));
}
