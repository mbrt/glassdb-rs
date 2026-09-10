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
use glassdb_backend::middleware::{BackendOp, HookBackend, HookFuture, RecordingBackend};
use glassdb_backend::{Backend, BackendError, memory::MemoryBackend};
use glassdb_concurr::RetryConfig;
use glassdb_data::{CollectionAddress, CollectionId, DbRoot, LogicalKey, ObjectPath};
use glassdb_storage::transaction::{
    TxCollectionChange, TxCollectionOp, TxCommitStatus, TxLock, TxLog, TxWrite,
};
use glassdb_storage::{
    CollectionRecord, CollectionStore, CurrentState, LeafBody, LeafEntry, LockType, Node,
    Requirement, Timeline, TreeRouter,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

struct UnexpectedTopologySettler;

struct NoSplitHints;

impl SplitHinter for NoSplitHints {
    fn observe_leaf(&self, _path: &ObjectPath, _leaf: &LeafBody) {}
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
    tl: TLogger,
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

    // Cover routing, key-lock CAS, and nested directory-lock CAS races.
    for phase in 0..3 {
        let backend = Arc::new(MemoryBackend::new());
        let peer = new_ctx_with(backend.clone()).await;
        let hooked = HookBackend::new(backend);
        let config = EngineConfig::default();
        let db_root = DbRoot::try_from("db").unwrap();
        let owner = AssemblyFixture::new(hooked.clone(), db_root.clone(), &config);
        let engine = engine_fixture(&owner, db_root, config, false);
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
        hooked.set_before({
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
            move |operation| {
                let matches_kind = if phase == 0 {
                    matches!(
                        operation,
                        BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                    )
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
                            .load_record(&prepared, Requirement::AtLeast(timeline.now()))
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
        });

        assert_eq!(
            engine.algo.commit(&mut handle).await.unwrap(),
            BodyDecision::ReplayBody
        );
        assert!(triggered.load(Ordering::SeqCst));
        assert_ne!(handle.id(), &old_id);
        assert_eq!(
            owner
                .tlogger
                .commit_status_at(&old_id, Requirement::Any)
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
    let foundation = AssemblyFixture::new(backend, DbRoot::try_from("db").unwrap(), &config);
    let tl = foundation.tlogger.clone();
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
            tl.clone(),
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
        tl.clone(),
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
    );
    Ctx {
        gc,
        hints,
        tl,
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
    TxLock::Entry {
        key: key_path(k),
        typ: LockType::Write,
    }
}

async fn store_entry(ctx: &Ctx, _key: &[u8], entry: LeafEntry) {
    let path = root_path();
    let loaded = ctx
        .nodes
        .load_leaf(&path, Requirement::AtLeast(ctx.timeline.now()))
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
    assert!(ctx.nodes.commit_leaf(edit).await.unwrap());
}

async fn lookup_entry(ctx: &Ctx, key: &[u8]) -> Option<LeafEntry> {
    let loaded = ctx
        .nodes
        .load_leaf(&root_path(), Requirement::AtLeast(ctx.timeline.now()))
        .await
        .unwrap();
    loaded.entries().lookup(key).cloned()
}

fn committed(id: TxId, offset: Duration, writes: &[&[u8]], locks: &[&[u8]]) -> TxLog {
    TxLog {
        id,
        timestamp: Some(base() - offset),
        status: TxCommitStatus::Ok,
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

async fn is_gone(tl: &TLogger, id: &TxId) -> bool {
    matches!(
        tl.get_at(id, Requirement::Any).await,
        Err(StorageError::NotFound)
    )
}

async fn check_candidate(gc: &Gc, tid: &TxId) -> Result<GcOutcome, TransError> {
    let status = gc
        .filter_candidate(tid, Requirement::AtLeast(gc.timeline.now()))
        .await?;
    let requirement = Requirement::AtLeast(gc.timeline.now());
    match status {
        GcEligibility::Ready(observation) => gc.try_reclaim(tid, &observation, requirement).await,
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
        .tl
        .scan_transaction_ids(0, 0, None, backend::ListLimit::new(1000).unwrap())
        .await
        .unwrap();
    candidates.extend(page.ids);
    let candidates: BTreeSet<_> = candidates.into_iter().collect();
    for tid in candidates {
        let _ = check_candidate(&ctx.gc, &tid).await;
    }
}

// A committed object whose written key has since been overwritten by a newer
// writer holds no reference and is swept. Fed via the paged list (no hint),
// so the list feed is exercised too.
#[tokio::test(start_paused = true)]
async fn committed_unreferenced_is_collected() {
    let ctx = new_ctx().await;
    let (old, new) = (tx(1), tx(2));
    ctx.tl
        .set(&committed(old.clone(), PAST_HORIZON, &[b"k"], &[b"k"]))
        .await
        .unwrap();
    // The key now points at a newer writer, not `old`.
    store_entry(&ctx, b"k", writer_entry(b"k", &new)).await;

    check_hints_and_scan_page(&ctx).await;

    assert!(is_gone(&ctx.tl, &old).await);
    assert_eq!(
        ctx.coord.stats_and_reset().submissions,
        0,
        "an unreferenced committed object needs no key-lock release"
    );
}

// ADR-051: the successor that superseded a committed writer may be a logless
// commit with no transaction object of its own. Liveness is decided by the
// entry's writer id, not by that successor's existence, so the predecessor is
// still swept.
#[tokio::test(start_paused = true)]
async fn committed_superseded_by_a_logless_writer_is_collected() {
    let ctx = new_ctx().await;
    let (old, logless) = (tx(1), tx(2));
    ctx.tl
        .set(&committed(old.clone(), PAST_HORIZON, &[b"k"], &[]))
        .await
        .unwrap();
    store_entry(
        &ctx,
        b"k",
        LeafEntry::new(b"k").with_current(CurrentState::Inline {
            writer: logless.clone(),
            value: Arc::from(&b"v2"[..]),
        }),
    )
    .await;

    check_hints_and_scan_page(&ctx).await;

    assert!(is_gone(&ctx.tl, &old).await);
    assert!(
        is_gone(&ctx.tl, &logless).await,
        "the logless successor never had an object to collect"
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
    let mut log = committed(id.clone(), PAST_HORIZON, &[], &[]);
    log.prepared_collections.push(prepared.clone());
    ctx.tl.set(&log).await.unwrap();

    check_hints_and_scan_page(&ctx).await;

    assert!(is_gone(&ctx.tl, &id).await);
    assert!(matches!(
        ctx.nodes.load_root(&prepared, Requirement::Any).await,
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
    let mut log = committed(id.clone(), PAST_HORIZON, &[], &[]);
    log.status = TxCommitStatus::Aborted;
    log.prepared_collections.push(prepared.clone());
    ctx.tl.set(&log).await.unwrap();
    ctx.hints.schedule(id.clone());

    check_hints_and_scan_page(&ctx).await;

    assert!(is_gone(&ctx.tl, &id).await);
    assert!(matches!(
        ctx.nodes.load_root(&prepared, Requirement::Any).await,
        Err(StorageError::NotFound)
    ));
    assert!(matches!(
        ctx.records.load_record(&prepared, Requirement::Any).await,
        Err(StorageError::NotFound)
    ));
}

#[tokio::test(start_paused = true)]
async fn collection_cleanup_conflict_keeps_the_recovery_manifest() {
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
    let mut log = committed(id.clone(), PAST_HORIZON, &[], &[]);
    log.prepared_collections.push(prepared.clone());
    ctx.tl.set(&log).await.unwrap();

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
        !is_gone(&ctx.tl, &id).await,
        "cleanup conflicts must retain the only durable orphan manifest"
    );
    assert!(
        ctx.records
            .load_record(&prepared, Requirement::Any)
            .await
            .is_ok()
    );

    backend.clear_before();
    ctx.hints.schedule(id.clone());
    check_hints_and_scan_page(&ctx).await;
    assert!(is_gone(&ctx.tl, &id).await);
    assert!(matches!(
        ctx.nodes.load_root(&prepared, Requirement::Any).await,
        Err(StorageError::NotFound)
    ));
}

#[tokio::test(start_paused = true)]
async fn committed_drop_is_recovered_while_the_log_stores_a_live_value() {
    let ctx = new_ctx().await;
    let id = tx(1);
    let child = CollectionAddress::new(
        "db",
        CollectionId::from_slice(&[8; 16]).expect("fixed ID has the required width"),
    );

    store_entry(&ctx, b"k", writer_entry(b"k", &id)).await;
    let (mut parent_record, parent_observed) = ctx
        .records
        .load_record(&collection(), Requirement::AtLeast(ctx.timeline.now()))
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
    child_node.set_collection_delete_intent(id.clone());
    assert!(ctx.nodes.create_root(&child, &child_node).await.unwrap());

    let mut log = committed(id.clone(), PAST_HORIZON, &[b"k"], &[]);
    log.locks.extend([
        TxLock::Directory {
            collection: collection(),
            typ: LockType::Write,
        },
        TxLock::Directory {
            collection: child.clone(),
            typ: LockType::Read,
        },
    ]);
    log.collection_changes.push(TxCollectionChange {
        parent: collection(),
        name: b"child".to_vec(),
        collection: child.clone(),
        op: TxCollectionOp::Drop,
    });
    ctx.tl.set(&log).await.unwrap();
    ctx.hints.schedule(id.clone());

    check_hints_and_scan_page(&ctx).await;

    assert!(
        !is_gone(&ctx.tl, &id).await,
        "the live root value still needs its transaction object"
    );
    let (parent_record, _) = ctx
        .records
        .load_record(&collection(), Requirement::AtLeast(ctx.timeline.now()))
        .await
        .unwrap();
    assert_eq!(parent_record.child(b"child"), None);
    assert!(!parent_record.directory_lock().contains(&id));
    assert!(matches!(
        ctx.nodes.load_root(&child, Requirement::Any).await,
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
    let mut log = TxLog::new(id.clone(), TxCommitStatus::Ok);
    log.timestamp = Some(base() - PAST_HORIZON);
    log.writes.push(TxWrite {
        key: key.clone(),
        value: Arc::from(&b"v"[..]),
        deleted: false,
        prev_writer: TxId::default(),
    });
    log.locks.push(TxLock::Entry {
        key,
        typ: LockType::Write,
    });
    ctx.tl.set(&log).await.unwrap();
    ctx.hints.schedule(id.clone());

    check_hints_and_scan_page(&ctx).await;

    assert!(is_gone(&ctx.tl, &id).await);
}

// A committed object still named by its key's `current_writer` is the live
// value: it must never be collected.
#[tokio::test(start_paused = true)]
async fn committed_still_referenced_is_kept() {
    let ctx = new_ctx().await;
    let t = tx(1);
    ctx.tl
        .set(&committed(t.clone(), PAST_HORIZON, &[b"k"], &[]))
        .await
        .unwrap();
    store_entry(&ctx, b"k", writer_entry(b"k", &t)).await;
    ctx.hints.schedule(t.clone());

    check_hints_and_scan_page(&ctx).await;

    let log = ctx.tl.get_at(&t, Requirement::Any).await.unwrap();
    let log = log.value().unwrap();
    assert_eq!(log.status, TxCommitStatus::Ok);
}

#[tokio::test(start_paused = true)]
async fn committed_entry_lock_keeps_the_log_and_lock() {
    let ctx = new_ctx().await;
    let id = tx(1);
    ctx.tl
        .set(&committed(id.clone(), PAST_HORIZON, &[b"k"], &[b"k"]))
        .await
        .unwrap();
    store_entry(&ctx, b"k", locked_entry(b"k", &id)).await;

    check_hints_and_scan_page(&ctx).await;

    assert!(!is_gone(&ctx.tl, &id).await);
    assert_eq!(
        lookup_entry(&ctx, b"k").await.unwrap().lock_holders(),
        std::slice::from_ref(&id)
    );
    assert_eq!(ctx.coord.stats_and_reset().submissions, 0);
}

#[tokio::test(start_paused = true)]
async fn committed_membership_lock_is_released_before_deletion() {
    let ctx = new_ctx().await;
    let id = tx(1);
    let mut log = committed(id.clone(), PAST_HORIZON, &[b"k"], &[b"k"]);
    log.locks.push(TxLock::Membership {
        leaf: glassdb_data::LeafRef::root(collection()),
        typ: LockType::Write,
    });
    ctx.tl.set(&log).await.unwrap();
    store_entry(&ctx, b"k", writer_entry(b"k", &tx(2))).await;
    let loaded = ctx
        .nodes
        .load_leaf(&root_path(), Requirement::Any)
        .await
        .unwrap();
    let mut locks = loaded.locks().clone();
    locks.set_membership_writer(id.clone());
    let mut edit = loaded.into_edit();
    edit.set_locks(locks);
    assert!(ctx.nodes.commit_leaf(edit).await.unwrap());

    check_hints_and_scan_page(&ctx).await;

    assert!(is_gone(&ctx.tl, &id).await);
    let loaded = ctx
        .nodes
        .load_leaf(&root_path(), Requirement::AtLeast(ctx.timeline.now()))
        .await
        .unwrap();
    assert!(loaded.node().membership_lock().holders().is_empty());
    assert_eq!(ctx.coord.stats_and_reset().submissions, 1);
}

#[tokio::test(start_paused = true)]
async fn pending_and_wounded_candidates_only_read_their_logs() {
    for (status, age) in [TxCommitStatus::Pending, TxCommitStatus::Wounded]
        .into_iter()
        .flat_map(|status| [Duration::ZERO, PAST_HORIZON].map(|age| (status, age)))
    {
        let backend = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let operations = backend.log();
        let ctx = new_ctx_with(Arc::new(backend)).await;
        let id = tx(1);
        let mut log = TxLog::new(id.clone(), status);
        log.timestamp = Some(base() - age);
        log.locks = vec![write_lock(b"k")];
        ctx.tl.set(&log).await.unwrap();
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
            db_root: DbRoot::try_from("db").unwrap(),
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
                "pending and wounded candidates must only read their logs"
            );
        }
        let stats = ctx.gc.stats_and_reset();
        assert_eq!(stats.progress, 0);
        assert_eq!(stats.failures, 0);
        let diagnostics = ctx.gc.diagnostics();
        assert_eq!(diagnostics.ready, 0);
        assert_eq!(diagnostics.deferred, 0);
        assert_eq!(diagnostics.in_flight, 0);
        let got = ctx.tl.get_at(&id, Requirement::Any).await.unwrap();
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
    let mut log = TxLog::new(wounded.clone(), TxCommitStatus::Wounded);
    log.timestamp = Some(base() - PAST_HORIZON);
    log.locks = vec![write_lock(b"k")];
    ctx.tl.set(&log).await.unwrap();
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
            Requirement::AtLeast(ctx.timeline.now()),
        )
        .await
        .unwrap();
    assert!(matches!(outcome, LockOutcome::Locked(_)));
    let entry = lookup_entry(&ctx, b"k").await.unwrap();
    assert!(!entry.is_locked_by(&wounded));
    assert!(entry.is_locked_by(&contender));
    check_hints_and_scan_page(&ctx).await;
    let got = ctx.tl.get_at(&wounded, Requirement::Any).await.unwrap();
    assert_eq!(got.value().unwrap().status, TxCommitStatus::Wounded);
}

// An acknowledged aborted object still within its cleanup horizon keeps its
// locks and remains available to ordinary late observers.
#[tokio::test(start_paused = true)]
async fn recent_aborted_tombstone_is_kept() {
    let ctx = new_ctx().await;
    let t = tx(1);
    let mut log = TxLog::new(t.clone(), TxCommitStatus::Aborted);
    log.timestamp = Some(base());
    log.locks = vec![write_lock(b"k")];
    ctx.tl.set(&log).await.unwrap();
    store_entry(&ctx, b"k", locked_entry(b"k", &t)).await;
    ctx.hints.schedule(t.clone());

    check_hints_and_scan_page(&ctx).await;

    assert!(!is_gone(&ctx.tl, &t).await);
    let e = lookup_entry(&ctx, b"k").await.unwrap();
    assert_eq!(e.lock_holders(), std::slice::from_ref(&t));
}

// An aborted object past its tombstone lease has its recorded lock pruned
// (the vestigial entry removed) and is then deleted.
#[tokio::test(start_paused = true)]
async fn expired_aborted_prunes_locks_and_is_deleted() {
    let ctx = new_ctx().await;
    let t = tx(1);
    let mut log = TxLog::new(t.clone(), TxCommitStatus::Aborted);
    log.timestamp = Some(base() - PAST_HORIZON);
    log.locks = vec![write_lock(b"k")];
    ctx.tl.set(&log).await.unwrap();
    store_entry(&ctx, b"k", locked_entry(b"k", &t)).await;
    ctx.hints.schedule(t.clone());

    check_hints_and_scan_page(&ctx).await;

    assert!(is_gone(&ctx.tl, &t).await);
    assert!(lookup_entry(&ctx, b"k").await.is_none());
}

// A shared hint for a logless writer has no object and is a harmless no-op.
#[tokio::test(start_paused = true)]
async fn logless_cleanup_hint_is_a_noop() {
    let ctx = new_ctx().await;
    let t = tx(9);
    ctx.hints.schedule(t.clone());
    check_hints_and_scan_page(&ctx).await;
    assert!(is_gone(&ctx.tl, &t).await);
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
    let log = rec.log();
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
        .load_leaf(&leaf_path, Requirement::AtLeast(ctx.timeline.now()))
        .await
        .unwrap();
    let mut edit = loaded.into_edit();
    edit.set_entries(leaf);
    assert!(ctx.nodes.commit_leaf(edit).await.unwrap());

    let mut dead_log = TxLog::new(dead.clone(), TxCommitStatus::Aborted);
    dead_log.timestamp = Some(base() - PAST_HORIZON);
    dead_log.locks = vec![write_lock(&ka)];
    ctx.tl.set(&dead_log).await.unwrap();

    let live = TxId::with_priority(2_000_000_000, b"live");
    ctx.mon.begin_tx(&live);

    let before = count_stores(&log, &leaf_path_string);
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
    let lock_requirement = Requirement::AtLeast(ctx.timeline.now());
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
        count_stores(&log, &leaf_path_string) - before,
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

/// Counts the CAS stores (conditional write / create) issued against `path`.
fn count_stores(log: &glassdb_backend::middleware::OpLog, path: &str) -> usize {
    log.lock()
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
    ctx.tl
        .set(&committed(id.clone(), PAST_HORIZON, &[], &[]))
        .await
        .unwrap();
    let peer = TLogger::new(
        CachedStore::new(backend, 1 << 20, Timeline::new(), None),
        DbRoot::try_from("db").unwrap(),
    );
    let observed = peer.get_at(&id, Requirement::Any).await.unwrap();
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
    assert!(is_gone(&ctx.tl, &id).await);

    assert_eq!(
        check_candidate(&ctx.gc, &id).await.unwrap(),
        GcOutcome::Retained
    );
}

#[tokio::test(start_paused = true)]
async fn hinted_candidates_wait_without_restarting_the_deadline() {
    let ctx = new_ctx().await;
    let id = tx(11);
    ctx.tl
        .set(&committed(id.clone(), PAST_HORIZON, &[], &[]))
        .await
        .unwrap();
    let bg = Arc::new(glassdb_concurr::Background::new());
    ctx.gc.start(&bg);
    ctx.hints.schedule_all([id.clone(), id.clone()]);
    for _ in 0..64 {
        rt::yield_now().await;
    }
    assert!(!is_gone(&ctx.tl, &id).await);
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
    assert!(!is_gone(&ctx.tl, &id).await);
    tokio::time::advance(delay / 2).await;
    for _ in 0..64 {
        rt::yield_now().await;
    }
    assert!(is_gone(&ctx.tl, &id).await);
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
        ctx.tl
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
        .zip([TxCommitStatus::Ok, TxCommitStatus::Aborted])
    {
        let mut log = committed(id.clone(), Duration::ZERO, &[b"k"], &[b"k"]);
        log.status = status;
        ctx.tl.set(&log).await.unwrap();
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
    ctx.tl
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
    assert!(is_gone(&ctx.tl, &id).await);
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
    let mut log = committed(id.clone(), Duration::ZERO, &[b"k"], &[b"k"]);
    log.status = TxCommitStatus::Pending;
    ctx.tl.set(&log).await.unwrap();
    store_entry(&ctx, b"k", writer_entry(b"k", &tx(3))).await;
    let entered = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    hooked.set_before({
        let path = ObjectPath::Transaction {
            db_root: DbRoot::try_from("db").unwrap(),
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
    // reference check using that first bound would wrongly accept this leaf.
    ctx.nodes
        .load_leaf(&root_path(), Requirement::AtLeast(ctx.timeline.now()))
        .await
        .unwrap();
    let peer = AssemblyFixture::new(
        backend,
        DbRoot::try_from("db").unwrap(),
        &EngineConfig::default(),
    );
    let mut edit = peer
        .nodes
        .load_leaf(&root_path(), Requirement::Any)
        .await
        .unwrap()
        .into_edit();
    edit.set_entries(LeafBody::from_entries([locked_entry(b"k", &id)]));
    assert!(peer.nodes.commit_leaf(edit).await.unwrap());
    log.status = TxCommitStatus::Ok;
    // Native paused time does not advance wall time. The old timestamp stands
    // for a filter delayed until after this commit's safety horizon.
    log.timestamp = Some(base() - PAST_HORIZON);
    let pending = peer.tlogger.get_at(&id, Requirement::Any).await.unwrap();
    peer.tlogger.set_if(&log, &pending).await.unwrap();
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
    assert!(!is_gone(&ctx.tl, &id).await);
    assert!(lookup_entry(&ctx, b"k").await.unwrap().is_locked_by(&id));
}

#[tokio::test(start_paused = true)]
async fn a_transient_failure_keeps_the_candidate_until_retry() {
    let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
    let ctx = new_ctx_with_interval(backend.clone(), Duration::from_millis(10)).await;
    let id = tx(12);
    ctx.tl
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
    assert!(!is_gone(&ctx.tl, &id).await);
    assert_eq!(ctx.gc.diagnostics().deferred, 1);
    for _ in 0..20 {
        tokio::time::advance(Duration::from_millis(2)).await;
        for _ in 0..8 {
            rt::yield_now().await;
        }
    }
    assert!(is_gone(&ctx.tl, &id).await);
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
    ctx.tl
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
    assert!(is_gone(&ctx.tl, &id).await);
    assert!(reclaimed_with_ready_hints.load(Ordering::SeqCst));
    bg.shutdown().await;
}

#[test]
fn candidate_capacity_is_preserved_across_deferrals_and_completion() {
    let now = rt::Instant::now();
    let counters = Counters::default();
    let mut candidates = Candidates::default();
    let ids: Vec<_> = (0u64..HINT_CAPACITY as u64)
        .map(|i| TxId::from_bytes(i.to_be_bytes().to_vec()))
        .collect();
    for id in &ids {
        candidates.admit(id.clone(), false, now, &counters);
    }
    let overflow = TxId::from_bytes(b"overflow".to_vec());
    candidates.admit(overflow.clone(), false, now, &counters);
    assert_eq!(counters.take().dropped_candidates, 1);
    for _ in 0..HINT_CAPACITY {
        let id = candidates.take_ready().unwrap();
        let candidate = candidates.complete(&id);
        candidates.defer(id, candidate, now + Duration::from_secs(1));
    }
    candidates.record_backlog(&counters);
    assert_eq!(counters.diagnostics().deferred, HINT_CAPACITY as u64);
    assert_eq!(counters.diagnostics().ready, 0);
    candidates.admit(overflow.clone(), false, now, &counters);
    assert_eq!(counters.take().dropped_candidates, 1);

    candidates.promote_due(now + Duration::from_secs(1));
    candidates.record_backlog(&counters);
    assert_eq!(counters.diagnostics().ready, ADMISSION_BATCH as u64);
    assert_eq!(
        counters.diagnostics().deferred,
        (HINT_CAPACITY - ADMISSION_BATCH) as u64
    );
    let id = candidates.take_ready().unwrap();
    candidates.complete(&id);
    candidates.admit(overflow, false, now, &counters);
    assert_eq!(counters.take().dropped_candidates, 0);
    assert_eq!(candidates.oldest_ready(), Some(now));
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
    let mut candidates = Candidates::new(delay);
    let counters = Counters::default();
    let id = tx(21);
    candidates.admit(id.clone(), false, now, &counters);
    candidates.admit(id.clone(), true, now + delay / 2, &counters);
    assert_eq!(candidates.next_due(), Some(now + delay));
    assert_eq!(candidates.take_ready(), None);
    candidates.promote_due(now + delay);
    assert_eq!(candidates.take_ready(), Some(id));
}
