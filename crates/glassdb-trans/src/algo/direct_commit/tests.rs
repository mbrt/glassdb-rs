//! Direct-path behavior and fallback coverage.

use std::collections::BTreeMap;

use super::super::tests::{
    Tctx, begin_data, commit_access, commit_writes, do_read, entry, key_ref, new_algo,
    new_algo_from_backend, new_recording_algo, new_recording_algo_big_cache, read_outcome,
    shard_reads, test_collection, test_root_path, wa, wdel, write_counts,
};
use super::super::*;
use super::*;
use crate::key_state_resolver::KeyStateResolver;
use crate::shard_coord::{FoldOutcome, ReloadCause, ResolveCtx, ShardResolver, Step};
use glassdb_backend::middleware::{BackendOp, HookBackend, HookFuture, OpLog, RecordingBackend};
use glassdb_backend::{Backend, memory::MemoryBackend};
use glassdb_storage::{CurrentState, NodeLocks, Shard, ShardEntry};

/// Runs one resolver fold and retains its complete classification.
async fn fold_step(
    resolver: &dyn ShardResolver,
    tctx: &Tctx,
    cause: ReloadCause,
    staged: &BTreeMap<Vec<u8>, ShardEntry>,
    locks: &NodeLocks,
) -> Step {
    let key_state = KeyStateResolver::new(tctx.tmon.clone());
    let ctx = ResolveCtx {
        key_state: &key_state,
        tmon: &tctx.tmon,
        requirement: Requirement::Any,
        cause,
    };
    resolver.resolve(&ctx, staged, locks).await.unwrap()
}

/// Runs one fold of `resolver` over the leaf state a coordinator round would
/// hand it, and reports the outcome it classifies. Lets a test drive the
/// `cause` and node-lock combinations that a live interleaving can only
/// produce by luck.
async fn fold(
    resolver: &dyn ShardResolver,
    tctx: &Tctx,
    cause: ReloadCause,
    staged: &BTreeMap<Vec<u8>, ShardEntry>,
    locks: &NodeLocks,
) -> FoldOutcome {
    match fold_step(resolver, tctx, cause, staged, locks).await {
        Step::Skip { outcome } | Step::Stage { outcome, .. } => outcome,
    }
}

// Single-rw commit (ADR-030): a lone read-modify-write whose read was
// superseded by *another instance* is caught with a transparent retry, never
// a surfaced error, and never commits its stale value. This client's cached
// snapshot predates the peer's create, so the key reads as absent — an
// unsupported shape rather than a certified stale read, which is why the
// locked path takes over instead of replaying the body (ADR-053). It resolves
// as `Wounded` or `Retry` depending on whether the snapshot survived to the
// commit fold; both converge on a fresh read.
#[tokio::test]
async fn single_rw_stale_read_renews_and_converges() {
    let (tm, tctx) = new_algo().await;
    let (tm2, _t2) = new_algo_from_backend(tctx.backend.clone()).await;
    let keyp = key_ref(b"k");

    commit_writes(&tm2, vec![wa(&keyp, b"v1")]).await;
    let ra = do_read(&tctx, &keyp).await;

    // Another client overwrites the key, making `ra` stale.
    let h2 = commit_writes(&tm2, vec![wa(&keyp, b"v2")]).await;

    let mut h = begin_data(
        &tm,
        Data {
            reads: vec![ra],
            writes: vec![wa(&keyp, b"v3")],
            scans: Vec::new(),
        },
    );
    let err = tm.commit(&mut h).await.unwrap_err();
    assert!(
        matches!(err, TransError::Wounded | TransError::Retry),
        "a stale read is a transparent retry, got {err:?}"
    );
    tm.end(&mut h).await.unwrap();

    // The stale write never committed: v2 is still current (the abandoned
    // attempt's object is unreferenced, so help-forward cannot promote it).
    assert_eq!(
        do_read(&tctx, &keyp).await.last_writer().cloned().unwrap(),
        *h2.id(),
        "the stale write did not commit; v2 is still current"
    );

    // A fresh read + commit converges (the re-run observes v2 and commits).
    let ra2 = do_read(&tctx, &keyp).await;
    let h3 = commit_access(
        &tm,
        Data {
            reads: vec![ra2],
            writes: vec![wa(&keyp, b"v3")],
            scans: Vec::new(),
        },
    )
    .await;
    assert_eq!(
        do_read(&tctx, &keyp).await.last_writer().cloned().unwrap(),
        *h3.id(),
        "the renewed attempt commits"
    );
}

/// Controls a hook that gates the coordinator's bounded seed read.
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
                        .fetch_update(SeqCst, SeqCst, |n| n.checked_sub(1))
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
        // Point routing is cache-local; the coordinator seed is now the
        // first backend read in the lock phase.
        self.skip.store(0, std::sync::atomic::Ordering::SeqCst);
        self.armed.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn release(&self) {
        self.notify.notify_one();
    }
}

/// Controls a post-hook that reports one successfully landed leaf CAS as in-doubt.
struct InDoubtCas {
    armed: std::sync::atomic::AtomicBool,
}

impl InDoubtCas {
    fn wrap(inner: Arc<dyn Backend>) -> (Arc<HookBackend>, Arc<Self>) {
        let in_doubt = Arc::new(Self {
            armed: std::sync::atomic::AtomicBool::new(false),
        });
        let backend = HookBackend::new(inner);
        backend.set_after({
            let in_doubt = in_doubt.clone();
            move |op, outcome| {
                use std::sync::atomic::Ordering::SeqCst;
                let fail = outcome.is_success()
                    && matches!(op, BackendOp::WriteIf { path, .. }
                            if path.contains("/_n/") || path.ends_with("/_r"))
                    && in_doubt
                        .armed
                        .compare_exchange(true, false, SeqCst, SeqCst)
                        .is_ok();
                let result = if fail {
                    Err(glassdb_backend::BackendError::Unavailable(
                        "simulated in-doubt shard CAS".into(),
                    ))
                } else {
                    Ok(())
                };
                let future: HookFuture = Box::pin(async move { result });
                future
            }
        });
        (backend, in_doubt)
    }

    fn arm(&self) {
        self.armed.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

/// A distinct key that shares the same leaf as `base`, for exercising
/// disjoint-key contention within one leaf object. With split deferred, every
/// key lives in the collection's single leaf `_r` (ADR-031), so any distinct
/// key qualifies.
fn same_shard_sibling(base: &[u8]) -> Vec<u8> {
    let sib = b"sibling".to_vec();
    assert_ne!(sib, base, "sibling must differ from the base key");
    sib
}

fn shard_stores(log: &OpLog, path: &str) -> usize {
    log.lock()
        .unwrap()
        .iter()
        .filter(|r| r.path == path && (r.op == "write_if" || r.op == "write_if_not_exists"))
        .count()
}

// ADR-028: the logless direct commit is folded by the same shard coordinator
// as ordinary lock acquisition, so a direct commit and a disjoint-key
// acquire contending one shard batch into a single CAS round instead of
// racing two separate loads+CASes. The commit publishes its value and the
// acquire installs its lock in the one store.
#[tokio::test(start_paused = true)]
async fn direct_commit_merges_with_disjoint_acquire() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let (backend, gate) = Gate::wrap(mem);
    let rec = Arc::new(RecordingBackend::new(backend));
    let log = rec.log();
    let (tm, tctx) = new_algo_from_backend(rec).await;

    let ka = b"k".to_vec();
    let kb = same_shard_sibling(&ka);
    let kap = key_ref(&ka);
    let kbp = key_ref(&kb);

    // Seed keys A and B committed: the direct commit builds on A's
    // predecessor, and the disjoint acquire overwrites an existing B, so it
    // takes no membership root lock and the round stays a single shard CAS.
    commit_writes(&tm, vec![wa(&kap, b"v1")]).await;
    commit_writes(&tm, vec![wa(&kbp, b"vb1")]).await;

    let txb = TxId::with_priority(2_000_000_000, b"acquire");
    tctx.tmon.begin_tx(&txb);

    let shard_path = test_root_path().to_string();
    log.lock().unwrap().clear();
    gate.arm();

    // The disjoint acquire is submitted first and becomes the dedup driver,
    // parking in the gated current-bound load; the direct commit then joins
    // that open batch. (Post-ADR-030 the commit's own first attempt is
    // `Any` and would skip the load on a warm cache, so it merges via
    // the driver's already-loading round rather than racing a solo, cache-
    // served CAS — which is exactly the ADR-028 single-round behavior.)
    let (ca, cb) = (tm.clone(), tctx.locker.clone());
    let data_b = Data {
        reads: Vec::new(),
        writes: vec![wa(&kbp, b"vb2")],
        scans: Vec::new(),
    };
    let tb = txb.clone();
    let lock_requirement = Requirement::AtLeast(tctx.timeline.now());
    let acquire = tokio::spawn(async move {
        cb.keys()
            .lock_at(&tb, &data_b, false, lock_requirement)
            .await
    });

    // Let the driver park in the gated load before the commit joins.
    rt::sleep(Duration::from_secs(1)).await;

    let mut ha = begin_data(
        &tm,
        Data {
            reads: Vec::new(),
            writes: vec![wa(&kap, b"v2")],
            scans: Vec::new(),
        },
    );
    let txa = ha.id().clone();
    let commit = tokio::spawn(async move {
        let result = ca.commit(&mut ha).await;
        (ha, result)
    });

    // Once the commit has queued into the open batch, release the load.
    rt::sleep(Duration::from_secs(1)).await;
    gate.release();

    let (_ha, committed) = commit.await.unwrap();
    let acquire = acquire.await.unwrap().unwrap();
    committed.expect("the direct commit must land");
    assert!(
        matches!(acquire, LockOutcome::Locked(_)),
        "the disjoint acquire must lock"
    );

    assert_eq!(
        shard_stores(&log, &shard_path),
        1,
        "direct commit and disjoint acquire share one CAS"
    );

    // Both mutations landed in the shared shard write.
    let ea = entry(&tctx, &ka).await.unwrap();
    assert_eq!(
        ea.current,
        CurrentState::Inline {
            writer: txa,
            value: Arc::from(b"v2".as_slice()),
        },
        "the direct commit published its value"
    );
    let eb = entry(&tctx, &kb).await.unwrap();
    assert!(eb.is_locked_by(&txb), "acquire holds B's lock");
}

// ADR-028 regression (batched in-doubt): a direct commit co-batched with a
// disjoint-key acquire whose shared CAS comes back in-doubt (`Unavailable`)
// recovers idempotently — the engine reloads and re-folds, the commit finds
// its own marker already published (`Landed`), and the acquire re-installs
// its own lock (`Locked`) without double-applying. No error is surfaced.
#[tokio::test(start_paused = true)]
async fn direct_commit_batched_in_doubt_recovers() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let (backend, indoubt) = InDoubtCas::wrap(mem);
    let (backend, gate) = Gate::wrap(backend);
    let (tm, tctx) = new_algo_from_backend(backend).await;

    let ka = b"k".to_vec();
    let kb = same_shard_sibling(&ka);
    let kap = key_ref(&ka);
    let kbp = key_ref(&kb);

    // Seed keys A and B committed (un-gated, before arming): the commit has
    // a predecessor and the acquire overwrites an existing B, so it takes no
    // membership root lock and the round stays a single shard CAS.
    commit_writes(&tm, vec![wa(&kap, b"v1")]).await;
    commit_writes(&tm, vec![wa(&kbp, b"vb1")]).await;

    let txb = TxId::with_priority(2_000_000_000, b"acquire");
    tctx.tmon.begin_tx(&txb);

    // Arm the merge gate and the in-doubt first CAS together.
    indoubt.arm();
    gate.arm();

    let (ca, cb) = (tm.clone(), tctx.locker.clone());
    let mut ha = begin_data(
        &tm,
        Data {
            reads: Vec::new(),
            writes: vec![wa(&kap, b"v2")],
            scans: Vec::new(),
        },
    );
    let txa = ha.id().clone();
    let commit = tokio::spawn(async move {
        let result = ca.commit(&mut ha).await;
        (ha, result)
    });
    let data_b = Data {
        reads: Vec::new(),
        writes: vec![wa(&kbp, b"vb2")],
        scans: Vec::new(),
    };
    let tb = txb.clone();
    let lock_requirement = Requirement::AtLeast(tctx.timeline.now());
    let acquire = tokio::spawn(async move {
        cb.keys()
            .lock_at(&tb, &data_b, false, lock_requirement)
            .await
    });

    rt::sleep(Duration::from_secs(1)).await;
    gate.release();

    // The in-doubt CAS actually landed, so the re-fold sees both members
    // applied: the commit classifies itself Landed, the acquire re-locks.
    let (_ha, committed) = commit.await.unwrap();
    let acquire = acquire.await.unwrap().unwrap();
    committed.expect("the commit recovers as landed, not in-doubt");
    assert!(
        matches!(acquire, LockOutcome::Locked(_)),
        "the co-batched acquire re-locks idempotently"
    );

    assert_eq!(
        entry(&tctx, &ka).await.unwrap().current,
        CurrentState::Inline {
            writer: txa,
            value: Arc::from(b"v2".as_slice()),
        }
    );
    assert!(entry(&tctx, &kb).await.unwrap().is_locked_by(&txb));
}

// A value the inline per-value budget rejects, so its transaction takes the
// regular locked path instead of ADR-051's logless one (ADR-053).
fn logged_value() -> Vec<u8> {
    vec![b'v'; glassdb_storage::InlinePolicy::default().max_value_bytes + 1]
}

// ADR-053: a single-key read-modify-write whose value misses the inline
// budget has no logged fast path to fall to, so it commits through the
// regular locked protocol: one committed `_t/` object write, one leaf lock
// CAS, one leaf write-back CAS (run synchronously here because there is no
// background executor), and no separate membership write — and the new
// value is durable and readable. With split deferred the leaf is the
// collection root `_r`, so both leaf CAS's land there (ADR-031).
#[tokio::test]
async fn an_overwrite_over_the_inline_budget_takes_the_locked_path() {
    let (tm, tctx, log) = new_recording_algo().await;
    let keyp = key_ref(b"k");

    commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;
    let r = do_read(&tctx, &keyp).await;

    log.lock().unwrap().clear();
    tctx.locker.stats_and_reset();
    let mut h = begin_data(
        &tm,
        Data {
            reads: vec![r],
            writes: vec![wa(&keyp, &logged_value())],
            scans: Vec::new(),
        },
    );
    let tid = h.id().clone();
    tm.commit(&mut h).await.unwrap();
    tm.end(&mut h).await.unwrap();

    assert!(
        tctx.locker.stats_and_reset().calls >= 1,
        "an over-budget value goes straight to locking, it never replays"
    );
    let c = write_counts(&log);
    assert_eq!(
        c.leaf, 2,
        "locked path: one lock CAS plus one write-back CAS, no membership: {c:?}"
    );
    assert_eq!(c.tx, 1, "one committed-object write: {c:?}");

    // The commit landed: the shard points at us with no live lock, a
    // committed `_t/` object exists, and the value reads back as ours.
    let e = entry(&tctx, b"k").await.unwrap();
    assert_eq!(e.current.writer(), Some(&tid));
    assert!(e.lock_holders().is_empty());
    let status = tctx
        .tlogger
        .commit_status_at(&tid, Requirement::Any)
        .await
        .unwrap();
    assert_eq!(status.status, TxCommitStatus::Ok);
    let r = do_read(&tctx, &keyp).await;
    assert_eq!(r.last_writer().cloned().unwrap(), tid);
}

#[tokio::test(start_paused = true)]
async fn single_rw_observing_a_gate_uses_the_full_locked_path() {
    let (tm, tctx) = new_algo().await;
    let keyp = key_ref(b"k");
    commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;
    let read = do_read(&tctx, &keyp).await;

    let gate = TxId::with_priority(0, b"gate");
    tctx.tmon.begin_tx(&gate);
    let (mut root, version) = tctx
        .shards
        .load_root(&test_collection(), Requirement::Any)
        .await
        .unwrap();
    root.set_structural_gate(gate.clone());
    assert!(
        tctx.shards
            .store_root(&test_collection(), &root, &version)
            .await
            .unwrap()
    );

    tctx.locker.stats_and_reset();
    let mut handle = begin_data(
        &tm,
        Data {
            reads: vec![read],
            writes: vec![wa(&keyp, b"v2")],
            scans: Vec::new(),
        },
    );
    let committing_tm = tm.clone();
    let committing = tokio::spawn(async move {
        let result = committing_tm.commit(&mut handle).await;
        (handle, result)
    });
    rt::sleep(Duration::from_millis(50)).await;
    assert!(!committing.is_finished());

    tctx.tmon
        .commit_tx(TxLog::new(gate, TxCommitStatus::Ok))
        .await
        .unwrap();
    let (mut handle, result) = committing.await.unwrap();
    result.unwrap();
    tm.end(&mut handle).await.unwrap();
    assert!(
        tctx.locker.stats_and_reset().calls >= 1,
        "an observed gate bypasses the direct commit"
    );
    assert_eq!(
        do_read(&tctx, &keyp).await.last_writer().cloned(),
        Some(handle.id().clone())
    );
}

// ADR-030: a warm single read-write commit reuses the shard the read cached
// for both its eligibility check and its lock-install fold (`Any`), so
// it issues no backend shard read for either. The successful install CAS
// supplies the write-back's lower bound too, so write-back also reuses the
// installed cached state. A revalidating eligibility, install, or write-back
// would add a `read_if_modified`, so pinning the total to zero guards the
// receipt propagation. A large cache keeps this deterministic (nothing is
// evicted between the read and the commit).
#[tokio::test]
async fn single_rw_commit_reuses_cached_shard() {
    let (tm, tctx, log) = new_recording_algo_big_cache().await;
    let keyp = key_ref(b"k");

    commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;
    // The read warms the shard in the object cache.
    let r = do_read(&tctx, &keyp).await;

    log.lock().unwrap().clear();
    let mut h = begin_data(
        &tm,
        Data {
            reads: vec![r],
            writes: vec![wa(&keyp, b"v2")],
            scans: Vec::new(),
        },
    );
    tm.commit(&mut h).await.unwrap();
    tm.end(&mut h).await.unwrap();

    let (full, revalidate) = shard_reads(&log);
    assert_eq!(full, 0, "no cold shard read on a warm commit");
    assert_eq!(
        revalidate, 0,
        "eligibility, install, and write-back reuse cache/CAS evidence"
    );
}

// A blind single-key put over an existing key (no read) takes the same
// locked path when its value misses the inline budget.
#[tokio::test]
async fn a_blind_put_over_the_inline_budget_takes_the_locked_path() {
    let (tm, tctx, log) = new_recording_algo().await;
    let keyp = key_ref(b"k");

    commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;

    log.lock().unwrap().clear();
    let mut h = begin_data(
        &tm,
        Data {
            reads: Vec::new(),
            writes: vec![wa(&keyp, &logged_value())],
            scans: Vec::new(),
        },
    );
    let tid = h.id().clone();
    tm.commit(&mut h).await.unwrap();
    tm.end(&mut h).await.unwrap();

    let c = write_counts(&log);
    assert_eq!(
        c.leaf, 2,
        "locked path: one lock CAS plus one write-back CAS, no membership: {c:?}"
    );
    assert_eq!(c.tx, 1, "one committed-object write: {c:?}");
    assert_eq!(
        entry(&tctx, b"k").await.unwrap().current.writer(),
        Some(&tid)
    );
}

// ADR-020 regression: the locked path leaves a write lock held by the
// *committed* writer until its asynchronous write-back publishes the pointer
// and releases it. A single-key writer arriving in that window must treat the
// committed holder as effectively unlocked — help-forwarding it as the
// predecessor — and stay on the lock-free direct path, rather than bailing to
// the locked path on the mere presence of the lock (the measured regression).
// A stale read replays instead.
#[tokio::test]
async fn a_committed_holder_keeps_the_next_writer_on_the_direct_path() {
    let (tm, tctx) = new_algo().await;
    let keyp = key_ref(b"k");
    let leaf_path = test_root_path();
    let raw = b"k".to_vec();

    // H0 publishes v1; H1 overwrites through the locked path (its value
    // misses the inline budget), so it has a committed transaction object.
    let h0 = commit_writes(&tm, vec![wa(&keyp, b"v1")])
        .await
        .id()
        .clone();
    let h1 = commit_writes(&tm, vec![wa(&keyp, &logged_value())])
        .await
        .id()
        .clone();

    // Recreate the commit window before write-back: the lock is still held by
    // the committed H1 while the pointer lags at its predecessor H0.
    let loaded = tctx
        .shards
        .load_leaf(&leaf_path, Requirement::AtLeast(tctx.timeline.now()))
        .await
        .unwrap();
    let windowed = Shard::from_entries(loaded.entries().entries().cloned().map(|mut e| {
        if e.key == raw {
            e.replace_write_lock(h1.clone());
            e.current = CurrentState::External { writer: h0.clone() };
        }
        e
    }));
    let mut edit = loaded.into_edit();
    edit.set_entries(windowed);
    assert!(tctx.shards.commit_leaf(edit).await.unwrap());

    // The window is observably at the committed holder H1 (v2), not the
    // lagging pointer H0: the shared resolver already help-forwards it.
    let r = do_read(&tctx, &keyp).await;
    assert_eq!(r.last_writer().cloned().unwrap(), h1);

    // Eligibility mirrors that resolution: given the reconciled lock state,
    // an RMW that read H1 and a blind put are both committable and build on
    // H1, while a read of the superseded H0 is still rejected as stale.
    let requirement = Requirement::AtLeast(tm.timeline.now());
    let (res, _) = tm
        .resolver
        .resolve_key_holders(&keyp, None, requirement)
        .await
        .unwrap();
    assert_eq!(
        eligible_writer(&res, Some(&h1)),
        Ok(h1.clone()),
        "an RMW that read the committed holder builds on it"
    );
    assert_eq!(
        eligible_writer(&res, None),
        Ok(h1.clone()),
        "a blind put builds on the committed holder"
    );
    assert_eq!(
        eligible_writer(&res, Some(&h0)),
        Err(Ineligible::Replay),
        "a read of the superseded value is stale, and replayable"
    );

    // End to end: the writer commits directly over H1 (help-forwarding it
    // into the chain, not orphaning it), taking no lock of its own.
    tctx.locker.stats_and_reset();
    let mut h = begin_data(
        &tm,
        Data {
            reads: vec![r],
            writes: vec![wa(&keyp, b"v3")],
            scans: Vec::new(),
        },
    );
    let h2 = h.id().clone();
    tm.commit(&mut h).await.unwrap();
    tm.end(&mut h).await.unwrap();

    assert_eq!(
        tctx.locker.stats_and_reset().calls,
        0,
        "the committed holder did not push the writer onto the locked path"
    );
    let e = entry(&tctx, b"k").await.unwrap();
    assert_eq!(e.current.writer(), Some(&h2));
    assert!(e.lock_holders().is_empty());
    assert_eq!(
        do_read(&tctx, &keyp).await.last_writer().cloned().unwrap(),
        h2
    );
}

// ADR-051: an eligible small overwrite commits in a single conditional leaf
// CAS that publishes the value itself — no lock, no transaction object, and
// nothing to write back — and the value reads back from the leaf alone.
#[tokio::test]
async fn direct_commit_overwrites_in_one_leaf_cas() {
    let (tm, tctx, log) = new_recording_algo().await;
    let keyp = key_ref(b"k");

    commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;
    let r = do_read(&tctx, &keyp).await;

    log.lock().unwrap().clear();
    let mut h = begin_data(
        &tm,
        Data {
            reads: vec![r],
            writes: vec![wa(&keyp, b"v2")],
            scans: Vec::new(),
        },
    );
    let tid = h.id().clone();
    tm.commit(&mut h).await.unwrap();
    tm.end(&mut h).await.unwrap();

    let c = write_counts(&log);
    assert_eq!(c.leaf, 1, "the commit is one leaf CAS: {c:?}");
    assert_eq!(c.tx, 0, "the transaction has no object at all: {c:?}");

    let e = entry(&tctx, b"k").await.unwrap();
    assert_eq!(
        e.current,
        CurrentState::Inline {
            writer: tid.clone(),
            value: Arc::from(b"v2".as_slice()),
        }
    );
    assert!(e.lock_holders().is_empty(), "no lock was ever installed");
    assert_eq!(
        read_outcome(&tctx, &keyp)
            .await
            .last_writer()
            .cloned()
            .unwrap(),
        tid
    );
}

// ADR-051 regression: a direct commit lands on an entry whose write lock is
// still held by an *already-committed* writer awaiting write-back, so it must
// replace that holder. Left in place, writer resolution help-forwards to the
// holder and resolves the entry *backwards* — to the value this commit just
// superseded — silently losing updates.
#[tokio::test]
async fn direct_commit_replaces_a_committed_holder() {
    let (tm, tctx) = new_algo().await;
    let keyp = key_ref(b"k");
    let leaf_path = test_root_path();
    let raw = b"k".to_vec();

    let h0 = commit_writes(&tm, vec![wa(&keyp, b"v1")])
        .await
        .id()
        .clone();
    let h1 = commit_writes(&tm, vec![wa(&keyp, &logged_value())])
        .await
        .id()
        .clone();

    // The locked path's commit window: the lock is still held by the committed
    // H1 while the current state lags at its predecessor H0.
    let loaded = tctx
        .shards
        .load_leaf(&leaf_path, Requirement::AtLeast(tctx.timeline.now()))
        .await
        .unwrap();
    let windowed = Shard::from_entries(loaded.entries().entries().cloned().map(|mut e| {
        if e.key == raw {
            e.replace_write_lock(h1.clone());
            e.current = CurrentState::External { writer: h0.clone() };
        }
        e
    }));
    let mut edit = loaded.into_edit();
    edit.set_entries(windowed);
    assert!(tctx.shards.commit_leaf(edit).await.unwrap());

    let mut h = begin_data(
        &tm,
        Data {
            reads: Vec::new(),
            writes: vec![wa(&keyp, b"v3")],
            scans: Vec::new(),
        },
    );
    let h2 = h.id().clone();
    tm.commit(&mut h).await.unwrap();
    tm.end(&mut h).await.unwrap();

    let e = entry(&tctx, b"k").await.unwrap();
    assert_eq!(
        e.current,
        CurrentState::Inline {
            writer: h2.clone(),
            value: Arc::from(b"v3".as_slice()),
        }
    );
    assert!(
        e.lock_holders().is_empty(),
        "the superseded holder was replaced, not preserved"
    );
    let outcome = read_outcome(&tctx, &keyp).await;
    assert_eq!(outcome.last_writer().cloned().unwrap(), h2);
    assert_eq!(&*outcome.value.unwrap().value, b"v3");
}

// ADR-051 regression: every reason a fold declines to publish the commit
// marker must be classified against the round's in-doubt evidence, not just
// the lost-race one. A structural gate or a collection-delete fence that
// appears *after* an uncertain CAS is no proof that the CAS did not land, so
// reporting `Moved` there would let the logged protocol re-run a body whose
// logless commit may already be durable (and since superseded, invisible).
#[tokio::test]
async fn direct_commit_blocked_after_uncertain_cas_stays_in_doubt() {
    let (tm, tctx) = new_algo().await;
    let keyp = key_ref(b"k");
    commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;

    let seed = entry(&tctx, b"k").await.unwrap();
    let resolver = DirectCommitResolver {
        id: TxId::with_priority(2, b"direct"),
        raw_key: b"k".to_vec(),
        leaf_path: test_root_path(),
        key: keyp.clone(),
        value: Arc::from(b"v2".as_slice()),
        read_version: seed.current.writer().cloned(),
        inline: InlinePolicy::default(),
        split_hints: tm.direct_commit.split_hints.clone(),
        staged_over: Mutex::new(None),
    };
    let staged = BTreeMap::from([(b"k".to_vec(), seed)]);

    let mut gated = NodeLocks::default();
    gated.set_structural_gate(TxId::with_priority(1, b"splitter"));
    let mut fenced = NodeLocks::default();
    fenced.set_delete_intent(TxId::with_priority(1, b"dropper"));

    for (what, locks) in [("a structural gate", &gated), ("a delete fence", &fenced)] {
        // Nothing was written yet, so the logged path may take over.
        let outcome = fold(&resolver, &tctx, ReloadCause::Fresh, &staged, locks).await;
        assert!(
            matches!(outcome, FoldOutcome::Moved),
            "{what} on a fresh fold proves nothing was written, got {outcome:?}"
        );

        let outcome = fold(
            &resolver,
            &tctx,
            ReloadCause::Reloaded { in_doubt: true },
            &staged,
            locks,
        )
        .await;
        assert!(
            matches!(outcome, FoldOutcome::InDoubt(_)),
            "{what} cannot disprove a landed uncertain CAS, got {outcome:?}"
        );
    }
}

// ADR-051 regression (fuzz `history` crash-3ddc66ba): a blind put is
// last-writer-wins on a fresh fold, but after its own uncertain CAS the entry
// may already hold a commit that read the very value that CAS published.
// Republishing then rolls the key back behind a commit whose writer was told it
// succeeded, losing that update. Only an entry still naming the writer the
// uncertain CAS built on proves nothing landed.
#[tokio::test]
async fn a_blind_put_after_an_uncertain_cas_never_republishes_over_a_newer_writer() {
    let (tm, tctx) = new_algo().await;
    let keyp = key_ref(b"k");
    commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;
    let seed = entry(&tctx, b"k").await.unwrap();
    let locks = NodeLocks::default();

    let blind = DirectCommitResolver {
        id: TxId::with_priority(9, b"blind"),
        raw_key: b"k".to_vec(),
        leaf_path: test_root_path(),
        key: keyp.clone(),
        value: Arc::from(b"v2".as_slice()),
        read_version: None,
        inline: InlinePolicy::default(),
        split_hints: tm.direct_commit.split_hints.clone(),
        staged_over: Mutex::new(None),
    };
    let staged = BTreeMap::from([(b"k".to_vec(), seed)]);

    // The fresh fold publishes over the seeded writer; its CAS is the one that
    // comes back uncertain.
    assert!(matches!(
        fold_step(&blind, &tctx, ReloadCause::Fresh, &staged, &locks).await,
        Step::Stage {
            outcome: FoldOutcome::Landed,
            ..
        }
    ));

    // The entry still names that writer, so the uncertain CAS provably did not
    // land and the publication is retried rather than surfaced as in-doubt.
    assert!(matches!(
        fold_step(
            &blind,
            &tctx,
            ReloadCause::Reloaded { in_doubt: true },
            &staged,
            &locks,
        )
        .await,
        Step::Stage {
            outcome: FoldOutcome::Landed,
            ..
        }
    ));

    let superseded = BTreeMap::from([(
        b"k".to_vec(),
        ShardEntry::new(b"k".to_vec()).with_current(CurrentState::Inline {
            writer: TxId::with_priority(10, b"newer"),
            value: Arc::from(b"v3".as_slice()),
        }),
    )]);
    let outcome = fold(
        &blind,
        &tctx,
        ReloadCause::Reloaded { in_doubt: true },
        &superseded,
        &locks,
    )
    .await;
    assert!(
        matches!(outcome, FoldOutcome::InDoubt(_)),
        "a newer writer cannot disprove a landed uncertain CAS, got {outcome:?}"
    );
}

// ADR-053: only a *superseded read* certifies the body-replay case, and an
// uncertain CAS still outranks it. Every other way a fold declines is either
// state the direct path cannot arbitrate or evidence that proves nothing, so
// it reports `Moved` and the locked protocol takes over. Classifying too
// broadly would spin the body forever against a holder or a closed budget.
#[tokio::test]
async fn direct_commit_replays_only_a_certified_superseded_read() {
    let (tm, tctx) = new_algo().await;
    let keyp = key_ref(b"k");
    commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;
    let seed = entry(&tctx, b"k").await.unwrap();
    let current = seed.current.writer().cloned().unwrap();
    let locks = NodeLocks::default();
    let split_hints = tm.direct_commit.split_hints.clone();

    let direct = |read_version| DirectCommitResolver {
        id: TxId::with_priority(9, b"direct"),
        raw_key: b"k".to_vec(),
        leaf_path: test_root_path(),
        key: keyp.clone(),
        value: Arc::from(b"v2".as_slice()),
        read_version,
        inline: InlinePolicy::default(),
        split_hints: split_hints.clone(),
        staged_over: Mutex::new(None),
    };

    // A read the entry has moved past: nothing is staged and the loss is
    // definitive, so the body is reevaluated against the winner.
    let stale = direct(Some(TxId::with_priority(1, b"stale")));
    let staged = BTreeMap::from([(b"k".to_vec(), seed.clone())]);
    let outcome = fold(&stale, &tctx, ReloadCause::Fresh, &staged, &locks).await;
    assert!(
        matches!(outcome, FoldOutcome::Replay),
        "a superseded read staged nothing and can be reevaluated, got {outcome:?}"
    );
    let outcome = fold(
        &stale,
        &tctx,
        ReloadCause::Reloaded { in_doubt: true },
        &staged,
        &locks,
    )
    .await;
    assert!(
        matches!(outcome, FoldOutcome::InDoubt(_)),
        "an uncertain CAS is never downgraded to a replay, got {outcome:?}"
    );

    // A live pending holder is a genuine conflict only wound-wait resolves,
    // even though this transaction also staged nothing.
    let holder = TxId::with_priority(1, b"holder");
    tctx.tmon.begin_tx(&holder);
    let mut held = seed.clone();
    held.replace_write_lock(holder);
    let outcome = fold(
        &direct(Some(current.clone())),
        &tctx,
        ReloadCause::Fresh,
        &BTreeMap::from([(b"k".to_vec(), held)]),
        &locks,
    )
    .await;
    assert!(
        matches!(outcome, FoldOutcome::Moved),
        "a live holder needs the locked protocol, not a replay, got {outcome:?}"
    );

    // A key read as deleted names the very writer that deleted it, so the
    // read is *not* superseded. Testing existence before the read version is
    // what keeps this unsupported shape off the replay path.
    let deleter = TxId::with_priority(1, b"deleter");
    let buried = seed.clone().with_current(CurrentState::Tombstone {
        writer: deleter.clone(),
    });
    let outcome = fold(
        &direct(Some(deleter)),
        &tctx,
        ReloadCause::Fresh,
        &BTreeMap::from([(b"k".to_vec(), buried)]),
        &locks,
    )
    .await;
    assert!(
        matches!(outcome, FoldOutcome::Moved),
        "a put over a tombstone is unsupported, not stale, got {outcome:?}"
    );

    // Aggregate inline admission is owned by the direct resolver. Existing
    // inline values consume the leaf budget, while this key's prior state
    // is replaced rather than double-counted.
    let budgeted = DirectCommitResolver {
        inline: InlinePolicy {
            max_value_bytes: 64,
            max_leaf_bytes: 5,
        },
        ..direct(Some(current.clone()))
    };
    let other_writer = TxId::with_priority(1, b"other");
    let crowded = BTreeMap::from([
        (b"k".to_vec(), seed),
        (
            b"other".to_vec(),
            ShardEntry::new(b"other").with_current(CurrentState::Inline {
                writer: other_writer,
                value: Arc::from(b"four".as_slice()),
            }),
        ),
    ]);
    assert!(matches!(
        fold_step(&budgeted, &tctx, ReloadCause::Fresh, &crowded, &locks).await,
        Step::Skip {
            outcome: FoldOutcome::Moved
        }
    ));
    assert_eq!(split_hints.pending_inline_pressure(), 1);
    assert!(matches!(
        fold_step(
            &budgeted,
            &tctx,
            ReloadCause::Reloaded { in_doubt: true },
            &crowded,
            &locks,
        )
        .await,
        Step::Skip {
            outcome: FoldOutcome::InDoubt(_)
        }
    ));
    assert_eq!(split_hints.pending_inline_pressure(), 2);

    let impossible = DirectCommitResolver {
        inline: InlinePolicy {
            max_value_bytes: 64,
            max_leaf_bytes: 1,
        },
        ..direct(Some(current.clone()))
    };
    assert!(matches!(
        fold_step(&impossible, &tctx, ReloadCause::Fresh, &crowded, &locks).await,
        Step::Skip {
            outcome: FoldOutcome::Moved
        }
    ));
    assert_eq!(
        split_hints.pending_inline_pressure(),
        2,
        "a value no leaf can admit does not request a split"
    );

    // The round-level classifications: a same-key claim proves this member
    // folded nothing, while a spent CAS budget proves nothing about an
    // earlier attempt of the same round. And a blind overwrite has no
    // read-dependent computation to reevaluate.
    let rmw = direct(Some(current));
    assert!(matches!(rmw.excluded_outcome(false), FoldOutcome::Replay));
    assert!(matches!(
        rmw.excluded_outcome(true),
        FoldOutcome::InDoubt(_)
    ));
    assert!(
        matches!(rmw.exhausted_outcome(false), FoldOutcome::Moved),
        "an exhausted budget does not certify a replay"
    );
    assert!(
        matches!(direct(None).excluded_outcome(false), FoldOutcome::Moved),
        "a blind overwrite takes the locked protocol instead of replaying"
    );
}

// ADR-053: a read-modify-write whose observed version is superseded before
// anything is published reevaluates its body under the same id. Nothing was
// staged, so the attempt neither renews nor takes a lock — publishing one
// would make the key's next direct attempt ineligible for no reason.
#[tokio::test]
async fn direct_commit_superseded_read_replays_in_place() {
    let (tm, tctx) = new_algo().await;
    let keyp = key_ref(b"k");
    commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;

    // Read v1, then let a later commit supersede it. Both versions are this
    // client's own, so its snapshot sees the winner rather than a stale leaf.
    let stale = do_read(&tctx, &keyp).await;
    let winner = commit_writes(&tm, vec![wa(&keyp, b"v2")])
        .await
        .id()
        .clone();

    let mut h = begin_data(
        &tm,
        Data {
            reads: vec![stale],
            writes: vec![wa(&keyp, b"v3")],
            scans: Vec::new(),
        },
    );
    let err = tm.commit(&mut h).await.unwrap_err();
    assert!(
        matches!(err, TransError::Retry),
        "a superseded read replays its body in place, got {err:?}"
    );
    tm.end(&mut h).await.unwrap();
    let status = tctx
        .tlogger
        .commit_status_at(h.id(), Requirement::Any)
        .await
        .unwrap();
    assert_eq!(
        status.status,
        TxCommitStatus::Unknown,
        "ending a replayed attempt writes no transaction object"
    );

    // The stale value never committed and the key kept its logless shape.
    let e = entry(&tctx, b"k").await.unwrap();
    assert_eq!(e.current.writer(), Some(&winner));
    assert!(
        e.lock_holders().is_empty(),
        "a replayed attempt publishes no holder"
    );

    // Reevaluating against the winner commits directly.
    let fresh = do_read(&tctx, &keyp).await;
    let replayed = commit_access(
        &tm,
        Data {
            reads: vec![fresh],
            writes: vec![wa(&keyp, b"v3")],
            scans: Vec::new(),
        },
    )
    .await;
    assert_eq!(
        entry(&tctx, b"k").await.unwrap().current,
        CurrentState::Inline {
            writer: replayed.id().clone(),
            value: Arc::from(b"v3".as_slice()),
        },
        "the replayed body commits in one leaf CAS"
    );
}

// ADR-053 regression: two eligible read-modify-writes on one key share a
// coordinator round, where only one may stage its logless commit. The loser
// must reevaluate its body under the same id rather than publish a holder —
// creating one would make every subsequent direct attempt on the key
// ineligible, turning a local scheduling loss into a lasting logged phase.
#[tokio::test(start_paused = true)]
async fn direct_commit_same_key_round_loser_replays_its_body() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let (backend, gate) = Gate::wrap(mem);
    let (tm, tctx) = new_algo_from_backend(backend).await;

    let ka = b"k".to_vec();
    let kb = same_shard_sibling(&ka);
    let kap = key_ref(&ka);
    let kbp = key_ref(&kb);
    commit_writes(&tm, vec![wa(&kap, b"v1")]).await;
    commit_writes(&tm, vec![wa(&kbp, b"vb1")]).await;

    // Both attempts read the same current version, so both are eligible.
    let ra1 = do_read(&tctx, &kap).await;
    let ra2 = do_read(&tctx, &kap).await;
    let rmw = |read| {
        begin_data(
            &tm,
            Data {
                reads: vec![read],
                writes: vec![wa(&kap, b"v2")],
                scans: Vec::new(),
            },
        )
    };
    let (mut h1, mut h2) = (rmw(ra1), rmw(ra2));

    // A disjoint-key acquire drives the round and parks in the gated load, so
    // both direct commits queue into one still-open batch. Their own first
    // fold attempt is cache-served (`Any`, ADR-030), so without a driver they
    // would each win a solo round and never contend.
    gate.arm();
    let driver = TxId::with_priority(1, b"driver");
    tctx.tmon.begin_tx(&driver);
    let locker = tctx.locker.clone();
    let data_b = Data {
        reads: Vec::new(),
        writes: vec![wa(&kbp, b"vb2")],
        scans: Vec::new(),
    };
    let requirement = Requirement::AtLeast(tctx.timeline.now());
    let acquire = tokio::spawn(async move {
        locker
            .keys()
            .lock_at(&driver, &data_b, false, requirement)
            .await
    });
    rt::sleep(Duration::from_secs(1)).await;

    let ta = tm.clone();
    let first = tokio::spawn(async move {
        let res = ta.commit(&mut h1).await;
        (h1, res)
    });
    let tb = tm.clone();
    let second = tokio::spawn(async move {
        let res = tb.commit(&mut h2).await;
        (h2, res)
    });
    rt::sleep(Duration::from_secs(1)).await;
    gate.release();

    assert!(matches!(
        acquire.await.unwrap().unwrap(),
        LockOutcome::Locked(_)
    ));
    let (h1, r1) = first.await.unwrap();
    let (h2, r2) = second.await.unwrap();

    // Which member wins the round's claim depends on id order; that exactly
    // one does is the property under test.
    let (winner, mut replayed) = match (&r1, &r2) {
        (Ok(()), Err(TransError::Retry)) => (h1.id().clone(), h2),
        (Err(TransError::Retry), Ok(())) => (h2.id().clone(), h1),
        other => panic!("expected one commit and one replay, got {other:?}"),
    };

    // The winner's commit is the leaf CAS itself, and the loser left nothing
    // behind for a peer to resolve: no holder on the key and no transaction
    // object under its still-unengaged id.
    let e = entry(&tctx, &ka).await.unwrap();
    assert_eq!(
        e.current,
        CurrentState::Inline {
            writer: winner,
            value: Arc::from(b"v2".as_slice()),
        },
        "the round's winner published its value directly"
    );
    assert!(
        e.lock_holders().is_empty(),
        "the contended round published no holder"
    );
    tm.end(&mut replayed).await.unwrap();
    let status = tctx
        .tlogger
        .commit_status_at(replayed.id(), Requirement::Any)
        .await
        .unwrap();
    assert_eq!(
        status.status,
        TxCommitStatus::Unknown,
        "ending the replayed attempt writes no transaction object"
    );

    // Reevaluating the body against the winner converges without locking.
    let ra3 = do_read(&tctx, &kap).await;
    let mut h = rmw(ra3);
    tm.commit(&mut h).await.unwrap();
    tm.end(&mut h).await.unwrap();
    assert_eq!(
        entry(&tctx, &ka).await.unwrap().current.writer(),
        Some(h.id()),
        "the replayed body commits directly on its next attempt"
    );
}

// Creating a key is ineligible for the direct commit path (it has no
// predecessor value to build on), so it takes the locked path. The direct
// path never calls the locker, so a non-zero lock-call count proves the
// locked path was taken. The membership-write lock is folded into the same
// leaf CAS as the entry lock (ADR-032), so lock install + write-back is
// exactly two.
#[tokio::test]
async fn single_rw_create_uses_full_path() {
    let (tm, tctx, log) = new_recording_algo().await;
    let keyp = key_ref(b"new");

    log.lock().unwrap().clear();
    tctx.locker.stats_and_reset();
    let mut h = begin_data(
        &tm,
        Data {
            reads: Vec::new(),
            writes: vec![wa(&keyp, b"v")],
            scans: Vec::new(),
        },
    );
    tm.commit(&mut h).await.unwrap();
    tm.end(&mut h).await.unwrap();

    assert!(
        tctx.locker.stats_and_reset().calls >= 1,
        "a create takes the full locked path"
    );
    let c = write_counts(&log);
    assert_eq!(
        c.leaf, 2,
        "create folds membership locking into lock install + write-back: {c:?}"
    );
    assert!(entry(&tctx, b"new").await.unwrap().exists());
}

// A delete is ineligible for the direct path too (it publishes a tombstone, not
// a pointer over a predecessor), so it takes the full locked path; the
// non-zero lock-call count proves it. Membership locking folds into the
// entry-lock CAS (ADR-032).
#[tokio::test]
async fn single_rw_delete_uses_full_path() {
    let (tm, tctx, log) = new_recording_algo().await;
    let keyp = key_ref(b"k");

    commit_writes(&tm, vec![wa(&keyp, b"v")]).await;
    let r = do_read(&tctx, &keyp).await;

    log.lock().unwrap().clear();
    tctx.locker.stats_and_reset();
    let mut h = begin_data(
        &tm,
        Data {
            reads: vec![r],
            writes: vec![wdel(&keyp)],
            scans: Vec::new(),
        },
    );
    tm.commit(&mut h).await.unwrap();
    tm.end(&mut h).await.unwrap();

    assert!(
        tctx.locker.stats_and_reset().calls >= 1,
        "a delete takes the full locked path"
    );
    let c = write_counts(&log);
    assert_eq!(
        c.leaf, 2,
        "delete folds membership locking into lock install + write-back: {c:?}"
    );
    assert!(entry(&tctx, b"k").await.unwrap().current.is_tombstone());
}

// A two-key write is ineligible (the direct path publishes one value), so
// the logged path stores external pointers for both committed values
// (ADR-054).
#[tokio::test]
async fn single_rw_multi_key_uses_full_path() {
    let (tm, tctx, log) = new_recording_algo().await;
    let ka = key_ref(b"a");
    let kb = key_ref(b"b");

    commit_writes(&tm, vec![wa(&ka, b"v1"), wa(&kb, b"v1")]).await;

    log.lock().unwrap().clear();
    let mut h = begin_data(
        &tm,
        Data {
            reads: Vec::new(),
            writes: vec![wa(&ka, b"v2"), wa(&kb, b"v2")],
            scans: Vec::new(),
        },
    );
    tm.commit(&mut h).await.unwrap();
    tm.end(&mut h).await.unwrap();

    let c = write_counts(&log);
    assert!(c.leaf >= 2, "a multi-key write takes the full path: {c:?}");
    let writer = h.id().clone();
    for (key, key_ref) in [(b"a".as_slice(), &ka), (b"b".as_slice(), &kb)] {
        assert_eq!(
            entry(&tctx, key).await.unwrap().current,
            CurrentState::External {
                writer: writer.clone()
            }
        );
        assert_eq!(
            read_outcome(&tctx, key_ref)
                .await
                .value
                .unwrap()
                .value
                .as_ref(),
            b"v2"
        );
    }
}

// Reading a key other than the written one needs that key's shard validated,
// so the single-key write falls back to the full locked path.
#[tokio::test]
async fn single_rw_other_key_read_uses_full_path() {
    let (tm, tctx, log) = new_recording_algo().await;
    let ka = key_ref(b"a");
    let kb = key_ref(b"b");

    commit_writes(&tm, vec![wa(&ka, b"v1"), wa(&kb, b"v1")]).await;
    let ra = do_read(&tctx, &ka).await;

    log.lock().unwrap().clear();
    let mut h = begin_data(
        &tm,
        Data {
            reads: vec![ra],
            writes: vec![wa(&kb, b"v2")],
            scans: Vec::new(),
        },
    );
    tm.commit(&mut h).await.unwrap();
    tm.end(&mut h).await.unwrap();

    let c = write_counts(&log);
    assert!(
        c.leaf >= 2,
        "a read of another key forces the full path: {c:?}"
    );
}
