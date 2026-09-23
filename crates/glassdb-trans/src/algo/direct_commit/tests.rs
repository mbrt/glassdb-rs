//! Direct-path behavior and fallback coverage.

use std::collections::BTreeMap;

use super::super::tests::{
    Tctx, begin_accesses, commit_access, commit_writes, do_read, entry, leaf_reads, logical_key,
    new_algo, new_algo_from_backend, new_recording_algo, new_recording_algo_big_cache,
    read_outcome, test_collection, test_root_path, wa, wdel, write_counts,
};
use super::super::*;
use super::*;
use crate::key_state_resolver::KeyStateResolver;
use crate::leaf_coord::{MemberOutcome, MemberPolicy, ReloadCause, ResolveCtx, Step};
use glassdb_backend::middleware::{BackendOp, HookBackend, HookFuture, OpLog, RecordingBackend};
use glassdb_backend::{Backend, memory::MemoryBackend};
use glassdb_data::{CollectionAddress, CollectionId, NodeToken};
use glassdb_storage::transaction::{TxCommitStatus, TxRecord};
use glassdb_storage::{
    CollectionRecord, CurrentState, IndexNode, LeafBody, LeafEntry, Node, NodeLocks,
};

/// Evaluates one policy and retains its proposed decision.
async fn resolve_step(
    policy: &dyn MemberPolicy,
    tctx: &Tctx,
    cause: ReloadCause,
    staged: &BTreeMap<Vec<u8>, LeafEntry>,
    locks: &NodeLocks,
) -> Step {
    let key_state = KeyStateResolver::new(tctx.tmon.clone());
    let ctx = ResolveCtx {
        key_state: &key_state,
        tmon: &tctx.tmon,
        requirement: Requirement::ANY,
        cause,
    };
    policy.resolve(&ctx, staged, locks).await.unwrap()
}

/// Classifies a direct-commit outcome for a chosen leaf state and reload cause.
async fn resolve_outcome(
    policy: &dyn MemberPolicy,
    tctx: &Tctx,
    cause: ReloadCause,
    staged: &BTreeMap<Vec<u8>, LeafEntry>,
    locks: &NodeLocks,
) -> MemberOutcome {
    match resolve_step(policy, tctx, cause, staged, locks).await {
        Step::Skip { outcome } | Step::Stage { outcome, .. } => outcome,
    }
}

fn put_policy(
    tm: &Algo,
    id: TxId,
    key: LogicalKey,
    read_writer: Option<Option<TxId>>,
    value: &[u8],
) -> DirectCommitOperation {
    let has_reads = read_writer.is_some();
    DirectCommitOperation::new(
        id,
        test_root_path(),
        DirectMember {
            keys: vec![DirectKey {
                raw_key: key.key().to_vec(),
                key,
                read: read_writer.map(|writer| {
                    let absence_generation = writer.is_none().then_some(0);
                    ReadPredicate::new(writer, absence_generation)
                }),
                write: Some(DirectWrite::Put(Arc::from(value))),
            }]
            .into(),
            writes: 1,
            has_reads,
        },
        InlinePolicy::default(),
        tm.direct_commit.split_hints.clone(),
    )
}

async fn membership_generation(tctx: &Tctx) -> u64 {
    tctx.nodes
        .load_leaf(
            &test_root_path(),
            Requirement::after(tctx.timeline.currentness_barrier()),
        )
        .await
        .unwrap()
        .locks()
        .membership_generation()
}

// Single-rw commit (ADR-030): a lone read-modify-write whose read was
// superseded by *another instance* is caught with a transparent body replay, never
// a surfaced error, and never commits its stale value. This database instance's cached
// snapshot predates the peer's create, so the key reads as absent — an
// unsupported shape rather than a certified invalidated read, which is why the
// locked commit takes over instead of replaying the body (ADR-053). It resolves
// as `Wounded` or `Retry` depending on whether the snapshot survived to the
// direct-commit policy evaluation; both converge on a fresh read.
#[tokio::test]
async fn single_rw_invalidated_read_renews_and_converges() {
    let (tm, tctx) = new_algo().await;
    let (tm2, _t2) = new_algo_from_backend(tctx.backend.clone()).await;
    let keyp = logical_key(b"k");

    commit_writes(&tm2, vec![wa(&keyp, b"v1")]).await;
    let ra = do_read(&tctx, &keyp).await;

    // Another database instance overwrites the key, making `ra` stale.
    let h2 = commit_writes(&tm2, vec![wa(&keyp, b"v2")]).await;

    let mut h = begin_accesses(
        &tm,
        AccessSet::new(vec![ra], vec![wa(&keyp, b"v3")], Vec::new()),
    );
    assert_eq!(tm.commit(&mut h).await.unwrap(), BodyDecision::ReplayBody);
    tm.end(&mut h).await.unwrap();

    // The stale write never committed: v2 is still current (the discarded
    // identity's record is unreferenced, so help-forward cannot promote it).
    assert!(
        do_read(&tctx, &keyp).await.validates(Some(h2.id()), 0),
        "the stale write did not commit; v2 is still current"
    );

    // A fresh read + commit converges (the replay observes v2 and commits).
    let ra2 = do_read(&tctx, &keyp).await;
    let h3 = commit_access(
        &tm,
        AccessSet::new(vec![ra2], vec![wa(&keyp, b"v3")], Vec::new()),
    )
    .await;
    assert!(
        do_read(&tctx, &keyp).await.validates(Some(h3.id()), 0),
        "the renewed identity commits"
    );
}

/// Controls a hook that gates the coordinator's next CAS.
struct Gate {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    armed: std::sync::atomic::AtomicBool,
}

impl Gate {
    fn wrap_writes(inner: Arc<dyn Backend>) -> (Arc<HookBackend>, Arc<Self>) {
        let gate = Arc::new(Self {
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
            armed: std::sync::atomic::AtomicBool::new(false),
        });
        let backend = HookBackend::new(inner);
        backend.set_before({
            let gate = gate.clone();
            move |op| {
                use std::sync::atomic::Ordering::SeqCst;
                let matches = matches!(op, BackendOp::WriteIf { .. });
                let wait = matches && gate.armed.swap(false, SeqCst);
                let entered = gate.entered.clone();
                let release = gate.release.clone();
                let future: HookFuture = Box::pin(async move {
                    if wait {
                        entered.notify_one();
                        release.notified().await;
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

    async fn wait_until_blocked(&self) {
        self.entered.notified().await;
    }

    fn release(&self) {
        self.release.notify_one();
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
                        "simulated in-doubt leaf CAS".into(),
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
fn same_leaf_sibling(base: &[u8]) -> Vec<u8> {
    let sib = b"sibling".to_vec();
    assert_ne!(sib, base, "sibling must differ from the base key");
    sib
}

fn leaf_stores(log: &OpLog, path: &str) -> usize {
    log.lock()
        .unwrap()
        .iter()
        .filter(|r| r.path == path && (r.op == "write_if" || r.op == "write_if_not_exists"))
        .count()
}

// ADR-028: direct commit is coordinated by the same leaf coordinator
// as ordinary lock acquisition, so a direct commit and a disjoint-key
// acquire contending one leaf batch into a single CAS round instead of
// racing two separate loads+CASes. The commit publishes its value and the
// acquire installs its lock in the one store.
#[tokio::test]
async fn direct_commit_merges_with_disjoint_acquire() {
    let (tm, tctx, log) = new_recording_algo_big_cache().await;

    let ka = b"k".to_vec();
    let kb = same_leaf_sibling(&ka);
    let kap = logical_key(&ka);
    let kbp = logical_key(&kb);

    // Seed keys A and B committed: the direct commit builds on A's
    // predecessor, and the disjoint acquire overwrites an existing B, so it
    // takes no membership root lock and the round stays a single leaf CAS.
    commit_writes(&tm, vec![wa(&kap, b"v1")]).await;
    commit_writes(&tm, vec![wa(&kbp, b"vb1")]).await;

    let txb = TxId::with_priority(2_000_000_000, b"acquire");
    tctx.tmon.begin_tx(&txb);

    let leaf_path = test_root_path().to_string();
    log.lock().unwrap().clear();
    // Both submissions can use the warm leaf. The coordinator must provide
    // a join opportunity without depending on a backend read.
    let data_b = AccessSet::new(Vec::new(), vec![wa(&kbp, b"vb2")], Vec::new());
    let lock_requirement = Requirement::after(tctx.timeline.currentness_barrier());
    let mut ha = begin_accesses(
        &tm,
        AccessSet::new(Vec::new(), vec![wa(&kap, b"v2")], Vec::new()),
    );
    let txa = ha.id().clone();
    let (acquire, committed) = tokio::join!(
        tctx.locker
            .keys()
            .lock_at(&txb, &data_b, false, lock_requirement),
        tm.commit(&mut ha),
    );
    let acquire = acquire.unwrap();
    assert_eq!(committed.unwrap(), BodyDecision::ReturnOutcome);
    assert_eq!(leaf_reads(&log), (0, 0));
    assert!(
        matches!(acquire, LockOutcome::Locked(_)),
        "the disjoint acquire must lock"
    );

    assert_eq!(
        leaf_stores(&log, &leaf_path),
        1,
        "direct commit and disjoint acquire share one CAS"
    );

    // Both mutations landed in the shared leaf write.
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
// recovers idempotently — the engine reloads and rebuilds the mutation plan, the commit finds
// its own marker already published (`Landed`), and the acquire re-installs
// its own lock (`Locked`) without double-applying. No error is surfaced.
#[tokio::test(start_paused = true)]
async fn direct_commit_batched_in_doubt_recovers() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let (backend, indoubt) = InDoubtCas::wrap(mem);
    let recorder = Arc::new(RecordingBackend::new(backend));
    let log = recorder.log();
    let (tm, tctx) = new_algo_from_backend(recorder).await;

    let ka = b"k".to_vec();
    let kb = same_leaf_sibling(&ka);
    let kap = logical_key(&ka);
    let kbp = logical_key(&kb);

    // Seed keys A and B before arming the reply fault: the commit has
    // a predecessor and the acquire overwrites an existing B, so it takes no
    // membership root lock and the round stays a single leaf CAS.
    commit_writes(&tm, vec![wa(&kap, b"v1")]).await;
    commit_writes(&tm, vec![wa(&kbp, b"vb1")]).await;

    let txb = TxId::with_priority(2_000_000_000, b"acquire");
    tctx.tmon.begin_tx(&txb);

    indoubt.arm();
    log.lock().unwrap().clear();
    let mut ha = begin_accesses(
        &tm,
        AccessSet::new(Vec::new(), vec![wa(&kap, b"v2")], Vec::new()),
    );
    let txa = ha.id().clone();
    let data_b = AccessSet::new(Vec::new(), vec![wa(&kbp, b"vb2")], Vec::new());
    let requirement = Requirement::after(tctx.timeline.currentness_barrier());
    let (acquire, committed) = tokio::join!(
        tctx.locker
            .keys()
            .lock_at(&txb, &data_b, false, requirement),
        tm.commit(&mut ha),
    );

    // One in-doubt CAS must carry both members. Recovery must find the
    // installed value and hold without applying another mutation.
    let acquire = acquire.unwrap();
    assert_eq!(committed.unwrap(), BodyDecision::ReturnOutcome);
    assert_eq!(leaf_stores(&log, &test_root_path().to_string()), 1);
    assert!(!indoubt.armed.load(std::sync::atomic::Ordering::SeqCst));
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
// locked commit instead of ADR-051's direct one (ADR-053).
fn external_value() -> Vec<u8> {
    vec![b'v'; glassdb_storage::InlinePolicy::default().max_value_bytes + 1]
}

// ADR-053: a single-key read-modify-write whose value misses the inline
// budget has no direct commit to fall to, so it commits through locked commit:
// one committed transaction-record write, one leaf lock
// CAS, one leaf write-back CAS (run synchronously here because there is no
// background executor), and no separate membership write — and the new
// value is durable and readable. With split deferred the leaf is the
// tree root `_r`, so both leaf CAS's land there (ADR-031).
#[tokio::test]
async fn an_overwrite_over_the_inline_budget_uses_a_locked_commit() {
    let (tm, tctx, log) = new_recording_algo().await;
    let keyp = logical_key(b"k");

    commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;
    let r = do_read(&tctx, &keyp).await;

    log.lock().unwrap().clear();
    tctx.locker.stats_and_reset();
    let mut h = begin_accesses(
        &tm,
        AccessSet::new(vec![r], vec![wa(&keyp, &external_value())], Vec::new()),
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
        "locked commit: one lock CAS plus one write-back CAS, no membership: {c:?}"
    );
    assert_eq!(c.tx, 1, "one committed transaction-record write: {c:?}");

    // The commit landed: the leaf points at us with no live lock, a
    // committed transaction record exists, and the value reads back as ours.
    let e = entry(&tctx, b"k").await.unwrap();
    assert_eq!(e.current.writer(), Some(&tid));
    assert!(e.lock_holders().is_empty());
    let status = tctx
        .tx_records
        .commit_status_at(&tid, Requirement::ANY)
        .await
        .unwrap();
    assert_eq!(status.status, TxCommitStatus::Committed);
    let r = do_read(&tctx, &keyp).await;
    assert!(r.validates(Some(&tid), 0));
}

#[tokio::test(start_paused = true)]
async fn single_rw_observing_a_gate_uses_a_locked_commit() {
    let (tm, tctx) = new_algo().await;
    let keyp = logical_key(b"k");
    commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;
    let read = do_read(&tctx, &keyp).await;

    let gate = TxId::with_priority(0, b"gate");
    tctx.tmon.begin_tx(&gate);
    let (mut root, root_observation) = tctx
        .nodes
        .load_root(&test_collection(), Requirement::ANY)
        .await
        .unwrap();
    root.set_structural_gate(gate.clone());
    assert!(
        tctx.nodes
            .store_root(&test_collection(), &root, &root_observation)
            .await
            .unwrap()
    );

    tctx.locker.stats_and_reset();
    let mut handle = begin_accesses(
        &tm,
        AccessSet::new(vec![read], vec![wa(&keyp, b"v2")], Vec::new()),
    );
    let parallel_id = handle.id().clone();
    let committing_tm = tm.clone();
    let committing = tokio::spawn(async move {
        let result = committing_tm.commit(&mut handle).await;
        (handle, result)
    });
    rt::sleep(Duration::from_millis(50)).await;
    assert!(!committing.is_finished());

    tctx.tmon
        .commit_tx(TxRecord::new(gate, TxCommitStatus::Committed))
        .await
        .unwrap();
    let (mut handle, result) = committing.await.unwrap();
    assert_eq!(result.unwrap(), BodyDecision::ReturnOutcome);
    assert_ne!(*handle.id(), parallel_id);
    tm.end(&mut handle).await.unwrap();
    assert!(
        tctx.locker.stats_and_reset().calls >= 1,
        "an observed gate bypasses the direct commit"
    );
    assert!(do_read(&tctx, &keyp).await.validates(Some(handle.id()), 0));
}

// ADR-030: a warm single read-write commit reuses the leaf the read cached
// for both its eligibility check and its lock-install attempt (`Any`), so
// it issues no backend leaf read for either. The successful install CAS
// supplies the write-back's lower bound too, so write-back also reuses the
// installed cached state. A revalidating eligibility, install, or write-back
// would add a `read_if_modified`, so pinning the total to zero guards the
// receipt propagation. A large cache keeps this deterministic (nothing is
// evicted between the read and the commit).
#[tokio::test]
async fn single_rw_commit_reuses_cached_leaf() {
    let (tm, tctx, log) = new_recording_algo_big_cache().await;
    let keyp = logical_key(b"k");

    commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;
    // The read warms the leaf in the object cache.
    let r = do_read(&tctx, &keyp).await;

    log.lock().unwrap().clear();
    let mut h = begin_accesses(
        &tm,
        AccessSet::new(vec![r], vec![wa(&keyp, b"v2")], Vec::new()),
    );
    tm.commit(&mut h).await.unwrap();
    tm.end(&mut h).await.unwrap();

    let (full, revalidate) = leaf_reads(&log);
    assert_eq!(full, 0, "no cold leaf read on a warm commit");
    assert_eq!(
        revalidate, 0,
        "eligibility, install, and write-back reuse cache/CAS evidence"
    );
}

// A blind single-key put over an existing key (no read) takes the same
// locked commit when its value misses the inline budget.
#[tokio::test]
async fn a_blind_put_over_the_inline_budget_uses_a_locked_commit() {
    let (tm, tctx, log) = new_recording_algo().await;
    let keyp = logical_key(b"k");

    commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;

    log.lock().unwrap().clear();
    let mut h = begin_accesses(
        &tm,
        AccessSet::new(Vec::new(), vec![wa(&keyp, &external_value())], Vec::new()),
    );
    let tid = h.id().clone();
    tm.commit(&mut h).await.unwrap();
    tm.end(&mut h).await.unwrap();

    let c = write_counts(&log);
    assert_eq!(
        c.leaf, 2,
        "locked commit: one lock CAS plus one write-back CAS, no membership: {c:?}"
    );
    assert_eq!(c.tx, 1, "one committed transaction-record write: {c:?}");
    assert_eq!(
        entry(&tctx, b"k").await.unwrap().current.writer(),
        Some(&tid)
    );
}

// ADR-020 regression: locked commit leaves a write lock held by the
// *committed* writer until its asynchronous write-back publishes the current state
// and releases it. A single-key writer arriving in that window must treat the
// committed holder as effectively unlocked — help-forwarding it as the
// predecessor — and stay on direct commit, rather than bailing to
// locked commit on the mere presence of the lock (the measured regression).
// An invalidated read replays instead.
#[tokio::test]
async fn a_committed_holder_keeps_the_next_writer_on_direct_commit() {
    let (tm, tctx) = new_algo().await;
    let keyp = logical_key(b"k");
    let leaf_path = test_root_path();
    let raw = b"k".to_vec();

    // H0 publishes v1; H1 overwrites through locked commit (its value
    // misses the inline budget), so it has a committed transaction record.
    let h0 = commit_writes(&tm, vec![wa(&keyp, b"v1")])
        .await
        .id()
        .clone();
    let h1 = commit_writes(&tm, vec![wa(&keyp, &external_value())])
        .await
        .id()
        .clone();

    // Recreate the commit window before write-back: the lock is still held by
    // the committed H1 while the current state lags at its predecessor H0.
    let loaded = tctx
        .nodes
        .load_leaf(
            &leaf_path,
            Requirement::after(tctx.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    let windowed = LeafBody::from_entries(loaded.entries().entries().cloned().map(|mut e| {
        if e.key == raw {
            e.replace_write_lock(h1.clone());
            e.current = CurrentState::External { writer: h0.clone() };
        }
        e
    }));
    let mut edit = loaded.into_edit();
    edit.set_entries(windowed);
    assert!(tctx.nodes.commit_leaf(edit).await.unwrap().is_applied());

    // The window is observably at the committed holder H1 (v2), not the
    // lagging current state H0: the shared resolver already help-forwards it.
    let r = do_read(&tctx, &keyp).await;
    assert!(r.validates(Some(&h1), 0));

    // End to end: the writer commits directly over H1 (help-forwarding it
    // into the chain, not orphaning it), taking no lock of its own.
    tctx.locker.stats_and_reset();
    let mut h = begin_accesses(
        &tm,
        AccessSet::new(vec![r], vec![wa(&keyp, b"v3")], Vec::new()),
    );
    let h2 = h.id().clone();
    tm.commit(&mut h).await.unwrap();
    tm.end(&mut h).await.unwrap();

    assert_eq!(
        tctx.locker.stats_and_reset().calls,
        0,
        "the committed holder did not push the writer onto locked commit"
    );
    let e = entry(&tctx, b"k").await.unwrap();
    assert_eq!(e.current.writer(), Some(&h2));
    assert!(e.lock_holders().is_empty());
    assert!(do_read(&tctx, &keyp).await.validates(Some(&h2), 0));
}

// ADR-051: an eligible small overwrite commits in a single conditional leaf
// CAS that publishes the value itself — no lock, no transaction record, and
// nothing to write back — and the value reads back from the leaf alone.
#[tokio::test]
async fn direct_commit_overwrites_in_one_leaf_cas() {
    let (tm, tctx, log) = new_recording_algo().await;
    let keyp = logical_key(b"k");

    commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;
    let r = do_read(&tctx, &keyp).await;

    log.lock().unwrap().clear();
    let mut h = begin_accesses(
        &tm,
        AccessSet::new(vec![r], vec![wa(&keyp, b"v2")], Vec::new()),
    );
    let tid = h.id().clone();
    tm.commit(&mut h).await.unwrap();
    tm.end(&mut h).await.unwrap();

    let c = write_counts(&log);
    assert_eq!(c.leaf, 1, "the commit is one leaf CAS: {c:?}");
    assert_eq!(c.tx, 0, "the transaction has no record at all: {c:?}");

    let e = entry(&tctx, b"k").await.unwrap();
    assert_eq!(
        e.current,
        CurrentState::Inline {
            writer: tid.clone(),
            value: Arc::from(b"v2".as_slice()),
        }
    );
    assert!(e.lock_holders().is_empty(), "no lock was ever installed");
    let value = read_outcome(&tctx, &keyp).await.value.unwrap();
    assert_eq!(value.writer, tid);
}

// ADR-051 regression: a direct commit lands on an entry whose write lock is
// still held by an *already-committed* writer awaiting write-back, so it must
// replace that holder. Left in place, writer resolution help-forwards to the
// holder and resolves the entry *backwards* — to the value this commit just
// superseded — silently losing updates.
#[tokio::test]
async fn direct_commit_replaces_a_committed_holder() {
    let (tm, tctx) = new_algo().await;
    let keyp = logical_key(b"k");
    let leaf_path = test_root_path();
    let raw = b"k".to_vec();

    let h0 = commit_writes(&tm, vec![wa(&keyp, b"v1")])
        .await
        .id()
        .clone();
    let h1 = commit_writes(&tm, vec![wa(&keyp, &external_value())])
        .await
        .id()
        .clone();

    // Locked commit's commit window: the lock is still held by the committed
    // H1 while the current state lags at its predecessor H0.
    let loaded = tctx
        .nodes
        .load_leaf(
            &leaf_path,
            Requirement::after(tctx.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    let windowed = LeafBody::from_entries(loaded.entries().entries().cloned().map(|mut e| {
        if e.key == raw {
            e.replace_write_lock(h1.clone());
            e.current = CurrentState::External { writer: h0.clone() };
        }
        e
    }));
    let mut edit = loaded.into_edit();
    edit.set_entries(windowed);
    assert!(tctx.nodes.commit_leaf(edit).await.unwrap().is_applied());

    let mut h = begin_accesses(
        &tm,
        AccessSet::new(Vec::new(), vec![wa(&keyp, b"v3")], Vec::new()),
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
    let value = outcome.value.unwrap();
    assert_eq!(value.writer, h2);
    assert_eq!(&*value.value, b"v3");
}

// ADR-051 regression: every reason a policy declines to publish the commit
// marker must be classified against the round's in-doubt evidence, not just
// the lost-race one. A structural gate or a drop intent that
// appears *after* an in-doubt CAS is no proof that the CAS did not land, so
// reporting `Moved` there would let locked commit replay a body whose direct
// commit may already be durable (and since superseded, invisible).
#[tokio::test]
async fn direct_commit_blocked_after_in_doubt_cas_stays_in_doubt() {
    let (tm, tctx) = new_algo().await;
    let keyp = logical_key(b"k");
    commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;

    let seed = entry(&tctx, b"k").await.unwrap();
    let policy = put_policy(
        &tm,
        TxId::with_priority(2, b"direct"),
        keyp.clone(),
        Some(seed.current.writer().cloned()),
        b"v2",
    );
    let staged = BTreeMap::from([(b"k".to_vec(), seed)]);

    let mut gated = NodeLocks::default();
    gated.set_structural_gate(TxId::with_priority(1, b"splitter"));
    let mut fenced = NodeLocks::default();
    fenced.set_drop_intent(TxId::with_priority(1, b"dropper"));

    for (what, locks) in [("a structural gate", &gated), ("a drop intent", &fenced)] {
        // Nothing was written yet, so locked commit may take over.
        let outcome = resolve_outcome(&policy, &tctx, ReloadCause::Fresh, &staged, locks).await;
        assert!(
            matches!(outcome, MemberOutcome::Moved),
            "{what} on a first evaluation proves nothing was written, got {outcome:?}"
        );

        let outcome = resolve_outcome(
            &policy,
            &tctx,
            ReloadCause::Reloaded { in_doubt: true },
            &staged,
            locks,
        )
        .await;
        assert!(
            matches!(outcome, MemberOutcome::InDoubt(_)),
            "{what} cannot disprove a landed in-doubt CAS, got {outcome:?}"
        );
    }
}

#[tokio::test]
async fn direct_membership_change_neither_waits_for_nor_wounds_a_live_holder() {
    let (tm, tctx) = new_algo().await;
    let holder = TxId::with_priority(9, b"membership-holder");
    tctx.tmon.begin_tx(&holder);
    let direct = put_policy(
        &tm,
        TxId::with_priority(1, b"direct"),
        logical_key(b"new"),
        None,
        b"value",
    );
    let mut locks = NodeLocks::default();
    locks.set_membership_writer(holder.clone());

    let outcome =
        resolve_outcome(&direct, &tctx, ReloadCause::Fresh, &BTreeMap::new(), &locks).await;
    assert!(matches!(outcome, MemberOutcome::Moved));
    assert_eq!(
        tctx.tmon.tx_status(&holder).await.unwrap(),
        TxCommitStatus::Pending,
        "direct commit delegates waiting and wounding to locked commit"
    );
}

#[tokio::test]
async fn direct_commit_replays_an_absence_read_from_an_older_generation() {
    let (tm, tctx) = new_algo().await;
    let direct = put_policy(
        &tm,
        TxId::with_priority(1, b"direct"),
        logical_key(b"missing"),
        Some(None),
        b"value",
    );
    let mut locks = NodeLocks::default();
    locks.advance_membership_generation();

    assert!(matches!(
        resolve_step(&direct, &tctx, ReloadCause::Fresh, &BTreeMap::new(), &locks,).await,
        Step::Skip {
            outcome: MemberOutcome::Replay
        }
    ));
}

// ADR-051 regression (fuzz `history` crash-3ddc66ba): a blind put is
// last-writer-wins on a first evaluation, but after its own in-doubt CAS the entry
// may already hold a commit that read the very value that CAS published.
// Republishing then rolls the key back behind a commit whose writer was told it
// succeeded, losing that update. Only an entry still naming the writer the
// in-doubt CAS built on proves nothing landed.
#[tokio::test]
async fn a_blind_put_after_an_in_doubt_cas_never_republishes_over_a_newer_writer() {
    let (tm, tctx) = new_algo().await;
    let keyp = logical_key(b"k");
    commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;
    let seed = entry(&tctx, b"k").await.unwrap();
    let locks = NodeLocks::default();

    let blind = put_policy(
        &tm,
        TxId::with_priority(9, b"blind"),
        keyp.clone(),
        None,
        b"v2",
    );
    let staged = BTreeMap::from([(b"k".to_vec(), seed)]);

    // The first evaluation proposes publication over the seeded writer. Its CAS
    // comes back in doubt.
    assert!(matches!(
        resolve_step(&blind, &tctx, ReloadCause::Fresh, &staged, &locks).await,
        Step::Stage {
            outcome: MemberOutcome::Landed,
            ..
        }
    ));

    // The entry still names that writer, so the in-doubt CAS provably did not
    // land and the publication is retried rather than surfaced as in-doubt.
    assert!(matches!(
        resolve_step(
            &blind,
            &tctx,
            ReloadCause::Reloaded { in_doubt: true },
            &staged,
            &locks,
        )
        .await,
        Step::Stage {
            outcome: MemberOutcome::Landed,
            ..
        }
    ));

    let superseded = BTreeMap::from([(
        b"k".to_vec(),
        LeafEntry::new(b"k".to_vec()).with_current(CurrentState::Inline {
            writer: TxId::with_priority(10, b"newer"),
            value: Arc::from(b"v3".as_slice()),
        }),
    )]);
    let outcome = resolve_outcome(
        &blind,
        &tctx,
        ReloadCause::Reloaded { in_doubt: true },
        &superseded,
        &locks,
    )
    .await;
    assert!(
        matches!(outcome, MemberOutcome::InDoubt(_)),
        "a newer writer cannot disprove a landed in-doubt CAS, got {outcome:?}"
    );
}

#[tokio::test]
async fn any_exact_output_marker_proves_a_mixed_member_landed() {
    let (tm, tctx) = new_algo().await;
    let ka = logical_key(b"a");
    let kb = logical_key(b"b");
    let id = TxId::with_priority(9, b"mixed");
    let member = direct_member(&AccessSet::new(
        Vec::new(),
        vec![wa(&ka, b"a2"), wdel(&kb)],
        Vec::new(),
    ))
    .unwrap();
    let policy = DirectCommitOperation::new(
        id.clone(),
        test_root_path(),
        member,
        InlinePolicy::default(),
        tm.direct_commit.split_hints.clone(),
    );
    let pa = TxId::with_priority(1, b"pa");
    let pb = TxId::with_priority(1, b"pb");
    let predecessors = BTreeMap::from([
        (
            b"a".to_vec(),
            LeafEntry::new(b"a").with_current(CurrentState::External { writer: pa }),
        ),
        (
            b"b".to_vec(),
            LeafEntry::new(b"b").with_current(CurrentState::External { writer: pb }),
        ),
    ]);
    assert!(matches!(
        resolve_step(
            &policy,
            &tctx,
            ReloadCause::Fresh,
            &predecessors,
            &NodeLocks::default(),
        )
        .await,
        Step::Stage { .. }
    ));

    let recovered = BTreeMap::from([
        (
            b"a".to_vec(),
            LeafEntry::new(b"a").with_current(CurrentState::Inline {
                writer: TxId::with_priority(10, b"later"),
                value: Arc::from(b"a3".as_slice()),
            }),
        ),
        (
            b"b".to_vec(),
            LeafEntry::new(b"b").with_current(CurrentState::Tombstone { writer: id.clone() }),
        ),
    ]);
    assert!(matches!(
        resolve_step(
            &policy,
            &tctx,
            ReloadCause::Reloaded { in_doubt: true },
            &recovered,
            &NodeLocks::default(),
        )
        .await,
        Step::Skip {
            outcome: MemberOutcome::Landed
        }
    ));
    assert!(policy.proven_landed());
}

#[tokio::test]
async fn reclaimed_all_absent_delete_markers_leave_recovery_in_doubt() {
    let (tm, tctx) = new_algo().await;
    let id = TxId::with_priority(9, b"deletes");
    let member = direct_member(&AccessSet::new(
        Vec::new(),
        vec![wdel(&logical_key(b"a")), wdel(&logical_key(b"b"))],
        Vec::new(),
    ))
    .unwrap();
    let policy = DirectCommitOperation::new(
        id,
        test_root_path(),
        member,
        InlinePolicy::default(),
        tm.direct_commit.split_hints.clone(),
    );
    let empty = BTreeMap::new();
    assert!(matches!(
        resolve_step(
            &policy,
            &tctx,
            ReloadCause::Fresh,
            &empty,
            &NodeLocks::default(),
        )
        .await,
        Step::Stage { .. }
    ));
    assert!(matches!(
        resolve_step(
            &policy,
            &tctx,
            ReloadCause::Reloaded { in_doubt: true },
            &empty,
            &NodeLocks::default(),
        )
        .await,
        Step::Skip {
            outcome: MemberOutcome::InDoubt(_)
        }
    ));
}

#[tokio::test]
async fn a_surviving_predecessor_can_still_prove_mixed_deletes_did_not_land() {
    let (tm, tctx) = new_algo().await;
    let id = TxId::with_priority(9, b"deletes");
    let member = direct_member(&AccessSet::new(
        Vec::new(),
        vec![wdel(&logical_key(b"a")), wdel(&logical_key(b"b"))],
        Vec::new(),
    ))
    .unwrap();
    let policy = DirectCommitOperation::new(
        id,
        test_root_path(),
        member,
        InlinePolicy::default(),
        tm.direct_commit.split_hints.clone(),
    );
    let predecessor = TxId::with_priority(1, b"predecessor");
    let unchanged = BTreeMap::from([(
        b"a".to_vec(),
        LeafEntry::new(b"a").with_current(CurrentState::Tombstone {
            writer: predecessor,
        }),
    )]);
    assert!(matches!(
        resolve_step(
            &policy,
            &tctx,
            ReloadCause::Fresh,
            &unchanged,
            &NodeLocks::default(),
        )
        .await,
        Step::Stage { .. }
    ));
    assert!(matches!(
        resolve_step(
            &policy,
            &tctx,
            ReloadCause::Reloaded { in_doubt: true },
            &unchanged,
            &NodeLocks::default(),
        )
        .await,
        Step::Stage {
            outcome: MemberOutcome::Landed,
            ..
        }
    ));
}

// ADR-053: only a *superseded read* certifies the body-replay case, and an
// in-doubt CAS still outranks it. Every other way a policy declines is either
// state direct commit cannot arbitrate or evidence that proves nothing, so
// it reports `Moved` and locked commit takes over. Classifying too
// broadly would spin the body forever against a holder or a closed budget.
#[tokio::test]
async fn direct_commit_replays_only_a_certified_superseded_read() {
    let (tm, tctx) = new_algo().await;
    let keyp = logical_key(b"k");
    commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;
    let seed = entry(&tctx, b"k").await.unwrap();
    let current = seed.current.writer().cloned().unwrap();
    let locks = NodeLocks::default();
    let split_hints = tm.direct_commit.split_hints.clone();

    let direct = |read_writer: Option<TxId>| {
        put_policy(
            &tm,
            TxId::with_priority(9, b"direct"),
            keyp.clone(),
            read_writer.map(Some),
            b"v2",
        )
    };

    // A read the entry has moved past: nothing is staged and the loss is
    // definitive, so the body is reevaluated against the winner.
    let stale = direct(Some(TxId::with_priority(1, b"stale")));
    let staged = BTreeMap::from([(b"k".to_vec(), seed.clone())]);
    let outcome = resolve_outcome(&stale, &tctx, ReloadCause::Fresh, &staged, &locks).await;
    assert!(
        matches!(outcome, MemberOutcome::Replay),
        "a superseded read staged nothing and can be reevaluated, got {outcome:?}"
    );
    let outcome = resolve_outcome(
        &stale,
        &tctx,
        ReloadCause::Reloaded { in_doubt: true },
        &staged,
        &locks,
    )
    .await;
    assert!(
        matches!(outcome, MemberOutcome::InDoubt(_)),
        "an in-doubt CAS is never downgraded to a replay, got {outcome:?}"
    );

    // A live pending holder is a genuine conflict only wound-wait resolves,
    // even though this transaction also staged nothing.
    let holder = TxId::with_priority(1, b"holder");
    tctx.tmon.begin_tx(&holder);
    let mut held = seed.clone();
    held.replace_write_lock(holder);
    let outcome = resolve_outcome(
        &direct(Some(current.clone())),
        &tctx,
        ReloadCause::Fresh,
        &BTreeMap::from([(b"k".to_vec(), held)]),
        &locks,
    )
    .await;
    assert!(
        matches!(outcome, MemberOutcome::Moved),
        "a live holder needs locked commit, not a replay, got {outcome:?}"
    );

    // A key read as deleted names the very writer that deleted it. ADR-061 can
    // now create directly over that tombstone.
    let deleter = TxId::with_priority(1, b"deleter");
    let buried = seed.clone().with_current(CurrentState::Tombstone {
        writer: deleter.clone(),
    });
    let outcome = resolve_outcome(
        &direct(Some(deleter)),
        &tctx,
        ReloadCause::Fresh,
        &BTreeMap::from([(b"k".to_vec(), buried)]),
        &locks,
    )
    .await;
    assert!(
        matches!(outcome, MemberOutcome::Landed),
        "a put over a tombstone is a direct create, got {outcome:?}"
    );

    // Aggregate inline admission is owned by the direct-commit policy. Existing
    // inline values consume the leaf budget, while this key's prior state
    // is replaced rather than double-counted.
    let mut budgeted = direct(Some(current.clone()));
    budgeted.inline = InlinePolicy {
        max_value_bytes: 64,
        max_leaf_bytes: 5,
    };
    let other_writer = TxId::with_priority(1, b"other");
    let crowded = BTreeMap::from([
        (b"k".to_vec(), seed),
        (
            b"other".to_vec(),
            LeafEntry::new(b"other").with_current(CurrentState::Inline {
                writer: other_writer,
                value: Arc::from(b"four".as_slice()),
            }),
        ),
    ]);
    assert!(matches!(
        resolve_step(&budgeted, &tctx, ReloadCause::Fresh, &crowded, &locks).await,
        Step::Skip {
            outcome: MemberOutcome::Moved
        }
    ));
    assert_eq!(split_hints.pending_inline_pressure(), 1);
    assert!(matches!(
        resolve_step(
            &budgeted,
            &tctx,
            ReloadCause::Reloaded { in_doubt: true },
            &crowded,
            &locks,
        )
        .await,
        Step::Skip {
            outcome: MemberOutcome::InDoubt(_)
        }
    ));
    assert_eq!(
        split_hints.pending_inline_pressure(),
        1,
        "an unproved in-doubt outcome returns before creating new pressure"
    );

    let mut impossible = direct(Some(current.clone()));
    impossible.inline = InlinePolicy {
        max_value_bytes: 64,
        max_leaf_bytes: 1,
    };
    assert!(matches!(
        resolve_step(&impossible, &tctx, ReloadCause::Fresh, &crowded, &locks).await,
        Step::Skip {
            outcome: MemberOutcome::Moved
        }
    ));
    assert_eq!(
        split_hints.pending_inline_pressure(),
        1,
        "a value no leaf can admit does not request a split"
    );

    // The round-level classifications: a same-key reservation proves this member
    // staged nothing, while a spent CAS budget proves nothing about an
    // earlier evaluation of the same round. And a blind overwrite has no
    // read-dependent computation to reevaluate.
    let rmw = direct(Some(current));
    assert!(matches!(rmw.excluded_outcome(false), MemberOutcome::Replay));
    assert!(matches!(
        rmw.excluded_outcome(true),
        MemberOutcome::InDoubt(_)
    ));
    assert!(
        matches!(rmw.exhausted_outcome(false), MemberOutcome::Moved),
        "an exhausted budget does not certify a replay"
    );
    assert!(
        matches!(direct(None).excluded_outcome(false), MemberOutcome::Moved),
        "a blind overwrite uses locked commit instead of replaying the body"
    );
}

// ADR-053: a read-modify-write whose observed writer is superseded before
// anything is published reevaluates its body under the same id. Nothing was
// staged, so the identity neither renews nor takes a lock — publishing one
// would make the key's next direct commit ineligible for no reason.
#[tokio::test]
async fn direct_commit_superseded_read_replays_in_place() {
    let (tm, tctx) = new_algo().await;
    let keyp = logical_key(b"k");
    commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;

    // Read v1, then let a later commit supersede it. Both values are this
    // database instance's own, so its snapshot sees the winner rather than a stale leaf.
    let stale = do_read(&tctx, &keyp).await;
    let winner = commit_writes(&tm, vec![wa(&keyp, b"v2")])
        .await
        .id()
        .clone();

    let mut h = begin_accesses(
        &tm,
        AccessSet::new(vec![stale], vec![wa(&keyp, b"v3")], Vec::new()),
    );
    assert_eq!(tm.commit(&mut h).await.unwrap(), BodyDecision::ReplayBody);
    tm.end(&mut h).await.unwrap();
    let status = tctx
        .tx_records
        .commit_status_at(h.id(), Requirement::ANY)
        .await
        .unwrap();
    assert_eq!(
        status.status,
        TxCommitStatus::Unknown,
        "a commit pass that asks for a body replay leaves no transaction record"
    );

    // The stale value never committed and the key kept its direct shape.
    let e = entry(&tctx, b"k").await.unwrap();
    assert_eq!(e.current.writer(), Some(&winner));
    assert!(
        e.lock_holders().is_empty(),
        "a commit pass that asks for a body replay publishes no holder"
    );

    // Reevaluating against the winner commits directly.
    let fresh = do_read(&tctx, &keyp).await;
    let replayed = commit_access(
        &tm,
        AccessSet::new(vec![fresh], vec![wa(&keyp, b"v3")], Vec::new()),
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
// coordinator round, where only one may stage its direct commit. The loser
// must reevaluate its body under the same id rather than publish a holder —
// creating one would make every subsequent direct commit on the key
// ineligible, turning a local scheduling loss into a lasting locked phase.
#[tokio::test]
async fn direct_commit_same_key_round_loser_replays_its_body() {
    let (tm, tctx, log) = new_recording_algo_big_cache().await;

    let ka = b"k".to_vec();
    let kb = same_leaf_sibling(&ka);
    let kap = logical_key(&ka);
    let kbp = logical_key(&kb);
    commit_writes(&tm, vec![wa(&kap, b"v1")]).await;
    commit_writes(&tm, vec![wa(&kbp, b"vb1")]).await;

    // Both transactions read the same current writer, so both are eligible.
    let ra1 = do_read(&tctx, &kap).await;
    let ra2 = do_read(&tctx, &kap).await;
    let rmw = |read| {
        begin_accesses(
            &tm,
            AccessSet::new(vec![read], vec![wa(&kap, b"v2")], Vec::new()),
        )
    };
    let (mut h1, mut h2) = (rmw(ra1), rmw(ra2));

    // All three submissions use the warm leaf and join before planning.
    let driver = TxId::with_priority(1, b"driver");
    tctx.tmon.begin_tx(&driver);
    let data_b = AccessSet::new(Vec::new(), vec![wa(&kbp, b"vb2")], Vec::new());
    let requirement = Requirement::after(tctx.timeline.currentness_barrier());
    log.lock().unwrap().clear();
    let (acquire, r1, r2) = tokio::join!(
        tctx.locker
            .keys()
            .lock_at(&driver, &data_b, false, requirement),
        tm.commit(&mut h1),
        tm.commit(&mut h2),
    );
    assert!(matches!(acquire.unwrap(), LockOutcome::Locked(_)));
    assert_eq!(leaf_stores(&log, &test_root_path().to_string()), 1);
    assert!(entry(&tctx, &kb).await.unwrap().is_locked_by(&driver));

    // Which member wins the round's reservation depends on id order; that exactly
    // one does is the property under test.
    let (winner, mut replayed) = match (&r1, &r2) {
        (Ok(BodyDecision::ReturnOutcome), Ok(BodyDecision::ReplayBody)) => (h1.id().clone(), h2),
        (Ok(BodyDecision::ReplayBody), Ok(BodyDecision::ReturnOutcome)) => (h2.id().clone(), h1),
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
        .tx_records
        .commit_status_at(replayed.id(), Requirement::ANY)
        .await
        .unwrap();
    assert_eq!(
        status.status,
        TxCommitStatus::Unknown,
        "the commit pass that asked for a body replay leaves no transaction record"
    );

    // Reevaluating the body against the winner converges without locking.
    let ra3 = do_read(&tctx, &kap).await;
    let mut h = rmw(ra3);
    tm.commit(&mut h).await.unwrap();
    tm.end(&mut h).await.unwrap();
    assert_eq!(
        entry(&tctx, &ka).await.unwrap().current.writer(),
        Some(h.id()),
        "the replayed body commits directly on its next commit pass"
    );
}

// ADR-061: a create publishes its inline value and membership generation in
// the same direct leaf CAS.
#[tokio::test]
async fn direct_create_uses_one_leaf_cas() {
    let (tm, tctx, log) = new_recording_algo().await;
    let keyp = logical_key(b"new");
    let absent = do_read(&tctx, &keyp).await;

    log.lock().unwrap().clear();
    tctx.locker.stats_and_reset();
    let mut h = begin_accesses(
        &tm,
        AccessSet::new(vec![absent], vec![wa(&keyp, b"v")], Vec::new()),
    );
    let tid = h.id().clone();
    tm.commit(&mut h).await.unwrap();
    tm.end(&mut h).await.unwrap();

    assert_eq!(tctx.locker.stats_and_reset().calls, 0);
    let c = write_counts(&log);
    assert_eq!(c.leaf, 1, "the create is one leaf CAS: {c:?}");
    assert_eq!(c.tx, 0, "the create has no transaction record: {c:?}");
    assert_eq!(
        entry(&tctx, b"new").await.unwrap().current,
        CurrentState::Inline {
            writer: tid,
            value: Arc::from(b"v".as_slice()),
        }
    );
}

// ADR-061: a delete's tombstone is its authoritative direct commit marker.
#[tokio::test]
async fn direct_delete_uses_one_leaf_cas() {
    let (tm, tctx, log) = new_recording_algo().await;
    let keyp = logical_key(b"k");

    commit_writes(&tm, vec![wa(&keyp, b"v")]).await;
    let r = do_read(&tctx, &keyp).await;

    log.lock().unwrap().clear();
    tctx.locker.stats_and_reset();
    let mut h = begin_accesses(&tm, AccessSet::new(vec![r], vec![wdel(&keyp)], Vec::new()));
    let tid = h.id().clone();
    tm.commit(&mut h).await.unwrap();
    tm.end(&mut h).await.unwrap();

    assert_eq!(tctx.locker.stats_and_reset().calls, 0);
    let c = write_counts(&log);
    assert_eq!(c.leaf, 1, "the delete is one leaf CAS: {c:?}");
    assert_eq!(c.tx, 0, "the delete has no transaction record: {c:?}");
    assert_eq!(
        entry(&tctx, b"k").await.unwrap().current,
        CurrentState::Tombstone { writer: tid }
    );
}

// ADR-061: a same-leaf multi-key write is one atomic direct member.
#[tokio::test]
async fn direct_multi_key_put_uses_one_leaf_cas() {
    let (tm, tctx, log) = new_recording_algo().await;
    let ka = logical_key(b"a");
    let kb = logical_key(b"b");

    commit_writes(&tm, vec![wa(&ka, b"v1"), wa(&kb, b"v1")]).await;

    log.lock().unwrap().clear();
    let mut h = begin_accesses(
        &tm,
        AccessSet::new(Vec::new(), vec![wa(&ka, b"v2"), wa(&kb, b"v2")], Vec::new()),
    );
    tm.commit(&mut h).await.unwrap();
    tm.end(&mut h).await.unwrap();

    let c = write_counts(&log);
    assert_eq!(c.leaf, 1, "the multi-key write is one leaf CAS: {c:?}");
    assert_eq!(c.tx, 0, "the member has no transaction record: {c:?}");
    let writer = h.id().clone();
    for (key, logical_key) in [(b"a".as_slice(), &ka), (b"b".as_slice(), &kb)] {
        assert_eq!(
            entry(&tctx, key).await.unwrap().current,
            CurrentState::Inline {
                writer: writer.clone(),
                value: Arc::from(b"v2".as_slice()),
            }
        );
        assert_eq!(
            read_outcome(&tctx, logical_key)
                .await
                .value
                .unwrap()
                .value
                .as_ref(),
            b"v2"
        );
    }
}

#[tokio::test]
async fn direct_blind_puts_cover_two_eight_and_thirty_two_keys() {
    let (tm, tctx, log) = new_recording_algo().await;
    tm.direct_commit_stats_and_reset();

    for count in [2usize, 8, 32] {
        let keys: Vec<LogicalKey> = (0..count)
            .map(|index| logical_key(format!("n{count}-{index:02}").as_bytes()))
            .collect();
        log.lock().unwrap().clear();
        let mut h = begin_accesses(
            &tm,
            AccessSet::new(
                Vec::new(),
                keys.iter().map(|key| wa(key, b"v")).collect(),
                Vec::new(),
            ),
        );
        let tid = h.id().clone();
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        let counts = write_counts(&log);
        assert_eq!(counts.leaf, 1, "{count}-key member uses one CAS");
        assert_eq!(counts.tx, 0, "{count}-key member stays direct");
        for key in &keys {
            assert_eq!(
                entry(&tctx, key.key()).await.unwrap().current.writer(),
                Some(&tid)
            );
        }
    }
    assert_eq!(
        tm.direct_commit_stats_and_reset(),
        DirectCommitStats {
            candidates: 3,
            landed: 3,
        }
    );
}

#[tokio::test]
async fn multi_key_aggregate_rejection_is_atomic_and_does_not_hint() {
    let (tm, tctx, log) = new_recording_algo().await;
    let keys: Vec<LogicalKey> = (0..32)
        .map(|index| logical_key(format!("large-{index:02}").as_bytes()))
        .collect();
    let value = vec![b'v'; 600];
    assert!(InlinePolicy::default().admits_value(value.len()));

    log.lock().unwrap().clear();
    tm.direct_commit_stats_and_reset();
    let mut h = begin_accesses(
        &tm,
        AccessSet::new(
            Vec::new(),
            keys.iter().map(|key| wa(key, &value)).collect(),
            Vec::new(),
        ),
    );
    let tid = h.id().clone();
    tm.commit(&mut h).await.unwrap();
    tm.end(&mut h).await.unwrap();

    let counts = write_counts(&log);
    assert_eq!(counts.tx, 1, "the whole member uses one transaction record");
    assert!(counts.leaf >= 2, "the whole member uses locked commit");
    assert_eq!(
        tm.direct_commit_stats_and_reset(),
        DirectCommitStats {
            candidates: 1,
            landed: 0,
        }
    );
    assert_eq!(tm.direct_commit.split_hints.pending_inline_pressure(), 0);
    for key in &keys {
        assert_eq!(
            entry(&tctx, key.key()).await.unwrap().current,
            CurrentState::External {
                writer: tid.clone(),
            },
            "no output was published directly before fallback"
        );
    }
}

#[tokio::test]
async fn cross_key_aggregate_rejection_does_not_hint() {
    let (tm, tctx) = new_algo().await;
    let source = logical_key(b"source");
    let destination = logical_key(b"destination");
    let predecessor = TxId::with_priority(0, b"predecessor");
    let member = DirectMember {
        keys: vec![
            DirectKey {
                raw_key: source.key().to_vec(),
                key: source.clone(),
                read: Some(ReadPredicate::new(Some(predecessor.clone()), None)),
                write: None,
            },
            DirectKey {
                raw_key: destination.key().to_vec(),
                key: destination,
                read: None,
                write: Some(DirectWrite::Put(Arc::from(b"x".as_slice()))),
            },
        ]
        .into(),
        writes: 1,
        has_reads: true,
    };
    let policy = DirectCommitOperation::new(
        TxId::with_priority(1, b"direct"),
        test_root_path(),
        member,
        InlinePolicy {
            max_value_bytes: 8,
            max_leaf_bytes: 8,
        },
        tm.direct_commit.split_hints.clone(),
    );
    let staged = BTreeMap::from([(
        source.key().to_vec(),
        LeafEntry::new(source.key()).with_current(CurrentState::Inline {
            writer: predecessor,
            value: Arc::from(b"12345678".as_slice()),
        }),
    )]);

    assert!(matches!(
        resolve_step(
            &policy,
            &tctx,
            ReloadCause::Fresh,
            &staged,
            &NodeLocks::default(),
        )
        .await,
        Step::Skip {
            outcome: MemberOutcome::Moved
        }
    ));
    assert_eq!(tm.direct_commit.split_hints.pending_inline_pressure(), 0);
}

// One member can mix every ADR-061 output shape. Membership generation advances
// once for the member, not once per changed key, and logical no-op membership
// writes do not advance it again.
#[tokio::test]
async fn direct_mixed_member_is_atomic_and_advances_membership_once() {
    let (tm, tctx, log) = new_recording_algo().await;
    let ka = logical_key(b"a");
    let kb = logical_key(b"b");
    let kc = logical_key(b"c");
    let kd = logical_key(b"d");

    commit_writes(&tm, vec![wa(&ka, b"a1"), wa(&kc, b"c1")]).await;
    let before = membership_generation(&tctx).await;
    log.lock().unwrap().clear();
    tctx.locker.stats_and_reset();
    tm.direct_commit_stats_and_reset();

    let mut h = begin_accesses(
        &tm,
        AccessSet::new(
            Vec::new(),
            vec![wa(&ka, b"a2"), wa(&kb, b"b1"), wdel(&kc), wdel(&kd)],
            Vec::new(),
        ),
    );
    let tid = h.id().clone();
    tm.commit(&mut h).await.unwrap();
    tm.end(&mut h).await.unwrap();

    assert_eq!(tctx.locker.stats_and_reset().calls, 0);
    assert_eq!(write_counts(&log).leaf, 1);
    assert_eq!(write_counts(&log).tx, 0);
    assert_eq!(membership_generation(&tctx).await, before.wrapping_add(1));
    assert_eq!(
        tm.direct_commit_stats_and_reset(),
        DirectCommitStats {
            candidates: 1,
            landed: 1,
        }
    );
    for (key, current) in [
        (
            b"a".as_slice(),
            CurrentState::Inline {
                writer: tid.clone(),
                value: Arc::from(b"a2".as_slice()),
            },
        ),
        (
            b"b".as_slice(),
            CurrentState::Inline {
                writer: tid.clone(),
                value: Arc::from(b"b1".as_slice()),
            },
        ),
        (
            b"c".as_slice(),
            CurrentState::Tombstone {
                writer: tid.clone(),
            },
        ),
        (
            b"d".as_slice(),
            CurrentState::Tombstone {
                writer: tid.clone(),
            },
        ),
    ] {
        assert_eq!(entry(&tctx, key).await.unwrap().current, current);
    }

    let stable = membership_generation(&tctx).await;
    commit_writes(&tm, vec![wa(&ka, b"a3"), wdel(&kc), wdel(&kd)]).await;
    assert_eq!(
        membership_generation(&tctx).await,
        stable,
        "overwrite and already-absent deletes preserve membership"
    );
}

// One concurrent split is worth a fresh regroup because the complete member
// may still share a leaf. A second split means topology is churning; the direct
// path must stop there instead of inheriting the coordinator's CAS retry budget.
#[tokio::test(start_paused = true)]
async fn direct_commit_reroutes_once_then_falls_back() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let (backend, gate) = Gate::wrap_writes(mem.clone());
    let (tm, tctx) = new_algo_from_backend(backend.clone()).await;

    let l0 = NodeToken::from_bytes([0; 16]);
    let l1 = NodeToken::from_bytes([1; 16]);
    let l2 = NodeToken::from_bytes([2; 16]);
    let seed = TxId::with_priority(1, b"seed");
    let seeded_l0 = || {
        Node::leaf(LeafBody::from_entries([LeafEntry::new(b"a").with_current(
            CurrentState::Inline {
                writer: seed.clone(),
                value: Arc::from(b"a0".as_slice()),
            },
        )]))
    };
    assert!(
        tctx.nodes
            .store_node(&test_collection(), &l0, &seeded_l0(), None)
            .await
            .unwrap()
    );
    let root = tctx
        .nodes
        .load_leaf(
            &test_root_path(),
            Requirement::after(tctx.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert!(
        tctx.nodes
            .store_root(
                &test_collection(),
                &Node::index(IndexNode::from_children([(Vec::new(), l0.to_string(),)])),
                root.observation(),
            )
            .await
            .unwrap()
    );

    // This independent cache mutates topology while the main coordinator's
    // CAS is paused, as a splitter on another process could.
    let (_peer, peer) = new_algo_from_backend(mem.clone()).await;

    // Park the candidate's L0 CAS after it has grouped its keys and planned the
    // mutation there. Moving z to L1 now makes that first CAS stale.
    gate.arm();
    let direct = tm.direct_commit.clone();
    let direct_id = TxId::with_priority(3, b"direct");
    let candidate = tokio::spawn(async move {
        let mut state = HandleState::new();
        direct
            .try_commit(
                &direct_id,
                &AccessSet::new(Vec::new(), vec![wa(&logical_key(b"z"), b"z1")], Vec::new()),
                &mut state,
            )
            .await
    });
    gate.wait_until_blocked().await;

    assert!(
        peer.nodes
            .store_node(&test_collection(), &l1, &Node::leaf(LeafBody::new()), None)
            .await
            .unwrap()
    );
    let (_, observed_l0) = peer
        .nodes
        .load_node(&test_collection(), &l0, Requirement::ANY)
        .await
        .unwrap();
    let bounded_l0 = seeded_l0()
        .with_high_key(Some(b"m".to_vec()))
        .with_right_sibling(Some(l1.to_string()));
    assert!(
        peer.nodes
            .store_node(&test_collection(), &l0, &bounded_l0, Some(&observed_l0),)
            .await
            .unwrap()
    );

    // Arm the next write before releasing L0. The coordinator reloads the
    // moved key, reports one reroute, and direct commit stages its next CAS
    // on L1, where the second barrier catches it.
    gate.arm();
    gate.release();
    gate.wait_until_blocked().await;

    assert!(
        peer.nodes
            .store_node(&test_collection(), &l2, &Node::leaf(LeafBody::new()), None)
            .await
            .unwrap()
    );
    let (_, observed_l1) = peer
        .nodes
        .load_node(&test_collection(), &l1, Requirement::ANY)
        .await
        .unwrap();
    let bounded_l1 = Node::leaf(LeafBody::new())
        .with_high_key(Some(b"y".to_vec()))
        .with_right_sibling(Some(l2.to_string()));
    assert!(
        peer.nodes
            .store_node(&test_collection(), &l1, &bounded_l1, Some(&observed_l1))
            .await
            .unwrap()
    );
    gate.release();

    assert_eq!(candidate.await.unwrap().unwrap(), DirectOutcome::Locked);
    assert_eq!(
        tm.direct_commit_stats_and_reset(),
        DirectCommitStats {
            candidates: 1,
            landed: 0,
        }
    );
}

// The complete dependency set, not just the writes, must have one physical CAS
// target. Distinct tree roots are a deterministic two-leaf fixture.
#[tokio::test]
async fn cross_leaf_member_uses_a_locked_commit() {
    let (tm, tctx, log) = new_recording_algo().await;
    let other = CollectionAddress::new(
        test_collection().db_prefix(),
        CollectionId::from_slice(&[9; 16]).unwrap(),
    );
    tctx.records
        .create_record(&other, &CollectionRecord::new())
        .await
        .unwrap();
    tctx.nodes
        .create_root(&other, &Node::leaf(LeafBody::new()))
        .await
        .unwrap();
    let ka = logical_key(b"a");
    let kb = LogicalKey::new(other, b"b");

    log.lock().unwrap().clear();
    tctx.locker.stats_and_reset();
    tm.direct_commit_stats_and_reset();
    let mut h = begin_accesses(
        &tm,
        AccessSet::new(Vec::new(), vec![wa(&ka, b"a"), wa(&kb, b"b")], Vec::new()),
    );
    tm.commit(&mut h).await.unwrap();
    tm.end(&mut h).await.unwrap();

    assert!(tctx.locker.stats_and_reset().calls >= 1);
    assert_eq!(write_counts(&log).tx, 1);
    assert_eq!(
        tm.direct_commit_stats_and_reset(),
        DirectCommitStats::default(),
        "a dependency set spanning leaves is not a direct candidate"
    );
}

// ADR-061: a point read may guard a different output key when both share the
// commit leaf.
#[tokio::test]
async fn direct_cross_key_read_modify_write_uses_one_leaf_cas() {
    let (tm, tctx, log) = new_recording_algo().await;
    let ka = logical_key(b"a");
    let kb = logical_key(b"b");

    commit_writes(&tm, vec![wa(&ka, b"v1"), wa(&kb, b"v1")]).await;
    let ra = do_read(&tctx, &ka).await;

    log.lock().unwrap().clear();
    let mut h = begin_accesses(
        &tm,
        AccessSet::new(vec![ra], vec![wa(&kb, b"v2")], Vec::new()),
    );
    tm.commit(&mut h).await.unwrap();
    tm.end(&mut h).await.unwrap();

    let c = write_counts(&log);
    assert_eq!(c.leaf, 1, "the cross-key RMW is one leaf CAS: {c:?}");
    assert_eq!(c.tx, 0, "the cross-key RMW is direct: {c:?}");
    assert_eq!(
        entry(&tctx, b"b").await.unwrap().current,
        CurrentState::Inline {
            writer: h.id().clone(),
            value: Arc::from(b"v2".as_slice()),
        }
    );
}

#[tokio::test]
async fn direct_publication_reports_external_predecessors_only() {
    let (tm, tctx) = new_algo().await;
    let key = logical_key(b"k");
    let large = vec![b'x'; InlinePolicy::default().max_value_bytes + 1];
    commit_writes(&tm, vec![wa(&key, &large)]).await;
    let external = entry(&tctx, b"k").await.unwrap();
    assert!(matches!(external.current, CurrentState::External { .. }));
    let writer = external.current.writer().unwrap().clone();
    commit_writes(&tm, vec![wa(&key, b"inline")]).await;
    assert_eq!(tm.gc_hints.pending(), vec![writer.clone()]);
    commit_writes(&tm, vec![wa(&key, b"next")]).await;
    commit_writes(&tm, vec![wdel(&key)]).await;
    commit_writes(&tm, vec![wa(&key, b"after-delete")]).await;
    assert_eq!(tm.gc_hints.pending(), vec![writer]);
}

#[tokio::test]
async fn an_absence_read_uses_locked_write_back_for_a_membership_writer_with_final_status() {
    let (tm, tctx) = new_algo().await;
    let holder = TxId::with_priority(1, b"membership-holder");
    tctx.tmon.begin_tx(&holder);
    tctx.tmon
        .commit_tx(TxRecord::new(holder.clone(), TxCommitStatus::Committed))
        .await
        .unwrap();
    let mut locks = NodeLocks::default();
    locks.set_membership_writer(holder);
    let mut direct = put_policy(
        &tm,
        TxId::with_priority(2, b"direct"),
        logical_key(b"missing"),
        Some(None),
        b"value",
    );
    Arc::make_mut(&mut direct.member.keys)[0].read = Some(ReadPredicate::new(
        None,
        Some(locks.membership_generation()),
    ));
    assert!(
        matches!(
            resolve_step(&direct, &tctx, ReloadCause::Fresh, &BTreeMap::new(), &locks).await,
            Step::Skip {
                outcome: MemberOutcome::Moved
            }
        ),
        "locked commit must persist cleanup; replay alone sees the same generation"
    );
}
