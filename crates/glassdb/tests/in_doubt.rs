//! Regression tests for the in-doubt (unknown-outcome) commit contract.
//!
//! Object storage (S3/GCS) offers no at-most-once request id: if a conditional
//! write's first attempt lands but its acknowledgement is lost, a retry — at any
//! layer (the SDK, a proxy, the service) — observes a precondition failure that
//! is indistinguishable from a genuine rejection. A backend reports such an
//! in-doubt conditional write as [`BackendError::Unavailable`] rather than a
//! confident `Precondition`.
//!
//! In v2 every commit point is a CAS on a coordination object whose durable
//! state disambiguates the outcome, so the engine recovers most in-doubt
//! outcomes by reading that object back:
//!
//! - Direct commit (ADR-061) commits an eligible complete
//!   same-leaf point transaction with one leaf CAS that publishes every inline
//!   value or tombstone. A lost ack is resolved by reloading the leaf: any exact
//!   own output proves the whole member committed, while unchanged predecessors
//!   prove it did not land (retry the idempotent CAS).
//!
//!   An in-doubt CAS stays in doubt exactly when the read-back cannot
//!   prove that state either way — surfaced as [`Error::InDoubt`] rather than
//!   risking a double-apply on a body replay under a renewed identity. A *fast follow-on writer* that
//!   moves the entry first is the reachable case and is covered below; anything
//!   else that prevents resolver evaluation after a reload from proving it
//!   (a structural gate or a drop intent arriving in the same window)
//!   is classified the same way, pinned by unit tests next to the resolvers
//!   because no interleaving reproduces it reliably.
//! - Locked commit's commit point (the `_t/` flip) and its leaf lock CAS
//!   (a node `_n/` or the root `_r`) are recovered in place the same way (they
//!   are idempotent under their own preconditions). A value the inline budgets
//!   reject uses locked commit (ADR-053), so its lost-ack lock CAS is proved by the
//!   lock this transaction already holds.
//!
//! The engine never replays a transaction body *transparently* across an
//! in-doubt commit point in a way that could double-apply a landed write. The caller
//! decides whether to retry a surfaced in-doubt (with its own idempotency) or
//! accept the uncertainty.
//!
//! These tests drive that contract deterministically with a [`HookBackend`],
//! a small middleware that wraps every conditional write in a `before`/`after`
//! pair (see [`Before`]/[`After`]): a `before` hook may short-circuit the op
//! *without* applying it (a clean `Precondition`, or an `Unavailable` for a
//! write that never landed), while an `after` hook sees the *landed* result and
//! may transform it (turn an `Ok` into `Unavailable`, modelling a lost ack) and
//! run async side effects. A normal in-memory backend never produces
//! `Unavailable`, so the harness injects it. To exercise direct commit's one
//! irreducible in-doubt an `after` hook can interpose a genuine competing
//! transaction at the instant a lost-ack write lands, rather than forging any
//! protocol state.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use glassdb::backend::memory::MemoryBackend;
use glassdb::backend::middleware::{BackendOp, HookBackend, HookFuture, HookOutcome};
use glassdb::backend::{Backend, BackendError};
use glassdb::{
    Collection, CollectionPath, Database, Error, InlinePolicy, ProtocolTiming, Transaction,
};
use glassdb_storage::transaction::TxCommitStatus;

type Before = Box<dyn for<'a> Fn(&BackendOp<'a>) -> Result<(), BackendError> + Send + Sync>;
type After = Box<dyn for<'a, 'b> Fn(&BackendOp<'a>, HookOutcome<'b>) -> HookFuture + Send + Sync>;
type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type Competitor = Box<dyn FnOnce() -> BoxFuture<()> + Send + Sync>;

fn is_committed_tx_record(body: &[u8]) -> bool {
    glassdb_storage::txrecord::status(body)
        .map(|status| status == TxCommitStatus::Ok)
        .unwrap_or(false)
}

fn leaf_cas(op: &BackendOp<'_>) -> bool {
    matches!(op, BackendOp::WriteIf { path, .. }
        if path.contains("/_n/") || path.ends_with("/_r"))
}

fn committed_record(op: &BackendOp<'_>) -> bool {
    matches!(
        op,
        BackendOp::WriteIf { path, value, .. }
            | BackendOp::WriteIfNotExists { path, value }
            if path.contains("/_t/") && is_committed_tx_record(value)
    )
}

#[tokio::test(start_paused = true)]
async fn delayed_local_commit_acknowledgement_does_not_hide_committed_values() {
    for publish_writer in [false, true] {
        let memory = Arc::new(MemoryBackend::new());
        let backend = HookBackend::new(memory.clone());
        let owner = Database::builder("published", backend.clone())
            .inline_policy(InlinePolicy::none())
            .open()
            .await
            .unwrap();
        let peer = Database::builder("published", memory)
            .inline_policy(InlinePolicy::none())
            .open()
            .await
            .unwrap();
        let root = owner.root_collection();
        root.write(b"key", b"old").await.unwrap();

        let applied = Arc::new(tokio::sync::Notify::new());
        let acknowledge = Arc::new(tokio::sync::Notify::new());
        backend.set_after({
            let applied = applied.clone();
            let acknowledge = acknowledge.clone();
            move |op, outcome| {
                let delay = outcome.is_success() && committed_record(op);
                let applied = applied.clone();
                let acknowledge = acknowledge.clone();
                Box::pin(async move {
                    if delay {
                        applied.notify_one();
                        acknowledge.notified().await;
                    }
                    Ok(())
                })
            }
        });
        let writer = tokio::spawn({
            let root = root.clone();
            async move { root.write(b"key", b"committed").await }
        });
        applied.notified().await;
        backend.clear_after();

        // After the peer reads this commit, every later read must include it,
        // whether the peer also publishes its writer in the leaf or not.
        let seen = peer
            .tx(|tx| async move {
                let root = tx.root_collection();
                let value = tx.read(&root, b"key").await?;
                if publish_writer {
                    tx.write(&root, b"other", b"peer")?;
                }
                Ok(value)
            })
            .await
            .unwrap();
        assert_eq!(seen, Some(b"committed".to_vec()));

        let read = root.read(b"key");
        tokio::pin!(read);
        let value = tokio::select! {
            value = &mut read => {
                acknowledge.notify_one();
                value
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {
                acknowledge.notify_one();
                read.await
            }
        };
        writer.await.unwrap().unwrap();
        assert_eq!(
            value.unwrap(),
            Some(b"committed".to_vec()),
            "publish_writer={publish_writer}"
        );
        owner.shutdown().await;
        peer.shutdown().await;
    }
}

fn fail_before(
    when: impl for<'a> Fn(&BackendOp<'a>) -> bool + Send + Sync + 'static,
    err: impl Fn() -> BackendError + Send + Sync + 'static,
) -> Before {
    let armed = AtomicBool::new(true);
    Box::new(move |op| {
        if armed.load(Ordering::SeqCst) && when(op) {
            armed.store(false, Ordering::SeqCst);
            Err(err())
        } else {
            Ok(())
        }
    })
}

fn lost_ack_after_racing(
    when: impl for<'a> Fn(&BackendOp<'a>) -> bool + Send + Sync + 'static,
    competitor: Competitor,
) -> After {
    let armed = Mutex::new(Some(competitor));
    Box::new(move |op, outcome| {
        let competitor = if outcome.is_success() && when(op) {
            armed.lock().unwrap().take()
        } else {
            None
        };
        Box::pin(async move {
            if let Some(run) = competitor {
                run().await;
                Err(lost_ack("write"))
            } else {
                Ok(())
            }
        })
    })
}

fn lost_ack_after(when: impl for<'a> Fn(&BackendOp<'a>) -> bool + Send + Sync + 'static) -> After {
    lost_ack_after_racing(when, Box::new(|| Box::pin(async {})))
}

fn arm_before(backend: &HookBackend, before: Before) {
    backend.set_before(move |op| {
        let result = before(op);
        Box::pin(async move { result })
    });
}

fn arm_after(backend: &HookBackend, after: After) -> Arc<AtomicUsize> {
    let committed_record_writes = Arc::new(AtomicUsize::new(0));
    backend.set_after({
        let committed_record_writes = committed_record_writes.clone();
        move |op, outcome| {
            if committed_record(op) {
                committed_record_writes.fetch_add(1, Ordering::SeqCst);
            }
            after(op, outcome)
        }
    });
    committed_record_writes
}

fn lost_ack(op: &str) -> BackendError {
    BackendError::Unavailable(format!("injected lost ack on a landed {op}"))
}

fn not_applied(op: &str) -> BackendError {
    BackendError::Unavailable(format!("injected in-doubt without applying {op}"))
}

fn write_int(n: i64) -> Vec<u8> {
    n.to_le_bytes().to_vec()
}

fn try_read_int(b: &[u8]) -> Option<i64> {
    Some(i64::from_le_bytes(b.get(..8)?.try_into().ok()?))
}

fn read_int(b: &[u8]) -> i64 {
    try_read_int(b).expect("integer value has the wrong width")
}

async fn read_existing_int(tx: &Transaction, coll: &Collection, key: &[u8]) -> Result<i64, Error> {
    let value = tx.read(coll, key).await?.ok_or(Error::NotFound)?;
    try_read_int(&value)
        .ok_or_else(|| Error::internal(format!("key {key:?} has invalid integer value {value:?}")))
}

fn incremented_value(key: &[u8], current: i64) -> Result<Vec<u8>, Error> {
    current
        .checked_add(1)
        .map(write_int)
        .ok_or_else(|| Error::internal(format!("integer overflow for key {key:?}")))
}

/// Encodes `n` padded past the inline per-value budget (ADR-051), so its commit
/// uses a locked commit rather than direct commit (ADR-053).
fn write_padded_int(n: i64) -> Vec<u8> {
    let mut v = write_int(n);
    v.resize(4096, 0);
    v
}

async fn seed(coll: &Collection, key: &[u8], v: i64) {
    coll.write(key, &write_int(v)).await.unwrap();
}

/// Lets a committed transaction's background write-back (the spawned leaf CAS
/// that publishes `current_writer` and releases locks) settle before a hook is
/// armed, so the hook fires on the operation under test rather than a lingering
/// write-back's leaf CAS. Deterministic under `start_paused`: the paused clock
/// auto-advances and the ready write-back task is polled to completion.
async fn settle_writebacks() {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
}

/// A single-key read-modify-write over an existing key whose small value the
/// inline budgets admit: its commit uses direct commit (ADR-051).
async fn increment(db: &Database, coll: &Collection, key: &'static [u8]) -> Result<(), Error> {
    increment_with(db, coll, key, write_int).await
}

/// The same read-modify-write with a value the inline budgets reject, so its
/// commit uses a locked commit: a write lock, a committed record, then a
/// write-back that publishes the pointer.
async fn increment_padded(
    db: &Database,
    coll: &Collection,
    key: &'static [u8],
) -> Result<(), Error> {
    increment_with(db, coll, key, write_padded_int).await
}

async fn increment_with(
    db: &Database,
    coll: &Collection,
    key: &'static [u8],
    encode: fn(i64) -> Vec<u8>,
) -> Result<(), Error> {
    // `coll` is already a reference, so `async move` copies it (references are
    // `Copy`); the closure stays `FnMut` and can be replayed.
    db.tx(|tx| async move {
        let cur = match tx.read(coll, key).await {
            Ok(Some(v)) => try_read_int(&v).ok_or_else(|| {
                Error::internal(format!("key {key:?} has invalid integer value {v:?}"))
            })?,
            Ok(None) => 0,
            Err(e) => return Err(e),
        };
        let next = cur
            .checked_add(1)
            .ok_or_else(|| Error::internal(format!("integer overflow for key {key:?}")))?;
        tx.write(coll, key, &encode(next))
    })
    .await
}

/// Direct commit (ADR-051): a lost ack on the commit CAS is
/// *resolved to committed* by reading the leaf back — the entry now holds this
/// transaction's exact inline value, so the write demonstrably landed. The
/// engine returns a commit outcome (not in-doubt) and applies the value exactly once.
/// Unlike v1's direct path, the published state itself is the disambiguating
/// coordination evidence.
#[tokio::test(start_paused = true)]
async fn single_rw_lost_ack_on_leaf_cas_resolves_committed() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let backend = HookBackend::new(mem);
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"c")
        .await
        .unwrap();

    // Seed the key so the read finds a value and the overwrite is an eligible
    // single read-write.
    seed(&coll, b"k", 10).await;

    settle_writebacks().await;

    // Trap the commit CAS (the first `write_if` on the coordination leaf — the
    // root `/_r` here): let it land, then lose the ack.
    let _ = arm_after(&backend, lost_ack_after(leaf_cas));

    increment(&db, &coll, b"k")
        .await
        .expect("a landed-but-lost-ack commit CAS resolves to committed via read-back");

    // The write landed exactly once: 11, never 12 (double-apply) nor unchanged.
    let got = read_int(&coll.read(b"k").await.unwrap().unwrap());
    assert_eq!(got, 11, "value must be applied exactly once");
}

/// The same recovery for the locked fallback a value over the inline budget
/// takes: the lost-ack CAS installed our write lock, so reading the leaf back
/// proves the commit through the lock rather than through a published value.
#[tokio::test(start_paused = true)]
async fn locked_single_rw_lost_ack_on_lock_cas_resolves_committed() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let backend = HookBackend::new(mem);
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"c")
        .await
        .unwrap();
    seed(&coll, b"k", 10).await;
    settle_writebacks().await;

    let _ = arm_after(&backend, lost_ack_after(leaf_cas));

    increment_padded(&db, &coll, b"k")
        .await
        .expect("a landed-but-lost-ack lock CAS resolves to committed via read-back");

    let got = read_int(&coll.read(b"k").await.unwrap().unwrap());
    assert_eq!(got, 11, "value must be applied exactly once");
}

/// Direct commit's one irreducible in-doubt (ADR-051): our commit CAS
/// lands but loses its ack *and*, in the window before
/// we read the leaf back, a **genuine competing transaction** takes the key and
/// moves the entry past us. The read-back shows another writer, so the engine can
/// no longer tell whether our write landed first and was then superseded, or
/// never landed at all; it surfaces [`Error::InDoubt`] rather than risking a
/// double-apply.
#[tokio::test(start_paused = true)]
async fn single_rw_lost_ack_then_moved_surfaces_in_doubt() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let backend = HookBackend::new(mem);
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"c")
        .await
        .unwrap();
    seed(&coll, b"k", 10).await;

    // A second, independent database instance over the same backend is the competitor.
    let other = Database::open("example", backend.clone()).await.unwrap();
    let other_coll = other
        .open_collection(&CollectionPath::new(b"c").unwrap())
        .await
        .unwrap();

    settle_writebacks().await;

    // The moment our lock CAS lands (but before its ack is lost), let the
    // competing database instance overwrite the key. It finds our lock, help-forwards our
    // committed value, then commits its own — so our subsequent read-back finds
    // the entry moved past us to a real, committed transaction, not a forged one.
    let _ = arm_after(
        &backend,
        lost_ack_after_racing(
            leaf_cas,
            Box::new(move || {
                Box::pin(async move {
                    other_coll.write(b"k", &write_int(99)).await.unwrap();
                    settle_writebacks().await;
                })
            }),
        ),
    );

    let res = increment(&db, &coll, b"k").await;
    assert!(
        matches!(res, Err(Error::InDoubt(_))),
        "a competing commit that moved the pointer after our lost-ack CAS is \
         irreducibly in-doubt, got {res:?}"
    );

    // The competitor's write is the durable one; our in-doubt write did not win.
    assert_eq!(read_int(&coll.read(b"k").await.unwrap().unwrap()), 99);
}

/// A single read-write in-doubt outcome on a commit CAS that did *not* land
/// (e.g. the backend exhausted its retry budget on transient errors) is recovered
/// transparently. Reading the leaf back shows the entry unchanged and still
/// committable, so the engine re-issues the idempotent CAS; the one-shot fault is
/// spent, the retry lands, and the value commits exactly once — no
/// `Error::InDoubt`, no double-apply.
#[tokio::test(start_paused = true)]
async fn single_rw_in_doubt_not_landed_retries_and_commits() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let backend = HookBackend::new(mem);
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"c")
        .await
        .unwrap();

    // Seed the key so the overwrite is an eligible single read-write.
    seed(&coll, b"k", 10).await;

    settle_writebacks().await;

    // Trap the commit CAS (the first `write_if` on the leaf `_r`): report it as
    // in-doubt *without* applying it, modelling a write that never landed. The
    // hook is one-shot, so the engine's idempotent re-issue lands.
    arm_before(&backend, fail_before(leaf_cas, || not_applied("write_if")));

    increment(&db, &coll, b"k")
        .await
        .expect("an in-doubt CAS that did not land must be retried, not surfaced");

    // The retry landed exactly once: 11, never 12 (double-apply) and never
    // unchanged (lost write).
    let got = read_int(&coll.read(b"k").await.unwrap().unwrap());
    assert_eq!(got, 11, "the increment must be applied exactly once");
}

// Local ownership must end even when the commit result remains unknown, or a
// later transaction waits forever on the stopped owner's in-memory Pending state.
async fn local_write_after_in_doubt_locked_commit(landed: bool) {
    let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
    let db = Database::builder("test", backend.clone())
        .protocol_timing(glassdb::ProtocolTiming::simulation())
        .open()
        .await
        .unwrap();
    let coll = db.root_collection();
    seed(&coll, b"key", 10).await;
    settle_writebacks().await;

    if landed {
        backend.set_after(|operation, _| {
            let unavailable = committed_record(operation)
                || matches!(operation,
                    BackendOp::Read { path } | BackendOp::ReadIfModified { path, .. }
                        if path.contains("/_t/"));
            Box::pin(async move {
                if unavailable {
                    Err(lost_ack("commit or status read"))
                } else {
                    Ok(())
                }
            })
        });
    } else {
        // The refresher must not create Pending first: that would let commit
        // recovery retry instead of reaching the in-doubt missing record.
        backend.set_before(|operation| {
            let unavailable = matches!(operation,
                BackendOp::WriteIf { path, .. } | BackendOp::WriteIfNotExists { path, .. }
                    if path.contains("/_t/"));
            Box::pin(async move {
                if unavailable {
                    Err(not_applied("transaction record write"))
                } else {
                    Ok(())
                }
            })
        });
    }
    let result = increment_padded(&db, &coll, b"key").await;
    assert!(matches!(result, Err(Error::InDoubt(_))), "{result:?}");
    backend.clear_before();
    backend.clear_after();

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        increment_padded(&db.clone(), &coll, b"key"),
    )
    .await
    .expect("local transaction remained pending after its owner returned")
    .expect("the next local transaction must recover the previous outcome");
    let value = coll.read(b"key").await.unwrap().unwrap();
    assert_eq!(read_int(&value), if landed { 12 } else { 11 });
    db.shutdown().await;
}

#[tokio::test]
async fn local_write_recovers_after_a_commit_did_not_land() {
    local_write_after_in_doubt_locked_commit(false).await;
}

#[tokio::test]
async fn local_write_recovers_after_an_unconfirmed_commit_landed() {
    local_write_after_in_doubt_locked_commit(true).await;
}

/// Locked commit: when the *committed* transaction-record write —
/// the commit point — lands but loses its ack, the engine must recover the
/// outcome transparently instead of surfacing the uncertainty.
///
/// It recovers by reading the record status back. The record is keyed by
/// transaction identity, and only this database instance writes `committed` under it.
/// Thus, a final `committed` status is its own landed write and resolves to a
/// commit outcome. Reading instead of issuing the conditional write again
/// keeps the commit point driven exactly once. Thus, no extra body execution
/// widens the window in which GC could reclaim the very record the engine
/// needs to read (ADR-057).
#[tokio::test(start_paused = true)]
async fn locked_commit_lost_ack_recovers_transparently() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let backend = HookBackend::new(mem);
    let db = Database::builder("example", backend.clone())
        .inline_policy(InlinePolicy::none())
        .open()
        .await
        .unwrap();
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"c")
        .await
        .unwrap();
    seed(&coll, b"a", 0).await;
    seed(&coll, b"b", 0).await;

    // Seeding committed its own records; count only commit points from here on.

    // Trap the commit point: the transaction record written as committed (a write
    // to a `/_t/` path whose body decodes as committed). Let it land, then lose
    // the ack.
    let committed_record_writes = arm_after(&backend, lost_ack_after(committed_record));

    // The disabled inline policy makes this fixture explicitly exercise the
    // locked commit. Capture `coll` by reference so the body stays
    // `FnMut` (replayable).
    let coll = &coll;
    db.tx(|tx| async move {
        let a = read_existing_int(&tx, coll, b"a").await?;
        let b = read_existing_int(&tx, coll, b"b").await?;
        tx.write(coll, b"a", &incremented_value(b"a", a)?)?;
        tx.write(coll, b"b", &incremented_value(b"b", b)?)
    })
    .await
    .expect("the locked commit must recover the in-doubt record write transparently");

    // Each write applied exactly once — the safety invariant.
    assert_eq!(read_int(&coll.read(b"a").await.unwrap().unwrap()), 1);
    assert_eq!(read_int(&coll.read(b"b").await.unwrap().unwrap()), 1);

    // The commit point is driven exactly once — the lost-ack write itself.
    // Anything above one would mean the engine re-issued the conditional write
    // instead of recognizing its own landed record by reading the status back.
    assert_eq!(
        committed_record_writes.load(Ordering::SeqCst),
        1,
        "the in-doubt commit point must be resolved by reading, not re-issued",
    );
}

#[tokio::test(start_paused = true)]
async fn locked_commit_recovery_deadline_bounds_status_reads() {
    let horizon = Duration::from_secs(1);
    for (ack_delay, status_delay) in [
        (Duration::ZERO, None),
        (horizon / 2, None),
        (horizon, None),
        (Duration::ZERO, Some(horizon)),
    ] {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let db = Database::builder("deadline", backend.clone())
            .inline_policy(InlinePolicy::none())
            .protocol_timing(ProtocolTiming::new(horizon, Duration::from_secs(30)))
            .open()
            .await
            .unwrap();
        let coll = db.root_collection();
        let commit_applied = Arc::new(AtomicBool::new(false));
        let status_reads = Arc::new(AtomicUsize::new(0));
        let release_reads = Arc::new(tokio::sync::Notify::new());
        let commit_writes = arm_after(
            &backend,
            Box::new({
                let commit_applied = commit_applied.clone();
                let status_reads = status_reads.clone();
                let release_reads = release_reads.clone();
                move |operation, outcome| {
                    let commit = outcome.is_success() && committed_record(operation);
                    let status_read = commit_applied.load(Ordering::SeqCst)
                        && matches!(operation,
                        BackendOp::Read { path } | BackendOp::ReadIfModified { path, .. }
                            if path.contains("/_t/"));
                    let commit_applied = commit_applied.clone();
                    let status_reads = status_reads.clone();
                    let release_reads = release_reads.clone();
                    Box::pin(async move {
                        if commit {
                            commit_applied.store(true, Ordering::SeqCst);
                            if !ack_delay.is_zero() {
                                tokio::time::sleep(ack_delay).await;
                            }
                            return Err(lost_ack("commit"));
                        }
                        if status_read {
                            status_reads.fetch_add(1, Ordering::SeqCst);
                            if let Some(delay) = status_delay {
                                tokio::time::sleep(delay).await;
                            } else {
                                release_reads.notified().await;
                            }
                        }
                        Ok(())
                    })
                }
            }),
        );

        let started = tokio::time::Instant::now();
        let result = tokio::time::timeout(horizon * 2, coll.write(b"key", b"value"))
            .await
            .expect("a stalled status read exceeded the commit recovery deadline");
        assert!(matches!(result, Err(Error::InDoubt(_))), "{result:?}");
        assert_eq!(started.elapsed(), horizon, "ack_delay={ack_delay:?}");
        assert_eq!(commit_writes.load(Ordering::SeqCst), 1);
        assert_eq!(
            status_reads.load(Ordering::SeqCst),
            usize::from(ack_delay < horizon),
            "an expired recovery budget must not start another read"
        );

        backend.clear_after();
        release_reads.notify_waiters();
        assert_eq!(
            coll.read(b"key").await.unwrap().as_deref(),
            Some(&b"value"[..])
        );
        db.shutdown().await;
    }
}

/// Lock acquisition is a *pre-commit* operation: no durable user value has been
/// produced yet, so a lost ack on a conditional lock write is recoverable
/// in place by re-reading the lock metadata (which reveals whether the write
/// took). The locker therefore retries on `Unavailable` instead of surfacing
/// it, exactly as it already does for a stale `Precondition`. The whole
/// transaction commits successfully without a body replay.
#[tokio::test(start_paused = true)]
async fn lock_acquisition_lost_ack_retries_in_place() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let backend = HookBackend::new(mem);
    let db = Database::builder("example", backend.clone())
        .inline_policy(InlinePolicy::none())
        .open()
        .await
        .unwrap();
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"c")
        .await
        .unwrap();
    seed(&coll, b"a", 0).await;
    seed(&coll, b"b", 0).await;

    // Trap the first leaf lock CAS (a `write_if` on the leaf `_r` — how a
    // lock is installed in v2). Let it land, then lose the ack: the lock is
    // actually applied but the locker observes `Unavailable`.
    let _ = arm_after(&backend, lost_ack_after(leaf_cas));

    // Inline publication is disabled so this fixture takes a locked commit.
    // Capture `coll` by reference so the body stays `FnMut` (replayable,
    // though we expect no body replay here — the lock retry is invisible to
    // `Database::tx`).
    let coll = &coll;
    db.tx(|tx| async move {
        let a = read_existing_int(&tx, coll, b"a").await?;
        let b = read_existing_int(&tx, coll, b"b").await?;
        tx.write(coll, b"a", &incremented_value(b"a", a)?)?;
        tx.write(coll, b"b", &incremented_value(b"b", b)?)
    })
    .await
    .expect("a pre-commit in-doubt lock outcome must be recovered in place");

    // Each write applied exactly once — the safety invariant.
    assert_eq!(read_int(&coll.read(b"a").await.unwrap().unwrap()), 1);
    assert_eq!(read_int(&coll.read(b"b").await.unwrap().unwrap()), 1);
}

/// A *clean* precondition (no lost ack) on a single read-write commit CAS is a
/// genuine lost race, and the engine still resolves it transparently: reading the
/// leaf back shows the entry unchanged and committable, so the CAS is re-issued
/// and commits, applying the increment exactly once. This guards against
/// over-eagerly treating every precondition as in-doubt, which would break
/// liveness (and the fault-free exact invariant) under normal contention.
#[tokio::test(start_paused = true)]
async fn clean_rejection_on_single_rw_still_commits() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let backend = HookBackend::new(mem);
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"c")
        .await
        .unwrap();
    seed(&coll, b"k", 41).await;

    settle_writebacks().await;

    // Inject one clean precondition on the commit CAS, without applying it: a
    // genuine lost race that never landed. The fast path should reload and retry,
    // and the second attempt (hook consumed) commits.
    arm_before(
        &backend,
        fail_before(leaf_cas, || BackendError::Precondition),
    );

    increment(&db, &coll, b"k")
        .await
        .expect("a clean rejection must be retried transparently, not surfaced");

    let got = read_int(&coll.read(b"k").await.unwrap().unwrap());
    assert_eq!(got, 42, "the increment must be applied exactly once");
}
