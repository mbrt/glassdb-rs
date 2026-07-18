//! Regression tests for the in-doubt (unknown-outcome) commit contract.
//!
//! Object storage (S3/GCS) offers no at-most-once request id: if a conditional
//! write's first attempt lands but its acknowledgement is lost, a retry — at any
//! layer (the SDK, a proxy, the service) — observes a precondition failure that
//! is indistinguishable from a genuine conflict. A backend reports such an
//! uncertain conditional write as [`BackendError::Unavailable`] rather than a
//! confident `Precondition`.
//!
//! In v2 every commit point is a CAS on a coordination object whose durable
//! state disambiguates the outcome, so the engine recovers most in-doubt
//! outcomes by reading that object back:
//!
//! - The single read-write fast path (ADR-027) inserts itself into the version
//!   chain with one shard CAS that installs a **write lock**
//!   (`locked_by = [txid]`), issued in parallel with its committed `_t/` object
//!   (a later asynchronous write-back converts the lock to a `current_writer`
//!   pointer). A lost ack on that lock CAS is resolved by reloading the shard: if
//!   our lock (or a help-forwarded `current_writer == txid`) is present the write
//!   landed (commit), if the entry is unchanged the write did not land (retry the
//!   idempotent CAS). Only a *fast follow-on writer* that moves the entry before
//!   we can read it back is irreducibly in-doubt — surfaced as [`Error::InDoubt`]
//!   rather than risking a double-apply on a renewed re-run.
//! - The logged path's commit point (the `_t/` flip) and its leaf lock CAS
//!   (a node `_n/` or the root `_i`) are recovered in place the same way (they
//!   are idempotent under their own preconditions).
//!
//! The engine never retries a transaction *transparently* across an in-doubt
//! commit point in a way that could double-apply a landed write. The caller
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
//! `Unavailable`, so the harness injects it. To exercise the fast path's one
//! irreducible in-doubt an `after` hook can interpose a genuine competing
//! transaction at the instant a lost-ack write lands, rather than forging any
//! protocol state.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use glassdb::backend::memory::MemoryBackend;
use glassdb::backend::middleware::{BackendOp, HookBackend, HookFuture, HookOutcome};
use glassdb::backend::{Backend, BackendError};
use glassdb::{Collection, Database, Error};
use glassdb_storage::TxCommitStatus;

type Before = Box<dyn for<'a> Fn(&BackendOp<'a>) -> Result<(), BackendError> + Send + Sync>;
type After = Box<dyn for<'a, 'b> Fn(&BackendOp<'a>, HookOutcome<'b>) -> HookFuture + Send + Sync>;
type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type Competitor = Box<dyn FnOnce() -> BoxFuture<()> + Send + Sync>;

fn is_committed_tx_log(body: &[u8]) -> bool {
    glassdb_storage::txobject::status(body)
        .map(|status| status == TxCommitStatus::Ok)
        .unwrap_or(false)
}

fn shard_cas(op: &BackendOp<'_>) -> bool {
    matches!(op, BackendOp::WriteIf { path, .. }
        if path.contains("/_n/") || path.ends_with("/_i"))
}

fn committed_log(op: &BackendOp<'_>) -> bool {
    matches!(
        op,
        BackendOp::WriteIf { path, value, .. }
            | BackendOp::WriteIfNotExists { path, value }
            if path.contains("/_t/") && is_committed_tx_log(value)
    )
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
    let committed_log_writes = Arc::new(AtomicUsize::new(0));
    backend.set_after({
        let committed_log_writes = committed_log_writes.clone();
        move |op, outcome| {
            if committed_log(op) {
                committed_log_writes.fetch_add(1, Ordering::SeqCst);
            }
            after(op, outcome)
        }
    });
    committed_log_writes
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

fn read_int(b: &[u8]) -> i64 {
    let mut arr = [0u8; 8];
    arr.copy_from_slice(b);
    i64::from_le_bytes(arr)
}

async fn seed(coll: &Collection, key: &[u8], v: i64) {
    coll.write(key, &write_int(v)).await.unwrap();
}

/// Lets a committed transaction's background write-back (the spawned shard CAS
/// that publishes `current_writer` and releases locks) settle before a hook is
/// armed, so the hook fires on the operation under test rather than a lingering
/// write-back's shard CAS. Deterministic under `start_paused`: the paused clock
/// auto-advances and the ready write-back task is polled to completion.
async fn settle_writebacks() {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
}

/// A single-key read-modify-write over an existing key: its commit takes the
/// single read-write fast path (ADR-027), which installs a write lock and its
/// committed object in parallel, then writes back the pointer.
async fn increment(db: &Database, coll: &Collection, key: &'static [u8]) -> Result<(), Error> {
    // `coll` is already a reference, so `async move` copies it (references are
    // `Copy`); the closure stays `FnMut` and can be re-run on a transparent retry.
    db.tx(|tx| async move {
        let cur = match tx.read(coll, key).await {
            Ok(Some(v)) => read_int(&v),
            Ok(None) => 0,
            Err(e) => return Err(e),
        };
        tx.write(coll, key, &write_int(cur + 1))
    })
    .await
}

/// The single read-write fast path: a lost ack on the lock CAS is *resolved to
/// committed* by reading the shard back — the entry is now locked by this
/// transaction (and its committed object exists), so the write demonstrably
/// landed. The engine returns success (not in-doubt) and the value is applied
/// exactly once. This is the v2 improvement over the old logless path, whose
/// value write had no durable coordination state to disambiguate a lost ack.
#[tokio::test(start_paused = true)]
async fn single_rw_lost_ack_on_shard_cas_resolves_committed() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let backend = HookBackend::new(mem);
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db.collection(b"c");
    coll.create().await.unwrap();

    // Seed the key so the read finds a value and the overwrite is an eligible
    // single read-write.
    seed(&coll, b"k", 10).await;

    settle_writebacks().await;

    // Trap the fast path's lock CAS (the first `write_if` on the coordination
    // leaf — the root `/_i` here, which installs `locked_by`): let it land, then
    // lose the ack.
    let _ = arm_after(&backend, lost_ack_after(shard_cas));

    increment(&db, &coll, b"k")
        .await
        .expect("a landed-but-lost-ack lock CAS resolves to committed via read-back");

    // The write landed exactly once: 11, never 12 (double-apply) nor unchanged.
    let got = read_int(&coll.read(b"k").await.unwrap().unwrap());
    assert_eq!(got, 11, "value must be applied exactly once");
}

/// The single read-write fast path's one irreducible in-doubt (retained under
/// ADR-027): our lock CAS lands but loses its ack *and*, in the window before we
/// read the shard back, a **genuine competing transaction** takes the key and
/// moves the entry past us — help-forwarding our committed value into the chain
/// and then overwriting it. The read-back shows another writer, so the engine can
/// no longer tell whether our lock landed first (and was help-forwarded away) or
/// never landed; it surfaces [`Error::InDoubt`] rather than risking a
/// double-apply.
#[tokio::test(start_paused = true)]
async fn single_rw_lost_ack_then_moved_surfaces_in_doubt() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let backend = HookBackend::new(mem);
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db.collection(b"c");
    coll.create().await.unwrap();
    seed(&coll, b"k", 10).await;

    // A second, independent client over the same backend is the competitor.
    let other = Database::open("example", backend.clone()).await.unwrap();
    let other_coll = other.collection(b"c");

    settle_writebacks().await;

    // The moment our lock CAS lands (but before its ack is lost), let the
    // competing client overwrite the key. It finds our lock, help-forwards our
    // committed value, then commits its own — so our subsequent read-back finds
    // the entry moved past us to a real, committed transaction, not a forged one.
    let _ = arm_after(
        &backend,
        lost_ack_after_racing(
            shard_cas,
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

    // The competitor's write is the durable one; our uncertain write did not win.
    assert_eq!(read_int(&coll.read(b"k").await.unwrap().unwrap()), 99);
}

/// The single read-write fast path: an in-doubt outcome on the lock CAS that did
/// *not* land (e.g. the backend exhausted its retry budget on transient errors)
/// is recovered transparently. Reading the shard back shows the entry unchanged
/// and still committable, so the engine re-issues the idempotent lock CAS; the
/// one-shot fault is spent, the retry lands, and the value commits exactly once —
/// no `Error::InDoubt`, no double-apply.
#[tokio::test(start_paused = true)]
async fn single_rw_in_doubt_not_landed_retries_and_commits() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let backend = HookBackend::new(mem);
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db.collection(b"c");
    coll.create().await.unwrap();

    // Seed the key so the overwrite is an eligible single read-write.
    seed(&coll, b"k", 10).await;

    settle_writebacks().await;

    // Trap the fast path's lock CAS (the first `write_if` on the leaf `_i`):
    // report it as in-doubt *without* applying it, modelling a write that never landed. The
    // hook is one-shot, so the engine's idempotent re-issue lands.
    arm_before(&backend, fail_before(shard_cas, || not_applied("write_if")));

    increment(&db, &coll, b"k")
        .await
        .expect("an in-doubt CAS that did not land must be retried, not surfaced");

    // The retry landed exactly once: 11, never 12 (double-apply) and never
    // unchanged (lost write).
    let got = read_int(&coll.read(b"k").await.unwrap().unwrap());
    assert_eq!(got, 11, "the increment must be applied exactly once");
}

/// The logged (multi-write) path: when the *committed* transaction-log write —
/// the commit point — lands but loses its ack, the engine must retry the log
/// write transparently and recognize the landed log as its own previously
/// successful attempt. The log is keyed by tx id and only this client writes
/// its own log, so the conditional write is idempotent: a transparent retry
/// cannot double-apply.
///
/// The retry's `write_if_not_exists` sees the landed log and is rejected by a
/// real `Precondition`. The engine then reads the log status, sees the final
/// `committed` matching its own intent, and returns success. We observe two
/// attempts at the committed-log path (the original lost-ack one + a single
/// retry that fails with `Precondition`), but the writes themselves are
/// applied exactly once.
#[tokio::test(start_paused = true)]
async fn logged_commit_lost_ack_retries_transparently() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let backend = HookBackend::new(mem);
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db.collection(b"c");
    coll.create().await.unwrap();
    seed(&coll, b"a", 0).await;
    seed(&coll, b"b", 0).await;

    // Seeding committed its own logs; count only commit points from here on.

    // Trap the commit point: the transaction log written as committed (a write
    // to a `/_t/` path whose body decodes as committed). Let it land, then lose
    // the ack.
    let committed_log_writes = arm_after(&backend, lost_ack_after(committed_log));

    // Two distinct writes force the locked, log-based commit path. Capture `coll`
    // by reference so the body stays `FnMut` (re-runnable on a retry).
    let coll = &coll;
    db.tx(|tx| async move {
        let a = read_int(&tx.read(coll, b"a").await.unwrap().unwrap());
        let b = read_int(&tx.read(coll, b"b").await.unwrap().unwrap());
        tx.write(coll, b"a", &write_int(a + 1))?;
        tx.write(coll, b"b", &write_int(b + 1))
    })
    .await
    .expect("the logged commit must retry the in-doubt log write transparently");

    // Each write applied exactly once — the safety invariant.
    assert_eq!(read_int(&coll.read(b"a").await.unwrap().unwrap()), 1);
    assert_eq!(read_int(&coll.read(b"b").await.unwrap().unwrap()), 1);

    // Bound the retry: the engine drives the commit point exactly twice (the
    // original lost-ack write, then a single retry that observes the landed
    // log via `Precondition` and resolves to success). A bound above 2 would
    // mean the engine kept hammering the committed-log path instead of
    // recognizing its own landed write.
    assert_eq!(
        committed_log_writes.load(Ordering::SeqCst),
        2,
        "expected one original + one retry attempt on the committed-log path",
    );
}

/// Lock acquisition is a *pre-commit* operation: no durable user value has been
/// produced yet, so a lost ack on a conditional lock write is recoverable
/// in place by re-reading the lock metadata (which reveals whether the write
/// took). The locker therefore retries on `Unavailable` instead of surfacing
/// it, exactly as it already does for a stale `Precondition`. The whole
/// transaction commits successfully without re-running the user's closure.
#[tokio::test(start_paused = true)]
async fn lock_acquisition_lost_ack_retries_in_place() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let backend = HookBackend::new(mem);
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db.collection(b"c");
    coll.create().await.unwrap();
    seed(&coll, b"a", 0).await;
    seed(&coll, b"b", 0).await;

    // Trap the first leaf lock CAS (a `write_if` on the leaf `_i` — how a
    // lock is installed in v2). Let it land, then lose the ack: the lock is
    // actually applied but the locker observes `Unavailable`.
    let _ = arm_after(&backend, lost_ack_after(shard_cas));

    // Two writes force the locked, log-based commit path. Capture `coll` by
    // reference so the body stays `FnMut` (re-runnable, though we expect no
    // closure re-run here — the lock retry is invisible to `Database::tx`).
    let coll = &coll;
    db.tx(|tx| async move {
        let a = read_int(&tx.read(coll, b"a").await.unwrap().unwrap());
        let b = read_int(&tx.read(coll, b"b").await.unwrap().unwrap());
        tx.write(coll, b"a", &write_int(a + 1))?;
        tx.write(coll, b"b", &write_int(b + 1))
    })
    .await
    .expect("a pre-commit in-doubt lock outcome must be recovered in place");

    // Each write applied exactly once — the safety invariant.
    assert_eq!(read_int(&coll.read(b"a").await.unwrap().unwrap()), 1);
    assert_eq!(read_int(&coll.read(b"b").await.unwrap().unwrap()), 1);
}

/// A *clean* precondition (no lost ack) on the fast path's lock CAS is a genuine
/// lost race, and the engine still resolves it transparently: reading the shard
/// back shows the entry unchanged and committable, so the lock CAS is re-issued
/// and commits, applying the increment exactly once. This guards against
/// over-eagerly treating every precondition as in-doubt, which would break
/// liveness (and the fault-free exact invariant) under normal contention.
#[tokio::test(start_paused = true)]
async fn clean_conflict_on_single_rw_still_commits() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let backend = HookBackend::new(mem);
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db.collection(b"c");
    coll.create().await.unwrap();
    seed(&coll, b"k", 41).await;

    settle_writebacks().await;

    // Inject one clean precondition on the fast path's lock CAS, without applying
    // it: a genuine lost race that never landed. The fast path should reload and
    // retry, and the second attempt (hook consumed) commits.
    arm_before(
        &backend,
        fail_before(shard_cas, || BackendError::Precondition),
    );

    increment(&db, &coll, b"k")
        .await
        .expect("a clean conflict must be retried transparently, not surfaced");

    let got = read_int(&coll.read(b"k").await.unwrap().unwrap());
    assert_eq!(got, 42, "the increment must be applied exactly once");
}
