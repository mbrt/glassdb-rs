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
//! - The logged path's commit point (the `_t/` flip) and its lock CAS (`_s/`)
//!   are recovered in place the same way (they are idempotent under their own
//!   preconditions).
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

use async_trait::async_trait;
use glassdb::backend::memory::MemoryBackend;
use glassdb::backend::{Backend, BackendError, ReadReply, Version};
use glassdb::{Collection, Database, Error};
use glassdb_storage::TxCommitStatus;

/// The conditional write a [`HookBackend`] hook is inspecting: its kind
/// (`"write_if"` / `"write_if_not_exists"`), storage path, and object body.
struct WriteCtx<'a> {
    kind: &'static str,
    path: &'a str,
    value: &'a [u8],
}

/// A `before` verdict: let the write reach the backend, or short-circuit it
/// with an error *without* applying it (a write that never landed).
enum Pre {
    Proceed,
    Fail(BackendError),
}

/// Pre-op hook: runs before a conditional write reaches the backend and may
/// short-circuit it (see [`Pre`]). Synchronous — every pre-decision is.
type Before = Box<dyn Fn(&WriteCtx) -> Pre + Send + Sync>;

/// Post-op hook: runs after a conditional write has landed, receiving its
/// result. It may transform the result (e.g. turn an `Ok` into `Unavailable` to
/// model a lost ack) and/or run async side effects — e.g. a genuine competing
/// transaction. The returned future is `'static`, so a hook that needs data
/// from the [`WriteCtx`] must read it synchronously before building the future.
type After = Box<
    dyn Fn(&WriteCtx, Result<Version, BackendError>) -> BoxFuture<Result<Version, BackendError>>
        + Send
        + Sync,
>;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// A one-shot side effect run by an `after` hook the moment a matching write
/// lands. It exists to interpose a **genuine competing transaction** at exactly
/// that instant (rather than forging protocol state), so a real concurrent
/// commit can move a shard pointer in the window between our CAS landing and our
/// reading it back.
type Competitor = Box<dyn FnOnce() -> BoxFuture<()> + Send + Sync>;

/// Reports whether `body` is a transaction object that has committed. With the
/// slimmed tagless backend (ADR-023) the commit status lives in the object body,
/// so the harness decodes it instead of reading a `commit-status` tag.
fn is_committed_tx_log(body: &[u8]) -> bool {
    glassdb_storage::txobject::decode(&glassdb_data::TxId::default(), body)
        .map(|l| l.status == TxCommitStatus::Ok)
        .unwrap_or(false)
}

/// Matches a single-key fast path shard CAS: a `write_if` on a shard object
/// (`/_s/`). In the ADR-027 fast path the first such write installs the write
/// lock (the in-chain point), and a later one is the write-back that publishes
/// `current_writer`; the one-shot hooks below target the first (the lock CAS).
fn shard_cas(c: &WriteCtx) -> bool {
    c.kind == "write_if" && c.path.contains("/_s/")
}

/// Matches the logged path's commit point: writing a *committed* transaction log
/// object (a `/_t/` path whose body decodes as committed).
fn committed_log(c: &WriteCtx) -> bool {
    c.path.contains("/_t/") && is_committed_tx_log(c.value)
}

/// A one-shot [`Before`] that fails the first conditional write matching `when`
/// with the error from `err` — the op never reaches the backend — and lets
/// every other write proceed.
fn fail_before(
    when: impl Fn(&WriteCtx) -> bool + Send + Sync + 'static,
    err: impl Fn() -> BackendError + Send + Sync + 'static,
) -> Before {
    let armed = AtomicBool::new(true);
    Box::new(move |c| {
        if armed.load(Ordering::SeqCst) && when(c) {
            armed.store(false, Ordering::SeqCst);
            Pre::Fail(err())
        } else {
            Pre::Proceed
        }
    })
}

/// A one-shot [`After`] that, on the first landed write matching `when`, runs
/// `competitor` and then reports the ack as lost (`Ok` -> `Unavailable`); all
/// other writes pass through unchanged.
fn lost_ack_after_racing(
    when: impl Fn(&WriteCtx) -> bool + Send + Sync + 'static,
    competitor: Competitor,
) -> After {
    let armed = Mutex::new(Some(competitor));
    Box::new(move |c, r| {
        // Fire once, on the first matching write that actually landed. Reading
        // the context and taking the competitor is synchronous; only the
        // competitor's own work runs in the returned future.
        let competitor = if r.is_ok() && when(c) {
            armed.lock().unwrap().take()
        } else {
            None
        };
        Box::pin(async move {
            match competitor {
                Some(run) => {
                    run().await;
                    Err(lost_ack("write"))
                }
                None => r,
            }
        })
    })
}

/// A one-shot [`After`] that lets the first landed write matching `when` lose
/// its ack (`Ok` -> `Unavailable`), with no competing side effect.
fn lost_ack_after(when: impl Fn(&WriteCtx) -> bool + Send + Sync + 'static) -> After {
    lost_ack_after_racing(when, Box::new(|| Box::pin(async {})))
}

/// A [`Backend`] decorator that wraps every conditional write in a
/// `before`/`after` middleware pair (see [`Before`]/[`After`]) to inject
/// targeted in-doubt outcomes. Reads and unconditional writes pass straight
/// through. Every committed-log write is counted so a test can assert how many
/// times the engine drove the commit point (a transparent retry would show up
/// as a second committed-log write).
struct HookBackend {
    inner: Arc<dyn Backend>,
    before: Mutex<Option<Before>>,
    after: Mutex<Option<After>>,
    /// Count of conditional writes of a committed transaction log — i.e. how
    /// many times a commit point was driven.
    committed_log_writes: AtomicUsize,
}

impl HookBackend {
    fn new(inner: Arc<dyn Backend>) -> Arc<Self> {
        Arc::new(HookBackend {
            inner,
            before: Mutex::new(None),
            after: Mutex::new(None),
            committed_log_writes: AtomicUsize::new(0),
        })
    }

    /// Installs the pre-op hook.
    fn arm_before(&self, before: Before) {
        *self.before.lock().unwrap() = Some(before);
    }

    /// Installs the post-op hook.
    fn arm_after(&self, after: After) {
        *self.after.lock().unwrap() = Some(after);
    }

    fn committed_log_writes(&self) -> usize {
        self.committed_log_writes.load(Ordering::SeqCst)
    }

    /// Runs a conditional write through the middleware: count it, consult the
    /// `before` hook (which may short-circuit it), forward it to the real
    /// backend, then hand the landed result to the `after` hook. Locks are
    /// always released before awaiting, so a hook's own backend calls (a genuine
    /// competing transaction) can re-enter without deadlocking.
    async fn conditional<Fut>(
        &self,
        ctx: WriteCtx<'_>,
        forward: impl FnOnce() -> Fut,
    ) -> Result<Version, BackendError>
    where
        Fut: Future<Output = Result<Version, BackendError>>,
    {
        if committed_log(&ctx) {
            self.committed_log_writes.fetch_add(1, Ordering::SeqCst);
        }
        let verdict = match self.before.lock().unwrap().as_ref() {
            Some(before) => before(&ctx),
            None => Pre::Proceed,
        };
        let landed = match verdict {
            Pre::Fail(e) => return Err(e),
            Pre::Proceed => forward().await,
        };
        let after = match self.after.lock().unwrap().as_ref() {
            Some(after) => Ok(after(&ctx, landed)),
            None => Err(landed),
        };
        match after {
            Ok(fut) => fut.await,
            Err(landed) => landed,
        }
    }
}

#[async_trait]
impl Backend for HookBackend {
    async fn read_if_modified(
        &self,
        path: &str,
        expected: &Version,
    ) -> Result<ReadReply, BackendError> {
        self.inner.read_if_modified(path, expected).await
    }

    async fn read(&self, path: &str) -> Result<ReadReply, BackendError> {
        self.inner.read(path).await
    }

    async fn write(&self, path: &str, value: Vec<u8>) -> Result<Version, BackendError> {
        // Unconditional overwrite: idempotent, never faulted.
        self.inner.write(path, value).await
    }

    async fn write_if(
        &self,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BackendError> {
        let ctx = WriteCtx {
            kind: "write_if",
            path,
            value: &value,
        };
        self.conditional(ctx, || self.inner.write_if(path, value.clone(), expected))
            .await
    }

    async fn write_if_not_exists(
        &self,
        path: &str,
        value: Vec<u8>,
    ) -> Result<Version, BackendError> {
        let ctx = WriteCtx {
            kind: "write_if_not_exists",
            path,
            value: &value,
        };
        self.conditional(ctx, || self.inner.write_if_not_exists(path, value.clone()))
            .await
    }

    async fn delete(&self, path: &str) -> Result<(), BackendError> {
        self.inner.delete(path).await
    }

    async fn list(&self, dir_path: &str) -> Result<Vec<String>, BackendError> {
        self.inner.list(dir_path).await
    }
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
            Ok(v) => read_int(&v),
            Err(Error::NotFound) => 0,
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

    // Trap the fast path's lock CAS (the first `write_if` on the shard `/_s/`,
    // which installs `locked_by`): let it land, then lose the ack.
    backend.arm_after(lost_ack_after(shard_cas));

    increment(&db, &coll, b"k")
        .await
        .expect("a landed-but-lost-ack lock CAS resolves to committed via read-back");

    // The write landed exactly once: 11, never 12 (double-apply) nor unchanged.
    let got = read_int(&coll.read(b"k").await.unwrap());
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
    backend.arm_after(lost_ack_after_racing(
        shard_cas,
        Box::new(move || {
            Box::pin(async move {
                other_coll.write(b"k", &write_int(99)).await.unwrap();
                settle_writebacks().await;
            })
        }),
    ));

    let res = increment(&db, &coll, b"k").await;
    assert!(
        matches!(res, Err(Error::InDoubt(_))),
        "a competing commit that moved the pointer after our lost-ack CAS is \
         irreducibly in-doubt, got {res:?}"
    );

    // The competitor's write is the durable one; our uncertain write did not win.
    assert_eq!(read_int(&coll.read(b"k").await.unwrap()), 99);
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

    // Trap the fast path's lock CAS (the first `write_if` on `/_s/`): report it
    // as in-doubt *without* applying it, modelling a write that never landed. The
    // hook is one-shot, so the engine's idempotent re-issue lands.
    backend.arm_before(fail_before(shard_cas, || not_applied("write_if")));

    increment(&db, &coll, b"k")
        .await
        .expect("an in-doubt CAS that did not land must be retried, not surfaced");

    // The retry landed exactly once: 11, never 12 (double-apply) and never
    // unchanged (lost write).
    let got = read_int(&coll.read(b"k").await.unwrap());
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
    let before = backend.committed_log_writes();

    // Trap the commit point: the transaction log written as committed (a write
    // to a `/_t/` path whose body decodes as committed). Let it land, then lose
    // the ack.
    backend.arm_after(lost_ack_after(committed_log));

    // Two distinct writes force the locked, log-based commit path. Capture `coll`
    // by reference so the body stays `FnMut` (re-runnable on a retry).
    let coll = &coll;
    db.tx(|tx| async move {
        let a = read_int(&tx.read(coll, b"a").await.unwrap());
        let b = read_int(&tx.read(coll, b"b").await.unwrap());
        tx.write(coll, b"a", &write_int(a + 1))?;
        tx.write(coll, b"b", &write_int(b + 1))
    })
    .await
    .expect("the logged commit must retry the in-doubt log write transparently");

    // Each write applied exactly once — the safety invariant.
    assert_eq!(read_int(&coll.read(b"a").await.unwrap()), 1);
    assert_eq!(read_int(&coll.read(b"b").await.unwrap()), 1);

    // Bound the retry: the engine drives the commit point exactly twice (the
    // original lost-ack write, then a single retry that observes the landed
    // log via `Precondition` and resolves to success). A bound above 2 would
    // mean the engine kept hammering the committed-log path instead of
    // recognizing its own landed write.
    assert_eq!(
        backend.committed_log_writes() - before,
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

    // Trap the first shard lock CAS (a `write_if` on a shard path `/_s/` — how a
    // lock is installed in v2). Let it land, then lose the ack: the lock is
    // actually applied but the locker observes `Unavailable`.
    backend.arm_after(lost_ack_after(shard_cas));

    // Two writes force the locked, log-based commit path. Capture `coll` by
    // reference so the body stays `FnMut` (re-runnable, though we expect no
    // closure re-run here — the lock retry is invisible to `Database::tx`).
    let coll = &coll;
    db.tx(|tx| async move {
        let a = read_int(&tx.read(coll, b"a").await.unwrap());
        let b = read_int(&tx.read(coll, b"b").await.unwrap());
        tx.write(coll, b"a", &write_int(a + 1))?;
        tx.write(coll, b"b", &write_int(b + 1))
    })
    .await
    .expect("a pre-commit in-doubt lock outcome must be recovered in place");

    // Each write applied exactly once — the safety invariant.
    assert_eq!(read_int(&coll.read(b"a").await.unwrap()), 1);
    assert_eq!(read_int(&coll.read(b"b").await.unwrap()), 1);
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
    backend.arm_before(fail_before(shard_cas, || BackendError::Precondition));

    increment(&db, &coll, b"k")
        .await
        .expect("a clean conflict must be retried transparently, not surfaced");

    let got = read_int(&coll.read(b"k").await.unwrap());
    assert_eq!(got, 42, "the increment must be applied exactly once");
}
