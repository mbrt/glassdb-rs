//! Regression tests for the in-doubt (unknown-outcome) commit contract.
//!
//! Object storage (S3/GCS) offers no at-most-once request id: if a conditional
//! write's first attempt lands but its acknowledgement is lost, a retry — at any
//! layer (the SDK, a proxy, the service) — observes a precondition failure that
//! is indistinguishable from a genuine conflict. The logless single-RW fast path
//! has no durable record to disambiguate it, so an exactly-once *transparent*
//! retry is impossible.
//!
//! The contract instead is at-most-once + surface in-doubt to the caller: a
//! backend reports such an uncertain conditional write as
//! [`BackendError::Unavailable`] rather than a confident `Precondition`, and the
//! engine surfaces it as [`Error::InDoubt`] without retrying the transaction
//! transparently (a transparent retry could double-apply a write that actually
//! landed). The caller decides whether to retry (with its own idempotency) or
//! accept the uncertainty.
//!
//! These tests drive that contract deterministically with a [`FaultBackend`]
//! that, on a chosen conditional write, either (a) forwards it to the real
//! backend so it *lands* and then returns `Unavailable` (modelling a lost ack on
//! a successful write), or (b) returns a clean `Precondition` *without* applying
//! it (modelling a genuine conflict). A normal in-memory backend never produces
//! `Unavailable`, so the harness must inject it.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use glassdb::backend::memory::MemoryBackend;
use glassdb::backend::{Backend, BackendError, Metadata, ReadReply, Tags, Version, WriterId};
use glassdb::{Collection, Database, Error};

/// What the [`FaultBackend`] should do when its trap matches a conditional write.
#[derive(Clone, Copy)]
enum Action {
    /// Forward the op (so it really lands), then return `Unavailable`: the write
    /// succeeded but its acknowledgement was "lost".
    LostAck,
    /// Return a clean `Precondition` without applying the op: a genuine conflict
    /// that never landed.
    Precondition,
}

/// Decides, for a conditional write, whether (and how) to fault it. Receives the
/// op kind, the storage path, and the tags being written.
type Trap = Box<dyn Fn(&str, &str, &Tags) -> Option<Action> + Send + Sync>;

/// A [`Backend`] decorator that injects a single, targeted conditional-write
/// fault. Reads and unconditional writes pass straight through. Every observed
/// conditional write is recorded so a test can assert how many times the engine
/// drove the commit point (a transparent retry would show up as a second
/// committed-log write).
struct FaultBackend {
    inner: Arc<dyn Backend>,
    trap: Mutex<Option<Trap>>,
    /// Count of conditional writes of a committed (`commit-status=committed`)
    /// transaction log — i.e. how many times a commit point was driven.
    committed_log_writes: AtomicUsize,
}

impl FaultBackend {
    fn new(inner: Arc<dyn Backend>) -> Arc<Self> {
        Arc::new(FaultBackend {
            inner,
            trap: Mutex::new(None),
            committed_log_writes: AtomicUsize::new(0),
        })
    }

    /// Arms the (one-shot) trap. It fires at most once: the first matching
    /// conditional write consumes it.
    fn arm(&self, trap: Trap) {
        *self.trap.lock().unwrap() = Some(trap);
    }

    fn committed_log_writes(&self) -> usize {
        self.committed_log_writes.load(Ordering::SeqCst)
    }

    /// Records the conditional write and, if the armed trap matches, consumes it
    /// and returns the action to take.
    fn intercept(&self, kind: &str, path: &str, tags: &Tags) -> Option<Action> {
        let is_committed_log = path.contains("/_t/")
            && tags.get("commit-status").map(String::as_str) == Some("committed");
        if is_committed_log {
            self.committed_log_writes.fetch_add(1, Ordering::SeqCst);
        }
        let mut t = self.trap.lock().unwrap();
        let action = t.as_ref().and_then(|trap| trap(kind, path, tags));
        if action.is_some() {
            *t = None;
        }
        action
    }
}

#[async_trait]
impl Backend for FaultBackend {
    async fn read_if_modified(
        &self,
        path: &str,
        expected_writer: &WriterId,
    ) -> Result<ReadReply, BackendError> {
        self.inner.read_if_modified(path, expected_writer).await
    }

    async fn read(&self, path: &str) -> Result<ReadReply, BackendError> {
        self.inner.read(path).await
    }

    async fn get_metadata(&self, path: &str) -> Result<Metadata, BackendError> {
        self.inner.get_metadata(path).await
    }

    async fn set_tags_if(
        &self,
        path: &str,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        match self.intercept("set_tags_if", path, &tags) {
            Some(Action::Precondition) => Err(BackendError::Precondition),
            Some(Action::LostAck) => match self.inner.set_tags_if(path, expected, tags).await {
                Ok(_) => Err(lost_ack("set_tags_if")),
                Err(e) => Err(e),
            },
            None => self.inner.set_tags_if(path, expected, tags).await,
        }
    }

    async fn write(
        &self,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        // Unconditional overwrite: idempotent, never faulted.
        self.inner.write(path, value, tags).await
    }

    async fn write_if(
        &self,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        match self.intercept("write_if", path, &tags) {
            Some(Action::Precondition) => Err(BackendError::Precondition),
            Some(Action::LostAck) => match self.inner.write_if(path, value, expected, tags).await {
                Ok(_) => Err(lost_ack("write_if")),
                Err(e) => Err(e),
            },
            None => self.inner.write_if(path, value, expected, tags).await,
        }
    }

    async fn write_if_not_exists(
        &self,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        match self.intercept("write_if_not_exists", path, &tags) {
            Some(Action::Precondition) => Err(BackendError::Precondition),
            Some(Action::LostAck) => {
                match self.inner.write_if_not_exists(path, value, tags).await {
                    Ok(_) => Err(lost_ack("write_if_not_exists")),
                    Err(e) => Err(e),
                }
            }
            None => self.inner.write_if_not_exists(path, value, tags).await,
        }
    }

    async fn delete(&self, path: &str) -> Result<(), BackendError> {
        self.inner.delete(path).await
    }

    async fn delete_if(&self, path: &str, expected: &Version) -> Result<(), BackendError> {
        match self.intercept("delete_if", path, &Tags::new()) {
            Some(Action::Precondition) => Err(BackendError::Precondition),
            Some(Action::LostAck) => match self.inner.delete_if(path, expected).await {
                Ok(()) => Err(lost_ack("delete_if")),
                Err(e) => Err(e),
            },
            None => self.inner.delete_if(path, expected).await,
        }
    }

    async fn list(&self, dir_path: &str) -> Result<Vec<String>, BackendError> {
        self.inner.list(dir_path).await
    }
}

fn lost_ack(op: &str) -> BackendError {
    BackendError::Unavailable(format!("injected lost ack on a landed {op}"))
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

/// A single-key read-modify-write whose commit takes the logless fast path.
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

/// The single-RW fast path: a conditional value write that lands but loses its
/// ack must surface as in-doubt, and the value must be applied *exactly once* —
/// never re-applied. This is the bug the in-doubt contract fixes: previously the
/// lost-ack write was reported as a `Precondition`, the engine treated it as a
/// conflict, fell back to the locked path, and incremented a second time.
#[tokio::test(start_paused = true)]
async fn single_rw_lost_ack_surfaces_in_doubt_without_double_apply() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let backend = FaultBackend::new(mem);
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db.collection(b"c");
    coll.create().await.unwrap();

    // Seed the key so the read finds a value and the commit takes the single-RW
    // fast path (which requires a found read version).
    seed(&coll, b"k", 10).await;

    // Trap the fast path's value write (a `write_if` on a key path `/_k/`): let
    // it land, then report the ack as lost.
    backend.arm(Box::new(|kind, path, _tags| {
        if kind == "write_if" && path.contains("/_k/") {
            Some(Action::LostAck)
        } else {
            None
        }
    }));

    let res = increment(&db, &coll, b"k").await;
    assert!(
        matches!(res, Err(Error::InDoubt(_))),
        "expected an in-doubt error, got {res:?}"
    );

    // The write landed exactly once. The engine must not have retried and
    // applied it again: the value is 11, never 12.
    let got = read_int(&coll.read(b"k").await.unwrap());
    assert_eq!(
        got, 11,
        "value must be applied at most once (no double-apply)"
    );
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
    let backend = FaultBackend::new(mem);
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db.collection(b"c");
    coll.create().await.unwrap();
    seed(&coll, b"a", 0).await;
    seed(&coll, b"b", 0).await;

    // Seeding committed its own logs; count only commit points from here on.
    let before = backend.committed_log_writes();

    // Trap the commit point: the transaction log written as committed (a write
    // to a `/_t/` path tagged `commit-status=committed`). Let it land, then lose
    // the ack.
    backend.arm(Box::new(|_kind, path, tags| {
        let committed = path.contains("/_t/")
            && tags.get("commit-status").map(String::as_str) == Some("committed");
        committed.then_some(Action::LostAck)
    }));

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
    let backend = FaultBackend::new(mem);
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db.collection(b"c");
    coll.create().await.unwrap();
    seed(&coll, b"a", 0).await;
    seed(&coll, b"b", 0).await;

    // Trap the first `set_tags_if` on a key path — that's how a non-create
    // write lock is taken on an existing object. Let it land, then lose the
    // ack: the lock is actually applied but the locker observes `Unavailable`.
    backend.arm(Box::new(|kind, path, _tags| {
        if kind == "set_tags_if" && path.contains("/_k/") {
            Some(Action::LostAck)
        } else {
            None
        }
    }));

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

/// A *clean* precondition (no lost ack) is a genuine conflict, and the engine
/// must still resolve it transparently: the single-RW path retries and commits
/// successfully, applying the increment exactly once. This guards against
/// over-eagerly treating every precondition as in-doubt, which would break
/// liveness (and the fault-free exact invariant) under normal contention.
#[tokio::test(start_paused = true)]
async fn clean_conflict_on_single_rw_still_commits() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let backend = FaultBackend::new(mem);
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db.collection(b"c");
    coll.create().await.unwrap();
    seed(&coll, b"k", 41).await;

    // Inject one clean precondition on the first fast-path value write, without
    // applying it: a genuine conflict that never landed. The fast path should
    // re-read and retry, and the second attempt (trap consumed) commits.
    backend.arm(Box::new(|kind, path, _tags| {
        if kind == "write_if" && path.contains("/_k/") {
            Some(Action::Precondition)
        } else {
            None
        }
    }));

    increment(&db, &coll, b"k")
        .await
        .expect("a clean conflict must be retried transparently, not surfaced");

    let got = read_int(&coll.read(b"k").await.unwrap());
    assert_eq!(got, 42, "the increment must be applied exactly once");
}
