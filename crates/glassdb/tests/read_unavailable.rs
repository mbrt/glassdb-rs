//! Regression tests for transient read unavailability.
//!
//! A read is side-effect-free, so unlike a conditional write it can always be
//! retried safely (ADR-009). The engine therefore retries an in-doubt
//! (`Unavailable`) read in place with backoff, recovering a transient backend
//! outage transparently; a sustained outage surfaces as the dedicated
//! [`Error::Unavailable`] (never the in-doubt [`Error::InDoubt`], which concerns
//! a possibly-applied mutation, nor a generic [`Error::Internal`]).
//!
//! A normal in-memory backend never produces `Unavailable`, so a decorator
//! injects it a configurable number of times on reads of one object kind. In v2
//! a value read resolves a key by descending to its leaf (the lock table + MVCC
//! index, ADR-031), so faulting the leaf read (a node `/_n/` or the collection
//! root `/_r`) is the key read-path outage under test; faulting a collection
//! record (`/_i`) is the directory read-path outage. The key is seeded through a
//! separate database over the same store so the reading database's cache is cold
//! and the read actually reaches the (faulty) backend.

use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use glassdb::backend::memory::MemoryBackend;
use glassdb::backend::middleware::{BackendOp, HookBackend, HookFuture};
use glassdb::backend::{Backend, BackendError};
use glassdb::{CollectionPath, Database, Error};

/// The object kind whose reads a [`ReadFaults`] decorator faults.
#[derive(Clone, Copy)]
enum FaultTarget {
    /// A coordination leaf: a node (`/_n/`) or the collection root (`/_r`).
    Leaf,
    /// A collection's lifecycle and directory record (`/_i`).
    CollectionRecord,
}

impl FaultTarget {
    fn matches(self, path: &str) -> bool {
        match self {
            Self::Leaf => path.contains("/_n/") || path.ends_with("/_r"),
            Self::CollectionRecord => path.ends_with("/_i"),
        }
    }
}

/// Controls a hook that injects `Unavailable` on reads of one object kind.
struct ReadFaults {
    target: FaultTarget,
    /// Remaining targeted reads to fault. `i64::MAX` models a sustained outage;
    /// a small positive value models a transient blip.
    fail_remaining: AtomicI64,
    reads: AtomicUsize,
}

impl ReadFaults {
    fn wrap(inner: Arc<dyn Backend>, target: FaultTarget) -> (Arc<HookBackend>, Arc<Self>) {
        let faults = Arc::new(Self {
            target,
            fail_remaining: AtomicI64::new(0),
            reads: AtomicUsize::new(0),
        });
        let backend = HookBackend::new(inner);
        backend.set_before({
            let faults = faults.clone();
            move |op| {
                let result = match op {
                    BackendOp::Read { path } | BackendOp::ReadIfModified { path, .. } => {
                        faults.maybe_fault(path).map_or(Ok(()), Err)
                    }
                    _ => Ok(()),
                };
                let future: HookFuture = Box::pin(async move { result });
                future
            }
        });
        (backend, faults)
    }

    /// Faults the next `n` targeted reads, then lets them through.
    fn fail_next_reads(&self, n: i64) {
        self.fail_remaining.store(n, Ordering::SeqCst);
    }

    /// Faults every targeted read from now on (a sustained outage).
    fn fail_reads_forever(&self) {
        self.fail_remaining.store(i64::MAX, Ordering::SeqCst);
    }

    fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }

    /// For a targeted object, records the read and, if the fault budget is not
    /// exhausted, consumes one unit and returns an injected `Unavailable`.
    fn maybe_fault(&self, path: &str) -> Option<BackendError> {
        if !self.target.matches(path) {
            return None;
        }
        self.reads.fetch_add(1, Ordering::SeqCst);
        loop {
            let cur = self.fail_remaining.load(Ordering::SeqCst);
            if cur <= 0 {
                return None;
            }
            if self
                .fail_remaining
                .compare_exchange(cur, cur - 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Some(BackendError::Unavailable(format!(
                    "injected transient read outage on {path}"
                )));
            }
        }
    }
}

fn write_int(n: i64) -> Vec<u8> {
    n.to_le_bytes().to_vec()
}

fn read_int(b: &[u8]) -> i64 {
    let mut arr = [0u8; 8];
    arr.copy_from_slice(b);
    i64::from_le_bytes(arr)
}

/// Seeds `key` with `v` through a plain database over `mem`, leaving the value
/// durable in the shared store (and absent from any other database's cache).
async fn seed_shared(mem: Arc<dyn Backend>, key: &[u8], v: i64) {
    let db = Database::open("example", mem).await.unwrap();
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"c")
        .await
        .unwrap();
    coll.write(key, &write_int(v)).await.unwrap();
}

/// A transient read outage is ridden over by the reader's bounded in-place
/// retry: the value is returned and the transaction's closure runs only once
/// (the retry happens below `Database::tx`, not as a whole-transaction retry).
#[tokio::test(start_paused = true)]
async fn transient_read_unavailability_is_retried_transparently() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    seed_shared(mem.clone(), b"k", 10).await;

    // A second database with a cold cache reads through the faulty transport.
    let (backend, faults) = ReadFaults::wrap(mem.clone(), FaultTarget::Leaf);
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db
        .open_collection(&CollectionPath::new(b"c").unwrap())
        .await
        .unwrap();

    // Fault the first two key reads; the bounded retry rides over them.
    faults.fail_next_reads(2);

    let calls = Arc::new(AtomicUsize::new(0));
    let coll = &coll;
    let got = db
        .tx(|tx| {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                tx.read(coll, b"k").await
            }
        })
        .await
        .expect("a transient read outage must be retried, not surfaced");

    assert_eq!(read_int(&got.unwrap()), 10);
    // The retry happened inside the reader, not as a whole-transaction retry.
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // The two injected faults plus the successful read.
    assert!(
        faults.reads() >= 3,
        "expected at least 3 key reads (2 faulted + 1 ok), got {}",
        faults.reads()
    );
}

/// An error returned by a transaction body is snapshot-transparent only because
/// its reads are validated before it escapes. When that validation cannot
/// complete, the body's error must not escape: the value it was derived from may
/// be a stale cached read that a committed writer already superseded. The caller
/// learns about the failed validation instead, as the retry-safe
/// `Error::Unavailable` (a read-only attempt stages no write, so nothing is in
/// doubt).
#[tokio::test(start_paused = true)]
async fn error_outcome_does_not_escape_failed_validation() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    seed_shared(mem.clone(), b"k", 10).await;

    let (backend, faults) = ReadFaults::wrap(mem.clone(), FaultTarget::Leaf);
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db
        .open_collection(&CollectionPath::new(b"c").unwrap())
        .await
        .unwrap();

    // The body reads through a healthy transport and derives an error from the
    // value; the outage starts only once the body has returned, so it hits
    // read validation alone.
    let bodies = Arc::new(AtomicUsize::new(0));
    let coll = &coll;
    let faults = &faults;
    let res: Result<(), Error> = db
        .tx(|tx| {
            let bodies = bodies.clone();
            async move {
                bodies.fetch_add(1, Ordering::SeqCst);
                let value = tx.read(coll, b"k").await?;
                faults.fail_reads_forever();
                Err(Error::InvalidInput(format!(
                    "k is {:?}, which the body rejects",
                    value.map(|value| read_int(&value))
                )))
            }
        })
        .await;

    assert!(
        matches!(res, Err(Error::Unavailable(_))),
        "an unvalidated error outcome must surface the failed validation, got {res:?}"
    );
    assert_eq!(
        bodies.load(Ordering::SeqCst),
        1,
        "the body must not be replayed when validation could not complete"
    );
}

/// An explicit abort is an error outcome. It must not escape until GlassDB has
/// validated the reads that led to it.
#[tokio::test(start_paused = true)]
async fn explicit_abort_does_not_escape_failed_validation() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    seed_shared(mem.clone(), b"k", 10).await;

    let (backend, faults) = ReadFaults::wrap(mem.clone(), FaultTarget::Leaf);
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db
        .open_collection(&CollectionPath::new(b"c").unwrap())
        .await
        .unwrap();

    let bodies = Arc::new(AtomicUsize::new(0));
    let coll = &coll;
    let faults = &faults;
    let result: Result<(), Error> = db
        .tx(|tx| {
            let bodies = bodies.clone();
            async move {
                bodies.fetch_add(1, Ordering::SeqCst);
                tx.read(coll, b"k").await?;
                faults.fail_reads_forever();
                tx.abort()
            }
        })
        .await;

    assert!(
        matches!(result, Err(Error::Unavailable(_))),
        "an unvalidated explicit abort must surface the failed validation, got {result:?}"
    );
    assert_eq!(
        bodies.load(Ordering::SeqCst),
        1,
        "the body must not be replayed when validation could not complete"
    );
}

/// A sustained read outage exhausts the bounded retry and surfaces as the
/// dedicated `Error::Unavailable` — never `InDoubt` (no mutation is in question)
/// and never a generic `Internal`.
#[tokio::test(start_paused = true)]
async fn sustained_read_unavailability_surfaces_unavailable() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    seed_shared(mem.clone(), b"k", 10).await;

    let (backend, faults) = ReadFaults::wrap(mem.clone(), FaultTarget::Leaf);
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db
        .open_collection(&CollectionPath::new(b"c").unwrap())
        .await
        .unwrap();

    faults.fail_reads_forever();
    let reads_before = faults.reads();

    let coll = &coll;
    let res = db.tx(|tx| async move { tx.read(coll, b"k").await }).await;

    assert_eq!(faults.reads() - reads_before, 6);
    assert!(
        matches!(res, Err(Error::Unavailable(_))),
        "a sustained read outage must surface as Unavailable, got {res:?}"
    );
    assert!(
        !matches!(res, Err(Error::InDoubt(_)) | Err(Error::Internal { .. })),
        "read unavailability must not be classified as in-doubt or internal"
    );
}

#[tokio::test(start_paused = true)]
async fn read_retry_budget_applies_to_each_point_read() {
    let memory: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    seed_shared(memory.clone(), b"k", 10).await;
    let (backend, faults) = ReadFaults::wrap(memory, FaultTarget::Leaf);
    let db = Database::builder("example", backend)
        .retry_initial_interval(Duration::ZERO)
        .retry_max_interval(Duration::ZERO)
        .open()
        .await
        .unwrap();
    let coll = db
        .open_collection(&CollectionPath::new(b"c").unwrap())
        .await
        .unwrap();

    faults.fail_reads_forever();
    for stale in [false, true] {
        let reads_before = faults.reads();
        let result = if stale {
            coll.read_stale(b"k", Duration::ZERO).await
        } else {
            coll.read(b"k").await
        };
        assert!(matches!(result, Err(Error::Unavailable(_))));
        assert_eq!(faults.reads() - reads_before, 6);
    }

    faults.fail_next_reads(5);
    let reads_before = faults.reads();
    let value = coll
        .read_stale(b"k", Duration::ZERO)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_int(&value), 10);
    assert_eq!(faults.reads() - reads_before, 6);
    db.shutdown().await;
}

/// Resolving a collection name loads the parent's directory record. That load
/// leaves the calling transaction's staged changes untouched, so an outage
/// during it is the retry-safe `Error::Unavailable`, not `Error::InDoubt`.
/// Reporting it as in-doubt claims the transaction may have committed when it
/// never left its body (found by the `history` fuzz target, whose oracle
/// rejects an in-doubt outcome that no commit outcome can explain).
///
/// The assertion is on the error the body itself observes. The error that
/// escapes `tx` can come from the validation that follows an error outcome,
/// which classifies its own failures separately.
#[tokio::test(start_paused = true)]
async fn directory_read_outage_is_unavailable_inside_the_body() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    seed_shared(mem.clone(), b"k", 10).await;
    {
        let db = Database::open("example", mem.clone()).await.unwrap();
        let parent = db
            .open_collection(&CollectionPath::new(b"c").unwrap())
            .await
            .unwrap();
        parent.create_collection_if_absent(b"child").await.unwrap();
    }

    // A second database resolves the nested path through the faulty transport.
    let (backend, faults) = ReadFaults::wrap(mem.clone(), FaultTarget::CollectionRecord);
    let db = Database::open("example", backend).await.unwrap();

    faults.fail_reads_forever();

    let observed = Arc::new(Mutex::new(None));
    let _ = db
        .tx(|tx| {
            let observed = observed.clone();
            async move {
                let result = async {
                    let parent = tx.open_collection(&tx.root_collection(), b"c").await?;
                    tx.open_collection(&parent, b"child").await
                }
                .await;
                if let Err(error) = &result
                    && let Ok(mut slot) = observed.lock()
                {
                    *slot = Some(error.clone());
                }
                result
            }
        })
        .await;

    let observed = observed.lock().unwrap().clone();
    assert!(
        matches!(observed, Some(Error::Unavailable(_))),
        "a directory read outage must reach the body as Unavailable, got {observed:?}"
    );
}
