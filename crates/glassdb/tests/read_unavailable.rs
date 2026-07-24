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
//! injects it on reads of the coordination leaf objects (a node `/_n/` or the
//! collection root `/_i`) a configurable number of times. In v2 a value read
//! resolves a key by descending to its leaf (the lock table + MVCC index,
//! ADR-031), so faulting the leaf read is the read-path outage under test. The
//! key is seeded through a separate database over the same store so the reading
//! database's cache is cold and the read actually reaches the (faulty) backend.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use glassdb::backend::memory::MemoryBackend;
use glassdb::backend::middleware::{BackendOp, HookBackend, HookFuture};
use glassdb::backend::{Backend, BackendError};
use glassdb::{CollectionPath, Database, Error};

/// Controls a hook that injects `Unavailable` on coordination-leaf reads.
struct ReadFaults {
    /// Remaining leaf reads to fault. `i64::MAX` models a sustained outage; a
    /// small positive value models a transient blip.
    fail_remaining: AtomicI64,
    key_reads: AtomicUsize,
}

impl ReadFaults {
    fn wrap(inner: Arc<dyn Backend>) -> (Arc<HookBackend>, Arc<Self>) {
        let faults = Arc::new(Self {
            fail_remaining: AtomicI64::new(0),
            key_reads: AtomicUsize::new(0),
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

    /// Faults the next `n` key reads, then lets them through.
    fn fail_next_key_reads(&self, n: i64) {
        self.fail_remaining.store(n, Ordering::SeqCst);
    }

    /// Faults every key read from now on (a sustained outage).
    fn fail_key_reads_forever(&self) {
        self.fail_remaining.store(i64::MAX, Ordering::SeqCst);
    }

    fn key_reads(&self) -> usize {
        self.key_reads.load(Ordering::SeqCst)
    }

    /// For a coordination leaf object, records the read and, if the fault budget
    /// is not exhausted, consumes one unit and returns an injected `Unavailable`.
    fn maybe_fault(&self, path: &str) -> Option<BackendError> {
        if !(path.contains("/_n/") || path.ends_with("/_i")) {
            return None;
        }
        self.key_reads.fetch_add(1, Ordering::SeqCst);
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
    let (backend, faults) = ReadFaults::wrap(mem.clone());
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db
        .open_collection(&CollectionPath::new(b"c").unwrap())
        .await
        .unwrap();

    // Fault the first two key reads; the bounded retry rides over them.
    faults.fail_next_key_reads(2);

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
        faults.key_reads() >= 3,
        "expected at least 3 key reads (2 faulted + 1 ok), got {}",
        faults.key_reads()
    );
}

/// A sustained read outage exhausts the bounded retry and surfaces as the
/// dedicated `Error::Unavailable` — never `InDoubt` (no mutation is in question)
/// and never a generic `Internal`.
#[tokio::test(start_paused = true)]
async fn sustained_read_unavailability_surfaces_unavailable() {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    seed_shared(mem.clone(), b"k", 10).await;

    let (backend, faults) = ReadFaults::wrap(mem.clone());
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db
        .open_collection(&CollectionPath::new(b"c").unwrap())
        .await
        .unwrap();

    faults.fail_key_reads_forever();

    let coll = &coll;
    let res = db.tx(|tx| async move { tx.read(coll, b"k").await }).await;

    assert!(
        matches!(res, Err(Error::Unavailable(_))),
        "a sustained read outage must surface as Unavailable, got {res:?}"
    );
    assert!(
        !matches!(res, Err(Error::InDoubt(_)) | Err(Error::Internal { .. })),
        "read unavailability must not be classified as in-doubt or internal"
    );
}
