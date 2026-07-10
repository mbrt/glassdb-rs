//! Active transaction state. Ported from the Go `tx.go`: staged writes and
//! tracked reads (to provide repeatable reads and avoid phantom reads), plus
//! access collection for the commit algorithm.
//!
//! [`Transaction`] is a cheap, `Send` handle over shared, interior-mutable state
//! (`Arc<Mutex<TransactionInner>>`). It is passed *by value* into the transaction
//! closure so the resulting future is `Send` and can be `tokio::spawn`-ed; the
//! framework keeps its own handle (see [`Transaction::handle`]) to read the collected
//! accesses after the closure returns and to reset between retries. All methods
//! take `&self` and only hold the lock briefly — never across an `.await` — so
//! several reads can run concurrently within a single transaction.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use glassdb_concurr::RetryConfig;
use glassdb_data::paths;
use glassdb_storage::{MAX_STALENESS, ShardStore, StorageError, ValueCache};
use glassdb_trans::{
    Data, LeafCoverage, ReadAccess, ReadVersion, Reader, Resolver, ScanAccess, WriteAccess,
};

use crate::collection::Collection;
use crate::error::Error;

/// An active database transaction. Reads and writes are buffered and only
/// applied atomically when the surrounding [`crate::Database::tx`] commits.
///
/// Awaiting [`Transaction::read`] (and the enclosing [`crate::Database::tx`] future) is
/// durability-safe to cancel by being dropped (`tokio::time::timeout`,
/// `select!`, or `JoinHandle::abort`). When the future is dropped mid-flight
/// the surrounding `Database::tx` arranges (via an internal RAII guard) for the
/// engine-side transaction to be asynchronously aborted, so locks are
/// released promptly instead of waiting for lease expiry.
pub struct Transaction {
    reader: Reader,
    resolver: Resolver,
    inner: Arc<Mutex<TransactionInner>>,
}

#[derive(Default)]
struct TransactionInner {
    staged: HashMap<String, StagedValue>,
    reads: HashMap<String, ReadState>,
    scans: Vec<ScanAccess>,
    aborted: bool,
}

impl Transaction {
    /// Reads the value for `key` within the transaction. Repeatable: a value
    /// read once is returned consistently, and a key not found stays not found
    /// (avoiding phantom reads).
    ///
    /// Takes `&self`, so multiple reads can be polled concurrently (e.g. with
    /// `futures::future::join_all`) to fetch keys in parallel.
    pub async fn read(&self, c: &Collection, key: &[u8]) -> Result<Vec<u8>, Error> {
        let p = paths::from_key(c.prefix(), key);
        // Brief lock to consult the per-transaction cache. The guard is dropped
        // before the backend read below so it is never held across `.await`.
        {
            let inner = self.inner.lock().unwrap();
            if let Some(staged) = inner.staged.get(&p) {
                return staged.read();
            }
            if let Some(ReadState::NotFound) = inner.reads.get(&p) {
                // Be consistent with values not found the first time.
                return Err(Error::NotFound);
            }
        }

        match self.reader.read(&p, MAX_STALENESS).await {
            Err(StorageError::NotFound) => {
                let mut inner = self.inner.lock().unwrap();
                inner.reads.insert(p, ReadState::NotFound);
                Err(Error::NotFound)
            }
            // A read is side-effect-free; `from_read` centralizes the mapping
            // (notably a sustained outage becomes the retry-safe
            // `Error::Unavailable` rather than `InDoubt`).
            Err(e) => Err(Error::from_read(e)),
            Ok(rv) => {
                let mut inner = self.inner.lock().unwrap();
                inner
                    .staged
                    .insert(p.clone(), StagedValue::Read(rv.value.clone()));
                inner.reads.insert(
                    p,
                    ReadState::Found(ReadVersion {
                        last_writer: rv.version.writer,
                    }),
                );
                Ok(rv.value.to_vec())
            }
        }
    }

    /// Stages a write of `value` to `key`.
    pub fn write(&self, c: &Collection, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let p = paths::from_key(c.prefix(), key);
        self.inner
            .lock()
            .unwrap()
            .staged
            .insert(p, StagedValue::Put(Arc::from(value)));
        Ok(())
    }

    /// Lists the live keys of collection `c` in key order. Repeatable and
    /// strongly consistent.
    ///
    /// Intended for a read-only listing transaction (the [`Collection::keys`]
    /// entry point). The scan reflects the committed state, not this
    /// transaction's own staged writes.
    pub(crate) async fn keys(&self, c: &Collection) -> Result<Vec<Vec<u8>>, Error> {
        // TODO: Merge staged writes into the scan result so a listing
        //       transaction sees its own writes.
        let scan = self
            .resolver
            .live_keys_scan(c.prefix())
            .await
            .map_err(Error::from_read)?;
        let covered: Vec<LeafCoverage> = scan.covered;
        self.inner.lock().unwrap().scans.push(ScanAccess {
            prefix: c.prefix().into(),
            covered,
        });
        Ok(scan.keys)
    }

    /// Marks `key` for deletion within the transaction.
    pub fn delete(&self, c: &Collection, key: &[u8]) -> Result<(), Error> {
        let p = paths::from_key(c.prefix(), key);
        self.inner
            .lock()
            .unwrap()
            .staged
            .insert(p, StagedValue::Delete);
        Ok(())
    }

    /// Explicitly aborts the transaction. Returns [`Error::Aborted`].
    pub fn abort(&self) -> Result<(), Error> {
        self.inner.lock().unwrap().aborted = true;
        Err(Error::Aborted)
    }

    pub(crate) fn new(
        shards: ShardStore,
        values: ValueCache,
        tmon: glassdb_trans::Monitor,
        retry: RetryConfig,
    ) -> Self {
        Transaction {
            reader: Reader::new(values, shards.clone(), tmon.clone(), retry),
            resolver: Resolver::new(shards, tmon),
            inner: Arc::new(Mutex::new(TransactionInner::default())),
        }
    }

    /// Returns another handle to the same transaction state. The framework
    /// passes a handle to the user closure (which consumes it) while keeping one
    /// to inspect the staged accesses and reset between retries.
    pub(crate) fn handle(&self) -> Transaction {
        Transaction {
            reader: self.reader.clone(),
            resolver: self.resolver.clone(),
            inner: self.inner.clone(),
        }
    }

    pub(crate) fn aborted(&self) -> bool {
        self.inner.lock().unwrap().aborted
    }

    pub(crate) fn reset(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.staged.clear();
        inner.reads.clear();
        inner.scans.clear();
    }

    pub(crate) fn collect_accesses(&self) -> Data {
        let inner = self.inner.lock().unwrap();
        let mut writes = Vec::new();
        for (k, v) in &inner.staged {
            match v {
                StagedValue::Read(_) => {}
                StagedValue::Put(val) => {
                    writes.push(WriteAccess::put(k.as_str().into(), val.clone()))
                }
                StagedValue::Delete => writes.push(WriteAccess::delete(k.as_str().into())),
            }
        }
        let mut reads = Vec::new();
        for (k, v) in &inner.reads {
            let version = match v {
                ReadState::Found(version) => Some(version.clone()),
                ReadState::NotFound => None,
            };
            reads.push(ReadAccess {
                path: k.as_str().into(),
                version,
            });
        }
        // Emit accesses in a stable path order so the commit path (transaction
        // log contents, lock acquisition order, validation order) is
        // independent of `HashMap`'s randomized iteration, and of the order in
        // which concurrent reads happened to insert their entries. This makes a
        // simulation replay byte-for-byte identical and is harmless in production.
        writes.sort_by(|a, b| a.path.cmp(&b.path));
        reads.sort_by(|a, b| a.path.cmp(&b.path));
        // Scans are recorded in listing order, which is already deterministic
        // (leaves scanned left-to-right), so they need no re-sorting.
        let scans = inner.scans.clone();
        Data {
            reads,
            writes,
            scans,
        }
    }
}

enum StagedValue {
    Read(Arc<[u8]>),
    Put(Arc<[u8]>),
    Delete,
}

impl StagedValue {
    fn read(&self) -> Result<Vec<u8>, Error> {
        match self {
            StagedValue::Read(value) | StagedValue::Put(value) => Ok(value.to_vec()),
            StagedValue::Delete => Err(Error::NotFound),
        }
    }
}

enum ReadState {
    Found(ReadVersion),
    NotFound,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use crate::{Backend, Database, memory::MemoryBackend};

    // ADR-031 phantom prevention, the in-flight case: when a key is created
    // *while* a listing transaction is running — after it scanned the leaf but
    // before it validated — the create rewrites the covered leaf, bumping its
    // version. The listing's commit validation detects the changed snapshot and
    // re-runs the transaction; the retry re-scans the fresh leaf and therefore
    // includes the racing key. A create is never silently dropped from a listing
    // it raced.
    #[tokio::test]
    async fn listing_retries_to_include_a_key_added_during_the_scan() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let db = Database::open("example", backend).await.unwrap();
        let coll = db.collection(b"phantom-retry");
        coll.create().await.unwrap();

        let seed: Vec<Vec<u8>> = (0u32..5).map(|i| i.to_be_bytes().to_vec()).collect();
        for k in &seed {
            coll.write(k, b"v").await.unwrap();
        }

        let extra = 999u32.to_be_bytes().to_vec();
        let first_attempt = AtomicBool::new(true);

        // The listing runs in a read-only transaction. On its first attempt a
        // concurrent transaction commits a new key *after* the scan recorded the
        // leaf version, modeling a create that lands mid-listing. That
        // invalidates the recorded snapshot, forcing the listing to retry.
        let listed = db
            .tx(|tx| {
                let coll = coll.clone();
                let extra = extra.clone();
                let first_attempt = &first_attempt;
                async move {
                    let keys = tx.keys(&coll).await?;
                    if first_attempt.swap(false, Ordering::SeqCst) {
                        coll.write(&extra, b"v").await?;
                    }
                    Ok(keys)
                }
            })
            .await
            .unwrap();

        assert!(
            listed.contains(&extra),
            "the key created during the listing is included after the retry"
        );
        let mut expected: Vec<Vec<u8>> = seed;
        expected.push(extra);
        expected.sort();
        assert_eq!(
            listed, expected,
            "the listing observes the full, sorted committed set"
        );
        assert!(
            db.stats().tx_retries >= 1,
            "the listing must have retried after its snapshot was invalidated"
        );
    }
}
