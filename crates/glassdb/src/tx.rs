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
use glassdb_data::{KeyRef, TxId};
use glassdb_storage::{LeafObservation, ShardStore, Timeline};
use glassdb_trans::{Data, ReadAccess, Reader, Resolver, ScanAccess, ScanMutation, WriteAccess};

use crate::collection::Collection;
use crate::error::Error;
use crate::scan::{KeyPage, KeyScan};

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
    staged: HashMap<KeyRef, StagedValue>,
    reads: HashMap<KeyRef, ReadState>,
    scans: Vec<ScanAccess>,
    aborted: bool,
}

pub(crate) struct TransactionMetrics {
    pub(crate) cache_hits: u64,
}

impl TransactionInner {
    fn record_read(&mut self, key: KeyRef, mut state: ReadState) {
        // Concurrent reads of one path can both miss the transaction-local
        // state. Preserve a hit observed by either result while still counting
        // the path once, consistently with `tx_reads`.
        if self.reads.get(&key).is_some_and(ReadState::cache_hit) {
            state.set_cache_hit();
        }
        self.reads.insert(key, state);
    }
}

impl Transaction {
    /// Reads the value for `key` within the transaction, returning `None` when
    /// the key is absent. Repeatable: a value read once is returned consistently,
    /// and a key not found stays not found (avoiding phantom reads).
    ///
    /// Takes `&self`, so multiple reads can be polled concurrently (e.g. with
    /// `futures::future::join_all`) to fetch keys in parallel.
    pub async fn read(&self, c: &Collection, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let key = KeyRef::new(c.address().clone(), key);
        // Brief lock to consult the per-transaction cache. The guard is dropped
        // before the backend read below so it is never held across `.await`.
        {
            let inner = self.inner.lock().unwrap();
            if let Some(staged) = inner.staged.get(&key) {
                return Ok(staged.read());
            }
            if let Some(ReadState::NotFound { .. }) = inner.reads.get(&key) {
                // Be consistent with values not found the first time.
                return Ok(None);
            }
        }

        match self.reader.read(&key, std::time::Duration::MAX).await {
            Ok(outcome) => match outcome.value {
                None => {
                    let mut inner = self.inner.lock().unwrap();
                    inner.record_read(
                        key,
                        ReadState::NotFound {
                            last_writer: outcome.last_writer,
                            cache_hit: outcome.cache_hit,
                            leaf: outcome.leaf,
                        },
                    );
                    Ok(None)
                }
                Some(rv) => {
                    let mut inner = self.inner.lock().unwrap();
                    inner
                        .staged
                        .insert(key.clone(), StagedValue::Read(rv.value.clone()));
                    inner.record_read(
                        key,
                        ReadState::Found {
                            last_writer: rv.version.writer,
                            cache_hit: outcome.cache_hit,
                            leaf: outcome.leaf,
                        },
                    );
                    Ok(Some(rv.value.to_vec()))
                }
            },
            // A read is side-effect-free; `from_read` centralizes the mapping
            // (notably a sustained outage becomes the retry-safe
            // `Error::Unavailable` rather than `InDoubt`).
            Err(e) => Err(Error::from_read(e)),
        }
    }

    /// Materializes one sorted page of keys within this transaction.
    ///
    /// The scan participates in serializable validation and reflects writes and
    /// deletes staged before this call. Values remain separate tracked reads.
    pub async fn scan_keys(&self, c: &Collection, scan: KeyScan<'_>) -> Result<KeyPage, Error> {
        let range = scan.normalize()?;
        let limit = range.limit;
        let mut overlay = {
            let inner = self.inner.lock().unwrap();
            inner
                .staged
                .iter()
                .filter_map(|(key, value)| {
                    if key.collection() != c.address() {
                        return None;
                    }
                    let present = match value {
                        StagedValue::Read(_) => return None,
                        StagedValue::Put(_) => true,
                        StagedValue::Delete => false,
                    };
                    Some(ScanMutation {
                        key: key.key().to_vec(),
                        present,
                    })
                })
                .collect::<Vec<_>>()
        };
        overlay.sort_by(|a, b| a.key.cmp(&b.key));

        let result = self
            .resolver
            .scan_keys(c.address(), &range, &overlay, None, None)
            .await
            .map_err(Error::from_read)?;
        let keys = result.keys;
        self.inner.lock().unwrap().scans.push(ScanAccess {
            collection: c.address().clone(),
            range,
            overlay,
            keys: keys.clone(),
            frontier: result.frontier,
            covered: result.covered,
        });
        Ok(KeyPage::new(keys, limit))
    }

    /// Stages a write of `value` to `key`.
    pub fn write(&self, c: &Collection, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let key = KeyRef::new(c.address().clone(), key);
        self.inner
            .lock()
            .unwrap()
            .staged
            .insert(key, StagedValue::Put(Arc::from(value)));
        Ok(())
    }

    /// Marks `key` for deletion within the transaction.
    pub fn delete(&self, c: &Collection, key: &[u8]) -> Result<(), Error> {
        let key = KeyRef::new(c.address().clone(), key);
        self.inner
            .lock()
            .unwrap()
            .staged
            .insert(key, StagedValue::Delete);
        Ok(())
    }

    /// Explicitly aborts the transaction. Returns [`Error::Aborted`].
    pub fn abort(&self) -> Result<(), Error> {
        self.inner.lock().unwrap().aborted = true;
        Err(Error::Aborted)
    }

    pub(crate) fn new(
        shards: ShardStore,
        timeline: Timeline,
        tmon: glassdb_trans::Monitor,
        retry: RetryConfig,
    ) -> Self {
        let resolver = Resolver::new(shards, tmon);
        Transaction {
            reader: Reader::new(resolver.clone(), timeline, retry),
            resolver,
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
                StagedValue::Put(val) => writes.push(WriteAccess::put(k.clone(), val.clone())),
                StagedValue::Delete => writes.push(WriteAccess::delete(k.clone())),
            }
        }
        let mut reads = Vec::new();
        for (k, v) in &inner.reads {
            let (last_writer, leaf) = match v {
                ReadState::Found {
                    last_writer, leaf, ..
                } => (Some(last_writer.clone()), leaf.clone()),
                ReadState::NotFound {
                    last_writer, leaf, ..
                } => (last_writer.clone(), leaf.clone()),
            };
            reads.push(ReadAccess {
                key: k.clone(),
                last_writer,
                leaf,
            });
        }
        // Emit accesses in a stable path order so the commit path (transaction
        // log contents, lock acquisition order, validation order) is
        // independent of `HashMap`'s randomized iteration, and of the order in
        // which concurrent reads happened to insert their entries. This makes a
        // simulation replay byte-for-byte identical and is harmless in production.
        writes.sort_by(|a, b| a.key.cmp(&b.key));
        reads.sort_by(|a, b| a.key.cmp(&b.key));
        // Scans are recorded in listing order, which is already deterministic
        // (leaves scanned left-to-right), so they need no re-sorting.
        let scans = inner.scans.clone();
        Data {
            reads,
            writes,
            scans,
        }
    }

    pub(crate) fn metrics(&self) -> TransactionMetrics {
        let inner = self.inner.lock().unwrap();
        TransactionMetrics {
            cache_hits: inner.reads.values().filter(|r| r.cache_hit()).count() as u64,
        }
    }
}

enum StagedValue {
    Read(Arc<[u8]>),
    Put(Arc<[u8]>),
    Delete,
}

impl StagedValue {
    fn read(&self) -> Option<Vec<u8>> {
        match self {
            StagedValue::Read(value) | StagedValue::Put(value) => Some(value.to_vec()),
            StagedValue::Delete => None,
        }
    }
}

enum ReadState {
    Found {
        last_writer: TxId,
        cache_hit: bool,
        leaf: LeafObservation,
    },
    NotFound {
        last_writer: Option<TxId>,
        cache_hit: bool,
        leaf: LeafObservation,
    },
}

impl ReadState {
    fn cache_hit(&self) -> bool {
        match self {
            ReadState::Found { cache_hit, .. } | ReadState::NotFound { cache_hit, .. } => {
                *cache_hit
            }
        }
    }

    fn set_cache_hit(&mut self) {
        match self {
            ReadState::Found { cache_hit, .. } | ReadState::NotFound { cache_hit, .. } => {
                *cache_hit = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use crate::{Backend, CollectionPath, Database, KeyScan, memory::MemoryBackend};

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
        let path = CollectionPath::new(b"phantom-retry").unwrap();
        let coll = db.create_collection(&path).await.unwrap();

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
                    let keys = tx.scan_keys(&coll, KeyScan::all()).await?.into_keys();
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
