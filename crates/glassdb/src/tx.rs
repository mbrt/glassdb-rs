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

use glassdb_data::paths;
use glassdb_storage::{Global, Local, MAX_STALENESS, StorageError};
use glassdb_trans::{Data, ReadAccess, ReadVersion, Reader, WriteAccess};

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
    inner: Arc<Mutex<TransactionInner>>,
}

#[derive(Default)]
struct TransactionInner {
    staged: HashMap<String, StagedValue>,
    reads: HashMap<String, ReadState>,
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
            Err(e) => Err(Error::with_source("reading from storage", e)),
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

    pub(crate) fn new(global: Global, local: Local, tmon: glassdb_trans::Monitor) -> Self {
        Transaction {
            reader: Reader::new(local, global, tmon),
            inner: Arc::new(Mutex::new(TransactionInner::default())),
        }
    }

    /// Returns another handle to the same transaction state. The framework
    /// passes a handle to the user closure (which consumes it) while keeping one
    /// to inspect the staged accesses and reset between retries.
    pub(crate) fn handle(&self) -> Transaction {
        Transaction {
            reader: self.reader.clone(),
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
        Data { reads, writes }
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
