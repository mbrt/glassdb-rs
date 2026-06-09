//! Active transaction state. Ported from the Go `tx.go`: staged writes and
//! tracked reads (to provide repeatable reads and avoid phantom reads), plus
//! access collection for the commit algorithm.
//!
//! [`Tx`] is a cheap, `Send` handle over shared, interior-mutable state
//! (`Arc<Mutex<TxInner>>`). It is passed *by value* into the transaction
//! closure so the resulting future is `Send` and can be `tokio::spawn`-ed; the
//! framework keeps its own handle (see [`Tx::handle`]) to read the collected
//! accesses after the closure returns and to reset between retries. All methods
//! take `&self` and only hold the lock briefly — never across an `.await` — so
//! several reads can run concurrently within a single transaction.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use glassdb_concurr::Ctx;
use glassdb_data::paths;
use glassdb_storage::{Global, Local, MAX_STALENESS};
use glassdb_trans::{Data, ReadAccess, ReadVersion, Reader, WriteAccess};

use crate::collection::Collection;
use crate::error::Error;

/// An active database transaction. Reads and writes are buffered and only
/// applied atomically when the surrounding [`crate::DB::tx`] commits.
///
/// Awaiting [`Tx::read`] (and the enclosing [`crate::DB::tx`] future) is
/// durability-safe to cancel; see the cancellation note on [`crate::DB::tx`] for
/// why callers should prefer `Ctx` cancellation over dropping the future.
pub struct Tx {
    ctx: Ctx,
    reader: Reader,
    inner: Arc<Mutex<TxInner>>,
}

#[derive(Default)]
struct TxInner {
    staged: HashMap<String, Tvalue>,
    reads: HashMap<String, ReadInfo>,
    aborted: bool,
}

impl Tx {
    pub(crate) fn new(
        ctx: Ctx,
        global: Global,
        local: Local,
        tmon: glassdb_trans::Monitor,
    ) -> Self {
        Tx {
            ctx,
            reader: Reader::new(local, global, tmon),
            inner: Arc::new(Mutex::new(TxInner::default())),
        }
    }

    /// Returns another handle to the same transaction state. The framework
    /// passes a handle to the user closure (which consumes it) while keeping one
    /// to inspect the staged accesses and reset between retries.
    pub(crate) fn handle(&self) -> Tx {
        Tx {
            ctx: self.ctx.clone(),
            reader: self.reader.clone(),
            inner: self.inner.clone(),
        }
    }

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
            if let Some(tv) = inner.staged.get(&p) {
                return Ok(tv.val.clone());
            }
            if let Some(info) = inner.reads.get(&p)
                && !info.found
            {
                // Be consistent with values not found the first time.
                return Err(Error::NotFound);
            }
        }

        match self.reader.read(&self.ctx, &p, MAX_STALENESS).await {
            Err(e) if e.is_not_found() => {
                let mut inner = self.inner.lock().unwrap();
                inner.reads.insert(
                    p,
                    ReadInfo {
                        version: ReadVersion::default(),
                        found: false,
                    },
                );
                Err(Error::NotFound)
            }
            Err(e) => Err(Error::Other(format!("reading from storage: {e}"))),
            Ok(rv) => {
                let mut inner = self.inner.lock().unwrap();
                inner.staged.insert(
                    p.clone(),
                    Tvalue {
                        val: rv.value.clone(),
                        modified: false,
                        deleted: false,
                    },
                );
                inner.reads.insert(
                    p,
                    ReadInfo {
                        version: ReadVersion {
                            last_writer: rv.version.writer,
                        },
                        found: true,
                    },
                );
                Ok(rv.value)
            }
        }
    }

    /// Stages a write of `value` to `key`.
    pub fn write(&self, c: &Collection, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let p = paths::from_key(c.prefix(), key);
        self.inner.lock().unwrap().staged.insert(
            p,
            Tvalue {
                val: value.to_vec(),
                modified: true,
                deleted: false,
            },
        );
        Ok(())
    }

    /// Marks `key` for deletion within the transaction.
    pub fn delete(&self, c: &Collection, key: &[u8]) -> Result<(), Error> {
        let p = paths::from_key(c.prefix(), key);
        self.inner.lock().unwrap().staged.insert(
            p,
            Tvalue {
                val: Vec::new(),
                modified: false,
                deleted: true,
            },
        );
        Ok(())
    }

    /// Explicitly aborts the transaction. Returns [`Error::Aborted`].
    pub fn abort(&self) -> Result<(), Error> {
        self.inner.lock().unwrap().aborted = true;
        Err(Error::Aborted)
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
            if !v.modified && !v.deleted {
                continue;
            }
            writes.push(WriteAccess {
                path: k.as_str().into(),
                val: v.val.clone(),
                delete: v.deleted,
            });
        }
        let mut reads = Vec::new();
        for (k, v) in &inner.reads {
            reads.push(ReadAccess {
                path: k.as_str().into(),
                version: v.version.clone(),
                found: v.found,
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

struct Tvalue {
    val: Vec<u8>,
    modified: bool,
    deleted: bool,
}

struct ReadInfo {
    version: ReadVersion,
    found: bool,
}
