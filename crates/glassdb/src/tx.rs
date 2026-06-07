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
pub struct Tx {
    ctx: Ctx,
    reader: Reader,
    inner: Arc<Mutex<TxInner>>,
}

#[derive(Default)]
struct TxInner {
    // Reads and writes for a path share one entry, so a found read allocates the
    // path key once instead of inserting it into two maps. `staged` holds a
    // read-cached value, a write, or a delete; `read` records the version read
    // (for validation / repeatable reads).
    entries: HashMap<String, Entry>,
    aborted: bool,
}

#[derive(Default)]
struct Entry {
    staged: Option<Tvalue>,
    read: Option<ReadInfo>,
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
            if let Some(e) = inner.entries.get(&p) {
                if let Some(tv) = &e.staged {
                    return Ok(tv.val.to_vec());
                }
                if let Some(info) = &e.read {
                    if !info.found {
                        // Be consistent with values not found the first time.
                        return Err(Error::NotFound);
                    }
                }
            }
        }

        match self.reader.read(&self.ctx, &p, MAX_STALENESS).await {
            Err(e) if e.is_not_found() => {
                let mut inner = self.inner.lock().unwrap();
                inner.entries.entry(p).or_default().read = Some(ReadInfo {
                    version: ReadVersion::default(),
                    found: false,
                });
                Err(Error::NotFound)
            }
            Err(e) => Err(Error::Other(format!("reading from storage: {e}"))),
            Ok(rv) => {
                let mut inner = self.inner.lock().unwrap();
                let e = inner.entries.entry(p).or_default();
                // Stage the shared value (refcount bump) for repeatable reads and
                // hand the caller an owned copy; only this final copy allocates.
                e.staged = Some(Tvalue {
                    val: rv.value.clone(),
                    modified: false,
                    deleted: false,
                });
                e.read = Some(ReadInfo {
                    version: ReadVersion {
                        last_writer: rv.version.writer,
                    },
                    found: true,
                });
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
            .entries
            .entry(p)
            .or_default()
            .staged = Some(Tvalue {
            val: Arc::from(value),
            modified: true,
            deleted: false,
        });
        Ok(())
    }

    /// Marks `key` for deletion within the transaction.
    pub fn delete(&self, c: &Collection, key: &[u8]) -> Result<(), Error> {
        let p = paths::from_key(c.prefix(), key);
        self.inner
            .lock()
            .unwrap()
            .entries
            .entry(p)
            .or_default()
            .staged = Some(Tvalue {
            val: Arc::from(&[] as &[u8]),
            modified: false,
            deleted: true,
        });
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
        self.inner.lock().unwrap().entries.clear();
    }

    pub(crate) fn collect_accesses(&self) -> Data {
        // Drain the entries: this is called once per attempt and the map is reset
        // (or dropped) afterwards, so move out the staged values instead of
        // cloning them, and build each path's `Arc<str>` once even when a key is
        // both read and written.
        let entries = std::mem::take(&mut self.inner.lock().unwrap().entries);
        let mut writes = Vec::new();
        let mut reads = Vec::new();
        for (k, e) in entries {
            let has_write = e.staged.as_ref().is_some_and(|v| v.modified || v.deleted);
            match (has_write, e.read.is_some()) {
                (false, false) => {}
                (true, false) => {
                    let v = e.staged.unwrap();
                    writes.push(WriteAccess {
                        path: k.as_str().into(),
                        val: v.val,
                        delete: v.deleted,
                    });
                }
                (false, true) => {
                    let r = e.read.unwrap();
                    reads.push(ReadAccess {
                        path: k.as_str().into(),
                        version: r.version,
                        found: r.found,
                    });
                }
                (true, true) => {
                    let path: Arc<str> = k.as_str().into();
                    let v = e.staged.unwrap();
                    writes.push(WriteAccess {
                        path: path.clone(),
                        val: v.val,
                        delete: v.deleted,
                    });
                    let r = e.read.unwrap();
                    reads.push(ReadAccess {
                        path,
                        version: r.version,
                        found: r.found,
                    });
                }
            }
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
    val: Arc<[u8]>,
    modified: bool,
    deleted: bool,
}

struct ReadInfo {
    version: ReadVersion,
    found: bool,
}
