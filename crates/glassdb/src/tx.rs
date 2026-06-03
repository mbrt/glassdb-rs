//! Active transaction state. Ported from the Go `tx.go`: staged writes and
//! tracked reads (to provide repeatable reads and avoid phantom reads), plus
//! access collection for the commit algorithm.

use std::collections::HashMap;

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
            staged: HashMap::new(),
            reads: HashMap::new(),
            aborted: false,
        }
    }

    /// Reads the value for `key` within the transaction. Repeatable: a value
    /// read once is returned consistently, and a key not found stays not found
    /// (avoiding phantom reads).
    pub async fn read(&mut self, c: &Collection, key: &[u8]) -> Result<Vec<u8>, Error> {
        let p = paths::from_key(c.prefix(), key);
        if let Some(tv) = self.staged.get(&p) {
            return Ok(tv.val.clone());
        }
        if let Some(info) = self.reads.get(&p) {
            if !info.found {
                // Be consistent with values not found the first time.
                return Err(Error::NotFound);
            }
        }

        match self.reader.read(&self.ctx, &p, MAX_STALENESS).await {
            Err(e) if e.is_not_found() => {
                self.reads.insert(
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
                self.staged.insert(
                    p.clone(),
                    Tvalue {
                        val: rv.value.clone(),
                        modified: false,
                        deleted: false,
                    },
                );
                self.reads.insert(
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

    /// Reads multiple keys, fetching uncached ones concurrently.
    pub async fn read_multi(&mut self, ks: &[FqKey]) -> Vec<ReadResult> {
        if ks.is_empty() {
            return Vec::new();
        }
        let mut res: Vec<ReadResult> = vec![ReadResult::default(); ks.len()];
        let mut to_fetch: Vec<(usize, String)> = Vec::new();

        for (i, key) in ks.iter().enumerate() {
            let p = paths::from_key(key.collection.prefix(), &key.key);
            if let Some(tv) = self.staged.get(&p) {
                res[i] = ReadResult {
                    value: tv.val.clone(),
                    err: None,
                };
                continue;
            }
            if let Some(info) = self.reads.get(&p) {
                if !info.found {
                    res[i] = ReadResult {
                        value: Vec::new(),
                        err: Some(Error::NotFound),
                    };
                    continue;
                }
            }
            to_fetch.push((i, p));
        }

        // Fetch the uncached keys in parallel.
        let futs = to_fetch.iter().map(|(_, p)| {
            let reader = self.reader.clone();
            let ctx = self.ctx.clone();
            let p = p.clone();
            async move { reader.read(&ctx, &p, MAX_STALENESS).await }
        });
        let fetched = futures::future::join_all(futs).await;

        // Update the staged/read maps serially to avoid races.
        for ((i, p), r) in to_fetch.into_iter().zip(fetched) {
            match r {
                Ok(rv) => {
                    res[i] = ReadResult {
                        value: rv.value.clone(),
                        err: None,
                    };
                    self.staged.insert(
                        p.clone(),
                        Tvalue {
                            val: rv.value,
                            modified: false,
                            deleted: false,
                        },
                    );
                    self.reads.insert(
                        p,
                        ReadInfo {
                            version: ReadVersion {
                                last_writer: rv.version.writer,
                            },
                            found: true,
                        },
                    );
                }
                Err(e) if e.is_not_found() => {
                    res[i] = ReadResult {
                        value: Vec::new(),
                        err: Some(Error::NotFound),
                    };
                    self.reads.insert(
                        p,
                        ReadInfo {
                            version: ReadVersion::default(),
                            found: false,
                        },
                    );
                }
                Err(e) => {
                    res[i] = ReadResult {
                        value: Vec::new(),
                        err: Some(Error::Other(format!("reading from storage: {e}"))),
                    };
                }
            }
        }

        res
    }

    /// Stages a write of `value` to `key`.
    pub fn write(&mut self, c: &Collection, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let p = paths::from_key(c.prefix(), key);
        self.staged.insert(
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
    pub fn delete(&mut self, c: &Collection, key: &[u8]) -> Result<(), Error> {
        let p = paths::from_key(c.prefix(), key);
        self.staged.insert(
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
    pub fn abort(&mut self) -> Result<(), Error> {
        self.aborted = true;
        Err(Error::Aborted)
    }

    pub(crate) fn aborted(&self) -> bool {
        self.aborted
    }

    pub(crate) fn reset(&mut self) {
        self.staged.clear();
        self.reads.clear();
    }

    pub(crate) fn collect_accesses(&self) -> Data {
        let mut writes = Vec::new();
        for (k, v) in &self.staged {
            if !v.modified && !v.deleted {
                continue;
            }
            writes.push(WriteAccess {
                path: k.clone(),
                val: v.val.clone(),
                delete: v.deleted,
            });
        }
        let mut reads = Vec::new();
        for (k, v) in &self.reads {
            reads.push(ReadAccess {
                path: k.clone(),
                version: v.version.clone(),
                found: v.found,
            });
        }
        // Emit accesses in a stable path order so the commit path (transaction
        // log contents, lock acquisition order, validation order) is
        // independent of `HashMap`'s randomized iteration. This makes a madsim
        // replay byte-for-byte identical and is harmless in production.
        writes.sort_by(|a, b| a.path.cmp(&b.path));
        reads.sort_by(|a, b| a.path.cmp(&b.path));
        Data { reads, writes }
    }
}

/// A fully qualified key: a collection plus a key name.
#[derive(Clone)]
pub struct FqKey {
    pub collection: Collection,
    pub key: Vec<u8>,
}

/// The value or error from a single read in [`Tx::read_multi`].
#[derive(Debug, Clone, Default)]
pub struct ReadResult {
    pub value: Vec<u8>,
    pub err: Option<Error>,
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
