//! The transactional read path. Ported from the Go `internal/trans/reader.go`.
//!
//! Reads from local then global storage, and resolves keys that may be locked
//! in "create" (i.e. uncommitted) by consulting the transaction monitor.

use std::time::Duration;

use glassdb_backend::{BackendError, Metadata};
use glassdb_concurr::Ctx;
use glassdb_storage::{
    tags_lock_info, Global, Local, LockType, StorageError, TxCommitStatus, Version,
};

use crate::monitor::Monitor;

/// The result of reading a key: the raw value and its storage version.
#[derive(Debug, Clone, Default)]
pub struct ReadValue {
    pub value: Vec<u8>,
    pub version: Version,
}

/// Reads values from local and global storage, resolving create-locked keys.
#[derive(Clone)]
pub struct Reader {
    local: Local,
    global: Global,
    tmon: Monitor,
}

impl Reader {
    /// Creates a reader over local/global storage and a monitor.
    pub fn new(local: Local, global: Global, tmon: Monitor) -> Self {
        Reader {
            local,
            global,
            tmon,
        }
    }

    /// Reads the value for `key`, accepting cached values up to `max_stale`.
    pub async fn read(
        &self,
        ctx: &Ctx,
        key: &str,
        max_stale: Duration,
    ) -> Result<ReadValue, StorageError> {
        if let Some(lr) = self.local.read(key, max_stale) {
            if !lr.outdated {
                if lr.deleted {
                    return Err(BackendError::NotFound.into());
                }
                let lres = ReadValue {
                    value: lr.value,
                    version: lr.version,
                };
                return self.handle_lock_create(ctx, key, lres).await;
            }
        }
        let gr = self.global.read(ctx, key).await?;
        let gres = ReadValue {
            value: gr.value,
            version: gr.version,
        };
        self.handle_lock_create(ctx, key, gres).await
    }

    /// Returns the object metadata, using the local cache when fresh enough.
    pub async fn get_metadata(
        &self,
        ctx: &Ctx,
        key: &str,
        max_stale: Duration,
    ) -> Result<Metadata, StorageError> {
        if let Some(lm) = self.local.get_meta(key, max_stale) {
            if !lm.outdated {
                return Ok(lm.m);
            }
        }
        self.global.get_metadata(ctx, key).await
    }

    /// Resolves a possibly-empty read. An empty value can be a lock placeholder
    /// rather than a genuinely-empty committed value: acquiring a create lock
    /// writes an empty object, and a write/read lock can leave the object empty
    /// while the locker holds it. In every locked case the authoritative value
    /// lives in the transaction log, so we resolve it through the monitor,
    /// mirroring the commit-time logic in `algo::validate_locked_read`. When the
    /// value cannot be authoritatively resolved we report `NotFound` rather than
    /// the misleading empty bytes, which makes the surrounding transaction retry
    /// and re-read the committed value.
    async fn handle_lock_create(
        &self,
        ctx: &Ctx,
        key: &str,
        rv: ReadValue,
    ) -> Result<ReadValue, StorageError> {
        if !rv.value.is_empty() {
            // A non-empty value is never a lock placeholder.
            return Ok(rv);
        }
        let meta = self.global.get_metadata(ctx, key).await?;
        let info = tags_lock_info(&meta.tags)?;

        // Determine whose committed value is authoritative for this empty object.
        let writer = if info.typ == LockType::None {
            // Unlocked but empty: a committed writer can release its lock before
            // its value reaches the object (the value still lives in its log).
            // Resolve through the recorded last writer; with none recorded the
            // empty value is genuinely committed.
            if info.last_writer.is_empty() {
                return Ok(rv);
            }
            info.last_writer.clone()
        } else if info.locked_by.len() == 1 {
            let locker = info.locked_by[0].clone();
            // The locker if it has committed and wrote this key, otherwise the
            // previous last writer.
            match self.tmon.tx_status(ctx, &locker).await {
                Ok(TxCommitStatus::Ok) => {
                    match self.tmon.committed_value(ctx, key, &locker).await {
                        Ok(cv) if cv.status == TxCommitStatus::Ok && !cv.value.not_written => {
                            // The locker's own committed write is authoritative.
                            return self.materialize(key, locker, cv.value);
                        }
                        // Committed but did not write this key: fall back to the
                        // previous writer.
                        Ok(_) => info.last_writer.clone(),
                        Err(_) => return Err(BackendError::NotFound.into()),
                    }
                }
                Ok(TxCommitStatus::Aborted) | Ok(TxCommitStatus::Pending) => {
                    info.last_writer.clone()
                }
                Ok(TxCommitStatus::Unknown) => {
                    return Err(StorageError::Other("unknown tx commit status".into()))
                }
                Err(_) => return Err(BackendError::NotFound.into()),
            }
        } else {
            return Err(BackendError::NotFound.into());
        };

        if writer.is_empty() {
            // No prior committed value (e.g. a pending create): not found.
            return Err(BackendError::NotFound.into());
        }
        match self.tmon.committed_value(ctx, key, &writer).await {
            Ok(cv) if cv.status == TxCommitStatus::Ok && !cv.value.not_written => {
                self.materialize(key, writer, cv.value)
            }
            // Unresolvable (e.g. the writer committed via the single-RW fast
            // path, which writes no log): retry rather than trust empty bytes.
            _ => Err(BackendError::NotFound.into()),
        }
    }

    /// Caches and returns a resolved committed value, or reports `NotFound` for a
    /// tombstone.
    fn materialize(
        &self,
        key: &str,
        writer: glassdb_data::TxId,
        value: glassdb_storage::TValue,
    ) -> Result<ReadValue, StorageError> {
        let version = Version {
            b: glassdb_backend::Version::default(),
            writer,
        };
        if value.deleted {
            self.local.mark_deleted(key, version);
            return Err(BackendError::NotFound.into());
        }
        self.local.write(key, value.value.clone(), version.clone());
        Ok(ReadValue {
            value: value.value,
            version,
        })
    }
}
