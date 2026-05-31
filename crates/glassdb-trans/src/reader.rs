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

    async fn handle_lock_create(
        &self,
        ctx: &Ctx,
        key: &str,
        rv: ReadValue,
    ) -> Result<ReadValue, StorageError> {
        if !rv.value.is_empty() {
            // Safe to return: it wasn't locked in create.
            return Ok(rv);
        }
        // We might have read a value locked in create (not yet committed).
        // Check with an extra metadata read.
        let meta = self.global.get_metadata(ctx, key).await?;
        let info = tags_lock_info(&meta.tags)?;
        if info.typ != LockType::Create {
            // A genuinely committed empty value.
            return Ok(rv);
        }
        if info.locked_by.len() != 1 {
            return Err(BackendError::NotFound.into());
        }
        let locker_id = info.locked_by[0].clone();

        let cv = match self.tmon.committed_value(ctx, key, &locker_id).await {
            Ok(cv) => cv,
            Err(_) => return Err(BackendError::NotFound.into()),
        };
        if cv.status != TxCommitStatus::Ok || cv.value.not_written {
            return Err(BackendError::NotFound.into());
        }

        let version = Version {
            b: glassdb_backend::Version::default(),
            writer: locker_id,
        };
        if cv.value.deleted {
            self.local.mark_deleted(key, version);
            return Err(BackendError::NotFound.into());
        }
        self.local
            .write(key, cv.value.value.clone(), version.clone());
        Ok(ReadValue {
            value: cv.value.value,
            version,
        })
    }
}
