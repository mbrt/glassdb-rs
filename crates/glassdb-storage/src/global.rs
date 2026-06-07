//! Read-through / write-through caching over a backend. Ported from the Go
//! `internal/storage/global.go`.

use std::sync::Arc;

use glassdb_backend::{self as backend, Backend, Metadata, Tags, WriterId};
use glassdb_concurr::Ctx;
use glassdb_data::TxId;

use crate::error::StorageError;
use crate::local::{Local, MAX_STALENESS};
use crate::version::{version_from_meta, Version};

/// The result of reading a value from global storage.
#[derive(Debug, Clone)]
pub struct GlobalRead {
    pub value: Vec<u8>,
    pub version: Version,
}

impl GlobalRead {
    /// The transaction ID of the last writer (empty if none recorded).
    pub fn writer(&self) -> TxId {
        self.version.writer.clone()
    }
}

/// Wraps a backend with a local cache, performing read-through and
/// write-through caching of objects and metadata.
#[derive(Clone)]
pub struct Global {
    backend: Arc<dyn Backend>,
    local: Local,
}

impl Global {
    /// Creates a global storage reading and writing through `backend`, using
    /// `local` as a cache.
    pub fn new(backend: Arc<dyn Backend>, local: Local) -> Self {
        Global { backend, local }
    }

    /// Reads the value at `key`, using the cache when possible.
    pub async fn read(&self, ctx: &Ctx, key: &str) -> Result<GlobalRead, StorageError> {
        if let Some(e) = self.local.read(key, MAX_STALENESS) {
            // For a local override or a value known to be outdated, do a
            // regular read instead.
            if !e.outdated && !e.version.b.is_null() {
                let writer = WriterId::new(e.version.writer.as_bytes().to_vec());
                let mut modified = true;
                let mut reply = backend::ReadReply::default();
                match self.backend.read_if_modified(ctx, key, &writer).await {
                    Ok(r) => reply = r,
                    Err(err) => {
                        if !err.is_precondition() {
                            return Err(StorageError::Backend(err));
                        }
                        modified = false;
                    }
                }
                if modified {
                    let meta = Arc::new(Metadata {
                        tags: Arc::new(reply.tags),
                        version: reply.version,
                    });
                    self.local
                        .write_with_meta(key, reply.contents.clone(), meta.clone());
                    return Ok(GlobalRead {
                        value: reply.contents,
                        version: version_from_meta(&meta),
                    });
                }
                return Ok(GlobalRead {
                    value: e.value,
                    version: e.version,
                });
            }
        }

        let r = self.backend.read(ctx, key).await?;
        let meta = Arc::new(Metadata {
            tags: Arc::new(r.tags),
            version: r.version,
        });
        self.local
            .write_with_meta(key, r.contents.clone(), meta.clone());
        Ok(GlobalRead {
            value: r.contents,
            version: version_from_meta(&meta),
        })
    }

    /// Fetches metadata from the backend and updates the cache. The returned
    /// metadata is shared (`Arc`) with the cache entry, so neither the cache
    /// update nor the caller deep-copies the tag map.
    pub async fn get_metadata(&self, ctx: &Ctx, key: &str) -> Result<Arc<Metadata>, StorageError> {
        let meta = Arc::new(self.backend.get_metadata(ctx, key).await?);
        self.local.set_meta(key, meta.clone());
        Ok(meta)
    }

    /// Conditionally sets tags and updates the metadata cache.
    pub async fn set_tags_if(
        &self,
        ctx: &Ctx,
        key: &str,
        expected: &backend::Version,
        t: Tags,
    ) -> Result<Arc<Metadata>, StorageError> {
        let meta = Arc::new(self.backend.set_tags_if(ctx, key, expected, t).await?);
        self.local.set_meta(key, meta.clone());
        Ok(meta)
    }

    /// Unconditionally writes and updates the cache.
    pub async fn write(
        &self,
        ctx: &Ctx,
        key: &str,
        value: Vec<u8>,
        t: Tags,
    ) -> Result<Arc<Metadata>, StorageError> {
        let meta = Arc::new(self.backend.write(ctx, key, value.clone(), t).await?);
        self.local.write_with_meta(key, value, meta.clone());
        Ok(meta)
    }

    /// Conditionally writes and updates the cache.
    pub async fn write_if(
        &self,
        ctx: &Ctx,
        key: &str,
        value: Vec<u8>,
        expected: &backend::Version,
        t: Tags,
    ) -> Result<Arc<Metadata>, StorageError> {
        let meta = Arc::new(
            self.backend
                .write_if(ctx, key, value.clone(), expected, t)
                .await?,
        );
        self.local.write_with_meta(key, value, meta.clone());
        Ok(meta)
    }

    /// Creates the object if absent and updates the cache.
    pub async fn write_if_not_exists(
        &self,
        ctx: &Ctx,
        key: &str,
        value: Vec<u8>,
        t: Tags,
    ) -> Result<Arc<Metadata>, StorageError> {
        let meta = Arc::new(
            self.backend
                .write_if_not_exists(ctx, key, value.clone(), t)
                .await?,
        );
        self.local.write_with_meta(key, value, meta.clone());
        Ok(meta)
    }

    /// Deletes the object and removes it from the cache.
    pub async fn delete(&self, ctx: &Ctx, key: &str) -> Result<(), StorageError> {
        self.backend.delete(ctx, key).await?;
        self.local.delete(key);
        Ok(())
    }

    /// Conditionally deletes the object and removes it from the cache.
    pub async fn delete_if(
        &self,
        ctx: &Ctx,
        key: &str,
        expected: &backend::Version,
    ) -> Result<(), StorageError> {
        self.backend.delete_if(ctx, key, expected).await?;
        self.local.delete(key);
        Ok(())
    }

    /// Lists object paths under `dir_path`.
    pub async fn list(&self, ctx: &Ctx, dir_path: &str) -> Result<Vec<String>, StorageError> {
        Ok(self.backend.list(ctx, dir_path).await?)
    }
}
