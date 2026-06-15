//! Read-through / write-through caching over a backend. Ported from the Go
//! `internal/storage/global.go`.

use std::sync::Arc;

use glassdb_backend::{self as backend, Backend, Metadata, Tags, WriterId};
use glassdb_data::TxId;

use crate::error::StorageError;
use crate::local::{Local, MAX_STALENESS};
use crate::version::{Version, version_from_meta};

/// The result of reading a value from global storage.
#[derive(Debug, Clone)]
pub struct GlobalRead {
    pub value: Arc<[u8]>,
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
    pub async fn read(&self, key: &str) -> Result<GlobalRead, StorageError> {
        if let Some(e) = self.local.read(key, MAX_STALENESS) {
            // For a local override or a value known to be outdated, do a
            // regular read instead.
            if !e.outdated && !e.version.b.is_unset() {
                let writer = WriterId::new(e.version.writer.as_bytes().to_vec());
                let mut modified = true;
                let mut reply = backend::ReadReply::default();
                match self.backend.read_if_modified(key, &writer).await {
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
                    let contents: Arc<[u8]> = Arc::from(reply.contents);
                    self.local
                        .write_with_meta(key, contents.clone(), meta.clone());
                    return Ok(GlobalRead {
                        value: contents,
                        version: version_from_meta(&meta),
                    });
                }
                return Ok(GlobalRead {
                    value: e.value,
                    version: e.version,
                });
            }
        }

        let r = self.backend.read(key).await?;
        let meta = Arc::new(Metadata {
            tags: Arc::new(r.tags),
            version: r.version,
        });
        let contents: Arc<[u8]> = Arc::from(r.contents);
        self.local
            .write_with_meta(key, contents.clone(), meta.clone());
        Ok(GlobalRead {
            value: contents,
            version: version_from_meta(&meta),
        })
    }

    /// Fetches metadata from the backend and updates the cache. The returned
    /// metadata is shared (`Arc`) with the cache entry, so neither the cache
    /// update nor the caller deep-copies the tag map.
    pub async fn get_metadata(&self, key: &str) -> Result<Arc<Metadata>, StorageError> {
        let meta = Arc::new(self.backend.get_metadata(key).await?);
        self.local.set_meta(key, meta.clone());
        Ok(meta)
    }

    /// Conditionally sets tags and updates the metadata cache.
    pub async fn set_tags_if(
        &self,
        key: &str,
        expected: &backend::Version,
        t: Tags,
    ) -> Result<Arc<Metadata>, StorageError> {
        let meta = Arc::new(self.backend.set_tags_if(key, expected, t).await?);
        self.local.set_meta(key, meta.clone());
        Ok(meta)
    }

    /// Unconditionally writes and updates the cache.
    pub async fn write(
        &self,
        key: &str,
        value: Arc<[u8]>,
        t: Tags,
    ) -> Result<Arc<Metadata>, StorageError> {
        let meta = Arc::new(self.backend.write(key, value.to_vec(), t).await?);
        self.local.write_with_meta(key, value, meta.clone());
        Ok(meta)
    }

    /// Conditionally writes and updates the cache.
    pub async fn write_if(
        &self,
        key: &str,
        value: Arc<[u8]>,
        expected: &backend::Version,
        t: Tags,
    ) -> Result<Arc<Metadata>, StorageError> {
        let meta = Arc::new(
            self.backend
                .write_if(key, value.to_vec(), expected, t)
                .await?,
        );
        self.local.write_with_meta(key, value, meta.clone());
        Ok(meta)
    }

    /// Creates the object if absent and updates the cache.
    pub async fn write_if_not_exists(
        &self,
        key: &str,
        value: Arc<[u8]>,
        t: Tags,
    ) -> Result<Arc<Metadata>, StorageError> {
        let meta = Arc::new(
            self.backend
                .write_if_not_exists(key, value.to_vec(), t)
                .await?,
        );
        self.local.write_with_meta(key, value, meta.clone());
        Ok(meta)
    }

    /// Deletes the object and removes it from the cache.
    pub async fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.backend.delete(key).await?;
        self.local.delete(key);
        Ok(())
    }

    /// Conditionally deletes the object and removes it from the cache.
    pub async fn delete_if(
        &self,
        key: &str,
        expected: &backend::Version,
    ) -> Result<(), StorageError> {
        self.backend.delete_if(key, expected).await?;
        self.local.delete(key);
        Ok(())
    }

    /// Lists object paths under `dir_path`.
    pub async fn list(&self, dir_path: &str) -> Result<Vec<String>, StorageError> {
        Ok(self.backend.list(dir_path).await?)
    }
}
