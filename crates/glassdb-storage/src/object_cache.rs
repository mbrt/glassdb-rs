//! The backend-version-keyed, read-through / write-through object cache
//! (ADR-023). Coordination objects (shards, collection roots, transaction
//! logs) change in place by content CAS, so a cached copy is identified by the
//! object's backend version (an opaque content-CAS token) and revalidated with
//! a version-conditional read.
//!
//! It is one of two facades over a single shared LRU ([`crate::SharedCache`]);
//! the other is the writer-keyed [`crate::ValueCache`]. Both are built from the
//! same [`crate::SharedCache`], so a single `cache_size` bounds both.

use std::sync::Arc;

use glassdb_backend::{self as backend, Backend};

use crate::cache::Cache;
use crate::entry::{CacheEntry, ObjectEntry, SharedCache};
use crate::error::StorageError;

/// The result of reading an object from the object cache: its body and backend
/// version (an opaque content-CAS token).
#[derive(Debug, Clone)]
pub struct ObjectRead {
    pub value: Arc<[u8]>,
    pub version: backend::Version,
}

/// Whether a cached coordination object must be revalidated against the backend
/// before it is served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Freshness {
    /// Revalidate a cached copy with a version-conditional read so the caller
    /// observes the current backend state (the default for coordination reads).
    Latest,
    /// Serve a cached copy as-is when present, skipping revalidation. For a
    /// caller that only needs a compare-and-swap seed and validates the version
    /// itself (the single read-write commit, ADR-030): a stale copy costs at
    /// most a failed CAS and a reload, never correctness.
    AllowStale,
}

/// Wraps a backend with read-through / write-through caching of coordination
/// objects, revalidated by the object's backend version.
#[derive(Clone)]
pub struct ObjectCache {
    backend: Arc<dyn Backend>,
    cache: Arc<Cache<CacheEntry>>,
}

impl ObjectCache {
    /// Creates the object facade reading and writing through `backend`, over
    /// `cache`.
    pub fn new(backend: Arc<dyn Backend>, cache: &SharedCache) -> Self {
        ObjectCache {
            backend,
            cache: cache.handle(),
        }
    }

    /// Reads the object at `key`, using the cache when possible.
    ///
    /// With [`Freshness::Latest`] a cached entry is revalidated with a
    /// version-conditional read: the backend returns the body only if it
    /// changed, otherwise [`backend::BackendError::Precondition`] meaning "your
    /// cached copy is still current". The backend version changes on every
    /// content write, which is exactly when the cache must be invalidated
    /// (ADR-023). With [`Freshness::AllowStale`] a cached entry is served
    /// as-is, skipping the revalidation round-trip.
    pub async fn read(&self, key: &str, freshness: Freshness) -> Result<ObjectRead, StorageError> {
        if let Some(CacheEntry::Object(e)) = self.cache.get(key)
            && !e.version.is_unset()
        {
            if matches!(freshness, Freshness::AllowStale) {
                return Ok(ObjectRead {
                    value: e.bytes,
                    version: e.version,
                });
            }
            match self.backend.read_if_modified(key, &e.version).await {
                Ok(r) => return Ok(self.cache_read(key, r)),
                Err(backend::BackendError::Precondition) => {
                    return Ok(ObjectRead {
                        value: e.bytes,
                        version: e.version,
                    });
                }
                Err(err) => return Err(err.into()),
            }
        }

        let r = self.backend.read(key).await?;
        Ok(self.cache_read(key, r))
    }

    /// Returns the cached object for `key` without contacting the backend.
    /// Callers use this only for objects that are immutable once written (e.g. a
    /// finalized transaction log), where a cached copy needs no revalidation.
    pub fn peek(&self, key: &str) -> Option<ObjectRead> {
        match self.cache.get(key)? {
            CacheEntry::Object(e) => Some(ObjectRead {
                value: e.bytes,
                version: e.version,
            }),
            _ => None,
        }
    }

    /// Unconditionally writes and updates the cache.
    pub async fn write(
        &self,
        key: &str,
        value: Arc<[u8]>,
    ) -> Result<backend::Version, StorageError> {
        let v = self.backend.write(key, value.to_vec()).await?;
        self.cache_store(key, value, v.clone());
        Ok(v)
    }

    /// Conditionally writes and updates the cache.
    pub async fn write_if(
        &self,
        key: &str,
        value: Arc<[u8]>,
        expected: &backend::Version,
    ) -> Result<backend::Version, StorageError> {
        let v = self.backend.write_if(key, value.to_vec(), expected).await?;
        self.cache_store(key, value, v.clone());
        Ok(v)
    }

    /// Creates the object if absent and updates the cache.
    pub async fn write_if_not_exists(
        &self,
        key: &str,
        value: Arc<[u8]>,
    ) -> Result<backend::Version, StorageError> {
        let v = self
            .backend
            .write_if_not_exists(key, value.to_vec())
            .await?;
        self.cache_store(key, value, v.clone());
        Ok(v)
    }

    /// Deletes the object and removes it from the cache.
    pub async fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.backend.delete(key).await?;
        self.cache.delete(key);
        Ok(())
    }

    /// Lists object paths under `dir_path`. Goes straight to the backend: a
    /// listing enumerates keys rather than fetching bodies, so there is nothing
    /// to serve from (or store in) the object cache.
    pub async fn list(&self, dir_path: &str) -> Result<Vec<String>, StorageError> {
        Ok(self.backend.list(dir_path).await?)
    }

    /// Caches a freshly-read object body and returns it as an [`ObjectRead`].
    fn cache_read(&self, key: &str, r: backend::ReadReply) -> ObjectRead {
        let bytes: Arc<[u8]> = Arc::from(r.contents);
        self.cache_store(key, bytes.clone(), r.version.clone());
        ObjectRead {
            value: bytes,
            version: r.version,
        }
    }

    /// Caches an object body keyed on its backend version.
    fn cache_store(&self, key: &str, bytes: Arc<[u8]>, version: backend::Version) {
        self.cache
            .set(key, CacheEntry::Object(ObjectEntry { bytes, version }));
    }
}
