//! Read-through / write-through caching over a backend. Ported from the Go
//! `internal/storage/global.go`, adapted to the slimmed content-CAS-only trait
//! (ADR-023): cache revalidation is keyed on the object's backend version
//! (ETag/generation), not on a writer tag.

use std::sync::Arc;

use glassdb_backend::{self as backend, Backend};
use glassdb_data::TxId;

use crate::error::StorageError;
use crate::local::{Local, MAX_STALENESS};
use crate::version::Version;

/// The result of reading a value from global storage.
#[derive(Debug, Clone)]
pub struct GlobalRead {
    pub value: Arc<[u8]>,
    pub version: Version,
}

/// Wraps a backend with a local cache, performing read-through and
/// write-through caching of objects.
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
    ///
    /// A cached, not-outdated entry with a known backend version is revalidated
    /// with a version-conditional read: the backend returns the body only if it
    /// changed, otherwise [`backend::BackendError::Precondition`] meaning "your
    /// cached copy is still current". The ETag changes on every content write,
    /// which is exactly when the cache must be invalidated (ADR-023).
    pub async fn read(&self, key: &str) -> Result<GlobalRead, StorageError> {
        if let Some(e) = self.local.read(key, MAX_STALENESS)
            && !e.outdated
            && !e.version.b.is_unset()
        {
            match self.backend.read_if_modified(key, &e.version.b).await {
                Ok(r) => return Ok(self.cache_read(key, r)),
                Err(backend::BackendError::Precondition) => {
                    return Ok(GlobalRead {
                        value: e.value,
                        version: e.version,
                    });
                }
                Err(err) => return Err(err.into()),
            }
        }

        let r = self.backend.read(key).await?;
        Ok(self.cache_read(key, r))
    }

    /// Unconditionally writes and updates the cache.
    pub async fn write(
        &self,
        key: &str,
        value: Arc<[u8]>,
    ) -> Result<backend::Version, StorageError> {
        let v = self.backend.write(key, value.to_vec()).await?;
        self.cache_write(key, value, v.clone());
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
        self.cache_write(key, value, v.clone());
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
        self.cache_write(key, value, v.clone());
        Ok(v)
    }

    /// Deletes the object and removes it from the cache.
    pub async fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.backend.delete(key).await?;
        self.local.delete(key);
        Ok(())
    }

    /// Lists object paths under `dir_path`.
    pub async fn list(&self, dir_path: &str) -> Result<Vec<String>, StorageError> {
        Ok(self.backend.list(dir_path).await?)
    }

    /// Caches a freshly-read object body and returns it as a [`GlobalRead`].
    fn cache_read(&self, key: &str, r: backend::ReadReply) -> GlobalRead {
        let version = Version {
            b: r.version,
            writer: TxId::default(),
        };
        let contents: Arc<[u8]> = Arc::from(r.contents);
        self.local.write(key, contents.clone(), version.clone());
        GlobalRead {
            value: contents,
            version,
        }
    }

    /// Caches a freshly-written object body keyed on its new backend version.
    fn cache_write(&self, key: &str, value: Arc<[u8]>, v: backend::Version) {
        self.local.write(
            key,
            value,
            Version {
                b: v,
                writer: TxId::default(),
            },
        );
    }
}
