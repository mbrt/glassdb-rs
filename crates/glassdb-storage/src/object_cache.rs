//! The coordination-object facade over the decoded object cache (ADR-036).
//!
//! Coordination objects (shards, collection roots, transaction logs) change in
//! place by content CAS. This facade routes their reads and writes through the
//! shared [`CachedStore`], which keys entries by physical path and tracks a
//! local validation watermark per entry. The facade caches raw object bodies
//! (an identity codec). Callers express freshness directly as a [`Requirement`]:
//! [`Requirement::Any`] accepts any cached copy, [`Requirement::AtLeast`]
//! requires a copy validated no earlier than the given [`ValidationTime`] (a
//! version-conditional revalidation otherwise). A "latest" read is
//! `AtLeast(cache.now())`.

use std::sync::Arc;

use glassdb_backend::{self as backend, Backend};

use crate::cached_store::{CachedStore, CasResult, Codec, Requirement, Revision, ValidationTime};
use crate::entry::SharedCache;
use crate::error::StorageError;

/// The result of reading an object from the object cache: its body and backend
/// version (an opaque content-CAS token).
#[derive(Debug, Clone)]
pub struct ObjectRead {
    pub value: Arc<[u8]>,
    pub version: backend::Version,
}

/// The identity codec: coordination bodies are cached as raw bytes, decoded by
/// the typed store above this facade.
struct RawBytes;

impl Codec for RawBytes {
    type Value = Arc<[u8]>;

    fn decode(bytes: &[u8]) -> Result<Arc<[u8]>, StorageError> {
        Ok(Arc::from(bytes))
    }

    fn encode(value: &Arc<[u8]>) -> Result<Vec<u8>, StorageError> {
        Ok(value.to_vec())
    }

    fn size(value: &Arc<[u8]>) -> usize {
        value.len()
    }
}

/// Reads and writes coordination-object bodies through the shared
/// [`CachedStore`], revalidated by the object's content version.
#[derive(Clone)]
pub struct ObjectCache {
    store: CachedStore,
}

impl ObjectCache {
    /// Creates the object facade reading and writing through `backend`, over a
    /// decoded store sized to the same byte budget as `cache`.
    pub fn new(backend: Arc<dyn Backend>, cache: &SharedCache) -> Self {
        ObjectCache {
            store: CachedStore::new(backend, cache.max_size_b()),
        }
    }

    /// The current logical time: a bound `t` such that `AtLeast(t)` forces a
    /// revalidation of every entry validated before now. A "latest" read passes
    /// `AtLeast(self.now())`.
    pub fn now(&self) -> ValidationTime {
        self.store.now()
    }

    /// Reads the object at `key`, using the cache when possible.
    ///
    /// With [`Requirement::AtLeast`] a cached entry older than the bound is
    /// revalidated with a version-conditional read; with [`Requirement::Any`] a
    /// cached entry is served as-is. A nonexistent object surfaces
    /// [`StorageError::NotFound`], as the byte-body callers expect.
    pub async fn read(&self, key: &str, req: Requirement) -> Result<ObjectRead, StorageError> {
        let obs = self.store.read::<RawBytes>(key, req).await?;
        Self::into_read(obs).ok_or(StorageError::NotFound)
    }

    /// Returns the cached object for `key` without contacting the backend.
    /// Callers use this only for objects that are immutable once written (e.g. a
    /// finalized transaction log), where a cached copy needs no revalidation.
    pub fn peek(&self, key: &str) -> Option<ObjectRead> {
        self.store
            .peek::<RawBytes>(key)
            .ok()
            .flatten()
            .and_then(Self::into_read)
    }

    /// Unconditionally writes and updates the cache.
    pub async fn write(
        &self,
        key: &str,
        value: Arc<[u8]>,
    ) -> Result<backend::Version, StorageError> {
        let obs = self.store.write::<RawBytes>(key, Arc::new(value)).await?;
        Ok(Self::version(&obs))
    }

    /// Conditionally writes and updates the cache.
    pub async fn write_if(
        &self,
        key: &str,
        value: Arc<[u8]>,
        expected: &backend::Version,
    ) -> Result<backend::Version, StorageError> {
        let expected = Revision::from_version(expected.clone());
        match self
            .store
            .cas::<RawBytes>(key, Arc::new(value), &expected)
            .await?
        {
            CasResult::Committed(obs) => Ok(Self::version(&obs)),
            CasResult::Conflict => Err(StorageError::Precondition),
        }
    }

    /// Creates the object if absent and updates the cache.
    pub async fn write_if_not_exists(
        &self,
        key: &str,
        value: Arc<[u8]>,
    ) -> Result<backend::Version, StorageError> {
        match self.store.create::<RawBytes>(key, Arc::new(value)).await? {
            CasResult::Committed(obs) => Ok(Self::version(&obs)),
            CasResult::Conflict => Err(StorageError::Precondition),
        }
    }

    /// Deletes the object and installs a validated absence.
    pub async fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.store.delete(key).await
    }

    /// The running count of object bodies the underlying store has transferred
    /// from the backend, sampled around a logical read to detect a body-free
    /// read (a cache hit, possibly after cheap conditional revalidation).
    pub fn body_reads(&self) -> u64 {
        self.store.body_reads()
    }

    /// Lists one page of object paths under `prefix`, bypassing the body cache.
    pub async fn list(
        &self,
        prefix: &str,
        cursor: Option<&backend::ListCursor>,
        limit: backend::ListLimit,
    ) -> Result<backend::ListPage, StorageError> {
        self.store.list(prefix, cursor, limit).await
    }

    /// Extracts a byte-body read from a present observation, or `None` for an
    /// observed absence.
    fn into_read(obs: crate::cached_store::Observation<Arc<[u8]>>) -> Option<ObjectRead> {
        let version = obs
            .revision()
            .map(|r| r.version().clone())
            .unwrap_or_default();
        obs.into_value().map(|value| ObjectRead {
            value: (*value).clone(),
            version,
        })
    }

    /// The backend version of a committed observation.
    fn version(obs: &crate::cached_store::Observation<Arc<[u8]>>) -> backend::Version {
        obs.revision()
            .map(|r| r.version().clone())
            .unwrap_or_default()
    }
}
