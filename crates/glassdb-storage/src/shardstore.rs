//! Fresh, compare-and-swap I/O for the v2 coordination objects (ADR-017/018).
//!
//! Shards (`{prefix}/_s/<i>`) and collection roots (`{prefix}/_i`) are the v2
//! coordination units. Each store is an unconditional create-if-absent or a
//! version-conditional compare-and-swap — the only coordination primitive v2
//! needs (ADR-023).
//!
//! TODO(perf): route shard/root reads back through the [`Global`] cache and let
//! it revalidate with the version-conditional `read_if_modified`
//! (`If-None-Match` → `304 Not Modified`), so a hot unchanged shard revalidates
//! without re-transferring its body. The ETag changes on every content write —
//! exactly when a cached copy must be invalidated — so this is safe; today this
//! store full-fetches every read for simplicity.

use std::sync::Arc;

use glassdb_backend::{self as backend, Backend, BackendError};
use glassdb_data::paths;

use crate::error::StorageError;
use crate::root::CollectionRoot;
use crate::shard::Shard;

/// Reads and compare-and-swaps shard and collection-root objects directly
/// against the backend (no caching), the v2 coordination substrate.
#[derive(Clone)]
pub struct ShardStore {
    backend: Arc<dyn Backend>,
}

impl ShardStore {
    /// Creates a shard store that issues fresh, uncached compare-and-swap I/O
    /// directly against `backend`.
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        ShardStore { backend }
    }

    /// Loads shard `idx` under `prefix`. Returns the empty shard with no version
    /// when it does not exist yet (shards are created lazily on first lock).
    pub async fn load_shard(
        &self,
        prefix: &str,
        idx: u32,
    ) -> Result<(Shard, Option<backend::Version>), StorageError> {
        match self.backend.read(&paths::from_shard(prefix, idx)).await {
            Ok(r) => Ok((Shard::decode(&r.contents)?, Some(r.version))),
            Err(BackendError::NotFound) => Ok((Shard::new(), None)),
            Err(e) => Err(e.into()),
        }
    }

    /// Compare-and-swaps shard `idx`. `expected = None` means create-if-absent.
    /// Returns `false` on a precondition miss (the caller reloads and retries),
    /// `true` on success.
    pub async fn store_shard(
        &self,
        prefix: &str,
        idx: u32,
        shard: &Shard,
        expected: Option<&backend::Version>,
    ) -> Result<bool, StorageError> {
        let path = paths::from_shard(prefix, idx);
        let body = shard.encode();
        let res = match expected {
            Some(v) => self.backend.write_if(&path, body, v).await,
            None => self.backend.write_if_not_exists(&path, body).await,
        };
        match res {
            Ok(_) => Ok(true),
            Err(BackendError::Precondition) | Err(BackendError::NotFound) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Loads the collection root under `prefix`, or [`StorageError::NotFound`] if
    /// the collection does not exist.
    pub async fn load_root(
        &self,
        prefix: &str,
    ) -> Result<(CollectionRoot, backend::Version), StorageError> {
        match self.backend.read(&paths::collection_info(prefix)).await {
            Ok(r) => Ok((CollectionRoot::decode(&r.contents)?, r.version)),
            Err(BackendError::NotFound) => Err(StorageError::NotFound),
            Err(e) => Err(e.into()),
        }
    }

    /// Compare-and-swaps the collection root. Returns `false` on a precondition
    /// miss, `true` on success.
    pub async fn store_root(
        &self,
        prefix: &str,
        root: &CollectionRoot,
        expected: &backend::Version,
    ) -> Result<bool, StorageError> {
        match self
            .backend
            .write_if(&paths::collection_info(prefix), root.encode(), expected)
            .await
        {
            Ok(_) => Ok(true),
            Err(BackendError::Precondition) | Err(BackendError::NotFound) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Creates the collection root if absent, treating a lost create race as
    /// success (ADR-018: existence == the root exists).
    pub async fn create_root_if_absent(
        &self,
        prefix: &str,
        root: &CollectionRoot,
    ) -> Result<(), StorageError> {
        match self
            .backend
            .write_if_not_exists(&paths::collection_info(prefix), root.encode())
            .await
        {
            Ok(_) | Err(BackendError::Precondition) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Creates the collection root if absent, reporting whether this call won the
    /// create (`true`) or found it already present (`false`). The membership lock
    /// path uses this to learn whether it holds the lock it just installed, so it
    /// can fall back to a reload-and-retry when it lost the create race.
    pub async fn create_root(
        &self,
        prefix: &str,
        root: &CollectionRoot,
    ) -> Result<bool, StorageError> {
        match self
            .backend
            .write_if_not_exists(&paths::collection_info(prefix), root.encode())
            .await
        {
            Ok(_) => Ok(true),
            Err(BackendError::Precondition) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}
