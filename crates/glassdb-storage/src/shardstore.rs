//! Compare-and-swap I/O for the v2 coordination objects (ADR-017/018).
//!
//! Shards (`{prefix}/_s/<i>`) and collection roots (`{prefix}/_i`) are the v2
//! coordination units. Each store is an unconditional create-if-absent or a
//! version-conditional compare-and-swap — the only coordination primitive v2
//! needs (ADR-023).
//!
//! Reads and writes go through the [`ObjectCache`], so a hot shard/root
//! revalidates with a version-conditional `read_if_modified` and serves its
//! cached body without a re-transfer. The backend version changes on every
//! content write — exactly when a cached copy must be invalidated — so this is
//! safe.

use std::sync::Arc;

use glassdb_backend as backend;
use glassdb_data::paths;

use crate::error::StorageError;
use crate::object_cache::ObjectCache;
use crate::root::CollectionRoot;
use crate::shard::Shard;

/// Reads and compare-and-swaps shard and collection-root objects through the
/// [`ObjectCache`], the v2 coordination substrate.
#[derive(Clone)]
pub struct ShardStore {
    objects: ObjectCache,
}

impl ShardStore {
    /// Creates a shard store that reads and compare-and-swaps through `objects`.
    pub fn new(objects: ObjectCache) -> Self {
        ShardStore { objects }
    }

    /// Loads shard `idx` under `prefix`. Returns the empty shard with no version
    /// when it does not exist yet (shards are created lazily on first lock).
    pub async fn load_shard(
        &self,
        prefix: &str,
        idx: u32,
    ) -> Result<(Shard, Option<backend::Version>), StorageError> {
        match self.objects.read(&paths::from_shard(prefix, idx)).await {
            Ok(r) => Ok((Shard::decode(&r.value)?, Some(r.version))),
            Err(StorageError::NotFound) => Ok((Shard::new(), None)),
            Err(e) => Err(e),
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
        let body: Arc<[u8]> = Arc::from(shard.encode());
        let res = match expected {
            Some(v) => self.objects.write_if(&path, body, v).await,
            None => self.objects.write_if_not_exists(&path, body).await,
        };
        match res {
            Ok(_) => Ok(true),
            Err(StorageError::Precondition | StorageError::NotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Loads the collection root under `prefix`, or [`StorageError::NotFound`] if
    /// the collection does not exist.
    pub async fn load_root(
        &self,
        prefix: &str,
    ) -> Result<(CollectionRoot, backend::Version), StorageError> {
        let r = self.objects.read(&paths::collection_info(prefix)).await?;
        Ok((CollectionRoot::decode(&r.value)?, r.version))
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
            .objects
            .write_if(
                &paths::collection_info(prefix),
                Arc::from(root.encode()),
                expected,
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(StorageError::Precondition | StorageError::NotFound) => Ok(false),
            Err(e) => Err(e),
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
            .objects
            .write_if_not_exists(&paths::collection_info(prefix), Arc::from(root.encode()))
            .await
        {
            Ok(_) | Err(StorageError::Precondition) => Ok(()),
            Err(e) => Err(e),
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
            .objects
            .write_if_not_exists(&paths::collection_info(prefix), Arc::from(root.encode()))
            .await
        {
            Ok(_) => Ok(true),
            Err(StorageError::Precondition) => Ok(false),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{OpLog, RecordingBackend};
    use glassdb_data::shard::shard_index;

    use crate::entry::SharedCache;

    const COLL: &str = "coll";

    fn store_over(backend: Arc<dyn Backend>) -> ShardStore {
        ShardStore::new(ObjectCache::new(backend, &SharedCache::new(1 << 20)))
    }

    fn count(log: &OpLog, op: &str) -> usize {
        log.lock().unwrap().iter().filter(|r| r.op == op).count()
    }

    // A shard object exists in the backend. A cold cache full-fetches it on the
    // first load; a subsequent hot load revalidates with `read_if_modified`
    // instead of re-fetching, and returns the same version (ADR-023).
    #[tokio::test]
    async fn hot_reload_revalidates_without_full_read() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        let idx = shard_index(b"k");

        // Seed the shard through a separate cache so the reader below starts cold.
        assert!(
            store_over(backend.clone())
                .store_shard(COLL, idx, &Shard::new(), None)
                .await
                .unwrap()
        );

        let reader = store_over(backend.clone());
        let (_, v1) = reader.load_shard(COLL, idx).await.unwrap();
        assert_eq!(count(&log, "read"), 1, "cold load full-reads");
        assert_eq!(count(&log, "read_if_modified"), 0);

        let (_, v2) = reader.load_shard(COLL, idx).await.unwrap();
        assert_eq!(count(&log, "read"), 1, "hot load must not full-read");
        assert_eq!(
            count(&log, "read_if_modified"),
            1,
            "hot load revalidates conditionally"
        );
        assert_eq!(v1, v2, "unchanged shard keeps its version");
    }

    // A write-through store updates the cache, so a load after a CAS observes the
    // freshly written content and its new version.
    #[tokio::test]
    async fn store_is_visible_to_next_load() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let idx = shard_index(b"k");

        assert!(
            store
                .store_shard(COLL, idx, &Shard::new(), None)
                .await
                .unwrap()
        );
        let (_, v1) = store.load_shard(COLL, idx).await.unwrap();
        let v1 = v1.expect("shard exists after create");

        // CAS a new generation over the loaded version, then confirm the next
        // load reflects it.
        assert!(
            store
                .store_shard(COLL, idx, &Shard::new(), Some(&v1))
                .await
                .unwrap()
        );
        let (_, v2) = store.load_shard(COLL, idx).await.unwrap();
        assert_ne!(Some(v1), v2, "a CAS store advances the version");
    }
}
