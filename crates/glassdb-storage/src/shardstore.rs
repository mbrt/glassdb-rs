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
use glassdb_data::shard::{ShardKeys, group_by_owning_shard, shard_index};

use crate::error::StorageError;
use crate::object_cache::ObjectCache;
use crate::root::CollectionRoot;
use crate::shard::{Shard, ShardEntry};

/// Reads and compare-and-swaps shard and collection-root objects through the
/// [`ObjectCache`], the v2 coordination substrate.
#[derive(Clone)]
pub struct ShardStore {
    objects: ObjectCache,
}

/// One shard returned by [`ShardStore::load_by_keys`]: the loaded `shard` and
/// its `version`, together with the raw `keys` (and their payloads) routed to
/// it. Callers look each key up in the single loaded `shard`.
pub struct LoadedShard<T> {
    pub shard: Shard,
    pub version: Option<backend::Version>,
    pub keys: ShardKeys<T>,
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

    /// Loads several shards concurrently, one `(Shard, version)` per target in
    /// the given order (a missing shard yields the empty shard with no version,
    /// as [`load_shard`] does). Callers pass distinct `(prefix, idx)` targets so
    /// each shard is fetched once; the batched effective-writer resolution
    /// groups a transaction's read set by shard before calling this.
    ///
    /// [`load_shard`]: Self::load_shard
    pub async fn load_shards(
        &self,
        targets: &[(String, u32)],
    ) -> Result<Vec<(Shard, Option<backend::Version>)>, StorageError> {
        let loads = targets
            .iter()
            .map(|(prefix, idx)| self.load_shard(prefix, *idx));
        futures::future::join_all(loads).await.into_iter().collect()
    }

    /// Routes `(key_path, payload)` items to their owning shards and loads those
    /// shards once each (concurrently), returning one entry per touched shard —
    /// its loaded `(Shard, version)` together with the raw keys and payloads that
    /// landed in it, in deterministic shard order. The batched form the read and
    /// GC paths use: they attach whatever per-key data they need to check, then
    /// look each key up in its shard's single loaded copy. Key→shard routing
    /// lives entirely in [`group_by_owning_shard`].
    pub async fn load_by_keys<P: AsRef<str>, T>(
        &self,
        items: impl IntoIterator<Item = (P, T)>,
    ) -> Result<Vec<LoadedShard<T>>, StorageError> {
        let groups = group_by_owning_shard(items)
            .map_err(|e| StorageError::with_source("grouping keys by shard", e))?;
        let targets: Vec<(String, u32)> = groups.keys().cloned().collect();
        let shards = self.load_shards(&targets).await?;
        Ok(shards
            .into_iter()
            .zip(groups.into_values())
            .map(|((shard, version), keys)| LoadedShard {
                shard,
                version,
                keys,
            })
            .collect())
    }

    /// Loads the coordination entry for `key_path`, or `None` if the key has no
    /// entry yet. The singular counterpart to [`load_by_keys`]: it routes the one
    /// key to its owning shard and looks it up, so the caller never computes a
    /// shard index. The read path uses this before resolving a single key.
    pub async fn load_entry(&self, key_path: &str) -> Result<Option<ShardEntry>, StorageError> {
        let (prefix, raw_key) = paths::split_key(key_path)
            .map_err(|e| StorageError::with_source("parsing key path", e))?;
        let (shard, _) = self.load_shard(&prefix, shard_index(&raw_key)).await?;
        Ok(shard.lookup(&raw_key).cloned())
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
    use glassdb_data::TxId;
    use glassdb_data::shard::shard_index;

    use crate::entry::SharedCache;
    use crate::lock::LockType;
    use crate::shard::ShardEntry;

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

    // A batch load returns one result per target, in order: an existing shard
    // with its version and a missing shard as the empty shard with no version.
    // Each distinct target is fetched exactly once (one backend read apiece).
    #[tokio::test]
    async fn load_shards_returns_aligned_results() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        let present = shard_index(b"present");
        let absent = shard_index(b"absent");
        assert_ne!(present, absent, "test keys must map to distinct shards");

        // Seed only the `present` shard through a separate cache so the reader
        // below starts cold and full-reads it.
        assert!(
            store_over(backend.clone())
                .store_shard(COLL, present, &Shard::new(), None)
                .await
                .unwrap()
        );

        let reader = store_over(backend.clone());
        let out = reader
            .load_shards(&[(COLL.to_string(), present), (COLL.to_string(), absent)])
            .await
            .unwrap();

        assert_eq!(out.len(), 2, "one result per target, in order");
        assert!(out[0].1.is_some(), "the seeded shard carries a version");
        assert!(
            out[1].1.is_none() && out[1].0.is_empty(),
            "a missing shard is the empty shard with no version"
        );
        // Each distinct shard was fetched exactly once.
        assert_eq!(count(&log, "read"), 2);
    }

    // load_by_keys routes each key to its owning shard, loads it once, and
    // returns the raw keys and payloads that landed there alongside the loaded
    // copy — so a caller looks each key up in the single fetched shard.
    #[tokio::test]
    async fn load_by_keys_routes_and_aligns_payloads() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let present = b"present".to_vec();
        let absent = b"absent".to_vec();
        assert_ne!(
            shard_index(&present),
            shard_index(&absent),
            "test keys must map to distinct shards"
        );

        let tid = TxId::from_bytes(vec![1, 2, 3]);
        let entry = ShardEntry {
            key: present.clone(),
            lock_type: LockType::None,
            locked_by: Vec::new(),
            current_writer: Some(tid),
            deleted: false,
        };
        store
            .store_shard(
                COLL,
                shard_index(&present),
                &Shard::from_entries([entry]),
                None,
            )
            .await
            .unwrap();

        let out = store
            .load_by_keys([
                (paths::from_key(COLL, &present), "P"),
                (paths::from_key(COLL, &absent), "A"),
            ])
            .await
            .unwrap();

        assert_eq!(out.len(), 2, "one entry per distinct owning shard");

        let present_group = out.iter().find(|g| g.keys[0].1 == "P").unwrap();
        assert_eq!(present_group.keys, vec![(present.clone(), "P")]);
        assert!(
            present_group.shard.lookup(&present).is_some(),
            "the seeded entry is visible in its loaded shard"
        );
        assert!(
            present_group.version.is_some(),
            "seeded shard carries a version"
        );

        let absent_group = out.iter().find(|g| g.keys[0].1 == "A").unwrap();
        assert!(
            absent_group.shard.is_empty() && absent_group.version.is_none(),
            "a missing shard is the empty shard with no version"
        );
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
