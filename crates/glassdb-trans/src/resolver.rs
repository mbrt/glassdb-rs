//! Effective-writer resolution over the v2 shard coordination objects
//! (ADR-017/020/024).
//!
//! A key's value lives in the transaction object of whichever transaction last
//! committed it, so a shard entry only points at the *effective writer*: the
//! `current_writer` pointer, plus any committed-but-not-yet-written-back
//! exclusive holder that must be help-forwarded (aborted/expired holders are
//! dropped). Resolving that pointer is a coordination concern shared by three
//! consumers with different needs:
//!
//! - the [`Reader`](crate::Reader) materializes the value the writer holds,
//! - the commit algorithm ([`Algo`](crate::Algo)) validates reads by comparing
//!   the observed writer against the current one (ADR-024), and
//! - the locker acquires a shard's locks, which first resolves the same holders
//!   (and additionally wound-waits the live pending ones) via
//!   [`resolve_holders`](Resolver::resolve_holders).
//!
//! This module owns that single resolution routine so all three go through one
//! place and none re-implement help-forwarding. It reads shards fresh (no value
//! cache), so every resolve observes the authoritative coordination state.

use std::collections::HashMap;
use std::sync::Arc;

use glassdb_data::{TxId, paths};
use glassdb_storage::{LockType, ShardEntry, ShardStore, StorageError, TxCommitStatus};

use crate::error::{TransError, trans_to_storage};
use crate::monitor::Monitor;

/// A read key grouped for batched resolution: its full storage path (the map
/// key of the returned effective-writer set) paired with its decoded raw key
/// (the shard-entry lookup key).
/// The resolved view of a shard entry, after help-forwarding committed holders.
#[derive(Debug, Clone, Default)]
struct Resolved {
    /// The effective committed writer holding the key's value (the MVCC
    /// pointer), or `None` if the key has no committed value.
    writer: Option<TxId>,
    /// Whether that writer's value for the key is a tombstone.
    deleted: bool,
}

impl Resolved {
    /// The existence-aware validation token: the effective writer iff the key
    /// currently exists (committed and not tombstoned), else `None`. This is the
    /// value a read observes, so it is what optimistic validation compares.
    fn token(self) -> Option<TxId> {
        match self.writer {
            Some(w) if !self.deleted => Some(w),
            _ => None,
        }
    }
}

/// The outcome of interpreting a shard entry's holder set against transaction
/// status: the effective committed writer (after help-forwarding), whether that
/// writer's value is a tombstone, and the foreign holders still live-pending.
/// The read path uses `writer`/`deleted`; the lock path also wound-waits the
/// `pending` holders.
pub(crate) struct HolderResolution {
    pub writer: Option<TxId>,
    pub deleted: bool,
    pub pending: Vec<TxId>,
}

/// Resolves a key's shard entry to its effective committed writer, help-
/// forwarding committed holders through the [`Monitor`]. The single home for
/// "who currently holds this key's value", shared by the read and commit paths.
#[derive(Clone)]
pub struct Resolver {
    shards: ShardStore,
    tmon: Monitor,
}

impl Resolver {
    /// Creates a resolver over the shard coordination store and the monitor. The
    /// `shards` store revalidates shard objects by their backend version
    /// (ADR-023), so a resolve always observes the current coordination state
    /// without re-transferring an unchanged shard's body.
    pub fn new(shards: ShardStore, tmon: Monitor) -> Self {
        Resolver { shards, tmon }
    }

    /// Returns the raw keys that currently exist (committed and not tombstoned)
    /// in the given shards of `prefix`, help-forwarding committed holders so a
    /// key whose writer committed but has not yet published its `current_writer`
    /// pointer (write-back is asynchronous) still lists. The listing path uses
    /// this instead of reading `current_writer` directly, so a collection's keys
    /// are visible immediately on commit rather than only after write-back.
    pub async fn live_keys(
        &self,
        prefix: &str,
        shard_indices: &[u32],
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        let targets: Vec<(String, u32)> = shard_indices
            .iter()
            .map(|&idx| (prefix.to_string(), idx))
            .collect();
        let shards = self.shards.load_shards(&targets).await?;

        let mut keys = Vec::new();
        for (shard, _) in shards {
            for e in shard.entries() {
                let key_path = paths::from_key(prefix, &e.key);
                let resolved = self
                    .resolve_entry(&key_path, Some(e))
                    .await
                    .map_err(trans_to_storage)?;
                if resolved.token().is_some() {
                    keys.push(e.key.clone());
                }
            }
        }
        Ok(keys)
    }

    /// Returns the effective committed writer of every `key` (the validation
    /// tokens): `Some(writer)` if the key currently exists, `None` if it is
    /// absent or tombstoned. Keys are grouped by shard so each touched shard is
    /// loaded once (concurrently), then every key in it is resolved against the
    /// one loaded copy — this is the batched form the commit path validates
    /// against.
    pub(crate) async fn effective_writers(
        &self,
        keys: &[Arc<str>],
    ) -> Result<HashMap<Arc<str>, Option<TxId>>, StorageError> {
        // Route the keys to their shards and load each once; the key→shard
        // grouping (and its deterministic order) lives in `load_by_keys`. Each
        // key rides along as its own payload so it can key the output map.
        let groups = self
            .shards
            .load_by_keys(keys.iter().map(|k| (k.clone(), k.clone())))
            .await?;

        let mut out = HashMap::with_capacity(keys.len());
        for loaded in &groups {
            for (raw_key, key) in &loaded.keys {
                let resolved = self
                    .resolve_entry(key, loaded.shard.lookup(raw_key))
                    .await
                    .map_err(trans_to_storage)?;
                out.insert(key.clone(), resolved.token());
            }
        }
        Ok(out)
    }

    /// Returns the effective committed writer of a single `key`: `Some(writer)`
    /// if the key currently exists, `None` if it is absent or tombstoned. The
    /// singular form the read path uses before materializing the value.
    pub(crate) async fn effective_writer(&self, key: &str) -> Result<Option<TxId>, StorageError> {
        let entry = self.shards.load_entry(key).await?;
        let resolved = self
            .resolve_entry(key, entry.as_ref())
            .await
            .map_err(trans_to_storage)?;
        Ok(resolved.token())
    }

    /// Interprets `entry`'s holders against transaction status — the step shared
    /// by read resolution and lock acquisition (the locker): help-forward a
    /// committed exclusive holder's value (one that committed but has not yet
    /// published its `current_writer` pointer), drop aborted/unknown holders,
    /// and collect the live pending ones. `skip` is the caller's own id, never
    /// treated as a foreign holder. Only an exclusive (write/create) entry
    /// help-forwards; a read-locked entry's holders never change the value but
    /// are still classified so a writer can wound-wait them. `key_path` is the
    /// full storage path of the key, used to fetch a help-forwarded writer's
    /// value.
    pub(crate) async fn resolve_holders(
        &self,
        key_path: &str,
        entry: &ShardEntry,
        skip: Option<&TxId>,
    ) -> Result<HolderResolution, TransError> {
        let exclusive = matches!(entry.lock_type, LockType::Write | LockType::Create);
        let mut writer = entry.current_writer.clone();
        let mut deleted = entry.deleted;
        let mut pending = Vec::new();
        for holder in &entry.locked_by {
            if Some(holder) == skip {
                continue;
            }
            match self.tmon.tx_status(holder).await? {
                TxCommitStatus::Ok => {
                    if exclusive {
                        let cv = self.tmon.committed_value(key_path, holder).await?;
                        if cv.status == TxCommitStatus::Ok && !cv.value.not_written {
                            writer = Some(holder.clone());
                            deleted = cv.value.deleted;
                        }
                    }
                }
                TxCommitStatus::Pending => pending.push(holder.clone()),
                // Aborted / Unknown: the lock is dead; drop it.
                _ => {}
            }
        }
        Ok(HolderResolution {
            writer,
            deleted,
            pending,
        })
    }

    /// Resolves `entry` against the transaction monitor: help-forward a committed
    /// exclusive holder (one that committed but has not yet published its
    /// `current_writer` pointer) and drop aborted/absent holders. `key_path` is
    /// the full storage path of the key, used to fetch the help-forwarded
    /// writer's value. A `None` entry resolves to "no value".
    async fn resolve_entry(
        &self,
        key_path: &str,
        entry: Option<&ShardEntry>,
    ) -> Result<Resolved, TransError> {
        let Some(e) = entry else {
            return Ok(Resolved::default());
        };

        // A read-locked (non-exclusive) entry's holders never change the value,
        // so skip the holder scan entirely — no monitor lookups for a key that
        // only has shared readers.
        if !matches!(e.lock_type, LockType::Write | LockType::Create) {
            return Ok(Resolved {
                writer: e.current_writer.clone(),
                deleted: e.deleted,
            });
        }

        let r = self.resolve_holders(key_path, e, None).await?;
        Ok(Resolved {
            writer: r.writer,
            deleted: r.deleted,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{BTreeMap, BTreeSet};

    use glassdb_data::shard::shard_index;

    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{OpLog, RecordingBackend};
    use glassdb_concurr::Background;
    use glassdb_storage::{ObjectCache, Shard, SharedCache, TLogger, ValueCache};

    const COLL: &str = "coll";

    // A resolver over `backend` with its own fresh cache, so it starts cold,
    // paired with the monitor backing it (a clone, sharing its caches) so a test
    // can commit holder values the resolver then help-forwards. The returned
    // `Background` must be kept alive for the monitor's lifetime.
    fn resolver_over(backend: Arc<dyn Backend>) -> (Resolver, Monitor, Arc<Background>) {
        let cache = SharedCache::new(1 << 20);
        let values = ValueCache::new(&cache);
        let objects = ObjectCache::new(backend, &cache);
        let tl = TLogger::new(objects.clone(), COLL);
        let bg = Arc::new(Background::new());
        let mon = Monitor::new(values, tl, Arc::downgrade(&bg));
        let shards = ShardStore::new(objects);
        (Resolver::new(shards, mon.clone()), mon, bg)
    }

    // Installs a committed pointer for `key` directly in its shard (no lock
    // holders), so the entry resolves to `writer` — or to no writer when it is a
    // tombstone.
    async fn seed_writer(store: &ShardStore, key: &[u8], writer: &TxId, deleted: bool) {
        let idx = shard_index(key);
        let (shard, ver) = store.load_shard(COLL, idx).await.unwrap();
        let mut entries: BTreeMap<Vec<u8>, ShardEntry> = shard
            .entries()
            .cloned()
            .map(|e| (e.key.clone(), e))
            .collect();
        entries.insert(
            key.to_vec(),
            ShardEntry {
                key: key.to_vec(),
                lock_type: LockType::None,
                locked_by: Vec::new(),
                current_writer: Some(writer.clone()),
                deleted,
            },
        );
        let new_shard = Shard::from_entries(entries.into_values());
        assert!(
            store
                .store_shard(COLL, idx, &new_shard, ver.as_ref())
                .await
                .unwrap()
        );
    }

    // Commits `writer`'s value for `key` through the monitor (a tombstone when
    // `deleted`), so a later help-forward of that holder observes it.
    async fn commit_value(mon: &Monitor, key: &[u8], writer: &TxId, deleted: bool) {
        use glassdb_storage::{TxLog, TxWrite};
        mon.begin_tx(writer);
        let mut tl = TxLog::new(writer.clone(), TxCommitStatus::Ok);
        tl.writes = vec![TxWrite {
            path: paths::from_key(COLL, key),
            value: Arc::from(b"v".as_slice()),
            deleted,
            prev_writer: TxId::default(),
        }];
        mon.commit_tx(tl).await.unwrap();
    }

    // Installs a write-locked entry for `key` whose only holder is `holder` and
    // whose `current_writer` pointer is not yet published — the help-forward
    // case: the effective writer must be discovered from the committed holder,
    // not the (stale, empty) pointer.
    async fn seed_locked(store: &ShardStore, key: &[u8], holder: &TxId) {
        let idx = shard_index(key);
        let (shard, ver) = store.load_shard(COLL, idx).await.unwrap();
        let mut entries: BTreeMap<Vec<u8>, ShardEntry> = shard
            .entries()
            .cloned()
            .map(|e| (e.key.clone(), e))
            .collect();
        entries.insert(
            key.to_vec(),
            ShardEntry {
                key: key.to_vec(),
                lock_type: LockType::Write,
                locked_by: vec![holder.clone()],
                current_writer: None,
                deleted: false,
            },
        );
        let new_shard = Shard::from_entries(entries.into_values());
        assert!(
            store
                .store_shard(COLL, idx, &new_shard, ver.as_ref())
                .await
                .unwrap()
        );
    }

    // Two distinct keys that hash to the same shard, found by a small scan (a
    // collision is overwhelmingly likely within a few dozen keys for
    // SHARD_COUNT=1024). Proves the batch collapses same-shard keys to one load.
    fn colliding_keys() -> (Vec<u8>, Vec<u8>) {
        let mut seen: HashMap<u32, Vec<u8>> = HashMap::new();
        for i in 0..100_000u32 {
            let k = format!("key-{i}").into_bytes();
            let idx = shard_index(&k);
            if let Some(prev) = seen.get(&idx) {
                return (prev.clone(), k);
            }
            seen.insert(idx, k);
        }
        panic!("no colliding key pair found");
    }

    fn count_shard_reads(log: &OpLog) -> usize {
        log.lock()
            .unwrap()
            .iter()
            .filter(|r| (r.op == "read" || r.op == "read_if_modified") && r.path.contains("/_s/"))
            .count()
    }

    // Keys are grouped by shard: a live pointer, a tombstone, and an absent key
    // all resolve to the right token, and the batch issues exactly one shard
    // read per distinct shard — not one per key (the colliding pair loads its
    // shard once).
    #[tokio::test]
    async fn effective_writers_batches_by_shard() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);

        // Seed through a separate cache so the resolver-under-test starts cold.
        let seed_store = ShardStore::new(ObjectCache::new(
            backend.clone(),
            &SharedCache::new(1 << 20),
        ));
        let (a, b) = colliding_keys();
        let c = b"lonely".to_vec();
        let live = TxId::with_priority(1, b"live");
        let tomb = TxId::with_priority(2, b"tomb");

        seed_writer(&seed_store, &a, &live, false).await;
        seed_writer(&seed_store, &b, &tomb, true).await;
        // `c` is deliberately left absent.

        let (resolver, _mon, _bg) = resolver_over(backend.clone());
        log.lock().unwrap().clear();

        let pa: Arc<str> = paths::from_key(COLL, &a).into();
        let pb: Arc<str> = paths::from_key(COLL, &b).into();
        let pc: Arc<str> = paths::from_key(COLL, &c).into();
        let out = resolver
            .effective_writers(&[pa.clone(), pb.clone(), pc.clone()])
            .await
            .unwrap();

        assert_eq!(out.get(&pa).cloned(), Some(Some(live)));
        assert_eq!(
            out.get(&pb).cloned(),
            Some(None),
            "a tombstone resolves to no writer"
        );
        assert_eq!(
            out.get(&pc).cloned(),
            Some(None),
            "an absent key resolves to no writer"
        );

        let distinct: BTreeSet<u32> = [&a, &b, &c].iter().map(|k| shard_index(k)).collect();
        assert_eq!(
            count_shard_reads(&log),
            distinct.len(),
            "each distinct shard is loaded once, regardless of keys per shard"
        );
        // The colliding pair means 3 keys span at most 2 shards: a per-key
        // resolve would have read 3 times.
        assert!(distinct.len() < 3, "the colliding pair shares a shard");
    }

    // The singular resolve mirrors the batched one for one key: a live pointer
    // yields its writer, a tombstone and an absent key yield none.
    #[tokio::test]
    async fn effective_writer_resolves_single_key() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let seed_store = ShardStore::new(ObjectCache::new(
            backend.clone(),
            &SharedCache::new(1 << 20),
        ));
        let live = TxId::with_priority(1, b"live");
        seed_writer(&seed_store, b"live-key", &live, false).await;
        seed_writer(
            &seed_store,
            b"dead-key",
            &TxId::with_priority(2, b"dead"),
            true,
        )
        .await;

        let (resolver, _mon, _bg) = resolver_over(backend);
        assert_eq!(
            resolver
                .effective_writer(&paths::from_key(COLL, b"live-key"))
                .await
                .unwrap(),
            Some(live)
        );
        assert_eq!(
            resolver
                .effective_writer(&paths::from_key(COLL, b"dead-key"))
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            resolver
                .effective_writer(&paths::from_key(COLL, b"missing"))
                .await
                .unwrap(),
            None
        );
    }

    // A committed exclusive holder that has not yet published its `current_writer`
    // pointer is help-forwarded: the read path discovers the effective writer
    // (and its tombstone flag) from the holder's committed value, not the stale
    // pointer. This is the branch now shared with the locker via
    // `resolve_holders`.
    #[tokio::test]
    async fn effective_writer_help_forwards_committed_holder() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let seed_store = ShardStore::new(ObjectCache::new(
            backend.clone(),
            &SharedCache::new(1 << 20),
        ));
        let (resolver, mon, _bg) = resolver_over(backend);

        let live = TxId::with_priority(1, b"live");
        commit_value(&mon, b"live-key", &live, false).await;
        seed_locked(&seed_store, b"live-key", &live).await;

        let tomb = TxId::with_priority(2, b"tomb");
        commit_value(&mon, b"dead-key", &tomb, true).await;
        seed_locked(&seed_store, b"dead-key", &tomb).await;

        assert_eq!(
            resolver
                .effective_writer(&paths::from_key(COLL, b"live-key"))
                .await
                .unwrap(),
            Some(live),
            "a committed exclusive holder is help-forwarded as the writer"
        );
        assert_eq!(
            resolver
                .effective_writer(&paths::from_key(COLL, b"dead-key"))
                .await
                .unwrap(),
            None,
            "a help-forwarded tombstone resolves to no writer"
        );
    }
}
