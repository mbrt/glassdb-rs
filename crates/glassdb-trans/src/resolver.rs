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
//! place and none re-implement help-forwarding. Callers supply one requirement
//! requirement, which is propagated through the leaf and transaction-object
//! dependencies used by that resolve.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use glassdb_data::{TxId, paths};
use glassdb_storage::{
    Directory, LeafLocator, LockType, Requirement, ShardEntry, ShardStore, StorageError,
    TxCommitStatus,
};

use crate::algo::{LeafCoverage, ScanMutation, ScanRange};
use crate::error::{TransError, trans_to_storage};
use crate::monitor::{KeyCommitStatus, Monitor};

/// The result of a phantom-safe scan: the live keys in key order, the covered
/// leaves' membership dependencies, and the effective page frontier.
#[derive(Debug, Clone)]
pub struct ScanResult {
    pub keys: Vec<Vec<u8>>,
    pub covered: Vec<LeafCoverage>,
    /// Inclusive validation/locking frontier; `None` means positive infinity.
    pub frontier: Option<Vec<u8>>,
}

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
/// `pending` holders. The single read-write fast path consumes the whole view
/// to decide eligibility (a live `pending` holder is a conflict it cannot
/// take). `pending_writer` is set only for the single exclusive holder whose
/// commit can supersede the resolved value without first changing the leaf.
#[derive(Debug, Clone)]
pub(crate) struct HolderResolution {
    pub writer: Option<TxId>,
    pub deleted: bool,
    pub pending: Vec<TxId>,
    pub pending_writer: Option<TxId>,
    pub cache_hit: bool,
}

impl Default for HolderResolution {
    fn default() -> Self {
        Self {
            writer: None,
            deleted: false,
            pending: Vec::new(),
            pending_writer: None,
            cache_hit: true,
        }
    }
}

/// Resolves a key's shard entry to its effective committed writer, help-
/// forwarding committed holders through the [`Monitor`]. The single home for
/// "who currently holds this key's value", shared by the read and commit paths.
#[derive(Clone)]
pub struct Resolver {
    dir: Directory,
    tmon: Monitor,
}

impl Resolver {
    /// Creates a resolver over the shard coordination store and the monitor. Key
    /// routing descends the collection's B-link directory through the shared
    /// decoded object store, so unchanged nodes need no body transfer.
    pub fn new(shards: ShardStore, tmon: Monitor) -> Self {
        Resolver {
            dir: Directory::new(shards),
            tmon,
        }
    }

    /// Returns the current instant on the shared object store's clock.
    pub fn now(&self) -> glassdb_storage::Instant {
        self.dir.now()
    }

    /// Scans `prefix` left-to-right and returns both the raw keys that currently
    /// exist (committed and not tombstoned, in key order) and the membership
    /// dependencies of every leaf the scan covered (ADR-032 phantom prevention).
    ///
    /// Committed holders are help-forwarded, so a key whose writer committed but
    /// has not yet published its `current_writer` pointer (write-back is
    /// asynchronous) still lists. The scan follows the leaf right-sibling chain
    /// ([`Directory::leaves`]), so an in-progress split is absorbed rather than
    /// dropping or duplicating keys. Membership versions and pending membership
    /// holders detect creates/deletes without conflicting with value overwrites;
    /// a changed covered-leaf set falls back to logical page validation.
    pub async fn live_keys_scan(&self, prefix: &str) -> Result<ScanResult, StorageError> {
        self.scan_keys(prefix, &ScanRange::all(), &[], None, None)
            .await
    }

    /// Resolves one bounded, forward page and its membership dependencies.
    /// `cap` is an optional inclusive validation frontier that prevents a
    /// limited-page recheck from reading beyond the range already protected.
    pub async fn scan_keys(
        &self,
        prefix: &str,
        range: &ScanRange,
        overlay: &[ScanMutation],
        own_lock_holder: Option<&TxId>,
        cap: Option<&[u8]>,
    ) -> Result<ScanResult, StorageError> {
        self.scan_keys_at(
            prefix,
            range,
            overlay,
            own_lock_holder,
            cap,
            Requirement::AtLeast(self.dir.now()),
        )
        .await
    }

    /// Builds a freshness requirement against the resolver's shared object
    /// store clock.
    pub(crate) fn requirement_within(&self, max_staleness: Duration) -> Requirement {
        self.dir.requirement_within(max_staleness)
    }

    /// Returns the committed value a resolved writer recorded for `key`.
    pub(crate) async fn committed_value(
        &self,
        key: &str,
        writer: &TxId,
    ) -> Result<KeyCommitStatus, TransError> {
        self.tmon.committed_value(key, writer).await
    }

    /// Resolves a page and all dependent transaction states against one shared
    /// freshness requirement.
    pub(crate) async fn scan_keys_at(
        &self,
        prefix: &str,
        range: &ScanRange,
        overlay: &[ScanMutation],
        own_lock_holder: Option<&TxId>,
        cap: Option<&[u8]>,
        requirement: Requirement,
    ) -> Result<ScanResult, StorageError> {
        let Some(mut loc) = self
            .dir
            .first_leaf_at(prefix, &range.start, requirement)
            .await?
        else {
            return Err(StorageError::NotFound);
        };

        if range.is_empty() {
            return Ok(ScanResult {
                keys: Vec::new(),
                covered: Vec::new(),
                frontier: Some(range.start.clone()),
            });
        }

        let mut overlay: BTreeMap<Vec<u8>, bool> = overlay
            .iter()
            .filter(|mutation| in_scan_window(range, &mutation.key, cap))
            .map(|mutation| (mutation.key.clone(), mutation.present))
            .collect();
        let mut keys = Vec::new();
        let mut covered = Vec::new();

        loop {
            let coverage = self
                .leaf_coverage(&loc, own_lock_holder, requirement)
                .await?;
            let node = loc
                .node()
                .ok_or_else(|| StorageError::other("existing leaf has no decoded node"))?;
            let leaf = node
                .as_leaf()
                .ok_or_else(|| StorageError::other("leaf scan reached a non-leaf node"))?;
            let mut candidates: BTreeSet<Vec<u8>> = leaf
                .entries()
                .filter(|entry| in_scan_window(range, &entry.key, cap))
                .map(|entry| entry.key.clone())
                .collect();
            let overlay_keys: Vec<Vec<u8>> = overlay
                .keys()
                .take_while(|key| node.owns(key))
                .cloned()
                .collect();
            let leaf_overlay: BTreeMap<Vec<u8>, bool> = overlay_keys
                .into_iter()
                .map(|key| {
                    let present = overlay
                        .remove(&key)
                        .expect("overlay key was selected from the map");
                    (key, present)
                })
                .collect();
            candidates.extend(leaf_overlay.keys().cloned());

            for key in candidates {
                let present = match leaf_overlay.get(key.as_slice()) {
                    Some(present) => *present,
                    None => {
                        let key_path = paths::from_key(prefix, &key);
                        match leaf.lookup(&key) {
                            None => false,
                            Some(entry)
                                if !matches!(
                                    entry.lock_type,
                                    LockType::Write | LockType::Create
                                ) =>
                            {
                                Resolved {
                                    writer: entry.current_writer.clone(),
                                    deleted: entry.deleted,
                                }
                                .token()
                                .is_some()
                            }
                            Some(entry) => {
                                let resolved = self
                                    .resolve_holders_at(
                                        &key_path,
                                        entry,
                                        own_lock_holder,
                                        requirement,
                                    )
                                    .await
                                    .map_err(trans_to_storage)?;
                                Resolved {
                                    writer: resolved.writer,
                                    deleted: resolved.deleted,
                                }
                                .token()
                                .is_some()
                            }
                        }
                    }
                };
                if !present {
                    continue;
                }
                keys.push(key);
                if range.limit.is_some_and(|limit| keys.len() == limit) {
                    covered.push(coverage);
                    return Ok(ScanResult {
                        frontier: keys.last().cloned(),
                        keys,
                        covered,
                    });
                }
            }
            covered.push(coverage);

            let target = cap.or(range.end.as_deref());
            if target.is_some_and(|target| node.owns(target)) {
                break;
            }
            let Some(next) = self.dir.next_leaf(prefix, &loc, requirement).await? else {
                break;
            };
            loc = next;
        }

        Ok(ScanResult {
            keys,
            covered,
            frontier: cap.map(<[u8]>::to_vec).or_else(|| range.end.clone()),
        })
    }

    /// Loads only a scan's physical validation dependencies, without resolving
    /// the leaf entries themselves.
    pub(crate) async fn scan_coverage(
        &self,
        prefix: &str,
        range: &ScanRange,
        frontier: Option<&[u8]>,
        own_lock_holder: Option<&TxId>,
        requirement: Requirement,
    ) -> Result<Vec<LeafCoverage>, StorageError> {
        if range.is_empty() {
            if self
                .dir
                .first_leaf_at(prefix, &range.start, requirement)
                .await?
                .is_none()
            {
                return Err(StorageError::NotFound);
            }
            return Ok(Vec::new());
        }

        let leaves = self
            .dir
            .leaves_through(prefix, &range.start, frontier, requirement)
            .await?;
        let mut covered = Vec::with_capacity(leaves.len());
        for leaf in leaves {
            covered.push(
                self.leaf_coverage(&leaf, own_lock_holder, requirement)
                    .await?,
            );
        }
        Ok(covered)
    }

    async fn leaf_coverage(
        &self,
        loc: &LeafLocator,
        own_lock_holder: Option<&TxId>,
        requirement: Requirement,
    ) -> Result<LeafCoverage, StorageError> {
        let mut pending_membership = Vec::new();
        let node = loc.node();
        if let Some(node) = node
            && node.membership_lock().lock_type() == LockType::Write
        {
            for holder in node.membership_lock().holders() {
                if own_lock_holder == Some(holder) {
                    continue;
                }
                let status = self
                    .tmon
                    .tx_status_at(holder, requirement)
                    .await
                    .map_err(trans_to_storage)?;
                if status == TxCommitStatus::Pending {
                    pending_membership.push(holder.clone());
                }
            }
        }
        pending_membership.sort();
        Ok(LeafCoverage {
            path: loc.path.as_str().into(),
            membership_version: node.map_or(0, |node| node.membership_version()),
            pending_membership,
            observation: loc.observation.clone(),
        })
    }

    /// Returns the effective committed writer of every `key` (the validation
    /// tokens): `Some(writer)` if the key currently exists, `None` if it is
    /// absent or tombstoned. Keys are routed to their owning leaves by descent
    /// so each touched leaf is loaded once, then every key in it is resolved
    /// against the one loaded copy — this is the batched form the commit path
    /// validates against.
    #[cfg(test)]
    pub(crate) async fn effective_writers(
        &self,
        keys: &[Arc<str>],
    ) -> Result<HashMap<Arc<str>, Option<TxId>>, StorageError> {
        self.effective_writers_at(keys, Requirement::AtLeast(self.dir.now()))
            .await
    }

    /// Resolves effective writers against one shared freshness requirement.
    pub(crate) async fn effective_writers_at(
        &self,
        keys: &[Arc<str>],
        requirement: Requirement,
    ) -> Result<HashMap<Arc<str>, Option<TxId>>, StorageError> {
        // Route the keys to their leaves and load each once; the key→leaf
        // grouping (and its deterministic order) lives in `group_keys_by_leaf`.
        // Each key rides along as its own payload so it can key the output map.
        // Collect first so the returned future does not close over a borrowing
        // iterator (which would not be higher-ranked / `Send` when the commit
        // path spawns this resolution).
        let items: Vec<(Arc<str>, Arc<str>)> =
            keys.iter().map(|k| (k.clone(), k.clone())).collect();
        let groups = self.dir.group_keys_by_leaf(items, requirement).await?;

        let mut out = HashMap::with_capacity(keys.len());
        for group in &groups {
            let leaf = group
                .node()
                .map(|node| {
                    node.as_leaf().ok_or_else(|| {
                        StorageError::other("descent grouped keys under a non-leaf node")
                    })
                })
                .transpose()?;
            for (raw_key, key) in &group.keys {
                let resolved = self
                    .resolve_entry_at(
                        key,
                        leaf.and_then(|leaf| leaf.lookup(raw_key)),
                        None,
                        requirement,
                    )
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
    #[cfg(test)]
    async fn effective_writer(&self, key: &str) -> Result<Option<TxId>, StorageError> {
        let (prefix, raw_key) =
            paths::split_key(key).map_err(|e| StorageError::with_source("parsing key path", e))?;
        let loc = self
            .dir
            .leaf_for(&prefix, &raw_key, Requirement::AtLeast(self.dir.now()))
            .await?;
        let leaf = loc
            .node()
            .map(|node| {
                node.as_leaf()
                    .ok_or_else(|| StorageError::other("descent resolved a non-leaf node"))
            })
            .transpose()?;
        let resolved = self
            .resolve_entry_at(
                key,
                leaf.and_then(|leaf| leaf.lookup(&raw_key)),
                None,
                Requirement::AtLeast(self.dir.now()),
            )
            .await
            .map_err(trans_to_storage)?;
        Ok(resolved.token())
    }

    /// Resolves `key_path` to its owning leaf and interprets that entry's
    /// holders (help-forwarding committed ones, collecting the live pending
    /// ones) into a [`HolderResolution`], returning the located leaf alongside.
    /// Unlike [`effective_writer`](Self::effective_writer) it exposes the full
    /// view — including the `pending` conflicts — so the single read-write fast
    /// path can decide eligibility for itself without the resolver embedding that
    /// policy. An absent key resolves to an empty view.
    ///
    /// `requirement` is forwarded to the descent: the single read-write commit
    /// passes [`Requirement::Any`] so its eligibility check reuses the leaf
    /// the read already cached, without a revalidation round-trip; a stale copy is
    /// caught by the commit-install's version-conditional CAS (ADR-030).
    pub(crate) async fn resolve_key(
        &self,
        key_path: &str,
        requirement: Requirement,
    ) -> Result<(HolderResolution, LeafLocator), TransError> {
        let (prefix, raw_key) = paths::split_key(key_path)
            .map_err(|e| TransError::with_source("parsing key path", e))?;
        // Interior index nodes are served from cache (ADR-031 hot-path
        // invariant); only the terminal leaf honors the caller's `requirement`
        // (the fast path's `Any` reuse, else a current lower bound), so the root `_i`
        // is not revalidated on every commit.
        let loc = self
            .dir
            .leaf_for_fresh(&prefix, &raw_key, Requirement::Any, requirement)
            .await?;
        let leaf = loc
            .node()
            .map(|node| {
                node.as_leaf()
                    .ok_or_else(|| TransError::other("descent resolved a non-leaf node"))
            })
            .transpose()?;
        let holders = match leaf.and_then(|leaf| leaf.lookup(&raw_key)) {
            Some(entry) => {
                self.resolve_holders_at(key_path, entry, None, requirement)
                    .await?
            }
            None => HolderResolution::default(),
        };
        Ok((holders, loc))
    }

    /// Interprets `entry`'s holders against transaction status — the step shared
    /// by read resolution and lock acquisition (the locker): help-forward a
    /// committed exclusive holder's value (one that committed but has not yet
    /// published its `current_writer` pointer), drop aborted/unknown holders,
    /// and collect the live pending ones. `own_lock_holder` identifies the
    /// caller's own lock, which is never treated as a foreign holder. Only an
    /// exclusive (write/create) entry
    /// help-forwards; a read-locked entry's holders never change the value but
    /// are still classified so a writer can wound-wait them. `key_path` is the
    /// full storage path of the key, used to fetch a help-forwarded writer's
    /// value.
    pub(crate) async fn resolve_holders(
        &self,
        key_path: &str,
        entry: &ShardEntry,
        own_lock_holder: Option<&TxId>,
    ) -> Result<HolderResolution, TransError> {
        self.resolve_holders_at(
            key_path,
            entry,
            own_lock_holder,
            Requirement::AtLeast(self.dir.now()),
        )
        .await
    }

    /// Interprets holders against one shared freshness requirement.
    pub(crate) async fn resolve_holders_at(
        &self,
        key_path: &str,
        entry: &ShardEntry,
        own_lock_holder: Option<&TxId>,
        requirement: Requirement,
    ) -> Result<HolderResolution, TransError> {
        let exclusive = matches!(entry.lock_type, LockType::Write | LockType::Create);
        let mut writer = entry.current_writer.clone();
        let mut deleted = entry.deleted;
        let mut pending = Vec::new();
        let mut pending_writer = None;
        let mut cache_hit = true;
        if exclusive && entry.locked_by.len() > 1 {
            return Err(TransError::other(
                "exclusive shard entry has more than one holder",
            ));
        }
        for holder in &entry.locked_by {
            if Some(holder) == own_lock_holder {
                continue;
            }
            let (status, status_cache_hit) = self
                .tmon
                .tx_status_at_with_cache(holder, requirement)
                .await?;
            cache_hit &= status_cache_hit;
            match status {
                TxCommitStatus::Ok => {
                    if exclusive {
                        let cv = self
                            .tmon
                            .committed_value_at(key_path, holder, requirement)
                            .await?;
                        if cv.status == TxCommitStatus::Ok && !cv.value.not_written {
                            writer = Some(holder.clone());
                            deleted = cv.value.deleted;
                        }
                    }
                }
                TxCommitStatus::Pending | TxCommitStatus::Unknown => {
                    pending.push(holder.clone());
                    if exclusive {
                        pending_writer = Some(holder.clone());
                    }
                }
                // An aborted lock is dead; drop it.
                _ => {}
            }
        }
        Ok(HolderResolution {
            writer,
            deleted,
            pending,
            pending_writer,
            cache_hit,
        })
    }

    /// Resolves `entry` against the transaction monitor: help-forward a committed
    /// exclusive holder (one that committed but has not yet published its
    /// `current_writer` pointer) and drop aborted/absent holders. `key_path` is
    /// the full storage path of the key, used to fetch the help-forwarded
    /// writer's value. A `None` entry resolves to "no value".
    async fn resolve_entry_at(
        &self,
        key_path: &str,
        entry: Option<&ShardEntry>,
        own_lock_holder: Option<&TxId>,
        requirement: Requirement,
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

        let r = self
            .resolve_holders_at(key_path, e, own_lock_holder, requirement)
            .await?;
        Ok(Resolved {
            writer: r.writer,
            deleted: r.deleted,
        })
    }
}

fn in_scan_window(range: &ScanRange, key: &[u8], cap: Option<&[u8]>) -> bool {
    range.contains(key) && cap.is_none_or(|cap| key <= cap)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{OpLog, RecordingBackend};
    use glassdb_concurr::Background;
    use glassdb_storage::{CachedStore, Shard, TLogger};

    const COLL: &str = "coll";

    // A resolver over `backend` with its own fresh cache, so it starts cold,
    // paired with the monitor backing it (a clone, sharing its caches) so a test
    // can commit holder values the resolver then help-forwards. The returned
    // `Background` must be kept alive for the monitor's lifetime.
    fn resolver_over(backend: Arc<dyn Backend>) -> (Resolver, Monitor, Arc<Background>) {
        let objects = CachedStore::new(backend, 1 << 20);
        let tl = TLogger::new(objects.clone(), COLL);
        let bg = Arc::new(Background::new());
        let mon = Monitor::new(tl, Arc::downgrade(&bg));
        let shards = ShardStore::new(objects);
        (Resolver::new(shards, mon.clone()), mon, bg)
    }

    // Installs a committed pointer for `key` directly in the collection's leaf
    // `_i` (no lock holders), so the entry resolves to `writer` — or to no writer
    // when it is a tombstone.
    async fn seed_writer(store: &ShardStore, key: &[u8], writer: &TxId, deleted: bool) {
        let path = paths::collection_info(COLL);
        let loaded = store
            .load_leaf(&path, Requirement::AtLeast(store.now()))
            .await
            .unwrap();
        let mut entries: BTreeMap<Vec<u8>, ShardEntry> = loaded
            .entries
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
                .store_leaf(
                    &path,
                    &new_shard,
                    &loaded.locks,
                    loaded.kind(),
                    &loaded.observation,
                )
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
        let path = paths::collection_info(COLL);
        let loaded = store
            .load_leaf(&path, Requirement::AtLeast(store.now()))
            .await
            .unwrap();
        let mut entries: BTreeMap<Vec<u8>, ShardEntry> = loaded
            .entries
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
                .store_leaf(
                    &path,
                    &new_shard,
                    &loaded.locks,
                    loaded.kind(),
                    &loaded.observation,
                )
                .await
                .unwrap()
        );
    }

    fn count_shard_reads(log: &OpLog) -> usize {
        log.lock()
            .unwrap()
            .iter()
            .filter(|r| {
                (r.op == "read" || r.op == "read_if_modified")
                    && (r.path.contains("/_n/") || r.path.ends_with("/_i"))
            })
            .count()
    }

    // With split deferred every key lives in the collection's single leaf `_i`
    // (ADR-031), so a batch of keys resolves against that one leaf: a live
    // pointer, a tombstone, and an absent key each resolve to the right token.
    #[tokio::test]
    async fn effective_writers_resolve_against_the_single_leaf() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());

        // Seed through a separate cache so the resolver-under-test starts cold.
        let seed_store = ShardStore::new(CachedStore::new(backend.clone(), 1 << 20));
        let a = b"apple".to_vec();
        let b = b"mango".to_vec();
        let c = b"lonely".to_vec();
        let live = TxId::with_priority(1, b"live");
        let tomb = TxId::with_priority(2, b"tomb");

        seed_writer(&seed_store, &a, &live, false).await;
        seed_writer(&seed_store, &b, &tomb, true).await;
        // `c` is deliberately left absent.

        let (resolver, _mon, _bg) = resolver_over(backend.clone());

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
    }

    // `resolve_key` with `Any` reuses a shard already in the resolver's
    // cache without any backend read, while a current bound revalidates it with one
    // conditional read (ADR-030). This is what lets the single read-write
    // commit's eligibility check reuse the shard the transaction body's read
    // cached, adding no shard load at commit.
    #[tokio::test]
    async fn resolve_key_any_reuses_cached_shard() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);

        // Seed through a separate cache so the resolver-under-test starts cold.
        let seed_store = ShardStore::new(CachedStore::new(backend.clone(), 1 << 20));
        let key = b"rmw-key";
        let writer = TxId::with_priority(1, b"w");
        seed_writer(&seed_store, key, &writer, false).await;

        let (resolver, _mon, _bg) = resolver_over(backend.clone());
        let key_path = paths::from_key(COLL, key);

        // Warm the resolver's own cache with one cold load.
        resolver
            .resolve_key(&key_path, Requirement::AtLeast(resolver.now()))
            .await
            .unwrap();
        log.lock().unwrap().clear();

        // `Any` serves the cached shard: no backend read at all.
        let (holders, _) = resolver
            .resolve_key(&key_path, Requirement::Any)
            .await
            .unwrap();
        assert_eq!(holders.writer, Some(writer.clone()), "still resolves");
        assert_eq!(
            count_shard_reads(&log),
            0,
            "Any reuses the cached shard without a backend read"
        );

        // A current bound revalidates the cached shard with one conditional read.
        log.lock().unwrap().clear();
        resolver
            .resolve_key(&key_path, Requirement::AtLeast(resolver.now()))
            .await
            .unwrap();
        assert_eq!(
            count_shard_reads(&log),
            1,
            "a current bound revalidates the cached shard"
        );
    }

    // The singular resolve mirrors the batched one for one key: a live pointer
    // yields its writer, a tombstone and an absent key yield none.
    #[tokio::test]
    async fn effective_writer_resolves_single_key() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let seed_store = ShardStore::new(CachedStore::new(backend.clone(), 1 << 20));
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
        let seed_store = ShardStore::new(CachedStore::new(backend.clone(), 1 << 20));
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
