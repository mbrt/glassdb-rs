//! Effective-writer resolution over the v2 shard coordination objects
//! (ADR-017/020/024).
//!
//! A key's value lives in the transaction object of whichever transaction last
//! committed it, so a shard entry only points at the *effective writer*: the
//! `current_writer` pointer, plus any committed-but-not-yet-written-back
//! exclusive holder that must be help-forwarded. Resolving that pointer is a
//! coordination concern shared by three
//! consumers with different needs:
//!
//! - the [`Reader`](crate::Reader) materializes the value the writer holds,
//! - the commit algorithm ([`Algo`](crate::Algo)) validates reads by comparing
//!   the observed writer against the current one (ADR-024), and
//! - coordination paths resolve the writer and live holders as one coherent
//!   view before changing an entry.
//!
//! This module owns both projections so all consumers go through one place and
//! none re-implement help-forwarding. The caller's requirement is propagated
//! through the leaf and transaction-state dependencies of the resolution.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use glassdb_data::{CollectionAddress, KeyRef, TxId};
use glassdb_storage::{
    CurrentState, Directory, LeafLocator, LockType, Requirement, ShardEntry, ShardStore,
    StorageError, TxCommitStatus,
};

use crate::algo::{LeafCoverage, ScanMutation, ScanRange};
use crate::error::{TransError, trans_to_storage};
use crate::monitor::{KeyCommitStatus, Monitor, TxFinalStatus};

/// The result of a phantom-safe scan: the live keys in key order, the covered
/// leaves' membership dependencies, and the effective page frontier.
#[derive(Debug, Clone)]
pub struct ScanResult {
    pub keys: Vec<Vec<u8>>,
    pub covered: Vec<LeafCoverage>,
    /// Inclusive validation/locking frontier; `None` means positive infinity.
    pub frontier: Option<Vec<u8>>,
}

/// What is known about the effective writer's value alongside its identity.
///
/// A leaf entry that names the effective writer is authoritative about its own
/// value (ADR-051), so resolution can answer "what did this writer write?"
/// without touching a transaction object.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) enum ResolvedValue {
    /// The leaf names a writer behind the effective one — a help-forwarded
    /// committed holder — so only that holder's transaction object says what it
    /// wrote. A predecessor's bytes are never its value.
    #[default]
    Unresolved,
    /// The effective writer's value lives in its transaction object.
    External,
    /// The effective writer's authoritative value bytes.
    Inline(Arc<[u8]>),
    /// The effective writer deleted the key.
    Tombstone,
}

impl ResolvedValue {
    /// What `current` proves about its own writer's value.
    pub(crate) fn from_current(current: &CurrentState) -> Self {
        match current {
            CurrentState::Absent => ResolvedValue::Unresolved,
            CurrentState::External { .. } => ResolvedValue::External,
            CurrentState::Inline { value, .. } => ResolvedValue::Inline(value.clone()),
            CurrentState::Tombstone { .. } => ResolvedValue::Tombstone,
        }
    }

    /// Reports whether the key exists, or `None` when the value is unresolved
    /// and only the writer's transaction object can say.
    pub(crate) fn exists(&self) -> Option<bool> {
        match self {
            ResolvedValue::Unresolved => None,
            ResolvedValue::External | ResolvedValue::Inline(_) => Some(true),
            ResolvedValue::Tombstone => Some(false),
        }
    }
}

/// The effective writer resolved using transaction-state evidence satisfying a
/// requested freshness requirement.
#[derive(Debug, Clone)]
pub(crate) struct WriterResolution {
    /// The effective committed writer holding the key's value (the MVCC
    /// pointer), or `None` if the key has no committed value.
    pub writer: Option<TxId>,
    /// What the leaf proved about that writer's value.
    pub value: ResolvedValue,
    pub cache_hit: bool,
}

impl Default for WriterResolution {
    fn default() -> Self {
        Self {
            writer: None,
            value: ResolvedValue::Unresolved,
            cache_hit: true,
        }
    }
}

/// One coherent interpretation of an entry's foreign lock holders.
///
/// The effective writer and the holders that remain live are derived from the
/// same transaction-status observation for each holder. They must not be
/// resolved independently: a holder can commit between two local status reads,
/// producing a predecessor writer paired with an incorrectly removable lock.
#[derive(Debug, Clone, Default)]
pub(crate) struct HolderResolution {
    /// The effective committed writer after help-forwarding a committed
    /// exclusive holder.
    pub(crate) writer: Option<TxId>,
    /// Whether the effective writer deleted the key.
    pub(crate) deleted: bool,
    /// Foreign holders still pending or not yet classifiable.
    pub(crate) pending: Vec<TxId>,
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
    /// Creates a resolver over the shard coordination store and monitor. Key
    /// routing descends the collection's B-link directory through the shared
    /// decoded object store, so unchanged nodes need no body transfer.
    pub fn new(shards: ShardStore, tmon: Monitor) -> Self {
        Resolver {
            dir: Directory::new(shards),
            tmon,
        }
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
    pub async fn live_keys_scan(
        &self,
        collection: &CollectionAddress,
    ) -> Result<ScanResult, StorageError> {
        self.scan_keys(collection, &ScanRange::all(), &[], None, None)
            .await
    }

    /// Resolves one bounded, forward page and its membership dependencies.
    /// `cap` is an optional inclusive validation frontier that prevents a
    /// limited-page recheck from reading beyond the range already protected.
    pub async fn scan_keys(
        &self,
        collection: &CollectionAddress,
        range: &ScanRange,
        overlay: &[ScanMutation],
        own_lock_holder: Option<&TxId>,
        cap: Option<&[u8]>,
    ) -> Result<ScanResult, StorageError> {
        // A transaction scan retains every covered leaf and validates those
        // observations after its validation barrier. Requiring "now" here
        // would only duplicate work; stale execution is safe and retryable.
        self.scan_keys_at(
            collection,
            range,
            overlay,
            own_lock_holder,
            cap,
            Requirement::Any,
        )
        .await
    }

    /// Returns the committed value a resolved writer recorded for `key`.
    pub(crate) async fn committed_value(
        &self,
        key: &KeyRef,
        writer: &TxId,
    ) -> Result<KeyCommitStatus, TransError> {
        self.tmon.committed_value(key, writer).await
    }

    /// Resolves a page and all dependent transaction states against one shared
    /// freshness requirement.
    pub(crate) async fn scan_keys_at(
        &self,
        collection: &CollectionAddress,
        range: &ScanRange,
        overlay: &[ScanMutation],
        own_lock_holder: Option<&TxId>,
        cap: Option<&[u8]>,
        requirement: Requirement,
    ) -> Result<ScanResult, StorageError> {
        let prefix = collection.physical_prefix();
        let Some(mut loc) = self
            .dir
            .first_leaf_at(&prefix, &range.start, requirement)
            .await
            .map_err(|error| error.classify_collection_absence(collection))?
        else {
            return Err(StorageError::NotFound.classify_collection_absence(collection));
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
                        let key_ref = KeyRef::new(collection.clone(), &key);
                        match leaf.lookup(&key) {
                            None => false,
                            Some(entry) => self
                                .entry_exists_at(&key_ref, entry, own_lock_holder, requirement)
                                .await
                                .map_err(trans_to_storage)?,
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
            let Some(next) = self
                .dir
                .next_leaf(&prefix, &loc, requirement)
                .await
                .map_err(|error| error.classify_collection_absence(collection))?
            else {
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
        collection: &CollectionAddress,
        range: &ScanRange,
        frontier: Option<&[u8]>,
        own_lock_holder: Option<&TxId>,
        requirement: Requirement,
    ) -> Result<Vec<LeafCoverage>, StorageError> {
        let prefix = collection.physical_prefix();
        if range.is_empty() {
            if self
                .dir
                .first_leaf_at(&prefix, &range.start, requirement)
                .await
                .map_err(|error| error.classify_collection_absence(collection))?
                .is_none()
            {
                return Err(StorageError::NotFound.classify_collection_absence(collection));
            }
            return Ok(Vec::new());
        }

        let leaves = self
            .dir
            .leaves_through(&prefix, &range.start, frontier, requirement)
            .await
            .map_err(|error| error.classify_collection_absence(collection))?;
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
        if let Some(node) = node {
            self.ensure_collection_live(node).await?;
        }
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

    /// Resolves effective writers against one shared freshness requirement.
    pub(crate) async fn effective_writers(
        &self,
        keys: &[KeyRef],
        requirement: Requirement,
    ) -> Result<HashMap<KeyRef, Option<TxId>>, StorageError> {
        // Route the keys to their leaves and load each once; the key→leaf
        // grouping (and its deterministic order) lives in `group_keys_by_leaf`.
        // Each key rides along as its own payload so it can key the output map.
        // Collect first so the returned future does not close over a borrowing
        // iterator (which would not be higher-ranked / `Send` when the commit
        // path spawns this resolution).
        let items: Vec<(KeyRef, KeyRef)> = keys.iter().map(|k| (k.clone(), k.clone())).collect();
        let groups = self.dir.group_keys_by_leaf(items, requirement).await?;

        let mut out = HashMap::with_capacity(keys.len());
        for group in &groups {
            if let Some(node) = group.node() {
                self.ensure_collection_live(node).await?;
            }
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
                    .resolve_writer_at(key, leaf.and_then(|leaf| leaf.lookup(raw_key)), requirement)
                    .await
                    .map_err(trans_to_storage)?;
                out.insert(key.clone(), resolved.writer);
            }
        }
        Ok(out)
    }

    /// Resolves `key` to its owning leaf and effective writer, returning
    /// the located leaf alongside. An absent key resolves to no writer.
    ///
    /// `requirement` is forwarded to the descent: the single read-write commit
    /// passes [`Requirement::Any`] so its eligibility check reuses the leaf
    /// the read already cached, without a revalidation round-trip; a stale copy is
    /// caught by the commit-install's version-conditional CAS (ADR-030).
    pub(crate) async fn resolve_key(
        &self,
        key: &KeyRef,
        requirement: Requirement,
    ) -> Result<(WriterResolution, LeafLocator), TransError> {
        let loc = self.locate_key(key, requirement).await?;
        let writer = self
            .resolve_writer_at(key, Self::entry_at(&loc, key.key())?, requirement)
            .await?;
        Ok((writer, loc))
    }

    /// Resolves `key` to its owning leaf and a coherent view of its effective
    /// writer and foreign holders.
    ///
    /// Mutation paths use this form when the located entry will be changed:
    /// routing and holder classification remain one logical observation, with
    /// no discarded writer-only status lookup.
    pub(crate) async fn resolve_key_holders(
        &self,
        key: &KeyRef,
        own_lock_holder: Option<&TxId>,
        requirement: Requirement,
    ) -> Result<(HolderResolution, LeafLocator), TransError> {
        let loc = self.locate_key(key, requirement).await?;
        let holders = self
            .resolve_holders_at(
                key,
                Self::entry_at(&loc, key.key())?,
                own_lock_holder,
                requirement,
            )
            .await?;
        Ok((holders, loc))
    }

    async fn locate_key(
        &self,
        key: &KeyRef,
        requirement: Requirement,
    ) -> Result<LeafLocator, TransError> {
        let prefix = key.collection().physical_prefix();
        // Interior index nodes are served from cache (ADR-031 hot-path
        // invariant); only the terminal leaf honors the caller's `requirement`
        // (the fast path's `Any` reuse, else a current lower bound), so the root `_r`
        // is not revalidated on every commit.
        let loc = self
            .dir
            .leaf_for_fresh(&prefix, key.key(), Requirement::Any, requirement)
            .await
            .map_err(|error| error.classify_collection_absence(key.collection()))?;
        if let Some(node) = loc.node() {
            self.ensure_collection_live(node)
                .await
                .map_err(TransError::from)?;
        }
        Ok(loc)
    }

    fn entry_at<'a>(
        loc: &'a LeafLocator,
        raw_key: &[u8],
    ) -> Result<Option<&'a ShardEntry>, TransError> {
        let leaf = loc
            .node()
            .map(|node| {
                node.as_leaf()
                    .ok_or_else(|| TransError::other("descent resolved a non-leaf node"))
            })
            .transpose()?;
        Ok(leaf.and_then(|leaf| leaf.lookup(raw_key)))
    }

    async fn ensure_collection_live(
        &self,
        node: &glassdb_storage::Node,
    ) -> Result<(), StorageError> {
        let Some(holder) = node.collection_delete_intent() else {
            return Ok(());
        };
        match self
            .tmon
            .await_tx_final(holder)
            .await
            .map_err(trans_to_storage)?
        {
            TxFinalStatus::Committed => Err(StorageError::StaleCollection),
            TxFinalStatus::Aborted => Ok(()),
        }
    }

    /// Resolves the effective writer named by `entry`, using Monitor evidence
    /// satisfying `requirement` to help-forward a committed exclusive holder.
    pub(crate) async fn resolve_writer_at(
        &self,
        key: &KeyRef,
        entry: Option<&ShardEntry>,
        requirement: Requirement,
    ) -> Result<WriterResolution, TransError> {
        let Some(entry) = entry else {
            return Ok(WriterResolution::default());
        };
        let exclusive = matches!(entry.lock_type, LockType::Write | LockType::Create);
        let mut writer = entry.current.writer().cloned();
        let mut value = ResolvedValue::from_current(&entry.current);
        let mut cache_hit = true;
        if exclusive && entry.locked_by.len() > 1 {
            return Err(TransError::other(
                "exclusive shard entry has more than one holder",
            ));
        }
        if exclusive && let Some(holder) = entry.locked_by.first() {
            let (status, status_cache_hit) = self
                .tmon
                .tx_status_at_with_cache(holder, requirement)
                .await?;
            cache_hit &= status_cache_hit;
            if status == TxCommitStatus::Ok {
                let cv = self
                    .tmon
                    .committed_value_at(key, holder, requirement)
                    .await?;
                cache_hit &= cv.cache_hit;
                if cv.status == TxCommitStatus::Ok && !cv.value.not_written {
                    writer = Some(holder.clone());
                    // The leaf still records the predecessor, so its state says
                    // nothing about the holder we just moved ahead to.
                    value = ResolvedValue::Unresolved;
                }
            }
        }
        Ok(WriterResolution {
            writer,
            value,
            cache_hit,
        })
    }

    /// Resolves both an entry's effective writer and its live foreign holders
    /// from one status observation per holder.
    ///
    /// Coordination paths use this richer view because publishing a resolved
    /// writer and removing a finalized lock are one logical decision. Pure
    /// reads keep using [`Resolver::resolve_writer_at`], which need not inspect
    /// shared read-lock holders that cannot affect the value.
    pub(crate) async fn resolve_holders_at(
        &self,
        key: &KeyRef,
        entry: Option<&ShardEntry>,
        own_lock_holder: Option<&TxId>,
        requirement: Requirement,
    ) -> Result<HolderResolution, TransError> {
        let Some(entry) = entry else {
            return Ok(HolderResolution::default());
        };
        let exclusive = matches!(entry.lock_type, LockType::Write | LockType::Create);
        if exclusive && entry.locked_by.len() > 1 {
            return Err(TransError::other(
                "exclusive shard entry has more than one holder",
            ));
        }

        let mut writer = entry.current.writer().cloned();
        let mut deleted = entry.current.is_tombstone();
        let mut pending = Vec::new();
        for holder in &entry.locked_by {
            if Some(holder) == own_lock_holder {
                continue;
            }
            let status = self.tmon.tx_status_at(holder, requirement).await?;
            match status {
                TxCommitStatus::Ok if exclusive => {
                    // Committed is terminal, so reading the value after this
                    // observation cannot cross back into a live-holder state.
                    let value = self
                        .tmon
                        .committed_value_at(key, holder, requirement)
                        .await?;
                    if value.status == TxCommitStatus::Ok && !value.value.not_written {
                        writer = Some(holder.clone());
                        deleted = value.value.deleted;
                    }
                }
                TxCommitStatus::Pending | TxCommitStatus::Unknown => {
                    pending.push(holder.clone());
                }
                TxCommitStatus::Ok | TxCommitStatus::Aborted => {}
            }
        }
        Ok(HolderResolution {
            writer,
            deleted,
            pending,
        })
    }

    /// Reports whether `entry` names a live key at `requirement`.
    async fn entry_exists_at(
        &self,
        key: &KeyRef,
        entry: &ShardEntry,
        own_lock_holder: Option<&TxId>,
        requirement: Requirement,
    ) -> Result<bool, TransError> {
        let resolved = if own_lock_holder.is_some_and(|id| {
            matches!(entry.lock_type, LockType::Write | LockType::Create)
                && entry.locked_by.iter().any(|holder| holder == id)
        }) {
            // Our own exclusive hold means no foreign holder can be ahead of
            // the recorded state, so the entry alone answers.
            WriterResolution {
                writer: entry.current.writer().cloned(),
                value: ResolvedValue::from_current(&entry.current),
                cache_hit: true,
            }
        } else {
            self.resolve_writer_at(key, Some(entry), requirement)
                .await?
        };
        let Some(writer) = resolved.writer else {
            return Ok(false);
        };
        if let Some(exists) = resolved.value.exists() {
            return Ok(exists);
        }
        let value = self
            .tmon
            .committed_value_at(key, &writer, requirement)
            .await?;
        Ok(value.status == TxCommitStatus::Ok && !value.value.not_written && !value.value.deleted)
    }
}

fn in_scan_window(range: &ScanRange, key: &[u8], cap: Option<&[u8]>) -> bool {
    range.contains(key) && cap.is_none_or(|cap| key <= cap)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{OpLog, RecordingBackend};
    use glassdb_concurr::{Background, RetryConfig};
    use glassdb_data::{CollectionId, paths};
    use glassdb_storage::{
        CachedStore, CurrentState, Node, Shard, TLogger, Timeline, TxLog, TxWrite,
    };

    use crate::reader::Reader;

    const DB: &str = "db";
    const COLL: &str = "db/_c/0000000000000000000000";

    fn collection() -> CollectionAddress {
        CollectionAddress::root(DB)
    }

    fn key_ref(key: &[u8]) -> KeyRef {
        KeyRef::new(collection(), key)
    }

    fn missing_collection() -> CollectionAddress {
        CollectionAddress::new(DB, CollectionId::from_slice(&[1; 16]).unwrap())
    }

    // A resolver over `backend` with its own fresh cache, so it starts cold,
    // paired with the monitor backing it (a clone, sharing its caches) so a test
    // can commit holder values the resolver then help-forwards. The returned
    // `Background` must be kept alive for the monitor's lifetime.
    async fn resolver_over(
        backend: Arc<dyn Backend>,
    ) -> (Resolver, Monitor, Timeline, Arc<Background>) {
        let timeline = Timeline::new();
        let objects = CachedStore::new(backend, 1 << 20, timeline.clone(), None);
        let tl = TLogger::new(objects.clone(), DB);
        let bg = Arc::new(Background::new());
        let mon = Monitor::new(tl, timeline.clone(), Arc::downgrade(&bg));
        let shards = ShardStore::new(objects);
        shards
            .create_root(COLL, &Node::leaf(Shard::new()))
            .await
            .unwrap();
        (Resolver::new(shards, mon.clone()), mon, timeline, bg)
    }

    struct TestStore {
        shards: ShardStore,
        timeline: Timeline,
    }

    impl std::ops::Deref for TestStore {
        type Target = ShardStore;

        fn deref(&self) -> &Self::Target {
            &self.shards
        }
    }

    async fn store_over(backend: Arc<dyn Backend>) -> TestStore {
        let timeline = Timeline::new();
        let shards = ShardStore::new(CachedStore::new(backend, 1 << 20, timeline.clone(), None));
        shards
            .create_root(COLL, &Node::leaf(Shard::new()))
            .await
            .unwrap();
        TestStore { shards, timeline }
    }

    async fn effective_writer(resolver: &Resolver, key: &KeyRef) -> Option<TxId> {
        resolver
            .resolve_key(key, Requirement::Any)
            .await
            .unwrap()
            .0
            .writer
    }

    // Installs a committed pointer for `key` directly in the collection's leaf
    // `_r` (no lock holders), so the entry resolves to `writer` regardless of
    // whether that writer recorded a live value or tombstone.
    async fn seed_writer(store: &TestStore, key: &[u8], writer: &TxId, deleted: bool) {
        let path = paths::tree_root(COLL);
        let loaded = store
            .load_leaf(&path, Requirement::AtLeast(store.timeline.now()))
            .await
            .unwrap();
        let mut entries: BTreeMap<Vec<u8>, ShardEntry> = loaded
            .entries
            .entries()
            .cloned()
            .map(|e| (e.key.clone(), e))
            .collect();
        let current = if deleted {
            CurrentState::Tombstone {
                writer: writer.clone(),
            }
        } else {
            CurrentState::External {
                writer: writer.clone(),
            }
        };
        entries.insert(
            key.to_vec(),
            ShardEntry {
                current,
                ..ShardEntry::new(key)
            },
        );
        let new_shard = Shard::from_entries(entries.into_values());
        assert!(
            store
                .store_leaf(&path, &new_shard, &loaded.locks, &loaded.observation,)
                .await
                .unwrap()
        );
    }

    // Installs an inline committed value for `key` directly in the leaf (no lock
    // holders): the ADR-051 state a reader must serve without any other object.
    async fn seed_inline(store: &TestStore, key: &[u8], writer: &TxId, value: &[u8]) {
        seed_entry(
            store,
            key,
            ShardEntry {
                current: CurrentState::Inline {
                    writer: writer.clone(),
                    value: Arc::from(value),
                },
                ..ShardEntry::new(key)
            },
        )
        .await;
    }

    // Adds `holder` as the entry's exclusive lock holder, keeping whatever
    // current value the entry already records.
    async fn seed_hold(store: &TestStore, key: &[u8], holder: &TxId) {
        let existing = store
            .load_leaf(
                &paths::tree_root(COLL),
                Requirement::AtLeast(store.timeline.now()),
            )
            .await
            .unwrap();
        let mut entry = existing.entries.lookup(key).cloned().unwrap();
        entry.lock_type = LockType::Write;
        entry.locked_by = vec![holder.clone()];
        seed_entry(store, key, entry).await;
    }

    // Replaces `key`'s entry in the collection's leaf `_r` with `entry`.
    async fn seed_entry(store: &TestStore, key: &[u8], entry: ShardEntry) {
        let path = paths::tree_root(COLL);
        let loaded = store
            .load_leaf(&path, Requirement::AtLeast(store.timeline.now()))
            .await
            .unwrap();
        let mut entries: BTreeMap<Vec<u8>, ShardEntry> = loaded
            .entries
            .entries()
            .cloned()
            .map(|e| (e.key.clone(), e))
            .collect();
        entries.insert(key.to_vec(), entry);
        let new_shard = Shard::from_entries(entries.into_values());
        assert!(
            store
                .store_leaf(&path, &new_shard, &loaded.locks, &loaded.observation)
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
            key: key_ref(key),
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
    async fn seed_locked(store: &TestStore, key: &[u8], holder: &TxId) {
        let path = paths::tree_root(COLL);
        let loaded = store
            .load_leaf(&path, Requirement::AtLeast(store.timeline.now()))
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
                lock_type: LockType::Write,
                locked_by: vec![holder.clone()],
                ..ShardEntry::new(key)
            },
        );
        let new_shard = Shard::from_entries(entries.into_values());
        assert!(
            store
                .store_leaf(&path, &new_shard, &loaded.locks, &loaded.observation,)
                .await
                .unwrap()
        );
    }

    fn count_tx_reads(log: &OpLog) -> usize {
        log.lock()
            .unwrap()
            .iter()
            .filter(|r| (r.op == "read" || r.op == "read_if_modified") && r.path.contains("/_t/"))
            .count()
    }

    fn count_shard_reads(log: &OpLog) -> usize {
        log.lock()
            .unwrap()
            .iter()
            .filter(|r| {
                (r.op == "read" || r.op == "read_if_modified")
                    && (r.path.contains("/_n/") || r.path.ends_with("/_r"))
            })
            .count()
    }

    // Writer and liveness must describe the same holder state. Resolving them
    // in separate passes could instead pair the pending predecessor with the
    // committed holder's removable lock.
    #[tokio::test]
    async fn holder_resolution_is_coherent_across_commit() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (resolver, monitor, _timeline, _background) = resolver_over(backend).await;
        let key = key_ref(b"key");
        let predecessor = TxId::with_priority(1, b"predecessor");
        let holder = TxId::with_priority(2, b"holder");

        monitor.begin_tx(&holder);
        let mut committed = TxLog::new(holder.clone(), TxCommitStatus::Pending);
        committed.writes = vec![TxWrite {
            key: key.clone(),
            value: Arc::from(b"v".as_slice()),
            deleted: false,
            prev_writer: predecessor.clone(),
        }];
        let entry = ShardEntry {
            lock_type: LockType::Write,
            locked_by: vec![holder.clone()],
            current: CurrentState::External {
                writer: predecessor.clone(),
            },
            ..ShardEntry::new(key.key())
        };

        let pending = resolver
            .resolve_holders_at(&key, Some(&entry), None, Requirement::Any)
            .await
            .unwrap();
        assert_eq!(pending.writer, Some(predecessor));
        assert_eq!(pending.pending, vec![holder.clone()]);

        monitor.commit_tx(committed).await.unwrap();
        assert_eq!(
            monitor.tx_status(&holder).await.unwrap(),
            TxCommitStatus::Ok,
            "the holder committed before the second resolution"
        );

        let committed = resolver
            .resolve_holders_at(&key, Some(&entry), None, Requirement::Any)
            .await
            .unwrap();
        assert_eq!(committed.writer, Some(holder));
        assert!(committed.pending.is_empty());
    }

    #[tokio::test]
    async fn missing_bound_collection_is_classified_during_routing() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (resolver, _monitor, _timeline, _background) = resolver_over(backend).await;
        let collection = missing_collection();
        let key = KeyRef::new(collection.clone(), b"k");
        let range = ScanRange::all();

        assert!(matches!(
            resolver.resolve_key(&key, Requirement::Any).await,
            Err(TransError::Storage(StorageError::StaleCollection))
        ));
        assert!(matches!(
            resolver
                .scan_keys(&collection, &range, &[], None, None)
                .await,
            Err(StorageError::StaleCollection)
        ));
        assert!(matches!(
            resolver
                .scan_coverage(&collection, &range, None, None, Requirement::Any)
                .await,
            Err(StorageError::StaleCollection)
        ));
        assert!(matches!(
            resolver.effective_writers(&[key], Requirement::Any).await,
            Err(StorageError::StaleCollection)
        ));
    }

    // With split deferred every key lives in the collection's single leaf `_r`
    // (ADR-031), so a batch of keys resolves against that one leaf: a live
    // pointer, a tombstone, and an absent key each resolve to the right writer.
    #[tokio::test]
    async fn effective_writers_resolve_against_the_single_leaf() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());

        // Seed through a separate cache so the resolver-under-test starts cold.
        let seed_store = store_over(backend.clone()).await;
        let a = b"apple".to_vec();
        let b = b"mango".to_vec();
        let c = b"lonely".to_vec();
        let live = TxId::with_priority(1, b"live");
        let tomb = TxId::with_priority(2, b"tomb");

        seed_writer(&seed_store, &a, &live, false).await;
        seed_writer(&seed_store, &b, &tomb, true).await;
        // `c` is deliberately left absent.

        let (resolver, _mon, _timeline, _bg) = resolver_over(backend.clone()).await;

        let pa = key_ref(&a);
        let pb = key_ref(&b);
        let pc = key_ref(&c);
        let out = resolver
            .effective_writers(&[pa.clone(), pb.clone(), pc.clone()], Requirement::Any)
            .await
            .unwrap();

        assert_eq!(out.get(&pa).cloned(), Some(Some(live)));
        assert_eq!(
            out.get(&pb).cloned(),
            Some(Some(tomb)),
            "a tombstone still has a writer"
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
        let seed_store = store_over(backend.clone()).await;
        let key = b"rmw-key";
        let writer = TxId::with_priority(1, b"w");
        seed_writer(&seed_store, key, &writer, false).await;

        let (resolver, _mon, timeline, _bg) = resolver_over(backend.clone()).await;
        let key_path = key_ref(key);

        // Warm the resolver's own cache with one cold load.
        resolver
            .resolve_key(&key_path, Requirement::Any)
            .await
            .unwrap();
        log.lock().unwrap().clear();

        // `Any` serves the cached shard: no backend read at all.
        let (resolved, _) = resolver
            .resolve_key(&key_path, Requirement::Any)
            .await
            .unwrap();
        assert_eq!(resolved.writer, Some(writer.clone()), "still resolves");
        assert_eq!(
            count_shard_reads(&log),
            0,
            "Any reuses the cached shard without a backend read"
        );

        // A current bound revalidates the cached shard with one conditional read.
        log.lock().unwrap().clear();
        resolver
            .resolve_key(&key_path, Requirement::AtLeast(timeline.now()))
            .await
            .unwrap();
        assert_eq!(
            count_shard_reads(&log),
            1,
            "a current bound revalidates the cached shard"
        );
    }

    // The singular resolve mirrors the batched one for one key: live and
    // tombstone pointers yield their writer, while an absent key yields none.
    #[tokio::test]
    async fn effective_writer_resolves_single_key() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let seed_store = store_over(backend.clone()).await;
        let live = TxId::with_priority(1, b"live");
        let dead = TxId::with_priority(2, b"dead");
        seed_writer(&seed_store, b"live-key", &live, false).await;
        seed_writer(&seed_store, b"dead-key", &dead, true).await;

        let (resolver, _mon, _timeline, _bg) = resolver_over(backend).await;
        assert_eq!(
            effective_writer(&resolver, &key_ref(b"live-key")).await,
            Some(live)
        );
        assert_eq!(
            effective_writer(&resolver, &key_ref(b"dead-key")).await,
            Some(dead)
        );
        assert_eq!(
            effective_writer(&resolver, &key_ref(b"missing")).await,
            None
        );
    }

    // A committed exclusive holder that has not yet published its `current_writer`
    // pointer is help-forwarded: writer identity is resolved independently of
    // whether the committed value is live or a tombstone.
    #[tokio::test]
    async fn effective_writer_help_forwards_committed_holder() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let seed_store = store_over(backend.clone()).await;
        let (resolver, mon, _timeline, _bg) = resolver_over(backend).await;

        let live = TxId::with_priority(1, b"live");
        commit_value(&mon, b"live-key", &live, false).await;
        seed_locked(&seed_store, b"live-key", &live).await;

        let tomb = TxId::with_priority(2, b"tomb");
        commit_value(&mon, b"dead-key", &tomb, true).await;
        seed_locked(&seed_store, b"dead-key", &tomb).await;

        assert_eq!(
            effective_writer(&resolver, &key_ref(b"live-key")).await,
            Some(live),
            "a committed exclusive holder is help-forwarded as the writer"
        );
        assert_eq!(
            effective_writer(&resolver, &key_ref(b"dead-key")).await,
            Some(tomb),
            "a help-forwarded tombstone still resolves its writer"
        );
    }

    // ADR-051: an inline value in the leaf is the writer's own authoritative
    // evidence, so the read serves it without opening a transaction object.
    #[tokio::test]
    async fn inline_value_reads_without_a_transaction_object() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);

        let seed_store = store_over(backend.clone()).await;
        let writer = TxId::with_priority(1, b"inline");
        seed_inline(&seed_store, b"k", &writer, b"hello").await;

        let (resolver, _mon, timeline, _bg) = resolver_over(backend).await;
        let reader = Reader::new(resolver, timeline, RetryConfig::default());
        log.lock().unwrap().clear();

        let out = reader.read(&key_ref(b"k"), Duration::MAX).await.unwrap();
        let value = out.value.expect("inline value is present");
        assert_eq!(value.value.as_ref(), b"hello");
        assert_eq!(value.version.writer, writer);
        assert_eq!(out.last_writer, Some(writer));
        assert_eq!(
            count_tx_reads(&log),
            0,
            "an inline value needs no transaction object"
        );
    }

    // A tombstone is equally authoritative: absence is decided from the leaf.
    #[tokio::test]
    async fn tombstone_reads_absent_without_a_transaction_object() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);

        let seed_store = store_over(backend.clone()).await;
        let writer = TxId::with_priority(1, b"tomb");
        seed_writer(&seed_store, b"k", &writer, true).await;

        let (resolver, _mon, timeline, _bg) = resolver_over(backend).await;
        let reader = Reader::new(resolver, timeline, RetryConfig::default());
        log.lock().unwrap().clear();

        let out = reader.read(&key_ref(b"k"), Duration::MAX).await.unwrap();
        assert!(out.value.is_none());
        assert_eq!(out.last_writer, Some(writer));
        assert_eq!(
            count_tx_reads(&log),
            0,
            "a tombstone needs no transaction object"
        );
    }

    // A committed exclusive holder is ahead of the recorded inline predecessor,
    // so the predecessor's bytes must never be served as the holder's value.
    #[tokio::test]
    async fn help_forwarded_holder_over_an_inline_predecessor_serves_its_own_value() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let seed_store = store_over(backend.clone()).await;
        let (resolver, mon, timeline, _bg) = resolver_over(backend).await;

        let old = TxId::with_priority(1, b"old");
        seed_inline(&seed_store, b"k", &old, b"old-value").await;
        let new = TxId::with_priority(2, b"new");
        commit_value(&mon, b"k", &new, false).await;
        seed_hold(&seed_store, b"k", &new).await;

        let reader = Reader::new(resolver, timeline, RetryConfig::default());
        let out = reader.read(&key_ref(b"k"), Duration::MAX).await.unwrap();
        let value = out.value.expect("the holder committed a live value");
        // `commit_value` writes b"v"; the stale inline b"old-value" must not win.
        assert_eq!(value.value.as_ref(), b"v");
        assert_eq!(value.version.writer, new);
    }

    // Read validation is a writer-identity comparison, not a value comparison:
    // two versions holding identical bytes are still distinct versions.
    #[tokio::test]
    async fn equal_inline_bytes_from_different_writers_are_distinct_versions() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let seed_store = store_over(backend.clone()).await;
        let (resolver, _mon, timeline, _bg) = resolver_over(backend).await;
        let reader = Reader::new(resolver, timeline, RetryConfig::default());

        let first = TxId::with_priority(1, b"first");
        seed_inline(&seed_store, b"k", &first, b"same").await;
        let before = reader.read(&key_ref(b"k"), Duration::MAX).await.unwrap();

        let second = TxId::with_priority(2, b"second");
        seed_inline(&seed_store, b"k", &second, b"same").await;
        let after = reader
            .read(&key_ref(b"k"), Duration::from_secs(0))
            .await
            .unwrap();

        assert_eq!(before.value.as_ref().unwrap().value.as_ref(), b"same");
        assert_eq!(after.value.as_ref().unwrap().value.as_ref(), b"same");
        assert_ne!(
            before.value.unwrap().version,
            after.value.unwrap().version,
            "equal bytes under different writers are different versions"
        );
    }
}
