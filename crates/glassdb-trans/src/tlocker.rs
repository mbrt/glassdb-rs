//! Distributed locking over the v2 shard/root coordination objects (ADR-017,
//! ADR-020). Ported in spirit from the Go `internal/trans/tlocker.go`, but
//! re-keyed from per-key objects onto shards.
//!
//! The only coordination primitive is a content compare-and-swap on a shard
//! (`{prefix}/_s/<i>`) or a collection root (`{prefix}/_i`). A transaction
//! groups its accessed keys by shard and locks each shard with a single
//! read-modify-write CAS: load the shard, resolve every touched key's holders
//! (help-forward committed holders, drop aborted ones, wound-wait the live
//! pending ones via the [`Monitor`]), install this transaction's locks, then CAS
//! the shard back. A membership change (create/delete) additionally takes the
//! collection root's write lock. Write-back republishes `current_writer`
//! pointers and releases the locks.
//!
//! Lock acquisition has two modes (ADR-020): the default **parallel** path locks
//! every touched shard concurrently; the **serial** fallback locks them one at a
//! time in ascending shard path order so equal-priority contenders queue on the
//! lowest contended shard and exactly one wins it (first-CAS-wins), guaranteeing
//! progress where the parallel path could livelock.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures::future::join_all;
use glassdb_concurr::{DedupKeySnapshot, RetryConfig, rt, shard::Sharded};
use glassdb_data::shard::shard_index;
use glassdb_data::{TxId, paths};
use glassdb_storage::{
    CollectionRoot, LockType, PathLock, ShardEntry, ShardStore, StorageError, TxCommitStatus,
};

use crate::algo::{Data, WriteOp};
use crate::error::TransError;
use crate::monitor::Monitor;

/// Maximum inner CAS retries on a single shard/root before treating the
/// operation as conflicted and restarting the transaction.
const CAS_RETRIES: usize = 50;

/// Counters for lock operations performed by a [`Locker`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LockStats {
    pub calls: usize,
    pub hits: usize,
    pub retries: usize,
}

#[derive(Default)]
struct Stats {
    n_calls: AtomicU64,
    n_hits: AtomicU64,
    n_retries: AtomicU64,
}

/// Diagnostic snapshot of one transaction's locally-tracked held locks.
///
/// Returned by [`Locker::tx_locks_snapshot`] for operators investigating hangs.
/// The locks list is sorted by path for stable display. In v2 the tracked paths
/// are the shard and root objects the transaction holds (not per-key objects).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxLockSnapshot {
    pub tx_id: TxId,
    pub locks: Vec<PathLock>,
}

/// The lock a transaction wants on a key's shard entry.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Desired {
    Read,
    Put,
    Delete,
}

/// One key's lock intention within a shard.
#[derive(Clone)]
struct KeyIntent {
    /// Raw user key bytes (the shard-entry key).
    pub raw_key: Vec<u8>,
    /// Full storage path of the key (`{prefix}/_k/<b64>`), used to fetch a
    /// help-forwarded writer's value.
    pub key_path: String,
    /// The lock to install.
    pub desired: Desired,
    /// The optimistic-validation token observed at read time, if the key was
    /// read: `Some(token)` validates against the resolved state (`token` is the
    /// effective writer iff the key existed, else `None`); the outer `None`
    /// means a blind write with nothing to validate.
    pub observed: Option<Option<TxId>>,
}

/// The keys a transaction touches in one shard, plus the shard's location.
struct ShardGroup {
    /// Collection prefix owning the shard.
    prefix: String,
    /// Shard index within the collection.
    idx: u32,
    /// Per-key intentions, in ascending raw-key order.
    intents: Vec<KeyIntent>,
}

/// The locks a transaction acquired, returned by [`Locker::lock`] and consumed
/// by [`Locker::write_back`]. Opaque to the caller: it carries the per-shard key
/// groups and the collection prefixes whose root membership lock was taken.
pub struct LockedTx {
    groups: BTreeMap<String, ShardGroup>,
    membership: BTreeSet<String>,
}

/// Groups a transaction's accessed keys by shard. Each key gets one intent
/// carrying its desired lock and, if it was read, the optimistic-validation
/// token observed at read time.
fn build_groups(data: &Data) -> Result<BTreeMap<String, ShardGroup>, TransError> {
    struct Access {
        desired: Desired,
        observed: Option<Option<TxId>>,
    }
    let mut by_path: BTreeMap<String, Access> = BTreeMap::new();
    for w in &data.writes {
        let desired = match w.op {
            WriteOp::Delete => Desired::Delete,
            WriteOp::Put(_) => Desired::Put,
        };
        by_path
            .entry(w.path.to_string())
            .and_modify(|a| a.desired = desired)
            .or_insert(Access {
                desired,
                observed: None,
            });
    }
    for r in &data.reads {
        let token = r.version.as_ref().map(|v| v.last_writer.clone());
        by_path
            .entry(r.path.to_string())
            .and_modify(|a| a.observed = Some(token.clone()))
            .or_insert(Access {
                desired: Desired::Read,
                observed: Some(token),
            });
    }

    let mut groups: BTreeMap<String, ShardGroup> = BTreeMap::new();
    for (path, access) in by_path {
        let (prefix, raw_key) = paths::split_key(&path)
            .map_err(|e| TransError::with_source(format!("parsing key path {path:?}"), e))?;
        let idx = shard_index(&raw_key);
        let shard_path = paths::from_shard(&prefix, idx);
        groups
            .entry(shard_path)
            .or_insert_with(|| ShardGroup {
                prefix,
                idx,
                intents: Vec::new(),
            })
            .intents
            .push(KeyIntent {
                raw_key,
                key_path: path,
                desired: access.desired,
                observed: access.observed,
            });
    }
    for group in groups.values_mut() {
        group.intents.sort_by(|a, b| a.raw_key.cmp(&b.raw_key));
    }
    Ok(groups)
}

/// Outcome of acquiring locks on a single shard.
enum ShardOutcome {
    /// Locked; `membership` is true if the shard saw a create/delete.
    Locked { membership: bool },
    /// The transaction must restart (conflict / lost wound-wait).
    Conflict,
}

/// Acquires and releases distributed locks on the shard/root coordination
/// objects, hiding waits, wound-wait, and CAS retries from callers.
#[derive(Clone)]
pub struct Locker {
    inner: Arc<LockerState>,
}

/// One independent partition of the per-transaction held-lock bookkeeping.
type LockerShard = Mutex<HashMap<TxId, HashMap<String, LockType>>>;

struct LockerState {
    tmon: Monitor,
    shards: ShardStore,
    retry: RetryConfig,
    tlocks: Sharded<LockerShard>,
    stats: Arc<Stats>,
}

impl Locker {
    /// Creates a locker over the shared shard store and the transaction monitor.
    /// `retry` configures the exponential backoff applied between CAS retries on a
    /// contended shard or root, so contention is never busy-retried.
    pub fn new(shards: ShardStore, tmon: Monitor, retry: RetryConfig) -> Self {
        Locker {
            inner: Arc::new(LockerState {
                shards,
                tmon,
                retry,
                tlocks: Sharded::new(|_| Mutex::new(HashMap::new())),
                stats: Arc::new(Stats::default()),
            }),
        }
    }

    /// No-op kept for API compatibility: v2 spawns no per-key dedup owner tasks,
    /// so there is nothing to drain on shutdown.
    pub async fn close(&self) {}

    /// Returns and resets the accumulated lock statistics.
    pub fn stats_and_reset(&self) -> LockStats {
        LockStats {
            calls: self.inner.stats.n_calls.swap(0, Ordering::Relaxed) as usize,
            hits: self.inner.stats.n_hits.swap(0, Ordering::Relaxed) as usize,
            retries: self.inner.stats.n_retries.swap(0, Ordering::Relaxed) as usize,
        }
    }

    /// Per-key dedup state no longer exists in v2 (locks coordinate per shard via
    /// direct CAS), so this diagnostic is always empty. Kept for API stability.
    pub fn dedup_snapshot(&self) -> Vec<DedupKeySnapshot> {
        Vec::new()
    }

    /// Returns one entry per transaction that currently holds any shard/root
    /// lock, with the held paths sorted by path. Output is sorted by transaction
    /// id for stable display.
    pub fn tx_locks_snapshot(&self) -> Vec<TxLockSnapshot> {
        let mut out = Vec::new();
        self.inner.tlocks.each(|shard| {
            let m = shard.lock().unwrap();
            for (tx_id, locks) in m.iter() {
                if locks.is_empty() {
                    continue;
                }
                let mut paths: Vec<PathLock> = locks
                    .iter()
                    .map(|(p, t)| PathLock {
                        path: p.clone(),
                        typ: *t,
                    })
                    .collect();
                paths.sort_by(|a, b| a.path.cmp(&b.path));
                out.push(TxLockSnapshot {
                    tx_id: tx_id.clone(),
                    locks: paths,
                });
            }
        });
        out.sort_by(|a, b| a.tx_id.cmp(&b.tx_id));
        out
    }

    // --- Crate-facing lock protocol ----------------------------------------

    /// Groups the transaction's accessed keys by shard, validates its reads, and
    /// acquires every lock it needs: the touched shards plus the collection roots
    /// for any membership change (create/delete). Returns a [`LockedTx`] handle to
    /// drive write-back on commit, or `None` if the transaction must restart.
    ///
    /// `serial` selects the sorted sequential fallback over the default parallel
    /// path (ADR-020).
    pub async fn lock(
        &self,
        id: &TxId,
        data: &Data,
        serial: bool,
    ) -> Result<Option<LockedTx>, TransError> {
        let groups = build_groups(data)?;
        let Some(membership) = self.lock_shards(id, &groups, serial).await? else {
            return Ok(None);
        };
        for prefix in &membership {
            if !self.lock_root(prefix, id).await? {
                return Ok(None);
            }
        }
        Ok(Some(LockedTx { groups, membership }))
    }

    /// Acquires this transaction's locks across every touched shard. Returns
    /// `Some(prefixes)` listing the collections whose root membership lock must
    /// still be taken (the shards that saw a create/delete), or `None` if the
    /// transaction must restart.
    async fn lock_shards(
        &self,
        id: &TxId,
        groups: &BTreeMap<String, ShardGroup>,
        serial: bool,
    ) -> Result<Option<BTreeSet<String>>, TransError> {
        self.inner.stats.n_calls.fetch_add(1, Ordering::Relaxed);
        // The first lock for this transaction starts the background refresh so a
        // long-lived holder's pending object is written lazily, keeping its
        // lease alive (the tx object is otherwise written only at commit).
        if !groups.is_empty() {
            self.inner.tmon.start_refresh_tx(id);
        }

        let mut membership = BTreeSet::new();
        if serial {
            // Ascending shard-path order is the global lock order: the BTreeMap
            // already iterates sorted by `{prefix}/_s/<idx>`.
            for group in groups.values() {
                match self.lock_shard(id, group).await? {
                    ShardOutcome::Locked { membership: m } => {
                        if m {
                            membership.insert(group.prefix.clone());
                        }
                    }
                    ShardOutcome::Conflict => return Ok(None),
                }
            }
        } else {
            let outcomes = join_all(groups.values().map(|group| self.lock_shard(id, group))).await;
            for (group, outcome) in groups.values().zip(outcomes) {
                match outcome? {
                    ShardOutcome::Locked { membership: m } => {
                        if m {
                            membership.insert(group.prefix.clone());
                        }
                    }
                    ShardOutcome::Conflict => return Ok(None),
                }
            }
        }
        Ok(Some(membership))
    }

    /// Validates reads and installs this transaction's locks in one shard with a
    /// single read-modify-write CAS, retried on contention.
    async fn lock_shard(&self, id: &TxId, group: &ShardGroup) -> Result<ShardOutcome, TransError> {
        let mut backoff = self.inner.retry.backoff();
        for attempt in 0..CAS_RETRIES {
            if attempt > 0 {
                rt::sleep(backoff.next_delay()).await;
                self.inner.stats.n_retries.fetch_add(1, Ordering::Relaxed);
            }
            let (shard, ver) = self
                .inner
                .shards
                .load_shard(&group.prefix, group.idx)
                .await?;
            let mut entries: BTreeMap<Vec<u8>, ShardEntry> = shard
                .entries()
                .cloned()
                .map(|e| (e.key.clone(), e))
                .collect();
            let mut membership = false;
            let mut conflict = false;

            for intent in &group.intents {
                match self
                    .resolve_and_lock(id, intent, entries.get(&intent.raw_key).cloned())
                    .await?
                {
                    Some((entry, m)) => {
                        membership |= m;
                        entries.insert(intent.raw_key.clone(), entry);
                    }
                    None => {
                        conflict = true;
                        break;
                    }
                }
            }
            if conflict {
                return Ok(ShardOutcome::Conflict);
            }

            let new_shard = glassdb_storage::Shard::from_entries(entries.into_values());
            match self
                .inner
                .shards
                .store_shard(&group.prefix, group.idx, &new_shard, ver.as_ref())
                .await
            {
                Ok(true) => {
                    self.record_shard_lock(id, group);
                    return Ok(ShardOutcome::Locked { membership });
                }
                // Precondition: the shard changed under us; reload and retry.
                Ok(false) => {}
                // In-doubt lock CAS: the write may or may not have landed. Lock
                // acquisition is a pre-commit operation with no durable user
                // value yet, and re-installing our own lock over a freshly-read
                // shard is idempotent (we skip ourselves when resolving
                // holders), so recover in place by reloading and retrying rather
                // than surfacing the in-doubt error (ADR-009).
                Err(StorageError::Unavailable(_)) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(ShardOutcome::Conflict)
    }

    /// Resolves the holders of an entry (help-forward committed, drop aborted,
    /// wound-wait the live pending ones), validates the read token, and installs
    /// this transaction's lock. Returns the new entry plus whether the change is
    /// a membership change, or `None` if the transaction must restart.
    async fn resolve_and_lock(
        &self,
        id: &TxId,
        intent: &KeyIntent,
        entry: Option<ShardEntry>,
    ) -> Result<Option<(ShardEntry, bool)>, TransError> {
        let mut e = entry.unwrap_or_else(|| ShardEntry {
            key: intent.raw_key.clone(),
            lock_type: LockType::None,
            locked_by: Vec::new(),
            current_writer: None,
            deleted: false,
        });

        // Resolve existing holders other than us. A committed exclusive holder is
        // help-forwarded (its value becomes the effective one); aborted/missing
        // holders are dropped; pending holders remain as live conflicts. The
        // monitor folds lease expiry and the unknown-tx grace period into
        // `tx_status`, so a holder still seen as `Pending` here is genuinely
        // live (ADR-021).
        let exclusive = matches!(e.lock_type, LockType::Write | LockType::Create);
        let mut pending: Vec<TxId> = Vec::new();
        for holder in e.locked_by.clone() {
            if &holder == id {
                continue;
            }
            match self.inner.tmon.tx_status(&holder).await? {
                TxCommitStatus::Ok => {
                    if exclusive {
                        let cv = self
                            .inner
                            .tmon
                            .committed_value(&intent.key_path, &holder)
                            .await?;
                        if cv.status == TxCommitStatus::Ok && !cv.value.not_written {
                            e.current_writer = Some(holder.clone());
                            e.deleted = cv.value.deleted;
                        }
                    }
                }
                TxCommitStatus::Pending => pending.push(holder),
                // Aborted / Unknown: the lock is dead; drop it.
                _ => {}
            }
        }

        // Validate the read against the resolved state. A reader observes the
        // existence-aware token (the effective writer iff the key exists), so the
        // value the transaction read must still be effective. Doing this after
        // help-forward closes the validate-then-commit race.
        let token = if e.current_writer.is_some() && !e.deleted {
            e.current_writer.clone()
        } else {
            None
        };
        if let Some(observed) = &intent.observed
            && observed != &token
        {
            return Ok(None);
        }

        let exists_before = e.current_writer.is_some() && !e.deleted;

        // Read locks share with other read holders; everything else is exclusive
        // and must clear the live pending holders via wound-wait.
        let compatible = matches!(intent.desired, Desired::Read)
            && !matches!(e.lock_type, LockType::Write | LockType::Create);
        if !compatible {
            for holder in &pending {
                if !self.try_reclaim(id, holder).await? {
                    return Ok(None);
                }
            }
            pending.clear();
        }

        let membership = match intent.desired {
            Desired::Put => !exists_before,
            Desired::Delete => exists_before,
            Desired::Read => false,
        };

        match intent.desired {
            Desired::Read => {
                let mut holders = pending;
                if !holders.contains(id) {
                    holders.push(id.clone());
                }
                e.locked_by = holders;
                e.lock_type = LockType::Read;
            }
            Desired::Put | Desired::Delete => {
                e.locked_by = vec![id.clone()];
                e.lock_type = if exists_before {
                    LockType::Write
                } else if matches!(intent.desired, Desired::Put) {
                    LockType::Create
                } else {
                    LockType::Write
                };
            }
        }
        Ok(Some((e, membership)))
    }

    /// Reclaim decision against a live pending `holder`: `id` may take the lock
    /// if it **outranks** the holder by wound-wait priority. Lease expiry and the
    /// unknown-tx grace are folded into the monitor, so a holder seen as pending
    /// here is live and only an outranking transaction may wound it. When it may,
    /// it wounds the holder (CAS pending → aborted) and confirms the abort.
    /// `false` means `id` must restart.
    async fn try_reclaim(&self, id: &TxId, holder: &TxId) -> Result<bool, TransError> {
        if !should_wound(id, holder) {
            return Ok(false);
        }
        self.inner.tmon.wound_tx(holder).await?;
        Ok(self.inner.tmon.tx_status(holder).await? == TxCommitStatus::Aborted)
    }

    /// Acquires the collection root's membership write lock for `prefix`
    /// (ADR-018), with the same resolve/wound-wait rules as a shard. Auto-creates
    /// the root if absent so a write that creates the collection's first key
    /// works without a prior explicit `create` (matching v1's on-demand
    /// collection-info lock object). Returns `false` if the transaction must
    /// restart.
    async fn lock_root(&self, prefix: &str, id: &TxId) -> Result<bool, TransError> {
        let mut backoff = self.inner.retry.backoff();
        for attempt in 0..CAS_RETRIES {
            if attempt > 0 {
                rt::sleep(backoff.next_delay()).await;
            }
            let (mut root, ver) = match self.inner.shards.load_root(prefix).await {
                Ok(rv) => rv,
                Err(StorageError::NotFound) => {
                    // The collection does not exist yet: create its root holding
                    // our membership lock. If we lose the create race, reload.
                    let mut root = CollectionRoot::new(glassdb_data::shard::SHARD_COUNT);
                    root.set_membership_lock(LockType::Write, [id.clone()]);
                    match self.inner.shards.create_root(prefix, &root).await {
                        Ok(true) => {
                            self.record_root_lock(id, prefix);
                            return Ok(true);
                        }
                        // Lost the create race, or an in-doubt create whose
                        // landing we can't confirm: reload and retry (idempotent).
                        Ok(false) => continue,
                        Err(StorageError::Unavailable(_)) => continue,
                        Err(e) => return Err(e.into()),
                    }
                }
                Err(e) => return Err(e.into()),
            };

            let mut pending: Vec<TxId> = Vec::new();
            for holder in root.membership_locked_by().to_vec() {
                if &holder == id {
                    continue;
                }
                if self.inner.tmon.tx_status(&holder).await? == TxCommitStatus::Pending {
                    pending.push(holder);
                }
            }
            let mut lost = false;
            for holder in &pending {
                if !self.try_reclaim(id, holder).await? {
                    lost = true;
                    break;
                }
            }
            if lost {
                return Ok(false);
            }

            root.set_membership_lock(LockType::Write, [id.clone()]);
            match self.inner.shards.store_root(prefix, &root, &ver).await {
                Ok(true) => {
                    self.record_root_lock(id, prefix);
                    return Ok(true);
                }
                // Precondition or in-doubt: reload and retry; re-installing our
                // own membership lock is idempotent (ADR-009).
                Ok(false) => {}
                Err(StorageError::Unavailable(_)) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(false)
    }

    /// Publishes `current_writer` pointers / tombstones and releases this
    /// transaction's locks across the shards it touched, then releases the root
    /// membership locks. Every CAS is idempotent; errors are best-effort
    /// (a failure leaves the locks to be reclaimed lazily by the next contender
    /// or lease expiry), so this never fails an already-committed transaction.
    pub async fn write_back(&self, id: &TxId, locked: &LockedTx) {
        for group in locked.groups.values() {
            let _ = self.write_back_shard(id, group).await;
        }
        for prefix in &locked.membership {
            let _ = self.release_root(prefix, id).await;
        }
        self.clear_tx_locks(id);
    }

    async fn write_back_shard(&self, id: &TxId, group: &ShardGroup) -> Result<(), TransError> {
        let mut backoff = self.inner.retry.backoff();
        for attempt in 0..CAS_RETRIES {
            if attempt > 0 {
                rt::sleep(backoff.next_delay()).await;
            }
            let (shard, ver) = self
                .inner
                .shards
                .load_shard(&group.prefix, group.idx)
                .await?;
            let mut entries: BTreeMap<Vec<u8>, ShardEntry> = shard
                .entries()
                .cloned()
                .map(|e| (e.key.clone(), e))
                .collect();
            for intent in &group.intents {
                let Some(e) = entries.get_mut(&intent.raw_key) else {
                    continue;
                };
                if !e.locked_by.contains(id) {
                    continue;
                }
                match intent.desired {
                    Desired::Put => {
                        e.current_writer = Some(id.clone());
                        e.deleted = false;
                    }
                    Desired::Delete => {
                        e.current_writer = Some(id.clone());
                        e.deleted = true;
                    }
                    Desired::Read => {}
                }
                e.locked_by.retain(|h| h != id);
                if e.locked_by.is_empty() {
                    e.lock_type = LockType::None;
                }
            }
            let new_shard = glassdb_storage::Shard::from_entries(entries.into_values());
            if self
                .inner
                .shards
                .store_shard(&group.prefix, group.idx, &new_shard, ver.as_ref())
                .await?
            {
                return Ok(());
            }
        }
        Ok(())
    }

    async fn release_root(&self, prefix: &str, id: &TxId) -> Result<(), TransError> {
        let mut backoff = self.inner.retry.backoff();
        for attempt in 0..CAS_RETRIES {
            if attempt > 0 {
                rt::sleep(backoff.next_delay()).await;
            }
            let (mut root, ver) = match self.inner.shards.load_root(prefix).await {
                Ok(rv) => rv,
                Err(StorageError::NotFound) => return Ok(()),
                Err(e) => return Err(e.into()),
            };
            if !root.membership_locked_by().contains(id) {
                return Ok(());
            }
            root.clear_membership_lock();
            if self.inner.shards.store_root(prefix, &root, &ver).await? {
                return Ok(());
            }
        }
        Ok(())
    }

    // --- Per-tx held-lock bookkeeping (diagnostics) -------------------------

    fn record_shard_lock(&self, id: &TxId, group: &ShardGroup) {
        // Represent the shard hold with its strongest intention so the
        // diagnostic snapshot distinguishes read-only from write holders.
        let typ = if group
            .intents
            .iter()
            .any(|i| !matches!(i.desired, Desired::Read))
        {
            LockType::Write
        } else {
            LockType::Read
        };
        let path = paths::from_shard(&group.prefix, group.idx);
        let mut tlocks = self.inner.tlocks.for_key(id.as_bytes()).lock().unwrap();
        tlocks.entry(id.clone()).or_default().insert(path, typ);
    }

    fn record_root_lock(&self, id: &TxId, prefix: &str) {
        let path = paths::collection_info(prefix);
        let mut tlocks = self.inner.tlocks.for_key(id.as_bytes()).lock().unwrap();
        tlocks
            .entry(id.clone())
            .or_default()
            .insert(path, LockType::Write);
    }

    fn clear_tx_locks(&self, id: &TxId) {
        let mut tlocks = self.inner.tlocks.for_key(id.as_bytes()).lock().unwrap();
        tlocks.remove(id);
    }
}

/// Wound-wait priority decision: a strictly-older transaction wounds a younger
/// holder (ADR-002). Equal-priority transactions are deliberately **not**
/// ordered — neither wounds the other — exactly like [`TxId::older`]. A
/// prefix-based tiebreak must not be used: a retry mints a fresh prefix
/// ([`TxId::renew`]), so a prefix tiebreak would flip the winner on every retry
/// and let two equal-priority transactions wound each other forever (livelock).
/// The equal-priority case is resolved by the serial sorted-locking fallback.
fn should_wound(me: &TxId, holder: &TxId) -> bool {
    me.older(holder)
}

#[cfg(test)]
mod tests {
    use super::*;
    use glassdb_backend::{Backend, memory::MemoryBackend};
    use glassdb_concurr::{Background, RetryConfig};
    use glassdb_data::paths;
    use glassdb_data::shard::shard_index;
    use glassdb_storage::{Global, Local, Shard, TLogger, TxCommitStatus};
    use std::sync::Arc;

    struct TlCtx {
        shards: ShardStore,
        monitor: Monitor,
        _bg: Arc<Background>,
    }

    fn new_test_locker(b: Arc<dyn Backend>) -> (Locker, TlCtx) {
        let local = Local::new(1024);
        let global = Global::new(b.clone(), local.clone());
        let tl = TLogger::new(global.clone(), local.clone(), "test");
        let bg = Arc::new(Background::new());
        let mon = Monitor::new(local.clone(), tl, Arc::downgrade(&bg));
        let shards = ShardStore::new(b.clone());
        let locker = Locker::new(shards.clone(), mon.clone(), RetryConfig::default());
        (
            locker,
            TlCtx {
                shards,
                monitor: mon,
                _bg: bg,
            },
        )
    }

    fn init_tl_test() -> (Locker, TlCtx) {
        new_test_locker(Arc::new(MemoryBackend::new()))
    }

    // Builds a deterministic, valid transaction ID. A smaller `order` yields an
    // older (higher-priority) transaction under the wound-wait rule.
    fn mk_tid(order: u64, name: &str) -> TxId {
        TxId::with_priority(order * 1_000_000_000, name.as_bytes())
    }

    const COLL: &str = "example";

    fn read_intent(key: &[u8], observed: Option<Option<TxId>>) -> KeyIntent {
        KeyIntent {
            raw_key: key.to_vec(),
            key_path: paths::from_key(COLL, key),
            desired: Desired::Read,
            observed,
        }
    }

    fn put_intent(key: &[u8], observed: Option<Option<TxId>>) -> KeyIntent {
        KeyIntent {
            raw_key: key.to_vec(),
            key_path: paths::from_key(COLL, key),
            desired: Desired::Put,
            observed,
        }
    }

    fn group_of(key: &[u8], intent: KeyIntent) -> BTreeMap<String, ShardGroup> {
        let idx = shard_index(key);
        let mut g = BTreeMap::new();
        g.insert(
            paths::from_shard(COLL, idx),
            ShardGroup {
                prefix: COLL.to_string(),
                idx,
                intents: vec![intent],
            },
        );
        g
    }

    async fn entry_of(ctx: &TlCtx, key: &[u8]) -> Option<ShardEntry> {
        let (shard, _) = ctx.shards.load_shard(COLL, shard_index(key)).await.unwrap();
        shard.lookup(key).cloned()
    }

    #[tokio::test]
    async fn lock_write_creates_entry() {
        let (locker, ctx) = init_tl_test();
        let key = b"key";
        let tx = mk_tid(1, "tx");
        ctx.monitor.begin_tx(&tx);

        let groups = group_of(key, put_intent(key, None));
        let membership = locker.lock_shards(&tx, &groups, false).await.unwrap();
        assert_eq!(membership, Some(BTreeSet::from([COLL.to_string()])));

        let e = entry_of(&ctx, key).await.expect("entry installed");
        assert_eq!(e.lock_type, LockType::Create);
        assert_eq!(e.locked_by, vec![tx.clone()]);
    }

    #[tokio::test]
    async fn shared_read_locks() {
        let (locker, ctx) = init_tl_test();
        let key = b"key";
        let tx1 = mk_tid(1, "tx1");
        let tx2 = mk_tid(2, "tx2");
        ctx.monitor.begin_tx(&tx1);
        ctx.monitor.begin_tx(&tx2);

        locker
            .lock_shards(&tx1, &group_of(key, read_intent(key, None)), false)
            .await
            .unwrap();
        locker
            .lock_shards(&tx2, &group_of(key, read_intent(key, None)), false)
            .await
            .unwrap();

        let e = entry_of(&ctx, key).await.unwrap();
        assert_eq!(e.lock_type, LockType::Read);
        let mut holders = e.locked_by.clone();
        holders.sort_by_key(|t| t.to_string());
        let mut expected = vec![tx1.clone(), tx2.clone()];
        expected.sort_by_key(|t| t.to_string());
        assert_eq!(holders, expected);
    }

    #[tokio::test(start_paused = true)]
    async fn older_wounds_younger_write_holder() {
        let (locker, ctx) = init_tl_test();
        let key = b"key";

        // Seed a committed value so the key exists (write lock, not create).
        seed_committed(&ctx, key, b"v0").await;

        let young = mk_tid(2, "young");
        ctx.monitor.begin_tx(&young);
        locker
            .lock_shards(&young, &group_of(key, put_intent(key, None)), false)
            .await
            .unwrap();

        let old = mk_tid(1, "old");
        ctx.monitor.begin_tx(&old);
        let res = locker
            .lock_shards(&old, &group_of(key, put_intent(key, None)), false)
            .await
            .unwrap();
        assert!(res.is_some(), "older tx should win the lock");

        let e = entry_of(&ctx, key).await.unwrap();
        assert_eq!(e.locked_by, vec![old.clone()]);
        assert_eq!(
            ctx.monitor.tx_status(&young).await.unwrap(),
            TxCommitStatus::Aborted
        );
    }

    #[tokio::test(start_paused = true)]
    async fn younger_conflicts_on_write_holder() {
        let (locker, ctx) = init_tl_test();
        let key = b"key";
        seed_committed(&ctx, key, b"v0").await;

        let old = mk_tid(1, "old");
        ctx.monitor.begin_tx(&old);
        locker
            .lock_shards(&old, &group_of(key, put_intent(key, None)), false)
            .await
            .unwrap();

        let young = mk_tid(2, "young");
        ctx.monitor.begin_tx(&young);
        let res = locker
            .lock_shards(&young, &group_of(key, put_intent(key, None)), false)
            .await
            .unwrap();
        assert!(res.is_none(), "younger tx must not wound an older holder");
        assert_eq!(
            ctx.monitor.tx_status(&old).await.unwrap(),
            TxCommitStatus::Pending
        );
    }

    #[tokio::test]
    async fn stale_read_token_conflicts() {
        let (locker, ctx) = init_tl_test();
        let key = b"key";
        seed_committed(&ctx, key, b"v0").await;
        let writer = entry_of(&ctx, key).await.unwrap().current_writer.unwrap();

        let tx = mk_tid(1, "tx");
        ctx.monitor.begin_tx(&tx);
        // Observed a different (stale) writer than what the shard records.
        let stale = mk_tid(9, "stale");
        let intent = read_intent(key, Some(Some(stale)));
        let res = locker
            .lock_shards(&tx, &group_of(key, intent), false)
            .await
            .unwrap();
        assert!(res.is_none(), "stale read token must conflict");

        // The authoritative writer is unchanged.
        assert_eq!(
            entry_of(&ctx, key).await.unwrap().current_writer,
            Some(writer)
        );
    }

    #[tokio::test]
    async fn write_back_publishes_and_releases() {
        let (locker, ctx) = init_tl_test();
        let key = b"key";
        let tx = mk_tid(1, "tx");
        ctx.monitor.begin_tx(&tx);

        let groups = group_of(key, put_intent(key, None));
        let membership = locker
            .lock_shards(&tx, &groups, false)
            .await
            .unwrap()
            .unwrap();
        locker
            .write_back(&tx, &LockedTx { groups, membership })
            .await;

        let e = entry_of(&ctx, key).await.unwrap();
        assert_eq!(e.lock_type, LockType::None);
        assert!(e.locked_by.is_empty());
        assert_eq!(e.current_writer, Some(tx.clone()));
        assert!(locker.tx_locks_snapshot().is_empty());
    }

    #[tokio::test]
    async fn tx_locks_snapshot_lists_held_shards() {
        let (locker, ctx) = init_tl_test();
        let key = b"key";
        let tx = mk_tid(1, "tx");
        ctx.monitor.begin_tx(&tx);

        locker
            .lock_shards(&tx, &group_of(key, put_intent(key, None)), false)
            .await
            .unwrap();

        let snap = locker.tx_locks_snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].tx_id, tx);
        // A write intention records the shard as a write lock, plus the root for
        // the membership change.
        let shard_path = paths::from_shard(COLL, shard_index(key));
        let root_path = paths::collection_info(COLL);
        // lock_root is taken by the Algo, so only the shard appears here; assert
        // the shard hold is present and write-typed.
        let _ = root_path;
        assert!(
            snap[0]
                .locks
                .iter()
                .any(|l| l.path == shard_path && l.typ == LockType::Write)
        );
    }

    // Helper: commit a value for `key` so the shard records a `current_writer`,
    // making the key exist (so subsequent writes take a Write, not Create, lock).
    async fn seed_committed(ctx: &TlCtx, key: &[u8], value: &[u8]) {
        use glassdb_storage::{TxLog, TxWrite};
        let writer = mk_tid(0, "seed");
        ctx.monitor.begin_tx(&writer);
        let mut tl = TxLog::new(writer.clone(), TxCommitStatus::Ok);
        tl.writes = vec![TxWrite {
            path: paths::from_key(COLL, key),
            value: Arc::from(value),
            deleted: false,
            prev_writer: TxId::default(),
        }];
        ctx.monitor.commit_tx(tl).await.unwrap();

        // Install the committed pointer directly in the shard.
        let idx = shard_index(key);
        let (shard, ver) = ctx.shards.load_shard(COLL, idx).await.unwrap();
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
                current_writer: Some(writer),
                deleted: false,
            },
        );
        let new_shard = Shard::from_entries(entries.into_values());
        assert!(
            ctx.shards
                .store_shard(COLL, idx, &new_shard, ver.as_ref())
                .await
                .unwrap()
        );
    }
}
