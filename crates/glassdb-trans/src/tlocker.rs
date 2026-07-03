//! Distributed locking over the v2 shard/root coordination objects (ADR-017,
//! ADR-020, ADR-024), with cross-transaction request **deduplication**
//! (ADR-025). Ported in spirit from the Go `internal/trans/tlocker.go`, but
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
//! **Deduplication (ADR-025):** the per-object lock step is not run directly but
//! submitted to a [`Dedup`] keyed on the object path, so several transactions
//! contending the same shard **merge** into one owner-driven load + CAS whenever
//! they do not exclusively conflict on the same key (disjoint keys or shared
//! reads). N GET+CAS round-trips collapse to one; same-key writers (and root
//! membership requests) queue and resolve by the unchanged wound-wait. The
//! [`Dedup`] fans out one shared result, so each transaction's own outcome
//! (`CasOutcome`) travels back through a per-submission slot the caller reads
//! once its submission resolves.
//!
//! Lock acquisition has two modes (ADR-020): the default **parallel** path locks
//! every touched shard concurrently; the **serial** fallback locks them one at a
//! time in ascending shard path order so equal-priority contenders queue on the
//! lowest contended shard and exactly one wins it (first-CAS-wins), guaranteeing
//! progress where the parallel path could livelock.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::join_all;
use glassdb_concurr::{
    BatchHandle, Dedup, DedupError, DedupKeySnapshot, MergeRequest, RetryConfig, Worker, rt,
    shard::Sharded,
};
use glassdb_data::shard::group_by_owning_shard;
use glassdb_data::{TxId, paths};
use glassdb_storage::{
    CollectionRoot, LockType, PathLock, ShardEntry, ShardStore, StorageError, TxCommitStatus,
};

use crate::algo::{Data, WriteOp};
use crate::error::TransError;
use crate::monitor::Monitor;
use crate::resolver::Resolver;

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
pub(crate) struct LockedTx {
    groups: BTreeMap<String, ShardGroup>,
    membership: BTreeSet<String>,
}

impl LockedTx {
    /// The per-key and membership-root paths this transaction holds, as the
    /// `PathLock` set GC records on the transaction object for its reverse
    /// liveness check and lock pruning (ADR-022). Keys map to their `_k/` path,
    /// membership prefixes to their collection-info path; GC ignores the lock
    /// type, so it is only kept faithful for diagnostics.
    pub(crate) fn locked_paths(&self) -> Vec<PathLock> {
        let mut out = Vec::new();
        for group in self.groups.values() {
            for intent in &group.intents {
                out.push(PathLock {
                    path: intent.key_path.clone(),
                    typ: lock_type(intent.desired),
                });
            }
        }
        for prefix in &self.membership {
            out.push(PathLock {
                path: paths::collection_info(prefix),
                typ: LockType::Write,
            });
        }
        out
    }
}

/// The lock type a `Desired` intention installs, for the recorded `PathLock`
/// set (a read lock for a key only read, a write lock for any mutation).
fn lock_type(desired: Desired) -> LockType {
    match desired {
        Desired::Read => LockType::Read,
        Desired::Put | Desired::Delete => LockType::Write,
    }
}

/// Groups a transaction's accessed keys by shard. Each key gets one intent
/// carrying the lock to install: a write/create/delete for a written key, a
/// read lock for a key only read. Optimistic read validation is the engine's
/// job (it validates after locking, ADR-024), so no read token is carried here.
fn build_groups(data: &Data) -> Result<BTreeMap<String, ShardGroup>, TransError> {
    let mut by_path: BTreeMap<String, Desired> = BTreeMap::new();
    for w in &data.writes {
        let desired = match w.op {
            WriteOp::Delete => Desired::Delete,
            WriteOp::Put(_) => Desired::Put,
        };
        // A later write to the same key wins (e.g. put-then-delete).
        by_path.insert(w.path.to_string(), desired);
    }
    for r in &data.reads {
        // A key that is also written keeps its exclusive intent.
        by_path.entry(r.path.to_string()).or_insert(Desired::Read);
    }

    let grouped = group_by_owning_shard(by_path)
        .map_err(|e| TransError::with_source("grouping keys by shard", e))?;

    let mut groups: BTreeMap<String, ShardGroup> = BTreeMap::new();
    for ((prefix, idx), keys) in grouped {
        let mut intents: Vec<KeyIntent> = keys
            .into_iter()
            .map(|(raw_key, desired)| KeyIntent {
                key_path: paths::from_key(&prefix, &raw_key),
                raw_key,
                desired,
            })
            .collect();
        intents.sort_by(|a, b| a.raw_key.cmp(&b.raw_key));
        groups.insert(
            paths::from_shard(&prefix, idx),
            ShardGroup {
                prefix,
                idx,
                intents,
            },
        );
    }
    Ok(groups)
}

/// Final outcome of acquiring every lock a transaction needs.
pub(crate) enum LockOutcome {
    /// All locks held; drives write-back on commit.
    Locked(LockedTx),
    /// Lost a CAS-contention race (the bounded retry budget was exhausted under
    /// churn). Handled **internally** by [`super::algo::Algo`]: it releases the
    /// partial locks and re-acquires under the **same id** after a backoff — no
    /// renew and no body re-run (escalating to the serial order if contention
    /// persists). Never surfaces to the database retry loop.
    Conflict,
}

/// Outcome of acquiring locks across all touched shards.
enum ShardsOutcome {
    Locked(BTreeSet<String>),
    Conflict,
}

/// Outcome of acquiring locks on a single shard (after any hold-and-wait).
enum ShardOutcome {
    /// Locked; `membership` is true if the shard saw a create/delete.
    Locked {
        membership: bool,
    },
    Conflict,
}

/// One transaction's outcome for a single deduplicated CAS round (ADR-025),
/// deposited by the worker into that transaction's [`OutcomeSlot`] and read by
/// its caller once the [`Dedup`] submission resolves.
enum CasOutcome {
    /// Locked; `membership` is true if the shard saw a create/delete.
    Locked { membership: bool },
    /// A touched key is held by a live holder this transaction does not
    /// outrank: wait for `holder` to finalize, then re-submit (hold-and-wait,
    /// ADR-024). Nothing was staged for this transaction in the round's CAS.
    Wait(TxId),
    /// The bounded CAS budget was exhausted under churn; release and re-lock.
    Conflict,
    /// A release or write-back completed (ADR-026). Idempotent and best-effort:
    /// there is nothing to wait on and nothing for the caller to retry.
    /// `superseded` carries the `current_writer` transaction ids a write-back
    /// overwrote — GC reverse-check candidates (ADR-022); empty for a release.
    Released { superseded: Vec<TxId> },
}

/// Per-submission mailbox carrying one transaction's [`CasOutcome`] back from
/// the dedup worker. Owned by the caller and cloned into the merged request, so
/// it lives exactly as long as either side needs it and never leaks when a
/// caller's future is dropped mid-round.
type OutcomeSlot = Arc<Mutex<Option<CasOutcome>>>;

/// What a shard member does to the object in a merged round (ADR-025, ADR-026).
#[derive(Clone, Copy, PartialEq, Eq)]
enum ShardAction {
    /// Acquire locks on the member's keys (resolve holders, wound-wait, install).
    Acquire,
    /// Publish the member's committed writes on its keys and drop its holds
    /// (post-commit write-back).
    WriteBack,
    /// Drop every hold the member has in the shard, publishing nothing.
    Release,
}

/// Whether a root request acquires or releases the membership lock (ADR-026).
#[derive(Clone, Copy, PartialEq, Eq)]
enum RootAction {
    Acquire,
    Release,
}

/// One transaction's participation in a shard CAS batch: what it does, the keys
/// it touches, and where to deliver its outcome.
#[derive(Clone)]
struct ShardMember {
    action: ShardAction,
    intents: Arc<Vec<KeyIntent>>,
    slot: OutcomeSlot,
}

impl ShardMember {
    /// A member is reorderable — free to join any batch instead of FIFO-queueing
    /// behind an unrelated writer — unless it is an *exclusive acquire*. Only an
    /// acquirer taking a write/create/delete lock needs FIFO order to preserve
    /// wound-wait priority and the equal-priority serial guarantee; releases and
    /// write-backs never contend, so they always reorder (ADR-026).
    fn is_reorderable(&self) -> bool {
        match self.action {
            ShardAction::Release | ShardAction::WriteBack => true,
            ShardAction::Acquire => self
                .intents
                .iter()
                .all(|i| matches!(i.desired, Desired::Read)),
        }
    }
}

/// A deduplication request for one CAS coordination object (ADR-025): the unit
/// merged by [`Dedup`], keyed on the object path. A single submission carries
/// one transaction; a merged request accumulates several compatible ones.
#[derive(Clone)]
enum CasReq {
    /// Lock keys in a shard. `members` maps each contending transaction to its
    /// intents and outcome slot.
    Shard {
        prefix: String,
        idx: u32,
        members: BTreeMap<TxId, ShardMember>,
    },
    /// Acquire or release the collection root's exclusive membership lock. Roots
    /// never merge, so a request always carries exactly one transaction; the
    /// dedup only serializes contenders through one owner (ADR-025, ADR-026).
    Root {
        prefix: String,
        tx: TxId,
        action: RootAction,
        slot: OutcomeSlot,
    },
}

impl MergeRequest for CasReq {
    fn merge(&self, other: &Self) -> Option<Self> {
        match (self, other) {
            (
                CasReq::Shard {
                    prefix,
                    idx,
                    members: a,
                },
                CasReq::Shard { members: b, .. },
            ) => {
                // Merge only when the two sides do not exclusively conflict: for
                // every key touched by both, both must hold a read-only intent
                // (disjoint keys or shared reads). A write/create/delete overlap
                // is a genuine lock conflict and must be ordered by wound-wait,
                // so the loser stays queued.
                if shard_reqs_conflict(a, b) {
                    return None;
                }
                let mut members = a.clone();
                for (tx, m) in b {
                    members.insert(tx.clone(), m.clone());
                }
                Some(CasReq::Shard {
                    prefix: prefix.clone(),
                    idx: *idx,
                    members,
                })
            }
            // A root takes the single exclusive membership lock, so two root
            // requests never merge; a shard and a root never share a dedup key.
            _ => None,
        }
    }

    fn can_reorder(&self) -> bool {
        match self {
            // Read-only acquires, releases, and write-backs can join any batch
            // instead of FIFO-blocking behind an unrelated writer (ADR-026).
            CasReq::Shard { members, .. } => members.values().all(|m| m.is_reorderable()),
            // A root release never contends, so it can reorder ahead of a queued
            // acquire; a root acquire keeps FIFO order.
            CasReq::Root { action, .. } => matches!(action, RootAction::Release),
        }
    }
}

/// Reports whether two shard requests exclusively conflict: some key that both
/// sides want to **acquire** carries a write/create/delete intent on either
/// side. Disjoint key sets and shared read-only acquires do not conflict and may
/// merge into one CAS. Release and write-back members never contend (they only
/// drop their own holds), so they never block a merge (ADR-026).
fn shard_reqs_conflict(a: &BTreeMap<TxId, ShardMember>, b: &BTreeMap<TxId, ShardMember>) -> bool {
    let ca = shard_key_exclusive(a);
    let cb = shard_key_exclusive(b);
    ca.iter().any(|(k, ea)| match cb.get(k) {
        Some(eb) => *ea || *eb,
        None => false,
    })
}

/// Maps each key an **acquiring** member touches to whether any acquirer holds
/// an exclusive (write/create/delete) intent on it. Release and write-back
/// members are ignored: they never contend for a lock (ADR-026).
fn shard_key_exclusive(members: &BTreeMap<TxId, ShardMember>) -> HashMap<Vec<u8>, bool> {
    let mut out: HashMap<Vec<u8>, bool> = HashMap::new();
    for m in members.values() {
        if !matches!(m.action, ShardAction::Acquire) {
            continue;
        }
        for it in m.intents.iter() {
            let excl = !matches!(it.desired, Desired::Read);
            out.entry(it.raw_key.clone())
                .and_modify(|e| *e |= excl)
                .or_insert(excl);
        }
    }
    out
}

/// Stages `id`'s write-back on its `intents`: publish the committed pointer
/// (`current_writer` / tombstone) for each key it still holds and drop its hold
/// (ADR-020). Returns one changed entry per affected key; keys `id` no longer
/// holds are skipped, so re-running is a no-op (idempotent, ADR-009). Publishing
/// only `id`'s own monotonic pointer, this never conflicts with another member.
fn writeback_changes(
    id: &TxId,
    intents: &[KeyIntent],
    entries: &BTreeMap<Vec<u8>, ShardEntry>,
) -> WritebackStaged {
    let mut changes = Vec::new();
    let mut superseded = Vec::new();
    for intent in intents {
        let Some(e) = entries.get(&intent.raw_key) else {
            continue;
        };
        if !e.locked_by.contains(id) {
            continue;
        }
        let mut e = e.clone();
        match intent.desired {
            Desired::Put | Desired::Delete => {
                if let Some(prev) = &e.current_writer
                    && prev != id
                {
                    superseded.push(prev.clone());
                }
                e.current_writer = Some(id.clone());
                e.deleted = matches!(intent.desired, Desired::Delete);
            }
            Desired::Read => {}
        }
        e.locked_by.retain(|h| h != id);
        if e.locked_by.is_empty() {
            e.lock_type = LockType::None;
        }
        changes.push((intent.raw_key.clone(), e));
    }
    WritebackStaged {
        changes,
        superseded,
    }
}

/// The staged result of a write-back: the entry changes to apply and the
/// `current_writer`s they superseded (GC candidates, ADR-022).
struct WritebackStaged {
    changes: Vec<(Vec<u8>, ShardEntry)>,
    superseded: Vec<TxId>,
}

/// Stages `id`'s release: drop its hold from **every** entry in the shard,
/// publishing nothing. Release does not know the tx's keys (it runs from the
/// per-tx bookkeeping, ADR-024), so it sweeps the loaded entries. Idempotent —
/// entries `id` does not hold are untouched.
fn release_changes(
    id: &TxId,
    entries: &BTreeMap<Vec<u8>, ShardEntry>,
) -> Vec<(Vec<u8>, ShardEntry)> {
    let mut changes = Vec::new();
    for (k, e) in entries {
        if !e.locked_by.contains(id) {
            continue;
        }
        let mut e = e.clone();
        e.locked_by.retain(|h| h != id);
        if e.locked_by.is_empty() {
            e.lock_type = LockType::None;
        }
        changes.push((k.clone(), e));
    }
    changes
}

/// One member's resolution against a loaded shard within a batch round.
enum MemberResolution {
    /// The member's locks are ready in `changes` (one entry per touched key);
    /// `membership` is true if it saw a create/delete.
    Locked {
        changes: Vec<(Vec<u8>, ShardEntry)>,
        membership: bool,
    },
    /// A touched key is held by a live holder this member cannot wound: it must
    /// wait for `holder` and re-submit; nothing is staged for it.
    Wait(TxId),
    /// A release / write-back applied its changes (ADR-026); `changes` may be
    /// empty when the member held nothing. It never waits or conflicts.
    /// `superseded` carries the `current_writer`s a write-back overwrote (GC
    /// candidates, ADR-022); empty for a release.
    Released {
        changes: Vec<(Vec<u8>, ShardEntry)>,
        superseded: Vec<TxId>,
    },
}

/// How a hold-and-wait wake happened, so the re-poll cadence can be tuned: a
/// holder *finalizing* is real progress, while a poll timeout saw no event and
/// only re-checks for a lock released without finalizing.
enum Woke {
    /// `wait_for_tx` fired: the holder committed or aborted.
    Finalized,
    /// The backed-off poll timer elapsed with no finalize event.
    PollTimeout,
}

/// Outcome of one bounded CAS attempt at locking the collection root.
enum RootAttempt {
    Locked,
    Conflict,
    Wait(TxId),
}

/// Per-key resolution within a shard CAS attempt.
enum EntryResolution {
    /// The lock is installed in `entry`; `membership` is true for a
    /// create/delete.
    Locked { entry: ShardEntry, membership: bool },
    /// A live pending holder this transaction does not outrank: wait for it.
    Wait(TxId),
}

/// Reclaim decision against a single live pending holder under wound-wait.
enum Reclaim {
    /// The holder was wounded (or is already aborted): proceed past it.
    Wounded,
    /// Cannot reclaim it now (younger-or-equal, or it committed before the
    /// wound landed): wait for it to finalize, then re-resolve.
    Wait,
}

/// Acquires and releases distributed locks on the shard/root coordination
/// objects, hiding waits, wound-wait, and CAS retries from callers.
#[derive(Clone)]
pub struct Locker {
    inner: Arc<LockerState>,
}

/// One independent partition of the per-transaction held-lock bookkeeping.
type LockerShard = Mutex<HashMap<TxId, HashMap<String, LockType>>>;

/// State shared by the [`Locker`] and its dedup [`CasWorker`]: the storage
/// handles, retry config, per-transaction held-lock bookkeeping, and stats.
struct LockerCore {
    tmon: Monitor,
    shards: ShardStore,
    resolver: Resolver,
    retry: RetryConfig,
    tlocks: Sharded<LockerShard>,
    stats: Stats,
}

impl LockerCore {
    fn record_shard_lock(&self, id: &TxId, prefix: &str, idx: u32, intents: &[KeyIntent]) {
        // Represent the shard hold with its strongest intention so the
        // diagnostic snapshot distinguishes read-only from write holders.
        let typ = if intents.iter().any(|i| !matches!(i.desired, Desired::Read)) {
            LockType::Write
        } else {
            LockType::Read
        };
        let path = paths::from_shard(prefix, idx);
        let mut tlocks = self.tlocks.for_key(id.as_bytes()).lock().unwrap();
        tlocks.entry(id.clone()).or_default().insert(path, typ);
    }

    fn record_root_lock(&self, id: &TxId, prefix: &str) {
        let path = paths::collection_info(prefix);
        let mut tlocks = self.tlocks.for_key(id.as_bytes()).lock().unwrap();
        tlocks
            .entry(id.clone())
            .or_default()
            .insert(path, LockType::Write);
    }

    fn clear_tx_locks(&self, id: &TxId) {
        let mut tlocks = self.tlocks.for_key(id.as_bytes()).lock().unwrap();
        tlocks.remove(id);
    }
}

struct LockerState {
    core: Arc<LockerCore>,
    dedup: Dedup<CasReq, TransError, CasWorker>,
}

/// The [`Dedup`] worker driving one merged lock-acquisition round per CAS object
/// (ADR-025): it loads the shard/root once, resolves every merged member, does a
/// single CAS, and deposits each member's [`CasOutcome`] into its slot.
struct CasWorker {
    core: Arc<LockerCore>,
}

/// Returns the merged shard request's members, erroring if the dedup key somehow
/// produced a root request (shard and root paths never collide).
fn shard_members(
    batch: &BatchHandle<CasReq, TransError>,
) -> Result<BTreeMap<TxId, ShardMember>, TransError> {
    match batch.merged() {
        CasReq::Shard { members, .. } => Ok(members),
        CasReq::Root { .. } => Err(TransError::other("shard dedup key produced a root request")),
    }
}

impl CasWorker {
    /// Resolves the holders of an entry (help-forward committed, drop aborted,
    /// wound-wait the live pending ones) and installs `id`'s lock. Returns
    /// [`EntryResolution::Locked`] with the new entry and whether the change is a
    /// membership change; or [`Wait`] if a live holder this transaction cannot
    /// wound must be waited on (hold-and-wait, ADR-024).
    ///
    /// Read-version validation is not done here — the engine validates reads
    /// after every lock is held (ADR-024).
    ///
    /// [`Wait`]: EntryResolution::Wait
    async fn resolve_and_lock(
        &self,
        id: &TxId,
        intent: &KeyIntent,
        entry: Option<ShardEntry>,
    ) -> Result<EntryResolution, TransError> {
        let mut e = entry.unwrap_or_else(|| ShardEntry {
            key: intent.raw_key.clone(),
            lock_type: LockType::None,
            locked_by: Vec::new(),
            current_writer: None,
            deleted: false,
        });

        // Resolve existing holders other than us via the shared resolver: a
        // committed exclusive holder is help-forwarded (its value becomes the
        // effective one), aborted/missing holders are dropped, and the live
        // pending ones come back as conflicts to wound-wait. The monitor folds
        // lease expiry and the unknown-tx grace period into `tx_status`, so a
        // holder still seen as `Pending` here is genuinely live (ADR-021).
        let resolved = self
            .core
            .resolver
            .resolve_holders(&intent.key_path, &e, Some(id))
            .await?;
        e.current_writer = resolved.writer;
        e.deleted = resolved.deleted;
        let mut pending = resolved.pending;

        let exists_before = e.current_writer.is_some() && !e.deleted;

        // Read locks share with other read holders; everything else is exclusive
        // and must clear the live pending holders via wound-wait: wound the ones
        // we outrank, and wait for the first one we do not (hold-and-wait,
        // ADR-024) — keeping every lock already acquired elsewhere.
        let compatible = matches!(intent.desired, Desired::Read)
            && !matches!(e.lock_type, LockType::Write | LockType::Create);
        if !compatible {
            for holder in &pending {
                match self.try_reclaim(id, holder).await? {
                    Reclaim::Wounded => {}
                    Reclaim::Wait => return Ok(EntryResolution::Wait(holder.clone())),
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
        Ok(EntryResolution::Locked {
            entry: e,
            membership,
        })
    }

    /// Reclaim decision against a live pending `holder`: `id` may take the lock
    /// only if it **outranks** the holder by wound-wait priority. Lease expiry
    /// and the unknown-tx grace are folded into the monitor, so a holder seen as
    /// pending here is live and only an outranking transaction may wound it.
    ///
    /// Returns [`Reclaim::Wounded`] if `id` outranks the holder and the wound
    /// took (CAS pending → aborted), so it may proceed past it. Returns
    /// [`Reclaim::Wait`] if `id` is younger-or-equal (it must not wound an older
    /// peer — that is the hold-and-wait case), or if the holder finalized as
    /// committed before the wound landed (re-resolve via a now-immediate wait so
    /// the committed value is help-forwarded).
    async fn try_reclaim(&self, id: &TxId, holder: &TxId) -> Result<Reclaim, TransError> {
        if !should_wound(id, holder) {
            return Ok(Reclaim::Wait);
        }
        self.core.tmon.wound_tx(holder).await?;
        if self.core.tmon.tx_status(holder).await? == TxCommitStatus::Aborted {
            Ok(Reclaim::Wounded)
        } else {
            Ok(Reclaim::Wait)
        }
    }

    /// Resolves all of one member's key intents against the currently-staged
    /// shard `entries`. Returns [`MemberResolution::Locked`] with one resolved
    /// entry per key, or [`MemberResolution::Wait`] the moment any key is held by
    /// a live holder the member cannot wound — staging nothing for it, so the
    /// batch's other members still proceed.
    async fn resolve_member(
        &self,
        tx: &TxId,
        intents: &[KeyIntent],
        entries: &BTreeMap<Vec<u8>, ShardEntry>,
    ) -> Result<MemberResolution, TransError> {
        let mut changes = Vec::with_capacity(intents.len());
        let mut membership = false;
        for intent in intents {
            let cur = entries.get(&intent.raw_key).cloned();
            match self.resolve_and_lock(tx, intent, cur).await? {
                EntryResolution::Locked {
                    entry,
                    membership: m,
                } => {
                    membership |= m;
                    changes.push((intent.raw_key.clone(), entry));
                }
                EntryResolution::Wait(holder) => return Ok(MemberResolution::Wait(holder)),
            }
        }
        Ok(MemberResolution::Locked {
            changes,
            membership,
        })
    }

    /// Drives one merged shard round: load once, resolve every member, CAS once,
    /// and deposit each member's outcome. A member that must wait stages nothing
    /// and is delivered [`CasOutcome::Wait`], so the owner never blocks — its
    /// caller waits and re-submits while the other members make progress.
    async fn run_shard(
        &self,
        prefix: &str,
        idx: u32,
        batch: &BatchHandle<CasReq, TransError>,
    ) -> Result<(), TransError> {
        let mut backoff = self.core.retry.backoff();
        for attempt in 0..CAS_RETRIES {
            if attempt > 0 {
                rt::sleep(backoff.next_delay()).await;
                self.core.stats.n_retries.fetch_add(1, Ordering::Relaxed);
            }
            let (shard, ver) = self.core.shards.load_shard(prefix, idx).await?;
            // Read the merged set *after* the load so this round absorbs every
            // member that queued while the load I/O was in flight (ADR-025) — the
            // window that turns N contenders' loads+CASes into one.
            let members = shard_members(batch)?;
            let mut entries: BTreeMap<Vec<u8>, ShardEntry> = shard
                .entries()
                .cloned()
                .map(|e| (e.key.clone(), e))
                .collect();

            // Resolve every member against the shared entry set. The merge rule
            // guarantees acquirers overlap only on shared-read keys, and releases
            // / write-backs touch only their own holds, so applying one member's
            // staged changes cannot clobber another's (ADR-025, ADR-026). Members
            // are visited in deterministic `TxId` order for the DST op-stream.
            let mut results: Vec<(TxId, CasOutcome)> = Vec::with_capacity(members.len());
            let mut staged = false;
            for (tx, m) in &members {
                let resolution = match m.action {
                    ShardAction::Acquire => self.resolve_member(tx, &m.intents, &entries).await?,
                    ShardAction::WriteBack => {
                        let WritebackStaged {
                            changes,
                            superseded,
                        } = writeback_changes(tx, &m.intents, &entries);
                        MemberResolution::Released {
                            changes,
                            superseded,
                        }
                    }
                    ShardAction::Release => MemberResolution::Released {
                        changes: release_changes(tx, &entries),
                        superseded: Vec::new(),
                    },
                };
                match resolution {
                    MemberResolution::Locked {
                        changes,
                        membership,
                    } => {
                        for (k, e) in changes {
                            entries.insert(k, e);
                        }
                        staged = true;
                        results.push((tx.clone(), CasOutcome::Locked { membership }));
                    }
                    MemberResolution::Wait(holder) => {
                        results.push((tx.clone(), CasOutcome::Wait(holder)));
                    }
                    MemberResolution::Released {
                        changes,
                        superseded,
                    } => {
                        staged |= !changes.is_empty();
                        for (k, e) in changes {
                            entries.insert(k, e);
                        }
                        results.push((tx.clone(), CasOutcome::Released { superseded }));
                    }
                }
            }

            if staged {
                let new_shard = glassdb_storage::Shard::from_entries(entries.into_values());
                match self
                    .core
                    .shards
                    .store_shard(prefix, idx, &new_shard, ver.as_ref())
                    .await
                {
                    Ok(true) => {}
                    // Precondition: the shard changed under us; reload and retry.
                    Ok(false) => continue,
                    // In-doubt lock CAS (ADR-009): re-installing our own locks
                    // over a freshly-read shard is idempotent, so recover in
                    // place by reloading and retrying.
                    Err(StorageError::Unavailable(_)) => continue,
                    Err(e) => return Err(e.into()),
                }
            }

            // The CAS landed (or nothing needed staging): record held locks and
            // publish each member's outcome into its slot before returning, so
            // the deposit happens-before the dedup delivers to the caller.
            for (tx, outcome) in results {
                if let (CasOutcome::Locked { .. }, Some(m)) = (&outcome, members.get(&tx)) {
                    self.core.record_shard_lock(&tx, prefix, idx, &m.intents);
                }
                if let Some(m) = members.get(&tx) {
                    *m.slot.lock().unwrap() = Some(outcome);
                }
            }
            return Ok(());
        }
        // Bounded CAS budget exhausted under churn: acquirers get Conflict (their
        // callers release and re-lock, ADR-024); releases / write-backs are
        // best-effort, so they report Released and leave any straggler holds to
        // lazy reclaim or lease expiry (ADR-026).
        for m in shard_members(batch)?.values() {
            let outcome = match m.action {
                ShardAction::Acquire => CasOutcome::Conflict,
                ShardAction::WriteBack | ShardAction::Release => CasOutcome::Released {
                    superseded: Vec::new(),
                },
            };
            *m.slot.lock().unwrap() = Some(outcome);
        }
        Ok(())
    }

    /// Drives one root membership round. Roots never merge, so the batch carries
    /// exactly one transaction; its outcome goes to `slot`. Acquire resolves the
    /// membership lock (wound-wait / hold-and-wait); release drops it (ADR-026).
    async fn run_root(
        &self,
        prefix: &str,
        id: TxId,
        action: RootAction,
        slot: OutcomeSlot,
    ) -> Result<(), TransError> {
        let outcome = match action {
            RootAction::Acquire => match self.lock_root_attempt(prefix, &id).await? {
                RootAttempt::Locked => CasOutcome::Locked { membership: false },
                RootAttempt::Conflict => CasOutcome::Conflict,
                RootAttempt::Wait(holder) => CasOutcome::Wait(holder),
            },
            RootAction::Release => {
                self.release_root_attempt(prefix, &id).await?;
                CasOutcome::Released {
                    superseded: Vec::new(),
                }
            }
        };
        *slot.lock().unwrap() = Some(outcome);
        Ok(())
    }

    /// Best-effort release of `id`'s root membership lock: reload and clear it,
    /// retrying only on CAS contention / in-doubt within the bounded budget
    /// (ADR-026). Idempotent — a root `id` no longer holds, or that is gone, is a
    /// no-op.
    async fn release_root_attempt(&self, prefix: &str, id: &TxId) -> Result<(), TransError> {
        let mut backoff = self.core.retry.backoff();
        for attempt in 0..CAS_RETRIES {
            if attempt > 0 {
                rt::sleep(backoff.next_delay()).await;
            }
            let (mut root, ver) = match self.core.shards.load_root(prefix).await {
                Ok(rv) => rv,
                Err(StorageError::NotFound) => return Ok(()),
                Err(e) => return Err(e.into()),
            };
            if !root.membership_locked_by().contains(id) {
                return Ok(());
            }
            root.clear_membership_lock();
            match self.core.shards.store_root(prefix, &root, &ver).await {
                Ok(true) => return Ok(()),
                // Precondition or in-doubt: reload and retry; clearing our own
                // membership lock is idempotent (ADR-009).
                Ok(false) => {}
                Err(StorageError::Unavailable(_)) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    /// One bounded CAS attempt at the root membership lock. Returns
    /// [`RootAttempt::Wait`] *without* writing when the membership lock is held
    /// by a live holder this transaction cannot wound, so the caller can wait
    /// and re-submit (hold-and-wait, ADR-024). Auto-creates the root if absent so
    /// a write that creates the collection's first key works without a prior
    /// explicit `create` (ADR-018).
    async fn lock_root_attempt(&self, prefix: &str, id: &TxId) -> Result<RootAttempt, TransError> {
        let mut backoff = self.core.retry.backoff();
        for attempt in 0..CAS_RETRIES {
            if attempt > 0 {
                rt::sleep(backoff.next_delay()).await;
            }
            let (mut root, ver) = match self.core.shards.load_root(prefix).await {
                Ok(rv) => rv,
                Err(StorageError::NotFound) => {
                    // The collection does not exist yet: create its root holding
                    // our membership lock. If we lose the create race, reload.
                    let mut root = CollectionRoot::new(glassdb_data::shard::SHARD_COUNT);
                    root.set_membership_lock(LockType::Write, [id.clone()]);
                    match self.core.shards.create_root(prefix, &root).await {
                        Ok(true) => {
                            self.core.record_root_lock(id, prefix);
                            return Ok(RootAttempt::Locked);
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
                if self.core.tmon.tx_status(&holder).await? == TxCommitStatus::Pending {
                    pending.push(holder);
                }
            }
            for holder in &pending {
                match self.try_reclaim(id, holder).await? {
                    Reclaim::Wounded => {}
                    Reclaim::Wait => return Ok(RootAttempt::Wait(holder.clone())),
                }
            }

            root.set_membership_lock(LockType::Write, [id.clone()]);
            match self.core.shards.store_root(prefix, &root, &ver).await {
                Ok(true) => {
                    self.core.record_root_lock(id, prefix);
                    return Ok(RootAttempt::Locked);
                }
                // Precondition or in-doubt: reload and retry; re-installing our
                // own membership lock is idempotent (ADR-009).
                Ok(false) => {}
                Err(StorageError::Unavailable(_)) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(RootAttempt::Conflict)
    }
}

#[async_trait]
impl Worker<CasReq, TransError> for CasWorker {
    async fn run(
        &self,
        _key: &str,
        batch: &BatchHandle<CasReq, TransError>,
    ) -> Result<(), TransError> {
        // The dedup key fixes the object kind (shard vs root paths never
        // collide), so the first merged snapshot selects the resolver.
        match batch.merged() {
            CasReq::Shard { prefix, idx, .. } => self.run_shard(&prefix, idx, batch).await,
            CasReq::Root {
                prefix,
                tx,
                action,
                slot,
            } => self.run_root(&prefix, tx, action, slot).await,
        }
    }
}

impl Locker {
    /// Creates a locker over the shared shard store and the transaction monitor.
    /// `retry` configures the exponential backoff applied both between CAS
    /// retries on a contended shard or root and between hold-and-wait re-polls of
    /// a conflicting holder, so neither contention nor a wait is ever
    /// busy-retried (its `max_interval` caps the wait re-poll cadence).
    pub fn new(shards: ShardStore, tmon: Monitor, retry: RetryConfig) -> Self {
        let resolver = Resolver::new(shards.clone(), tmon.clone());
        let core = Arc::new(LockerCore {
            tmon,
            shards,
            resolver,
            retry,
            tlocks: Sharded::new(|_| Mutex::new(HashMap::new())),
            stats: Stats::default(),
        });
        let dedup = Dedup::new(CasWorker { core: core.clone() });
        Locker {
            inner: Arc::new(LockerState { core, dedup }),
        }
    }

    /// Cancels in-flight lock work and awaits any spawned dedup owner tasks, so
    /// none leak when the database shuts down (ADR-025).
    pub async fn close(&self) {
        self.inner.dedup.close().await;
    }

    /// Returns and resets the accumulated lock statistics.
    pub fn stats_and_reset(&self) -> LockStats {
        LockStats {
            calls: self.inner.core.stats.n_calls.swap(0, Ordering::Relaxed) as usize,
            hits: self.inner.core.stats.n_hits.swap(0, Ordering::Relaxed) as usize,
            retries: self.inner.core.stats.n_retries.swap(0, Ordering::Relaxed) as usize,
        }
    }

    /// Returns a per-object dedup coordination snapshot (ADR-025): one entry per
    /// shard/root with an in-flight or queued lock batch, for operators
    /// investigating hangs.
    pub fn dedup_snapshot(&self) -> Vec<DedupKeySnapshot> {
        self.inner.dedup.snapshot()
    }

    /// Returns one entry per transaction that currently holds any shard/root
    /// lock, with the held paths sorted by path. Output is sorted by transaction
    /// id for stable display.
    pub fn tx_locks_snapshot(&self) -> Vec<TxLockSnapshot> {
        let mut out = Vec::new();
        self.inner.core.tlocks.each(|shard| {
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

    /// Groups the transaction's accessed keys by shard and acquires every lock it
    /// needs: the touched shards plus the collection roots for any membership
    /// change (create/delete). Returns a [`LockedTx`] handle to drive write-back
    /// on commit, or [`LockOutcome::Conflict`] when a CAS race was lost and the
    /// caller must release and re-lock under the same id.
    ///
    /// Read validation is **not** done here. The engine ([`super::algo::Algo`])
    /// validates reads *after* every touched key is locked and its value frozen
    /// (ADR-024); the locker is a pure locking mechanism.
    ///
    /// `serial` selects the sorted sequential fallback over the default parallel
    /// path (ADR-020).
    pub(crate) async fn lock(
        &self,
        id: &TxId,
        data: &Data,
        serial: bool,
    ) -> Result<LockOutcome, TransError> {
        let groups = build_groups(data)?;
        let membership = match self.lock_shards(id, &groups, serial).await? {
            ShardsOutcome::Locked(m) => m,
            ShardsOutcome::Conflict => return Ok(LockOutcome::Conflict),
        };
        for prefix in &membership {
            if !self.lock_root(prefix, id).await? {
                return Ok(LockOutcome::Conflict);
            }
        }
        Ok(LockOutcome::Locked(LockedTx { groups, membership }))
    }

    /// Releases every lock `id` holds across the shards and collection roots it
    /// has acquired, **without publishing any value** and **leaving the
    /// transaction object pending**. Unlike [`Locker::write_back`] (the
    /// post-commit release that republishes `current_writer` pointers), this
    /// just clears `id` from the lock holders so the transaction can re-acquire
    /// its locks from scratch under the same id.
    ///
    /// This is the deadlock-timeout serial fallback's release step (ADR-024):
    /// when a parallel acquisition blocks past the deadlock budget, the
    /// transaction drops the locks it grabbed out of order and re-acquires them
    /// in the global sorted order, where one contender always makes progress.
    /// Holding the out-of-order locks across the re-acquire would recreate the
    /// very cycle serial locking exists to break, so they must be released
    /// first. The held set is read from the per-tx bookkeeping the same way v1's
    /// `unlock_all` consulted `locked_paths`. Idempotent and best-effort.
    pub(crate) async fn release_locks(&self, id: &TxId) -> Result<(), TransError> {
        let held: Vec<String> = {
            let tlocks = self
                .inner
                .core
                .tlocks
                .for_key(id.as_bytes())
                .lock()
                .unwrap();
            let mut paths: Vec<String> = tlocks
                .get(id)
                .map(|m| m.keys().cloned().collect())
                .unwrap_or_default();
            // The bookkeeping is a `HashMap`, so sort to release in a
            // deterministic (ascending path) order: the simulation op-stream
            // oracle requires the backend CAS sequence to be reproducible.
            paths.sort();
            paths
        };
        for path in held {
            let pr = paths::parse(&path).map_err(|e| {
                TransError::with_source(format!("parsing held lock path {path:?}"), e)
            })?;
            match pr.typ {
                paths::Type::CollectionInfo => self.release_root(&pr.prefix, id).await?,
                paths::Type::Shard => {
                    let idx = pr.suffix.parse::<u32>().map_err(|_| {
                        TransError::other(format!("malformed shard lock path {path:?}"))
                    })?;
                    self.release_shard_locks(&pr.prefix, idx, id).await?;
                }
                // Only shards and roots carry transaction locks in v2.
                _ => {}
            }
        }
        self.inner.core.clear_tx_locks(id);
        Ok(())
    }

    /// Publishes `current_writer` pointers / tombstones and releases this
    /// transaction's locks across the shards it touched, then releases the root
    /// membership locks. Every CAS is idempotent; errors are best-effort
    /// (a failure leaves the locks to be reclaimed lazily by the next contender
    /// or lease expiry), so this never fails an already-committed transaction.
    ///
    /// Returns the transaction ids each published pointer *superseded* (the
    /// former `current_writer` an overwrite replaced): these just lost a
    /// reference and are GC write-back hint candidates (ADR-022).
    pub(crate) async fn write_back(&self, id: &TxId, locked: &LockedTx) -> Vec<TxId> {
        let mut superseded = Vec::new();
        for group in locked.groups.values() {
            if let Ok(mut s) = self.write_back_shard(id, group).await {
                superseded.append(&mut s);
            }
        }
        for prefix in &locked.membership {
            let _ = self.release_root(prefix, id).await;
        }
        self.inner.core.clear_tx_locks(id);
        superseded
    }

    /// Acquires this transaction's locks across every touched shard. Returns the
    /// collections whose root membership lock must still be taken (the shards
    /// that saw a create/delete), or [`ShardsOutcome::Conflict`] if a shard lost
    /// its bounded CAS race and the transaction must release and re-lock under
    /// the same id (the first conflicting shard wins, in deterministic shard-path
    /// order).
    async fn lock_shards(
        &self,
        id: &TxId,
        groups: &BTreeMap<String, ShardGroup>,
        serial: bool,
    ) -> Result<ShardsOutcome, TransError> {
        self.inner
            .core
            .stats
            .n_calls
            .fetch_add(1, Ordering::Relaxed);
        // The first lock for this transaction starts the background refresh so a
        // long-lived holder's pending object is written lazily, keeping its
        // lease alive (the tx object is otherwise written only at commit).
        if !groups.is_empty() {
            self.inner.core.tmon.start_refresh_tx(id);
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
                    ShardOutcome::Conflict => return Ok(ShardsOutcome::Conflict),
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
                    ShardOutcome::Conflict => return Ok(ShardsOutcome::Conflict),
                }
            }
        }
        Ok(ShardsOutcome::Locked(membership))
    }

    /// Installs this transaction's locks on every key it touches in one shard,
    /// through the shared [`Dedup`] (ADR-025): the submission merges with other
    /// transactions contending the same shard whenever they do not exclusively
    /// conflict, so one owner-driven load + CAS serves the whole batch.
    ///
    /// Hold-and-wait (ADR-024): if the worker reports [`CasOutcome::Wait`] — a
    /// key is held by a live holder this transaction cannot wound — it **waits**
    /// for that holder to finalize (keeping every lock already acquired on other
    /// shards) then re-submits. The wait is *not* charged to the bounded
    /// CAS-contention budget; the algo-level deadlock timeout bounds the total
    /// wait and escalates to the cannot-deadlock serial order.
    async fn lock_shard(&self, id: &TxId, group: &ShardGroup) -> Result<ShardOutcome, TransError> {
        let shard_path = paths::from_shard(&group.prefix, group.idx);
        let intents = Arc::new(group.intents.clone());
        // Paces the hold-and-wait re-poll. It advances across successive blind
        // polls of a holder that will not budge, and resets whenever a holder
        // finalizes — real progress.
        let mut backoff = self.inner.core.retry.backoff();
        loop {
            let slot: OutcomeSlot = Arc::new(Mutex::new(None));
            let mut members = BTreeMap::new();
            members.insert(
                id.clone(),
                ShardMember {
                    action: ShardAction::Acquire,
                    intents: intents.clone(),
                    slot: slot.clone(),
                },
            );
            let req = CasReq::Shard {
                prefix: group.prefix.clone(),
                idx: group.idx,
                members,
            };
            match self.inner.dedup.run(&shard_path, req).await {
                Ok(()) => {}
                Err(DedupError::Work(e)) => return Err((*e).clone()),
                Err(DedupError::Cancelled) => {
                    return Err(TransError::other("locker shut down while locking shard"));
                }
            }
            let outcome = slot.lock().unwrap().take();
            match outcome {
                Some(CasOutcome::Locked { membership }) => {
                    return Ok(ShardOutcome::Locked { membership });
                }
                Some(CasOutcome::Wait(holder)) => {
                    let delay = backoff.next_delay();
                    if let Woke::Finalized = self.wait_for_holder(&holder, delay).await {
                        backoff = self.inner.core.retry.backoff();
                    }
                }
                // `Released` cannot reach an acquire; a missing outcome means the
                // round was abandoned or shut down. Either way, report a conflict
                // for a safe release-and-relock.
                Some(CasOutcome::Conflict | CasOutcome::Released { .. }) | None => {
                    return Ok(ShardOutcome::Conflict);
                }
            }
        }
    }

    /// Parks until the conflicting `holder` finalizes **or** `timeout` elapses,
    /// whichever comes first, then lets the caller re-resolve, reporting which
    /// woke it.
    async fn wait_for_holder(&self, holder: &TxId, timeout: Duration) -> Woke {
        let wait = self.inner.core.tmon.wait_for_tx(holder);
        tokio::select! {
            _ = wait => Woke::Finalized,
            _ = rt::sleep(timeout) => Woke::PollTimeout,
        }
    }

    /// Acquires the collection root's membership write lock for `prefix`
    /// (ADR-018) through the shared [`Dedup`] (ADR-025). Roots take the single
    /// exclusive membership lock, so requests never merge — the dedup only
    /// serializes contenders through one owner, removing the CAS race. Auto-
    /// creates the root if absent so a write that creates the collection's first
    /// key works without a prior explicit `create`. Returns `false` if the
    /// transaction must restart.
    async fn lock_root(&self, prefix: &str, id: &TxId) -> Result<bool, TransError> {
        let root_path = paths::collection_info(prefix);
        // Same backed-off hold-and-wait re-poll as `lock_shard`.
        let mut backoff = self.inner.core.retry.backoff();
        loop {
            let slot: OutcomeSlot = Arc::new(Mutex::new(None));
            let req = CasReq::Root {
                prefix: prefix.to_string(),
                tx: id.clone(),
                action: RootAction::Acquire,
                slot: slot.clone(),
            };
            match self.inner.dedup.run(&root_path, req).await {
                Ok(()) => {}
                Err(DedupError::Work(e)) => return Err((*e).clone()),
                Err(DedupError::Cancelled) => {
                    return Err(TransError::other("locker shut down while locking root"));
                }
            }
            let outcome = slot.lock().unwrap().take();
            match outcome {
                Some(CasOutcome::Locked { .. }) => return Ok(true),
                Some(CasOutcome::Conflict | CasOutcome::Released { .. }) | None => {
                    return Ok(false);
                }
                Some(CasOutcome::Wait(holder)) => {
                    let delay = backoff.next_delay();
                    if let Woke::Finalized = self.wait_for_holder(&holder, delay).await {
                        backoff = self.inner.core.retry.backoff();
                    }
                }
            }
        }
    }

    /// Publishes `id`'s committed writes on a shard and drops its holds, batched
    /// through the shared [`Dedup`] (ADR-026): the write-back merges into any
    /// in-flight round for the shard (it never lock-conflicts), so N committers
    /// on one shard collapse to one CAS. Best-effort — a lost race leaves the
    /// holds to lazy reclaim / lease expiry. Returns the `current_writer`s it
    /// superseded, GC candidates (ADR-022).
    async fn write_back_shard(
        &self,
        id: &TxId,
        group: &ShardGroup,
    ) -> Result<Vec<TxId>, TransError> {
        self.submit_shard_unlock(
            &group.prefix,
            group.idx,
            id,
            ShardAction::WriteBack,
            Arc::new(group.intents.clone()),
        )
        .await
    }

    /// Removes `id` from the lock holders of every entry in shard `idx`,
    /// returning each entry to an unlocked state when `id` was its only holder
    /// (a shared read lock keeps its other holders). Publishes nothing — the
    /// transaction has not committed. Batched through the [`Dedup`] like
    /// write-back (ADR-026); idempotent and best-effort.
    async fn release_shard_locks(
        &self,
        prefix: &str,
        idx: u32,
        id: &TxId,
    ) -> Result<(), TransError> {
        self.submit_shard_unlock(prefix, idx, id, ShardAction::Release, Arc::new(Vec::new()))
            .await
            .map(|_| ())
    }

    /// Submits one release / write-back member for a shard through the [`Dedup`]
    /// and awaits it. The worker retries CAS contention / in-doubt internally and
    /// then reports done, so this is best-effort (ADR-026): a shutdown mid-flight
    /// simply leaves the holds to lease expiry, and only a genuine storage error
    /// surfaces. Returns the `current_writer`s a write-back overwrote — GC
    /// candidates (ADR-022), always empty for a release.
    async fn submit_shard_unlock(
        &self,
        prefix: &str,
        idx: u32,
        id: &TxId,
        action: ShardAction,
        intents: Arc<Vec<KeyIntent>>,
    ) -> Result<Vec<TxId>, TransError> {
        let shard_path = paths::from_shard(prefix, idx);
        let slot: OutcomeSlot = Arc::new(Mutex::new(None));
        let mut members = BTreeMap::new();
        members.insert(
            id.clone(),
            ShardMember {
                action,
                intents,
                slot: slot.clone(),
            },
        );
        let req = CasReq::Shard {
            prefix: prefix.to_string(),
            idx,
            members,
        };
        match self.inner.dedup.run(&shard_path, req).await {
            Ok(()) => Ok(match slot.lock().unwrap().take() {
                Some(CasOutcome::Released { superseded }) => superseded,
                _ => Vec::new(),
            }),
            Err(DedupError::Work(e)) => Err((*e).clone()),
            Err(DedupError::Cancelled) => Ok(Vec::new()),
        }
    }

    /// Releases `id`'s root membership lock through the [`Dedup`] (ADR-026).
    /// Roots never merge, so this only serializes the release through the root's
    /// owner — removing the CAS race with a concurrent acquire. Best-effort.
    async fn release_root(&self, prefix: &str, id: &TxId) -> Result<(), TransError> {
        let root_path = paths::collection_info(prefix);
        let slot: OutcomeSlot = Arc::new(Mutex::new(None));
        let req = CasReq::Root {
            prefix: prefix.to_string(),
            tx: id.clone(),
            action: RootAction::Release,
            slot,
        };
        match self.inner.dedup.run(&root_path, req).await {
            Ok(()) => Ok(()),
            Err(DedupError::Work(e)) => Err((*e).clone()),
            Err(DedupError::Cancelled) => Ok(()),
        }
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
    use glassdb_backend::middleware::{OpLog, RecordingBackend};
    use glassdb_backend::{Backend, BackendError, ReadReply, Version, memory::MemoryBackend};
    use glassdb_concurr::{Background, RetryConfig};
    use glassdb_data::paths;
    use glassdb_data::shard::shard_index;
    use glassdb_storage::{ObjectCache, Shard, SharedCache, TLogger, TxCommitStatus, ValueCache};
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;
    use tokio::sync::Notify;

    struct TlCtx {
        shards: ShardStore,
        monitor: Monitor,
        _bg: Arc<Background>,
    }

    fn new_test_locker(b: Arc<dyn Backend>) -> (Locker, TlCtx) {
        let cache = SharedCache::new(1024);
        let values = ValueCache::new(&cache);
        let objects = ObjectCache::new(b.clone(), &cache);
        let tl = TLogger::new(objects.clone(), "test");
        let bg = Arc::new(Background::new());
        let mon = Monitor::new(values.clone(), tl, Arc::downgrade(&bg));
        let shards = ShardStore::new(objects.clone());
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

    fn read_intent(key: &[u8]) -> KeyIntent {
        KeyIntent {
            raw_key: key.to_vec(),
            key_path: paths::from_key(COLL, key),
            desired: Desired::Read,
        }
    }

    fn put_intent(key: &[u8]) -> KeyIntent {
        KeyIntent {
            raw_key: key.to_vec(),
            key_path: paths::from_key(COLL, key),
            desired: Desired::Put,
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

    // Acquires shard locks in parallel mode, asserting success, and returns the
    // set of collections whose membership lock must still be taken.
    async fn lock_ok(
        locker: &Locker,
        id: &TxId,
        groups: &BTreeMap<String, ShardGroup>,
    ) -> BTreeSet<String> {
        match locker.lock_shards(id, groups, false).await.unwrap() {
            ShardsOutcome::Locked(m) => m,
            ShardsOutcome::Conflict => panic!("expected lock acquisition to succeed"),
        }
    }

    #[tokio::test]
    async fn lock_write_creates_entry() {
        let (locker, ctx) = init_tl_test();
        let key = b"key";
        let tx = mk_tid(1, "tx");
        ctx.monitor.begin_tx(&tx);

        let groups = group_of(key, put_intent(key));
        let membership = lock_ok(&locker, &tx, &groups).await;
        assert_eq!(membership, BTreeSet::from([COLL.to_string()]));

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

        lock_ok(&locker, &tx1, &group_of(key, read_intent(key))).await;
        lock_ok(&locker, &tx2, &group_of(key, read_intent(key))).await;

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
        lock_ok(&locker, &young, &group_of(key, put_intent(key))).await;

        let old = mk_tid(1, "old");
        ctx.monitor.begin_tx(&old);
        // The older tx wounds the younger holder and takes the lock immediately.
        lock_ok(&locker, &old, &group_of(key, put_intent(key))).await;

        let e = entry_of(&ctx, key).await.unwrap();
        assert_eq!(e.locked_by, vec![old.clone()]);
        assert_eq!(
            ctx.monitor.tx_status(&young).await.unwrap(),
            TxCommitStatus::Aborted
        );
    }

    // Hold-and-wait (ADR-024): a younger transaction cannot wound an older
    // holder, so it *waits* for it (keeping any other locks) and proceeds once
    // the holder finalizes — it never aborts on the conflict.
    #[tokio::test(start_paused = true)]
    async fn younger_waits_for_older_holder() {
        let (locker, ctx) = init_tl_test();
        let key = b"key";
        seed_committed(&ctx, key, b"v0").await;

        let old = mk_tid(1, "old");
        ctx.monitor.begin_tx(&old);
        lock_ok(&locker, &old, &group_of(key, put_intent(key))).await;

        // Drive the younger lock concurrently; it must block while `old` holds.
        let young = mk_tid(2, "young");
        ctx.monitor.begin_tx(&young);
        let locker2 = locker.clone();
        let young2 = young.clone();
        let groups = group_of(key, put_intent(key));
        let waiting =
            tokio::spawn(async move { locker2.lock_shards(&young2, &groups, false).await });

        // Under paused time the sleep only auto-advances once every task is
        // idle, so it lands with `young` parked waiting on `old`.
        rt::sleep(Duration::from_millis(50)).await;
        assert!(
            !waiting.is_finished(),
            "younger must wait for the older holder, not conflict"
        );

        // Finalizing `old` releases `young`, which reloads and takes the lock.
        ctx.monitor.abort_tx(&old).await.unwrap();
        let outcome = waiting.await.unwrap().unwrap();
        assert!(
            matches!(outcome, ShardsOutcome::Locked(_)),
            "younger proceeds once the holder finalizes"
        );

        let e = entry_of(&ctx, key).await.unwrap();
        assert_eq!(e.locked_by, vec![young.clone()]);
    }

    // ADR-024: after waiting, a younger transaction help-forwards a holder that
    // *commits* (rather than aborts) — taking the lock over the holder's now
    // committed value.
    #[tokio::test(start_paused = true)]
    async fn younger_proceeds_after_older_holder_commits() {
        use glassdb_storage::{TxLog, TxWrite};
        let (locker, ctx) = init_tl_test();
        let key = b"key";
        seed_committed(&ctx, key, b"v0").await;

        let old = mk_tid(1, "old");
        ctx.monitor.begin_tx(&old);
        let old_groups = group_of(key, put_intent(key));
        let old_membership = lock_ok(&locker, &old, &old_groups).await;

        // Younger contender blocks waiting for `old`.
        let young = mk_tid(2, "young");
        ctx.monitor.begin_tx(&young);
        let locker2 = locker.clone();
        let young2 = young.clone();
        let groups = group_of(key, put_intent(key));
        let waiting =
            tokio::spawn(async move { locker2.lock_shards(&young2, &groups, false).await });

        rt::sleep(Duration::from_millis(50)).await;
        assert!(
            !waiting.is_finished(),
            "younger must wait for the older holder"
        );

        // `old` commits its write, then publishes the pointer and releases.
        let mut tl = TxLog::new(old.clone(), TxCommitStatus::Ok);
        tl.writes = vec![TxWrite {
            path: paths::from_key(COLL, key),
            value: Arc::from(&b"v1"[..]),
            deleted: false,
            prev_writer: TxId::default(),
        }];
        ctx.monitor.commit_tx(tl).await.unwrap();
        locker
            .write_back(
                &old,
                &LockedTx {
                    groups: old_groups,
                    membership: old_membership,
                },
            )
            .await;

        let outcome = waiting.await.unwrap().unwrap();
        assert!(
            matches!(outcome, ShardsOutcome::Locked(_)),
            "younger proceeds once the holder commits"
        );

        let e = entry_of(&ctx, key).await.unwrap();
        assert_eq!(e.locked_by, vec![young.clone()]);
        // The committed writer was help-forwarded as the effective value.
        assert_eq!(e.current_writer, Some(old.clone()));
    }

    #[tokio::test]
    async fn write_back_publishes_and_releases() {
        let (locker, ctx) = init_tl_test();
        let key = b"key";
        let tx = mk_tid(1, "tx");
        ctx.monitor.begin_tx(&tx);

        let groups = group_of(key, put_intent(key));
        let membership = lock_ok(&locker, &tx, &groups).await;
        // First writer of a fresh key overwrites no pointer: no GC hint.
        let superseded = locker
            .write_back(&tx, &LockedTx { groups, membership })
            .await;
        assert!(superseded.is_empty());

        let e = entry_of(&ctx, key).await.unwrap();
        assert_eq!(e.lock_type, LockType::None);
        assert!(e.locked_by.is_empty());
        assert_eq!(e.current_writer, Some(tx.clone()));
        assert!(locker.tx_locks_snapshot().is_empty());
    }

    // Write-back over an existing key returns the `current_writer` it overwrote:
    // that txid just lost its reference and is the GC candidate hint (ADR-022).
    #[tokio::test]
    async fn write_back_returns_superseded_writer() {
        let (locker, ctx) = init_tl_test();
        let key = b"key";

        // First committer publishes the pointer for `key`; it supersedes nothing.
        let old = mk_tid(1, "old");
        let lt_old = lock_commit(&locker, &ctx, &old, key).await;
        assert!(locker.write_back(&old, &lt_old).await.is_empty());
        assert_eq!(
            entry_of(&ctx, key).await.unwrap().current_writer,
            Some(old.clone())
        );

        // A second committer overwrites the same key; its write-back reports the
        // pointer it replaced.
        let new = mk_tid(2, "new");
        let lt_new = lock_commit(&locker, &ctx, &new, key).await;
        assert_eq!(locker.write_back(&new, &lt_new).await, vec![old]);
        assert_eq!(entry_of(&ctx, key).await.unwrap().current_writer, Some(new));
    }

    // The deadlock-timeout serial fallback releases held locks *without*
    // publishing a value (the transaction has not committed), leaving the tx
    // pending so it can re-acquire under the same id (ADR-024).
    #[tokio::test]
    async fn release_locks_drops_held_locks_without_publishing() {
        let (locker, ctx) = init_tl_test();
        let key = b"key";
        let tx = mk_tid(1, "tx");
        ctx.monitor.begin_tx(&tx);

        // A blind put locks the key's shard and the collection root (membership).
        let data = Data {
            reads: Vec::new(),
            writes: vec![crate::algo::WriteAccess::put(
                paths::from_key(COLL, key).into(),
                Arc::from(&b"v"[..]),
            )],
        };
        let out = locker.lock(&tx, &data, false).await.unwrap();
        assert!(matches!(out, LockOutcome::Locked(_)));
        assert!(!locker.tx_locks_snapshot().is_empty());

        locker.release_locks(&tx).await.unwrap();

        let e = entry_of(&ctx, key).await.unwrap();
        assert!(e.locked_by.is_empty(), "shard lock released");
        assert_eq!(e.lock_type, LockType::None);
        assert_eq!(e.current_writer, None, "a release publishes no value");

        let (root, _) = ctx.shards.load_root(COLL).await.unwrap();
        assert!(
            root.membership_locked_by().is_empty(),
            "root membership lock released"
        );
        assert!(locker.tx_locks_snapshot().is_empty());
    }

    #[tokio::test]
    async fn tx_locks_snapshot_lists_held_shards() {
        let (locker, ctx) = init_tl_test();
        let key = b"key";
        let tx = mk_tid(1, "tx");
        ctx.monitor.begin_tx(&tx);

        lock_ok(&locker, &tx, &group_of(key, put_intent(key))).await;

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

    // --- ADR-025: cross-transaction lock-acquisition deduplication ----------

    /// Test backend that, while **armed**, blocks the next `read` on a gate until
    /// released — so a test can park the dedup driver mid-load while other
    /// contenders queue, forcing them into one merged CAS round. Every other call
    /// passes through. Arming is deferred (`arm`) so a test can run un-gated setup
    /// first, then gate only the phase under test.
    struct GateBackend {
        inner: Arc<dyn Backend>,
        gate: Arc<Notify>,
        armed: AtomicBool,
    }

    impl GateBackend {
        fn new(inner: Arc<dyn Backend>, armed: bool) -> Arc<Self> {
            Arc::new(GateBackend {
                inner,
                gate: Arc::new(Notify::new()),
                armed: AtomicBool::new(armed),
            })
        }
        /// Gate the next read until [`Self::release`].
        fn arm(&self) {
            self.armed.store(true, Ordering::SeqCst);
        }
        /// Wake the read parked by the gate.
        fn release(&self) {
            self.gate.notify_one();
        }
    }

    #[async_trait]
    impl Backend for GateBackend {
        async fn read(&self, path: &str) -> Result<ReadReply, BackendError> {
            if self.armed.swap(false, Ordering::SeqCst) {
                self.gate.notified().await;
            }
            self.inner.read(path).await
        }
        async fn read_if_modified(
            &self,
            path: &str,
            expected: &Version,
        ) -> Result<ReadReply, BackendError> {
            // Gate the cache-revalidation path too: after un-gated setup warms
            // the object cache, a shard reload arrives here rather than as a cold
            // `read`, so a deferred-arm test must park it as well.
            if self.armed.swap(false, Ordering::SeqCst) {
                self.gate.notified().await;
            }
            self.inner.read_if_modified(path, expected).await
        }
        async fn write(&self, path: &str, value: Vec<u8>) -> Result<Version, BackendError> {
            self.inner.write(path, value).await
        }
        async fn write_if(
            &self,
            path: &str,
            value: Vec<u8>,
            expected: &Version,
        ) -> Result<Version, BackendError> {
            self.inner.write_if(path, value, expected).await
        }
        async fn write_if_not_exists(
            &self,
            path: &str,
            value: Vec<u8>,
        ) -> Result<Version, BackendError> {
            self.inner.write_if_not_exists(path, value).await
        }
        async fn delete(&self, path: &str) -> Result<(), BackendError> {
            self.inner.delete(path).await
        }
        async fn list(&self, dir_path: &str) -> Result<Vec<String>, BackendError> {
            self.inner.list(dir_path).await
        }
    }

    /// A locker whose backend records ops and gates the first read.
    fn gated_locker() -> (Locker, TlCtx, OpLog, Arc<GateBackend>) {
        gated_locker_with(true)
    }

    /// As [`gated_locker`], but `armed` chooses whether the gate is active from
    /// the start (gate acquisition) or deferred until `arm` (gate a later phase,
    /// e.g. write-back, after un-gated setup).
    fn gated_locker_with(armed: bool) -> (Locker, TlCtx, OpLog, Arc<GateBackend>) {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let gate = GateBackend::new(mem, armed);
        let recorder = Arc::new(RecordingBackend::new(gate.clone() as Arc<dyn Backend>));
        let log = recorder.log();
        let (locker, ctx) = new_test_locker(recorder);
        (locker, ctx, log, gate)
    }

    /// A locker whose backend records ops (no gating).
    fn recording_locker() -> (Locker, TlCtx, OpLog) {
        let recorder = Arc::new(RecordingBackend::new(Arc::new(MemoryBackend::new())));
        let log = recorder.log();
        let (locker, ctx) = new_test_locker(recorder);
        (locker, ctx, log)
    }

    /// Counts the CAS stores (create or conditional write) issued against `path`.
    fn count_stores(log: &OpLog, path: &str) -> usize {
        log.lock()
            .unwrap()
            .iter()
            .filter(|r| r.path == path && (r.op == "write_if" || r.op == "write_if_not_exists"))
            .count()
    }

    /// A distinct key that hashes to the same shard as `base`, for exercising
    /// disjoint-key contention within a single shard object.
    fn same_shard_sibling(base: &[u8]) -> Vec<u8> {
        let idx = shard_index(base);
        for i in 0u32.. {
            let k = format!("sib-{i}").into_bytes();
            if k != base && shard_index(&k) == idx {
                return k;
            }
        }
        unreachable!("a same-shard sibling must exist")
    }

    // Two concurrent read-lockers on one key merge into a single CAS round: one
    // load + one store serves both, and both end up holding the shared read lock.
    #[tokio::test(start_paused = true)]
    async fn concurrent_readers_share_one_cas() {
        let (locker, ctx, log, gate) = gated_locker();
        let key = b"key";
        let tx1 = mk_tid(1, "r1");
        let tx2 = mk_tid(2, "r2");
        ctx.monitor.begin_tx(&tx1);
        ctx.monitor.begin_tx(&tx2);

        let (l1, l2) = (locker.clone(), locker.clone());
        let (t1, t2) = (tx1.clone(), tx2.clone());
        let g1 = group_of(key, read_intent(key));
        let g2 = group_of(key, read_intent(key));
        let h1 = tokio::spawn(async move { l1.lock_shards(&t1, &g1, false).await });
        let h2 = tokio::spawn(async move { l2.lock_shards(&t2, &g2, false).await });

        // Under paused time this sleep only fires once both tasks are parked (the
        // driver in the gated load, the second queued); then release the load.
        rt::sleep(Duration::from_millis(50)).await;
        gate.release();

        assert!(matches!(
            h1.await.unwrap().unwrap(),
            ShardsOutcome::Locked(_)
        ));
        assert!(matches!(
            h2.await.unwrap().unwrap(),
            ShardsOutcome::Locked(_)
        ));

        let shard_path = paths::from_shard(COLL, shard_index(key));
        assert_eq!(
            count_stores(&log, &shard_path),
            1,
            "two readers must share a single CAS"
        );
        let e = entry_of(&ctx, key).await.unwrap();
        assert_eq!(e.lock_type, LockType::Read);
        assert_eq!(e.locked_by.len(), 2, "both readers hold the shared lock");
    }

    // Two concurrent writers on *disjoint* keys of the same shard do not conflict,
    // so they batch into one CAS round rather than each doing its own load+store.
    #[tokio::test(start_paused = true)]
    async fn concurrent_disjoint_writers_share_one_cas() {
        let (locker, ctx, log, gate) = gated_locker();
        let ka = b"key-a".to_vec();
        let kb = same_shard_sibling(&ka);
        let tx1 = mk_tid(1, "w1");
        let tx2 = mk_tid(2, "w2");
        ctx.monitor.begin_tx(&tx1);
        ctx.monitor.begin_tx(&tx2);

        let (l1, l2) = (locker.clone(), locker.clone());
        let (t1, t2) = (tx1.clone(), tx2.clone());
        let g1 = group_of(&ka, put_intent(&ka));
        let g2 = group_of(&kb, put_intent(&kb));
        let h1 = tokio::spawn(async move { l1.lock_shards(&t1, &g1, false).await });
        let h2 = tokio::spawn(async move { l2.lock_shards(&t2, &g2, false).await });

        rt::sleep(Duration::from_millis(50)).await;
        gate.release();

        assert!(matches!(
            h1.await.unwrap().unwrap(),
            ShardsOutcome::Locked(_)
        ));
        assert!(matches!(
            h2.await.unwrap().unwrap(),
            ShardsOutcome::Locked(_)
        ));

        let shard_path = paths::from_shard(COLL, shard_index(&ka));
        assert_eq!(
            count_stores(&log, &shard_path),
            1,
            "disjoint writers batch into one CAS"
        );
        assert_eq!(entry_of(&ctx, &ka).await.unwrap().locked_by, vec![tx1]);
        assert_eq!(entry_of(&ctx, &kb).await.unwrap().locked_by, vec![tx2]);
    }

    // Locks + commits `key` for `tx`, leaving the shard entry holding the write
    // lock, so a later `write_back` publishes it. Returns the acquired handle.
    async fn lock_commit(locker: &Locker, ctx: &TlCtx, tx: &TxId, key: &[u8]) -> LockedTx {
        use glassdb_storage::{TxLog, TxWrite};
        ctx.monitor.begin_tx(tx);
        let groups = group_of(key, put_intent(key));
        let membership = lock_ok(locker, tx, &groups).await;
        let mut tl = TxLog::new(tx.clone(), TxCommitStatus::Ok);
        tl.writes = vec![TxWrite {
            path: paths::from_key(COLL, key),
            value: Arc::from(&b"v"[..]),
            deleted: false,
            prev_writer: TxId::default(),
        }];
        ctx.monitor.commit_tx(tl).await.unwrap();
        LockedTx { groups, membership }
    }

    // Two committed transactions writing *disjoint* keys of one shard write back
    // concurrently. Write-backs never lock-conflict, so they merge into a single
    // CAS round (ADR-026) that publishes both pointers and drops both holds.
    #[tokio::test(start_paused = true)]
    async fn concurrent_write_backs_share_one_cas() {
        // Gate is deferred so the un-gated lock+commit setup runs first.
        let (locker, ctx, log, gate) = gated_locker_with(false);
        let ka = b"key-a".to_vec();
        let kb = same_shard_sibling(&ka);
        let shard_path = paths::from_shard(COLL, shard_index(&ka));

        let tx1 = mk_tid(1, "w1");
        let tx2 = mk_tid(2, "w2");
        let lt1 = lock_commit(&locker, &ctx, &tx1, &ka).await;
        let lt2 = lock_commit(&locker, &ctx, &tx2, &kb).await;

        // Gate only the write-back phase and count the stores it adds.
        let before = count_stores(&log, &shard_path);
        gate.arm();
        let (l1, l2) = (locker.clone(), locker.clone());
        let (t1, t2) = (tx1.clone(), tx2.clone());
        let h1 = tokio::spawn(async move { l1.write_back(&t1, &lt1).await });
        let h2 = tokio::spawn(async move { l2.write_back(&t2, &lt2).await });
        rt::sleep(Duration::from_millis(50)).await;
        gate.release();
        h1.await.unwrap();
        h2.await.unwrap();

        assert_eq!(
            count_stores(&log, &shard_path) - before,
            1,
            "two write-backs on one shard share a single CAS"
        );
        let ea = entry_of(&ctx, &ka).await.unwrap();
        assert_eq!(ea.current_writer, Some(tx1.clone()));
        assert!(ea.locked_by.is_empty());
        let eb = entry_of(&ctx, &kb).await.unwrap();
        assert_eq!(eb.current_writer, Some(tx2.clone()));
        assert!(eb.locked_by.is_empty());
    }

    // A write-back reorders into a concurrent acquire round for the same shard on
    // a disjoint key (ADR-026): one CAS both publishes the committer's pointer and
    // installs the new acquirer's lock.
    #[tokio::test(start_paused = true)]
    async fn write_back_folds_into_acquire_round() {
        let (locker, ctx, log, gate) = gated_locker_with(false);
        let ka = b"key-a".to_vec();
        let kb = same_shard_sibling(&ka);
        let shard_path = paths::from_shard(COLL, shard_index(&ka));

        let tx1 = mk_tid(1, "w1");
        let lt1 = lock_commit(&locker, &ctx, &tx1, &ka).await;
        let tx2 = mk_tid(2, "w2");
        ctx.monitor.begin_tx(&tx2);
        let g2 = group_of(&kb, put_intent(&kb));

        let before = count_stores(&log, &shard_path);
        gate.arm();
        let (l1, l2) = (locker.clone(), locker.clone());
        let (t1, t2) = (tx1.clone(), tx2.clone());
        // The write-back is the driver (parks in the gated load); the acquire
        // queues and is absorbed once the load returns.
        let hw = tokio::spawn(async move { l1.write_back(&t1, &lt1).await });
        let ha = tokio::spawn(async move { l2.lock_shards(&t2, &g2, false).await });
        rt::sleep(Duration::from_millis(50)).await;
        gate.release();
        hw.await.unwrap();
        assert!(matches!(
            ha.await.unwrap().unwrap(),
            ShardsOutcome::Locked(_)
        ));

        assert_eq!(
            count_stores(&log, &shard_path) - before,
            1,
            "the write-back folds into the acquire's CAS round"
        );
        assert_eq!(entry_of(&ctx, &ka).await.unwrap().current_writer, Some(tx1));
        assert_eq!(entry_of(&ctx, &kb).await.unwrap().locked_by, vec![tx2]);
    }

    // Two transactions releasing disjoint keys of one shard (the serial-fallback
    // release path) batch into one CAS round (ADR-026); neither publishes a value.
    #[tokio::test(start_paused = true)]
    async fn concurrent_releases_share_one_cas() {
        let (locker, ctx, log, gate) = gated_locker_with(false);
        let ka = b"key-a".to_vec();
        let kb = same_shard_sibling(&ka);
        let shard_path = paths::from_shard(COLL, shard_index(&ka));

        let tx1 = mk_tid(1, "r1");
        let tx2 = mk_tid(2, "r2");
        ctx.monitor.begin_tx(&tx1);
        ctx.monitor.begin_tx(&tx2);
        lock_ok(&locker, &tx1, &group_of(&ka, put_intent(&ka))).await;
        lock_ok(&locker, &tx2, &group_of(&kb, put_intent(&kb))).await;

        let before = count_stores(&log, &shard_path);
        gate.arm();
        let (l1, l2) = (locker.clone(), locker.clone());
        let (t1, t2) = (tx1.clone(), tx2.clone());
        let h1 = tokio::spawn(async move { l1.release_locks(&t1).await });
        let h2 = tokio::spawn(async move { l2.release_locks(&t2).await });
        rt::sleep(Duration::from_millis(50)).await;
        gate.release();
        h1.await.unwrap().unwrap();
        h2.await.unwrap().unwrap();

        assert_eq!(
            count_stores(&log, &shard_path) - before,
            1,
            "two releases on one shard share a single CAS"
        );
        let ea = entry_of(&ctx, &ka).await.unwrap();
        assert!(ea.locked_by.is_empty());
        assert_eq!(ea.current_writer, None, "a release publishes no value");
        assert!(entry_of(&ctx, &kb).await.unwrap().locked_by.is_empty());
    }

    // Two writers on the *same* key exclusively conflict, so they cannot share a
    // CAS: each takes its own round (two stores) and wound-wait orders them.
    #[tokio::test]
    async fn same_key_writers_do_not_merge() {
        let (locker, ctx, log) = recording_locker();
        let key = b"key";
        let young = mk_tid(2, "young");
        let old = mk_tid(1, "old");
        ctx.monitor.begin_tx(&young);
        ctx.monitor.begin_tx(&old);

        lock_ok(&locker, &young, &group_of(key, put_intent(key))).await;
        // The older tx wounds the younger holder in its own round.
        lock_ok(&locker, &old, &group_of(key, put_intent(key))).await;

        let shard_path = paths::from_shard(COLL, shard_index(key));
        assert_eq!(
            count_stores(&log, &shard_path),
            2,
            "same-key writers each take their own CAS"
        );
        let e = entry_of(&ctx, key).await.unwrap();
        assert_eq!(e.locked_by, vec![old.clone()]);
        assert_eq!(
            ctx.monitor.tx_status(&young).await.unwrap(),
            TxCommitStatus::Aborted
        );
    }

    // `close` cancels new submissions; the dedup snapshot tracks only live
    // coordination, so it is empty while idle and after an uncontended lock.
    #[tokio::test]
    async fn close_cancels_new_locks_and_snapshot_tracks_idle() {
        let (locker, ctx) = init_tl_test();
        assert!(
            locker.dedup_snapshot().is_empty(),
            "no coordination while idle"
        );

        let tx = mk_tid(1, "tx");
        ctx.monitor.begin_tx(&tx);
        lock_ok(&locker, &tx, &group_of(b"key", put_intent(b"key"))).await;
        assert!(
            locker.dedup_snapshot().is_empty(),
            "an uncontended lock leaves no dedup key behind"
        );

        locker.close().await;
        let err = locker
            .lock_shards(&tx, &group_of(b"key2", put_intent(b"key2")), false)
            .await;
        assert!(err.is_err(), "locking after close is cancelled");
    }

    // Dropping a waiting lock future mid-wait (the deadlock-timeout analog) must
    // not wedge the locker: the holder can still release and a fresh transaction
    // acquires the key without hanging.
    #[tokio::test(start_paused = true)]
    async fn dropped_waiter_leaves_locker_usable() {
        let (locker, ctx) = init_tl_test();
        let key = b"key";
        seed_committed(&ctx, key, b"v0").await;

        let old = mk_tid(1, "old");
        ctx.monitor.begin_tx(&old);
        lock_ok(&locker, &old, &group_of(key, put_intent(key))).await;

        let young = mk_tid(2, "young");
        ctx.monitor.begin_tx(&young);
        let l = locker.clone();
        let y = young.clone();
        let g = group_of(key, put_intent(key));
        let waiting = tokio::spawn(async move { l.lock_shards(&y, &g, false).await });
        rt::sleep(Duration::from_millis(50)).await;
        assert!(!waiting.is_finished(), "younger blocks on the older holder");
        waiting.abort();
        let _ = waiting.await;

        locker.release_locks(&old).await.unwrap();
        let other = mk_tid(3, "other");
        ctx.monitor.begin_tx(&other);
        lock_ok(&locker, &other, &group_of(key, put_intent(key))).await;
        assert_eq!(entry_of(&ctx, key).await.unwrap().locked_by, vec![other]);
    }
}
