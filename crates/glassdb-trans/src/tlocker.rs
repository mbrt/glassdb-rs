//! Distributed locking **policy** over the v2 shard/root coordination objects
//! (ADR-017, ADR-020, ADR-024). Ported in spirit from the Go
//! `internal/trans/tlocker.go`, but re-keyed from per-key objects onto shards.
//!
//! A transaction groups its accessed keys by shard and locks each shard with a
//! single read-modify-write CAS: resolve every touched key's holders
//! (help-forward committed holders, drop aborted ones, wound-wait the live
//! pending ones), install this transaction's locks, then CAS the shard back.
//! Create/delete additionally take the owning leaf's membership-write lock,
//! while every coordination-object mutation holds its structure-read lock
//! (ADR-032). Write-back republishes `current_writer` pointers and releases all
//! three scopes together.
//!
//! The [`Locker`] owns the *policy*: how a transaction groups its keys, the
//! parallel/serial acquisition strategy, the hold-and-wait loop, and the
//! per-transaction held-lock bookkeeping (which shards a transaction
//! holds, for release and diagnostics). The *mechanism* — deduplicated load +
//! resolve + CAS with retry — lives in the
//! [`ShardCoordinator`](crate::shard_coord::ShardCoordinator) below it, which
//! the locker shares with the commit algorithm so every shard/root mutation
//! flows through one place (ADR-028).
//!
//! Lock acquisition has two modes (ADR-020): the default **parallel** path locks
//! every touched shard concurrently; the **serial** fallback locks them one at a
//! time in ascending shard path order so equal-priority contenders queue on the
//! lowest contended shard and exactly one wins it (first-CAS-wins), guaranteeing
//! progress where the parallel path could livelock.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::join_all;
use glassdb_concurr::{RetryConfig, rt, shard::Sharded};
use glassdb_data::{TxId, paths};
use glassdb_storage::{
    Directory, LeafObservation, LockScope, LockType, NodeLocks, PathLock, Requirement, ShardEntry,
};

use crate::algo::{Data, WriteOp};
use crate::error::TransError;
use crate::monitor::Monitor;
use crate::node_locking::{Reclaim, ReconciledLeaf, resolve_entry_locks, try_reclaim};
use crate::shard_coord::{
    FoldOutcome, ResolveCtx, ShardCoordinator, ShardResolver, StageAdmission, Step,
};

/// One independent partition of the per-transaction held-lock bookkeeping: the
/// shard/root paths each transaction holds and their lock type.
type LockerShard = Mutex<HashMap<TxId, HashMap<String, HeldLeaf>>>;

#[derive(Clone, Copy)]
struct HeldLeaf {
    typ: LockType,
    membership: LockType,
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

/// The keys a transaction touches in one leaf, plus the leaf's location
/// (ADR-031).
struct ShardGroup {
    /// The leaf's object path: the collection root `_i` for a small collection's
    /// single leaf, else a standalone node `_n`, resolved by descent. This is
    /// the coordinator submit target and the recorded held-lock path.
    path: String,
    /// Per-key intentions, in ascending raw-key order.
    intents: Vec<KeyIntent>,
    membership: LockType,
}

/// The locks a transaction acquired, returned by [`Locker::lock`] and consumed
/// by [`Locker::write_back`]. Opaque to the caller: it carries the per-leaf key
/// groups this transaction holds.
pub(crate) struct LockedTx {
    groups: BTreeMap<String, ShardGroup>,
    validations: Vec<LeafObservation>,
}

impl LockedTx {
    /// Reports whether this transaction's successful lock CAS validated the
    /// exact leaf state that was observed earlier.
    pub(crate) fn validated(&self, observed: &LeafObservation) -> bool {
        self.validations
            .iter()
            .any(|validated| validated.same_state(observed))
    }

    /// The per-key paths this transaction holds, as the `PathLock` set GC records
    /// on the transaction object for its reverse liveness check and lock pruning
    /// (ADR-022). Keys map to their `_k/` path; GC ignores the lock type, so it
    /// is only kept faithful for diagnostics.
    pub(crate) fn locked_paths(&self) -> Vec<PathLock> {
        let mut out = Vec::new();
        for group in self.groups.values() {
            for intent in &group.intents {
                out.push(PathLock {
                    path: intent.key_path.clone(),
                    typ: lock_type(intent.desired),
                    scope: LockScope::Entry,
                });
            }
            out.push(PathLock {
                path: group.path.clone(),
                typ: LockType::Read,
                scope: LockScope::Structure,
            });
            if group.membership != LockType::None {
                out.push(PathLock {
                    path: group.path.clone(),
                    typ: group.membership,
                    scope: LockScope::Membership,
                });
            }
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

fn lock_scope_order(scope: LockScope) -> u8 {
    match scope {
        LockScope::Entry => 0,
        LockScope::Structure => 1,
        LockScope::Membership => 2,
    }
}

/// Groups a transaction's accessed keys by their owning leaf, descending the
/// collection directory (ADR-031). Each key gets one intent carrying the lock to
/// install: a write/create/delete for a written key, a read lock for a key only
/// read. Optimistic read validation is the engine's job (it validates after
/// locking, ADR-024), so no read token is carried here.
async fn build_groups(
    dir: &Directory,
    data: &Data,
    scan_requirement: Requirement,
) -> Result<BTreeMap<String, ShardGroup>, TransError> {
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

    // Collect before descending so the returned future does not close over a
    // borrowing iterator (which would not be higher-ranked / `Send` when a
    // caller spawns the lock).
    let items: Vec<(String, Desired)> = by_path.into_iter().collect();
    // Route with interior nodes served from cache (ADR-031 hot-path invariant):
    // a stale index misroute self-corrects via right-links, and the leaf's own
    // coordination CAS revalidates at the version, so neither the root `_i` nor
    // the terminal leaf needs a separate validation read.
    let grouped = dir
        .group_keys_by_leaf_fresh(items, Requirement::Any, Requirement::Any)
        .await
        .map_err(|e| TransError::with_source("grouping keys by leaf", e))?;

    let mut groups: BTreeMap<String, ShardGroup> = BTreeMap::new();
    for group in grouped {
        let prefix = paths::parse(&group.path)
            .map_err(|e| TransError::with_source(format!("parsing leaf path {:?}", group.path), e))?
            .prefix;
        let mut intents: Vec<KeyIntent> = group
            .keys
            .into_iter()
            .map(|(raw_key, desired)| KeyIntent {
                key_path: paths::from_key(&prefix, &raw_key),
                raw_key,
                desired,
            })
            .collect();
        intents.sort_by(|a, b| a.raw_key.cmp(&b.raw_key));
        groups.insert(
            group.path.clone(),
            ShardGroup {
                path: group.path,
                intents,
                membership: LockType::None,
            },
        );
    }
    // Lock the current cover, not the body's earlier cover. If a split moved
    // the range before locking, validation reconciles the logical page while
    // the new leaves are protected.
    for scan in &data.scans {
        if scan.range.is_empty() {
            continue;
        }
        for leaf in dir
            .leaves_through(
                &scan.prefix,
                &scan.range.start,
                scan.frontier.as_deref(),
                scan_requirement,
            )
            .await?
        {
            let group = groups
                .entry(leaf.path.clone())
                .or_insert_with(|| ShardGroup {
                    path: leaf.path,
                    intents: Vec::new(),
                    membership: LockType::None,
                });
            if group.membership == LockType::None {
                group.membership = LockType::Read;
            }
        }
    }
    Ok(groups)
}

// --- Shard resolvers (the locking policy the Locker installs, ADR-028) ------

/// Acquires locks on its keys: resolve every key's holders (help-forward
/// committed, drop aborted, wound-wait the live pending ones) and install this
/// transaction's lock (ADR-024).
struct AcquireResolver {
    id: TxId,
    path: String,
    intents: Arc<Vec<KeyIntent>>,
    membership: LockType,
}

#[async_trait]
impl ShardResolver for AcquireResolver {
    async fn resolve(
        &self,
        ctx: &ResolveCtx<'_>,
        staged: &BTreeMap<Vec<u8>, ShardEntry>,
        staged_locks: &NodeLocks,
    ) -> Result<Step, TransError> {
        let mut leaf = ReconciledLeaf::new(ctx, &self.path, &self.id, staged, staged_locks).await?;
        let mut membership = self.membership;
        let mut admission = StageAdmission::ExistingKeys;
        for intent in self.intents.iter() {
            let cur = leaf.entry(&intent.raw_key).cloned();
            match resolve_and_lock(ctx, &self.id, intent, cur).await? {
                EntryResolution::Locked(entry, changes_membership) => {
                    if changes_membership {
                        membership = LockType::Write;
                        if intent.desired == Desired::Put {
                            admission = StageAdmission::AddsKey;
                        }
                    }
                    leaf.insert_entry(intent.raw_key.clone(), entry);
                }
                // A member stages all its keys or none (member atomicity): the
                // moment a key must wait, stage nothing and return Wait.
                EntryResolution::Wait(holder) => {
                    return Ok(Step::Skip {
                        outcome: FoldOutcome::Wait(holder),
                    });
                }
            }
        }

        if let Some(holder) = leaf.acquire_mutation_locks(ctx, membership).await? {
            return Ok(Step::Skip {
                outcome: FoldOutcome::Wait(holder),
            });
        }
        if membership != LockType::None {
            membership = leaf.membership_lock_type();
        }
        let (entries, locks) = leaf.into_stage(staged);
        Ok(Step::Stage {
            entries,
            locks,
            admission,
            outcome: FoldOutcome::Locked {
                typ: shard_lock_type(&self.intents),
                membership,
                validated: None,
            },
        })
    }

    fn reorderable(&self) -> bool {
        self.intents
            .iter()
            .all(|i| matches!(i.desired, Desired::Read))
    }

    fn exhausted_outcome(&self, _in_doubt: bool) -> FoldOutcome {
        FoldOutcome::Conflict
    }

    fn owned_keys(&self) -> Vec<&[u8]> {
        // Acquiring a lock may create the key's entry, so it must land on the
        // owning leaf; re-route (release and re-lock) if a split moved a key
        // after routing (ADR-031).
        self.intents.iter().map(|i| i.raw_key.as_slice()).collect()
    }
}

/// The lock type recorded for a shard hold: its strongest intention, so the
/// diagnostic snapshot distinguishes read-only from write holders.
fn shard_lock_type(intents: &[KeyIntent]) -> LockType {
    if intents.iter().any(|i| !matches!(i.desired, Desired::Read)) {
        LockType::Write
    } else {
        LockType::Read
    }
}

/// Publishes its committed writes on its keys and drops its holds (ADR-020).
/// Never lock-conflicts, so it always folds into any round.
struct WriteBackResolver {
    id: TxId,
    intents: Arc<Vec<KeyIntent>>,
}

#[async_trait]
impl ShardResolver for WriteBackResolver {
    async fn resolve(
        &self,
        _ctx: &ResolveCtx<'_>,
        staged: &BTreeMap<Vec<u8>, ShardEntry>,
        staged_locks: &NodeLocks,
    ) -> Result<Step, TransError> {
        let WritebackStaged {
            changes,
            superseded,
        } = writeback_changes(&self.id, &self.intents, staged);
        let outcome = FoldOutcome::Released { superseded };
        let mut locks = staged_locks.clone();
        let locks_changed = locks.release(&self.id);
        if changes.is_empty() && !locks_changed {
            Ok(Step::Skip { outcome })
        } else {
            Ok(Step::Stage {
                entries: changes,
                locks,
                admission: StageAdmission::ExistingKeys,
                outcome,
            })
        }
    }

    fn reorderable(&self) -> bool {
        true
    }

    fn exhausted_outcome(&self, _in_doubt: bool) -> FoldOutcome {
        FoldOutcome::Released {
            superseded: Vec::new(),
        }
    }
}

/// Drops every hold this transaction has in the shard, publishing nothing
/// (ADR-024 serial-fallback release). Never lock-conflicts.
struct ReleaseResolver {
    id: TxId,
}

#[async_trait]
impl ShardResolver for ReleaseResolver {
    async fn resolve(
        &self,
        _ctx: &ResolveCtx<'_>,
        staged: &BTreeMap<Vec<u8>, ShardEntry>,
        staged_locks: &NodeLocks,
    ) -> Result<Step, TransError> {
        let changes = release_changes(&self.id, staged);
        let outcome = FoldOutcome::Released {
            superseded: Vec::new(),
        };
        let mut locks = staged_locks.clone();
        let locks_changed = locks.release(&self.id);
        if changes.is_empty() && !locks_changed {
            Ok(Step::Skip { outcome })
        } else {
            Ok(Step::Stage {
                entries: changes,
                locks,
                admission: StageAdmission::ExistingKeys,
                outcome,
            })
        }
    }

    fn reorderable(&self) -> bool {
        true
    }

    fn exhausted_outcome(&self, _in_doubt: bool) -> FoldOutcome {
        FoldOutcome::Released {
            superseded: Vec::new(),
        }
    }
}

/// Per-key resolution within a shard CAS attempt.
enum EntryResolution {
    /// The lock is installed in `entry`. The boolean is true when the intent
    /// creates or deletes a visible key, requiring a membership write lock.
    Locked(ShardEntry, bool),
    /// A live pending holder this transaction does not outrank: wait for it.
    Wait(TxId),
}

/// The staged result of a write-back: the entry changes to apply and the
/// `current_writer`s they superseded (GC candidates, ADR-022).
struct WritebackStaged {
    changes: Vec<(Vec<u8>, ShardEntry)>,
    superseded: Vec<TxId>,
}

/// Resolves the holders of an entry (help-forward committed, drop aborted,
/// wound-wait the live pending ones) and installs `id`'s lock. Returns
/// [`EntryResolution::Locked`] with the new entry; or [`EntryResolution::Wait`]
/// if a live holder this transaction cannot wound must be waited on
/// (hold-and-wait, ADR-024).
///
/// Read-version validation is not done here — the engine validates reads after
/// every lock is held (ADR-024).
async fn resolve_and_lock(
    ctx: &ResolveCtx<'_>,
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
    // effective one), aborted/missing holders are dropped, and the live pending
    // ones come back as conflicts to wound-wait. The monitor folds lease expiry
    // and the unknown-tx grace period into `tx_status`, so a holder still seen
    // as `Pending` here is genuinely live (ADR-021).
    let resolved = resolve_entry_locks(ctx, &intent.key_path, Some(&e), Some(id)).await?;
    e.current_writer = resolved.writer;
    e.deleted = resolved.deleted;
    let mut pending = resolved.pending;

    let exists_before = e.current_writer.is_some() && !e.deleted;

    // Read locks share with other read holders; everything else is exclusive and
    // must clear the live pending holders via wound-wait: wound the ones we
    // outrank, and wait for the first one we do not (hold-and-wait, ADR-024) —
    // keeping every lock already acquired elsewhere.
    let compatible = matches!(intent.desired, Desired::Read)
        && !matches!(e.lock_type, LockType::Write | LockType::Create);
    if !compatible {
        for holder in &pending {
            match try_reclaim(ctx.tmon, id, holder).await? {
                Reclaim::Wounded => {}
                Reclaim::Wait => return Ok(EntryResolution::Wait(holder.clone())),
            }
        }
        pending.clear();
    }

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
    let changes_membership = match intent.desired {
        Desired::Put => !exists_before,
        Desired::Delete => exists_before,
        Desired::Read => false,
    };
    Ok(EntryResolution::Locked(e, changes_membership))
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

/// Final outcome of acquiring every lock a transaction needs.
pub(crate) enum LockOutcome {
    /// All locks held; drives write-back on commit.
    Locked(LockedTx),
    /// Lost a CAS-contention race or reached the absolute object limit without
    /// adding a user key. Handled **internally** by [`super::algo::Algo`]: it
    /// releases the partial locks and re-acquires under the **same id** after a
    /// backoff — no renew and no body re-run (escalating to the serial order if
    /// contention persists). Never surfaces to the database retry loop.
    Conflict,
    /// A create reached a leaf's reserved content cap. The caller releases any
    /// partial locks, backs off without serial escalation, and retries after the
    /// background split has had an opportunity to run.
    LeafFull,
}

/// Outcome of acquiring locks across all touched shards.
enum ShardsOutcome {
    Locked(Vec<LeafObservation>),
    Conflict,
    LeafFull,
}

/// Outcome of acquiring locks on a single shard (after any hold-and-wait).
enum ShardOutcome {
    Locked(LeafObservation),
    Conflict,
    LeafFull,
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

/// Acquires and releases distributed locks on the shard/leaf coordination
/// objects, hiding waits, wound-wait, and CAS retries from callers. A thin
/// policy layer over the shared [`ShardCoordinator`] (ADR-028).
#[derive(Clone)]
pub struct Locker {
    /// The shared shard-mutation mechanism: dedup + resolve + CAS. Also held by
    /// the commit algorithm, so both drive one dedup.
    coord: ShardCoordinator,
    /// Routes a transaction's keys to their owning leaves by descent (ADR-031).
    dir: Directory,
    /// Used to park on a conflicting holder during hold-and-wait.
    tmon: Monitor,
    /// Backoff config for the hold-and-wait re-poll cadence.
    retry: RetryConfig,
    /// Per-transaction held-lock bookkeeping (which leaves a transaction
    /// holds): recorded when an acquire lands, read to drive the serial-fallback
    /// release, and surfaced for diagnostics. Shared across clones so the locker
    /// the algorithm drives and any diagnostics clone see one map.
    tlocks: Arc<Sharded<LockerShard>>,
    /// Count of lock-acquisition calls (one per `lock()` attempt, including the
    /// serial-fallback re-lock). Shared across clones. The coordinator cannot
    /// compute it — it only sees per-shard submissions — so the locker owns it.
    calls: Arc<AtomicU64>,
}

impl Locker {
    /// Creates a locker over the shared [`ShardCoordinator`], the directory used
    /// to route keys to leaves (ADR-031), and the transaction monitor. `retry`
    /// configures the exponential backoff applied between hold-and-wait re-polls
    /// of a conflicting holder, so a wait is never busy-retried (its
    /// `max_interval` caps the re-poll cadence).
    pub fn new(coord: ShardCoordinator, dir: Directory, tmon: Monitor, retry: RetryConfig) -> Self {
        Locker {
            coord,
            dir,
            tmon,
            retry,
            tlocks: Arc::new(Sharded::new(|_| Mutex::new(HashMap::new()))),
            calls: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Returns and resets the count of lock-acquisition calls (one per `lock()`
    /// attempt, including serial-fallback re-locks).
    pub fn lock_calls_and_reset(&self) -> usize {
        self.calls.swap(0, Ordering::Relaxed) as usize
    }

    /// Returns one entry per transaction that currently holds any leaf lock,
    /// with the held paths sorted by path. Output is sorted by transaction id
    /// for stable display.
    pub fn tx_locks_snapshot(&self) -> Vec<TxLockSnapshot> {
        let mut out = Vec::new();
        self.tlocks.each(|shard| {
            let m = shard.lock().unwrap();
            for (tx_id, locks) in m.iter() {
                if locks.is_empty() {
                    continue;
                }
                let mut paths = Vec::new();
                for (p, held) in locks {
                    paths.push(PathLock {
                        path: p.clone(),
                        typ: held.typ,
                        scope: LockScope::Structure,
                    });
                    if held.membership != LockType::None {
                        paths.push(PathLock {
                            path: p.clone(),
                            typ: held.membership,
                            scope: LockScope::Membership,
                        });
                    }
                }
                paths.sort_by(|a, b| {
                    a.path
                        .cmp(&b.path)
                        .then_with(|| lock_scope_order(a.scope).cmp(&lock_scope_order(b.scope)))
                });
                out.push(TxLockSnapshot {
                    tx_id: tx_id.clone(),
                    locks: paths,
                });
            }
        });
        out.sort_by(|a, b| a.tx_id.cmp(&b.tx_id));
        out
    }

    /// Acquires a transaction's locks while resolving predicate-lock coverage
    /// against the supplied pre-lock requirement barrier.
    pub(crate) async fn lock_at(
        &self,
        id: &TxId,
        data: &Data,
        serial: bool,
        scan_requirement: Requirement,
    ) -> Result<LockOutcome, TransError> {
        let mut groups = build_groups(&self.dir, data, scan_requirement).await?;
        let validations = match self
            .lock_shards_at(id, &groups, serial, scan_requirement)
            .await?
        {
            ShardsOutcome::Locked(validations) => validations,
            ShardsOutcome::Conflict => return Ok(LockOutcome::Conflict),
            ShardsOutcome::LeafFull => return Ok(LockOutcome::LeafFull),
        };
        for group in groups.values_mut() {
            group.membership = self.held_membership(id, &group.path);
        }
        Ok(LockOutcome::Locked(LockedTx {
            groups,
            validations,
        }))
    }

    /// Releases every lock `id` holds across the leaves it has acquired,
    /// **without publishing any value** and **leaving the transaction object
    /// pending**. Unlike [`Locker::write_back`] (the
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
    /// first. The held set is read from the coordinator's per-tx bookkeeping.
    /// Idempotent and best-effort.
    pub(crate) async fn release_locks(&self, id: &TxId) -> Result<(), TransError> {
        let requirement = Requirement::AtLeast(self.coord.now());
        for path in self.held_paths(id) {
            let pr = paths::parse(&path).map_err(|e| {
                TransError::with_source(format!("parsing held lock path {path:?}"), e)
            })?;
            match pr.typ {
                // The collection root `_i` is the small collection's single leaf
                // (ADR-031); a standalone `_n` node is a leaf too. Both carry
                // only key entries, so releasing the leaf clears every hold.
                paths::Type::CollectionInfo | paths::Type::Node => {
                    self.release_leaf_at(id, &path, requirement).await?
                }
                // Only leaves carry transaction locks.
                _ => {}
            }
        }
        self.clear_tx_locks(id);
        Ok(())
    }

    /// Publishes `current_writer` pointers / tombstones and releases this
    /// transaction's locks across the leaves it touched. Every CAS is
    /// idempotent; errors are best-effort (a failure leaves the locks to be
    /// reclaimed lazily by the next contender or lease expiry), so this never
    /// fails an already-committed transaction.
    ///
    /// Returns the transaction ids each published pointer *superseded* (the
    /// former `current_writer` an overwrite replaced): these just lost a
    /// reference and are GC write-back hint candidates (ADR-022).
    pub(crate) async fn write_back(&self, id: &TxId, locked: &LockedTx) -> Vec<TxId> {
        let mut superseded = Vec::new();
        let requirement = Requirement::AtLeast(self.coord.now());
        for group in locked.groups.values() {
            if let Ok(mut s) = self
                .write_back_shard(
                    id,
                    &group.path,
                    Arc::new(group.intents.clone()),
                    requirement,
                )
                .await
            {
                superseded.append(&mut s);
            }
        }
        self.clear_tx_locks(id);
        superseded
    }

    /// Publishes the single read-write fast path's committed pointer and releases
    /// its write lock on one key (ADR-027): the fast path installs
    /// `locked_by = [id]` through the coordinator's commit-install fold, so this
    /// converts that lock to `current_writer = id` and drops it. Routed through
    /// the same deduplicated write-back path the full commit uses (ADR-026), so
    /// it batches with any in-flight round for the shard. Best-effort and
    /// idempotent — a lost race leaves the lock to lazy reclaim / lease expiry.
    /// Returns the `current_writer` it superseded, a GC candidate hint (ADR-022).
    pub(crate) async fn write_back_single_put(
        &self,
        id: &TxId,
        leaf_path: &str,
        raw_key: &[u8],
        key_path: &str,
    ) -> Vec<TxId> {
        let intents = Arc::new(vec![KeyIntent {
            raw_key: raw_key.to_vec(),
            key_path: key_path.to_string(),
            desired: Desired::Put,
        }]);
        self.write_back_shard(
            id,
            leaf_path,
            intents,
            Requirement::AtLeast(self.coord.now()),
        )
        .await
        .unwrap_or_default()
    }

    async fn lock_shards_at(
        &self,
        id: &TxId,
        groups: &BTreeMap<String, ShardGroup>,
        serial: bool,
        requirement: Requirement,
    ) -> Result<ShardsOutcome, TransError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        // The first lock for this transaction starts the background refresh so a
        // long-lived holder's pending object is written lazily, keeping its
        // lease alive (the tx object is otherwise written only at commit).
        if !groups.is_empty() {
            self.tmon.start_refresh_tx(id);
        }

        let mut validations = Vec::with_capacity(groups.len());
        if serial {
            // Ascending leaf-path order is the global lock order: the BTreeMap
            // already iterates sorted by leaf path.
            for group in groups.values() {
                match self.lock_shard(id, group, requirement).await? {
                    ShardOutcome::Locked(validated) => validations.push(validated),
                    ShardOutcome::Conflict => return Ok(ShardsOutcome::Conflict),
                    ShardOutcome::LeafFull => return Ok(ShardsOutcome::LeafFull),
                }
            }
        } else {
            let outcomes = join_all(
                groups
                    .values()
                    .map(|group| self.lock_shard(id, group, requirement)),
            )
            .await;
            for outcome in outcomes {
                match outcome? {
                    ShardOutcome::Locked(validated) => validations.push(validated),
                    ShardOutcome::Conflict => return Ok(ShardsOutcome::Conflict),
                    ShardOutcome::LeafFull => return Ok(ShardsOutcome::LeafFull),
                }
            }
        }
        Ok(ShardsOutcome::Locked(validations))
    }

    /// Installs this transaction's [`AcquireResolver`] on a shard through the
    /// shared [`ShardCoordinator`] and returns its single-round [`FoldOutcome`].
    /// The hold-and-wait loop (on [`FoldOutcome::Wait`]) lives in
    /// [`lock_shard`](Self::lock_shard) above. A shutdown mid-flight surfaces as
    /// an error so the caller aborts the lock rather than silently proceeding.
    async fn acquire(
        &self,
        id: &TxId,
        path: &str,
        intents: Arc<Vec<KeyIntent>>,
        membership: LockType,
        requirement: Requirement,
    ) -> Result<FoldOutcome, TransError> {
        let resolver = Arc::new(AcquireResolver {
            id: id.clone(),
            path: path.to_string(),
            intents: intents.clone(),
            membership,
        });
        match self
            .coord
            .submit_shard(path, id, resolver, requirement)
            .await?
        {
            // The lock landed: record the leaf hold so the serial-fallback
            // release and diagnostics can find it (the engine no longer tracks
            // this, ADR-028). The outcome carries the acquired strength, so the
            // caller records it without re-deriving from the intents.
            Some(
                outcome @ FoldOutcome::Locked {
                    typ, membership, ..
                },
            ) => {
                self.record_leaf_lock(id, path, typ, membership);
                Ok(outcome)
            }
            Some(outcome) => Ok(outcome),
            None => Err(TransError::other(
                "coordinator shut down while locking leaf",
            )),
        }
    }

    /// Installs this transaction's [`WriteBackResolver`] on a shard and returns
    /// the `current_writer`s it superseded (GC candidates, ADR-022). Best-effort:
    /// a shutdown mid-flight leaves the holds to lazy reclaim / lease expiry.
    async fn write_back_shard(
        &self,
        id: &TxId,
        path: &str,
        intents: Arc<Vec<KeyIntent>>,
        requirement: Requirement,
    ) -> Result<Vec<TxId>, TransError> {
        let resolver = Arc::new(WriteBackResolver {
            id: id.clone(),
            intents,
        });
        match self
            .coord
            .submit_shard(path, id, resolver, requirement)
            .await?
        {
            Some(FoldOutcome::Released { superseded }) => Ok(superseded),
            _ => Ok(Vec::new()),
        }
    }

    /// Installs this transaction's [`ReleaseResolver`] on a leaf (drop its
    /// holds, publish nothing). Best-effort and idempotent, and stateless with
    /// respect to the per-transaction bookkeeping, so GC drives it to reclaim a
    /// dead transaction's leaf holds (ADR-029) without corrupting live
    /// tracking.
    pub(crate) async fn release_leaf(&self, id: &TxId, path: &str) -> Result<(), TransError> {
        self.release_leaf_at(id, path, Requirement::AtLeast(self.coord.now()))
            .await
    }

    async fn release_leaf_at(
        &self,
        id: &TxId,
        path: &str,
        requirement: Requirement,
    ) -> Result<(), TransError> {
        let resolver = Arc::new(ReleaseResolver { id: id.clone() });
        self.coord
            .submit_shard(path, id, resolver, requirement)
            .await
            .map(|_| ())
    }

    /// Installs this transaction's locks on every key it touches in one shard,
    /// through the shared [`ShardCoordinator`] (ADR-025/028): the submission
    /// merges with other transactions contending the same shard whenever they
    /// do not exclusively conflict, so one owner-driven load + CAS serves the
    /// whole batch.
    async fn lock_shard(
        &self,
        id: &TxId,
        group: &ShardGroup,
        requirement: Requirement,
    ) -> Result<ShardOutcome, TransError> {
        let intents = Arc::new(group.intents.clone());
        // Paces the hold-and-wait re-poll. It advances across successive blind
        // polls of a holder that will not budge, and resets whenever a holder
        // finalizes — real progress.
        let mut backoff = self.retry.backoff();
        loop {
            match self
                .acquire(
                    id,
                    &group.path,
                    intents.clone(),
                    group.membership,
                    requirement,
                )
                .await?
            {
                FoldOutcome::Locked { validated, .. } => {
                    return validated.map(ShardOutcome::Locked).ok_or_else(|| {
                        TransError::other("lock CAS returned no validation receipt")
                    });
                }
                // Hold-and-wait (ADR-024): if the coordinator reports
                // [`FoldOutcome::Wait`] — a key is held by a live holder this
                // transaction cannot wound — it **waits** for that holder to
                // finalize (keeping every lock already acquired on other
                // shards) then re-submits. The wait is *not* charged to the
                // bounded CAS-contention budget; the algo-level deadlock
                // timeout bounds the total wait and escalates to the
                // cannot-deadlock serial order.
                FoldOutcome::Wait(holder) => {
                    let delay = backoff.next_delay();
                    if let Woke::Finalized = self.wait_for_holder(&holder, delay).await {
                        backoff = self.retry.backoff();
                    }
                }
                FoldOutcome::LeafFull => return Ok(ShardOutcome::LeafFull),
                // Release, write-back, and commit-install outcomes cannot reach
                // an acquire. Treat one defensively as a conflict so the caller
                // takes the safe release-and-relock path.
                FoldOutcome::Conflict
                | FoldOutcome::Released { .. }
                | FoldOutcome::Landed
                | FoldOutcome::Moved
                | FoldOutcome::InDoubt(_) => {
                    return Ok(ShardOutcome::Conflict);
                }
            }
        }
    }

    /// Parks until the conflicting `holder` finalizes **or** `timeout` elapses,
    /// whichever comes first, then lets the caller re-resolve, reporting which
    /// woke it.
    async fn wait_for_holder(&self, holder: &TxId, timeout: Duration) -> Woke {
        let wait = self.tmon.wait_for_tx(holder);
        tokio::select! {
            _ = wait => Woke::Finalized,
            _ = rt::sleep(timeout) => Woke::PollTimeout,
        }
    }

    /// Records that `id` holds the leaf at `path` at `typ`.
    fn record_leaf_lock(&self, id: &TxId, path: &str, typ: LockType, membership: LockType) {
        let mut tlocks = self.tlocks.for_key(id.as_bytes()).lock().unwrap();
        tlocks
            .entry(id.clone())
            .or_default()
            .insert(path.to_string(), HeldLeaf { typ, membership });
    }

    fn held_membership(&self, id: &TxId, path: &str) -> LockType {
        self.tlocks
            .for_key(id.as_bytes())
            .lock()
            .unwrap()
            .get(id)
            .and_then(|paths| paths.get(path))
            .map(|held| held.membership)
            .unwrap_or(LockType::None)
    }

    /// The leaf paths `id` currently holds, sorted ascending for a
    /// deterministic release order (the simulation op-stream oracle requires the
    /// backend CAS sequence to be reproducible).
    fn held_paths(&self, id: &TxId) -> Vec<String> {
        let tlocks = self.tlocks.for_key(id.as_bytes()).lock().unwrap();
        let mut paths: Vec<String> = tlocks
            .get(id)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();
        paths.sort();
        paths
    }

    /// Drops `id`'s held-lock bookkeeping once its locks are released.
    fn clear_tx_locks(&self, id: &TxId) {
        let mut tlocks = self.tlocks.for_key(id.as_bytes()).lock().unwrap();
        tlocks.remove(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolver::Resolver;
    use glassdb_backend::middleware::{
        BackendOp, HookBackend, HookFuture, OpLog, RecordingBackend,
    };
    use glassdb_backend::{Backend, memory::MemoryBackend};
    use glassdb_concurr::{Background, RetryConfig};
    use glassdb_data::paths;
    use glassdb_storage::{
        CachedStore, CollectionRoot, Directory, Node, Shard, ShardEntry, ShardStore, SplitPolicy,
        TLogger, TxCommitStatus,
    };
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tokio::sync::Notify;

    struct TlCtx {
        shards: ShardStore,
        monitor: Monitor,
        coord: ShardCoordinator,
        _bg: Arc<Background>,
    }

    fn new_test_locker(b: Arc<dyn Backend>) -> (Locker, TlCtx) {
        new_test_locker_with_policy(b, SplitPolicy::default())
    }

    fn new_test_locker_with_policy(b: Arc<dyn Backend>, policy: SplitPolicy) -> (Locker, TlCtx) {
        let objects = CachedStore::new(b.clone(), 1024);
        let tl = TLogger::new(objects.clone(), "test");
        let bg = Arc::new(Background::new());
        let mon = Monitor::new(tl, Arc::downgrade(&bg));
        let shards = ShardStore::new(objects.clone());
        let resolver = Resolver::new(shards.clone(), mon.clone());
        let dir = Directory::new(shards.clone());
        let coord = ShardCoordinator::with_hinter(
            shards.clone(),
            resolver,
            mon.clone(),
            RetryConfig::default(),
            policy,
            Arc::new(crate::shard_coord::NoSplitHints),
        );
        let locker = Locker::new(coord.clone(), dir, mon.clone(), RetryConfig::default());
        (
            locker,
            TlCtx {
                shards,
                monitor: mon,
                coord,
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

    // Routes an intent to the collection's single leaf `_i` (ADR-031: with split
    // deferred, every key coordinates on the root leaf). The `key` is carried by
    // the intent itself, so it is only used for readability at call sites.
    fn group_of(_key: &[u8], intent: KeyIntent) -> BTreeMap<String, ShardGroup> {
        let path = paths::collection_info(COLL);
        let mut g = BTreeMap::new();
        g.insert(
            path.clone(),
            ShardGroup {
                path,
                intents: vec![intent],
                membership: LockType::None,
            },
        );
        g
    }

    async fn entry_of(ctx: &TlCtx, key: &[u8]) -> Option<ShardEntry> {
        let loaded = ctx
            .shards
            .load_leaf(
                &paths::collection_info(COLL),
                Requirement::AtLeast(ctx.shards.now()),
            )
            .await
            .unwrap();
        loaded.entries.lookup(key).cloned()
    }

    // Acquires shard locks in parallel mode, asserting success.
    async fn lock_ok(locker: &Locker, id: &TxId, groups: &BTreeMap<String, ShardGroup>) {
        match locker
            .lock_shards_at(id, groups, false, Requirement::AtLeast(locker.coord.now()))
            .await
            .unwrap()
        {
            ShardsOutcome::Locked(_) => {}
            ShardsOutcome::Conflict => panic!("expected lock acquisition to succeed"),
            ShardsOutcome::LeafFull => panic!("expected leaf to have capacity"),
        }
    }

    #[tokio::test]
    async fn lock_write_creates_entry() {
        let (locker, ctx) = init_tl_test();
        let key = b"key";
        let tx = mk_tid(1, "tx");
        ctx.monitor.begin_tx(&tx);

        let groups = group_of(key, put_intent(key));
        lock_ok(&locker, &tx, &groups).await;

        // A create installs the entry lock together with structure-R and
        // membership-W in the same leaf CAS.
        let e = entry_of(&ctx, key).await.expect("entry installed");
        assert_eq!(e.lock_type, LockType::Create);
        assert_eq!(e.locked_by, vec![tx.clone()]);
        let loaded = ctx
            .shards
            .load_leaf(
                &paths::collection_info(COLL),
                Requirement::AtLeast(ctx.shards.now()),
            )
            .await
            .unwrap();
        assert_eq!(loaded.node().structure_lock().lock_type(), LockType::Read);
        assert!(loaded.node().structure_lock().contains(&tx));
        assert_eq!(loaded.node().membership_lock().lock_type(), LockType::Write);
        assert!(loaded.node().membership_lock().contains(&tx));
        assert_eq!(loaded.node().membership_version(), 1);
    }

    #[tokio::test]
    async fn create_at_content_cap_reports_leaf_full_without_staging() {
        let writer = mk_tid(0, "seed");
        let tx = mk_tid(1, "tx");
        let existing = ShardEntry {
            key: b"a".to_vec(),
            lock_type: LockType::None,
            locked_by: Vec::new(),
            current_writer: Some(writer),
            deleted: false,
        };
        let created = ShardEntry {
            key: b"z".to_vec(),
            lock_type: LockType::Create,
            locked_by: vec![tx.clone()],
            current_writer: None,
            deleted: false,
        };
        let mut node = Node::leaf(Shard::from_entries([existing, created]));
        node.add_structure_reader(tx.clone());
        node.set_membership_writer(tx.clone());
        let mut root = CollectionRoot::new();
        root.set_node(node);
        let content_limit = root.content_encoded_len() - 1;
        let node_max_bytes = root.encoded_len() + 64;
        let policy = SplitPolicy {
            node_max_bytes,
            split_headroom_bytes: node_max_bytes - content_limit,
            ..SplitPolicy::default()
        };

        let (locker, ctx) = new_test_locker_with_policy(Arc::new(MemoryBackend::new()), policy);
        seed_committed(&ctx, b"a", b"old").await;
        ctx.monitor.begin_tx(&tx);

        let outcome = locker
            .lock_shards_at(
                &tx,
                &group_of(b"z", put_intent(b"z")),
                false,
                Requirement::AtLeast(locker.coord.now()),
            )
            .await
            .unwrap();
        assert!(matches!(outcome, ShardsOutcome::LeafFull));
        assert!(entry_of(&ctx, b"z").await.is_none());
        let loaded = ctx
            .shards
            .load_leaf(
                &paths::collection_info(COLL),
                Requirement::AtLeast(ctx.shards.now()),
            )
            .await
            .unwrap();
        assert!(!loaded.node().structure_lock().contains(&tx));
        assert!(!loaded.node().membership_lock().contains(&tx));
    }

    #[tokio::test]
    async fn overwrite_does_not_take_membership_lock() {
        let (locker, ctx) = init_tl_test();
        let key = b"key";
        seed_committed(&ctx, key, b"old").await;
        let tx = mk_tid(1, "tx");
        ctx.monitor.begin_tx(&tx);

        lock_ok(&locker, &tx, &group_of(key, put_intent(key))).await;
        let loaded = ctx
            .shards
            .load_leaf(
                &paths::collection_info(COLL),
                Requirement::AtLeast(ctx.shards.now()),
            )
            .await
            .unwrap();
        assert!(loaded.node().structure_lock().contains(&tx));
        assert!(loaded.node().membership_lock().holders().is_empty());
        assert_eq!(loaded.node().membership_version(), 0);
    }

    #[tokio::test]
    async fn scan_membership_reader_does_not_bump_version() {
        let (locker, ctx) = init_tl_test();
        let key = b"key";
        seed_committed(&ctx, key, b"old").await;
        let tx = mk_tid(1, "scan");
        ctx.monitor.begin_tx(&tx);

        let mut groups = group_of(key, put_intent(key));
        groups
            .get_mut(&paths::collection_info(COLL))
            .unwrap()
            .membership = LockType::Read;
        lock_ok(&locker, &tx, &groups).await;

        let path = paths::collection_info(COLL);
        let loaded = ctx
            .shards
            .load_leaf(&path, Requirement::AtLeast(ctx.shards.now()))
            .await
            .unwrap();
        assert_eq!(loaded.node().membership_lock().lock_type(), LockType::Read);
        assert!(loaded.node().membership_lock().contains(&tx));
        assert_eq!(loaded.node().membership_version(), 0);

        locker.release_leaf(&tx, &path).await.unwrap();
        let loaded = ctx
            .shards
            .load_leaf(&path, Requirement::AtLeast(ctx.shards.now()))
            .await
            .unwrap();
        assert!(loaded.node().membership_lock().holders().is_empty());
        assert_eq!(loaded.node().membership_version(), 0);
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
        let waiting = tokio::spawn(async move {
            locker2
                .lock_shards_at(
                    &young2,
                    &groups,
                    false,
                    Requirement::AtLeast(locker2.coord.now()),
                )
                .await
        });

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
        lock_ok(&locker, &old, &old_groups).await;

        // Younger contender blocks waiting for `old`.
        let young = mk_tid(2, "young");
        ctx.monitor.begin_tx(&young);
        let locker2 = locker.clone();
        let young2 = young.clone();
        let groups = group_of(key, put_intent(key));
        let waiting = tokio::spawn(async move {
            locker2
                .lock_shards_at(
                    &young2,
                    &groups,
                    false,
                    Requirement::AtLeast(locker2.coord.now()),
                )
                .await
        });

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
                    validations: Vec::new(),
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
        lock_ok(&locker, &tx, &groups).await;
        // First writer of a fresh key overwrites no pointer: no GC hint.
        let superseded = locker
            .write_back(
                &tx,
                &LockedTx {
                    groups,
                    validations: Vec::new(),
                },
            )
            .await;
        assert!(superseded.is_empty());

        let e = entry_of(&ctx, key).await.unwrap();
        assert_eq!(e.lock_type, LockType::None);
        assert!(e.locked_by.is_empty());
        assert_eq!(e.current_writer, Some(tx.clone()));
        let loaded = ctx
            .shards
            .load_leaf(
                &paths::collection_info(COLL),
                Requirement::AtLeast(ctx.shards.now()),
            )
            .await
            .unwrap();
        assert!(loaded.node().structure_lock().holders().is_empty());
        assert!(loaded.node().membership_lock().holders().is_empty());
        assert_eq!(loaded.node().membership_version(), 2);
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

        // A blind put installs both the key Create lock and the leaf
        // membership-write lock.
        let data = Data {
            reads: Vec::new(),
            writes: vec![crate::algo::WriteAccess::put(
                paths::from_key(COLL, key).into(),
                Arc::from(&b"v"[..]),
            )],
            scans: Vec::new(),
        };
        let out = locker
            .lock_at(&tx, &data, false, Requirement::AtLeast(locker.dir.now()))
            .await
            .unwrap();
        assert!(matches!(out, LockOutcome::Locked(_)));
        assert!(!locker.tx_locks_snapshot().is_empty());

        locker.release_locks(&tx).await.unwrap();

        // The released create-lock left the fresh key with no holder and no
        // committed writer, so the fold pruned the now-vestigial entry (ADR-029):
        // a release publishes no value and leaves no dead entry behind.
        assert!(
            entry_of(&ctx, key).await.is_none(),
            "vestigial entry pruned on release"
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
        // A write intention records the held leaf (the small collection's root
        // `_i`) as a write lock.
        let shard_path = paths::collection_info(COLL);
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

        // Install the committed pointer directly in the collection's leaf `_i`.
        let path = paths::collection_info(COLL);
        let loaded = ctx
            .shards
            .load_leaf(&path, Requirement::AtLeast(ctx.shards.now()))
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
                current_writer: Some(writer),
                deleted: false,
            },
        );
        let new_shard = Shard::from_entries(entries.into_values());
        assert!(
            ctx.shards
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

    // --- ADR-025: cross-transaction lock-acquisition deduplication ----------

    /// Test hook that, while **armed**, blocks the next read on a gate until
    /// released — so a test can park the dedup driver mid-load while other
    /// contenders queue, forcing them into one merged CAS round. Every other call
    /// passes through. Arming is deferred (`arm`) so a test can run un-gated setup
    /// first, then gate only the phase under test.
    struct Gate {
        gate: Arc<Notify>,
        armed: AtomicBool,
    }

    impl Gate {
        fn wrap(inner: Arc<dyn Backend>, armed: bool) -> (Arc<HookBackend>, Arc<Self>) {
            let gate = Arc::new(Gate {
                gate: Arc::new(Notify::new()),
                armed: AtomicBool::new(armed),
            });
            let backend = HookBackend::new(inner);
            backend.set_before({
                let gate = gate.clone();
                move |op| {
                    let wait = matches!(
                        op,
                        BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                    ) && gate.armed.swap(false, Ordering::SeqCst);
                    let notify = gate.gate.clone();
                    let future: HookFuture = Box::pin(async move {
                        if wait {
                            notify.notified().await;
                        }
                        Ok(())
                    });
                    future
                }
            });
            (backend, gate)
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

    /// A locker whose backend records ops and gates the first read.
    fn gated_locker() -> (Locker, TlCtx, OpLog, Arc<Gate>) {
        gated_locker_with(true)
    }

    /// As [`gated_locker`], but `armed` chooses whether the gate is active from
    /// the start (gate acquisition) or deferred until `arm` (gate a later phase,
    /// e.g. write-back, after un-gated setup).
    fn gated_locker_with(armed: bool) -> (Locker, TlCtx, OpLog, Arc<Gate>) {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (backend, gate) = Gate::wrap(mem, armed);
        let recorder = Arc::new(RecordingBackend::new(backend));
        let log = recorder.log();
        let (locker, ctx) = new_test_locker(recorder);
        (locker, ctx, log, gate)
    }

    /// Counts the CAS stores (create or conditional write) issued against `path`.
    fn count_stores(log: &OpLog, path: &str) -> usize {
        log.lock()
            .unwrap()
            .iter()
            .filter(|r| r.path == path && (r.op == "write_if" || r.op == "write_if_not_exists"))
            .count()
    }

    /// A distinct key that shares the same leaf as `base`, for exercising
    /// disjoint-key contention within a single leaf object. With split deferred,
    /// every key lives in the collection's single leaf `_i` (ADR-031), so any
    /// distinct key qualifies.
    fn same_shard_sibling(base: &[u8]) -> Vec<u8> {
        let sib = b"sibling".to_vec();
        assert_ne!(sib, base, "sibling must differ from the base key");
        sib
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
        let h1 = tokio::spawn(async move {
            l1.lock_shards_at(&t1, &g1, false, Requirement::AtLeast(l1.coord.now()))
                .await
        });
        let h2 = tokio::spawn(async move {
            l2.lock_shards_at(&t2, &g2, false, Requirement::AtLeast(l2.coord.now()))
                .await
        });

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

        let shard_path = paths::collection_info(COLL);
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
        let (locker, ctx, log, gate) = gated_locker_with(false);
        let ka = b"key-a".to_vec();
        let kb = same_shard_sibling(&ka);
        seed_committed(&ctx, &ka, b"a").await;
        seed_committed(&ctx, &kb, b"b").await;
        log.lock().unwrap().clear();
        gate.arm();
        let tx1 = mk_tid(1, "w1");
        let tx2 = mk_tid(2, "w2");
        ctx.monitor.begin_tx(&tx1);
        ctx.monitor.begin_tx(&tx2);

        let (l1, l2) = (locker.clone(), locker.clone());
        let (t1, t2) = (tx1.clone(), tx2.clone());
        let g1 = group_of(&ka, put_intent(&ka));
        let g2 = group_of(&kb, put_intent(&kb));
        let h1 = tokio::spawn(async move {
            l1.lock_shards_at(&t1, &g1, false, Requirement::AtLeast(l1.coord.now()))
                .await
        });
        let h2 = tokio::spawn(async move {
            l2.lock_shards_at(&t2, &g2, false, Requirement::AtLeast(l2.coord.now()))
                .await
        });

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

        let shard_path = paths::collection_info(COLL);
        assert_eq!(
            count_stores(&log, &shard_path),
            1,
            "disjoint writers batch into one CAS"
        );
        assert_eq!(entry_of(&ctx, &ka).await.unwrap().locked_by, vec![tx1]);
        assert_eq!(entry_of(&ctx, &kb).await.unwrap().locked_by, vec![tx2]);
    }

    #[tokio::test(start_paused = true)]
    async fn disjoint_creates_serialize_on_membership_write() {
        let (locker, ctx) = init_tl_test();
        let ka = b"key-a".to_vec();
        let kb = same_shard_sibling(&ka);
        let old = mk_tid(1, "old");
        let young = mk_tid(2, "young");
        ctx.monitor.begin_tx(&old);
        ctx.monitor.begin_tx(&young);

        lock_ok(&locker, &old, &group_of(&ka, put_intent(&ka))).await;

        let (waiting_locker, waiting_id) = (locker.clone(), young.clone());
        let waiting_group = group_of(&kb, put_intent(&kb));
        let waiting = tokio::spawn(async move {
            waiting_locker
                .lock_shards_at(
                    &waiting_id,
                    &waiting_group,
                    false,
                    Requirement::AtLeast(waiting_locker.coord.now()),
                )
                .await
        });
        rt::sleep(Duration::from_millis(50)).await;
        assert!(!waiting.is_finished());

        ctx.monitor.abort_tx(&old).await.unwrap();
        assert!(matches!(
            waiting.await.unwrap().unwrap(),
            ShardsOutcome::Locked(_)
        ));
        assert_eq!(entry_of(&ctx, &kb).await.unwrap().locked_by, vec![young]);
    }

    // Locks + commits `key` for `tx`, leaving the shard entry holding the write
    // lock, so a later `write_back` publishes it. Returns the acquired handle.
    async fn lock_commit(locker: &Locker, ctx: &TlCtx, tx: &TxId, key: &[u8]) -> LockedTx {
        use glassdb_storage::{TxLog, TxWrite};
        ctx.monitor.begin_tx(tx);
        let groups = group_of(key, put_intent(key));
        lock_ok(locker, tx, &groups).await;
        let mut tl = TxLog::new(tx.clone(), TxCommitStatus::Ok);
        tl.writes = vec![TxWrite {
            path: paths::from_key(COLL, key),
            value: Arc::from(&b"v"[..]),
            deleted: false,
            prev_writer: TxId::default(),
        }];
        ctx.monitor.commit_tx(tl).await.unwrap();
        LockedTx {
            groups,
            validations: Vec::new(),
        }
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
        let shard_path = paths::collection_info(COLL);

        let tx1 = mk_tid(1, "w1");
        let tx2 = mk_tid(2, "w2");
        seed_committed(&ctx, &ka, b"a").await;
        seed_committed(&ctx, &kb, b"b").await;
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
        let shard_path = paths::collection_info(COLL);

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
        let ha = tokio::spawn(async move {
            l2.lock_shards_at(&t2, &g2, false, Requirement::AtLeast(l2.coord.now()))
                .await
        });
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
        let shard_path = paths::collection_info(COLL);

        let tx1 = mk_tid(1, "r1");
        let tx2 = mk_tid(2, "r2");
        ctx.monitor.begin_tx(&tx1);
        ctx.monitor.begin_tx(&tx2);
        seed_committed(&ctx, &ka, b"a").await;
        seed_committed(&ctx, &kb, b"b").await;
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
        // Both locks are gone; the seeded committed pointers remain unchanged.
        assert!(
            entry_of(&ctx, &ka).await.unwrap().locked_by.is_empty(),
            "first lock released"
        );
        assert!(
            entry_of(&ctx, &kb).await.unwrap().locked_by.is_empty(),
            "second lock released"
        );
    }

    // ADR-028: two writers on the *same* key now share one CAS round. The
    // monotonic fold visits the older first — it stages its lock — and the
    // younger, observing that live staged holder it cannot wound, emits `Wait`
    // and blocks (hold-and-wait). One store serves the round; the younger is not
    // wounded, it simply waits its turn.
    #[tokio::test(start_paused = true)]
    async fn same_key_writers_share_one_cas() {
        let (locker, ctx, log, gate) = gated_locker();
        let key = b"key";
        let old = mk_tid(1, "old");
        let young = mk_tid(2, "young");
        ctx.monitor.begin_tx(&old);
        ctx.monitor.begin_tx(&young);

        let (lo, ly) = (locker.clone(), locker.clone());
        let (to, ty) = (old.clone(), young.clone());
        let go = group_of(key, put_intent(key));
        let gy = group_of(key, put_intent(key));
        let ho = tokio::spawn(async move {
            lo.lock_shards_at(&to, &go, false, Requirement::AtLeast(lo.coord.now()))
                .await
        });
        let hy = tokio::spawn(async move {
            ly.lock_shards_at(&ty, &gy, false, Requirement::AtLeast(ly.coord.now()))
                .await
        });

        // Once both tasks are parked (driver in the gated load, the other queued),
        // release the load so the round folds both members.
        rt::sleep(Duration::from_millis(50)).await;
        gate.release();

        // The older locks; the younger is left waiting on it, not wounded.
        assert!(matches!(
            ho.await.unwrap().unwrap(),
            ShardsOutcome::Locked(_)
        ));
        rt::sleep(Duration::from_millis(50)).await;
        assert!(!hy.is_finished(), "the younger waits for the older holder");

        let shard_path = paths::collection_info(COLL);
        assert_eq!(
            count_stores(&log, &shard_path),
            1,
            "same-key writers share a single CAS round"
        );
        assert_eq!(
            entry_of(&ctx, key).await.unwrap().locked_by,
            vec![old.clone()]
        );
        assert_eq!(
            ctx.monitor.tx_status(&young).await.unwrap(),
            TxCommitStatus::Pending,
            "the younger is not wounded, only waiting"
        );

        // Drain the still-waiting younger so the test's spawned task does not leak.
        hy.abort();
        let _ = hy.await;
    }

    // ADR-028 regression (monotonic fold): after the older releases its same-key
    // lock, the waiting younger makes progress and acquires — the fold order
    // guarantees liveness without either transaction being wounded.
    #[tokio::test(start_paused = true)]
    async fn same_key_younger_proceeds_after_older_releases() {
        let (locker, ctx, log, gate) = gated_locker();
        let key = b"key";
        let old = mk_tid(1, "old");
        let young = mk_tid(2, "young");
        ctx.monitor.begin_tx(&old);
        ctx.monitor.begin_tx(&young);

        let (lo, ly) = (locker.clone(), locker.clone());
        let (to, ty) = (old.clone(), young.clone());
        let go = group_of(key, put_intent(key));
        let gy = group_of(key, put_intent(key));
        let ho = tokio::spawn(async move {
            lo.lock_shards_at(&to, &go, false, Requirement::AtLeast(lo.coord.now()))
                .await
        });
        let hy = tokio::spawn(async move {
            ly.lock_shards_at(&ty, &gy, false, Requirement::AtLeast(ly.coord.now()))
                .await
        });

        rt::sleep(Duration::from_millis(50)).await;
        gate.release();
        assert!(matches!(
            ho.await.unwrap().unwrap(),
            ShardsOutcome::Locked(_)
        ));

        // The older releases; the younger's hold-and-wait loop then re-acquires.
        locker.release_locks(&old).await.unwrap();
        assert!(matches!(
            hy.await.unwrap().unwrap(),
            ShardsOutcome::Locked(_)
        ));
        assert_eq!(entry_of(&ctx, key).await.unwrap().locked_by, vec![young]);

        // A load per poll, but only three CAS stores: the older's acquire, the
        // older's release, then the younger's acquire. The younger's waiting
        // rounds stage nothing, so they add no stores.
        let shard_path = paths::collection_info(COLL);
        assert_eq!(count_stores(&log, &shard_path), 3);
    }

    // ADR-028 regression (equal priority): two same-priority writers on one key
    // never wound each other (that would livelock across renews). The monotonic
    // fold's round-local byte tiebreak still picks one deterministic winner; the
    // loser waits and, after the winner releases, proceeds. Both make progress.
    #[tokio::test(start_paused = true)]
    async fn equal_priority_same_key_one_winner_no_livelock() {
        let (locker, ctx, log, gate) = gated_locker();
        let key = b"key";
        // Same priority (order 1), distinct prefixes: `aaaa` < `bbbb` by the
        // fold's byte tiebreak, so `a` is the deterministic round winner.
        let a = mk_tid(1, "aaaa");
        let b = mk_tid(1, "bbbb");
        assert!(
            !a.older(&b) && !b.older(&a),
            "the two must be equal priority"
        );
        ctx.monitor.begin_tx(&a);
        ctx.monitor.begin_tx(&b);

        let (la, lb) = (locker.clone(), locker.clone());
        let (ta, tb) = (a.clone(), b.clone());
        let ga = group_of(key, put_intent(key));
        let gb = group_of(key, put_intent(key));
        let ha = tokio::spawn(async move {
            la.lock_shards_at(&ta, &ga, false, Requirement::AtLeast(la.coord.now()))
                .await
        });
        let hb = tokio::spawn(async move {
            lb.lock_shards_at(&tb, &gb, false, Requirement::AtLeast(lb.coord.now()))
                .await
        });

        rt::sleep(Duration::from_millis(50)).await;
        gate.release();

        // The tiebreak winner locks; the loser waits (not wounded).
        assert!(matches!(
            ha.await.unwrap().unwrap(),
            ShardsOutcome::Locked(_)
        ));
        rt::sleep(Duration::from_millis(50)).await;
        assert!(!hb.is_finished(), "the loser waits without being wounded");
        assert_eq!(
            entry_of(&ctx, key).await.unwrap().locked_by,
            vec![a.clone()]
        );
        assert_eq!(
            ctx.monitor.tx_status(&b).await.unwrap(),
            TxCommitStatus::Pending
        );

        // After the winner releases, the loser proceeds: progress, no livelock.
        locker.release_locks(&a).await.unwrap();
        assert!(matches!(
            hb.await.unwrap().unwrap(),
            ShardsOutcome::Locked(_)
        ));
        assert_eq!(entry_of(&ctx, key).await.unwrap().locked_by, vec![b]);

        // Three CAS stores: the winner's acquire, its release, then the loser's
        // acquire. The loser's waiting rounds stage nothing.
        let shard_path = paths::collection_info(COLL);
        assert_eq!(count_stores(&log, &shard_path), 3);
    }

    // ADR-028 regression (commute): a committed holder's write-back and another
    // transaction's acquire of the *same* key fold into one CAS round with the
    // same result regardless of wound-wait fold order — the write-back publishes
    // the committed pointer and drops its hold, the acquirer ends holding the
    // lock over the help-forwarded value. Run both orderings to show it commutes.
    #[tokio::test(start_paused = true)]
    async fn release_and_acquire_same_key_commute() {
        for (wb_order, acq_order) in [(1u64, 2u64), (2u64, 1u64)] {
            let (locker, ctx, log, gate) = gated_locker_with(false);
            let key = b"key";
            let shard_path = paths::collection_info(COLL);

            // A committed holder leaves its write lock held pending write-back.
            let committer = mk_tid(wb_order, "wb");
            let lt = lock_commit(&locker, &ctx, &committer, key).await;
            let acquirer = mk_tid(acq_order, "acq");
            ctx.monitor.begin_tx(&acquirer);
            let g = group_of(key, put_intent(key));

            let before = count_stores(&log, &shard_path);
            gate.arm();
            let (lw, la) = (locker.clone(), locker.clone());
            let (cw, ca) = (committer.clone(), acquirer.clone());
            let hw = tokio::spawn(async move { lw.write_back(&cw, &lt).await });
            let ha = tokio::spawn(async move {
                la.lock_shards_at(&ca, &g, false, Requirement::AtLeast(la.coord.now()))
                    .await
            });
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
                "write-back and acquire share one CAS (order {wb_order}/{acq_order})"
            );
            let e = entry_of(&ctx, key).await.unwrap();
            assert_eq!(
                e.locked_by,
                vec![acquirer],
                "the acquirer holds the lock (order {wb_order}/{acq_order})"
            );
            assert_eq!(
                e.current_writer,
                Some(committer),
                "the committed value is published (order {wb_order}/{acq_order})"
            );
        }
    }

    // `close` cancels new submissions; the dedup snapshot tracks only live
    // coordination, so it is empty while idle and after an uncontended lock.
    #[tokio::test]
    async fn close_cancels_new_locks_and_snapshot_tracks_idle() {
        let (locker, ctx) = init_tl_test();
        assert!(
            ctx.coord.dedup_snapshot().is_empty(),
            "no coordination while idle"
        );

        let tx = mk_tid(1, "tx");
        ctx.monitor.begin_tx(&tx);
        lock_ok(&locker, &tx, &group_of(b"key", put_intent(b"key"))).await;
        assert!(
            ctx.coord.dedup_snapshot().is_empty(),
            "an uncontended lock leaves no dedup key behind"
        );

        ctx.coord.close().await;
        let err = locker
            .lock_shards_at(
                &tx,
                &group_of(b"key2", put_intent(b"key2")),
                false,
                Requirement::AtLeast(locker.coord.now()),
            )
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
        let waiting = tokio::spawn(async move {
            l.lock_shards_at(&y, &g, false, Requirement::AtLeast(l.coord.now()))
                .await
        });
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
