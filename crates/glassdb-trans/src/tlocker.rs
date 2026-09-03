//! Distributed locking **policy** over the v2 leaf/root coordination objects
//! (ADR-017, ADR-020, ADR-024). Ported in spirit from the Go
//! `internal/trans/tlocker.go`, but re-keyed from per-key objects onto nodes.
//!
//! A transaction groups its accessed keys by leaf and locks each leaf with a
//! single read-modify-write CAS: resolve every touched key's holders
//! (help-forward committed holders, drop abort-side terminal ones, wound-wait the live
//! pending ones), install this transaction's locks, then CAS the leaf back.
//! Create/delete additionally take the routed leaf's membership-write lock.
//! Every node rewrite proves the exclusive structural gate absent in the state
//! it conditionally replaces (ADR-044).
//!
//! [`Locker`] is the common lock boundary. Its key view owns how a transaction
//! groups keys, the parallel/serial acquisition strategy, the hold-and-wait
//! loop; its collection view owns collection locks and their committed effects.
//! The data mutation *mechanism* — deduplicated
//! load + resolve + CAS with retry — lives in the
//! [`LeafCoordinator`](crate::leaf_coord::LeafCoordinator) below it, which
//! the locker shares with the direct commit mechanism so every leaf/root
//! mutation flows through one place (ADR-028, ADR-061).
//!
//! Lock acquisition has two modes (ADR-020): the default **parallel** path locks
//! every touched leaf concurrently; the **serial** fallback locks them one at a
//! time in ascending leaf path order so equal-priority contenders queue on the
//! lowest contended leaf and exactly one wins it (first-CAS-wins), guaranteeing
//! progress where the parallel path could livelock.

use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroUsize;
use std::ops::{AddAssign, Sub};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use glassdb_concurr::{RetryConfig, join_all_bounded, rt};
use glassdb_data::{LeafRef, LogicalKey, ObjectPath, TxId};
use glassdb_storage::transaction::TxLock;
use glassdb_storage::{
    CurrentState, EntryLockState, LeafEntry, LeafObservation, LockType, NodeLocks, Requirement,
    TreeRouter,
};

use crate::access::{AccessSet, WriteOp};
use crate::collection_coordination::{CollectionLocker, CollectionStateResolver};
use crate::error::TransError;
use crate::leaf_coord::{
    CoordinatedOutcome, CoordinationEvidence, FoldOutcome, LeafCoordinator, LeafOperation,
    LeafResolver, ResolveCtx, StageAdmission, Step,
};
use crate::monitor::Monitor;
use crate::node_locking::NodeLockReconciler;
use crate::wound_wait::{Reclaim, try_reclaim};

#[derive(Clone, Copy)]
struct HeldLeaf {
    entry_lock: LockType,
    membership: LockType,
}

/// Snapshot of distributed-locker activity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LockerStats {
    /// Lock-acquisition calls, including acquisition by renewed identities.
    pub calls: u64,
}

impl AddAssign for LockerStats {
    fn add_assign(&mut self, rhs: Self) {
        self.calls += rhs.calls;
    }
}

impl Sub for LockerStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            calls: self.calls.saturating_sub(rhs.calls),
        }
    }
}

/// The lock a transaction wants on a key's leaf entry.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Desired {
    Read,
    Put,
    Delete,
}

/// One key's lock intention within a leaf.
#[derive(Clone)]
struct KeyIntent {
    /// Raw user key bytes (the leaf-entry key).
    pub raw_key: Vec<u8>,
    /// Logical key used to fetch a help-forwarded writer's value.
    pub key: LogicalKey,
    /// The lock to install.
    pub desired: Desired,
}

/// The keys a transaction touches in one leaf, plus the leaf's location
/// (ADR-031).
struct RoutedLockGroup {
    /// The leaf's object path: the collection root `_r` for a small collection's
    /// single leaf, else a standalone node `_n`, resolved by descent. This is
    /// the coordinator submit target.
    path: ObjectPath,
    leaf: LeafRef,
    /// Per-key intentions, in ascending raw-key order.
    intents: Vec<KeyIntent>,
    membership: LockType,
}

/// Proof that one transaction holds all its requested locks on one leaf.
struct LeafHoldReceipt {
    evidence: CoordinationEvidence,
    held: HeldLeaf,
}

impl LeafHoldReceipt {
    /// Returns the leaf state that proves the hold.
    fn observation(&self) -> &LeafObservation {
        self.evidence.observation()
    }

    /// Returns the aggregate lock strengths held on the leaf.
    fn held(&self) -> HeldLeaf {
        self.held
    }
}

/// One routed group paired with the receipt that proves its locks are held.
/// Keeping these together prevents successful lock acquisition from later
/// reconstructing commit and validation evidence from diagnostic bookkeeping.
struct HeldLockGroup {
    path: ObjectPath,
    leaf: LeafRef,
    intents: Vec<KeyIntent>,
    receipt: LeafHoldReceipt,
}

/// The locks acquired through [`Locker::keys`]. Opaque to the caller: it carries
/// the per-leaf key groups this transaction holds and is later passed back for
/// write-back.
pub(crate) struct LockedTx {
    groups: BTreeMap<ObjectPath, HeldLockGroup>,
}

impl LockedTx {
    /// Pairs every routed group with its hold receipt for that path.
    fn from_receipts(
        groups: BTreeMap<ObjectPath, RoutedLockGroup>,
        mut receipts: BTreeMap<ObjectPath, LeafHoldReceipt>,
    ) -> Result<Self, TransError> {
        let mut locked = BTreeMap::new();
        for (path, group) in groups {
            let receipt = receipts.remove(&path).ok_or_else(|| {
                TransError::other(format!("lock acquisition returned no receipt for {path}"))
            })?;
            locked.insert(
                path,
                HeldLockGroup {
                    path: group.path,
                    leaf: group.leaf,
                    intents: group.intents,
                    receipt,
                },
            );
        }
        if !receipts.is_empty() {
            return Err(TransError::other(
                "lock acquisition returned a receipt for an unknown leaf",
            ));
        }
        Ok(Self { groups: locked })
    }

    /// Reports whether this transaction acquired its hold from the exact leaf
    /// state that was observed earlier.
    pub(crate) fn validated(&self, observed: &LeafObservation) -> bool {
        self.groups
            .values()
            .any(|group| group.receipt.observation().same_state(observed))
    }

    /// The typed entry and leaf locks GC records on the transaction object for
    /// its reverse liveness check and lock pruning (ADR-022).
    pub(crate) fn locked_paths(&self) -> Vec<TxLock> {
        let mut out = Vec::new();
        for group in self.groups.values() {
            debug_assert_eq!(
                group.receipt.held().entry_lock,
                entry_lock_type(&group.intents)
            );
            for intent in &group.intents {
                out.push(TxLock::Entry {
                    key: intent.key.clone(),
                    typ: lock_type(intent.desired),
                });
            }
            if group.receipt.held().membership != LockType::None {
                out.push(TxLock::Membership {
                    leaf: group.leaf.clone(),
                    typ: group.receipt.held().membership,
                });
            }
        }
        out
    }
}

/// The lock type a `Desired` intention records for an entry lock.
fn lock_type(desired: Desired) -> LockType {
    match desired {
        Desired::Read => LockType::Read,
        Desired::Put | Desired::Delete => LockType::Write,
    }
}

/// Groups a transaction's accessed keys by their routed leaf, descending the
/// collection directory (ADR-031). Each key gets one intent carrying the lock to
/// install: a write/create/delete for a written key, a read lock for a key only
/// read. Optimistic read validation is the engine's job (it validates after
/// locking, ADR-024), so no read token is carried here.
async fn build_groups(
    router: &TreeRouter,
    accesses: &AccessSet,
    scan_requirement: Requirement,
) -> Result<BTreeMap<ObjectPath, RoutedLockGroup>, TransError> {
    // Collect before descending so the returned future does not close over a
    // borrowing iterator (which would not be higher-ranked / `Send` when a
    // caller spawns the lock).
    let items: Vec<(LogicalKey, (LogicalKey, Desired))> = accesses
        .points()
        .map(|point| {
            let desired = match point.write.map(|write| write.operation()) {
                Some(WriteOp::Delete) => Desired::Delete,
                Some(WriteOp::Put(_)) => Desired::Put,
                None => Desired::Read,
            };
            let key = point.key.clone();
            (key.clone(), (key, desired))
        })
        .collect();
    // Route with interior nodes served from cache (ADR-031 hot-path invariant):
    // a stale index misroute self-corrects via right-links, and the leaf's own
    // coordination CAS revalidates at the version, so neither the root `_r` nor
    // the terminal leaf needs a separate validation read.
    let grouped = router
        .route_keys_with_requirements(items, Requirement::Any, Requirement::Any)
        .await
        .map_err(|error| TransError::from(error).context("grouping keys by leaf"))?;

    let mut groups: BTreeMap<ObjectPath, RoutedLockGroup> = BTreeMap::new();
    for group in grouped {
        let path = group.path().clone();
        let leaf = leaf_ref(&path)?;
        let mut intents: Vec<KeyIntent> = group
            .keys
            .into_iter()
            .map(|(raw_key, (key, desired))| KeyIntent {
                key,
                raw_key,
                desired,
            })
            .collect();
        intents.sort_by(|a, b| a.raw_key.cmp(&b.raw_key));
        groups.insert(
            path.clone(),
            RoutedLockGroup {
                path,
                leaf,
                intents,
                membership: LockType::None,
            },
        );
    }
    // Lock the current cover, not the body's earlier cover. If a split moved
    // the range before locking, validation reconciles the logical page while
    // the new leaves are protected.
    for scan in accesses.range_scans() {
        if scan.range().is_empty() {
            continue;
        }
        for leaf in router
            .leaves_through(
                scan.collection(),
                &scan.range().start,
                scan.frontier(),
                scan_requirement,
            )
            .await
            .map_err(|error| error.classify_collection_absence(scan.collection()))?
        {
            let group = groups
                .entry(leaf.path.clone())
                .or_insert_with(|| RoutedLockGroup {
                    leaf: leaf_ref(&leaf.path).expect("router returned a physical leaf path"),
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

fn leaf_ref(path: &ObjectPath) -> Result<LeafRef, TransError> {
    match path {
        ObjectPath::TreeRoot { collection } => Ok(LeafRef::root(collection.clone())),
        ObjectPath::Node { collection, token } => {
            Ok(LeafRef::node(collection.clone(), token.clone()))
        }
        _ => Err(TransError::other("router returned a non-leaf object path")),
    }
}

// --- LeafBody operations (the locking policy the Locker supplies, ADR-028) -----

/// Acquires locks on its keys: resolve every key's holders (help-forward
/// committed, drop abort-side terminal, wound-wait the live pending ones) and install this
/// transaction's lock (ADR-024).
struct AcquireOperation {
    id: TxId,
    path: ObjectPath,
    intents: Arc<Vec<KeyIntent>>,
    membership: LockType,
    requirement: Requirement,
}

#[async_trait]
impl LeafResolver for AcquireOperation {
    async fn resolve(
        &self,
        ctx: &ResolveCtx<'_>,
        staged: &BTreeMap<Vec<u8>, LeafEntry>,
        staged_locks: &NodeLocks,
    ) -> Result<Step, TransError> {
        let mut locks = staged_locks.clone();
        let reconciler = NodeLockReconciler::new(ctx.key_state, ctx.tmon, &self.id);
        if let Some(holder) = reconciler.admit_non_structural(&mut locks).await? {
            return Ok(Step::Skip {
                outcome: FoldOutcome::Wait(holder),
            });
        }
        let mut membership = self.membership;
        let mut admission = StageAdmission::ExistingKeys;
        let mut entries = Vec::with_capacity(self.intents.len());
        for intent in self.intents.iter() {
            let cur = staged.get(&intent.raw_key).cloned();
            match resolve_and_lock(ctx, &self.id, intent, cur).await? {
                EntryResolution::Locked(entry, changes_membership) => {
                    if changes_membership {
                        membership = LockType::Write;
                        if intent.desired == Desired::Put {
                            admission = StageAdmission::AddsKey;
                        }
                    }
                    entries.push((intent.raw_key.clone(), entry));
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

        if membership != LockType::None {
            if let Some(holder) = reconciler
                .acquire_membership(&mut locks, membership)
                .await?
            {
                return Ok(Step::Skip {
                    outcome: FoldOutcome::Wait(holder),
                });
            }
            membership = locks.membership().lock_type();
        }
        let outcome = FoldOutcome::Locked {
            typ: entry_lock_type(&self.intents),
            membership,
        };
        let retained = locks == *staged_locks
            && entries
                .iter()
                .all(|(key, entry)| staged.get(key) == Some(entry));
        if retained {
            return Ok(Step::Skip { outcome });
        }
        Ok(Step::Stage {
            entries,
            locks,
            admission,
            outcome,
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

    fn leaf_scope_keys(&self) -> Vec<&[u8]> {
        // Acquiring a lock may create the key's entry, so it must land on the
        // routed leaf; re-route (release and re-lock) if a split moved a key
        // after routing (ADR-031).
        self.intents.iter().map(|i| i.raw_key.as_slice()).collect()
    }
}

impl LeafOperation for AcquireOperation {
    type Output = AcquireOutcome;

    fn path(&self) -> &ObjectPath {
        &self.path
    }

    fn id(&self) -> &TxId {
        &self.id
    }

    fn first_requirement(&self) -> Requirement {
        self.requirement
    }

    fn complete(&self, outcome: Option<CoordinatedOutcome>) -> Result<Self::Output, TransError> {
        let Some(coordinated) = outcome else {
            return Err(TransError::other(
                "coordinator shut down while locking leaf",
            ));
        };
        let CoordinatedOutcome { outcome, evidence } = coordinated;
        match outcome {
            FoldOutcome::Locked { typ, membership } => {
                let held = HeldLeaf {
                    entry_lock: typ,
                    membership,
                };
                evidence
                    .map(|evidence| AcquireOutcome::Locked(LeafHoldReceipt { evidence, held }))
                    .ok_or_else(|| TransError::other("lock round returned no hold receipt"))
            }
            FoldOutcome::Wait(holder) => Ok(AcquireOutcome::Wait(holder)),
            FoldOutcome::LeafFull => Ok(AcquireOutcome::LeafFull),
            // A result from another operation kind is not proof that this lock
            // landed. The safe response is the ordinary release-and-relock path.
            FoldOutcome::Conflict
            | FoldOutcome::Released { .. }
            | FoldOutcome::Reroute
            | FoldOutcome::Landed
            | FoldOutcome::Moved
            | FoldOutcome::Replay
            | FoldOutcome::InDoubt(_) => Ok(AcquireOutcome::Conflict),
        }
    }
}

/// The lock type recorded for a leaf hold: its strongest intention, so the
/// diagnostic snapshot distinguishes read-only from write holders.
fn entry_lock_type(intents: &[KeyIntent]) -> LockType {
    if intents.iter().any(|i| !matches!(i.desired, Desired::Read)) {
        LockType::Write
    } else if intents.is_empty() {
        LockType::None
    } else {
        LockType::Read
    }
}

/// Publishes its committed writes on its keys and drops its holds (ADR-020).
/// A gated leaf is mutated only after the current routed state proves this
/// transaction still has a holder that needs publishing.
struct WriteBackOperation {
    id: TxId,
    path: ObjectPath,
    intents: Arc<Vec<KeyIntent>>,
    requirement: Requirement,
}

#[async_trait]
impl LeafResolver for WriteBackOperation {
    async fn resolve(
        &self,
        ctx: &ResolveCtx<'_>,
        staged: &BTreeMap<Vec<u8>, LeafEntry>,
        staged_locks: &NodeLocks,
    ) -> Result<Step, TransError> {
        let owns_entry = self.intents.iter().any(|intent| {
            staged
                .get(&intent.raw_key)
                .is_some_and(|entry| entry.is_locked_by(&self.id))
        });
        let owns_membership = staged_locks.membership().contains(&self.id);
        let mut locks = staged_locks.clone();
        if let Some(holder) = NodeLockReconciler::new(ctx.key_state, ctx.tmon, &self.id)
            .admit_non_structural(&mut locks)
            .await?
        {
            if !owns_entry && !owns_membership {
                return Ok(Step::Skip {
                    outcome: FoldOutcome::Released {
                        superseded: Vec::new(),
                    },
                });
            }
            return Ok(Step::Skip {
                outcome: FoldOutcome::Wait(holder),
            });
        }
        let WritebackStaged {
            changes,
            superseded,
        } = writeback_changes(&self.id, &self.intents, staged);
        let outcome = FoldOutcome::Released { superseded };
        let locks_changed = locks.release_membership(&self.id);
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
        // Exhaustion proves neither publication nor that gate acquisition
        // removed our holder. Re-descend and keep converging from current
        // routing state.
        FoldOutcome::Reroute
    }

    fn reroute_outcome(&self, _in_doubt: bool) -> FoldOutcome {
        FoldOutcome::Reroute
    }

    fn leaf_scope_keys(&self) -> Vec<&[u8]> {
        self.intents
            .iter()
            .map(|intent| intent.raw_key.as_slice())
            .collect()
    }

    fn publication_keys(&self) -> Vec<&[u8]> {
        self.intents
            .iter()
            .filter(|intent| matches!(intent.desired, Desired::Put | Desired::Delete))
            .map(|intent| intent.raw_key.as_slice())
            .collect()
    }
}

impl LeafOperation for WriteBackOperation {
    type Output = WriteBackOutcome;

    fn path(&self) -> &ObjectPath {
        &self.path
    }

    fn id(&self) -> &TxId {
        &self.id
    }

    fn first_requirement(&self) -> Requirement {
        self.requirement
    }

    fn complete(&self, outcome: Option<CoordinatedOutcome>) -> Result<Self::Output, TransError> {
        match outcome {
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Released { superseded },
                ..
            }) => Ok(WriteBackOutcome::Released(superseded)),
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Reroute,
                ..
            }) => Ok(WriteBackOutcome::Reroute),
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Wait(_),
                ..
            }) => {
                // The committed log makes later publication and cleanup
                // recoverable, so a live structural gate need not delay commit.
                Ok(WriteBackOutcome::Deferred)
            }
            Some(_) => Err(TransError::other(
                "write-back produced a non-cleanup outcome",
            )),
            None => Err(TransError::other("coordinator shut down during write-back")),
        }
    }
}

/// Drops every hold this transaction has in one recovery-selected leaf,
/// publishing nothing.
#[derive(Clone)]
struct ReleaseOperation {
    id: TxId,
    path: ObjectPath,
    requirement: Requirement,
}

#[async_trait]
impl LeafResolver for ReleaseOperation {
    async fn resolve(
        &self,
        ctx: &ResolveCtx<'_>,
        staged: &BTreeMap<Vec<u8>, LeafEntry>,
        staged_locks: &NodeLocks,
    ) -> Result<Step, TransError> {
        let owns_entry = staged.values().any(|entry| entry.is_locked_by(&self.id));
        let owns_membership = staged_locks.membership().contains(&self.id);
        let mut locks = staged_locks.clone();
        if let Some(holder) = NodeLockReconciler::new(ctx.key_state, ctx.tmon, &self.id)
            .admit_non_structural(&mut locks)
            .await?
        {
            if !owns_entry && !owns_membership {
                return Ok(Step::Skip {
                    outcome: FoldOutcome::Released {
                        superseded: Vec::new(),
                    },
                });
            }
            return Ok(Step::Skip {
                outcome: FoldOutcome::Wait(holder),
            });
        }
        let changes = release_changes(&self.id, staged);
        let outcome = FoldOutcome::Released {
            superseded: Vec::new(),
        };
        let locks_changed = locks.release_membership(&self.id);
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

impl LeafOperation for ReleaseOperation {
    type Output = ReleaseOutcome;

    fn path(&self) -> &ObjectPath {
        &self.path
    }

    fn id(&self) -> &TxId {
        &self.id
    }

    fn first_requirement(&self) -> Requirement {
        self.requirement
    }

    fn complete(&self, outcome: Option<CoordinatedOutcome>) -> Result<Self::Output, TransError> {
        match outcome {
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Released { .. },
                ..
            }) => Ok(ReleaseOutcome::Released),
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Wait(holder),
                ..
            }) => Ok(ReleaseOutcome::Wait(holder)),
            Some(_) => Err(TransError::other("release produced a non-cleanup outcome")),
            None => Err(TransError::other("coordinator shut down during release")),
        }
    }
}

/// Per-key resolution within a leaf CAS attempt.
enum EntryResolution {
    /// The lock is installed in `entry`. The boolean is true when the intent
    /// creates or deletes a visible key, requiring a membership write lock.
    Locked(LeafEntry, bool),
    /// A live pending holder this transaction does not outrank: wait for it.
    Wait(TxId),
}

/// The staged result of a write-back: the entry changes to apply and the
/// `current_writer`s they superseded (GC candidates, ADR-022).
struct WritebackStaged {
    changes: Vec<(Vec<u8>, LeafEntry)>,
    superseded: Vec<TxId>,
}

enum WriteBackOutcome {
    Released(Vec<TxId>),
    Reroute,
    Deferred,
}

enum AcquireOutcome {
    Locked(LeafHoldReceipt),
    Wait(TxId),
    Conflict,
    LeafFull,
}

enum ReleaseOutcome {
    Released,
    Wait(TxId),
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
    entry: Option<LeafEntry>,
) -> Result<EntryResolution, TransError> {
    let mut e = entry.unwrap_or_else(|| LeafEntry::new(intent.raw_key.clone()));

    // Resolve existing holders other than us via the shared resolver: a
    // committed exclusive holder is help-forwarded (its value becomes the
    // effective one), aborted/missing holders are dropped, and the live pending
    // ones come back as conflicts to wound-wait. The monitor folds lease expiry
    // and the unknown-tx grace period into `tx_status`, so a holder still seen
    // as `Pending` here is genuinely live (ADR-021).
    let resolved = ctx
        .key_state
        .resolve_holders(&intent.key, Some(&e), Some(id), ctx.requirement)
        .await?;
    e.current = resolved.resolved_current(Some(&e));
    let mut pending = resolved.pending;

    let exists_before = e.current.exists();

    // Read locks share with other read holders; everything else is exclusive and
    // must clear the live pending holders via wound-wait: wound the ones we
    // outrank, and wait for the first one we do not (hold-and-wait, ADR-024) —
    // keeping every lock already acquired elsewhere.
    let compatible = matches!(intent.desired, Desired::Read)
        && !matches!(e.lock_type(), LockType::Write | LockType::Create);
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
            let mut lock = EntryLockState::read(id.clone());
            for holder in pending {
                lock.acquire_read(holder);
            }
            e.replace_lock(lock);
        }
        Desired::Put | Desired::Delete => {
            if !exists_before && matches!(intent.desired, Desired::Put) {
                e.replace_create_lock(id.clone());
            } else {
                e.replace_write_lock(id.clone());
            }
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
    entries: &BTreeMap<Vec<u8>, LeafEntry>,
) -> WritebackStaged {
    let mut changes = Vec::new();
    let mut superseded = Vec::new();
    for intent in intents {
        let Some(e) = entries.get(&intent.raw_key) else {
            continue;
        };
        if !e.is_locked_by(id) {
            continue;
        }
        let mut e = e.clone();
        match intent.desired {
            Desired::Put | Desired::Delete => {
                if let Some(prev) = e.current.writer()
                    && prev != id
                {
                    superseded.push(prev.clone());
                }
                // Replaying a write-back must not demote an authoritative
                // direct-commit value. A newly published logged write points
                // to its transaction object instead (ADR-054).
                if e.current.writer() != Some(id) {
                    e.current = if matches!(intent.desired, Desired::Delete) {
                        CurrentState::Tombstone { writer: id.clone() }
                    } else {
                        CurrentState::External { writer: id.clone() }
                    };
                }
            }
            Desired::Read => {}
        }
        e.release_lock(id);
        changes.push((intent.raw_key.clone(), e));
    }
    WritebackStaged {
        changes,
        superseded,
    }
}

/// Stages `id`'s release: drop its hold from **every** entry in the leaf,
/// publishing nothing. Release does not know the tx's keys (it runs from the
/// per-tx bookkeeping, ADR-024), so it sweeps the loaded entries. Idempotent —
/// entries `id` does not hold are untouched.
fn release_changes(id: &TxId, entries: &BTreeMap<Vec<u8>, LeafEntry>) -> Vec<(Vec<u8>, LeafEntry)> {
    let mut changes = Vec::new();
    for (k, e) in entries {
        if !e.is_locked_by(id) {
            continue;
        }
        let mut e = e.clone();
        e.release_lock(id);
        changes.push((k.clone(), e));
    }
    changes
}

/// Final outcome of acquiring every lock a transaction needs.
pub(crate) enum LockOutcome {
    /// All locks held; drives write-back on commit.
    Locked(LockedTx),
    /// Lost a CAS-contention race or reached the absolute object limit without
    /// adding a user key. The caller retains landed same-identity holds and
    /// retries the complete access set. Sustained parallel contention requests
    /// owner-level renewal before sorted serial acquisition.
    Conflict,
    /// A create reached a leaf's reserved content cap. The caller retains other
    /// leaf holds, backs off without serial escalation, and retries after the
    /// background split has had an opportunity to run.
    LeafFull,
}

/// Outcome of acquiring locks across all touched leaves.
enum LeafSetOutcome {
    Locked(BTreeMap<ObjectPath, LeafHoldReceipt>),
    Conflict,
    LeafFull,
}

/// Outcome of acquiring locks on a single leaf (after any hold-and-wait).
enum LeafOutcome {
    Locked(LeafHoldReceipt),
    Conflict,
    LeafFull,
}

/// How a hold-and-wait wake happened, so the re-poll cadence can be tuned: a
/// holder *finalizing* is real progress, while a poll timeout saw no event and
/// only re-checks for a lock released without finalizing.
enum Woke {
    /// The holder's committed or aborted status was durably verified.
    Finalized,
    /// The backed-off poll timer elapsed with no finalize event.
    PollTimeout,
}

/// Exposes data-node and collection locking through separate views.
///
/// The key view hides routing, waits, wound-wait, and CAS retries behind a
/// policy layer over the shared [`LeafCoordinator`] (ADR-028). The collection
/// view coordinates collection records independently from B-link nodes.
#[derive(Clone)]
pub struct Locker {
    keys: KeyLocker,
    collections: CollectionLocker,
}

/// Coordinates locks and committed effects on data leaves.
#[derive(Clone)]
pub(crate) struct KeyLocker {
    /// The shared leaf-mutation mechanism: dedup + resolve + CAS. Also held by
    /// the commit algorithm, so both drive one dedup.
    coord: LeafCoordinator,
    /// Routes a transaction's keys to their owning leaves by descent (ADR-031).
    router: TreeRouter,
    /// Used to park on a conflicting holder during hold-and-wait.
    tmon: Monitor,
    /// Backoff config for the hold-and-wait re-poll cadence.
    retry: RetryConfig,
    /// Maximum incomplete leaf operations in one transaction phase.
    parallelism: NonZeroUsize,
    /// Count of lock-acquisition calls (one per `lock()` attempt). Shared across
    /// clones. The coordinator cannot
    /// compute it — it only sees per-leaf submissions — so the locker owns it.
    calls: Arc<AtomicU64>,
}

impl Locker {
    /// Creates data and collection locking over their shared coordination
    /// dependencies.
    pub fn new(
        coord: LeafCoordinator,
        router: TreeRouter,
        collection_state: CollectionStateResolver,
        tmon: Monitor,
        retry: RetryConfig,
        parallelism: NonZeroUsize,
    ) -> Self {
        Locker {
            keys: KeyLocker::new(coord, router, tmon.clone(), retry, parallelism),
            collections: CollectionLocker::new(collection_state),
        }
    }

    /// Returns and resets distributed-locker activity counters.
    pub fn stats_and_reset(&self) -> LockerStats {
        self.keys.stats_and_reset()
    }

    /// Returns the data-leaf locking interface.
    pub(crate) fn keys(&self) -> &KeyLocker {
        &self.keys
    }

    /// Returns the collection locking interface.
    pub(crate) fn collections(&self) -> &CollectionLocker {
        &self.collections
    }
}

impl KeyLocker {
    /// Acquires a transaction's locks while resolving predicate-lock coverage
    /// against the supplied pre-lock requirement barrier.
    pub(crate) async fn lock_at(
        &self,
        id: &TxId,
        accesses: &AccessSet,
        serial: bool,
        scan_requirement: Requirement,
    ) -> Result<LockOutcome, TransError> {
        let groups = build_groups(&self.router, accesses, scan_requirement).await?;
        let receipts = match self
            .lock_leaves_at(id, &groups, serial, scan_requirement)
            .await?
        {
            LeafSetOutcome::Locked(receipts) => receipts,
            LeafSetOutcome::Conflict => return Ok(LockOutcome::Conflict),
            LeafSetOutcome::LeafFull => return Ok(LockOutcome::LeafFull),
        };
        Ok(LockOutcome::Locked(LockedTx::from_receipts(
            groups, receipts,
        )?))
    }

    /// Publishes `current_writer` pointers / tombstones and releases this
    /// transaction's locks across the leaves it touched. Every CAS is
    /// idempotent; errors are best-effort (a failure leaves the locks to be
    /// reclaimed lazily by the next contender or lease expiry), so this never
    /// fails an already-committed transaction. A live structural holder defers
    /// the affected leaf rather than making post-commit cleanup wait.
    /// Cancellation can leave a partial pass, but the committed log remains
    /// authoritative and every landed CAS is safe to repeat.
    ///
    /// Returns the transaction identities each published pointer *superseded* (the
    /// former `current_writer` an overwrite replaced): these just lost a
    /// reference and are GC write-back hint candidates (ADR-022).
    pub(crate) async fn write_back(&self, id: &TxId, locked: &LockedTx) -> Vec<TxId> {
        // A cancelled partial pass may lose these hints; GC's paged scan is
        // complete without them.
        let mut operations = Vec::with_capacity(locked.groups.len());
        for group in locked.groups.values() {
            operations.push(async move {
                // The hold receipt is the write-back's freshness barrier.
                let requirement = Requirement::AtLeast(group.receipt.observation().current_after());
                self.write_back_routed(
                    id,
                    &group.path,
                    Arc::new(group.intents.clone()),
                    requirement,
                )
                .await
            });
        }
        let results = join_all_bounded(operations, self.parallelism).await;
        let mut superseded = results.into_iter().flatten().collect::<Vec<_>>();
        superseded.sort();
        superseded.dedup();
        superseded
    }

    /// Publishes one committed put and releases its lock, aimed at an explicit
    /// leaf path rather than at a held [`LockedTx`], so a test can direct a
    /// delayed write-back at a path a split may already have retired.
    #[cfg(test)]
    pub(crate) async fn write_back_one_put(
        &self,
        id: &TxId,
        leaf_path: &ObjectPath,
        raw_key: &[u8],
        key: &LogicalKey,
    ) -> Vec<TxId> {
        let intents = Arc::new(vec![KeyIntent {
            raw_key: raw_key.to_vec(),
            key: key.clone(),
            desired: Desired::Put,
        }]);
        self.write_back_routed(id, leaf_path, intents, Requirement::Any)
            .await
    }

    /// Releases `id` from one exact leaf path.
    pub(crate) async fn release_leaf(
        &self,
        id: &TxId,
        path: &ObjectPath,
    ) -> Result<(), TransError> {
        // A release stages no decision that can become unsafe from a stale
        // seed; its CAS arbitrates with any newer leaf and retries on conflict.
        self.release_leaf_at(id, path, Requirement::Any).await
    }

    fn new(
        coord: LeafCoordinator,
        router: TreeRouter,
        tmon: Monitor,
        retry: RetryConfig,
        parallelism: NonZeroUsize,
    ) -> Self {
        Self {
            coord,
            router,
            tmon,
            retry,
            parallelism,
            calls: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Returns and resets distributed-locker activity counters.
    fn stats_and_reset(&self) -> LockerStats {
        LockerStats {
            calls: self.calls.swap(0, Ordering::Relaxed),
        }
    }

    async fn lock_leaves_at(
        &self,
        id: &TxId,
        groups: &BTreeMap<ObjectPath, RoutedLockGroup>,
        serial: bool,
        requirement: Requirement,
    ) -> Result<LeafSetOutcome, TransError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        // The first lock for this transaction starts the background refresh so a
        // long-lived holder's pending object is written lazily, keeping its
        // lease alive (the tx object is otherwise written only at commit).
        if !groups.is_empty() {
            self.tmon.start_refresh_tx(id);
        }

        let mut receipts = BTreeMap::new();
        if serial {
            // Ascending leaf-path order is the global lock order: the BTreeMap
            // already iterates sorted by leaf path.
            for group in groups.values() {
                match self.lock_leaf(id, group, requirement).await? {
                    LeafOutcome::Locked(receipt) => {
                        receipts.insert(group.path.clone(), receipt);
                    }
                    LeafOutcome::Conflict => return Ok(LeafSetOutcome::Conflict),
                    LeafOutcome::LeafFull => return Ok(LeafSetOutcome::LeafFull),
                }
            }
        } else {
            let mut operations = Vec::with_capacity(groups.len());
            for group in groups.values() {
                operations.push(self.lock_leaf(id, group, requirement));
            }
            let outcomes = join_all_bounded(operations, self.parallelism).await;
            for (group, outcome) in groups.values().zip(outcomes) {
                match outcome? {
                    LeafOutcome::Locked(receipt) => {
                        receipts.insert(group.path.clone(), receipt);
                    }
                    LeafOutcome::Conflict => return Ok(LeafSetOutcome::Conflict),
                    LeafOutcome::LeafFull => return Ok(LeafSetOutcome::LeafFull),
                }
            }
        }
        Ok(LeafSetOutcome::Locked(receipts))
    }

    /// Publishes a group and re-descends when a split moved any of its keys.
    async fn write_back_routed(
        &self,
        id: &TxId,
        path: &ObjectPath,
        intents: Arc<Vec<KeyIntent>>,
        requirement: Requirement,
    ) -> Vec<TxId> {
        let mut pending = VecDeque::from([(path.clone(), intents)]);
        let mut superseded = Vec::new();
        while let Some((path, intents)) = pending.pop_front() {
            let outcome = match self
                .write_back_leaf(id, &path, intents.clone(), requirement)
                .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    tracing::warn!(
                        target: "glassdb::write_back",
                        tx_id = %id,
                        path = %path,
                        error = %error,
                        "committed leaf write-back failed"
                    );
                    continue;
                }
            };
            match outcome {
                WriteBackOutcome::Released(mut ids) => superseded.append(&mut ids),
                WriteBackOutcome::Reroute => {
                    let items: Vec<(LogicalKey, KeyIntent)> = intents
                        .iter()
                        .cloned()
                        .map(|intent| (intent.key.clone(), intent))
                        .collect();
                    let groups = match self
                        .router
                        .route_keys_with_requirements(items, Requirement::Any, requirement)
                        .await
                    {
                        Ok(groups) => groups,
                        Err(error) => {
                            tracing::warn!(
                                target: "glassdb::write_back",
                                tx_id = %id,
                                path = %path,
                                error = %error,
                                "committed leaf write-back rerouting failed"
                            );
                            continue;
                        }
                    };
                    pending.extend(groups.into_iter().map(|group| {
                        let path = group.path().clone();
                        let intents = group.keys.into_iter().map(|(_, intent)| intent).collect();
                        (path, Arc::new(intents))
                    }));
                }
                // A gate on one routed leaf does not prevent independent leaves
                // from completing their best-effort cleanup in this pass.
                WriteBackOutcome::Deferred => tracing::warn!(
                    target: "glassdb::write_back",
                    tx_id = %id,
                    path = %path,
                    "committed leaf write-back was deferred"
                ),
            }
        }
        superseded
    }

    /// Coordinates this transaction's write-back on one routed leaf.
    async fn write_back_leaf(
        &self,
        id: &TxId,
        path: &ObjectPath,
        intents: Arc<Vec<KeyIntent>>,
        requirement: Requirement,
    ) -> Result<WriteBackOutcome, TransError> {
        let operation = WriteBackOperation {
            id: id.clone(),
            path: path.clone(),
            intents,
            requirement,
        };
        self.coord.coordinate(operation).await
    }

    async fn release_leaf_at(
        &self,
        id: &TxId,
        path: &ObjectPath,
        requirement: Requirement,
    ) -> Result<(), TransError> {
        let operation = ReleaseOperation {
            id: id.clone(),
            path: path.clone(),
            requirement,
        };
        let mut backoff = self.retry.backoff();
        loop {
            match self.coord.coordinate(operation.clone()).await? {
                ReleaseOutcome::Released => return Ok(()),
                ReleaseOutcome::Wait(holder) => {
                    let delay = backoff.next_delay();
                    if let Woke::Finalized = self.wait_for_holder(&holder, delay).await? {
                        backoff = self.retry.backoff();
                    }
                }
            }
        }
    }

    /// Installs this transaction's locks on every key it touches in one leaf,
    /// through the shared [`LeafCoordinator`] (ADR-025/028): the submission
    /// merges with other transactions contending the same leaf whenever they
    /// do not exclusively conflict, so one owner-driven load + CAS serves the
    /// whole batch.
    async fn lock_leaf(
        &self,
        id: &TxId,
        group: &RoutedLockGroup,
        requirement: Requirement,
    ) -> Result<LeafOutcome, TransError> {
        let intents = Arc::new(group.intents.clone());
        // Paces the hold-and-wait re-poll. It advances across successive blind
        // polls of a holder that will not budge, and resets whenever a holder
        // finalizes — real progress.
        let mut backoff = self.retry.backoff();
        loop {
            let operation = AcquireOperation {
                id: id.clone(),
                path: group.path.clone(),
                intents: intents.clone(),
                membership: group.membership,
                requirement,
            };
            match self.coord.coordinate(operation).await? {
                AcquireOutcome::Locked(receipt) => return Ok(LeafOutcome::Locked(receipt)),
                // Hold-and-wait (ADR-024): if the coordinated acquire reports
                // [`AcquireOutcome::Wait`] — a key is held by a live holder this
                // transaction cannot wound — it **waits** for that holder to
                // finalize (keeping every lock already acquired on other
                // nodes) then re-submits. The wait is *not* charged to the
                // bounded CAS-contention budget; the algo-level deadlock
                // timeout bounds the total wait and escalates to the
                // cannot-deadlock serial order.
                AcquireOutcome::Wait(holder) => {
                    let delay = backoff.next_delay();
                    if let Woke::Finalized = self.wait_for_holder(&holder, delay).await? {
                        backoff = self.retry.backoff();
                    }
                }
                AcquireOutcome::LeafFull => return Ok(LeafOutcome::LeafFull),
                AcquireOutcome::Conflict => return Ok(LeafOutcome::Conflict),
            }
        }
    }

    /// Parks until the conflicting `holder` finalizes **or** `timeout` elapses,
    /// whichever comes first, then lets the caller re-resolve, reporting which
    /// woke it.
    async fn wait_for_holder(&self, holder: &TxId, timeout: Duration) -> Result<Woke, TransError> {
        let wait = self.tmon.await_tx_final(holder);
        tokio::select! {
            status = wait => {
                status?;
                Ok(Woke::Finalized)
            },
            _ = rt::sleep(timeout) => Ok(Woke::PollTimeout),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection_coordination::CollectionStateResolver;
    use crate::engine::{AssemblyFixture, EngineConfig};
    use crate::key_state_resolver::KeyStateResolver;
    use crate::leaf_coord::SplitHinter;
    use crate::monitor::ProtocolTiming;
    use futures::future::poll_fn;
    use glassdb_backend::middleware::{
        BackendOp, HookBackend, HookFuture, OpLog, RecordingBackend,
    };
    use glassdb_backend::{Backend, memory::MemoryBackend};
    use glassdb_concurr::RetryConfig;
    use glassdb_data::{CollectionAddress, CollectionId, DbRoot, ObjectPath};
    use glassdb_storage::transaction::TxCommitStatus;
    use glassdb_storage::{
        CollectionRecord, LeafBody, LeafEntry, Node, NodeStore, SplitPolicy, Timeline, TreeRouter,
    };
    use std::future::Future;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::Poll;
    use std::time::Duration;
    use tokio::sync::Notify;

    struct NoSplitHints;

    impl SplitHinter for NoSplitHints {
        fn observe_leaf(&self, _path: &ObjectPath, _leaf: &LeafBody) {}
    }

    struct TlCtx {
        nodes: NodeStore,
        timeline: Timeline,
        monitor: Monitor,
        coord: LeafCoordinator,
        _foundation: AssemblyFixture,
    }

    async fn new_test_locker(b: Arc<dyn Backend>) -> (Locker, TlCtx) {
        new_test_locker_with_policy(b, SplitPolicy::default()).await
    }

    async fn new_test_locker_with_policy(
        b: Arc<dyn Backend>,
        policy: SplitPolicy,
    ) -> (Locker, TlCtx) {
        new_test_locker_with_parallelism(b, policy, NonZeroUsize::MIN).await
    }

    async fn new_test_locker_with_parallelism(
        b: Arc<dyn Backend>,
        policy: SplitPolicy,
        parallelism: NonZeroUsize,
    ) -> (Locker, TlCtx) {
        let mut config = EngineConfig::default();
        config.set_cache_size(1024);
        config.set_protocol_timing(ProtocolTiming::simulation());
        config.set_transaction_leaf_parallelism(parallelism);
        let foundation = AssemblyFixture::new(b, DbRoot::try_from("test").unwrap(), &config);
        let timeline = foundation.timeline.clone();
        let tl = foundation.tlogger.clone();
        let mon = foundation.monitor.clone();
        let records = foundation.records.clone();
        let nodes = foundation.nodes.clone();
        assert!(
            records
                .create_record(&collection(), &CollectionRecord::new())
                .await
                .unwrap()
        );
        assert!(
            nodes
                .create_root(&collection(), &Node::leaf(LeafBody::new()))
                .await
                .unwrap()
        );
        let key_state = KeyStateResolver::new(mon.clone());
        let router = TreeRouter::new(nodes.clone(), parallelism);
        let coord = LeafCoordinator::with_hinter(
            nodes.clone(),
            key_state,
            mon.clone(),
            RetryConfig::default(),
            policy,
            Arc::new(NoSplitHints),
        );
        let locker = Locker::new(
            coord.clone(),
            router,
            CollectionStateResolver::new(records, tl, mon.clone(), RetryConfig::default()),
            mon.clone(),
            RetryConfig::default(),
            parallelism,
        );
        (
            locker,
            TlCtx {
                nodes,
                timeline,
                monitor: mon,
                coord,
                _foundation: foundation,
            },
        )
    }

    async fn init_tl_test() -> (Locker, TlCtx) {
        new_test_locker(Arc::new(MemoryBackend::new())).await
    }

    // Builds a deterministic, valid transaction identity. A smaller `order` yields an
    // older (higher-priority) transaction under the wound-wait rule.
    fn mk_tid(order: u64, name: &str) -> TxId {
        TxId::with_priority(order * 1_000_000_000, name.as_bytes())
    }

    fn collection() -> CollectionAddress {
        CollectionAddress::root("test")
    }

    fn root_path() -> ObjectPath {
        ObjectPath::TreeRoot {
            collection: collection(),
        }
    }

    fn logical_key(key: &[u8]) -> LogicalKey {
        LogicalKey::new(collection(), key)
    }

    fn read_intent(key: &[u8]) -> KeyIntent {
        KeyIntent {
            raw_key: key.to_vec(),
            key: logical_key(key),
            desired: Desired::Read,
        }
    }

    fn put_intent(key: &[u8]) -> KeyIntent {
        KeyIntent {
            raw_key: key.to_vec(),
            key: logical_key(key),
            desired: Desired::Put,
        }
    }

    #[test]
    fn exhausted_write_back_requires_rerouting() {
        let resolver = WriteBackOperation {
            id: mk_tid(1, "writer"),
            path: root_path(),
            intents: Arc::new(vec![put_intent(b"key")]),
            requirement: Requirement::Any,
        };

        assert!(matches!(
            resolver.exhausted_outcome(false),
            FoldOutcome::Reroute
        ));
        assert!(matches!(
            resolver.exhausted_outcome(true),
            FoldOutcome::Reroute
        ));
    }

    // Routes an intent to the collection's single leaf `_r` (ADR-031: with split
    // deferred, every key coordinates on the root leaf). The `key` is carried by
    // the intent itself, so it is only used for readability at call sites.
    fn group_of(_key: &[u8], intent: KeyIntent) -> BTreeMap<ObjectPath, RoutedLockGroup> {
        group_of_intents(vec![intent])
    }

    // Several intents held by one transaction on that same leaf.
    fn group_of_intents(intents: Vec<KeyIntent>) -> BTreeMap<ObjectPath, RoutedLockGroup> {
        let path = root_path();
        let mut g = BTreeMap::new();
        g.insert(
            path.clone(),
            RoutedLockGroup {
                path,
                leaf: LeafRef::root(collection()),
                intents,
                membership: LockType::None,
            },
        );
        g
    }

    async fn entry_of(ctx: &TlCtx, key: &[u8]) -> Option<LeafEntry> {
        let loaded = ctx
            .nodes
            .load_leaf(&root_path(), Requirement::AtLeast(ctx.timeline.now()))
            .await
            .unwrap();
        loaded.entries().lookup(key).cloned()
    }

    async fn replace_root(ctx: &TlCtx, root: &Node) {
        let (_, observed) = ctx
            .nodes
            .load_root(&collection(), Requirement::AtLeast(ctx.timeline.now()))
            .await
            .unwrap();
        assert!(
            ctx.nodes
                .store_root(&collection(), root, &observed)
                .await
                .unwrap()
        );
    }

    // Acquires leaf locks in parallel mode, asserting success.
    async fn lock_ok(
        locker: &Locker,
        id: &TxId,
        groups: &BTreeMap<ObjectPath, RoutedLockGroup>,
    ) -> BTreeMap<ObjectPath, LeafHoldReceipt> {
        lock_ok_at(locker, id, groups, Requirement::Any).await
    }

    // Acquires leaf locks against an explicit pre-lock requirement.
    async fn lock_ok_at(
        locker: &Locker,
        id: &TxId,
        groups: &BTreeMap<ObjectPath, RoutedLockGroup>,
        requirement: Requirement,
    ) -> BTreeMap<ObjectPath, LeafHoldReceipt> {
        match locker
            .keys()
            .lock_leaves_at(id, groups, false, requirement)
            .await
            .unwrap()
        {
            LeafSetOutcome::Locked(receipts) => receipts,
            LeafSetOutcome::Conflict => panic!("expected lock acquisition to succeed"),
            LeafSetOutcome::LeafFull => panic!("expected leaf to have capacity"),
        }
    }

    #[tokio::test]
    async fn lock_write_creates_entry() {
        let (locker, ctx) = init_tl_test().await;
        let key = b"key";
        let tx = mk_tid(1, "tx");
        ctx.monitor.begin_tx(&tx);

        let groups = group_of(key, put_intent(key));
        lock_ok(&locker, &tx, &groups).await;

        // A create installs the entry lock and membership-W while proving the
        // structural gate open in the same leaf CAS.
        let e = entry_of(&ctx, key).await.expect("entry installed");
        assert_eq!(e.lock_type(), LockType::Create);
        assert_eq!(e.lock_holders(), std::slice::from_ref(&tx));
        let loaded = ctx
            .nodes
            .load_leaf(&root_path(), Requirement::AtLeast(ctx.timeline.now()))
            .await
            .unwrap();
        assert!(loaded.node().structural_gate().holders().is_empty());
        assert_eq!(loaded.node().membership_lock().lock_type(), LockType::Write);
        assert!(loaded.node().membership_lock().contains(&tx));
        assert_eq!(loaded.node().membership_version(), 1);
    }

    #[tokio::test]
    async fn complete_retained_hold_needs_no_second_cas() {
        let recorder = Arc::new(RecordingBackend::new(Arc::new(MemoryBackend::new())));
        let log = recorder.log();
        let (locker, ctx) = new_test_locker(recorder).await;
        let tx = mk_tid(1, "tx");
        ctx.monitor.begin_tx(&tx);
        let path = root_path().to_string();
        let one_key = group_of_intents(vec![put_intent(b"apple")]);

        log.lock().unwrap().clear();
        let receipts = lock_ok(&locker, &tx, &one_key).await;
        assert!(matches!(
            receipts[&root_path()],
            LeafHoldReceipt {
                evidence: CoordinationEvidence::Installed(_),
                ..
            }
        ));
        assert_eq!(count_stores(&log, &path), 1);

        log.lock().unwrap().clear();
        let receipts = lock_ok(&locker, &tx, &one_key).await;
        assert!(matches!(
            receipts[&root_path()],
            LeafHoldReceipt {
                evidence: CoordinationEvidence::Observed(_),
                ..
            }
        ));
        assert_eq!(count_stores(&log, &path), 0);

        let two_keys = group_of_intents(vec![put_intent(b"apple"), put_intent(b"mango")]);
        log.lock().unwrap().clear();
        let receipts = lock_ok(&locker, &tx, &two_keys).await;
        assert!(matches!(
            receipts[&root_path()],
            LeafHoldReceipt {
                evidence: CoordinationEvidence::Installed(_),
                ..
            }
        ));
        assert_eq!(count_stores(&log, &path), 1);
    }

    #[tokio::test]
    async fn every_hold_receipt_meets_the_acquisition_requirement() {
        let (locker, ctx) = init_tl_test().await;
        let tx = mk_tid(1, "tx");
        ctx.monitor.begin_tx(&tx);
        let groups = group_of_intents(vec![put_intent(b"apple")]);

        let installed_at = ctx.timeline.now();
        let installed = lock_ok_at(&locker, &tx, &groups, Requirement::AtLeast(installed_at)).await;
        let receipt = &installed[&root_path()];
        assert!(matches!(
            receipt,
            LeafHoldReceipt {
                evidence: CoordinationEvidence::Installed(_),
                ..
            }
        ));
        assert!(receipt.observation().current_after() >= installed_at);

        let observed_at = ctx.timeline.now();
        let observed = lock_ok_at(&locker, &tx, &groups, Requirement::AtLeast(observed_at)).await;
        let receipt = &observed[&root_path()];
        assert!(matches!(
            receipt,
            LeafHoldReceipt {
                evidence: CoordinationEvidence::Observed(_),
                ..
            }
        ));
        assert!(receipt.observation().current_after() >= observed_at);
    }

    #[tokio::test]
    async fn stable_mutation_does_not_resolve_unrelated_entry_holders() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let recorder = Arc::new(RecordingBackend::new(backend));
        let log = recorder.log();
        let (locker, ctx) = new_test_locker(recorder).await;
        let unrelated = mk_tid(1, "unrelated");
        let tx = mk_tid(2, "tx");
        let mut other = LeafEntry::new(b"other");
        other.replace_write_lock(unrelated.clone());
        let root = Node::leaf(LeafBody::from_entries([other]));
        replace_root(&ctx, &root).await;
        log.lock().unwrap().clear();
        ctx.monitor.begin_tx(&tx);

        lock_ok(&locker, &tx, &group_of(b"target", put_intent(b"target"))).await;

        let unrelated_path = ObjectPath::Transaction {
            db_root: DbRoot::try_from("test").unwrap(),
            id: unrelated,
        }
        .to_string();
        assert!(
            log.lock()
                .unwrap()
                .iter()
                .all(|op| op.path != unrelated_path),
            "a disjoint mutation must not inspect an unrelated holder"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn mutation_waits_for_a_live_structural_gate() {
        let (locker, ctx) = init_tl_test().await;
        let gate = mk_tid(1, "gate");
        let tx = mk_tid(2, "tx");
        ctx.monitor.begin_tx(&gate);
        ctx.monitor.begin_tx(&tx);
        let mut node = Node::leaf(LeafBody::new());
        node.set_structural_gate(gate.clone());
        replace_root(&ctx, &node).await;

        let waiting_locker = locker.clone();
        let waiting_tx = tx.clone();
        let waiting = tokio::spawn(async move {
            waiting_locker
                .keys()
                .lock_leaves_at(
                    &waiting_tx,
                    &group_of(b"target", put_intent(b"target")),
                    false,
                    Requirement::Any,
                )
                .await
        });
        rt::sleep(Duration::from_millis(50)).await;
        assert!(!waiting.is_finished());

        ctx.monitor
            .commit_tx(glassdb_storage::transaction::TxLog::new(
                gate,
                TxCommitStatus::Ok,
            ))
            .await
            .unwrap();
        assert!(matches!(
            waiting.await.unwrap().unwrap(),
            LeafSetOutcome::Locked(_)
        ));
        let loaded = ctx
            .nodes
            .load_leaf(&root_path(), Requirement::AtLeast(ctx.timeline.now()))
            .await
            .unwrap();
        assert!(loaded.node().structural_gate().holders().is_empty());
        assert_eq!(
            loaded.entries().lookup(b"target").unwrap().lock_holders(),
            std::slice::from_ref(&tx)
        );
    }

    #[tokio::test]
    async fn create_at_content_cap_reports_leaf_full_without_staging() {
        let writer = mk_tid(0, "seed");
        let tx = mk_tid(1, "tx");
        let existing = LeafEntry::new(b"a").with_current(CurrentState::External { writer });
        let mut created = LeafEntry::new(b"z");
        created.replace_create_lock(tx.clone());
        let mut node = Node::leaf(LeafBody::from_entries([existing, created]));
        node.set_membership_writer(tx.clone());
        let content_limit = node.content_encoded_len() - 1;
        let node_max_bytes = node.encoded_len() + 64;
        let policy = SplitPolicy::builder()
            .node_max_bytes(node_max_bytes)
            .split_headroom_bytes(node_max_bytes - content_limit)
            .build()
            .unwrap();

        let (locker, ctx) =
            new_test_locker_with_policy(Arc::new(MemoryBackend::new()), policy).await;
        seed_committed(&ctx, b"a", b"old").await;
        ctx.monitor.begin_tx(&tx);

        let outcome = locker
            .keys()
            .lock_leaves_at(
                &tx,
                &group_of(b"z", put_intent(b"z")),
                false,
                Requirement::Any,
            )
            .await
            .unwrap();
        assert!(matches!(outcome, LeafSetOutcome::LeafFull));
        assert!(entry_of(&ctx, b"z").await.is_none());
        let loaded = ctx
            .nodes
            .load_leaf(&root_path(), Requirement::AtLeast(ctx.timeline.now()))
            .await
            .unwrap();
        assert!(loaded.node().structural_gate().holders().is_empty());
        assert!(!loaded.node().membership_lock().contains(&tx));
    }

    #[tokio::test]
    async fn overwrite_does_not_take_membership_lock() {
        let (locker, ctx) = init_tl_test().await;
        let key = b"key";
        seed_committed(&ctx, key, b"old").await;
        let tx = mk_tid(1, "tx");
        ctx.monitor.begin_tx(&tx);

        lock_ok(&locker, &tx, &group_of(key, put_intent(key))).await;
        let loaded = ctx
            .nodes
            .load_leaf(&root_path(), Requirement::AtLeast(ctx.timeline.now()))
            .await
            .unwrap();
        assert!(loaded.node().structural_gate().holders().is_empty());
        assert!(loaded.node().membership_lock().holders().is_empty());
        assert_eq!(loaded.node().membership_version(), 0);
    }

    #[tokio::test]
    async fn scan_membership_reader_does_not_bump_version() {
        let (locker, ctx) = init_tl_test().await;
        let key = b"key";
        seed_committed(&ctx, key, b"old").await;
        let tx = mk_tid(1, "scan");
        ctx.monitor.begin_tx(&tx);

        let mut groups = group_of(key, put_intent(key));
        groups.get_mut(&root_path()).unwrap().membership = LockType::Read;
        lock_ok(&locker, &tx, &groups).await;

        let path = root_path();
        let loaded = ctx
            .nodes
            .load_leaf(&path, Requirement::AtLeast(ctx.timeline.now()))
            .await
            .unwrap();
        assert_eq!(loaded.node().membership_lock().lock_type(), LockType::Read);
        assert!(loaded.node().membership_lock().contains(&tx));
        assert_eq!(loaded.node().membership_version(), 0);

        locker.keys().release_leaf(&tx, &path).await.unwrap();
        let loaded = ctx
            .nodes
            .load_leaf(&path, Requirement::AtLeast(ctx.timeline.now()))
            .await
            .unwrap();
        assert!(loaded.node().membership_lock().holders().is_empty());
        assert_eq!(loaded.node().membership_version(), 0);
    }

    #[tokio::test]
    async fn shared_read_locks() {
        let (locker, ctx) = init_tl_test().await;
        let key = b"key";
        let tx1 = mk_tid(1, "tx1");
        let tx2 = mk_tid(2, "tx2");
        ctx.monitor.begin_tx(&tx1);
        ctx.monitor.begin_tx(&tx2);

        lock_ok(&locker, &tx1, &group_of(key, read_intent(key))).await;
        lock_ok(&locker, &tx2, &group_of(key, read_intent(key))).await;

        let e = entry_of(&ctx, key).await.unwrap();
        assert_eq!(e.lock_type(), LockType::Read);
        let mut holders = e.lock_holders().to_vec();
        holders.sort_by_key(|t| t.to_string());
        let mut expected = vec![tx1.clone(), tx2.clone()];
        expected.sort_by_key(|t| t.to_string());
        assert_eq!(holders, expected);
    }

    #[tokio::test(start_paused = true)]
    async fn older_wounds_younger_write_holder() {
        let (locker, ctx) = init_tl_test().await;
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
        assert_eq!(e.lock_holders(), std::slice::from_ref(&old));
        assert_eq!(
            ctx.monitor.tx_status(&young).await.unwrap(),
            TxCommitStatus::Wounded
        );
    }

    // Hold-and-wait (ADR-024): a younger transaction cannot wound an older
    // holder, so it *waits* for it (keeping any other locks) and proceeds once
    // the holder finalizes — it never aborts on the conflict.
    #[tokio::test(start_paused = true)]
    async fn younger_waits_for_older_holder() {
        let (locker, ctx) = init_tl_test().await;
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
                .keys()
                .lock_leaves_at(&young2, &groups, false, Requirement::Any)
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
        ctx.monitor.abort_owned_tx(&old).await.unwrap();
        let outcome = waiting.await.unwrap().unwrap();
        assert!(
            matches!(outcome, LeafSetOutcome::Locked(_)),
            "younger proceeds once the holder finalizes"
        );

        let e = entry_of(&ctx, key).await.unwrap();
        assert_eq!(e.lock_holders(), std::slice::from_ref(&young));
    }

    // ADR-024: after waiting, a younger transaction help-forwards a holder that
    // *commits* (rather than aborts) — taking the lock over the holder's now
    // committed value.
    #[tokio::test(start_paused = true)]
    async fn younger_proceeds_after_older_holder_commits() {
        use glassdb_storage::transaction::{TxLog, TxWrite};
        let (locker, ctx) = init_tl_test().await;
        let key = b"key";
        seed_committed(&ctx, key, b"v0").await;

        let old = mk_tid(1, "old");
        ctx.monitor.begin_tx(&old);
        let old_groups = group_of(key, put_intent(key));
        let old_receipts = lock_ok(&locker, &old, &old_groups).await;
        let old_locked = LockedTx::from_receipts(old_groups, old_receipts).unwrap();

        // Younger contender blocks waiting for `old`.
        let young = mk_tid(2, "young");
        ctx.monitor.begin_tx(&young);
        let locker2 = locker.clone();
        let young2 = young.clone();
        let groups = group_of(key, put_intent(key));
        let waiting = tokio::spawn(async move {
            locker2
                .keys()
                .lock_leaves_at(&young2, &groups, false, Requirement::Any)
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
            key: logical_key(key),
            value: Arc::from(&b"v1"[..]),
            deleted: false,
            prev_writer: TxId::default(),
        }];
        ctx.monitor.commit_tx(tl).await.unwrap();
        locker.keys().write_back(&old, &old_locked).await;

        let outcome = waiting.await.unwrap().unwrap();
        assert!(
            matches!(outcome, LeafSetOutcome::Locked(_)),
            "younger proceeds once the holder commits"
        );

        let e = entry_of(&ctx, key).await.unwrap();
        assert_eq!(e.lock_holders(), std::slice::from_ref(&young));
        // The committed writer was help-forwarded as the effective value.
        assert_eq!(e.current.writer(), Some(&old));
    }

    #[tokio::test]
    async fn write_back_publishes_and_releases() {
        let (locker, ctx) = init_tl_test().await;
        let key = b"key";
        let tx = mk_tid(1, "tx");
        ctx.monitor.begin_tx(&tx);

        let groups = group_of(key, put_intent(key));
        let receipts = lock_ok(&locker, &tx, &groups).await;
        let locked = LockedTx::from_receipts(groups, receipts).unwrap();
        // First writer of a fresh key overwrites no pointer: no GC hint.
        let superseded = locker.keys().write_back(&tx, &locked).await;
        assert!(superseded.is_empty());

        let e = entry_of(&ctx, key).await.unwrap();
        assert_eq!(e.lock_type(), LockType::None);
        assert!(e.lock_holders().is_empty());
        assert_eq!(e.current, CurrentState::External { writer: tx });
        let loaded = ctx
            .nodes
            .load_leaf(&root_path(), Requirement::AtLeast(ctx.timeline.now()))
            .await
            .unwrap();
        assert!(loaded.node().structural_gate().holders().is_empty());
        assert!(loaded.node().membership_lock().holders().is_empty());
        assert_eq!(loaded.node().membership_version(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn write_back_defers_at_a_live_structural_gate() {
        let (locker, ctx) = init_tl_test().await;
        let key = b"key";
        seed_committed(&ctx, key, b"old").await;

        let writer = mk_tid(2, "writer");
        let locked = lock_commit(&locker, &ctx, &writer, key).await;
        let gate = mk_tid(1, "gate");
        ctx.monitor.begin_tx(&gate);
        let loaded = ctx
            .nodes
            .load_leaf(&root_path(), Requirement::AtLeast(ctx.timeline.now()))
            .await
            .unwrap();
        let mut node = loaded.node().clone();
        node.set_structural_gate(gate.clone());
        replace_root(&ctx, &node).await;

        let superseded = tokio::time::timeout(
            Duration::from_secs(1),
            locker.keys().write_back(&writer, &locked),
        )
        .await
        .expect("post-commit write-back waited on a structural gate");
        assert!(superseded.is_empty());

        let loaded = ctx
            .nodes
            .load_leaf(&root_path(), Requirement::AtLeast(ctx.timeline.now()))
            .await
            .unwrap();
        assert_eq!(loaded.node().structural_gate().holders(), &[gate]);
        let entry = loaded.entries().lookup(key).unwrap();
        assert_eq!(entry.lock_holders(), std::slice::from_ref(&writer));
        assert_eq!(entry.current.writer(), Some(&mk_tid(0, "seed")));
    }

    // Write-back over an existing key returns the `current_writer` it overwrote:
    // that transaction identity lost its reference and is the GC candidate hint (ADR-022).
    #[tokio::test]
    async fn write_back_returns_superseded_writer() {
        let (locker, ctx) = init_tl_test().await;
        let key = b"key";

        // First committer publishes the pointer for `key`; it supersedes nothing.
        let old = mk_tid(1, "old");
        let lt_old = lock_commit(&locker, &ctx, &old, key).await;
        assert!(locker.keys().write_back(&old, &lt_old).await.is_empty());
        assert_eq!(
            entry_of(&ctx, key).await.unwrap().current.writer(),
            Some(&old)
        );

        // A second committer overwrites the same key; its write-back reports the
        // pointer it replaced.
        let new = mk_tid(2, "new");
        let lt_new = lock_commit(&locker, &ctx, &new, key).await;
        assert_eq!(locker.keys().write_back(&new, &lt_new).await, vec![old]);
        assert_eq!(
            entry_of(&ctx, key).await.unwrap().current.writer(),
            Some(&new)
        );
    }

    // A later acquisition help-forwards committed holders as transaction-object
    // pointers. Logged values are not copied into leaf entries (ADR-054).
    #[tokio::test]
    async fn committed_logged_values_are_help_forwarded_as_external() {
        use glassdb_storage::transaction::{TxLog, TxWrite};
        let (locker, ctx) = init_tl_test().await;
        let first = b"key-a".to_vec();
        let second = same_leaf_sibling(&first);

        // A committed writer whose write-back never ran, so the next acquisition
        // must help-forward both of its keys.
        let writer = mk_tid(1, "writer");
        ctx.monitor.begin_tx(&writer);
        lock_ok(
            &locker,
            &writer,
            &group_of_intents(vec![put_intent(&first), put_intent(&second)]),
        )
        .await;
        let mut tl = TxLog::new(writer.clone(), TxCommitStatus::Ok);
        tl.writes = [(&first, b"aaaaa"), (&second, b"bbbbb")]
            .into_iter()
            .map(|(key, value)| TxWrite {
                key: logical_key(key),
                value: Arc::from(&value[..]),
                deleted: false,
                prev_writer: TxId::default(),
            })
            .collect();
        ctx.monitor.commit_tx(tl).await.unwrap();

        let reader = mk_tid(2, "reader");
        ctx.monitor.begin_tx(&reader);
        lock_ok(
            &locker,
            &reader,
            &group_of_intents(vec![read_intent(&first), read_intent(&second)]),
        )
        .await;

        assert_eq!(
            entry_of(&ctx, &first).await.unwrap().current,
            CurrentState::External {
                writer: writer.clone()
            }
        );
        assert_eq!(
            entry_of(&ctx, &second).await.unwrap().current,
            CurrentState::External { writer }
        );
    }

    // Existing inline values are grandfathered: a later lock acquisition
    // republishes the entry's own state rather than demoting it. An inline value
    // may be its key's only copy.
    #[tokio::test]
    async fn an_acquisition_preserves_an_existing_inline_value() {
        let (locker, ctx) = init_tl_test().await;
        let key = b"key";
        let inlined = CurrentState::Inline {
            writer: mk_tid(1, "writer"),
            value: Arc::from(b"kept".as_slice()),
        };
        replace_root(
            &ctx,
            &Node::leaf(LeafBody::from_entries([
                LeafEntry::new(key).with_current(inlined.clone())
            ])),
        )
        .await;

        let reader = mk_tid(2, "reader");
        ctx.monitor.begin_tx(&reader);
        lock_ok(&locker, &reader, &group_of(key, read_intent(key))).await;

        assert_eq!(
            entry_of(&ctx, key).await.unwrap().current,
            inlined,
            "the existing inline value survives lock reconciliation"
        );
    }

    // A replayed cleanup for the same writer must likewise preserve an inline
    // value: it may be the only durable copy.
    #[tokio::test]
    async fn replayed_write_back_preserves_its_existing_inline_value() {
        let (locker, ctx) = init_tl_test().await;
        let key = b"key";
        let tx = mk_tid(1, "writer");
        let inlined = CurrentState::Inline {
            writer: tx.clone(),
            value: Arc::from(b"kept".as_slice()),
        };
        let mut entry = LeafEntry::new(key).with_current(inlined.clone());
        entry.replace_write_lock(tx.clone());
        replace_root(&ctx, &Node::leaf(LeafBody::from_entries([entry]))).await;

        let group = group_of(key, put_intent(key)).remove(&root_path()).unwrap();
        locker
            .keys()
            .write_back_routed(&tx, &group.path, Arc::new(group.intents), Requirement::Any)
            .await;

        let entry = entry_of(&ctx, key).await.unwrap();
        assert_eq!(entry.current, inlined);
        assert!(entry.lock_holders().is_empty());
    }

    // The node's hard cap is no licence to demote either: an acquisition whose
    // staged entry no longer fits is refused rather than republishing the value
    // it carries forward as a pointer. That value may be the key's only copy —
    // a logless writer (ADR-051) has no transaction object to restore it from.
    #[tokio::test]
    async fn a_full_leaf_refuses_a_lock_rather_than_dropping_an_inline_value() {
        let key = b"key";
        let writer = mk_tid(1, "writer");
        let reader = mk_tid(2, "reader");
        let inlined = CurrentState::Inline {
            writer: writer.clone(),
            value: Arc::from(b"kept".as_slice()),
        };
        let seeded = LeafEntry::new(key).with_current(inlined.clone());
        // A cap with room for the read lock over a pointer, but not over the
        // inline bytes the entry actually carries: demoting the payload is the
        // only way this acquisition could fit.
        let mut demoted = LeafEntry::new(key).with_current(CurrentState::External { writer });
        demoted.acquire_read_lock(reader.clone());
        let policy = SplitPolicy::builder()
            .node_max_bytes(Node::leaf(LeafBody::from_entries([demoted])).encoded_len())
            .split_headroom_bytes(0)
            .build()
            .unwrap();
        assert!(
            Node::leaf(LeafBody::from_entries([seeded.clone()])).encoded_len()
                <= policy.node_max_bytes(),
            "the seeded leaf must fit, so only the acquisition can overflow it"
        );
        let (locker, ctx) =
            new_test_locker_with_policy(Arc::new(MemoryBackend::new()), policy).await;
        replace_root(&ctx, &Node::leaf(LeafBody::from_entries([seeded]))).await;

        ctx.monitor.begin_tx(&reader);
        let outcome = locker
            .keys()
            .lock_leaves_at(
                &reader,
                &group_of(key, read_intent(key)),
                false,
                Requirement::Any,
            )
            .await
            .unwrap();

        assert!(
            matches!(outcome, LeafSetOutcome::Conflict),
            "a leaf with no room for the lock conflicts"
        );
        assert_eq!(
            entry_of(&ctx, key).await.unwrap().current,
            inlined,
            "the inline value the acquisition carried forward is intact"
        );
    }

    // Helper: commit a value for `key` so the leaf records a `current_writer`,
    // making the key exist (so subsequent writes take a Write, not Create, lock).
    async fn seed_committed(ctx: &TlCtx, key: &[u8], value: &[u8]) {
        use glassdb_storage::transaction::{TxLog, TxWrite};
        let writer = mk_tid(0, "seed");
        ctx.monitor.begin_tx(&writer);
        let mut tl = TxLog::new(writer.clone(), TxCommitStatus::Ok);
        tl.writes = vec![TxWrite {
            key: logical_key(key),
            value: Arc::from(value),
            deleted: false,
            prev_writer: TxId::default(),
        }];
        ctx.monitor.commit_tx(tl).await.unwrap();

        // Install the committed pointer directly in the collection's leaf `_r`.
        let path = root_path();
        let loaded = ctx
            .nodes
            .load_leaf(&path, Requirement::AtLeast(ctx.timeline.now()))
            .await
            .unwrap();
        let mut entries: BTreeMap<Vec<u8>, LeafEntry> = loaded
            .entries()
            .entries()
            .cloned()
            .map(|e| (e.key.clone(), e))
            .collect();
        entries.insert(
            key.to_vec(),
            LeafEntry::new(key).with_current(CurrentState::External { writer }),
        );
        let new_leaf = LeafBody::from_entries(entries.into_values());
        let mut edit = loaded.into_edit();
        edit.set_entries(new_leaf);
        assert!(ctx.nodes.commit_leaf(edit).await.unwrap());
    }

    // --- ADR-025: cross-transaction lock-acquisition deduplication ----------

    /// Test hook that, while **armed**, blocks the next configured backend
    /// operation until released. Every other call passes through. Arming is
    /// deferred so setup can finish before the phase under test is gated.
    #[derive(Clone, Copy)]
    enum GateKind {
        Read,
        Write,
    }

    struct Gate {
        gate: Arc<Notify>,
        armed: AtomicBool,
        kind: GateKind,
    }

    impl Gate {
        fn wrap(inner: Arc<dyn Backend>, armed: bool) -> (Arc<HookBackend>, Arc<Self>) {
            Self::wrap_kind(inner, armed, GateKind::Read)
        }

        fn wrap_writes(inner: Arc<dyn Backend>, armed: bool) -> (Arc<HookBackend>, Arc<Self>) {
            Self::wrap_kind(inner, armed, GateKind::Write)
        }

        fn wrap_kind(
            inner: Arc<dyn Backend>,
            armed: bool,
            kind: GateKind,
        ) -> (Arc<HookBackend>, Arc<Self>) {
            let gate = Arc::new(Gate {
                gate: Arc::new(Notify::new()),
                armed: AtomicBool::new(armed),
                kind,
            });
            let backend = HookBackend::new(inner);
            backend.set_before({
                let gate = gate.clone();
                move |op| {
                    let matches = match gate.kind {
                        GateKind::Read => matches!(
                            op,
                            BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                        ),
                        GateKind::Write => matches!(op, BackendOp::WriteIf { .. }),
                    };
                    let wait = matches && gate.armed.swap(false, Ordering::SeqCst);
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
        /// Gates the next configured operation until [`Self::release`].
        fn arm(&self) {
            self.armed.store(true, Ordering::SeqCst);
        }
        /// Wakes the operation parked by the gate.
        fn release(&self) {
            self.gate.notify_one();
        }
    }

    /// Holds every conditional leaf write while it is armed.
    struct WriteBatchGate {
        armed: AtomicBool,
        parked: AtomicUsize,
        released: AtomicUsize,
    }

    impl WriteBatchGate {
        fn install(backend: &Arc<HookBackend>) -> Arc<Self> {
            let gate = Arc::new(Self {
                armed: AtomicBool::new(false),
                parked: AtomicUsize::new(0),
                released: AtomicUsize::new(0),
            });
            backend.set_before({
                let gate = gate.clone();
                move |operation| {
                    let gate = gate.clone();
                    let wait = gate.armed.load(Ordering::SeqCst)
                        && matches!(operation, BackendOp::WriteIf { .. });
                    let future: HookFuture = Box::pin(async move {
                        if wait {
                            gate.park().await;
                        }
                        Ok(())
                    });
                    future
                }
            });
            gate
        }

        fn arm(&self) {
            self.armed.store(true, Ordering::SeqCst);
        }

        async fn park(&self) {
            let ticket = self.parked.fetch_add(1, Ordering::SeqCst);
            while self.released.load(Ordering::SeqCst) <= ticket {
                rt::yield_now().await;
            }
        }

        fn release_parked(&self) -> usize {
            let parked = self.parked.load(Ordering::SeqCst);
            parked - self.released.swap(parked, Ordering::SeqCst)
        }
    }

    async fn write_widths<F: Future>(
        operation: F,
        gate: &WriteBatchGate,
    ) -> (F::Output, Vec<usize>) {
        let mut operation = std::pin::pin!(operation);
        let mut widths = Vec::new();
        loop {
            let mut stable = 0;
            let mut seen = gate.parked.load(Ordering::SeqCst);
            let settled = loop {
                if let Poll::Ready(output) =
                    poll_fn(|cx| Poll::Ready(operation.as_mut().poll(cx))).await
                {
                    break Some(output);
                }
                let parked = gate.parked.load(Ordering::SeqCst);
                if parked == seen {
                    stable += 1;
                    if stable == 8 {
                        break None;
                    }
                } else {
                    seen = parked;
                    stable = 0;
                }
                rt::yield_now().await;
            };
            match settled {
                Some(output) => return (output, widths),
                None => {
                    let width = gate.release_parked();
                    assert!(width > 0, "leaf work stopped without a conditional write");
                    widths.push(width);
                }
            }
        }
    }

    async fn batch_gated_locker(parallelism: NonZeroUsize) -> (Locker, TlCtx, Arc<WriteBatchGate>) {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let gate = WriteBatchGate::install(&backend);
        let (locker, ctx) =
            new_test_locker_with_parallelism(backend, SplitPolicy::default(), parallelism).await;
        (locker, ctx, gate)
    }

    async fn groups_on_distinct_leaves(
        ctx: &TlCtx,
        count: usize,
    ) -> BTreeMap<ObjectPath, RoutedLockGroup> {
        let mut groups = BTreeMap::new();
        for index in 0..count {
            let id = CollectionId::from_slice(&[index as u8 + 1; 16]).unwrap();
            let collection = CollectionAddress::new("test", id);
            ctx.nodes
                .create_root(&collection, &Node::leaf(LeafBody::new()))
                .await
                .unwrap();
            let path = ObjectPath::TreeRoot {
                collection: collection.clone(),
            };
            groups.insert(
                path.clone(),
                RoutedLockGroup {
                    path,
                    leaf: LeafRef::root(collection.clone()),
                    intents: vec![KeyIntent {
                        raw_key: b"key".to_vec(),
                        key: LogicalKey::new(collection, b"key"),
                        desired: Desired::Put,
                    }],
                    membership: LockType::None,
                },
            );
        }
        groups
    }

    /// A locker whose backend records ops and gates the first read.
    async fn gated_locker() -> (Locker, TlCtx, OpLog, Arc<Gate>) {
        gated_locker_with(true).await
    }

    /// As [`gated_locker`], but `armed` chooses whether the gate is active from
    /// the start (gate acquisition) or deferred until `arm` (gate a later phase,
    /// e.g. write-back, after un-gated setup).
    async fn gated_locker_with(armed: bool) -> (Locker, TlCtx, OpLog, Arc<Gate>) {
        gated_locker_for(armed, GateKind::Read).await
    }

    async fn write_gated_locker() -> (Locker, TlCtx, OpLog, Arc<Gate>) {
        gated_locker_for(false, GateKind::Write).await
    }

    async fn gated_locker_for(armed: bool, kind: GateKind) -> (Locker, TlCtx, OpLog, Arc<Gate>) {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (backend, gate) = match kind {
            GateKind::Read => Gate::wrap(mem, armed),
            GateKind::Write => Gate::wrap_writes(mem, armed),
        };
        let recorder = Arc::new(RecordingBackend::new(backend));
        let log = recorder.log();
        let (locker, ctx) = new_test_locker(recorder).await;
        log.lock().unwrap().clear();
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

    #[tokio::test]
    async fn parallel_lock_acquisition_obeys_the_leaf_bound() {
        let (locker, ctx, gate) = batch_gated_locker(NonZeroUsize::new(2).unwrap()).await;
        let groups = groups_on_distinct_leaves(&ctx, 5).await;
        let tx = mk_tid(1, "tx");
        ctx.monitor.begin_tx(&tx);

        gate.arm();
        let (outcome, widths) = write_widths(
            locker
                .keys()
                .lock_leaves_at(&tx, &groups, false, Requirement::Any),
            &gate,
        )
        .await;

        assert!(matches!(outcome.unwrap(), LeafSetOutcome::Locked(_)));
        assert_eq!(widths, vec![2, 2, 1]);
    }

    #[tokio::test]
    async fn committed_write_back_obeys_the_leaf_bound() {
        use glassdb_storage::transaction::{TxLog, TxWrite};

        let (locker, ctx, gate) = batch_gated_locker(NonZeroUsize::new(2).unwrap()).await;
        let groups = groups_on_distinct_leaves(&ctx, 5).await;
        let tx = mk_tid(1, "tx");
        ctx.monitor.begin_tx(&tx);
        let writes = groups
            .values()
            .flat_map(|group| &group.intents)
            .map(|intent| TxWrite {
                key: intent.key.clone(),
                value: Arc::from(b"value".as_slice()),
                deleted: false,
                prev_writer: TxId::default(),
            })
            .collect();
        let receipts = lock_ok(&locker, &tx, &groups).await;
        let locked = LockedTx::from_receipts(groups, receipts).unwrap();
        let mut log = TxLog::new(tx.clone(), TxCommitStatus::Ok);
        log.writes = writes;
        ctx.monitor.commit_tx(log).await.unwrap();

        gate.arm();
        let (superseded, widths) =
            write_widths(locker.keys().write_back(&tx, &locked), &gate).await;

        assert!(superseded.is_empty());
        assert_eq!(widths, vec![2, 2, 1]);
    }

    /// A distinct key that shares the same leaf as `base`, for exercising
    /// disjoint-key contention within a single leaf object. With split deferred,
    /// every key lives in the collection's single leaf `_r` (ADR-031), so any
    /// distinct key qualifies.
    fn same_leaf_sibling(base: &[u8]) -> Vec<u8> {
        let sib = b"sibling".to_vec();
        assert_ne!(sib, base, "sibling must differ from the base key");
        sib
    }

    // Two concurrent read-lockers on one key merge into a single CAS round: one
    // load + one store serves both, and both end up holding the shared read lock.
    #[tokio::test(start_paused = true)]
    async fn concurrent_readers_share_one_cas() {
        let (locker, ctx, log, gate) = gated_locker().await;
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
            l1.keys()
                .lock_leaves_at(&t1, &g1, false, Requirement::Any)
                .await
        });
        let h2 = tokio::spawn(async move {
            l2.keys()
                .lock_leaves_at(&t2, &g2, false, Requirement::Any)
                .await
        });

        // Under paused time this sleep only fires once both tasks are parked (the
        // driver in the gated load, the second queued); then release the load.
        rt::sleep(Duration::from_millis(50)).await;
        gate.release();

        assert!(matches!(
            h1.await.unwrap().unwrap(),
            LeafSetOutcome::Locked(_)
        ));
        assert!(matches!(
            h2.await.unwrap().unwrap(),
            LeafSetOutcome::Locked(_)
        ));

        let leaf_path = root_path().to_string();
        assert_eq!(
            count_stores(&log, &leaf_path),
            1,
            "two readers must share a single CAS"
        );
        let e = entry_of(&ctx, key).await.unwrap();
        assert_eq!(e.lock_type(), LockType::Read);
        assert_eq!(
            e.lock_holders().len(),
            2,
            "both readers hold the shared lock"
        );
    }

    // Two concurrent writers on *disjoint* keys of the same leaf do not conflict,
    // so they batch into one CAS round rather than each doing its own load+store.
    #[tokio::test(start_paused = true)]
    async fn concurrent_disjoint_writers_share_one_cas() {
        let (locker, ctx, log, gate) = gated_locker_with(false).await;
        let ka = b"key-a".to_vec();
        let kb = same_leaf_sibling(&ka);
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
            l1.keys()
                .lock_leaves_at(&t1, &g1, false, Requirement::Any)
                .await
        });
        let h2 = tokio::spawn(async move {
            l2.keys()
                .lock_leaves_at(&t2, &g2, false, Requirement::Any)
                .await
        });

        rt::sleep(Duration::from_millis(50)).await;
        gate.release();

        assert!(matches!(
            h1.await.unwrap().unwrap(),
            LeafSetOutcome::Locked(_)
        ));
        assert!(matches!(
            h2.await.unwrap().unwrap(),
            LeafSetOutcome::Locked(_)
        ));

        let leaf_path = root_path().to_string();
        assert_eq!(
            count_stores(&log, &leaf_path),
            1,
            "disjoint writers batch into one CAS"
        );
        assert_eq!(
            entry_of(&ctx, &ka).await.unwrap().lock_holders(),
            std::slice::from_ref(&tx1)
        );
        assert_eq!(
            entry_of(&ctx, &kb).await.unwrap().lock_holders(),
            std::slice::from_ref(&tx2)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn disjoint_creates_serialize_on_membership_write() {
        let (locker, ctx) = init_tl_test().await;
        let ka = b"key-a".to_vec();
        let kb = same_leaf_sibling(&ka);
        let old = mk_tid(1, "old");
        let young = mk_tid(2, "young");
        ctx.monitor.begin_tx(&old);
        ctx.monitor.begin_tx(&young);

        lock_ok(&locker, &old, &group_of(&ka, put_intent(&ka))).await;

        let (waiting_locker, waiting_id) = (locker.clone(), young.clone());
        let waiting_group = group_of(&kb, put_intent(&kb));
        let waiting = tokio::spawn(async move {
            waiting_locker
                .keys()
                .lock_leaves_at(&waiting_id, &waiting_group, false, Requirement::Any)
                .await
        });
        rt::sleep(Duration::from_millis(50)).await;
        assert!(!waiting.is_finished());

        ctx.monitor.abort_owned_tx(&old).await.unwrap();
        assert!(matches!(
            waiting.await.unwrap().unwrap(),
            LeafSetOutcome::Locked(_)
        ));
        assert_eq!(
            entry_of(&ctx, &kb).await.unwrap().lock_holders(),
            std::slice::from_ref(&young)
        );
    }

    // Locks + commits `key` for `tx`, leaving the leaf entry holding the write
    // lock, so a later `write_back` publishes it. Returns the acquired handle.
    async fn lock_commit(locker: &Locker, ctx: &TlCtx, tx: &TxId, key: &[u8]) -> LockedTx {
        use glassdb_storage::transaction::{TxLog, TxWrite};
        ctx.monitor.begin_tx(tx);
        let groups = group_of(key, put_intent(key));
        let receipts = lock_ok(locker, tx, &groups).await;
        let locked = LockedTx::from_receipts(groups, receipts).unwrap();
        let mut tl = TxLog::new(tx.clone(), TxCommitStatus::Ok);
        tl.writes = vec![TxWrite {
            key: logical_key(key),
            value: Arc::from(&b"v"[..]),
            deleted: false,
            prev_writer: TxId::default(),
        }];
        ctx.monitor.commit_tx(tl).await.unwrap();
        locked
    }

    // These coordinator-fold tests deliberately drive cleanup with `Any` so a
    // gated backend load keeps the round open long enough for peers to merge.
    // Production write-back instead uses each retained acquisition receipt.
    async fn write_back_any(locker: &Locker, id: &TxId, locked: &LockedTx) {
        for group in locked.groups.values() {
            locker
                .keys()
                .write_back_routed(
                    id,
                    &group.path,
                    Arc::new(group.intents.clone()),
                    Requirement::Any,
                )
                .await;
        }
    }

    // Two committed transactions writing *disjoint* keys of one leaf write back
    // concurrently. Write-backs never lock-conflict, so they merge into a single
    // CAS round (ADR-026) that publishes both pointers and drops both holds.
    #[tokio::test(start_paused = true)]
    async fn concurrent_write_backs_share_one_cas() {
        // Gate is deferred so the un-gated lock+commit setup runs first.
        let (locker, ctx, log, gate) = gated_locker_with(false).await;
        let ka = b"key-a".to_vec();
        let kb = same_leaf_sibling(&ka);
        let leaf_path = root_path().to_string();

        let tx1 = mk_tid(1, "w1");
        let tx2 = mk_tid(2, "w2");
        seed_committed(&ctx, &ka, b"a").await;
        seed_committed(&ctx, &kb, b"b").await;
        let lt1 = lock_commit(&locker, &ctx, &tx1, &ka).await;
        let lt2 = lock_commit(&locker, &ctx, &tx2, &kb).await;

        // Gate only the write-back phase and count the stores it adds.
        let before = count_stores(&log, &leaf_path);
        gate.arm();
        let (l1, l2) = (locker.clone(), locker.clone());
        let (t1, t2) = (tx1.clone(), tx2.clone());
        let h1 = tokio::spawn(async move { write_back_any(&l1, &t1, &lt1).await });
        let h2 = tokio::spawn(async move { write_back_any(&l2, &t2, &lt2).await });
        rt::sleep(Duration::from_millis(50)).await;
        gate.release();
        h1.await.unwrap();
        h2.await.unwrap();

        assert_eq!(
            count_stores(&log, &leaf_path) - before,
            1,
            "two write-backs on one leaf share a single CAS"
        );
        let ea = entry_of(&ctx, &ka).await.unwrap();
        assert_eq!(ea.current.writer(), Some(&tx1));
        assert!(ea.lock_holders().is_empty());
        let eb = entry_of(&ctx, &kb).await.unwrap();
        assert_eq!(eb.current.writer(), Some(&tx2));
        assert!(eb.lock_holders().is_empty());
    }

    // A write-back reorders into a concurrent acquire round for the same leaf on
    // a disjoint key (ADR-026): one CAS both publishes the committer's pointer and
    // installs the new acquirer's lock.
    #[tokio::test(start_paused = true)]
    async fn write_back_folds_into_acquire_round() {
        let (locker, ctx, log, gate) = gated_locker_with(false).await;
        let ka = b"key-a".to_vec();
        let kb = same_leaf_sibling(&ka);
        let leaf_path = root_path().to_string();

        let tx1 = mk_tid(1, "w1");
        let lt1 = lock_commit(&locker, &ctx, &tx1, &ka).await;
        let tx2 = mk_tid(2, "w2");
        ctx.monitor.begin_tx(&tx2);
        let g2 = group_of(&kb, put_intent(&kb));

        let before = count_stores(&log, &leaf_path);
        gate.arm();
        let (l1, l2) = (locker.clone(), locker.clone());
        let (t1, t2) = (tx1.clone(), tx2.clone());
        // The write-back is the driver (parks in the gated load); the acquire
        // queues and is absorbed once the load returns.
        let hw = tokio::spawn(async move { write_back_any(&l1, &t1, &lt1).await });
        let ha = tokio::spawn(async move {
            l2.keys()
                .lock_leaves_at(&t2, &g2, false, Requirement::Any)
                .await
        });
        rt::sleep(Duration::from_millis(50)).await;
        gate.release();
        hw.await.unwrap();
        assert!(matches!(
            ha.await.unwrap().unwrap(),
            LeafSetOutcome::Locked(_)
        ));

        assert_eq!(
            count_stores(&log, &leaf_path) - before,
            1,
            "the write-back folds into the acquire's CAS round"
        );
        assert_eq!(
            entry_of(&ctx, &ka).await.unwrap().current.writer(),
            Some(&tx1)
        );
        assert_eq!(
            entry_of(&ctx, &kb).await.unwrap().lock_holders(),
            std::slice::from_ref(&tx2)
        );
    }

    // Cancelling the inline write-back driver must hand a merged live acquire
    // to a new dedup owner. The committed write remains recoverable and a later
    // idempotent write-back can finish it.
    #[tokio::test(start_paused = true)]
    async fn cancelled_write_back_hands_off_a_merged_acquire() {
        let (locker, ctx, _log, gate) = write_gated_locker().await;
        let written_key = b"key-a".to_vec();
        let acquired_key = same_leaf_sibling(&written_key);
        seed_committed(&ctx, &written_key, b"old-a").await;
        seed_committed(&ctx, &acquired_key, b"old-b").await;

        let writer = mk_tid(1, "writer");
        let locked = Arc::new(lock_commit(&locker, &ctx, &writer, &written_key).await);
        let acquirer = mk_tid(2, "acquirer");
        ctx.monitor.begin_tx(&acquirer);
        let acquire_group = group_of(&acquired_key, put_intent(&acquired_key));

        gate.arm();
        let write_locker = locker.clone();
        let write_id = writer.clone();
        let write_locked = locked.clone();
        let write_back = tokio::spawn(async move {
            write_locker
                .keys()
                .write_back(&write_id, &write_locked)
                .await
        });
        rt::sleep(Duration::from_millis(50)).await;
        assert!(
            !write_back.is_finished(),
            "write-back must be the gated driver"
        );

        let acquire_locker = locker.clone();
        let acquire_id = acquirer.clone();
        let acquire = tokio::spawn(async move {
            acquire_locker
                .keys()
                .lock_leaves_at(&acquire_id, &acquire_group, false, Requirement::Any)
                .await
        });
        rt::sleep(Duration::from_millis(50)).await;
        assert!(
            !acquire.is_finished(),
            "the acquire must be queued behind write-back"
        );

        write_back.abort();
        assert!(write_back.await.unwrap_err().is_cancelled());
        gate.release();

        let acquired = tokio::time::timeout(Duration::from_secs(1), acquire)
            .await
            .expect("cancelling write-back orphaned the merged acquire")
            .unwrap()
            .unwrap();
        assert!(matches!(acquired, LeafSetOutcome::Locked(_)));

        let written = entry_of(&ctx, &written_key).await.unwrap();
        assert_eq!(written.lock_holders(), std::slice::from_ref(&writer));
        assert_ne!(written.current.writer(), Some(&writer));
        assert_eq!(
            entry_of(&ctx, &acquired_key).await.unwrap().lock_holders(),
            std::slice::from_ref(&acquirer)
        );

        locker.keys().write_back(&writer, &locked).await;
        let written = entry_of(&ctx, &written_key).await.unwrap();
        assert!(written.lock_holders().is_empty());
        assert_eq!(written.current.writer(), Some(&writer));
    }

    // A write-back CAS can land immediately before its future is cancelled.
    // CachedStore must discard uncertain local knowledge, and replaying the
    // idempotent write-back must converge on the landed committed state.
    #[tokio::test]
    async fn cancelled_landed_write_back_is_recoverable() {
        let memory: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let hook = HookBackend::new(memory);
        let (locker, ctx) = new_test_locker(hook.clone()).await;
        let key = b"key";
        seed_committed(&ctx, key, b"old").await;

        let writer = mk_tid(1, "writer");
        let locked = Arc::new(lock_commit(&locker, &ctx, &writer, key).await);
        let landed = Arc::new(Notify::new());
        let leaf_path = root_path().to_string();
        hook.set_after({
            let landed = landed.clone();
            move |operation, outcome| {
                let park = matches!(operation, BackendOp::WriteIf { path, .. }
                    if *path == leaf_path)
                    && outcome.is_success();
                let landed = landed.clone();
                Box::pin(async move {
                    if park {
                        landed.notify_one();
                        std::future::pending::<()>().await;
                    }
                    Ok(())
                })
            }
        });

        let task_locker = locker.clone();
        let task_writer = writer.clone();
        let task_locked = locked.clone();
        let write_back = tokio::spawn(async move {
            task_locker
                .keys()
                .write_back(&task_writer, &task_locked)
                .await
        });
        landed.notified().await;
        write_back.abort();
        assert!(write_back.await.unwrap_err().is_cancelled());
        hook.clear_after();

        let entry = entry_of(&ctx, key).await.unwrap();
        assert!(entry.lock_holders().is_empty());
        assert_eq!(entry.current.writer(), Some(&writer));

        locker.keys().write_back(&writer, &locked).await;
        let entry = entry_of(&ctx, key).await.unwrap();
        assert!(entry.lock_holders().is_empty());
        assert_eq!(entry.current.writer(), Some(&writer));
    }

    // Two recovery releases for disjoint keys of one leaf batch into one CAS
    // round (ADR-026); neither publishes a value.
    #[tokio::test(start_paused = true)]
    async fn concurrent_releases_share_one_cas() {
        let (locker, ctx, log, gate) = gated_locker_with(false).await;
        let ka = b"key-a".to_vec();
        let kb = same_leaf_sibling(&ka);
        let leaf_path = root_path().to_string();

        let tx1 = mk_tid(1, "r1");
        let tx2 = mk_tid(2, "r2");
        ctx.monitor.begin_tx(&tx1);
        ctx.monitor.begin_tx(&tx2);
        seed_committed(&ctx, &ka, b"a").await;
        seed_committed(&ctx, &kb, b"b").await;
        lock_ok(&locker, &tx1, &group_of(&ka, put_intent(&ka))).await;
        lock_ok(&locker, &tx2, &group_of(&kb, put_intent(&kb))).await;

        let before = count_stores(&log, &leaf_path);
        gate.arm();
        let (l1, l2) = (locker.clone(), locker.clone());
        let (t1, t2) = (tx1.clone(), tx2.clone());
        let path1 = root_path();
        let path2 = root_path();
        let h1 = tokio::spawn(async move { l1.keys().release_leaf(&t1, &path1).await });
        let h2 = tokio::spawn(async move { l2.keys().release_leaf(&t2, &path2).await });
        rt::sleep(Duration::from_millis(50)).await;
        gate.release();
        h1.await.unwrap().unwrap();
        h2.await.unwrap().unwrap();

        assert_eq!(
            count_stores(&log, &leaf_path) - before,
            1,
            "two releases on one leaf share a single CAS"
        );
        // Both locks are gone; the seeded committed pointers remain unchanged.
        assert!(
            entry_of(&ctx, &ka).await.unwrap().lock_holders().is_empty(),
            "first lock released"
        );
        assert!(
            entry_of(&ctx, &kb).await.unwrap().lock_holders().is_empty(),
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
        let (locker, ctx, log, gate) = gated_locker().await;
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
            lo.keys()
                .lock_leaves_at(&to, &go, false, Requirement::Any)
                .await
        });
        let hy = tokio::spawn(async move {
            ly.keys()
                .lock_leaves_at(&ty, &gy, false, Requirement::Any)
                .await
        });

        // Once both tasks are parked (driver in the gated load, the other queued),
        // release the load so the round folds both members.
        rt::sleep(Duration::from_millis(50)).await;
        gate.release();

        // The older locks; the younger is left waiting on it, not wounded.
        assert!(matches!(
            ho.await.unwrap().unwrap(),
            LeafSetOutcome::Locked(_)
        ));
        rt::sleep(Duration::from_millis(50)).await;
        assert!(!hy.is_finished(), "the younger waits for the older holder");

        let leaf_path = root_path().to_string();
        assert_eq!(
            count_stores(&log, &leaf_path),
            1,
            "same-key writers share a single CAS round"
        );
        assert_eq!(
            entry_of(&ctx, key).await.unwrap().lock_holders(),
            std::slice::from_ref(&old)
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
        let (locker, ctx, log, gate) = gated_locker().await;
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
            lo.keys()
                .lock_leaves_at(&to, &go, false, Requirement::Any)
                .await
        });
        let hy = tokio::spawn(async move {
            ly.keys()
                .lock_leaves_at(&ty, &gy, false, Requirement::Any)
                .await
        });

        rt::sleep(Duration::from_millis(50)).await;
        gate.release();
        assert!(matches!(
            ho.await.unwrap().unwrap(),
            LeafSetOutcome::Locked(_)
        ));

        // The older releases; the younger's hold-and-wait loop then re-acquires.
        locker
            .keys()
            .release_leaf(&old, &root_path())
            .await
            .unwrap();
        assert!(matches!(
            hy.await.unwrap().unwrap(),
            LeafSetOutcome::Locked(_)
        ));
        assert_eq!(
            entry_of(&ctx, key).await.unwrap().lock_holders(),
            std::slice::from_ref(&young)
        );

        // A load per poll, but only three CAS stores: the older's acquire, the
        // older's release, then the younger's acquire. The younger's waiting
        // rounds stage nothing, so they add no stores.
        let leaf_path = root_path().to_string();
        assert_eq!(count_stores(&log, &leaf_path), 3);
    }

    // ADR-028 regression (equal priority): two same-priority writers on one key
    // never wound each other (that would livelock across renews). The monotonic
    // fold's round-local byte tiebreak still picks one deterministic winner; the
    // loser waits and, after the winner releases, proceeds. Both make progress.
    #[tokio::test(start_paused = true)]
    async fn equal_priority_same_key_one_winner_no_livelock() {
        let (locker, ctx, log, gate) = gated_locker().await;
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
            la.keys()
                .lock_leaves_at(&ta, &ga, false, Requirement::Any)
                .await
        });
        let hb = tokio::spawn(async move {
            lb.keys()
                .lock_leaves_at(&tb, &gb, false, Requirement::Any)
                .await
        });

        rt::sleep(Duration::from_millis(50)).await;
        gate.release();

        // The tiebreak winner locks; the loser waits (not wounded).
        assert!(matches!(
            ha.await.unwrap().unwrap(),
            LeafSetOutcome::Locked(_)
        ));
        rt::sleep(Duration::from_millis(50)).await;
        assert!(!hb.is_finished(), "the loser waits without being wounded");
        assert_eq!(
            entry_of(&ctx, key).await.unwrap().lock_holders(),
            std::slice::from_ref(&a)
        );
        assert_eq!(
            ctx.monitor.tx_status(&b).await.unwrap(),
            TxCommitStatus::Pending
        );

        // After the winner releases, the loser proceeds: progress, no livelock.
        locker.keys().release_leaf(&a, &root_path()).await.unwrap();
        assert!(matches!(
            hb.await.unwrap().unwrap(),
            LeafSetOutcome::Locked(_)
        ));
        assert_eq!(
            entry_of(&ctx, key).await.unwrap().lock_holders(),
            std::slice::from_ref(&b)
        );

        // Three CAS stores: the winner's acquire, its release, then the loser's
        // acquire. The loser's waiting rounds stage nothing.
        let leaf_path = root_path().to_string();
        assert_eq!(count_stores(&log, &leaf_path), 3);
    }

    // ADR-028 regression (commute): a committed holder's write-back and another
    // transaction's acquire of the *same* key fold into one CAS round with the
    // same result regardless of wound-wait fold order — the write-back publishes
    // the committed pointer and drops its hold, the acquirer ends holding the
    // lock over the help-forwarded value. Run both orderings to show it commutes.
    #[tokio::test(start_paused = true)]
    async fn release_and_acquire_same_key_commute() {
        for (wb_order, acq_order) in [(1u64, 2u64), (2u64, 1u64)] {
            let (locker, ctx, log, gate) = gated_locker_with(false).await;
            let key = b"key";
            let leaf_path = root_path().to_string();

            // A committed holder leaves its write lock held pending write-back.
            let committer = mk_tid(wb_order, "wb");
            let lt = lock_commit(&locker, &ctx, &committer, key).await;
            let acquirer = mk_tid(acq_order, "acq");
            ctx.monitor.begin_tx(&acquirer);
            let g = group_of(key, put_intent(key));

            let before = count_stores(&log, &leaf_path);
            gate.arm();
            let (lw, la) = (locker.clone(), locker.clone());
            let (cw, ca) = (committer.clone(), acquirer.clone());
            let hw = tokio::spawn(async move { write_back_any(&lw, &cw, &lt).await });
            let ha = tokio::spawn(async move {
                la.keys()
                    .lock_leaves_at(&ca, &g, false, Requirement::Any)
                    .await
            });
            rt::sleep(Duration::from_millis(50)).await;
            gate.release();
            hw.await.unwrap();
            assert!(matches!(
                ha.await.unwrap().unwrap(),
                LeafSetOutcome::Locked(_)
            ));

            assert_eq!(
                count_stores(&log, &leaf_path) - before,
                1,
                "write-back and acquire share one CAS (order {wb_order}/{acq_order})"
            );
            let e = entry_of(&ctx, key).await.unwrap();
            assert_eq!(
                e.lock_holders(),
                std::slice::from_ref(&acquirer),
                "the acquirer holds the lock (order {wb_order}/{acq_order})"
            );
            assert_eq!(
                e.current.writer(),
                Some(&committer),
                "the committed value is published (order {wb_order}/{acq_order})"
            );
        }
    }

    // `close` cancels new submissions; the dedup snapshot tracks only live
    // coordination, so it is empty while idle and after an uncontended lock.
    #[tokio::test]
    async fn close_cancels_new_locks_and_snapshot_tracks_idle() {
        let (locker, ctx) = init_tl_test().await;
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
            .keys()
            .lock_leaves_at(
                &tx,
                &group_of(b"key2", put_intent(b"key2")),
                false,
                Requirement::Any,
            )
            .await;
        assert!(err.is_err(), "locking after close is cancelled");
    }

    // Dropping a waiting lock future mid-wait (the deadlock-timeout analog) must
    // not wedge the locker: the holder can still release and a fresh transaction
    // acquires the key without hanging.
    #[tokio::test(start_paused = true)]
    async fn dropped_waiter_leaves_locker_usable() {
        let (locker, ctx) = init_tl_test().await;
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
            l.keys()
                .lock_leaves_at(&y, &g, false, Requirement::Any)
                .await
        });
        rt::sleep(Duration::from_millis(50)).await;
        assert!(!waiting.is_finished(), "younger blocks on the older holder");
        waiting.abort();
        let _ = waiting.await;

        locker
            .keys()
            .release_leaf(&old, &root_path())
            .await
            .unwrap();
        let other = mk_tid(3, "other");
        ctx.monitor.begin_tx(&other);
        lock_ok(&locker, &other, &group_of(key, put_intent(key))).await;
        assert_eq!(
            entry_of(&ctx, key).await.unwrap().lock_holders(),
            std::slice::from_ref(&other)
        );
    }
}
