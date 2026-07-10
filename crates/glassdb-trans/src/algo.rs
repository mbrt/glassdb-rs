//! The transaction commit protocol with serializable isolation for the v2
//! object-native engine (ADR-016 … ADR-021).
//!
//! A read-write transaction validates its reads and installs its locks with one
//! read-modify-write CAS per touched shard (create/delete is coordinated by the
//! per-key entry lock in the owning leaf, ADR-031), flips its transaction object
//! to committed (the commit point), then publishes `current_writer` pointers and
//! releases its locks
//! (write-back). A read-only transaction takes a pure optimistic fast path: it
//! re-resolves each read's effective writer against the shards and commits if
//! none changed, taking no locks.
//!
//! Concurrency control (ADR-002 / ADR-020 / ADR-021 / ADR-024): strict two-phase
//! locking with wound-wait and leases for crash recovery. On a conflict it cannot
//! win, a younger-or-equal transaction **waits while holding its locks**
//! (hold-and-wait, ADR-024) instead of aborting; an older one wounds the holder
//! and proceeds. Distinct priorities cannot deadlock (wound-wait keeps the
//! wait-for graph acyclic); two equal-priority transactions that would cycle are
//! broken by escalating to the serial order. Lock acquisition has two modes: the
//! default **parallel** path locks every shard concurrently; after a
//! [`MAX_DEADLOCK_TIMEOUT`] wait or [`SERIAL_FALLBACK_AFTER`] failed attempts a
//! transaction releases its locks and re-acquires them under the **serial**
//! sorted order (same id, no body re-run), where first-CAS-wins on the lowest
//! contended shard guarantees one contender makes progress. Only a genuine wound
//! aborts-and-renews with priority preserved ([`TxId::renew`]).

use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use glassdb_concurr::{Background, Backoff, Clock, RetryConfig, rt};
use glassdb_data::{TxId, paths};
use glassdb_storage::{
    Freshness, LockType, PathLock, ShardEntry, StorageError, TxCommitStatus, TxLog, TxWrite,
    ValueCache, Version,
};

use crate::error::TransError;
use crate::gc::Gc;
use crate::monitor::Monitor;
use crate::resolver::{HolderResolution, Resolver};
use crate::shard_coord::{
    FoldOutcome, ReloadCause, ResolveCtx, ShardCoordinator, ShardResolver, Step,
};
use crate::tlocker::{LockOutcome, LockedTx, Locker};

/// Number of failed parallel-locking attempts before a transaction escalates to
/// the serial sorted-locking fallback (ADR-020). The parallel path is fast but
/// can *livelock* two equal-priority transactions that each grab a different
/// shard first; after this many failures the transaction switches to sorted
/// acquisition, where first-CAS-wins on the lowest contended shard guarantees
/// one of them makes progress.
const SERIAL_FALLBACK_AFTER: usize = 3;

/// Upper bound on how long a transaction blocks acquiring its locks in the
/// default parallel mode before suspecting a deadlock and escalating to the
/// serial sorted-locking fallback (ADR-024). Under hold-and-wait a
/// younger-or-equal transaction *waits* for a conflicting holder while keeping
/// its locks; distinct priorities cannot cycle (wound-wait), but two
/// equal-priority transactions can each wait on the other forever. This timeout
/// bounds that wait: on elapse the transaction releases its locks and
/// re-acquires them in the global sorted order, where one contender always
/// completes. Reuses v1's 5s budget (ADR-002 / architecture.md).
const MAX_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Status {
    New,
    Validating,
    Committed,
}

/// A single key read within a transaction.
#[derive(Debug, Clone)]
pub struct ReadAccess {
    pub path: Arc<str>,
    pub version: Option<ReadVersion>,
}

/// Identifies the version read by a transaction (the writer's transaction ID).
#[derive(Debug, Clone, Default)]
pub struct ReadVersion {
    pub last_writer: TxId,
}

impl ReadVersion {
    /// Converts to a storage version (the writer that last committed the value).
    pub(crate) fn to_storage_version(&self) -> Version {
        Version {
            writer: self.last_writer.clone(),
        }
    }
}

/// A single key write within a transaction.
#[derive(Debug, Clone)]
pub struct WriteAccess {
    pub path: Arc<str>,
    pub(crate) op: WriteOp,
}

/// The write operation staged for a key.
#[derive(Debug, Clone)]
pub(crate) enum WriteOp {
    Put(Arc<[u8]>),
    Delete,
}

impl WriteAccess {
    pub fn put(path: Arc<str>, value: Arc<[u8]>) -> Self {
        Self {
            path,
            op: WriteOp::Put(value),
        }
    }

    pub fn delete(path: Arc<str>) -> Self {
        Self {
            path,
            op: WriteOp::Delete,
        }
    }
}

/// A range/sorted listing performed within a transaction (ADR-031 phantom
/// prevention). It records the object version of every leaf the scan covered;
/// commit re-scans the same range and confirms the covered leaves and their
/// versions are unchanged, so no create/delete — which bumps its leaf's version
/// — raced the scan. Following the leaf right-sibling chain during the scan
/// makes a concurrent split visible as a changed cover (one retry), never a
/// lost or duplicated key.
#[derive(Debug, Clone)]
pub struct ScanAccess {
    /// Collection prefix the scan ranged over.
    pub prefix: Arc<str>,
    /// The leaves the scan covered, in key order, each with the object version
    /// observed at scan time (`None` for an uncreated collection root).
    pub covered: Vec<LeafCoverage>,
}

/// One leaf a scan covered: its object path and the version seen at scan time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafCoverage {
    pub path: Arc<str>,
    pub version: Option<glassdb_backend::Version>,
}

/// The reads, writes, and range scans that make up a transaction.
#[derive(Debug, Clone, Default)]
pub struct Data {
    pub reads: Vec<ReadAccess>,
    pub writes: Vec<WriteAccess>,
    pub scans: Vec<ScanAccess>,
}

/// An opaque handle to an in-progress transaction managed by [`Algo`].
pub struct Handle {
    data: Data,
    status: Status,
    id: TxId,
    /// Number of restarts so far; drives the serial-locking escalation.
    attempts: usize,
    /// Whether the transaction registered with the monitor and may hold locks,
    /// so [`Algo::end`] knows it must abort (a pure read-only fast path never
    /// engages, so it has nothing to release).
    engaged: bool,
    /// Per-transaction backoff for the internal CAS-contention retry in
    /// [`Algo::acquire_locks`] (a lost shard/root CAS race): advanced before each
    /// same-id re-lock so churning contenders spread out instead of busy-looping.
    /// The lock-holding restart paths (`restart`, `revalidate`) and the read-only
    /// validation paths deliberately do not back off.
    backoff: Backoff,
}

impl Handle {
    /// The transaction's ID.
    pub fn id(&self) -> &TxId {
        &self.id
    }
}

/// Terminal outcome of [`Algo::acquire_locks`]. CAS contention and suspected
/// deadlocks are resolved *inside* `acquire_locks` (release + same-id re-lock),
/// so they are not represented here — only the two outcomes the commit path must
/// act on remain. Read-version validation happens *after* this returns
/// [`Acquired::Locked`], so a stale read is not an acquisition outcome.
enum Acquired {
    /// Every lock is held; proceed to validate reads, then the commit point.
    Locked(LockedTx),
    /// A higher-priority peer aborted this transaction: renew the id and re-run
    /// ([`TransError::Wounded`]).
    Wounded,
}

/// The result of installing the single read-write fast path's write lock through
/// the coordinator (ADR-027 / ADR-028): the shard side of the two parallel
/// commit writes, combined by [`Algo`] with the committed-object write to decide
/// the transaction's fate.
enum InstallOutcome {
    /// The write lock is installed (or we are already in the chain): this
    /// transaction is inserted into the shard's version history.
    Landed,
    /// The entry moved out from under us: the fast path lost the race and must
    /// renew (its committed object, if written, becomes an orphan for GC).
    Moved,
    /// The lock CAS was in-doubt and the entry then moved, so it cannot be told
    /// whether the lock landed first: irreducibly in-doubt.
    InDoubt(String),
}

/// Installs the single read-write fast path's write lock (ADR-027): resolve the
/// key's effective committed writer against the freshly-folded entry, then stage
/// `locked_by = [id]` with `current_writer` help-forwarded to that writer.
/// Commit-critical and self-classifying: unlike the locker's resolvers it reports
/// its own fate (`Landed` / `Moved` / `InDoubt`) instead of a generic lock, and
/// consults [`ReloadCause`] to tell a definitive loss from an irreducible
/// in-doubt. Installed by [`Algo`] on the shard coordinator (ADR-028).
///
/// It re-resolves eligibility on **every** fold (never trusting the caller's
/// pre-check across the round): the fold loads the shard fresh, so a holder
/// installed after the pre-check must still be observed here or it would be
/// silently stomped instead of losing the race (ADR-027 / ADR-028).
struct CommitInstallResolver {
    id: TxId,
    raw_key: Vec<u8>,
    key_path: String,
    read_version: Option<TxId>,
}

#[async_trait]
impl ShardResolver for CommitInstallResolver {
    async fn resolve(
        &self,
        ctx: &ResolveCtx<'_>,
        staged: &std::collections::BTreeMap<Vec<u8>, ShardEntry>,
    ) -> Result<Step, TransError> {
        let cur = staged.get(&self.raw_key);

        // Already in the chain: our lock is installed, or a follow-on writer
        // help-forwarded us into the pointer (idempotent success, ADR-027).
        if let Some(e) = cur
            && (e.locked_by.contains(&self.id) || e.current_writer.as_ref() == Some(&self.id))
        {
            return Ok(Step::Skip {
                outcome: FoldOutcome::Landed,
            });
        }

        // Re-resolve the effective writer / eligibility against the current
        // entry: a live pending holder, a moved pointer, or a superseded read
        // means we lost the race.
        let res = match cur {
            Some(e) => {
                ctx.resolver
                    .resolve_holders(&self.key_path, e, None)
                    .await?
            }
            None => HolderResolution::default(),
        };
        let Some(effective) = eligible_writer(&res, self.read_version.as_ref()) else {
            // After an in-doubt CAS we cannot tell whether our lock landed first
            // and was then help-forwarded away, so surface in-doubt; otherwise
            // the loss is definitive and the fast path renews (ADR-027).
            let in_doubt = matches!(ctx.cause, ReloadCause::Reloaded { in_doubt: true });
            let outcome = if in_doubt {
                FoldOutcome::InDoubt(format!(
                    "single-rw lock for {} in-doubt: entry moved after uncertain CAS",
                    self.id
                ))
            } else {
                FoldOutcome::Moved
            };
            return Ok(Step::Skip { outcome });
        };

        // Stage the write lock, publishing the resolved predecessor into the
        // pointer so replacing a committed-but-not-written-back holder in
        // `locked_by` help-forwards its value instead of orphaning it (ADR-027).
        let mut e = cur.cloned().unwrap_or_else(|| ShardEntry {
            key: self.raw_key.clone(),
            lock_type: LockType::None,
            locked_by: Vec::new(),
            current_writer: None,
            deleted: false,
        });
        e.lock_type = LockType::Write;
        e.locked_by = vec![self.id.clone()];
        e.current_writer = Some(effective);
        e.deleted = false;
        Ok(Step::Stage {
            entries: vec![(self.raw_key.clone(), e)],
            // The lock is installed only once the round's CAS confirms it; on a
            // precondition/in-doubt the engine re-folds and re-classifies.
            outcome: FoldOutcome::Landed,
        })
    }

    fn reorderable(&self) -> bool {
        false
    }

    fn exhausted_outcome(&self) -> FoldOutcome {
        // Pure version churn exhausted the budget: renew and re-run (the commit
        // point is a single CAS, so this absorbs contention, not conflict).
        FoldOutcome::Moved
    }

    fn owned_keys(&self) -> Vec<&[u8]> {
        // Installing the write lock may create the entry, so it must land on the
        // leaf that owns the key — re-route (renew and re-run) if a split moved
        // it after routing (ADR-031).
        vec![self.raw_key.as_slice()]
    }
}

/// Decides the effective committed writer the single read-write fast path must
/// build on from an already-resolved holder view, or `None` when the key cannot
/// take the fast path's commit CAS.
///
/// The [`Resolver`] supplies the raw view (help-forwarding committed holders so
/// a lock held by an already-committed writer whose write-back is still pending
/// is not treated as a conflict); this predicate decides eligibility from it:
/// only a *live pending* holder blocks the fast path, and a create / put over a
/// tombstone or a read-modify-write whose read was superseded is rejected
/// (ADR-027).
fn eligible_writer(res: &HolderResolution, read_version: Option<&TxId>) -> Option<TxId> {
    // A live holder is a genuine conflict: defer to the full locked path so it
    // can wound-wait. Committed/aborted holders never reach `pending`.
    if !res.pending.is_empty() {
        return None;
    }
    // The key must currently exist; a create or a put over a tombstone has no
    // predecessor value, which the fast path does not handle.
    let writer = match &res.writer {
        Some(w) if !res.deleted => w.clone(),
        _ => return None,
    };
    match read_version {
        // A read-modify-write commits only if its read is still current.
        Some(rv) if rv != &writer => None,
        // A blind put (no read) is last-writer-wins and always serializable.
        _ => Some(writer),
    }
}

/// Coordinates transactions: read validation, locking, commit, and write-back.
#[derive(Clone)]
pub struct Algo {
    values: ValueCache,
    resolver: Resolver,
    locker: Locker,
    // The single shard-mutation coordinator (ADR-028), shared with the locker:
    // the single read-write fast path installs its lock through this — one
    // deduplicated fold round — instead of a bespoke racing shard CAS.
    coord: ShardCoordinator,
    mon: Monitor,
    gc: Gc,
    clock: Clock,
    // Weak so a captured `Algo` clone inside a spawned async-abort task does not
    // keep [`Background`] alive past DB shutdown.
    background: Option<Weak<Background>>,
}

impl Algo {
    /// Creates an algorithm coordinator. `clock` is the wall-clock source for
    /// transaction-id timestamps; pass the same clock the monitor uses so
    /// priorities and lease timing share one time base.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        values: ValueCache,
        locker: Locker,
        coord: ShardCoordinator,
        mon: Monitor,
        clock: Clock,
        gc: Gc,
        background: Option<Weak<Background>>,
        resolver: Resolver,
    ) -> Self {
        Algo {
            values,
            resolver,
            locker,
            coord,
            mon,
            gc,
            clock,
            background,
        }
    }

    /// Starts a new transaction with the given data. The id's random prefix and
    /// timestamp are deterministic under `--cfg sim`.
    pub fn begin(&self, d: Data) -> Handle {
        let id = TxId::new_at(self.clock.now());
        Handle {
            data: d,
            status: Status::New,
            id,
            attempts: 0,
            engaged: false,
            backoff: RetryConfig::default().backoff(),
        }
    }

    /// Restarts a wounded transaction, preserving its priority (timestamp) while
    /// minting a fresh log identity ([`TxId::renew`]) so it keeps its wound-wait
    /// rank and cannot be starved. Carries the backoff forward and bumps the
    /// attempt counter (which drives the serial-locking escalation).
    pub fn rebegin(&self, old: Handle) -> Handle {
        Handle {
            id: old.id.renew(),
            data: old.data,
            status: Status::New,
            attempts: old.attempts + 1,
            engaged: false,
            backoff: old.backoff,
        }
    }

    /// Validates all reads and applies all writes. Returns [`TransError::Wounded`]
    /// only when a higher-priority peer aborted this transaction, so it must
    /// retry with a fresh id (priority preserved), or [`TransError::Retry`] when
    /// the body must re-run in place — a read-only transaction whose reads
    /// changed, or a read-write transaction whose read moved before it locked
    /// the key (re-run holding its locks, ADR-024). CAS contention and suspected
    /// deadlocks are handled internally.
    pub async fn commit(&self, tx: &mut Handle) -> Result<(), TransError> {
        if tx.data.writes.is_empty() {
            return self.commit_readonly(tx).await;
        }
        // Try the single read-write fast path first (ADR-020): a lone overwrite
        // of an existing key commits with one object write + one shard CAS. On
        // ineligibility nothing has been written, so the full locked path takes
        // over under the same id.
        if self.try_commit_single_rw(tx).await?.is_some() {
            return Ok(());
        }
        self.commit_read_write(tx).await
    }

    /// Validates the reads and range scans of a read-only transaction (the
    /// error-recovery path in the db retry loop), returning [`TransError::Retry`]
    /// if any was invalidated. It holds no locks and does not back off before
    /// signalling the retry.
    pub async fn validate_reads(&self, tx: &mut Handle) -> Result<(), TransError> {
        if !tx.data.writes.is_empty() {
            return Err(TransError::other(
                "cannot validate only reads when writes are present",
            ));
        }
        if self.validate(&tx.data).await? {
            return Ok(());
        }
        self.invalidate_reads(&tx.data);
        Err(TransError::Retry)
    }

    /// Replaces the transaction's data. Allowed before commit (the db retry loop
    /// resets accesses between attempts).
    pub fn reset(&self, tx: &mut Handle, data: Data) {
        assert!(
            tx.status != Status::Committed,
            "cannot reset a committed transaction"
        );
        tx.data = data;
    }

    /// Aborts a non-committed, engaged transaction, releasing its locks (lazily,
    /// by marking its transaction object aborted). A pure read-only transaction
    /// never engaged, so there is nothing to abort.
    pub async fn end(&self, tx: &mut Handle) -> Result<(), TransError> {
        if tx.status == Status::Committed || !tx.engaged {
            return Ok(());
        }
        self.mon.abort_tx(&tx.id).await
    }

    /// Clean-shutdown asynchronous abort of `tx_id`, used when a transaction's
    /// future is dropped mid-flight so [`Algo::end`] never ran. Schedules a
    /// spawned task and returns immediately; idempotent.
    pub fn async_abort(&self, tx_id: &TxId) {
        let Some(bg) = self.background.as_ref().and_then(|w| w.upgrade()) else {
            return;
        };
        let mon = self.mon.clone();
        let tx_id = tx_id.clone();
        bg.spawn_waited(async move {
            let _ = mon.abort_tx(&tx_id).await;
        });
    }

    /// Read-only fast path: re-resolve each read's effective writer against the
    /// shards and commit if none changed. Takes no locks and writes nothing, so
    /// it never registers with the monitor.
    ///
    /// A failed validation does not back off before signalling [`Retry`]: the
    /// re-run re-reads the authoritative values (the cache was just invalidated)
    /// rather than busy-spinning on the stale ones, and an idle delay would only
    /// add commit latency.
    ///
    /// [`Retry`]: TransError::Retry
    async fn commit_readonly(&self, tx: &mut Handle) -> Result<(), TransError> {
        if self.validate(&tx.data).await? {
            tx.status = Status::Committed;
            return Ok(());
        }
        self.invalidate_reads(&tx.data);
        Err(TransError::Retry)
    }

    /// Read-write path: lock the touched shards, validate the reads now that
    /// their values are frozen, flip the transaction object to committed, then
    /// spawn the write-back in the background so commit returns without waiting
    /// for it.
    async fn commit_read_write(&self, tx: &mut Handle) -> Result<(), TransError> {
        if tx.status == Status::New {
            self.mon.begin_tx(&tx.id);
            tx.status = Status::Validating;
            tx.engaged = true;
        }

        let locked = match self.acquire_locks(tx).await? {
            Acquired::Locked(l) => l,
            // A higher-priority peer aborted us: renew the id and re-run.
            Acquired::Wounded => return self.restart(tx).await,
        };

        // Record the held lock set so both the committed object (below) and the
        // refresher's pending object describe their own back-references, which
        // is what lets GC prune this transaction's locks by reverse check
        // (ADR-022). This tracks the latest acquire; a `revalidate` re-run that
        // drops keys may under-record, which only defers those stale locks to
        // lazy reclaim, never a correctness loss.
        let locks = locked.locked_paths();
        self.mon.record_tx_locks(&tx.id, locks.clone());

        // Optimistic read validation (ADR-024): now that every touched key is
        // locked, its value is frozen, so re-resolve each read's effective
        // writer and check it still matches what the body observed. A read that
        // moved before we locked means that our snapshot is stale: re-run the
        // body to observe the new value, holding our locks and keeping our id.
        // This is the only conflict that re-runs the body.
        if !self.validate_reads_inner(&tx.data).await? {
            return self.revalidate(tx).await;
        }

        // Commit point: create-or-flip the transaction object to committed.
        if let Err(e) = self.commit_writes(&tx.data.writes, locks, &tx.id).await {
            if matches!(e, TransError::AlreadyFinalized) {
                // The log was finalized as `aborted` out from under us: a wound
                // landed between locking and commit.
                return self.restart(tx).await;
            }
            return Err(e.context(format!("committing writes for tx {}", tx.id)));
        }
        tx.status = Status::Committed;

        self.write_back(&tx.id, locked).await;
        Ok(())
    }

    /// The single read-write fast path (ADR-027, superseding ADR-020): a
    /// transaction that overwrites exactly one already-existing key commits with
    /// **two parallel writes** — the committed transaction object and one shard
    /// CAS that installs a write lock — followed by an asynchronous write-back
    /// that converts the lock to a `current_writer` pointer. Reads may only touch
    /// that same key (a found RMW or a blind put); anything else needs the full
    /// path.
    ///
    /// The lock (rather than a bare pointer) is what lets the two writes overlap:
    /// a locked entry is resolved through the holder's status, which tolerates a
    /// not-yet-discoverable object, so the object write carries no happens-before
    /// requirement against the lock write (contrast ADR-020/ADR-007).
    ///
    /// Returns `Ok(Some(()))` on a fast commit; `Ok(None)` when the transaction
    /// is not eligible, in which case *nothing has been written* and the caller
    /// falls back to the full locked path under the **same id**;
    /// [`TransError::Wounded`] when a lost race (or a wound landing in the
    /// parallel window) forces a renewed re-run (the speculatively-written
    /// committed object is left unreferenced and GC'd); and an in-doubt
    /// [`StorageError::Unavailable`] for the one irreducible ambiguity (a fast
    /// follow-on writer moved the entry during an in-doubt lock CAS).
    ///
    /// Once the committed object is written the fast path never returns
    /// `Ok(None)`: a fall-back would re-run the body under the same id against an
    /// already-committed, immutable object holding stale values. It only
    /// completes, renews, or surfaces in-doubt.
    async fn try_commit_single_rw(&self, tx: &mut Handle) -> Result<Option<()>, TransError> {
        // Static eligibility: exactly one write, and it is a put (a delete
        // publishes a tombstone, which the fast path does not handle).
        if tx.data.writes.len() != 1 {
            return Ok(None);
        }
        let write = &tx.data.writes[0];
        let WriteOp::Put(value) = &write.op else {
            return Ok(None);
        };
        let value = value.clone();
        let key_path = write.path.clone();

        // Every read must be of the written key and found: a read of another key
        // needs its own shard validated, and a not-found read of the written key
        // makes this a create (no predecessor for the fast path to build on).
        let mut read_version: Option<TxId> = None;
        for r in &tx.data.reads {
            if r.path != key_path {
                return Ok(None);
            }
            match &r.version {
                Some(v) => read_version = Some(v.last_writer.clone()),
                None => return Ok(None),
            }
        }

        let (prefix, raw_key) = paths::split_key(&key_path)
            .map_err(|e| TransError::with_source("parsing single-rw key path", e))?;

        // Check dynamic eligibility before writing anything, so a create /
        // genuinely-conflicting entry falls back to the full path with the same
        // id. A lock left by an *already-committed* writer (its write-back is
        // still pending) does not block us: it is help-forwarded to its effective
        // writer, which is the predecessor we build on (ADR-027).
        //
        // Resolve on the shard the transaction body's read already cached
        // (`AllowStale`: no revalidation round-trip). The commit-install fold
        // below re-reads the same shard through the cache (also `AllowStale`), so
        // a steady-state read-modify-write adds no backend shard load at commit
        // (ADR-030). Both are cache lookups; deduplicating the decode is a
        // separate concern (caching decoded objects).
        //
        // A stale cached snapshot stays safe: it can only make a superseded
        // read-modify-write *look* eligible, in which case the fold's
        // version-conditional CAS misses, the coordinator reloads fresh,
        // re-folds, and finds the read superseded — the fast path then renews
        // (`Wounded`).
        let (holders, locator) = self
            .resolver
            .resolve_key(&key_path, Freshness::AllowStale)
            .await?;
        let Some(effective) = eligible_writer(&holders, read_version.as_ref()) else {
            return Ok(None);
        };
        // The leaf that owns this key, resolved by descent (ADR-031). Both the
        // commit-install fold and the write-back target it directly instead of
        // recomputing a fixed-hash shard index.
        let leaf_path = locator.path;

        // Build the committed transaction object. It records the write (and the
        // pointer it will supersede, for GC's reverse check) plus the write lock
        // it holds, so a dead-but-committed object still describes its own
        // back-references (ADR-022). Unlike `commit_tx` this does *not* populate
        // the value cache: the cache is written only once the commit is
        // confirmed, so a fast path that ends up wounded or in-doubt never leaves
        // a stale entry keyed by an uncommitted writer.
        //
        // The recorded predecessor is the resolved effective writer, so it names
        // the true committed value even when the shard's `current_writer` pointer
        // still lags behind a help-forwarded holder.
        let recorded_prev = effective;
        let mut tl = TxLog::new(tx.id.clone(), TxCommitStatus::Ok);
        tl.locks = vec![PathLock {
            path: key_path.to_string(),
            typ: LockType::Write,
        }];
        tl.writes.push(TxWrite {
            path: key_path.to_string(),
            value: value.clone(),
            deleted: false,
            prev_writer: recorded_prev,
        });

        // Issue both commit-critical writes concurrently (ADR-027): the committed
        // object (its existence is the unambiguous, idempotent commit signal) and
        // the shard lock install (which inserts us into the version chain). The
        // install goes through the shard coordinator (ADR-028), so it merges with
        // any disjoint-key acquire/write-back on the same shard into one CAS
        // round instead of racing its own.
        let object = self.mon.set_final_log(&tl);
        let install = self.commit_install(
            &tx.id,
            &leaf_path,
            raw_key.clone(),
            key_path.to_string(),
            read_version.clone(),
        );
        let (object, install) = tokio::join!(object, install);

        match (object, install?) {
            // Committed: the object is durable and our lock is in the chain.
            (Ok(()), InstallOutcome::Landed) => {
                self.finish_single_rw(&tx.id, &raw_key, &prefix, &value);
                tx.status = Status::Committed;
                self.write_back_single_rw(&tx.id, &leaf_path, &raw_key, &key_path)
                    .await;
                Ok(Some(()))
            }
            // A wound landed in the parallel window: our object was finalized
            // `aborted` out from under us. The write did not commit — renew.
            (Err(TransError::AlreadyFinalized), _) => self.abandon_single_rw(tx),
            (Err(e), _) => Err(e.context(format!("writing single-rw tx object for {}", tx.id))),
            // The object committed but our lock never inserted us into the chain
            // (a follow-on writer built on the old value): the committed object
            // is an orphan — renew and let GC reclaim it.
            (Ok(()), InstallOutcome::Moved) => self.abandon_single_rw(tx),
            // In-doubt lock install whose entry then moved: we cannot tell whether
            // we committed. Surface it rather than risk a double-apply.
            (Ok(()), InstallOutcome::InDoubt(msg)) => {
                Err(TransError::Storage(StorageError::Unavailable(msg)))
            }
        }
    }

    /// Installs the single read-write fast path's write lock on `raw_key`'s shard
    /// through the shard coordinator's fold engine (ADR-028): one deduplicated
    /// round that merges with disjoint acquires/write-backs on the same shard
    /// instead of racing its own bespoke CAS. Never waits (a live holder makes it
    /// [`InstallOutcome::Moved`], not a wait). `read_version` is the read this
    /// write depends on (for a read-modify-write) or `None` for a blind put; the
    /// effective predecessor is re-resolved inside the fold against the current
    /// shard state.
    async fn commit_install(
        &self,
        id: &TxId,
        leaf_path: &str,
        raw_key: Vec<u8>,
        key_path: String,
        read_version: Option<TxId>,
    ) -> Result<InstallOutcome, TransError> {
        let resolver = Arc::new(CommitInstallResolver {
            id: id.clone(),
            raw_key,
            key_path,
            read_version,
        });
        // The commit's eligibility check just resolved this leaf through the
        // cache, so the fold's first attempt reuses that cached copy without a
        // revalidation round-trip (`AllowStale`); a stale copy self-corrects via
        // the version-conditional CAS + reload (ADR-030).
        match self
            .coord
            .submit_shard(leaf_path, id, resolver, Freshness::AllowStale)
            .await?
        {
            Some(FoldOutcome::Landed) => Ok(InstallOutcome::Landed),
            Some(FoldOutcome::InDoubt(msg)) => Ok(InstallOutcome::InDoubt(msg)),
            // A shutdown mid-flight leaves the lock un-installed, so the fast
            // path renews (its committed object, if any, is an orphan for GC).
            Some(FoldOutcome::Moved) | None => Ok(InstallOutcome::Moved),
            // Commit-install never waits, releases, or takes a generic lock.
            Some(_) => Err(TransError::other(
                "commit-install produced a non-install outcome",
            )),
        }
    }

    /// Records a fast-path commit by publishing the value to the cache keyed by
    /// this writer. The superseded pointer is handed to GC later, from the
    /// asynchronous write-back that actually overwrites `current_writer`.
    fn finish_single_rw(&self, id: &TxId, raw_key: &[u8], prefix: &str, value: &Arc<[u8]>) {
        self.values.write(
            &paths::from_key(prefix, raw_key),
            value.clone(),
            Version { writer: id.clone() },
        );
    }

    /// Converts the fast path's write lock to a published `current_writer`
    /// pointer and releases it (ADR-027 write-back), reusing the deduplicated
    /// write-back path (ADR-026). Spawned in the background so commit returns
    /// without waiting for it; run inline when no background executor exists
    /// (unit tests, or after shutdown dropped it) so the lock is not left to
    /// lazy reclaim. Best-effort: the transaction is already committed, so a
    /// failure only delays lazy lock cleanup. Feeds the superseded writer to GC.
    async fn write_back_single_rw(
        &self,
        id: &TxId,
        leaf_path: &str,
        raw_key: &[u8],
        key_path: &str,
    ) {
        match self.background.as_ref().and_then(|w| w.upgrade()) {
            Some(bg) => {
                let locker = self.locker.clone();
                let gc = self.gc.clone();
                let id = id.clone();
                let leaf_path = leaf_path.to_string();
                let raw_key = raw_key.to_vec();
                let key_path = key_path.to_string();
                bg.spawn_waited(async move {
                    let superseded = locker
                        .write_back_single_put(&id, &leaf_path, &raw_key, &key_path)
                        .await;
                    feed_gc_hints(&gc, superseded);
                });
            }
            None => {
                let superseded = self
                    .locker
                    .write_back_single_put(id, leaf_path, raw_key, key_path)
                    .await;
                feed_gc_hints(&self.gc, superseded);
            }
        }
    }

    /// Abandons a fast-path attempt whose committed object was already written
    /// but whose commit-point CAS did not land: invalidate the stale cached
    /// reads, hand the now-orphaned object to GC, and signal a renewed re-run
    /// ([`TransError::Wounded`]) so the retry gets a fresh id.
    fn abandon_single_rw(&self, tx: &Handle) -> Result<Option<()>, TransError> {
        self.invalidate_reads(&tx.data);
        self.gc.schedule_tx_cleanup(tx.id.clone());
        Err(TransError::Wounded)
    }

    /// Publishes the committed transaction's pointers and releases its locks.
    /// Idempotent and best-effort: the transaction is already durably committed,
    /// so a write-back failure only delays lazy lock cleanup, never the result.
    /// It is spawned in the background so commit returns immediately rather than
    /// waiting for the pointer publishes and lock releases; a shutdown drains
    /// the spawned task (`Background::spawn_waited`). Without a background
    /// executor (unit tests, or after shutdown dropped it) it releases inline so
    /// locks are not left to lazy reclaim.
    async fn write_back(&self, id: &TxId, locked: LockedTx) {
        match self.background.as_ref().and_then(|w| w.upgrade()) {
            Some(bg) => {
                let locker = self.locker.clone();
                let gc = self.gc.clone();
                let id = id.clone();
                bg.spawn_waited(async move {
                    let superseded = locker.write_back(&id, &locked).await;
                    feed_gc_hints(&gc, superseded);
                });
            }
            None => {
                let superseded = self.locker.write_back(id, &locked).await;
                feed_gc_hints(&self.gc, superseded);
            }
        }
    }

    /// Signals the read-write restart after a genuine wound: invalidate stale
    /// cached reads (so the retry re-reads the authoritative value rather than
    /// the stale one it would otherwise re-validate and re-conflict on) and
    /// return [`TransError::Wounded`] so the caller renews the id and re-runs.
    /// Does not back off: the wound already aborted us (its locks are
    /// immediately reclaimable), the locker's CAS loop backs off real lock
    /// contention, and a delay here would only slow the renewed retry.
    async fn restart(&self, tx: &mut Handle) -> Result<(), TransError> {
        self.invalidate_reads(&tx.data);
        Err(TransError::Wounded)
    }

    /// Acquires every lock the transaction needs, resolving both **CAS
    /// contention** and **suspected deadlocks** internally — without renewing
    /// the id or re-running the body (ADR-020/024). Only one non-success outcome
    /// leaves this loop: [`Acquired::Wounded`], a higher-priority peer having
    /// aborted us (the one conflict that must renew the id and re-run).
    ///
    /// - **CAS contention** (a shard/root lost its bounded CAS race): drop the
    ///   partial locks ([`Locker::release_locks`]) and retry under the **same
    ///   id** after backing off, so a transaction that merely lost a race never
    ///   discards its executed body. Persistent contention escalates to the
    ///   serial order, which removes the equal-priority livelock.
    /// - **Suspected deadlock** (the parallel wait exceeded
    ///   [`MAX_DEADLOCK_TIMEOUT`]): drop the out-of-order locks and re-acquire in
    ///   the global serial sorted order, where first-CAS-wins on the lowest
    ///   contended shard guarantees one contender always completes. Serial mode
    ///   cannot deadlock, so it arms no timeout.
    ///
    /// `tx.attempts` (genuine-wound restarts) starts a heavily-restarted
    /// transaction directly in the serial order as a backstop.
    async fn acquire_locks(&self, tx: &mut Handle) -> Result<Acquired, TransError> {
        let mut serial = tx.attempts >= SERIAL_FALLBACK_AFTER;
        let mut conflicts: usize = 0;
        loop {
            // A higher-priority peer may have aborted us; re-checked each
            // iteration so a wound landing during a long wait surfaces promptly
            // rather than driving a pointless re-lock.
            if self.was_wounded(tx).await {
                return Ok(Acquired::Wounded);
            }
            let outcome = if serial {
                self.locker.lock(&tx.id, &tx.data, true).await
            } else {
                tokio::select! {
                    res = self.locker.lock(&tx.id, &tx.data, false) => res,
                    _ = rt::sleep(MAX_DEADLOCK_TIMEOUT) => Err(TransError::LockTimeout),
                }
            };
            match outcome {
                Ok(LockOutcome::Locked(l)) => return Ok(Acquired::Locked(l)),
                // CAS contention: drop the partial locks and retry under the same
                // id after backing off — no renew, no body re-run. Escalate to
                // the serial order if contention persists.
                Ok(LockOutcome::Conflict) => {
                    self.release_for_retry(tx).await?;
                    conflicts += 1;
                    serial = serial || conflicts >= SERIAL_FALLBACK_AFTER;
                    rt::sleep(tx.backoff.next_delay()).await;
                }
                // Suspected deadlock: drop the out-of-order locks and re-acquire
                // in the cannot-deadlock serial order, keeping our id.
                Err(TransError::LockTimeout) => {
                    self.release_for_retry(tx).await?;
                    serial = true;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Releases every lock the transaction currently holds before an in-place,
    /// same-id re-lock (the CAS-contention and deadlock-timeout retries). The
    /// transaction object stays pending; only the shard/root lock entries clear.
    async fn release_for_retry(&self, tx: &Handle) -> Result<(), TransError> {
        self.locker
            .release_locks(&tx.id)
            .await
            .map_err(|e| e.context(format!("releasing locks before re-lock for tx {}", tx.id)))
    }

    /// Signals a stale-read re-validation restart (ADR-024): a read's value moved
    /// before it was locked, so the body must re-run to observe the new value —
    /// but, unlike [`Algo::restart`], **holding the locks already acquired** and
    /// **without renewing the id**. Invalidates the stale cached reads so the
    /// re-run re-reads the authoritative values, then returns
    /// [`TransError::Retry`], which the db retry loop re-runs in place (the
    /// transaction object stays pending and its locks stay installed). Any lock
    /// left on a key the re-run no longer touches is reclaimed lazily by the next
    /// contender (ADR-021).
    ///
    /// Unlike [`Algo::restart`] this does **not** back off: the transaction holds
    /// *live* locks here (its object is still pending), so sleeping would block
    /// every peer waiting on those keys and only delay our own release.
    async fn revalidate(&self, tx: &mut Handle) -> Result<(), TransError> {
        self.invalidate_reads(&tx.data);
        Err(TransError::Retry)
    }

    /// Reports whether the transaction was already aborted by a higher-priority
    /// transaction. Best-effort: a status read error is not treated as a wound.
    async fn was_wounded(&self, tx: &Handle) -> bool {
        matches!(
            self.mon.tx_status(&tx.id).await,
            Ok(TxCommitStatus::Aborted)
        )
    }

    /// Reports whether the transaction's snapshot still holds: every read's
    /// effective writer is unchanged (ADR-024) **and** every range scan's
    /// covered leaves are unchanged (ADR-031 phantom prevention).
    async fn validate(&self, data: &Data) -> Result<bool, TransError> {
        Ok(self.validate_reads_inner(data).await? && self.validate_scans_inner(data).await?)
    }

    /// Re-resolves every read's effective writer and reports whether they all
    /// still match what the transaction observed (a consistent snapshot exists).
    /// The read set is resolved in one shard-batched pass (each touched shard is
    /// loaded once) rather than one shard load per key.
    async fn validate_reads_inner(&self, data: &Data) -> Result<bool, TransError> {
        if data.reads.is_empty() {
            return Ok(true);
        }
        let keys: Vec<Arc<str>> = data.reads.iter().map(|r| r.path.clone()).collect();
        let current = self.resolver.effective_writers(&keys).await?;
        for r in &data.reads {
            let observed = r.version.as_ref().map(|v| v.last_writer.clone());
            if current.get(&r.path).cloned().flatten() != observed {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Re-scans every range the transaction listed and reports whether each
    /// still covers exactly the same leaves at the same object versions
    /// (ADR-031). A create/delete in the range bumps its leaf's version and a
    /// split changes the covered leaf set, so either makes the re-scan differ —
    /// which fails validation and re-runs the listing over the fresh topology,
    /// preventing phantom appearances/disappearances. Following the right-link
    /// chain (via [`Resolver::live_keys_scan`]) keeps the re-scan consistent
    /// with an in-progress split rather than erroring on it.
    async fn validate_scans_inner(&self, data: &Data) -> Result<bool, TransError> {
        for scan in &data.scans {
            let current = self.resolver.live_keys_scan(&scan.prefix).await?.covered;
            if current != scan.covered {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Invalidates the local cache entries for the transaction's found reads, so
    /// a retry re-reads the authoritative value instead of re-validating the
    /// stale cached one (which would re-conflict forever).
    fn invalidate_reads(&self, data: &Data) {
        for r in &data.reads {
            if let Some(v) = &r.version {
                self.values
                    .mark_value_outdated(&r.path, v.to_storage_version());
            }
        }
    }

    /// Builds and writes the committed transaction object (the commit point).
    /// Records `locks` (the held lock set) alongside `writes` so the object
    /// carries its full back-reference set for GC's reverse check (ADR-022).
    async fn commit_writes(
        &self,
        writes: &[WriteAccess],
        locks: Vec<PathLock>,
        id: &TxId,
    ) -> Result<(), TransError> {
        let mut tl = TxLog::new(id.clone(), TxCommitStatus::Ok);
        tl.locks = locks;
        for w in writes {
            let (value, deleted): (Arc<[u8]>, bool) = match &w.op {
                WriteOp::Put(value) => (value.clone(), false),
                WriteOp::Delete => (Arc::from(&[] as &[u8]), true),
            };
            tl.writes.push(TxWrite {
                path: w.path.to_string(),
                value,
                deleted,
                prev_writer: TxId::default(),
            });
        }
        // `context` preserves the `AlreadyFinalized` sentinel and any in-doubt
        // outcome instead of collapsing them into a generic error.
        self.mon
            .commit_tx(tl)
            .await
            .map_err(|e| e.context("creating transaction object"))
    }
}

/// Feeds the transaction ids a write-back superseded to GC as reverse-check
/// candidates (ADR-022): each is a former `current_writer` a fresh commit's
/// pointer overwrote, so it just lost a reference and may now be collectable.
fn feed_gc_hints(gc: &Gc, superseded: Vec<TxId>) {
    for prev in superseded {
        gc.schedule_tx_cleanup(prev);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::Reader;
    use glassdb_backend::middleware::{OpLog, RecordingBackend};
    use glassdb_backend::{Backend, memory::MemoryBackend};
    use glassdb_concurr::{Background, RetryConfig};
    use glassdb_data::paths;
    use glassdb_storage::{
        CollectionRoot, Directory, MAX_STALENESS, ObjectCache, Shard, ShardEntry, ShardStore,
        SharedCache, StorageError, TLogger, TxCommitStatus, ValueCache,
    };

    const TEST_COLL: &str = "testp";

    struct Tctx {
        backend: Arc<dyn Backend>,
        values: ValueCache,
        tlogger: TLogger,
        tmon: Monitor,
        shards: ShardStore,
        locker: Locker,
    }

    async fn new_algo() -> (Algo, Tctx) {
        new_algo_from_backend(Arc::new(MemoryBackend::new())).await
    }

    async fn new_algo_from_backend(b: Arc<dyn Backend>) -> (Algo, Tctx) {
        new_algo_from_backend_with_cache(b, 1024).await
    }

    async fn new_algo_from_backend_with_cache(
        b: Arc<dyn Backend>,
        cache_bytes: usize,
    ) -> (Algo, Tctx) {
        let cache = SharedCache::new(cache_bytes);
        let values = ValueCache::new(&cache);
        let objects = ObjectCache::new(b.clone(), &cache);
        let tlogger = TLogger::new(objects.clone(), TEST_COLL);
        let bg = Arc::new(Background::new());
        let bg_weak = Arc::downgrade(&bg);
        // Leak the background so spawned async aborts can run for the test's
        // lifetime without us threading the owner through every helper.
        std::mem::forget(bg);
        let tmon = Monitor::new(values.clone(), tlogger.clone(), bg_weak.clone());
        let shards = ShardStore::new(objects.clone());
        let resolver = Resolver::new(shards.clone(), tmon.clone());
        let dir = Directory::new(shards.clone());
        let coord = crate::shard_coord::ShardCoordinator::new(
            shards.clone(),
            resolver.clone(),
            tmon.clone(),
            RetryConfig::default(),
        );
        let locker = Locker::new(coord.clone(), dir, tmon.clone(), RetryConfig::default());
        let gc = Gc::new(
            bg_weak.clone(),
            tlogger.clone(),
            shards.clone(),
            locker.clone(),
            tmon.clone(),
            Clock::real(),
            TEST_COLL,
        );

        // Create the collection root so the test collection exists up front.
        shards
            .create_root(TEST_COLL, &CollectionRoot::new())
            .await
            .unwrap();

        let algo = Algo::new(
            values.clone(),
            locker.clone(),
            coord.clone(),
            tmon.clone(),
            Clock::real(),
            gc,
            None,
            resolver,
        );
        (
            algo,
            Tctx {
                backend: b,
                values,
                tlogger,
                tmon,
                shards,
                locker,
            },
        )
    }

    fn wa(path: &str, val: &[u8]) -> WriteAccess {
        WriteAccess::put(path.into(), Arc::from(val))
    }

    fn wdel(path: &str) -> WriteAccess {
        WriteAccess::delete(path.into())
    }

    fn ra_found(path: &str, last_writer: TxId) -> ReadAccess {
        ReadAccess {
            path: path.into(),
            version: Some(ReadVersion { last_writer }),
        }
    }

    fn ra_not_found(path: &str) -> ReadAccess {
        ReadAccess {
            path: path.into(),
            version: None,
        }
    }

    async fn do_read(tctx: &Tctx, path: &str) -> ReadAccess {
        let reader = Reader::new(
            tctx.values.clone(),
            tctx.shards.clone(),
            tctx.tmon.clone(),
            RetryConfig::default(),
        );
        match reader.read(path, MAX_STALENESS).await {
            Ok(rv) => ra_found(path, rv.version.writer),
            Err(StorageError::NotFound) => ra_not_found(path),
            Err(e) => panic!("reading {path}: {e:?}"),
        }
    }

    async fn commit_access(tm: &Algo, d: Data) -> Handle {
        let mut h = tm.begin(d);
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();
        h
    }

    async fn commit_writes(tm: &Algo, ws: Vec<WriteAccess>) -> Handle {
        commit_access(
            tm,
            Data {
                reads: Vec::new(),
                writes: ws,
                scans: Vec::new(),
            },
        )
        .await
    }

    async fn entry(tctx: &Tctx, key: &[u8]) -> Option<ShardEntry> {
        let loaded = tctx
            .shards
            .load_leaf(&paths::collection_info(TEST_COLL), Freshness::Latest)
            .await
            .unwrap();
        loaded.entries.lookup(key).cloned()
    }

    #[tokio::test]
    async fn write_new() {
        let (tm, tctx) = new_algo().await;
        let keyp = paths::from_key(TEST_COLL, b"k");
        let val = b"v";

        let mut h = tm.begin(Data {
            reads: Vec::new(),
            writes: vec![wa(&keyp, val)],
            scans: Vec::new(),
        });
        tm.commit(&mut h).await.unwrap();
        let tid = h.id().clone();
        tm.end(&mut h).await.unwrap();

        let status = tctx.tlogger.commit_status(&tid).await.unwrap();
        assert_eq!(status.status, TxCommitStatus::Ok);
        let (txlog, _) = tctx.tlogger.get(&tid).await.unwrap();
        assert_eq!(txlog.writes.len(), 1);
        assert_eq!(txlog.writes[0].path, keyp);
        assert_eq!(&*txlog.writes[0].value, val);

        // The shard entry points at the committed writer and the lock is gone.
        let e = entry(&tctx, b"k").await.unwrap();
        assert_eq!(e.current_writer, Some(tid));
        assert!(e.locked_by.is_empty());
    }

    // Regression (review 1.1 / ADR-022): the committed transaction object must
    // record its full lock set, not just its writes, so GC's reverse liveness
    // check and lock pruning operate on real logs. A transaction that reads one
    // key and creates another records a read lock on the read key and a write
    // lock on the created key. Membership is coordinated by that per-key entry
    // lock, so there is no separate root lock to record (ADR-031).
    #[tokio::test]
    async fn commit_records_locks() {
        let (tm, tctx) = new_algo().await;
        let readp = paths::from_key(TEST_COLL, b"r");
        let writep = paths::from_key(TEST_COLL, b"w");

        // Seed the read key so it resolves to a committed value.
        commit_writes(&tm, vec![wa(&readp, b"seed")]).await;

        let r = do_read(&tctx, &readp).await;
        let mut h = tm.begin(Data {
            reads: vec![r],
            writes: vec![wa(&writep, b"v")],
            scans: Vec::new(),
        });
        tm.commit(&mut h).await.unwrap();
        let tid = h.id().clone();
        tm.end(&mut h).await.unwrap();

        let (txlog, _) = tctx.tlogger.get(&tid).await.unwrap();
        let locked: std::collections::BTreeSet<&str> =
            txlog.locks.iter().map(|l| l.path.as_str()).collect();
        assert!(
            locked.contains(readp.as_str()),
            "read lock recorded: {locked:?}"
        );
        assert!(
            locked.contains(writep.as_str()),
            "write lock recorded: {locked:?}"
        );
    }

    #[tokio::test]
    async fn read_then_write_round_trips() {
        let (tm, tctx) = new_algo().await;
        let keyp = paths::from_key(TEST_COLL, b"k");

        let h = commit_writes(&tm, vec![wa(&keyp, b"init")]).await;
        let _ = h;

        let r = do_read(&tctx, &keyp).await;
        let mut h = tm.begin(Data {
            reads: vec![r],
            writes: vec![wa(&keyp, b"v2")],
            scans: Vec::new(),
        });
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        let r = do_read(&tctx, &keyp).await;
        assert_eq!(r.version.as_ref().unwrap().last_writer, *h.id());
    }

    // Full path (ADR-024): a read whose value moved before it was locked does not
    // abort-and-renew; it re-runs the body in place (`Retry`) while holding its
    // locks. The engine validates *after* locking, so unlike a pre-lock check the
    // moved key is itself locked during the re-run window — the v1 guarantee that
    // the retry holds all its locks. Two writes force the full locked path (the
    // single-rw fast path handles a lone write; see the test below).
    #[tokio::test]
    async fn stale_read_write_retries_holding_locks() {
        let (tm, tctx) = new_algo().await;
        let (tm2, _t2) = new_algo_from_backend(tctx.backend.clone()).await;
        let ka = paths::from_key(TEST_COLL, b"k");
        let kb = paths::from_key(TEST_COLL, b"k2");

        // Seed both keys so the writes are overwrites (not creates), keeping the
        // transaction on the read-write path rather than a membership change.
        commit_writes(&tm2, vec![wa(&ka, b"v1")]).await;
        commit_writes(&tm2, vec![wa(&kb, b"x1")]).await;
        let ra = do_read(&tctx, &ka).await;

        // Another client overwrites `k`, making `ra` stale.
        commit_writes(&tm2, vec![wa(&ka, b"v2")]).await;

        let mut h = tm.begin(Data {
            reads: vec![ra],
            writes: vec![wa(&ka, b"v3"), wa(&kb, b"x2")],
            scans: Vec::new(),
        });
        let err = tm.commit(&mut h).await.unwrap_err();
        assert!(matches!(err, TransError::Retry), "got {err:?}");

        // The moved key is locked by us when the stale read is signalled: the
        // re-run owns the lock and cannot lose it again to the same race.
        let e = entry(&tctx, b"k").await.expect("entry exists");
        assert_eq!(e.locked_by, vec![h.id().clone()]);

        tm.end(&mut h).await.unwrap();
    }

    // Single-rw fast path (ADR-030): a lone read-modify-write whose read was
    // superseded is caught with a transparent retry, never a surfaced error, and
    // never commits its stale value. The exact retry flavour depends only on
    // whether the commit's `AllowStale` eligibility snapshot was still cached:
    // `Wounded` when a stale snapshot passed the check and the seeded CAS then
    // missed (renew via the regular path, no lock held), or `Retry` when the
    // snapshot was evicted, so the eligibility read fell through to fresh bytes
    // and the full path validated after locking. Both converge on a fresh read.
    #[tokio::test]
    async fn single_rw_stale_read_renews_and_converges() {
        let (tm, tctx) = new_algo().await;
        let (tm2, _t2) = new_algo_from_backend(tctx.backend.clone()).await;
        let keyp = paths::from_key(TEST_COLL, b"k");

        commit_writes(&tm2, vec![wa(&keyp, b"v1")]).await;
        let ra = do_read(&tctx, &keyp).await;

        // Another client overwrites the key, making `ra` stale.
        let h2 = commit_writes(&tm2, vec![wa(&keyp, b"v2")]).await;

        let mut h = tm.begin(Data {
            reads: vec![ra],
            writes: vec![wa(&keyp, b"v3")],
            scans: Vec::new(),
        });
        let err = tm.commit(&mut h).await.unwrap_err();
        assert!(
            matches!(err, TransError::Wounded | TransError::Retry),
            "a stale read is a transparent retry, got {err:?}"
        );
        tm.end(&mut h).await.unwrap();

        // The stale write never committed: v2 is still current (the abandoned
        // fast-path object is unreferenced, so help-forward cannot promote it).
        assert_eq!(
            do_read(&tctx, &keyp).await.version.unwrap().last_writer,
            *h2.id(),
            "the stale write did not commit; v2 is still current"
        );

        // A fresh read + commit converges (the re-run observes v2 and commits).
        let ra2 = do_read(&tctx, &keyp).await;
        let h3 = commit_access(
            &tm,
            Data {
                reads: vec![ra2],
                writes: vec![wa(&keyp, b"v3")],
                scans: Vec::new(),
            },
        )
        .await;
        assert_eq!(
            do_read(&tctx, &keyp).await.version.unwrap().last_writer,
            *h3.id(),
            "the renewed attempt commits"
        );
    }

    // ADR-024: a suspected deadlock is broken *inside* `Algo`, never surfaced. A
    // transaction that cannot wound the holder of a lock it needs waits; the
    // wait is bounded by `MAX_DEADLOCK_TIMEOUT`, after which the transaction
    // releases its locks and re-acquires them in the cannot-deadlock serial
    // order — under the *same id*, re-running no body. It never returns
    // `LockTimeout`, and once the holder finalizes it commits.
    #[tokio::test(start_paused = true)]
    async fn deadlock_timeout_relocks_serially_keeping_id() {
        use crate::tlocker::LockOutcome;
        use std::time::Duration;
        let (tm, tctx) = new_algo().await;
        let keyp = paths::from_key(TEST_COLL, b"k");

        // An older holder takes the key's write lock and does not finalize.
        let holder = TxId::with_priority(0, b"holder");
        tctx.tmon.begin_tx(&holder);
        let held = tctx
            .locker
            .lock(
                &holder,
                &Data {
                    reads: Vec::new(),
                    writes: vec![wa(&keyp, b"h")],
                    scans: Vec::new(),
                },
                false,
            )
            .await
            .unwrap();
        assert!(
            matches!(held, LockOutcome::Locked(_)),
            "older holder should acquire its lock"
        );

        // A younger transaction wants the same key; it cannot wound the holder.
        // Drive its commit concurrently so we can observe it parked waiting.
        let mut h = tm.begin(Data {
            reads: Vec::new(),
            writes: vec![wa(&keyp, b"a")],
            scans: Vec::new(),
        });
        let id_before = h.id().clone();
        let tm2 = tm.clone();
        let committing = tokio::spawn(async move {
            let res = tm2.commit(&mut h).await;
            (h, res)
        });

        // Let the parallel wait time out and escalate to serial. Serial cannot
        // wound the older peer either, so the transaction keeps waiting — it has
        // not aborted and has surfaced no error.
        rt::sleep(MAX_DEADLOCK_TIMEOUT + Duration::from_secs(1)).await;
        assert!(
            !committing.is_finished(),
            "younger keeps waiting on the older holder after escalating to serial"
        );

        // Finalizing the holder releases the younger, which commits under its
        // original id without ever surfacing `LockTimeout`.
        tctx.tmon.abort_tx(&holder).await.unwrap();
        let (mut h, res) = committing.await.unwrap();
        res.expect("younger commits once the holder releases");
        assert_eq!(
            *h.id(),
            id_before,
            "the id is preserved across the serial fallback (no renew)"
        );
        tm.end(&mut h).await.unwrap();
    }

    /// A [`Backend`] that, once armed, makes the first `budget` leaf-lock CAS
    /// writes (`write_if` on a `/_n/` node or `/_i` root leaf) miss their precondition,
    /// then passes through. Sustained misses force the lock acquisition past the
    /// parallel deadlock timeout into the serial order and then exhaust the
    /// serial CAS budget, which is the only way a `Conflict` reaches
    /// `acquire_locks`.
    struct FlakyShardCas {
        inner: Arc<dyn Backend>,
        armed: std::sync::atomic::AtomicBool,
        remaining: std::sync::atomic::AtomicUsize,
    }

    impl FlakyShardCas {
        fn new(inner: Arc<dyn Backend>, budget: usize) -> Arc<Self> {
            Arc::new(FlakyShardCas {
                inner,
                armed: std::sync::atomic::AtomicBool::new(false),
                remaining: std::sync::atomic::AtomicUsize::new(budget),
            })
        }

        fn arm(&self) {
            self.armed.store(true, std::sync::atomic::Ordering::SeqCst);
        }

        fn remaining(&self) -> usize {
            self.remaining.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl Backend for FlakyShardCas {
        async fn read(
            &self,
            path: &str,
        ) -> Result<glassdb_backend::ReadReply, glassdb_backend::BackendError> {
            self.inner.read(path).await
        }

        async fn read_if_modified(
            &self,
            path: &str,
            expected: &glassdb_backend::Version,
        ) -> Result<glassdb_backend::ReadReply, glassdb_backend::BackendError> {
            self.inner.read_if_modified(path, expected).await
        }

        async fn write(
            &self,
            path: &str,
            value: Vec<u8>,
        ) -> Result<glassdb_backend::Version, glassdb_backend::BackendError> {
            self.inner.write(path, value).await
        }

        async fn write_if(
            &self,
            path: &str,
            value: Vec<u8>,
            expected: &glassdb_backend::Version,
        ) -> Result<glassdb_backend::Version, glassdb_backend::BackendError> {
            use std::sync::atomic::Ordering;
            if self.armed.load(Ordering::SeqCst)
                && (path.contains("/_n/") || path.ends_with("/_i"))
                && self
                    .remaining
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                    .is_ok()
            {
                return Err(glassdb_backend::BackendError::Precondition);
            }
            self.inner.write_if(path, value, expected).await
        }

        async fn write_if_not_exists(
            &self,
            path: &str,
            value: Vec<u8>,
        ) -> Result<glassdb_backend::Version, glassdb_backend::BackendError> {
            self.inner.write_if_not_exists(path, value).await
        }

        async fn delete(&self, path: &str) -> Result<(), glassdb_backend::BackendError> {
            self.inner.delete(path).await
        }

        async fn list(&self, dir_path: &str) -> Result<Vec<String>, glassdb_backend::BackendError> {
            self.inner.list(dir_path).await
        }
    }

    // Backend that parks the dedup driver's coordination-object load on a gate
    // while **armed**, so a test can hold the driver mid-load until a second
    // contender has queued — forcing them into one merged CAS round. The first
    // `skip` reads after arming pass through: descent reads the collection root
    // `_i` to route a key before the coordinator loads the leaf (ADR-031), so the
    // gate must skip that routing read and park the coordinator's load instead.
    // Arming is deferred so un-gated setup runs first.
    struct GateBackend {
        inner: Arc<dyn Backend>,
        gate: Arc<tokio::sync::Notify>,
        armed: std::sync::atomic::AtomicBool,
        skip: std::sync::atomic::AtomicUsize,
    }

    impl GateBackend {
        fn new(inner: Arc<dyn Backend>) -> Arc<Self> {
            Arc::new(GateBackend {
                inner,
                gate: Arc::new(tokio::sync::Notify::new()),
                armed: std::sync::atomic::AtomicBool::new(false),
                skip: std::sync::atomic::AtomicUsize::new(0),
            })
        }
        fn arm(&self) {
            // Skip the driver's descent (routing) read of `_i`, then gate its
            // coordinator leaf load.
            self.skip.store(1, std::sync::atomic::Ordering::SeqCst);
            self.armed.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        fn release(&self) {
            self.gate.notify_one();
        }
        async fn gate_if_armed(&self) {
            use std::sync::atomic::Ordering::SeqCst;
            if !self.armed.load(SeqCst) {
                return;
            }
            if self
                .skip
                .fetch_update(SeqCst, SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return;
            }
            self.armed.store(false, SeqCst);
            self.gate.notified().await;
        }
    }

    #[async_trait::async_trait]
    impl Backend for GateBackend {
        async fn read(
            &self,
            path: &str,
        ) -> Result<glassdb_backend::ReadReply, glassdb_backend::BackendError> {
            self.gate_if_armed().await;
            self.inner.read(path).await
        }
        async fn read_if_modified(
            &self,
            path: &str,
            expected: &glassdb_backend::Version,
        ) -> Result<glassdb_backend::ReadReply, glassdb_backend::BackendError> {
            self.gate_if_armed().await;
            self.inner.read_if_modified(path, expected).await
        }
        async fn write(
            &self,
            path: &str,
            value: Vec<u8>,
        ) -> Result<glassdb_backend::Version, glassdb_backend::BackendError> {
            self.inner.write(path, value).await
        }
        async fn write_if(
            &self,
            path: &str,
            value: Vec<u8>,
            expected: &glassdb_backend::Version,
        ) -> Result<glassdb_backend::Version, glassdb_backend::BackendError> {
            self.inner.write_if(path, value, expected).await
        }
        async fn write_if_not_exists(
            &self,
            path: &str,
            value: Vec<u8>,
        ) -> Result<glassdb_backend::Version, glassdb_backend::BackendError> {
            self.inner.write_if_not_exists(path, value).await
        }
        async fn delete(&self, path: &str) -> Result<(), glassdb_backend::BackendError> {
            self.inner.delete(path).await
        }
        async fn list(&self, dir_path: &str) -> Result<Vec<String>, glassdb_backend::BackendError> {
            self.inner.list(dir_path).await
        }
    }

    // Backend that, while **armed**, makes the first shard CAS *in-doubt*: it
    // applies the write to storage but returns `Unavailable`, so the caller
    // cannot tell whether it landed (ADR-009). Arming is deferred so un-gated
    // setup runs first.
    struct InDoubtShardCas {
        inner: Arc<dyn Backend>,
        armed: std::sync::atomic::AtomicBool,
    }

    impl InDoubtShardCas {
        fn new(inner: Arc<dyn Backend>) -> Arc<Self> {
            Arc::new(InDoubtShardCas {
                inner,
                armed: std::sync::atomic::AtomicBool::new(false),
            })
        }
        fn arm(&self) {
            self.armed.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl Backend for InDoubtShardCas {
        async fn read(
            &self,
            path: &str,
        ) -> Result<glassdb_backend::ReadReply, glassdb_backend::BackendError> {
            self.inner.read(path).await
        }
        async fn read_if_modified(
            &self,
            path: &str,
            expected: &glassdb_backend::Version,
        ) -> Result<glassdb_backend::ReadReply, glassdb_backend::BackendError> {
            self.inner.read_if_modified(path, expected).await
        }
        async fn write(
            &self,
            path: &str,
            value: Vec<u8>,
        ) -> Result<glassdb_backend::Version, glassdb_backend::BackendError> {
            self.inner.write(path, value).await
        }
        async fn write_if(
            &self,
            path: &str,
            value: Vec<u8>,
            expected: &glassdb_backend::Version,
        ) -> Result<glassdb_backend::Version, glassdb_backend::BackendError> {
            use std::sync::atomic::Ordering;
            if (path.contains("/_n/") || path.ends_with("/_i"))
                && self
                    .armed
                    .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                // Apply the write, then report the ack as lost: the CAS landed but
                // the caller must re-fold to discover that.
                let _ = self.inner.write_if(path, value, expected).await;
                return Err(glassdb_backend::BackendError::Unavailable(
                    "simulated in-doubt shard CAS".into(),
                ));
            }
            self.inner.write_if(path, value, expected).await
        }
        async fn write_if_not_exists(
            &self,
            path: &str,
            value: Vec<u8>,
        ) -> Result<glassdb_backend::Version, glassdb_backend::BackendError> {
            self.inner.write_if_not_exists(path, value).await
        }
        async fn delete(&self, path: &str) -> Result<(), glassdb_backend::BackendError> {
            self.inner.delete(path).await
        }
        async fn list(&self, dir_path: &str) -> Result<Vec<String>, glassdb_backend::BackendError> {
            self.inner.list(dir_path).await
        }
    }

    /// A distinct key that shares the same leaf as `base`, for exercising
    /// disjoint-key contention within one leaf object. With split deferred, every
    /// key lives in the collection's single leaf `_i` (ADR-031), so any distinct
    /// key qualifies.
    fn same_shard_sibling(base: &[u8]) -> Vec<u8> {
        let sib = b"sibling".to_vec();
        assert_ne!(sib, base, "sibling must differ from the base key");
        sib
    }

    fn shard_stores(log: &OpLog, path: &str) -> usize {
        log.lock()
            .unwrap()
            .iter()
            .filter(|r| r.path == path && (r.op == "write_if" || r.op == "write_if_not_exists"))
            .count()
    }

    // ADR-028: the single read-write commit-install is folded by the same shard
    // coordinator as ordinary lock acquisition, so an install and a disjoint-key
    // acquire contending one shard batch into a single CAS round instead of
    // racing two separate loads+CASes. The install lands its write lock and the
    // acquire installs its lock in the one store.
    #[tokio::test(start_paused = true)]
    async fn single_rw_install_merges_with_disjoint_acquire() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let gate = GateBackend::new(mem);
        let rec = Arc::new(RecordingBackend::new(gate.clone() as Arc<dyn Backend>));
        let log = rec.log();
        let (tm, tctx) = new_algo_from_backend(rec).await;

        let ka = b"k".to_vec();
        let kb = same_shard_sibling(&ka);
        let kap = paths::from_key(TEST_COLL, &ka);
        let kbp = paths::from_key(TEST_COLL, &kb);

        // Seed keys A and B committed: the fast-path install builds on A's
        // predecessor, and the disjoint acquire overwrites an existing B, so it
        // takes no membership root lock and the round stays a single shard CAS.
        commit_writes(&tm, vec![wa(&kap, b"v1")]).await;
        commit_writes(&tm, vec![wa(&kbp, b"vb1")]).await;

        let txa = TxId::with_priority(1_000_000_000, b"install");
        let txb = TxId::with_priority(2_000_000_000, b"acquire");
        tctx.tmon.begin_tx(&txa);
        tctx.tmon.begin_tx(&txb);

        let shard_path = paths::collection_info(TEST_COLL);
        log.lock().unwrap().clear();
        gate.arm();

        // The disjoint acquire is submitted first and becomes the dedup driver,
        // parking in the gated (`Latest`) load; the single-rw install then joins
        // that open batch. (Post-ADR-030 the install's own first attempt is
        // `AllowStale` and would skip the load on a warm cache, so it merges via
        // the driver's already-loading round rather than racing a solo, cache-
        // served CAS — which is exactly the ADR-028 single-round behavior.)
        let (ca, cb) = (tm.clone(), tctx.locker.clone());
        let data_b = Data {
            reads: Vec::new(),
            writes: vec![wa(&kbp, b"vb2")],
            scans: Vec::new(),
        };
        let tb = txb.clone();
        let acquire = tokio::spawn(async move { cb.lock(&tb, &data_b, false).await });

        // Let the driver park in the gated load before the install joins.
        rt::sleep(Duration::from_secs(1)).await;

        let (ta, pa, ka2, kap2) = (
            txa.clone(),
            paths::collection_info(TEST_COLL),
            ka.clone(),
            kap.clone(),
        );
        let install =
            tokio::spawn(async move { ca.commit_install(&ta, &pa, ka2, kap2, None).await });

        // Once the install has queued into the open batch, release the load.
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        let install = install.await.unwrap().unwrap();
        let acquire = acquire.await.unwrap().unwrap();
        assert!(
            matches!(install, InstallOutcome::Landed),
            "the fast-path install must land"
        );
        assert!(
            matches!(acquire, LockOutcome::Locked(_)),
            "the disjoint acquire must lock"
        );

        assert_eq!(
            shard_stores(&log, &shard_path),
            1,
            "install and disjoint acquire share one CAS"
        );

        // Both mutations landed in the shared shard write.
        let ea = entry(&tctx, &ka).await.unwrap();
        assert_eq!(ea.locked_by, vec![txa], "install holds A's write lock");
        let eb = entry(&tctx, &kb).await.unwrap();
        assert!(eb.locked_by.contains(&txb), "acquire holds B's lock");
    }

    // ADR-028 regression (batched in-doubt): a commit-install co-batched with a
    // disjoint-key acquire whose shared CAS comes back in-doubt (`Unavailable`)
    // recovers idempotently — the engine reloads and re-folds, the install finds
    // itself already in the chain (`Landed`), and the acquire re-installs its own
    // lock (`Locked`) without double-applying. No error is surfaced.
    #[tokio::test(start_paused = true)]
    async fn commit_install_batched_in_doubt_recovers() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let indoubt = InDoubtShardCas::new(mem);
        let gate = GateBackend::new(indoubt.clone() as Arc<dyn Backend>);
        let (tm, tctx) = new_algo_from_backend(gate.clone() as Arc<dyn Backend>).await;

        let ka = b"k".to_vec();
        let kb = same_shard_sibling(&ka);
        let kap = paths::from_key(TEST_COLL, &ka);
        let kbp = paths::from_key(TEST_COLL, &kb);

        // Seed keys A and B committed (un-gated, before arming): the install has
        // a predecessor and the acquire overwrites an existing B, so it takes no
        // membership root lock and the round stays a single shard CAS.
        commit_writes(&tm, vec![wa(&kap, b"v1")]).await;
        commit_writes(&tm, vec![wa(&kbp, b"vb1")]).await;

        let txa = TxId::with_priority(1_000_000_000, b"install");
        let txb = TxId::with_priority(2_000_000_000, b"acquire");
        tctx.tmon.begin_tx(&txa);
        tctx.tmon.begin_tx(&txb);

        // Arm the merge gate and the in-doubt first CAS together.
        indoubt.arm();
        gate.arm();

        let (ca, cb) = (tm.clone(), tctx.locker.clone());
        let (ta, pa, ka2, kap2) = (
            txa.clone(),
            paths::collection_info(TEST_COLL),
            ka.clone(),
            kap.clone(),
        );
        let install =
            tokio::spawn(async move { ca.commit_install(&ta, &pa, ka2, kap2, None).await });
        let data_b = Data {
            reads: Vec::new(),
            writes: vec![wa(&kbp, b"vb2")],
            scans: Vec::new(),
        };
        let tb = txb.clone();
        let acquire = tokio::spawn(async move { cb.lock(&tb, &data_b, false).await });

        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        // The in-doubt CAS actually landed, so the re-fold sees both members in
        // the chain: the install classifies itself Landed, the acquire re-locks.
        let install = install.await.unwrap().unwrap();
        let acquire = acquire.await.unwrap().unwrap();
        assert!(
            matches!(install, InstallOutcome::Landed),
            "the install recovers as landed, not in-doubt"
        );
        assert!(
            matches!(acquire, LockOutcome::Locked(_)),
            "the co-batched acquire re-locks idempotently"
        );

        assert_eq!(entry(&tctx, &ka).await.unwrap().locked_by, vec![txa]);
        assert!(entry(&tctx, &kb).await.unwrap().locked_by.contains(&txb));
    }

    // ADR-020/024: CAS contention is resolved *inside* `Algo`. A transaction that
    // loses the shard-lock CAS repeatedly releases its (partial) locks and
    // re-acquires them under the *same id* — no renew, no body re-run — escalating
    // to the serial order. It never surfaces `Wounded` for a mere lost race, and
    // commits unchanged once the contention clears. A budget far larger than the
    // ~handful of parallel attempts that fit before the deadlock timeout forces
    // the serial CAS budget to be exhausted, i.e. the `Conflict` path.
    //
    // Uses a two-key write so the transaction is ineligible for the single
    // read-write fast path (ADR-020) and genuinely exercises the full locked
    // path's same-id serial-fallback behaviour.
    #[tokio::test(start_paused = true)]
    async fn cas_contention_relocks_keeping_id() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let flaky = FlakyShardCas::new(mem, 70);
        let backend: Arc<dyn Backend> = flaky.clone();
        let (tm, tctx) = new_algo_from_backend(backend).await;
        let keyp = paths::from_key(TEST_COLL, b"k");
        let keyp2 = paths::from_key(TEST_COLL, b"k2");

        // Seed the keys over a clean connection so their shards exist (the lock
        // CAS is then a `write_if`, the thing we fault).
        commit_writes(&tm, vec![wa(&keyp, b"v1"), wa(&keyp2, b"v1")]).await;

        flaky.arm();
        let mut h = tm.begin(Data {
            reads: Vec::new(),
            writes: vec![wa(&keyp, b"v2"), wa(&keyp2, b"v2")],
            scans: Vec::new(),
        });
        let id_before = h.id().clone();
        tm.commit(&mut h)
            .await
            .expect("commits despite sustained CAS contention");
        assert_eq!(
            *h.id(),
            id_before,
            "CAS contention retries under the same id (no renew)"
        );
        tm.end(&mut h).await.unwrap();

        // The whole budget was consumed, so the transaction did exhaust the
        // serial CAS budget (the `Conflict` path), not merely time out in
        // parallel mode.
        assert_eq!(flaky.remaining(), 0, "expected sustained CAS contention");
        // It still committed: the shards point at our writer with no live lock.
        let e = entry(&tctx, b"k").await.unwrap();
        assert_eq!(e.current_writer, Some(id_before.clone()));
        assert!(e.locked_by.is_empty());
        let e2 = entry(&tctx, b"k2").await.unwrap();
        assert_eq!(e2.current_writer, Some(id_before));
        assert!(e2.locked_by.is_empty());
    }

    // Builds an algo whose backend records every operation, so tests can prove
    // which commit path ran by counting the CAS writes it issued.
    async fn new_recording_algo() -> (Algo, Tctx, OpLog) {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let rec = Arc::new(RecordingBackend::new(mem));
        let log = rec.log();
        let (tm, tctx) = new_algo_from_backend(rec).await;
        (tm, tctx, log)
    }

    // CAS-write counts by object kind, the fingerprint of a commit path: the
    // single read-write fast path (ADR-027) issues one tx-object write and two
    // shard writes (the lock CAS then the write-back CAS that publishes the
    // pointer — here inline because tests build the algo with no background
    // executor); the full locked path also issues two shard writes (entry lock +
    // write-back) but writes its tx object differently. Membership is coordinated
    // by the per-key entry lock, so neither path issues a separate root CAS
    // (ADR-031).
    #[derive(Debug, Default)]
    struct WriteCounts {
        // Writes to a leaf coordination object (ADR-031): a standalone node
        // `/_n/` or the collection root `/_i`, which holds the small collection's
        // single leaf entries. Entry-lock and write-back CAS both land here and
        // cannot be told apart by path alone.
        leaf: usize,
        tx: usize,
    }

    fn write_counts(log: &OpLog) -> WriteCounts {
        let mut c = WriteCounts::default();
        for o in log.lock().unwrap().iter() {
            if o.op != "write_if" && o.op != "write_if_not_exists" {
                continue;
            }
            if o.path.contains("/_n/") || o.path.ends_with("/_i") {
                c.leaf += 1;
            } else if o.path.contains("/_t/") {
                c.tx += 1;
            }
        }
        c
    }

    // Backend reads against leaf objects: `read` is a cold full read (cache
    // miss), `read_if_modified` a revalidation of a cached copy.
    fn shard_reads(log: &OpLog) -> (usize, usize) {
        let (mut full, mut revalidate) = (0, 0);
        for o in log.lock().unwrap().iter() {
            if !(o.path.contains("/_n/") || o.path.ends_with("/_i")) {
                continue;
            }
            if o.op == "read" {
                full += 1;
            } else if o.op == "read_if_modified" {
                revalidate += 1;
            }
        }
        (full, revalidate)
    }

    // A recording algo with a cache large enough that nothing is evicted, so a
    // warm-cache op count is deterministic across executors.
    async fn new_recording_algo_big_cache() -> (Algo, Tctx, OpLog) {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let rec = Arc::new(RecordingBackend::new(mem));
        let log = rec.log();
        let (tm, tctx) = new_algo_from_backend_with_cache(rec, 1 << 20).await;
        (tm, tctx, log)
    }

    // An eligible single-key overwrite commits through the fast path (ADR-027):
    // one committed `_t/` object write, one leaf lock CAS, one leaf write-back
    // CAS (inline here, no background executor), and no separate membership
    // write — and the new value is durable and readable. With split deferred the
    // leaf is the collection root `_i`, so both leaf CAS's land there (ADR-031).
    #[tokio::test]
    async fn single_rw_overwrite_takes_fast_path() {
        let (tm, tctx, log) = new_recording_algo().await;
        let keyp = paths::from_key(TEST_COLL, b"k");

        commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;
        let r = do_read(&tctx, &keyp).await;

        log.lock().unwrap().clear();
        let mut h = tm.begin(Data {
            reads: vec![r],
            writes: vec![wa(&keyp, b"v2")],
            scans: Vec::new(),
        });
        let tid = h.id().clone();
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        let c = write_counts(&log);
        assert_eq!(
            c.leaf, 2,
            "fast path: one lock CAS plus one write-back CAS, no membership: {c:?}"
        );
        assert_eq!(c.tx, 1, "one committed-object write: {c:?}");

        // The commit landed: the shard points at us with no live lock, a
        // committed `_t/` object exists, and the value reads back as ours.
        let e = entry(&tctx, b"k").await.unwrap();
        assert_eq!(e.current_writer, Some(tid.clone()));
        assert!(e.locked_by.is_empty());
        let status = tctx.tlogger.commit_status(&tid).await.unwrap();
        assert_eq!(status.status, TxCommitStatus::Ok);
        let r = do_read(&tctx, &keyp).await;
        assert_eq!(r.version.unwrap().last_writer, tid);
    }

    // ADR-030: a warm single read-write commit reuses the shard the read cached
    // for both its eligibility check and its lock-install fold (`AllowStale`), so
    // it issues no backend shard read for either — only the inline write-back
    // revalidates (`Latest`). A revalidating eligibility or install would each
    // add a `read_if_modified`, so pinning the total to one read guards the
    // reuse. A large cache keeps this deterministic (nothing is evicted between
    // the read and the commit).
    #[tokio::test]
    async fn single_rw_commit_reuses_cached_shard() {
        let (tm, tctx, log) = new_recording_algo_big_cache().await;
        let keyp = paths::from_key(TEST_COLL, b"k");

        commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;
        // The read warms the shard in the object cache.
        let r = do_read(&tctx, &keyp).await;

        log.lock().unwrap().clear();
        let mut h = tm.begin(Data {
            reads: vec![r],
            writes: vec![wa(&keyp, b"v2")],
            scans: Vec::new(),
        });
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        let (full, revalidate) = shard_reads(&log);
        assert_eq!(full, 0, "no cold shard read on a warm commit");
        assert_eq!(
            revalidate, 1,
            "only the write-back revalidates; eligibility and install reuse cache"
        );
    }

    // A blind single-key put over an existing key (no read) is also eligible.
    #[tokio::test]
    async fn single_rw_blind_put_takes_fast_path() {
        let (tm, tctx, log) = new_recording_algo().await;
        let keyp = paths::from_key(TEST_COLL, b"k");

        commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;

        log.lock().unwrap().clear();
        let mut h = tm.begin(Data {
            reads: Vec::new(),
            writes: vec![wa(&keyp, b"v2")],
            scans: Vec::new(),
        });
        let tid = h.id().clone();
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        let c = write_counts(&log);
        assert_eq!(
            c.leaf, 2,
            "fast path: one lock CAS plus one write-back CAS, no membership: {c:?}"
        );
        assert_eq!(c.tx, 1, "one committed-object write: {c:?}");
        assert_eq!(entry(&tctx, b"k").await.unwrap().current_writer, Some(tid));
    }

    // ADR-027 regression: the fast path leaves a write lock held by the
    // *committed* writer until its asynchronous write-back publishes the pointer
    // and releases it. A single-key writer arriving in that window must treat the
    // committed holder as effectively unlocked — help-forwarding it as the
    // predecessor — and stay on the lock-free fast path, rather than bailing to
    // the full locked path on the mere presence of the lock (the measured
    // regression). A stale read still bails.
    #[tokio::test]
    async fn single_rw_committed_holder_stays_on_fast_path() {
        let (tm, tctx) = new_algo().await;
        let keyp = paths::from_key(TEST_COLL, b"k");
        let leaf_path = paths::collection_info(TEST_COLL);
        let raw = b"k".to_vec();

        // H0 publishes v1; H1 overwrites with v2 through the fast path, leaving
        // the pointer at H1 with no lock.
        let h0 = commit_writes(&tm, vec![wa(&keyp, b"v1")])
            .await
            .id()
            .clone();
        let h1 = commit_writes(&tm, vec![wa(&keyp, b"v2")])
            .await
            .id()
            .clone();

        // Recreate the ADR-027 commit window before write-back: the lock is still
        // held by the committed H1 while the pointer lags at its predecessor H0.
        let loaded = tctx
            .shards
            .load_leaf(&leaf_path, Freshness::Latest)
            .await
            .unwrap();
        let windowed = Shard::from_entries(loaded.entries.entries().cloned().map(|mut e| {
            if e.key == raw {
                e.lock_type = LockType::Write;
                e.locked_by = vec![h1.clone()];
                e.current_writer = Some(h0.clone());
                e.deleted = false;
            }
            e
        }));
        assert!(
            tctx.shards
                .store_leaf(
                    &leaf_path,
                    &windowed,
                    loaded.kind(),
                    loaded.version.as_ref()
                )
                .await
                .unwrap()
        );

        // The window is observably at the committed holder H1 (v2), not the
        // lagging pointer H0: the shared resolver already help-forwards it.
        let r = do_read(&tctx, &keyp).await;
        assert_eq!(r.version.clone().unwrap().last_writer, h1);

        // Eligibility mirrors that resolution: given the one resolved holder view,
        // an RMW that read H1 and a blind put are both committable and build on
        // H1, while a read of the superseded H0 is still rejected as stale.
        let (res, _) = tm
            .resolver
            .resolve_key(&keyp, Freshness::Latest)
            .await
            .unwrap();
        assert_eq!(
            eligible_writer(&res, Some(&h1)),
            Some(h1.clone()),
            "an RMW that read the committed holder builds on it"
        );
        assert_eq!(
            eligible_writer(&res, None),
            Some(h1.clone()),
            "a blind put builds on the committed holder"
        );
        assert_eq!(
            eligible_writer(&res, Some(&h0)),
            None,
            "a read of the superseded value is still stale"
        );

        // End to end: the writer commits over H1 (help-forwarding it into the
        // chain, not orphaning it), and its value reads back.
        let mut h = tm.begin(Data {
            reads: vec![r],
            writes: vec![wa(&keyp, b"v3")],
            scans: Vec::new(),
        });
        let h2 = h.id().clone();
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        let e = entry(&tctx, b"k").await.unwrap();
        assert_eq!(e.current_writer, Some(h2.clone()));
        assert!(e.locked_by.is_empty());
        assert_eq!(do_read(&tctx, &keyp).await.version.unwrap().last_writer, h2);
    }

    // ADR-027/028: the fast path's two commit writes are independent. If the lock
    // install never lands (here: sustained shard-CAS contention exhausting the
    // coordinator's bounded fold budget) while the committed object write *did*
    // land, the transaction is not in the version chain — its committed object is
    // an orphan. The fast path must renew (surface `Wounded`) rather than report
    // success, and must never double-apply: a renewed attempt commits the value
    // exactly once.
    #[tokio::test(start_paused = true)]
    async fn single_rw_lock_cas_contention_renews_and_commits_once() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        // Fail exactly the coordinator's whole fold budget of shard-lock CAS
        // attempts, so the object write lands but the lock install never does.
        let flaky = FlakyShardCas::new(mem, crate::shard_coord::CAS_RETRIES);
        let backend: Arc<dyn Backend> = flaky.clone();
        let (tm, tctx) = new_algo_from_backend(backend).await;
        let keyp = paths::from_key(TEST_COLL, b"k");

        // Seed over the (unarmed) backend so the key exists and is committable.
        commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;
        let seed_writer = entry(&tctx, b"k").await.unwrap().current_writer.unwrap();
        let r = do_read(&tctx, &keyp).await;

        flaky.arm();
        let mut h = tm.begin(Data {
            reads: vec![r.clone()],
            writes: vec![wa(&keyp, b"v2")],
            scans: Vec::new(),
        });
        let orphan = h.id().clone();
        let err = tm.commit(&mut h).await.unwrap_err();
        assert!(
            matches!(err, TransError::Wounded),
            "a lock CAS that never lands must renew, got {err:?}"
        );

        // The whole budget was spent (sustained contention on the lock CAS), and
        // the orphan committed object never entered the chain: the entry still
        // points at the seed writer, unlocked.
        assert_eq!(
            flaky.remaining(),
            0,
            "expected sustained lock-CAS contention"
        );
        let e = entry(&tctx, b"k").await.unwrap();
        assert_eq!(e.current_writer, Some(seed_writer));
        assert!(e.locked_by.is_empty());

        // The renewed attempt (same priority, fresh id) commits exactly once.
        let mut h2 = tm.rebegin(h);
        assert_ne!(*h2.id(), orphan, "renew mints a fresh id");
        tm.commit(&mut h2).await.unwrap();
        tm.end(&mut h2).await.unwrap();

        let e = entry(&tctx, b"k").await.unwrap();
        assert_eq!(e.current_writer, Some(h2.id().clone()));
        assert!(e.locked_by.is_empty());
        let rv = do_read(&tctx, &keyp).await;
        assert_eq!(rv.version.unwrap().last_writer, *h2.id());
    }

    // Creating a key is ineligible for the single read-write fast path (it has no
    // predecessor value to build on), so it takes the full locked path. The fast
    // path never calls the locker, so a non-zero lock-call count proves the full
    // path was taken. Membership is coordinated by the per-key Create lock, so no
    // separate root CAS is issued (ADR-031): the leaf-write count is the full
    // path's entry-lock + write-back, exactly two.
    #[tokio::test]
    async fn single_rw_create_uses_full_path() {
        let (tm, tctx, log) = new_recording_algo().await;
        let keyp = paths::from_key(TEST_COLL, b"new");

        log.lock().unwrap().clear();
        tctx.locker.lock_calls_and_reset();
        let mut h = tm.begin(Data {
            reads: Vec::new(),
            writes: vec![wa(&keyp, b"v")],
            scans: Vec::new(),
        });
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        assert!(
            tctx.locker.lock_calls_and_reset() >= 1,
            "a create takes the full locked path"
        );
        let c = write_counts(&log);
        assert_eq!(
            c.leaf, 2,
            "create issues entry-lock + write-back, no membership CAS: {c:?}"
        );
        assert!(entry(&tctx, b"new").await.unwrap().exists());
    }

    // A delete is ineligible for the fast path too (it publishes a tombstone, not
    // a pointer over a predecessor), so it takes the full locked path; the
    // non-zero lock-call count proves it. No separate membership CAS is issued
    // (ADR-031).
    #[tokio::test]
    async fn single_rw_delete_uses_full_path() {
        let (tm, tctx, log) = new_recording_algo().await;
        let keyp = paths::from_key(TEST_COLL, b"k");

        commit_writes(&tm, vec![wa(&keyp, b"v")]).await;
        let r = do_read(&tctx, &keyp).await;

        log.lock().unwrap().clear();
        tctx.locker.lock_calls_and_reset();
        let mut h = tm.begin(Data {
            reads: vec![r],
            writes: vec![wdel(&keyp)],
            scans: Vec::new(),
        });
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        assert!(
            tctx.locker.lock_calls_and_reset() >= 1,
            "a delete takes the full locked path"
        );
        let c = write_counts(&log);
        assert_eq!(
            c.leaf, 2,
            "delete issues entry-lock + write-back, no membership CAS: {c:?}"
        );
        assert!(entry(&tctx, b"k").await.unwrap().deleted);
    }

    // A two-key write is ineligible (the fast path publishes one pointer): full
    // locked path.
    #[tokio::test]
    async fn single_rw_multi_key_uses_full_path() {
        let (tm, _tctx, log) = new_recording_algo().await;
        let ka = paths::from_key(TEST_COLL, b"a");
        let kb = paths::from_key(TEST_COLL, b"b");

        commit_writes(&tm, vec![wa(&ka, b"v1"), wa(&kb, b"v1")]).await;

        log.lock().unwrap().clear();
        let mut h = tm.begin(Data {
            reads: Vec::new(),
            writes: vec![wa(&ka, b"v2"), wa(&kb, b"v2")],
            scans: Vec::new(),
        });
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        let c = write_counts(&log);
        assert!(c.leaf >= 2, "a multi-key write takes the full path: {c:?}");
    }

    // Reading a key other than the written one needs that key's shard validated,
    // so the single-key write falls back to the full locked path.
    #[tokio::test]
    async fn single_rw_other_key_read_uses_full_path() {
        let (tm, tctx, log) = new_recording_algo().await;
        let ka = paths::from_key(TEST_COLL, b"a");
        let kb = paths::from_key(TEST_COLL, b"b");

        commit_writes(&tm, vec![wa(&ka, b"v1"), wa(&kb, b"v1")]).await;
        let ra = do_read(&tctx, &ka).await;

        log.lock().unwrap().clear();
        let mut h = tm.begin(Data {
            reads: vec![ra],
            writes: vec![wa(&kb, b"v2")],
            scans: Vec::new(),
        });
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        let c = write_counts(&log);
        assert!(
            c.leaf >= 2,
            "a read of another key forces the full path: {c:?}"
        );
    }

    #[tokio::test]
    async fn readonly_validates() {
        let (tm, tctx) = new_algo().await;
        let keyp = paths::from_key(TEST_COLL, b"k");

        commit_writes(&tm, vec![wa(&keyp, b"v")]).await;
        let r = do_read(&tctx, &keyp).await;

        let mut h = tm.begin(Data {
            reads: vec![r],
            writes: Vec::new(),
            scans: Vec::new(),
        });
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();
    }

    #[tokio::test]
    async fn readonly_after_remote_change_retries() {
        let (tm, tctx) = new_algo().await;
        let (tm2, _t2) = new_algo_from_backend(tctx.backend.clone()).await;
        let keyp = paths::from_key(TEST_COLL, b"k");

        commit_writes(&tm2, vec![wa(&keyp, b"v1")]).await;
        let r = do_read(&tctx, &keyp).await;
        commit_writes(&tm2, vec![wa(&keyp, b"v2")]).await;

        let mut h = tm.begin(Data {
            reads: vec![r],
            writes: Vec::new(),
            scans: Vec::new(),
        });
        let err = tm.commit(&mut h).await.unwrap_err();
        assert!(matches!(err, TransError::Retry), "got {err:?}");

        // After re-reading, validation passes.
        let r = do_read(&tctx, &keyp).await;
        tm.reset(
            &mut h,
            Data {
                reads: vec![r],
                writes: Vec::new(),
                scans: Vec::new(),
            },
        );
        tm.commit(&mut h).await.unwrap();
    }

    #[tokio::test]
    async fn readonly_after_delete_not_found() {
        let (tm, tctx) = new_algo().await;
        let keyp = paths::from_key(TEST_COLL, b"k");

        commit_writes(&tm, vec![wa(&keyp, b"v")]).await;
        commit_writes(&tm, vec![wdel(&keyp)]).await;

        // A read now resolves to not-found.
        let r = do_read(&tctx, &keyp).await;
        assert!(r.version.is_none());
        let mut h = tm.begin(Data {
            reads: vec![r],
            writes: Vec::new(),
            scans: Vec::new(),
        });
        tm.commit(&mut h).await.unwrap();
    }

    #[tokio::test]
    async fn delete_round_trips() {
        let (tm, tctx) = new_algo().await;
        let keyp = paths::from_key(TEST_COLL, b"k");

        commit_writes(&tm, vec![wa(&keyp, b"v")]).await;
        let r = do_read(&tctx, &keyp).await;
        let mut h = tm.begin(Data {
            reads: vec![r],
            writes: vec![wdel(&keyp)],
            scans: Vec::new(),
        });
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        let e = entry(&tctx, b"k").await.unwrap();
        assert!(e.deleted);
        let r = do_read(&tctx, &keyp).await;
        assert!(r.version.is_none());
    }

    #[tokio::test]
    async fn multi_key_commit() {
        let (tm, tctx) = new_algo().await;
        let k1 = paths::from_key(TEST_COLL, b"k1");
        let k2 = paths::from_key(TEST_COLL, b"k2");

        let mut h = tm.begin(Data {
            reads: Vec::new(),
            writes: vec![wa(&k1, b"v1"), wa(&k2, b"v2")],
            scans: Vec::new(),
        });
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        assert!(entry(&tctx, b"k1").await.unwrap().exists());
        assert!(entry(&tctx, b"k2").await.unwrap().exists());
    }

    // Installs live committed pointers for `keys` directly in the collection's
    // root leaf `_i` (no lock holders, no pending write-back), so the leaf's
    // object version is stable — a scan taken afterwards is not perturbed by an
    // asynchronous write-back settling.
    async fn seed_live_keys(tctx: &Tctx, keys: &[&[u8]]) {
        let path = paths::collection_info(TEST_COLL);
        let loaded = tctx
            .shards
            .load_leaf(&path, Freshness::Latest)
            .await
            .unwrap();
        let mut entries: std::collections::BTreeMap<Vec<u8>, ShardEntry> = loaded
            .entries
            .entries()
            .cloned()
            .map(|e| (e.key.clone(), e))
            .collect();
        for (i, k) in keys.iter().enumerate() {
            let w = TxId::with_priority((i as u64) + 1, b"seed");
            entries.insert(
                k.to_vec(),
                ShardEntry {
                    key: k.to_vec(),
                    lock_type: LockType::None,
                    locked_by: Vec::new(),
                    current_writer: Some(w),
                    deleted: false,
                },
            );
        }
        let shard = Shard::from_entries(entries.into_values());
        assert!(
            tctx.shards
                .store_leaf(&path, &shard, loaded.kind(), loaded.version.as_ref())
                .await
                .unwrap()
        );
    }

    // Builds a read-only listing transaction's [`Data`] from a fresh scan of the
    // test collection, returning the scan's live keys alongside so a test can
    // assert on the snapshot and later re-validate the same coverage.
    async fn scan_data(tctx: &Tctx) -> (Data, Vec<Vec<u8>>) {
        let resolver = Resolver::new(tctx.shards.clone(), tctx.tmon.clone());
        let scan = resolver.live_keys_scan(TEST_COLL).await.unwrap();
        let data = Data {
            reads: Vec::new(),
            writes: Vec::new(),
            scans: vec![ScanAccess {
                prefix: TEST_COLL.into(),
                covered: scan.covered,
            }],
        };
        (data, scan.keys)
    }

    // ADR-031 phantom prevention: a listing whose covered leaves are unchanged
    // commits, but one whose leaf a concurrent create mutated (bumping the leaf
    // version) fails validation and must re-run — so the create can never appear
    // as a phantom inside an already-validated snapshot.
    #[tokio::test]
    async fn scan_detects_racing_create() {
        let (tm, tctx) = new_algo().await;
        seed_live_keys(&tctx, &[b"a", b"c"]).await;

        let (data, keys) = scan_data(&tctx).await;
        assert_eq!(keys, vec![b"a".to_vec(), b"c".to_vec()]);

        // No concurrent change: the listing validates and commits.
        let mut h = tm.begin(data.clone());
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        // A create between the scan and (re-)validation bumps the covered leaf.
        commit_writes(&tm, vec![wa(&paths::from_key(TEST_COLL, b"b"), b"1")]).await;

        let mut stale = tm.begin(data);
        let err = tm.commit(&mut stale).await.unwrap_err();
        assert!(matches!(err, TransError::Retry), "got {err:?}");
    }

    // ADR-031 phantom prevention: a delete of a listed key also bumps the covered
    // leaf's version, so a scan taken before it fails re-validation (the key must
    // not silently vanish from an already-validated snapshot).
    #[tokio::test]
    async fn scan_detects_racing_delete() {
        let (tm, tctx) = new_algo().await;
        let bp = paths::from_key(TEST_COLL, b"b");
        seed_live_keys(&tctx, &[b"a", b"b"]).await;

        let (data, keys) = scan_data(&tctx).await;
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);

        commit_writes(&tm, vec![wdel(&bp)]).await;

        let mut stale = tm.begin(data);
        let err = tm.commit(&mut stale).await.unwrap_err();
        assert!(matches!(err, TransError::Retry), "got {err:?}");
    }

    // ADR-031: a split that lands between the scan and validation changes the
    // covered leaf set (one leaf becomes an index root over two leaves), which
    // validation detects and re-runs — the listing then re-scans the fresh
    // topology, following right-links, so no key is dropped or duplicated.
    #[tokio::test]
    async fn scan_detects_concurrent_split() {
        let (tm, tctx) = new_algo().await;
        seed_live_keys(&tctx, &[b"a", b"m"]).await;

        let (data, _keys) = scan_data(&tctx).await;

        // Grow the tree in place: rewrite `_i` from its single leaf into an index
        // root pointing at two fresh leaves (the shape the background splitter
        // produces), so the covered leaf set is no longer just `_i`.
        split_root_in_place(&tctx).await;

        let mut stale = tm.begin(data);
        let err = tm.commit(&mut stale).await.unwrap_err();
        assert!(matches!(err, TransError::Retry), "got {err:?}");
    }

    // ADR-031 boundary protection: on a multi-leaf tree a full scan covers every
    // leaf including the endpoints, so a create landing in the *last* leaf bumps
    // that covered leaf's version and re-validation fails — a boundary phantom
    // cannot slip past an already-validated listing.
    #[tokio::test]
    async fn scan_detects_boundary_membership_change() {
        use glassdb_storage::{IndexNode, Node};
        let (tm, tctx) = new_algo().await;

        // Two-leaf tree: index root over S0(a,c | high "m") -> S1(m,p).
        let leaf = |ks: &[&[u8]], high: Option<&[u8]>, right: Option<&str>| {
            let mut n = Node::leaf(Shard::from_entries(ks.iter().map(|k| ShardEntry {
                key: k.to_vec(),
                lock_type: LockType::None,
                locked_by: Vec::new(),
                current_writer: Some(TxId::with_priority(1, b"seed")),
                deleted: false,
            })));
            n.set_high_key(high.map(<[u8]>::to_vec));
            n.set_right_sibling(right.map(str::to_string));
            n
        };
        tctx.shards
            .store_node(
                TEST_COLL,
                "S0",
                &leaf(&[b"a", b"c"], Some(b"m"), Some("S1")),
                None,
            )
            .await
            .unwrap();
        tctx.shards
            .store_node(TEST_COLL, "S1", &leaf(&[b"m", b"p"], None, None), None)
            .await
            .unwrap();
        let mut root = CollectionRoot::new();
        root.set_node(Node::index(IndexNode::from_children([
            (b"".to_vec(), "S0".to_string()),
            (b"m".to_vec(), "S1".to_string()),
        ])));
        let cur = tctx
            .shards
            .load_leaf(&paths::collection_info(TEST_COLL), Freshness::Latest)
            .await
            .unwrap();
        tctx.shards
            .store_root(TEST_COLL, &root, cur.version.as_ref().unwrap())
            .await
            .unwrap();

        let (data, keys) = scan_data(&tctx).await;
        assert_eq!(
            keys,
            vec![b"a".to_vec(), b"c".to_vec(), b"m".to_vec(), b"p".to_vec()]
        );

        // Append a key past the current maximum: it lands in the last covered
        // leaf S1, bumping its version.
        let (s1, ver) = tctx
            .shards
            .load_node(TEST_COLL, "S1", Freshness::Latest)
            .await
            .unwrap();
        let mut entries: Vec<ShardEntry> = s1.as_leaf().unwrap().entries().cloned().collect();
        entries.push(ShardEntry {
            key: b"z".to_vec(),
            lock_type: LockType::None,
            locked_by: Vec::new(),
            current_writer: Some(TxId::with_priority(2, b"boundary")),
            deleted: false,
        });
        let mut new_s1 = Node::leaf(Shard::from_entries(entries));
        new_s1.set_high_key(None);
        tctx.shards
            .store_node(TEST_COLL, "S1", &new_s1, Some(&ver))
            .await
            .unwrap();

        let mut stale = tm.begin(data);
        let err = tm.commit(&mut stale).await.unwrap_err();
        assert!(matches!(err, TransError::Retry), "got {err:?}");
    }

    // Rewrites the test collection's root `_i` (a single leaf holding `a`,`m`)
    // into a two-level tree: an index root over leaf `S0` (a) and `S1` (m),
    // chained by right-sibling. A CAS on `_i` makes this the topology-growth
    // linearization point, mirroring the in-place root split (ADR-031).
    async fn split_root_in_place(tctx: &Tctx) {
        use glassdb_storage::{IndexNode, Node};

        let loaded = tctx
            .shards
            .load_leaf(&paths::collection_info(TEST_COLL), Freshness::Latest)
            .await
            .unwrap();
        let entries: Vec<ShardEntry> = loaded.entries.entries().cloned().collect();
        let (lower, upper): (Vec<_>, Vec<_>) = entries
            .into_iter()
            .partition(|e| e.key.as_slice() < b"m".as_slice());

        let mut s0 = Node::leaf(Shard::from_entries(lower));
        s0.set_high_key(Some(b"m".to_vec()));
        s0.set_right_sibling(Some("S1".to_string()));
        tctx.shards
            .store_node(TEST_COLL, "S0", &s0, None)
            .await
            .unwrap();
        let s1 = Node::leaf(Shard::from_entries(upper));
        tctx.shards
            .store_node(TEST_COLL, "S1", &s1, None)
            .await
            .unwrap();

        let mut root = CollectionRoot::new();
        root.set_node(Node::index(IndexNode::from_children([
            (b"".to_vec(), "S0".to_string()),
            (b"m".to_vec(), "S1".to_string()),
        ])));
        assert!(
            tctx.shards
                .store_root(TEST_COLL, &root, loaded.version.as_ref().unwrap())
                .await
                .unwrap(),
            "root split CAS must win"
        );
    }
}
