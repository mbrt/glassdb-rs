//! The transaction commit protocol with serializable isolation for the v2
//! object-native engine (ADR-016 … ADR-021).
//!
//! A read-write transaction validates its reads and installs its locks with one
//! read-modify-write CAS per touched shard (plus the collection root for
//! membership changes), flips its transaction object to committed (the commit
//! point), then publishes `current_writer` pointers and releases its locks
//! (write-back). A read-only transaction takes a pure optimistic fast path: it
//! re-resolves each read's effective writer against the shards and commits if
//! none changed, taking no locks.
//!
//! Concurrency control (ADR-002 / ADR-020 / ADR-021): strict two-phase locking
//! with wound-wait and leases for crash recovery. On a conflict it cannot win a
//! transaction **aborts and retries with its priority preserved**
//! ([`TxId::renew`]) rather than blocking while holding locks, so it cannot
//! deadlock. Lock acquisition has two modes: the default **parallel** path locks
//! every shard concurrently; after [`SERIAL_FALLBACK_AFTER`] failed attempts a
//! transaction escalates to the **serial** sorted-locking fallback that breaks
//! the equal-priority livelock (one contender always wins the lowest shard).

use std::sync::{Arc, Weak};
use std::time::Duration;

use glassdb_concurr::{Background, Backoff, Clock, RetryConfig, rt};
use glassdb_data::TxId;
use glassdb_storage::{TxCommitStatus, TxLog, TxWrite, ValueCache, Version};

use crate::error::TransError;
use crate::gc::Gc;
use crate::monitor::Monitor;
use crate::resolver::Resolver;
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

/// The reads and writes that make up a transaction.
#[derive(Debug, Clone, Default)]
pub struct Data {
    pub reads: Vec<ReadAccess>,
    pub writes: Vec<WriteAccess>,
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

/// Coordinates transactions: read validation, locking, commit, and write-back.
#[derive(Clone)]
pub struct Algo {
    values: ValueCache,
    resolver: Resolver,
    locker: Locker,
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
    pub fn new(
        values: ValueCache,
        locker: Locker,
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
            mon,
            gc,
            clock,
            background,
        }
    }

    /// Releases coordinator resources. A no-op in v2 (the locker spawns no owner
    /// tasks), kept for call-site stability.
    pub async fn close(&self) {
        self.locker.close().await;
    }

    /// Returns a reference to the underlying [`Locker`] for diagnostics.
    pub fn locker(&self) -> &Locker {
        &self.locker
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
        self.commit_read_write(tx).await
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
        if self.validate_reads_inner(&tx.data).await? {
            tx.status = Status::Committed;
            return Ok(());
        }
        self.invalidate_reads(&tx.data);
        Err(TransError::Retry)
    }

    /// Read-write path: lock the touched shards (and roots for membership
    /// changes), validate the reads now that their values are frozen, flip the
    /// transaction object to committed, then spawn the write-back in the
    /// background so commit returns without waiting for it.
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
        if let Err(e) = self.commit_writes(&tx.data.writes, &tx.id).await {
            if matches!(e, TransError::AlreadyFinalized) {
                // The log was finalized as `aborted` out from under us: a wound
                // landed between locking and commit.
                return self.restart(tx).await;
            }
            return Err(e.context(format!("committing writes for tx {}", tx.id)));
        }
        tx.status = Status::Committed;

        self.write_back(&tx.id, locked).await;
        // GC is inert in v2 (ADR-022 deferred); this records nothing but keeps
        // the write-back hook wired for the future mark-sweep collector.
        self.gc.schedule_tx_cleanup(tx.id.clone());
        Ok(())
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
                let id = id.clone();
                bg.spawn_waited(async move {
                    locker.write_back(&id, &locked).await;
                });
            }
            None => self.locker.write_back(id, &locked).await,
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

    /// Validates the reads of a read-only transaction (the error-recovery path
    /// in the db retry loop), returning [`TransError::Retry`] if any read was
    /// invalidated. It holds no locks and does not back off before signalling
    /// the retry.
    pub async fn validate_reads(&self, tx: &mut Handle) -> Result<(), TransError> {
        if !tx.data.writes.is_empty() {
            return Err(TransError::other(
                "cannot validate only reads when writes are present",
            ));
        }
        if self.validate_reads_inner(&tx.data).await? {
            return Ok(());
        }
        self.invalidate_reads(&tx.data);
        Err(TransError::Retry)
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
    async fn commit_writes(&self, writes: &[WriteAccess], id: &TxId) -> Result<(), TransError> {
        let mut tl = TxLog::new(id.clone(), TxCommitStatus::Ok);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::Reader;
    use glassdb_backend::{Backend, memory::MemoryBackend};
    use glassdb_concurr::{Background, RetryConfig};
    use glassdb_data::paths;
    use glassdb_data::shard::shard_index;
    use glassdb_storage::{
        CollectionRoot, MAX_STALENESS, ObjectCache, ShardEntry, ShardStore, SharedCache,
        StorageError, TLogger, TxCommitStatus, ValueCache,
    };

    const TEST_COLL: &str = "testp";

    struct Tctx {
        backend: Arc<dyn Backend>,
        values: ValueCache,
        tlogger: TLogger,
        tmon: Monitor,
        shards: ShardStore,
    }

    async fn new_algo() -> (Algo, Tctx) {
        new_algo_from_backend(Arc::new(MemoryBackend::new())).await
    }

    async fn new_algo_from_backend(b: Arc<dyn Backend>) -> (Algo, Tctx) {
        let cache = SharedCache::new(1024);
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
        let locker = Locker::new(shards.clone(), tmon.clone(), RetryConfig::default());
        let gc = Gc::new(bg_weak.clone(), tlogger.clone());

        // Create the collection root so membership locks have a home.
        shards
            .create_root_if_absent(
                TEST_COLL,
                &CollectionRoot::new(glassdb_data::shard::SHARD_COUNT),
            )
            .await
            .unwrap();

        let algo = Algo::new(
            values.clone(),
            locker,
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
            },
        )
        .await
    }

    async fn entry(tctx: &Tctx, key: &[u8]) -> Option<ShardEntry> {
        let (shard, _) = tctx
            .shards
            .load_shard(TEST_COLL, shard_index(key))
            .await
            .unwrap();
        shard.lookup(key).cloned()
    }

    #[tokio::test]
    async fn write_new() {
        let (tm, tctx) = new_algo().await;
        let keyp = paths::from_key(TEST_COLL, b"k");
        let val = b"v";

        let mut h = tm.begin(Data {
            reads: Vec::new(),
            writes: vec![wa(&keyp, val)],
        });
        tm.commit(&mut h).await.unwrap();
        let tid = h.id().clone();
        tm.end(&mut h).await.unwrap();

        let status = tctx.tlogger.commit_status(&tid).await.unwrap();
        assert_eq!(status.status, TxCommitStatus::Ok);
        let txlog = tctx.tlogger.get(&tid).await.unwrap();
        assert_eq!(txlog.writes.len(), 1);
        assert_eq!(txlog.writes[0].path, keyp);
        assert_eq!(&*txlog.writes[0].value, val);

        // The shard entry points at the committed writer and the lock is gone.
        let e = entry(&tctx, b"k").await.unwrap();
        assert_eq!(e.current_writer, Some(tid));
        assert!(e.locked_by.is_empty());
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
        });
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        let r = do_read(&tctx, &keyp).await;
        assert_eq!(r.version.as_ref().unwrap().last_writer, *h.id());
    }

    // A read whose value moved before it was locked does not abort-and-renew; it
    // re-runs the body in place (`Retry`) while holding its locks (ADR-024). The
    // engine validates *after* locking, so unlike a pre-lock check the moved key
    // is itself locked during the re-run window — the v1 guarantee that the retry
    // holds all its locks.
    #[tokio::test]
    async fn stale_read_write_retries_holding_locks() {
        let (tm, tctx) = new_algo().await;
        let (tm2, _t2) = new_algo_from_backend(tctx.backend.clone()).await;
        let keyp = paths::from_key(TEST_COLL, b"k");

        commit_writes(&tm2, vec![wa(&keyp, b"v1")]).await;
        let ra = do_read(&tctx, &keyp).await;

        // Another client overwrites the key, making `ra` stale.
        commit_writes(&tm2, vec![wa(&keyp, b"v2")]).await;

        let mut h = tm.begin(Data {
            reads: vec![ra],
            writes: vec![wa(&keyp, b"v3")],
        });
        let err = tm.commit(&mut h).await.unwrap_err();
        assert!(matches!(err, TransError::Retry), "got {err:?}");

        // The moved key is locked by us when the stale read is signalled: the
        // re-run owns the lock and cannot lose it again to the same race.
        let e = entry(&tctx, b"k").await.expect("entry exists");
        assert_eq!(e.locked_by, vec![h.id().clone()]);

        tm.end(&mut h).await.unwrap();
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
        let held = tm
            .locker()
            .lock(
                &holder,
                &Data {
                    reads: Vec::new(),
                    writes: vec![wa(&keyp, b"h")],
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

    /// A [`Backend`] that, once armed, makes the first `budget` shard-lock CAS
    /// writes (`write_if` on a `/_s/` shard object) miss their precondition,
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
                && path.contains("/_s/")
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

    // ADR-020/024: CAS contention is resolved *inside* `Algo`. A transaction that
    // loses the shard-lock CAS repeatedly releases its (partial) locks and
    // re-acquires them under the *same id* — no renew, no body re-run — escalating
    // to the serial order. It never surfaces `Wounded` for a mere lost race, and
    // commits unchanged once the contention clears. A budget far larger than the
    // ~handful of parallel attempts that fit before the deadlock timeout forces
    // the serial CAS budget to be exhausted, i.e. the `Conflict` path.
    #[tokio::test(start_paused = true)]
    async fn cas_contention_relocks_keeping_id() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let flaky = FlakyShardCas::new(mem, 70);
        let backend: Arc<dyn Backend> = flaky.clone();
        let (tm, tctx) = new_algo_from_backend(backend).await;
        let keyp = paths::from_key(TEST_COLL, b"k");

        // Seed the key over a clean connection so the shard exists (its lock CAS
        // is then a `write_if`, the thing we fault).
        commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;

        flaky.arm();
        let mut h = tm.begin(Data {
            reads: Vec::new(),
            writes: vec![wa(&keyp, b"v2")],
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
        // It still committed: the shard points at our writer with no live lock.
        let e = entry(&tctx, b"k").await.unwrap();
        assert_eq!(e.current_writer, Some(id_before));
        assert!(e.locked_by.is_empty());
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
        });
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        assert!(entry(&tctx, b"k1").await.unwrap().exists());
        assert!(entry(&tctx, b"k2").await.unwrap().exists());
    }
}
