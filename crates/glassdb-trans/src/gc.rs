//! Garbage collection of finalized transaction objects by candidate-driven
//! reverse mark-sweep (ADR-022).
//!
//! In the v2 object-native layout a committed transaction object *is* the value
//! store: a key's live value lives in the object its shard entry's
//! `current_writer` points at, and readers help-forward through it. So a
//! transaction object is **live** exactly while some shard still references its
//! txid (`current_writer` or `locked_by`), and GC is a reachability problem, not
//! a timer.
//!
//! A forward mark (list every shard, union the referenced txids) costs the whole
//! database per cycle. Instead each candidate `_t/` object records its own
//! back-references (its `locks ∪ writes`), so GC works **backward**: it reads a
//! batch of candidates and confirms each one dead by GET-ing only the handful of
//! shards it names — never a database-wide scan. Candidates come from the
//! write-back hint ([`Gc::schedule_tx_cleanup`], the `current_writer` a fresh
//! commit just superseded) and paged walks of the sharded `{db}/_t/{ss}/`
//! namespace (which makes the candidate set complete regardless of lost hints).
//!
//! Safety rests on the ADR-021 lease as a horizon (`is_expired`): a candidate
//! within the horizon is always kept, because the non-atomic reverse check can
//! race a lock a live transaction has taken but not yet published (ADR-024's
//! lazy object materialization). Past the horizon, resolution is by status:
//! a committed object is deleted only once its complete record proves it
//! unreferenced; a dead pending one is first **force-aborted** (`pending →
//! aborted` CAS) so its death is durable before any lock moves; and an aborted
//! object is a **tombstone**, its locks released at once but the object kept a
//! full lease past the abort (its `timestamp` is the abort instant, so the same
//! `is_expired` gate enforces this) so a stuck owner is neither resurrected by
//! its own create-if-absent refresher nor left pointing at a prematurely-missing
//! object.
//!
//! Lock reclamation flows through the shard-mutation coordinator (ADR-029): GC
//! calls the [`Locker`]'s stateless per-object unlock methods rather than issuing
//! its own shard/root CAS, so every mutation goes through one place.

use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use std::time::UNIX_EPOCH;

use glassdb_backend as backend;
use glassdb_concurr::{Background, Clock, rt};
use glassdb_data::{KeyRef, TxId, paths, shuffle};
use glassdb_storage::{
    Directory, Observation, Requirement, ShardStore, StorageError, TLogger, Timeline,
    TxCommitStatus, TxLock, TxLog,
};

use crate::error::TransError;
use crate::monitor::Monitor;
use crate::tlocker::Locker;

/// How often the collector runs a sweep cycle. Reuses the lease timeout: a
/// candidate cannot become collectable faster than one lease anyway, so a
/// tighter cadence would only re-GET still-referenced objects.
/// Maximum number of transaction objects returned by one listing request.
const GC_LIST_PAGE: usize = 128;

/// Maximum listing requests issued by one cycle. Empty shards still cost a
/// request, so this bounds work independently of how sparse the log namespace
/// is.
const GC_LIST_REQUEST_BUDGET: usize = 64;

/// Upper bound on the buffered write-back hint queue. The paged list guarantees
/// completeness, so dropping the oldest hint when the queue is full only delays
/// a delete, never causes an unsafe one (ADR-022).
const HINT_QUEUE_CAP: usize = 4096;

/// Garbage collector for finalized transaction objects (ADR-022).
#[derive(Clone)]
pub struct Gc {
    // Weak so a `Gc` clone captured inside the spawned sweep loop does not keep
    // the [`Background`] alive across DB shutdown; the single strong owner is
    // `DbInner::background`.
    bg: Weak<Background>,
    tl: TLogger,
    dir: Directory,
    locker: Locker,
    mon: Monitor,
    clock: Clock,
    timeline: Timeline,
    // Write-back hint feed: txids a fresh commit just superseded (primary
    // candidate source). Deduplicated when drained.
    hints: Arc<Mutex<VecDeque<TxId>>>,
}

/// Task-local traversal state for the transaction-log shards.
struct TxScan {
    shards: Vec<usize>,
    current: usize,
    cursor: Option<backend::ListCursor>,
}

impl TxScan {
    fn shuffled() -> Self {
        let mut shards = (0..paths::TRANSACTION_SHARD_COUNT).collect::<Vec<_>>();
        shuffle(&mut shards);
        TxScan {
            shards,
            current: 0,
            cursor: None,
        }
    }

    fn begin_next_pass(&mut self) {
        shuffle(&mut self.shards);
        self.current = 0;
        self.cursor = None;
    }

    fn finish_shard(&mut self) {
        self.current += 1;
        self.cursor = None;
    }
}

impl Gc {
    /// Creates a collector over the transaction log, shard store, locker, and
    /// monitor. Freshness barriers use `timeline`; lease horizons use `clock`
    /// so they remain deterministic under the DST executor.
    pub fn new(
        bg: Weak<Background>,
        tl: TLogger,
        shards: ShardStore,
        timeline: Timeline,
        locker: Locker,
        mon: Monitor,
        clock: Clock,
    ) -> Self {
        let dir = Directory::new(shards.clone());
        Gc {
            bg,
            tl,
            dir,
            locker,
            mon,
            clock,
            timeline,
            hints: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Starts the background sweep loop on the [`Background`] executor. It runs
    /// one cycle every transaction-liveness interval until the executor is
    /// dropped. If the executor is already gone (DB shut down) nothing is
    /// started.
    pub fn start(&self) {
        let Some(bg) = self.bg.upgrade() else {
            return;
        };
        let gc = self.clone();
        let mut scan = TxScan::shuffled();
        bg.spawn(async move {
            loop {
                rt::sleep(gc.mon.protocol_timing().pending_timeout()).await;
                gc.run_once(&mut scan).await;
            }
        });
    }

    /// Enqueues a superseded transaction id as a reverse-check candidate: the
    /// former `current_writer` a fresh commit's write-back just overwrote, which
    /// therefore just lost a reference (ADR-022). The oldest hint is dropped when
    /// the queue is full; the paged list still visits it eventually.
    pub(crate) fn schedule_tx_cleanup(&self, tid: TxId) {
        let mut q = self.hints.lock().unwrap();
        if q.len() >= HINT_QUEUE_CAP {
            q.pop_front();
        }
        q.push_back(tid);
    }

    /// Runs a single sweep cycle: the buffered hints plus at most one non-empty
    /// transaction-log page, each checked by the reverse liveness check.
    /// Best-effort — a transient error on one candidate only delays its delete
    /// to a later cycle, so it is logged and the cycle continues.
    async fn run_once(&self, scan: &mut TxScan) {
        let mut seen: BTreeSet<TxId> = BTreeSet::new();
        let mut candidates: Vec<TxId> = Vec::new();
        {
            let mut q = self.hints.lock().unwrap();
            while let Some(tid) = q.pop_front() {
                if seen.insert(tid.clone()) {
                    candidates.push(tid);
                }
            }
        }
        for tid in self.next_list_page(scan).await {
            if seen.insert(tid.clone()) {
                candidates.push(tid);
            }
        }

        for tid in candidates {
            if let Err(e) = self.check_candidate(&tid).await {
                tracing::debug!(tx = %tid, error = %e, "gc candidate check deferred");
            }
        }
    }

    /// Returns at most one non-empty transaction-log page, advancing through a
    /// shuffled shard order while staying within the per-cycle request budget.
    async fn next_list_page(&self, scan: &mut TxScan) -> Vec<TxId> {
        let limit = backend::ListLimit::new(GC_LIST_PAGE).unwrap();

        for _ in 0..GC_LIST_REQUEST_BUDGET {
            if scan.current == scan.shards.len() {
                scan.begin_next_pass();
            }
            let shard = scan.shards[scan.current];
            let cursor = scan.cursor.clone();
            match self
                .tl
                .list_transaction_ids(shard, cursor.as_ref(), limit)
                .await
            {
                Ok(page) => {
                    scan.cursor = page.next;
                    if scan.cursor.is_none() {
                        scan.finish_shard();
                    }
                    if !page.ids.is_empty() {
                        return page.ids;
                    }
                }
                Err(StorageError::InvalidCursor) => {
                    // Provider tokens can expire; restarting only this shard
                    // preserves progress through the rest of the pass.
                    scan.cursor = None;
                }
                Err(e) => {
                    tracing::debug!(shard, error = %e, "gc transaction-log listing failed");
                    return Vec::new();
                }
            }
        }
        Vec::new()
    }

    /// The reverse liveness check for one candidate (ADR-022): read it, keep it
    /// if within the safety horizon, else resolve by status.
    async fn check_candidate(&self, tid: &TxId) -> Result<(), TransError> {
        // GC has no preceding transaction barrier or CAS receipt: it must
        // establish one candidate-check epoch before deciding that no durable
        // leaf still references the transaction. The same epoch is propagated
        // through routing and release so the decision is one coherent sweep.
        let candidate_check = Requirement::AtLeast(self.timeline.now());
        let observed = match self.tl.get_at(tid, Requirement::Any).await {
            Ok(v) => v,
            // Already reclaimed (or never existed): nothing to do.
            Err(StorageError::NotFound) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let log = observed
            .value()
            .ok_or_else(|| StorageError::other("transaction disappeared after a present read"))?;

        // Within the horizon: keep. A recent pending object may be a live
        // transaction whose lock this non-atomic check has not observed yet
        // (ADR-024 materializes the object lazily, after the locks are taken);
        // a recent committed/aborted one is left for a later cycle.
        let ts = log.timestamp.unwrap_or(UNIX_EPOCH);
        if !self.mon.protocol_timing().is_expired(ts, self.clock.now()) {
            return Ok(());
        }

        match log.status {
            TxCommitStatus::Ok => {
                self.reclaim_committed(tid, log, &observed, candidate_check)
                    .await
            }
            TxCommitStatus::Aborted => {
                self.reclaim_aborted(tid, &log.locks, &observed, candidate_check)
                    .await
            }
            TxCommitStatus::Pending => {
                self.reclaim_dead_pending(tid, log, &observed, candidate_check)
                    .await
            }
            TxCommitStatus::Unknown => Ok(()),
        }
    }

    /// A committed candidate past the horizon is deleted only once its complete
    /// record proves it unreferenced. Any recorded write still pointed at by a
    /// `current_writer`, or any recorded lock still held (the commit→write-back
    /// gap), keeps it. A committed object is never pruned or force-aborted — its
    /// locks become `current_writer` through write-back, never through GC.
    async fn reclaim_committed(
        &self,
        tid: &TxId,
        log: &TxLog,
        observation: &Observation<TxLog>,
        requirement: Requirement,
    ) -> Result<(), TransError> {
        if self.still_referenced(tid, log, requirement).await? {
            return Ok(());
        }
        self.release_locks(tid, &log.locks, requirement).await?;
        self.tl.delete(observation).await?;
        Ok(())
    }

    /// An aborted candidate holds no value, and past the horizon its `timestamp`
    /// (the abort instant) proves the tombstone has outlived any client that
    /// could still act under its txid. Release its recorded locks (pruning any
    /// entry left vestigial) and delete it. A minimal abort with no recorded
    /// locks simply has nothing to release.
    async fn reclaim_aborted(
        &self,
        tid: &TxId,
        locks: &[TxLock],
        observation: &Observation<TxLog>,
        requirement: Requirement,
    ) -> Result<(), TransError> {
        self.release_locks(tid, locks, requirement).await?;
        self.tl.delete(observation).await?;
        Ok(())
    }

    /// A dead pending candidate is reclaimed with the official expiry sequence
    /// (ADR-021/024), never by dropping its locks in place: force-abort it
    /// (`pending → aborted` CAS) so its death is durable and final. If a live
    /// owner committed or refreshed first the CAS loses and it is left alone. On
    /// a successful abort its locks are released now (the fresh aborted object
    /// records none), but it is **not** deleted — the abort stamped a new lease,
    /// so a later cycle deletes it once past the horizon from the abort.
    async fn reclaim_dead_pending(
        &self,
        tid: &TxId,
        log: &TxLog,
        observation: &Observation<TxLog>,
        requirement: Requirement,
    ) -> Result<(), TransError> {
        match self.mon.force_abort(tid, observation).await? {
            TxCommitStatus::Aborted => self.release_locks(tid, &log.locks, requirement).await,
            // Committed or refreshed first: it was alive. Leave it.
            _ => Ok(()),
        }
    }

    /// Reports whether any entry the candidate recorded still names its txid: a
    /// written key's `current_writer` or a locked key's `locked_by`. Checking
    /// only the recorded set is equivalent to scanning every shard, because an
    /// entry can name `txid` only if `txid` put it there.
    ///
    /// The recorded keys are routed to their leaves by descent
    /// ([`Directory::group_keys_by_leaf`]) so each touched leaf is fetched once —
    /// a write and its write-lock name the same key, and sibling keys share a
    /// leaf, so a per-key load would re-read the same leaf several times per
    /// candidate. Each key carries the [`CheckKind`] that says which field to
    /// inspect.
    async fn still_referenced(
        &self,
        tid: &TxId,
        log: &TxLog,
        requirement: Requirement,
    ) -> Result<bool, TransError> {
        let mut items: Vec<(KeyRef, CheckKind)> = log
            .writes
            .iter()
            .map(|write| (write.key.clone(), CheckKind::Writer))
            .collect();
        for lock in &log.locks {
            if let TxLock::Entry { key, .. } = lock {
                items.push((key.clone(), CheckKind::Holder));
            }
        }

        for group in self.dir.group_keys_by_leaf(items, requirement).await? {
            let Some(leaf) = group.node().and_then(|node| node.as_leaf()) else {
                continue;
            };
            for (raw_key, kind) in &group.keys {
                let referenced = match kind {
                    CheckKind::Writer => {
                        leaf.lookup(raw_key).and_then(|e| e.current_writer.as_ref()) == Some(tid)
                    }
                    CheckKind::Holder => leaf
                        .lookup(raw_key)
                        .is_some_and(|e| e.locked_by.contains(tid)),
                };
                if referenced {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Releases `tid` from every leaf its recorded `locks` name, grouping the
    /// paths by descent so each leaf is visited once (targeted pruning, never a
    /// whole-key-space scan). Each release flows through the locker's
    /// coordinator-backed unlock method (ADR-029) — one deduplicated fold round
    /// per object that clears `tid` and drops any entry it thereby leaves
    /// vestigial — so GC issues no leaf CAS of its own. `current_writer` is
    /// never touched.
    async fn release_locks(
        &self,
        tid: &TxId,
        locks: &[TxLock],
        requirement: Requirement,
    ) -> Result<(), TransError> {
        let mut key_locks: Vec<(KeyRef, ())> = Vec::new();
        let mut leaf_paths = BTreeSet::new();
        for lock in locks {
            match lock {
                TxLock::Entry { key, .. } => key_locks.push((key.clone(), ())),
                TxLock::Membership { leaf, .. } => {
                    leaf_paths.insert(leaf.physical_path());
                }
            }
        }
        for group in self.dir.group_keys_by_leaf(key_locks, requirement).await? {
            leaf_paths.insert(group.path);
        }
        for path in leaf_paths {
            self.locker.release_leaf(tid, &path).await?;
        }
        Ok(())
    }
}

/// Which shard-entry field a liveness check consults for a recorded key: a
/// written key is referenced while it is the entry's `current_writer`; a locked
/// key while it appears in `locked_by`. Rides along as the per-key payload of
/// [`Gc::still_referenced`]'s batched shard load.
enum CheckKind {
    Writer,
    Holder,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolver::Resolver;
    use crate::shard_coord::ShardCoordinator;
    use crate::tlocker::LockOutcome;
    use glassdb_backend::middleware::{BackendOp, HookBackend, HookFuture, RecordingBackend};
    use glassdb_backend::{Backend, memory::MemoryBackend};
    use glassdb_concurr::RetryConfig;
    use glassdb_storage::{
        CachedStore, CollectionRoot, Directory, LockType, Shard, ShardEntry, Timeline, TxWrite,
    };
    use std::collections::BTreeMap;
    use std::time::{Duration, SystemTime};

    const COLL: &str = "db/_c/0000000000000000000000";

    fn collection() -> glassdb_data::CollectionAddress {
        glassdb_data::CollectionAddress::root("db")
    }

    // A fixed wall-clock anchor so the horizon is a pure function of the
    // offsets the tests choose, independent of the machine's real clock.
    fn base() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)
    }

    // Comfortably past the 45s (timeout + skew) safety horizon.
    const PAST_HORIZON: Duration = Duration::from_secs(120);

    struct Ctx {
        gc: Gc,
        tl: TLogger,
        shards: ShardStore,
        timeline: Timeline,
        locker: Locker,
        mon: Monitor,
    }

    async fn new_ctx() -> Ctx {
        new_ctx_with(Arc::new(MemoryBackend::new())).await
    }

    async fn new_ctx_with(backend: Arc<dyn Backend>) -> Ctx {
        let timeline = Timeline::new();
        let objects = CachedStore::new(backend, 1 << 20, timeline.clone(), None);
        let tl = TLogger::new(objects.clone(), "db");
        let shards = ShardStore::new(objects);
        assert!(
            shards
                .create_root(COLL, &CollectionRoot::new())
                .await
                .unwrap()
        );
        let bg = Arc::new(Background::new());
        let clock = Clock::anchored_at(base());
        let mon = Monitor::with_config(
            tl.clone(),
            timeline.clone(),
            Arc::downgrade(&bg),
            clock.clone(),
            RetryConfig::default(),
            crate::monitor::ProtocolTiming::default(),
        );
        let resolver = Resolver::new(shards.clone(), mon.clone());
        let coord = ShardCoordinator::new(
            shards.clone(),
            resolver,
            mon.clone(),
            RetryConfig::default(),
        );
        let dir = Directory::new(shards.clone());
        let locker = Locker::new(coord.clone(), dir, mon.clone(), RetryConfig::default());
        let gc = Gc::new(
            Arc::downgrade(&bg),
            tl.clone(),
            shards.clone(),
            timeline.clone(),
            locker.clone(),
            mon.clone(),
            clock,
        );
        Ctx {
            gc,
            tl,
            shards,
            timeline,
            locker,
            mon,
        }
    }

    fn tx(n: u8) -> TxId {
        TxId::from_bytes(vec![n])
    }

    fn key_path(k: &[u8]) -> KeyRef {
        KeyRef::new(collection(), k)
    }

    fn write_lock(k: &[u8]) -> TxLock {
        TxLock::Entry {
            key: key_path(k),
            typ: LockType::Write,
        }
    }

    async fn store_entry(ctx: &Ctx, _key: &[u8], entry: ShardEntry) {
        let path = paths::collection_info(COLL);
        let loaded = ctx
            .shards
            .load_leaf(&path, Requirement::AtLeast(ctx.timeline.now()))
            .await
            .unwrap();
        let mut entries: BTreeMap<Vec<u8>, ShardEntry> = loaded
            .entries
            .entries()
            .cloned()
            .map(|e| (e.key.clone(), e))
            .collect();
        entries.insert(entry.key.clone(), entry);
        let shard = Shard::from_entries(entries.into_values());
        assert!(
            ctx.shards
                .store_leaf(
                    &path,
                    &shard,
                    &loaded.locks,
                    loaded.kind(),
                    &loaded.observation,
                )
                .await
                .unwrap()
        );
    }

    async fn lookup_entry(ctx: &Ctx, key: &[u8]) -> Option<ShardEntry> {
        let loaded = ctx
            .shards
            .load_leaf(
                &paths::collection_info(COLL),
                Requirement::AtLeast(ctx.timeline.now()),
            )
            .await
            .unwrap();
        loaded.entries.lookup(key).cloned()
    }

    fn committed(id: TxId, offset: Duration, writes: &[&[u8]], locks: &[&[u8]]) -> TxLog {
        TxLog {
            id,
            timestamp: Some(base() - offset),
            status: TxCommitStatus::Ok,
            writes: writes
                .iter()
                .map(|k| TxWrite {
                    key: key_path(k),
                    value: Arc::from(&b"v"[..]),
                    deleted: false,
                    prev_writer: TxId::default(),
                })
                .collect(),
            locks: locks.iter().map(|k| write_lock(k)).collect(),
        }
    }

    fn writer_entry(key: &[u8], writer: &TxId) -> ShardEntry {
        ShardEntry {
            key: key.to_vec(),
            lock_type: LockType::None,
            locked_by: Vec::new(),
            current_writer: Some(writer.clone()),
            deleted: false,
        }
    }

    fn locked_entry(key: &[u8], holder: &TxId) -> ShardEntry {
        ShardEntry {
            key: key.to_vec(),
            lock_type: LockType::Write,
            locked_by: vec![holder.clone()],
            current_writer: None,
            deleted: false,
        }
    }

    async fn is_gone(tl: &TLogger, id: &TxId) -> bool {
        matches!(
            tl.get_at(id, Requirement::Any).await,
            Err(StorageError::NotFound)
        )
    }

    fn test_scan(shards: Vec<usize>, cursor: Option<backend::ListCursor>) -> TxScan {
        TxScan {
            shards,
            current: 0,
            cursor,
        }
    }

    async fn run_once(gc: &Gc) {
        let mut scan = TxScan::shuffled();
        gc.run_once(&mut scan).await;
    }

    // A committed object whose written key has since been overwritten by a newer
    // writer holds no reference and is swept. Fed via the paged list (no hint),
    // so the list feed is exercised too.
    #[tokio::test(start_paused = true)]
    async fn committed_unreferenced_is_collected() {
        let ctx = new_ctx().await;
        let (old, new) = (tx(1), tx(2));
        ctx.tl
            .set(&committed(old.clone(), PAST_HORIZON, &[b"k"], &[]))
            .await
            .unwrap();
        // The key now points at a newer writer, not `old`.
        store_entry(&ctx, b"k", writer_entry(b"k", &new)).await;
        let mut scan = test_scan(vec![paths::transaction_shard(&old)], None);

        ctx.gc.run_once(&mut scan).await;

        assert!(is_gone(&ctx.tl, &old).await);
    }

    // A committed object still named by its key's `current_writer` is the live
    // value: it must never be collected.
    #[tokio::test(start_paused = true)]
    async fn committed_still_referenced_is_kept() {
        let ctx = new_ctx().await;
        let t = tx(1);
        ctx.tl
            .set(&committed(t.clone(), PAST_HORIZON, &[b"k"], &[]))
            .await
            .unwrap();
        store_entry(&ctx, b"k", writer_entry(b"k", &t)).await;
        ctx.gc.schedule_tx_cleanup(t.clone());

        run_once(&ctx.gc).await;

        let log = ctx.tl.get_at(&t, Requirement::Any).await.unwrap();
        let log = log.value().unwrap();
        assert_eq!(log.status, TxCommitStatus::Ok);
    }

    // A recent pending object (within the safety horizon) is kept: it may be a
    // live transaction whose lock this non-atomic check cannot yet rule out.
    #[tokio::test(start_paused = true)]
    async fn recent_pending_is_kept() {
        let ctx = new_ctx().await;
        let t = tx(1);
        let mut log = TxLog::new(t.clone(), TxCommitStatus::Pending);
        log.timestamp = Some(base());
        log.locks = vec![write_lock(b"k")];
        ctx.tl.set(&log).await.unwrap();
        store_entry(&ctx, b"k", locked_entry(b"k", &t)).await;
        ctx.gc.schedule_tx_cleanup(t.clone());

        run_once(&ctx.gc).await;

        let got = ctx.tl.get_at(&t, Requirement::Any).await.unwrap();
        let got = got.value().unwrap();
        assert_eq!(got.status, TxCommitStatus::Pending);
        // Its lock is untouched.
        let e = lookup_entry(&ctx, b"k").await.unwrap();
        assert_eq!(e.locked_by, vec![t]);
    }

    // A dead pending object past the horizon is force-aborted (its death made
    // durable) and its locks released, but it is retained as a fresh tombstone —
    // never deleted in the same cycle it is aborted. Fed via the write-back hint.
    #[tokio::test(start_paused = true)]
    async fn dead_pending_is_force_aborted_and_locks_released() {
        let ctx = new_ctx().await;
        let t = tx(1);
        let mut log = TxLog::new(t.clone(), TxCommitStatus::Pending);
        log.timestamp = Some(base());
        log.locks = vec![write_lock(b"k")];
        ctx.tl.set(&log).await.unwrap();
        store_entry(&ctx, b"k", locked_entry(b"k", &t)).await;

        tokio::time::sleep(PAST_HORIZON).await;
        ctx.gc.schedule_tx_cleanup(t.clone());
        run_once(&ctx.gc).await;

        // Death is durable...
        let got = ctx.tl.get_at(&t, Requirement::Any).await.unwrap();
        let got = got.value().unwrap();
        assert_eq!(got.status, TxCommitStatus::Aborted);
        // ...its lock is released (the now-vestigial entry pruned)...
        assert!(lookup_entry(&ctx, b"k").await.is_none());
        // ...but the fresh tombstone is retained, not swept this cycle.
        assert!(!is_gone(&ctx.tl, &t).await);
    }

    // An aborted object still within its tombstone lease keeps its locks and is
    // retained, so a stuck owner cannot be resurrected or stranded.
    #[tokio::test(start_paused = true)]
    async fn recent_aborted_tombstone_is_kept() {
        let ctx = new_ctx().await;
        let t = tx(1);
        let mut log = TxLog::new(t.clone(), TxCommitStatus::Aborted);
        log.timestamp = Some(base());
        log.locks = vec![write_lock(b"k")];
        ctx.tl.set(&log).await.unwrap();
        store_entry(&ctx, b"k", locked_entry(b"k", &t)).await;
        ctx.gc.schedule_tx_cleanup(t.clone());

        run_once(&ctx.gc).await;

        assert!(!is_gone(&ctx.tl, &t).await);
        let e = lookup_entry(&ctx, b"k").await.unwrap();
        assert_eq!(e.locked_by, vec![t]);
    }

    // An aborted object past its tombstone lease has its recorded lock pruned
    // (the vestigial entry removed) and is then deleted.
    #[tokio::test(start_paused = true)]
    async fn expired_aborted_prunes_locks_and_is_deleted() {
        let ctx = new_ctx().await;
        let t = tx(1);
        let mut log = TxLog::new(t.clone(), TxCommitStatus::Aborted);
        log.timestamp = Some(base() - PAST_HORIZON);
        log.locks = vec![write_lock(b"k")];
        ctx.tl.set(&log).await.unwrap();
        store_entry(&ctx, b"k", locked_entry(b"k", &t)).await;
        ctx.gc.schedule_tx_cleanup(t.clone());

        run_once(&ctx.gc).await;

        assert!(is_gone(&ctx.tl, &t).await);
        assert!(lookup_entry(&ctx, b"k").await.is_none());
    }

    // A candidate with no object at all is a harmless no-op.
    #[tokio::test(start_paused = true)]
    async fn missing_candidate_is_noop() {
        let ctx = new_ctx().await;
        let t = tx(9);
        ctx.gc.schedule_tx_cleanup(t.clone());
        run_once(&ctx.gc).await;
        assert!(is_gone(&ctx.tl, &t).await);
    }

    // Sparse namespaces must not turn one GC cycle into thousands of provider
    // calls merely because most deterministic shards are empty.
    #[tokio::test(start_paused = true)]
    async fn sparse_scan_obeys_request_budget() {
        let rec = Arc::new(RecordingBackend::new(Arc::new(MemoryBackend::new())));
        let log = rec.log();
        let ctx = new_ctx_with(rec).await;
        let mut scan = test_scan((0..paths::TRANSACTION_SHARD_COUNT).collect(), None);

        assert!(ctx.gc.next_list_page(&mut scan).await.is_empty());

        let lists = log
            .lock()
            .unwrap()
            .iter()
            .filter(|record| record.op == "list")
            .count();
        assert_eq!(lists, GC_LIST_REQUEST_BUDGET);
        assert_eq!(scan.current, GC_LIST_REQUEST_BUDGET);
    }

    // Provider continuation tokens are opaque and may expire. GC restarts the
    // affected shard instead of abandoning the pass or discarding its order.
    #[tokio::test(start_paused = true)]
    async fn invalid_cursor_restarts_current_shard() {
        let ctx = new_ctx().await;
        let t = tx(1);
        ctx.tl
            .set(&committed(t.clone(), PAST_HORIZON, &[], &[]))
            .await
            .unwrap();
        let mut scan = test_scan(
            vec![paths::transaction_shard(&t)],
            Some(backend::ListCursor::new("stale")),
        );

        ctx.gc.run_once(&mut scan).await;

        assert!(is_gone(&ctx.tl, &t).await);
    }

    // ADR-029: GC's lock reclamation flows through the shard-mutation coordinator
    // (via the locker's unlock methods), so a GC release and a live disjoint-key
    // acquire contending one shard batch into a *single* CAS round instead of GC
    // racing its own store. The release clears (and prunes) the dead holder's
    // entry and the acquire installs its lock, all in one shard write.
    #[tokio::test(start_paused = true)]
    async fn gc_release_merges_into_live_acquire_round() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (backend, gate) = Gate::wrap(mem);
        let rec = Arc::new(RecordingBackend::new(backend));
        let log = rec.log();
        let ctx = new_ctx_with(rec).await;

        let ka = b"key-a".to_vec();
        let kb = same_shard_sibling(&ka);
        let shard_path = paths::collection_info(COLL);

        // Seed both entries in the one shared leaf `_i`: a dead transaction holds
        // A's write lock (no committed writer), so GC's release will clear and
        // prune the now-vestigial entry; B exists committed, so the live
        // overwrite takes a Write lock (not a Create) and needs no membership
        // root lock — the round stays one leaf CAS.
        let dead = tx(1);
        let seed = tx(9);
        let shard = Shard::from_entries([locked_entry(&ka, &dead), writer_entry(&kb, &seed)]);
        let loaded = ctx
            .shards
            .load_leaf(&shard_path, Requirement::AtLeast(ctx.timeline.now()))
            .await
            .unwrap();
        assert!(
            ctx.shards
                .store_leaf(
                    &shard_path,
                    &shard,
                    &loaded.locks,
                    loaded.kind(),
                    &loaded.observation,
                )
                .await
                .unwrap()
        );

        let live = TxId::with_priority(2_000_000_000, b"live");
        ctx.mon.begin_tx(&live);

        let before = count_stores(&log, &shard_path);
        gate.arm();

        // Drive GC's release and the live acquire concurrently: the first becomes
        // the dedup driver and parks in the gated load; the second queues and
        // merges into its round.
        let gc = ctx.gc.clone();
        let dead2 = dead.clone();
        let dead_locks = vec![write_lock(&ka)];
        let release = tokio::spawn(async move {
            gc.release_locks(&dead2, &dead_locks, Requirement::Any)
                .await
        });
        let locker = ctx.locker.clone();
        let data = crate::algo::Data {
            reads: Vec::new(),
            writes: vec![crate::algo::WriteAccess::put(
                key_path(&kb),
                Arc::from(&b"v2"[..]),
            )],
            scans: Vec::new(),
        };
        let live2 = live.clone();
        let lock_requirement = Requirement::AtLeast(ctx.timeline.now());
        let acquire =
            tokio::spawn(
                async move { locker.lock_at(&live2, &data, false, lock_requirement).await },
            );

        // Under paused time this fires only once both tasks are parked (driver in
        // the gated load, the second queued); then release the load.
        rt::sleep(std::time::Duration::from_millis(50)).await;
        gate.release();

        release.await.unwrap().unwrap();
        let outcome = acquire.await.unwrap().unwrap();
        assert!(
            matches!(outcome, LockOutcome::Locked(_)),
            "the live acquire must lock"
        );

        assert_eq!(
            count_stores(&log, &shard_path) - before,
            1,
            "GC release and the live acquire share a single shard CAS"
        );
        // The dead holder's entry was cleared and, being vestigial, pruned; the
        // live acquirer holds B's lock.
        assert!(
            lookup_entry(&ctx, &ka).await.is_none(),
            "GC released and pruned the dead holder's entry"
        );
        assert_eq!(lookup_entry(&ctx, &kb).await.unwrap().locked_by, vec![live]);
    }

    /// Counts the CAS stores (conditional write / create) issued against `path`.
    fn count_stores(log: &glassdb_backend::middleware::OpLog, path: &str) -> usize {
        log.lock()
            .unwrap()
            .iter()
            .filter(|r| r.path == path && (r.op == "write_if" || r.op == "write_if_not_exists"))
            .count()
    }

    /// A distinct key that shares the collection's single leaf `_i` with `base`
    /// (ADR-031, split deferred), for exercising a GC release and a live acquire
    /// contending one leaf object.
    fn same_shard_sibling(base: &[u8]) -> Vec<u8> {
        let sib = b"sibling".to_vec();
        assert_ne!(sib, base, "sibling must differ from the base key");
        sib
    }

    /// Controls a hook that skips one routing read, then gates the coordinator load.
    struct Gate {
        notify: Arc<tokio::sync::Notify>,
        armed: std::sync::atomic::AtomicBool,
        skip: std::sync::atomic::AtomicUsize,
    }

    impl Gate {
        fn wrap(inner: Arc<dyn Backend>) -> (Arc<HookBackend>, Arc<Self>) {
            let gate = Arc::new(Self {
                notify: Arc::new(tokio::sync::Notify::new()),
                armed: std::sync::atomic::AtomicBool::new(false),
                skip: std::sync::atomic::AtomicUsize::new(0),
            });
            let backend = HookBackend::new(inner);
            backend.set_before({
                let gate = gate.clone();
                move |op| {
                    use std::sync::atomic::Ordering::SeqCst;
                    let wait = matches!(
                        op,
                        BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                    ) && gate.armed.load(SeqCst)
                        && gate
                            .skip
                            .fetch_update(SeqCst, SeqCst, |n| n.checked_sub(1))
                            .is_err();
                    if wait {
                        gate.armed.store(false, SeqCst);
                    }
                    let notify = gate.notify.clone();
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

        fn arm(&self) {
            self.skip.store(1, std::sync::atomic::Ordering::SeqCst);
            self.armed.store(true, std::sync::atomic::Ordering::SeqCst);
        }

        fn release(&self) {
            self.notify.notify_one();
        }
    }
}
