//! Garbage collection of finalized transaction objects by candidate-driven
//! reverse mark-sweep (ADR-022).
//!
//! In the v2 object-native layout a committed transaction object *is* the value
//! store: a key's live value lives in the object its shard entry's
//! `current_writer` points at, and readers help-forward through it. So a
//! transaction object is **live** exactly while some shard or root still
//! references its txid (`current_writer`, `locked_by`, or the root's
//! `membership_locked_by`), and GC is a reachability problem, not a timer.
//!
//! A forward mark (list every shard, union the referenced txids) costs the whole
//! database per cycle. Instead each candidate `_t/` object records its own
//! back-references (its `locks ∪ writes`), so GC works **backward**: it reads a
//! batch of candidates and confirms each one dead by GET-ing only the handful of
//! shards/root it names — never a database-wide scan. Candidates come from the
//! write-back hint ([`Gc::schedule_tx_cleanup`], the `current_writer` a fresh
//! commit just superseded) and a paged walk of the flat `{db}/_t/` directory
//! (which makes the candidate set complete regardless of lost hints).
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
use glassdb_data::{TxId, paths};
use glassdb_storage::{
    Directory, Freshness, PathLock, ShardStore, StorageError, TLogger, TxCommitStatus, TxLog,
};

use crate::error::TransError;
use crate::monitor::{Monitor, PENDING_TX_TIMEOUT, is_expired};
use crate::tlocker::Locker;

/// How often the collector runs a sweep cycle. Reuses the lease timeout: a
/// candidate cannot become collectable faster than one lease anyway, so a
/// tighter cadence would only re-GET still-referenced objects.
const GC_INTERVAL: std::time::Duration = PENDING_TX_TIMEOUT;

/// Maximum number of `_t/` objects visited from the paged list per cycle, so a
/// large store is walked incrementally instead of all at once.
const GC_LIST_PAGE: usize = 128;

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
    shards: ShardStore,
    dir: Directory,
    locker: Locker,
    mon: Monitor,
    clock: Clock,
    // Write-back hint feed: txids a fresh commit just superseded (primary
    // candidate source). Deduplicated when drained.
    hints: Arc<Mutex<VecDeque<TxId>>>,
    // Paged `_t/` list cursor: the last txid visited, so successive cycles walk
    // the flat transaction directory a page at a time and wrap around.
    cursor: Arc<Mutex<Option<TxId>>>,
}

impl Gc {
    /// Creates a collector over the transaction log, shard store, locker, and
    /// monitor, timed off `clock` so its horizon is deterministic under the DST
    /// executor.
    pub fn new(
        bg: Weak<Background>,
        tl: TLogger,
        shards: ShardStore,
        locker: Locker,
        mon: Monitor,
        clock: Clock,
    ) -> Self {
        let dir = Directory::new(shards.clone());
        Gc {
            bg,
            tl,
            shards,
            dir,
            locker,
            mon,
            clock,
            hints: Arc::new(Mutex::new(VecDeque::new())),
            cursor: Arc::new(Mutex::new(None)),
        }
    }

    /// Starts the background sweep loop on the [`Background`] executor. It runs
    /// one cycle every [`GC_INTERVAL`] until the executor is dropped. If the
    /// executor is already gone (DB shut down) nothing is started.
    pub fn start(&self) {
        let Some(bg) = self.bg.upgrade() else {
            return;
        };
        let gc = self.clone();
        bg.spawn(async move {
            loop {
                rt::sleep(GC_INTERVAL).await;
                gc.run_once().await;
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

    /// Runs a single sweep cycle: the buffered hints plus one page of the `_t/`
    /// list, each checked by the reverse liveness check. Best-effort — a
    /// transient error on one candidate only delays its delete to a later cycle,
    /// so it is logged and the cycle continues.
    async fn run_once(&self) {
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
        for tid in self.next_list_page().await {
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

    /// Returns the next page of the flat `{db}/_t/` directory, advancing the
    /// paging cursor and wrapping to the start once the tail is consumed. The
    /// list is sorted so paging is stable and deterministic under the DST
    /// executor.
    async fn next_list_page(&self) -> Vec<TxId> {
        let mut all = match self.tl.list_transaction_ids().await {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!(error = %e, "gc list of transaction objects failed");
                return Vec::new();
            }
        };
        if all.is_empty() {
            *self.cursor.lock().unwrap() = None;
            return Vec::new();
        }
        all.sort();

        let start = match self.cursor.lock().unwrap().clone() {
            Some(c) => all.iter().position(|t| *t > c).unwrap_or(0),
            None => 0,
        };
        let page: Vec<TxId> = all.iter().skip(start).take(GC_LIST_PAGE).cloned().collect();
        // Wrap when this page consumed the tail; otherwise resume after it.
        *self.cursor.lock().unwrap() = if start + page.len() >= all.len() {
            None
        } else {
            page.last().cloned()
        };
        page
    }

    /// The reverse liveness check for one candidate (ADR-022): read it, keep it
    /// if within the safety horizon, else resolve by status.
    async fn check_candidate(&self, tid: &TxId) -> Result<(), TransError> {
        let (log, version) = match self.tl.get(tid).await {
            Ok(v) => v,
            // Already reclaimed (or never existed): nothing to do.
            Err(StorageError::NotFound) => return Ok(()),
            Err(e) => return Err(e.into()),
        };

        // Within the horizon: keep. A recent pending object may be a live
        // transaction whose lock this non-atomic check has not observed yet
        // (ADR-024 materializes the object lazily, after the locks are taken);
        // a recent committed/aborted one is left for a later cycle.
        let ts = log.timestamp.unwrap_or(UNIX_EPOCH);
        if !is_expired(ts, self.clock.now()) {
            return Ok(());
        }

        match log.status {
            TxCommitStatus::Ok => self.reclaim_committed(tid, &log).await,
            TxCommitStatus::Aborted => self.reclaim_aborted(tid, &log.locks).await,
            TxCommitStatus::Pending => self.reclaim_dead_pending(tid, &log, &version).await,
            TxCommitStatus::Unknown => Ok(()),
        }
    }

    /// A committed candidate past the horizon is deleted only once its complete
    /// record proves it unreferenced. Any recorded write still pointed at by a
    /// `current_writer`, or any recorded lock still held (the commit→write-back
    /// gap) or root membership still held, keeps it. A committed object is never
    /// pruned or force-aborted — its locks become `current_writer` through
    /// write-back, never through GC.
    async fn reclaim_committed(&self, tid: &TxId, log: &TxLog) -> Result<(), TransError> {
        if self.still_referenced(tid, log).await? {
            return Ok(());
        }
        self.tl.delete(tid).await?;
        Ok(())
    }

    /// An aborted candidate holds no value, and past the horizon its `timestamp`
    /// (the abort instant) proves the tombstone has outlived any client that
    /// could still act under its txid. Release its recorded locks (pruning any
    /// entry left vestigial) and delete it. A minimal abort with no recorded
    /// locks simply has nothing to release.
    async fn reclaim_aborted(&self, tid: &TxId, locks: &[PathLock]) -> Result<(), TransError> {
        self.release_locks(tid, locks).await?;
        self.tl.delete(tid).await?;
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
        version: &backend::Version,
    ) -> Result<(), TransError> {
        match self.mon.force_abort(tid, version).await? {
            TxCommitStatus::Aborted => self.release_locks(tid, &log.locks).await,
            // Committed or refreshed first: it was alive. Leave it.
            _ => Ok(()),
        }
    }

    /// Reports whether any entry the candidate recorded still names its txid: a
    /// written key's `current_writer`, a locked key's `locked_by`, or the root's
    /// `membership_locked_by`. Checking only the recorded set is equivalent to
    /// scanning every shard, because an entry can name `txid` only if `txid` put
    /// it there.
    ///
    /// The recorded keys are routed to their leaves by descent
    /// ([`Directory::group_keys_by_leaf`]) so each touched leaf is fetched once —
    /// a write and its write-lock name the same key, and sibling keys share a
    /// leaf, so a per-key load would re-read the same leaf several times per
    /// candidate. Each key carries the [`CheckKind`] that says which field to
    /// inspect.
    async fn still_referenced(&self, tid: &TxId, log: &TxLog) -> Result<bool, TransError> {
        let mut items: Vec<(&str, CheckKind)> = log
            .writes
            .iter()
            .map(|w| (w.path.as_str(), CheckKind::Writer))
            .collect();
        let mut roots: BTreeSet<String> = BTreeSet::new();
        for l in &log.locks {
            let Ok(pr) = paths::parse(&l.path) else {
                continue;
            };
            match pr.typ {
                paths::Type::CollectionInfo => {
                    roots.insert(pr.prefix);
                }
                paths::Type::Key => items.push((l.path.as_str(), CheckKind::Holder)),
                _ => {}
            }
        }

        for group in self
            .dir
            .group_keys_by_leaf(items, Freshness::Latest)
            .await?
        {
            let Some(leaf) = group.node.as_leaf() else {
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

        for prefix in roots {
            match self.shards.load_root(&prefix).await {
                Ok((root, _)) if root.membership_locked_by().contains(tid) => return Ok(true),
                Ok(_) | Err(StorageError::NotFound) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(false)
    }

    /// Releases `tid` from every leaf and root its recorded `locks` name,
    /// grouping the paths by descent so each leaf/root is visited once (targeted
    /// pruning, never a whole-key-space scan). Each release flows through the
    /// locker's coordinator-backed unlock methods (ADR-029) — one deduplicated
    /// fold round per object that clears `tid` and drops any entry it thereby
    /// leaves vestigial — so GC issues no leaf/root CAS of its own.
    /// `current_writer` is never touched.
    async fn release_locks(&self, tid: &TxId, locks: &[PathLock]) -> Result<(), TransError> {
        let mut key_locks: Vec<(&str, ())> = Vec::new();
        let mut roots: BTreeSet<String> = BTreeSet::new();
        for l in locks {
            let Ok(pr) = paths::parse(&l.path) else {
                continue;
            };
            match pr.typ {
                paths::Type::CollectionInfo => {
                    roots.insert(pr.prefix);
                }
                paths::Type::Key => key_locks.push((l.path.as_str(), ())),
                _ => {}
            }
        }
        for group in self
            .dir
            .group_keys_by_leaf(key_locks, Freshness::Latest)
            .await?
        {
            self.locker.release_leaf(tid, &group.path).await?;
        }
        for prefix in roots {
            self.locker.release_root(&prefix, tid).await?;
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
    use glassdb_backend::middleware::RecordingBackend;
    use glassdb_backend::{Backend, memory::MemoryBackend};
    use glassdb_concurr::RetryConfig;
    use glassdb_storage::{
        Directory, Freshness, LockType, ObjectCache, Shard, ShardEntry, SharedCache, TxWrite,
        ValueCache,
    };
    use std::collections::BTreeMap;
    use std::time::{Duration, SystemTime};

    const COLL: &str = "db/coll";

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
        locker: Locker,
        mon: Monitor,
    }

    fn new_ctx() -> Ctx {
        new_ctx_with(Arc::new(MemoryBackend::new()))
    }

    fn new_ctx_with(backend: Arc<dyn Backend>) -> Ctx {
        let cache = SharedCache::new(1 << 20);
        let values = ValueCache::new(&cache);
        let objects = ObjectCache::new(backend, &cache);
        let tl = TLogger::new(objects.clone(), "db");
        let shards = ShardStore::new(objects);
        let bg = Arc::new(Background::new());
        let clock = Clock::anchored_at(base());
        let mon = Monitor::with_config(
            values,
            tl.clone(),
            Arc::downgrade(&bg),
            clock.clone(),
            RetryConfig::default(),
        );
        let resolver = Resolver::new(shards.clone(), mon.clone());
        let dir = Directory::new(shards.clone());
        let coord = ShardCoordinator::new(
            shards.clone(),
            resolver,
            mon.clone(),
            RetryConfig::default(),
        );
        let locker = Locker::new(coord, dir, mon.clone(), RetryConfig::default());
        let gc = Gc::new(
            Arc::downgrade(&bg),
            tl.clone(),
            shards.clone(),
            locker.clone(),
            mon.clone(),
            clock,
        );
        Ctx {
            gc,
            tl,
            shards,
            locker,
            mon,
        }
    }

    fn tx(n: u8) -> TxId {
        TxId::from_bytes(vec![n])
    }

    fn key_path(k: &[u8]) -> String {
        paths::from_key(COLL, k)
    }

    fn write_lock(k: &[u8]) -> PathLock {
        PathLock {
            path: key_path(k),
            typ: LockType::Write,
        }
    }

    async fn store_entry(shards: &ShardStore, _key: &[u8], entry: ShardEntry) {
        let path = paths::collection_info(COLL);
        let loaded = shards.load_leaf(&path, Freshness::Latest).await.unwrap();
        let mut entries: BTreeMap<Vec<u8>, ShardEntry> = loaded
            .entries
            .entries()
            .cloned()
            .map(|e| (e.key.clone(), e))
            .collect();
        entries.insert(entry.key.clone(), entry);
        let shard = Shard::from_entries(entries.into_values());
        assert!(
            shards
                .store_leaf(&path, &shard, loaded.kind(), loaded.version.as_ref())
                .await
                .unwrap()
        );
    }

    async fn lookup_entry(shards: &ShardStore, key: &[u8]) -> Option<ShardEntry> {
        let loaded = shards
            .load_leaf(&paths::collection_info(COLL), Freshness::Latest)
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
                    path: key_path(k),
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
        matches!(tl.get(id).await, Err(StorageError::NotFound))
    }

    // A committed object whose written key has since been overwritten by a newer
    // writer holds no reference and is swept. Fed via the paged list (no hint),
    // so the list feed is exercised too.
    #[tokio::test(start_paused = true)]
    async fn committed_unreferenced_is_collected() {
        let ctx = new_ctx();
        let (old, new) = (tx(1), tx(2));
        ctx.tl
            .set(&committed(old.clone(), PAST_HORIZON, &[b"k"], &[]))
            .await
            .unwrap();
        // The key now points at a newer writer, not `old`.
        store_entry(&ctx.shards, b"k", writer_entry(b"k", &new)).await;

        ctx.gc.run_once().await;

        assert!(is_gone(&ctx.tl, &old).await);
    }

    // A committed object still named by its key's `current_writer` is the live
    // value: it must never be collected.
    #[tokio::test(start_paused = true)]
    async fn committed_still_referenced_is_kept() {
        let ctx = new_ctx();
        let t = tx(1);
        ctx.tl
            .set(&committed(t.clone(), PAST_HORIZON, &[b"k"], &[]))
            .await
            .unwrap();
        store_entry(&ctx.shards, b"k", writer_entry(b"k", &t)).await;

        ctx.gc.run_once().await;

        let (log, _) = ctx.tl.get(&t).await.unwrap();
        assert_eq!(log.status, TxCommitStatus::Ok);
    }

    // A recent pending object (within the safety horizon) is kept: it may be a
    // live transaction whose lock this non-atomic check cannot yet rule out.
    #[tokio::test(start_paused = true)]
    async fn recent_pending_is_kept() {
        let ctx = new_ctx();
        let t = tx(1);
        let mut log = TxLog::new(t.clone(), TxCommitStatus::Pending);
        log.timestamp = Some(base());
        log.locks = vec![write_lock(b"k")];
        ctx.tl.set(&log).await.unwrap();
        store_entry(&ctx.shards, b"k", locked_entry(b"k", &t)).await;

        ctx.gc.run_once().await;

        let (got, _) = ctx.tl.get(&t).await.unwrap();
        assert_eq!(got.status, TxCommitStatus::Pending);
        // Its lock is untouched.
        let e = lookup_entry(&ctx.shards, b"k").await.unwrap();
        assert_eq!(e.locked_by, vec![t]);
    }

    // A dead pending object past the horizon is force-aborted (its death made
    // durable) and its locks released, but it is retained as a fresh tombstone —
    // never deleted in the same cycle it is aborted. Fed via the write-back hint.
    #[tokio::test(start_paused = true)]
    async fn dead_pending_is_force_aborted_and_locks_released() {
        let ctx = new_ctx();
        let t = tx(1);
        let mut log = TxLog::new(t.clone(), TxCommitStatus::Pending);
        log.timestamp = Some(base());
        log.locks = vec![write_lock(b"k")];
        ctx.tl.set(&log).await.unwrap();
        store_entry(&ctx.shards, b"k", locked_entry(b"k", &t)).await;

        tokio::time::sleep(PAST_HORIZON).await;
        ctx.gc.schedule_tx_cleanup(t.clone());
        ctx.gc.run_once().await;

        // Death is durable...
        let (got, _) = ctx.tl.get(&t).await.unwrap();
        assert_eq!(got.status, TxCommitStatus::Aborted);
        // ...its lock is released (the now-vestigial entry pruned)...
        assert!(lookup_entry(&ctx.shards, b"k").await.is_none());
        // ...but the fresh tombstone is retained, not swept this cycle.
        assert!(!is_gone(&ctx.tl, &t).await);
    }

    // An aborted object still within its tombstone lease keeps its locks and is
    // retained, so a stuck owner cannot be resurrected or stranded.
    #[tokio::test(start_paused = true)]
    async fn recent_aborted_tombstone_is_kept() {
        let ctx = new_ctx();
        let t = tx(1);
        let mut log = TxLog::new(t.clone(), TxCommitStatus::Aborted);
        log.timestamp = Some(base());
        log.locks = vec![write_lock(b"k")];
        ctx.tl.set(&log).await.unwrap();
        store_entry(&ctx.shards, b"k", locked_entry(b"k", &t)).await;

        ctx.gc.run_once().await;

        assert!(!is_gone(&ctx.tl, &t).await);
        let e = lookup_entry(&ctx.shards, b"k").await.unwrap();
        assert_eq!(e.locked_by, vec![t]);
    }

    // An aborted object past its tombstone lease has its recorded lock pruned
    // (the vestigial entry removed) and is then deleted.
    #[tokio::test(start_paused = true)]
    async fn expired_aborted_prunes_locks_and_is_deleted() {
        let ctx = new_ctx();
        let t = tx(1);
        let mut log = TxLog::new(t.clone(), TxCommitStatus::Aborted);
        log.timestamp = Some(base() - PAST_HORIZON);
        log.locks = vec![write_lock(b"k")];
        ctx.tl.set(&log).await.unwrap();
        store_entry(&ctx.shards, b"k", locked_entry(b"k", &t)).await;

        ctx.gc.run_once().await;

        assert!(is_gone(&ctx.tl, &t).await);
        assert!(lookup_entry(&ctx.shards, b"k").await.is_none());
    }

    // An aborted object holding a membership (root) lock has it released too.
    #[tokio::test(start_paused = true)]
    async fn expired_aborted_releases_root_membership_lock() {
        let ctx = new_ctx();
        let t = tx(1);
        let mut root = glassdb_storage::CollectionRoot::new();
        root.set_membership_lock(LockType::Write, [t.clone()]);
        ctx.shards.create_root(COLL, &root).await.unwrap();

        let mut log = TxLog::new(t.clone(), TxCommitStatus::Aborted);
        log.timestamp = Some(base() - PAST_HORIZON);
        log.locks = vec![PathLock {
            path: paths::collection_info(COLL),
            typ: LockType::Write,
        }];
        ctx.tl.set(&log).await.unwrap();

        ctx.gc.run_once().await;

        assert!(is_gone(&ctx.tl, &t).await);
        let (root, _) = ctx.shards.load_root(COLL).await.unwrap();
        assert!(root.membership_locked_by().is_empty());
    }

    // A candidate with no object at all is a harmless no-op.
    #[tokio::test(start_paused = true)]
    async fn missing_candidate_is_noop() {
        let ctx = new_ctx();
        let t = tx(9);
        ctx.gc.schedule_tx_cleanup(t.clone());
        ctx.gc.run_once().await;
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
        let gate = GateBackend::new(mem);
        let rec = Arc::new(RecordingBackend::new(gate.clone() as Arc<dyn Backend>));
        let log = rec.log();
        let ctx = new_ctx_with(rec);

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
            .load_leaf(&shard_path, Freshness::Latest)
            .await
            .unwrap();
        assert!(
            ctx.shards
                .store_leaf(&shard_path, &shard, loaded.kind(), loaded.version.as_ref())
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
        let release = tokio::spawn(async move { gc.release_locks(&dead2, &dead_locks).await });
        let locker = ctx.locker.clone();
        let data = crate::algo::Data {
            reads: Vec::new(),
            writes: vec![crate::algo::WriteAccess::put(
                key_path(&kb).into(),
                Arc::from(&b"v2"[..]),
            )],
        };
        let live2 = live.clone();
        let acquire = tokio::spawn(async move { locker.lock(&live2, &data, false).await });

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
            lookup_entry(&ctx.shards, &ka).await.is_none(),
            "GC released and pruned the dead holder's entry"
        );
        assert_eq!(
            lookup_entry(&ctx.shards, &kb).await.unwrap().locked_by,
            vec![live]
        );
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

    /// Test backend that, while **armed**, parks the dedup driver's coordination
    /// leaf load on a gate until released — so a test can hold the driver
    /// mid-load while a second contender queues, forcing them into one merged CAS
    /// round. The first `skip` reads after arming pass through: descent reads the
    /// collection root `_i` to route a key before the coordinator loads the leaf
    /// (ADR-031), so the gate skips that routing read and parks the coordinator's
    /// load instead. Arming is deferred so un-gated setup runs first.
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
}
