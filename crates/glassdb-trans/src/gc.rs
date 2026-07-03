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

use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use std::time::UNIX_EPOCH;

use glassdb_backend as backend;
use glassdb_concurr::{Background, Clock, RetryConfig, rt};
use glassdb_data::shard::group_by_owning_shard;
use glassdb_data::{TxId, paths};
use glassdb_storage::{
    LockType, PathLock, Shard, ShardEntry, ShardStore, StorageError, TLogger, TxCommitStatus, TxLog,
};

use crate::error::TransError;
use crate::monitor::{Monitor, PENDING_TX_TIMEOUT, is_expired};

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

/// Bounded CAS retries when releasing a candidate's locks from a contended
/// shard/root before giving up (a later cycle re-attempts).
const GC_CAS_RETRIES: usize = 10;

/// Garbage collector for finalized transaction objects (ADR-022).
#[derive(Clone)]
pub struct Gc {
    // Weak so a `Gc` clone captured inside the spawned sweep loop does not keep
    // the [`Background`] alive across DB shutdown; the single strong owner is
    // `DbInner::background`.
    bg: Weak<Background>,
    tl: TLogger,
    shards: ShardStore,
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
    /// Creates a collector over the transaction log, shard store, and monitor,
    /// timed off `clock` so its horizon is deterministic under the DST executor.
    pub fn new(
        bg: Weak<Background>,
        tl: TLogger,
        shards: ShardStore,
        mon: Monitor,
        clock: Clock,
    ) -> Self {
        Gc {
            bg,
            tl,
            shards,
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
    /// The recorded keys are routed to their shards by [`ShardStore::load_by_keys`]
    /// so each touched shard is fetched once (concurrently) — a write and its
    /// write-lock name the same key, and sibling keys share a shard, so a
    /// per-key load would re-read the same shard several times per candidate.
    /// Each key carries the [`CheckKind`] that says which field to inspect.
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

        for loaded in self.shards.load_by_keys(items).await? {
            for (raw_key, kind) in &loaded.keys {
                let referenced = match kind {
                    CheckKind::Writer => {
                        loaded
                            .shard
                            .lookup(raw_key)
                            .and_then(|e| e.current_writer.as_ref())
                            == Some(tid)
                    }
                    CheckKind::Holder => loaded
                        .shard
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

    /// Releases `tid` from every shard and root its recorded `locks` name,
    /// grouping the paths so each shard/root is visited once (targeted pruning,
    /// never a whole-shard-space scan). `current_writer` is never touched.
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
        let shards = group_by_owning_shard(key_locks)
            .map_err(|e| TransError::with_source("grouping locks by shard", e))?;
        for (prefix, idx) in shards.into_keys() {
            self.release_shard_holder(&prefix, idx, tid).await?;
        }
        for prefix in roots {
            self.release_root_holder(&prefix, tid).await?;
        }
        Ok(())
    }

    /// Clears `tid` from the `locked_by` of every entry in shard `idx`, and drops
    /// any entry it thereby leaves vestigial (no lock, no `current_writer`). One
    /// idempotent CAS, retried on contention or an in-doubt store (re-clearing an
    /// already-absent holder is a no-op, inheriting ADR-009 parity).
    async fn release_shard_holder(
        &self,
        prefix: &str,
        idx: u32,
        tid: &TxId,
    ) -> Result<(), TransError> {
        let mut backoff = RetryConfig::default().backoff();
        for attempt in 0..GC_CAS_RETRIES {
            if attempt > 0 {
                rt::sleep(backoff.next_delay()).await;
            }
            let (shard, ver) = self.shards.load_shard(prefix, idx).await?;
            let Some(ver) = ver else {
                // No shard object: nothing to release.
                return Ok(());
            };
            let mut changed = false;
            let mut kept: Vec<ShardEntry> = Vec::new();
            for mut e in shard.entries().cloned() {
                if e.locked_by.contains(tid) {
                    e.locked_by.retain(|h| h != tid);
                    if e.locked_by.is_empty() {
                        e.lock_type = LockType::None;
                    }
                    changed = true;
                    if is_vestigial(&e) {
                        continue;
                    }
                }
                kept.push(e);
            }
            if !changed {
                return Ok(());
            }
            let new_shard = Shard::from_entries(kept);
            match self
                .shards
                .store_shard(prefix, idx, &new_shard, Some(&ver))
                .await
            {
                Ok(true) => return Ok(()),
                // Precondition (shard changed under us) or in-doubt: reload and
                // retry; clearing our own lock is idempotent.
                Ok(false) => {}
                Err(StorageError::Unavailable(_)) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    /// Clears `tid` from the collection root's membership lock, if held. One
    /// idempotent CAS, retried on contention.
    async fn release_root_holder(&self, prefix: &str, tid: &TxId) -> Result<(), TransError> {
        let mut backoff = RetryConfig::default().backoff();
        for attempt in 0..GC_CAS_RETRIES {
            if attempt > 0 {
                rt::sleep(backoff.next_delay()).await;
            }
            let (mut root, ver) = match self.shards.load_root(prefix).await {
                Ok(rv) => rv,
                Err(StorageError::NotFound) => return Ok(()),
                Err(e) => return Err(e.into()),
            };
            if !root.membership_locked_by().contains(tid) {
                return Ok(());
            }
            root.clear_membership_lock();
            match self.shards.store_root(prefix, &root, &ver).await {
                Ok(true) => return Ok(()),
                Ok(false) => {}
                Err(StorageError::Unavailable(_)) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

/// A shard entry left with nothing to record: no holders and no committed
/// writer (not even a tombstone, which always keeps a `current_writer`). GC
/// drops such entries when it clears the last lock off them.
fn is_vestigial(e: &ShardEntry) -> bool {
    e.locked_by.is_empty() && e.current_writer.is_none()
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
    use glassdb_backend::{Backend, memory::MemoryBackend};
    use glassdb_data::shard::shard_index;
    use glassdb_storage::{ObjectCache, SharedCache, TxWrite, ValueCache};
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
        _bg: Arc<Background>,
    }

    fn new_ctx() -> Ctx {
        let cache = SharedCache::new(1 << 20);
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
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
        let gc = Gc::new(Arc::downgrade(&bg), tl.clone(), shards.clone(), mon, clock);
        Ctx {
            gc,
            tl,
            shards,
            _bg: bg,
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

    async fn store_entry(shards: &ShardStore, key: &[u8], entry: ShardEntry) {
        let shard = Shard::from_entries([entry]);
        assert!(
            shards
                .store_shard(COLL, shard_index(key), &shard, None)
                .await
                .unwrap()
        );
    }

    async fn lookup_entry(shards: &ShardStore, key: &[u8]) -> Option<ShardEntry> {
        let (shard, _) = shards.load_shard(COLL, shard_index(key)).await.unwrap();
        shard.lookup(key).cloned()
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
        let mut root = glassdb_storage::CollectionRoot::new(1024);
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
}
