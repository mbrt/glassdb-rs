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

use glassdb_concurr::{Background, Backoff, RetryConfig, rt};
use glassdb_data::TxId;
use glassdb_storage::{Global, Local, TxCommitStatus, TxLog, TxWrite, Version};

use crate::error::TransError;
use crate::gc::Gc;
use crate::monitor::Monitor;
use crate::reader::Reader;
use crate::tlocker::Locker;

/// Number of failed parallel-locking attempts before a transaction escalates to
/// the serial sorted-locking fallback (ADR-020). The parallel path is fast but
/// can *livelock* two equal-priority transactions that each grab a different
/// shard first; after this many failures the transaction switches to sorted
/// acquisition, where first-CAS-wins on the lowest contended shard guarantees
/// one of them makes progress.
const SERIAL_FALLBACK_AFTER: usize = 3;

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
    /// Converts to a storage version (writer-only; the backend version is unused
    /// in v2 since shards carry no per-key backend version).
    pub fn to_storage_version(&self) -> Version {
        Version {
            b: glassdb_backend::Version::default(),
            writer: self.last_writer.clone(),
        }
    }
}

/// A single key write within a transaction.
#[derive(Debug, Clone)]
pub struct WriteAccess {
    pub path: Arc<str>,
    pub op: WriteOp,
}

/// The write operation staged for a key.
#[derive(Debug, Clone)]
pub enum WriteOp {
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
    /// Per-transaction backoff, advanced before each conflict restart so a
    /// transaction waiting on a (possibly dead) holder yields virtual time for
    /// the holder's lease to expire instead of busy-looping.
    backoff: Backoff,
}

impl Handle {
    /// The transaction's ID.
    pub fn id(&self) -> &TxId {
        &self.id
    }
}

/// Coordinates transactions: read validation, locking, commit, and write-back.
#[derive(Clone)]
pub struct Algo {
    local: Local,
    reader: Reader,
    locker: Locker,
    mon: Monitor,
    gc: Gc,
    // Weak so a captured `Algo` clone inside a spawned async-abort task does not
    // keep [`Background`] alive past DB shutdown.
    background: Option<Weak<Background>>,
}

impl Algo {
    /// Creates an algorithm coordinator. `global` is unused directly (the reader
    /// and locker own their own storage handles) but kept in the signature for
    /// call-site stability.
    pub fn new(
        _global: Global,
        local: Local,
        locker: Locker,
        mon: Monitor,
        gc: Gc,
        background: Option<Weak<Background>>,
        reader: Reader,
    ) -> Self {
        Algo {
            local,
            reader,
            locker,
            mon,
            gc,
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
        let id = TxId::new_at(self.mon.clock_now());
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
    /// when a read-write transaction must abort and retry with a fresh id, or
    /// [`TransError::Retry`] when a read-only transaction must re-run.
    pub async fn commit(&self, tx: &mut Handle) -> Result<(), TransError> {
        if tx.data.writes.is_empty() {
            return self.commit_readonly(tx).await;
        }
        self.commit_read_write(tx).await
    }

    /// Read-only fast path: re-resolve each read's effective writer against the
    /// shards and commit if none changed. Takes no locks and writes nothing, so
    /// it never registers with the monitor.
    async fn commit_readonly(&self, tx: &mut Handle) -> Result<(), TransError> {
        if self.validate_reads_inner(&tx.data).await? {
            tx.status = Status::Committed;
            return Ok(());
        }
        self.invalidate_reads(&tx.data);
        rt::sleep(tx.backoff.next_delay()).await;
        Err(TransError::Retry)
    }

    /// Read-write path: lock the touched shards (and roots for membership
    /// changes), flip the transaction object to committed, then write back.
    async fn commit_read_write(&self, tx: &mut Handle) -> Result<(), TransError> {
        if tx.status == Status::New {
            self.mon.begin_tx(&tx.id);
            tx.status = Status::Validating;
            tx.engaged = true;
        }

        // Stop early if a higher-priority transaction wounded us already.
        if self.was_wounded(tx).await {
            return self.restart(tx).await;
        }

        let serial = tx.attempts >= SERIAL_FALLBACK_AFTER;
        let locked = match self.locker.lock(&tx.id, &tx.data, serial).await? {
            Some(l) => l,
            None => return self.restart(tx).await,
        };

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

        // Write-back publishes pointers and releases locks. Idempotent and
        // best-effort: the transaction is already durably committed, so a
        // write-back failure only delays lazy lock cleanup, never the result.
        // TODO: Do it in the background to not delay commit return
        self.locker.write_back(&tx.id, &locked).await;
        // GC is inert in v2 (ADR-022 deferred); this records nothing but keeps
        // the write-back hook wired for the future mark-sweep collector.
        self.gc.schedule_tx_cleanup(tx.id.clone());
        Ok(())
    }

    /// Backs off and signals the read-write restart: invalidate stale cached
    /// reads (so the retry re-reads the authoritative value rather than the
    /// stale one it would otherwise re-validate and re-conflict on) and return
    /// [`TransError::Wounded`] so the caller renews the id and re-runs.
    async fn restart(&self, tx: &mut Handle) -> Result<(), TransError> {
        self.invalidate_reads(&tx.data);
        rt::sleep(tx.backoff.next_delay()).await;
        Err(TransError::Wounded)
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
    /// invalidated.
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
        rt::sleep(tx.backoff.next_delay()).await;
        Err(TransError::Retry)
    }

    /// Re-resolves every read's effective writer and reports whether they all
    /// still match what the transaction observed (a consistent snapshot exists).
    async fn validate_reads_inner(&self, data: &Data) -> Result<bool, TransError> {
        for r in &data.reads {
            let observed = r.version.as_ref().map(|v| v.last_writer.clone());
            let current = self.reader.effective_writer(&r.path).await?;
            if current != observed {
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
                self.local
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
    use glassdb_backend::{Backend, memory::MemoryBackend};
    use glassdb_concurr::{Background, RetryConfig};
    use glassdb_data::paths;
    use glassdb_data::shard::shard_index;
    use glassdb_storage::{
        CollectionRoot, Local, MAX_STALENESS, ShardEntry, ShardStore, StorageError, TLogger,
        TxCommitStatus,
    };

    const TEST_COLL: &str = "testp";

    struct Tctx {
        backend: Arc<dyn Backend>,
        local: Local,
        tlogger: TLogger,
        tmon: Monitor,
        shards: ShardStore,
    }

    async fn new_algo() -> (Algo, Tctx) {
        new_algo_from_backend(Arc::new(MemoryBackend::new())).await
    }

    async fn new_algo_from_backend(b: Arc<dyn Backend>) -> (Algo, Tctx) {
        let local = Local::new(1024);
        let global = Global::new(b.clone(), local.clone());
        let tlogger = TLogger::new(global.clone(), local.clone(), TEST_COLL);
        let bg = Arc::new(Background::new());
        let bg_weak = Arc::downgrade(&bg);
        // Leak the background so spawned async aborts can run for the test's
        // lifetime without us threading the owner through every helper.
        std::mem::forget(bg);
        let tmon = Monitor::new(local.clone(), tlogger.clone(), bg_weak.clone());
        let shards = ShardStore::new(b.clone());
        let reader = Reader::new(
            local.clone(),
            shards.clone(),
            tmon.clone(),
            RetryConfig::default(),
        );
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
            global.clone(),
            local.clone(),
            locker,
            tmon.clone(),
            gc,
            None,
            reader,
        );
        (
            algo,
            Tctx {
                backend: b,
                local,
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
            tctx.local.clone(),
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

    #[tokio::test]
    async fn stale_read_write_is_wounded() {
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
        assert!(matches!(err, TransError::Wounded), "got {err:?}");
        tm.end(&mut h).await.unwrap();
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
