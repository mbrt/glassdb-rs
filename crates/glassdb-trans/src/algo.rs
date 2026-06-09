//! The transaction commit protocol with serializable isolation. Ported from the
//! Go `internal/trans/algo.go`.
//!
//! Highlights: a read-only fast path, a single read-write CAS fast path, and a
//! general validate-and-lock path that locks in parallel and, on a suspected
//! deadlock (lock timeout), falls back to serialized, sorted locking.

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use glassdb_backend::{self as backend, Metadata};
use glassdb_concurr::{Background, rt};
use glassdb_data::{TxId, paths};
use glassdb_storage::{
    Global, Local, LockType, PathLock, TValue, TxLog, TxWrite, Version, tags_lock_info,
};

use crate::error::TransError;
use crate::gc::Gc;
use crate::monitor::Monitor;
use crate::reader::Reader;
use crate::tlocker::Locker;

const ALGO_CONCURRENCY: usize = 10;
const BACKGROUND_CONCURRENCY: usize = 3;
const LOCK_LATENCY: Duration = Duration::from_millis(90);
const MAX_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(5);
const BG_CLEANUP_TIMEOUT: Duration = Duration::from_secs(60);

/// Converts a wall-clock instant to UnixNano, used to derive a transaction's
/// wound-wait priority. The `SystemTime`->`u64` conversion lives here in `trans`
/// (rather than in the pure `data` crate) so the priority can be sourced from
/// the monitor's clock, which is anchored to tokio's virtual time in tests.
fn now_unix_nanos(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Status {
    New,
    Validating,
    Committed,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum VResult {
    Unknown,
    Ok,
    Retry,
    NeedsCLock,
}

/// A single key read within a transaction.
#[derive(Debug, Clone)]
pub struct ReadAccess {
    pub path: Arc<str>,
    pub version: ReadVersion,
    pub found: bool,
}

/// Identifies the version read by a transaction (the writer's transaction ID).
#[derive(Debug, Clone, Default)]
pub struct ReadVersion {
    pub last_writer: TxId,
}

impl ReadVersion {
    /// Converts to a storage version.
    pub fn to_storage_version(&self) -> Version {
        Version {
            b: backend::Version::default(),
            writer: self.last_writer.clone(),
        }
    }
}

/// A single key write within a transaction.
#[derive(Debug, Clone)]
pub struct WriteAccess {
    pub path: Arc<str>,
    pub val: Vec<u8>,
    pub delete: bool,
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
    require_locks: bool,
    serial_locking: bool,
}

impl Handle {
    /// The transaction's ID.
    pub fn id(&self) -> &TxId {
        &self.id
    }
}

#[derive(Clone)]
struct PathState {
    path: Arc<str>,
    read: bool,
    write: bool,
    not_found: bool,
    delete: bool,
    read_version: Version,
    result: VResult,
}

impl PathState {
    fn needs_locks(&self) -> Result<Vec<PathLock>, TransError> {
        let lt = if self.read {
            LockType::Read
        } else if self.write || self.delete {
            LockType::Write
        } else {
            return Ok(Vec::new());
        };
        let mut res = vec![PathLock {
            path: self.path.to_string(),
            typ: lt,
        }];
        if !self.not_found {
            return Ok(res);
        }
        let pr = paths::parse(&self.path).map_err(|e| TransError::Other(e.to_string()))?;
        if pr.typ != paths::Type::Key {
            return Err(TransError::Other(format!(
                "expected only keys while locking, got path {:?}",
                self.path
            )));
        }
        let cpath = paths::collection_info(&pr.prefix);
        res.push(PathLock {
            path: cpath,
            typ: lt,
        });
        Ok(res)
    }
}

struct ValidationState {
    paths: Vec<PathState>,
}

impl ValidationState {
    fn outcome(&self) -> Result<(), TransError> {
        let mut retry = 0usize;
        let mut unknown = 0usize;
        let mut needsclock = 0usize;
        for p in &self.paths {
            match p.result {
                VResult::Retry => retry += 1,
                VResult::Unknown => unknown += 1,
                VResult::NeedsCLock => needsclock += 1,
                VResult::Ok => {}
            }
        }
        // Retry wins over everything else.
        if retry > 0 {
            return Err(TransError::Retry);
        }
        if unknown > 0 || needsclock > 0 {
            return Err(TransError::ValidateRetry);
        }
        Ok(())
    }
}

fn init_validation(h: &Handle) -> ValidationState {
    // `collect_accesses` emits reads and writes already sorted and unique by
    // path (see ADR-008), so merge the two sorted runs directly into a
    // path-sorted `PathState` list. This avoids a per-transaction `HashMap`
    // allocation (plus its hashing and the separate final sort) while keeping
    // the same deterministic, deduplicated validation order: a key that is both
    // read and written yields a single merged entry.
    let reads = &h.data.reads;
    let writes = &h.data.writes;
    let mut paths: Vec<PathState> = Vec::with_capacity(reads.len() + writes.len());
    let (mut i, mut j) = (0, 0);
    loop {
        let (take_read, take_write) = match (reads.get(i), writes.get(j)) {
            (Some(r), Some(w)) => match r.path.cmp(&w.path) {
                std::cmp::Ordering::Less => (true, false),
                std::cmp::Ordering::Greater => (false, true),
                std::cmp::Ordering::Equal => (true, true),
            },
            (Some(_), None) => (true, false),
            (None, Some(_)) => (false, true),
            (None, None) => break,
        };
        let path = if take_read {
            reads[i].path.clone()
        } else {
            writes[j].path.clone()
        };
        let mut ps = PathState {
            path,
            read: false,
            write: false,
            not_found: false,
            delete: false,
            read_version: Version::default(),
            result: VResult::Unknown,
        };
        if take_read {
            let r = &reads[i];
            ps.read = true;
            ps.read_version = r.version.to_storage_version();
            ps.not_found = !r.found;
            i += 1;
        }
        if take_write {
            let w = &writes[j];
            ps.write = true;
            ps.delete = w.delete;
            j += 1;
        }
        paths.push(ps);
    }
    ValidationState { paths }
}

fn is_single_rw(data: &Data) -> bool {
    if data.reads.len() != 1 || data.writes.len() != 1 {
        return false;
    }
    if data.reads[0].path != data.writes[0].path {
        return false;
    }
    data.reads[0].found
}

fn same_version_after_lock(v: &Version, meta: &Metadata) -> bool {
    v.equal_meta_contents(meta)
}

fn to_log(id: TxId, writes: &[WriteAccess]) -> TxLog {
    let mut tl = TxLog::new(id, glassdb_storage::TxCommitStatus::Ok);
    for w in writes {
        tl.writes.push(TxWrite {
            path: w.path.to_string(),
            value: w.val.clone(),
            deleted: w.delete,
            prev_writer: TxId::default(),
        });
    }
    tl
}

fn collections_locks(vstate: &ValidationState) -> Result<Vec<PathLock>, TransError> {
    let mut locks: HashMap<String, LockType> = HashMap::new();

    for info in &vstate.paths {
        // A blind write (write without a prior read) has unknown existence: it
        // may need to create the key, which requires the collection lock for
        // phantom prevention. Acquire it up front so the key can take the
        // create-or-write path directly, instead of first attempting a write
        // lock (a wasted metadata read that returns not-found for a new key)
        // and only then retrying under a collection lock.
        let blind_write = info.write && !info.read;
        if !info.not_found && !info.delete && info.result != VResult::NeedsCLock && !blind_write {
            // Only not-found, deleted, and blind-write items require collection
            // locks.
            continue;
        }
        let pr = paths::parse(&info.path).map_err(|e| TransError::Other(e.to_string()))?;
        if pr.typ != paths::Type::Key {
            return Err(TransError::Other(format!(
                "expected only keys while locking, got path {:?}",
                info.path
            )));
        }
        if info.write {
            locks.insert(pr.prefix.clone(), LockType::Write);
            continue;
        }
        if !info.read {
            continue;
        }
        if let Some(LockType::Write) = locks.get(&pr.prefix) {
            continue;
        }
        locks.insert(pr.prefix.clone(), LockType::Read);
    }

    // Sort by collection path for the same determinism reason as
    // `init_validation`: a stable lock order independent of `HashMap` iteration.
    let mut out: Vec<PathLock> = locks
        .into_iter()
        .map(|(p, t)| PathLock {
            path: paths::collection_info(&p),
            typ: t,
        })
        .collect();
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

/// Coordinates transactions: read validation, locking, and write application.
#[derive(Clone)]
pub struct Algo {
    global: Global,
    local: Local,
    reader: Reader,
    locker: Locker,
    mon: Monitor,
    gc: Gc,
    // Weak so a captured `Algo` clone inside a spawned async-cleanup task
    // does not keep [`Background`] alive past DB shutdown.
    background: Option<Weak<Background>>,
}

impl Algo {
    /// Creates an algorithm coordinator.
    pub fn new(
        global: Global,
        local: Local,
        locker: Locker,
        mon: Monitor,
        gc: Gc,
        background: Option<Weak<Background>>,
    ) -> Self {
        let reader = Reader::new(local.clone(), global.clone(), mon.clone());
        Algo {
            global,
            local,
            reader,
            locker,
            mon,
            gc,
            background,
        }
    }

    /// Releases coordinator resources, awaiting any spawned locker dedup owner
    /// tasks so none leak on database close.
    pub async fn close(&self) {
        self.locker.close().await;
    }

    /// Returns a reference to the underlying [`Locker`], so higher layers can
    /// pull lock-coordination diagnostics (dedup state and per-transaction held
    /// locks) without needing access to the locker directly.
    pub fn locker(&self) -> &Locker {
        &self.locker
    }

    /// Starts a new transaction with the given data. The id's random prefix
    /// and timestamp are deterministic under `--cfg sim` because they are
    /// drawn from the seeded executor RNG and the anchored clock
    /// respectively, so wound-wait priorities replay byte-for-byte.
    pub fn begin(&self, d: Data) -> Handle {
        let id = TxId::new_at(now_unix_nanos(self.mon.clock_now()));
        Handle {
            data: d,
            status: Status::New,
            id,
            require_locks: false,
            serial_locking: false,
        }
    }

    /// Restarts a wounded or retried transaction, preserving its priority
    /// (timestamp) while minting a fresh log identity. Reusing the original
    /// priority prevents a restarted transaction from being starved by an
    /// endless stream of younger peers.
    pub fn rebegin(&self, old: Handle) -> Handle {
        Handle {
            id: old.id.renew(),
            data: old.data,
            status: Status::New,
            require_locks: false,
            serial_locking: false,
        }
    }

    /// Validates all reads and applies all writes, returning [`TransError::Retry`]
    /// on a detected conflict.
    pub async fn commit(&self, tx: &mut Handle) -> Result<(), TransError> {
        if tx.status == Status::New {
            self.mon.begin_tx(&tx.id);
            tx.status = Status::Validating;
        }
        let mut vstate = init_validation(tx);

        loop {
            // Stop early if a higher-priority transaction wounded us while we
            // were validating: there's no point acquiring more locks.
            if self.was_wounded(tx).await {
                self.update_local_cache(&vstate);
                return Err(TransError::Wounded);
            }
            match self.validate_round(&mut vstate, tx).await {
                Ok(()) => break,
                Err(TransError::ValidateRetry) => continue,
                Err(e) => {
                    self.update_local_cache(&vstate);
                    return Err(e);
                }
            }
        }

        if let Err(e) = self.commit_writes(&tx.data.writes, &tx.id).await {
            if matches!(e, TransError::AlreadyFinalized) {
                // The log was already finalized as `aborted`: we were wounded
                // (or reclaimed as expired) between validation and commit.
                // Third parties only write our log to wound (status
                // `aborted`); a `committed` status can only come from our own
                // previously-landed attempt, which `set_final_log` resolves to
                // `Ok` internally.
                return Err(TransError::Wounded);
            }
            return Err(TransError::Other(format!(
                "committing writes for tx {}: {e}",
                tx.id
            )));
        }
        tx.status = Status::Committed;
        self.async_cleanup(tx);
        Ok(())
    }

    /// Reports whether the transaction was already aborted by a higher-priority
    /// transaction. Best-effort: a status read error is not treated as a wound.
    async fn was_wounded(&self, tx: &Handle) -> bool {
        matches!(
            self.mon.tx_status(&tx.id).await,
            Ok(glassdb_storage::TxCommitStatus::Aborted)
        )
    }

    /// Validates the reads of a read-only transaction, returning
    /// [`TransError::Retry`] if any read was invalidated.
    pub async fn validate_reads(&self, tx: &mut Handle) -> Result<(), TransError> {
        if tx.status == Status::New {
            self.mon.begin_tx(&tx.id);
            tx.status = Status::Validating;
        }
        if !tx.data.writes.is_empty() {
            return Err(TransError::Other(
                "cannot validate only reads when writes are present".into(),
            ));
        }
        let mut vstate = init_validation(tx);
        if let Err(e) = self.validate_readonly(&mut vstate, tx).await {
            self.update_local_cache(&vstate);
            return Err(e);
        }
        Ok(())
    }

    /// Replaces the transaction's data, preserving acquired locks.
    pub fn reset(&self, tx: &mut Handle, data: Data) {
        assert!(
            tx.status != Status::Committed,
            "cannot reset a committed transaction"
        );
        tx.data = data;
    }

    /// Aborts a non-committed transaction, releasing its locks.
    pub async fn end(&self, tx: &mut Handle) -> Result<(), TransError> {
        if tx.status == Status::Committed {
            return Ok(());
        }
        if let Err(e) = self.mon.abort_tx(&tx.id).await {
            // A timeout here is fine; we follow up with an async cleanup.
            self.async_cleanup(tx);
            return Err(e);
        }
        Ok(())
    }

    /// Best-effort asynchronous abort of `tx_id`, used when a transaction's
    /// future is dropped mid-flight so [`Algo::end`] never ran. Synchronous:
    /// schedules a spawned task on the background executor and returns
    /// immediately. The spawned task writes the Aborted log entry; if it
    /// fails, the transaction's locks linger until lease expiry, exactly the
    /// behaviour we'd have without this method. Idempotent (a transaction
    /// that already finalized is a no-op in `mon.abort_tx`).
    pub fn async_abort(&self, tx_id: &TxId) {
        let Some(bg) = self.background.as_ref().and_then(|w| w.upgrade()) else {
            return;
        };
        let mon = self.mon.clone();
        let tx_id = tx_id.clone();
        bg.spawn(async move {
            let _ = mon.abort_tx(&tx_id).await;
        });
    }

    async fn validate_round(
        &self,
        vstate: &mut ValidationState,
        tx: &mut Handle,
    ) -> Result<(), TransError> {
        if tx.require_locks {
            return self.validate_read_write(vstate, tx).await;
        }
        if tx.data.writes.is_empty() {
            return self.validate_readonly(vstate, tx).await;
        }
        // The single-RW fast path writes the value straight to the object,
        // which *is* its commit point; it bypasses the lock/transaction-log
        // protocol. That is only sound when this transaction holds no locks. A
        // previous attempt retried with the same id (via `tx.reset`) may still
        // hold locks it deliberately preserved; taking the fast path then would
        // make the value durable and *then* have `commit_writes` try to
        // finalize a log for those leftover locks, fail with `AlreadyFinalized`,
        // and trigger a retry that applies the write a second time. Commit
        // through the lock-based path instead.
        if is_single_rw(&tx.data) && !self.locker.has_locks(&tx.id) {
            match self.commit_single_rw(tx).await {
                Err(TransError::NoSingleWrite) | Err(TransError::Retry) => {
                    // Fall back to regular validation, acquiring locks early.
                    tx.require_locks = true;
                    return Err(TransError::ValidateRetry);
                }
                other => return other,
            }
        }
        self.validate_read_write(vstate, tx).await
    }

    async fn validate_read_write(
        &self,
        vstate: &mut ValidationState,
        tx: &mut Handle,
    ) -> Result<(), TransError> {
        if tx.serial_locking {
            return self.serial_validate(vstate, tx).await;
        }
        match self.parallel_validate(vstate, tx).await {
            Ok(()) => Ok(()),
            Err(TransError::LockTimeout) => {
                // Most likely deadlocked: restart with serialized locking.
                tx.serial_locking = true;
                let held = self.locker.locked_paths(&tx.id);
                tracing::debug!(
                    target: "glassdb::algo",
                    tx = %tx.id,
                    needed_paths = vstate.paths.len(),
                    held_locks = held.len(),
                    "parallel_lock_timeout_fallback_to_serial",
                );
                Err(TransError::ValidateRetry)
            }
            Err(e) => Err(e),
        }
    }

    async fn commit_single_rw(&self, tx: &mut Handle) -> Result<(), TransError> {
        let read = tx.data.reads[0].clone();
        let write = tx.data.writes[0].clone();

        let mut meta = match self.reader.get_metadata(&read.path, MAX_STALE).await {
            Ok(m) => m,
            Err(e) if e.is_not_found() => return Err(TransError::NoSingleWrite),
            Err(e) => {
                return Err(TransError::Other(format!(
                    "getting metadata for {:?}: {e}",
                    read.path
                )));
            }
        };

        // Try validating twice without retrying the whole transaction.
        for _ in 0..2 {
            if let Err(e) = self.check_read_version_unlocked(&read.version, &meta) {
                if e.is_retry() {
                    self.local
                        .mark_value_outdated(&write.path, read.version.to_storage_version());
                }
                return Err(e);
            }
            let slocker = glassdb_storage::Locker::new(self.global.clone());
            let update = glassdb_storage::LockUpdate {
                typ: LockType::None,
                writer: tx.id.clone(),
                value: TValue {
                    value: write.val.clone(),
                    ..Default::default()
                },
                ..Default::default()
            };
            match slocker
                .update_lock(&read.path, &meta.version, &update)
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) if e.is_precondition() => {
                    // Raced: refresh metadata with a strong read and retry.
                    meta = match self.global.get_metadata(&read.path).await {
                        Ok(m) => m,
                        Err(e) if e.is_not_found() => return Err(TransError::NoSingleWrite),
                        Err(e) => {
                            return Err(TransError::Other(format!(
                                "getting metadata for {:?}: {e}",
                                read.path
                            )));
                        }
                    };
                }
                Err(e) => return Err(e.into()),
            }
        }

        // We keep getting raced against; do regular validations from now on.
        self.local
            .mark_value_outdated(&read.path, read.version.to_storage_version());
        Err(TransError::Retry)
    }

    async fn validate_readonly(
        &self,
        vstate: &mut ValidationState,
        tx: &mut Handle,
    ) -> Result<(), TransError> {
        let paths = &vstate.paths;
        let n = paths.len();
        let (outs, err) = self
            .run_indexed(n, |i| {
                let mut item = paths[i].clone();
                async move {
                    if item.not_found {
                        self.validate_read_not_found(&mut item).await?;
                    } else {
                        self.validate_read(&mut item).await?;
                    }
                    Ok(item)
                }
            })
            .await;
        for (i, o) in outs.into_iter().enumerate() {
            if let Some(it) = o {
                vstate.paths[i] = it;
            }
        }
        err?;

        let res = vstate.outcome();
        if let Err(TransError::Retry) = &res {
            // Avoid retrying too often: do regular validation after locking.
            tx.require_locks = true;
        }
        res
    }

    async fn validate_read(&self, item: &mut PathState) -> Result<(), TransError> {
        let meta = match self.global.get_metadata(&item.path).await {
            Ok(m) => m,
            Err(e) if e.is_not_found() => {
                item.result = VResult::Retry;
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };
        let read_from = item.read_version.writer.clone();
        let li = tags_lock_info(&meta.tags)?;

        if li.typ == LockType::None || li.typ == LockType::Read {
            if li.last_writer != read_from {
                item.result = VResult::Retry;
            } else {
                item.result = VResult::Ok;
            }
            return Ok(());
        }
        self.validate_locked_read(item, &li, &read_from).await
    }

    async fn validate_locked_read(
        &self,
        item: &mut PathState,
        li: &glassdb_storage::LockInfo,
        read_from: &TxId,
    ) -> Result<(), TransError> {
        if li.locked_by.len() != 1 {
            return Err(TransError::Other(format!(
                "bad lock: {:?} with {} lockers",
                li.typ,
                li.locked_by.len()
            )));
        }
        let locker = li.locked_by[0].clone();
        let status = self.mon.tx_status(&locker).await?;

        let expected_writer;
        let mut expected_val = None;

        match status {
            glassdb_storage::TxCommitStatus::Ok => {
                let v = self.mon.committed_value(&item.path, &locker).await?;
                if v.value.not_written {
                    expected_writer = li.last_writer.clone();
                } else if v.value.deleted {
                    item.result = VResult::Retry;
                    self.update_local(
                        &WriteAccess {
                            path: item.path.clone(),
                            val: v.value.value,
                            delete: true,
                        },
                        &locker,
                    );
                    return Ok(());
                } else {
                    expected_writer = locker.clone();
                    expected_val = Some(v);
                }
            }
            glassdb_storage::TxCommitStatus::Aborted | glassdb_storage::TxCommitStatus::Pending => {
                expected_writer = li.last_writer.clone();
            }
            glassdb_storage::TxCommitStatus::Unknown => {
                return Err(TransError::Other("unknown tx commit status".into()));
            }
        }

        if *read_from == expected_writer {
            item.result = VResult::Ok;
            return Ok(());
        }

        // We read from an old value: update our local copy and retry.
        item.result = VResult::Retry;

        let mut ev = match expected_val {
            Some(v) => v,
            None => match self.mon.committed_value(&item.path, &expected_writer).await {
                Ok(v) => v,
                Err(_) => {
                    self.local
                        .mark_value_outdated(&item.path, item.read_version.clone());
                    return Ok(());
                }
            },
        };

        if ev.status != glassdb_storage::TxCommitStatus::Ok {
            ev = match self.mon.committed_value(&item.path, &expected_writer).await {
                Ok(v) => v,
                Err(_) => {
                    self.local
                        .mark_value_outdated(&item.path, item.read_version.clone());
                    return Ok(());
                }
            };
        }

        if ev.status != glassdb_storage::TxCommitStatus::Ok || ev.value.not_written {
            // We cannot authoritatively resolve expected_writer's value. This
            // happens when expected_writer committed through the single-RW fast
            // path, which writes no transaction log, so its log-based status is
            // unknown/aborted even though it did commit. Caching a guessed value
            // here would be corrupting: it would pair value bytes with a writer
            // that did not produce them, and a later read could trust that
            // (writer matches the live last-writer) and overwrite a newer value,
            // losing an update. Instead, invalidate the stale cached value so the
            // retry re-reads the authoritative one straight from storage.
            self.local
                .mark_value_outdated(&item.path, item.read_version.clone());
            return Ok(());
        }

        self.update_local(
            &WriteAccess {
                path: item.path.clone(),
                val: ev.value.value,
                delete: ev.value.deleted,
            },
            &expected_writer,
        );
        Ok(())
    }

    async fn validate_read_not_found(&self, item: &mut PathState) -> Result<(), TransError> {
        let meta = match self.global.get_metadata(&item.path).await {
            Ok(m) => m,
            Err(e) if e.is_not_found() => {
                item.result = VResult::Ok;
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };
        let li = tags_lock_info(&meta.tags)?;

        if li.typ == LockType::None || li.typ == LockType::Read {
            item.result = VResult::Retry;
            return Ok(());
        }
        if li.locked_by.len() != 1 {
            return Err(TransError::Other(format!(
                "bad lock: {:?} with {} lockers",
                li.typ,
                li.locked_by.len()
            )));
        }
        let locker = li.locked_by[0].clone();
        let status = self.mon.tx_status(&locker).await?;
        let last_writer = match status {
            glassdb_storage::TxCommitStatus::Ok => locker.clone(),
            glassdb_storage::TxCommitStatus::Aborted | glassdb_storage::TxCommitStatus::Pending => {
                li.last_writer.clone()
            }
            glassdb_storage::TxCommitStatus::Unknown => {
                return Err(TransError::Other("unknown tx commit status".into()));
            }
        };

        let v = self.mon.committed_value(&item.path, &last_writer).await?;
        if v.value.deleted {
            item.result = VResult::Ok;
            return Ok(());
        }

        // Was written to: retry. Only refresh the local cache when we could
        // authoritatively resolve the value. If last_writer committed via the
        // single-RW fast path it has no transaction log, so the value is
        // unresolvable here; caching a guessed value would corrupt the entry, so we
        // just retry and let the next read fetch the authoritative value.
        if v.status == glassdb_storage::TxCommitStatus::Ok && !v.value.not_written {
            self.update_local(
                &WriteAccess {
                    path: item.path.clone(),
                    val: v.value.value,
                    delete: v.value.deleted,
                },
                &last_writer,
            );
        }

        item.result = VResult::Retry;
        Ok(())
    }

    fn check_read_version_unlocked(
        &self,
        rv: &ReadVersion,
        meta: &Metadata,
    ) -> Result<(), TransError> {
        let linfo = tags_lock_info(&meta.tags)?;
        let same_last_writer = linfo.last_writer == rv.last_writer && linfo.typ == LockType::None;
        let locked_by_writer = linfo.locked_by.len() == 1 && linfo.locked_by[0] == rv.last_writer;
        if !same_last_writer && !locked_by_writer {
            return Err(TransError::Retry);
        }
        Ok(())
    }

    async fn parallel_validate(
        &self,
        vstate: &mut ValidationState,
        tx: &mut Handle,
    ) -> Result<(), TransError> {
        let timeout = deadlock_timeout(vstate);
        let validate = async {
            self.lock_collections(vstate, tx)
                .await
                .map_err(|e| TransError::Other(format!("locking collections: {e}")))?;
            self.lock_validate(vstate, tx)
                .await
                .map_err(|e| TransError::Other(format!("failed validation: {e}")))?;
            Ok::<_, TransError>(())
        };
        // Bound the locking work to break deadlocks. Dropping the validate
        // future on the sleep arm cancels every in-flight `lock_*` future,
        // which the locker (`PushGuard` in `tlocker`) and dedup
        // (`DriverGuard`/`WaiterDropGuard`) handle cleanly: ambiguous locks
        // are marked `Unknown` in the per-tx state so the serial fallback
        // observes the mismatch and unlocks before re-acquiring.
        let res = match timeout {
            Some(t) => tokio::select! {
                biased;
                r = validate => r,
                _ = rt::sleep(t) => Err(TransError::LockTimeout),
            },
            None => validate.await,
        };
        res?;
        vstate.outcome()
    }

    async fn lock_collections(
        &self,
        vstate: &ValidationState,
        tx: &Handle,
    ) -> Result<(), TransError> {
        let colocks = collections_locks(vstate)?;
        if colocks.is_empty() {
            return Ok(());
        }
        let (_, err) = self
            .run_indexed(colocks.len(), |i| {
                let cl = colocks[i].clone();
                async move {
                    self.lock_path(&cl.path, cl.typ, tx).await.map_err(|e| {
                        TransError::Other(format!("locking collection {:?}: {e}", cl.path))
                    })
                }
            })
            .await;
        err
    }

    async fn serial_validate(
        &self,
        vstate: &mut ValidationState,
        tx: &mut Handle,
    ) -> Result<(), TransError> {
        if !self.already_locked(vstate, tx) {
            // We need to lock in the right order, so first unlock everything.
            self.unlock_all(tx).await.map_err(|e| {
                TransError::Other(format!(
                    "unlocking before serial validate for tx {}: {e}",
                    tx.id
                ))
            })?;
            for item in &mut vstate.paths {
                item.result = VResult::Unknown;
            }
        }

        // Lock collections first, sorted.
        let mut colocks = collections_locks(vstate)?;
        if !colocks.is_empty() {
            colocks.sort_by(|a, b| a.path.cmp(&b.path));
            for cl in &colocks {
                self.lock_path(&cl.path, cl.typ, tx).await.map_err(|e| {
                    TransError::Other(format!("locking collection {:?}: {e}", cl.path))
                })?;
            }
        }

        // Then sort keys and validate them in order.
        vstate.paths.sort_by(|a, b| a.path.cmp(&b.path));
        let n = vstate.paths.len();
        for i in 0..n {
            let mut item = vstate.paths[i].clone();
            let r = self.lock_validate_key(&mut item, tx).await;
            vstate.paths[i] = item;
            r?;
        }
        vstate.outcome()
    }

    fn already_locked(&self, vstate: &ValidationState, tx: &Handle) -> bool {
        let mut need_locks: HashMap<String, LockType> = HashMap::new();
        for ps in &vstate.paths {
            let path_locks = ps.needs_locks().unwrap_or_default();
            for pl in path_locks {
                match need_locks.get(&pl.path) {
                    Some(_) if pl.typ != LockType::Write => {}
                    _ => {
                        need_locks.insert(pl.path, pl.typ);
                    }
                }
            }
        }
        let held = self.locker.locked_paths(&tx.id);
        for (p, elt) in &need_locks {
            // The tx must already hold a compatible lock for *every* needed path.
            // A path it holds with too weak a type, or does not hold at all,
            // means the held set is partial: serial validation must release
            // everything and re-acquire in sorted order. Treating a partial set
            // as "already locked" keeps out-of-order locks (e.g. those an aborted
            // parallel attempt left behind) and can re-create the very deadlock
            // serial locking exists to break.
            let compatible = held
                .iter()
                .find(|lp| &lp.path == p)
                .is_some_and(|lp| lp.typ == *elt || lp.typ == LockType::Write);
            if !compatible {
                tracing::debug!(
                    target: "glassdb::algo",
                    tx = %tx.id,
                    missing_path = %p,
                    needed_lock = %elt,
                    held_count = held.len(),
                    needed_count = need_locks.len(),
                    "serial_validate_held_set_partial",
                );
                return false;
            }
        }
        true
    }

    async fn lock_validate(
        &self,
        vstate: &mut ValidationState,
        tx: &Handle,
    ) -> Result<(), TransError> {
        let paths = &vstate.paths;
        let n = paths.len();
        let (outs, err) = self
            .run_indexed(n, |i| {
                let mut item = paths[i].clone();
                async move {
                    self.lock_validate_key(&mut item, tx).await?;
                    Ok(item)
                }
            })
            .await;
        for (i, o) in outs.into_iter().enumerate() {
            if let Some(it) = o {
                vstate.paths[i] = it;
            }
        }
        err
    }

    async fn lock_validate_key(&self, item: &mut PathState, tx: &Handle) -> Result<(), TransError> {
        if item.result == VResult::Ok {
            return Ok(());
        }
        if item.not_found {
            return self.lock_validate_not_found_key(item, tx).await;
        }
        // A blind write (write with no prior read) has unknown existence. Take
        // the create-or-write path: try a conditional create first (cheap for a
        // new key, no metadata read), falling back to a write lock if the key
        // already exists. The collection lock it relies on was acquired up
        // front by `collections_locks`. This avoids the wasted write-lock
        // attempt (and its not-found metadata read) on keys that must be
        // created.
        if item.write && !item.read {
            return self.lock_validate_not_found_key(item, tx).await;
        }
        self.lock_validate_found_key(item, tx).await
    }

    async fn lock_validate_found_key(
        &self,
        item: &mut PathState,
        tx: &Handle,
    ) -> Result<(), TransError> {
        let lock_res = if item.write {
            self.locker.lock_write(&item.path, &tx.id).await
        } else if item.read {
            self.locker.lock_read(&item.path, &tx.id).await
        } else {
            Ok(())
        };

        if let Err(e) = lock_res {
            if e.is_not_found() {
                if item.read {
                    item.result = VResult::Retry;
                    return Ok(());
                }
                item.not_found = true;
                if self.is_key_collection_locked(&item.path, LockType::Write, tx) {
                    return self.lock_validate_not_found_key(item, tx).await;
                }
                item.result = VResult::NeedsCLock;
                return Ok(());
            }
            return Err(TransError::Other(format!("failed locking: {e}")));
        }
        if !item.read {
            item.result = VResult::Ok;
            return Ok(());
        }

        let meta = self.reader.get_metadata(&item.path, MAX_STALE).await?;
        if !same_version_after_lock(&item.read_version, &meta) {
            item.result = VResult::Retry;
            return Ok(());
        }
        item.result = VResult::Ok;
        Ok(())
    }

    async fn lock_validate_not_found_key(
        &self,
        item: &mut PathState,
        tx: &Handle,
    ) -> Result<(), TransError> {
        if item.read && item.write {
            match self.locker.lock_create(&item.path, &tx.id).await {
                Ok(()) => {
                    item.result = VResult::Ok;
                    Ok(())
                }
                Err(e) if e.is_precondition() => {
                    item.result = VResult::Retry;
                    Ok(())
                }
                Err(e) => Err(e),
            }
        } else if item.read && !item.write {
            match self.global.get_metadata(&item.path).await {
                Err(e) if e.is_not_found() => {
                    item.result = VResult::Ok;
                    Ok(())
                }
                Err(e) => Err(e.into()),
                Ok(_) => {
                    // The item exists now; lock read and validate.
                    match self.locker.lock_read(&item.path, &tx.id).await {
                        Err(e) if e.is_not_found() => {
                            item.result = VResult::Ok;
                            Ok(())
                        }
                        Err(e) => Err(e),
                        Ok(()) => {
                            item.result = VResult::Retry;
                            Ok(())
                        }
                    }
                }
            }
        } else if item.write {
            match self.locker.lock_create(&item.path, &tx.id).await {
                Ok(()) => {
                    item.result = VResult::Ok;
                    Ok(())
                }
                Err(e) if e.is_precondition() => {
                    // Found now; lock it in write instead.
                    self.locker.lock_write(&item.path, &tx.id).await?;
                    item.result = VResult::Ok;
                    Ok(())
                }
                Err(e) => Err(e),
            }
        } else {
            Ok(())
        }
    }

    async fn lock_path(&self, path: &str, lt: LockType, tx: &Handle) -> Result<(), TransError> {
        match lt {
            LockType::Read => self.locker.lock_read(path, &tx.id).await,
            LockType::Write => self.locker.lock_write(path, &tx.id).await,
            LockType::Create => self.locker.lock_create(path, &tx.id).await,
            other => Err(TransError::Other(format!(
                "unsupported lock type {other:?}"
            ))),
        }
        .map_err(|e| TransError::Other(format!("locking path {path:?}: {e}")))
    }

    fn update_local_cache(&self, vstate: &ValidationState) {
        for ps in &vstate.paths {
            if ps.result == VResult::Retry {
                self.local
                    .mark_value_outdated(&ps.path, ps.read_version.clone());
            }
        }
    }

    async fn commit_writes(&self, writes: &[WriteAccess], id: &TxId) -> Result<(), TransError> {
        let mut tl = to_log(id.clone(), writes);
        tl.locks = self.locker.locked_paths(id);
        self.mon.commit_tx(tl).await.map_err(|e| match e {
            // Preserve AlreadyFinalized so the commit path can map it to a wound.
            TransError::AlreadyFinalized => TransError::AlreadyFinalized,
            other => TransError::Other(format!("creating transaction object: {other}")),
        })
    }

    fn update_local(&self, w: &WriteAccess, tid: &TxId) {
        let version = Version {
            b: backend::Version::default(),
            writer: tid.clone(),
        };
        if w.delete {
            self.local.mark_deleted(&w.path, version);
        } else {
            self.local.write(&w.path, w.val.clone(), version);
        }
    }

    async fn unlock_all(&self, tx: &Handle) -> Result<(), TransError> {
        let ps = self.locker.locked_paths(&tx.id);
        if ps.is_empty() {
            return Ok(());
        }
        let (outs, _) = self
            .run_indexed(ps.len(), |i| {
                let pl = ps[i].clone();
                async move {
                    match self.locker.unlock(&pl.path, &tx.id).await {
                        Ok(()) => Ok(None::<TransError>),
                        Err(e) => Ok(Some(TransError::Other(format!(
                            "unlocking {:?}: {e}",
                            pl.path
                        )))),
                    }
                }
            })
            .await;
        let errs: Vec<TransError> = outs.into_iter().flatten().flatten().collect();
        if !errs.is_empty() {
            return Err(TransError::Other(format!(
                "unlocking all for tx {}: {} errors",
                tx.id,
                errs.len()
            )));
        }
        Ok(())
    }

    fn async_cleanup(&self, tx: &Handle) {
        let Some(bg) = self.background.as_ref().and_then(|w| w.upgrade()) else {
            return;
        };
        let ps = self.locker.locked_paths(&tx.id);
        if ps.is_empty() {
            return;
        }
        let algo = self.clone();
        let tid = tx.id.clone();
        bg.spawn(async move {
            let handle = Handle {
                data: Data::default(),
                status: Status::Committed,
                id: tid.clone(),
                require_locks: false,
                serial_locking: false,
            };
            // Bound the cleanup with a sleep-based watchdog: when it elapses
            // the `select!` drops the `run_limited` future, which in turn
            // drops every in-flight unlock attempt. Using `rt::sleep` keeps
            // the watchdog deterministic under `--cfg sim`.
            let ps_len = ps.len();
            let algo_ref = &algo;
            let handle_ref = &handle;
            let ps_ref = &ps;
            let work = algo.run_limited(BACKGROUND_CONCURRENCY, ps_len, move |i| async move {
                match algo_ref
                    .locker
                    .unlock(&ps_ref[i].path, &handle_ref.id)
                    .await
                {
                    Ok(()) => Ok(None::<()>),
                    Err(_) => Ok(Some(())),
                }
            });
            let outs = tokio::select! {
                biased;
                r = work => r.0,
                _ = rt::sleep(BG_CLEANUP_TIMEOUT) => return,
            };
            let failures = outs.into_iter().flatten().flatten().count();
            if failures == 0 {
                algo.gc.schedule_tx_cleanup(tid);
            }
        });
    }

    async fn run_indexed<T, F, Fut>(
        &self,
        num: usize,
        f: F,
    ) -> (Vec<Option<T>>, Result<(), TransError>)
    where
        F: Fn(usize) -> Fut,
        Fut: std::future::Future<Output = Result<T, TransError>>,
    {
        self.run_limited(ALGO_CONCURRENCY, num, f).await
    }

    async fn run_limited<T, F, Fut>(
        &self,
        limit: usize,
        num: usize,
        f: F,
    ) -> (Vec<Option<T>>, Result<(), TransError>)
    where
        F: Fn(usize) -> Fut,
        Fut: std::future::Future<Output = Result<T, TransError>>,
    {
        let mut outs: Vec<Option<T>> = (0..num).map(|_| None).collect();
        if num == 0 {
            return (outs, Ok(()));
        }
        let f = &f;
        let mut stream = futures::stream::iter(0..num)
            .map(|i| async move { (i, f(i).await) })
            .buffer_unordered(limit.max(1));

        // On the first error we break out of the loop and drop the stream;
        // dropping `buffer_unordered` cancels every still-pending sibling
        // (their futures are dropped). Already-completed `Ok` results stay in
        // `outs` for the caller to consume.
        let mut result = Ok(());
        while let Some((i, r)) = stream.next().await {
            match r {
                Ok(v) => outs[i] = Some(v),
                Err(e) => {
                    result = Err(e);
                    break;
                }
            }
        }
        (outs, result)
    }

    fn is_key_collection_locked(&self, key: &str, expected: LockType, tx: &Handle) -> bool {
        let pr = match paths::parse(key) {
            Ok(pr) => pr,
            Err(_) => return false,
        };
        let cpath = paths::collection_info(&pr.prefix);
        self.locker.lock_type(&cpath, &tx.id) == expected
    }
}

/// Per-tx deadlock budget. Returns `None` for trivially serialised work
/// (`paths.len() <= 1`), in which case no timeout is applied.
fn deadlock_timeout(vstate: &ValidationState) -> Option<Duration> {
    if vstate.paths.len() <= 1 {
        return None;
    }
    Some(std::cmp::min(
        LOCK_LATENCY * 4 * vstate.paths.len() as u32,
        MAX_DEADLOCK_TIMEOUT,
    ))
}

const MAX_STALE: Duration = glassdb_storage::MAX_STALENESS;

#[cfg(test)]
mod tests {
    use super::*;
    use glassdb_backend::{Backend, Tags, memory::MemoryBackend};
    use glassdb_data::TxId;
    use glassdb_storage::{LockType, MAX_STALENESS, TLogger, TxCommitStatus};

    const TEST_COLL: &str = "testp";
    const COLL_INFO: &[u8] = b"__foo__";

    struct Tctx {
        backend: Arc<dyn Backend>,
        global: Global,
        local: Local,
        tlogger: TLogger,
        tmon: Monitor,
        locker: Locker,
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
        let tmon = Monitor::new(local.clone(), tlogger.clone(), bg_weak.clone());
        let locker = Locker::new(local.clone(), global.clone(), tmon.clone());
        let gc = Gc::new(bg_weak.clone(), tlogger.clone());

        global
            .write(
                &paths::collection_info(TEST_COLL),
                COLL_INFO.to_vec(),
                Tags::new(),
            )
            .await
            .unwrap();

        // Disable algo background tasks (async cleanup) to keep tests deterministic.
        let algo = Algo::new(
            global.clone(),
            local.clone(),
            locker.clone(),
            tmon.clone(),
            gc,
            None,
        );
        (
            algo,
            Tctx {
                backend: b,
                global,
                local,
                tlogger,
                tmon,
                locker,
            },
        )
    }

    fn wa(path: &str, val: &[u8]) -> WriteAccess {
        WriteAccess {
            path: path.into(),
            val: val.to_vec(),
            delete: false,
        }
    }

    fn wdel(path: &str) -> WriteAccess {
        WriteAccess {
            path: path.into(),
            val: Vec::new(),
            delete: true,
        }
    }

    async fn do_read(tctx: &Tctx, path: &str) -> ReadAccess {
        let reader = Reader::new(tctx.local.clone(), tctx.global.clone(), tctx.tmon.clone());
        match reader.read(path, MAX_STALENESS).await {
            Ok(rv) => ReadAccess {
                path: path.into(),
                version: ReadVersion {
                    last_writer: rv.version.writer,
                },
                found: true,
            },
            Err(e) if e.is_not_found() => ReadAccess {
                path: path.into(),
                version: ReadVersion::default(),
                found: false,
            },
            Err(e) => panic!("reading {path}: {e:?}"),
        }
    }

    async fn do_reads(tctx: &Tctx, ps: &[&str]) -> Vec<ReadAccess> {
        let mut res = Vec::new();
        for p in ps {
            res.push(do_read(tctx, p).await);
        }
        res
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

    async fn flush_writes(tm: &Algo, h: &Handle) {
        tm.unlock_all(h).await.unwrap();
    }

    async fn lock_info(tctx: &Tctx, key: &str) -> glassdb_storage::LockInfo {
        let m = tctx.global.get_metadata(key).await.unwrap();
        tags_lock_info(&m.tags).unwrap()
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

        tctx.global.read(&keyp).await.unwrap();
        let status = tctx.tlogger.commit_status(&tid).await.unwrap();
        assert_eq!(status.status, TxCommitStatus::Ok);

        let txlog = tctx.tlogger.get(&tid).await.unwrap();
        assert!(txlog.timestamp.is_some());
        assert_eq!(txlog.writes.len(), 1);
        assert_eq!(txlog.writes[0].path, keyp);
        assert_eq!(txlog.writes[0].value, val);
        let mut locks = txlog.locks.clone();
        locks.sort_by(|a, b| a.path.cmp(&b.path));
        let mut expected = vec![
            PathLock {
                path: paths::collection_info(TEST_COLL),
                typ: LockType::Write,
            },
            PathLock {
                path: keyp.clone(),
                typ: LockType::Create,
            },
        ];
        expected.sort_by(|a, b| a.path.cmp(&b.path));
        assert_eq!(locks, expected);
    }

    #[tokio::test]
    async fn read_not_found_write() {
        let (tm, tctx) = new_algo().await;
        let keyp = paths::from_key(TEST_COLL, b"k");
        let val = b"v";

        let mut h = tm.begin(Data {
            reads: vec![ReadAccess {
                path: keyp.as_str().into(),
                version: ReadVersion::default(),
                found: false,
            }],
            writes: vec![wa(&keyp, val)],
        });
        tm.commit(&mut h).await.unwrap();
        let tid = h.id().clone();
        tm.end(&mut h).await.unwrap();

        tctx.global.read(&keyp).await.unwrap();
        let status = tctx.tlogger.commit_status(&tid).await.unwrap();
        assert_eq!(status.status, TxCommitStatus::Ok);
    }

    #[tokio::test]
    async fn single_read_write() {
        let (tm, tctx) = new_algo().await;
        let keyp = paths::from_key(TEST_COLL, b"k");
        let val = b"v";

        let h = commit_writes(&tm, vec![wa(&keyp, b"init")]).await;
        flush_writes(&tm, &h).await;

        let gr = tctx.global.read(&keyp).await.unwrap();

        let mut h = tm.begin(Data {
            reads: vec![ReadAccess {
                path: keyp.as_str().into(),
                version: ReadVersion {
                    last_writer: gr.version.writer,
                },
                found: true,
            }],
            writes: vec![wa(&keyp, val)],
        });
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        let gr = tctx.global.read(&keyp).await.unwrap();
        assert_eq!(gr.value, val);
    }

    #[tokio::test]
    async fn read_write_while_lock_create() {
        let (tm, tctx) = new_algo().await;
        let keyp = paths::from_key(TEST_COLL, b"k");
        let val = b"v";

        // Initialize the variable from another algo, without flushing.
        let (tm2, _t2) = new_algo_from_backend(tctx.backend.clone()).await;
        commit_writes(&tm2, vec![wa(&keyp, b"init")]).await;

        let gr = tctx.global.read(&keyp).await.unwrap();

        let mut h = tm.begin(Data {
            reads: vec![ReadAccess {
                path: keyp.as_str().into(),
                version: ReadVersion {
                    last_writer: gr.version.writer,
                },
                found: true,
            }],
            writes: vec![wa(&keyp, val)],
        });
        let err = tm.commit(&mut h).await.unwrap_err();
        assert!(err.is_retry(), "expected retry, got {err:?}");

        let gr = tctx.global.read(&keyp).await.unwrap();
        tm.reset(
            &mut h,
            Data {
                reads: vec![ReadAccess {
                    path: keyp.as_str().into(),
                    version: ReadVersion {
                        last_writer: gr.version.writer,
                    },
                    found: true,
                }],
                writes: vec![wa(&keyp, val)],
            },
        );
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        let lr = tctx.local.read(&keyp, MAX_STALENESS).unwrap();
        assert_eq!(lr.value, val);
        assert_eq!(lr.version.writer, *h.id());

        tm.unlock_all(&h).await.unwrap();
        let gr = tctx.global.read(&keyp).await.unwrap();
        assert_eq!(gr.value, val);
    }

    #[tokio::test]
    async fn readonly() {
        let (tm, tctx) = new_algo().await;
        let keyp = paths::from_key(TEST_COLL, b"k");
        let val = b"v";

        let h = commit_writes(&tm, vec![wa(&keyp, val)]).await;
        flush_writes(&tm, &h).await;

        let gr = tctx.global.read(&keyp).await.unwrap();

        let mut h = tm.begin(Data {
            reads: vec![ReadAccess {
                path: keyp.as_str().into(),
                version: ReadVersion {
                    last_writer: gr.version.writer,
                },
                found: true,
            }],
            writes: Vec::new(),
        });
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        let gr = tctx.global.read(&keyp).await.unwrap();
        assert_eq!(gr.value, val);
    }

    #[tokio::test]
    async fn readonly_in_lock_create() {
        let (tm, tctx) = new_algo().await;
        let keyp = paths::from_key(TEST_COLL, b"k");
        let val = b"v";

        let (tm2, _t2) = new_algo_from_backend(tctx.backend.clone()).await;
        commit_writes(&tm2, vec![wa(&keyp, val)]).await;

        let r = do_read(&tctx, &keyp).await;
        let mut h = tm.begin(Data {
            reads: vec![r],
            writes: Vec::new(),
        });
        tm.commit(&mut h).await.unwrap();
    }

    #[tokio::test]
    async fn readonly_after_delete() {
        let (tm, tctx) = new_algo().await;
        let keyp = paths::from_key(TEST_COLL, b"k");

        let (tm2, _t2) = new_algo_from_backend(tctx.backend.clone()).await;
        commit_writes(&tm2, vec![wa(&keyp, b"v")]).await;
        commit_writes(&tm2, vec![wdel(&keyp)]).await;

        let r = do_read(&tctx, &keyp).await;
        let mut h = tm.begin(Data {
            reads: vec![r],
            writes: Vec::new(),
        });
        let err = tm.commit(&mut h).await.unwrap_err();
        assert!(err.is_retry(), "expected retry, got {err:?}");

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
    async fn readonly_local_after_delete() {
        let (tm, tctx) = new_algo().await;
        let keyp = paths::from_key(TEST_COLL, b"k");

        commit_writes(&tm, vec![wa(&keyp, b"v")]).await;
        commit_writes(&tm, vec![wdel(&keyp)]).await;

        let r = do_read(&tctx, &keyp).await;
        let mut h = tm.begin(Data {
            reads: vec![r],
            writes: Vec::new(),
        });
        tm.commit(&mut h).await.unwrap();
    }

    #[tokio::test]
    async fn readonly_local_after_delete_flushed() {
        let (tm, tctx) = new_algo().await;
        let keyp = paths::from_key(TEST_COLL, b"k");

        commit_writes(&tm, vec![wa(&keyp, b"v")]).await;
        let h = commit_writes(&tm, vec![wdel(&keyp)]).await;
        flush_writes(&tm, &h).await;

        let r = do_read(&tctx, &keyp).await;
        let mut h = tm.begin(Data {
            reads: vec![r],
            writes: Vec::new(),
        });
        tm.commit(&mut h).await.unwrap();
    }

    #[tokio::test]
    async fn single_rw_retry() {
        let (tm, tctx) = new_algo().await;
        let (tm2, _t2) = new_algo_from_backend(tctx.backend.clone()).await;
        let keyp = paths::from_key(TEST_COLL, b"k");

        commit_writes(&tm2, vec![wa(&keyp, b"v1")]).await;

        let ra = do_read(&tctx, &keyp).await;
        assert!(ra.found);

        // Modify the value from another algo as a single-RW transaction.
        commit_access(
            &tm2,
            Data {
                reads: vec![ra.clone()],
                writes: vec![wa(&keyp, b"v")],
            },
        )
        .await;

        let mut h = tm.begin(Data {
            reads: vec![ra],
            writes: vec![wa(&keyp, b"v2")],
        });
        let err = tm.commit(&mut h).await.unwrap_err();
        assert!(err.is_retry(), "expected retry, got {err:?}");

        let ra = do_read(&tctx, &keyp).await;
        tm.reset(
            &mut h,
            Data {
                reads: vec![ra],
                writes: vec![wa(&keyp, b"v2")],
            },
        );
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();
    }

    /// Regression test for ADR-007: a single-RW writer leaves no transaction log,
    /// so validation must not cache an unresolvable value tagged with that writer.
    #[tokio::test]
    async fn single_rw_lost_update() {
        let (tm_writer, tctx_w) = new_algo().await;
        let (tm_victim, tctx_v) = new_algo_from_backend(tctx_w.backend.clone()).await;
        let (_, tctx_l) = new_algo_from_backend(tctx_w.backend.clone()).await;
        let key = paths::from_key(TEST_COLL, b"k");
        let v0 = b"v0";
        let v1 = b"v1";

        // 1. Create k=v0 through a normal (logged) commit and flush it.
        let h0 = commit_writes(&tm_writer, vec![wa(&key, v0)]).await;
        flush_writes(&tm_writer, &h0).await;

        // 2. The victim reads k, caching v0@W0 in its local storage.
        let ra0 = do_read(&tctx_v, &key).await;
        assert!(ra0.found);

        // 3. The writer overwrites k=v1 through the single-RW fast path.
        let ra_w = do_read(&tctx_w, &key).await;
        let h_w1 = commit_access(
            &tm_writer,
            Data {
                reads: vec![ra_w],
                writes: vec![wa(&key, v1)],
            },
        )
        .await;
        let w1 = h_w1.id.clone();
        let st = tctx_w.tlogger.commit_status(&w1).await.unwrap();
        assert_eq!(st.status, TxCommitStatus::Unknown);
        let gr = tctx_w.global.read(&key).await.unwrap();
        assert_eq!(gr.value, v1);
        assert_eq!(gr.writer(), w1);

        // 4. A third client write-locks k and stays pending.
        let w2 = TxId::with_priority(5 * 1_000_000_000, b"w2");
        tctx_l.tmon.begin_tx(&w2);
        tctx_l.locker.lock_write(&key, &w2).await.unwrap();
        let info = lock_info(&tctx_l, &key).await;
        assert_eq!(info.typ, LockType::Write);
        assert_eq!(info.locked_by.len(), 1);
        assert_eq!(info.locked_by[0], w2);
        assert_eq!(info.last_writer, w1);

        // 5. The victim validates its now-stale read of k.
        let mut hv = tm_victim.begin(Data {
            reads: vec![ra0],
            writes: Vec::new(),
        });
        let err = tm_victim.commit(&mut hv).await.unwrap_err();
        assert!(err.is_retry(), "expected retry, got {err:?}");
        tm_victim.end(&mut hv).await.unwrap();

        // 6. The third client releases its lock; k is unlocked at v1@W1.
        tctx_l.locker.unlock(&key, &w2).await.unwrap();
        let info = lock_info(&tctx_l, &key).await;
        assert_eq!(info.typ, LockType::None);
        assert_eq!(info.last_writer, w1);
        tctx_l.tmon.abort_tx(&w2).await.unwrap();

        // 7. Reading k must return the authoritative committed value v1.
        let reader = Reader::new(
            tctx_v.local.clone(),
            tctx_v.global.clone(),
            tctx_v.tmon.clone(),
        );
        let rv = reader.read(&key, MAX_STALENESS).await.unwrap();
        assert_eq!(rv.value, v1);
        assert_eq!(rv.version.writer, w1);
    }

    /// Regression: a timed-out parallel-locking attempt leaves a transaction
    /// holding a *subset* of the locks it needs, possibly in the wrong order.
    /// Serial validation breaks the resulting deadlock only if it releases and
    /// re-acquires everything in sorted order, which it gates on
    /// `already_locked`. The guard must therefore report a partial set as *not*
    /// already locked; otherwise two equal-priority transactions (a tie
    /// wound-wait cannot order) keep their cross-held locks and wait on each
    /// other forever — the livelock a fuzz run surfaced.
    #[tokio::test]
    async fn already_locked_requires_every_needed_lock() {
        let (tm, tctx) = new_algo().await;
        let k1 = paths::from_key(TEST_COLL, b"k1");
        let k2 = paths::from_key(TEST_COLL, b"k2");

        // Create both keys committed and unlocked on a separate client.
        let (tm2, _t2) = new_algo_from_backend(tctx.backend.clone()).await;
        let setup = commit_writes(&tm2, vec![wa(&k1, b"0"), wa(&k2, b"0")]).await;
        flush_writes(&tm2, &setup).await;

        // A transaction that writes both keys needs a write lock on each.
        let h = tm.begin(Data {
            reads: Vec::new(),
            writes: vec![wa(&k1, b"1"), wa(&k2, b"1")],
        });
        tctx.tmon.begin_tx(h.id());
        let vstate = init_validation(&h);

        // Holding only one of the two needed locks is a partial set.
        tctx.locker.lock_write(&k1, h.id()).await.unwrap();
        assert!(
            !tm.already_locked(&vstate, &h),
            "a partial lock set must not count as already locked"
        );

        // With every needed lock held, the serial re-lock is a no-op.
        tctx.locker.lock_write(&k2, h.id()).await.unwrap();
        assert!(
            tm.already_locked(&vstate, &h),
            "holding every needed lock should count as already locked"
        );
    }

    #[tokio::test]
    async fn change_writes_clean_abort() {
        let (tm, tctx) = new_algo().await;
        let keys = [
            paths::from_key(TEST_COLL, b"k1"),
            paths::from_key(TEST_COLL, b"k2"),
            paths::from_key(TEST_COLL, b"k3"),
        ];

        let (tm2, _t2) = new_algo_from_backend(tctx.backend.clone()).await;
        let h = commit_writes(
            &tm2,
            vec![wa(&keys[0], b"0"), wa(&keys[1], b"0"), wa(&keys[2], b"0")],
        )
        .await;
        flush_writes(&tm2, &h).await;

        let reads = do_reads(&tctx, &[&keys[0], &keys[1]]).await;
        commit_writes(&tm2, vec![wa(&keys[0], b"x")]).await;

        let mut h = tm.begin(Data {
            reads,
            writes: vec![wa(&keys[0], b"1"), wa(&keys[1], b"1")],
        });
        let err = tm.commit(&mut h).await.unwrap_err();
        assert!(err.is_retry(), "expected retry, got {err:?}");

        let reads = do_reads(&tctx, &[&keys[1], &keys[2]]).await;
        commit_writes(&tm2, vec![wa(&keys[2], b"y")]).await;

        tm.reset(
            &mut h,
            Data {
                reads,
                writes: vec![wa(&keys[0], b"1")],
            },
        );
        let err = tm.commit(&mut h).await.unwrap_err();
        assert!(err.is_retry(), "expected retry, got {err:?}");

        tm.end(&mut h).await.unwrap();

        // The keys should be lockable now.
        let txtest = TxId::new_random();
        tctx.tmon.begin_tx(&txtest);
        for key in &keys {
            tctx.locker.lock_write(key, &txtest).await.unwrap();
            tctx.locker.unlock(key, &txtest).await.unwrap();
        }
        tctx.tmon.abort_tx(&txtest).await.unwrap();
    }

    #[tokio::test]
    async fn clean_abort() {
        for num_writes in 1..=3 {
            let (tm, tctx) = new_algo().await;
            let (tm2, _t2) = new_algo_from_backend(tctx.backend.clone()).await;

            let keys: Vec<String> = (0..num_writes)
                .map(|i| paths::from_key(TEST_COLL, format!("k{i}").as_bytes()))
                .collect();

            let writes: Vec<WriteAccess> = keys.iter().map(|k| wa(k, b"0")).collect();
            let h = commit_writes(&tm2, writes).await;
            flush_writes(&tm2, &h).await;

            let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
            let reads = do_reads(&tctx, &key_refs).await;

            // Change the last value from another algo.
            commit_writes(&tm2, vec![wa(&keys[num_writes - 1], b"x")]).await;

            let writes: Vec<WriteAccess> = keys.iter().map(|k| wa(k, b"1")).collect();
            let mut h = tm.begin(Data { reads, writes });
            let err = tm.commit(&mut h).await.unwrap_err();
            assert!(err.is_retry(), "[{num_writes}] expected retry, got {err:?}");

            tm.end(&mut h).await.unwrap();

            // The keys should be lockable now.
            let txtest = TxId::new_random();
            tctx.tmon.begin_tx(&txtest);
            for key in &keys {
                tctx.locker.lock_write(key, &txtest).await.unwrap();
                tctx.locker.unlock(key, &txtest).await.unwrap();
            }
            tctx.tmon.abort_tx(&txtest).await.unwrap();
        }
    }

    #[tokio::test]
    async fn readonly_from_uncommitted() {
        let (tm, tctx) = new_algo().await;
        let keyp = paths::from_key(TEST_COLL, b"k");
        let val = b"v";

        let wh = commit_writes(&tm, vec![wa(&keyp, b"xxx")]).await;
        flush_writes(&tm, &wh).await;

        let ra1 = do_read(&tctx, &keyp).await;

        let wh = commit_writes(&tm, vec![wa(&keyp, val)]).await;
        flush_writes(&tm, &wh).await;

        // A transaction tries to update from a stale read; it fails and leaves
        // the item locked in write.
        let mut h1 = tm.begin(Data {
            reads: vec![ra1.clone()],
            writes: vec![wa(&keyp, b"tmpw")],
        });
        let err = tm.commit(&mut h1).await.unwrap_err();
        assert!(err.is_retry(), "expected retry, got {err:?}");

        let info = lock_info(&tctx, &keyp).await;
        assert_eq!(info.typ, LockType::Write);
        assert_eq!(info.locked_by[0], *h1.id());

        // The read-only transition arrives with the old read and asks for retry.
        let mut h2 = tm.begin(Data {
            reads: vec![ra1],
            writes: Vec::new(),
        });
        let err = tm.commit(&mut h2).await.unwrap_err();
        assert!(err.is_retry(), "expected retry, got {err:?}");

        let ra2 = do_read(&tctx, &keyp).await;
        assert_eq!(ra2.version.last_writer, *wh.id());
    }

    /// Variant of [`new_algo_from_backend`] that wires a real [`Background`]
    /// into the [`Algo`] so `async_cleanup` actually runs.
    async fn new_algo_with_bg() -> (Algo, Tctx, Arc<Background>, Gc) {
        let b = Arc::new(MemoryBackend::new());
        let local = Local::new(1024);
        let global = Global::new(b.clone(), local.clone());
        let tlogger = TLogger::new(global.clone(), local.clone(), TEST_COLL);
        let bg = Arc::new(Background::new());
        let bg_weak = Arc::downgrade(&bg);
        let tmon = Monitor::new(local.clone(), tlogger.clone(), bg_weak.clone());
        let locker = Locker::new(local.clone(), global.clone(), tmon.clone());
        let gc = Gc::new(bg_weak.clone(), tlogger.clone());

        global
            .write(
                &paths::collection_info(TEST_COLL),
                COLL_INFO.to_vec(),
                Tags::new(),
            )
            .await
            .unwrap();

        let algo = Algo::new(
            global.clone(),
            local.clone(),
            locker.clone(),
            tmon.clone(),
            gc.clone(),
            Some(bg_weak),
        );
        (
            algo,
            Tctx {
                backend: b,
                global,
                local,
                tlogger,
                tmon,
                locker,
            },
            bg,
            gc,
        )
    }

    /// Yield-driven polling for background side effects. Yields between checks
    /// so spawned tasks get a scheduler slice without consuming virtual time
    /// (so it composes with `start_paused` tests that later advance the clock
    /// via `tokio::time::sleep`).
    async fn wait_for(label: &str, mut cond: impl FnMut() -> bool) {
        for _ in 0..10_000 {
            if cond() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("timed out waiting for: {label}");
    }

    /// Happy path: after a normal commit, the spawned cleanup task releases
    /// every lock the transaction held, both in the local cache and in
    /// storage. We never call `unlock_all` ourselves.
    #[tokio::test]
    async fn async_cleanup_releases_locks() {
        let (tm, tctx, bg, _gc) = new_algo_with_bg().await;
        let k1 = paths::from_key(TEST_COLL, b"k1");
        let k2 = paths::from_key(TEST_COLL, b"k2");

        let mut h = tm.begin(Data {
            reads: Vec::new(),
            writes: vec![wa(&k1, b"v1"), wa(&k2, b"v2")],
        });
        tm.commit(&mut h).await.unwrap();
        let tid = h.id().clone();

        wait_for("async cleanup released the local lock set", || {
            tctx.locker.locked_paths(&tid).is_empty()
        })
        .await;

        for k in [&k1, &k2] {
            let info = lock_info(&tctx, k).await;
            assert_eq!(info.typ, LockType::None, "{k} still locked in storage");
            assert_eq!(info.last_writer, tid);
        }

        drop(bg);
    }

    /// On success the cleanup schedules the transaction log for delayed GC.
    /// We exercise the GC loop end-to-end: time auto-advances past the
    /// cleanup interval and the log must be deleted from storage.
    #[tokio::test(start_paused = true)]
    async fn async_cleanup_schedules_tx_log_gc() {
        let (tm, tctx, bg, gc) = new_algo_with_bg().await;
        gc.start();
        let k1 = paths::from_key(TEST_COLL, b"k1");

        let mut h = tm.begin(Data {
            reads: Vec::new(),
            writes: vec![wa(&k1, b"v")],
        });
        tm.commit(&mut h).await.unwrap();
        let tid = h.id().clone();

        assert!(
            tctx.tlogger.get(&tid).await.is_ok(),
            "tx log must exist right after commit"
        );

        wait_for("async cleanup finished unlocking", || {
            tctx.locker.locked_paths(&tid).is_empty()
        })
        .await;

        // Two GC cleanup intervals plus slack: scheduling adds an item with
        // `due = now + CLEANUP_INTERVAL`, and the loop drains items every
        // `CLEANUP_INTERVAL`, so a single tick can miss it depending on
        // ordering. Two ticks is always enough.
        tokio::time::sleep(Duration::from_secs(180)).await;

        let err = tctx.tlogger.get(&tid).await.unwrap_err();
        assert!(err.is_not_found(), "expected log deleted, got {err:?}");

        drop(bg);
    }

    /// Best-effort guarantee: tearing down the `Background` while a cleanup is
    /// still in flight must not hang. `Drop` aborts the cleanup synchronously;
    /// the in-flight unlocks are dropped at their next `.await`.
    #[tokio::test]
    async fn async_cleanup_does_not_block_background_close() {
        let (tm, _tctx, bg, _gc) = new_algo_with_bg().await;
        let k1 = paths::from_key(TEST_COLL, b"k1");

        let mut h = tm.begin(Data {
            reads: Vec::new(),
            writes: vec![wa(&k1, b"v")],
        });
        tm.commit(&mut h).await.unwrap();

        // Dropping `bg` races the freshly-spawned cleanup task. `Drop` is
        // synchronous and aborts every tracked handle; nothing can hang.
        drop(bg);
    }

    /// `async_abort` is the fire-and-forget abort used when a transaction's
    /// future is dropped between `begin` and `end`: the tx log entry must end
    /// up marked Aborted without the caller awaiting anything. We `begin` a
    /// transaction (skipping `commit` to leave it unfinalized) and verify the
    /// stored status flips to `Aborted` after the background task runs.
    #[tokio::test]
    async fn async_abort_marks_tx_aborted() {
        let (tm, tctx, bg, _gc) = new_algo_with_bg().await;
        let h = tm.begin(Data {
            reads: Vec::new(),
            writes: vec![wa(&paths::from_key(TEST_COLL, b"k1"), b"v")],
        });
        let tid = h.id().clone();

        tm.async_abort(&tid);

        for _ in 0..10_000 {
            let cs = tctx.tlogger.commit_status(&tid).await.unwrap();
            if cs.status == glassdb_storage::TxCommitStatus::Aborted {
                drop(bg);
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("async_abort never marked tx as Aborted");
    }
}
