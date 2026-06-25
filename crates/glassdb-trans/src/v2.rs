//! Minimal v2 transaction engine (ADR-016 … ADR-021).
//!
//! This is a self-contained, correctness-first implementation of the
//! object-storage-native layout, built **alongside** the v1 engine so the
//! existing suite keeps passing while v2 is validated end to end. It talks
//! directly to a [`Backend`] using only the slimmed primitives ADR-023 targets
//! (`read` / `write_if` / `write_if_not_exists` / `delete` / `list`) and the
//! pure data types from [`glassdb_storage`] (`Shard`, `CollectionRoot`, the
//! `txobject` codec).
//!
//! Protocol (ADR-020): a read-write transaction creates a pending transaction
//! object, validates its reads and installs its locks with one read-modify-write
//! CAS per shard (plus the collection root for membership changes), flips the
//! transaction object to committed (the commit point), then publishes
//! `current_writer` pointers and releases locks (write-back).
//!
//! Concurrency control (ADR-002 / ADR-021): strict two-phase locking with
//! wound-wait. The "wait" of wound-wait is realised as **abort-and-retry with a
//! preserved-or-fresh priority**: a transaction that meets an
//! older-or-equal live holder aborts and retries (so it never holds locks while
//! blocked, which also makes the protocol deadlock-free), while an older
//! transaction wounds the younger holder (CAS its object pending → aborted) and
//! proceeds. Committed holders are resolved by help-forward; aborted holders are
//! cleared lazily by the next contender.
//!
//! Deliberate simplifications, each a follow-up:
//! - TODO(ADR-021 leases): no lease/refresh, so a *crashed* transaction's locks
//!   are never reclaimed (only live conflicts are). Recovery/crash tests need
//!   this. The lease is the pending object's timestamp, already written here.
//! - TODO(ADR-022 GC): aborted/unreferenced transaction objects and empty shard
//!   entries are never collected; write-back is synchronous (no mark-sweep).
//! - TODO(perf): no caching layer (every read hits the backend), no batched or
//!   async write-back, no proactive lock release on abort, fresh pending object
//!   per attempt. Performance is explicitly out of scope for v1.
//! - TODO(ADR-020 serial fallback): equal-priority deadlocks rely on a byte
//!   tiebreaker rather than the serial sorted-by-path re-acquisition; correct for
//!   distinct priorities, best-effort under a frozen clock.
//! - TODO(cutover): re-point the DST oracles (serializability, cycle ring) at
//!   this engine, then retire the v1 tag-based engine and slim the `Backend`
//!   trait (ADR-023).

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use glassdb_backend::{Backend, BackendError, Tags, Version};
use glassdb_concurr::{RetryConfig, rt};
use glassdb_data::shard::{SHARD_COUNT, shard_index};
use glassdb_data::{TxId, paths};
use glassdb_storage::{
    CollectionRoot, LockType, Shard, ShardEntry, StorageError, TxCommitStatus, TxLog, TxWrite,
    txobject,
};

use crate::error::TransError;

/// Maximum number of execute → commit attempts before giving up.
const MAX_ATTEMPTS: usize = 200;
/// Maximum inner CAS retries on a single shard/root before treating the
/// operation as conflicted and restarting the transaction.
const CAS_RETRIES: usize = 50;

/// A staged write within a transaction.
#[derive(Debug, Clone)]
enum WriteOp {
    Put(Vec<u8>),
    Delete,
}

/// What a read observed, for snapshot reads and optimistic validation.
#[derive(Debug, Clone)]
struct ReadRecord {
    /// The effective writer txid at read time (the validation token), or `None`
    /// if the key was absent.
    writer: Option<TxId>,
    /// The value returned to the caller, cached for read-your-reads snapshot
    /// consistency within the transaction.
    value: Option<Vec<u8>>,
}

#[derive(Default)]
struct TxState {
    reads: HashMap<Vec<u8>, ReadRecord>,
    writes: BTreeMap<Vec<u8>, WriteOp>,
}

/// A handle to one collection, the unit the v2 engine operates on.
#[derive(Clone)]
pub struct Collection {
    inner: Arc<CollInner>,
}

struct CollInner {
    backend: Arc<dyn Backend>,
    /// Storage path prefix for this collection's objects (e.g. `db/users`).
    prefix: String,
}

impl std::fmt::Debug for Collection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Collection")
            .field("prefix", &self.inner.prefix)
            .finish()
    }
}

/// The keys a transaction touches in one shard.
#[derive(Default)]
struct ShardGroup {
    read_only: Vec<Vec<u8>>,
    writes: Vec<(Vec<u8>, WriteOp)>,
}

/// Outcome of acquiring locks on a single shard.
enum ShardOutcome {
    /// Locked; `membership` is true if the shard saw a create/delete.
    Locked { membership: bool },
    /// The transaction must restart (conflict / lost wound-wait).
    Retry,
}

/// The lock a transaction wants on a key's entry.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Desired {
    Read,
    Put,
    Delete,
}

impl Collection {
    /// Opens `prefix` as a collection, creating its root if absent (ADR-018:
    /// existence == the root exists). A lost create race is treated as success.
    pub async fn create(
        backend: Arc<dyn Backend>,
        prefix: impl Into<String>,
    ) -> Result<Self, TransError> {
        let coll = Collection {
            inner: Arc::new(CollInner {
                backend,
                prefix: prefix.into(),
            }),
        };
        let root = CollectionRoot::new(SHARD_COUNT);
        let path = coll.root_path();
        match coll
            .backend()
            .write_if_not_exists(&path, root.encode(), Tags::new())
            .await
        {
            Ok(_) | Err(BackendError::Precondition) => Ok(coll),
            Err(e) => Err(e.into()),
        }
    }

    /// Opens an existing collection without creating it.
    pub async fn open(
        backend: Arc<dyn Backend>,
        prefix: impl Into<String>,
    ) -> Result<Self, TransError> {
        let coll = Collection {
            inner: Arc::new(CollInner {
                backend,
                prefix: prefix.into(),
            }),
        };
        match coll.backend().read(&coll.root_path()).await {
            Ok(_) => Ok(coll),
            // A missing root object means the collection does not exist.
            Err(BackendError::NotFound) => Err(StorageError::NotFound.into()),
            Err(e) => Err(e.into()),
        }
    }

    fn backend(&self) -> &Arc<dyn Backend> {
        &self.inner.backend
    }

    fn prefix(&self) -> &str {
        &self.inner.prefix
    }

    fn root_path(&self) -> String {
        paths::collection_info(self.prefix())
    }

    fn key_path(&self, key: &[u8]) -> String {
        paths::from_key(self.prefix(), key)
    }

    fn shard_path(&self, idx: u32) -> String {
        paths::from_shard(self.prefix(), idx)
    }

    fn tx_path(&self, id: &TxId) -> String {
        paths::from_transaction(self.prefix(), id)
    }

    // --- Public single-key + transactional API -----------------------------

    /// Reads a key, returning its value or `None` if absent. A standalone read
    /// is linearizable; use [`Collection::transact`] for multi-key isolation.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, TransError> {
        Ok(self.read_value(key).await?.0)
    }

    /// Writes (creates or overwrites) a key.
    pub async fn put(&self, key: &[u8], value: Vec<u8>) -> Result<(), TransError> {
        let key = key.to_vec();
        self.transact(move |tx| {
            let key = key.clone();
            let value = value.clone();
            async move {
                tx.put(&key, value);
                Ok(())
            }
        })
        .await
    }

    /// Deletes a key (a no-op if it does not exist).
    pub async fn delete(&self, key: &[u8]) -> Result<(), TransError> {
        let key = key.to_vec();
        self.transact(move |tx| {
            let key = key.clone();
            async move {
                tx.delete(&key);
                Ok(())
            }
        })
        .await
    }

    /// Runs a transaction body, re-executing it on conflict until it commits or
    /// the retry budget is exhausted. The body stages reads/writes through [`Tx`].
    pub async fn transact<F, Fut, T>(&self, f: F) -> Result<T, TransError>
    where
        F: Fn(Tx) -> Fut,
        Fut: Future<Output = Result<T, TransError>>,
    {
        let mut backoff = RetryConfig::default().backoff();
        for _ in 0..MAX_ATTEMPTS {
            let id = TxId::new_at(rt::system_now());
            let tx = Tx::new(self.clone());
            let value = f(tx.clone()).await?;
            let (reads, writes) = tx.snapshot();

            let committed = if writes.is_empty() {
                self.validate_readonly(&reads).await?
            } else {
                self.commit_once(&id, &reads, &writes).await?
            };
            if committed {
                return Ok(value);
            }
            rt::sleep(backoff.next_delay()).await;
        }
        Err(retry_exhausted())
    }

    /// Lists the existing keys, as a consistent snapshot (ADR-018: optimistic on
    /// the root version).
    pub async fn list(&self) -> Result<Vec<Vec<u8>>, TransError> {
        for _ in 0..MAX_ATTEMPTS {
            let (_, v0) = self.read_root().await?;
            let mut keys = Vec::new();
            let shard_paths = self
                .backend()
                .list(&paths::shards_prefix(self.prefix()))
                .await?;
            for sp in &shard_paths {
                let r = match self.backend().read(sp).await {
                    Ok(r) => r,
                    Err(BackendError::NotFound) => continue,
                    Err(e) => return Err(e.into()),
                };
                let shard = Shard::decode(&r.contents)?;
                for entry in shard.entries() {
                    if self.read_value(&entry.key).await?.0.is_some() {
                        keys.push(entry.key.clone());
                    }
                }
            }
            let (_, v1) = self.read_root().await?;
            if v0 == v1 {
                keys.sort();
                keys.dedup();
                return Ok(keys);
            }
        }
        Err(retry_exhausted())
    }

    // --- Shard / root / tx-object I/O --------------------------------------

    /// Loads a shard, returning the empty shard with no version if it does not
    /// exist yet (shards are created lazily on first write).
    async fn load_shard(&self, idx: u32) -> Result<(Shard, Option<Version>), TransError> {
        match self.backend().read(&self.shard_path(idx)).await {
            Ok(r) => Ok((Shard::decode(&r.contents)?, Some(r.version))),
            Err(BackendError::NotFound) => Ok((Shard::new(), None)),
            Err(e) => Err(e.into()),
        }
    }

    /// CAS-stores a shard. `expected = None` means create-if-absent. Returns
    /// `false` on a precondition miss (the caller reloads and retries).
    async fn store_shard(
        &self,
        idx: u32,
        entries: BTreeMap<Vec<u8>, ShardEntry>,
        expected: Option<&Version>,
    ) -> Result<bool, TransError> {
        let body = Shard::from_entries(entries.into_values()).encode();
        let path = self.shard_path(idx);
        let res = match expected {
            Some(v) => self.backend().write_if(&path, body, v, Tags::new()).await,
            None => {
                self.backend()
                    .write_if_not_exists(&path, body, Tags::new())
                    .await
            }
        };
        match res {
            Ok(_) => Ok(true),
            Err(BackendError::Precondition) | Err(BackendError::NotFound) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    async fn read_root(&self) -> Result<(CollectionRoot, Version), TransError> {
        match self.backend().read(&self.root_path()).await {
            Ok(r) => Ok((CollectionRoot::decode(&r.contents)?, r.version)),
            Err(BackendError::NotFound) => Err(StorageError::NotFound.into()),
            Err(e) => Err(e.into()),
        }
    }

    /// Reads the status (and version) of a transaction object; `Unknown` if it
    /// does not exist.
    async fn tx_status(&self, id: &TxId) -> Result<(TxCommitStatus, Option<Version>), TransError> {
        match self.backend().read(&self.tx_path(id)).await {
            Ok(r) => {
                let obj = txobject::decode(id, &r.contents)?;
                Ok((obj.status, Some(r.version)))
            }
            Err(BackendError::NotFound) => Ok((TxCommitStatus::Unknown, None)),
            Err(e) => Err(e.into()),
        }
    }

    async fn read_tx_object(&self, id: &TxId) -> Result<Option<TxLog>, TransError> {
        match self.backend().read(&self.tx_path(id)).await {
            Ok(r) => Ok(Some(txobject::decode(id, &r.contents)?)),
            Err(BackendError::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Creates the pending transaction object (ADR-019/020 Prepare). Its
    /// timestamp is the lease anchor (ADR-021).
    async fn create_pending(&self, id: &TxId, ts: SystemTime) -> Result<Version, TransError> {
        let log = TxLog {
            id: id.clone(),
            timestamp: Some(ts),
            status: TxCommitStatus::Pending,
            writes: Vec::new(),
            // TODO(ADR-021): record lock intentions here for richer recovery; the
            // current resolution path only needs the status.
            locks: Vec::new(),
        };
        let body = txobject::encode(&log)?;
        let m = self
            .backend()
            .write_if_not_exists(&self.tx_path(id), body, Tags::new())
            .await?;
        Ok(m.version)
    }

    /// The single commit CAS: flips pending → committed with the value map
    /// (ADR-020). Returns `false` if the object was no longer pending (wounded).
    async fn flip_committed(
        &self,
        id: &TxId,
        ts: SystemTime,
        writes: &BTreeMap<Vec<u8>, WriteOp>,
        pending_v: &Version,
    ) -> Result<bool, TransError> {
        let mut wv = Vec::new();
        for (k, op) in writes {
            let (value, deleted) = match op {
                WriteOp::Put(v) => (Arc::from(v.as_slice()), false),
                WriteOp::Delete => (Arc::from(&[] as &[u8]), true),
            };
            wv.push(TxWrite {
                path: self.key_path(k),
                value,
                deleted,
                // TODO(ADR-022): record prev_writer for the GC version chain.
                prev_writer: TxId::default(),
            });
        }
        let log = TxLog {
            id: id.clone(),
            timestamp: Some(ts),
            status: TxCommitStatus::Ok,
            writes: wv,
            locks: Vec::new(),
        };
        let body = txobject::encode(&log)?;
        match self
            .backend()
            .write_if(&self.tx_path(id), body, pending_v, Tags::new())
            .await
        {
            Ok(_) => Ok(true),
            Err(BackendError::Precondition) | Err(BackendError::NotFound) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Best-effort marks our own transaction aborted, so the locks it installed
    /// resolve as dead and are cleared lazily by the next contender.
    async fn mark_aborted(&self, id: &TxId, ts: SystemTime) {
        let log = TxLog {
            id: id.clone(),
            timestamp: Some(ts),
            status: TxCommitStatus::Aborted,
            writes: Vec::new(),
            locks: Vec::new(),
        };
        if let Ok(body) = txobject::encode(&log) {
            let _ = self
                .backend()
                .write(&self.tx_path(id), body, Tags::new())
                .await;
        }
    }

    /// Forces a victim transaction pending → aborted (the wound CAS, ADR-021).
    /// Returns the resulting status: `Aborted` if we (or someone) wounded it,
    /// `Ok` if it committed first (we lose the race).
    async fn wound(&self, victim: &TxId, ts: SystemTime) -> Result<TxCommitStatus, TransError> {
        for _ in 0..CAS_RETRIES {
            let (status, ver) = self.tx_status(victim).await?;
            match status {
                TxCommitStatus::Ok | TxCommitStatus::Aborted => return Ok(status),
                _ => {}
            }
            let log = TxLog {
                id: victim.clone(),
                timestamp: Some(ts),
                status: TxCommitStatus::Aborted,
                writes: Vec::new(),
                locks: Vec::new(),
            };
            let body = txobject::encode(&log)?;
            let path = self.tx_path(victim);
            let res = match &ver {
                Some(v) => self.backend().write_if(&path, body, v, Tags::new()).await,
                // Missing object (in-doubt create / GC'd): claim the abort.
                None => {
                    self.backend()
                        .write_if_not_exists(&path, body, Tags::new())
                        .await
                }
            };
            match res {
                Ok(_) => return Ok(TxCommitStatus::Aborted),
                Err(BackendError::Precondition) | Err(BackendError::NotFound) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(TxCommitStatus::Pending)
    }

    // --- Read resolution ---------------------------------------------------

    /// Resolves the effective current writer of an entry: the committed holder of
    /// an exclusive lock (committed-but-not-written-back), else `current_writer`.
    async fn effective_writer(
        &self,
        entry: Option<&ShardEntry>,
    ) -> Result<Option<TxId>, TransError> {
        let Some(e) = entry else {
            return Ok(None);
        };
        if matches!(e.lock_type, LockType::Write | LockType::Create)
            && let Some(holder) = e.locked_by.first()
        {
            let (status, _) = self.tx_status(holder).await?;
            if status == TxCommitStatus::Ok {
                return Ok(Some(holder.clone()));
            }
        }
        Ok(e.current_writer.clone())
    }

    /// Reads a key's effective value and the writer it resolved through (the
    /// validation token).
    async fn read_value(&self, key: &[u8]) -> Result<(Option<Vec<u8>>, Option<TxId>), TransError> {
        let (shard, _) = self.load_shard(shard_index(key)).await?;
        let writer = self.effective_writer(shard.lookup(key)).await?;
        let Some(tid) = writer.clone() else {
            return Ok((None, None));
        };
        let obj = self.read_tx_object(&tid).await?;
        let key_path = self.key_path(key);
        let value = match obj
            .as_ref()
            .and_then(|o| txobject::find_write(o, &key_path))
        {
            Some(w) if w.deleted => None,
            Some(w) => Some(w.value.to_vec()),
            None => None,
        };
        Ok((value, writer))
    }

    // --- Commit ------------------------------------------------------------

    async fn validate_readonly(
        &self,
        reads: &HashMap<Vec<u8>, ReadRecord>,
    ) -> Result<bool, TransError> {
        for (key, rec) in reads {
            let (shard, _) = self.load_shard(shard_index(key)).await?;
            let writer = self.effective_writer(shard.lookup(key)).await?;
            if writer != rec.writer {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// One commit attempt. Returns `true` if committed, `false` if the
    /// transaction must restart.
    async fn commit_once(
        &self,
        id: &TxId,
        reads: &HashMap<Vec<u8>, ReadRecord>,
        writes: &BTreeMap<Vec<u8>, WriteOp>,
    ) -> Result<bool, TransError> {
        let ts = rt::system_now();
        let pending_v = self.create_pending(id, ts).await?;

        // Group accessed keys by shard.
        let mut groups: BTreeMap<u32, ShardGroup> = BTreeMap::new();
        for (k, op) in writes {
            groups
                .entry(shard_index(k))
                .or_default()
                .writes
                .push((k.clone(), op.clone()));
        }
        for k in reads.keys() {
            if !writes.contains_key(k) {
                groups
                    .entry(shard_index(k))
                    .or_default()
                    .read_only
                    .push(k.clone());
            }
        }

        // Validate + lock each shard (S2PL acquisition).
        let mut membership = false;
        for (idx, group) in &groups {
            match self.lock_shard(id, *idx, group, reads, ts).await? {
                ShardOutcome::Locked { membership: m } => membership |= m,
                ShardOutcome::Retry => {
                    self.mark_aborted(id, ts).await;
                    return Ok(false);
                }
            }
        }

        // Membership changes also lock the collection root (ADR-018).
        if membership && !self.lock_root(id, ts).await? {
            self.mark_aborted(id, ts).await;
            return Ok(false);
        }

        // Commit point.
        if !self.flip_committed(id, ts, writes, &pending_v).await? {
            // Wounded between lock and commit; the object is already aborted.
            return Ok(false);
        }

        // Write-back: publish pointers and release locks (idempotent).
        // TODO: Make this async and best-effort.
        self.write_back(id, &groups, membership).await?;
        Ok(true)
    }

    /// Validates reads and installs this transaction's locks in one shard, with
    /// one read-modify-write CAS (retried on contention).
    async fn lock_shard(
        &self,
        id: &TxId,
        idx: u32,
        group: &ShardGroup,
        reads: &HashMap<Vec<u8>, ReadRecord>,
        ts: SystemTime,
    ) -> Result<ShardOutcome, TransError> {
        for _ in 0..CAS_RETRIES {
            let (shard, ver) = self.load_shard(idx).await?;
            let mut entries: BTreeMap<Vec<u8>, ShardEntry> = shard
                .entries()
                .cloned()
                .map(|e| (e.key.clone(), e))
                .collect();
            let mut membership = false;
            let mut conflict = false;

            // Resolve holders, validate reads, and install locks per key. Read
            // validation happens *after* help-forward (inside `resolve_and_lock`)
            // so a holder that commits between a separate validate and lock step
            // cannot mask a stale read.
            for key in &group.read_only {
                let observed = reads.get(key).map(|r| &r.writer);
                match self
                    .resolve_and_lock(
                        id,
                        key,
                        Desired::Read,
                        entries.get(key).cloned(),
                        observed,
                        ts,
                    )
                    .await?
                {
                    Some((entry, _)) => {
                        entries.insert(key.clone(), entry);
                    }
                    None => {
                        conflict = true;
                        break;
                    }
                }
            }

            if !conflict {
                for (key, op) in &group.writes {
                    let desired = match op {
                        WriteOp::Put(_) => Desired::Put,
                        WriteOp::Delete => Desired::Delete,
                    };
                    let observed = reads.get(key).map(|r| &r.writer);
                    match self
                        .resolve_and_lock(id, key, desired, entries.get(key).cloned(), observed, ts)
                        .await?
                    {
                        Some((entry, m)) => {
                            membership |= m;
                            entries.insert(key.clone(), entry);
                        }
                        None => {
                            conflict = true;
                            break;
                        }
                    }
                }
            }

            if conflict {
                return Ok(ShardOutcome::Retry);
            }

            if self.store_shard(idx, entries, ver.as_ref()).await? {
                return Ok(ShardOutcome::Locked { membership });
            }
            // Precondition: the shard changed under us, reload and retry.
        }
        Ok(ShardOutcome::Retry)
    }

    /// Resolves the holders of an entry (help-forward committed, drop aborted,
    /// wound-wait pending) and installs this transaction's lock, returning the
    /// new entry and whether the change is a membership change. `None` means the
    /// transaction must restart.
    async fn resolve_and_lock(
        &self,
        id: &TxId,
        key: &[u8],
        desired: Desired,
        entry: Option<ShardEntry>,
        observed: Option<&Option<TxId>>,
        ts: SystemTime,
    ) -> Result<Option<(ShardEntry, bool)>, TransError> {
        let mut e = entry.unwrap_or_else(|| ShardEntry {
            key: key.to_vec(),
            lock_type: LockType::None,
            locked_by: Vec::new(),
            current_writer: None,
            deleted: false,
        });

        // Resolve existing holders (other than us). Committed holders are
        // help-forwarded (their value published as current_writer, their lock
        // released); aborted/missing holders are dropped; pending holders remain
        // as live conflicts.
        let key_path = self.key_path(key);
        let mut pending: Vec<TxId> = Vec::new();
        for holder in e.locked_by.clone() {
            if &holder == id {
                continue;
            }
            let (status, _) = self.tx_status(&holder).await?;
            match status {
                TxCommitStatus::Ok => {
                    if let Some(obj) = self.read_tx_object(&holder).await?
                        && let Some(w) = txobject::find_write(&obj, &key_path)
                    {
                        e.current_writer = Some(holder.clone());
                        e.deleted = w.deleted;
                    }
                }
                TxCommitStatus::Pending => pending.push(holder),
                // Aborted / Unknown(missing): drop.
                _ => {}
            }
        }

        // Validate the read against the resolved state: a reader sees the
        // committed `current_writer` (pending holders are invisible), so the
        // value the transaction read must still be the effective one. Doing this
        // after help-forward closes the validate-then-commit race where a holder
        // commits between a separate validation and this lock step.
        if let Some(obs) = observed
            && &e.current_writer != obs
        {
            return Ok(None);
        }

        let exists_before = e.current_writer.is_some() && !e.deleted;

        // Compatibility: read locks share with other read holders; everything
        // else is exclusive.
        let compatible = matches!(desired, Desired::Read)
            && !matches!(e.lock_type, LockType::Write | LockType::Create);

        if !compatible {
            // Must clear every remaining pending holder via wound-wait.
            for holder in &pending {
                if !self.win_wound_wait(id, holder, ts).await? {
                    return Ok(None);
                }
            }
            pending.clear();
        }

        let membership = match desired {
            Desired::Put => !exists_before,
            Desired::Delete => exists_before,
            Desired::Read => false,
        };

        match desired {
            Desired::Read => {
                if !pending.contains(id) && !e.locked_by.contains(id) {
                    pending.push(id.clone());
                }
                e.locked_by = pending;
                e.lock_type = LockType::Read;
            }
            Desired::Put | Desired::Delete => {
                e.locked_by = vec![id.clone()];
                e.lock_type = if exists_before {
                    LockType::Write
                } else if matches!(desired, Desired::Put) {
                    LockType::Create
                } else {
                    LockType::Write
                };
            }
        }
        Ok(Some((e, membership)))
    }

    /// Wound-wait decision against a live pending `holder`: returns whether *we*
    /// win (the holder is now aborted, so we may take the lock). A loss means we
    /// must restart.
    async fn win_wound_wait(
        &self,
        id: &TxId,
        holder: &TxId,
        ts: SystemTime,
    ) -> Result<bool, TransError> {
        if !should_wound(id, holder) {
            return Ok(false);
        }
        Ok(self.wound(holder, ts).await? == TxCommitStatus::Aborted)
    }

    /// Acquires the collection root's membership write lock (ADR-018), with the
    /// same resolve/wound-wait rules. Returns `false` if the transaction must
    /// restart.
    async fn lock_root(&self, id: &TxId, ts: SystemTime) -> Result<bool, TransError> {
        for _ in 0..CAS_RETRIES {
            let (mut root, ver) = self.read_root().await?;
            let mut pending: Vec<TxId> = Vec::new();
            for holder in root.membership_locked_by().to_vec() {
                if &holder == id {
                    continue;
                }
                // Committed/aborted/missing membership holders are simply
                // released (their root write-back is idempotent); only pending
                // ones are live conflicts.
                if self.tx_status(&holder).await?.0 == TxCommitStatus::Pending {
                    pending.push(holder);
                }
            }
            let mut lost = false;
            for holder in &pending {
                if !self.win_wound_wait(id, holder, ts).await? {
                    lost = true;
                    break;
                }
            }
            if lost {
                return Ok(false);
            }
            root.set_membership_lock(LockType::Write, [id.clone()]);
            match self
                .backend()
                .write_if(&self.root_path(), root.encode(), &ver, Tags::new())
                .await
            {
                Ok(_) => return Ok(true),
                Err(BackendError::Precondition) | Err(BackendError::NotFound) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(false)
    }

    /// Publishes `current_writer` pointers / tombstones and releases this
    /// transaction's locks across the shards it touched, then releases the root
    /// membership lock (which bumps the root version, invalidating listers).
    /// Every CAS is idempotent.
    async fn write_back(
        &self,
        id: &TxId,
        groups: &BTreeMap<u32, ShardGroup>,
        membership: bool,
    ) -> Result<(), TransError> {
        for (idx, group) in groups {
            for _ in 0..CAS_RETRIES {
                let (shard, ver) = self.load_shard(*idx).await?;
                let mut entries: BTreeMap<Vec<u8>, ShardEntry> = shard
                    .entries()
                    .cloned()
                    .map(|e| (e.key.clone(), e))
                    .collect();

                for (key, op) in &group.writes {
                    if let Some(e) = entries.get_mut(key)
                        && e.locked_by.contains(id)
                    {
                        e.current_writer = Some(id.clone());
                        e.deleted = matches!(op, WriteOp::Delete);
                        e.locked_by.retain(|h| h != id);
                        if e.locked_by.is_empty() {
                            e.lock_type = LockType::None;
                        }
                    }
                }
                for key in &group.read_only {
                    if let Some(e) = entries.get_mut(key) {
                        e.locked_by.retain(|h| h != id);
                        if e.locked_by.is_empty() {
                            e.lock_type = LockType::None;
                        }
                    }
                }

                if self.store_shard(*idx, entries, ver.as_ref()).await? {
                    break;
                }
            }
        }

        if membership {
            for _ in 0..CAS_RETRIES {
                let (mut root, ver) = self.read_root().await?;
                if root.membership_locked_by().contains(id) {
                    root.clear_membership_lock();
                }
                match self
                    .backend()
                    .write_if(&self.root_path(), root.encode(), &ver, Tags::new())
                    .await
                {
                    Ok(_) => break,
                    Err(BackendError::Precondition) | Err(BackendError::NotFound) => continue,
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Ok(())
    }
}

/// A transaction context handed to a [`Collection::transact`] body. Cheap to
/// clone (shared interior state).
#[derive(Clone)]
pub struct Tx {
    inner: Arc<TxInner>,
}

struct TxInner {
    coll: Collection,
    state: Mutex<TxState>,
}

impl Tx {
    fn new(coll: Collection) -> Self {
        Tx {
            inner: Arc::new(TxInner {
                coll,
                state: Mutex::new(TxState::default()),
            }),
        }
    }

    /// Reads a key within the transaction (read-your-writes, snapshot reads).
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, TransError> {
        {
            let st = self.inner.state.lock().unwrap();
            if let Some(op) = st.writes.get(key) {
                return Ok(match op {
                    WriteOp::Put(v) => Some(v.clone()),
                    WriteOp::Delete => None,
                });
            }
            if let Some(rec) = st.reads.get(key) {
                return Ok(rec.value.clone());
            }
        }
        let (value, writer) = self.inner.coll.read_value(key).await?;
        let mut st = self.inner.state.lock().unwrap();
        st.reads.entry(key.to_vec()).or_insert(ReadRecord {
            writer,
            value: value.clone(),
        });
        Ok(value)
    }

    /// Stages a write.
    pub fn put(&self, key: &[u8], value: Vec<u8>) {
        let mut st = self.inner.state.lock().unwrap();
        st.writes.insert(key.to_vec(), WriteOp::Put(value));
    }

    /// Stages a delete.
    pub fn delete(&self, key: &[u8]) {
        let mut st = self.inner.state.lock().unwrap();
        st.writes.insert(key.to_vec(), WriteOp::Delete);
    }

    fn snapshot(&self) -> (HashMap<Vec<u8>, ReadRecord>, BTreeMap<Vec<u8>, WriteOp>) {
        let st = self.inner.state.lock().unwrap();
        (st.reads.clone(), st.writes.clone())
    }
}

/// Wound-wait priority decision: an older transaction wounds a younger holder.
/// Equal priorities (same timestamp) fall back to a deterministic byte tiebreak
/// so exactly one side wins a given encounter.
/// TODO(ADR-020): replace the tiebreak with the serial sorted-by-path fallback
/// for full correctness under a frozen clock.
fn should_wound(me: &TxId, holder: &TxId) -> bool {
    if me.older(holder) {
        return true;
    }
    if holder.older(me) {
        return false;
    }
    me.as_bytes() < holder.as_bytes()
}

/// The terminal error when a transaction exhausts [`MAX_ATTEMPTS`]. Wound-wait
/// guarantees progress, so hitting this bound indicates pathological contention
/// (or a bug); reuses [`TransError::Other`] rather than a bespoke variant.
fn retry_exhausted() -> TransError {
    TransError::other("transaction retry budget exhausted")
}
