//! Transaction-log persistence. Ported from the Go
//! `internal/storage/tlogger.go`. Logs are protobuf bodies; the commit status
//! and timestamp live in the body itself (ADR-019/ADR-023), not in object tags.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use glassdb_backend as backend;
use glassdb_concurr::rt;
use glassdb_data::{TxId, paths};
use glassdb_proto as pb;
use prost::Message;

use crate::error::StorageError;
use crate::lock::LockType;
use crate::object_cache::{Freshness, ObjectCache};

/// The commit state of a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TxCommitStatus {
    #[default]
    Unknown,
    Ok,
    Aborted,
    Pending,
}

impl TxCommitStatus {
    /// Reports whether the status is terminal (committed or aborted).
    pub fn is_final(self) -> bool {
        matches!(self, TxCommitStatus::Ok | TxCommitStatus::Aborted)
    }
}

/// The full contents of a transaction log entry.
#[derive(Debug, Clone)]
pub struct TxLog {
    pub id: TxId,
    /// `None` means "use the current time when persisting".
    pub timestamp: Option<SystemTime>,
    pub status: TxCommitStatus,
    pub writes: Vec<TxWrite>,
    pub locks: Vec<PathLock>,
}

impl TxLog {
    /// Creates an empty log for the given transaction.
    pub fn new(id: TxId, status: TxCommitStatus) -> Self {
        TxLog {
            id,
            timestamp: None,
            status,
            writes: Vec::new(),
            locks: Vec::new(),
        }
    }
}

/// A single write within a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxWrite {
    pub path: String,
    pub value: Arc<[u8]>,
    pub deleted: bool,
    pub prev_writer: TxId,
}

/// A value written by a transaction, including whether it was a deletion or was
/// not written at all.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TValue {
    pub value: Arc<[u8]>,
    pub deleted: bool,
    /// True when the transaction committed but did not write this value (e.g.
    /// read-only lock).
    pub not_written: bool,
}

/// The commit status of a transaction along with its timestamp and version.
#[derive(Debug, Clone)]
pub struct TxStatus {
    pub status: TxCommitStatus,
    pub last_update: SystemTime,
    pub version: backend::Version,
}

/// One backend page of transaction IDs from a deterministic log shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxListPage {
    pub ids: Vec<TxId>,
    pub next: Option<backend::ListCursor>,
}

/// A storage path together with its lock type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathLock {
    pub path: String,
    pub typ: LockType,
    pub scope: LockScope,
}

/// The coordination namespace a transaction-log lock backreference belongs to.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LockScope {
    #[default]
    Entry,
    Structure,
    Membership,
}

impl std::fmt::Display for LockScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

/// Reads and writes transaction logs under a path prefix.
#[derive(Clone)]
pub struct TLogger {
    prefix: String,
    objects: ObjectCache,
}

impl TLogger {
    /// Creates a logger storing logs under `prefix`.
    pub fn new(objects: ObjectCache, prefix: impl Into<String>) -> Self {
        TLogger {
            prefix: prefix.into(),
            objects,
        }
    }

    /// Returns the commit status of transaction `id`, using the cache when
    /// possible. The status and timestamp are read from the transaction object
    /// body (ADR-019); an absent object means the transaction is unknown.
    pub async fn commit_status(&self, id: &TxId) -> Result<TxStatus, StorageError> {
        let p = paths::from_transaction(&self.prefix, id);
        // A finalized (committed/aborted) log is immutable, so a cached copy can
        // answer without a backend revalidation round-trip.
        if let Some(o) = self.objects.peek(&p)
            && let Ok(ts) = decode_status(&o.value, o.version)
            && ts.status.is_final()
        {
            return Ok(ts);
        }
        match self.objects.read(&p, Freshness::Latest).await {
            Ok(gr) => decode_status(&gr.value, gr.version),
            Err(StorageError::NotFound) => Ok(TxStatus {
                status: TxCommitStatus::Unknown,
                last_update: UNIX_EPOCH,
                version: backend::Version::default(),
            }),
            Err(e) => Err(e),
        }
    }

    /// Reads and parses the full transaction log for `id`, together with its
    /// backend version. The version is the CAS token GC needs to force-abort a
    /// dead pending object and to prune its locks (ADR-022); callers that only
    /// need the log body ignore it.
    ///
    /// A finalized (committed/aborted) log is immutable, so a cached copy — and
    /// its version — is authoritative and answers without a backend round-trip.
    /// A pending log can still change, so it is read through, revalidating via
    /// the version-conditional GET (the backend version changes on every content
    /// write — refresh or finalization — ADR-023).
    pub async fn get(&self, id: &TxId) -> Result<(TxLog, backend::Version), StorageError> {
        let p = paths::from_transaction(&self.prefix, id);
        if let Some(o) = self.objects.peek(&p) {
            let log = decode_tx_log(id, &o.value)?;
            if log.status.is_final() {
                return Ok((log, o.version));
            }
        }
        let gr = self.objects.read(&p, Freshness::Latest).await?;
        Ok((decode_tx_log(id, &gr.value)?, gr.version))
    }

    /// Creates a new transaction log entry, failing if one already exists.
    pub async fn set(&self, l: &TxLog) -> Result<backend::Version, StorageError> {
        let ts = l.timestamp.unwrap_or_else(rt::system_now);
        let buf = marshal_log(l, ts)?;
        self.objects
            .write_if_not_exists(
                &paths::from_transaction(&self.prefix, &l.id),
                Arc::from(buf),
            )
            .await
    }

    /// Updates the log only if its current version matches `expected`.
    pub async fn set_if(
        &self,
        l: &TxLog,
        expected: &backend::Version,
    ) -> Result<backend::Version, StorageError> {
        let ts = l.timestamp.unwrap_or_else(rt::system_now);
        let buf = marshal_log(l, ts)?;
        self.objects
            .write_if(
                &paths::from_transaction(&self.prefix, &l.id),
                Arc::from(buf),
                expected,
            )
            .await
    }

    /// Lists one page of transaction IDs from `shard`.
    pub async fn list_transaction_ids(
        &self,
        shard: usize,
        cursor: Option<&backend::ListCursor>,
        limit: backend::ListLimit,
    ) -> Result<TxListPage, StorageError> {
        let prefix = paths::transaction_shard_prefix(&self.prefix, shard);
        let page = self.objects.list(&prefix, cursor, limit).await?;
        let ids = page
            .objects
            .iter()
            .filter_map(|path| paths::transaction_id_of(path).ok())
            .collect();
        Ok(TxListPage {
            ids,
            next: page.next,
        })
    }

    /// Removes the log for `id`, ignoring not-found errors.
    pub async fn delete(&self, id: &TxId) -> Result<(), StorageError> {
        match self
            .objects
            .delete(&paths::from_transaction(&self.prefix, id))
            .await
        {
            Ok(()) => Ok(()),
            Err(StorageError::NotFound) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

fn write_value(w: &pb::Write) -> Arc<[u8]> {
    match &w.val_delete {
        Some(pb::write::ValDelete::Value(v)) => Arc::from(v.as_slice()),
        _ => Arc::from(&[] as &[u8]),
    }
}

fn write_deleted(w: &pb::Write) -> bool {
    matches!(&w.val_delete, Some(pb::write::ValDelete::Deleted(true)))
}

fn parse_log(buf: &[u8]) -> Result<pb::TransactionLog, StorageError> {
    pb::TransactionLog::decode(buf)
        .map_err(|e| StorageError::with_source("unmarshalling transaction log", e))
}

/// Decodes a transaction-log protobuf body into a [`TxLog`]. The status and
/// timestamp are taken from the body (not tags), which is what the v2 unified
/// transaction object relies on (ADR-019). Shared by [`TLogger::get`] and the
/// v2 [`crate::txobject`] codec.
pub(crate) fn decode_tx_log(id: &TxId, buf: &[u8]) -> Result<TxLog, StorageError> {
    decode_tx_log_from_proto(id, &parse_log(buf)?)
}

fn decode_tx_log_from_proto(id: &TxId, tr: &pb::TransactionLog) -> Result<TxLog, StorageError> {
    let status = match tr.status() {
        pb::transaction_log::Status::Committed => TxCommitStatus::Ok,
        pb::transaction_log::Status::Aborted => TxCommitStatus::Aborted,
        pb::transaction_log::Status::Pending => TxCommitStatus::Pending,
        pb::transaction_log::Status::Default => {
            return Err(StorageError::other("unknown commit status"));
        }
    };
    let mut res = TxLog {
        id: id.clone(),
        timestamp: tr.timestamp.map(proto_ts_to_system),
        status,
        writes: Vec::new(),
        locks: Vec::new(),
    };

    for cw in &tr.writes {
        for w in &cw.writes {
            res.writes.push(TxWrite {
                path: format!("{}/{}", cw.prefix, w.suffix),
                value: write_value(w),
                deleted: write_deleted(w),
                prev_writer: TxId::from_bytes(w.prev_tid.clone()),
            });
        }
        if let Some(locks) = &cw.locks {
            // A collection lock is present only when set to a real lock type. The
            // proto default is UNKNOWN(0) (e.g. the empty `CollectionLocks` a
            // key-only write group carries), which must not decode as a spurious
            // collection lock.
            let clt = locks.collection_lock;
            if clt != pb::lock::LockType::None as i32 && clt != pb::lock::LockType::Unknown as i32 {
                res.locks.push(PathLock {
                    path: paths::collection_info(&cw.prefix),
                    typ: parse_lock_type(clt),
                    scope: LockScope::Entry,
                });
            }
            for l in &locks.locks {
                res.locks.push(PathLock {
                    path: format!("{}/{}", cw.prefix, l.suffix),
                    typ: parse_lock_type(l.lock_type),
                    scope: parse_lock_scope(l.scope),
                });
            }
        }
    }
    Ok(res)
}

pub(crate) fn marshal_log(l: &TxLog, ts: SystemTime) -> Result<Vec<u8>, StorageError> {
    if l.id.is_unset() {
        return Err(StorageError::other("empty transaction ID"));
    }
    let mut coll_writes: BTreeMap<String, pb::CollectionWrites> = BTreeMap::new();

    for e in &l.writes {
        marshal_write(&mut coll_writes, e)?;
    }
    for e in &l.locks {
        marshal_lock(&mut coll_writes, e)?;
    }

    let status = match l.status {
        TxCommitStatus::Ok => pb::transaction_log::Status::Committed,
        TxCommitStatus::Aborted => pb::transaction_log::Status::Aborted,
        TxCommitStatus::Pending => pb::transaction_log::Status::Pending,
        TxCommitStatus::Unknown => {
            return Err(StorageError::other("unsupported commit status"));
        }
    };

    let tr = pb::TransactionLog {
        timestamp: Some(system_to_proto_ts(ts)),
        status: status as i32,
        writes: coll_writes.into_values().collect(),
    };
    Ok(tr.encode_to_vec())
}

fn marshal_write(
    coll_writes: &mut BTreeMap<String, pb::CollectionWrites>,
    e: &TxWrite,
) -> Result<(), StorageError> {
    let pr = paths::parse(&e.path)
        .map_err(|err| StorageError::with_source("parsing transaction-log write path", err))?;
    if pr.typ != paths::Type::Key {
        return Err(StorageError::other(format!(
            "expected 'key' path, got path {:?}",
            e.path
        )));
    }
    let val_delete = if e.deleted {
        pb::write::ValDelete::Deleted(true)
    } else {
        pb::write::ValDelete::Value(e.value.to_vec())
    };
    let write = pb::Write {
        suffix: format!("{}/{}", pr.typ.as_str(), pr.suffix),
        prev_tid: e.prev_writer.as_bytes().to_vec(),
        val_delete: Some(val_delete),
    };
    let coll = coll_writes
        .entry(pr.prefix.clone())
        .or_insert_with(|| pb::CollectionWrites {
            prefix: pr.prefix.clone(),
            writes: Vec::new(),
            locks: Some(pb::CollectionLocks::default()),
        });
    coll.writes.push(write);
    Ok(())
}

fn marshal_lock(
    coll_writes: &mut BTreeMap<String, pb::CollectionWrites>,
    e: &PathLock,
) -> Result<(), StorageError> {
    let lt = lock_type_to_proto(e.typ);
    let pr = paths::parse(&e.path)
        .map_err(|err| StorageError::with_source("parsing transaction-log lock path", err))?;

    let coll = coll_writes
        .entry(pr.prefix.clone())
        .or_insert_with(|| pb::CollectionWrites {
            prefix: pr.prefix.clone(),
            writes: Vec::new(),
            locks: Some(pb::CollectionLocks::default()),
        });
    let clocks = coll.locks.get_or_insert_with(pb::CollectionLocks::default);

    if pr.typ == paths::Type::CollectionInfo && e.scope == LockScope::Entry {
        clocks.collection_lock = lt as i32;
    } else {
        clocks.locks.push(pb::Lock {
            suffix: if pr.suffix.is_empty() {
                pr.typ.as_str().to_string()
            } else {
                format!("{}/{}", pr.typ.as_str(), pr.suffix)
            },
            lock_type: lt as i32,
            scope: lock_scope_to_proto(e.scope) as i32,
        });
    }
    Ok(())
}

fn lock_scope_to_proto(scope: LockScope) -> pb::lock::Scope {
    match scope {
        LockScope::Entry => pb::lock::Scope::Entry,
        LockScope::Structure => pb::lock::Scope::Structure,
        LockScope::Membership => pb::lock::Scope::Membership,
    }
}

fn parse_lock_scope(scope: i32) -> LockScope {
    match pb::lock::Scope::try_from(scope) {
        Ok(pb::lock::Scope::Structure) => LockScope::Structure,
        Ok(pb::lock::Scope::Membership) => LockScope::Membership,
        _ => LockScope::Entry,
    }
}

fn lock_type_to_proto(t: LockType) -> pb::lock::LockType {
    match t {
        LockType::None => pb::lock::LockType::None,
        LockType::Read => pb::lock::LockType::Read,
        LockType::Write => pb::lock::LockType::Write,
        LockType::Create => pb::lock::LockType::Create,
        LockType::Unknown => pb::lock::LockType::Unknown,
    }
}

fn parse_lock_type(t: i32) -> LockType {
    match pb::lock::LockType::try_from(t) {
        Ok(pb::lock::LockType::None) => LockType::None,
        Ok(pb::lock::LockType::Read) => LockType::Read,
        Ok(pb::lock::LockType::Write) => LockType::Write,
        Ok(pb::lock::LockType::Create) => LockType::Create,
        _ => LockType::Unknown,
    }
}

/// Decodes a transaction object body into its commit status and timestamp,
/// pairing them with the object's backend `version`. The status and timestamp
/// live in the proto body itself (ADR-019), so this is the v2 replacement for
/// the v1 tag read.
fn decode_status(buf: &[u8], version: backend::Version) -> Result<TxStatus, StorageError> {
    let tr = parse_log(buf)?;
    let status = match tr.status() {
        pb::transaction_log::Status::Committed => TxCommitStatus::Ok,
        pb::transaction_log::Status::Aborted => TxCommitStatus::Aborted,
        pb::transaction_log::Status::Pending => TxCommitStatus::Pending,
        pb::transaction_log::Status::Default => {
            return Err(StorageError::other("unknown commit status in tx log"));
        }
    };
    let last_update = tr.timestamp.map(proto_ts_to_system).unwrap_or(UNIX_EPOCH);
    Ok(TxStatus {
        status,
        last_update,
        version,
    })
}

fn system_to_proto_ts(t: SystemTime) -> prost_types::Timestamp {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => prost_types::Timestamp {
            seconds: d.as_secs() as i64,
            nanos: d.subsec_nanos() as i32,
        },
        Err(e) => {
            let d = e.duration();
            prost_types::Timestamp {
                seconds: -(d.as_secs() as i64),
                nanos: -(d.subsec_nanos() as i32),
            }
        }
    }
}

fn proto_ts_to_system(ts: prost_types::Timestamp) -> SystemTime {
    if ts.seconds >= 0 {
        UNIX_EPOCH + Duration::new(ts.seconds as u64, ts.nanos.max(0) as u32)
    } else {
        UNIX_EPOCH - Duration::new((-ts.seconds) as u64, ts.nanos.unsigned_abs())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::SharedCache;
    use glassdb_backend::memory::MemoryBackend;

    fn new_tlogger() -> TLogger {
        let cache = SharedCache::new(1 << 20);
        let backend = Arc::new(MemoryBackend::new());
        let objects = ObjectCache::new(backend, &cache);
        TLogger::new(objects, "db")
    }

    #[tokio::test]
    async fn round_trip() {
        let t = new_tlogger();
        let id = TxId::from_bytes(vec![1, 2, 3, 4]);
        let key_path = paths::from_key("db/root", b"hello");
        let log = TxLog {
            id: id.clone(),
            timestamp: Some(UNIX_EPOCH + Duration::from_millis(1_700_000_000_000)),
            status: TxCommitStatus::Ok,
            writes: vec![TxWrite {
                path: key_path.clone(),
                value: Arc::from(&b"world"[..]),
                deleted: false,
                prev_writer: TxId::from_bytes(vec![9]),
            }],
            locks: vec![
                PathLock {
                    path: paths::collection_info("db/root"),
                    typ: LockType::Read,
                    scope: LockScope::Entry,
                },
                PathLock {
                    path: key_path.clone(),
                    typ: LockType::Write,
                    scope: LockScope::Entry,
                },
            ],
        };
        t.set(&log).await.unwrap();

        let (got, _) = t.get(&id).await.unwrap();
        assert_eq!(got.status, TxCommitStatus::Ok);
        assert_eq!(got.writes, log.writes);
        // Locks include the collection lock and the key lock.
        assert!(got.locks.contains(&PathLock {
            path: paths::collection_info("db/root"),
            typ: LockType::Read,
            scope: LockScope::Entry,
        }));
        assert!(got.locks.contains(&PathLock {
            path: key_path,
            typ: LockType::Write,
            scope: LockScope::Entry,
        }));

        let status = t.commit_status(&id).await.unwrap();
        assert_eq!(status.status, TxCommitStatus::Ok);
    }

    #[tokio::test]
    async fn commit_status_unknown_when_absent() {
        let t = new_tlogger();
        let status = t.commit_status(&TxId::from_bytes(vec![7])).await.unwrap();
        assert_eq!(status.status, TxCommitStatus::Unknown);
    }

    #[tokio::test]
    async fn get_returns_log_and_version() {
        let t = new_tlogger();
        let id = TxId::from_bytes(vec![1, 2, 3, 4]);
        let key_path = paths::from_key("db/root", b"hello");
        let mut log = TxLog::new(id.clone(), TxCommitStatus::Ok);
        log.timestamp = Some(UNIX_EPOCH + Duration::from_secs(1_700_000_000));
        log.writes = vec![TxWrite {
            path: key_path.clone(),
            value: Arc::from(&b"world"[..]),
            deleted: false,
            prev_writer: TxId::default(),
        }];
        let stored_v = t.set(&log).await.unwrap();

        let (got, version) = t.get(&id).await.unwrap();
        assert_eq!(got.status, TxCommitStatus::Ok);
        assert_eq!(got.writes, log.writes);
        assert_eq!(got.timestamp, log.timestamp);
        assert_eq!(version, stored_v);
    }

    #[tokio::test]
    async fn list_transaction_ids_pages_one_shard() {
        let t = new_tlogger();
        let ids = [
            TxId::from_bytes(vec![1, 2]),
            TxId::from_bytes(vec![1, 3]),
            TxId::from_bytes(vec![1, 4]),
        ];
        for id in &ids {
            t.set(&TxLog::new(id.clone(), TxCommitStatus::Aborted))
                .await
                .unwrap();
        }
        let shard = paths::transaction_shard(&ids[0]);
        assert!(ids.iter().all(|id| paths::transaction_shard(id) == shard));
        let limit = backend::ListLimit::new(2).unwrap();
        let first = t.list_transaction_ids(shard, None, limit).await.unwrap();
        assert_eq!(first.ids.len(), 2);
        let second = t
            .list_transaction_ids(shard, first.next.as_ref(), limit)
            .await
            .unwrap();
        assert!(second.next.is_none());
        let mut listed = first.ids;
        listed.extend(second.ids);
        listed.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        let mut expected: Vec<TxId> = ids.to_vec();
        expected.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        assert_eq!(listed, expected);
    }
}
