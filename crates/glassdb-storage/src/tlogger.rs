//! Transaction-log persistence. Ported from the Go
//! `internal/storage/tlogger.go`. Logs are protobuf bodies with commit-status
//! and timestamp tags.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use glassdb_backend::{self as backend, Tags};
use glassdb_concurr::{rt, Ctx};
use glassdb_data::{gopath, paths, TxId};
use glassdb_proto as pb;
use prost::Message;

use crate::error::StorageError;
use crate::global::Global;
use crate::local::{Local, MAX_STALENESS};
use crate::locker::LockType;

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

const COMMIT_STATUS_TAG: &str = "commit-status";
const TIMESTAMP_TAG: &str = "timestamp";
const COMMIT_STATUS_OK: &str = "committed";
const COMMIT_STATUS_ABORTED: &str = "aborted";
const COMMIT_STATUS_PENDING: &str = "pending";

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
    pub value: Vec<u8>,
    pub deleted: bool,
    pub prev_writer: TxId,
}

/// The commit status of a transaction along with its timestamp and version.
#[derive(Debug, Clone)]
pub struct TxStatus {
    pub status: TxCommitStatus,
    pub last_update: SystemTime,
    pub version: backend::Version,
}

/// A storage path together with its lock type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathLock {
    pub path: String,
    pub typ: LockType,
}

/// Reads and writes transaction logs under a path prefix.
#[derive(Clone)]
pub struct TLogger {
    prefix: String,
    global: Global,
    local: Local,
}

impl TLogger {
    /// Creates a logger storing logs under `prefix`.
    pub fn new(global: Global, local: Local, prefix: impl Into<String>) -> Self {
        TLogger {
            prefix: prefix.into(),
            global,
            local,
        }
    }

    /// Returns the commit status of transaction `id`, using the cache when
    /// possible.
    pub async fn commit_status(&self, ctx: &Ctx, id: &TxId) -> Result<TxStatus, StorageError> {
        match self.read_tags(ctx, id).await {
            Ok(ts) => Ok(ts),
            Err(e) if e.is_not_found() => Ok(TxStatus {
                status: TxCommitStatus::Unknown,
                last_update: UNIX_EPOCH,
                version: backend::Version::default(),
            }),
            Err(e) => Err(e),
        }
    }

    /// Reads and parses the full transaction log for `id`.
    pub async fn get(&self, ctx: &Ctx, id: &TxId) -> Result<TxLog, StorageError> {
        let tr = self.read_log(ctx, id).await?;
        let status = match tr.status() {
            pb::transaction_log::Status::Committed => TxCommitStatus::Ok,
            pb::transaction_log::Status::Aborted => TxCommitStatus::Aborted,
            pb::transaction_log::Status::Pending => TxCommitStatus::Pending,
            pb::transaction_log::Status::Default => {
                return Err(StorageError::Other("unknown commit status".into()))
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
                    path: gopath::join(&[&cw.prefix, &w.suffix]),
                    value: write_value(w),
                    deleted: write_deleted(w),
                    prev_writer: TxId::from_bytes(w.prev_tid.clone()),
                });
            }
            if let Some(locks) = &cw.locks {
                if locks.collection_lock != pb::lock::LockType::None as i32 {
                    res.locks.push(PathLock {
                        path: paths::collection_info(&cw.prefix),
                        typ: parse_lock_type(locks.collection_lock),
                    });
                }
                for l in &locks.locks {
                    res.locks.push(PathLock {
                        path: gopath::join(&[&cw.prefix, &l.suffix]),
                        typ: parse_lock_type(l.lock_type),
                    });
                }
            }
        }
        Ok(res)
    }

    /// Creates a new transaction log entry, failing if one already exists.
    ///
    /// Borrows the log: the commit path writes the same `TxLog` repeatedly
    /// (retries), and a locked commit hands its log here without cloning it.
    pub async fn set(&self, ctx: &Ctx, l: &TxLog) -> Result<backend::Version, StorageError> {
        let ts = l.timestamp.unwrap_or_else(rt::system_now);
        let buf = marshal_log(l, ts)?;
        let tags = log_tags(l, ts);
        let m = self
            .global
            .write_if_not_exists(
                ctx,
                &paths::from_transaction(&self.prefix, &l.id),
                buf,
                tags,
            )
            .await?;
        Ok(m.version.clone())
    }

    /// Updates the log only if its current version matches `expected`.
    pub async fn set_if(
        &self,
        ctx: &Ctx,
        l: &TxLog,
        expected: &backend::Version,
    ) -> Result<backend::Version, StorageError> {
        let ts = l.timestamp.unwrap_or_else(rt::system_now);
        let buf = marshal_log(l, ts)?;
        let tags = log_tags(l, ts);
        let m = self
            .global
            .write_if(
                ctx,
                &paths::from_transaction(&self.prefix, &l.id),
                buf,
                expected,
                tags,
            )
            .await?;
        Ok(m.version.clone())
    }

    /// Removes the log for `id`, ignoring not-found errors.
    pub async fn delete(&self, ctx: &Ctx, id: &TxId) -> Result<(), StorageError> {
        match self
            .global
            .delete(ctx, &paths::from_transaction(&self.prefix, id))
            .await
        {
            Ok(()) => Ok(()),
            Err(e) if e.is_not_found() => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn read_log(&self, ctx: &Ctx, id: &TxId) -> Result<pb::TransactionLog, StorageError> {
        let p = paths::from_transaction(&self.prefix, id);
        if let Some(lr) = self.local.read(&p, MAX_STALENESS) {
            let log = parse_log(&lr.value)?;
            if log.status() != pb::transaction_log::Status::Pending {
                return Ok(log);
            }
            // Pending logs can't be trusted from the cache: tx-log writes change
            // content but not the last-writer tag, so writer-based
            // ReadIfModified wouldn't detect the change. Mark outdated so
            // global.read bypasses ReadIfModified.
            self.local.mark_value_outdated(&p, lr.version);
        }
        let gr = self.global.read(ctx, &p).await?;
        parse_log(&gr.value)
    }

    async fn read_tags(&self, ctx: &Ctx, id: &TxId) -> Result<TxStatus, StorageError> {
        let p = paths::from_transaction(&self.prefix, id);
        if let Some(lm) = self.local.get_meta(&p, MAX_STALENESS) {
            let mut ts = parse_log_tags(&lm.m.tags)?;
            ts.version = lm.m.version.clone();
            if ts.status != TxCommitStatus::Pending {
                return Ok(ts);
            }
            // Pending: the cached value could be stale, read globally.
        }
        let gm = self.global.get_metadata(ctx, &p).await?;
        let mut ts = parse_log_tags(&gm.tags)?;
        ts.version = gm.version.clone();
        Ok(ts)
    }
}

fn write_value(w: &pb::Write) -> Vec<u8> {
    match &w.val_delete {
        Some(pb::write::ValDelete::Value(v)) => v.clone(),
        _ => Vec::new(),
    }
}

fn write_deleted(w: &pb::Write) -> bool {
    matches!(&w.val_delete, Some(pb::write::ValDelete::Deleted(true)))
}

fn parse_log(buf: &[u8]) -> Result<pb::TransactionLog, StorageError> {
    pb::TransactionLog::decode(buf)
        .map_err(|e| StorageError::Other(format!("unmarshalling transaction log: {e}")))
}

fn marshal_log(l: &TxLog, ts: SystemTime) -> Result<Vec<u8>, StorageError> {
    if l.id.is_empty() {
        return Err(StorageError::Other("empty transaction ID".into()));
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
            return Err(StorageError::Other("unsupported commit status".into()))
        }
    };

    let tr = pb::TransactionLog {
        timestamp: Some(system_to_proto_ts(ts)),
        status: status as i32,
        writes: coll_writes.into_values().collect(),
    };
    Ok(tr.encode_to_vec())
}

/// Returns the collection-writes entry for `prefix`, creating it if absent.
/// Borrows `prefix` and only allocates when inserting a new collection, so a
/// log writing many keys under one collection (e.g. a batch write) clones the
/// prefix once instead of on every write/lock.
fn coll_entry<'a>(
    coll_writes: &'a mut BTreeMap<String, pb::CollectionWrites>,
    prefix: &str,
) -> &'a mut pb::CollectionWrites {
    if !coll_writes.contains_key(prefix) {
        coll_writes.insert(
            prefix.to_string(),
            pb::CollectionWrites {
                prefix: prefix.to_string(),
                writes: Vec::new(),
                locks: Some(pb::CollectionLocks::default()),
            },
        );
    }
    coll_writes.get_mut(prefix).unwrap()
}

fn marshal_write(
    coll_writes: &mut BTreeMap<String, pb::CollectionWrites>,
    e: &TxWrite,
) -> Result<(), StorageError> {
    let pr = paths::parse_ref(&e.path).map_err(|e| StorageError::Other(e.to_string()))?;
    if pr.typ != paths::Type::Key {
        return Err(StorageError::Other(format!(
            "expected 'key' path, got path {:?}",
            e.path
        )));
    }
    let val_delete = if e.deleted {
        pb::write::ValDelete::Deleted(true)
    } else {
        pb::write::ValDelete::Value(e.value.clone())
    };
    let write = pb::Write {
        // `pr.suffix` is already the protobuf suffix (`_k/<b64>`); no re-join.
        suffix: pr.suffix.to_string(),
        prev_tid: e.prev_writer.as_bytes().to_vec(),
        val_delete: Some(val_delete),
    };
    coll_entry(coll_writes, pr.prefix).writes.push(write);
    Ok(())
}

fn marshal_lock(
    coll_writes: &mut BTreeMap<String, pb::CollectionWrites>,
    e: &PathLock,
) -> Result<(), StorageError> {
    let lt = lock_type_to_proto(e.typ);
    let pr = paths::parse_ref(&e.path).map_err(|e| StorageError::Other(e.to_string()))?;

    let coll = coll_entry(coll_writes, pr.prefix);
    let clocks = coll.locks.get_or_insert_with(pb::CollectionLocks::default);

    if pr.typ == paths::Type::CollectionInfo {
        clocks.collection_lock = lt as i32;
    } else {
        clocks.locks.push(pb::Lock {
            suffix: pr.suffix.to_string(),
            lock_type: lt as i32,
        });
    }
    Ok(())
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

fn log_tags(l: &TxLog, ts: SystemTime) -> Tags {
    let status = match l.status {
        TxCommitStatus::Ok => COMMIT_STATUS_OK,
        TxCommitStatus::Pending => COMMIT_STATUS_PENDING,
        _ => COMMIT_STATUS_ABORTED,
    };
    let ts = ts
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let mut tags = Tags::new();
    tags.insert(COMMIT_STATUS_TAG.to_string(), status.to_string());
    tags.insert(TIMESTAMP_TAG.to_string(), ts.to_string());
    tags
}

fn parse_log_tags(t: &Tags) -> Result<TxStatus, StorageError> {
    let st = t
        .get(COMMIT_STATUS_TAG)
        .ok_or_else(|| StorageError::Other("commit-status tag not found in tx log".into()))?;
    let status = match st.as_str() {
        COMMIT_STATUS_OK => TxCommitStatus::Ok,
        COMMIT_STATUS_ABORTED => TxCommitStatus::Aborted,
        COMMIT_STATUS_PENDING => TxCommitStatus::Pending,
        other => {
            return Err(StorageError::Other(format!(
                "unknown commit-status tag {other:?}"
            )))
        }
    };
    let ts = t
        .get(TIMESTAMP_TAG)
        .ok_or_else(|| StorageError::Other("timestamp tag not found in tx log".into()))?;
    let unix_milli: i64 = ts
        .parse()
        .map_err(|e| StorageError::Other(format!("parsing timestamp tag {ts:?}: {e}")))?;

    Ok(TxStatus {
        status,
        // Matches Go's time.Unix(unixMilli/1000, 0): second precision only.
        last_update: UNIX_EPOCH + Duration::from_secs((unix_milli / 1000) as u64),
        version: backend::Version::default(),
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
    use glassdb_backend::memory::MemoryBackend;
    use std::sync::Arc;

    fn new_tlogger() -> TLogger {
        let local = Local::new(1 << 20);
        let backend = Arc::new(MemoryBackend::new());
        let global = Global::new(backend, local.clone());
        TLogger::new(global, local, "db")
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
                value: b"world".to_vec(),
                deleted: false,
                prev_writer: TxId::from_bytes(vec![9]),
            }],
            locks: vec![
                PathLock {
                    path: paths::collection_info("db/root"),
                    typ: LockType::Read,
                },
                PathLock {
                    path: key_path.clone(),
                    typ: LockType::Write,
                },
            ],
        };
        let ctx = Ctx::background();
        t.set(&ctx, &log).await.unwrap();

        let got = t.get(&ctx, &id).await.unwrap();
        assert_eq!(got.status, TxCommitStatus::Ok);
        assert_eq!(got.writes, log.writes);
        // Locks include the collection lock and the key lock.
        assert!(got.locks.contains(&PathLock {
            path: paths::collection_info("db/root"),
            typ: LockType::Read
        }));
        assert!(got.locks.contains(&PathLock {
            path: key_path,
            typ: LockType::Write
        }));

        let status = t.commit_status(&ctx, &id).await.unwrap();
        assert_eq!(status.status, TxCommitStatus::Ok);
    }

    #[tokio::test]
    async fn commit_status_unknown_when_absent() {
        let t = new_tlogger();
        let status = t
            .commit_status(&Ctx::background(), &TxId::from_bytes(vec![7]))
            .await
            .unwrap();
        assert_eq!(status.status, TxCommitStatus::Unknown);
    }
}
