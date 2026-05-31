//! Lock state encoding and the pure lock-transition logic. Ported from the Go
//! `internal/storage/locker.go`.

use base64::Engine;
use glassdb_backend::Tags;
use glassdb_concurr::Ctx;
use glassdb_data::TxId;

use crate::error::StorageError;
use crate::global::Global;
use crate::tlogger::TxCommitStatus;

/// The type of lock held on a storage object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LockType {
    #[default]
    Unknown,
    None,
    Read,
    Write,
    Create,
}

const LOCKED_BY_TAG: &str = "locked-by";
const LOCK_TYPE_TAG: &str = "lock-type";
const LAST_WRITER_TAG: &str = "last-writer";

const LOCK_TAG_READ: &str = "r";
const LOCK_TAG_WRITE: &str = "w";
const LOCK_TAG_CREATE: &str = "c";
const LOCK_TAG_NONE: &str = "-";

impl LockType {
    fn to_tag(self) -> Result<&'static str, StorageError> {
        match self {
            LockType::None => Ok(LOCK_TAG_NONE),
            LockType::Read => Ok(LOCK_TAG_READ),
            LockType::Write => Ok(LOCK_TAG_WRITE),
            LockType::Create => Ok(LOCK_TAG_CREATE),
            LockType::Unknown => Err(StorageError::Other("unknown lock type".into())),
        }
    }
}

fn is_writer_type(lt: LockType) -> bool {
    matches!(lt, LockType::Create | LockType::Write)
}

/// Splits the (pending) holders of a lock into those the requesters must wait
/// for and those they may wound, following the wound-wait rule. A holder is
/// wounded when at least one requester is older (has higher priority);
/// otherwise the requesters must wait for it. A holder that is also one of the
/// requesters is never wounded.
fn partition_wound_wait(holders: &[TxId], requesters: &[TxId]) -> LockOps {
    let mut ops = LockOps::default();
    for h in holders {
        if !requesters.contains(h) && any_older(requesters, h) {
            ops.wound.push(h.clone());
        } else {
            ops.wait_for.push(h.clone());
        }
    }
    ops
}

/// Reports whether any of the requesters has higher priority (is older) than the
/// holder.
fn any_older(requesters: &[TxId], holder: &TxId) -> bool {
    requesters.iter().any(|r| r.older(holder))
}

/// A value written by a transaction, including whether it was a deletion or was
/// not written at all.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TValue {
    pub value: Vec<u8>,
    pub deleted: bool,
    /// True when the transaction committed but did not write this value (e.g.
    /// read-only lock).
    pub not_written: bool,
}

/// The desired state of a lock after an update.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LockUpdate {
    pub typ: LockType,
    pub prev_type: LockType,
    pub lockers: Vec<TxId>,
    pub writer: TxId,
    pub value: TValue,
}

/// The transactions that want to acquire or release a lock.
#[derive(Debug, Clone, Default)]
pub struct LockRequest {
    pub typ: LockType,
    pub lockers: Vec<TxId>,
    pub unlockers: Vec<TxId>,
}

/// A transaction's commit status and the value it wrote for a path.
#[derive(Debug, Clone)]
pub struct TxPathState {
    pub tx: TxId,
    pub status: TxCommitStatus,
    pub value: TValue,
}

/// The next possible update for a lock, plus the transactions to wait for and
/// the operation's effects.
#[derive(Debug, Clone, Default)]
pub struct LockOps {
    pub update: LockUpdate,
    pub has_update: bool,
    /// Higher-priority (older) holders the requester must wait for under the
    /// wound-wait rule.
    pub wait_for: Vec<TxId>,
    /// Lower-priority (younger) holders the requester is allowed to abort, so it
    /// can take the lock without waiting.
    pub wound: Vec<TxId>,
    pub locked_for: Vec<TxId>,
    pub unlocked_for: Vec<TxId>,
}

/// The current lock state of a storage object.
#[derive(Debug, Clone, Default)]
pub struct LockInfo {
    pub typ: LockType,
    pub locked_by: Vec<TxId>,
    pub last_writer: TxId,
}

impl LockInfo {
    /// Checks the lock info is internally consistent.
    pub fn valid(&self) -> Result<(), StorageError> {
        if self.locked_by.is_empty() {
            if self.typ != LockType::None {
                return Err(StorageError::Other(
                    "got zero lockers, but lock type is not none".into(),
                ));
            }
            return Ok(());
        }
        if self.locked_by.len() > 1 && (self.typ == LockType::Create || self.typ == LockType::Write)
        {
            return Err(StorageError::Other(format!(
                "got {} lockers with writer lock",
                self.locked_by.len()
            )));
        }
        Ok(())
    }
}

fn tids_diff(a: &[TxId], b: &[TxId]) -> Vec<TxId> {
    a.iter().filter(|x| !b.contains(x)).cloned().collect()
}

fn tids_intersect(a: &[TxId], b: &[TxId]) -> Vec<TxId> {
    a.iter().filter(|x| b.contains(x)).cloned().collect()
}

fn find_tx_path_state<'a>(txs: &'a [TxPathState], tid: &TxId) -> Option<&'a TxPathState> {
    txs.iter().find(|s| &s.tx == tid)
}

/// Determines the next lock operation given the current state, a request, and
/// the status of relevant transactions.
pub fn compute_lock_update(
    curr: LockInfo,
    req: &LockRequest,
    txs: &[TxPathState],
) -> Result<LockOps, StorageError> {
    let unlock_ops = compute_unlock_update(&curr, &req.unlockers, txs)?;
    if req.lockers.is_empty() {
        return Ok(handle_no_ops(&curr, unlock_ops));
    }
    if unlock_ops.has_update && unlock_ops.update.value.deleted {
        return Ok(unlock_ops);
    }

    let mut curr = curr;
    if unlock_ops.has_update {
        curr.typ = unlock_ops.update.typ;
        curr.last_writer = unlock_ops.update.writer.clone();
        curr.locked_by = unlock_ops.update.lockers.clone();
    }
    let lock_ops = compute_lock_update_inner(&curr, req.typ, &req.lockers)?;
    if (!lock_ops.wait_for.is_empty() || !lock_ops.wound.is_empty())
        && !lock_ops.unlocked_for.is_empty()
    {
        // Instead of waiting (or wounding), apply the pending unlock first.
        return Ok(handle_no_ops(&curr, unlock_ops));
    }
    let mut ops = LockOps {
        update: unlock_ops.update,
        has_update: unlock_ops.has_update || lock_ops.has_update,
        wait_for: lock_ops.wait_for,
        wound: lock_ops.wound,
        locked_for: lock_ops.locked_for,
        unlocked_for: unlock_ops.unlocked_for,
    };
    if !lock_ops.has_update {
        return Ok(handle_no_ops(&curr, ops));
    }
    ops.update.typ = lock_ops.update.typ;
    ops.update.lockers = lock_ops.update.lockers;
    Ok(ops)
}

fn compute_unlock_update(
    curr: &LockInfo,
    unlockers: &[TxId],
    txs: &[TxPathState],
) -> Result<LockOps, StorageError> {
    if curr.typ == LockType::None {
        return Ok(LockOps {
            unlocked_for: unlockers.to_vec(),
            ..Default::default()
        });
    }

    let already_unlocked = tids_diff(unlockers, &curr.locked_by);

    let mut update = LockUpdate {
        typ: curr.typ,
        prev_type: curr.typ,
        ..Default::default()
    };
    let mut unlocked_for = already_unlocked;

    for tx in &curr.locked_by {
        let v = find_tx_path_state(txs, tx)
            .ok_or_else(|| StorageError::Other(format!("missing state for tx {tx}")))?;
        if !v.status.is_final() && !unlockers.contains(tx) {
            update.lockers.push(tx.clone());
            continue;
        }
        unlocked_for.push(tx.clone());
        if is_writer_type(curr.typ) && v.status == TxCommitStatus::Ok {
            update.value = v.value.clone();
            update.writer = tx.clone();
        }
    }
    if update.lockers.is_empty() {
        update.typ = LockType::None;
    }

    let has_update = !unlocked_for.is_empty();
    Ok(LockOps {
        update,
        has_update,
        unlocked_for,
        ..Default::default()
    })
}

fn compute_lock_update_inner(
    curr: &LockInfo,
    lt: LockType,
    lockers: &[TxId],
) -> Result<LockOps, StorageError> {
    if lt == LockType::Unknown || lt == LockType::None {
        return Err(StorageError::Other(format!("cannot lock with type {lt:?}")));
    }
    if is_writer_type(lt) && lockers.len() != 1 {
        return Err(StorageError::Other(format!(
            "cannot lock in write with {} lockers",
            lockers.len()
        )));
    }

    if curr.typ == lt {
        let already_locked = tids_intersect(&curr.locked_by, lockers);
        if !already_locked.is_empty() {
            return Ok(LockOps {
                locked_for: already_locked,
                ..Default::default()
            });
        }
    }
    if lt == LockType::Create {
        return Ok(LockOps {
            update: LockUpdate {
                typ: LockType::Create,
                lockers: lockers.to_vec(),
                ..Default::default()
            },
            has_update: true,
            locked_for: lockers.to_vec(),
            ..Default::default()
        });
    }

    if lt == LockType::Write
        && curr.typ == LockType::Read
        && curr.locked_by.len() == 1
        && curr.locked_by[0] == lockers[0]
    {
        return Ok(LockOps {
            update: LockUpdate {
                typ: LockType::Write,
                lockers: lockers.to_vec(),
                ..Default::default()
            },
            has_update: true,
            locked_for: lockers.to_vec(),
            ..Default::default()
        });
    }

    // Check whether we conflict with the current lockers. Under the wound-wait
    // rule, the requester waits for higher-priority (older) holders and wounds
    // lower-priority (younger) ones.
    if is_writer_type(curr.typ) || (curr.typ == LockType::Read && is_writer_type(lt)) {
        return Ok(partition_wound_wait(&curr.locked_by, lockers));
    }

    let mut new_lockers = curr.locked_by.clone();
    new_lockers.extend_from_slice(lockers);
    Ok(LockOps {
        update: LockUpdate {
            typ: lt,
            prev_type: curr.typ,
            lockers: new_lockers,
            ..Default::default()
        },
        has_update: true,
        locked_for: lockers.to_vec(),
        ..Default::default()
    })
}

fn handle_no_ops(curr: &LockInfo, mut ops: LockOps) -> LockOps {
    if ops.locked_for.is_empty() && ops.unlocked_for.is_empty() {
        return ops;
    }
    if ops.has_update {
        return ops;
    }
    ops.has_update = true;
    ops.update = LockUpdate {
        typ: curr.typ,
        lockers: curr.locked_by.clone(),
        ..Default::default()
    };
    ops
}

/// Parses lock-managing tags into a [`LockInfo`].
pub fn tags_lock_info(tags: &Tags) -> Result<LockInfo, StorageError> {
    let mut res = LockInfo {
        typ: LockType::None,
        ..Default::default()
    };

    if let Some(v) = tags.get(LOCK_TYPE_TAG) {
        res.typ = match v.as_str() {
            LOCK_TAG_READ => LockType::Read,
            LOCK_TAG_WRITE => LockType::Write,
            LOCK_TAG_CREATE => LockType::Create,
            LOCK_TAG_NONE | "" => LockType::None,
            other => return Err(StorageError::Other(format!("unknown lock type {other:?}"))),
        };
    }
    if let Some(v) = tags.get(LOCKED_BY_TAG) {
        if !v.is_empty() {
            for lt in v.split(',') {
                let d = tag_to_tid(lt)
                    .map_err(|e| StorageError::Other(format!("invalid locked-by tag: {e}")))?;
                res.locked_by.push(d);
            }
        }
    }
    // On a malformed last-writer tag, leave it empty (matches Go).
    res.last_writer = last_writer_from_tags(tags);

    Ok(res)
}

/// Extracts the last-writer transaction ID from object tags. Returns an empty
/// `TxId` when the tag is absent or malformed.
pub fn last_writer_from_tags(tags: &Tags) -> TxId {
    match tags.get(LAST_WRITER_TAG) {
        Some(v) => tag_to_tid(v).unwrap_or_default(),
        None => TxId::default(),
    }
}

fn tag_to_tid(a: &str) -> Result<TxId, base64::DecodeError> {
    base64::engine::general_purpose::URL_SAFE
        .decode(a)
        .map(TxId::from_bytes)
}

fn tid_to_tag(t: &TxId) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(t.as_bytes())
}

/// Applies lock updates to storage objects via conditional writes.
#[derive(Clone)]
pub struct Locker {
    global: Global,
}

impl Locker {
    /// Creates a locker over the given global storage.
    pub fn new(global: Global) -> Self {
        Locker { global }
    }

    /// Applies `update` to the object at `key`, using a conditional write that
    /// requires the object's version to match `expected`.
    pub async fn update_lock(
        &self,
        ctx: &Ctx,
        key: &str,
        expected: &glassdb_backend::Version,
        update: &LockUpdate,
    ) -> Result<(), StorageError> {
        if let Some(res) = self.handle_lock_deletion(ctx, key, expected, update).await {
            return res;
        }
        if expected.is_null() {
            match update.typ {
                LockType::None => return Ok(()),
                LockType::Read | LockType::Write => {
                    return Err(StorageError::Backend(
                        glassdb_backend::BackendError::NotFound,
                    ))
                }
                _ => {}
            }
        }
        self.apply_lock_tags(ctx, key, expected, update).await
    }

    async fn handle_lock_deletion(
        &self,
        ctx: &Ctx,
        key: &str,
        expected: &glassdb_backend::Version,
        update: &LockUpdate,
    ) -> Option<Result<(), StorageError>> {
        if update.typ == LockType::None && update.value.deleted {
            if update.prev_type != LockType::Create && update.prev_type != LockType::Write {
                return Some(Err(StorageError::Other(format!(
                    "cannot delete from unlock type {:?}",
                    update.prev_type
                ))));
            }
            return Some(self.global.delete_if(ctx, key, expected).await);
        }
        let is_unlock_create = update.typ == LockType::None && update.prev_type == LockType::Create;
        if is_unlock_create && (update.writer.is_empty() || update.value.not_written) {
            return Some(self.global.delete_if(ctx, key, expected).await);
        }
        None
    }

    async fn apply_lock_tags(
        &self,
        ctx: &Ctx,
        key: &str,
        expected: &glassdb_backend::Version,
        update: &LockUpdate,
    ) -> Result<(), StorageError> {
        let ltype = update.typ.to_tag()?;
        let lockers: Vec<String> = update.lockers.iter().map(tid_to_tag).collect();

        let mut new_tags = Tags::new();
        new_tags.insert(LOCK_TYPE_TAG.to_string(), ltype.to_string());
        new_tags.insert(LOCKED_BY_TAG.to_string(), lockers.join(","));

        if update.typ == LockType::Create {
            self.global
                .write_if_not_exists(ctx, key, Vec::new(), new_tags)
                .await?;
            return Ok(());
        }
        if !update.writer.is_empty() && !update.value.not_written {
            new_tags.insert(LAST_WRITER_TAG.to_string(), tid_to_tag(&update.writer));
            self.global
                .write_if(ctx, key, update.value.value.clone(), expected, new_tags)
                .await?;
            return Ok(());
        }
        self.global
            .set_tags_if(ctx, key, expected, new_tags)
            .await?;
        Ok(())
    }

    /// Releases a create-lock on an uncommitted object, deleting it.
    pub async fn unlock_create_uncommitted(
        &self,
        ctx: &Ctx,
        key: &str,
        expected: &glassdb_backend::Version,
    ) -> Result<(), StorageError> {
        self.update_lock(
            ctx,
            key,
            expected,
            &LockUpdate {
                typ: LockType::None,
                prev_type: LockType::Create,
                ..Default::default()
            },
        )
        .await
    }
}

impl std::fmt::Display for LockType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx(n: u8) -> TxId {
        TxId::from_bytes(vec![n])
    }

    fn final_states(ids: &[(u8, TxCommitStatus)]) -> Vec<TxPathState> {
        ids.iter()
            .map(|(n, s)| TxPathState {
                tx: tx(*n),
                status: *s,
                value: TValue::default(),
            })
            .collect()
    }

    #[test]
    fn read_lock_on_unlocked() {
        let curr = LockInfo {
            typ: LockType::None,
            ..Default::default()
        };
        let req = LockRequest {
            typ: LockType::Read,
            lockers: vec![tx(1)],
            unlockers: vec![],
        };
        let ops = compute_lock_update(curr, &req, &[]).unwrap();
        assert!(ops.has_update);
        assert_eq!(ops.update.typ, LockType::Read);
        assert_eq!(ops.update.lockers, vec![tx(1)]);
        assert_eq!(ops.locked_for, vec![tx(1)]);
    }

    #[test]
    fn write_lock_waits_for_existing_reader() {
        let curr = LockInfo {
            typ: LockType::Read,
            locked_by: vec![tx(2)],
            ..Default::default()
        };
        let req = LockRequest {
            typ: LockType::Write,
            lockers: vec![tx(1)],
            unlockers: vec![],
        };
        let txs = final_states(&[(2, TxCommitStatus::Pending)]);
        let ops = compute_lock_update(curr, &req, &txs).unwrap();
        assert_eq!(ops.wait_for, vec![tx(2)]);
        assert!(!ops.has_update);
    }

    #[test]
    fn read_to_write_upgrade_for_sole_reader() {
        let curr = LockInfo {
            typ: LockType::Read,
            locked_by: vec![tx(1)],
            ..Default::default()
        };
        let req = LockRequest {
            typ: LockType::Write,
            lockers: vec![tx(1)],
            unlockers: vec![],
        };
        let txs = final_states(&[(1, TxCommitStatus::Pending)]);
        let ops = compute_lock_update(curr, &req, &txs).unwrap();
        assert!(ops.has_update);
        assert_eq!(ops.update.typ, LockType::Write);
        assert_eq!(ops.update.lockers, vec![tx(1)]);
    }

    #[test]
    fn unlock_committed_writer_records_value() {
        let curr = LockInfo {
            typ: LockType::Write,
            locked_by: vec![tx(1)],
            ..Default::default()
        };
        let req = LockRequest {
            typ: LockType::Unknown,
            lockers: vec![],
            unlockers: vec![tx(1)],
        };
        let txs = vec![TxPathState {
            tx: tx(1),
            status: TxCommitStatus::Ok,
            value: TValue {
                value: b"v".to_vec(),
                deleted: false,
                not_written: false,
            },
        }];
        // Empty lockers => unlock-only path.
        let ops = compute_lock_update(curr, &req, &txs).unwrap();
        assert!(ops.has_update);
        assert_eq!(ops.update.typ, LockType::None);
        assert_eq!(ops.update.writer, tx(1));
        assert_eq!(ops.update.value.value, b"v");
        assert_eq!(ops.unlocked_for, vec![tx(1)]);
    }

    #[test]
    fn create_lock_no_wait() {
        let curr = LockInfo {
            typ: LockType::None,
            ..Default::default()
        };
        let req = LockRequest {
            typ: LockType::Create,
            lockers: vec![tx(1)],
            unlockers: vec![],
        };
        let ops = compute_lock_update(curr, &req, &[]).unwrap();
        assert!(ops.has_update);
        assert_eq!(ops.update.typ, LockType::Create);
        assert!(ops.wait_for.is_empty());
    }

    #[test]
    fn tags_round_trip() {
        let mut update = LockUpdate {
            typ: LockType::Read,
            lockers: vec![tx(1), tx(2)],
            ..Default::default()
        };
        update.writer = tx(3);
        // Build the tags the way apply_lock_tags would.
        let mut tags = Tags::new();
        tags.insert(
            LOCK_TYPE_TAG.to_string(),
            update.typ.to_tag().unwrap().to_string(),
        );
        tags.insert(
            LOCKED_BY_TAG.to_string(),
            update
                .lockers
                .iter()
                .map(tid_to_tag)
                .collect::<Vec<_>>()
                .join(","),
        );
        tags.insert(LAST_WRITER_TAG.to_string(), tid_to_tag(&update.writer));

        let info = tags_lock_info(&tags).unwrap();
        assert_eq!(info.typ, LockType::Read);
        assert_eq!(info.locked_by, vec![tx(1), tx(2)]);
        assert_eq!(info.last_writer, tx(3));
    }

    // Builds a deterministic TxId whose priority follows `secs`: a smaller
    // `secs` is older (higher priority).
    fn mk_tid(secs: u64, name: &[u8]) -> TxId {
        TxId::with_priority(secs * 1_000_000_000, name)
    }

    fn pending(ids: &[TxId]) -> Vec<TxPathState> {
        ids.iter()
            .map(|id| TxPathState {
                tx: id.clone(),
                status: TxCommitStatus::Pending,
                value: TValue::default(),
            })
            .collect()
    }

    #[test]
    fn compute_lock_update_wound_wait() {
        let older = mk_tid(100, b"old");
        let younger = mk_tid(200, b"new");
        let holder = mk_tid(150, b"hold");

        struct Case {
            name: &'static str,
            curr: LockInfo,
            req: LockRequest,
            txs: Vec<TxPathState>,
            want_wound: Vec<TxId>,
            want_wait_for: Vec<TxId>,
        }

        let cases = vec![
            Case {
                name: "write requester older than write holder wounds it",
                curr: LockInfo {
                    typ: LockType::Write,
                    locked_by: vec![younger.clone()],
                    ..Default::default()
                },
                req: LockRequest {
                    typ: LockType::Write,
                    lockers: vec![older.clone()],
                    unlockers: vec![],
                },
                txs: pending(std::slice::from_ref(&younger)),
                want_wound: vec![younger.clone()],
                want_wait_for: vec![],
            },
            Case {
                name: "write requester younger than write holder waits",
                curr: LockInfo {
                    typ: LockType::Write,
                    locked_by: vec![older.clone()],
                    ..Default::default()
                },
                req: LockRequest {
                    typ: LockType::Write,
                    lockers: vec![younger.clone()],
                    unlockers: vec![],
                },
                txs: pending(std::slice::from_ref(&older)),
                want_wound: vec![],
                want_wait_for: vec![older.clone()],
            },
            Case {
                name: "read requester older than write holder wounds it",
                curr: LockInfo {
                    typ: LockType::Write,
                    locked_by: vec![younger.clone()],
                    ..Default::default()
                },
                req: LockRequest {
                    typ: LockType::Read,
                    lockers: vec![older.clone()],
                    unlockers: vec![],
                },
                txs: pending(std::slice::from_ref(&younger)),
                want_wound: vec![younger.clone()],
                want_wait_for: vec![],
            },
            Case {
                name: "read requester younger than write holder waits",
                curr: LockInfo {
                    typ: LockType::Write,
                    locked_by: vec![older.clone()],
                    ..Default::default()
                },
                req: LockRequest {
                    typ: LockType::Read,
                    lockers: vec![younger.clone()],
                    unlockers: vec![],
                },
                txs: pending(std::slice::from_ref(&older)),
                want_wound: vec![],
                want_wait_for: vec![older.clone()],
            },
            Case {
                name: "write requester partitions mixed read holders",
                curr: LockInfo {
                    typ: LockType::Read,
                    locked_by: vec![older.clone(), younger.clone()],
                    ..Default::default()
                },
                req: LockRequest {
                    typ: LockType::Write,
                    lockers: vec![holder.clone()],
                    unlockers: vec![],
                },
                txs: pending(&[older.clone(), younger.clone()]),
                want_wound: vec![younger.clone()],
                want_wait_for: vec![older.clone()],
            },
            Case {
                name: "requester does not wound itself among readers",
                curr: LockInfo {
                    typ: LockType::Read,
                    locked_by: vec![older.clone(), younger.clone()],
                    ..Default::default()
                },
                req: LockRequest {
                    typ: LockType::Write,
                    lockers: vec![younger.clone()],
                    unlockers: vec![],
                },
                txs: pending(&[older.clone(), younger.clone()]),
                want_wound: vec![],
                want_wait_for: vec![older.clone(), younger.clone()],
            },
        ];

        for c in cases {
            let ops = compute_lock_update(c.curr, &c.req, &c.txs).unwrap();
            assert_eq!(ops.wound, c.want_wound, "{}: wound", c.name);
            assert_eq!(ops.wait_for, c.want_wait_for, "{}: wait_for", c.name);
            // Conflicts never produce a storage update.
            assert!(!ops.has_update, "{}: has_update", c.name);
            assert!(ops.locked_for.is_empty(), "{}: locked_for", c.name);
        }
    }
}
