//! The transactional read path for the v2 object-native engine (ADR-017/020).
//!
//! A key's value no longer lives in a per-key object; it lives in the
//! transaction object of whichever transaction last committed it. Reading a key
//! therefore resolves its shard entry to an *effective writer* (help-forwarding
//! a committed-but-not-written-back exclusive holder, dropping aborted/expired
//! holders) and then materializes the value from that writer's transaction
//! object through the [`Monitor`]. Resolved values are cached in the
//! [`ValueCache`], keyed by writer, so a hot key does not re-resolve its shard
//! on every read; the cache is invalidated by the commit path when validation
//! detects a stale read.

use std::sync::Arc;
use std::time::Duration;

use glassdb_concurr::{RetryConfig, rt};
use glassdb_data::shard::shard_index;
use glassdb_data::{TxId, paths};
use glassdb_storage::{
    LockType, ShardEntry, ShardStore, StorageError, TxCommitStatus, ValueCache, Version,
};

use crate::error::TransError;
use crate::monitor::Monitor;

/// Extra attempts made when a read fails with an in-doubt (`Unavailable`)
/// outcome before the error is surfaced. Reads are idempotent (ADR-009), so
/// re-reading is always safe; this recovers transient backend unavailability in
/// place, mirroring the commit-side in-place retries. The cap keeps a sustained
/// outage from looping forever — it surfaces as `Unavailable` for the caller to
/// classify — while a caller `timeout` still bounds the total wait by dropping
/// the future.
const READ_UNAVAILABLE_RETRIES: usize = 5;

/// The result of reading a key: the raw value and its storage version. The
/// version's writer is the *effective writer* the read resolved through, which
/// is the optimistic-validation token the commit path checks.
#[derive(Debug, Clone, Default)]
pub struct ReadValue {
    pub value: Arc<[u8]>,
    pub version: Version,
}

/// The resolved view of a shard entry, after help-forwarding committed holders.
#[derive(Debug, Clone, Default)]
struct Resolved {
    /// The effective committed writer holding the key's value (the MVCC
    /// pointer), or `None` if the key has no committed value.
    writer: Option<TxId>,
    /// Whether that writer's value for the key is a tombstone.
    deleted: bool,
}

impl Resolved {
    /// The existence-aware validation token: the effective writer iff the key
    /// currently exists (committed and not tombstoned), else `None`. This is the
    /// value a read observes, so it is what optimistic validation compares.
    fn token(self) -> Option<TxId> {
        match self.writer {
            Some(w) if !self.deleted => Some(w),
            _ => None,
        }
    }
}

/// Reads values by resolving a key's shard entry to its effective committed
/// writer and materializing the value from that writer's transaction object.
#[derive(Clone)]
pub struct Reader {
    values: ValueCache,
    shards: ShardStore,
    tmon: Monitor,
    retry: RetryConfig,
}

impl Reader {
    /// Creates a reader over the value cache, the shard coordination store, and
    /// a monitor. The `shards` store revalidates shard objects by their backend
    /// version (ADR-023), so a read always observes the current coordination
    /// state without re-transferring an unchanged shard's body.
    pub fn new(values: ValueCache, shards: ShardStore, tmon: Monitor, retry: RetryConfig) -> Self {
        Reader {
            values,
            shards,
            tmon,
            retry,
        }
    }

    /// Reads the value for `key`, accepting cached values up to `max_stale`.
    ///
    /// A read is idempotent, so a transient in-doubt (`Unavailable`) outcome is
    /// retried in place with exponential backoff up to
    /// [`READ_UNAVAILABLE_RETRIES`] times. A persistent outage surfaces the last
    /// `Unavailable` error for the caller to classify; the caller cancels by
    /// dropping the future at any `.await` (e.g. via `tokio::time::timeout`).
    pub async fn read(&self, key: &str, max_stale: Duration) -> Result<ReadValue, StorageError> {
        let mut backoff = self.retry.backoff();
        for _ in 0..READ_UNAVAILABLE_RETRIES {
            match self.read_once(key, max_stale).await {
                Err(StorageError::Unavailable(_)) => rt::sleep(backoff.next_delay()).await,
                other => return other,
            }
        }
        // Final attempt: surface whatever it returns, including a persistent
        // `Unavailable` that the caller maps to `Error::Unavailable`.
        self.read_once(key, max_stale).await
    }

    /// A single read attempt: local cache then shard resolution. Wrapped by
    /// [`Reader::read`] for in-place retries.
    async fn read_once(&self, key: &str, max_stale: Duration) -> Result<ReadValue, StorageError> {
        if let Some(lr) = self.values.read(key, max_stale)
            && !lr.outdated
        {
            if lr.deleted {
                return Err(StorageError::NotFound);
            }
            return Ok(ReadValue {
                value: lr.value,
                version: lr.version,
            });
        }
        self.resolve_value(key).await
    }

    /// Returns the effective committed writer of `key` (the validation token):
    /// `Some(writer)` if the key currently exists, `None` if it is absent or
    /// tombstoned. Always reads the shard fresh (no value cache), so the commit
    /// path observes the authoritative coordination state.
    pub(crate) async fn effective_writer(&self, key: &str) -> Result<Option<TxId>, StorageError> {
        let (prefix, raw_key) = paths::split_key(key)
            .map_err(|e| StorageError::with_source(format!("parsing key path {key:?}"), e))?;
        let (shard, _) = self
            .shards
            .load_shard(&prefix, shard_index(&raw_key))
            .await?;
        let resolved = self
            .resolve_entry(key, shard.lookup(&raw_key))
            .await
            .map_err(trans_to_storage)?;
        Ok(resolved.token())
    }

    /// Resolves `key` through its shard to the effective writer, then
    /// materializes the value from that writer's transaction object.
    async fn resolve_value(&self, key: &str) -> Result<ReadValue, StorageError> {
        let (prefix, raw_key) = paths::split_key(key)
            .map_err(|e| StorageError::with_source(format!("parsing key path {key:?}"), e))?;
        let (shard, _) = self
            .shards
            .load_shard(&prefix, shard_index(&raw_key))
            .await?;
        let resolved = self
            .resolve_entry(key, shard.lookup(&raw_key))
            .await
            .map_err(trans_to_storage)?;
        let Some(writer) = resolved.token() else {
            // Absent or tombstoned: not found. (A tombstone could be cached, but
            // the next validation re-resolves the shard anyway, so we keep the
            // read path simple and only cache materialized live values.)
            return Err(StorageError::NotFound);
        };
        let cv = self
            .tmon
            .committed_value(key, &writer)
            .await
            .map_err(trans_to_storage)?;
        if cv.status != TxCommitStatus::Ok || cv.value.not_written {
            // The writer's value is not authoritatively resolvable yet (e.g. its
            // object is in-doubt). Report not-found so the caller retries rather
            // than trusting an empty placeholder.
            return Err(StorageError::NotFound);
        }
        self.materialize(key, writer, cv.value)
    }

    /// Resolves `entry` against the transaction monitor: help-forward a committed
    /// exclusive holder (one that committed but has not yet published its
    /// `current_writer` pointer) and drop aborted/absent holders. `key_path` is
    /// the full storage path of the key, used to fetch the help-forwarded
    /// writer's value. A `None` entry resolves to "no value".
    async fn resolve_entry(
        &self,
        key_path: &str,
        entry: Option<&ShardEntry>,
    ) -> Result<Resolved, TransError> {
        let Some(e) = entry else {
            return Ok(Resolved::default());
        };

        let mut writer = e.current_writer.clone();
        let mut deleted = e.deleted;

        // Only an exclusive (write/create) holder can change the committed value
        // by help-forwarding. Read-lock holders never change the value, and
        // pending/aborted holders are ignored (a pending holder has published no
        // value; an aborted holder's lock is dead).
        if matches!(e.lock_type, LockType::Write | LockType::Create) {
            for holder in &e.locked_by {
                if self.tmon.tx_status(holder).await? != TxCommitStatus::Ok {
                    continue;
                }
                let cv = self.tmon.committed_value(key_path, holder).await?;
                if cv.status == TxCommitStatus::Ok && !cv.value.not_written {
                    writer = Some(holder.clone());
                    deleted = cv.value.deleted;
                }
            }
        }

        Ok(Resolved { writer, deleted })
    }

    /// Caches and returns a resolved committed value, or reports `NotFound` for a
    /// tombstone.
    fn materialize(
        &self,
        key: &str,
        writer: glassdb_data::TxId,
        value: glassdb_storage::TValue,
    ) -> Result<ReadValue, StorageError> {
        let version = Version { writer };
        if value.deleted {
            self.values.mark_deleted(key, version);
            return Err(StorageError::NotFound);
        }
        self.values.write(key, value.value.clone(), version.clone());
        Ok(ReadValue {
            value: value.value,
            version,
        })
    }
}

/// Converts a transaction-engine error into a storage error for the read path.
pub(crate) fn trans_to_storage(e: TransError) -> StorageError {
    match e {
        TransError::Storage(s) => s,
        other => StorageError::other(other.to_string()),
    }
}
