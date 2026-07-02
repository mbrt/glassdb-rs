//! The transactional read path for the v2 object-native engine (ADR-017/020).
//!
//! A key's value no longer lives in a per-key object; it lives in the
//! transaction object of whichever transaction last committed it. Reading a key
//! therefore resolves its shard entry to an *effective writer* — delegated to
//! the [`Resolver`], the shared home for that coordination step — and then
//! materializes the value from that writer's transaction object through the
//! [`Monitor`]. Resolved values are cached in the [`ValueCache`], keyed by
//! writer, so a hot key does not re-resolve its shard on every read; the cache
//! is invalidated by the commit path when validation detects a stale read.

use std::sync::Arc;
use std::time::Duration;

use glassdb_concurr::{RetryConfig, rt};
use glassdb_storage::{ShardStore, StorageError, TxCommitStatus, ValueCache, Version};

use crate::error::trans_to_storage;
use crate::monitor::Monitor;
use crate::resolver::Resolver;

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

/// Reads values by resolving a key's shard entry to its effective committed
/// writer (via the [`Resolver`]) and materializing the value from that writer's
/// transaction object.
#[derive(Clone)]
pub struct Reader {
    resolver: Resolver,
    values: ValueCache,
    tmon: Monitor,
    retry: RetryConfig,
}

impl Reader {
    /// Creates a reader over the value cache, the shard coordination store, and
    /// a monitor. Effective-writer resolution is delegated to a [`Resolver`]
    /// built over the same `shards`/`tmon`; the shard store revalidates shard
    /// objects by their backend version (ADR-023), so a read always observes the
    /// current coordination state without re-transferring an unchanged shard's
    /// body.
    pub fn new(values: ValueCache, shards: ShardStore, tmon: Monitor, retry: RetryConfig) -> Self {
        Reader {
            resolver: Resolver::new(shards, tmon.clone()),
            values,
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

    /// Resolves `key` to its effective writer (via the [`Resolver`]), then
    /// materializes the value from that writer's transaction object.
    async fn resolve_value(&self, key: &str) -> Result<ReadValue, StorageError> {
        let Some(writer) = self.resolver.effective_writer(key).await? else {
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
