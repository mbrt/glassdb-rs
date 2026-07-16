//! The transactional read path for the v2 object-native engine (ADR-017/020).
//!
//! A key's value no longer lives in a per-key object; it lives in the
//! transaction object of whichever transaction last committed it. Reading a key
//! therefore resolves its shard entry to an *effective writer* — delegated to
//! the [`Resolver`], the shared home for that coordination step — and then
//! materializes the value from that writer's transaction object through the
//! [`Monitor`]. Both the shard entry and the (terminal) transaction object are
//! served from the decoded [`CachedStore`](glassdb_storage::CachedStore); a hot
//! key resolves and materializes with no backend round-trip, and serializability
//! is enforced by the commit path revalidating every read against the current
//! coordination state (ADR-036).

use std::sync::Arc;

use glassdb_concurr::{RetryConfig, rt};
use glassdb_storage::{
    ObjectCache, Requirement, ShardStore, StorageError, TxCommitStatus, Version,
};

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

/// The outcome of reading a key, including whether the decoded cache served the
/// result without a backend round-trip. An absent value can still be a cache hit
/// when the shard entry (a tombstone) and its writer's object were both cached.
#[derive(Debug, Clone, Default)]
pub struct ReadOutcome {
    /// The resolved value, or `None` when the key is absent or deleted.
    pub value: Option<ReadValue>,
    /// Whether the read completed entirely from cache (no backend read).
    pub cache_hit: bool,
}

/// Reads values by resolving a key's shard entry to its effective committed
/// writer (via the [`Resolver`]) and materializing the value from that writer's
/// transaction object.
#[derive(Clone)]
pub struct Reader {
    resolver: Resolver,
    objects: ObjectCache,
    tmon: Monitor,
    retry: RetryConfig,
}

impl Reader {
    /// Creates a reader over the shard coordination store and a monitor.
    /// Effective-writer resolution is delegated to a [`Resolver`] built over the
    /// same `shards`/`tmon`. A read revalidates the coordination state it needs
    /// (`Latest`): a cached node whose backend version is unchanged is served
    /// without re-transferring its body (a conditional round-trip), and a
    /// terminal transaction object is served straight from cache (ADR-036), so a
    /// hot read transfers no object bodies while still observing current state.
    pub fn new(shards: ShardStore, tmon: Monitor, retry: RetryConfig) -> Self {
        Reader {
            objects: shards.object_cache(),
            resolver: Resolver::new(shards, tmon.clone()),
            tmon,
            retry,
        }
    }

    /// Reads `key`, returning `None` when the key is absent or deleted.
    ///
    /// A read is idempotent, so a transient in-doubt (`Unavailable`) outcome is
    /// retried in place with exponential backoff up to
    /// [`READ_UNAVAILABLE_RETRIES`] times. A persistent outage surfaces the last
    /// `Unavailable` error for the caller to classify; the caller cancels by
    /// dropping the future at any `.await` (e.g. via `tokio::time::timeout`).
    pub async fn read(&self, key: &str) -> Result<ReadOutcome, StorageError> {
        let mut backoff = self.retry.backoff();
        for _ in 0..READ_UNAVAILABLE_RETRIES {
            match self.read_once(key).await {
                Err(StorageError::Unavailable(_)) => rt::sleep(backoff.next_delay()).await,
                other => return other,
            }
        }
        // Final attempt: surface whatever it returns, including a persistent
        // `Unavailable` that the caller maps to `Error::Unavailable`.
        self.read_once(key).await
    }

    /// A single read attempt. Resolves and materializes from cache when possible,
    /// reporting a cache hit when it transferred no object bodies from the
    /// backend (a cheap conditional revalidation still counts as a hit). Wrapped
    /// by [`Reader::read`] for in-place retries.
    async fn read_once(&self, key: &str) -> Result<ReadOutcome, StorageError> {
        let before = self.objects.body_reads();
        let value = self.resolve_value(key).await?;
        let cache_hit = self.objects.body_reads() == before;
        Ok(ReadOutcome { value, cache_hit })
    }

    /// Resolves `key` to its effective writer (via the [`Resolver`]), then
    /// materializes the value from that writer's transaction object. Uses cached
    /// coordination state where available; the commit path enforces freshness.
    async fn resolve_value(&self, key: &str) -> Result<Option<ReadValue>, StorageError> {
        let Some(writer) = self
            .resolver
            .effective_writer(key, Requirement::Any)
            .await?
        else {
            // Absent or tombstoned: not found.
            return Ok(None);
        };
        let cv = self
            .tmon
            .committed_value(key, &writer, Requirement::Any)
            .await
            .map_err(trans_to_storage)?;
        if cv.status != TxCommitStatus::Ok || cv.value.not_written || cv.value.deleted {
            // Not authoritatively resolvable as a live value (in-doubt, not
            // written, or a tombstone). Report absence so validation retries
            // rather than trusting a placeholder.
            return Ok(None);
        }
        Ok(Some(ReadValue {
            value: cv.value.value,
            version: Version { writer },
        }))
    }
}
