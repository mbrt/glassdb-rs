//! The transactional read path for the v2 object-native engine (ADR-017/020).
//!
//! A key's value no longer lives in a per-key object; it lives in the
//! transaction object of whichever transaction last committed it. Reading a key
//! therefore resolves its shard entry to an *effective writer* — delegated to
//! the [`Resolver`], the shared home for that coordination step — and then
//! materializes the value from that writer's decoded transaction object through
//! the [`Monitor`].

use std::sync::Arc;
use std::time::Duration;

use glassdb_concurr::{RetryConfig, rt};
use glassdb_data::TxId;
use glassdb_storage::{LeafObservation, StorageError, TxCommitStatus, Version};

use crate::error::trans_to_storage;
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

/// The outcome of reading a key, including whether every physical object used
/// to derive it was served locally. An absent value may still be a cache hit.
#[derive(Debug, Clone)]
pub struct ReadOutcome {
    /// The resolved value, or `None` when the key is absent or deleted.
    pub value: Option<ReadValue>,
    /// Effective writer resolved for the key, including a tombstone writer.
    pub last_writer: Option<TxId>,
    /// Whether every physical dependency was served locally.
    pub cache_hit: bool,
    /// Exact leaf state used to derive this logical value.
    pub leaf: LeafObservation,
}

/// Reads values by resolving a key's shard entry to its effective committed
/// writer (via the [`Resolver`]) and materializing the value from that writer's
/// transaction object.
#[derive(Clone)]
pub struct Reader {
    resolver: Resolver,
    retry: RetryConfig,
}

impl Reader {
    /// Creates a reader that resolves and materializes values through
    /// `resolver` using `retry` for transient read failures.
    pub fn new(resolver: Resolver, retry: RetryConfig) -> Self {
        Reader { resolver, retry }
    }

    /// Reads `key`, accepting cached outcomes up to `max_stale` and returning
    /// `None` when the key is absent or deleted.
    ///
    /// A read is idempotent, so a transient in-doubt (`Unavailable`) outcome is
    /// retried in place with exponential backoff up to
    /// [`READ_UNAVAILABLE_RETRIES`] times. A persistent outage surfaces the last
    /// `Unavailable` error for the caller to classify; the caller cancels by
    /// dropping the future at any `.await` (e.g. via `tokio::time::timeout`).
    pub async fn read(&self, key: &str, max_stale: Duration) -> Result<ReadOutcome, StorageError> {
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
    async fn read_once(&self, key: &str, max_stale: Duration) -> Result<ReadOutcome, StorageError> {
        self.resolve_value(key, self.resolver.requirement_within(max_stale))
            .await
    }

    /// Resolves `key` to its effective writer (via the [`Resolver`]), then
    /// materializes the value from that writer's transaction object.
    async fn resolve_value(
        &self,
        key: &str,
        requirement: glassdb_storage::Requirement,
    ) -> Result<ReadOutcome, StorageError> {
        let (resolved, leaf) = self
            .resolver
            .resolve_key(key, requirement)
            .await
            .map_err(trans_to_storage)?;
        let mut cache_hit = leaf.cache_hit;
        cache_hit &= resolved.cache_hit;
        let leaf = leaf.observation;
        let Some(writer) = resolved.writer else {
            return Ok(ReadOutcome {
                value: None,
                last_writer: None,
                cache_hit,
                leaf,
            });
        };
        let last_writer = Some(writer.clone());
        let cv = self
            .resolver
            .committed_value(key, &writer)
            .await
            .map_err(trans_to_storage)?;
        if cv.status != TxCommitStatus::Ok || cv.value.not_written {
            // The writer's value is not authoritatively resolvable yet (e.g. its
            // object is in-doubt). Report absence so transaction validation can
            // retry rather than trusting an empty placeholder.
            return Ok(ReadOutcome {
                value: None,
                last_writer,
                cache_hit: false,
                leaf,
            });
        }
        cache_hit &= cv.cache_hit;
        let version = Version { writer };
        let value = (!cv.value.deleted).then_some(ReadValue {
            value: cv.value.value,
            version,
        });
        Ok(ReadOutcome {
            value,
            last_writer,
            cache_hit,
            leaf,
        })
    }
}
