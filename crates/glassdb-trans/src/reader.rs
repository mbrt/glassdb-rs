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
use glassdb_data::{KeyRef, TxId};
use glassdb_storage::{
    LeafObservation, Requirement, StorageError, Timeline, TxCommitStatus, Version,
};

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
    timeline: Timeline,
    retry: RetryConfig,
}

impl Reader {
    /// Creates a reader that resolves and materializes values through
    /// `resolver` using `retry` for transient read failures.
    pub fn new(resolver: Resolver, timeline: Timeline, retry: RetryConfig) -> Self {
        Reader {
            resolver,
            timeline,
            retry,
        }
    }

    /// Reads `key`, accepting cached outcomes up to `max_stale` and returning
    /// `None` when the key is absent or deleted.
    ///
    /// A read is idempotent, so a transient in-doubt (`Unavailable`) outcome is
    /// retried in place with exponential backoff up to
    /// [`READ_UNAVAILABLE_RETRIES`] times. A persistent outage surfaces the last
    /// `Unavailable` error for the caller to classify; the caller cancels by
    /// dropping the future at any `.await` (e.g. via `tokio::time::timeout`).
    pub async fn read(
        &self,
        key: &KeyRef,
        max_stale: Duration,
    ) -> Result<ReadOutcome, StorageError> {
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
    async fn read_once(
        &self,
        key: &KeyRef,
        max_stale: Duration,
    ) -> Result<ReadOutcome, StorageError> {
        // Bounded-staleness reads are the one foreground operation that owns a
        // freshness policy rather than inheriting a transaction/CAS watermark.
        self.resolve_value(key, Requirement::within(&self.timeline, max_stale))
            .await
    }

    /// Resolves `key` to its effective writer (via the [`Resolver`]), then
    /// materializes the value from that writer's transaction object.
    async fn resolve_value(
        &self,
        key: &KeyRef,
        requirement: glassdb_storage::Requirement,
    ) -> Result<ReadOutcome, StorageError> {
        let mut requirement = requirement;
        let mut refreshed = false;
        loop {
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
            if cv.status != TxCommitStatus::Ok {
                // The resolved writer's transaction object is not authoritatively
                // committed. A staleness-tolerant resolution can name a writer
                // whose committed log was already garbage-collected: it read a
                // cached leaf still pointing at a `current_writer` that newer
                // commits superseded, and GC reclaimed that log once no *fresh*
                // leaf referenced it (ADR-022). That is a stale-leaf signal, not
                // a genuine absence, so re-resolve once against fresh evidence,
                // which sees the key's current writer whose log still exists. A
                // writer that is still unresolvable under fresh evidence is truly
                // in-doubt: report absence so transaction validation retries
                // rather than trusting an empty placeholder.
                let fresh = Requirement::AtLeast(self.timeline.now());
                if !refreshed && requirement.stricter(fresh) != requirement {
                    requirement = fresh;
                    refreshed = true;
                    continue;
                }
                return Ok(ReadOutcome {
                    value: None,
                    last_writer,
                    cache_hit: false,
                    leaf,
                });
            }
            if cv.value.not_written {
                // The writer committed but wrote no value for this key: a genuine
                // absence, independent of freshness.
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
            return Ok(ReadOutcome {
                value,
                last_writer,
                cache_hit,
                leaf,
            });
        }
    }
}
