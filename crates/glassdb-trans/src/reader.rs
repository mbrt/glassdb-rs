//! The transactional read path for the v2 object-native engine (ADR-017/020).
//!
//! A key's value no longer lives in a per-key object; it lives in the
//! transaction object of whichever transaction last committed it. Reading a key
//! therefore resolves its leaf entry to an *effective writer* — delegated to
//! the [`KeyResolver`], the shared home for that coordination step — and then
//! materializes the value from that writer's decoded transaction object through
//! the [`Monitor`].

use std::sync::Arc;
use std::time::Duration;

use glassdb_concurr::{RetryConfig, rt};
use glassdb_data::LogicalKey;
use glassdb_storage::transaction::TxCommitStatus;
use glassdb_storage::{Requirement, StorageError, Timeline, Version};

use crate::access::ReadEvidence;
use crate::error::trans_to_storage;
use crate::key_resolver::KeyResolver;
use crate::key_state_resolver::ResolvedValue;

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
    /// Whether every physical dependency was served locally.
    pub cache_hit: bool,
    evidence: ReadEvidence,
}

impl ReadOutcome {
    /// Creates a read outcome carrying opaque validation evidence.
    pub fn new(value: Option<ReadValue>, cache_hit: bool, evidence: ReadEvidence) -> Self {
        Self {
            value,
            cache_hit,
            evidence,
        }
    }

    /// Consumes the outcome into its value, cache status, and validation evidence.
    pub fn into_parts(self) -> (Option<ReadValue>, bool, ReadEvidence) {
        (self.value, self.cache_hit, self.evidence)
    }
}

/// Reads values by resolving a key's leaf entry to its effective committed
/// writer (via the [`KeyResolver`]) and materializing the value from that writer's
/// transaction object.
#[derive(Clone)]
pub struct Reader {
    resolver: KeyResolver,
    timeline: Timeline,
    retry: RetryConfig,
}

impl Reader {
    /// Creates a reader that resolves and materializes values through
    /// `resolver` using `retry` for transient read failures.
    pub fn new(resolver: KeyResolver, timeline: Timeline, retry: RetryConfig) -> Self {
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
        key: &LogicalKey,
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

    /// A single read attempt: local cache then leaf resolution. Wrapped by
    /// [`Reader::read`] for in-place retries.
    async fn read_once(
        &self,
        key: &LogicalKey,
        max_stale: Duration,
    ) -> Result<ReadOutcome, StorageError> {
        // Bounded-staleness reads are the one foreground operation that owns a
        // freshness policy rather than inheriting a transaction/CAS watermark.
        self.resolve_value(key, Requirement::within(&self.timeline, max_stale))
            .await
    }

    /// Resolves `key` to its effective writer (via the [`KeyResolver`]), then
    /// materializes the value from that writer's transaction object.
    async fn resolve_value(
        &self,
        key: &LogicalKey,
        requirement: glassdb_storage::Requirement,
    ) -> Result<ReadOutcome, StorageError> {
        let mut requirement = requirement;
        let mut refreshed = false;
        loop {
            let (resolved, leaf) = match self.resolver.resolve_key(key, requirement).await {
                Ok(resolved) => resolved,
                Err(crate::error::TransError::ValidateRetry(fresh)) => {
                    requirement = requirement.stricter(fresh);
                    rt::yield_now().await;
                    continue;
                }
                Err(error) => return Err(trans_to_storage(error)),
            };
            let mut cache_hit = leaf.cache_hit;
            cache_hit &= resolved.cache_hit;
            let leaf = leaf.observation;
            let Some(writer) = resolved.writer else {
                return Ok(ReadOutcome::new(
                    None,
                    cache_hit,
                    ReadEvidence::new(None, leaf),
                ));
            };
            let last_writer = Some(writer.clone());
            // An inline value or tombstone in the leaf is the writer's own
            // authoritative evidence, so the transaction object adds nothing
            // (ADR-051).
            match resolved.value {
                ResolvedValue::Inline(value) => {
                    return Ok(ReadOutcome::new(
                        Some(ReadValue {
                            value,
                            version: Version { writer },
                        }),
                        cache_hit,
                        ReadEvidence::new(last_writer, leaf),
                    ));
                }
                ResolvedValue::Tombstone => {
                    return Ok(ReadOutcome::new(
                        None,
                        cache_hit,
                        ReadEvidence::new(last_writer, leaf),
                    ));
                }
                ResolvedValue::External | ResolvedValue::Unresolved => {}
            }
            let cv = match self.resolver.committed_value(key, &writer).await {
                Ok(value) => value,
                Err(crate::error::TransError::ValidateRetry(fresh)) => {
                    requirement = requirement.stricter(fresh);
                    rt::yield_now().await;
                    continue;
                }
                Err(error) => return Err(trans_to_storage(error)),
            };
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
                return Ok(ReadOutcome::new(
                    None,
                    false,
                    ReadEvidence::new(last_writer, leaf),
                ));
            }
            if cv.value.not_written {
                // The writer committed but wrote no value for this key: a genuine
                // absence, independent of freshness.
                return Ok(ReadOutcome::new(
                    None,
                    false,
                    ReadEvidence::new(last_writer, leaf),
                ));
            }
            cache_hit &= cv.cache_hit;
            let version = Version { writer };
            let value = (!cv.value.deleted).then_some(ReadValue {
                value: cv.value.value,
                version,
            });
            return Ok(ReadOutcome::new(
                value,
                cache_hit,
                ReadEvidence::new(last_writer, leaf),
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{AssemblyFixture, EngineConfig};
    use crate::key_state_resolver::KeyStateResolver;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_data::{CollectionAddress, DbRoot, TxId};
    use glassdb_storage::transaction::{TxLog, TxWrite};
    use glassdb_storage::{CurrentState, LeafBody, LeafEntry, Node, TreeRouter};
    use std::num::NonZeroUsize;

    #[tokio::test]
    async fn reclaimed_transaction_bodies_refresh_cached_writers_and_holders() {
        for (held, operation) in [(false, 0), (true, 0), (true, 1), (true, 2), (true, 3)] {
            let backend = Arc::new(MemoryBackend::new());
            let local = AssemblyFixture::new(
                backend.clone(),
                DbRoot::try_from("db").unwrap(),
                &EngineConfig::default(),
            );
            let peer = AssemblyFixture::new(
                backend,
                DbRoot::try_from("db").unwrap(),
                &EngineConfig::default(),
            );
            let collection = CollectionAddress::root("db");
            let key = LogicalKey::new(collection.clone(), b"key");
            let old = TxId::from_bytes(vec![1]);
            let mut log = TxLog::new(old.clone(), TxCommitStatus::Ok);
            log.writes.push(TxWrite {
                key: key.clone(),
                value: Arc::from(&b"old"[..]),
                deleted: false,
                prev_writer: TxId::default(),
            });
            let local_log = local.tlogger.set(&log).await.unwrap();
            let mut entry = LeafEntry::new(b"key");
            if held {
                entry.replace_write_lock(old.clone());
            } else {
                entry.current = CurrentState::External {
                    writer: old.clone(),
                };
            }
            local
                .nodes
                .create_root(&collection, &Node::leaf(LeafBody::from_entries([entry])))
                .await
                .unwrap();
            let resolver = KeyResolver::new(
                TreeRouter::new(local.nodes.clone(), NonZeroUsize::MIN),
                KeyStateResolver::new(local.monitor.clone()),
                NonZeroUsize::MIN,
            );
            let reader = Reader::new(
                resolver.clone(),
                local.timeline.clone(),
                RetryConfig::default(),
            );
            let (value, _, _) = reader.read(&key, Duration::MAX).await.unwrap().into_parts();
            assert_eq!(value.unwrap().value.as_ref(), b"old");
            let (_, root) = peer
                .nodes
                .load_root(&collection, Requirement::Any)
                .await
                .unwrap();
            let updated = Node::leaf(LeafBody::from_entries([LeafEntry::new(b"key")
                .with_current(CurrentState::Inline {
                    writer: TxId::from_bytes(vec![2]),
                    value: Arc::from(&b"new"[..]),
                })]));
            peer.nodes
                .store_root(&collection, &updated, &root)
                .await
                .unwrap();
            let observed = peer.tlogger.get_at(&old, Requirement::Any).await.unwrap();
            peer.tlogger.delete(&observed).await.unwrap();
            local.tlogger.delete(&local_log).await.unwrap();
            assert!(matches!(
                local.tlogger.get_at(&old, Requirement::Any).await,
                Err(StorageError::NotFound)
            ));
            match operation {
                0 => {
                    let (value, _, _) =
                        reader.read(&key, Duration::MAX).await.unwrap().into_parts();
                    assert_eq!(value.unwrap().value.as_ref(), b"new");
                }
                1 => {
                    let page = resolver
                        .scan_keys(
                            &collection,
                            &crate::access::ScanRange::all(),
                            &[],
                            None,
                            None,
                        )
                        .await
                        .unwrap();
                    assert_eq!(page.keys(), &[b"key".to_vec()]);
                }
                2 => {
                    let states = resolver
                        .effective_point_states(std::slice::from_ref(&key), None, Requirement::Any)
                        .await
                        .unwrap();
                    assert_eq!(states[0].writer, Some(TxId::from_bytes(vec![2])));
                }
                _ => {
                    use crate::access::{AccessSet, WriteAccess};
                    use crate::collection_coordination::CollectionStateResolver;
                    use crate::leaf_coord::{LeafCoordinator, SplitHinter};
                    use crate::tlocker::{LockOutcome, Locker};
                    struct NoSplitHints;
                    impl SplitHinter for NoSplitHints {
                        fn observe_leaf(&self, _: &glassdb_data::ObjectPath, _: &LeafBody) {}
                    }
                    let coord = LeafCoordinator::with_hinter(
                        local.nodes.clone(),
                        KeyStateResolver::new(local.monitor.clone()),
                        local.monitor.clone(),
                        RetryConfig::default(),
                        glassdb_storage::SplitPolicy::default(),
                        Arc::new(NoSplitHints),
                    );
                    let state = CollectionStateResolver::new(
                        local.records.clone(),
                        local.tlogger.clone(),
                        local.timeline.clone(),
                        local.monitor.clone(),
                        RetryConfig::default(),
                    );
                    let locker = Locker::new(
                        coord,
                        TreeRouter::new(local.nodes.clone(), NonZeroUsize::MIN),
                        state,
                        local.monitor.clone(),
                        RetryConfig::default(),
                        NonZeroUsize::MIN,
                    );
                    let writer = TxId::from_bytes(vec![3]);
                    local.monitor.begin_tx(&writer);
                    let accesses = AccessSet::new(
                        Vec::new(),
                        vec![WriteAccess::put(key.clone(), Arc::from(&b"next"[..]))],
                        Vec::new(),
                    );
                    assert!(matches!(
                        locker
                            .keys()
                            .lock_at(&writer, &accesses, false, Requirement::Any)
                            .await
                            .unwrap(),
                        LockOutcome::Locked(_)
                    ));
                    let (node, _) = peer
                        .nodes
                        .load_root(&collection, Requirement::AtLeast(peer.timeline.now()))
                        .await
                        .unwrap();
                    assert_eq!(
                        node.as_leaf()
                            .unwrap()
                            .lookup(b"key")
                            .unwrap()
                            .lock_holders(),
                        std::slice::from_ref(&writer)
                    );
                }
            }
        }
    }
}
