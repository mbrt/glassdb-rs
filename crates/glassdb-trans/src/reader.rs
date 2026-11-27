//! The transactional read path (ADR-017/020).
//!
//! A key's value does not live in a per-key object. It lives inline in the
//! leaf, or in the transaction record of the key's writer. Reading a key
//! therefore resolves its leaf entry to an *effective writer* — delegated to
//! the [`KeyResolver`], the shared home for that coordination step — and then
//! materializes the value from that writer's decoded transaction record through
//! the [`Monitor`].

use std::sync::Arc;

use glassdb_concurr::{RetrySchedule, rt};
use glassdb_data::{CollectionAddress, LogicalKey, TxId};
use glassdb_storage::transaction::TxCommitStatus;
use glassdb_storage::{Requirement, StorageError, Timeline};

use crate::access::{ReadEvidence, ScanMutation, ScanRange};
use crate::error::trans_to_storage;
use crate::key_resolver::{KeyResolver, ScanResult};
use crate::key_state_resolver::ResolvedValue;

const READ_UNAVAILABLE_RETRIES: usize = 5;

/// Reads the keys of one execution of a transaction body. The reader is fixed
/// for the execution: the first execution of a stale view call reads at the
/// floor of the view (ADR-103), and a body replay gets another reader.
#[derive(Clone)]
pub struct BodyReader {
    reader: Reader,
    resolver: KeyResolver,
}

impl BodyReader {
    pub(crate) fn new(resolver: KeyResolver, timeline: Timeline, retry: RetrySchedule) -> Self {
        Self {
            reader: Reader::new(resolver.clone(), timeline, retry),
            resolver,
        }
    }

    /// Reads one logical key from any usable cached state. The transaction
    /// validates the read later.
    pub async fn read(&self, key: &LogicalKey) -> Result<ReadOutcome, StorageError> {
        self.reader.read(key).await
    }

    /// Scans one logical key range and returns its validation evidence.
    pub async fn scan(
        &self,
        collection: &CollectionAddress,
        range: &ScanRange,
        overlay: &[ScanMutation],
    ) -> Result<ScanResult, StorageError> {
        match self
            .resolver
            .scan_keys(collection, range, overlay, None, None)
            .await
        {
            // A missing route can be a cached absence. Scan again after a new
            // barrier (missing-object retries), so that only a bounded absence
            // reports a dropped collection.
            Err(StorageError::StaleCollection) => {
                let fresh = Requirement::after(self.reader.timeline.currentness_barrier());
                self.resolver
                    .scan_keys_at(collection, range, overlay, None, None, fresh)
                    .await
            }
            result => result,
        }
    }
}

/// The result of reading a key: the raw value and its writer. The writer is the
/// *effective writer* the read resolved through, which optimistic validation
/// checks at commit.
#[derive(Debug, Clone)]
pub struct ReadValue {
    pub value: Arc<[u8]>,
    pub writer: TxId,
}

/// The value of a logical key and its validation evidence.
#[derive(Debug, Clone)]
pub struct ReadOutcome {
    /// The resolved value, or `None` when the key is absent or deleted.
    pub value: Option<ReadValue>,
    evidence: ReadEvidence,
}

impl ReadOutcome {
    /// Creates a read outcome carrying opaque validation evidence.
    pub fn new(value: Option<ReadValue>, evidence: ReadEvidence) -> Self {
        Self { value, evidence }
    }

    /// Consumes the outcome into its value and validation evidence.
    pub fn into_parts(self) -> (Option<ReadValue>, ReadEvidence) {
        (self.value, self.evidence)
    }
}

/// Reads values by resolving a key's leaf entry to its effective committed
/// writer (via the [`KeyResolver`]) and materializing the value from that writer's
/// transaction record.
#[derive(Clone)]
pub struct Reader {
    resolver: KeyResolver,
    timeline: Timeline,
    retry: RetrySchedule,
}

impl Reader {
    /// Creates a point reader with retry backoff for transient failures.
    pub fn new(resolver: KeyResolver, timeline: Timeline, retry: RetrySchedule) -> Self {
        Reader {
            resolver,
            timeline,
            retry,
        }
    }

    /// Reads `key` from any usable cached state, returning `None` when the key
    /// is absent or deleted. The transaction validates the read later.
    ///
    /// A read is idempotent, so a transient in-doubt (`Unavailable`) outcome is
    /// retried in place with exponential backoff up to five times.
    /// A persistent outage surfaces the last `Unavailable` error for the caller
    /// to classify; the caller cancels by
    /// dropping the future at any `.await` (e.g. via `tokio::time::timeout`).
    pub async fn read(&self, key: &LogicalKey) -> Result<ReadOutcome, StorageError> {
        let mut backoff = self.retry.backoff();
        // Proof: later validation. `resolve_value` proves a missing route.
        for _ in 0..READ_UNAVAILABLE_RETRIES {
            match self.resolve_value(key, Requirement::ANY).await {
                Err(StorageError::Unavailable(_)) => rt::sleep(backoff.next_delay()).await,
                other => return other,
            }
        }
        // Final attempt: surface whatever it returns, including a persistent
        // `Unavailable` that the caller maps to `Error::Unavailable`.
        self.resolve_value(key, Requirement::ANY).await
    }

    /// Resolves `key` to its effective writer (via the [`KeyResolver`]), then
    /// materializes the value from that writer's transaction record.
    async fn resolve_value(
        &self,
        key: &LogicalKey,
        requirement: glassdb_storage::Requirement,
    ) -> Result<ReadOutcome, StorageError> {
        let mut requirement = requirement;
        // The last writer whose record GC deleted after the leaf named it.
        let mut reclaimed = None;
        let mut missing_retried = false;
        loop {
            let (resolved, leaf) = match self.resolver.resolve_key(key, requirement).await {
                Ok(resolved) => resolved,
                Err(crate::error::TransError::ValidateRetry(fresh)) => {
                    requirement = requirement.stricter(fresh);
                    rt::yield_now().await;
                    continue;
                }
                // A missing route can be a cached absence. Route again after a
                // new barrier (missing-object retries), so that only a bounded
                // absence reports a dropped collection.
                Err(crate::error::TransError::Storage(StorageError::StaleCollection))
                    if !missing_retried =>
                {
                    missing_retried = true;
                    requirement = requirement
                        .stricter(Requirement::after(self.timeline.currentness_barrier()));
                    continue;
                }
                Err(error) => return Err(trans_to_storage(error)),
            };
            let leaf = leaf.observation;
            let Some(writer) = resolved.writer else {
                return Ok(ReadOutcome::new(None, ReadEvidence::new(None, leaf)));
            };
            debug_assert_ne!(
                Some(writer),
                reclaimed,
                "a leaf load after the record of the writer was deleted names it again"
            );
            let last_writer = Some(writer);
            // An inline value or tombstone in the leaf is the writer's own
            // authoritative evidence, so the transaction record adds nothing
            // (ADR-051).
            match resolved.value {
                ResolvedValue::Inline(value) => {
                    return Ok(ReadOutcome::new(
                        Some(ReadValue { value, writer }),
                        ReadEvidence::new(last_writer, leaf),
                    ));
                }
                ResolvedValue::Tombstone => {
                    return Ok(ReadOutcome::new(None, ReadEvidence::new(last_writer, leaf)));
                }
                ResolvedValue::External | ResolvedValue::Unresolved => {}
            }
            // A cached status can be older than the commit that the leaf
            // shows, so the status read is current.
            let current = Requirement::after(self.timeline.currentness_barrier());
            let cv = match self.resolver.committed_value(key, &writer, current).await {
                Ok(value) => value,
                Err(crate::error::TransError::ValidateRetry(fresh)) => {
                    requirement = requirement.stricter(fresh);
                    rt::yield_now().await;
                    continue;
                }
                Err(error) => return Err(trans_to_storage(error)),
            };
            if cv.status != TxCommitStatus::Committed {
                // The resolver names only a committed writer: the current
                // writer of the entry, or a holder whose status it read as
                // committed. So another status means that GC deleted the
                // record after the leaf load, and the monitor reads an empty
                // record path. GC deletes the record of a committed writer
                // only after no leaf names it, and a node state never repeats
                // (ADR-105). So a leaf load after this status read names a
                // newer writer. The read must not report no value here:
                // validation compares only the writer, so it would accept the
                // read.
                reclaimed = Some(writer);
                requirement = Requirement::after(self.timeline.currentness_barrier());
                rt::yield_now().await;
                continue;
            }
            if cv.value.not_written {
                // The writer committed but wrote no value for this key: a genuine
                // absence, independent of freshness.
                return Ok(ReadOutcome::new(None, ReadEvidence::new(last_writer, leaf)));
            }
            let value = (!cv.value.deleted).then_some(ReadValue {
                value: cv.value.value,
                writer,
            });
            return Ok(ReadOutcome::new(
                value,
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
    use crate::retry_timing::RetryTiming;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_data::{CollectionAddress, DbPrefix, TxId};
    use glassdb_storage::transaction::{TxRecord, TxWrite};
    use glassdb_storage::{CurrentState, LeafBody, LeafEntry, Node, TreeRouter};
    use std::num::NonZeroUsize;

    fn tx_id(prefix: &[u8]) -> TxId {
        TxId::with_priority(0, prefix)
    }

    #[tokio::test]
    async fn reclaimed_transaction_bodies_refresh_cached_writers_and_holders() {
        let cases = [(false, 0), (true, 0), (true, 1), (true, 2), (true, 3)];
        for ((held, operation), wounded) in cases
            .into_iter()
            .flat_map(|case| [(case, false), (case, true)])
        {
            let backend = Arc::new(MemoryBackend::new());
            let local = AssemblyFixture::new(
                backend.clone(),
                DbPrefix::try_from("db").unwrap(),
                &EngineConfig::default(),
            );
            let peer = AssemblyFixture::new(
                backend,
                DbPrefix::try_from("db").unwrap(),
                &EngineConfig::default(),
            );
            let collection = CollectionAddress::root("db");
            let key = LogicalKey::new(collection.clone(), b"key");
            let old = tx_id(&[1]);
            let mut record = TxRecord::new(old, TxCommitStatus::Committed);
            record.writes.push(TxWrite {
                key: key.clone(),
                value: Arc::from(&b"old"[..]),
                deleted: false,
            });
            let local_record = local.tx_records.set(&record).await.unwrap();
            let mut entry = LeafEntry::new(b"key");
            if held {
                entry.replace_write_lock(old);
            } else {
                entry.current = CurrentState::External { writer: old };
            }
            let mut leaf = Node::leaf(LeafBody::from_entries([entry]));
            if held {
                // A committed writer of a key that can change the existence
                // of the key holds the membership lock too.
                leaf.set_membership_writer(old);
            }
            local.nodes.create_root(&collection, &leaf).await.unwrap();
            let resolver = KeyResolver::new(
                TreeRouter::new(local.nodes.clone(), NonZeroUsize::MIN),
                KeyStateResolver::new(local.monitor.clone()),
                NonZeroUsize::MIN,
            );
            let reader = Reader::new(
                resolver.clone(),
                local.timeline.clone(),
                RetrySchedule::default(),
            );
            let (value, _) = reader.read(&key).await.unwrap().into_parts();
            assert_eq!(value.unwrap().value.as_ref(), b"old");
            let (_, root) = peer
                .nodes
                .load_root(&collection, Requirement::ANY)
                .await
                .unwrap();
            let updated = Node::leaf(LeafBody::from_entries([LeafEntry::new(b"key")
                .with_current(CurrentState::Inline {
                    writer: tx_id(&[2]),
                    value: Arc::from(&b"new"[..]),
                })]));
            peer.nodes
                .store_root(&collection, &updated, &root)
                .await
                .unwrap();
            let observed = peer
                .tx_records
                .get_at(&old, Requirement::ANY)
                .await
                .unwrap();
            peer.tx_records.delete(&observed).await.unwrap();
            local.tx_records.delete(&local_record).await.unwrap();
            assert!(matches!(
                local.tx_records.get_at(&old, Requirement::ANY).await,
                Err(StorageError::NotFound)
            ));
            if wounded {
                // A peer that resolves the holder from a leaf observation
                // older than the write-back finds no record and wounds the
                // identity (a leaked wound, ADR-079).
                peer.tx_records
                    .set(&TxRecord::new(old, TxCommitStatus::BlindWounded))
                    .await
                    .unwrap();
            }
            match operation {
                0 => {
                    let (value, _) = reader.read(&key).await.unwrap().into_parts();
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
                        .effective_point_states(std::slice::from_ref(&key), None, Requirement::ANY)
                        .await
                        .unwrap();
                    assert_eq!(states[0].writer, Some(tx_id(&[2])));
                }
                _ => {
                    use crate::access::{AccessSet, WriteAccess};
                    use crate::tlocker::LockOutcome;
                    let locker = new_locker(&local);
                    let writer = tx_id(&[3]);
                    local.monitor.begin_tx(&writer);
                    let accesses = AccessSet::new(
                        Vec::new(),
                        vec![WriteAccess::put(key.clone(), Arc::from(&b"next"[..]))],
                        Vec::new(),
                    );
                    assert!(matches!(
                        locker
                            .keys()
                            .lock_at(&writer, &accesses, false, Requirement::ANY)
                            .await
                            .unwrap(),
                        LockOutcome::Locked(_)
                    ));
                    let (node, _) = peer
                        .nodes
                        .load_root(
                            &collection,
                            Requirement::after(peer.timeline.currentness_barrier()),
                        )
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

    fn new_locker(local: &AssemblyFixture) -> crate::tlocker::Locker {
        use crate::collection_coordination::CollectionStateResolver;
        use crate::leaf_coord::{CoordinatorConfig, LeafCoordinator, NoStructuralHints};
        use crate::tlocker::Locker;
        let coord = LeafCoordinator::with_hinter(
            local.nodes.clone(),
            KeyStateResolver::new(local.monitor.clone()),
            local.monitor.clone(),
            CoordinatorConfig::default(),
            Arc::new(NoStructuralHints),
        );
        let state = CollectionStateResolver::new(
            local.records.clone(),
            local.tx_records.clone(),
            local.timeline.clone(),
            local.monitor.clone(),
            RetrySchedule::default(),
        );
        Locker::new(
            coord,
            TreeRouter::new(local.nodes.clone(), NonZeroUsize::MIN),
            state,
            local.monitor.clone(),
            RetryTiming::default(),
            NonZeroUsize::MIN,
        )
    }

    // A stray holder of a committed identity is a lock on a key that it did
    // not write (ADR-095). ADR-105 stops the protocol from leaving one, but the
    // reader rule does not depend on that. A current leaf still names it after GC
    // reclaimed its record. A leaf observation after this instance found the
    // record reclaimed shows no value of the identity (ADR-104), so the key
    // resolves after one reload and is not resolved again and again.
    #[tokio::test]
    async fn a_stray_holder_of_a_reclaimed_commit_does_not_block_its_key() {
        use crate::access::{AccessSet, WriteAccess};
        use crate::tlocker::LockOutcome;
        use glassdb_backend::middleware::RecordingBackend;

        for (lock, wounded) in [(false, false), (false, true), (true, false), (true, true)] {
            let backend = Arc::new(MemoryBackend::new());
            let recording = RecordingBackend::new(backend.clone());
            let operations = recording.log();
            let local = AssemblyFixture::new(
                Arc::new(recording),
                DbPrefix::try_from("db").unwrap(),
                &EngineConfig::default(),
            );
            let peer = AssemblyFixture::new(
                backend,
                DbPrefix::try_from("db").unwrap(),
                &EngineConfig::default(),
            );
            let collection = CollectionAddress::root("db");
            let a = LogicalKey::new(collection.clone(), b"a");
            let b = LogicalKey::new(collection.clone(), b"b");
            let stray = tx_id(&[1]);
            let mut record = TxRecord::new(stray, TxCommitStatus::Committed);
            record.writes.push(TxWrite {
                key: a.clone(),
                value: Arc::from(&b"a1"[..]),
                deleted: false,
            });
            let local_record = local.tx_records.set(&record).await.unwrap();
            let mut held = LeafEntry::new(b"b").with_current(CurrentState::Inline {
                writer: tx_id(&[0]),
                value: Arc::from(&b"b0"[..]),
            });
            held.replace_write_lock(stray);
            let leaf = Node::leaf(LeafBody::from_entries([
                LeafEntry::new(b"a").with_current(CurrentState::External { writer: stray }),
                held.clone(),
            ]));
            local.nodes.create_root(&collection, &leaf).await.unwrap();
            let reader_of = |fixture: &AssemblyFixture| {
                Reader::new(
                    KeyResolver::new(
                        TreeRouter::new(fixture.nodes.clone(), NonZeroUsize::MIN),
                        KeyStateResolver::new(fixture.monitor.clone()),
                        NonZeroUsize::MIN,
                    ),
                    fixture.timeline.clone(),
                    RetrySchedule::default(),
                )
            };
            let reader = reader_of(&local);
            // The instance learns that the identity committed.
            let (value, _) = reader.read(&a).await.unwrap().into_parts();
            assert_eq!(value.unwrap().value.as_ref(), b"a1");

            // A later writer replaces the identity on `a`, and GC on this
            // instance deletes its record. The stray holder on `b` stays.
            let (_, root) = peer
                .nodes
                .load_root(&collection, Requirement::ANY)
                .await
                .unwrap();
            let updated = Node::leaf(LeafBody::from_entries([
                LeafEntry::new(b"a").with_current(CurrentState::Inline {
                    writer: tx_id(&[2]),
                    value: Arc::from(&b"a2"[..]),
                }),
                held,
            ]));
            peer.nodes
                .store_root(&collection, &updated, &root)
                .await
                .unwrap();
            local.tx_records.delete(&local_record).await.unwrap();
            if wounded {
                // A leaked wound (ADR-079).
                peer.tx_records
                    .set(&TxRecord::new(stray, TxCommitStatus::BlindWounded))
                    .await
                    .unwrap();
            }
            // A peer that does not know the identity reads the value of the
            // earlier writer.
            let (value, _) = reader_of(&peer).read(&b).await.unwrap().into_parts();
            assert_eq!(value.unwrap().value.as_ref(), b"b0");

            operations.lock().unwrap().clear();
            let timeout = std::time::Duration::from_secs(5);
            if lock {
                let writer = tx_id(&[3]);
                local.monitor.begin_tx(&writer);
                let accesses = AccessSet::new(
                    Vec::new(),
                    vec![WriteAccess::put(b.clone(), Arc::from(&b"next"[..]))],
                    Vec::new(),
                );
                let locker = new_locker(&local);
                let outcome = tokio::time::timeout(
                    timeout,
                    locker
                        .keys()
                        .lock_at(&writer, &accesses, false, Requirement::ANY),
                )
                .await
                .expect("the lock of b did not finish")
                .unwrap();
                assert!(matches!(outcome, LockOutcome::Locked(_)));
            } else {
                let (value, _) = tokio::time::timeout(timeout, reader.read(&b))
                    .await
                    .expect("the read of b did not finish")
                    .unwrap()
                    .into_parts();
                assert_eq!(value.unwrap().value.as_ref(), b"b0");
            }
            let count = operations.lock().unwrap().len();
            assert!(count <= 8, "{count} backend operations");
        }
    }

    /// Reads a key whose writer GC reclaims before each status read, so a
    /// read never names a writer that it can resolve. Each reclaim needs a
    /// newer writer on the leaf, so a later load names the next writer.
    #[tokio::test]
    async fn a_read_resolves_again_while_gc_reclaims_each_writer_that_it_names() {
        let inner = Arc::new(MemoryBackend::new());
        let hooked = glassdb_backend::middleware::HookBackend::new(inner.clone());
        let local = AssemblyFixture::new(
            hooked.clone(),
            DbPrefix::try_from("db").unwrap(),
            &EngineConfig::default(),
        );
        let peer = Arc::new(AssemblyFixture::new(
            inner,
            DbPrefix::try_from("db").unwrap(),
            &EngineConfig::default(),
        ));
        let collection = CollectionAddress::root("db");
        let key = LogicalKey::new(collection.clone(), b"key");
        let writers = [tx_id(&[1]), tx_id(&[2]), tx_id(&[3])];
        for (i, writer) in writers.iter().enumerate() {
            let mut record = TxRecord::new(*writer, TxCommitStatus::Committed);
            record.writes.push(TxWrite {
                key: key.clone(),
                value: Arc::from(format!("v{i}").as_bytes()),
                deleted: false,
            });
            peer.tx_records.set(&record).await.unwrap();
        }
        let leaf_of = |writer: TxId| {
            Node::leaf(LeafBody::from_entries([
                LeafEntry::new(b"key").with_current(CurrentState::External { writer })
            ]))
        };
        peer.nodes
            .create_root(&collection, &leaf_of(writers[0]))
            .await
            .unwrap();

        // Before this instance reads the record of the first two writers,
        // the next writer replaces it on the leaf and GC deletes its record.
        let record_paths: Vec<String> = writers
            .iter()
            .map(|id| {
                glassdb_data::ObjectPath::Transaction {
                    db_prefix: DbPrefix::try_from("db").unwrap(),
                    id: *id,
                }
                .to_string()
            })
            .collect();
        let reclaimed = Arc::new(std::sync::Mutex::new(0));
        hooked.set_before(move |op| {
            let path = op.path().to_string();
            let peer = peer.clone();
            let reclaimed = reclaimed.clone();
            let record_paths = record_paths.clone();
            let collection = collection.clone();
            Box::pin(async move {
                let next = {
                    let mut reclaimed = reclaimed.lock().unwrap();
                    let n = *reclaimed;
                    if n < 2 && path == record_paths[n] {
                        *reclaimed += 1;
                        Some(n)
                    } else {
                        None
                    }
                };
                if let Some(n) = next {
                    let (_, root) = peer
                        .nodes
                        .load_root(&collection, Requirement::ANY)
                        .await
                        .unwrap();
                    peer.nodes
                        .store_root(&collection, &leaf_of(writers[n + 1]), &root)
                        .await
                        .unwrap();
                    let observed = peer
                        .tx_records
                        .get_at(&writers[n], Requirement::ANY)
                        .await
                        .unwrap();
                    peer.tx_records.delete(&observed).await.unwrap();
                }
                Ok(())
            })
        });

        let reader = Reader::new(
            KeyResolver::new(
                TreeRouter::new(local.nodes.clone(), NonZeroUsize::MIN),
                KeyStateResolver::new(local.monitor.clone()),
                NonZeroUsize::MIN,
            ),
            local.timeline.clone(),
            RetrySchedule::default(),
        );
        let (value, _) = reader.read(&key).await.unwrap().into_parts();
        let value = value.expect("the key has a committed value");
        assert_eq!(value.value.as_ref(), b"v2");
        assert_eq!(value.writer, writers[2]);
    }
}
