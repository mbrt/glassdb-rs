//! Transaction-aware interpretation of already-loaded key and node state.

use std::sync::Arc;

use glassdb_data::{KeyRef, TxId};
use glassdb_storage::transaction::TxCommitStatus;
use glassdb_storage::{CurrentState, LockType, Node, Requirement, ShardEntry, StorageError};

use crate::error::{TransError, trans_to_storage};
use crate::monitor::{KeyCommitStatus, Monitor, TxFinalStatus};

/// What is known about the effective writer's value alongside its identity.
///
/// A leaf entry that names the effective writer is authoritative about its own
/// value (ADR-051), so resolution can answer whether that state is inline
/// without touching a transaction object.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) enum ResolvedValue {
    /// The leaf names a writer behind the effective one, so only the effective
    /// writer's transaction object can supply its value.
    #[default]
    Unresolved,
    /// The effective writer's value lives in its transaction object.
    External,
    /// The effective writer's authoritative value bytes.
    Inline(Arc<[u8]>),
    /// The effective writer deleted the key.
    Tombstone,
}

impl ResolvedValue {
    /// Returns what `current` proves about its named writer's value.
    pub(crate) fn from_current(current: &CurrentState) -> Self {
        match current {
            CurrentState::Absent => ResolvedValue::Unresolved,
            CurrentState::External { .. } => ResolvedValue::External,
            CurrentState::Inline { value, .. } => ResolvedValue::Inline(value.clone()),
            CurrentState::Tombstone { .. } => ResolvedValue::Tombstone,
        }
    }
}

/// The effective writer resolved from one loaded shard entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriterResolution {
    pub(crate) writer: Option<TxId>,
    pub(crate) value: ResolvedValue,
    pub(crate) cache_hit: bool,
}

/// One coherent interpretation of an entry's foreign lock holders.
///
/// The effective writer and remaining live holders use one status observation
/// per holder so a concurrent commit cannot produce an inconsistent pair.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct HolderResolution {
    pub(crate) writer: Option<TxId>,
    pub(crate) deleted: bool,
    pub(crate) pending: Vec<TxId>,
}

impl HolderResolution {
    /// Returns the current state that should remain after applying this
    /// resolution to `entry`.
    pub(crate) fn resolved_current(&self, entry: Option<&ShardEntry>) -> CurrentState {
        let Some(writer) = self.writer.clone() else {
            return CurrentState::Absent;
        };
        if let Some(entry) = entry
            && entry.current.writer() == Some(&writer)
        {
            return entry.current.clone();
        }
        if self.deleted {
            CurrentState::Tombstone { writer }
        } else {
            CurrentState::External { writer }
        }
    }
}

/// The effective committed state resolved from one loaded shard entry.
#[derive(Debug, Clone)]
struct EffectiveResolution {
    writer: Option<TxId>,
    value: ResolvedValue,
    deleted: bool,
    cache_hit: bool,
    pending: Vec<TxId>,
}

impl Default for EffectiveResolution {
    fn default() -> Self {
        Self {
            writer: None,
            value: ResolvedValue::Unresolved,
            deleted: false,
            cache_hit: true,
            pending: Vec::new(),
        }
    }
}

impl EffectiveResolution {
    fn from_current(current: &CurrentState) -> Self {
        Self {
            writer: current.writer().cloned(),
            value: ResolvedValue::from_current(current),
            deleted: current.is_tombstone(),
            ..Self::default()
        }
    }

    fn into_writer(self) -> WriterResolution {
        WriterResolution {
            writer: self.writer,
            value: self.value,
            cache_hit: self.cache_hit,
        }
    }

    fn into_holders(self) -> HolderResolution {
        HolderResolution {
            writer: self.writer,
            deleted: self.deleted,
            pending: self.pending,
        }
    }

    fn exists(&self) -> bool {
        self.writer.is_some() && !self.deleted
    }
}

/// Resolves transaction-dependent state already present in loaded B-link
/// nodes and shard entries.
#[derive(Clone)]
pub(crate) struct KeyStateResolver {
    monitor: Monitor,
}

impl KeyStateResolver {
    /// Creates key-state resolution over the transaction monitor.
    pub(crate) fn new(monitor: Monitor) -> Self {
        Self { monitor }
    }

    /// Returns the committed value recorded by `writer` for `key`.
    pub(crate) async fn committed_value(
        &self,
        key: &KeyRef,
        writer: &TxId,
    ) -> Result<KeyCommitStatus, TransError> {
        self.monitor.committed_value(key, writer).await
    }

    /// Rejects a node whose collection-delete intent committed, waiting for a
    /// pending intent and ignoring an aborted one.
    pub(crate) async fn ensure_collection_live(&self, node: &Node) -> Result<(), StorageError> {
        let Some(holder) = node.collection_delete_intent() else {
            return Ok(());
        };
        match self
            .monitor
            .await_tx_final(holder)
            .await
            .map_err(trans_to_storage)?
        {
            TxFinalStatus::Committed => Err(StorageError::StaleCollection),
            TxFinalStatus::Aborted => Ok(()),
        }
    }

    /// Returns foreign pending membership writers represented by `node`.
    pub(crate) async fn pending_membership(
        &self,
        node: &Node,
        own_lock_holder: Option<&TxId>,
        requirement: Requirement,
    ) -> Result<Vec<TxId>, StorageError> {
        let mut pending = Vec::new();
        if node.membership_lock().lock_type() == LockType::Write {
            for holder in node.membership_lock().holders() {
                if own_lock_holder == Some(holder) {
                    continue;
                }
                let status = self
                    .monitor
                    .tx_status_at(holder, requirement)
                    .await
                    .map_err(trans_to_storage)?;
                if status == TxCommitStatus::Pending {
                    pending.push(holder.clone());
                }
            }
        }
        pending.sort();
        Ok(pending)
    }

    /// Resolves an entry's effective committed writer.
    pub(crate) async fn resolve_writer(
        &self,
        key: &KeyRef,
        entry: Option<&ShardEntry>,
        requirement: Requirement,
    ) -> Result<WriterResolution, TransError> {
        Ok(self
            .resolve_effective(key, entry, None, requirement)
            .await?
            .into_writer())
    }

    /// Resolves an entry's effective writer and live foreign holders from one
    /// status observation per holder.
    pub(crate) async fn resolve_holders(
        &self,
        key: &KeyRef,
        entry: Option<&ShardEntry>,
        own_lock_holder: Option<&TxId>,
        requirement: Requirement,
    ) -> Result<HolderResolution, TransError> {
        let mut resolved = self
            .resolve_effective(key, entry, own_lock_holder, requirement)
            .await?;
        if let Some(entry) = entry
            && entry.lock_type == LockType::Read
        {
            for holder in &entry.locked_by {
                if Some(holder) == own_lock_holder {
                    continue;
                }
                let status = self.monitor.tx_status_at(holder, requirement).await?;
                if matches!(status, TxCommitStatus::Pending | TxCommitStatus::Unknown) {
                    resolved.pending.push(holder.clone());
                }
            }
        }
        Ok(resolved.into_holders())
    }

    /// Reports whether one loaded entry represents a live key.
    pub(crate) async fn entry_exists(
        &self,
        key: &KeyRef,
        entry: &ShardEntry,
        own_lock_holder: Option<&TxId>,
        requirement: Requirement,
    ) -> Result<bool, TransError> {
        Ok(self
            .resolve_effective(key, Some(entry), own_lock_holder, requirement)
            .await?
            .exists())
    }

    /// Resolves the effective committed state represented by one loaded entry.
    async fn resolve_effective(
        &self,
        key: &KeyRef,
        entry: Option<&ShardEntry>,
        own_lock_holder: Option<&TxId>,
        requirement: Requirement,
    ) -> Result<EffectiveResolution, TransError> {
        let Some(entry) = entry else {
            return Ok(EffectiveResolution::default());
        };
        let mut resolved = EffectiveResolution::from_current(&entry.current);
        if !matches!(entry.lock_type, LockType::Write | LockType::Create) {
            return Ok(resolved);
        }
        if entry.locked_by.len() > 1 {
            return Err(TransError::other(
                "exclusive shard entry has more than one holder",
            ));
        }
        let Some(holder) = entry.locked_by.first() else {
            return Ok(resolved);
        };
        if Some(holder) == own_lock_holder {
            return Ok(resolved);
        }

        let committed = self
            .monitor
            .committed_value_at(key, holder, requirement)
            .await?;
        resolved.cache_hit &= committed.cache_hit;
        match committed.status {
            TxCommitStatus::Ok => {
                if !committed.value.not_written {
                    resolved.writer = Some(holder.clone());
                    resolved.value = ResolvedValue::Unresolved;
                    resolved.deleted = committed.value.deleted;
                }
            }
            TxCommitStatus::Pending | TxCommitStatus::Unknown => {
                resolved.pending.push(holder.clone());
            }
            TxCommitStatus::Aborted | TxCommitStatus::Wounded => {}
        }
        Ok(resolved)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{OpLog, RecordingBackend};
    use glassdb_concurr::{Background, RetryConfig};
    use glassdb_data::CollectionAddress;
    use glassdb_storage::transaction::{TLogger, TxLock, TxLog, TxWrite};
    use glassdb_storage::{CachedStore, Timeline};

    use super::*;
    use crate::monitor::ProtocolTiming;

    const INLINE_VALUE: &[u8] = b"inline-value";

    fn monitor_over(backend: Arc<dyn Backend>) -> (Monitor, Arc<Background>) {
        let timeline = Timeline::new();
        let objects = CachedStore::new(backend, 1 << 20, timeline.clone(), None);
        let transactions = TLogger::new(objects, "db");
        let background = Arc::new(Background::new());
        let monitor = Monitor::with_config(
            transactions,
            timeline,
            Arc::downgrade(&background),
            RetryConfig::default(),
            ProtocolTiming::default(),
        );
        (monitor, background)
    }

    fn new_monitor() -> (Monitor, Arc<Background>) {
        monitor_over(Arc::new(MemoryBackend::new()))
    }

    struct ResolutionHarness {
        backend: Arc<dyn Backend>,
        operations: OpLog,
        transactions: TLogger,
    }

    impl ResolutionHarness {
        fn new() -> Self {
            let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
            let operations = recorder.log();
            let backend: Arc<dyn Backend> = Arc::new(recorder);
            let timeline = Timeline::new();
            let objects = CachedStore::new(backend.clone(), 1 << 20, timeline, None);
            let transactions = TLogger::new(objects, "db");
            Self {
                backend,
                operations,
                transactions,
            }
        }

        fn resolver(&self) -> (KeyStateResolver, Arc<Background>) {
            let (monitor, background) = monitor_over(self.backend.clone());
            (KeyStateResolver::new(monitor), background)
        }

        async fn seed_transaction(
            &self,
            key: &KeyRef,
            holder: &TxId,
            lock_type: LockType,
            status: TxCommitStatus,
            deleted: Option<bool>,
        ) {
            let mut log = TxLog::new(holder.clone(), status);
            log.locks.push(TxLock::Entry {
                key: key.clone(),
                typ: lock_type,
            });
            if let Some(deleted) = deleted {
                log.writes.push(TxWrite {
                    key: key.clone(),
                    value: Arc::from(b"holder-value".as_slice()),
                    deleted,
                    prev_writer: TxId::default(),
                });
            }
            self.transactions.set(&log).await.unwrap();
        }

        fn clear_operations(&self) {
            self.operations.lock().unwrap().clear();
        }

        fn assert_operations(&self, expected: usize, context: &str) {
            let operations = self.operations.lock().unwrap();
            assert_eq!(
                operations.len(),
                expected,
                "{context}: unexpected operations: {operations:?}"
            );
            assert!(
                operations.iter().all(|operation| {
                    matches!(operation.op, "read" | "read_if_modified")
                        && operation.path.contains("/_t/")
                }),
                "{context}: resolution must only read transaction objects: {operations:?}"
            );
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum CurrentCase {
        Absent,
        External,
        Inline,
        Tombstone,
    }

    impl CurrentCase {
        const ALL: [Self; 4] = [Self::Absent, Self::External, Self::Inline, Self::Tombstone];

        fn name(self) -> &'static str {
            match self {
                Self::Absent => "absent",
                Self::External => "external",
                Self::Inline => "inline",
                Self::Tombstone => "tombstone",
            }
        }

        fn current(self, predecessor: &TxId) -> CurrentState {
            match self {
                Self::Absent => CurrentState::Absent,
                Self::External => CurrentState::External {
                    writer: predecessor.clone(),
                },
                Self::Inline => CurrentState::Inline {
                    writer: predecessor.clone(),
                    value: Arc::from(INLINE_VALUE),
                },
                Self::Tombstone => CurrentState::Tombstone {
                    writer: predecessor.clone(),
                },
            }
        }

        fn writer(self, predecessor: &TxId) -> Option<TxId> {
            match self {
                Self::Absent => None,
                Self::External | Self::Inline | Self::Tombstone => Some(predecessor.clone()),
            }
        }

        fn value(self) -> ResolvedValue {
            match self {
                Self::Absent => ResolvedValue::Unresolved,
                Self::External => ResolvedValue::External,
                Self::Inline => ResolvedValue::Inline(Arc::from(INLINE_VALUE)),
                Self::Tombstone => ResolvedValue::Tombstone,
            }
        }

        fn deleted(self) -> bool {
            matches!(self, Self::Tombstone)
        }

        fn exists(self) -> bool {
            matches!(self, Self::External | Self::Inline)
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum ExclusiveCase {
        Pending,
        CommittedLive,
        CommittedDeleted,
        Aborted,
        Wounded,
    }

    impl ExclusiveCase {
        const ALL: [Self; 5] = [
            Self::Pending,
            Self::CommittedLive,
            Self::CommittedDeleted,
            Self::Aborted,
            Self::Wounded,
        ];

        fn name(self) -> &'static str {
            match self {
                Self::Pending => "pending",
                Self::CommittedLive => "committed-live",
                Self::CommittedDeleted => "committed-deleted",
                Self::Aborted => "aborted",
                Self::Wounded => "wounded",
            }
        }

        fn status(self) -> TxCommitStatus {
            match self {
                Self::Pending => TxCommitStatus::Pending,
                Self::CommittedLive | Self::CommittedDeleted => TxCommitStatus::Ok,
                Self::Aborted => TxCommitStatus::Aborted,
                Self::Wounded => TxCommitStatus::Wounded,
            }
        }

        fn committed_deletion(self) -> Option<bool> {
            match self {
                Self::CommittedLive => Some(false),
                Self::CommittedDeleted => Some(true),
                Self::Pending | Self::Aborted | Self::Wounded => None,
            }
        }

        fn is_committed(self) -> bool {
            matches!(self, Self::CommittedLive | Self::CommittedDeleted)
        }
    }

    struct ProjectionOperations {
        writer: usize,
        holders: usize,
        existence: usize,
    }

    struct ProjectionExpectation {
        writer: WriterResolution,
        holders: HolderResolution,
        current: CurrentState,
        exists: bool,
        operations: ProjectionOperations,
    }

    async fn assert_projections(
        harness: &ResolutionHarness,
        key: &KeyRef,
        entry: &ShardEntry,
        expected: &ProjectionExpectation,
        context: &str,
    ) {
        let (state, _background) = harness.resolver();
        harness.clear_operations();
        let writer = state
            .resolve_writer(key, Some(entry), Requirement::Any)
            .await
            .unwrap();
        assert_eq!(writer, expected.writer, "{context}: cold writer");
        harness.assert_operations(
            expected.operations.writer,
            &format!("{context}: cold writer"),
        );

        harness.clear_operations();
        let writer = state
            .resolve_writer(key, Some(entry), Requirement::Any)
            .await
            .unwrap();
        let mut warm_writer = expected.writer.clone();
        warm_writer.cache_hit = true;
        assert_eq!(writer, warm_writer, "{context}: warm writer");
        harness.assert_operations(0, &format!("{context}: warm writer"));

        let (state, _background) = harness.resolver();
        harness.clear_operations();
        let holders = state
            .resolve_holders(key, Some(entry), None, Requirement::Any)
            .await
            .unwrap();
        assert_eq!(holders, expected.holders, "{context}: holders");
        assert_eq!(
            holders.resolved_current(Some(entry)),
            expected.current,
            "{context}: current projection"
        );
        harness.assert_operations(expected.operations.holders, &format!("{context}: holders"));

        let (state, _background) = harness.resolver();
        harness.clear_operations();
        assert_eq!(
            state
                .entry_exists(key, entry, None, Requirement::Any)
                .await
                .unwrap(),
            expected.exists,
            "{context}: existence"
        );
        harness.assert_operations(
            expected.operations.existence,
            &format!("{context}: existence"),
        );
    }

    async fn assert_exclusive_case(
        current_case: CurrentCase,
        exclusive_case: ExclusiveCase,
        lock_type: LockType,
    ) {
        let harness = ResolutionHarness::new();
        let key = KeyRef::new(CollectionAddress::root("db"), b"key");
        let predecessor = TxId::with_priority(1, b"previous");
        let holder = TxId::with_priority(2, b"holder");
        let current = current_case.current(&predecessor);
        let entry = ShardEntry {
            lock_type,
            locked_by: vec![holder.clone()],
            current: current.clone(),
            ..ShardEntry::new(key.key())
        };
        harness
            .seed_transaction(
                &key,
                &holder,
                lock_type,
                exclusive_case.status(),
                exclusive_case.committed_deletion(),
            )
            .await;

        let context = format!(
            "{} current / {} {lock_type} holder",
            current_case.name(),
            exclusive_case.name()
        );
        let deleted = match exclusive_case.committed_deletion() {
            Some(deleted) => deleted,
            None => current_case.deleted(),
        };
        let writer = if exclusive_case.is_committed() {
            Some(holder.clone())
        } else {
            current_case.writer(&predecessor)
        };
        let expected = ProjectionExpectation {
            writer: WriterResolution {
                writer: writer.clone(),
                value: if exclusive_case.is_committed() {
                    ResolvedValue::Unresolved
                } else {
                    current_case.value()
                },
                cache_hit: false,
            },
            holders: HolderResolution {
                writer,
                deleted,
                pending: if matches!(exclusive_case, ExclusiveCase::Pending) {
                    vec![holder.clone()]
                } else {
                    Vec::new()
                },
            },
            current: if !exclusive_case.is_committed() {
                current
            } else if deleted {
                CurrentState::Tombstone {
                    writer: holder.clone(),
                }
            } else {
                CurrentState::External {
                    writer: holder.clone(),
                }
            },
            exists: match exclusive_case.committed_deletion() {
                Some(deleted) => !deleted,
                None => current_case.exists(),
            },
            operations: ProjectionOperations {
                writer: 1,
                holders: 1,
                existence: 1,
            },
        };
        assert_projections(&harness, &key, &entry, &expected, &context).await;
    }

    #[tokio::test]
    async fn exclusive_holder_resolution_matrix() {
        for current_case in CurrentCase::ALL {
            for exclusive_case in ExclusiveCase::ALL {
                assert_exclusive_case(current_case, exclusive_case, LockType::Write).await;
            }
        }

        assert_exclusive_case(
            CurrentCase::Absent,
            ExclusiveCase::CommittedLive,
            LockType::Create,
        )
        .await;
    }

    #[tokio::test]
    async fn shared_reader_resolution_matrix() {
        for current_case in CurrentCase::ALL {
            let harness = ResolutionHarness::new();
            let key = KeyRef::new(CollectionAddress::root("db"), b"key");
            let predecessor = TxId::with_priority(1, b"previous");
            let pending = TxId::with_priority(2, b"pending");
            let committed = TxId::with_priority(3, b"committed");
            let aborted = TxId::with_priority(4, b"aborted");
            let wounded = TxId::with_priority(5, b"wounded");
            for (holder, status) in [
                (&pending, TxCommitStatus::Pending),
                (&committed, TxCommitStatus::Ok),
                (&aborted, TxCommitStatus::Aborted),
                (&wounded, TxCommitStatus::Wounded),
            ] {
                harness
                    .seed_transaction(&key, holder, LockType::Read, status, None)
                    .await;
            }

            let current = current_case.current(&predecessor);
            let entry = ShardEntry {
                lock_type: LockType::Read,
                locked_by: vec![pending.clone(), committed, aborted, wounded],
                current: current.clone(),
                ..ShardEntry::new(key.key())
            };
            let context = format!("{} current / shared readers", current_case.name());
            // Writer-only resolution must not pay to reconcile compatible
            // readers; holder resolution is the projection that needs them.
            let writer = current_case.writer(&predecessor);
            let expected = ProjectionExpectation {
                writer: WriterResolution {
                    writer: writer.clone(),
                    value: current_case.value(),
                    cache_hit: true,
                },
                holders: HolderResolution {
                    writer,
                    deleted: current_case.deleted(),
                    pending: vec![pending.clone()],
                },
                current: current.clone(),
                exists: current_case.exists(),
                operations: ProjectionOperations {
                    writer: 0,
                    holders: 4,
                    existence: 0,
                },
            };
            assert_projections(&harness, &key, &entry, &expected, &context).await;
        }
    }

    #[tokio::test]
    async fn own_holder_is_excluded_from_resolution() {
        for lock_type in [LockType::Write, LockType::Read] {
            let harness = ResolutionHarness::new();
            let key = KeyRef::new(CollectionAddress::root("db"), b"key");
            let predecessor = TxId::with_priority(1, b"previous");
            let holder = TxId::with_priority(2, b"holder");
            harness
                .seed_transaction(
                    &key,
                    &holder,
                    lock_type,
                    if lock_type == LockType::Write {
                        TxCommitStatus::Ok
                    } else {
                        TxCommitStatus::Pending
                    },
                    (lock_type == LockType::Write).then_some(true),
                )
                .await;
            let current = CurrentState::Inline {
                writer: predecessor.clone(),
                value: Arc::from(INLINE_VALUE),
            };
            let entry = ShardEntry {
                lock_type,
                locked_by: vec![holder.clone()],
                current: current.clone(),
                ..ShardEntry::new(key.key())
            };

            let (state, _background) = harness.resolver();
            harness.clear_operations();
            let holders = state
                .resolve_holders(&key, Some(&entry), Some(&holder), Requirement::Any)
                .await
                .unwrap();
            assert_eq!(
                holders,
                HolderResolution {
                    writer: Some(predecessor),
                    deleted: false,
                    pending: Vec::new(),
                },
                "own {lock_type} holder"
            );
            assert_eq!(
                holders.resolved_current(Some(&entry)),
                current,
                "own {lock_type} holder: current projection"
            );
            harness.assert_operations(0, &format!("own {lock_type} holder: holders"));

            harness.clear_operations();
            assert!(
                state
                    .entry_exists(&key, &entry, Some(&holder), Requirement::Any,)
                    .await
                    .unwrap(),
                "the predecessor's inline value remains effective"
            );
            harness.assert_operations(0, &format!("own {lock_type} holder: existence"));
        }
    }

    #[tokio::test]
    async fn invalid_exclusive_cardinality_fails_without_io() {
        for lock_type in [LockType::Write, LockType::Create] {
            let harness = ResolutionHarness::new();
            let key = KeyRef::new(CollectionAddress::root("db"), b"key");
            let predecessor = TxId::with_priority(1, b"previous");
            let first = TxId::with_priority(2, b"first");
            let second = TxId::with_priority(3, b"second");
            for holder in [&first, &second] {
                harness
                    .seed_transaction(&key, holder, lock_type, TxCommitStatus::Pending, None)
                    .await;
            }
            let entry = ShardEntry {
                lock_type,
                locked_by: vec![first, second],
                current: CurrentState::External {
                    writer: predecessor,
                },
                ..ShardEntry::new(key.key())
            };
            let (state, _background) = harness.resolver();

            harness.clear_operations();
            let writer_error = state
                .resolve_writer(&key, Some(&entry), Requirement::Any)
                .await
                .unwrap_err();
            assert_eq!(
                writer_error.to_string(),
                "exclusive shard entry has more than one holder"
            );
            harness.assert_operations(0, &format!("invalid {lock_type}: writer"));

            harness.clear_operations();
            let holder_error = state
                .resolve_holders(&key, Some(&entry), None, Requirement::Any)
                .await
                .unwrap_err();
            assert_eq!(
                holder_error.to_string(),
                "exclusive shard entry has more than one holder"
            );
            harness.assert_operations(0, &format!("invalid {lock_type}: holders"));

            harness.clear_operations();
            let existence_error = state
                .entry_exists(&key, &entry, None, Requirement::Any)
                .await
                .unwrap_err();
            assert_eq!(
                existence_error.to_string(),
                "exclusive shard entry has more than one holder"
            );
            harness.assert_operations(0, &format!("invalid {lock_type}: existence"));
        }
    }

    // Writer and liveness must describe the same holder state. Resolving them
    // in separate passes could instead pair the pending predecessor with the
    // committed holder's removable lock.
    #[tokio::test]
    async fn holder_resolution_is_coherent_across_commit() {
        let (monitor, _background) = new_monitor();
        let key = KeyRef::new(CollectionAddress::root("db"), b"key");
        let predecessor = TxId::with_priority(1, b"predecessor");
        let holder = TxId::with_priority(2, b"holder");

        monitor.begin_tx(&holder);
        let mut committed = TxLog::new(holder.clone(), TxCommitStatus::Pending);
        committed.writes = vec![TxWrite {
            key: key.clone(),
            value: Arc::from(b"v".as_slice()),
            deleted: false,
            prev_writer: predecessor.clone(),
        }];
        let entry = ShardEntry {
            lock_type: LockType::Write,
            locked_by: vec![holder.clone()],
            current: CurrentState::External {
                writer: predecessor.clone(),
            },
            ..ShardEntry::new(key.key())
        };

        let state = KeyStateResolver::new(monitor.clone());
        let pending = state
            .resolve_holders(&key, Some(&entry), None, Requirement::Any)
            .await
            .unwrap();
        assert_eq!(pending.writer, Some(predecessor));
        assert_eq!(pending.pending, vec![holder.clone()]);

        monitor.commit_tx(committed).await.unwrap();
        assert_eq!(
            monitor.tx_status(&holder).await.unwrap(),
            TxCommitStatus::Ok,
            "the holder committed before the second resolution"
        );

        let committed = state
            .resolve_holders(&key, Some(&entry), None, Requirement::Any)
            .await
            .unwrap();
        assert_eq!(committed.writer, Some(holder));
        assert!(committed.pending.is_empty());
    }
}
