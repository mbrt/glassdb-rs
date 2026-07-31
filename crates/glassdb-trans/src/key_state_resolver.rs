//! Transaction-aware interpretation of already-loaded key and node state.

use std::sync::Arc;

use glassdb_data::{KeyRef, TxId};
use glassdb_storage::{
    CurrentState, LockType, Node, Requirement, ShardEntry, StorageError, TxCommitStatus,
};

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

    /// Reports whether the key exists when the loaded state proves the answer.
    pub(crate) fn exists(&self) -> Option<bool> {
        match self {
            ResolvedValue::Unresolved => None,
            ResolvedValue::External | ResolvedValue::Inline(_) => Some(true),
            ResolvedValue::Tombstone => Some(false),
        }
    }
}

/// The effective writer resolved from one loaded shard entry.
#[derive(Debug, Clone)]
pub(crate) struct WriterResolution {
    pub(crate) writer: Option<TxId>,
    pub(crate) value: ResolvedValue,
    pub(crate) cache_hit: bool,
}

impl Default for WriterResolution {
    fn default() -> Self {
        Self {
            writer: None,
            value: ResolvedValue::Unresolved,
            cache_hit: true,
        }
    }
}

/// One coherent interpretation of an entry's foreign lock holders.
///
/// The effective writer and remaining live holders use one status observation
/// per holder so a concurrent commit cannot produce an inconsistent pair.
#[derive(Debug, Clone, Default)]
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
        let Some(entry) = entry else {
            return Ok(WriterResolution::default());
        };
        let exclusive = matches!(entry.lock_type, LockType::Write | LockType::Create);
        let mut writer = entry.current.writer().cloned();
        let mut value = ResolvedValue::from_current(&entry.current);
        let mut cache_hit = true;
        if exclusive && entry.locked_by.len() > 1 {
            return Err(TransError::other(
                "exclusive shard entry has more than one holder",
            ));
        }
        if exclusive && let Some(holder) = entry.locked_by.first() {
            let (status, status_cache_hit) = self
                .monitor
                .tx_status_at_with_cache(holder, requirement)
                .await?;
            cache_hit &= status_cache_hit;
            if status == TxCommitStatus::Ok {
                let committed = self
                    .monitor
                    .committed_value_at(key, holder, requirement)
                    .await?;
                cache_hit &= committed.cache_hit;
                if committed.status == TxCommitStatus::Ok && !committed.value.not_written {
                    writer = Some(holder.clone());
                    value = ResolvedValue::Unresolved;
                }
            }
        }
        Ok(WriterResolution {
            writer,
            value,
            cache_hit,
        })
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
        let Some(entry) = entry else {
            return Ok(HolderResolution::default());
        };
        let exclusive = matches!(entry.lock_type, LockType::Write | LockType::Create);
        if exclusive && entry.locked_by.len() > 1 {
            return Err(TransError::other(
                "exclusive shard entry has more than one holder",
            ));
        }

        let mut writer = entry.current.writer().cloned();
        let mut deleted = entry.current.is_tombstone();
        let mut pending = Vec::new();
        for holder in &entry.locked_by {
            if Some(holder) == own_lock_holder {
                continue;
            }
            let status = self.monitor.tx_status_at(holder, requirement).await?;
            match status {
                TxCommitStatus::Ok if exclusive => {
                    let committed = self
                        .monitor
                        .committed_value_at(key, holder, requirement)
                        .await?;
                    if committed.status == TxCommitStatus::Ok && !committed.value.not_written {
                        writer = Some(holder.clone());
                        deleted = committed.value.deleted;
                    }
                }
                TxCommitStatus::Pending | TxCommitStatus::Unknown => {
                    pending.push(holder.clone());
                }
                TxCommitStatus::Ok | TxCommitStatus::Aborted => {}
            }
        }
        Ok(HolderResolution {
            writer,
            deleted,
            pending,
        })
    }

    /// Reports whether one loaded entry represents a live key.
    pub(crate) async fn entry_exists(
        &self,
        key: &KeyRef,
        entry: &ShardEntry,
        own_lock_holder: Option<&TxId>,
        requirement: Requirement,
    ) -> Result<bool, TransError> {
        let resolved = if own_lock_holder.is_some_and(|id| {
            matches!(entry.lock_type, LockType::Write | LockType::Create)
                && entry.locked_by.iter().any(|holder| holder == id)
        }) {
            WriterResolution {
                writer: entry.current.writer().cloned(),
                value: ResolvedValue::from_current(&entry.current),
                cache_hit: true,
            }
        } else {
            self.resolve_writer(key, Some(entry), requirement).await?
        };
        let Some(writer) = resolved.writer else {
            return Ok(false);
        };
        if let Some(exists) = resolved.value.exists() {
            return Ok(exists);
        }
        let committed = self
            .monitor
            .committed_value_at(key, &writer, requirement)
            .await?;
        Ok(committed.status == TxCommitStatus::Ok
            && !committed.value.not_written
            && !committed.value.deleted)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use glassdb_backend::memory::MemoryBackend;
    use glassdb_concurr::{Background, Clock, RetryConfig};
    use glassdb_data::CollectionAddress;
    use glassdb_storage::{CachedStore, TLogger, Timeline, TxLog, TxWrite};

    use super::*;
    use crate::monitor::ProtocolTiming;

    fn new_monitor() -> (Monitor, Arc<Background>) {
        let timeline = Timeline::new();
        let objects = CachedStore::new(
            Arc::new(MemoryBackend::new()),
            1 << 20,
            timeline.clone(),
            None,
        );
        let transactions = TLogger::new(objects, "db");
        let background = Arc::new(Background::new());
        let monitor = Monitor::with_config(
            transactions,
            timeline,
            Arc::downgrade(&background),
            Clock::real(),
            RetryConfig::default(),
            ProtocolTiming::default(),
        );
        (monitor, background)
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
