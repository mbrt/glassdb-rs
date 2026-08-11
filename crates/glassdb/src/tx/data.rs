//! Transaction-local key/value reads, writes, and range-scan state.

use std::collections::HashMap;
use std::sync::Arc;

use glassdb_data::{CollectionAddress, KeyRef};
use glassdb_trans::{Data, ReadAccess, ReadEvidence, ScanAccess, ScanMutation, WriteAccess};

/// The result of consulting the transaction-local point-read state.
pub(super) enum OverlayRead {
    Unknown,
    Known(Option<Vec<u8>>),
}

/// Accumulates key/value accesses for one execution of a transaction body.
#[derive(Default)]
pub(super) struct DataOverlay {
    staged: HashMap<KeyRef, StagedValue>,
    reads: HashMap<KeyRef, ReadState>,
    scans: Vec<ScanAccess>,
}

impl DataOverlay {
    /// Returns the transaction-local result for a point read, when known.
    pub(super) fn read(&self, key: &KeyRef) -> OverlayRead {
        if let Some(staged) = self.staged.get(key) {
            return OverlayRead::Known(staged.read());
        }
        if matches!(self.reads.get(key), Some(ReadState::NotFound { .. })) {
            return OverlayRead::Known(None);
        }
        OverlayRead::Unknown
    }

    /// Records an absent point read and its validation evidence.
    pub(super) fn record_not_found(
        &mut self,
        key: KeyRef,
        cache_hit: bool,
        evidence: ReadEvidence,
    ) {
        self.record_read(
            key,
            ReadState::NotFound {
                cache_hit,
                evidence,
            },
        );
    }

    /// Records a present point read and its validation evidence.
    pub(super) fn record_found(
        &mut self,
        key: KeyRef,
        value: Arc<[u8]>,
        cache_hit: bool,
        evidence: ReadEvidence,
    ) {
        self.staged.insert(key.clone(), StagedValue::Read(value));
        self.record_read(
            key,
            ReadState::Found {
                cache_hit,
                evidence,
            },
        );
    }

    /// Returns staged membership changes for a collection scan.
    pub(super) fn scan_mutations(&self, collection: &CollectionAddress) -> Vec<ScanMutation> {
        let mut overlay = self
            .staged
            .iter()
            .filter_map(|(key, value)| {
                if key.collection() != collection {
                    return None;
                }
                let present = match value {
                    StagedValue::Read(_) => return None,
                    StagedValue::Put(_) => true,
                    StagedValue::Delete => false,
                };
                Some(ScanMutation {
                    key: key.key().to_vec(),
                    present,
                })
            })
            .collect::<Vec<_>>();
        overlay.sort_by(|a, b| a.key.cmp(&b.key));
        overlay
    }

    /// Records the validation access produced by a range scan.
    pub(super) fn record_scan(&mut self, access: ScanAccess) {
        self.scans.push(access);
    }

    /// Stages a value replacement for commit.
    pub(super) fn write(&mut self, key: KeyRef, value: Arc<[u8]>) {
        self.staged.insert(key, StagedValue::Put(value));
    }

    /// Stages a key deletion for commit.
    pub(super) fn delete(&mut self, key: KeyRef) {
        self.staged.insert(key, StagedValue::Delete);
    }

    /// Reports whether a collection has a staged data mutation.
    pub(super) fn has_writes_for(&self, collection: &CollectionAddress) -> bool {
        self.staged.iter().any(|(key, value)| {
            key.collection() == collection
                && matches!(value, StagedValue::Put(_) | StagedValue::Delete)
        })
    }

    /// Discards data accesses from the completed body attempt.
    pub(super) fn reset(&mut self) {
        self.staged.clear();
        self.reads.clear();
        self.scans.clear();
    }

    /// Serializes the accumulated logical accesses for the commit engine.
    pub(super) fn accesses(&self) -> Data {
        let mut writes = Vec::new();
        for (key, value) in &self.staged {
            match value {
                StagedValue::Read(_) => {}
                StagedValue::Put(value) => {
                    writes.push(WriteAccess::put(key.clone(), value.clone()))
                }
                StagedValue::Delete => writes.push(WriteAccess::delete(key.clone())),
            }
        }
        let mut reads = Vec::new();
        for (key, state) in &self.reads {
            reads.push(ReadAccess::new(key.clone(), state.evidence().clone()));
        }
        // Stable key order keeps transaction logs, locking, validation, and
        // deterministic simulation independent of HashMap iteration order.
        writes.sort_by(|a, b| a.key.cmp(&b.key));
        reads.sort_by(|a, b| a.key.cmp(&b.key));
        Data {
            reads,
            writes,
            // Scan order already follows deterministic left-to-right listing.
            scans: self.scans.clone(),
        }
    }

    /// Returns the number of distinct point reads served from decoded cache state.
    pub(super) fn cache_hits(&self) -> u64 {
        self.reads.values().filter(|read| read.cache_hit()).count() as u64
    }

    fn record_read(&mut self, key: KeyRef, mut state: ReadState) {
        // Concurrent reads can both miss local state. Preserve a cache hit
        // observed by either result while still counting the key only once.
        if self.reads.get(&key).is_some_and(ReadState::cache_hit) {
            state.set_cache_hit();
        }
        self.reads.insert(key, state);
    }
}

enum StagedValue {
    Read(Arc<[u8]>),
    Put(Arc<[u8]>),
    Delete,
}

impl StagedValue {
    fn read(&self) -> Option<Vec<u8>> {
        match self {
            StagedValue::Read(value) | StagedValue::Put(value) => Some(value.to_vec()),
            StagedValue::Delete => None,
        }
    }
}

enum ReadState {
    Found {
        cache_hit: bool,
        evidence: ReadEvidence,
    },
    NotFound {
        cache_hit: bool,
        evidence: ReadEvidence,
    },
}

impl ReadState {
    fn cache_hit(&self) -> bool {
        match self {
            ReadState::Found { cache_hit, .. } | ReadState::NotFound { cache_hit, .. } => {
                *cache_hit
            }
        }
    }

    fn set_cache_hit(&mut self) {
        match self {
            ReadState::Found { cache_hit, .. } | ReadState::NotFound { cache_hit, .. } => {
                *cache_hit = true;
            }
        }
    }

    fn evidence(&self) -> &ReadEvidence {
        match self {
            ReadState::Found { evidence, .. } | ReadState::NotFound { evidence, .. } => evidence,
        }
    }
}
