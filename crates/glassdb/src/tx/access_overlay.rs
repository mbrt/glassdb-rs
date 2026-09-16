//! Transaction-local point accesses and range-scan state.

use std::collections::HashMap;
use std::sync::Arc;

use glassdb_data::{CollectionAddress, LogicalKey};
use glassdb_trans::{AccessSet, ReadAccess, ReadEvidence, ScanAccess, ScanMutation, WriteAccess};

/// The result of consulting the transaction-local point-read state.
pub(super) enum OverlayRead {
    Unknown,
    Known(Option<Vec<u8>>),
}

/// Accumulates key/value accesses for one execution of a transaction body.
#[derive(Default)]
pub(super) struct AccessOverlay {
    staged: HashMap<LogicalKey, StagedValue>,
    reads: HashMap<LogicalKey, ReadEvidence>,
    scans: Vec<ScanAccess>,
}

impl AccessOverlay {
    /// Returns the transaction-local result for a point read, when known.
    pub(super) fn read(&self, key: &LogicalKey) -> OverlayRead {
        if let Some(staged) = self.staged.get(key) {
            return OverlayRead::Known(staged.read());
        }
        if self.reads.contains_key(key) {
            return OverlayRead::Known(None);
        }
        OverlayRead::Unknown
    }

    /// Records an absent point read and its validation evidence.
    pub(super) fn record_not_found(&mut self, key: LogicalKey, evidence: ReadEvidence) {
        self.reads.insert(key, evidence);
    }

    /// Records a present point read and its validation evidence.
    pub(super) fn record_found(
        &mut self,
        key: LogicalKey,
        value: Arc<[u8]>,
        evidence: ReadEvidence,
    ) {
        self.staged.insert(key.clone(), StagedValue::Read(value));
        self.reads.insert(key, evidence);
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
    pub(super) fn write(&mut self, key: LogicalKey, value: Arc<[u8]>) {
        self.staged.insert(key, StagedValue::Put(value));
    }

    /// Stages a key deletion for commit.
    pub(super) fn delete(&mut self, key: LogicalKey) {
        self.staged.insert(key, StagedValue::Delete);
    }

    /// Reports whether a collection has a staged key mutation.
    pub(super) fn has_writes_for(&self, collection: &CollectionAddress) -> bool {
        self.staged.iter().any(|(key, value)| {
            key.collection() == collection
                && matches!(value, StagedValue::Put(_) | StagedValue::Delete)
        })
    }

    /// Builds the immutable access set for the commit engine.
    pub(super) fn accesses(&self) -> AccessSet {
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
        for (key, evidence) in &self.reads {
            reads.push(ReadAccess::new(key.clone(), evidence.clone()));
        }
        AccessSet::new(reads, writes, self.scans.clone())
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
