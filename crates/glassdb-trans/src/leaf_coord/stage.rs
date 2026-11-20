//! The staged leaf of one coordinator round, and the stage that one member
//! drafts over it.

use std::collections::BTreeMap;

use glassdb_storage::{LeafBody, LeafEntry, NodeLocks};

use super::{MemberOutcome, ProposedStage, StageAdmission, Step};

/// The entries and node locks of one leaf after the loaded state and the
/// admitted stages of the earlier members of the round (ADR-028).
///
/// Members read the leaf only through it, and the coordinator alone applies
/// an admitted stage to it.
pub(crate) struct StagedLeaf {
    entries: BTreeMap<Vec<u8>, LeafEntry>,
    locks: NodeLocks,
}

impl StagedLeaf {
    /// Starts from the loaded entries and node locks of a leaf.
    pub(crate) fn new(leaf: &LeafBody, locks: &NodeLocks) -> Self {
        Self {
            entries: leaf
                .entries()
                .cloned()
                .map(|entry| (entry.key.clone(), entry))
                .collect(),
            locks: locks.clone(),
        }
    }

    /// Returns the staged entry of `key`.
    pub(crate) fn entry(&self, key: &[u8]) -> Option<&LeafEntry> {
        self.entries.get(key)
    }

    /// Returns every staged entry, in key order. Vestigial entries are
    /// included until the leaf is written.
    pub(crate) fn entries(&self) -> &BTreeMap<Vec<u8>, LeafEntry> {
        &self.entries
    }

    /// Returns the staged node locks.
    pub(crate) fn locks(&self) -> &NodeLocks {
        &self.locks
    }

    /// Starts the stage of one member over this leaf.
    pub(crate) fn draft(&self) -> StageDraft<'_> {
        StageDraft {
            staged: self,
            entries: BTreeMap::new(),
            locks: self.locks.clone(),
        }
    }

    /// Applies an admitted stage: its entries replace the staged entries of
    /// the same keys, and its locks replace the staged locks.
    pub(super) fn apply(&mut self, entries: Vec<(Vec<u8>, LeafEntry)>, locks: NodeLocks) {
        self.entries.extend(entries);
        self.locks = locks;
    }

    /// Returns the leaf body that the staged entries make. It drops vestigial
    /// entries: they name no transaction and read as absent (ADR-029).
    pub(super) fn leaf(&self) -> LeafBody {
        self.leaf_with(&[])
    }

    /// Takes the staged entries out as the leaf body to write, as
    /// [`Self::leaf`] makes it. The staged leaf has no entries after it.
    pub(super) fn take_leaf(&mut self) -> LeafBody {
        LeafBody::from_entries(
            std::mem::take(&mut self.entries)
                .into_values()
                .filter(|entry| !entry.is_vestigial()),
        )
    }

    /// Returns the leaf body that the staged entries make with `changes`
    /// applied, as [`Self::leaf`] does.
    pub(super) fn leaf_with(&self, changes: &[(Vec<u8>, LeafEntry)]) -> LeafBody {
        let mut entries: BTreeMap<&[u8], &LeafEntry> = self
            .entries
            .iter()
            .map(|(key, entry)| (key.as_slice(), entry))
            .collect();
        for (key, entry) in changes {
            entries.insert(key, entry);
        }
        LeafBody::from_entries(
            entries
                .into_values()
                .filter(|entry| !entry.is_vestigial())
                .cloned(),
        )
    }
}

/// The changes that one member drafts over a [`StagedLeaf`]: entries that
/// replace staged entries, and the node locks that replace the staged locks.
pub(crate) struct StageDraft<'a> {
    staged: &'a StagedLeaf,
    entries: BTreeMap<Vec<u8>, LeafEntry>,
    locks: NodeLocks,
}

impl<'a> StageDraft<'a> {
    /// Returns the staged leaf that this draft changes.
    pub(crate) fn staged(&self) -> &'a StagedLeaf {
        self.staged
    }

    /// Reports whether this draft already changes the entry of `key`.
    pub(crate) fn drafts(&self, key: &[u8]) -> bool {
        self.entries.contains_key(key)
    }

    /// Replaces the entry of `key`, also an entry that this draft already
    /// changed.
    pub(crate) fn put(&mut self, key: Vec<u8>, entry: LeafEntry) {
        self.entries.insert(key, entry);
    }

    /// Reports whether this draft changes any entry.
    pub(crate) fn changes_entries(&self) -> bool {
        !self.entries.is_empty()
    }

    /// Returns the drafted node locks.
    pub(crate) fn locks(&self) -> &NodeLocks {
        &self.locks
    }

    /// Returns the drafted node locks for a change.
    pub(crate) fn locks_mut(&mut self) -> &mut NodeLocks {
        &mut self.locks
    }

    /// Reports whether this draft leaves the staged leaf as it is: the same
    /// node locks, and each drafted entry equal to the staged one.
    pub(crate) fn is_unchanged(&self) -> bool {
        self.locks == self.staged.locks
            && self
                .entries
                .iter()
                .all(|(key, entry)| self.staged.entry(key) == Some(entry))
    }

    /// Reports whether this draft advanced the membership generation of the
    /// staged leaf, for example by removing a membership writer.
    pub(crate) fn membership_moved(&self) -> bool {
        self.locks.membership_generation() != self.staged.locks.membership_generation()
    }

    /// Proposes this draft as the member's stage.
    pub(crate) fn stage(self, admission: StageAdmission, outcome: MemberOutcome) -> Step {
        Step::Stage(ProposedStage {
            entries: self.entries.into_iter().collect(),
            locks: self.locks,
            admission,
            outcome,
        })
    }
}
