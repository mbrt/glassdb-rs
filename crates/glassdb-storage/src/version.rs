//! Storage value versions. In the v2 object-native layout a key's value lives
//! in the transaction object of whichever transaction last committed it, so a
//! value is identified by its **writer**, not by a per-key backend object
//! version. Coordination objects are checked by their backend version in
//! the decoded object store; that revision is tracked there, not here
//! (ADR-023).

use glassdb_data::TxId;

/// A storage value's version: the transaction that last committed the value.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Version {
    pub writer: TxId,
}

impl Version {
    /// Reports whether the version has no writer.
    pub fn is_unset(&self) -> bool {
        self.writer.is_unset()
    }

    /// Reports whether `self` and `other` refer to the same value (same writer).
    pub fn equal_contents(&self, other: &Version) -> bool {
        self.writer == other.writer
    }
}
