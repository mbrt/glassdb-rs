//! Storage object versions, combining a backend version with a local writer.
//! Ported from the Go `internal/storage/version.go`.

use glassdb_backend as backend;
use glassdb_data::TxId;

/// A storage object's version. It carries two independent identities, only one
/// of which is set at a time in the v2 layout:
///
/// - `b` — the backend content version (ETag/generation) of a coordination
///   object (shard, root, transaction log). Used to revalidate a cached copy
///   with a version-conditional read (ADR-023).
/// - `writer` — the transaction that last committed a key's value. A key's value
///   no longer lives in its own object (it lives in the writer's transaction
///   object), so a cached value is identified by its writer, not a backend
///   version.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Version {
    pub b: backend::Version,
    pub writer: TxId,
}

impl Version {
    /// Reports whether the version has no backend version and no writer.
    pub fn is_unset(&self) -> bool {
        self.b.is_unset() && self.writer.is_unset()
    }

    /// Reports whether the version was written by a local, not-yet-committed
    /// transaction.
    pub fn is_local(&self) -> bool {
        !self.writer.is_unset()
    }

    /// Reports whether `self` and `other` refer to the same value (same writer).
    pub fn equal_contents(&self, other: &Version) -> bool {
        self.writer == other.writer
    }
}
