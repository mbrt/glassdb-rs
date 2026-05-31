//! Storage object versions, combining a backend version with a local writer.
//! Ported from the Go `internal/storage/version.go`.

use glassdb_backend::{self as backend, Metadata};
use glassdb_data::TxId;

use crate::locker::last_writer_from_tags;

/// A storage object's version: a backend version plus the transaction that last
/// wrote it (empty if never written by GlassDB).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Version {
    pub b: backend::Version,
    pub writer: TxId,
}

impl Version {
    /// Reports whether the version has no backend version and no writer.
    pub fn is_null(&self) -> bool {
        self.b.is_null() && self.writer.is_empty()
    }

    /// Reports whether the version was written by a local, not-yet-committed
    /// transaction.
    pub fn is_local(&self) -> bool {
        !self.writer.is_empty()
    }

    /// Reports whether `self` and `other` refer to the same value (same writer).
    pub fn equal_contents(&self, other: &Version) -> bool {
        self.writer == other.writer
    }

    /// Reports whether `self` matches the version described by backend metadata.
    pub fn equal_meta_contents(&self, m: &Metadata) -> bool {
        self.writer == last_writer_from_tags(&m.tags)
    }
}

/// Constructs a [`Version`] from backend metadata.
pub fn version_from_meta(m: &Metadata) -> Version {
    Version {
        b: m.version.clone(),
        writer: last_writer_from_tags(&m.tags),
    }
}
