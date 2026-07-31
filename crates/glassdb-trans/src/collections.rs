//! Transactional collection catalog accesses and physical lifecycle changes.

mod catalog;
mod lifecycle;

use glassdb_data::{CollectionAddress, CollectionId};

pub use catalog::CollectionCatalog;
pub use lifecycle::{CollectionLifecycle, TopologySettler};

/// One directory dependency observed by a transaction body.
#[derive(Debug, Clone)]
pub struct DirectoryRead {
    pub parent: CollectionAddress,
    pub kind: DirectoryReadKind,
}

/// The logical portion of a child directory that was observed.
#[derive(Debug, Clone)]
pub enum DirectoryReadKind {
    Entry {
        name: Vec<u8>,
        collection: Option<CollectionId>,
    },
    Listing {
        version: u64,
    },
}

/// One staged direct-child binding mutation.
#[derive(Debug, Clone)]
pub struct CollectionChange {
    pub parent: CollectionAddress,
    pub name: Vec<u8>,
    pub collection: CollectionAddress,
    pub expected: Option<CollectionId>,
    pub op: CollectionOp,
}

/// The staged effect on a direct-child binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectionOp {
    Create,
    Drop,
}

/// Collection-management accesses carried beside ordinary key accesses.
#[derive(Debug, Clone, Default)]
pub struct CollectionData {
    pub reads: Vec<DirectoryRead>,
    pub changes: Vec<CollectionChange>,
}

impl CollectionData {
    /// Reports whether the transaction changes a collection binding.
    pub fn has_writes(&self) -> bool {
        !self.changes.is_empty()
    }

    /// Retains only directory observations used to validate a user error.
    pub fn into_read_only(mut self) -> Self {
        self.changes.clear();
        self
    }
}

/// A resolved, transactionally clean view of one direct-child directory.
#[derive(Debug, Clone)]
pub struct DirectorySnapshot {
    pub children: Vec<(Vec<u8>, CollectionId)>,
    pub version: u64,
}
