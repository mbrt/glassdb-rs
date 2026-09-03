//! Transactional collection catalog accesses and physical lifecycle changes.

mod lifecycle;

use glassdb_data::{CollectionAddress, CollectionId};

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
pub struct CatalogAccesses {
    pub reads: Vec<DirectoryRead>,
    pub changes: Vec<CollectionChange>,
}

impl CatalogAccesses {
    /// Reports whether the transaction changes a collection binding.
    pub fn has_writes(&self) -> bool {
        !self.changes.is_empty()
    }

    /// Retains only durable directory observations used to validate an error outcome.
    pub fn into_read_only(mut self) -> Self {
        let created = self
            .changes
            .iter()
            .filter(|change| matches!(change.op, CollectionOp::Create))
            .map(|change| change.collection.clone())
            .collect::<std::collections::HashSet<_>>();
        self.reads.retain(|read| !created.contains(&read.parent));
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

#[cfg(test)]
mod tests {
    use super::*;

    fn address(byte: u8) -> CollectionAddress {
        CollectionAddress::new(
            "db",
            CollectionId::from_slice(&[byte; 16]).expect("fixed ID has the required width"),
        )
    }

    #[test]
    fn read_only_accesses_discard_reads_inside_a_staged_creation() {
        let root = CollectionAddress::root("db");
        let created = address(1);
        let durable = address(2);
        let accesses = CatalogAccesses {
            reads: vec![
                DirectoryRead {
                    parent: root.clone(),
                    kind: DirectoryReadKind::Entry {
                        name: b"created".to_vec(),
                        collection: None,
                    },
                },
                DirectoryRead {
                    parent: created.clone(),
                    kind: DirectoryReadKind::Listing { version: 0 },
                },
                DirectoryRead {
                    parent: durable.clone(),
                    kind: DirectoryReadKind::Listing { version: 7 },
                },
            ],
            changes: vec![CollectionChange {
                parent: root,
                name: b"created".to_vec(),
                collection: created.clone(),
                expected: None,
                op: CollectionOp::Create,
            }],
        };

        let read_only = accesses.into_read_only();

        assert!(read_only.changes.is_empty());
        assert_eq!(read_only.reads.len(), 2);
        assert!(read_only.reads.iter().any(|read| read.parent == durable));
        assert!(read_only.reads.iter().all(|read| read.parent != created));
    }
}
