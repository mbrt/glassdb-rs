//! Transactional collection catalog accesses and physical lifecycle changes.

mod lifecycle;

use std::collections::BTreeSet;

use glassdb_data::{CollectionAddress, CollectionId, CollectionName};

use crate::access::AccessSet;

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
        name: CollectionName,
        collection: Option<CollectionId>,
    },
    Listing {
        generation: u64,
    },
}

/// One staged direct-child binding mutation.
#[derive(Debug, Clone)]
pub struct CollectionChange {
    pub parent: CollectionAddress,
    pub name: CollectionName,
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

    /// Returns these accesses without the covered binding reads of a commit
    /// pass of `keys` (ADR-092). The pass must lock and validate the rest.
    ///
    /// Each point access of the pass locks a leaf of its collection, or is
    /// part of the CAS of a direct commit on that leaf, so a drop of that
    /// collection cannot commit before the pass. A covered binding of a child
    /// also protects its parent, because a collection with a child cannot be
    /// dropped. Range scans do not count: a scan of an empty range can hold no
    /// leaf.
    pub(crate) fn without_covered_reads(&self, keys: &AccessSet) -> Self {
        let mut protected = keys
            .points()
            .map(|point| point.key.collection().clone())
            .collect::<BTreeSet<_>>();
        let mut covered = vec![false; self.reads.len()];
        let mut changed = true;
        while changed {
            changed = false;
            for (read, covered) in self.reads.iter().zip(covered.iter_mut()) {
                if !*covered && read.bound().is_some_and(|child| protected.contains(&child)) {
                    *covered = true;
                    changed |= protected.insert(read.parent.clone());
                }
            }
        }
        Self {
            reads: self
                .reads
                .iter()
                .zip(covered)
                .filter(|(_, covered)| !covered)
                .map(|(read, _)| read.clone())
                .collect(),
            changes: self.changes.clone(),
        }
    }
}

impl DirectoryRead {
    /// Returns the collection that this read found bound, if any.
    fn bound(&self) -> Option<CollectionAddress> {
        match &self.kind {
            DirectoryReadKind::Entry {
                collection: Some(child),
                ..
            } => Some(CollectionAddress::from_db_prefix(
                self.parent.db_prefix_component().clone(),
                *child,
            )),
            DirectoryReadKind::Entry {
                collection: None, ..
            }
            | DirectoryReadKind::Listing { .. } => None,
        }
    }
}

/// A resolved, transactionally clean view of one direct-child directory.
#[derive(Debug, Clone)]
pub struct DirectorySnapshot {
    pub children: Vec<(CollectionName, CollectionId)>,
    pub generation: u64,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use glassdb_data::LogicalKey;

    use super::*;
    use crate::access::WriteAccess;

    fn address(byte: u8) -> CollectionAddress {
        CollectionAddress::new("db", CollectionId::from_bytes([byte; 16]))
    }

    fn entry(parent: &CollectionAddress, name: &str, child: Option<u8>) -> DirectoryRead {
        DirectoryRead {
            parent: parent.clone(),
            kind: DirectoryReadKind::Entry {
                name: CollectionName::new(name).unwrap(),
                collection: child.map(|byte| CollectionId::from_bytes([byte; 16])),
            },
        }
    }

    fn names(accesses: &CatalogAccesses) -> Vec<&str> {
        accesses
            .reads
            .iter()
            .map(|read| match &read.kind {
                DirectoryReadKind::Entry { name, .. } => {
                    std::str::from_utf8(name.as_bytes()).unwrap()
                }
                DirectoryReadKind::Listing { .. } => "listing",
            })
            .collect()
    }

    #[test]
    fn key_accesses_cover_the_bindings_of_their_collections_and_ancestors() {
        let root = CollectionAddress::root("db");
        let accesses = CatalogAccesses {
            reads: vec![
                // Covered through `nested`, which binds a child of 1.
                entry(&root, "parent", Some(1)),
                entry(&address(1), "nested", Some(2)),
                entry(&root, "untouched", Some(3)),
                entry(&root, "missing", None),
                DirectoryRead {
                    parent: address(2),
                    kind: DirectoryReadKind::Listing { generation: 0 },
                },
            ],
            changes: Vec::new(),
        };
        let keys = AccessSet::new(
            Vec::new(),
            vec![WriteAccess::put(
                LogicalKey::new(address(2), b"key"),
                Arc::from(b"value".as_slice()),
            )],
            Vec::new(),
        );

        let uncovered = accesses.without_covered_reads(&keys);

        assert_eq!(names(&uncovered), vec!["untouched", "missing", "listing"]);
        assert_eq!(
            names(&accesses.without_covered_reads(&AccessSet::default())),
            names(&accesses)
        );
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
                        name: CollectionName::new("created").unwrap(),
                        collection: None,
                    },
                },
                DirectoryRead {
                    parent: created.clone(),
                    kind: DirectoryReadKind::Listing { generation: 0 },
                },
                DirectoryRead {
                    parent: durable.clone(),
                    kind: DirectoryReadKind::Listing { generation: 7 },
                },
            ],
            changes: vec![CollectionChange {
                parent: root,
                name: CollectionName::new("created").unwrap(),
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
