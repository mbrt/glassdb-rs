//! Transaction-local collection-directory reads and lifecycle changes.

use std::collections::{BTreeMap, HashMap, HashSet};

use glassdb_data::{CollectionAddress, CollectionId};
use glassdb_trans::{
    CollectionChange, CollectionData, CollectionOp, DirectoryRead, DirectoryReadKind,
    DirectorySnapshot,
};

use crate::error::Error;

/// Selects strict creation or create-if-absent behavior.
#[derive(Clone, Copy)]
pub(super) enum CreateMode {
    Strict,
    IfAbsent,
}

/// Accumulates collection-catalog accesses for one public transaction.
#[derive(Default)]
pub(super) struct CatalogOverlay {
    directories: HashMap<CollectionAddress, DirectoryState>,
    reads: Vec<DirectoryRead>,
    changes: BTreeMap<(CollectionAddress, Vec<u8>), CollectionChange>,
    created: HashSet<CollectionAddress>,
    dropped: HashSet<CollectionAddress>,
    dropped_bindings: HashSet<(CollectionAddress, Vec<u8>)>,
    reservations: HashMap<(CollectionAddress, Vec<u8>), CollectionId>,
}

struct DirectoryState {
    base: BTreeMap<Vec<u8>, CollectionId>,
    current: BTreeMap<Vec<u8>, CollectionId>,
    version: u64,
}

impl CatalogOverlay {
    /// Reports whether a collection was created by the current body attempt.
    pub(super) fn is_created(&self, collection: &CollectionAddress) -> bool {
        self.created.contains(collection)
    }

    /// Reports whether a collection was dropped by the current body attempt.
    pub(super) fn is_dropped(&self, collection: &CollectionAddress) -> bool {
        self.dropped.contains(collection)
    }

    /// Returns a child binding while retaining the base entry as validation evidence.
    pub(super) fn child(
        &mut self,
        parent: &CollectionAddress,
        name: &[u8],
    ) -> Result<Option<CollectionId>, Error> {
        if self.dropped.contains(parent) {
            return Err(Error::StaleCollection);
        }
        let state = self
            .directories
            .get(parent)
            .expect("directory was loaded above");
        let base = state.base.get(name).copied();
        let current = state.current.get(name).copied();
        self.reads.push(DirectoryRead {
            parent: parent.clone(),
            kind: DirectoryReadKind::Entry {
                name: name.to_vec(),
                collection: base,
            },
        });
        Ok(current)
    }

    /// Returns the current child bindings while retaining listing validation evidence.
    pub(super) fn children(
        &mut self,
        parent: &CollectionAddress,
    ) -> Result<Vec<(Vec<u8>, CollectionId)>, Error> {
        if self.dropped.contains(parent) {
            return Err(Error::StaleCollection);
        }
        let state = self
            .directories
            .get(parent)
            .expect("directory was loaded above");
        let version = state.version;
        let current = state
            .current
            .iter()
            .map(|(name, id)| (name.clone(), *id))
            .collect();
        self.reads.push(DirectoryRead {
            parent: parent.clone(),
            kind: DirectoryReadKind::Listing { version },
        });
        Ok(current)
    }

    /// Stages a child creation and returns its incarnation and creation result.
    pub(super) fn create_child(
        &mut self,
        parent: &CollectionAddress,
        name: &[u8],
        mode: CreateMode,
    ) -> Result<(CollectionAddress, bool), Error> {
        if self.dropped.contains(parent) {
            return Err(Error::StaleCollection);
        }
        let binding = (parent.clone(), name.to_vec());
        let state = self
            .directories
            .get(parent)
            .expect("directory was loaded above");
        let base = state.base.get(name).copied();
        let existing = state.current.get(name).copied();
        self.reads.push(DirectoryRead {
            parent: parent.clone(),
            kind: DirectoryReadKind::Entry {
                name: name.to_vec(),
                collection: base,
            },
        });
        if let Some(id) = existing {
            if matches!(mode, CreateMode::Strict) {
                return Err(Error::AlreadyExists);
            }
            let address = CollectionAddress::new(parent.db_root(), id);
            let created = self.created.contains(&address);
            return Ok((address, created));
        }
        if self.dropped_bindings.contains(&binding) {
            return Err(Error::InvalidInput(
                "cannot recreate a collection binding after dropping it in one transaction".into(),
            ));
        }
        let id = match self.reservations.get(&binding).copied() {
            Some(id) => id,
            None => {
                let id = CollectionId::new_random();
                self.reservations.insert(binding.clone(), id);
                id
            }
        };
        let address = CollectionAddress::new(parent.db_root(), id);
        self.directories
            .get_mut(parent)
            .expect("directory was loaded above")
            .current
            .insert(name.to_vec(), id);
        self.changes.insert(
            binding,
            CollectionChange {
                parent: parent.clone(),
                name: name.to_vec(),
                collection: address.clone(),
                expected: base,
                op: CollectionOp::Create,
            },
        );
        self.created.insert(address.clone());
        Ok((address, true))
    }

    /// Stages a non-recursive drop of an exact collection incarnation.
    pub(super) fn drop_collection(
        &mut self,
        parent: CollectionAddress,
        name: Vec<u8>,
        collection: &CollectionAddress,
        has_data_writes: bool,
    ) -> Result<(), Error> {
        if self
            .dropped_bindings
            .contains(&(parent.clone(), name.clone()))
        {
            return Err(Error::StaleCollection);
        }
        let parent_state = self
            .directories
            .get(&parent)
            .expect("parent directory was loaded above");
        let expected = parent_state.base.get(name.as_slice()).copied();
        let current = parent_state.current.get(name.as_slice()).copied();
        self.reads.push(DirectoryRead {
            parent: parent.clone(),
            kind: DirectoryReadKind::Entry {
                name: name.clone(),
                collection: expected,
            },
        });
        if current != Some(collection.id()) {
            return Err(Error::StaleCollection);
        }
        let target = self
            .directories
            .get(collection)
            .expect("target directory was loaded above");
        let target_version = target.version;
        let target_not_empty = !target.current.is_empty();
        self.reads.push(DirectoryRead {
            parent: collection.clone(),
            kind: DirectoryReadKind::Listing {
                version: target_version,
            },
        });
        if target_not_empty {
            return Err(Error::NotEmpty);
        }
        if has_data_writes {
            return Err(Error::InvalidInput(
                "cannot drop a collection after staging data writes to it".into(),
            ));
        }
        self.directories
            .get_mut(&parent)
            .expect("parent directory was loaded above")
            .current
            .remove(name.as_slice());
        let binding = (parent.clone(), name.clone());
        if self.created.remove(collection) {
            self.changes.remove(&binding);
            self.reads.retain(|read| &read.parent != collection);
        } else {
            self.changes.insert(
                binding.clone(),
                CollectionChange {
                    parent,
                    name,
                    collection: collection.clone(),
                    expected,
                    op: CollectionOp::Drop,
                },
            );
        }
        self.dropped.insert(collection.clone());
        self.dropped_bindings.insert(binding);
        Ok(())
    }

    /// Prepares local directory state and reports whether a snapshot must be loaded.
    pub(super) fn prepare_directory(
        &mut self,
        collection: &CollectionAddress,
    ) -> Result<bool, Error> {
        if self.directories.contains_key(collection) {
            return Ok(false);
        }
        if self.dropped.contains(collection) {
            return Err(Error::StaleCollection);
        }
        if self.created.contains(collection) {
            self.directories.insert(
                collection.clone(),
                DirectoryState {
                    base: BTreeMap::new(),
                    current: BTreeMap::new(),
                    version: 0,
                },
            );
            return Ok(false);
        }
        Ok(true)
    }

    /// Installs a loaded directory unless another concurrent caller won the race.
    pub(super) fn install_snapshot(
        &mut self,
        collection: CollectionAddress,
        snapshot: DirectorySnapshot,
    ) {
        let children = snapshot.children.into_iter().collect::<BTreeMap<_, _>>();
        self.directories
            .entry(collection)
            .or_insert_with(|| DirectoryState {
                base: children.clone(),
                current: children,
                version: snapshot.version,
            });
    }

    /// Discards catalog accesses from the completed body attempt.
    pub(super) fn reset(&mut self) {
        self.directories.clear();
        self.reads.clear();
        self.changes.clear();
        self.created.clear();
        self.dropped.clear();
        self.dropped_bindings.clear();
        // Reusing an incarnation across body retries avoids abandoning a
        // prepared collection under a no-longer-reachable physical prefix.
    }

    /// Serializes the accumulated logical accesses for the commit engine.
    pub(super) fn accesses(&self) -> CollectionData {
        CollectionData {
            reads: self.reads.clone(),
            changes: self.changes.values().cloned().collect(),
        }
    }
}
