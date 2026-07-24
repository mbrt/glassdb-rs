//! The collection root object: in-memory view and canonical protobuf encoding
//! (ADR-031, superseding ADR-018).
//!
//! The root (`{prefix}/_i`) *is* the B-link tree's root [`Node`] — a leaf while
//! the collection is small, an index once it grows — and also carries the
//! collection metadata: existence (the object's presence) and the subcollection
//! directory. Membership (create/delete) is coordinated per-key in the owning
//! leaf, not by a root-wide lock, so the root carries no membership lock. Its
//! body is the compare-and-swap unit, so the encoding is canonical (child
//! bindings sorted by raw name) and golden-anchored.
//!
//! This module defines an inert data type plus encode/decode and pure
//! accessors/mutators. It does no I/O.

use std::collections::BTreeMap;

use glassdb_data::{CollectionId, MAX_COLLECTION_NAME_BYTES};
use glassdb_proto as pb;
use prost::Message;

use crate::error::StorageError;
use crate::node::{Node, NodeLocks};
use crate::shard::Shard;

/// A decoded collection root: the B-link root node plus collection metadata.
///
/// Child bindings are held in a sorted map so iteration and encoding are
/// canonical regardless of insertion order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionRoot {
    node: Node,
    children: BTreeMap<Vec<u8>, CollectionId>,
}

impl CollectionRoot {
    /// Creates an empty root: an empty leaf spanning the whole key space and no
    /// child bindings — the shape a collection is created with.
    pub fn new() -> Self {
        CollectionRoot {
            node: Node::leaf(Shard::new()),
            children: BTreeMap::new(),
        }
    }

    /// The B-link tree root node held in this root object.
    pub fn node(&self) -> &Node {
        &self.node
    }

    /// Returns the mutable lock state of the B-link tree root node.
    pub fn node_locks_mut(&mut self) -> &mut NodeLocks {
        self.node.locks_mut()
    }

    /// Replaces the B-link tree root node (e.g. after an in-place root split).
    pub fn set_node(&mut self, node: Node) {
        self.node = node;
    }

    /// Returns the incarnation bound to direct child `name`.
    pub fn child(&self, name: &[u8]) -> Option<CollectionId> {
        self.children.get(name).copied()
    }

    /// Adds a valid direct child binding, returning whether the name was vacant.
    pub fn add_child(
        &mut self,
        name: impl Into<Vec<u8>>,
        id: CollectionId,
    ) -> Result<bool, StorageError> {
        use std::collections::btree_map::Entry;

        let name = name.into();
        if name.is_empty() || name.len() > MAX_COLLECTION_NAME_BYTES {
            return Err(StorageError::other("invalid child collection name"));
        }
        if id.is_root() {
            return Err(StorageError::other(
                "a child cannot use the reserved root collection ID",
            ));
        }
        Ok(match self.children.entry(name) {
            Entry::Vacant(entry) => {
                entry.insert(id);
                true
            }
            Entry::Occupied(_) => false,
        })
    }

    /// Removes a direct child binding, returning its former incarnation.
    pub fn remove_child(&mut self, name: &[u8]) -> Option<CollectionId> {
        self.children.remove(name)
    }

    /// Iterates child bindings in canonical raw-name order.
    pub fn children(&self) -> impl Iterator<Item = (&[u8], CollectionId)> {
        self.children
            .iter()
            .map(|(name, id)| (name.as_slice(), *id))
    }

    /// Encodes the root to its canonical protobuf body (the CAS unit).
    pub fn encode(&self) -> Vec<u8> {
        self.to_pb().encode_to_vec()
    }

    /// Returns the canonical protobuf size without allocating the encoded body.
    pub fn encoded_len(&self) -> usize {
        self.to_pb().encoded_len()
    }

    /// Returns the encoded size without transient node-lock holders.
    pub fn content_encoded_len(&self) -> usize {
        let mut root = self.clone();
        root.node.clear_node_locks();
        root.encoded_len()
    }

    /// Decodes a root from its protobuf body. A root with no node (the wire
    /// default) yields an empty leaf, matching [`CollectionRoot::new`].
    pub fn decode(buf: &[u8]) -> Result<Self, StorageError> {
        let raw = pb::CollectionRoot::decode(buf)
            .map_err(|e| StorageError::with_source("unmarshalling collection root", e))?;
        let mut children = BTreeMap::new();
        for child in raw.children {
            if child.name.is_empty() || child.name.len() > MAX_COLLECTION_NAME_BYTES {
                return Err(StorageError::other(
                    "collection root contains an invalid child name",
                ));
            }
            let id = CollectionId::from_slice(&child.collection_id).ok_or_else(|| {
                StorageError::other("collection root contains an invalid child ID")
            })?;
            if id.is_root() {
                return Err(StorageError::other(
                    "collection root binds a child to the reserved root ID",
                ));
            }
            if children.insert(child.name, id).is_some() {
                return Err(StorageError::other(
                    "collection root contains a duplicate child name",
                ));
            }
        }
        Ok(CollectionRoot {
            node: match raw.node {
                Some(node) => Node::from_pb(node)?,
                None => Node::leaf(Shard::new()),
            },
            children,
        })
    }

    fn to_pb(&self) -> pb::CollectionRoot {
        // Children are already canonical via the BTreeMap.
        pb::CollectionRoot {
            node: Some(self.node.to_pb()),
            children: self
                .children
                .iter()
                .map(|(name, id)| pb::CollectionDirectoryEntry {
                    name: name.clone(),
                    collection_id: id.as_bytes().to_vec(),
                })
                .collect(),
        }
    }
}

impl Default for CollectionRoot {
    fn default() -> Self {
        CollectionRoot::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::lock::LockType;
    use crate::node::NodeBody;
    use crate::shard::ShardEntry;
    use glassdb_data::TxId;

    fn collection_id(byte: u8) -> CollectionId {
        CollectionId::from_slice(&[byte; 16]).unwrap()
    }

    #[test]
    fn round_trip() {
        let mut root = CollectionRoot::new();
        root.add_child(b"users".to_vec(), collection_id(1)).unwrap();
        root.add_child(b"settings".to_vec(), collection_id(2))
            .unwrap();

        let decoded = CollectionRoot::decode(&root.encode()).unwrap();
        assert_eq!(decoded, root);
    }

    #[test]
    fn empty_round_trip() {
        let root = CollectionRoot::new();
        let decoded = CollectionRoot::decode(&root.encode()).unwrap();
        assert_eq!(decoded, root);
        assert_eq!(decoded.children().count(), 0);
        // A fresh root is an empty leaf spanning the whole key space.
        assert!(matches!(decoded.node().body(), NodeBody::Leaf(s) if s.is_empty()));
    }

    #[test]
    fn round_trip_preserves_root_node_entries() {
        // The root object carries the B-link root node; for a small collection
        // that node is a leaf holding the collection's key entries.
        let mut root = CollectionRoot::new();
        let node = super::Node::leaf(Shard::from_entries([ShardEntry {
            key: b"apple".to_vec(),
            lock_type: LockType::None,
            locked_by: Vec::new(),
            current_writer: Some(TxId::from_bytes(vec![1])),
            deleted: false,
        }]));
        root.set_node(node);

        let decoded = CollectionRoot::decode(&root.encode()).unwrap();
        assert_eq!(decoded, root);
        assert!(decoded.node().as_leaf().unwrap().exists(b"apple"));
    }

    #[test]
    fn child_directory_ops() {
        let mut root = CollectionRoot::new();
        assert!(root.add_child(b"a".to_vec(), collection_id(1)).unwrap());
        assert!(!root.add_child(b"a".to_vec(), collection_id(2)).unwrap());
        assert_eq!(root.child(b"a"), Some(collection_id(1)));
        assert_eq!(root.child(b"missing"), None);
        assert_eq!(root.remove_child(b"a"), Some(collection_id(1)));
        assert_eq!(root.remove_child(b"a"), None);
    }

    #[test]
    fn invalid_child_bindings_are_rejected_before_encoding() {
        let mut root = CollectionRoot::new();
        assert!(root.add_child(Vec::new(), collection_id(1)).is_err());
        assert!(
            root.add_child(vec![0; MAX_COLLECTION_NAME_BYTES + 1], collection_id(1))
                .is_err()
        );
        assert!(
            root.add_child(b"root".to_vec(), CollectionId::root())
                .is_err()
        );
        assert_eq!(root.children().count(), 0);
    }

    #[test]
    fn children_iterate_sorted() {
        let mut root = CollectionRoot::new();
        root.add_child(b"c".to_vec(), collection_id(3)).unwrap();
        root.add_child(b"a".to_vec(), collection_id(1)).unwrap();
        root.add_child(b"b".to_vec(), collection_id(2)).unwrap();
        let names: Vec<&[u8]> = root.children().map(|(name, _)| name).collect();
        assert_eq!(names, vec![b"a".as_slice(), b"b", b"c"]);
    }

    #[test]
    fn encoding_is_canonical_regardless_of_input_order() {
        let mk = |order: &[&[u8]]| {
            let mut r = CollectionRoot::new();
            for (i, n) in order.iter().enumerate() {
                r.add_child(n.to_vec(), collection_id(n[0] + i as u8))
                    .unwrap();
            }
            r
        };
        let a = mk(&[b"c", b"a", b"b"]);
        let b = {
            let mut root = CollectionRoot::new();
            root.add_child(b"a".to_vec(), collection_id(b'a' + 1))
                .unwrap();
            root.add_child(b"b".to_vec(), collection_id(b'b' + 2))
                .unwrap();
            root.add_child(b"c".to_vec(), collection_id(b'c')).unwrap();
            root
        };
        assert_eq!(a.encode(), b.encode());
    }

    // Golden vector: a fixed root must always encode to these exact bytes.
    // Changing the on-disk format must break this test.
    #[test]
    fn golden_encoding() {
        let mut root = CollectionRoot::new();
        root.add_child(b"users".to_vec(), collection_id(1)).unwrap();
        let got = root.encode();
        let want = [
            0x0a, 0x02, 0x1a, 0x00, 0x12, 0x19, 0x0a, 0x05, 0x75, 0x73, 0x65, 0x72, 0x73, 0x12,
            0x10, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
            0x01, 0x01, 0x01,
        ];
        assert_eq!(root.encoded_len(), got.len());
        assert_eq!(got, want, "collection-root encoding drifted: {got:02x?}");
    }
}
