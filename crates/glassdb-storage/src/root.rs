//! The collection root object: in-memory view and canonical protobuf encoding
//! (ADR-031, superseding ADR-018).
//!
//! The root (`{prefix}/_i`) *is* the B-link tree's root [`Node`] — a leaf while
//! the collection is small, an index once it grows — and also carries the
//! collection metadata: existence (the object's presence) and the subcollection
//! directory. Membership (create/delete) is coordinated per-key in the owning
//! leaf, not by a root-wide lock, so the root carries no membership lock. Its
//! body is the compare-and-swap unit, so the encoding is canonical
//! (subcollections sorted) and golden-anchored.
//!
//! This module defines an inert data type plus encode/decode and pure
//! accessors/mutators. It does no I/O.

use std::collections::BTreeSet;

use glassdb_proto as pb;
use prost::Message;

use crate::error::StorageError;
use crate::node::{Node, NodeLocks};
use crate::shard::Shard;

/// A decoded collection root: the B-link root node plus collection metadata.
///
/// Subcollection names are held in a sorted set so iteration and encoding are
/// canonical regardless of insertion order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionRoot {
    node: Node,
    subcollections: BTreeSet<Vec<u8>>,
}

impl CollectionRoot {
    /// Creates an empty root: an empty leaf spanning the whole key space and no
    /// subcollections — the shape a collection is created with.
    pub fn new() -> Self {
        CollectionRoot {
            node: Node::leaf(Shard::new()),
            subcollections: BTreeSet::new(),
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

    /// Reports whether subcollection `name` is in the directory.
    pub fn contains_subcollection(&self, name: &[u8]) -> bool {
        self.subcollections.contains(name)
    }

    /// Adds `name` to the subcollection directory, returning whether it was newly
    /// added.
    pub fn add_subcollection(&mut self, name: impl Into<Vec<u8>>) -> bool {
        self.subcollections.insert(name.into())
    }

    /// Removes `name` from the subcollection directory, returning whether it was
    /// present.
    pub fn remove_subcollection(&mut self, name: &[u8]) -> bool {
        self.subcollections.remove(name)
    }

    /// Iterates the subcollection names in canonical (sorted) order.
    pub fn subcollections(&self) -> impl Iterator<Item = &[u8]> {
        self.subcollections.iter().map(Vec::as_slice)
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
        Ok(CollectionRoot {
            node: raw
                .node
                .map(Node::from_pb)
                .unwrap_or_else(|| Node::leaf(Shard::new())),
            subcollections: raw.subcollections.into_iter().collect(),
        })
    }

    fn to_pb(&self) -> pb::CollectionRoot {
        // Subcollections are already canonical via the BTreeSet.
        pb::CollectionRoot {
            node: Some(self.node.to_pb()),
            subcollections: self.subcollections.iter().cloned().collect(),
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

    #[test]
    fn round_trip() {
        let mut root = CollectionRoot::new();
        root.add_subcollection(b"users".to_vec());
        root.add_subcollection(b"settings".to_vec());

        let decoded = CollectionRoot::decode(&root.encode()).unwrap();
        assert_eq!(decoded, root);
    }

    #[test]
    fn empty_round_trip() {
        let root = CollectionRoot::new();
        let decoded = CollectionRoot::decode(&root.encode()).unwrap();
        assert_eq!(decoded, root);
        assert_eq!(decoded.subcollections().count(), 0);
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
    fn subcollection_directory_ops() {
        let mut root = CollectionRoot::new();
        assert!(root.add_subcollection(b"a".to_vec()));
        assert!(!root.add_subcollection(b"a".to_vec()));
        assert!(root.contains_subcollection(b"a"));
        assert!(!root.contains_subcollection(b"missing"));
        assert!(root.remove_subcollection(b"a"));
        assert!(!root.remove_subcollection(b"a"));
    }

    #[test]
    fn subcollections_iterate_sorted() {
        let mut root = CollectionRoot::new();
        root.add_subcollection(b"c".to_vec());
        root.add_subcollection(b"a".to_vec());
        root.add_subcollection(b"b".to_vec());
        let names: Vec<&[u8]> = root.subcollections().collect();
        assert_eq!(names, vec![b"a".as_slice(), b"b", b"c"]);
    }

    #[test]
    fn encoding_is_canonical_regardless_of_input_order() {
        let mk = |order: &[&[u8]]| {
            let mut r = CollectionRoot::new();
            for n in order {
                r.add_subcollection(n.to_vec());
            }
            r
        };
        let a = mk(&[b"c", b"a", b"b"]);
        let b = mk(&[b"a", b"b", b"c"]);
        assert_eq!(a.encode(), b.encode());
    }

    // Golden vector: a fixed root must always encode to these exact bytes.
    // Changing the on-disk format must break this test.
    #[test]
    fn golden_encoding() {
        let mut root = CollectionRoot::new();
        root.add_subcollection(b"users".to_vec());
        let got = root.encode();
        let want = [
            0x0a, 0x02, 0x1a, 0x00, 0x12, 0x05, 0x75, 0x73, 0x65, 0x72, 0x73,
        ];
        assert_eq!(root.encoded_len(), got.len());
        assert_eq!(got, want, "collection-root encoding drifted: {got:02x?}");
    }
}
