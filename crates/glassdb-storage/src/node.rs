//! The B-link tree node: in-memory view and canonical protobuf encoding
//! (ADR-031).
//!
//! A node is the unit of the dynamic, range-partitioned coordination directory.
//! It is either a **leaf** — the per-key coordination entries of ADR-017 (a
//! [`Shard`]) for a contiguous key range — or an **index**, an ordered map from
//! separator keys to child-node tokens. Every node self-describes the range it
//! owns through a **high-key** (the exclusive upper bound; absent means
//! +infinity) and a **right-sibling** pointer, the two fields that let a descent
//! detect a concurrent split and self-correct by stepping right rather than
//! restarting from the root.
//!
//! Like the shard and root objects, a node body is a compare-and-swap unit, so
//! the encoding is canonical (leaf entries and index separators sorted, holder
//! sets sorted) and golden-anchored. This module is inert data plus encode/
//! decode and pure lookups; the split protocol and descent live above it.

use std::collections::BTreeMap;
use std::ops::Bound::{Included, Unbounded};

use glassdb_proto as pb;
use prost::Message;

use crate::error::StorageError;
use crate::shard::Shard;

/// The opaque identity token of a non-root node (`{prefix}/_n/<token>`). The
/// root has no token; it lives at the fixed `_i` path.
pub type NodeToken = String;

/// An index node body: the separator keys of an interior node, each mapping the
/// inclusive lower bound of a key range to the child node that owns it.
///
/// Separators are held sorted, so iteration and encoding are canonical and the
/// child owning a key is found by a single predecessor lookup.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IndexNode {
    children: BTreeMap<Vec<u8>, NodeToken>,
}

impl IndexNode {
    /// Builds an index node from `(separator, child)` pairs. The separator is the
    /// inclusive lower bound of the child's range; the leftmost child usually
    /// carries the empty separator (the node's own low bound).
    pub fn from_children<I: IntoIterator<Item = (Vec<u8>, NodeToken)>>(children: I) -> Self {
        IndexNode {
            children: children.into_iter().collect(),
        }
    }

    /// Returns the token of the child that owns `key`: the child whose separator
    /// is the greatest one not exceeding `key`. Falls back to the leftmost child
    /// when `key` precedes every separator (a defensive case a well-formed
    /// descent never hits, since the node's low bound is its first separator).
    pub fn child_for(&self, key: &[u8]) -> Option<&str> {
        self.children
            .range::<[u8], _>((Unbounded, Included(key)))
            .next_back()
            .map(|(_, c)| c.as_str())
            .or_else(|| self.children.values().next().map(String::as_str))
    }

    /// Iterates the `(separator, child)` pairs in canonical (separator-sorted)
    /// order.
    pub fn children(&self) -> impl Iterator<Item = (&[u8], &str)> {
        self.children
            .iter()
            .map(|(k, c)| (k.as_slice(), c.as_str()))
    }

    /// Number of children (separators) in the node.
    pub fn len(&self) -> usize {
        self.children.len()
    }

    /// Reports whether the node has no children.
    pub fn is_empty(&self) -> bool {
        self.children.is_empty()
    }

    fn to_pb(&self) -> pb::IndexNode {
        pb::IndexNode {
            entries: self
                .children
                .iter()
                .map(|(sep, child)| pb::IndexEntry {
                    separator_key: sep.clone(),
                    child: child.clone(),
                })
                .collect(),
        }
    }

    fn from_pb(raw: pb::IndexNode) -> Self {
        IndexNode {
            children: raw
                .entries
                .into_iter()
                .map(|e| (e.separator_key, e.child))
                .collect(),
        }
    }
}

/// The body of a [`Node`]: either a leaf's per-key entries or an index's
/// separators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeBody {
    /// A leaf: the ADR-017 coordination entries for the node's key range.
    Leaf(Shard),
    /// An index: separator keys mapping ranges to child nodes.
    Index(IndexNode),
}

/// A decoded B-link tree node: a body plus the high-key and right-sibling that
/// make descent self-correcting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    /// Exclusive upper bound of the owned key range; `None` means +infinity.
    high_key: Option<Vec<u8>>,
    /// Right-sibling token at the same level; `None` means none (rightmost).
    right_sibling: Option<NodeToken>,
    body: NodeBody,
}

impl Node {
    /// Creates a leaf node owning the whole key space (high-key +infinity, no
    /// right sibling) from `shard` — the shape of a brand-new root.
    pub fn leaf(shard: Shard) -> Self {
        Node {
            high_key: None,
            right_sibling: None,
            body: NodeBody::Leaf(shard),
        }
    }

    /// Creates an index node owning the whole key space from `index`.
    pub fn index(index: IndexNode) -> Self {
        Node {
            high_key: None,
            right_sibling: None,
            body: NodeBody::Index(index),
        }
    }

    /// Sets the exclusive upper bound of the owned range (`None` = +infinity).
    pub fn set_high_key(&mut self, high_key: Option<Vec<u8>>) {
        self.high_key = high_key;
    }

    /// Sets the right-sibling token (`None` = none).
    pub fn set_right_sibling(&mut self, right_sibling: Option<NodeToken>) {
        self.right_sibling = right_sibling;
    }

    /// The exclusive upper bound of the owned range, or `None` for +infinity.
    pub fn high_key(&self) -> Option<&[u8]> {
        self.high_key.as_deref()
    }

    /// The right-sibling token, or `None` if this is the rightmost node at its
    /// level.
    pub fn right_sibling(&self) -> Option<&str> {
        self.right_sibling.as_deref()
    }

    /// The node body.
    pub fn body(&self) -> &NodeBody {
        &self.body
    }

    /// The leaf body, or `None` if this is an index node.
    pub fn as_leaf(&self) -> Option<&Shard> {
        match &self.body {
            NodeBody::Leaf(s) => Some(s),
            NodeBody::Index(_) => None,
        }
    }

    /// The index body, or `None` if this is a leaf node.
    pub fn as_index(&self) -> Option<&IndexNode> {
        match &self.body {
            NodeBody::Index(i) => Some(i),
            NodeBody::Leaf(_) => None,
        }
    }

    /// Reports whether the node still owns `key`, i.e. `key` is below the
    /// high-key. A `false` result means a split has moved `key` to the right and
    /// the descent must follow the right-sibling link (the B-link property).
    pub fn owns(&self, key: &[u8]) -> bool {
        match &self.high_key {
            None => true,
            Some(hk) => key < hk.as_slice(),
        }
    }

    /// Encodes the node to its canonical protobuf body (the CAS unit).
    pub fn encode(&self) -> Vec<u8> {
        self.to_pb().encode_to_vec()
    }

    /// Decodes a node from its protobuf body. A message with no body is treated
    /// as an empty leaf spanning the whole key space (the shape of a fresh root).
    pub fn decode(buf: &[u8]) -> Result<Self, StorageError> {
        let raw = pb::Node::decode(buf)
            .map_err(|e| StorageError::with_source("unmarshalling node", e))?;
        Ok(Node::from_pb(raw))
    }

    pub(crate) fn to_pb(&self) -> pb::Node {
        let body = match &self.body {
            NodeBody::Leaf(shard) => pb::node::Body::Leaf(shard.to_pb()),
            NodeBody::Index(index) => pb::node::Body::Index(index.to_pb()),
        };
        pb::Node {
            high_key: self.high_key.clone().unwrap_or_default(),
            right_sibling: self.right_sibling.clone().unwrap_or_default(),
            body: Some(body),
        }
    }

    pub(crate) fn from_pb(raw: pb::Node) -> Self {
        let body = match raw.body {
            Some(pb::node::Body::Index(index)) => NodeBody::Index(IndexNode::from_pb(index)),
            Some(pb::node::Body::Leaf(leaf)) => NodeBody::Leaf(Shard::from_pb(leaf)),
            None => NodeBody::Leaf(Shard::new()),
        };
        Node {
            high_key: (!raw.high_key.is_empty()).then_some(raw.high_key),
            right_sibling: (!raw.right_sibling.is_empty()).then_some(raw.right_sibling),
            body,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use glassdb_data::TxId;

    use crate::lock::LockType;
    use crate::shard::ShardEntry;

    fn entry(key: &[u8], writer: u8) -> ShardEntry {
        ShardEntry {
            key: key.to_vec(),
            lock_type: LockType::None,
            locked_by: Vec::new(),
            current_writer: Some(TxId::from_bytes(vec![writer])),
            deleted: false,
        }
    }

    #[test]
    fn leaf_round_trip_preserves_bounds() {
        let mut node = Node::leaf(Shard::from_entries([entry(b"apple", 1), entry(b"cat", 2)]));
        node.set_high_key(Some(b"m".to_vec()));
        node.set_right_sibling(Some("sibToken".to_string()));

        let decoded = Node::decode(&node.encode()).unwrap();
        assert_eq!(decoded, node);
        assert_eq!(decoded.high_key(), Some(b"m".as_slice()));
        assert_eq!(decoded.right_sibling(), Some("sibToken"));
        assert!(decoded.as_leaf().is_some());
    }

    #[test]
    fn index_round_trip_and_child_lookup() {
        let index = IndexNode::from_children([
            (b"".to_vec(), "L0".to_string()),
            (b"f".to_vec(), "L1".to_string()),
            (b"m".to_vec(), "L2".to_string()),
        ]);
        let node = Node::index(index);
        let decoded = Node::decode(&node.encode()).unwrap();
        assert_eq!(decoded, node);

        let idx = decoded.as_index().unwrap();
        // The child owning a key is the greatest separator not exceeding it.
        assert_eq!(idx.child_for(b"apple"), Some("L0"));
        assert_eq!(idx.child_for(b"f"), Some("L1"));
        assert_eq!(idx.child_for(b"kiwi"), Some("L1"));
        assert_eq!(idx.child_for(b"mango"), Some("L2"));
    }

    #[test]
    fn owns_reflects_high_key() {
        let plus_inf = Node::leaf(Shard::new());
        assert!(plus_inf.owns(b"anything"));

        let mut bounded = Node::leaf(Shard::new());
        bounded.set_high_key(Some(b"m".to_vec()));
        assert!(bounded.owns(b"apple"));
        // The high-key is an exclusive upper bound.
        assert!(!bounded.owns(b"m"));
        assert!(!bounded.owns(b"zebra"));
    }

    #[test]
    fn encoding_is_canonical_regardless_of_input_order() {
        let a = Node::index(IndexNode::from_children([
            (b"m".to_vec(), "L2".to_string()),
            (b"".to_vec(), "L0".to_string()),
            (b"f".to_vec(), "L1".to_string()),
        ]));
        let b = Node::index(IndexNode::from_children([
            (b"".to_vec(), "L0".to_string()),
            (b"f".to_vec(), "L1".to_string()),
            (b"m".to_vec(), "L2".to_string()),
        ]));
        assert_eq!(a.encode(), b.encode());
    }

    #[test]
    fn empty_body_decodes_as_empty_leaf() {
        // A Node protobuf with no body (the wire default) is a fresh empty root.
        let raw = pb::Node::default();
        let node = Node::from_pb(raw);
        assert!(node.as_leaf().is_some_and(Shard::is_empty));
        assert_eq!(node.high_key(), None);
        assert_eq!(node.right_sibling(), None);
    }

    // Golden vectors: a fixed node must always encode to these exact bytes.
    // Changing the on-disk format must break these tests.
    #[test]
    fn golden_leaf_encoding() {
        let mut node = Node::leaf(Shard::from_entries([ShardEntry {
            key: b"Hello".to_vec(),
            lock_type: LockType::Write,
            locked_by: vec![TxId::from_bytes(vec![1, 2, 3, 4])],
            current_writer: Some(TxId::from_bytes(vec![0xaa, 0xbb])),
            deleted: false,
        }]));
        node.set_high_key(Some(b"m".to_vec()));
        node.set_right_sibling(Some("sib".to_string()));
        let got = node.encode();
        let want = [
            0x0a, 0x01, 0x6d, 0x12, 0x03, 0x73, 0x69, 0x62, 0x1a, 0x15, 0x0a, 0x13, 0x0a, 0x05,
            0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x10, 0x03, 0x1a, 0x04, 0x01, 0x02, 0x03, 0x04, 0x22,
            0x02, 0xaa, 0xbb,
        ];
        assert_eq!(got, want, "leaf node encoding drifted: {got:02x?}");
    }

    #[test]
    fn golden_index_encoding() {
        let node = Node::index(IndexNode::from_children([
            (b"".to_vec(), "L0".to_string()),
            (b"m".to_vec(), "L1".to_string()),
        ]));
        let got = node.encode();
        let want = [
            0x22, 0x0f, 0x0a, 0x04, 0x12, 0x02, 0x4c, 0x30, 0x0a, 0x07, 0x0a, 0x01, 0x6d, 0x12,
            0x02, 0x4c, 0x31,
        ];
        assert_eq!(got, want, "index node encoding drifted: {got:02x?}");
    }
}
