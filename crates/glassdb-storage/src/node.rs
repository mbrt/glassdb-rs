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
//! decode, pure lookups, and the in-memory split primitives ([`Node::split`]);
//! descent lives in `directory.rs` and the background split protocol in the
//! `glassdb-trans` `split` module.

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

    /// Inserts a `(separator, child)` pair, the parent-side effect of a child
    /// split (ADR-031). A separator already present is overwritten, so a
    /// re-driven insert is idempotent.
    pub fn insert_child(&mut self, separator: Vec<u8>, child: NodeToken) {
        self.children.insert(separator, child);
    }

    /// Splits the index at its median separator: retains the lower children in
    /// `self` and returns the upper children together with the separator that
    /// bounds them (the first separator of the upper half). Used for interior
    /// and in-place root splits (ADR-031). Requires at least two children.
    pub fn split_off_median(&mut self) -> (IndexNode, Vec<u8>) {
        debug_assert!(
            self.children.len() >= 2,
            "cannot split an index with fewer than two children"
        );
        let mid = self.children.len() / 2;
        let separator = self
            .children
            .keys()
            .nth(mid)
            .cloned()
            .expect("median index is in range");
        let upper = self.children.split_off(&separator);
        (IndexNode { children: upper }, separator)
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

/// The soft caps that trigger a background split (ADR-031). A node over any of
/// its caps is a split candidate. Injected rather than hard-coded so the split
/// maintainer's thresholds are tunable and tests can drive splits with tiny
/// nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SplitPolicy {
    /// Maximum leaf entries before it is a split candidate.
    pub leaf_max_entries: usize,
    /// Maximum encoded leaf bytes before it is a split candidate.
    pub leaf_max_bytes: usize,
    /// Maximum index children (fan-out) before it is a split candidate.
    pub index_max_children: usize,
}

impl Default for SplitPolicy {
    fn default() -> Self {
        // A ~256-entry leaf soft cap mirrors the old fixed keys-per-shard target
        // (ADR-017), and keeps each object small for the backend.
        SplitPolicy {
            leaf_max_entries: 256,
            leaf_max_bytes: 256 * 1024,
            index_max_children: 256,
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

    /// Reports whether the node is over any of `policy`'s soft caps, making it a
    /// background split candidate (ADR-031). A node with fewer than two
    /// entries/children can never be split, so it is never a candidate however
    /// large a single entry is (single-hot-key relief is out of scope).
    pub fn over_soft_cap(&self, policy: &SplitPolicy) -> bool {
        match &self.body {
            NodeBody::Leaf(shard) => {
                shard.len() >= 2
                    && (shard.len() > policy.leaf_max_entries
                        || self.encode().len() > policy.leaf_max_bytes)
            }
            NodeBody::Index(index) => index.len() >= 2 && index.len() > policy.index_max_children,
        }
    }

    /// Halves the node for a B-link split (ADR-031): retains the lower half in
    /// `self` (bounded above by the split key and linked to `right_token`) and
    /// returns the newly created right sibling — which inherits `self`'s former
    /// high-key and right-sibling — together with the split key to promote into
    /// the parent. Returns `None` when the node is too small to divide (fewer
    /// than two entries/children), so a caller never produces an empty node.
    ///
    /// This is a pure in-memory transform; persisting the two nodes (create the
    /// sibling, then CAS the shrunk source — the linearization point) is the
    /// caller's multi-step protocol.
    pub fn split(&mut self, right_token: &str) -> Option<(Node, Vec<u8>)> {
        let (right_body, split_key) = match &mut self.body {
            NodeBody::Leaf(shard) => {
                if shard.len() < 2 {
                    return None;
                }
                let (upper, split_key) = shard.split_off_median();
                (NodeBody::Leaf(upper), split_key)
            }
            NodeBody::Index(index) => {
                if index.len() < 2 {
                    return None;
                }
                let (upper, separator) = index.split_off_median();
                (NodeBody::Index(upper), separator)
            }
        };
        // The right sibling takes over the upper range: the old high-key and the
        // old right-sibling link now bound and follow it.
        let right = Node {
            high_key: self.high_key.take(),
            right_sibling: self.right_sibling.take(),
            body: right_body,
        };
        // The retained lower half is now bounded by the split key and links to
        // the new sibling.
        self.high_key = Some(split_key.clone());
        self.right_sibling = Some(right_token.to_string());
        Some((right, split_key))
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
    fn leaf_split_moves_upper_half_and_relinks() {
        // A leaf with an existing high-key and right-sibling splits: the new
        // sibling inherits both bounds, the source is rebounded to the split key
        // and linked to the sibling token.
        let mut src = Node::leaf(Shard::from_entries([
            entry(b"apple", 1),
            entry(b"cat", 2),
            entry(b"mango", 3),
            entry(b"pear", 4),
        ]));
        src.set_high_key(Some(b"tiger".to_vec()));
        src.set_right_sibling(Some("oldRight".to_string()));

        let (right, split_key) = src.split("newRight").expect("splittable");
        assert_eq!(split_key, b"mango");

        // Source keeps the lower half, bounded by the split key, linked to the
        // new sibling.
        let src_keys: Vec<&[u8]> = src
            .as_leaf()
            .unwrap()
            .entries()
            .map(|e| e.key.as_slice())
            .collect();
        assert_eq!(src_keys, vec![b"apple".as_slice(), b"cat"]);
        assert_eq!(src.high_key(), Some(b"mango".as_slice()));
        assert_eq!(src.right_sibling(), Some("newRight"));

        // The sibling holds the upper half and inherits the source's former
        // high-key and right-sibling.
        let right_keys: Vec<&[u8]> = right
            .as_leaf()
            .unwrap()
            .entries()
            .map(|e| e.key.as_slice())
            .collect();
        assert_eq!(right_keys, vec![b"mango".as_slice(), b"pear"]);
        assert_eq!(right.high_key(), Some(b"tiger".as_slice()));
        assert_eq!(right.right_sibling(), Some("oldRight"));
    }

    #[test]
    fn index_split_promotes_separator_and_relinks() {
        let mut src = Node::index(IndexNode::from_children([
            (b"".to_vec(), "L0".to_string()),
            (b"f".to_vec(), "L1".to_string()),
            (b"m".to_vec(), "L2".to_string()),
            (b"t".to_vec(), "L3".to_string()),
        ]));
        let (right, sep) = src.split("newRight").expect("splittable");
        assert_eq!(
            sep, b"m",
            "promoted separator is the right half's low bound"
        );

        let left_seps: Vec<&[u8]> = src.as_index().unwrap().children().map(|(s, _)| s).collect();
        assert_eq!(left_seps, vec![b"".as_slice(), b"f"]);
        assert_eq!(src.high_key(), Some(b"m".as_slice()));
        assert_eq!(src.right_sibling(), Some("newRight"));

        let right_seps: Vec<&[u8]> = right
            .as_index()
            .unwrap()
            .children()
            .map(|(s, _)| s)
            .collect();
        assert_eq!(right_seps, vec![b"m".as_slice(), b"t"]);
    }

    #[test]
    fn split_of_undersized_node_is_none() {
        assert!(
            Node::leaf(Shard::from_entries([entry(b"only", 1)]))
                .split("r")
                .is_none()
        );
        assert!(Node::leaf(Shard::new()).split("r").is_none());
        let one_child = Node::index(IndexNode::from_children([(b"".to_vec(), "L0".to_string())]));
        assert!(one_child.clone().split("r").is_none());
    }

    #[test]
    fn over_soft_cap_respects_policy_and_min_size() {
        let tiny = SplitPolicy {
            leaf_max_entries: 2,
            leaf_max_bytes: 1 << 20,
            index_max_children: 2,
        };
        let two = Node::leaf(Shard::from_entries([entry(b"a", 1), entry(b"b", 2)]));
        assert!(!two.over_soft_cap(&tiny), "at the cap is not over it");
        let three = Node::leaf(Shard::from_entries([
            entry(b"a", 1),
            entry(b"b", 2),
            entry(b"c", 3),
        ]));
        assert!(three.over_soft_cap(&tiny));
        // A single oversized entry is never a candidate: it cannot be split.
        let byte_policy = SplitPolicy {
            leaf_max_entries: 1000,
            leaf_max_bytes: 1,
            index_max_children: 1000,
        };
        assert!(!Node::leaf(Shard::from_entries([entry(b"solo", 1)])).over_soft_cap(&byte_policy));
        assert!(
            three.over_soft_cap(&byte_policy),
            "multi-entry over the byte cap splits"
        );
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
