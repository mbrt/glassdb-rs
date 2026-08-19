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
use crate::lock::{ExclusiveGate, LockType, SharedExclusiveLock};
use crate::shard::{Shard, ShardEntry};
use crate::wire_size::{length_delimited_field, nonempty_length_delimited_field};
use glassdb_data::{NodeToken as ValidatedNodeToken, TxId};

const SHARD_ENTRIES_TAG: u32 = 1;
const INDEX_ENTRIES_TAG: u32 = 1;
const INDEX_SEPARATOR_TAG: u32 = 1;
const INDEX_CHILD_TAG: u32 = 2;
const NODE_LEAF_TAG: u32 = 3;
const NODE_INDEX_TAG: u32 = 4;

/// The opaque identity token of a non-root node (`{prefix}/_n/<token>`). The
/// root has no token; it lives at the fixed `_r` path.
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
    leaf_max_entries: usize,
    /// Maximum encoded content bytes before either a leaf or index node is a
    /// split candidate.
    node_soft_max_bytes: usize,
    /// Maximum index children (fan-out) before it is a split candidate.
    index_max_children: usize,
    /// Maximum encoded coordination-object size, including transient locks.
    node_max_bytes: usize,
    /// Bytes reserved for transient node-lock metadata at the hard cap.
    split_headroom_bytes: usize,
}

/// Builds a validated [`SplitPolicy`], starting from production defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SplitPolicyBuilder {
    leaf_max_entries: usize,
    node_soft_max_bytes: usize,
    index_max_children: usize,
    node_max_bytes: usize,
    split_headroom_bytes: usize,
}

/// A split policy whose reserved headroom exceeds its hard node cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "split headroom ({split_headroom_bytes} bytes) exceeds the node hard cap ({node_max_bytes} bytes)"
)]
pub struct InvalidSplitPolicy {
    node_max_bytes: usize,
    split_headroom_bytes: usize,
}

impl SplitPolicy {
    /// Starts building a policy from the production defaults.
    pub fn builder() -> SplitPolicyBuilder {
        SplitPolicyBuilder::default()
    }

    /// Maximum leaf entries before a leaf is a split candidate.
    pub fn leaf_max_entries(&self) -> usize {
        self.leaf_max_entries
    }

    /// Maximum encoded content bytes before either node kind is a split candidate.
    pub fn node_soft_max_bytes(&self) -> usize {
        self.node_soft_max_bytes
    }

    /// Maximum index children before an index is a split candidate.
    pub fn index_max_children(&self) -> usize {
        self.index_max_children
    }

    /// Maximum encoded coordination-object size, including transient locks.
    pub fn node_max_bytes(&self) -> usize {
        self.node_max_bytes
    }

    /// Bytes reserved for transient node-lock metadata at the hard cap.
    pub fn split_headroom_bytes(&self) -> usize {
        self.split_headroom_bytes
    }

    /// The encoded content size a node's entries must stay under, reserving
    /// headroom for transient locks and the split's shrink CAS.
    pub fn content_limit(&self) -> usize {
        self.node_max_bytes - self.split_headroom_bytes
    }

    /// Reports whether one exact leaf entry fits the per-entry budget that
    /// preserves room for another independently admissible entry.
    pub fn entry_fits_split_budget(&self, entry: &ShardEntry) -> bool {
        Node::leaf_entry_content_encoded_len(entry) <= self.content_limit() / 2
    }

    /// Reports whether `key` can fit in both a splittable leaf entry and its
    /// eventual parent separator under this policy.
    pub fn key_fits(&self, key: &[u8]) -> bool {
        let content_limit = self.content_limit();
        Node::worst_case_leaf_entry_len(key.len()) <= content_limit / 2
            && self.parent_separator_fits(key)
    }

    fn parent_separator_fits(&self, key: &[u8]) -> bool {
        Node::worst_case_parent_separator_len(key.len()) <= self.content_limit()
    }
}

impl SplitPolicyBuilder {
    /// Sets the maximum leaf entry count before a split is requested.
    pub fn leaf_max_entries(mut self, value: usize) -> Self {
        self.leaf_max_entries = value;
        self
    }

    /// Sets the shared encoded-content soft cap for leaf and index nodes.
    pub fn node_soft_max_bytes(mut self, value: usize) -> Self {
        self.node_soft_max_bytes = value;
        self
    }

    /// Sets the maximum index fan-out before a split is requested.
    pub fn index_max_children(mut self, value: usize) -> Self {
        self.index_max_children = value;
        self
    }

    /// Sets the hard encoded size cap for a coordination node.
    pub fn node_max_bytes(mut self, value: usize) -> Self {
        self.node_max_bytes = value;
        self
    }

    /// Sets the hard-cap space reserved for transient split coordination.
    pub fn split_headroom_bytes(mut self, value: usize) -> Self {
        self.split_headroom_bytes = value;
        self
    }

    /// Validates the hard-cap relationship and returns the completed policy.
    pub fn build(self) -> Result<SplitPolicy, InvalidSplitPolicy> {
        if self.split_headroom_bytes > self.node_max_bytes {
            return Err(InvalidSplitPolicy {
                node_max_bytes: self.node_max_bytes,
                split_headroom_bytes: self.split_headroom_bytes,
            });
        }
        Ok(SplitPolicy {
            leaf_max_entries: self.leaf_max_entries,
            node_soft_max_bytes: self.node_soft_max_bytes,
            index_max_children: self.index_max_children,
            node_max_bytes: self.node_max_bytes,
            split_headroom_bytes: self.split_headroom_bytes,
        })
    }
}

impl Default for SplitPolicyBuilder {
    fn default() -> Self {
        Self {
            leaf_max_entries: 256,
            node_soft_max_bytes: 256 * 1024,
            index_max_children: 256,
            node_max_bytes: 1024 * 1024,
            split_headroom_bytes: 64 * 1024,
        }
    }
}

impl Default for SplitPolicy {
    fn default() -> Self {
        // A ~256-entry leaf soft cap mirrors the old fixed keys-per-shard target
        // (ADR-017), and keeps each object small for the backend.
        SplitPolicyBuilder::default()
            .build()
            .expect("default split policy is valid")
    }
}

/// The node-level coordination state threaded through a leaf CAS round.
///
/// Keeping this separate from the node's topology prevents transaction-engine
/// resolvers from replacing bounds, sibling links, or the node body while they
/// only intend to change locks.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeLocks {
    structure: ExclusiveGate,
    membership: SharedExclusiveLock,
    membership_version: u64,
    delete_intent: Option<TxId>,
}

impl NodeLocks {
    /// Returns the exclusive gate guarding changes to the node's physical shape.
    pub fn structural_gate(&self) -> &ExclusiveGate {
        &self.structure
    }

    /// Returns the membership lock guarding a leaf's live key set.
    pub fn membership(&self) -> &SharedExclusiveLock {
        &self.membership
    }

    /// Returns the membership generation used by scans and unmarked point absence.
    pub fn membership_version(&self) -> u64 {
        self.membership_version
    }

    /// Records one logical membership change without installing a holder.
    ///
    /// Logless commits have no prepare/release lock lifecycle, so their commit
    /// CAS advances the scan-validation generation directly (ADR-061).
    pub fn advance_membership_version(&mut self) {
        self.membership_version = self.membership_version.wrapping_add(1);
    }

    /// Returns the transaction preparing deletion of the containing collection.
    pub fn delete_intent(&self) -> Option<&TxId> {
        self.delete_intent.as_ref()
    }

    /// Installs the collection-delete intent owned by `id`.
    pub fn set_delete_intent(&mut self, id: TxId) {
        self.delete_intent = Some(id);
    }

    /// Removes the collection-delete intent when it is owned by `id`.
    pub fn remove_delete_intent(&mut self, id: &TxId) -> bool {
        if self.delete_intent.as_ref() != Some(id) {
            return false;
        }
        self.delete_intent = None;
        true
    }

    /// Closes the structural gate for one structural operation.
    pub fn set_structural_gate(&mut self, id: TxId) {
        self.structure.set_writer(id);
    }

    /// Opens the structural gate when held by `id`.
    pub fn remove_structural_gate(&mut self, id: &TxId) -> bool {
        self.structure.remove(id)
    }

    /// Installs a shared membership holder without recording write activity.
    pub fn add_membership_reader(&mut self, id: TxId) {
        self.membership.add_reader(id);
    }

    /// Installs an exclusive membership holder and records the activity.
    pub fn set_membership_writer(&mut self, id: TxId) {
        if self.membership.lock_type() == LockType::Write
            && self.membership.holders() == std::slice::from_ref(&id)
        {
            return;
        }
        self.membership.set_writer(id);
        self.membership_version = self.membership_version.wrapping_add(1);
    }

    /// Removes one membership holder and records released write activity.
    pub fn remove_membership_holder(&mut self, id: &TxId) -> bool {
        let was_writer =
            self.membership.lock_type() == LockType::Write && self.membership.contains(id);
        let removed = self.membership.remove(id);
        if removed && was_writer {
            self.membership_version = self.membership_version.wrapping_add(1);
        }
        removed
    }

    /// Removes the transaction's membership hold.
    ///
    /// Structural gates have a separate lifecycle and cannot be released by
    /// ordinary transaction cleanup.
    pub fn release_membership(&mut self, id: &TxId) -> bool {
        self.remove_membership_holder(id)
    }

    /// Clears transient holders while preserving the membership version.
    fn clear_holders(&mut self) {
        self.structure.clear();
        self.membership.clear();
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
    locks: NodeLocks,
}

impl Node {
    /// Creates a leaf node owning the whole key space (high-key +infinity, no
    /// right sibling) from `shard` — the shape of a brand-new root.
    pub fn leaf(shard: Shard) -> Self {
        Node {
            high_key: None,
            right_sibling: None,
            body: NodeBody::Leaf(shard),
            locks: NodeLocks::default(),
        }
    }

    /// Creates an index node owning the whole key space from `index`.
    pub fn index(index: IndexNode) -> Self {
        Node {
            high_key: None,
            right_sibling: None,
            body: NodeBody::Index(index),
            locks: NodeLocks::default(),
        }
    }

    /// Returns the node with the given exclusive upper range bound.
    #[must_use]
    pub fn with_high_key(mut self, high_key: Option<Vec<u8>>) -> Self {
        self.high_key = high_key;
        self
    }

    /// Returns the node with the given right-sibling link.
    #[must_use]
    pub fn with_right_sibling(mut self, right_sibling: Option<NodeToken>) -> Self {
        self.right_sibling = right_sibling;
        self
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

    /// Replaces the leaf body while preserving bounds and node coordination.
    pub fn set_leaf(&mut self, shard: Shard) -> Result<(), StorageError> {
        if matches!(self.body, NodeBody::Leaf(_)) {
            self.body = NodeBody::Leaf(shard);
            Ok(())
        } else {
            Err(StorageError::other("node is not a leaf"))
        }
    }

    /// Replaces the index body while preserving bounds and node coordination.
    pub fn set_index(&mut self, index: IndexNode) -> Result<(), StorageError> {
        if matches!(self.body, NodeBody::Index(_)) {
            self.body = NodeBody::Index(index);
            Ok(())
        } else {
            Err(StorageError::other("node is not an index"))
        }
    }

    /// Returns the node's exclusive structural gate.
    pub fn structural_gate(&self) -> &ExclusiveGate {
        self.locks.structural_gate()
    }

    /// Returns the transaction preparing deletion of this node's collection.
    pub fn collection_delete_intent(&self) -> Option<&TxId> {
        self.locks.delete_intent()
    }

    /// Installs a collection-delete intent on this node.
    pub fn set_collection_delete_intent(&mut self, id: TxId) {
        self.locks.set_delete_intent(id);
    }

    /// Clears a collection-delete intent owned by `id`.
    pub fn remove_collection_delete_intent(&mut self, id: &TxId) -> bool {
        self.locks.remove_delete_intent(id)
    }

    /// Returns the complete node-level coordination state.
    pub fn locks(&self) -> &NodeLocks {
        &self.locks
    }

    /// Returns the mutable node-level coordination state.
    /// Replaces the node-level coordination state.
    pub fn set_locks(&mut self, locks: NodeLocks) {
        self.locks = locks;
    }

    /// Closes the structural gate for one structural operation.
    pub fn set_structural_gate(&mut self, id: TxId) {
        self.locks.set_structural_gate(id);
    }

    /// Opens the structural gate when held by `id`.
    pub fn remove_structural_gate(&mut self, id: &TxId) -> bool {
        self.locks.remove_structural_gate(id)
    }

    /// Returns the leaf membership lock.
    pub fn membership_lock(&self) -> &SharedExclusiveLock {
        self.locks.membership()
    }

    /// Installs a membership-read holder without recording membership activity.
    pub fn add_membership_reader(&mut self, id: TxId) {
        self.locks.add_membership_reader(id);
    }

    /// Installs a membership-write holder and records the membership activity.
    pub fn set_membership_writer(&mut self, id: TxId) {
        self.locks.set_membership_writer(id);
    }

    /// Removes a membership-lock holder and records released write activity.
    pub fn remove_membership_holder(&mut self, id: &TxId) -> bool {
        self.locks.remove_membership_holder(id)
    }

    /// Returns the leaf membership generation.
    pub fn membership_version(&self) -> u64 {
        self.locks.membership_version()
    }

    /// Returns the canonical encoded size without transient node locks.
    pub fn content_encoded_len(&self) -> usize {
        let mut content = self.clone();
        content.clear_node_locks();
        content.encoded_len()
    }

    /// Returns the exact node-content size of a leaf containing only `entry`.
    pub fn leaf_entry_content_encoded_len(entry: &ShardEntry) -> usize {
        let shard_len = length_delimited_field(SHARD_ENTRIES_TAG, entry.encoded_len());
        length_delimited_field(NODE_LEAF_TAG, shard_len)
    }

    /// Returns the node-content size of a leaf containing the largest fixed
    /// coordination shape GlassDB can add for a key of `key_len` bytes.
    pub fn worst_case_leaf_entry_len(key_len: usize) -> usize {
        let shard_len = length_delimited_field(
            SHARD_ENTRIES_TAG,
            ShardEntry::worst_case_encoded_len(key_len),
        );
        length_delimited_field(NODE_LEAF_TAG, shard_len)
    }

    /// Returns the node-content size of the smallest parent that can contain a
    /// separator of `key_len` bytes, using maximum-length validated child tokens.
    pub fn worst_case_parent_separator_len(key_len: usize) -> usize {
        let child_len =
            length_delimited_field(INDEX_CHILD_TAG, ValidatedNodeToken::MAX_ENCODED_LEN);
        let entry_len = |separator_len| {
            nonempty_length_delimited_field(INDEX_SEPARATOR_TAG, separator_len) + child_len
        };
        let candidate_len = length_delimited_field(INDEX_ENTRIES_TAG, entry_len(key_len));
        let index_len = if key_len == 0 {
            // The candidate is itself the leftmost separator; a BTreeMap cannot
            // contain a second entry with the same empty key.
            candidate_len
        } else {
            length_delimited_field(INDEX_ENTRIES_TAG, entry_len(0)) + candidate_len
        };
        length_delimited_field(NODE_INDEX_TAG, index_len)
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
                    && (shard.len() > policy.leaf_max_entries()
                        || self.content_encoded_len() > policy.node_soft_max_bytes())
            }
            NodeBody::Index(index) => {
                index.len() >= 2
                    && (index.len() > policy.index_max_children()
                        || self.content_encoded_len() > policy.node_soft_max_bytes())
            }
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
            locks: {
                let mut locks = self.locks.clone();
                locks.clear_holders();
                locks
            },
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

    /// Returns the canonical protobuf size without allocating the encoded body.
    pub fn encoded_len(&self) -> usize {
        self.to_pb().encoded_len()
    }

    /// Decodes a node from its protobuf body. A message with no body is treated
    /// as an empty leaf spanning the whole key space (the shape of a fresh root).
    pub fn decode(buf: &[u8]) -> Result<Self, StorageError> {
        let raw = pb::Node::decode(buf)
            .map_err(|e| StorageError::with_source("unmarshalling node", e))?;
        Node::from_pb(raw)
    }

    /// Clears node locks before a split-created node becomes visible.
    pub(crate) fn clear_node_locks(&mut self) {
        self.locks.clear_holders();
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
            structure_lock: (!self.locks.structure.is_empty())
                .then(|| self.locks.structure.to_pb()),
            membership_lock: (!self.locks.membership.is_empty())
                .then(|| self.locks.membership.to_pb()),
            membership_version: self.locks.membership_version,
            collection_delete_intent: self
                .locks
                .delete_intent
                .as_ref()
                .map(|id| id.as_bytes().to_vec())
                .unwrap_or_default(),
        }
    }

    pub(crate) fn from_pb(raw: pb::Node) -> Result<Self, StorageError> {
        let body = match raw.body {
            Some(pb::node::Body::Index(index)) => NodeBody::Index(IndexNode::from_pb(index)),
            Some(pb::node::Body::Leaf(leaf)) => NodeBody::Leaf(Shard::from_pb(leaf)?),
            None => NodeBody::Leaf(Shard::new()),
        };
        let structure = ExclusiveGate::from_pb(raw.structure_lock).map_err(|_| {
            StorageError::other("node structural gate must be empty or have one write holder")
        })?;
        let membership = SharedExclusiveLock::from_pb(raw.membership_lock)
            .map_err(|_| StorageError::other("node has invalid membership lock"))?;
        let delete_intent = (!raw.collection_delete_intent.is_empty())
            .then(|| TxId::from_bytes(raw.collection_delete_intent));
        Ok(Node {
            high_key: (!raw.high_key.is_empty()).then_some(raw.high_key),
            right_sibling: (!raw.right_sibling.is_empty()).then_some(raw.right_sibling),
            body,
            locks: NodeLocks {
                structure,
                membership,
                membership_version: raw.membership_version,
                delete_intent,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use glassdb_data::TxId;

    use crate::shard::{CurrentState, ShardEntry};

    fn entry(key: &[u8], writer: u8) -> ShardEntry {
        ShardEntry::new(key).with_current(CurrentState::External {
            writer: TxId::from_bytes(vec![writer]),
        })
    }

    fn golden_entry() -> ShardEntry {
        let mut entry = ShardEntry::new(b"Hello").with_current(CurrentState::External {
            writer: TxId::from_bytes(vec![0xaa, 0xbb]),
        });
        entry.replace_write_lock(TxId::from_bytes(vec![1, 2, 3, 4]));
        entry
    }

    #[test]
    fn leaf_round_trip_preserves_bounds() {
        let node = Node::leaf(Shard::from_entries([entry(b"apple", 1), entry(b"cat", 2)]))
            .with_high_key(Some(b"m".to_vec()))
            .with_right_sibling(Some("sibToken".to_string()));

        let decoded = Node::decode(&node.encode()).unwrap();
        assert_eq!(decoded, node);
        assert_eq!(decoded.high_key(), Some(b"m".as_slice()));
        assert_eq!(decoded.right_sibling(), Some("sibToken"));
        assert!(decoded.as_leaf().is_some());
    }

    #[test]
    fn round_trip_preserves_node_locks_and_membership_version() {
        let gate = TxId::from_bytes(vec![2]);
        let writer = TxId::from_bytes(vec![1]);
        let mut node = Node::leaf(Shard::new());
        node.set_structural_gate(gate.clone());
        node.set_membership_writer(writer.clone());

        let decoded = Node::decode(&node.encode()).unwrap();
        assert_eq!(decoded.structural_gate().holders(), &[gate]);
        assert_eq!(decoded.membership_lock().holders(), &[writer]);
        assert_eq!(decoded.membership_version(), 1);
    }

    #[test]
    fn decode_rejects_invalid_structural_gate_states() {
        for gate in [
            pb::NodeLock {
                lock_type: pb::lock::LockType::Read as i32,
                locked_by: vec![vec![1]],
            },
            pb::NodeLock {
                lock_type: pb::lock::LockType::Create as i32,
                locked_by: vec![vec![1]],
            },
            pb::NodeLock {
                lock_type: pb::lock::LockType::Write as i32,
                locked_by: vec![vec![1], vec![2]],
            },
        ] {
            let raw = pb::Node {
                structure_lock: Some(gate),
                ..pb::Node::default()
            };
            assert!(Node::decode(&raw.encode_to_vec()).is_err());
        }
    }

    #[test]
    fn decode_rejects_create_membership_lock() {
        let raw = pb::Node {
            membership_lock: Some(pb::NodeLock {
                lock_type: pb::lock::LockType::Create as i32,
                locked_by: vec![vec![1]],
            }),
            ..pb::Node::default()
        };

        let error = Node::decode(&raw.encode_to_vec()).unwrap_err();
        assert_eq!(error.to_string(), "node has invalid membership lock");
    }

    #[test]
    fn decode_rejects_duplicate_node_lock_holders() {
        let raw = pb::Node {
            membership_lock: Some(pb::NodeLock {
                lock_type: pb::lock::LockType::Read as i32,
                locked_by: vec![vec![2], vec![1], vec![1]],
            }),
            ..pb::Node::default()
        };

        let error = Node::decode(&raw.encode_to_vec()).unwrap_err();
        assert_eq!(error.to_string(), "node has invalid membership lock");
    }

    #[test]
    fn membership_version_tracks_write_lock_activity() {
        let id = TxId::from_bytes(vec![1]);
        let mut node = Node::leaf(Shard::new());

        node.add_membership_reader(id.clone());
        assert_eq!(node.membership_version(), 0);
        assert!(node.remove_membership_holder(&id));
        assert_eq!(node.membership_version(), 0);

        node.set_membership_writer(id.clone());
        assert_eq!(node.membership_version(), 1);
        node.set_membership_writer(id.clone());
        assert_eq!(node.membership_version(), 1);
        assert!(node.remove_membership_holder(&id));
        assert_eq!(node.membership_version(), 2);
        assert!(!node.remove_membership_holder(&id));
        assert_eq!(node.membership_version(), 2);

        node.locks.advance_membership_version();
        assert_eq!(node.membership_version(), 3);

        node.locks.membership_version = u64::MAX;
        node.set_membership_writer(id);
        assert_eq!(node.membership_version(), 0);
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
        ]))
        .with_high_key(Some(b"tiger".to_vec()))
        .with_right_sibling(Some("oldRight".to_string()));

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
    fn leaf_split_preserves_membership_generation_in_both_outputs() {
        let mut src = Node::leaf(Shard::from_entries([
            entry(b"a", 1),
            entry(b"b", 2),
            entry(b"c", 3),
            entry(b"d", 4),
        ]));
        let mut locks = src.locks().clone();
        locks.advance_membership_version();
        locks.advance_membership_version();
        src.set_locks(locks);

        let (right, _) = src.split("newRight").expect("splittable");
        assert_eq!(src.membership_version(), 2);
        assert_eq!(right.membership_version(), 2);
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
        let tiny = SplitPolicy::builder()
            .leaf_max_entries(2)
            .node_soft_max_bytes(1 << 20)
            .index_max_children(2)
            .build()
            .unwrap();
        let two = Node::leaf(Shard::from_entries([entry(b"a", 1), entry(b"b", 2)]));
        assert!(!two.over_soft_cap(&tiny), "at the cap is not over it");
        let three = Node::leaf(Shard::from_entries([
            entry(b"a", 1),
            entry(b"b", 2),
            entry(b"c", 3),
        ]));
        assert!(three.over_soft_cap(&tiny));
        let two_index = Node::index(IndexNode::from_children([
            (b"".to_vec(), "L0".to_string()),
            (b"m".to_vec(), "L1".to_string()),
        ]));
        assert!(
            !two_index.over_soft_cap(&tiny),
            "index at the child cap is not over it"
        );
        let three_index = Node::index(IndexNode::from_children([
            (b"".to_vec(), "L0".to_string()),
            (b"m".to_vec(), "L1".to_string()),
            (b"t".to_vec(), "L2".to_string()),
        ]));
        assert!(three_index.over_soft_cap(&tiny));

        // A single oversized entry is never a candidate: it cannot be split.
        let byte_policy = SplitPolicy::builder()
            .leaf_max_entries(1000)
            .node_soft_max_bytes(1)
            .index_max_children(1000)
            .build()
            .unwrap();
        assert!(!Node::leaf(Shard::from_entries([entry(b"solo", 1)])).over_soft_cap(&byte_policy));
        for (kind, node) in [("leaf", two), ("index", two_index)] {
            let at_limit = SplitPolicy::builder()
                .leaf_max_entries(usize::MAX)
                .node_soft_max_bytes(node.content_encoded_len())
                .index_max_children(usize::MAX)
                .build()
                .unwrap();
            assert!(
                !node.over_soft_cap(&at_limit),
                "{kind} at the encoded-content cap is not over it"
            );
            assert!(
                node.over_soft_cap(
                    &SplitPolicy::builder()
                        .leaf_max_entries(usize::MAX)
                        .node_soft_max_bytes(at_limit.node_soft_max_bytes() - 1)
                        .index_max_children(usize::MAX)
                        .build()
                        .unwrap(),
                ),
                "{kind} one byte over the encoded-content cap splits"
            );
        }
    }

    #[test]
    fn exact_entry_split_budget_is_half_the_content_limit() {
        let exact_headroom = SplitPolicy::builder()
            .node_max_bytes(128)
            .split_headroom_bytes(128)
            .build()
            .unwrap();
        assert_eq!(exact_headroom.content_limit(), 0);
        assert!(
            SplitPolicy::builder()
                .node_max_bytes(128)
                .split_headroom_bytes(129)
                .build()
                .is_err()
        );

        let entry = entry(b"boundary", 1);
        let entry_len = Node::leaf(Shard::from_entries([entry.clone()])).content_encoded_len();
        let admitting = SplitPolicy::builder()
            .node_max_bytes(entry_len * 2)
            .split_headroom_bytes(0)
            .build()
            .unwrap();
        assert!(admitting.entry_fits_split_budget(&entry));

        let rejecting = SplitPolicy::builder()
            .node_max_bytes(entry_len * 2 - 1)
            .split_headroom_bytes(0)
            .build()
            .unwrap();
        assert!(!rejecting.entry_fits_split_budget(&entry));
    }

    #[test]
    fn maximum_key_admission_matches_real_nodes_at_the_exact_limit() {
        let maximum_key = vec![b'k'; 128];
        let id = TxId::with_priority(7, b"maximum");
        let mut entry = ShardEntry::new(maximum_key.clone())
            .with_current(CurrentState::External { writer: id.clone() });
        entry.replace_write_lock(id);
        let leaf = Node::leaf(Shard::from_entries([entry]));

        let parent = Node::index(IndexNode::from_children([
            (
                Vec::new(),
                ValidatedNodeToken::from_bytes([1; 16]).to_string(),
            ),
            (
                maximum_key.clone(),
                ValidatedNodeToken::from_bytes([2; 16]).to_string(),
            ),
        ]));
        assert_eq!(parent.as_index().unwrap().len(), 2);

        let leaf_requirement = leaf
            .content_encoded_len()
            .checked_mul(2)
            .expect("test leaf size fits usize");
        let required_limit = leaf_requirement.max(parent.content_encoded_len());
        let headroom = 17;
        let exact = SplitPolicy::builder()
            .node_max_bytes(
                required_limit
                    .checked_add(headroom)
                    .expect("test node size fits usize"),
            )
            .split_headroom_bytes(headroom)
            .build()
            .unwrap();
        assert_eq!(exact.content_limit(), required_limit);
        assert!(exact.key_fits(&maximum_key));

        let parent_limit = parent.content_encoded_len();
        let parent_exact = SplitPolicy::builder()
            .node_max_bytes(parent_limit)
            .split_headroom_bytes(0)
            .build()
            .unwrap();
        assert!(parent_exact.parent_separator_fits(&maximum_key));
        assert!(
            !SplitPolicy::builder()
                .node_max_bytes(parent_limit - 1)
                .split_headroom_bytes(0)
                .build()
                .unwrap()
                .parent_separator_fits(&maximum_key)
        );

        let one_byte_over = SplitPolicy::builder()
            .node_max_bytes(exact.node_max_bytes() - 1)
            .split_headroom_bytes(headroom)
            .build()
            .unwrap();
        assert_eq!(one_byte_over.content_limit(), required_limit - 1);
        assert!(!one_byte_over.key_fits(&maximum_key));

        let mut above_maximum = maximum_key;
        above_maximum.push(b'k');
        assert!(!exact.key_fits(&above_maximum));
    }

    #[test]
    fn owns_reflects_high_key() {
        let plus_inf = Node::leaf(Shard::new());
        assert!(plus_inf.owns(b"anything"));

        let bounded = Node::leaf(Shard::new()).with_high_key(Some(b"m".to_vec()));
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
    fn codec_size_predictions_match_varint_boundaries() {
        let id = TxId::from_bytes(vec![0; TxId::MAX_GENERATED_ENCODED_LEN]);
        let token = ValidatedNodeToken::from_bytes([0; 16]).to_string();
        assert_eq!(token.len(), ValidatedNodeToken::MAX_ENCODED_LEN);

        for key_len in [
            0, 1, 81, 82, 83, 84, 127, 128, 16_335, 16_336, 16_338, 16_339, 16_383, 16_384,
        ] {
            let mut entry = ShardEntry::new(vec![b'k'; key_len])
                .with_current(CurrentState::External { writer: id.clone() });
            entry.replace_write_lock(id.clone());
            let actual = Node::leaf(Shard::from_entries([entry.clone()])).content_encoded_len();

            assert_eq!(
                Node::leaf_entry_content_encoded_len(&entry),
                actual,
                "exact leaf entry with {key_len}-byte key"
            );
            assert_eq!(
                Node::worst_case_leaf_entry_len(key_len),
                actual,
                "worst-case leaf entry with {key_len}-byte key"
            );
        }

        for key_len in [
            0, 1, 73, 74, 101, 102, 127, 128, 16_327, 16_328, 16_356, 16_357, 16_383, 16_384,
        ] {
            let actual = Node::index(IndexNode::from_children([
                (Vec::new(), token.clone()),
                (vec![b'k'; key_len], token.clone()),
            ]))
            .content_encoded_len();

            assert_eq!(
                Node::worst_case_parent_separator_len(key_len),
                actual,
                "parent separator with {key_len}-byte key"
            );
        }
    }

    #[test]
    fn empty_body_decodes_as_empty_leaf() {
        // A Node protobuf with no body (the wire default) is a fresh empty root.
        let raw = pb::Node::default();
        let node = Node::from_pb(raw).unwrap();
        assert!(node.as_leaf().is_some_and(Shard::is_empty));
        assert_eq!(node.high_key(), None);
        assert_eq!(node.right_sibling(), None);
    }

    // Golden vectors: a fixed node must always encode to these exact bytes.
    // Changing the on-disk format must break these tests.
    #[test]
    fn golden_leaf_encoding() {
        let node = Node::leaf(Shard::from_entries([golden_entry()]))
            .with_high_key(Some(b"m".to_vec()))
            .with_right_sibling(Some("sib".to_string()));
        let got = node.encode();
        let want = [
            0x0a, 0x01, 0x6d, 0x12, 0x03, 0x73, 0x69, 0x62, 0x1a, 0x19, 0x0a, 0x17, 0x0a, 0x05,
            0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x10, 0x03, 0x1a, 0x04, 0x01, 0x02, 0x03, 0x04, 0x22,
            0x06, 0x0a, 0x02, 0xaa, 0xbb, 0x10, 0x01,
        ];
        assert_eq!(node.encoded_len(), got.len());
        assert_eq!(got, want, "leaf node encoding drifted: {got:02x?}");
    }

    #[test]
    fn released_node_lock_is_omitted_from_encoding() {
        let never_locked = Node::leaf(Shard::from_entries([entry(b"a", 1)]));
        let mut released = never_locked.clone();
        let holder = TxId::from_bytes(vec![0x11]);
        released.set_structural_gate(holder.clone());
        assert!(released.remove_structural_gate(&holder));
        assert_eq!(released.encode(), never_locked.encode());
    }

    // Golden vector for the ADR-032 node-lock fields. Changing their tags,
    // lock-type values, holder encoding, or membership-version encoding must
    // break this test.
    #[test]
    fn golden_node_locks_encoding() {
        let mut node = Node::leaf(Shard::from_entries([golden_entry()]));
        node.set_structural_gate(TxId::from_bytes(vec![0x11]));
        node.set_membership_writer(TxId::from_bytes(vec![0x22]));

        let got = node.encode();
        let want = [
            0x1a, 0x19, 0x0a, 0x17, 0x0a, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x10, 0x03, 0x1a,
            0x04, 0x01, 0x02, 0x03, 0x04, 0x22, 0x06, 0x0a, 0x02, 0xaa, 0xbb, 0x10, 0x01, 0x2a,
            0x05, 0x08, 0x03, 0x12, 0x01, 0x11, 0x32, 0x05, 0x08, 0x03, 0x12, 0x01, 0x22, 0x38,
            0x01,
        ];
        assert_eq!(node.encoded_len(), got.len());
        assert_eq!(got, want, "node-lock encoding drifted: {got:02x?}");
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
        assert_eq!(node.encoded_len(), got.len());
        assert_eq!(got, want, "index node encoding drifted: {got:02x?}");
    }
}
