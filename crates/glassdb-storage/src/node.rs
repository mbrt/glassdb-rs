//! The B-link tree node: in-memory view and canonical protobuf encoding
//! (ADR-031).
//!
//! A node is the unit of the dynamic, range-partitioned collection tree.
//! It is either a **leaf** — the per-key coordination entries of ADR-017 (a
//! [`LeafBody`]) for a contiguous key range — or an **index**, an ordered map from
//! separator keys to child node IDs. Every node self-describes the range it
//! covers through a **high-key** (the exclusive upper bound; absent means
//! +infinity) and a **right-sibling** pointer, the two fields that let a descent
//! detect a concurrent split and self-correct by stepping right rather than
//! restarting from the root.
//!
//! Like the leaf and root objects, a node body is a compare-and-swap unit, so
//! the encoding is canonical (leaf entries and index separators sorted, holder
//! sets sorted) and golden-anchored. This module is inert data plus encode/
//! decode, pure lookups, and the in-memory split primitives ([`Node::split`]);
//! descent lives in `directory.rs` and the background split protocol in the
//! `glassdb-trans` `split` module.

use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Included, Unbounded};

use glassdb_proto as pb;
use prost::Message;

use crate::error::StorageError;
use crate::leaf::{LeafBody, LeafEntry};
use crate::lock::{ExclusiveGate, LockType, SharedExclusiveLock};
use crate::wire_size::{length_delimited_field, nonempty_length_delimited_field};
use glassdb_data::{ID_BYTES, NodeId, StructuralIntentId, TxId};

const LEAF_ENTRIES_TAG: u32 = 1;
const INDEX_ENTRIES_TAG: u32 = 1;
const INDEX_SEPARATOR_TAG: u32 = 1;
const INDEX_CHILD_TAG: u32 = 2;
const NODE_LEAF_TAG: u32 = 3;
const NODE_INDEX_TAG: u32 = 4;

/// An index node body: the separator keys of an index node, each mapping the
/// inclusive lower bound of a key range to its routed child node.
///
/// Separators are held sorted, so iteration and encoding are canonical and the
/// routed child for a key is found by a single predecessor lookup.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IndexNode {
    children: BTreeMap<Vec<u8>, NodeId>,
}

impl IndexNode {
    /// Builds an index node from `(separator, child)` pairs. The separator is the
    /// inclusive lower bound of the child's range; the leftmost child usually
    /// carries the empty separator (the node's own low bound).
    pub fn from_children<I: IntoIterator<Item = (Vec<u8>, NodeId)>>(children: I) -> Self {
        IndexNode {
            children: children.into_iter().collect(),
        }
    }

    /// Returns the node ID of the routed child for `key`: the child whose
    /// separator is the greatest one not exceeding `key`. Falls back to the
    /// leftmost child when `key` precedes every separator (a defensive case a
    /// well-formed descent never hits, since the node's low bound is its first
    /// separator).
    pub fn child_for(&self, key: &[u8]) -> Option<NodeId> {
        self.children
            .range::<[u8], _>((Unbounded, Included(key)))
            .next_back()
            .map(|(_, child)| *child)
            .or_else(|| self.children.values().next().copied())
    }

    /// Returns the node ID of the routed child for keys just below `key`: the
    /// child whose separator is the greatest one below `key`. Falls back to the
    /// leftmost child like [`child_for`](Self::child_for).
    pub fn child_before(&self, key: &[u8]) -> Option<NodeId> {
        self.children
            .range::<[u8], _>((Unbounded, Excluded(key)))
            .next_back()
            .map(|(_, child)| *child)
            .or_else(|| self.children.values().next().copied())
    }

    /// Makes the separators of one right-link path of children agree with that
    /// path (ADR-073). `path` lists the children in link order, from the child
    /// routed for keys just below a key through the child that covers the key.
    pub fn reconcile(&mut self, path: &[(NodeId, &Node)]) {
        let mut live = Vec::with_capacity(path.len());
        for (position, (id, node)) in path.iter().enumerate() {
            if !node.is_drained() {
                live.push((*id, *node));
                continue;
            }
            let Some((target, _)) = path[position + 1..].iter().find(|(_, n)| !n.is_drained())
            else {
                continue;
            };
            for child in self.children.values_mut().filter(|child| *child == id) {
                *child = *target;
            }
        }
        for pair in live.windows(2) {
            let ((_, previous), (id, _)) = (pair[0], pair[1]);
            if let Some(separator) = previous.high_key() {
                self.children.entry(separator.to_vec()).or_insert(id);
            }
        }
        // Two adjacent entries that name the same child route like the first one.
        let mut previous: Option<NodeId> = None;
        self.children.retain(|_, child| {
            let duplicate = previous == Some(*child);
            previous = Some(*child);
            !duplicate
        });
    }

    /// Iterates the `(separator, child)` pairs in canonical (separator-sorted)
    /// order.
    pub fn children(&self) -> impl Iterator<Item = (&[u8], NodeId)> {
        self.children
            .iter()
            .map(|(separator, child)| (separator.as_slice(), *child))
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
    pub fn insert_child(&mut self, separator: Vec<u8>, child: NodeId) {
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
                .map(|(separator, child)| pb::IndexEntry {
                    separator_key: separator.clone(),
                    child: child.as_bytes().to_vec(),
                })
                .collect(),
        }
    }

    fn from_pb(raw: pb::IndexNode) -> Result<Self, StorageError> {
        let children = raw
            .entries
            .into_iter()
            .map(|entry| {
                let child = NodeId::from_slice(&entry.child)
                    .ok_or_else(|| StorageError::other("index node has an invalid child ID"))?;
                Ok((entry.separator_key, child))
            })
            .collect::<Result<_, StorageError>>()?;
        Ok(IndexNode { children })
    }
}

/// Size admission limits and soft thresholds for coordination-node splits and
/// merges.
///
/// The hard cap and reserved headroom are shared database settings. Soft
/// thresholds tune each database instance's background splits and merges
/// (ADR-031, ADR-072, ADR-073).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeSizePolicy {
    /// Maximum leaf entries before it is a split candidate.
    leaf_max_entries: usize,
    /// Maximum encoded content bytes before either a leaf or index node is a
    /// split candidate.
    node_soft_max_bytes: usize,
    /// Maximum index children (fan-out) before it is a split candidate.
    index_max_children: usize,
    /// Minimum live leaf entries below which a leaf is a merge candidate.
    leaf_min_entries: usize,
    /// Minimum index children below which an index is a merge candidate.
    index_min_children: usize,
    /// Maximum encoded coordination-object size, including transient locks.
    node_max_bytes: usize,
    /// Bytes reserved for transient node-lock metadata at the hard cap.
    split_headroom_bytes: usize,
}

/// Builds a validated [`NodeSizePolicy`], starting from production defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeSizePolicyBuilder {
    leaf_max_entries: usize,
    node_soft_max_bytes: usize,
    index_max_children: usize,
    leaf_min_entries: usize,
    index_min_children: usize,
    node_max_bytes: usize,
    split_headroom_bytes: usize,
}

/// A node size policy whose reserved headroom exceeds its hard node cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "split headroom ({split_headroom_bytes} bytes) exceeds the node hard cap ({node_max_bytes} bytes)"
)]
pub struct InvalidNodeSizePolicy {
    node_max_bytes: usize,
    split_headroom_bytes: usize,
}

impl NodeSizePolicy {
    /// Starts building a policy from the production defaults.
    pub fn builder() -> NodeSizePolicyBuilder {
        NodeSizePolicyBuilder::default()
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

    /// Minimum live leaf entries below which a leaf is a merge candidate.
    pub fn leaf_min_entries(&self) -> usize {
        self.leaf_min_entries
    }

    /// Minimum index children below which an index is a merge candidate.
    pub fn index_min_children(&self) -> usize {
        self.index_min_children
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
    pub fn entry_fits_split_budget(&self, entry: &LeafEntry) -> bool {
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

impl NodeSizePolicyBuilder {
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

    /// Sets the live leaf entry count below which a merge is requested. Zero
    /// disables leaf merges.
    pub fn leaf_min_entries(mut self, value: usize) -> Self {
        self.leaf_min_entries = value;
        self
    }

    /// Sets the index fan-out below which a merge is requested. Zero disables
    /// index merges.
    pub fn index_min_children(mut self, value: usize) -> Self {
        self.index_min_children = value;
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
    pub fn build(self) -> Result<NodeSizePolicy, InvalidNodeSizePolicy> {
        if self.split_headroom_bytes > self.node_max_bytes {
            return Err(InvalidNodeSizePolicy {
                node_max_bytes: self.node_max_bytes,
                split_headroom_bytes: self.split_headroom_bytes,
            });
        }
        Ok(NodeSizePolicy {
            leaf_max_entries: self.leaf_max_entries,
            node_soft_max_bytes: self.node_soft_max_bytes,
            index_max_children: self.index_max_children,
            leaf_min_entries: self.leaf_min_entries,
            index_min_children: self.index_min_children,
            node_max_bytes: self.node_max_bytes,
            split_headroom_bytes: self.split_headroom_bytes,
        })
    }
}

impl Default for NodeSizePolicyBuilder {
    fn default() -> Self {
        Self {
            leaf_max_entries: 256,
            node_soft_max_bytes: 256 * 1024,
            index_max_children: 256,
            leaf_min_entries: 64,
            index_min_children: 64,
            node_max_bytes: 1024 * 1024,
            split_headroom_bytes: 64 * 1024,
        }
    }
}

impl Default for NodeSizePolicy {
    fn default() -> Self {
        // A ~256-entry leaf soft cap mirrors the old fixed keys-per-leaf target
        // (ADR-017), and keeps each object small for the backend.
        NodeSizePolicyBuilder::default()
            .build()
            .expect("default node size policy is valid")
    }
}

/// The node-level coordination state threaded through a leaf CAS round.
///
/// Keeping this separate from the node's topology prevents transaction-engine
/// member policies from replacing bounds, sibling links, or the node body while they
/// only intend to change locks.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeLocks {
    structure: ExclusiveGate,
    membership: SharedExclusiveLock,
    membership_generation: u64,
    drop_intent: Option<TxId>,
    merge_reservation: Option<StructuralIntentId>,
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
    pub fn membership_generation(&self) -> u64 {
        self.membership_generation
    }

    /// Records one logical membership change without installing a holder.
    ///
    /// Direct commits have no prepare/release lock lifecycle, so their commit
    /// CAS advances the scan-validation generation directly (ADR-061).
    pub fn advance_membership_generation(&mut self) {
        self.membership_generation = self.membership_generation.wrapping_add(1);
    }

    /// Returns the transaction preparing a drop of the containing collection.
    pub fn drop_intent(&self) -> Option<&TxId> {
        self.drop_intent.as_ref()
    }

    /// Installs the drop intent owned by `id`.
    pub fn set_drop_intent(&mut self, id: TxId) {
        self.drop_intent = Some(id);
    }

    /// Removes the drop intent when it is owned by `id`.
    pub fn remove_drop_intent(&mut self, id: &TxId) -> bool {
        if self.drop_intent.as_ref() != Some(id) {
            return false;
        }
        self.drop_intent = None;
        true
    }

    /// Closes the structural gate for one structural operation.
    pub fn set_structural_gate(&mut self, id: TxId) {
        if self.structure.holders() == std::slice::from_ref(&id) {
            return;
        }
        self.structure.set_writer(id);
        // The node state from before the gate must never come back, so that a
        // late copy of this CAS cannot land after a recovery fence (ADR-073).
        self.advance_membership_generation();
    }

    /// Opens the structural gate when held by `id`.
    pub fn remove_structural_gate(&mut self, id: &TxId) -> bool {
        self.structure.remove(id)
    }

    /// Returns the structural intent of a merge into this node that can still
    /// land or be abandoned.
    pub fn merge_reservation(&self) -> Option<&StructuralIntentId> {
        self.merge_reservation.as_ref()
    }

    /// Installs the merge reservation of `intent`.
    pub fn set_merge_reservation(&mut self, intent: StructuralIntentId) {
        if self.merge_reservation.as_ref() == Some(&intent) {
            return;
        }
        self.merge_reservation = Some(intent);
        // Same fence as a structural gate installation.
        self.advance_membership_generation();
    }

    /// Removes the merge reservation when it names `intent`.
    pub fn remove_merge_reservation(&mut self, intent: &StructuralIntentId) -> bool {
        if self.merge_reservation.as_ref() != Some(intent) {
            return false;
        }
        self.merge_reservation = None;
        true
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
        self.membership_generation = self.membership_generation.wrapping_add(1);
    }

    /// Removes one membership holder and records released write activity.
    pub fn remove_membership_holder(&mut self, id: &TxId) -> bool {
        let was_writer =
            self.membership.lock_type() == LockType::Write && self.membership.contains(id);
        let removed = self.membership.remove(id);
        if removed && was_writer {
            self.membership_generation = self.membership_generation.wrapping_add(1);
        }
        removed
    }

    /// Removes the transaction's membership lock.
    ///
    /// Structural gates have a separate lifecycle and cannot be released by
    /// ordinary transaction cleanup.
    pub fn release_membership(&mut self, id: &TxId) -> bool {
        self.remove_membership_holder(id)
    }

    /// Clears transient holders while preserving the membership generation.
    fn clear_holders(&mut self) {
        self.structure.clear();
        self.membership.clear();
        self.merge_reservation = None;
    }
}

/// The body of a [`Node`]: either a leaf's per-key entries or an index's
/// separators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeBody {
    /// A leaf: the ADR-017 coordination entries for the node's key range.
    Leaf(LeafBody),
    /// An index: separator keys mapping ranges to child nodes.
    Index(IndexNode),
}

/// A decoded B-link tree node: a body plus the high-key and right-sibling that
/// make descent self-correcting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    /// Inclusive lower bound of the covered key range; empty for the first node
    /// at its level.
    low_key: Vec<u8>,
    /// Exclusive upper bound of the covered key range; `None` means +infinity.
    high_key: Option<Vec<u8>>,
    /// Right-sibling node ID at the same level; `None` means none (rightmost).
    right_sibling: Option<NodeId>,
    body: NodeBody,
    locks: NodeLocks,
    /// Set after a merge moved the range and entries into the right sibling.
    drained: bool,
}

impl Node {
    /// Creates a leaf node that covers the whole key space (high-key +infinity, no
    /// right sibling) from `leaf` — the shape of a brand-new root.
    pub fn leaf(leaf: LeafBody) -> Self {
        Node {
            low_key: Vec::new(),
            high_key: None,
            right_sibling: None,
            body: NodeBody::Leaf(leaf),
            locks: NodeLocks::default(),
            drained: false,
        }
    }

    /// Creates an index node that covers the whole key space from `index`.
    pub fn index(index: IndexNode) -> Self {
        Node {
            low_key: Vec::new(),
            high_key: None,
            right_sibling: None,
            body: NodeBody::Index(index),
            locks: NodeLocks::default(),
            drained: false,
        }
    }

    /// Returns the node with the given inclusive lower range bound.
    #[must_use]
    pub fn with_low_key(mut self, low_key: Vec<u8>) -> Self {
        self.low_key = low_key;
        self
    }

    /// Returns the node with the given exclusive upper range bound.
    #[must_use]
    pub fn with_high_key(mut self, high_key: Option<Vec<u8>>) -> Self {
        self.high_key = high_key;
        self
    }

    /// Returns the node with the given right-sibling link.
    #[must_use]
    pub fn with_right_sibling(mut self, right_sibling: Option<NodeId>) -> Self {
        self.right_sibling = right_sibling;
        self
    }

    /// The inclusive lower bound of the covered range; empty for the first node
    /// at its level.
    pub fn low_key(&self) -> &[u8] {
        &self.low_key
    }

    /// Replaces the inclusive lower range bound. Only a merge changes the low
    /// key of an existing node (ADR-073).
    pub fn set_low_key(&mut self, low_key: Vec<u8>) {
        self.low_key = low_key;
    }

    /// Reports whether this copy of the node is older than a merge that moved
    /// `key` into it, because its low key is above `key`. A reader must read the
    /// node again at a new currentness barrier (ADR-073).
    pub fn is_below_range(&self, key: &[u8]) -> bool {
        key < self.low_key.as_slice()
    }

    /// The exclusive upper bound of the covered range, or `None` for +infinity.
    pub fn high_key(&self) -> Option<&[u8]> {
        self.high_key.as_deref()
    }

    /// The right-sibling node ID, or `None` if this is the rightmost node at
    /// its level.
    pub fn right_sibling(&self) -> Option<NodeId> {
        self.right_sibling
    }

    /// The node body.
    pub fn body(&self) -> &NodeBody {
        &self.body
    }

    /// Reports whether a merge moved this node's range and entries into its
    /// right sibling.
    pub fn is_drained(&self) -> bool {
        self.drained
    }

    /// Empties the node and links it to the `target` that absorbed its entries,
    /// keeping its level.
    pub fn drain(&mut self, target: NodeId) {
        self.body = match self.body {
            NodeBody::Leaf(_) => NodeBody::Leaf(LeafBody::new()),
            NodeBody::Index(_) => NodeBody::Index(IndexNode::default()),
        };
        self.right_sibling = Some(target);
        self.locks.clear_holders();
        self.drained = true;
    }

    /// Adds the range and entries of `left`, the gated left sibling, to this
    /// node and reserves this node for the merge of `intent` (ADR-073). Fails
    /// if the two nodes are at different levels.
    pub fn absorb(&mut self, left: &Node, intent: StructuralIntentId) -> Result<(), StorageError> {
        self.body = match (&self.body, &left.body) {
            (NodeBody::Leaf(right), NodeBody::Leaf(left)) => NodeBody::Leaf(
                LeafBody::from_entries(left.entries().chain(right.entries()).cloned()),
            ),
            (NodeBody::Index(right), NodeBody::Index(left)) => NodeBody::Index(IndexNode {
                children: left
                    .children
                    .iter()
                    .chain(&right.children)
                    .map(|(separator, child)| (separator.clone(), *child))
                    .collect(),
            }),
            _ => return Err(StorageError::other("merge nodes are at different levels")),
        };
        self.low_key = left.low_key.clone();
        self.locks.set_merge_reservation(intent);
        // An absence read of the left range made while the left node was gated
        // recorded its generation, and stays valid because nothing changed.
        self.locks.membership_generation = self
            .locks
            .membership_generation
            .max(left.locks.membership_generation);
        Ok(())
    }

    /// Reverts the absorb of the merge of `intent`, after its drain can no
    /// longer land: restores `boundary` as the low key and removes the entries
    /// below it (ADR-073). Returns `false` and changes nothing if this node does
    /// not hold the merge reservation of `intent`.
    pub fn abandon_merge(&mut self, intent: &StructuralIntentId, boundary: &[u8]) -> bool {
        if !self.locks.remove_merge_reservation(intent) {
            return false;
        }
        match &mut self.body {
            NodeBody::Leaf(leaf) => {
                *leaf = LeafBody::from_entries(
                    leaf.entries()
                        .filter(|entry| entry.key.as_slice() >= boundary)
                        .cloned(),
                );
            }
            NodeBody::Index(index) => index
                .children
                .retain(|separator, _| separator.as_slice() >= boundary),
        }
        self.low_key = boundary.to_vec();
        true
    }

    /// Replaces the leaf body while preserving bounds and node coordination.
    pub fn set_leaf(&mut self, leaf: LeafBody) -> Result<(), StorageError> {
        if matches!(self.body, NodeBody::Leaf(_)) {
            self.body = NodeBody::Leaf(leaf);
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

    /// Returns the transaction preparing a drop of this node's collection.
    pub fn drop_intent(&self) -> Option<&TxId> {
        self.locks.drop_intent()
    }

    /// Installs a drop intent on this node.
    pub fn set_drop_intent(&mut self, id: TxId) {
        self.locks.set_drop_intent(id);
    }

    /// Clears a drop intent owned by `id`.
    pub fn remove_drop_intent(&mut self, id: &TxId) -> bool {
        self.locks.remove_drop_intent(id)
    }

    /// Returns the complete node-level coordination state.
    pub fn locks(&self) -> &NodeLocks {
        &self.locks
    }

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

    /// Removes the merge reservation when it names `intent`.
    pub fn remove_merge_reservation(&mut self, intent: &StructuralIntentId) -> bool {
        self.locks.remove_merge_reservation(intent)
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
    pub fn membership_generation(&self) -> u64 {
        self.locks.membership_generation()
    }

    /// Returns the canonical encoded size without transient node locks.
    pub fn content_encoded_len(&self) -> usize {
        let mut content = self.clone();
        content.clear_node_locks();
        content.encoded_len()
    }

    /// Returns the exact node-content size of a leaf containing only `entry`.
    pub fn leaf_entry_content_encoded_len(entry: &LeafEntry) -> usize {
        let leaf_len = length_delimited_field(LEAF_ENTRIES_TAG, entry.encoded_len());
        length_delimited_field(NODE_LEAF_TAG, leaf_len)
    }

    /// Returns the node-content size of a leaf containing the largest fixed
    /// coordination shape GlassDB can add for a key of `key_len` bytes.
    pub fn worst_case_leaf_entry_len(key_len: usize) -> usize {
        let leaf_len =
            length_delimited_field(LEAF_ENTRIES_TAG, LeafEntry::worst_case_encoded_len(key_len));
        length_delimited_field(NODE_LEAF_TAG, leaf_len)
    }

    /// Returns the node-content size of the smallest parent that can contain a
    /// separator of `key_len` bytes.
    pub fn worst_case_parent_separator_len(key_len: usize) -> usize {
        let child_len = length_delimited_field(INDEX_CHILD_TAG, ID_BYTES);
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
    pub fn as_leaf(&self) -> Option<&LeafBody> {
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

    /// Reports whether the node still covers `key`, i.e. the node is live and
    /// `key` is below the high-key. A `false` result means a split or a merge
    /// has moved `key` to the right and the descent must follow the
    /// right-sibling link (the B-link property).
    pub fn covers(&self, key: &[u8]) -> bool {
        if self.drained {
            return false;
        }
        match &self.high_key {
            None => true,
            Some(hk) => key < hk.as_slice(),
        }
    }

    /// Reports whether the node is over any of `policy`'s soft caps, making it a
    /// background split candidate (ADR-031). A node with fewer than two
    /// entries/children can never be split, so it is never a candidate however
    /// large a single entry is (single-hot-key relief is out of scope).
    pub fn over_soft_cap(&self, policy: &NodeSizePolicy) -> bool {
        match &self.body {
            NodeBody::Leaf(leaf) => {
                leaf.len() >= 2
                    && (leaf.len() > policy.leaf_max_entries()
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
    /// `self` (bounded above by the split key and linked to `right_id`) and
    /// returns the newly created right sibling — which inherits `self`'s former
    /// high-key and right-sibling — together with the split key to promote into
    /// the parent. Returns `None` when the node is too small to divide (fewer
    /// than two entries/children), so a caller never produces an empty node.
    ///
    /// This is a pure in-memory transform; persisting the two nodes (create the
    /// sibling, then CAS the shrunk source — the linearization point) is the
    /// caller's multi-step protocol.
    pub fn split(&mut self, right_id: NodeId) -> Option<(Node, Vec<u8>)> {
        let (right_body, split_key) = match &mut self.body {
            NodeBody::Leaf(leaf) => {
                if leaf.len() < 2 {
                    return None;
                }
                let (upper, split_key) = leaf.split_off_median();
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
            low_key: split_key.clone(),
            high_key: self.high_key.take(),
            right_sibling: self.right_sibling.take(),
            body: right_body,
            locks: {
                let mut locks = self.locks.clone();
                locks.clear_holders();
                locks
            },
            drained: false,
        };
        // The retained lower half is now bounded by the split key and links to
        // the new sibling.
        self.high_key = Some(split_key.clone());
        self.right_sibling = Some(right_id);
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
            NodeBody::Leaf(leaf) => pb::node::Body::Leaf(leaf.to_pb()),
            NodeBody::Index(index) => pb::node::Body::Index(index.to_pb()),
        };
        pb::Node {
            high_key: self.high_key.clone().unwrap_or_default(),
            right_sibling: self
                .right_sibling
                .map(|id| id.as_bytes().to_vec())
                .unwrap_or_default(),
            body: Some(body),
            structural_gate: (!self.locks.structure.is_empty())
                .then(|| self.locks.structure.to_pb()),
            membership_lock: (!self.locks.membership.is_empty())
                .then(|| self.locks.membership.to_pb()),
            membership_generation: self.locks.membership_generation,
            drop_intent: self
                .locks
                .drop_intent
                .as_ref()
                .map(|id| id.as_bytes().to_vec())
                .unwrap_or_default(),
            drained: self.drained,
            merge_reservation: self
                .locks
                .merge_reservation
                .map(|intent| intent.as_bytes().to_vec())
                .unwrap_or_default(),
            low_key: self.low_key.clone(),
        }
    }

    pub(crate) fn from_pb(raw: pb::Node) -> Result<Self, StorageError> {
        let body = match raw.body {
            Some(pb::node::Body::Index(index)) => NodeBody::Index(IndexNode::from_pb(index)?),
            Some(pb::node::Body::Leaf(leaf)) => NodeBody::Leaf(LeafBody::from_pb(leaf)?),
            None => NodeBody::Leaf(LeafBody::new()),
        };
        let structure = ExclusiveGate::from_pb(raw.structural_gate).map_err(|_| {
            StorageError::other("node structural gate must be empty or have one write holder")
        })?;
        let membership = SharedExclusiveLock::from_pb(raw.membership_lock)
            .map_err(|_| StorageError::other("node has invalid membership lock"))?;
        let drop_intent = (!raw.drop_intent.is_empty()).then(|| TxId::from_bytes(raw.drop_intent));
        let right_sibling = if raw.right_sibling.is_empty() {
            None
        } else {
            Some(
                NodeId::from_slice(&raw.right_sibling)
                    .ok_or_else(|| StorageError::other("node has an invalid right-sibling ID"))?,
            )
        };
        let merge_reservation = if raw.merge_reservation.is_empty() {
            None
        } else {
            Some(
                StructuralIntentId::from_slice(&raw.merge_reservation)
                    .ok_or_else(|| StorageError::other("node has an invalid merge reservation"))?,
            )
        };
        let body_is_empty = match &body {
            NodeBody::Leaf(leaf) => leaf.is_empty(),
            NodeBody::Index(index) => index.is_empty(),
        };
        if raw.drained && (!body_is_empty || right_sibling.is_none()) {
            return Err(StorageError::other(
                "drained node must have an empty body and a right sibling",
            ));
        }
        Ok(Node {
            low_key: raw.low_key,
            high_key: (!raw.high_key.is_empty()).then_some(raw.high_key),
            right_sibling,
            body,
            locks: NodeLocks {
                structure,
                membership,
                membership_generation: raw.membership_generation,
                drop_intent,
                merge_reservation,
            },
            drained: raw.drained,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use glassdb_data::TxId;

    use crate::leaf::{CurrentState, LeafEntry};

    /// A node ID that shows `name` in its bytes, so tests stay readable.
    fn id(name: &str) -> NodeId {
        let mut bytes = [0; ID_BYTES];
        bytes[..name.len()].copy_from_slice(name.as_bytes());
        NodeId::from_bytes(bytes)
    }

    fn intent_id(byte: u8) -> StructuralIntentId {
        StructuralIntentId::from_bytes([byte; ID_BYTES])
    }

    fn entry(key: &[u8], writer: u8) -> LeafEntry {
        LeafEntry::new(key).with_current(CurrentState::External {
            writer: TxId::from_bytes(vec![writer]),
        })
    }

    fn golden_entry() -> LeafEntry {
        let mut entry = LeafEntry::new(b"Hello").with_current(CurrentState::External {
            writer: TxId::from_bytes(vec![0xaa, 0xbb]),
        });
        entry.replace_write_lock(TxId::from_bytes(vec![1, 2, 3, 4]));
        entry
    }

    #[test]
    fn leaf_round_trip_preserves_bounds() {
        let node = Node::leaf(LeafBody::from_entries([
            entry(b"apple", 1),
            entry(b"cat", 2),
        ]))
        .with_low_key(b"a".to_vec())
        .with_high_key(Some(b"m".to_vec()))
        .with_right_sibling(Some(id("sib")));

        let decoded = Node::decode(&node.encode()).unwrap();
        assert_eq!(decoded, node);
        assert_eq!(decoded.low_key(), b"a");
        assert_eq!(decoded.high_key(), Some(b"m".as_slice()));
        assert_eq!(decoded.right_sibling(), Some(id("sib")));
        assert!(decoded.as_leaf().is_some());
    }

    #[test]
    fn round_trip_preserves_node_locks_and_membership_generation() {
        let gate = TxId::from_bytes(vec![2]);
        let writer = TxId::from_bytes(vec![1]);
        let mut node = Node::leaf(LeafBody::new());
        node.set_structural_gate(gate.clone());
        node.set_membership_writer(writer.clone());

        let decoded = Node::decode(&node.encode()).unwrap();
        assert_eq!(decoded.structural_gate().holders(), &[gate]);
        assert_eq!(decoded.membership_lock().holders(), &[writer]);
        assert_eq!(decoded.membership_generation(), 2);
    }

    #[test]
    fn round_trip_preserves_merge_fields() {
        let intent = intent_id(3);
        let mut target = Node::leaf(LeafBody::from_entries([entry(b"a", 1)]));
        let mut locks = target.locks().clone();
        locks.set_merge_reservation(intent);
        target.set_locks(locks);
        let decoded = Node::decode(&target.encode()).unwrap();
        assert_eq!(decoded, target);
        assert_eq!(decoded.locks().merge_reservation(), Some(&intent));

        let mut source = Node::index(IndexNode::from_children([(b"".to_vec(), id("L0"))]))
            .with_high_key(Some(b"m".to_vec()));
        source.drain(id("target"));
        let decoded = Node::decode(&source.encode()).unwrap();
        assert_eq!(decoded, source);
        assert!(decoded.is_drained());
    }

    #[test]
    fn drain_keeps_level_bounds_and_generation() {
        let gate = TxId::from_bytes(vec![2]);
        let mut source = Node::leaf(LeafBody::from_entries([entry(b"a", 1)]))
            .with_high_key(Some(b"m".to_vec()))
            .with_right_sibling(Some(id("drained")));
        source.set_structural_gate(gate);
        let generation = source.membership_generation();

        source.drain(id("target"));

        assert!(source.as_leaf().is_some_and(LeafBody::is_empty));
        assert_eq!(source.high_key(), Some(b"m".as_slice()));
        assert_eq!(source.right_sibling(), Some(id("target")));
        assert!(source.structural_gate().is_empty());
        assert_eq!(source.membership_generation(), generation);
        assert!(!source.covers(b"a"));
        assert!(source.split(id("sibling")).is_none());
    }

    #[test]
    fn absorb_takes_the_left_range_and_abandon_restores_the_target() {
        let intent = intent_id(3);
        let mut left = Node::leaf(LeafBody::from_entries([entry(b"b", 1)]))
            .with_low_key(b"a".to_vec())
            .with_high_key(Some(b"m".to_vec()));
        for _ in 0..5 {
            left.locks.advance_membership_generation();
        }
        let original = Node::leaf(LeafBody::from_entries([entry(b"n", 2)]))
            .with_low_key(b"m".to_vec())
            .with_high_key(Some(b"t".to_vec()));
        let mut right = original.clone();

        right.absorb(&left, intent).unwrap();

        let keys: Vec<_> = right
            .as_leaf()
            .unwrap()
            .entries()
            .map(|e| e.key.clone())
            .collect();
        assert_eq!(keys, [b"b".to_vec(), b"n".to_vec()]);
        assert_eq!(right.low_key(), b"a");
        assert_eq!(right.high_key(), Some(b"t".as_slice()));
        assert_eq!(right.locks().merge_reservation(), Some(&intent));
        // max(g_L, g_R + 1): absence reads of the gated left range stay valid.
        assert_eq!(right.membership_generation(), 5);

        let other = intent_id(4);
        assert!(!right.clone().abandon_merge(&other, b"m"));
        assert!(right.abandon_merge(&intent, b"m"));
        assert_eq!(right.as_leaf(), original.as_leaf());
        assert_eq!(right.low_key(), b"m");
        assert_eq!(right.locks().merge_reservation(), None);
        assert_eq!(right.membership_generation(), 5);
    }

    #[test]
    fn absorb_joins_index_children_at_one_level() {
        let intent = intent_id(3);
        let left = Node::index(IndexNode::from_children([(b"".to_vec(), id("A"))]))
            .with_high_key(Some(b"m".to_vec()));
        let mut right = Node::index(IndexNode::from_children([(b"m".to_vec(), id("B"))]))
            .with_low_key(b"m".to_vec());

        right.absorb(&left, intent).unwrap();
        let children: Vec<_> = right.as_index().unwrap().children().collect();
        assert_eq!(
            children,
            [(b"".as_slice(), id("A")), (b"m".as_slice(), id("B"))]
        );
        assert_eq!(right.as_index().unwrap().child_for(b"c"), Some(id("A")));
        assert_eq!(right.membership_generation(), 1);

        let error = Node::leaf(LeafBody::new())
            .absorb(&left, intent)
            .unwrap_err();
        assert_eq!(error.to_string(), "merge nodes are at different levels");
    }

    #[test]
    fn decode_rejects_drained_nodes_with_content_or_without_target() {
        let with_entries = pb::Node {
            right_sibling: id("target").as_bytes().to_vec(),
            body: Some(pb::node::Body::Leaf(
                LeafBody::from_entries([entry(b"a", 1)]).to_pb(),
            )),
            drained: true,
            ..pb::Node::default()
        };
        let without_target = pb::Node {
            drained: true,
            ..pb::Node::default()
        };
        for raw in [with_entries, without_target] {
            let error = Node::decode(&raw.encode_to_vec()).unwrap_err();
            assert_eq!(
                error.to_string(),
                "drained node must have an empty body and a right sibling"
            );
        }
    }

    #[test]
    fn decode_rejects_node_ids_that_are_not_16_bytes() {
        // IDs of the older string format have 22 bytes.
        for bad in [vec![7; 15], vec![7; 17], b"0000000000000000000000".to_vec()] {
            let index = pb::Node {
                body: Some(pb::node::Body::Index(pb::IndexNode {
                    entries: vec![pb::IndexEntry {
                        separator_key: Vec::new(),
                        child: bad.clone(),
                    }],
                })),
                ..pb::Node::default()
            };
            let sibling = pb::Node {
                right_sibling: bad.clone(),
                ..pb::Node::default()
            };
            let reservation = pb::Node {
                merge_reservation: bad,
                ..pb::Node::default()
            };
            for (raw, want) in [
                (index, "index node has an invalid child ID"),
                (sibling, "node has an invalid right-sibling ID"),
                (reservation, "node has an invalid merge reservation"),
            ] {
                let error = Node::decode(&raw.encode_to_vec()).unwrap_err();
                assert_eq!(error.to_string(), want);
            }
        }

        let empty_child = pb::Node {
            body: Some(pb::node::Body::Index(pb::IndexNode {
                entries: vec![pb::IndexEntry::default()],
            })),
            ..pb::Node::default()
        };
        let error = Node::decode(&empty_child.encode_to_vec()).unwrap_err();
        assert_eq!(error.to_string(), "index node has an invalid child ID");
    }

    #[test]
    fn installations_advance_the_generation_once() {
        let gate = TxId::from_bytes(vec![2]);
        let intent = intent_id(3);
        let mut locks = NodeLocks::default();

        locks.set_structural_gate(gate.clone());
        locks.set_structural_gate(gate.clone());
        assert_eq!(locks.membership_generation(), 1);
        locks.set_merge_reservation(intent);
        locks.set_merge_reservation(intent);
        assert_eq!(locks.membership_generation(), 2);

        assert!(locks.remove_structural_gate(&gate));
        assert!(locks.remove_merge_reservation(&intent));
        assert!(!locks.remove_merge_reservation(&intent));
        assert_eq!(locks.membership_generation(), 2);
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
                structural_gate: Some(gate),
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
    fn membership_generation_tracks_write_lock_activity() {
        let id = TxId::from_bytes(vec![1]);
        let mut node = Node::leaf(LeafBody::new());

        node.add_membership_reader(id.clone());
        assert_eq!(node.membership_generation(), 0);
        assert!(node.remove_membership_holder(&id));
        assert_eq!(node.membership_generation(), 0);

        node.set_membership_writer(id.clone());
        assert_eq!(node.membership_generation(), 1);
        node.set_membership_writer(id.clone());
        assert_eq!(node.membership_generation(), 1);
        assert!(node.remove_membership_holder(&id));
        assert_eq!(node.membership_generation(), 2);
        assert!(!node.remove_membership_holder(&id));
        assert_eq!(node.membership_generation(), 2);

        node.locks.advance_membership_generation();
        assert_eq!(node.membership_generation(), 3);

        node.locks.membership_generation = u64::MAX;
        node.set_membership_writer(id);
        assert_eq!(node.membership_generation(), 0);
    }

    #[test]
    fn index_round_trip_and_child_lookup() {
        let index = IndexNode::from_children([
            (b"".to_vec(), id("L0")),
            (b"f".to_vec(), id("L1")),
            (b"m".to_vec(), id("L2")),
        ]);
        let node = Node::index(index);
        let decoded = Node::decode(&node.encode()).unwrap();
        assert_eq!(decoded, node);

        let idx = decoded.as_index().unwrap();
        // The routed child is the greatest separator not exceeding the key.
        assert_eq!(idx.child_for(b"apple"), Some(id("L0")));
        assert_eq!(idx.child_for(b"f"), Some(id("L1")));
        assert_eq!(idx.child_for(b"kiwi"), Some(id("L1")));
        assert_eq!(idx.child_for(b"mango"), Some(id("L2")));
        // The child for keys just below a separator is the one before it.
        assert_eq!(idx.child_before(b"f"), Some(id("L0")));
        assert_eq!(idx.child_before(b"kiwi"), Some(id("L1")));
        assert_eq!(idx.child_before(b""), Some(id("L0")));
    }

    fn linked(high_key: &[u8], right: &str) -> Node {
        Node::leaf(LeafBody::new())
            .with_high_key(Some(high_key.to_vec()))
            .with_right_sibling(Some(id(right)))
    }

    fn drained(high_key: &[u8], target: &str) -> Node {
        let mut node = linked(high_key, "unused");
        node.drain(id(target));
        node
    }

    fn index(children: &[(&[u8], &str)]) -> IndexNode {
        IndexNode::from_children(
            children
                .iter()
                .map(|(separator, child)| (separator.to_vec(), id(child))),
        )
    }

    #[test]
    fn reconcile_adds_the_separators_of_every_live_child_on_the_path() {
        let (l0, l1, l4) = (
            linked(b"m", "L1"),
            linked(b"t", "L4"),
            Node::leaf(LeafBody::new()),
        );
        let path = [(id("L0"), &l0), (id("L1"), &l1), (id("L4"), &l4)];
        let mut parent = index(&[(b"", "L0")]);

        parent.reconcile(&path);
        let reconciled = index(&[(b"", "L0"), (b"m", "L1"), (b"t", "L4")]);
        assert_eq!(parent, reconciled);
        parent.reconcile(&path);
        assert_eq!(parent, reconciled, "reconciliation is idempotent");
    }

    #[test]
    fn reconcile_routes_the_range_of_a_drained_child_to_its_merge_target() {
        let (source, target) = (drained(b"m", "R"), linked(b"t", "S"));
        let mut parent = index(&[(b"", "A"), (b"f", "L"), (b"m", "R"), (b"t", "S")]);

        parent.reconcile(&[(id("L"), &source), (id("R"), &target)]);
        assert_eq!(parent, index(&[(b"", "A"), (b"f", "R"), (b"t", "S")]));
    }

    #[test]
    fn reconcile_handles_a_merge_and_an_unpublished_split_on_one_path() {
        let (source, target, split) = (
            drained(b"m", "R"),
            linked(b"t", "S"),
            Node::leaf(LeafBody::new()),
        );
        let mut parent = index(&[(b"", "L"), (b"m", "R")]);

        parent.reconcile(&[(id("L"), &source), (id("R"), &target), (id("S"), &split)]);
        assert_eq!(parent, index(&[(b"", "R"), (b"t", "S")]));
    }

    #[test]
    fn leaf_split_moves_upper_half_and_relinks() {
        // A leaf with an existing high-key and right-sibling splits: the new
        // sibling inherits both bounds, the source is rebounded to the split key
        // and linked to the sibling.
        let mut src = Node::leaf(LeafBody::from_entries([
            entry(b"apple", 1),
            entry(b"cat", 2),
            entry(b"mango", 3),
            entry(b"pear", 4),
        ]))
        .with_low_key(b"ant".to_vec())
        .with_high_key(Some(b"tiger".to_vec()))
        .with_right_sibling(Some(id("oldRight")));

        let (right, split_key) = src.split(id("newRight")).expect("splittable");
        assert_eq!(split_key, b"mango");
        assert_eq!(src.low_key(), b"ant");
        assert_eq!(right.low_key(), b"mango");

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
        assert_eq!(src.right_sibling(), Some(id("newRight")));

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
        assert_eq!(right.right_sibling(), Some(id("oldRight")));
    }

    #[test]
    fn leaf_split_preserves_membership_generation_in_both_outputs() {
        let mut src = Node::leaf(LeafBody::from_entries([
            entry(b"a", 1),
            entry(b"b", 2),
            entry(b"c", 3),
            entry(b"d", 4),
        ]));
        let mut locks = src.locks().clone();
        locks.advance_membership_generation();
        locks.advance_membership_generation();
        src.set_locks(locks);

        let (right, _) = src.split(id("newRight")).expect("splittable");
        assert_eq!(src.membership_generation(), 2);
        assert_eq!(right.membership_generation(), 2);
    }

    #[test]
    fn index_split_promotes_separator_and_relinks() {
        let mut src = Node::index(IndexNode::from_children([
            (b"".to_vec(), id("L0")),
            (b"f".to_vec(), id("L1")),
            (b"m".to_vec(), id("L2")),
            (b"t".to_vec(), id("L3")),
        ]));
        let (right, sep) = src.split(id("newRight")).expect("splittable");
        assert_eq!(
            sep, b"m",
            "promoted separator is the right half's low bound"
        );

        let left_seps: Vec<&[u8]> = src.as_index().unwrap().children().map(|(s, _)| s).collect();
        assert_eq!(left_seps, vec![b"".as_slice(), b"f"]);
        assert_eq!(src.high_key(), Some(b"m".as_slice()));
        assert_eq!(src.right_sibling(), Some(id("newRight")));

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
            Node::leaf(LeafBody::from_entries([entry(b"only", 1)]))
                .split(id("r"))
                .is_none()
        );
        assert!(Node::leaf(LeafBody::new()).split(id("r")).is_none());
        let one_child = Node::index(IndexNode::from_children([(b"".to_vec(), id("L0"))]));
        assert!(one_child.clone().split(id("r")).is_none());
    }

    #[test]
    fn over_soft_cap_respects_policy_and_min_size() {
        let tiny = NodeSizePolicy::builder()
            .leaf_max_entries(2)
            .node_soft_max_bytes(1 << 20)
            .index_max_children(2)
            .build()
            .unwrap();
        let two = Node::leaf(LeafBody::from_entries([entry(b"a", 1), entry(b"b", 2)]));
        assert!(!two.over_soft_cap(&tiny), "at the cap is not over it");
        let three = Node::leaf(LeafBody::from_entries([
            entry(b"a", 1),
            entry(b"b", 2),
            entry(b"c", 3),
        ]));
        assert!(three.over_soft_cap(&tiny));
        let two_index = Node::index(IndexNode::from_children([
            (b"".to_vec(), id("L0")),
            (b"m".to_vec(), id("L1")),
        ]));
        assert!(
            !two_index.over_soft_cap(&tiny),
            "index at the child cap is not over it"
        );
        let three_index = Node::index(IndexNode::from_children([
            (b"".to_vec(), id("L0")),
            (b"m".to_vec(), id("L1")),
            (b"t".to_vec(), id("L2")),
        ]));
        assert!(three_index.over_soft_cap(&tiny));

        // A single oversized entry is never a candidate: it cannot be split.
        let byte_policy = NodeSizePolicy::builder()
            .leaf_max_entries(1000)
            .node_soft_max_bytes(1)
            .index_max_children(1000)
            .build()
            .unwrap();
        assert!(
            !Node::leaf(LeafBody::from_entries([entry(b"solo", 1)])).over_soft_cap(&byte_policy)
        );
        for (kind, node) in [("leaf", two), ("index", two_index)] {
            let at_limit = NodeSizePolicy::builder()
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
                    &NodeSizePolicy::builder()
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
        let exact_headroom = NodeSizePolicy::builder()
            .node_max_bytes(128)
            .split_headroom_bytes(128)
            .build()
            .unwrap();
        assert_eq!(exact_headroom.content_limit(), 0);
        assert!(
            NodeSizePolicy::builder()
                .node_max_bytes(128)
                .split_headroom_bytes(129)
                .build()
                .is_err()
        );

        let entry = entry(b"boundary", 1);
        let entry_len = Node::leaf(LeafBody::from_entries([entry.clone()])).content_encoded_len();
        let admitting = NodeSizePolicy::builder()
            .node_max_bytes(entry_len * 2)
            .split_headroom_bytes(0)
            .build()
            .unwrap();
        assert!(admitting.entry_fits_split_budget(&entry));

        let rejecting = NodeSizePolicy::builder()
            .node_max_bytes(entry_len * 2 - 1)
            .split_headroom_bytes(0)
            .build()
            .unwrap();
        assert!(!rejecting.entry_fits_split_budget(&entry));
    }

    #[test]
    fn maximum_key_admission_matches_real_nodes_at_the_exact_limit() {
        let maximum_key = vec![b'k'; 128];
        let writer = TxId::with_priority(7, b"maximum");
        let mut entry = LeafEntry::new(maximum_key.clone()).with_current(CurrentState::External {
            writer: writer.clone(),
        });
        entry.replace_write_lock(writer);
        let leaf = Node::leaf(LeafBody::from_entries([entry]));

        let parent = Node::index(IndexNode::from_children([
            (Vec::new(), id("L0")),
            (maximum_key.clone(), id("L1")),
        ]));
        assert_eq!(parent.as_index().unwrap().len(), 2);

        let leaf_requirement = leaf
            .content_encoded_len()
            .checked_mul(2)
            .expect("test leaf size fits usize");
        let required_limit = leaf_requirement.max(parent.content_encoded_len());
        let headroom = 17;
        let exact = NodeSizePolicy::builder()
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
        let parent_exact = NodeSizePolicy::builder()
            .node_max_bytes(parent_limit)
            .split_headroom_bytes(0)
            .build()
            .unwrap();
        assert!(parent_exact.parent_separator_fits(&maximum_key));
        assert!(
            !NodeSizePolicy::builder()
                .node_max_bytes(parent_limit - 1)
                .split_headroom_bytes(0)
                .build()
                .unwrap()
                .parent_separator_fits(&maximum_key)
        );

        let one_byte_over = NodeSizePolicy::builder()
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
    fn covers_reflects_high_key() {
        let plus_inf = Node::leaf(LeafBody::new());
        assert!(plus_inf.covers(b"anything"));

        let bounded = Node::leaf(LeafBody::new()).with_high_key(Some(b"m".to_vec()));
        assert!(bounded.covers(b"apple"));
        // The high-key is an exclusive upper bound.
        assert!(!bounded.covers(b"m"));
        assert!(!bounded.covers(b"zebra"));

        let mut drained = Node::leaf(LeafBody::new());
        drained.drain(id("target"));
        assert!(!drained.covers(b""));
        assert!(!drained.covers(b"anything"));
    }

    #[test]
    fn a_key_below_the_low_key_marks_a_copy_older_than_a_merge() {
        let node = Node::leaf(LeafBody::new()).with_low_key(b"f".to_vec());
        assert!(node.is_below_range(b"a"));
        assert!(!node.is_below_range(b"f"));
        assert!(!node.is_below_range(b"z"));
        assert!(!Node::leaf(LeafBody::new()).is_below_range(b""));
    }

    #[test]
    fn encoding_is_canonical_regardless_of_input_order() {
        let a = Node::index(IndexNode::from_children([
            (b"m".to_vec(), id("L2")),
            (b"".to_vec(), id("L0")),
            (b"f".to_vec(), id("L1")),
        ]));
        let b = Node::index(IndexNode::from_children([
            (b"".to_vec(), id("L0")),
            (b"f".to_vec(), id("L1")),
            (b"m".to_vec(), id("L2")),
        ]));
        assert_eq!(a.encode(), b.encode());
    }

    #[test]
    fn codec_size_predictions_match_varint_boundaries() {
        let writer = TxId::from_bytes(vec![0; TxId::MAX_GENERATED_ENCODED_LEN]);
        let child = id("child");

        for key_len in [
            0, 1, 81, 82, 83, 84, 127, 128, 16_335, 16_336, 16_338, 16_339, 16_383, 16_384,
        ] {
            let mut entry =
                LeafEntry::new(vec![b'k'; key_len]).with_current(CurrentState::External {
                    writer: writer.clone(),
                });
            entry.replace_write_lock(writer.clone());
            let actual = Node::leaf(LeafBody::from_entries([entry.clone()])).content_encoded_len();

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
            0, 1, 85, 86, 107, 108, 127, 128, 16_339, 16_340, 16_362, 16_363, 16_383, 16_384,
        ] {
            let actual = Node::index(IndexNode::from_children([
                (Vec::new(), child),
                (vec![b'k'; key_len], child),
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
        assert!(node.as_leaf().is_some_and(LeafBody::is_empty));
        assert_eq!(node.high_key(), None);
        assert_eq!(node.right_sibling(), None);
    }

    // Golden vectors: a fixed node must always encode to these exact bytes.
    // Changing the on-disk format must break these tests.
    #[test]
    fn golden_leaf_encoding() {
        let node = Node::leaf(LeafBody::from_entries([golden_entry()]))
            .with_high_key(Some(b"m".to_vec()))
            .with_right_sibling(Some(NodeId::from_bytes([7; 16])));
        let got = node.encode();
        let want = [
            [0x0a, 0x01, 0x6d, 0x12, 0x10].as_slice(),
            &[7; 16],
            &[
                0x1a, 0x19, 0x0a, 0x17, 0x0a, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x10, 0x03, 0x1a,
                0x04, 0x01, 0x02, 0x03, 0x04, 0x22, 0x06, 0x0a, 0x02, 0xaa, 0xbb, 0x10, 0x01,
            ],
        ]
        .concat();
        assert_eq!(node.encoded_len(), got.len());
        assert_eq!(got, want, "leaf node encoding drifted: {got:02x?}");
    }

    // ADR-043 lets a late conditional write land when its expected revision comes
    // back, and content-based revisions come back with the same bytes. Removing
    // a gate or reservation must therefore never restore the earlier encoding.
    #[test]
    fn released_installations_never_restore_the_earlier_encoding() {
        let never_locked = Node::leaf(LeafBody::from_entries([entry(b"a", 1)]));
        let mut advanced = never_locked.clone();
        let mut locks = advanced.locks().clone();
        locks.advance_membership_generation();
        advanced.set_locks(locks);

        let holder = TxId::from_bytes(vec![0x11]);
        let mut released_gate = never_locked.clone();
        released_gate.set_structural_gate(holder.clone());
        assert!(released_gate.remove_structural_gate(&holder));

        let intent = intent_id(3);
        let mut released_reservation = never_locked.clone();
        let mut locks = released_reservation.locks().clone();
        locks.set_merge_reservation(intent);
        assert!(locks.remove_merge_reservation(&intent));
        released_reservation.set_locks(locks);

        for released in [released_gate, released_reservation] {
            assert_ne!(released.encode(), never_locked.encode());
            // Only the generation remains of the released installation.
            assert_eq!(released.encode(), advanced.encode());
        }
    }

    // Golden vector for the ADR-032 node-lock fields. Changing their tags,
    // lock-type values, holder encoding, or membership-generation encoding must
    // break this test.
    #[test]
    fn golden_node_locks_encoding() {
        let mut node = Node::leaf(LeafBody::from_entries([golden_entry()]));
        node.set_structural_gate(TxId::from_bytes(vec![0x11]));
        node.set_membership_writer(TxId::from_bytes(vec![0x22]));

        let got = node.encode();
        let want = [
            0x1a, 0x19, 0x0a, 0x17, 0x0a, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x10, 0x03, 0x1a,
            0x04, 0x01, 0x02, 0x03, 0x04, 0x22, 0x06, 0x0a, 0x02, 0xaa, 0xbb, 0x10, 0x01, 0x2a,
            0x05, 0x08, 0x03, 0x12, 0x01, 0x11, 0x32, 0x05, 0x08, 0x03, 0x12, 0x01, 0x22, 0x38,
            0x02,
        ];
        assert_eq!(node.encoded_len(), got.len());
        assert_eq!(got, want, "node-lock encoding drifted: {got:02x?}");
    }

    // Golden vectors for the ADR-073 merge fields.
    #[test]
    fn golden_merge_fields_encoding() {
        let mut drained = Node::leaf(LeafBody::new())
            .with_low_key(b"a".to_vec())
            .with_high_key(Some(b"m".to_vec()));
        drained.drain(NodeId::from_bytes([8; 16]));
        let got = drained.encode();
        let want = [
            [0x0a, 0x01, 0x6d, 0x12, 0x10].as_slice(),
            &[8; 16],
            &[0x1a, 0x00, 0x48, 0x01, 0x5a, 0x01, 0x61],
        ]
        .concat();
        assert_eq!(drained.encoded_len(), got.len());
        assert_eq!(got, want, "drained node encoding drifted: {got:02x?}");

        let mut reserved = Node::leaf(LeafBody::new());
        let mut locks = reserved.locks().clone();
        locks.set_merge_reservation(StructuralIntentId::from_bytes([3; 16]));
        reserved.set_locks(locks);
        let got = reserved.encode();
        let want = [[0x1a, 0x00, 0x38, 0x01, 0x52, 0x10].as_slice(), &[3; 16]].concat();
        assert_eq!(reserved.encoded_len(), got.len());
        assert_eq!(got, want, "merge reservation encoding drifted: {got:02x?}");
    }

    #[test]
    fn golden_index_encoding() {
        let node = Node::index(IndexNode::from_children([
            (b"".to_vec(), NodeId::from_bytes([1; 16])),
            (b"m".to_vec(), NodeId::from_bytes([2; 16])),
        ]));
        let got = node.encode();
        let want = [
            [0x22, 0x2b, 0x0a, 0x12, 0x12, 0x10].as_slice(),
            &[1; 16],
            &[0x0a, 0x15, 0x0a, 0x01, 0x6d, 0x12, 0x10],
            &[2; 16],
        ]
        .concat();
        assert_eq!(node.encoded_len(), got.len());
        assert_eq!(got, want, "index node encoding drifted: {got:02x?}");
    }
}
