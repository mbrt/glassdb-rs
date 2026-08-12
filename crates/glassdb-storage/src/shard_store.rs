//! Compare-and-swap storage for the v2 coordination objects (ADR-031).
//!
//! B-link nodes (`{prefix}/_r` and `{prefix}/_n/<token>`) are the coordination
//! units. Each mutation is a create-if-absent, a
//! version-conditional compare-and-swap, or an exact-revision delete
//! (ADR-023/ADR-042).
//!
//! Reads and mutations go through the decoded [`CachedStore`].

use glassdb_data::{CollectionAddress, NodeToken, ObjectPath};

use crate::cached_store::{CachedStore, Observation, Requirement};
use crate::error::StorageError;
use crate::node::Node;
use crate::node_store::{LeafEdit, LeafObservation, LeafObservationCheck, LoadedLeaf, NodeStore};
use crate::timeline::SequencePoint;

/// Reads and compare-and-swaps B-link nodes.
#[derive(Clone)]
pub struct ShardStore {
    nodes: NodeStore,
}

impl ShardStore {
    /// Creates a shard store that reads and compare-and-swaps through `objects`.
    pub fn new(objects: CachedStore) -> Self {
        ShardStore {
            nodes: NodeStore::new(objects),
        }
    }

    /// Returns the store responsible for B-link tree nodes.
    pub fn nodes(&self) -> &NodeStore {
        &self.nodes
    }

    /// Checks whether a retained leaf observation is still current after `bound`.
    pub async fn check_leaf_current(
        &self,
        observed: &LeafObservation,
        bound: SequencePoint,
    ) -> Result<LeafObservationCheck, StorageError> {
        self.nodes.check_leaf_current(observed, bound).await
    }

    /// Loads the fixed B-link tree root under `prefix`.
    pub async fn load_root_node(
        &self,
        collection: &CollectionAddress,
        requirement: Requirement,
    ) -> Result<Option<(Node, LeafObservation)>, StorageError> {
        self.nodes.load_root_node(collection, requirement).await
    }

    /// Loads the fixed tree root's exact observation, including absence.
    pub async fn load_root_state(
        &self,
        collection: &CollectionAddress,
        requirement: Requirement,
    ) -> Result<LeafObservation, StorageError> {
        self.nodes.load_root_state(collection, requirement).await
    }

    /// Loads an exact `_r` or `_n/<token>` node observation, including absence.
    pub async fn load_node_at_state(
        &self,
        path: &ObjectPath,
        requirement: Requirement,
    ) -> Result<LeafObservation, StorageError> {
        self.nodes.load_node_at_state(path, requirement).await
    }

    /// Loads an existing node at an exact `_r` or `_n/<token>` path.
    pub async fn load_node_at(
        &self,
        path: &ObjectPath,
        requirement: Requirement,
    ) -> Result<(Node, LeafObservation), StorageError> {
        self.nodes.load_node_at(path, requirement).await
    }

    /// Loads the non-root node's exact observation.
    pub async fn load_node_state(
        &self,
        collection: &CollectionAddress,
        token: &NodeToken,
        requirement: Requirement,
    ) -> Result<LeafObservation, StorageError> {
        self.nodes
            .load_node_state(collection, token, requirement)
            .await
    }

    /// Loads the non-root node named `token` (`{prefix}/_n/<token>`, ADR-031). A
    /// [`StorageError::NotFound`] means the node is missing — a dangling child or
    /// right-sibling reference, which a descent surfaces rather than silently
    /// skips.
    pub async fn load_node(
        &self,
        collection: &CollectionAddress,
        token: &NodeToken,
        requirement: Requirement,
    ) -> Result<(Node, LeafObservation), StorageError> {
        self.nodes.load_node(collection, token, requirement).await
    }

    /// Compare-and-swaps the non-root node named `token`. `expected = None` means
    /// create-if-absent (a freshly split-out sibling). Returns `false` on a
    /// precondition miss, `true` on success.
    pub async fn store_node(
        &self,
        collection: &CollectionAddress,
        token: &NodeToken,
        node: &Node,
        expected: Option<&LeafObservation>,
    ) -> Result<bool, StorageError> {
        self.nodes
            .store_node(collection, token, node, expected)
            .await
    }

    /// Compare-and-swaps an exact `_r` or `_n/<token>` node path.
    pub async fn store_node_at(
        &self,
        path: &ObjectPath,
        node: &Node,
        expected: &LeafObservation,
    ) -> Result<bool, StorageError> {
        self.nodes.store_node_at(path, node, expected).await
    }

    /// Deletes the exact observed standalone node, converging if it is missing.
    pub async fn delete_node(&self, expected: &Observation<Node>) -> Result<(), StorageError> {
        self.nodes.delete_node(expected).await
    }

    /// Lists every standalone node under one incarnation-unique collection
    /// prefix, including temporarily unreachable structural nodes.
    pub async fn list_nodes(
        &self,
        collection: &CollectionAddress,
        requirement: Requirement,
    ) -> Result<Vec<(NodeToken, Observation<Node>)>, StorageError> {
        self.nodes.list_nodes(collection, requirement).await
    }

    /// Loads the leaf node at `_r` or `_n/<token>`.
    pub async fn load_leaf(
        &self,
        path: &ObjectPath,
        requirement: Requirement,
    ) -> Result<LoadedLeaf, StorageError> {
        self.nodes.load_leaf(path, requirement).await
    }

    /// Compare-and-swaps an observation-bound leaf edit.
    pub async fn commit_leaf(&self, edit: LeafEdit) -> Result<bool, StorageError> {
        self.nodes.commit_leaf(edit).await
    }

    /// Loads the fixed tree root under `prefix`.
    pub async fn load_root(
        &self,
        collection: &CollectionAddress,
        requirement: Requirement,
    ) -> Result<(Node, LeafObservation), StorageError> {
        self.nodes.load_root(collection, requirement).await
    }

    /// Compare-and-swaps the fixed tree root.
    pub async fn store_root(
        &self,
        collection: &CollectionAddress,
        root: &Node,
        expected: &LeafObservation,
    ) -> Result<bool, StorageError> {
        self.nodes.store_root(collection, root, expected).await
    }

    /// Creates the fixed tree root if absent.
    pub async fn create_root(
        &self,
        collection: &CollectionAddress,
        root: &Node,
    ) -> Result<bool, StorageError> {
        self.nodes.create_root(collection, root).await
    }

    /// Creates a fixed tree root if absent and returns its exact installed
    /// observation. `None` means another object already occupies the path.
    pub async fn create_root_observed(
        &self,
        collection: &CollectionAddress,
        root: &Node,
    ) -> Result<Option<Observation<Node>>, StorageError> {
        self.nodes.create_root_observed(collection, root).await
    }

    /// Deletes the exact observed fixed tree root.
    pub async fn delete_root(&self, expected: &Observation<Node>) -> Result<(), StorageError> {
        self.nodes.delete_root(expected).await
    }
}
