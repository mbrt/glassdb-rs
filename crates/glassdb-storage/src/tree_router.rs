//! Tree descent over the B-link tree (ADR-031).
//!
//! The [`TreeRouter`] resolves a key to the leaf that owns it by descending from
//! the collection root `_r` through index nodes, and it enumerates the leaves
//! in key order for listing. Descent is **self-correcting**: every node carries
//! a high-key and a right-sibling, so a lookup that lands too far left —
//! because a split moved the key rightward after the cache was taken — steps
//! along the right-sibling link (the B-link property) instead of restarting
//! from the root.
//!
//! This layer is pure routing: it reads nodes through the [`NodeStore`] (hence
//! the decoded object store, so interior nodes stay cached and off the hot
//! path) and never mutates the tree. Splitting and locking live above it.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use futures::FutureExt;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use glassdb_data::{CollectionAddress, KeyRef, NodeToken, ObjectPath};

use crate::cached_store::Requirement;
use crate::error::StorageError;
use crate::node::{Node, NodeBody};
use crate::node_store::{LeafObservation, NodeStore};

/// The leaf that owns a key (or range endpoint), with everything needed to read
/// or compare-and-swap it: its object `path` and retained physical observation.
#[derive(Debug, Clone)]
pub struct LeafLocator {
    pub path: ObjectPath,
    pub observation: LeafObservation,
    /// Whether every object read while routing to this leaf was served locally.
    pub cache_hit: bool,
}

impl LeafLocator {
    /// Returns the observed node.
    pub fn node(&self) -> Option<&Node> {
        self.observation.value().map(AsRef::as_ref)
    }
}

/// A group of keys routed to one leaf by [`TreeRouter::group_keys_by_leaf`]: the
/// owning leaf and the raw keys (with their payloads) that landed in it.
pub struct RoutedLeafGroup<T> {
    pub observation: LeafObservation,
    pub keys: Vec<(Vec<u8>, T)>,
}

impl<T> RoutedLeafGroup<T> {
    /// Returns the observed leaf's physical object path.
    pub fn path(&self) -> &ObjectPath {
        self.observation.path()
    }

    /// Returns the observed node.
    pub fn node(&self) -> Option<&Node> {
        self.observation.value().map(AsRef::as_ref)
    }
}

struct RoutedItem<T> {
    ordinal: usize,
    key: KeyRef,
    raw_key: Vec<u8>,
    payload: T,
    stage: RouteStage,
}

#[derive(Clone, Copy)]
enum RouteStage {
    Interior,
    Leaf,
}

struct PendingPath<T> {
    items: Vec<RoutedItem<T>>,
}

struct CompletedPath<T> {
    observation: LeafObservation,
    items: Vec<RoutedItem<T>>,
}

type PathLoad = (
    ObjectPath,
    Requirement,
    Result<LeafObservation, StorageError>,
);

/// One node reached during a descent: its decoded body, object path, and
/// retained physical observation.
struct Located {
    path: ObjectPath,
    observation: LeafObservation,
    cache_hit: bool,
}

impl Located {
    fn node(&self) -> &Node {
        self.observation
            .value()
            .map(AsRef::as_ref)
            .expect("Located is only constructed for present objects")
    }

    fn after(mut self, prior_cache_hit: bool) -> Self {
        self.cache_hit &= prior_cache_hit;
        self
    }

    fn into_locator(self) -> LeafLocator {
        LeafLocator {
            path: self.path,
            observation: self.observation,
            cache_hit: self.cache_hit,
        }
    }
}

/// Stateful keyed descent through one collection tree.
struct DescentCursor<'a> {
    router: &'a TreeRouter,
    collection: &'a CollectionAddress,
    requirement: Requirement,
    current: Located,
}

impl<'a> DescentCursor<'a> {
    fn new(
        router: &'a TreeRouter,
        collection: &'a CollectionAddress,
        requirement: Requirement,
        current: Located,
    ) -> Self {
        DescentCursor {
            router,
            collection,
            requirement,
            current,
        }
    }

    fn into_locator(self) -> LeafLocator {
        self.current.into_locator()
    }

    /// Moves right until the current node owns `key`.
    async fn normalize_at(&mut self, key: &[u8]) -> Result<(), StorageError> {
        while !self.current.node().owns(key) {
            let Some(token) = self.current.node().right_sibling() else {
                break;
            };
            let token = node_token(token)?;
            let cache_hit = self.current.cache_hit;
            self.current = self
                .router
                .load_child(self.collection, &token, self.requirement)
                .await?
                .after(cache_hit);
        }
        Ok(())
    }

    /// Advances one index level and returns the normalized index it left.
    async fn advance_for(&mut self, key: &[u8]) -> Result<Option<Located>, StorageError> {
        let token = match self.current.node().body() {
            NodeBody::Leaf(_) => return Ok(None),
            NodeBody::Index(index) => node_token(
                index
                    .child_for(key)
                    .ok_or_else(|| StorageError::other("descent reached an empty index node"))?,
            )?,
        };
        let cache_hit = self.current.cache_hit;
        let child = self
            .router
            .load_child(self.collection, &token, self.requirement)
            .await?
            .after(cache_hit);
        Ok(Some(std::mem::replace(&mut self.current, child)))
    }

    /// Continues the keyed descent until it reaches a leaf.
    async fn descend_to_leaf(mut self, key: &[u8]) -> Result<Self, StorageError> {
        loop {
            self.normalize_at(key).await?;
            if self.advance_for(key).await?.is_none() {
                return Ok(self);
            }
        }
    }

    async fn run_until<S: DescentStop>(
        mut self,
        key: &[u8],
        mut stop: S,
    ) -> Result<S::Output, StorageError> {
        loop {
            self.normalize_at(key).await?;
            if let Some(output) = stop.stop_at(&self.current) {
                return Ok(output);
            }
            let Some(index) = self.advance_for(key).await? else {
                return Ok(stop.finish_at_leaf());
            };
            stop.descended_from(index);
        }
    }

    /// Reloads the exact current path at a new requirement without losing the
    /// hit state accumulated along the route.
    async fn reload_current(&mut self, requirement: Requirement) -> Result<(), StorageError> {
        let path = self.current.path.clone();
        let cache_hit = self.current.cache_hit;
        let observation = self
            .router
            .nodes
            .load_node_at_state(&path, requirement)
            .await?;
        if observation.is_absent() {
            return Err(StorageError::other("tree node vanished during descent"));
        }
        self.current = Located {
            path,
            cache_hit: observation.cache_hit(),
            observation,
        }
        .after(cache_hit);
        self.requirement = requirement;
        Ok(())
    }
}

trait DescentStop {
    type Output;

    fn stop_at(&mut self, current: &Located) -> Option<Self::Output>;

    fn finish_at_leaf(self) -> Self::Output;

    fn descended_from(&mut self, _index: Located) {}
}

struct ReachTarget {
    target: ObjectPath,
}

impl DescentStop for ReachTarget {
    type Output = bool;

    fn stop_at(&mut self, current: &Located) -> Option<Self::Output> {
        (current.path == self.target).then_some(true)
    }

    fn finish_at_leaf(self) -> Self::Output {
        false
    }
}

struct FindParent {
    parent: Option<Located>,
}

impl DescentStop for FindParent {
    type Output = Option<LeafLocator>;

    fn stop_at(&mut self, _current: &Located) -> Option<Self::Output> {
        None
    }

    fn finish_at_leaf(self) -> Self::Output {
        self.parent.map(Located::into_locator)
    }

    fn descended_from(&mut self, index: Located) {
        self.parent = Some(index);
    }
}

/// Walks retained leaf locators through their right-sibling links.
struct LeafChain<'a> {
    router: &'a TreeRouter,
    collection: &'a CollectionAddress,
    requirement: Requirement,
}

impl<'a> LeafChain<'a> {
    fn new(
        router: &'a TreeRouter,
        collection: &'a CollectionAddress,
        requirement: Requirement,
    ) -> Self {
        LeafChain {
            router,
            collection,
            requirement,
        }
    }

    async fn successor(&self, leaf: &LeafLocator) -> Result<Option<LeafLocator>, StorageError> {
        let Some(token) = leaf.node().and_then(Node::right_sibling) else {
            return Ok(None);
        };
        let token = node_token(token)?;
        Ok(Some(
            self.router
                .load_child(self.collection, &token, self.requirement)
                .await?
                .after(leaf.cache_hit)
                .into_locator(),
        ))
    }

    async fn collect_through(
        &self,
        mut leaf: LeafLocator,
        end: Option<&[u8]>,
    ) -> Result<Vec<LeafLocator>, StorageError> {
        let mut out = Vec::new();
        loop {
            let done = end.is_some_and(|end| leaf.node().is_some_and(|node| node.owns(end)));
            let next = if done {
                None
            } else {
                self.successor(&leaf).await?
            };
            out.push(leaf);
            match next {
                Some(right) => leaf = right,
                None => return Ok(out),
            }
        }
    }
}

/// Descends and scans a collection's B-link tree.
#[derive(Clone)]
pub struct TreeRouter {
    nodes: NodeStore,
    parallelism: NonZeroUsize,
}

impl TreeRouter {
    /// Creates a router that reads nodes through `nodes`.
    pub fn new(nodes: NodeStore, parallelism: NonZeroUsize) -> Self {
        TreeRouter { nodes, parallelism }
    }

    /// Resolves the leaf that owns `key`, descending from the root `_r` and
    /// following right-sibling links to self-correct past in-progress splits.
    ///
    /// A missing collection root is reported as [`StorageError::NotFound`].
    pub async fn leaf_for(
        &self,
        collection: &CollectionAddress,
        key: &[u8],
        requirement: Requirement,
    ) -> Result<LeafLocator, StorageError> {
        self.leaf_cursor(collection, key, requirement)
            .await?
            .map(DescentCursor::into_locator)
            .ok_or(StorageError::NotFound)
    }

    /// Returns the existing leaf that owns `key`, or `None` when the collection
    /// does not exist.
    pub async fn first_leaf_at(
        &self,
        collection: &CollectionAddress,
        key: &[u8],
        requirement: Requirement,
    ) -> Result<Option<LeafLocator>, StorageError> {
        Ok(self
            .leaf_cursor(collection, key, requirement)
            .await?
            .map(DescentCursor::into_locator))
    }

    /// Returns the right sibling of `leaf`, or `None` for the rightmost leaf.
    pub async fn next_leaf(
        &self,
        collection: &CollectionAddress,
        leaf: &LeafLocator,
        requirement: Requirement,
    ) -> Result<Option<LeafLocator>, StorageError> {
        LeafChain::new(self, collection, requirement)
            .successor(leaf)
            .await
    }

    /// Returns the leaves from the one owning `start` through the one owning
    /// the inclusive `end`; `None` scans through positive infinity.
    pub async fn leaves_through(
        &self,
        collection: &CollectionAddress,
        start: &[u8],
        end: Option<&[u8]>,
        requirement: Requirement,
    ) -> Result<Vec<LeafLocator>, StorageError> {
        let Some(first) = self.first_leaf_at(collection, start, requirement).await? else {
            return Err(StorageError::NotFound);
        };
        LeafChain::new(self, collection, requirement)
            .collect_through(first, end)
            .await
    }

    /// Resolves the owning leaf while keeping interior-node currentness checks
    /// off the hot path (ADR-031): descends the index spine at `interior` requirement
    /// (served from cache — a stale misroute self-corrects via right-links) and
    /// checks only the terminal leaf — the coordination/CAS unit — at `leaf`
    /// requirement. A grown tree thus never checks the root `_r` on every key
    /// coordination; a current lower bound stays where a CAS depends on it.
    ///
    /// When both freshnesses match this is exactly [`leaf_for`](Self::leaf_for).
    pub async fn leaf_for_fresh(
        &self,
        collection: &CollectionAddress,
        key: &[u8],
        interior: Requirement,
        leaf: Requirement,
    ) -> Result<LeafLocator, StorageError> {
        let mut cursor = self
            .leaf_cursor(collection, key, interior)
            .await?
            .ok_or(StorageError::NotFound)?;
        // The same requirement needs no terminal refresh.
        if interior == leaf {
            return Ok(cursor.into_locator());
        }
        // Check the terminal node at the stricter requirement and resume the
        // descent from it: the cached interior read may have routed us to `_r`
        // as a leaf while a concurrent split has since rewritten `_r` into an
        // index (or split the leaf), so we must keep descending — never hand
        // back an index masquerading as a leaf.
        cursor.reload_current(leaf).await?;
        Ok(cursor.descend_to_leaf(key).await?.into_locator())
    }

    /// Returns the leftmost leaf of the collection, or `None` if the collection
    /// does not exist. The entry point for an ordered/range scan.
    pub async fn leftmost_leaf(
        &self,
        collection: &CollectionAddress,
        requirement: Requirement,
    ) -> Result<Option<LeafLocator>, StorageError> {
        Ok(self
            .leaf_cursor(collection, b"", requirement)
            .await?
            .map(DescentCursor::into_locator))
    }

    /// Collects every leaf of the collection in key order, following the leaf
    /// right-sibling chain from the leftmost leaf. Empty when the collection does
    /// not exist.
    pub async fn leaves(
        &self,
        collection: &CollectionAddress,
        requirement: Requirement,
    ) -> Result<Vec<LeafLocator>, StorageError> {
        let Some(first) = self.leftmost_leaf(collection, requirement).await? else {
            return Ok(Vec::new());
        };
        LeafChain::new(self, collection, requirement)
            .collect_through(first, None)
            .await
    }

    /// Routes `(key, payload)` items to their owning leaves, returning one
    /// group per touched leaf with its loaded node and version. Callers hand it
    /// logical keys and never compute a location themselves; routing is by descent
    /// from the collection root, not by any fixed hash (ADR-031).
    ///
    /// Groups are keyed by leaf object path, so keys from different collections
    /// (distinct `_r`) never collide; input order is preserved within a group.
    /// Missing non-root collection trees are reported as
    /// [`StorageError::StaleCollection`] while the failing key still identifies
    /// its collection.
    pub async fn group_keys_by_leaf<T>(
        &self,
        items: impl IntoIterator<Item = (KeyRef, T)>,
        requirement: Requirement,
    ) -> Result<Vec<RoutedLeafGroup<T>>, StorageError> {
        self.group_keys_by_leaf_fresh(items, requirement, requirement)
            .await
    }

    /// [`group_keys_by_leaf`] with the interior-vs-leaf requirement split of
    /// [`leaf_for_fresh`], so the coordination hot path routes keys without
    /// checking the root `_r` (ADR-031).
    ///
    /// [`group_keys_by_leaf`]: Self::group_keys_by_leaf
    /// [`leaf_for_fresh`]: Self::leaf_for_fresh
    pub async fn group_keys_by_leaf_fresh<T>(
        &self,
        items: impl IntoIterator<Item = (KeyRef, T)>,
        interior: Requirement,
        leaf: Requirement,
    ) -> Result<Vec<RoutedLeafGroup<T>>, StorageError> {
        let mut items = items.into_iter();
        let Some(first) = items.next() else {
            return Ok(Vec::new());
        };
        let Some(second) = items.next() else {
            let (key, payload) = first;
            let raw_key = key.key().to_vec();
            let locator = self
                .leaf_for_fresh(key.collection(), &raw_key, interior, leaf)
                .await
                .map_err(|error| error.classify_collection_absence(key.collection()))?;
            return Ok(vec![RoutedLeafGroup {
                observation: locator.observation,
                keys: vec![(raw_key, payload)],
            }]);
        };

        self.group_keys_by_leaf_batched(
            std::iter::once(first)
                .chain(std::iter::once(second))
                .chain(items),
            interior,
            leaf,
        )
        .await
    }

    async fn group_keys_by_leaf_batched<T>(
        &self,
        items: impl IntoIterator<Item = (KeyRef, T)>,
        interior: Requirement,
        leaf: Requirement,
    ) -> Result<Vec<RoutedLeafGroup<T>>, StorageError> {
        let mut pending = BTreeMap::<ObjectPath, PendingPath<T>>::new();
        let mut ready = BTreeSet::<ObjectPath>::new();
        let mut in_flight_paths = BTreeSet::<ObjectPath>::new();
        let mut in_flight = FuturesUnordered::<BoxFuture<'static, PathLoad>>::new();
        let mut completed = BTreeMap::<ObjectPath, CompletedPath<T>>::new();
        let mut errors = Vec::<(usize, ObjectPath, StorageError)>::new();

        for (ordinal, (key, payload)) in items.into_iter().enumerate() {
            let path = ObjectPath::TreeRoot {
                collection: key.collection().clone(),
            };
            enqueue_routed_item(
                &mut pending,
                &mut ready,
                &in_flight_paths,
                path,
                RoutedItem {
                    ordinal,
                    raw_key: key.key().to_vec(),
                    key,
                    payload,
                    stage: RouteStage::Interior,
                },
            );
        }

        loop {
            while in_flight.len() < self.parallelism.get() {
                let Some(path) = next_ready_path(&ready, &pending) else {
                    break;
                };
                ready.remove(&path);
                let requirement = pending[&path]
                    .items
                    .iter()
                    .map(|item| route_requirement(item.stage, interior, leaf))
                    .fold(Requirement::Any, Requirement::stricter);
                in_flight_paths.insert(path.clone());
                let nodes = self.nodes.clone();
                in_flight.push(
                    async move {
                        let result = nodes.load_node_at_state(&path, requirement).await;
                        (path, requirement, result)
                    }
                    .boxed(),
                );
            }

            let Some((path, loaded_at, result)) = in_flight.next().await else {
                break;
            };
            in_flight_paths.remove(&path);
            let batch = pending
                .remove(&path)
                .expect("every admitted path keeps its routed items");

            let observation = match result {
                Ok(observation) if observation.exists() => observation,
                Ok(_) => {
                    let first = batch
                        .items
                        .iter()
                        .min_by_key(|item| item.ordinal)
                        .expect("a routed path has at least one item");
                    errors.push((
                        first.ordinal,
                        path,
                        StorageError::NotFound.classify_collection_absence(first.key.collection()),
                    ));
                    continue;
                }
                Err(error) => {
                    let first = batch
                        .items
                        .iter()
                        .min_by_key(|item| item.ordinal)
                        .expect("a routed path has at least one item");
                    errors.push((
                        first.ordinal,
                        path,
                        error.classify_collection_absence(first.key.collection()),
                    ));
                    continue;
                }
            };

            let mut routed_items = batch.items;
            if let Some(previous) = completed.remove(&path) {
                routed_items.extend(previous.items);
            }
            routed_items.sort_by_key(|item| item.ordinal);

            let node = observation
                .value()
                .cloned()
                .expect("present node observations have a decoded node");
            let mut finished = Vec::new();
            for mut item in routed_items {
                let required = route_requirement(item.stage, interior, leaf);
                if !loaded_at.covers(required)
                    || !required.is_satisfied_by(observation.current_after())
                {
                    enqueue_routed_item(
                        &mut pending,
                        &mut ready,
                        &in_flight_paths,
                        path.clone(),
                        item,
                    );
                    continue;
                }

                if !node.owns(&item.raw_key) {
                    let Some(token) = node.right_sibling() else {
                        finished.push(item);
                        continue;
                    };
                    let token = match node_token(token) {
                        Ok(token) => token,
                        Err(error) => {
                            errors.push((item.ordinal, path.clone(), error));
                            continue;
                        }
                    };
                    let target = ObjectPath::Node {
                        collection: item.key.collection().clone(),
                        token,
                    };
                    item.stage = match node.body() {
                        NodeBody::Leaf(_) => RouteStage::Leaf,
                        NodeBody::Index(_) => RouteStage::Interior,
                    };
                    enqueue_routed_item(&mut pending, &mut ready, &in_flight_paths, target, item);
                    continue;
                }

                match node.body() {
                    NodeBody::Index(index) => {
                        let Some(token) = index.child_for(&item.raw_key) else {
                            errors.push((
                                item.ordinal,
                                path.clone(),
                                StorageError::other("descent reached an empty index node"),
                            ));
                            continue;
                        };
                        let token = match node_token(token) {
                            Ok(token) => token,
                            Err(error) => {
                                errors.push((item.ordinal, path.clone(), error));
                                continue;
                            }
                        };
                        let target = ObjectPath::Node {
                            collection: item.key.collection().clone(),
                            token,
                        };
                        item.stage = RouteStage::Interior;
                        enqueue_routed_item(
                            &mut pending,
                            &mut ready,
                            &in_flight_paths,
                            target,
                            item,
                        );
                    }
                    NodeBody::Leaf(_) if leaf.is_satisfied_by(observation.current_after()) => {
                        finished.push(item);
                    }
                    NodeBody::Leaf(_) => {
                        item.stage = RouteStage::Leaf;
                        enqueue_routed_item(
                            &mut pending,
                            &mut ready,
                            &in_flight_paths,
                            path.clone(),
                            item,
                        );
                    }
                }
            }

            if !finished.is_empty() {
                completed.insert(
                    path,
                    CompletedPath {
                        observation,
                        items: finished,
                    },
                );
            }
        }

        if let Some((_, _, error)) = errors
            .into_iter()
            .min_by(|left, right| (left.0, &left.1).cmp(&(right.0, &right.1)))
        {
            return Err(error);
        }

        Ok(completed
            .into_values()
            .map(|mut group| {
                group.items.sort_by_key(|item| item.ordinal);
                RoutedLeafGroup {
                    observation: group.observation,
                    keys: group
                        .items
                        .into_iter()
                        .map(|item| (item.raw_key, item.payload))
                        .collect(),
                }
            })
            .collect())
    }

    /// Reports whether descent for `key` reaches the node named `target`.
    ///
    /// A split's new right sibling owns its recorded split key, so recovery can
    /// prove publication by following one B-link path instead of walking the
    /// collection's whole tree.
    pub async fn token_reachable_at_key(
        &self,
        collection: &CollectionAddress,
        key: &[u8],
        target: &NodeToken,
        requirement: Requirement,
    ) -> Result<bool, StorageError> {
        let Some(cursor) = self.start_descent(collection, requirement).await? else {
            return Ok(false);
        };
        match cursor
            .run_until(
                key,
                ReachTarget {
                    target: ObjectPath::Node {
                        collection: collection.clone(),
                        token: target.clone(),
                    },
                },
            )
            .await
        {
            Ok(reachable) => Ok(reachable),
            Err(StorageError::NotFound) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Finds the deepest index node that owns `key` — the parent of the leaf
    /// level on the descent toward `key`, into which a leaf split publishes its
    /// separator (ADR-031). Descends from the root (self-correcting through
    /// right-links) and returns the last index visited before reaching a leaf.
    /// Returns `None` when the collection does not exist or its root is still a
    /// single leaf (no index level yet).
    pub async fn parent_index_for(
        &self,
        collection: &CollectionAddress,
        key: &[u8],
        requirement: Requirement,
    ) -> Result<Option<LeafLocator>, StorageError> {
        let Some(cursor) = self.start_descent(collection, requirement).await? else {
            return Ok(None);
        };
        cursor.run_until(key, FindParent { parent: None }).await
    }

    async fn start_descent<'a>(
        &'a self,
        collection: &'a CollectionAddress,
        requirement: Requirement,
    ) -> Result<Option<DescentCursor<'a>>, StorageError> {
        let observation = self.nodes.load_root_state(collection, requirement).await?;
        if observation.is_absent() {
            return Ok(None);
        }
        let current = Located {
            path: ObjectPath::TreeRoot {
                collection: collection.clone(),
            },
            cache_hit: observation.cache_hit(),
            observation,
        };
        Ok(Some(DescentCursor::new(
            self,
            collection,
            requirement,
            current,
        )))
    }

    async fn leaf_cursor<'a>(
        &'a self,
        collection: &'a CollectionAddress,
        key: &[u8],
        requirement: Requirement,
    ) -> Result<Option<DescentCursor<'a>>, StorageError> {
        let Some(cursor) = self.start_descent(collection, requirement).await? else {
            return Ok(None);
        };
        Ok(Some(cursor.descend_to_leaf(key).await?))
    }

    async fn load_child(
        &self,
        collection: &CollectionAddress,
        token: &NodeToken,
        requirement: Requirement,
    ) -> Result<Located, StorageError> {
        let observation = self
            .nodes
            .load_node_state(collection, token, requirement)
            .await?;
        Ok(Located {
            path: ObjectPath::Node {
                collection: collection.clone(),
                token: token.clone(),
            },
            cache_hit: observation.cache_hit(),
            observation,
        })
    }
}

fn node_token(token: &str) -> Result<NodeToken, StorageError> {
    NodeToken::try_from(token)
        .map_err(|error| StorageError::with_source("invalid node reference", error))
}

fn route_requirement(stage: RouteStage, interior: Requirement, leaf: Requirement) -> Requirement {
    match stage {
        RouteStage::Interior => interior,
        RouteStage::Leaf => leaf,
    }
}

fn enqueue_routed_item<T>(
    pending: &mut BTreeMap<ObjectPath, PendingPath<T>>,
    ready: &mut BTreeSet<ObjectPath>,
    in_flight: &BTreeSet<ObjectPath>,
    path: ObjectPath,
    item: RoutedItem<T>,
) {
    pending
        .entry(path.clone())
        .or_insert_with(|| PendingPath { items: Vec::new() })
        .items
        .push(item);
    if !in_flight.contains(&path) {
        ready.insert(path);
    }
}

fn next_ready_path<T>(
    ready: &BTreeSet<ObjectPath>,
    pending: &BTreeMap<ObjectPath, PendingPath<T>>,
) -> Option<ObjectPath> {
    ready
        .iter()
        .min_by(|left, right| {
            let left_ordinal = pending[*left]
                .items
                .iter()
                .map(|item| item.ordinal)
                .min()
                .expect("a ready path has at least one routed item");
            let right_ordinal = pending[*right]
                .items
                .iter()
                .map(|item| item.ordinal)
                .min()
                .expect("a ready path has at least one routed item");
            (left_ordinal, *left).cmp(&(right_ordinal, *right))
        })
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{OpLog, RecordingBackend};
    use glassdb_data::{CollectionAddress, CollectionId};

    use crate::Timeline;
    use crate::cached_store::CachedStore;
    use crate::node::{IndexNode, Node};
    use crate::node_store::NodeStore;
    use crate::shard::Shard;
    use crate::shard::ShardEntry;

    #[derive(Clone)]
    struct TestStore {
        shards: NodeStore,
        timeline: Timeline,
    }

    impl std::ops::Deref for TestStore {
        type Target = NodeStore;

        fn deref(&self) -> &Self::Target {
            &self.shards
        }
    }

    fn store() -> TestStore {
        store_over(Arc::new(MemoryBackend::new()))
    }

    fn store_over(backend: Arc<dyn Backend>) -> TestStore {
        let timeline = Timeline::new();
        let objects = CachedStore::new(backend, 1 << 20, timeline.clone(), None);
        let shards = NodeStore::new(objects, std::num::NonZeroUsize::MIN);
        TestStore { shards, timeline }
    }

    fn recording_backend() -> (Arc<dyn Backend>, OpLog) {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        (Arc::new(recorder), log)
    }

    fn take_reads(log: &OpLog) -> Vec<(String, String)> {
        let mut log = log.lock().unwrap();
        let reads = log
            .iter()
            .filter(|record| matches!(record.op, "read" | "read_if_modified"))
            .map(|record| (record.op.to_string(), record.path.clone()))
            .collect();
        log.clear();
        reads
    }

    fn read(op: &str, path: ObjectPath) -> (String, String) {
        (op.to_string(), path.to_string())
    }

    fn root_path() -> ObjectPath {
        ObjectPath::TreeRoot {
            collection: collection(),
        }
    }

    fn assert_fresh(locator: &LeafLocator, bound: crate::SequencePoint) {
        assert!(locator.observation.current_after() >= bound);
    }

    fn live(key: &[u8]) -> ShardEntry {
        ShardEntry::new(key).with_current(crate::CurrentState::External {
            writer: glassdb_data::TxId::from_bytes(vec![1]),
        })
    }

    fn collection() -> CollectionAddress {
        CollectionAddress::root("db")
    }

    fn token(byte: u8) -> NodeToken {
        NodeToken::from_bytes([byte; 16])
    }

    fn node_path(byte: u8) -> ObjectPath {
        ObjectPath::Node {
            collection: collection(),
            token: token(byte),
        }
    }

    fn leaf(entries: &[&[u8]], high_key: Option<&[u8]>, right: Option<&NodeToken>) -> Node {
        Node::leaf(Shard::from_entries(entries.iter().map(|k| live(k))))
            .with_high_key(high_key.map(<[u8]>::to_vec))
            .with_right_sibling(right.map(ToString::to_string))
    }

    async fn store_leaf(
        s: &NodeStore,
        byte: u8,
        entries: &[&[u8]],
        high_key: Option<&[u8]>,
        right: Option<u8>,
    ) {
        let right = right.map(token);
        s.store_node(
            &collection(),
            &token(byte),
            &leaf(entries, high_key, right.as_ref()),
            None,
        )
        .await
        .unwrap();
    }

    // Seeds a two-level tree: root index -> {L0 (apple,cat), L1 (mango,pear)},
    // split at "m", with the leaves chained by right-sibling.
    async fn seed_two_level(s: &NodeStore) {
        let left = token(0);
        let right = token(1);
        store_leaf(s, 0, &[b"apple", b"cat"], Some(b"m"), Some(1)).await;
        store_leaf(s, 1, &[b"mango", b"pear"], None, None).await;
        let root = Node::index(IndexNode::from_children([
            (b"".to_vec(), left.to_string()),
            (b"m".to_vec(), right.to_string()),
        ]));
        s.create_root(&collection(), &root).await.unwrap();
    }

    // Models a leaf split whose parent is stale: R still routes every key to
    // L0, while L0's right-link moves keys at and above "m" to L1.
    async fn seed_stale_leaf_parent(s: &NodeStore) {
        let left = token(0);
        store_leaf(s, 0, &[b"apple", b"cat"], Some(b"m"), Some(1)).await;
        store_leaf(s, 1, &[b"mango", b"pear"], None, None).await;
        s.create_root(
            &collection(),
            &Node::index(IndexNode::from_children([(Vec::new(), left.to_string())])),
        )
        .await
        .unwrap();
    }

    // Three leaves behind one stale parent exercise both a bounded scan and
    // the leaf-to-leaf interface without involving another descent shape.
    async fn seed_three_leaf_chain(s: &NodeStore) {
        let first = token(0);
        store_leaf(s, 0, &[b"apple"], Some(b"m"), Some(1)).await;
        store_leaf(s, 1, &[b"mango"], Some(b"t"), Some(4)).await;
        store_leaf(s, 4, &[b"zebra"], None, None).await;
        s.create_root(
            &collection(),
            &Node::index(IndexNode::from_children([(Vec::new(), first.to_string())])),
        )
        .await
        .unwrap();
    }

    // Models an interior split whose parent is stale: R still routes to I0,
    // whose right-link moves the lookup to I1 before descending to L1.
    async fn seed_stale_interior_parent(s: &NodeStore) {
        let interior_left = token(2);
        let interior_right = token(3);
        let leaf_left = token(0);
        let leaf_right = token(1);
        store_leaf(s, 0, &[b"apple"], Some(b"m"), Some(1)).await;
        store_leaf(s, 1, &[b"pear"], None, None).await;
        s.store_node(
            &collection(),
            &interior_left,
            &Node::index(IndexNode::from_children([(
                Vec::new(),
                leaf_left.to_string(),
            )]))
            .with_high_key(Some(b"m".to_vec()))
            .with_right_sibling(Some(interior_right.to_string())),
            None,
        )
        .await
        .unwrap();
        s.store_node(
            &collection(),
            &interior_right,
            &Node::index(IndexNode::from_children([(
                b"m".to_vec(),
                leaf_right.to_string(),
            )])),
            None,
        )
        .await
        .unwrap();
        s.create_root(
            &collection(),
            &Node::index(IndexNode::from_children([(
                Vec::new(),
                interior_left.to_string(),
            )])),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn single_leaf_collection_resolves_to_root_without_parent() {
        let s = store();
        let root = Node::leaf(Shard::from_entries([live(b"only")]));
        s.create_root(&collection(), &root).await.unwrap();

        let router = TreeRouter::new(s.shards.clone(), std::num::NonZeroUsize::MIN);
        let requirement = Requirement::AtLeast(s.timeline.now());
        let loc = router
            .leaf_for(&collection(), b"only", requirement)
            .await
            .unwrap();
        assert_eq!(loc.path, root_path());
        assert!(!loc.observation.is_absent());
        assert!(loc.node().unwrap().as_leaf().unwrap().exists(b"only"));
        assert!(
            router
                .parent_index_for(&collection(), b"only", requirement)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn absent_collection_is_not_a_writable_empty_leaf() {
        let s = store();
        let router = TreeRouter::new(s.shards.clone(), std::num::NonZeroUsize::MIN);
        assert!(matches!(
            router
                .leaf_for(&collection(), b"k", Requirement::AtLeast(s.timeline.now()))
                .await,
            Err(StorageError::NotFound)
        ));
        // Structural traversal can still model absence as no reachable leaves.
        assert!(
            router
                .leaves(&collection(), Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .is_empty()
        );
        let requirement = Requirement::AtLeast(s.timeline.now());
        assert!(
            !router
                .token_reachable_at_key(&collection(), b"k", &token(9), requirement)
                .await
                .unwrap()
        );
        assert!(
            router
                .parent_index_for(&collection(), b"k", requirement)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn descends_index_to_correct_leaf() {
        let s = store();
        seed_two_level(&s).await;
        let router = TreeRouter::new(s.shards.clone(), std::num::NonZeroUsize::MIN);

        for (key, want_leaf) in [
            (b"apple".as_slice(), node_path(0)),
            (b"cat", node_path(0)),
            (b"mango", node_path(1)),
            (b"pear", node_path(1)),
            (b"zebra", node_path(1)),
        ] {
            let loc = router
                .leaf_for(&collection(), key, Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap();
            assert_eq!(loc.path, want_leaf, "wrong leaf for key {key:?}");
        }
    }

    #[tokio::test]
    async fn stale_leaf_right_hop_preserves_trace_and_cumulative_hits() {
        let (backend, log) = recording_backend();
        seed_stale_leaf_parent(&store_over(backend.clone())).await;
        take_reads(&log);

        let cold = store_over(backend.clone());
        let router = TreeRouter::new(cold.shards.clone(), std::num::NonZeroUsize::MIN);
        let loc = router
            .leaf_for(&collection(), b"pear", Requirement::Any)
            .await
            .unwrap();
        assert_eq!(loc.path, node_path(1));
        assert!(!loc.cache_hit);
        assert!(loc.node().unwrap().as_leaf().unwrap().exists(b"pear"));
        assert_eq!(
            take_reads(&log),
            [
                read("read", root_path()),
                read("read", node_path(0)),
                read("read", node_path(1)),
            ]
        );

        assert!(
            router
                .leaf_for(&collection(), b"pear", Requirement::Any)
                .await
                .unwrap()
                .cache_hit
        );
        assert!(take_reads(&log).is_empty());

        let terminal_warm = store_over(backend);
        terminal_warm
            .load_node_state(&collection(), &token(1), Requirement::Any)
            .await
            .unwrap();
        take_reads(&log);
        let loc = TreeRouter::new(terminal_warm.shards.clone(), std::num::NonZeroUsize::MIN)
            .leaf_for(&collection(), b"pear", Requirement::Any)
            .await
            .unwrap();
        assert!(!loc.cache_hit, "a warm leaf cannot hide cold prefix reads");
        assert_eq!(
            take_reads(&log),
            [read("read", root_path()), read("read", node_path(0))]
        );
    }

    #[tokio::test]
    async fn terminal_freshness_checks_only_the_leaf() {
        let (backend, log) = recording_backend();
        seed_stale_leaf_parent(&store_over(backend.clone())).await;
        take_reads(&log);

        let s = store_over(backend.clone());
        let router = TreeRouter::new(s.shards.clone(), std::num::NonZeroUsize::MIN);
        router
            .leaf_for(&collection(), b"pear", Requirement::Any)
            .await
            .unwrap();
        take_reads(&log);

        let bound = s.timeline.now();
        let loc = router
            .leaf_for_fresh(
                &collection(),
                b"pear",
                Requirement::Any,
                Requirement::AtLeast(bound),
            )
            .await
            .unwrap();
        assert_eq!(loc.path, node_path(1));
        assert!(loc.cache_hit);
        assert_fresh(&loc, bound);
        assert_eq!(take_reads(&log), [read("read_if_modified", node_path(1))]);

        let mixed = store_over(backend);
        for byte in [0, 1] {
            mixed
                .load_node_state(&collection(), &token(byte), Requirement::Any)
                .await
                .unwrap();
        }
        take_reads(&log);
        let bound = mixed.timeline.now();
        let loc = TreeRouter::new(mixed.shards.clone(), std::num::NonZeroUsize::MIN)
            .leaf_for_fresh(
                &collection(),
                b"pear",
                Requirement::Any,
                Requirement::AtLeast(bound),
            )
            .await
            .unwrap();
        assert!(!loc.cache_hit, "a terminal hit cannot erase a root miss");
        assert_fresh(&loc, bound);
        assert_eq!(
            take_reads(&log),
            [
                read("read", root_path()),
                read("read_if_modified", node_path(1)),
            ]
        );
    }

    #[tokio::test]
    async fn next_leaf_retains_a_prior_miss_when_the_sibling_is_warm() {
        let (backend, log) = recording_backend();
        seed_stale_leaf_parent(&store_over(backend.clone())).await;
        take_reads(&log);

        let s = store_over(backend);
        s.load_node_state(&collection(), &token(1), Requirement::Any)
            .await
            .unwrap();
        take_reads(&log);

        let router = TreeRouter::new(s.shards.clone(), std::num::NonZeroUsize::MIN);
        let first = router
            .first_leaf_at(&collection(), b"apple", Requirement::Any)
            .await
            .unwrap()
            .unwrap();
        assert!(!first.cache_hit);
        take_reads(&log);
        let middle = router
            .next_leaf(&collection(), &first, Requirement::Any)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(middle.path, node_path(1));
        assert!(
            !middle.cache_hit,
            "the retained prefix miss must survive the warm sibling read"
        );
        assert!(take_reads(&log).is_empty());
    }

    #[tokio::test]
    async fn bounded_scan_is_inclusive_without_prefetching() {
        let (backend, log) = recording_backend();
        seed_three_leaf_chain(&store_over(backend.clone())).await;
        take_reads(&log);

        let bounded = store_over(backend.clone());
        let leaves = TreeRouter::new(bounded.shards.clone(), std::num::NonZeroUsize::MIN)
            .leaves_through(&collection(), b"apple", Some(b"mango"), Requirement::Any)
            .await
            .unwrap();
        assert_eq!(
            leaves.iter().map(|leaf| &leaf.path).collect::<Vec<_>>(),
            [&node_path(0), &node_path(1)]
        );
        assert!(leaves.iter().all(|leaf| !leaf.cache_hit));
        assert_eq!(
            take_reads(&log),
            [
                read("read", root_path()),
                read("read", node_path(0)),
                read("read", node_path(1)),
            ],
            "the inclusive middle bound must not prefetch its successor"
        );

        let terminal_warm = store_over(backend);
        terminal_warm
            .load_node_state(&collection(), &token(4), Requirement::Any)
            .await
            .unwrap();
        take_reads(&log);
        let leaves = TreeRouter::new(terminal_warm.shards.clone(), std::num::NonZeroUsize::MIN)
            .leaves(&collection(), Requirement::Any)
            .await
            .unwrap();
        assert_eq!(
            leaves.iter().map(|leaf| &leaf.path).collect::<Vec<_>>(),
            [&node_path(0), &node_path(1), &node_path(4)]
        );
        assert!(
            leaves.iter().all(|leaf| !leaf.cache_hit),
            "a warm terminal leaf cannot erase an earlier miss"
        );
        assert_eq!(
            take_reads(&log),
            [
                read("read", root_path()),
                read("read", node_path(0)),
                read("read", node_path(1)),
            ]
        );
    }

    #[tokio::test]
    async fn interior_right_hop_preserves_visit_order_and_topology() {
        let (backend, log) = recording_backend();
        seed_stale_interior_parent(&store_over(backend.clone())).await;
        take_reads(&log);

        let s = store_over(backend);
        let router = TreeRouter::new(s.shards.clone(), std::num::NonZeroUsize::MIN);
        let loc = router
            .leaf_for(&collection(), b"pear", Requirement::Any)
            .await
            .unwrap();
        assert_eq!(loc.path, node_path(1));
        assert!(!loc.cache_hit);
        assert_eq!(
            take_reads(&log),
            [
                read("read", root_path()),
                read("read", node_path(2)),
                read("read", node_path(3)),
                read("read", node_path(1)),
            ]
        );

        assert!(
            router
                .token_reachable_at_key(&collection(), b"pear", &token(3), Requirement::Any)
                .await
                .unwrap()
        );
        let parent = router
            .parent_index_for(&collection(), b"pear", Requirement::Any)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(parent.path, node_path(3));
        assert!(matches!(parent.node().unwrap().body(), NodeBody::Index(_)));
        assert!(take_reads(&log).is_empty());
    }

    // ADR-031 P0 regression: a reader that cached the root as a leaf must
    // refresh it before routing after another reader rewrites it as an index.
    #[tokio::test]
    async fn fresh_leaf_lookup_refreshes_a_stale_root_before_routing() {
        let (backend, log) = recording_backend();
        let reader = store_over(backend.clone());
        let writer = store_over(backend);

        writer
            .create_root(
                &collection(),
                &Node::leaf(Shard::from_entries([live(b"apple"), live(b"pear")])),
            )
            .await
            .unwrap();
        let router = TreeRouter::new(reader.shards.clone(), std::num::NonZeroUsize::MIN);
        router
            .leaf_for(&collection(), b"pear", Requirement::Any)
            .await
            .unwrap();

        writer
            .store_node(
                &collection(),
                &token(0),
                &leaf(&[b"apple"], Some(b"m"), Some(&token(1))),
                None,
            )
            .await
            .unwrap();
        writer
            .store_node(
                &collection(),
                &token(1),
                &leaf(&[b"pear"], None, None),
                None,
            )
            .await
            .unwrap();
        let (_, version) = writer
            .load_root(&collection(), Requirement::AtLeast(writer.timeline.now()))
            .await
            .unwrap();
        assert!(
            writer
                .store_root(
                    &collection(),
                    &Node::index(IndexNode::from_children([
                        (Vec::new(), token(0).to_string()),
                        (b"m".to_vec(), token(1).to_string()),
                    ])),
                    &version,
                )
                .await
                .unwrap()
        );
        take_reads(&log);

        let bound = reader.timeline.now();
        let loc = router
            .leaf_for_fresh(
                &collection(),
                b"pear",
                Requirement::Any,
                Requirement::AtLeast(bound),
            )
            .await
            .unwrap();
        assert_eq!(loc.path, node_path(1));
        assert!(!loc.cache_hit);
        assert_fresh(&loc, bound);
        assert!(loc.node().unwrap().as_leaf().unwrap().exists(b"pear"));
        assert_eq!(
            take_reads(&log),
            [
                read("read_if_modified", root_path()),
                read("read", node_path(1)),
            ]
        );
    }

    #[tokio::test]
    async fn topology_queries_ignore_off_path_nodes_and_classify_dangling_links() {
        let (backend, log) = recording_backend();
        seed_two_level(&store_over(backend.clone())).await;
        take_reads(&log);
        let s = store_over(backend);
        assert!(
            !TreeRouter::new(s.shards.clone(), std::num::NonZeroUsize::MIN)
                .token_reachable_at_key(&collection(), b"pear", &token(0), Requirement::Any)
                .await
                .unwrap()
        );
        assert_eq!(
            take_reads(&log),
            [read("read", root_path()), read("read", node_path(1))],
            "reachability follows the key path and never reads the target directly"
        );

        let dangling = store();
        dangling
            .create_root(
                &collection(),
                &Node::index(IndexNode::from_children([(
                    Vec::new(),
                    token(9).to_string(),
                )])),
            )
            .await
            .unwrap();
        let router = TreeRouter::new(dangling.shards.clone(), std::num::NonZeroUsize::MIN);
        assert!(
            !router
                .token_reachable_at_key(&collection(), b"pear", &token(8), Requirement::Any)
                .await
                .unwrap()
        );
        assert!(matches!(
            router
                .parent_index_for(&collection(), b"pear", Requirement::Any)
                .await,
            Err(StorageError::NotFound)
        ));
    }

    #[tokio::test]
    async fn group_keys_by_leaf_routes_and_preserves_order() {
        let s = store();
        seed_two_level(&s).await;
        let router = TreeRouter::new(s.shards.clone(), std::num::NonZeroUsize::MIN);

        let groups = router
            .group_keys_by_leaf(
                [
                    (KeyRef::new(CollectionAddress::root("db"), b"cat"), 'c'),
                    (KeyRef::new(CollectionAddress::root("db"), b"mango"), 'm'),
                    (KeyRef::new(CollectionAddress::root("db"), b"apple"), 'a'),
                ],
                Requirement::AtLeast(s.timeline.now()),
            )
            .await
            .unwrap();

        assert_eq!(groups.len(), 2, "keys split across two leaves");
        let l0 = groups
            .iter()
            .find(|group| group.path() == &node_path(0))
            .unwrap();
        assert_eq!(
            l0.keys,
            vec![(b"cat".to_vec(), 'c'), (b"apple".to_vec(), 'a')],
            "same-leaf keys keep input order"
        );
        let l1 = groups
            .iter()
            .find(|group| group.path() == &node_path(1))
            .unwrap();
        assert_eq!(l1.keys, vec![(b"mango".to_vec(), 'm')]);
    }

    #[tokio::test]
    async fn path_batched_grouping_loads_each_shared_path_once() {
        let (backend, log) = recording_backend();
        seed_two_level(&store_over(backend.clone())).await;
        take_reads(&log);
        let cold = store_over(backend);
        let router = TreeRouter::new(cold.shards.clone(), NonZeroUsize::new(16).unwrap());

        let groups = router
            .group_keys_by_leaf(
                [
                    (KeyRef::new(collection(), b"pear"), 0),
                    (KeyRef::new(collection(), b"apple"), 1),
                    (KeyRef::new(collection(), b"mango"), 2),
                    (KeyRef::new(collection(), b"cat"), 3),
                ],
                Requirement::Any,
            )
            .await
            .unwrap();

        assert_eq!(groups.len(), 2);
        let reads = take_reads(&log);
        assert_eq!(reads.len(), 3);
        assert_eq!(
            reads
                .iter()
                .filter(|read| read.1 == root_path().to_string())
                .count(),
            1
        );
        assert_eq!(
            reads
                .iter()
                .filter(|read| read.1 == node_path(0).to_string())
                .count(),
            1
        );
        assert_eq!(
            reads
                .iter()
                .filter(|read| read.1 == node_path(1).to_string())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn one_grouped_key_uses_the_direct_descent_sequence() {
        let (backend, log) = recording_backend();
        seed_two_level(&store_over(backend.clone())).await;
        take_reads(&log);
        let cold = store_over(backend);

        let groups = TreeRouter::new(cold.shards.clone(), NonZeroUsize::new(16).unwrap())
            .group_keys_by_leaf([(KeyRef::new(collection(), b"pear"), ())], Requirement::Any)
            .await
            .unwrap();

        assert_eq!(groups.len(), 1);
        assert_eq!(
            take_reads(&log),
            [read("read", root_path()), read("read", node_path(1))]
        );
    }

    #[tokio::test]
    async fn grouped_routing_classifies_the_collection_that_failed() {
        let s = store();
        let router = TreeRouter::new(s.shards.clone(), std::num::NonZeroUsize::MIN);
        let root = CollectionAddress::root("db");
        let child = CollectionAddress::new("db", CollectionId::from_slice(&[1; 16]).unwrap());
        let requirement = Requirement::AtLeast(s.timeline.now());

        let root_error = router
            .group_keys_by_leaf(
                [
                    (KeyRef::new(root.clone(), b"root"), ()),
                    (KeyRef::new(child.clone(), b"child"), ()),
                ],
                requirement,
            )
            .await;
        assert!(matches!(root_error, Err(StorageError::NotFound)));

        let child_error = router
            .group_keys_by_leaf(
                [
                    (KeyRef::new(child, b"child"), ()),
                    (KeyRef::new(root, b"root"), ()),
                ],
                requirement,
            )
            .await;
        assert!(matches!(child_error, Err(StorageError::StaleCollection)));
    }
}
