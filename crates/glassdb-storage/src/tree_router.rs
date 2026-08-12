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

use std::collections::BTreeMap;

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
pub struct LeafGroup<T> {
    pub path: ObjectPath,
    pub observation: LeafObservation,
    pub keys: Vec<(Vec<u8>, T)>,
}

impl<T> LeafGroup<T> {
    /// Returns the observed node.
    pub fn node(&self) -> Option<&Node> {
        self.observation.value().map(AsRef::as_ref)
    }
}

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

/// Descends and scans a collection's B-link tree.
#[derive(Clone)]
pub struct TreeRouter {
    nodes: NodeStore,
}

impl TreeRouter {
    /// Creates a router that reads nodes through `nodes`.
    pub fn new(nodes: NodeStore) -> Self {
        TreeRouter { nodes }
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
        let path = ObjectPath::TreeRoot {
            collection: collection.clone(),
        };
        let observation = self.nodes.load_root_state(collection, requirement).await?;
        let cache_hit = observation.cache_hit();
        if observation.is_absent() {
            return Err(StorageError::NotFound);
        }
        let cur = Located {
            path,
            cache_hit,
            observation,
        };
        Ok(self
            .descend_to_leaf(collection, cur, key, requirement)
            .await?
            .into_locator())
    }

    /// Returns the existing leaf that owns `key`, or `None` when the collection
    /// does not exist.
    pub async fn first_leaf_at(
        &self,
        collection: &CollectionAddress,
        key: &[u8],
        requirement: Requirement,
    ) -> Result<Option<LeafLocator>, StorageError> {
        let path = ObjectPath::TreeRoot {
            collection: collection.clone(),
        };
        let observation = self.nodes.load_root_state(collection, requirement).await?;
        if observation.is_absent() {
            return Ok(None);
        }
        let cur = Located {
            path,
            cache_hit: observation.cache_hit(),
            observation,
        };
        Ok(Some(
            self.descend_to_leaf(collection, cur, key, requirement)
                .await?
                .into_locator(),
        ))
    }

    /// Returns the right sibling of `leaf`, or `None` for the rightmost leaf.
    pub async fn next_leaf(
        &self,
        collection: &CollectionAddress,
        leaf: &LeafLocator,
        requirement: Requirement,
    ) -> Result<Option<LeafLocator>, StorageError> {
        let Some(token) = leaf.node().and_then(Node::right_sibling) else {
            return Ok(None);
        };
        let token = node_token(token)?;
        Ok(Some(
            self.load_child(collection, &token, requirement)
                .await?
                .after(leaf.cache_hit)
                .into_locator(),
        ))
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
        let Some(mut leaf) = self.first_leaf_at(collection, start, requirement).await? else {
            return Err(StorageError::NotFound);
        };
        let mut out = Vec::new();
        loop {
            let done = end.is_some_and(|end| leaf.node().is_some_and(|node| node.owns(end)));
            let next = if done {
                None
            } else {
                self.next_leaf(collection, &leaf, requirement).await?
            };
            out.push(leaf);
            match next {
                Some(right) => leaf = right,
                None => return Ok(out),
            }
        }
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
        let loc = self.leaf_for(collection, key, interior).await?;
        // The same requirement needs no terminal refresh.
        if interior == leaf {
            return Ok(loc);
        }
        // Check the terminal node at the stricter requirement and resume the
        // descent from it: the cached interior read may have routed us to `_r`
        // as a leaf while a concurrent split has since rewritten `_r` into an
        // index (or split the leaf), so we must keep descending — never hand
        // back an index masquerading as a leaf.
        let located = self.reload(&loc.path, leaf).await?.after(loc.cache_hit);
        Ok(self
            .descend_to_leaf(collection, located, key, leaf)
            .await?
            .into_locator())
    }

    /// Returns the leftmost leaf of the collection, or `None` if the collection
    /// does not exist. The entry point for an ordered/range scan.
    pub async fn leftmost_leaf(
        &self,
        collection: &CollectionAddress,
        requirement: Requirement,
    ) -> Result<Option<LeafLocator>, StorageError> {
        let path = ObjectPath::TreeRoot {
            collection: collection.clone(),
        };
        let observation = self.nodes.load_root_state(collection, requirement).await?;
        if observation.is_absent() {
            return Ok(None);
        }
        let mut cur = Located {
            path,
            cache_hit: observation.cache_hit(),
            observation,
        };
        loop {
            match cur.node().body() {
                NodeBody::Leaf(_) => return Ok(Some(cur.into_locator())),
                NodeBody::Index(index) => {
                    let token = index
                        .children()
                        .next()
                        .map(|(_, token)| node_token(token))
                        .transpose()?
                        .ok_or_else(|| {
                            StorageError::other("descent reached an empty index node")
                        })?;
                    let cache_hit = cur.cache_hit;
                    cur = self
                        .load_child(collection, &token, requirement)
                        .await?
                        .after(cache_hit);
                }
            }
        }
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
        let mut out = Vec::new();
        let mut cur = first;
        loop {
            let next = cur
                .node()
                .and_then(Node::right_sibling)
                .map(node_token)
                .transpose()?;
            let cache_hit = cur.cache_hit;
            out.push(cur);
            match next {
                Some(token) => {
                    cur = self
                        .load_child(collection, &token, requirement)
                        .await?
                        .after(cache_hit)
                        .into_locator()
                }
                None => return Ok(out),
            }
        }
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
    ) -> Result<Vec<LeafGroup<T>>, StorageError> {
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
    ) -> Result<Vec<LeafGroup<T>>, StorageError> {
        let mut groups: BTreeMap<ObjectPath, LeafGroup<T>> = BTreeMap::new();
        for (key, payload) in items {
            let raw_key = key.key().to_vec();
            let loc = self
                .leaf_for_fresh(key.collection(), &raw_key, interior, leaf)
                .await
                .map_err(|error| error.classify_collection_absence(key.collection()))?;
            groups
                .entry(loc.path.clone())
                .or_insert_with(|| LeafGroup {
                    path: loc.path,
                    observation: loc.observation,
                    keys: Vec::new(),
                })
                .keys
                .push((raw_key, payload));
        }
        Ok(groups.into_values().collect())
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
        let observation = self.nodes.load_root_state(collection, requirement).await?;
        if observation.is_absent() {
            return Ok(false);
        }
        let target_path = ObjectPath::Node {
            collection: collection.clone(),
            token: target.clone(),
        };
        let mut cur = Located {
            path: ObjectPath::TreeRoot {
                collection: collection.clone(),
            },
            cache_hit: observation.cache_hit(),
            observation,
        };
        loop {
            cur = match self
                .step_right_until_owns(collection, cur, key, requirement)
                .await
            {
                Ok(cur) => cur,
                Err(StorageError::NotFound) => return Ok(false),
                Err(error) => return Err(error),
            };
            if cur.path == target_path {
                return Ok(true);
            }
            let token = match cur.node().body() {
                NodeBody::Leaf(_) => return Ok(false),
                NodeBody::Index(index) => {
                    node_token(index.child_for(key).ok_or_else(|| {
                        StorageError::other("descent reached an empty index node")
                    })?)?
                }
            };
            cur = match self.load_child(collection, &token, requirement).await {
                Ok(child) => child.after(cur.cache_hit),
                Err(StorageError::NotFound) => return Ok(false),
                Err(error) => return Err(error),
            };
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
        let observation = self.nodes.load_root_state(collection, requirement).await?;
        if observation.is_absent() {
            return Ok(None);
        }
        let mut cur = Located {
            path: ObjectPath::TreeRoot {
                collection: collection.clone(),
            },
            cache_hit: observation.cache_hit(),
            observation,
        };
        let mut parent: Option<Located> = None;
        loop {
            cur = self
                .step_right_until_owns(collection, cur, key, requirement)
                .await?;
            let token = match cur.node().body() {
                NodeBody::Leaf(_) => return Ok(parent.map(Located::into_locator)),
                NodeBody::Index(index) => {
                    node_token(index.child_for(key).ok_or_else(|| {
                        StorageError::other("descent reached an empty index node")
                    })?)?
                }
            };
            let child = self
                .load_child(collection, &token, requirement)
                .await?
                .after(cur.cache_hit);
            parent = Some(cur);
            cur = child;
        }
    }

    /// Descends from `cur` to the leaf that owns `key`: at each level step right
    /// to the owning node, then follow the index child pointer, until a leaf is
    /// reached. Self-correcting through right-links, so a stale interior read
    /// never traps the descent at the wrong node — and, crucially, a node that
    /// turns out to be an index (e.g. a freshly checked `_r` that split into one) is
    /// resolved to its child rather than returned as a leaf.
    async fn descend_to_leaf(
        &self,
        collection: &CollectionAddress,
        mut cur: Located,
        key: &[u8],
        requirement: Requirement,
    ) -> Result<Located, StorageError> {
        loop {
            cur = self
                .step_right_until_owns(collection, cur, key, requirement)
                .await?;
            match cur.node().body() {
                NodeBody::Leaf(_) => return Ok(cur),
                NodeBody::Index(index) => {
                    let token = node_token(index.child_for(key).ok_or_else(|| {
                        StorageError::other("descent reached an empty index node")
                    })?)?;
                    let cache_hit = cur.cache_hit;
                    cur = self
                        .load_child(collection, &token, requirement)
                        .await?
                        .after(cache_hit);
                }
            }
        }
    }

    /// Follows right-sibling links until the current node owns `key` (its
    /// high-key is above `key`). The rightmost node owns everything up to
    /// +infinity, so a node with no right sibling always terminates the walk.
    async fn step_right_until_owns(
        &self,
        collection: &CollectionAddress,
        mut cur: Located,
        key: &[u8],
        requirement: Requirement,
    ) -> Result<Located, StorageError> {
        while !cur.node().owns(key) {
            match cur.node().right_sibling() {
                Some(token) => {
                    let token = node_token(token)?;
                    let cache_hit = cur.cache_hit;
                    cur = self
                        .load_child(collection, &token, requirement)
                        .await?
                        .after(cache_hit);
                }
                None => break,
            }
        }
        Ok(cur)
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

    /// Re-reads the node at `path` (the root `_r` or a standalone `_n`) at
    /// `requirement`, for checking a terminal leaf reached through a cached
    /// interior descent.
    async fn reload(
        &self,
        path: &ObjectPath,
        requirement: Requirement,
    ) -> Result<Located, StorageError> {
        let observation = self.nodes.load_node_at_state(path, requirement).await?;
        if observation.is_absent() {
            return Err(StorageError::other("tree node vanished during descent"));
        }
        Ok(Located {
            path: path.clone(),
            cache_hit: observation.cache_hit(),
            observation,
        })
    }
}

fn node_token(token: &str) -> Result<NodeToken, StorageError> {
    NodeToken::try_from(token)
        .map_err(|error| StorageError::with_source("invalid node reference", error))
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
    use crate::shard::Shard;
    use crate::shard::ShardEntry;
    use crate::shard_store::ShardStore;

    const COLL_PREFIX: &str = "db/_c/0000000000000000000000";

    #[derive(Clone)]
    struct TestStore {
        shards: ShardStore,
        timeline: Timeline,
    }

    impl std::ops::Deref for TestStore {
        type Target = ShardStore;

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
        let shards = ShardStore::new(objects);
        TestStore { shards, timeline }
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

    // Seeds a two-level tree: root index -> {L0 (apple,cat), L1 (mango,pear)},
    // split at "m", with the leaves chained by right-sibling.
    async fn seed_two_level(s: &ShardStore) {
        let left = token(0);
        let right = token(1);
        s.store_node(
            &collection(),
            &left,
            &leaf(&[b"apple", b"cat"], Some(b"m"), Some(&right)),
            None,
        )
        .await
        .unwrap();
        s.store_node(
            &collection(),
            &right,
            &leaf(&[b"mango", b"pear"], None, None),
            None,
        )
        .await
        .unwrap();
        let root = Node::index(IndexNode::from_children([
            (b"".to_vec(), left.to_string()),
            (b"m".to_vec(), right.to_string()),
        ]));
        s.create_root(&collection(), &root).await.unwrap();
    }

    // Models a leaf split whose parent is stale: R still routes every key to
    // L0, while L0's right-link moves keys at and above "m" to L1.
    async fn seed_stale_leaf_parent(s: &ShardStore) {
        let left = token(0);
        let right = token(1);
        s.store_node(
            &collection(),
            &left,
            &leaf(&[b"apple", b"cat"], Some(b"m"), Some(&right)),
            None,
        )
        .await
        .unwrap();
        s.store_node(
            &collection(),
            &right,
            &leaf(&[b"mango", b"pear"], None, None),
            None,
        )
        .await
        .unwrap();
        s.create_root(
            &collection(),
            &Node::index(IndexNode::from_children([(Vec::new(), left.to_string())])),
        )
        .await
        .unwrap();
    }

    // Three leaves behind one stale parent exercise both a bounded scan and
    // the leaf-to-leaf interface without involving another descent shape.
    async fn seed_three_leaf_chain(s: &ShardStore) {
        let first = token(0);
        let middle = token(1);
        let last = token(4);
        s.store_node(
            &collection(),
            &first,
            &leaf(&[b"apple"], Some(b"m"), Some(&middle)),
            None,
        )
        .await
        .unwrap();
        s.store_node(
            &collection(),
            &middle,
            &leaf(&[b"mango"], Some(b"t"), Some(&last)),
            None,
        )
        .await
        .unwrap();
        s.store_node(&collection(), &last, &leaf(&[b"zebra"], None, None), None)
            .await
            .unwrap();
        s.create_root(
            &collection(),
            &Node::index(IndexNode::from_children([(Vec::new(), first.to_string())])),
        )
        .await
        .unwrap();
    }

    // Models an interior split whose parent is stale: R still routes to I0,
    // whose right-link moves the lookup to I1 before descending to L1.
    async fn seed_stale_interior_parent(s: &ShardStore) {
        let interior_left = token(2);
        let interior_right = token(3);
        let leaf_left = token(0);
        let leaf_right = token(1);
        s.store_node(
            &collection(),
            &leaf_left,
            &leaf(&[b"apple"], Some(b"m"), Some(&leaf_right)),
            None,
        )
        .await
        .unwrap();
        s.store_node(
            &collection(),
            &leaf_right,
            &leaf(&[b"pear"], None, None),
            None,
        )
        .await
        .unwrap();
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

    #[derive(Clone, Copy, Debug)]
    enum PointEntry {
        Leaf,
        First,
        Fresh,
        Group,
        GroupFresh,
        Reachable,
        Parent,
    }

    struct PointResult {
        path: ObjectPath,
        cache_hit: Option<bool>,
        current_after: crate::SequencePoint,
        is_leaf: Option<bool>,
        contains_key: Option<bool>,
    }

    fn point_observation(node: &Node, key: &[u8]) -> (bool, Option<bool>) {
        match node.body() {
            NodeBody::Leaf(shard) => (true, Some(shard.exists(key))),
            NodeBody::Index(_) => (false, None),
        }
    }

    fn assert_point_observation(entry: PointEntry, result: &PointResult) {
        match entry {
            PointEntry::Reachable => {
                assert_eq!(result.is_leaf, None);
                assert_eq!(result.contains_key, None);
            }
            PointEntry::Parent => {
                assert_eq!(result.is_leaf, Some(false), "parent must be an index");
                assert_eq!(result.contains_key, None);
            }
            _ => {
                assert_eq!(result.is_leaf, Some(true), "route must end at a leaf");
                assert_eq!(
                    result.contains_key,
                    Some(true),
                    "routed leaf must contain pear"
                );
            }
        }
    }

    async fn route_point(
        router: &TreeRouter,
        entry: PointEntry,
        requirement: Requirement,
    ) -> PointResult {
        let key = b"pear";
        match entry {
            PointEntry::Leaf => {
                let loc = router
                    .leaf_for(&collection(), key, requirement)
                    .await
                    .unwrap();
                let (is_leaf, contains_key) = point_observation(loc.node().unwrap(), key);
                PointResult {
                    path: loc.path,
                    cache_hit: Some(loc.cache_hit),
                    current_after: loc.observation.current_after(),
                    is_leaf: Some(is_leaf),
                    contains_key,
                }
            }
            PointEntry::First => {
                let loc = router
                    .first_leaf_at(&collection(), key, requirement)
                    .await
                    .unwrap()
                    .unwrap();
                let (is_leaf, contains_key) = point_observation(loc.node().unwrap(), key);
                PointResult {
                    path: loc.path,
                    cache_hit: Some(loc.cache_hit),
                    current_after: loc.observation.current_after(),
                    is_leaf: Some(is_leaf),
                    contains_key,
                }
            }
            PointEntry::Fresh => {
                let loc = router
                    .leaf_for_fresh(&collection(), key, Requirement::Any, requirement)
                    .await
                    .unwrap();
                let (is_leaf, contains_key) = point_observation(loc.node().unwrap(), key);
                PointResult {
                    path: loc.path,
                    cache_hit: Some(loc.cache_hit),
                    current_after: loc.observation.current_after(),
                    is_leaf: Some(is_leaf),
                    contains_key,
                }
            }
            PointEntry::Group | PointEntry::GroupFresh => {
                let items = [(KeyRef::new(collection(), key), ())];
                let groups = match entry {
                    PointEntry::Group => router.group_keys_by_leaf(items, requirement).await,
                    PointEntry::GroupFresh => {
                        router
                            .group_keys_by_leaf_fresh(items, Requirement::Any, requirement)
                            .await
                    }
                    _ => unreachable!(),
                }
                .unwrap();
                let (is_leaf, contains_key) = point_observation(groups[0].node().unwrap(), key);
                PointResult {
                    path: groups[0].path.clone(),
                    cache_hit: None,
                    current_after: groups[0].observation.current_after(),
                    is_leaf: Some(is_leaf),
                    contains_key,
                }
            }
            PointEntry::Reachable => {
                assert!(
                    router
                        .token_reachable_at_key(&collection(), key, &token(1), requirement)
                        .await
                        .unwrap()
                );
                PointResult {
                    path: node_path(1),
                    cache_hit: None,
                    current_after: crate::SequencePoint::default(),
                    is_leaf: None,
                    contains_key: None,
                }
            }
            PointEntry::Parent => {
                let loc = router
                    .parent_index_for(&collection(), key, requirement)
                    .await
                    .unwrap()
                    .unwrap();
                let (is_leaf, contains_key) = point_observation(loc.node().unwrap(), key);
                PointResult {
                    path: loc.path,
                    cache_hit: Some(loc.cache_hit),
                    current_after: loc.observation.current_after(),
                    is_leaf: Some(is_leaf),
                    contains_key,
                }
            }
        }
    }

    async fn assert_point_matrix(
        backend: Arc<dyn Backend>,
        log: &OpLog,
        expected_trace: &[(String, String)],
        expected_parent: ObjectPath,
    ) {
        for entry in [
            PointEntry::Leaf,
            PointEntry::First,
            PointEntry::Fresh,
            PointEntry::Group,
            PointEntry::GroupFresh,
            PointEntry::Reachable,
            PointEntry::Parent,
        ] {
            let s = store_over(backend.clone());
            let router = TreeRouter::new(s.shards.nodes().clone());
            let cold = route_point(&router, entry, Requirement::Any).await;
            assert_eq!(
                cold.path,
                if matches!(entry, PointEntry::Parent) {
                    expected_parent.clone()
                } else {
                    node_path(1)
                }
            );
            assert_point_observation(entry, &cold);
            assert_eq!(cold.cache_hit, cold.cache_hit.map(|_| false));
            assert_eq!(
                take_reads(log),
                expected_trace,
                "cold {entry:?} traversal trace"
            );

            let warm = route_point(&router, entry, Requirement::Any).await;
            assert_point_observation(entry, &warm);
            assert_eq!(warm.cache_hit, warm.cache_hit.map(|_| true));
            assert!(take_reads(log).is_empty(), "warm Any {entry:?} traversal");

            let bound = s.timeline.now();
            let fresh = route_point(&router, entry, Requirement::AtLeast(bound)).await;
            assert_point_observation(entry, &fresh);
            assert_eq!(fresh.cache_hit, fresh.cache_hit.map(|_| true));
            if !matches!(entry, PointEntry::Reachable) {
                assert!(fresh.current_after >= bound);
            }
            let expected = if matches!(entry, PointEntry::Fresh | PointEntry::GroupFresh) {
                vec![read("read_if_modified", node_path(1))]
            } else {
                expected_trace
                    .iter()
                    .map(|(_, path)| ("read_if_modified".to_string(), path.clone()))
                    .collect()
            };
            assert_eq!(
                take_reads(log),
                expected,
                "fresh {entry:?} traversal checks the required path"
            );
        }
    }

    #[tokio::test]
    async fn single_leaf_collection_resolves_to_root() {
        let s = store();
        let root = Node::leaf(Shard::from_entries([live(b"only")]));
        s.create_root(&collection(), &root).await.unwrap();

        let router = TreeRouter::new(s.shards.nodes().clone());
        let loc = router
            .leaf_for(
                &collection(),
                b"only",
                Requirement::AtLeast(s.timeline.now()),
            )
            .await
            .unwrap();
        assert_eq!(
            loc.path,
            ObjectPath::TreeRoot {
                collection: collection()
            }
        );
        assert!(!loc.observation.is_absent());
        assert!(loc.node().unwrap().as_leaf().unwrap().exists(b"only"));
    }

    #[tokio::test]
    async fn absent_collection_is_not_a_writable_empty_leaf() {
        let s = store();
        let router = TreeRouter::new(s.shards.nodes().clone());
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
    }

    #[tokio::test]
    async fn descends_index_to_correct_leaf() {
        let s = store();
        seed_two_level(&s).await;
        let router = TreeRouter::new(s.shards.nodes().clone());

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
    async fn stale_leaf_parent_entry_point_matrix_preserves_paths_hits_and_freshness() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        seed_stale_leaf_parent(&store_over(backend.clone())).await;
        take_reads(&log);
        let trace = [
            read("read", root_path()),
            read("read", node_path(0)),
            read("read", node_path(1)),
        ];
        assert_point_matrix(backend.clone(), &log, &trace, root_path()).await;

        // Warm only the prefix to prove aggregate hits remain false when the
        // stale-parent right hop itself is cold.
        let s = store_over(backend.clone());
        s.load_root_state(&collection(), Requirement::Any)
            .await
            .unwrap();
        s.load_node_state(&collection(), &token(0), Requirement::Any)
            .await
            .unwrap();
        take_reads(&log);
        let mixed = TreeRouter::new(s.shards.nodes().clone())
            .leaf_for(&collection(), b"pear", Requirement::Any)
            .await
            .unwrap();
        assert!(!mixed.cache_hit);
        assert_eq!(take_reads(&log), [read("read", node_path(1))]);

        // Invert the cache shape to diagnose aggregation independently of the
        // terminal read: the warm terminal cannot hide cold prefix misses.
        let s = store_over(backend);
        s.load_node_state(&collection(), &token(1), Requirement::Any)
            .await
            .unwrap();
        take_reads(&log);
        let mixed = TreeRouter::new(s.shards.nodes().clone())
            .leaf_for(&collection(), b"pear", Requirement::Any)
            .await
            .unwrap();
        assert!(!mixed.cache_hit);
        assert_eq!(
            take_reads(&log),
            [read("read", root_path()), read("read", node_path(0))]
        );

        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        seed_three_leaf_chain(&store_over(backend.clone())).await;
        take_reads(&log);

        let scan = store_over(backend.clone());
        let scan_router = TreeRouter::new(scan.shards.nodes().clone());
        let leaves = scan_router
            .leaves(&collection(), Requirement::Any)
            .await
            .unwrap();
        assert_eq!(
            leaves.iter().map(|leaf| &leaf.path).collect::<Vec<_>>(),
            [&node_path(0), &node_path(1), &node_path(4)]
        );
        assert!(leaves.iter().all(|leaf| !leaf.cache_hit));
        assert_eq!(
            take_reads(&log),
            [
                read("read", root_path()),
                read("read", node_path(0)),
                read("read", node_path(1)),
                read("read", node_path(4)),
            ]
        );
        let warm = scan_router
            .leaves(&collection(), Requirement::Any)
            .await
            .unwrap();
        assert!(warm.iter().all(|leaf| leaf.cache_hit));
        assert!(take_reads(&log).is_empty());

        let bounded = store_over(backend.clone());
        let bounded_router = TreeRouter::new(bounded.shards.nodes().clone());
        let leaves = bounded_router
            .leaves_through(&collection(), b"apple", Some(b"mango"), Requirement::Any)
            .await
            .unwrap();
        assert_eq!(
            leaves.iter().map(|leaf| &leaf.path).collect::<Vec<_>>(),
            [&node_path(0), &node_path(1)]
        );
        assert_eq!(
            take_reads(&log),
            [
                read("read", root_path()),
                read("read", node_path(0)),
                read("read", node_path(1)),
            ],
            "the inclusive middle bound avoids reading the following leaf"
        );
        let warm = bounded_router
            .leaves_through(&collection(), b"apple", Some(b"mango"), Requirement::Any)
            .await
            .unwrap();
        assert!(warm.iter().all(|leaf| leaf.cache_hit));
        assert!(take_reads(&log).is_empty());

        // `next_leaf` begins at a retained locator, so root/parent descent
        // topologies are not applicable; characterize its stale right-link,
        // freshness, cumulative-hit, and malformed-link behavior directly.
        let next = store_over(backend);
        let router = TreeRouter::new(next.shards.nodes().clone());
        let first = router
            .first_leaf_at(&collection(), b"apple", Requirement::Any)
            .await
            .unwrap()
            .unwrap();
        take_reads(&log);
        let middle = router
            .next_leaf(&collection(), &first, Requirement::Any)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(middle.path, node_path(1));
        assert!(
            !middle.cache_hit,
            "the cold input keeps the cumulative hit false"
        );
        assert_eq!(take_reads(&log), [read("read", node_path(1))]);

        let middle = router
            .next_leaf(&collection(), &first, Requirement::Any)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(middle.path, node_path(1));
        assert!(
            !middle.cache_hit,
            "a warm sibling cannot erase the retained input miss"
        );
        assert!(take_reads(&log).is_empty());

        let first = router
            .first_leaf_at(&collection(), b"apple", Requirement::Any)
            .await
            .unwrap()
            .unwrap();
        assert!(first.cache_hit);
        take_reads(&log);
        let bound = next.timeline.now();
        let middle = router
            .next_leaf(&collection(), &first, Requirement::AtLeast(bound))
            .await
            .unwrap()
            .unwrap();
        assert!(middle.cache_hit);
        assert_fresh(&middle, bound);
        assert_eq!(take_reads(&log), [read("read_if_modified", node_path(1))]);

        let invalid_backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let invalid = store_over(invalid_backend);
        invalid
            .create_root(
                &collection(),
                &Node::leaf(Shard::from_entries([live(b"apple")]))
                    .with_right_sibling(Some("invalid".to_string())),
            )
            .await
            .unwrap();
        let router = TreeRouter::new(invalid.shards.nodes().clone());
        let leaf = router
            .leaf_for(&collection(), b"apple", Requirement::Any)
            .await
            .unwrap();
        assert!(matches!(
            router
                .next_leaf(&collection(), &leaf, Requirement::Any)
                .await,
            Err(StorageError::Other { .. })
        ));
    }

    #[tokio::test]
    async fn interior_right_hop_entry_point_matrix_preserves_visit_order() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        seed_stale_interior_parent(&store_over(backend.clone())).await;
        take_reads(&log);
        let trace = [
            read("read", root_path()),
            read("read", node_path(2)),
            read("read", node_path(3)),
            read("read", node_path(1)),
        ];
        assert_point_matrix(backend.clone(), &log, &trace, node_path(3)).await;

        let s = store_over(backend);
        let leaves = TreeRouter::new(s.shards.nodes().clone())
            .leaves_through(&collection(), b"pear", Some(b"pear"), Requirement::Any)
            .await
            .unwrap();
        assert_eq!(leaves[0].path, node_path(1));
        assert!(!leaves[0].cache_hit);
        assert_eq!(take_reads(&log), trace);
    }

    // ADR-031 P0 regression: a reader that cached the root as a leaf must
    // refresh it before routing after another reader rewrites it as an index.
    #[tokio::test]
    async fn stale_root_entry_point_matrix_refreshes_before_routing() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        let entries = [
            PointEntry::Leaf,
            PointEntry::First,
            PointEntry::Fresh,
            PointEntry::Group,
            PointEntry::GroupFresh,
            PointEntry::Reachable,
            PointEntry::Parent,
        ];
        let readers = (0..entries.len() + 3)
            .map(|_| store_over(backend.clone()))
            .collect::<Vec<_>>();
        let writer = store_over(backend);

        writer
            .create_root(
                &collection(),
                &Node::leaf(Shard::from_entries([live(b"apple"), live(b"pear")])),
            )
            .await
            .unwrap();
        for reader in &readers {
            TreeRouter::new(reader.shards.nodes().clone())
                .leaf_for(&collection(), b"pear", Requirement::Any)
                .await
                .unwrap();
        }

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

        let expected = [
            read("read_if_modified", root_path()),
            read("read", node_path(1)),
        ];
        for (entry, reader) in entries.iter().copied().zip(&readers) {
            let bound = reader.timeline.now();
            let routed = route_point(
                &TreeRouter::new(reader.shards.nodes().clone()),
                entry,
                Requirement::AtLeast(bound),
            )
            .await;
            assert_eq!(
                routed.path,
                if matches!(entry, PointEntry::Parent) {
                    root_path()
                } else {
                    node_path(1)
                }
            );
            assert_point_observation(entry, &routed);
            assert_eq!(routed.cache_hit, routed.cache_hit.map(|_| false));
            if !matches!(entry, PointEntry::Reachable) {
                assert!(routed.current_after >= bound);
            }
            assert_eq!(
                take_reads(&log),
                expected,
                "stale-root {entry:?} traversal trace"
            );
        }

        let reader = &readers[entries.len()];
        let bound = reader.timeline.now();
        let leftmost = TreeRouter::new(reader.shards.nodes().clone())
            .leftmost_leaf(&collection(), Requirement::AtLeast(bound))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(leftmost.path, node_path(0));
        assert!(!leftmost.cache_hit);
        assert_fresh(&leftmost, bound);
        assert_eq!(
            take_reads(&log),
            [
                read("read_if_modified", root_path()),
                read("read", node_path(0))
            ]
        );

        for (offset, through) in [(1, true), (2, false)] {
            let reader = &readers[entries.len() + offset];
            let bound = reader.timeline.now();
            let requirement = Requirement::AtLeast(bound);
            let router = TreeRouter::new(reader.shards.nodes().clone());
            let leaves = if through {
                router
                    .leaves_through(&collection(), b"apple", Some(b"pear"), requirement)
                    .await
            } else {
                router.leaves(&collection(), requirement).await
            }
            .unwrap();
            assert_eq!(
                leaves.iter().map(|leaf| &leaf.path).collect::<Vec<_>>(),
                [&node_path(0), &node_path(1)]
            );
            assert!(leaves.iter().all(|leaf| !leaf.cache_hit));
            assert!(
                leaves
                    .iter()
                    .all(|leaf| leaf.observation.current_after() >= bound)
            );
            assert_eq!(
                take_reads(&log),
                [
                    read("read_if_modified", root_path()),
                    read("read", node_path(0)),
                    read("read", node_path(1)),
                ]
            );
        }
    }
    #[tokio::test]
    async fn parent_index_for_is_none_for_single_leaf() {
        let single = store();
        let root = Node::leaf(Shard::from_entries([live(b"only")]));
        single.create_root(&collection(), &root).await.unwrap();
        let single_dir = TreeRouter::new(single.shards.nodes().clone());
        assert!(
            single_dir
                .parent_index_for(
                    &collection(),
                    b"only",
                    Requirement::AtLeast(single.timeline.now()),
                )
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn group_keys_by_leaf_routes_and_preserves_order() {
        let s = store();
        seed_two_level(&s).await;
        let router = TreeRouter::new(s.shards.nodes().clone());
        assert_eq!(CollectionAddress::root("db").physical_prefix(), COLL_PREFIX);

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
            .find(|group| group.path == node_path(0))
            .unwrap();
        assert_eq!(
            l0.keys,
            vec![(b"cat".to_vec(), 'c'), (b"apple".to_vec(), 'a')],
            "same-leaf keys keep input order"
        );
        let l1 = groups
            .iter()
            .find(|group| group.path == node_path(1))
            .unwrap();
        assert_eq!(l1.keys, vec![(b"mango".to_vec(), 'm')]);
    }

    #[tokio::test]
    async fn grouped_routing_classifies_the_collection_that_failed() {
        let s = store();
        let router = TreeRouter::new(s.shards.nodes().clone());
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
