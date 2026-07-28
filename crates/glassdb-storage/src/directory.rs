//! Tree descent over the B-link coordination directory (ADR-031).
//!
//! The [`Directory`] resolves a key to the leaf that owns it by descending from
//! the collection root `_r` through index nodes, and it enumerates the leaves
//! in key order for listing. Descent is **self-correcting**: every node carries
//! a high-key and a right-sibling, so a lookup that lands too far left —
//! because a split moved the key rightward after the cache was taken — steps
//! along the right-sibling link (the B-link property) instead of restarting
//! from the root.
//!
//! This layer is pure routing: it reads nodes through the [`ShardStore`] (hence
//! the decoded object store, so interior nodes stay cached and off the hot path) and
//! never mutates the tree. Splitting and locking live above it.

use std::collections::BTreeMap;

use glassdb_data::KeyRef;
use glassdb_data::paths;

use crate::cached_store::Requirement;
use crate::error::StorageError;
use crate::node::{Node, NodeBody};
use crate::shard_store::{LeafObservation, ShardStore};

/// The leaf that owns a key (or range endpoint), with everything needed to read
/// or compare-and-swap it: its object `path` and retained physical observation.
#[derive(Debug, Clone)]
pub struct LeafLocator {
    pub path: String,
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

/// A group of keys routed to one leaf by [`Directory::group_keys_by_leaf`]: the
/// owning leaf and the raw keys (with their payloads) that landed in it.
pub struct LeafGroup<T> {
    pub path: String,
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
    path: String,
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

/// Descends and scans the B-link coordination directory of a collection.
#[derive(Clone)]
pub struct Directory {
    shards: ShardStore,
}

impl Directory {
    /// Creates a directory that reads nodes through `shards`.
    pub fn new(shards: ShardStore) -> Self {
        Directory { shards }
    }

    /// Resolves the leaf that owns `key`, descending from the root `_r` and
    /// following right-sibling links to self-correct past in-progress splits.
    ///
    /// A missing collection root is reported as [`StorageError::NotFound`].
    pub async fn leaf_for(
        &self,
        prefix: &str,
        key: &[u8],
        requirement: Requirement,
    ) -> Result<LeafLocator, StorageError> {
        let path = paths::tree_root(prefix);
        let observation = self.shards.load_root_state(prefix, requirement).await?;
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
            .descend_to_leaf(prefix, cur, key, requirement)
            .await?
            .into_locator())
    }

    /// Returns the existing leaf that owns `key`, or `None` when the collection
    /// does not exist.
    pub async fn first_leaf_at(
        &self,
        prefix: &str,
        key: &[u8],
        requirement: Requirement,
    ) -> Result<Option<LeafLocator>, StorageError> {
        let path = paths::tree_root(prefix);
        let observation = self.shards.load_root_state(prefix, requirement).await?;
        if observation.is_absent() {
            return Ok(None);
        }
        let cur = Located {
            path,
            cache_hit: observation.cache_hit(),
            observation,
        };
        Ok(Some(
            self.descend_to_leaf(prefix, cur, key, requirement)
                .await?
                .into_locator(),
        ))
    }

    /// Returns the right sibling of `leaf`, or `None` for the rightmost leaf.
    pub async fn next_leaf(
        &self,
        prefix: &str,
        leaf: &LeafLocator,
        requirement: Requirement,
    ) -> Result<Option<LeafLocator>, StorageError> {
        let Some(token) = leaf.node().and_then(Node::right_sibling) else {
            return Ok(None);
        };
        Ok(Some(
            self.load_child(prefix, token, requirement)
                .await?
                .after(leaf.cache_hit)
                .into_locator(),
        ))
    }

    /// Returns the leaves from the one owning `start` through the one owning
    /// the inclusive `end`; `None` scans through positive infinity.
    pub async fn leaves_through(
        &self,
        prefix: &str,
        start: &[u8],
        end: Option<&[u8]>,
        requirement: Requirement,
    ) -> Result<Vec<LeafLocator>, StorageError> {
        let Some(mut leaf) = self.first_leaf_at(prefix, start, requirement).await? else {
            return Err(StorageError::NotFound);
        };
        let mut out = Vec::new();
        loop {
            let done = end.is_some_and(|end| leaf.node().is_some_and(|node| node.owns(end)));
            let next = if done {
                None
            } else {
                self.next_leaf(prefix, &leaf, requirement).await?
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
        prefix: &str,
        key: &[u8],
        interior: Requirement,
        leaf: Requirement,
    ) -> Result<LeafLocator, StorageError> {
        let loc = self.leaf_for(prefix, key, interior).await?;
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
            .descend_to_leaf(prefix, located, key, leaf)
            .await?
            .into_locator())
    }

    /// Returns the leftmost leaf of the collection, or `None` if the collection
    /// does not exist. The entry point for an ordered/range scan.
    pub async fn leftmost_leaf(
        &self,
        prefix: &str,
        requirement: Requirement,
    ) -> Result<Option<LeafLocator>, StorageError> {
        let path = paths::tree_root(prefix);
        let observation = self.shards.load_root_state(prefix, requirement).await?;
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
                        .map(|(_, c)| c.to_string())
                        .ok_or_else(|| {
                            StorageError::other("descent reached an empty index node")
                        })?;
                    let cache_hit = cur.cache_hit;
                    cur = self
                        .load_child(prefix, &token, requirement)
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
        prefix: &str,
        requirement: Requirement,
    ) -> Result<Vec<LeafLocator>, StorageError> {
        let Some(first) = self.leftmost_leaf(prefix, requirement).await? else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        let mut cur = first;
        loop {
            let next = cur.node().and_then(Node::right_sibling).map(str::to_string);
            let cache_hit = cur.cache_hit;
            out.push(cur);
            match next {
                Some(token) => {
                    cur = self
                        .load_child(prefix, &token, requirement)
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
        let mut groups: BTreeMap<String, LeafGroup<T>> = BTreeMap::new();
        for (key, payload) in items {
            let prefix = key.collection().physical_prefix();
            let raw_key = key.key().to_vec();
            let loc = self
                .leaf_for_fresh(&prefix, &raw_key, interior, leaf)
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
        prefix: &str,
        key: &[u8],
        target: &str,
        requirement: Requirement,
    ) -> Result<bool, StorageError> {
        let observation = self.shards.load_root_state(prefix, requirement).await?;
        if observation.is_absent() {
            return Ok(false);
        }
        let target_path = paths::from_node(prefix, target);
        let mut cur = Located {
            path: paths::tree_root(prefix),
            cache_hit: observation.cache_hit(),
            observation,
        };
        loop {
            cur = match self
                .step_right_until_owns(prefix, cur, key, requirement)
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
                NodeBody::Index(index) => index
                    .child_for(key)
                    .ok_or_else(|| StorageError::other("descent reached an empty index node"))?
                    .to_string(),
            };
            cur = match self.load_child(prefix, &token, requirement).await {
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
        prefix: &str,
        key: &[u8],
        requirement: Requirement,
    ) -> Result<Option<LeafLocator>, StorageError> {
        let observation = self.shards.load_root_state(prefix, requirement).await?;
        if observation.is_absent() {
            return Ok(None);
        }
        let mut cur = Located {
            path: paths::tree_root(prefix),
            cache_hit: observation.cache_hit(),
            observation,
        };
        let mut parent: Option<Located> = None;
        loop {
            cur = self
                .step_right_until_owns(prefix, cur, key, requirement)
                .await?;
            let token = match cur.node().body() {
                NodeBody::Leaf(_) => return Ok(parent.map(Located::into_locator)),
                NodeBody::Index(index) => index
                    .child_for(key)
                    .ok_or_else(|| StorageError::other("descent reached an empty index node"))?
                    .to_string(),
            };
            let child = self
                .load_child(prefix, &token, requirement)
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
        prefix: &str,
        mut cur: Located,
        key: &[u8],
        requirement: Requirement,
    ) -> Result<Located, StorageError> {
        loop {
            cur = self
                .step_right_until_owns(prefix, cur, key, requirement)
                .await?;
            match cur.node().body() {
                NodeBody::Leaf(_) => return Ok(cur),
                NodeBody::Index(index) => {
                    let token = index
                        .child_for(key)
                        .ok_or_else(|| StorageError::other("descent reached an empty index node"))?
                        .to_string();
                    let cache_hit = cur.cache_hit;
                    cur = self
                        .load_child(prefix, &token, requirement)
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
        prefix: &str,
        mut cur: Located,
        key: &[u8],
        requirement: Requirement,
    ) -> Result<Located, StorageError> {
        while !cur.node().owns(key) {
            match cur.node().right_sibling() {
                Some(token) => {
                    let token = token.to_string();
                    let cache_hit = cur.cache_hit;
                    cur = self
                        .load_child(prefix, &token, requirement)
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
        prefix: &str,
        token: &str,
        requirement: Requirement,
    ) -> Result<Located, StorageError> {
        let observation = self
            .shards
            .load_node_state(prefix, token, requirement)
            .await?;
        Ok(Located {
            path: paths::from_node(prefix, token),
            cache_hit: observation.cache_hit(),
            observation,
        })
    }

    /// Re-reads the node at `path` (the root `_r` or a standalone `_n`) at
    /// `requirement`, for checking a terminal leaf reached through a cached
    /// interior descent.
    async fn reload(&self, path: &str, requirement: Requirement) -> Result<Located, StorageError> {
        let observation = self.shards.load_node_at_state(path, requirement).await?;
        if observation.is_absent() {
            return Err(StorageError::other("tree node vanished during descent"));
        }
        Ok(Located {
            path: path.to_string(),
            cache_hit: observation.cache_hit(),
            observation,
        })
    }
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

    const COLL: &str = "db/_c/0000000000000000000000";

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

    fn live(key: &[u8]) -> ShardEntry {
        ShardEntry {
            current: crate::CurrentState::External {
                writer: glassdb_data::TxId::from_bytes(vec![1]),
            },
            ..ShardEntry::new(key)
        }
    }

    fn leaf(entries: &[&[u8]], high_key: Option<&[u8]>, right: Option<&str>) -> Node {
        Node::leaf(Shard::from_entries(entries.iter().map(|k| live(k))))
            .with_high_key(high_key.map(<[u8]>::to_vec))
            .with_right_sibling(right.map(str::to_string))
    }

    // Seeds a two-level tree: root index -> {L0 (apple,cat), L1 (mango,pear)},
    // split at "m", with the leaves chained by right-sibling.
    async fn seed_two_level(s: &ShardStore) {
        s.store_node(
            COLL,
            "L0",
            &leaf(&[b"apple", b"cat"], Some(b"m"), Some("L1")),
            None,
        )
        .await
        .unwrap();
        s.store_node(COLL, "L1", &leaf(&[b"mango", b"pear"], None, None), None)
            .await
            .unwrap();
        let root = Node::index(IndexNode::from_children([
            (b"".to_vec(), "L0".to_string()),
            (b"m".to_vec(), "L1".to_string()),
        ]));
        s.create_root(COLL, &root).await.unwrap();
    }

    #[tokio::test]
    async fn single_leaf_collection_resolves_to_root() {
        let s = store();
        let root = Node::leaf(Shard::from_entries([live(b"only")]));
        s.create_root(COLL, &root).await.unwrap();

        let dir = Directory::new(s.shards.clone());
        let loc = dir
            .leaf_for(COLL, b"only", Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert_eq!(loc.path, paths::tree_root(COLL));
        assert!(!loc.observation.is_absent());
        assert!(loc.node().unwrap().as_leaf().unwrap().exists(b"only"));
    }

    #[tokio::test]
    async fn absent_collection_is_not_a_writable_empty_leaf() {
        let s = store();
        let dir = Directory::new(s.shards.clone());
        assert!(matches!(
            dir.leaf_for(COLL, b"k", Requirement::AtLeast(s.timeline.now()))
                .await,
            Err(StorageError::NotFound)
        ));
        // Structural traversal can still model absence as no reachable leaves.
        assert!(
            dir.leaves(COLL, Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn descends_index_to_correct_leaf() {
        let s = store();
        seed_two_level(&s).await;
        let dir = Directory::new(s.shards.clone());

        for (key, want_leaf) in [
            (b"apple".as_slice(), "_n/L0"),
            (b"cat", "_n/L0"),
            (b"mango", "_n/L1"),
            (b"pear", "_n/L1"),
            (b"zebra", "_n/L1"),
        ] {
            let loc = dir
                .leaf_for(COLL, key, Requirement::AtLeast(s.timeline.now()))
                .await
                .unwrap();
            assert!(
                loc.path.ends_with(want_leaf),
                "key {key:?} resolved to {}, want …{want_leaf}",
                loc.path
            );
        }
    }

    #[tokio::test]
    async fn follows_right_link_when_parent_is_stale() {
        // Model a split the parent has not yet learned about: the index still
        // points every key to L0, but L0's high-key ("m") shows keys >= "m" have
        // moved to its right sibling L1. Descent must step right, not fail.
        let s = store();
        s.store_node(
            COLL,
            "L0",
            &leaf(&[b"apple", b"cat"], Some(b"m"), Some("L1")),
            None,
        )
        .await
        .unwrap();
        s.store_node(COLL, "L1", &leaf(&[b"mango", b"pear"], None, None), None)
            .await
            .unwrap();
        let root = Node::index(IndexNode::from_children([(b"".to_vec(), "L0".to_string())]));
        s.create_root(COLL, &root).await.unwrap();

        let dir = Directory::new(s.shards.clone());
        let loc = dir
            .leaf_for(COLL, b"pear", Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert!(
            loc.path.ends_with("_n/L1"),
            "stale-parent lookup must follow the right link to L1, got {}",
            loc.path
        );
    }

    #[tokio::test]
    async fn leaves_are_returned_in_key_order() {
        let s = store();
        seed_two_level(&s).await;
        let dir = Directory::new(s.shards.clone());

        let leaves = dir
            .leaves(COLL, Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        let paths: Vec<&str> = leaves.iter().map(|l| l.path.as_str()).collect();
        assert_eq!(
            paths,
            vec![
                "db/_c/0000000000000000000000/_n/L0",
                "db/_c/0000000000000000000000/_n/L1"
            ]
        );

        let leftmost = dir
            .leftmost_leaf(COLL, Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap();
        assert!(leftmost.unwrap().path.ends_with("_n/L0"));
    }

    // ADR-031 hot-path invariant: with interior-vs-leaf requirement split, repeated
    // coordination on a non-root leaf serves the root index `_r` from cache
    // (never checking it) while still checking the terminal leaf.
    #[tokio::test]
    async fn interior_descent_does_not_check_root() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log: OpLog = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        let s = store_over(backend);
        seed_two_level(&s).await;
        let dir = Directory::new(s.shards.clone());

        // Warm the cache with a first descent, then measure only the steady state.
        dir.leaf_for_fresh(
            COLL,
            b"apple",
            Requirement::Any,
            Requirement::AtLeast(s.timeline.now()),
        )
        .await
        .unwrap();
        log.lock().unwrap().clear();

        for _ in 0..3 {
            let loc = dir
                .leaf_for_fresh(
                    COLL,
                    b"apple",
                    Requirement::Any,
                    Requirement::AtLeast(s.timeline.now()),
                )
                .await
                .unwrap();
            assert!(loc.path.ends_with("_n/L0"));
        }

        let reads = |suffix: &str| {
            log.lock()
                .unwrap()
                .iter()
                .filter(|r| {
                    r.path.ends_with(suffix) && (r.op == "read" || r.op == "read_if_modified")
                })
                .count()
        };
        assert_eq!(
            reads("/_r"),
            0,
            "root index is served from cache, never checked"
        );
        assert!(
            reads("_n/L0") >= 3,
            "the terminal leaf is checked each time"
        );
    }

    // ADR-031 P0 regression: a process that cached the root `_r` as a *leaf*
    // must still resolve to a real leaf after another process splits `_r` into
    // an index. Two independent cache views over one backend model the two
    // processes: the first warms its cache with the root-as-leaf at stale
    // requirement; the second splits the root in place; the first then resolves a
    // key at a current leaf bound and must descend into the fresh index
    // rather than return the index as if it were a leaf.
    #[tokio::test]
    async fn stale_root_leaf_cache_still_descends_after_root_split() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let s_a = store_over(backend.clone());
        let s_b = store_over(backend);

        // A single-leaf collection: the root `_r` holds the leaf directly.
        let root = Node::leaf(Shard::from_entries([live(b"a"), live(b"b")]));
        s_b.create_root(COLL, &root).await.unwrap();

        // Process A warms its cache with the root-as-leaf (stale requirement).
        let dir_a = Directory::new(s_a.shards.clone());
        let warm = dir_a
            .leaf_for_fresh(COLL, b"a", Requirement::Any, Requirement::Any)
            .await
            .unwrap();
        assert!(
            warm.node().unwrap().as_leaf().is_some(),
            "warm read sees a leaf"
        );

        // Process B splits the root in place: `_r` becomes a two-child index
        // over fresh leaves L (<"b") and R (>="b").
        s_b.store_node(COLL, "L", &leaf(&[b"a"], Some(b"b"), Some("R")), None)
            .await
            .unwrap();
        s_b.store_node(COLL, "R", &leaf(&[b"b"], None, None), None)
            .await
            .unwrap();
        let (_, ver) = s_b
            .load_root(COLL, Requirement::AtLeast(s_b.timeline.now()))
            .await
            .unwrap();
        let root2 = Node::index(IndexNode::from_children([
            (b"".to_vec(), "L".to_string()),
            (b"b".to_vec(), "R".to_string()),
        ]));
        assert!(s_b.store_root(COLL, &root2, &ver).await.unwrap());

        // Process A, still holding the stale root-as-leaf, resolves `a` with a
        // current leaf bound: it must descend into the fresh index and return leaf L.
        let loc = dir_a
            .leaf_for_fresh(
                COLL,
                b"a",
                Requirement::Any,
                Requirement::AtLeast(s_a.timeline.now()),
            )
            .await
            .unwrap();
        let shard = loc
            .node()
            .unwrap()
            .as_leaf()
            .expect("descent must yield a leaf, not the freshly-split root index");
        assert!(shard.exists(b"a"));
        assert!(
            loc.path.ends_with("_n/L"),
            "resolved to the owning child leaf"
        );
    }

    #[tokio::test]
    async fn parent_index_for_finds_leaf_parent_and_none_for_single_leaf() {
        let s = store();
        seed_two_level(&s).await;
        let dir = Directory::new(s.shards.clone());

        // The parent of any key's leaf is the root index `_r`.
        let parent = dir
            .parent_index_for(COLL, b"mango", Requirement::AtLeast(s.timeline.now()))
            .await
            .unwrap()
            .expect("a two-level tree has an index parent");
        assert!(parent.path.ends_with("/_r"));
        assert!(parent.node().unwrap().as_index().is_some());

        // A single-leaf collection has no index level, hence no parent.
        let single = store();
        let root = Node::leaf(Shard::from_entries([live(b"only")]));
        single.create_root(COLL, &root).await.unwrap();
        let single_dir = Directory::new(single.shards.clone());
        assert!(
            single_dir
                .parent_index_for(COLL, b"only", Requirement::AtLeast(single.timeline.now()))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn group_keys_by_leaf_routes_and_preserves_order() {
        let s = store();
        seed_two_level(&s).await;
        let dir = Directory::new(s.shards.clone());
        assert_eq!(CollectionAddress::root("db").physical_prefix(), COLL);

        let groups = dir
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
        let l0 = groups.iter().find(|g| g.path.ends_with("_n/L0")).unwrap();
        assert_eq!(
            l0.keys,
            vec![(b"cat".to_vec(), 'c'), (b"apple".to_vec(), 'a')],
            "same-leaf keys keep input order"
        );
        let l1 = groups.iter().find(|g| g.path.ends_with("_n/L1")).unwrap();
        assert_eq!(l1.keys, vec![(b"mango".to_vec(), 'm')]);
    }

    #[tokio::test]
    async fn grouped_routing_classifies_the_collection_that_failed() {
        let s = store();
        let dir = Directory::new(s.shards.clone());
        let root = CollectionAddress::root("db");
        let child = CollectionAddress::new("db", CollectionId::from_slice(&[1; 16]).unwrap());
        let requirement = Requirement::AtLeast(s.timeline.now());

        let root_error = dir
            .group_keys_by_leaf(
                [
                    (KeyRef::new(root.clone(), b"root"), ()),
                    (KeyRef::new(child.clone(), b"child"), ()),
                ],
                requirement,
            )
            .await;
        assert!(matches!(root_error, Err(StorageError::NotFound)));

        let child_error = dir
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
