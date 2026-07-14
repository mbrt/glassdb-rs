//! Tree descent over the B-link coordination directory (ADR-031).
//!
//! The [`Directory`] resolves a key to the leaf that owns it by descending from
//! the collection root `_i` through index nodes, and it enumerates the leaves
//! in key order for listing. Descent is **self-correcting**: every node carries
//! a high-key and a right-sibling, so a lookup that lands too far left —
//! because a split moved the key rightward after the cache was taken — steps
//! along the right-sibling link (the B-link property) instead of restarting
//! from the root.
//!
//! This layer is pure routing: it reads nodes through the [`ShardStore`] (hence
//! the [`ObjectCache`], so interior nodes stay cached and off the hot path) and
//! never mutates the tree. Splitting and locking live above it.
//!
//! [`ObjectCache`]: crate::object_cache::ObjectCache

use std::collections::{BTreeMap, BTreeSet};

use glassdb_backend as backend;
use glassdb_data::paths;

use crate::error::StorageError;
use crate::node::{Node, NodeBody};
use crate::object_cache::Freshness;
use crate::shard::Shard;
use crate::shardstore::ShardStore;

/// The leaf that owns a key (or range endpoint), with everything needed to read
/// or compare-and-swap it: its object `path`, the decoded `node`, and its
/// `version` (`None` when the leaf object does not exist yet, i.e. the
/// collection's root leaf is still to be created).
#[derive(Debug, Clone)]
pub struct LeafLocator {
    pub path: String,
    pub node: Node,
    pub version: Option<backend::Version>,
}

/// A group of keys routed to one leaf by [`Directory::group_keys_by_leaf`]: the
/// owning leaf and the raw keys (with their payloads) that landed in it.
pub struct LeafGroup<T> {
    pub path: String,
    pub node: Node,
    pub version: Option<backend::Version>,
    pub keys: Vec<(Vec<u8>, T)>,
}

/// One node reached during a descent: its decoded body, object path, and
/// version. `version` is `None` only for a not-yet-created root leaf.
struct Located {
    node: Node,
    path: String,
    version: Option<backend::Version>,
}

impl Located {
    fn into_locator(self) -> LeafLocator {
        LeafLocator {
            path: self.path,
            node: self.node,
            version: self.version,
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

    /// Resolves the leaf that owns `key`, descending from the root `_i` and
    /// following right-sibling links to self-correct past in-progress splits.
    ///
    /// Always returns a locator: when the collection does not exist yet the leaf
    /// is the (empty) root at `_i` with no version, so a caller can look the key
    /// up (finding it absent) or create the root by compare-and-swap.
    pub async fn leaf_for(
        &self,
        prefix: &str,
        key: &[u8],
        freshness: Freshness,
    ) -> Result<LeafLocator, StorageError> {
        let cur = match self.shards.load_root_node(prefix, freshness).await? {
            Some((node, version)) => Located {
                node,
                path: paths::collection_info(prefix),
                version: Some(version),
            },
            None => {
                return Ok(LeafLocator {
                    path: paths::collection_info(prefix),
                    node: Node::leaf(Shard::new()),
                    version: None,
                });
            }
        };
        Ok(self
            .descend_to_leaf(prefix, cur, key, freshness)
            .await?
            .into_locator())
    }

    /// Returns the existing leaf that owns `key`, or `None` when the collection
    /// does not exist.
    pub async fn first_leaf_at(
        &self,
        prefix: &str,
        key: &[u8],
        freshness: Freshness,
    ) -> Result<Option<LeafLocator>, StorageError> {
        let Some((node, version)) = self.shards.load_root_node(prefix, freshness).await? else {
            return Ok(None);
        };
        let cur = Located {
            node,
            path: paths::collection_info(prefix),
            version: Some(version),
        };
        Ok(Some(
            self.descend_to_leaf(prefix, cur, key, freshness)
                .await?
                .into_locator(),
        ))
    }

    /// Returns the right sibling of `leaf`, or `None` for the rightmost leaf.
    pub async fn next_leaf(
        &self,
        prefix: &str,
        leaf: &LeafLocator,
        freshness: Freshness,
    ) -> Result<Option<LeafLocator>, StorageError> {
        let Some(token) = leaf.node.right_sibling() else {
            return Ok(None);
        };
        Ok(Some(
            self.load_child(prefix, token, freshness)
                .await?
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
        freshness: Freshness,
    ) -> Result<Vec<LeafLocator>, StorageError> {
        let Some(mut leaf) = self.first_leaf_at(prefix, start, freshness).await? else {
            return Err(StorageError::NotFound);
        };
        let mut out = Vec::new();
        loop {
            let done = end.is_some_and(|end| leaf.node.owns(end));
            let next = if done {
                None
            } else {
                self.next_leaf(prefix, &leaf, freshness).await?
            };
            out.push(leaf);
            match next {
                Some(right) => leaf = right,
                None => return Ok(out),
            }
        }
    }

    /// Resolves the owning leaf while keeping interior-node revalidation off the
    /// hot path (ADR-031): descends the index spine at `interior` freshness
    /// (served from cache — a stale misroute self-corrects via right-links) and
    /// revalidates only the terminal leaf — the coordination/CAS unit — at `leaf`
    /// freshness. A grown tree thus never revalidates the root `_i` on every key
    /// coordination; `Latest` stays where a CAS depends on it.
    ///
    /// When both freshnesses match this is exactly [`leaf_for`](Self::leaf_for).
    pub async fn leaf_for_fresh(
        &self,
        prefix: &str,
        key: &[u8],
        interior: Freshness,
        leaf: Freshness,
    ) -> Result<LeafLocator, StorageError> {
        let loc = self.leaf_for(prefix, key, interior).await?;
        // Same freshness, or an uncreated root leaf (nothing to revalidate).
        if interior == leaf || loc.version.is_none() {
            return Ok(loc);
        }
        // Revalidate the terminal node at the stricter freshness and resume the
        // descent from it: the cached interior read may have routed us to `_i`
        // as a leaf while a concurrent split has since rewritten `_i` into an
        // index (or split the leaf), so we must keep descending — never hand
        // back an index masquerading as a leaf.
        let located = self.reload(prefix, &loc.path, leaf).await?;
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
        freshness: Freshness,
    ) -> Result<Option<LeafLocator>, StorageError> {
        let Some((node, version)) = self.shards.load_root_node(prefix, freshness).await? else {
            return Ok(None);
        };
        let mut cur = Located {
            node,
            path: paths::collection_info(prefix),
            version: Some(version),
        };
        loop {
            match cur.node.body() {
                NodeBody::Leaf(_) => {
                    return Ok(Some(LeafLocator {
                        path: cur.path,
                        node: cur.node,
                        version: cur.version,
                    }));
                }
                NodeBody::Index(index) => {
                    let token = index
                        .children()
                        .next()
                        .map(|(_, c)| c.to_string())
                        .ok_or_else(|| {
                            StorageError::other("descent reached an empty index node")
                        })?;
                    cur = self.load_child(prefix, &token, freshness).await?;
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
        freshness: Freshness,
    ) -> Result<Vec<LeafLocator>, StorageError> {
        let Some(first) = self.leftmost_leaf(prefix, freshness).await? else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        let mut cur = first;
        loop {
            let next = cur.node.right_sibling().map(str::to_string);
            out.push(cur);
            match next {
                Some(token) => {
                    cur = self
                        .load_child(prefix, &token, freshness)
                        .await?
                        .into_locator()
                }
                None => return Ok(out),
            }
        }
    }

    /// Routes `(key_path, payload)` items to their owning leaves, returning one
    /// group per touched leaf with its loaded node and version. Callers hand it
    /// key paths and never compute a location themselves; routing is by descent
    /// from the collection root, not by any fixed hash (ADR-031).
    ///
    /// Groups are keyed by leaf object path, so keys from different collections
    /// (distinct `_i`) never collide; input order is preserved within a group.
    pub async fn group_keys_by_leaf<P: AsRef<str>, T>(
        &self,
        items: impl IntoIterator<Item = (P, T)>,
        freshness: Freshness,
    ) -> Result<Vec<LeafGroup<T>>, StorageError> {
        self.group_keys_by_leaf_fresh(items, freshness, freshness)
            .await
    }

    /// [`group_keys_by_leaf`] with the interior-vs-leaf freshness split of
    /// [`leaf_for_fresh`], so the coordination hot path routes keys without
    /// revalidating the root `_i` (ADR-031).
    ///
    /// [`group_keys_by_leaf`]: Self::group_keys_by_leaf
    /// [`leaf_for_fresh`]: Self::leaf_for_fresh
    pub async fn group_keys_by_leaf_fresh<P: AsRef<str>, T>(
        &self,
        items: impl IntoIterator<Item = (P, T)>,
        interior: Freshness,
        leaf: Freshness,
    ) -> Result<Vec<LeafGroup<T>>, StorageError> {
        let mut groups: BTreeMap<String, LeafGroup<T>> = BTreeMap::new();
        for (path, payload) in items {
            let (prefix, raw_key) = paths::split_key(path.as_ref())
                .map_err(|e| StorageError::with_source("parsing key path", e))?;
            let loc = self
                .leaf_for_fresh(&prefix, &raw_key, interior, leaf)
                .await?;
            groups
                .entry(loc.path.clone())
                .or_insert_with(|| LeafGroup {
                    path: loc.path,
                    node: loc.node,
                    version: loc.version,
                    keys: Vec::new(),
                })
                .keys
                .push((raw_key, payload));
        }
        Ok(groups.into_values().collect())
    }

    /// Collects every `_n` node token reachable from the collection root
    /// (ADR-031): all index child pointers and every right-sibling link, walked
    /// transitively. Structural split recovery uses this set to decide whether
    /// its recorded created nodes became reachable. Empty when the collection
    /// does not exist.
    ///
    /// Reads at [`Freshness::Latest`] so a just-linked sibling is observed. A
    /// missing child reference is skipped because there is no node to traverse.
    pub async fn reachable_tokens(
        &self,
        prefix: &str,
        freshness: Freshness,
    ) -> Result<BTreeSet<String>, StorageError> {
        let mut reachable: BTreeSet<String> = BTreeSet::new();
        let Some((root, _)) = self.shards.load_root_node(prefix, freshness).await? else {
            return Ok(reachable);
        };
        // Seed the frontier with the root's direct references; the root itself
        // has no token (it lives at `_i`).
        let mut frontier: Vec<String> = referenced_tokens(&root);
        while let Some(token) = frontier.pop() {
            if !reachable.insert(token.clone()) {
                continue;
            }
            match self.shards.load_node(prefix, &token, freshness).await {
                Ok((node, _)) => frontier.extend(referenced_tokens(&node)),
                // A dangling reference (already reclaimed, or a crashed create):
                // it points at nothing, so there is nothing further to reach.
                Err(StorageError::NotFound) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(reachable)
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
        freshness: Freshness,
    ) -> Result<Option<LeafLocator>, StorageError> {
        let Some((node, version)) = self.shards.load_root_node(prefix, freshness).await? else {
            return Ok(None);
        };
        let mut cur = Located {
            node,
            path: paths::collection_info(prefix),
            version: Some(version),
        };
        let mut parent: Option<Located> = None;
        loop {
            cur = self
                .step_right_until_owns(prefix, cur, key, freshness)
                .await?;
            let token = match cur.node.body() {
                NodeBody::Leaf(_) => return Ok(parent.map(Located::into_locator)),
                NodeBody::Index(index) => index
                    .child_for(key)
                    .ok_or_else(|| StorageError::other("descent reached an empty index node"))?
                    .to_string(),
            };
            let child = self.load_child(prefix, &token, freshness).await?;
            parent = Some(cur);
            cur = child;
        }
    }

    /// Descends from `cur` to the leaf that owns `key`: at each level step right
    /// to the owning node, then follow the index child pointer, until a leaf is
    /// reached. Self-correcting through right-links, so a stale interior read
    /// never traps the descent at the wrong node — and, crucially, a node that
    /// turns out to be an index (e.g. a revalidated `_i` that split into one) is
    /// resolved to its child rather than returned as a leaf.
    async fn descend_to_leaf(
        &self,
        prefix: &str,
        mut cur: Located,
        key: &[u8],
        freshness: Freshness,
    ) -> Result<Located, StorageError> {
        loop {
            cur = self
                .step_right_until_owns(prefix, cur, key, freshness)
                .await?;
            match cur.node.body() {
                NodeBody::Leaf(_) => return Ok(cur),
                NodeBody::Index(index) => {
                    let token = index
                        .child_for(key)
                        .ok_or_else(|| StorageError::other("descent reached an empty index node"))?
                        .to_string();
                    cur = self.load_child(prefix, &token, freshness).await?;
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
        freshness: Freshness,
    ) -> Result<Located, StorageError> {
        while !cur.node.owns(key) {
            match cur.node.right_sibling() {
                Some(token) => {
                    let token = token.to_string();
                    cur = self.load_child(prefix, &token, freshness).await?;
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
        freshness: Freshness,
    ) -> Result<Located, StorageError> {
        let (node, version) = self.shards.load_node(prefix, token, freshness).await?;
        Ok(Located {
            node,
            path: paths::from_node(prefix, token),
            version: Some(version),
        })
    }

    /// Re-reads the node at `path` (the root `_i` or a standalone `_n`) at
    /// `freshness`, for revalidating a terminal leaf reached through a cached
    /// interior descent.
    async fn reload(
        &self,
        prefix: &str,
        path: &str,
        freshness: Freshness,
    ) -> Result<Located, StorageError> {
        if paths::is_collection_info(path) {
            let (node, version) = self
                .shards
                .load_root_node(prefix, freshness)
                .await?
                .ok_or_else(|| StorageError::other("collection root vanished during descent"))?;
            Ok(Located {
                node,
                path: path.to_string(),
                version: Some(version),
            })
        } else {
            let token = paths::node_token_of(path)
                .map_err(|e| StorageError::with_source("parsing node path", e))?;
            self.load_child(prefix, &token, freshness).await
        }
    }
}

/// The `_n` tokens a node points at: its index children (if any) and its
/// right-sibling link. The reachability walk follows these to find live nodes.
fn referenced_tokens(node: &Node) -> Vec<String> {
    let mut tokens: Vec<String> = match node.body() {
        NodeBody::Index(index) => index.children().map(|(_, c)| c.to_string()).collect(),
        NodeBody::Leaf(_) => Vec::new(),
    };
    if let Some(right) = node.right_sibling() {
        tokens.push(right.to_string());
    }
    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{OpLog, RecordingBackend};

    use crate::entry::SharedCache;
    use crate::lock::LockType;
    use crate::node::{IndexNode, Node};
    use crate::object_cache::ObjectCache;
    use crate::root::CollectionRoot;
    use crate::shard::Shard;
    use crate::shard::ShardEntry;
    use crate::shardstore::ShardStore;

    const COLL: &str = "db/coll";

    fn store() -> ShardStore {
        ShardStore::new(ObjectCache::new(
            Arc::new(MemoryBackend::new()) as Arc<dyn Backend>,
            &SharedCache::new(1 << 20),
        ))
    }

    fn live(key: &[u8]) -> ShardEntry {
        ShardEntry {
            key: key.to_vec(),
            lock_type: LockType::None,
            locked_by: Vec::new(),
            current_writer: Some(glassdb_data::TxId::from_bytes(vec![1])),
            deleted: false,
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
        let mut root = CollectionRoot::new();
        root.set_node(Node::index(IndexNode::from_children([
            (b"".to_vec(), "L0".to_string()),
            (b"m".to_vec(), "L1".to_string()),
        ])));
        s.create_root(COLL, &root).await.unwrap();
    }

    #[tokio::test]
    async fn single_leaf_collection_resolves_to_root() {
        let s = store();
        let mut root = CollectionRoot::new();
        root.set_node(Node::leaf(Shard::from_entries([live(b"only")])));
        s.create_root(COLL, &root).await.unwrap();

        let dir = Directory::new(s);
        let loc = dir
            .leaf_for(COLL, b"only", Freshness::Latest)
            .await
            .unwrap();
        assert_eq!(loc.path, paths::collection_info(COLL));
        assert!(loc.version.is_some());
        assert!(loc.node.as_leaf().unwrap().exists(b"only"));
    }

    #[tokio::test]
    async fn absent_collection_routes_to_uncreated_root_leaf() {
        let dir = Directory::new(store());
        let loc = dir.leaf_for(COLL, b"k", Freshness::Latest).await.unwrap();
        assert_eq!(loc.path, paths::collection_info(COLL));
        assert!(loc.version.is_none(), "root leaf is not created yet");
        assert!(loc.node.as_leaf().unwrap().is_empty());
        // Listing an absent collection yields no leaves.
        assert!(
            dir.leaves(COLL, Freshness::Latest)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn descends_index_to_correct_leaf() {
        let s = store();
        seed_two_level(&s).await;
        let dir = Directory::new(s);

        for (key, want_leaf) in [
            (b"apple".as_slice(), "_n/L0"),
            (b"cat", "_n/L0"),
            (b"mango", "_n/L1"),
            (b"pear", "_n/L1"),
            (b"zebra", "_n/L1"),
        ] {
            let loc = dir.leaf_for(COLL, key, Freshness::Latest).await.unwrap();
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
        let mut root = CollectionRoot::new();
        root.set_node(Node::index(IndexNode::from_children([(
            b"".to_vec(),
            "L0".to_string(),
        )])));
        s.create_root(COLL, &root).await.unwrap();

        let dir = Directory::new(s);
        let loc = dir
            .leaf_for(COLL, b"pear", Freshness::Latest)
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
        let dir = Directory::new(s);

        let leaves = dir.leaves(COLL, Freshness::Latest).await.unwrap();
        let paths: Vec<&str> = leaves.iter().map(|l| l.path.as_str()).collect();
        assert_eq!(paths, vec!["db/coll/_n/L0", "db/coll/_n/L1"]);

        let leftmost = dir.leftmost_leaf(COLL, Freshness::Latest).await.unwrap();
        assert!(leftmost.unwrap().path.ends_with("_n/L0"));
    }

    // ADR-031 hot-path invariant: with interior-vs-leaf freshness split, repeated
    // coordination on a non-root leaf serves the root index `_i` from cache
    // (never revalidating it) while still revalidating the terminal leaf.
    #[tokio::test]
    async fn interior_descent_does_not_revalidate_root() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log: OpLog = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        let s = ShardStore::new(ObjectCache::new(backend, &SharedCache::new(1 << 20)));
        seed_two_level(&s).await;
        let dir = Directory::new(s);

        // Warm the cache with a first descent, then measure only the steady state.
        dir.leaf_for_fresh(COLL, b"apple", Freshness::AllowStale, Freshness::Latest)
            .await
            .unwrap();
        log.lock().unwrap().clear();

        for _ in 0..3 {
            let loc = dir
                .leaf_for_fresh(COLL, b"apple", Freshness::AllowStale, Freshness::Latest)
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
            reads("/_i"),
            0,
            "root index is served from cache, never revalidated"
        );
        assert!(
            reads("_n/L0") >= 3,
            "the terminal leaf is revalidated each time"
        );
    }

    // ADR-031 P0 regression: a process that cached the root `_i` as a *leaf*
    // must still resolve to a real leaf after another process splits `_i` into
    // an index. Two independent cache views over one backend model the two
    // processes: the first warms its cache with the root-as-leaf at stale
    // freshness; the second splits the root in place; the first then resolves a
    // key at `Latest` leaf freshness and must descend into the fresh index
    // rather than return the index as if it were a leaf.
    #[tokio::test]
    async fn stale_root_leaf_cache_still_descends_after_root_split() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let s_a = ShardStore::new(ObjectCache::new(
            backend.clone(),
            &SharedCache::new(1 << 20),
        ));
        let s_b = ShardStore::new(ObjectCache::new(backend, &SharedCache::new(1 << 20)));

        // A single-leaf collection: the root `_i` holds the leaf directly.
        let mut root = CollectionRoot::new();
        root.set_node(Node::leaf(Shard::from_entries([live(b"a"), live(b"b")])));
        s_b.create_root(COLL, &root).await.unwrap();

        // Process A warms its cache with the root-as-leaf (stale freshness).
        let dir_a = Directory::new(s_a.clone());
        let warm = dir_a
            .leaf_for_fresh(COLL, b"a", Freshness::AllowStale, Freshness::AllowStale)
            .await
            .unwrap();
        assert!(warm.node.as_leaf().is_some(), "warm read sees a leaf");

        // Process B splits the root in place: `_i` becomes a two-child index
        // over fresh leaves L (<"b") and R (>="b").
        s_b.store_node(COLL, "L", &leaf(&[b"a"], Some(b"b"), Some("R")), None)
            .await
            .unwrap();
        s_b.store_node(COLL, "R", &leaf(&[b"b"], None, None), None)
            .await
            .unwrap();
        let (mut root2, ver) = s_b.load_root(COLL).await.unwrap();
        root2.set_node(Node::index(IndexNode::from_children([
            (b"".to_vec(), "L".to_string()),
            (b"b".to_vec(), "R".to_string()),
        ])));
        assert!(s_b.store_root(COLL, &root2, &ver).await.unwrap());

        // Process A, still holding the stale root-as-leaf, resolves `a` with a
        // `Latest` leaf: it must descend into the fresh index and return leaf L.
        let loc = dir_a
            .leaf_for_fresh(COLL, b"a", Freshness::AllowStale, Freshness::Latest)
            .await
            .unwrap();
        let shard = loc
            .node
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
        let dir = Directory::new(s.clone());

        // The parent of any key's leaf is the root index `_i`.
        let parent = dir
            .parent_index_for(COLL, b"mango", Freshness::Latest)
            .await
            .unwrap()
            .expect("a two-level tree has an index parent");
        assert!(parent.path.ends_with("/_i"));
        assert!(parent.node.as_index().is_some());

        // A single-leaf collection has no index level, hence no parent.
        let single = store();
        let mut root = CollectionRoot::new();
        root.set_node(Node::leaf(Shard::from_entries([live(b"only")])));
        single.create_root(COLL, &root).await.unwrap();
        assert!(
            Directory::new(single)
                .parent_index_for(COLL, b"only", Freshness::Latest)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn group_keys_by_leaf_routes_and_preserves_order() {
        let s = store();
        seed_two_level(&s).await;
        let dir = Directory::new(s);

        let groups = dir
            .group_keys_by_leaf(
                [
                    (paths::from_key(COLL, b"cat"), 'c'),
                    (paths::from_key(COLL, b"mango"), 'm'),
                    (paths::from_key(COLL, b"apple"), 'a'),
                ],
                Freshness::Latest,
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
}
