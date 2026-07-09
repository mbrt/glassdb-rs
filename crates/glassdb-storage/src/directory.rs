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

use std::collections::BTreeMap;

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
        let mut cur = match self.shards.load_root_node(prefix, freshness).await? {
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

        loop {
            cur = self
                .step_right_until_owns(prefix, cur, key, freshness)
                .await?;
            match cur.node.body() {
                NodeBody::Leaf(_) => {
                    return Ok(LeafLocator {
                        path: cur.path,
                        node: cur.node,
                        version: cur.version,
                    });
                }
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
        let mut groups: BTreeMap<String, LeafGroup<T>> = BTreeMap::new();
        for (path, payload) in items {
            let (prefix, raw_key) = paths::split_key(path.as_ref())
                .map_err(|e| StorageError::with_source("parsing key path", e))?;
            let loc = self.leaf_for(&prefix, &raw_key, freshness).await?;
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;

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
        let mut node = Node::leaf(Shard::from_entries(entries.iter().map(|k| live(k))));
        node.set_high_key(high_key.map(<[u8]>::to_vec));
        node.set_right_sibling(right.map(str::to_string));
        node
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
