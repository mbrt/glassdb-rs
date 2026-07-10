//! Background growth of the B-link coordination tree by leaf and node splits
//! (ADR-031).
//!
//! Coordination objects are grow-only: a leaf that crosses its soft cap is
//! halved so no single object becomes a scalability or contention bottleneck.
//! Splitting runs off the hot path in a periodic background task, fed
//! candidates the coordinator observes as it writes leaves — never a key-space
//! enumeration.
//!
//! Every split is a sequence of independent, idempotent compare-and-swaps, so a
//! crash between any two steps leaves the tree correct (descent self-corrects
//! through the B-link right-links) and any half-built node either becomes
//! reachable or is reclaimed by the GC orphan sweep:
//!
//! 0. Durably record the collection as split-active
//!    ([`ShardStore::mark_split_active`]) before any node object is created, so
//!    an orphan a later crash leaves is always discoverable by the GC orphan
//!    sweep — even in a fresh process that never observed the split.
//! 1. Create the right sibling (`write_if_not_exists`) holding the upper half
//!    and inheriting the source's former high-key and right-sibling.
//! 2. **Shrink the source in one CAS** — drop the upper half, set high-key to the
//!    split key, link to the sibling. This is the linearization point: descent
//!    now finds the moved keys by stepping right, and a concurrent locker that
//!    loaded the pre-shrink version loses its CAS and re-routes (ADR-031
//!    ownership re-check).
//! 3. Insert the separator into the parent so future descents skip the
//!    right-link hop; recurse when the parent itself overflows. Purely an
//!    optimization — correctness never depends on it landing.
//!
//! The collection root `_i` cannot move (its address is fixed), so when it
//! overflows it splits **in place**: two children are created and the root is
//! rewritten into a two-entry index over them, growing the tree's height while
//! preserving the collection metadata.

use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use glassdb_concurr::{Background, rt};
use glassdb_data::paths;
use glassdb_storage::{
    Directory, Freshness, IndexNode, Node, Shard, ShardStore, SplitPolicy, StorageError,
};

use crate::error::TransError;
use crate::shard_coord::SplitHinter;

/// How often the splitter drains its candidate queue. A split is a handful of
/// CAS round-trips, so a tight cadence keeps overflowing leaves short-lived; the
/// GC orphan sweep runs an order of magnitude slower, which is what makes its
/// generational safety horizon (ADR-031, M1-S4) longer than any split.
const SPLIT_INTERVAL: Duration = Duration::from_secs(1);

/// Upper bound on the buffered split-candidate queue. Candidates are only hints:
/// the splitter reloads and re-checks each one, so dropping the oldest when full
/// merely delays a split, never causes an unsafe one.
const CANDIDATE_QUEUE_CAP: usize = 4096;

/// Bounded attempts to insert a separator into a contended parent before
/// re-queuing it for a later sweep. Descent works meanwhile through right-links.
const PARENT_RETRIES: usize = 8;

/// Safety bound on the leaf right-link hops walked while reconciling separators,
/// so a malformed or concurrently-mutated chain can never spin the splitter. A
/// well-formed chain up to a split key is far shorter than this.
const MAX_RECONCILE_HOPS: usize = 4096;

/// A leaf separator a split could not publish into its parent index on the
/// first try (a lost CAS): re-driven by a later [`Splitter`] sweep so the
/// directory does not stay reliant on a right-link walk (ADR-031). Re-driving
/// reconciles the whole chain, so `split_key -> new_token` names only the
/// rightmost edge to publish.
#[derive(Clone)]
pub(crate) struct PendingSeparator {
    prefix: String,
    split_key: Vec<u8>,
    new_token: String,
}

/// The feed of leaves that may need splitting (ADR-031), owned by the
/// [`Splitter`]. A handle is handed to the coordinator behind the
/// [`SplitHinter`] seam: it pushes a leaf's path right after storing it over
/// the soft cap, and the splitter drains and re-checks. Cloneable (all fields
/// `Arc`), so the producer handle and the splitter share one queue and policy.
#[derive(Clone)]
pub(crate) struct SplitCandidates {
    policy: SplitPolicy,
    queue: Arc<Mutex<VecDeque<String>>>,
}

impl SplitCandidates {
    /// Creates an empty candidate feed governed by `policy`.
    pub(crate) fn new(policy: SplitPolicy) -> Self {
        SplitCandidates {
            policy,
            queue: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// The soft-cap policy shared by the feed and the splitter.
    pub(crate) fn policy(&self) -> &SplitPolicy {
        &self.policy
    }

    /// Drains every queued candidate, de-duplicated, for one sweep cycle.
    fn drain(&self) -> Vec<String> {
        let mut q = self.queue.lock().unwrap();
        let mut seen = BTreeSet::new();
        let mut out = Vec::new();
        while let Some(p) = q.pop_front() {
            if seen.insert(p.clone()) {
                out.push(p);
            }
        }
        out
    }
}

impl SplitHinter for SplitCandidates {
    /// Records that `path`'s leaf, now holding `entries`, may be a split
    /// candidate: over either the entry-count or the encoded-byte soft cap. A
    /// node needs at least two entries to be divisible, so a single hot key is
    /// never enqueued however large. The byte size is a hint the splitter
    /// re-checks authoritatively against the full node (which adds a little
    /// framing), so this need not account for it. The oldest hint is dropped
    /// when the queue is full.
    fn observe_leaf(&self, path: &str, entries: &Shard) {
        let over_cap = entries.len() >= 2
            && (entries.len() > self.policy.leaf_max_entries
                || entries.encoded_len() > self.policy.leaf_max_bytes);
        if !over_cap {
            return;
        }
        let mut q = self.queue.lock().unwrap();
        if q.len() >= CANDIDATE_QUEUE_CAP {
            q.pop_front();
        }
        q.push_back(path.to_string());
    }
}

/// Background executor that halves over-full B-link nodes (ADR-031). Holds no
/// per-transaction state: every split is a pure structural compare-and-swap
/// through the [`ShardStore`], recovered idempotently like any in-doubt CAS.
#[derive(Clone)]
pub struct Splitter {
    // Weak so a clone captured in the spawned loop does not keep the executor
    // alive across shutdown; the single strong owner is `DbInner::background`.
    bg: Weak<Background>,
    shards: ShardStore,
    dir: Directory,
    // The leaf-candidate feed this splitter owns and drains. The coordinator
    // fills it through the [`SplitHinter`] seam (see [`Splitter::hinter`]); a
    // clone here and behind the seam share one queue.
    candidates: SplitCandidates,
    // Separators a split could not publish on the first try; re-driven each
    // sweep so the parent index eventually learns them (ADR-031). Purely
    // splitter-internal — the coordinator never sees it.
    pending: Arc<Mutex<VecDeque<PendingSeparator>>>,
}

impl Splitter {
    /// Creates a splitter over `shards` with the default soft-cap policy,
    /// owning the candidate feed the coordinator fills through
    /// [`hinter`](Self::hinter).
    pub fn new(bg: Weak<Background>, shards: ShardStore) -> Self {
        Splitter::with_policy(bg, shards, SplitPolicy::default())
    }

    /// Creates a splitter with an explicit soft-cap `policy`, so a caller can
    /// drive splits with tiny nodes (e.g. the deterministic-simulation
    /// harness). The default policy is used by [`Splitter::new`].
    pub fn with_policy(bg: Weak<Background>, shards: ShardStore, policy: SplitPolicy) -> Self {
        Splitter::with_candidates(bg, shards, SplitCandidates::new(policy))
    }

    /// Creates a splitter over an explicit candidate feed. Lets a test drive the
    /// splitter with a tiny soft-cap policy.
    fn with_candidates(
        bg: Weak<Background>,
        shards: ShardStore,
        candidates: SplitCandidates,
    ) -> Self {
        let dir = Directory::new(shards.clone());
        Splitter {
            bg,
            shards,
            dir,
            candidates,
            pending: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// A [`SplitHinter`] handle for the coordinator to report over-cap leaf
    /// writes into this splitter's queue (ADR-031). The returned handle shares
    /// this splitter's queue and soft-cap policy, so the coordinator depends
    /// only on the seam, never on the candidate feed's type.
    pub fn hinter(&self) -> Arc<dyn SplitHinter> {
        Arc::new(self.candidates.clone())
    }

    /// Queues a separator whose parent insert must be re-driven by a later
    /// sweep. The oldest is dropped when full: descent still works via
    /// right-links, so a dropped retry only defers directory compaction.
    fn push_pending_separator(&self, sep: PendingSeparator) {
        let mut p = self.pending.lock().unwrap();
        if p.len() >= CANDIDATE_QUEUE_CAP {
            p.pop_front();
        }
        p.push_back(sep);
    }

    /// Drains the pending separators queued for re-driving this cycle.
    fn drain_pending(&self) -> Vec<PendingSeparator> {
        self.pending.lock().unwrap().drain(..).collect()
    }

    /// Starts the background split loop on the [`Background`] executor: one sweep
    /// every [`SPLIT_INTERVAL`] until the executor is dropped. A no-op if the
    /// executor is already gone (the database has shut down).
    pub fn start(&self) {
        let Some(bg) = self.bg.upgrade() else {
            return;
        };
        let splitter = self.clone();
        bg.spawn(async move {
            loop {
                rt::sleep(SPLIT_INTERVAL).await;
                splitter.run_once().await;
            }
        });
    }

    /// Runs one sweep: split every queued candidate. Best-effort — a transient
    /// error on one candidate only defers its split to a later cycle, so it is
    /// logged and the sweep continues.
    async fn run_once(&self) {
        for path in self.candidates.drain() {
            if let Err(e) = self.split_path(&path).await {
                tracing::debug!(path = %path, error = %e, "split candidate deferred");
            }
        }
        // Re-drive separators a previous cycle could not publish, so the parent
        // index eventually learns them and descent stops relying on right-links.
        for sep in self.drain_pending() {
            if let Err(e) = self
                .publish_separators(&sep.prefix, &sep.split_key, &sep.new_token)
                .await
            {
                tracing::debug!(error = %e, "separator publication deferred");
            }
        }
    }

    /// Splits the leaf at object `path` if it is still over the soft cap: an
    /// in-place root split when `path` is the collection root `_i`, else a
    /// standalone node half-split.
    async fn split_path(&self, path: &str) -> Result<(), TransError> {
        let pr = paths::parse(path)
            .map_err(|e| StorageError::with_source("parsing candidate path", e))?;
        if paths::is_collection_info(path) {
            self.split_root(&pr.prefix).await
        } else {
            self.split_nonroot(&pr.prefix, &pr.suffix).await
        }
    }

    /// Halves a standalone node `_n` (leaf or interior): create the sibling,
    /// shrink the source (linearization point), then insert the separator into
    /// the parent. A CAS lost to a concurrent mutation simply defers the split.
    async fn split_nonroot(&self, prefix: &str, token: &str) -> Result<(), TransError> {
        let (mut node, version) = self
            .shards
            .load_node(prefix, token, Freshness::Latest)
            .await?;
        if !node.over_soft_cap(self.candidates.policy()) {
            return Ok(());
        }
        let right_token = paths::random_node_token();
        let Some((right, split_key)) = node.split(&right_token) else {
            return Ok(());
        };
        // Record split activity durably *before* creating any node object, so a
        // crash-orphaned sibling is always discoverable by the GC orphan sweep
        // after a restart (ADR-031).
        self.shards.mark_split_active(prefix).await?;
        // 1. Create the right sibling. A fresh random token never collides, so a
        //    `false` here means someone else created it; defer.
        if !self
            .shards
            .store_node(prefix, &right_token, &right, None)
            .await?
        {
            return Ok(());
        }
        // 2. Shrink the source (linearization point). Lost to a concurrent
        //    locker CAS: leave the orphan sibling for the GC sweep and retry
        //    next cycle.
        if !self
            .shards
            .store_node(prefix, token, &node, Some(&version))
            .await?
        {
            return Ok(());
        }
        // 3. Publish the separator into the parent index; recurse if the parent
        //    overflows. A lost CAS is re-queued for a later sweep.
        self.publish_separators(prefix, &split_key, &right_token)
            .await
    }

    /// Grows the collection root in place: the root cannot move, so an
    /// overflowing `_i` splits into two freshly created children and is rewritten
    /// into a two-entry index over them (preserving collection metadata), raising
    /// the tree's height by one.
    async fn split_root(&self, prefix: &str) -> Result<(), TransError> {
        let (root, version) = match self.shards.load_root(prefix).await {
            Ok(rv) => rv,
            Err(StorageError::NotFound) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let node = root.node().clone();
        if !node.over_soft_cap(self.candidates.policy()) {
            return Ok(());
        }
        let l_token = paths::random_node_token();
        let r_token = paths::random_node_token();
        let (left, right, split_key) = split_into_children(&node, &r_token);
        // Record split activity durably before creating any child, so orphaned
        // children left by a crash are discoverable after a restart (ADR-031).
        self.shards.mark_split_active(prefix).await?;
        // Create both children before rewriting the root, so the root never
        // points at a missing child; on crash the unreferenced children are
        // reclaimed by the GC sweep.
        if !self
            .shards
            .store_node(prefix, &l_token, &left, None)
            .await?
            || !self
                .shards
                .store_node(prefix, &r_token, &right, None)
                .await?
        {
            return Ok(());
        }
        let index = Node::index(IndexNode::from_children([
            (Vec::new(), l_token),
            (split_key, r_token),
        ]));
        let mut new_root = root.clone();
        new_root.set_node(index);
        // The root rewrite is the linearization point. A lost CAS orphans the two
        // children (reclaimed by GC) and defers the split.
        self.shards.store_root(prefix, &new_root, &version).await?;
        Ok(())
    }

    /// Publishes the leaf separator(s) a split produced into the parent index so
    /// later descents route directly instead of walking right-links (ADR-031).
    ///
    /// Reconciles the leaf right-link chain against the parent: starting from
    /// the child the parent currently routes `split_key` to, it publishes every
    /// separator up to and including `split_key -> new_token` that the parent is
    /// missing. This heals a cascade — splitting a sibling whose own separator
    /// was never published still lands every intermediate separator — so the
    /// directory never grows unboundedly reliant on right-link walks. Idempotent
    /// (an already-published chain is a no-op) and re-drivable: a lost CAS is
    /// re-queued for a later sweep. On a successful insert that overflows the
    /// parent, recurses to split it.
    async fn publish_separators(
        &self,
        prefix: &str,
        split_key: &[u8],
        new_token: &str,
    ) -> Result<(), TransError> {
        for _ in 0..PARENT_RETRIES {
            let Some(parent) = self
                .dir
                .parent_index_for(prefix, split_key, Freshness::Latest)
                .await?
            else {
                // No index level (a single-leaf collection): nothing to publish.
                return Ok(());
            };
            let Some(index) = parent.node.as_index() else {
                return Ok(());
            };
            if index.child_for(split_key) == Some(new_token) {
                return Ok(()); // already published
            }
            let missing = self
                .missing_separators(prefix, &parent.node, split_key)
                .await?;
            if missing.is_empty() {
                return Ok(());
            }
            let mut new_index = index.clone();
            for (sep, tok) in &missing {
                new_index.insert_child(sep.clone(), tok.clone());
            }
            let mut updated = Node::index(new_index);
            updated.set_high_key(parent.node.high_key().map(<[u8]>::to_vec));
            updated.set_right_sibling(parent.node.right_sibling().map(str::to_string));

            let stored = if paths::is_collection_info(&parent.path) {
                // The root carries metadata the node view drops, so rewrite the
                // full root at the version we found it.
                let Some(pv) = parent.version.as_ref() else {
                    return Ok(());
                };
                let mut root = self.shards.load_root(prefix).await?.0;
                root.set_node(updated.clone());
                self.shards.store_root(prefix, &root, pv).await?
            } else {
                let token = paths::node_token_of(&parent.path)
                    .map_err(|e| StorageError::with_source("parsing parent token", e))?;
                self.shards
                    .store_node(prefix, &token, &updated, parent.version.as_ref())
                    .await?
            };
            if stored {
                // The inserts landed; a now-overflowing parent splits in turn.
                if updated.over_soft_cap(self.candidates.policy()) {
                    Box::pin(self.split_path(&parent.path)).await?;
                }
                return Ok(());
            }
            // Precondition miss: the parent changed, re-find and retry.
        }
        // Exhausted the retries: re-queue so a later sweep re-drives the
        // publication. Descent keeps working through right-links meanwhile.
        self.push_pending_separator(PendingSeparator {
            prefix: prefix.to_string(),
            split_key: split_key.to_vec(),
            new_token: new_token.to_string(),
        });
        Ok(())
    }

    /// The separators the parent `index` is missing along the leaf right-link
    /// chain up to `split_key`: starting from the child the parent routes
    /// `split_key` to, each `(boundary, right_token)` edge whose separator the
    /// parent does not yet record. Every collected separator is `<= split_key`,
    /// which the parent owns, so they all belong in this index.
    async fn missing_separators(
        &self,
        prefix: &str,
        parent: &Node,
        split_key: &[u8],
    ) -> Result<Vec<(Vec<u8>, String)>, TransError> {
        let Some(index) = parent.as_index() else {
            return Ok(Vec::new());
        };
        let Some(start) = index.child_for(split_key) else {
            return Ok(Vec::new());
        };
        let mut missing = Vec::new();
        let (mut cur, _) = self
            .shards
            .load_node(prefix, start, Freshness::Latest)
            .await?;
        for _ in 0..MAX_RECONCILE_HOPS {
            let (Some(right), Some(boundary)) = (cur.right_sibling(), cur.high_key()) else {
                break;
            };
            if boundary > split_key {
                break; // this sibling belongs beyond the target separator
            }
            let right = right.to_string();
            let boundary = boundary.to_vec();
            if index.child_for(&boundary) != Some(right.as_str()) {
                missing.push((boundary.clone(), right.clone()));
            }
            let reached_target = boundary.as_slice() == split_key;
            let (next, _) = self
                .shards
                .load_node(prefix, &right, Freshness::Latest)
                .await?;
            cur = next;
            if reached_target {
                break;
            }
        }
        Ok(missing)
    }
}

/// Splits `node` (a root leaf or root index) into a lower and an upper child for
/// an in-place root split, returning `(left, right, split_key)`. `left` links to
/// `right_token`; `right` inherits `node`'s former bounds.
fn split_into_children(node: &Node, right_token: &str) -> (Node, Node, Vec<u8>) {
    let mut source = node.clone();
    let (right, split_key) = source
        .split(right_token)
        .expect("root over the soft cap has at least two entries/children");
    (source, right, split_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_data::TxId;
    use glassdb_storage::{CollectionRoot, LockType, ObjectCache, ShardEntry, SharedCache};

    const COLL: &str = "db/coll";

    // A soft cap so tight a two-entry leaf is at the cap and a third overflows it,
    // and any three-child index overflows — so splits are driven by a handful of
    // keys instead of hundreds.
    fn tiny() -> SplitPolicy {
        SplitPolicy {
            leaf_max_entries: 2,
            leaf_max_bytes: 1 << 20,
            index_max_children: 2,
        }
    }

    fn store() -> ShardStore {
        ShardStore::new(ObjectCache::new(
            Arc::new(MemoryBackend::new()) as Arc<dyn Backend>,
            &SharedCache::new(1 << 20),
        ))
    }

    // A committed live key, so it counts as existing under a descent lookup.
    fn live(key: &[u8]) -> ShardEntry {
        ShardEntry {
            key: key.to_vec(),
            lock_type: LockType::None,
            locked_by: Vec::new(),
            current_writer: Some(TxId::from_bytes(vec![1])),
            deleted: false,
        }
    }

    fn leaf_node(keys: &[&[u8]], high: Option<&[u8]>, right: Option<&str>) -> Node {
        let mut n = Node::leaf(Shard::from_entries(keys.iter().map(|k| live(k))));
        n.set_high_key(high.map(<[u8]>::to_vec));
        n.set_right_sibling(right.map(str::to_string));
        n
    }

    fn splitter(shards: &ShardStore, bg: &Arc<Background>, policy: SplitPolicy) -> Splitter {
        Splitter::with_candidates(
            Arc::downgrade(bg),
            shards.clone(),
            SplitCandidates::new(policy),
        )
    }

    // A small collection whose single leaf lives in the root `_i`; when it grows
    // past the cap the root splits in place into a two-child index, raising the
    // height, and every key stays reachable in key order.
    #[tokio::test]
    async fn root_leaf_splits_in_place_into_an_index() {
        let s = store();
        let mut root = CollectionRoot::new();
        root.set_node(Node::leaf(Shard::from_entries(
            [b"a".as_slice(), b"b", b"c", b"d"].iter().map(|k| live(k)),
        )));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());

        splitter(&s, &bg, tiny())
            .split_path(&paths::collection_info(COLL))
            .await
            .unwrap();

        // The root is now an index (height grew from 1 to 2).
        let (node, _) = s
            .load_root_node(COLL, Freshness::Latest)
            .await
            .unwrap()
            .unwrap();
        assert!(node.as_index().is_some(), "root became an index");

        let dir = Directory::new(s.clone());
        let leaves = dir.leaves(COLL, Freshness::Latest).await.unwrap();
        assert_eq!(leaves.len(), 2, "one leaf became two");
        // The lower leaf is bounded by the split key and links to the upper one.
        assert!(leaves[0].node.right_sibling().is_some());
        assert_eq!(
            leaves[0].node.high_key(),
            Some(
                leaves[1]
                    .node
                    .as_leaf()
                    .unwrap()
                    .entries()
                    .next()
                    .unwrap()
                    .key
                    .as_slice()
            ),
        );
        // Every key remains reachable by descent, in order.
        for k in [b"a".as_slice(), b"b", b"c", b"d"] {
            let loc = dir.leaf_for(COLL, k, Freshness::Latest).await.unwrap();
            assert!(loc.node.as_leaf().unwrap().exists(k), "key {k:?} lost");
        }
    }

    // A standalone leaf over the cap half-splits: the upper half moves to a fresh
    // sibling, the source shrinks and links to it, and the parent index learns
    // the separator so later descents skip the right-link hop.
    #[tokio::test]
    async fn nonroot_leaf_half_splits_and_parent_learns_the_separator() {
        let s = store();
        s.store_node(
            COLL,
            "L",
            &leaf_node(&[b"a", b"b", b"c", b"d"], None, None),
            None,
        )
        .await
        .unwrap();
        let mut root = CollectionRoot::new();
        root.set_node(Node::index(IndexNode::from_children([(
            Vec::new(),
            "L".to_string(),
        )])));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());

        splitter(&s, &bg, tiny())
            .split_path(&paths::from_node(COLL, "L"))
            .await
            .unwrap();

        let dir = Directory::new(s.clone());
        let leaves = dir.leaves(COLL, Freshness::Latest).await.unwrap();
        assert_eq!(leaves.len(), 2, "leaf L split into two");
        // The parent index now routes the moved keys directly to the sibling, not
        // via a right-link walk: its child for the split key differs from L.
        let (root_node, _) = s
            .load_root_node(COLL, Freshness::Latest)
            .await
            .unwrap()
            .unwrap();
        let index = root_node.as_index().unwrap();
        assert_eq!(index.len(), 2, "parent gained the separator");
        for k in [b"a".as_slice(), b"b", b"c", b"d"] {
            let loc = dir.leaf_for(COLL, k, Freshness::Latest).await.unwrap();
            assert!(loc.node.as_leaf().unwrap().exists(k), "key {k:?} lost");
        }
    }

    // An index root that overflows its fan-out splits in place: two index children
    // are created and the root is rewritten over them, so all original children
    // remain reachable one level deeper.
    #[tokio::test]
    async fn root_index_splits_in_place_growing_height() {
        let s = store();
        // Three leaves under a three-child index root (over a two-child cap).
        for (tok, keys, high, right) in [
            (
                "L0",
                vec![b"a".as_slice()],
                Some(b"m".as_slice()),
                Some("L1"),
            ),
            (
                "L1",
                vec![b"m".as_slice()],
                Some(b"t".as_slice()),
                Some("L2"),
            ),
            ("L2", vec![b"t".as_slice()], None, None),
        ] {
            s.store_node(COLL, tok, &leaf_node(&keys, high, right), None)
                .await
                .unwrap();
        }
        let mut root = CollectionRoot::new();
        root.set_node(Node::index(IndexNode::from_children([
            (Vec::new(), "L0".to_string()),
            (b"m".to_vec(), "L1".to_string()),
            (b"t".to_vec(), "L2".to_string()),
        ])));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());

        splitter(&s, &bg, tiny())
            .split_path(&paths::collection_info(COLL))
            .await
            .unwrap();

        let (node, _) = s
            .load_root_node(COLL, Freshness::Latest)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            node.as_index().unwrap().len(),
            2,
            "root now has two index children"
        );
        // Every original leaf is still reached in order (now via one more hop).
        let dir = Directory::new(s.clone());
        for k in [b"a".as_slice(), b"m", b"t"] {
            let loc = dir.leaf_for(COLL, k, Freshness::Latest).await.unwrap();
            assert!(loc.node.as_leaf().unwrap().exists(k), "key {k:?} lost");
        }
    }

    // Re-running a split on a node already back under the cap is a no-op: the
    // splitter reloads, sees it is not over the cap, and leaves the tree alone.
    #[tokio::test]
    async fn re_split_of_a_settled_node_is_a_noop() {
        let s = store();
        let mut root = CollectionRoot::new();
        root.set_node(Node::leaf(Shard::from_entries(
            [b"a".as_slice(), b"b", b"c", b"d"].iter().map(|k| live(k)),
        )));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());
        let sp = splitter(&s, &bg, tiny());

        sp.split_path(&paths::collection_info(COLL)).await.unwrap();
        let after_first = Directory::new(s.clone())
            .leaves(COLL, Freshness::Latest)
            .await
            .unwrap();
        // Re-run: each resulting leaf holds two keys, which is at (not over) the
        // cap, so nothing changes.
        for leaf in &after_first {
            sp.split_path(&leaf.path).await.unwrap();
        }
        sp.split_path(&paths::collection_info(COLL)).await.unwrap();

        let after_second = Directory::new(s.clone())
            .leaves(COLL, Freshness::Latest)
            .await
            .unwrap();
        assert_eq!(
            after_first.len(),
            after_second.len(),
            "a settled tree does not keep splitting"
        );
    }

    // The candidate feed drives run_once end to end: a leaf pushed over the cap is
    // drained and split; the byte/entry gate keeps under-cap leaves out.
    #[tokio::test]
    async fn feed_drives_run_once() {
        let s = store();
        let mut root = CollectionRoot::new();
        root.set_node(Node::leaf(Shard::from_entries(
            [b"a".as_slice(), b"b", b"c", b"d"].iter().map(|k| live(k)),
        )));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());

        let candidates = SplitCandidates::new(tiny());
        // Under the cap: not enqueued.
        candidates.observe_leaf(
            &paths::collection_info(COLL),
            &Shard::from_entries([live(b"a"), live(b"b")]),
        );
        assert!(
            candidates.drain().is_empty(),
            "at-cap leaf is not a candidate"
        );
        // Over the cap: enqueued and split by a sweep.
        candidates.observe_leaf(
            &paths::collection_info(COLL),
            &Shard::from_entries([live(b"a"), live(b"b"), live(b"c"), live(b"d")]),
        );
        let sp = Splitter::with_candidates(Arc::downgrade(&bg), s.clone(), candidates);
        sp.run_once().await;

        let leaves = Directory::new(s.clone())
            .leaves(COLL, Freshness::Latest)
            .await
            .unwrap();
        assert_eq!(leaves.len(), 2, "the fed candidate was split");
    }

    // ADR-031 byte cap: a leaf well under the entry cap but over the encoded
    // byte cap is still fed and split. Regression for the byte cap having no
    // producer (only the entry-count crossing used to enqueue).
    #[tokio::test]
    async fn byte_cap_enqueues_and_splits_below_entry_cap() {
        let s = store();
        let mut root = CollectionRoot::new();
        root.set_node(Node::leaf(Shard::from_entries(
            [b"a".as_slice(), b"b", b"c", b"d"].iter().map(|k| live(k)),
        )));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());

        // A generous entry cap but a tiny byte cap: the four-entry leaf is far
        // under the entry cap yet over the byte cap.
        let policy = SplitPolicy {
            leaf_max_entries: 1000,
            leaf_max_bytes: 8,
            index_max_children: 1000,
        };
        let candidates = SplitCandidates::new(policy);
        candidates.observe_leaf(
            &paths::collection_info(COLL),
            &Shard::from_entries([live(b"a"), live(b"b"), live(b"c"), live(b"d")]),
        );

        let sp = Splitter::with_candidates(Arc::downgrade(&bg), s.clone(), candidates);
        sp.run_once().await;

        // The only cap crossed is the byte cap, so a split here proves the byte
        // cap now has a producer.
        let leaves = Directory::new(s.clone())
            .leaves(COLL, Freshness::Latest)
            .await
            .unwrap();
        assert_eq!(leaves.len(), 2, "byte-cap overflow triggered a split");
    }

    // ADR-031 cascade healing: splitting a sibling whose own separator was never
    // published still lands every separator. The parent index knows only the
    // leftmost child P0, while the leaf chain P0 -> S already extends past it via
    // a right-link (S's separator was never published). When S splits,
    // publication reconciles the whole chain, so the parent learns both the
    // previously-missing `S` separator and the new one — the directory is never
    // left permanently reliant on a right-link walk.
    #[tokio::test]
    async fn splitting_an_unpublished_sibling_reconciles_the_chain() {
        let s = store();
        s.store_node(
            COLL,
            "P0",
            &leaf_node(&[b"a", b"b"], Some(b"m"), Some("S")),
            None,
        )
        .await
        .unwrap();
        s.store_node(COLL, "S", &leaf_node(&[b"m", b"n", b"o"], None, None), None)
            .await
            .unwrap();
        let mut root = CollectionRoot::new();
        root.set_node(Node::index(IndexNode::from_children([(
            Vec::new(),
            "P0".to_string(),
        )])));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());

        // Tiny leaf cap so S splits, but a wide fan-out so the parent index does
        // not itself overflow — keeping the assertion on its separators direct.
        let policy = SplitPolicy {
            leaf_max_entries: 2,
            leaf_max_bytes: 1 << 20,
            index_max_children: 100,
        };
        splitter(&s, &bg, policy)
            .split_path(&paths::from_node(COLL, "S"))
            .await
            .unwrap();

        // The parent index now records the previously-missing `m -> S` separator
        // and the new one produced by S's split (`n`).
        let (root_node, _) = s
            .load_root_node(COLL, Freshness::Latest)
            .await
            .unwrap()
            .unwrap();
        let seps: Vec<Vec<u8>> = root_node
            .as_index()
            .unwrap()
            .children()
            .map(|(sep, _)| sep.to_vec())
            .collect();
        assert_eq!(
            seps,
            vec![b"".to_vec(), b"m".to_vec(), b"n".to_vec()],
            "the whole chain's separators are published"
        );

        // Every key is still reachable in order.
        let dir = Directory::new(s.clone());
        for k in [b"a".as_slice(), b"b", b"m", b"n", b"o"] {
            let loc = dir.leaf_for(COLL, k, Freshness::Latest).await.unwrap();
            assert!(loc.node.as_leaf().unwrap().exists(k), "key {k:?} lost");
        }
    }

    // ADR-031 durable retry path: a separator whose parent CAS keeps losing is
    // re-queued and published by a later sweep, so the directory is not left
    // permanently reliant on a right-link walk. A backend that blocks writes to
    // the root `_i` forces the publication to give up; healing it lets the
    // re-driven publication land.
    #[tokio::test]
    async fn lost_parent_cas_is_republished_by_a_later_sweep() {
        let backend = BlockRootWrites::new(Arc::new(MemoryBackend::new()));
        let s = ShardStore::new(ObjectCache::new(
            backend.clone() as Arc<dyn Backend>,
            &SharedCache::new(1 << 20),
        ));

        // A root index over a single leaf L[a,b,c] (over the cap).
        s.store_node(COLL, "L", &leaf_node(&[b"a", b"b", b"c"], None, None), None)
            .await
            .unwrap();
        let mut root = CollectionRoot::new();
        root.set_node(Node::index(IndexNode::from_children([(
            Vec::new(),
            "L".to_string(),
        )])));
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());
        let sp = splitter(&s, &bg, tiny());

        // Block the parent `_i` CAS: the split lands (L shrinks, a sibling is
        // created) but the separator publication cannot, so it is re-queued.
        backend.block(true);
        sp.split_path(&paths::from_node(COLL, "L")).await.unwrap();
        let (blocked_root, _) = s
            .load_root_node(COLL, Freshness::Latest)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            blocked_root.as_index().unwrap().len(),
            1,
            "separator is not published while the parent CAS is blocked"
        );
        assert_eq!(
            Directory::new(s.clone())
                .leaves(COLL, Freshness::Latest)
                .await
                .unwrap()
                .len(),
            2,
            "the leaves still split; only the parent separator is missing"
        );

        // Heal and sweep: the re-queued separator is published.
        backend.block(false);
        sp.run_once().await;
        let (healed_root, _) = s
            .load_root_node(COLL, Freshness::Latest)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            healed_root.as_index().unwrap().len(),
            2,
            "the deferred separator is republished by a later sweep"
        );
    }

    /// Test backend that, while blocked, fails every conditional write to a
    /// collection root `_i` with a precondition miss, so a split's parent
    /// separator CAS cannot land. Everything else passes through.
    struct BlockRootWrites {
        inner: Arc<dyn Backend>,
        blocked: std::sync::atomic::AtomicBool,
    }

    impl BlockRootWrites {
        fn new(inner: Arc<dyn Backend>) -> Arc<Self> {
            Arc::new(BlockRootWrites {
                inner,
                blocked: std::sync::atomic::AtomicBool::new(false),
            })
        }
        fn block(&self, on: bool) {
            self.blocked.store(on, std::sync::atomic::Ordering::SeqCst);
        }
        fn blocks(&self, path: &str) -> bool {
            self.blocked.load(std::sync::atomic::Ordering::SeqCst) && path.ends_with("/_i")
        }
    }

    #[async_trait::async_trait]
    impl Backend for BlockRootWrites {
        async fn read(
            &self,
            path: &str,
        ) -> Result<glassdb_backend::ReadReply, glassdb_backend::BackendError> {
            self.inner.read(path).await
        }
        async fn read_if_modified(
            &self,
            path: &str,
            expected: &glassdb_backend::Version,
        ) -> Result<glassdb_backend::ReadReply, glassdb_backend::BackendError> {
            self.inner.read_if_modified(path, expected).await
        }
        async fn write(
            &self,
            path: &str,
            value: Vec<u8>,
        ) -> Result<glassdb_backend::Version, glassdb_backend::BackendError> {
            self.inner.write(path, value).await
        }
        async fn write_if(
            &self,
            path: &str,
            value: Vec<u8>,
            expected: &glassdb_backend::Version,
        ) -> Result<glassdb_backend::Version, glassdb_backend::BackendError> {
            if self.blocks(path) {
                return Err(glassdb_backend::BackendError::Precondition);
            }
            self.inner.write_if(path, value, expected).await
        }
        async fn write_if_not_exists(
            &self,
            path: &str,
            value: Vec<u8>,
        ) -> Result<glassdb_backend::Version, glassdb_backend::BackendError> {
            if self.blocks(path) {
                return Err(glassdb_backend::BackendError::Precondition);
            }
            self.inner.write_if_not_exists(path, value).await
        }
        async fn delete(&self, path: &str) -> Result<(), glassdb_backend::BackendError> {
            self.inner.delete(path).await
        }
        async fn list(&self, dir_path: &str) -> Result<Vec<String>, glassdb_backend::BackendError> {
            self.inner.list(dir_path).await
        }
    }
}
