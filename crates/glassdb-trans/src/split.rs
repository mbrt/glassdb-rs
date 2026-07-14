//! Background growth of the B-link coordination tree by leaf and node splits
//! (ADR-031).
//!
//! Coordination objects are grow-only: a leaf that crosses its soft cap is
//! halved so no single object becomes a scalability or contention bottleneck.
//! Splitting runs off the hot path in a periodic background task, fed
//! candidates the coordinator observes as it writes leaves — never a key-space
//! enumeration.
//!
//! Every split is a sequence of independent, idempotent compare-and-swaps under
//! a one-node structure-write lock. A database-wide `_s` record is written
//! before any node object is created, so recovery can keep or delete created
//! nodes from tree reachability after a crash:
//!
//! 0. Write the structural record with the source version and created tokens.
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
//! A leaf split acquires structure-write through the shared
//! [`ShardCoordinator`], in the same folded CAS stream as data mutations on
//! that leaf. Interior indexes and roots still use direct structural CASes.
//! The source shrink (or root rewrite) releases structure-write inline, so no
//! unlocked post-split state is exposed before a separate release CAS.
//!
//! The collection root `_i` cannot move (its address is fixed), so when it
//! overflows it splits **in place**: two children are created and the root is
//! rewritten into a two-entry index over them, growing the tree's height while
//! preserving the collection metadata.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use glassdb_backend as backend;
use glassdb_concurr::{Background, Clock, RetryConfig, rt};
use glassdb_data::{TxId, paths};
use glassdb_storage::{
    Directory, Freshness, IndexNode, LockType, Node, Shard, ShardStore, SplitPolicy, StorageError,
    StructuralLog, TxCommitStatus, TxLog,
};
use tokio::sync::Notify;

use crate::error::TransError;
use crate::monitor::{Monitor, PENDING_TX_TIMEOUT};
use crate::node_locking::{NodeLockReconciler, StructureWriteResolver};
use crate::resolver::Resolver;
use crate::shard_coord::{FoldOutcome, ShardCoordinator, SplitHinter};

/// How often the splitter drains its candidate queue. A split is a handful of
/// CAS round-trips, so a tight cadence keeps overflowing leaves short-lived.
const SPLIT_INTERVAL: Duration = Duration::from_secs(1);

/// Retry unresolved structural records at the transaction-liveness horizon.
const STRUCTURAL_RECOVERY_ACTIVE_INTERVAL: Duration = PENDING_TX_TIMEOUT;

/// Back off empty structural-log listings independently of split candidates.
const STRUCTURAL_RECOVERY_IDLE_INTERVAL: Duration = Duration::from_secs(60);

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
    clock: Clock,
    queue: Arc<Mutex<VecDeque<SplitCandidate>>>,
}

#[derive(Clone)]
struct SplitCandidate {
    path: String,
    priority: TxId,
}

impl SplitCandidates {
    /// Creates an empty candidate feed using `clock` for wound-wait priority.
    fn with_clock(policy: SplitPolicy, clock: Clock) -> Self {
        SplitCandidates {
            policy,
            clock,
            queue: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// The soft-cap policy shared by the feed and the splitter.
    pub(crate) fn policy(&self) -> &SplitPolicy {
        &self.policy
    }

    /// Drains every queued candidate, de-duplicated, for one sweep cycle.
    fn drain(&self) -> Vec<SplitCandidate> {
        let mut q = self.queue.lock().unwrap();
        let mut by_path = std::collections::BTreeMap::<String, SplitCandidate>::new();
        while let Some(candidate) = q.pop_front() {
            match by_path.get_mut(&candidate.path) {
                Some(current) if candidate.priority.older(&current.priority) => {
                    *current = candidate;
                }
                None => {
                    by_path.insert(candidate.path.clone(), candidate);
                }
                _ => {}
            }
        }
        by_path.into_values().collect()
    }

    /// Requeues a deferred split without changing its wound-wait priority.
    fn requeue(&self, candidate: SplitCandidate) {
        let mut q = self.queue.lock().unwrap();
        if q.len() >= CANDIDATE_QUEUE_CAP {
            q.pop_front();
        }
        q.push_back(candidate);
    }

    /// Mints an operation id at normal transaction priority.
    fn new_id(&self) -> TxId {
        TxId::new_at(self.clock.now())
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
        q.push_back(SplitCandidate {
            path: path.to_string(),
            priority: self.new_id(),
        });
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
    mon: Monitor,
    resolver: Resolver,
    db_root: String,
    // The leaf-candidate feed this splitter drains. A clone injected into the
    // coordinator at construction reports over-cap leaf writes into it.
    candidates: SplitCandidates,
    // Separators a split could not publish on the first try; re-driven each
    // sweep so the parent index eventually learns them (ADR-031). Purely
    // splitter-internal — the coordinator never sees it.
    pending: Arc<Mutex<VecDeque<PendingSeparator>>>,
    // Wakes the independent recovery loop when a local split leaves `_s` work.
    recovery_wake: Arc<Notify>,
    // Co-wired with this splitter over the candidate feed at construction.
    // Only leaf structure-write acquisition uses it; root and interior nodes
    // remain direct structural CASes.
    coord: ShardCoordinator,
}

impl Splitter {
    /// Builds a splitter and coordinator that share one split-candidate feed.
    pub fn with_coordinator(
        bg: Weak<Background>,
        shards: ShardStore,
        mon: Monitor,
        clock: Clock,
        retry: RetryConfig,
        db_root: &str,
        policy: SplitPolicy,
    ) -> (ShardCoordinator, Self) {
        let candidates = SplitCandidates::with_clock(policy, clock);
        let resolver = Resolver::new(shards.clone(), mon.clone());
        let coord = ShardCoordinator::with_hinter(
            shards.clone(),
            resolver.clone(),
            mon.clone(),
            retry,
            policy,
            Arc::new(candidates.clone()),
        );
        let splitter = Splitter::with_candidates(
            bg,
            shards,
            mon,
            resolver,
            db_root,
            coord.clone(),
            candidates,
        );
        (coord, splitter)
    }

    /// Creates a splitter over an explicitly co-wired coordinator and feed.
    fn with_candidates(
        bg: Weak<Background>,
        shards: ShardStore,
        mon: Monitor,
        resolver: Resolver,
        db_root: &str,
        coord: ShardCoordinator,
        candidates: SplitCandidates,
    ) -> Self {
        let dir = Directory::new(shards.clone());
        Splitter {
            bg,
            shards,
            dir,
            mon,
            resolver,
            db_root: db_root.to_string(),
            candidates,
            pending: Arc::new(Mutex::new(VecDeque::new())),
            recovery_wake: Arc::new(Notify::new()),
            coord,
        }
    }

    /// Adds a child name to a parent collection under the root's structure-read
    /// protocol.
    pub async fn register_subcollection(
        &self,
        parent_prefix: &str,
        name: &[u8],
    ) -> Result<(), TransError> {
        let id = self.candidates.new_id();
        self.mon.begin_tx(&id);
        let mut acquired = false;
        for _ in 0..50 {
            let (mut root, version) = self.shards.load_root(parent_prefix).await?;
            let locks = root.node_locks_mut();
            if locks.structure().lock_type() == LockType::Write && !locks.structure().contains(&id)
            {
                rt::sleep(Duration::from_millis(5)).await;
                continue;
            }
            locks.add_structure_reader(id.clone());
            if self
                .shards
                .store_root(parent_prefix, &root, &version)
                .await?
            {
                acquired = true;
                break;
            }
        }
        if !acquired {
            self.mon.abort_tx(&id).await?;
            return Err(TransError::Retry);
        }

        let result = async {
            loop {
                let (mut root, version) = self.shards.load_root(parent_prefix).await?;
                if !root.node().structure_lock().contains(&id) {
                    return Err(TransError::Retry);
                }
                if !root.add_subcollection(name.to_vec()) {
                    return Ok(());
                }
                let content_limit = self
                    .candidates
                    .policy()
                    .node_max_bytes
                    .saturating_sub(self.candidates.policy().split_headroom_bytes);
                let mut index_root = root.clone();
                index_root.set_node(Node::index(IndexNode::from_children([
                    (Vec::new(), "x".repeat(24)),
                    (vec![0], "y".repeat(24)),
                ])));
                if root.content_encoded_len() > content_limit
                    || index_root.content_encoded_len() > content_limit
                {
                    return Err(TransError::InvalidInput(
                        "subcollection directory exceeds the node size limit".into(),
                    ));
                }
                if self
                    .shards
                    .store_root(parent_prefix, &root, &version)
                    .await?
                {
                    return Ok(());
                }
            }
        }
        .await;
        let _ = self.release_structure_write(parent_prefix, None, &id).await;
        let _ = self.mon.abort_tx(&id).await;
        result
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

    /// Starts independent split-candidate and structural-recovery loops.
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
        let recovery = self.clone();
        bg.spawn(async move {
            loop {
                let active = recovery.recover_structural_logs().await;
                let delay = if active {
                    STRUCTURAL_RECOVERY_ACTIVE_INTERVAL
                } else {
                    STRUCTURAL_RECOVERY_IDLE_INTERVAL
                };
                tokio::select! {
                    _ = rt::sleep(delay) => {}
                    _ = recovery.recovery_wake.notified() => {}
                }
            }
        });
    }

    /// Runs one sweep: split every queued candidate. Best-effort — a transient
    /// error on one candidate only defers its split to a later cycle, so it is
    /// logged and the sweep continues.
    async fn run_once(&self) {
        for candidate in self.candidates.drain() {
            if let Err(e) = self
                .split_path_with_id(&candidate.path, candidate.priority.renew())
                .await
            {
                tracing::debug!(path = %candidate.path, error = %e, "split candidate deferred");
                if !matches!(e, TransError::InvalidInput(_)) {
                    self.candidates.requeue(candidate);
                }
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
        self.split_path_with_id(path, self.candidates.new_id())
            .await
    }

    /// Splits `path` using an already-aged wound-wait priority.
    async fn split_path_with_id(&self, path: &str, id: TxId) -> Result<(), TransError> {
        let pr = paths::parse(path)
            .map_err(|e| StorageError::with_source("parsing candidate path", e))?;
        if paths::is_collection_info(path) {
            self.split_root(&pr.prefix, id).await
        } else {
            self.split_nonroot(&pr.prefix, &pr.suffix, id).await
        }
    }

    /// Acquires a source node's structure-write lock under wound-wait. A leaf
    /// joins the shared coordinator round; roots and interior indexes use the
    /// direct structural CAS path because they carry no data-mutation traffic.
    async fn acquire_structure_write(
        &self,
        prefix: &str,
        token: Option<&str>,
        id: &TxId,
    ) -> Result<Option<(Node, backend::Version)>, TransError> {
        if let Some(token) = token {
            let (node, _) = self
                .shards
                .load_node(prefix, token, Freshness::Latest)
                .await?;
            if node.as_leaf().is_some() {
                return self
                    .acquire_leaf_structure_write(&self.coord, prefix, token, id)
                    .await;
            }
        }
        self.acquire_structure_write_direct(prefix, token, id).await
    }

    /// Acquires a leaf's structure-write through the shard coordinator, then
    /// reloads the landed version needed by the split's shrink CAS.
    async fn acquire_leaf_structure_write(
        &self,
        coord: &ShardCoordinator,
        prefix: &str,
        token: &str,
        id: &TxId,
    ) -> Result<Option<(Node, backend::Version)>, TransError> {
        let path = paths::from_node(prefix, token);
        let outcome = coord
            .submit_shard(
                &path,
                id,
                Arc::new(StructureWriteResolver::new(id.clone(), path.clone())),
                Freshness::Latest,
            )
            .await?;
        if !matches!(
            outcome,
            Some(FoldOutcome::Locked {
                typ: LockType::Write,
                ..
            })
        ) {
            return Ok(None);
        }

        let (node, version) = self
            .shards
            .load_node(prefix, token, Freshness::Latest)
            .await?;
        if node.structure_lock().lock_type() == LockType::Write
            && node.structure_lock().contains(id)
        {
            Ok(Some((node, version)))
        } else {
            Ok(None)
        }
    }

    /// Direct structure-write acquisition for roots and interior index nodes.
    async fn acquire_structure_write_direct(
        &self,
        prefix: &str,
        token: Option<&str>,
        id: &TxId,
    ) -> Result<Option<(Node, backend::Version)>, TransError> {
        for _ in 0..PARENT_RETRIES {
            let (mut node, version) = match token {
                Some(token) => {
                    self.shards
                        .load_node(prefix, token, Freshness::Latest)
                        .await?
                }
                None => match self.shards.load_root(prefix).await {
                    Ok((root, version)) => (root.node().clone(), version),
                    Err(StorageError::NotFound) => return Ok(None),
                    Err(e) => return Err(e.into()),
                },
            };
            if node.structure_lock().lock_type() == LockType::Write
                && node.structure_lock().contains(id)
            {
                return Ok(Some((node, version)));
            }

            let mut locks = node.locks().clone();
            let reconciler = NodeLockReconciler::new(&self.mon, id);
            if reconciler
                .acquire_structure_write(&mut locks)
                .await?
                .is_some()
            {
                return Ok(None);
            }

            self.resolve_node_entries(prefix, &mut node, id).await?;
            node.set_locks(locks);
            if self
                .store_structural_node(prefix, token, &node, &version)
                .await?
            {
                let (_, locked_version) = match token {
                    Some(token) => {
                        self.shards
                            .load_node(prefix, token, Freshness::Latest)
                            .await?
                    }
                    None => {
                        let (root, version) = self.shards.load_root(prefix).await?;
                        (root.node().clone(), version)
                    }
                };
                return Ok(Some((node, locked_version)));
            }
        }
        Ok(None)
    }

    /// Releases a structure-write holder after its node mutation has landed.
    async fn release_structure_write(
        &self,
        prefix: &str,
        token: Option<&str>,
        id: &TxId,
    ) -> Result<(), TransError> {
        for _ in 0..PARENT_RETRIES {
            let (mut node, version) = match token {
                Some(token) => {
                    self.shards
                        .load_node(prefix, token, Freshness::Latest)
                        .await?
                }
                None => {
                    let (root, version) = self.shards.load_root(prefix).await?;
                    (root.node().clone(), version)
                }
            };
            if !node.remove_structure_holder(id) {
                return Ok(());
            }
            if self
                .store_structural_node(prefix, token, &node, &version)
                .await?
            {
                return Ok(());
            }
        }
        Err(TransError::Retry)
    }

    /// Stores a complete root or non-root node at an expected version.
    async fn store_structural_node(
        &self,
        prefix: &str,
        token: Option<&str>,
        node: &Node,
        version: &backend::Version,
    ) -> Result<bool, TransError> {
        match token {
            Some(token) => Ok(self
                .shards
                .store_node(prefix, token, node, Some(version))
                .await?),
            None => {
                let (mut root, current) = self.shards.load_root(prefix).await?;
                if current != *version {
                    return Ok(false);
                }
                root.set_node(node.clone());
                Ok(self.shards.store_root(prefix, &root, version).await?)
            }
        }
    }

    /// Help-forwards finalized entry holders before a split removes their
    /// structure-read holders.
    async fn resolve_node_entries(
        &self,
        prefix: &str,
        node: &mut Node,
        id: &TxId,
    ) -> Result<(), TransError> {
        let Some(shard) = node.as_leaf() else {
            return Ok(());
        };
        let mut entries = Vec::with_capacity(shard.len());
        for entry in shard.entries() {
            let resolved = self
                .resolver
                .resolve_holders(&paths::from_key(prefix, &entry.key), entry, Some(id))
                .await?;
            let mut entry = entry.clone();
            entry.current_writer = resolved.writer;
            entry.deleted = resolved.deleted;
            entry
                .locked_by
                .retain(|holder| holder == id || resolved.pending.contains(holder));
            if entry.locked_by.is_empty() {
                entry.lock_type = LockType::None;
            }
            entries.push(entry);
        }
        node.set_leaf(Shard::from_entries(entries))?;
        Ok(())
    }

    /// Fences the source writer before recovery classifies created nodes.
    async fn fence_source_writer_for_recovery(
        &self,
        prefix: &str,
        token: Option<&str>,
    ) -> Result<bool, TransError> {
        for _ in 0..PARENT_RETRIES {
            let node = match token {
                Some(token) => match self
                    .shards
                    .load_node(prefix, token, Freshness::Latest)
                    .await
                {
                    Ok((node, _)) => node,
                    Err(StorageError::NotFound) => return Ok(true),
                    Err(e) => return Err(e.into()),
                },
                None => match self
                    .shards
                    .load_root_node(prefix, Freshness::Latest)
                    .await?
                {
                    Some((node, _)) => node,
                    None => return Ok(true),
                },
            };
            if node.structure_lock().lock_type() != LockType::Write {
                return Ok(true);
            }
            let Some(holder) = node.structure_lock().holders().first() else {
                return Ok(true);
            };
            if self.mon.tx_status(holder).await? == TxCommitStatus::Pending {
                return Ok(false);
            }
            // A finalized holder may still have a shrink CAS in flight. This
            // cleanup CAS either wins first, fencing that shrink, or loses to
            // it and the next iteration observes the landed right-link.
            self.release_structure_write(prefix, token, holder).await?;
        }
        Err(TransError::Retry)
    }

    /// Halves a standalone node and finalizes its wound-wait participant.
    async fn split_nonroot(&self, prefix: &str, token: &str, id: TxId) -> Result<(), TransError> {
        self.mon.begin_tx(&id);
        let result = self.coordinate_nonroot_split(prefix, token, &id).await;
        self.finalize_split(&id).await;
        if result.is_err() {
            self.recovery_wake.notify_one();
        }
        result
    }

    /// Performs the write-ahead, sibling creation, shrink, and publication.
    async fn coordinate_nonroot_split(
        &self,
        prefix: &str,
        token: &str,
        id: &TxId,
    ) -> Result<(), TransError> {
        let Some((mut node, version)) = self
            .acquire_structure_write(prefix, Some(token), id)
            .await?
        else {
            return Err(TransError::Retry);
        };
        if !node.over_soft_cap(self.candidates.policy()) {
            return self.release_structure_write(prefix, Some(token), id).await;
        }

        let right_token = paths::random_node_token();
        let Some((right, split_key)) = node.split(&right_token) else {
            return self.release_structure_write(prefix, Some(token), id).await;
        };
        node.remove_structure_holder(id);

        let record_id = right_token.clone();
        self.shards
            .write_structural_log(
                &record_id,
                &StructuralLog {
                    prefix: prefix.to_string(),
                    source_token: token.to_string(),
                    source_version: version.token.to_string(),
                    created_tokens: vec![right_token.clone()],
                    split_key: split_key.clone(),
                    is_root: false,
                },
            )
            .await?;

        if !self
            .shards
            .store_node(prefix, &right_token, &right, None)
            .await?
        {
            return Err(TransError::Retry);
        }
        if !self
            .shards
            .store_node(prefix, token, &node, Some(&version))
            .await?
        {
            return Err(TransError::Retry);
        }
        self.publish_separators(prefix, &split_key, &right_token)
            .await?;
        self.shards
            .delete_structural_log(&self.db_root, &record_id)
            .await?;
        Ok(())
    }

    /// Grows an overflowing collection root into a two-child index.
    async fn split_root(&self, prefix: &str, id: TxId) -> Result<(), TransError> {
        self.mon.begin_tx(&id);
        let result = self.coordinate_root_split(prefix, &id).await;
        self.finalize_split(&id).await;
        if result.is_err() {
            self.recovery_wake.notify_one();
        }
        result
    }

    /// Performs the write-ahead, child creation, and root rewrite.
    async fn coordinate_root_split(&self, prefix: &str, id: &TxId) -> Result<(), TransError> {
        let Some((node, version)) = self.acquire_structure_write(prefix, None, id).await? else {
            return Err(TransError::Retry);
        };
        if !node.over_soft_cap(self.candidates.policy()) {
            return self.release_structure_write(prefix, None, id).await;
        }

        let l_token = paths::random_node_token();
        let r_token = paths::random_node_token();
        let (left, right, split_key) = split_into_children(&node, &r_token, id);
        let root_index =
            IndexNode::from_children([(Vec::new(), l_token.clone()), (split_key, r_token.clone())]);
        let index = Node::index(root_index);
        let mut sized_root = self.shards.load_root(prefix).await?.0;
        sized_root.set_node(index.clone());
        let content_limit = self
            .candidates
            .policy()
            .node_max_bytes
            .saturating_sub(self.candidates.policy().split_headroom_bytes);
        if sized_root.content_encoded_len() > content_limit
            || sized_root.encoded_len() > self.candidates.policy().node_max_bytes
        {
            self.release_structure_write(prefix, None, id).await?;
            return Err(TransError::InvalidInput(
                "root metadata exceeds the coordination node size limit".into(),
            ));
        }

        let record_id = r_token.clone();
        self.shards
            .write_structural_log(
                &record_id,
                &StructuralLog {
                    prefix: prefix.to_string(),
                    source_token: String::new(),
                    source_version: version.token.to_string(),
                    created_tokens: vec![l_token.clone(), r_token.clone()],
                    split_key: Vec::new(),
                    is_root: true,
                },
            )
            .await?;

        if !self
            .shards
            .store_node(prefix, &l_token, &left, None)
            .await?
            || !self
                .shards
                .store_node(prefix, &r_token, &right, None)
                .await?
        {
            return Err(TransError::Retry);
        }
        if !self
            .store_structural_node(prefix, None, &index, &version)
            .await?
        {
            return Err(TransError::Retry);
        }
        self.shards
            .delete_structural_log(&self.db_root, &record_id)
            .await?;
        Ok(())
    }

    /// Finalizes the split's ephemeral wound-wait identity without creating a
    /// transaction object. Structural state, not transaction status, records
    /// the split's durable outcome.
    async fn finalize_split(&self, id: &TxId) {
        if let Err(e) = self
            .mon
            .commit_tx(TxLog::new(id.clone(), TxCommitStatus::Ok))
            .await
        {
            tracing::debug!(error = %e, "finalizing split transaction failed");
        }
    }

    /// Releases a structural lock and finalizes its wound-wait identity.
    async fn finish_without_split(
        &self,
        prefix: &str,
        token: Option<&str>,
        id: &TxId,
    ) -> Result<(), TransError> {
        let release = self.release_structure_write(prefix, token, id).await;
        self.finalize_split(id).await;
        release?;
        Ok(())
    }

    /// Recovers every unresolved structural record in this database.
    async fn recover_structural_logs(&self) -> bool {
        let records = match self.shards.list_structural_logs(&self.db_root).await {
            Ok(records) => records,
            Err(e) => {
                tracing::debug!(error = %e, "listing structural records failed");
                return true;
            }
        };
        let active = !records.is_empty();
        for (record_id, record) in records {
            if let Err(e) = self.recover_record(&record_id, &record).await {
                tracing::debug!(record = %record_id, error = %e, "structural recovery deferred");
            }
        }
        active
    }

    /// Resolves one structural record from fenced tree reachability.
    async fn recover_record(
        &self,
        record_id: &str,
        record: &StructuralLog,
    ) -> Result<(), TransError> {
        let source_token = (!record.is_root).then_some(record.source_token.as_str());
        if !self
            .fence_source_writer_for_recovery(&record.prefix, source_token)
            .await?
        {
            return Err(TransError::Retry);
        }

        let reachable = self
            .dir
            .reachable_tokens(&record.prefix, Freshness::Latest)
            .await?;
        let applied = !record.created_tokens.is_empty()
            && record
                .created_tokens
                .iter()
                .all(|token| reachable.contains(token));
        if applied && !record.is_root {
            let right_token = record
                .created_tokens
                .first()
                .ok_or_else(|| TransError::InvalidInput("split record has no sibling".into()))?;
            self.publish_separators(&record.prefix, &record.split_key, right_token)
                .await?;
        } else if !applied {
            for token in &record.created_tokens {
                if !reachable.contains(token) {
                    self.shards.delete_node(&record.prefix, token).await?;
                }
            }
        }
        self.shards
            .delete_structural_log(&self.db_root, record_id)
            .await?;
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
            let parent_token = if paths::is_collection_info(&parent.path) {
                None
            } else {
                Some(
                    paths::node_token_of(&parent.path)
                        .map_err(|e| StorageError::with_source("parsing parent token", e))?,
                )
            };
            let lock_id = self.candidates.new_id();
            self.mon.begin_tx(&lock_id);
            let acquired = match self
                .acquire_structure_write(prefix, parent_token.as_deref(), &lock_id)
                .await
            {
                Ok(acquired) => acquired,
                Err(e) => {
                    self.finalize_split(&lock_id).await;
                    return Err(e);
                }
            };
            let Some((locked_parent, locked_version)) = acquired else {
                self.finalize_split(&lock_id).await;
                continue;
            };
            let Some(index) = locked_parent.as_index() else {
                self.finish_without_split(prefix, parent_token.as_deref(), &lock_id)
                    .await?;
                return Ok(());
            };
            if index.child_for(split_key) == Some(new_token) {
                self.finish_without_split(prefix, parent_token.as_deref(), &lock_id)
                    .await?;
                return Ok(()); // already published
            }
            let missing = match self
                .missing_separators(prefix, &locked_parent, split_key)
                .await
            {
                Ok(missing) => missing,
                Err(e) => {
                    let _ = self
                        .finish_without_split(prefix, parent_token.as_deref(), &lock_id)
                        .await;
                    return Err(e);
                }
            };
            if missing.is_empty() {
                self.finish_without_split(prefix, parent_token.as_deref(), &lock_id)
                    .await?;
                return Ok(());
            }
            let mut new_index = index.clone();
            for (sep, tok) in &missing {
                new_index.insert_child(sep.clone(), tok.clone());
            }
            let mut updated = locked_parent.clone();
            updated.set_index(new_index)?;
            let content_limit = self
                .candidates
                .policy()
                .node_max_bytes
                .saturating_sub(self.candidates.policy().split_headroom_bytes);
            if updated.content_encoded_len() > content_limit
                || updated.encoded_len() > self.candidates.policy().node_max_bytes
            {
                self.finish_without_split(prefix, parent_token.as_deref(), &lock_id)
                    .await?;
                if locked_parent.over_soft_cap(self.candidates.policy()) {
                    Box::pin(self.split_path(&parent.path)).await?;
                    continue;
                }
                return Err(TransError::InvalidInput(
                    "separator exceeds the coordination node size limit".into(),
                ));
            }
            let stored = match self
                .store_structural_node(prefix, parent_token.as_deref(), &updated, &locked_version)
                .await
            {
                Ok(stored) => stored,
                Err(e) => {
                    let _ = self
                        .finish_without_split(prefix, parent_token.as_deref(), &lock_id)
                        .await;
                    return Err(e);
                }
            };
            if stored {
                self.finish_without_split(prefix, parent_token.as_deref(), &lock_id)
                    .await?;
                // The inserts landed; a now-overflowing parent splits in turn.
                if updated.over_soft_cap(self.candidates.policy()) {
                    Box::pin(self.split_path(&parent.path)).await?;
                }
                return Ok(());
            }
            let _ = self
                .release_structure_write(prefix, parent_token.as_deref(), &lock_id)
                .await;
            self.finalize_split(&lock_id).await;
            // Precondition miss: the parent changed, re-find and retry.
        }
        // Exhausted the retries: re-queue so a later sweep re-drives the
        // publication. Descent keeps working through right-links meanwhile.
        self.push_pending_separator(PendingSeparator {
            prefix: prefix.to_string(),
            split_key: split_key.to_vec(),
            new_token: new_token.to_string(),
        });
        Err(TransError::Retry)
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
fn split_into_children(
    node: &Node,
    right_token: &str,
    structure_holder: &TxId,
) -> (Node, Node, Vec<u8>) {
    let mut source = node.clone();
    let (right, split_key) = source
        .split(right_token)
        .expect("root over the soft cap has at least two entries/children");
    source.remove_structure_holder(structure_holder);
    (source, right, split_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{BackendOp, HookBackend, HookFuture};
    use glassdb_data::TxId;
    use glassdb_storage::{
        CollectionRoot, LockType, ObjectCache, ShardEntry, SharedCache, TLogger, TxWrite,
        ValueCache,
    };

    const COLL: &str = "db/coll";

    // A soft cap so tight a two-entry leaf is at the cap and a third overflows it,
    // and any three-child index overflows — so splits are driven by a handful of
    // keys instead of hundreds.
    fn tiny() -> SplitPolicy {
        SplitPolicy {
            leaf_max_entries: 2,
            leaf_max_bytes: 1 << 20,
            index_max_children: 2,
            ..SplitPolicy::default()
        }
    }

    fn store() -> ShardStore {
        store_with_backend(Arc::new(MemoryBackend::new()))
    }

    fn store_with_backend(backend: Arc<dyn Backend>) -> ShardStore {
        ShardStore::new(ObjectCache::new(backend, &SharedCache::new(1 << 20)))
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
        Node::leaf(Shard::from_entries(keys.iter().map(|k| live(k))))
            .with_high_key(high.map(<[u8]>::to_vec))
            .with_right_sibling(right.map(str::to_string))
    }

    fn splitter(shards: &ShardStore, bg: &Arc<Background>, policy: SplitPolicy) -> Splitter {
        splitter_with_candidates(
            shards,
            bg,
            SplitCandidates::with_clock(policy, Clock::real()),
        )
    }

    fn splitter_with_candidates(
        shards: &ShardStore,
        bg: &Arc<Background>,
        candidates: SplitCandidates,
    ) -> Splitter {
        let tl = TLogger::new(shards.object_cache(), "db");
        let values = ValueCache::new(&SharedCache::new(1 << 20));
        let mon = Monitor::new(values, tl.clone(), Arc::downgrade(bg));
        splitter_with_monitor(shards, bg, mon, candidates)
    }

    fn splitter_with_monitor(
        shards: &ShardStore,
        bg: &Arc<Background>,
        mon: Monitor,
        candidates: SplitCandidates,
    ) -> Splitter {
        let resolver = Resolver::new(shards.clone(), mon.clone());
        let coord = ShardCoordinator::with_hinter(
            shards.clone(),
            resolver.clone(),
            mon.clone(),
            RetryConfig::default(),
            *candidates.policy(),
            Arc::new(candidates.clone()),
        );
        Splitter::with_candidates(
            Arc::downgrade(bg),
            shards.clone(),
            mon,
            resolver,
            "db",
            coord,
            candidates,
        )
    }

    fn splitter_at(
        shards: &ShardStore,
        bg: &Arc<Background>,
        policy: SplitPolicy,
        base_secs: u64,
    ) -> (Splitter, Monitor, u64) {
        let tl = TLogger::new(shards.object_cache(), "db");
        let values = ValueCache::new(&SharedCache::new(1 << 20));
        let mon = Monitor::new(values, tl.clone(), Arc::downgrade(bg));
        let clock = Clock::anchored_at(std::time::UNIX_EPOCH + Duration::from_secs(base_secs));
        let candidates = SplitCandidates::with_clock(policy, clock);
        let splitter = splitter_with_monitor(shards, bg, mon.clone(), candidates);
        (splitter, mon, base_secs * 1_000_000_000)
    }

    fn leaf_with_structure_reader(keys: &[&[u8]], holder: &TxId) -> Node {
        let mut node = leaf_node(keys, None, None);
        node.add_structure_reader(holder.clone());
        node
    }

    fn leaf_with_locked_entry(keys: &[&[u8]], holder: &TxId) -> Node {
        let mut entries: Vec<_> = keys.iter().map(|key| live(key)).collect();
        entries[0].lock_type = LockType::Write;
        entries[0].locked_by.push(holder.clone());
        let mut node = Node::leaf(Shard::from_entries(entries));
        node.add_structure_reader(holder.clone());
        node
    }

    fn nonroot_record(source: &str, right: &str, split_key: &[u8]) -> StructuralLog {
        StructuralLog {
            prefix: COLL.to_string(),
            source_token: source.to_string(),
            source_version: String::new(),
            created_tokens: vec![right.to_string()],
            split_key: split_key.to_vec(),
            is_root: false,
        }
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
        assert!(s.list_structural_logs("db").await.unwrap().is_empty());
        assert!(
            s.object_cache()
                .list(
                    &paths::transactions_prefix("db"),
                    None,
                    backend::ListLimit::new(1).unwrap(),
                )
                .await
                .unwrap()
                .objects
                .is_empty(),
            "a successful split does not create a transaction record"
        );
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
        assert!(s.list_structural_logs("db").await.unwrap().is_empty());
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

        let candidates = SplitCandidates::with_clock(tiny(), Clock::real());
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
        let sp = splitter_with_candidates(&s, &bg, candidates);
        sp.run_once().await;

        let leaves = Directory::new(s.clone())
            .leaves(COLL, Freshness::Latest)
            .await
            .unwrap();
        assert_eq!(leaves.len(), 2, "the fed candidate was split");
    }

    #[tokio::test]
    async fn contended_candidate_is_requeued() {
        let s = store();
        let holder = TxId::with_priority(0, b"holder");
        let mut node = Node::leaf(Shard::from_entries(
            [b"a".as_slice(), b"b", b"c", b"d"].iter().map(|k| live(k)),
        ));
        node.add_structure_reader(holder.clone());
        let mut root = CollectionRoot::new();
        root.set_node(node);
        s.create_root(COLL, &root).await.unwrap();
        let bg = Arc::new(Background::new());
        let candidates = SplitCandidates::with_clock(tiny(), Clock::real());
        candidates.observe_leaf(
            &paths::collection_info(COLL),
            &Shard::from_entries([live(b"a"), live(b"b"), live(b"c"), live(b"d")]),
        );
        let sp = splitter_with_candidates(&s, &bg, candidates);

        sp.run_once().await;
        assert!(
            s.load_root_node(COLL, Freshness::Latest)
                .await
                .unwrap()
                .unwrap()
                .0
                .as_leaf()
                .is_some(),
            "an older holder defers the split"
        );

        let (mut root, version) = s.load_root(COLL).await.unwrap();
        root.node_locks_mut().remove_structure_holder(&holder);
        assert!(s.store_root(COLL, &root, &version).await.unwrap());

        sp.run_once().await;
        assert!(
            s.load_root_node(COLL, Freshness::Latest)
                .await
                .unwrap()
                .unwrap()
                .0
                .as_index()
                .is_some(),
            "the retained candidate splits after the holder leaves"
        );
    }

    #[tokio::test]
    async fn split_wounds_a_younger_structure_reader_and_lands() {
        let s = store();
        let bg = Arc::new(Background::new());
        let (sp, mon, split_ts) = splitter_at(&s, &bg, tiny(), 1_000_000);
        let younger = TxId::with_priority(split_ts + 1_000_000_000, b"young");
        mon.begin_tx(&younger);
        s.store_node(
            COLL,
            "L",
            &leaf_with_locked_entry(&[b"a", b"b", b"c", b"d"], &younger),
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

        sp.split_path(&paths::from_node(COLL, "L")).await.unwrap();

        assert_eq!(
            mon.tx_status(&younger).await.unwrap(),
            TxCommitStatus::Aborted
        );
        let leaves = Directory::new(s.clone())
            .leaves(COLL, Freshness::Latest)
            .await
            .unwrap();
        assert_eq!(leaves.len(), 2);
        for leaf in leaves {
            let node = leaf.node;
            assert!(node.structure_lock().holders().is_empty());
            assert!(
                node.as_leaf()
                    .unwrap()
                    .entries()
                    .all(|entry| !entry.locked_by.contains(&younger))
            );
        }
    }

    #[tokio::test]
    async fn split_help_forwards_a_committed_structure_reader_before_moving_its_entry() {
        let s = store();
        let bg = Arc::new(Background::new());
        let (sp, mon, _) = splitter_at(&s, &bg, tiny(), 1_000_000);
        let holder = TxId::with_priority(1, b"committed");
        mon.begin_tx(&holder);
        let mut log = TxLog::new(holder.clone(), TxCommitStatus::Ok);
        log.writes.push(TxWrite {
            path: paths::from_key(COLL, b"d"),
            value: Arc::from(b"new-d".as_slice()),
            deleted: false,
            prev_writer: TxId::from_bytes(vec![1]),
        });
        mon.commit_tx(log).await.unwrap();

        let mut entries: Vec<_> = [b"a".as_slice(), b"b", b"c", b"d"]
            .iter()
            .map(|key| live(key))
            .collect();
        let upper = entries.last_mut().unwrap();
        upper.lock_type = LockType::Write;
        upper.locked_by.push(holder.clone());
        let mut node = Node::leaf(Shard::from_entries(entries));
        node.add_structure_reader(holder.clone());
        s.store_node(COLL, "L", &node, None).await.unwrap();
        let mut root = CollectionRoot::new();
        root.set_node(Node::index(IndexNode::from_children([(
            Vec::new(),
            "L".to_string(),
        )])));
        s.create_root(COLL, &root).await.unwrap();

        sp.split_path(&paths::from_node(COLL, "L")).await.unwrap();

        let leaf = Directory::new(s.clone())
            .leaf_for(COLL, b"d", Freshness::Latest)
            .await
            .unwrap();
        assert!(leaf.node.structure_lock().holders().is_empty());
        let entry = leaf
            .node
            .as_leaf()
            .unwrap()
            .entries()
            .find(|entry| entry.key == b"d")
            .unwrap();
        assert_eq!(entry.current_writer.as_ref(), Some(&holder));
        assert!(entry.locked_by.is_empty());
        assert_eq!(entry.lock_type, LockType::None);
    }

    #[tokio::test]
    async fn split_defers_to_an_older_structure_reader_then_lands() {
        let s = store();
        let bg = Arc::new(Background::new());
        let (sp, mon, split_ts) = splitter_at(&s, &bg, tiny(), 1_000_000);
        let older = TxId::with_priority(split_ts - 1_000_000_000, b"old");
        mon.begin_tx(&older);
        s.store_node(
            COLL,
            "L",
            &leaf_with_structure_reader(&[b"a", b"b", b"c", b"d"], &older),
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

        sp.candidates.observe_leaf(
            &paths::from_node(COLL, "L"),
            &Shard::from_entries([live(b"a"), live(b"b"), live(b"c"), live(b"d")]),
        );
        sp.run_once().await;
        assert_eq!(
            Directory::new(s.clone())
                .leaves(COLL, Freshness::Latest)
                .await
                .unwrap()
                .len(),
            1
        );

        mon.abort_tx(&older).await.unwrap();
        sp.run_once().await;
        assert_eq!(
            Directory::new(s.clone())
                .leaves(COLL, Freshness::Latest)
                .await
                .unwrap()
                .len(),
            2
        );
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
            ..SplitPolicy::default()
        };
        let candidates = SplitCandidates::with_clock(policy, Clock::real());
        candidates.observe_leaf(
            &paths::collection_info(COLL),
            &Shard::from_entries([live(b"a"), live(b"b"), live(b"c"), live(b"d")]),
        );

        let sp = splitter_with_candidates(&s, &bg, candidates);
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
            ..SplitPolicy::default()
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

    // ADR-032 retry path: a separator whose parent CAS keeps losing leaves its
    // structural record in progress and is re-queued for a later sweep. A backend that blocks writes to
    // the root `_i` forces the publication to give up; healing it lets the
    // re-driven publication land.
    #[tokio::test]
    async fn lost_parent_cas_is_republished_by_a_later_sweep() {
        let (backend, blocker) = RootWriteBlocker::wrap(Arc::new(MemoryBackend::new()));
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
        blocker.block(true);
        assert!(matches!(
            sp.split_path(&paths::from_node(COLL, "L")).await,
            Err(TransError::Retry)
        ));
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
        blocker.block(false);
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
        assert!(sp.recover_structural_logs().await);
        assert!(s.list_structural_logs("db").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn startup_structural_recovery_reclaims_an_orphan_after_restart() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let first = store_with_backend(backend.clone());
        first
            .store_node(COLL, "L", &leaf_node(&[b"a", b"b"], None, None), None)
            .await
            .unwrap();
        first
            .store_node(COLL, "R", &leaf_node(&[b"m", b"n"], None, None), None)
            .await
            .unwrap();
        let mut root = CollectionRoot::new();
        root.set_node(Node::index(IndexNode::from_children([(
            Vec::new(),
            "L".to_string(),
        )])));
        first.create_root(COLL, &root).await.unwrap();
        first
            .write_structural_log("R", &nonroot_record("L", "R", b"m"))
            .await
            .unwrap();
        drop(first);

        let second = store_with_backend(backend);
        let bg = Arc::new(Background::new());
        let splitter = splitter(&second, &bg, tiny());
        splitter.start();
        for _ in 0..20 {
            if matches!(
                second.load_node(COLL, "R", Freshness::Latest).await,
                Err(StorageError::NotFound)
            ) {
                break;
            }
            rt::yield_now().await;
        }

        assert!(matches!(
            second.load_node(COLL, "R", Freshness::Latest).await,
            Err(StorageError::NotFound)
        ));
        assert!(second.load_node(COLL, "L", Freshness::Latest).await.is_ok());
        assert!(second.list_structural_logs("db").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn structural_recovery_defers_while_the_source_writer_is_live() {
        let s = store();
        let bg = Arc::new(Background::new());
        let sp = splitter(&s, &bg, tiny());
        let id = TxId::with_priority(1, b"live-split");
        sp.mon.begin_tx(&id);

        let mut source = leaf_node(&[b"a", b"b"], None, None);
        source.set_structure_writer(id.clone());
        s.store_node(COLL, "L", &source, None).await.unwrap();
        s.store_node(COLL, "R", &leaf_node(&[b"m", b"n"], None, None), None)
            .await
            .unwrap();
        let mut root = CollectionRoot::new();
        root.set_node(Node::index(IndexNode::from_children([(
            Vec::new(),
            "L".to_string(),
        )])));
        s.create_root(COLL, &root).await.unwrap();
        let record = nonroot_record("L", "R", b"m");
        s.write_structural_log("R", &record).await.unwrap();

        assert!(matches!(
            sp.recover_record("R", &record).await,
            Err(TransError::Retry)
        ));
        assert!(s.load_node(COLL, "R", Freshness::Latest).await.is_ok());
        assert_eq!(s.list_structural_logs("db").await.unwrap().len(), 1);

        sp.mon.abort_tx(&id).await.unwrap();
        sp.recover_record("R", &record).await.unwrap();
        assert!(matches!(
            s.load_node(COLL, "R", Freshness::Latest).await,
            Err(StorageError::NotFound)
        ));
        assert!(s.list_structural_logs("db").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn recovery_rolls_forward_a_landed_nonroot_split() {
        let s = store();
        s.store_node(
            COLL,
            "L",
            &leaf_node(&[b"a", b"b"], Some(b"m"), Some("R")),
            None,
        )
        .await
        .unwrap();
        s.store_node(COLL, "R", &leaf_node(&[b"m", b"n"], None, None), None)
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

        let record = StructuralLog {
            prefix: COLL.to_string(),
            source_token: "L".to_string(),
            source_version: String::new(),
            created_tokens: vec!["R".to_string()],
            split_key: b"m".to_vec(),
            is_root: false,
        };
        s.write_structural_log("R", &record).await.unwrap();

        sp.recover_record("R", &record).await.unwrap();

        let (root_node, _) = s
            .load_root_node(COLL, Freshness::Latest)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(root_node.as_index().unwrap().child_for(b"m"), Some("R"));
        assert!(s.list_structural_logs("db").await.unwrap().is_empty());
    }

    /// Controls a hook that rejects conditional writes to the collection root.
    struct RootWriteBlocker {
        blocked: std::sync::atomic::AtomicBool,
    }

    impl RootWriteBlocker {
        fn wrap(inner: Arc<dyn Backend>) -> (Arc<HookBackend>, Arc<Self>) {
            let blocker = Arc::new(Self {
                blocked: std::sync::atomic::AtomicBool::new(false),
            });
            let backend = HookBackend::new(inner);
            backend.set_before({
                let blocker = blocker.clone();
                move |op| {
                    let blocked = matches!(
                        op,
                        BackendOp::WriteIf { path, .. }
                            | BackendOp::WriteIfNotExists { path, .. }
                            if blocker.blocked.load(std::sync::atomic::Ordering::SeqCst)
                                && path.ends_with("/_i")
                    );
                    let result = if blocked {
                        Err(glassdb_backend::BackendError::Precondition)
                    } else {
                        Ok(())
                    };
                    let future: HookFuture = Box::pin(async move { result });
                    future
                }
            });
            (backend, blocker)
        }

        fn block(&self, on: bool) {
            self.blocked.store(on, std::sync::atomic::Ordering::SeqCst);
        }
    }

    struct FirstSourceWriteGate {
        armed: std::sync::atomic::AtomicBool,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    impl FirstSourceWriteGate {
        fn wrap(inner: Arc<dyn Backend>, source_path: String) -> (Arc<HookBackend>, Arc<Self>) {
            let gate = Arc::new(Self {
                armed: std::sync::atomic::AtomicBool::new(false),
                entered: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
            });
            let backend = HookBackend::new(inner);
            backend.set_before({
                let gate = gate.clone();
                move |op| {
                    let wait = matches!(
                        op,
                        BackendOp::WriteIf { path, .. }
                            if path == &source_path
                                && gate
                                    .armed
                                    .swap(false, std::sync::atomic::Ordering::SeqCst)
                    );
                    let gate = gate.clone();
                    let future: HookFuture = Box::pin(async move {
                        if wait {
                            gate.entered.notify_one();
                            gate.release.notified().await;
                        }
                        Ok(())
                    });
                    future
                }
            });
            (backend, gate)
        }

        fn arm(&self) {
            self.armed.store(true, std::sync::atomic::Ordering::SeqCst);
        }

        async fn wait_until_entered(&self) {
            self.entered.notified().await;
        }

        fn release(&self) {
            self.release.notify_one();
        }
    }

    #[tokio::test]
    async fn recovery_fences_an_aborted_writer_before_reclaiming_its_sibling() {
        let source_path = paths::from_node(COLL, "L");
        let (backend, gate) =
            FirstSourceWriteGate::wrap(Arc::new(MemoryBackend::new()), source_path.clone());
        let s = ShardStore::new(ObjectCache::new(
            backend as Arc<dyn Backend>,
            &SharedCache::new(1 << 20),
        ));
        let bg = Arc::new(Background::new());
        let sp = splitter(&s, &bg, tiny());
        let id = TxId::with_priority(1, b"racing-split");

        let mut original = leaf_node(&[b"a", b"b", b"m", b"n"], None, None);
        original.set_structure_writer(id.clone());
        s.store_node(COLL, "L", &original, None).await.unwrap();
        let (mut shrunk, source_version) = s.load_node(COLL, "L", Freshness::Latest).await.unwrap();
        let (right, split_key) = shrunk.split("R").unwrap();
        shrunk.remove_structure_holder(&id);
        s.store_node(COLL, "R", &right, None).await.unwrap();
        let mut root = CollectionRoot::new();
        root.set_node(Node::index(IndexNode::from_children([(
            Vec::new(),
            "L".to_string(),
        )])));
        s.create_root(COLL, &root).await.unwrap();

        let record = StructuralLog {
            prefix: COLL.to_string(),
            source_token: "L".to_string(),
            source_version: source_version.token.to_string(),
            created_tokens: vec!["R".to_string()],
            split_key,
            is_root: false,
        };
        s.write_structural_log("R", &record).await.unwrap();
        sp.mon.begin_tx(&id);
        sp.mon.wound_tx(&id).await.unwrap();

        gate.arm();
        let recovering = {
            let sp = sp.clone();
            tokio::spawn(async move { sp.recover_record("R", &record).await })
        };
        gate.wait_until_entered().await;

        assert!(
            s.store_node(COLL, "L", &shrunk, Some(&source_version))
                .await
                .unwrap()
        );
        gate.release();
        recovering.await.unwrap().unwrap();

        assert!(s.load_node(COLL, "R", Freshness::Latest).await.is_ok());
        let (root_node, _) = s
            .load_root_node(COLL, Freshness::Latest)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(root_node.as_index().unwrap().child_for(b"m"), Some("R"));
    }
}
