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
//! a one-node structure-write lock. A structural transaction record is written
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
//! The collection root `_i` cannot move (its address is fixed), so when it
//! overflows it splits **in place**: two children are created and the root is
//! rewritten into a two-entry index over them, growing the tree's height while
//! preserving the collection metadata.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use glassdb_backend as backend;
use glassdb_concurr::{Background, Clock, rt};
use glassdb_data::{TxId, paths};
use glassdb_storage::{
    Directory, Freshness, IndexNode, LockScope, LockType, Node, PathLock, Shard, ShardStore,
    SplitPolicy, StorageError, StructuralSplit, StructuralSplitKind, StructuralSplitOutcome,
    TLogger, TxCommitStatus, TxLog,
};

use crate::error::TransError;
use crate::monitor::Monitor;
use crate::resolver::Resolver;
use crate::shard_coord::SplitHinter;

/// How often the splitter drains its candidate queue. A split is a handful of
/// CAS round-trips, so a tight cadence keeps overflowing leaves short-lived.
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
    clock: Clock,
    queue: Arc<Mutex<VecDeque<SplitCandidate>>>,
}

#[derive(Clone)]
struct SplitCandidate {
    path: String,
    priority: TxId,
}

impl SplitCandidates {
    /// Creates an empty candidate feed governed by `policy`.
    pub(crate) fn new(policy: SplitPolicy) -> Self {
        Self::with_clock(policy, Clock::real())
    }

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

    fn policy(&self) -> SplitPolicy {
        self.policy
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
    tl: TLogger,
    mon: Monitor,
    resolver: Resolver,
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
    pub fn new(
        bg: Weak<Background>,
        shards: ShardStore,
        tl: TLogger,
        mon: Monitor,
        resolver: Resolver,
    ) -> Self {
        Splitter::with_policy(bg, shards, tl, mon, resolver, SplitPolicy::default())
    }

    /// Creates a splitter with an explicit soft-cap `policy`, so a caller can
    /// drive splits with tiny nodes (e.g. the deterministic-simulation
    /// harness). The default policy is used by [`Splitter::new`].
    pub fn with_policy(
        bg: Weak<Background>,
        shards: ShardStore,
        tl: TLogger,
        mon: Monitor,
        resolver: Resolver,
        policy: SplitPolicy,
    ) -> Self {
        Splitter::with_candidates(bg, shards, tl, mon, resolver, SplitCandidates::new(policy))
    }

    /// Creates a splitter whose transaction priorities use the supplied clock.
    pub fn with_policy_and_clock(
        bg: Weak<Background>,
        shards: ShardStore,
        tl: TLogger,
        mon: Monitor,
        resolver: Resolver,
        policy: SplitPolicy,
        clock: Clock,
    ) -> Self {
        Splitter::with_candidates(
            bg,
            shards,
            tl,
            mon,
            resolver,
            SplitCandidates::with_clock(policy, clock),
        )
    }

    /// Creates a splitter over an explicit candidate feed. Lets a test drive the
    /// splitter with a tiny soft-cap policy.
    fn with_candidates(
        bg: Weak<Background>,
        shards: ShardStore,
        tl: TLogger,
        mon: Monitor,
        resolver: Resolver,
        candidates: SplitCandidates,
    ) -> Self {
        let dir = Directory::new(shards.clone());
        Splitter {
            bg,
            shards,
            dir,
            tl,
            mon,
            resolver,
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
            let node = root.node_mut_for_coordination();
            if node.structure_lock().lock_type() == LockType::Write
                && !node.structure_lock().contains(&id)
            {
                rt::sleep(Duration::from_millis(5)).await;
                continue;
            }
            node.add_structure_reader(id.clone());
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

    /// Acquires the source node's structure-write lock under wound-wait.
    async fn acquire_structure_write(
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

            let holders = node.structure_lock().holders().to_vec();
            let mut blocked = false;
            for holder in &holders {
                if holder == id {
                    continue;
                }
                if id.older(holder) && self.mon.tx_status(holder).await? == TxCommitStatus::Pending
                {
                    self.mon.wound_tx(holder).await?;
                }
                if self.mon.tx_status(holder).await? == TxCommitStatus::Pending {
                    blocked = true;
                }
            }
            if blocked {
                return Ok(None);
            }

            self.resolve_node_entries(prefix, &mut node, id).await?;
            for holder in holders {
                if &holder != id {
                    node.remove_structure_holder(&holder);
                }
            }
            let membership_holders = node.membership_lock().holders().to_vec();
            for holder in membership_holders {
                if &holder != id && self.mon.tx_status(&holder).await? != TxCommitStatus::Pending {
                    node.remove_membership_holder(&holder);
                }
            }
            node.set_structure_writer(id.clone());
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

    /// Reports whether structural recovery must defer because the source is
    /// still protected by a live structure writer.
    async fn source_write_is_live(
        &self,
        prefix: &str,
        token: Option<&str>,
    ) -> Result<bool, TransError> {
        let node = match token {
            Some(token) => match self
                .shards
                .load_node(prefix, token, Freshness::Latest)
                .await
            {
                Ok((node, _)) => node,
                Err(StorageError::NotFound) => return Ok(false),
                Err(e) => return Err(e.into()),
            },
            None => match self
                .shards
                .load_root_node(prefix, Freshness::Latest)
                .await?
            {
                Some((node, _)) => node,
                None => return Ok(false),
            },
        };
        if node.structure_lock().lock_type() != LockType::Write {
            return Ok(false);
        }
        for holder in node.structure_lock().holders() {
            if self.mon.tx_status(holder).await? == TxCommitStatus::Pending {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Halves a standalone node `_n` (leaf or interior): create the sibling,
    /// shrink the source (linearization point), then insert the separator into
    /// the parent. A CAS lost to a concurrent mutation simply defers the split.
    async fn split_nonroot(&self, prefix: &str, token: &str, id: TxId) -> Result<(), TransError> {
        let right_token = paths::random_node_token();
        self.mon.begin_tx(&id);
        let acquired = match self.acquire_structure_write(prefix, Some(token), &id).await {
            Ok(acquired) => acquired,
            Err(e) => {
                let _ = self.mon.abort_tx(&id).await;
                return Err(e);
            }
        };
        let Some((mut node, version)) = acquired else {
            self.mon.abort_tx(&id).await?;
            return Err(TransError::Retry);
        };
        if !node.over_soft_cap(self.candidates.policy()) {
            return self.finish_without_split(prefix, Some(token), &id).await;
        }
        let Some((right, split_key)) = node.split(&right_token) else {
            return self.finish_without_split(prefix, Some(token), &id).await;
        };
        let structural = StructuralSplit {
            source_path: paths::from_node(prefix, token),
            source_version: version.token.to_string(),
            created_tokens: vec![right_token.clone()],
            split_key: split_key.clone(),
            kind: StructuralSplitKind::NonRoot,
            outcome: StructuralSplitOutcome::InProgress,
            right_token: right_token.clone(),
        };
        let mut log = TxLog::new(id.clone(), TxCommitStatus::Pending);
        log.locks.push(PathLock {
            path: structural.source_path.clone(),
            typ: LockType::Write,
            scope: LockScope::Structure,
        });
        log.structural_splits.push(structural);
        self.mon
            .record_structural_splits(&id, log.structural_splits.clone());
        self.mon.record_tx_locks(&id, log.locks.clone());
        if let Err(e) = self.tl.set(&log).await {
            let _ = self.release_structure_write(prefix, Some(token), &id).await;
            self.mon
                .finish_structural_recovery(&id, TxCommitStatus::Unknown);
            return Err(e.into());
        }

        let created = match self
            .shards
            .store_node(prefix, &right_token, &right, None)
            .await
        {
            Ok(created) => created,
            Err(e) => {
                return self.recover_failed_split(&id, &log, e.into()).await;
            }
        };
        if !created {
            return self
                .recover_failed_split(&id, &log, TransError::Retry)
                .await;
        }
        let shrunk = match self
            .shards
            .store_node(prefix, token, &node, Some(&version))
            .await
        {
            Ok(shrunk) => shrunk,
            Err(e) => return self.recover_failed_split(&id, &log, e.into()).await,
        };
        if !shrunk {
            return self
                .recover_failed_split(&id, &log, TransError::Retry)
                .await;
        }
        if let Err(e) = self.release_structure_write(prefix, Some(token), &id).await {
            return self.recover_failed_split(&id, &log, e).await;
        }
        if let Err(e) = self
            .publish_separators(prefix, &split_key, &right_token)
            .await
        {
            return self.recover_failed_split(&id, &log, e).await;
        }
        match self
            .finish_structural_log(log.clone(), StructuralSplitOutcome::Applied)
            .await
        {
            Ok(()) => Ok(()),
            Err(e) => self.recover_failed_split(&id, &log, e).await,
        }
    }

    /// Grows the collection root in place: the root cannot move, so an
    /// overflowing `_i` splits into two freshly created children and is rewritten
    /// into a two-entry index over them (preserving collection metadata), raising
    /// the tree's height by one.
    async fn split_root(&self, prefix: &str, id: TxId) -> Result<(), TransError> {
        let l_token = paths::random_node_token();
        let r_token = paths::random_node_token();
        self.mon.begin_tx(&id);
        let acquired = match self.acquire_structure_write(prefix, None, &id).await {
            Ok(acquired) => acquired,
            Err(e) => {
                let _ = self.mon.abort_tx(&id).await;
                return Err(e);
            }
        };
        let Some((node, version)) = acquired else {
            self.mon.abort_tx(&id).await?;
            return Err(TransError::Retry);
        };
        if !node.over_soft_cap(self.candidates.policy()) {
            return self.finish_without_split(prefix, None, &id).await;
        }
        let (left, right, split_key) = split_into_children(&node, &r_token, &id);
        let structural = StructuralSplit {
            source_path: paths::collection_info(prefix),
            source_version: version.token.to_string(),
            created_tokens: vec![l_token.clone(), r_token.clone()],
            split_key: split_key.clone(),
            kind: StructuralSplitKind::Root,
            outcome: StructuralSplitOutcome::InProgress,
            right_token: r_token.clone(),
        };
        let mut log = TxLog::new(id.clone(), TxCommitStatus::Pending);
        log.locks.push(PathLock {
            path: structural.source_path.clone(),
            typ: LockType::Write,
            scope: LockScope::Structure,
        });
        log.structural_splits.push(structural);
        self.mon
            .record_structural_splits(&id, log.structural_splits.clone());
        self.mon.record_tx_locks(&id, log.locks.clone());
        if let Err(e) = self.tl.set(&log).await {
            let _ = self.release_structure_write(prefix, None, &id).await;
            self.mon
                .finish_structural_recovery(&id, TxCommitStatus::Unknown);
            return Err(e.into());
        }

        let left_created = match self.shards.store_node(prefix, &l_token, &left, None).await {
            Ok(created) => created,
            Err(e) => return self.recover_failed_split(&id, &log, e.into()).await,
        };
        let right_created = if left_created {
            match self.shards.store_node(prefix, &r_token, &right, None).await {
                Ok(created) => created,
                Err(e) => return self.recover_failed_split(&id, &log, e.into()).await,
            }
        } else {
            false
        };
        if !left_created || !right_created {
            return self
                .recover_failed_split(&id, &log, TransError::Retry)
                .await;
        }
        let root_index = IndexNode::from_children([(Vec::new(), l_token), (split_key, r_token)]);
        let mut index = Node::index(root_index);
        index.set_structure_writer(id.clone());
        let mut sized_root = match self.shards.load_root(prefix).await {
            Ok((root, _)) => root,
            Err(e) => return self.recover_failed_split(&id, &log, e.into()).await,
        };
        sized_root.set_node(index.clone());
        let content_limit = self
            .candidates
            .policy()
            .node_max_bytes
            .saturating_sub(self.candidates.policy().split_headroom_bytes);
        if sized_root.content_encoded_len() > content_limit
            || sized_root.encode().len() > self.candidates.policy().node_max_bytes
        {
            return self
                .recover_failed_split(
                    &id,
                    &log,
                    TransError::InvalidInput(
                        "root metadata exceeds the coordination node size limit".into(),
                    ),
                )
                .await;
        }
        let root_rewritten = match self
            .store_structural_node(prefix, None, &index, &version)
            .await
        {
            Ok(rewritten) => rewritten,
            Err(e) => return self.recover_failed_split(&id, &log, e).await,
        };
        if !root_rewritten {
            return self
                .recover_failed_split(&id, &log, TransError::Retry)
                .await;
        }
        if let Err(e) = self.release_structure_write(prefix, None, &id).await {
            return self.recover_failed_split(&id, &log, e).await;
        }
        match self
            .finish_structural_log(log.clone(), StructuralSplitOutcome::Applied)
            .await
        {
            Ok(()) => Ok(()),
            Err(e) => self.recover_failed_split(&id, &log, e).await,
        }
    }

    /// Resolves a structural attempt that can no longer continue in-line.
    async fn recover_failed_split(
        &self,
        id: &TxId,
        log: &TxLog,
        original: TransError,
    ) -> Result<(), TransError> {
        match self.recover_log(id, log, None).await {
            Ok(()) => Ok(()),
            Err(recovery) => {
                // The task has stopped, so a later GC must treat the durable
                // pending record as remote rather than indefinitely live-local.
                self.mon
                    .finish_structural_recovery(id, TxCommitStatus::Unknown);
                tracing::debug!(error = %recovery, "inline structural recovery deferred");
                Err(original)
            }
        }
    }

    /// Releases a structure writer when the candidate no longer needs a split.
    async fn finish_without_split(
        &self,
        prefix: &str,
        token: Option<&str>,
        id: &TxId,
    ) -> Result<(), TransError> {
        let release = self.release_structure_write(prefix, token, id).await;
        let abort = self.mon.abort_tx(id).await;
        release?;
        abort
    }

    /// Finalizes and removes a structural-only transaction log.
    async fn finish_structural_log(
        &self,
        mut log: TxLog,
        outcome: StructuralSplitOutcome,
    ) -> Result<(), TransError> {
        for split in &mut log.structural_splits {
            split.outcome = outcome;
        }
        log.locks.clear();
        log.status = TxCommitStatus::Ok;
        self.mon.commit_tx(log.clone()).await?;
        self.tl.delete(&log.id).await?;
        Ok(())
    }

    /// Recovers one structural write-ahead record from tree reachability.
    pub(crate) async fn recover_log(
        &self,
        id: &TxId,
        supplied: &TxLog,
        supplied_version: Option<&backend::Version>,
    ) -> Result<(), TransError> {
        let (mut log, mut version) = if let Some(version) = supplied_version {
            (supplied.clone(), version.clone())
        } else {
            self.tl.get(id).await?
        };
        let mut recovered_status = TxCommitStatus::Ok;

        for index in 0..log.structural_splits.len() {
            if log.structural_splits[index].outcome != StructuralSplitOutcome::InProgress {
                continue;
            }
            let split = log.structural_splits[index].clone();
            let parsed = paths::parse(&split.source_path)
                .map_err(|e| TransError::with_source("parsing structural source", e))?;
            let token = if parsed.typ == paths::Type::Node {
                Some(parsed.suffix.as_str())
            } else {
                None
            };
            // A GC-driven recovery may overlap the split that wrote this
            // record. The source lock distinguishes a live operation from a
            // crashed one; self-recovery passes no supplied version and already
            // knows that its split attempt has stopped.
            if supplied_version.is_some()
                && self.source_write_is_live(&parsed.prefix, token).await?
            {
                return Err(TransError::Retry);
            }
            let reachable = self
                .dir
                .reachable_tokens(&parsed.prefix, Freshness::Latest)
                .await?;
            let applied = !split.created_tokens.is_empty()
                && split
                    .created_tokens
                    .iter()
                    .all(|token| reachable.contains(token));

            self.release_structure_write(&parsed.prefix, token, id)
                .await?;
            if applied && split.kind == StructuralSplitKind::NonRoot {
                self.publish_separators(&parsed.prefix, &split.split_key, &split.right_token)
                    .await?;
            }
            if !applied {
                recovered_status = TxCommitStatus::Aborted;
                for token in &split.created_tokens {
                    self.shards.delete_node(&parsed.prefix, token).await?;
                }
            }
            log.structural_splits[index].outcome = if applied {
                StructuralSplitOutcome::Applied
            } else {
                StructuralSplitOutcome::RolledBack
            };
        }
        log.locks.clear();
        log.status = recovered_status;

        for _ in 0..PARENT_RETRIES {
            match self.tl.set_if(&log, &version).await {
                Ok(_) => {
                    self.mon.finish_structural_recovery(id, recovered_status);
                    self.tl.delete(id).await?;
                    return Ok(());
                }
                Err(StorageError::Precondition) => {
                    let (_, current_version) = self.tl.get(id).await?;
                    version = current_version;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(TransError::Retry)
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
                    let _ = self.mon.abort_tx(&lock_id).await;
                    return Err(e);
                }
            };
            let Some((locked_parent, locked_version)) = acquired else {
                self.mon.abort_tx(&lock_id).await?;
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
                || updated.encode().len() > self.candidates.policy().node_max_bytes
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
            self.mon.abort_tx(&lock_id).await?;
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
        CollectionRoot, LockType, ObjectCache, ShardEntry, SharedCache, ValueCache,
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
        Node::leaf(Shard::from_entries(keys.iter().map(|k| live(k))))
            .with_high_key(high.map(<[u8]>::to_vec))
            .with_right_sibling(right.map(str::to_string))
    }

    fn splitter(shards: &ShardStore, bg: &Arc<Background>, policy: SplitPolicy) -> Splitter {
        splitter_with_candidates(shards, bg, SplitCandidates::new(policy))
    }

    fn splitter_with_candidates(
        shards: &ShardStore,
        bg: &Arc<Background>,
        candidates: SplitCandidates,
    ) -> Splitter {
        let tl = TLogger::new(shards.object_cache(), "db");
        let values = ValueCache::new(&SharedCache::new(1 << 20));
        let mon = Monitor::new(values, tl.clone(), Arc::downgrade(bg));
        let resolver = Resolver::new(shards.clone(), mon.clone());
        Splitter::with_candidates(
            Arc::downgrade(bg),
            shards.clone(),
            tl,
            mon,
            resolver,
            candidates,
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
        let candidates = SplitCandidates::new(tiny());
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
        root.node_mut_for_coordination()
            .remove_structure_holder(&holder);
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
        let candidates = SplitCandidates::new(policy);
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
}
