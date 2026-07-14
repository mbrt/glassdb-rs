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
//! reachable or is reclaimed by structural-log recovery (ADR-032):
//!
//! 0. Write-ahead a structural-log record ([`ShardStore::write_structural_log`])
//!    before any node object is created, naming the source token+version, the
//!    created token(s), and the separator. A crash-orphaned sibling is thus
//!    always discoverable by recovery — even in a fresh process that never
//!    observed the split — which resolves it from tree-reachability, not status.
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
//! 4. Finalize: delete the structural-log record once the separator is
//!    published (the sibling is by then reachable via the right-link on its own).
//!
//! A **leaf** split acquires its source's structure-write lock through the
//! [`ShardCoordinator`](crate::ShardCoordinator), folding the acquire into the
//! same CAS round as the leaf's concurrent structure-read acquirers so
//! wound-wait resolves in one fold (ADR-032) — the split is a coordinator
//! participant, not a competitor. An interior-index or root node (which the
//! coordinator cannot load as a leaf) acquires through a direct structure-write
//! CAS; only concurrent splits contend there. The shrink itself stays the one
//! direct linearization CAS, releasing the structure-write inline.
//!
//! The collection root `_i` cannot move (its address is fixed), so when it
//! overflows it splits **in place**: two children are created and the root is
//! rewritten into a two-entry index over them, growing the tree's height while
//! preserving the collection metadata.

use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, UNIX_EPOCH};

use async_trait::async_trait;
use glassdb_concurr::{Background, Clock, RetryConfig, rt};
use glassdb_data::{TxId, paths};
use glassdb_storage::{
    Directory, Freshness, IndexNode, LockType, Node, Shard, ShardStore, SplitPolicy, StorageError,
    StructuralLog, TxCommitStatus, TxLog,
};

use crate::error::TransError;
use crate::monitor::Monitor;
use crate::resolver::Resolver;
use crate::shard_coord::{
    FoldOutcome, ResolveCtx, ShardCoordinator, ShardResolver, SplitHinter, Step,
};
use crate::tlocker::reconcile_node_lock;

/// How often the splitter drains its candidate queue and re-drives any
/// structural-log recovery. A split is a handful of CAS round-trips, so a tight
/// cadence keeps overflowing leaves short-lived and resolves a crash-orphaned
/// record promptly (ADR-032).
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

/// Bounded attempts to acquire a node's **structure-write** lock in one split
/// pass before deferring the split to a later sweep (ADR-032). Each attempt
/// reconciles the node's current structure holders by wound-wait: younger
/// uncommitted mutations are wounded, committed ones help-forwarded, and an
/// older holder makes the split back off. If an older holder keeps the node
/// through the whole budget, the split re-queues at its **preserved priority**
/// so wound-wait's restart rule (ADR-002) eventually makes it the oldest
/// contender and it lands.
const STRUCTURE_W_ATTEMPTS: usize = 8;

/// A split-candidate leaf path, plus the split's wound-wait priority preserved
/// across background re-queues (ADR-032). A fresh coordinator hint carries no
/// priority; the splitter mints one at normal (current-clock) priority on the
/// first attempt and, if the split must defer to an older structure holder,
/// re-queues the candidate carrying that priority so the retry keeps its place
/// in the wound-wait order and cannot be starved.
#[derive(Clone)]
struct Candidate {
    path: String,
    priority: Option<u64>,
}

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

/// The outcome of a structure-write-coordinated split's core steps (acquire,
/// create sibling, shrink), before the separately-locked parent follow-on
/// (ADR-032).
enum SplitStep {
    /// The shrink CAS landed: publish this separator into the parent, then
    /// finalize the structural-log record `record_id` (ADR-032).
    Published {
        split_key: Vec<u8>,
        right_token: String,
        record_id: String,
    },
    /// An older structure holder kept the node through the acquire budget:
    /// re-queue the candidate at its preserved priority for a later sweep.
    Deferred,
    /// Nothing to do this pass (already settled, or a CAS lost to a concurrent
    /// mutation): retry on a later sweep if still over the cap.
    NoOp,
}

/// A split's **structure-write** acquire, folded by the [`ShardCoordinator`]
/// into the same round as the leaf's concurrent structure-read acquirers
/// (ADR-032). Staging the write lock in-round is what makes the split a
/// wound-wait *participant* rather than a competitor: folded oldest-first, an
/// older split stages its write and a younger structure-reader observes it and
/// waits, while a younger split observing an older reader it cannot wound waits
/// in turn. The reconciliation is the same [`reconcile_node_lock`] every
/// node-lock holder uses.
struct StructureWResolver {
    split: TxId,
}

#[async_trait]
impl ShardResolver for StructureWResolver {
    async fn resolve(
        &self,
        ctx: &ResolveCtx<'_>,
        _staged: &std::collections::BTreeMap<Vec<u8>, glassdb_storage::ShardEntry>,
        staged_locks: &glassdb_storage::NodeLocks,
    ) -> Result<Step, TransError> {
        let mut locks = staged_locks.clone();
        if !locks.structure.holds(&self.split) {
            let holders: Vec<TxId> = locks.structure.holders.clone();
            if let Some(waited) =
                reconcile_node_lock(ctx.tmon, &self.split, &mut locks.structure, &holders).await?
            {
                // An older structure holder keeps the node: the split cannot
                // wound it, so it waits (the caller defers at preserved priority).
                return Ok(Step::Skip {
                    outcome: FoldOutcome::Wait(waited),
                });
            }
            locks.structure.acquire_write(&self.split);
        }
        Ok(Step::Stage {
            entries: Vec::new(),
            locks,
            outcome: FoldOutcome::Locked(LockType::Write),
        })
    }

    fn reorderable(&self) -> bool {
        // An exclusive structure-write acquire keeps FIFO order like a key
        // write acquire — it must not jump ahead of an unrelated writer.
        false
    }

    fn exhausted_outcome(&self) -> FoldOutcome {
        FoldOutcome::Conflict
    }
}

/// The feed of leaves that may need splitting (ADR-031), owned by the
/// [`Splitter`]. A handle is handed to the coordinator behind the
/// [`SplitHinter`] seam: it pushes a leaf's path right after storing it over
/// the soft cap, and the splitter drains and re-checks. Cloneable (all fields
/// `Arc`), so the producer handle and the splitter share one queue and policy.
#[derive(Clone)]
pub(crate) struct SplitCandidates {
    policy: SplitPolicy,
    queue: Arc<Mutex<VecDeque<Candidate>>>,
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

    /// Drains every queued candidate, de-duplicated by path (keeping the first,
    /// which carries any preserved priority), for one sweep cycle.
    fn drain(&self) -> Vec<Candidate> {
        let mut q = self.queue.lock().unwrap();
        let mut seen = BTreeSet::new();
        let mut out = Vec::new();
        while let Some(c) = q.pop_front() {
            if seen.insert(c.path.clone()) {
                out.push(c);
            }
        }
        out
    }

    /// Re-queues a candidate a split had to defer (an older structure holder
    /// kept the node) so a later sweep retries it at its preserved priority
    /// (ADR-032). The oldest hint is dropped when the queue is full; the split
    /// is still safe because descent works through right-links meanwhile.
    fn requeue(&self, cand: Candidate) {
        let mut q = self.queue.lock().unwrap();
        if q.len() >= CANDIDATE_QUEUE_CAP {
            q.pop_front();
        }
        q.push_back(cand);
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
        q.push_back(Candidate {
            path: path.to_string(),
            priority: None,
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
    // The transaction monitor: a split is a wound-wait participant, so it uses
    // the same machinery every mutation does to wound/wait on a node's
    // structure-lock holders and to publish its own liveness (ADR-032).
    tmon: Monitor,
    // Source of the split's normal-priority timestamp when it mints a split
    // transaction id (ADR-032). Shared with the monitor's clock so simulated
    // time is consistent.
    clock: Clock,
    // The leaf-candidate feed this splitter owns and drains. The coordinator
    // fills it through the [`SplitHinter`] seam (see [`Splitter::hinter`]); a
    // clone here and behind the seam share one queue.
    candidates: SplitCandidates,
    // Separators a split could not publish on the first try; re-driven each
    // sweep so the parent index eventually learns them (ADR-031). Purely
    // splitter-internal — the coordinator never sees it.
    pending: Arc<Mutex<VecDeque<PendingSeparator>>>,
    // The database name (the leading segment of every collection prefix), so
    // recovery can list the global structural-log directory `{db}/_s/` to
    // resolve crash-interrupted splits across all collections (ADR-032).
    db_root: String,
    // The shard coordinator a leaf split acquires its structure-write through,
    // so it folds into the same CAS round as concurrent structure-read
    // acquirers rather than racing them (ADR-032). Both share one candidate
    // feed, wired at construction ([`with_coordinator`](Self::with_coordinator)),
    // so neither is late-bound. For an interior-index/root node the coordinator
    // cannot load as a leaf, the split falls back to a direct structure-write
    // CAS instead.
    coord: ShardCoordinator,
}

impl Splitter {
    /// Builds a splitter together with the [`ShardCoordinator`] it drives leaf
    /// splits through (ADR-032), the two sharing one candidate feed (ADR-031):
    /// the coordinator reports every over-cap leaf write straight into the
    /// splitter's queue, and the splitter acquires a leaf split's
    /// structure-write through the coordinator so it folds into the same CAS
    /// round as concurrent mutations. Both dependencies are wired here at
    /// construction — the feed is owned by neither and injected into both — so
    /// neither is late-bound and there is no circular initialization.
    pub fn with_coordinator(
        bg: Weak<Background>,
        shards: ShardStore,
        tmon: Monitor,
        clock: Clock,
        retry: RetryConfig,
        db_root: &str,
        policy: SplitPolicy,
    ) -> (ShardCoordinator, Self) {
        let candidates = SplitCandidates::new(policy);
        let coord = ShardCoordinator::with_hinter(
            shards.clone(),
            Resolver::new(shards.clone(), tmon.clone()),
            tmon.clone(),
            retry,
            Arc::new(candidates.clone()),
        );
        let splitter =
            Splitter::with_candidates(bg, shards, tmon, clock, db_root, coord.clone(), candidates);
        (coord, splitter)
    }

    /// Creates a splitter over an explicit coordinator and candidate feed. Lets
    /// a test drive the splitter with a tiny soft-cap policy and a coordinator
    /// it also controls.
    fn with_candidates(
        bg: Weak<Background>,
        shards: ShardStore,
        tmon: Monitor,
        clock: Clock,
        db_root: &str,
        coord: ShardCoordinator,
        candidates: SplitCandidates,
    ) -> Self {
        let dir = Directory::new(shards.clone());
        Splitter {
            bg,
            shards,
            dir,
            tmon,
            clock,
            candidates,
            pending: Arc::new(Mutex::new(VecDeque::new())),
            db_root: db_root.to_string(),
            coord,
        }
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
        for cand in self.candidates.drain() {
            if let Err(e) = self.split_path(&cand.path, cand.priority).await {
                tracing::debug!(path = %cand.path, error = %e, "split candidate deferred");
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
        // Resolve any structural-log records a crashed or contended split left
        // behind: roll a landed split forward, reclaim a proven orphan, defer an
        // ambiguous one (ADR-032). Runs after the split pass so a completed split
        // this cycle has already finalized its own record.
        self.recover_structural_logs().await;
    }

    /// Resolves every in-progress split's structural-log record in the database
    /// (ADR-032 recovery). Best-effort: a transient error on one record only
    /// defers its resolution to a later sweep, so it is logged and the pass
    /// continues.
    async fn recover_structural_logs(&self) {
        let records = match self.shards.list_structural_logs(&self.db_root).await {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!(error = %e, "listing structural-log records failed");
                return;
            }
        };
        for (id, rec) in records {
            if let Err(e) = self.recover_record(&id, &rec).await {
                tracing::debug!(record = %id, error = %e, "structural-log recovery deferred");
            }
        }
    }

    /// Resolves one structural-log record from **structural** state (ADR-032).
    /// Reachability of the created node(s) from the root — following the full
    /// right-link chain — is the authority, not transaction status:
    ///
    /// - **All created nodes reachable** ⟹ the shrink CAS (or root rewrite)
    ///   landed ⟹ roll forward: idempotently publish the parent separator, then
    ///   finalize and delete the record.
    /// - **Some created node unreachable, source structure-write still held by a
    ///   live holder** ⟹ a split may be in progress (this or another process) ⟹
    ///   defer — never delete on ambiguity.
    /// - **Some created node unreachable, source structure-write not live** ⟹ the
    ///   shrink provably never landed ⟹ the created node is an orphan ⟹ delete
    ///   it and finalize the record aborted. The source is left as-is; the size
    ///   trigger re-splits it later.
    async fn recover_record(&self, id: &str, rec: &StructuralLog) -> Result<(), TransError> {
        let reachable = self
            .dir
            .reachable_tokens(&rec.prefix, Freshness::Latest)
            .await?;
        if rec.created_tokens.iter().all(|t| reachable.contains(t)) {
            if !rec.is_root
                && let Some(right) = rec.created_tokens.first()
            {
                self.publish_separators(&rec.prefix, &rec.split_key, right)
                    .await?;
            }
            self.shards.delete_structural_log(&self.db_root, id).await?;
            return Ok(());
        }
        if self.source_structure_w_live(rec).await? {
            // Ambiguous: a live structure-write holder means a split is still
            // in flight. Defer to a later sweep rather than guess (ADR-032).
            return Ok(());
        }
        for token in &rec.created_tokens {
            if !reachable.contains(token) {
                self.shards.delete_node(&rec.prefix, token).await?;
            }
        }
        self.shards.delete_structural_log(&self.db_root, id).await?;
        Ok(())
    }

    /// Reports whether the record's source node still carries a **live**
    /// structure-write holder — a split whose transaction the monitor still
    /// resolves as pending (ADR-032). A crashed split's holder resolves dead
    /// once its lease lapses (ADR-021), at which point the source is no longer
    /// "live" and its orphan may be reclaimed. A missing source object is not
    /// live.
    async fn source_structure_w_live(&self, rec: &StructuralLog) -> Result<bool, TransError> {
        let structure = if rec.is_root {
            match self.shards.load_root(&rec.prefix).await {
                Ok((root, _)) => root.node().locks().structure.clone(),
                Err(StorageError::NotFound) => return Ok(false),
                Err(e) => return Err(e.into()),
            }
        } else {
            match self
                .shards
                .load_node(&rec.prefix, &rec.source_token, Freshness::Latest)
                .await
            {
                Ok((node, _)) => node.locks().structure.clone(),
                Err(StorageError::NotFound) => return Ok(false),
                Err(e) => return Err(e.into()),
            }
        };
        for holder in &structure.holders {
            if self.tmon.tx_status(holder).await? == TxCommitStatus::Pending {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Splits the leaf at object `path` if it is still over the soft cap: an
    /// in-place root split when `path` is the collection root `_i`, else a
    /// standalone node half-split. `priority`, when set, is the wound-wait
    /// priority a deferred split preserved across the re-queue (ADR-032); a
    /// fresh candidate passes `None` so the split mints a normal-priority id.
    async fn split_path(&self, path: &str, priority: Option<u64>) -> Result<(), TransError> {
        let pr = paths::parse(path)
            .map_err(|e| StorageError::with_source("parsing candidate path", e))?;
        if paths::is_collection_info(path) {
            self.split_root(&pr.prefix, priority).await
        } else {
            self.split_nonroot(&pr.prefix, &pr.suffix, priority).await
        }
    }

    /// Mints a split transaction id at wound-wait priority. A deferred split
    /// preserves its `priority` across re-queues so it keeps its place in the
    /// wound-wait order (ADR-002/ADR-032); a fresh split takes the current-clock
    /// time as its normal priority. Every id gets a fresh random prefix so its
    /// (rare) durable log lands in a distinct storage partition. Returns the id
    /// and its priority timestamp, so a defer can re-queue at the same priority.
    fn mint_split_tx(&self, priority: Option<u64>) -> (TxId, u64) {
        let nanos = priority.unwrap_or_else(|| {
            self.clock
                .now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0)
        });
        (TxId::with_priority(nanos, &[]).renew(), nanos)
    }

    /// Finalizes a split transaction locally: it wrote no values and, once its
    /// structure-write lock is released by the shrink CAS, holds no key locks,
    /// so this only clears its local tracking and wakes any mutation that was
    /// waiting on it (ADR-032). It writes no transaction log — a split's outcome
    /// is read from **structural** state, never its transaction status (ADR-032
    /// recovery), so any structure-write holder it may have left is reclaimed by
    /// the next contender's wound-wait reconciliation regardless of this status.
    async fn finalize_split(&self, split: &TxId) {
        let _ = self
            .tmon
            .commit_tx(TxLog::new(split.clone(), TxCommitStatus::Ok))
            .await;
    }

    /// Acquires the standalone node `_n/token`'s **structure-write** lock for the
    /// split `split`, reconciling the node's current structure holders by
    /// wound-wait (ADR-032): younger uncommitted mutations are wounded,
    /// committed ones dropped (their write-back releases the lock; dropping the
    /// holder here completes that on this node), and an older holder makes the
    /// split back off and retry. Returns the freshly loaded node (with the split
    /// holding structure-write) and its version for the shrink CAS, or `None`
    /// when the node stays contended by an older holder through the whole budget
    /// (the caller re-queues at preserved priority).
    ///
    /// A **leaf** node acquires through the [`ShardCoordinator`], so the write
    /// folds into the same CAS round as concurrent structure-read acquirers it
    /// contends with (ADR-032). An interior-index or root node (which the
    /// coordinator cannot load as a leaf) falls back to the direct
    /// structure-write CAS loop below.
    async fn acquire_node_structure_w(
        &self,
        prefix: &str,
        token: &str,
        split: &TxId,
    ) -> Result<Option<(Node, glassdb_backend::Version)>, TransError> {
        let (node, _) = self
            .shards
            .load_node(prefix, token, Freshness::Latest)
            .await?;
        if node.as_leaf().is_some() {
            return self
                .acquire_leaf_structure_w_via_coord(&self.coord, prefix, token, split)
                .await;
        }
        let mut backoff = Duration::from_millis(1);
        for _ in 0..STRUCTURE_W_ATTEMPTS {
            let (mut node, version) = self
                .shards
                .load_node(prefix, token, Freshness::Latest)
                .await?;
            if !node.locks().structure.holds(split) {
                let mut locks = node.locks().clone();
                let holders: Vec<TxId> = locks.structure.holders.clone();
                if reconcile_node_lock(&self.tmon, split, &mut locks.structure, &holders)
                    .await?
                    .is_some()
                {
                    // An older holder keeps the node: wait, then retry.
                    rt::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_millis(64));
                    continue;
                }
                locks.structure.acquire_write(split);
                node.set_locks(locks);
                if !self
                    .shards
                    .store_node(prefix, token, &node, Some(&version))
                    .await?
                {
                    // Lost the acquire CAS (the node changed): reload and retry.
                    continue;
                }
            }
            // Reload after the acquire CAS to get the fresh version for the
            // shrink and to confirm we still hold structure-write: an older
            // mutation may have wounded the split and taken the node.
            let (node2, version2) = self
                .shards
                .load_node(prefix, token, Freshness::Latest)
                .await?;
            if node2.locks().structure.holds(split) {
                return Ok(Some((node2, version2)));
            }
        }
        Ok(None)
    }

    /// Acquires a **leaf** node's structure-write through the [`ShardCoordinator`]
    /// (ADR-032): the split submits a [`StructureWResolver`] that stages the
    /// write lock, so the coordinator folds it into the same round as the leaf's
    /// concurrent structure-read acquirers and resolves wound-wait in one fold —
    /// the split participates instead of racing a separate direct CAS. On a
    /// staged write it reloads for the fresh version the shrink CAS needs and
    /// confirms the split still holds the lock (an older mutation may have
    /// wounded it the instant after). Any other outcome — an older holder the
    /// split must wait for, exhausted contention, or a shut-down coordinator —
    /// returns `None`, and the caller defers at preserved priority.
    async fn acquire_leaf_structure_w_via_coord(
        &self,
        coord: &ShardCoordinator,
        prefix: &str,
        token: &str,
        split: &TxId,
    ) -> Result<Option<(Node, glassdb_backend::Version)>, TransError> {
        let path = paths::from_node(prefix, token);
        let resolver = Arc::new(StructureWResolver {
            split: split.clone(),
        });
        match coord
            .submit_shard(&path, split, resolver, Freshness::Latest)
            .await?
        {
            Some(FoldOutcome::Locked(_)) => {
                let (node, version) = self
                    .shards
                    .load_node(prefix, token, Freshness::Latest)
                    .await?;
                if node.locks().structure.holds(split) {
                    Ok(Some((node, version)))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    /// Halves a standalone node `_n` (leaf or interior) as a wound-wait split
    /// (ADR-032): acquire the node's structure-write lock (excluding concurrent
    /// splits and escalated scans, and letting the split wound or wait on the
    /// mutations racing it), create the sibling, then shrink the source in one
    /// CAS that is both the **linearization point** and the structure-write
    /// **release**. The parent separator insert is a separately-locked follow-on
    /// (ADR-032 step 4). A node kept by an older holder, or a lost shrink CAS,
    /// defers the split; a deferred split re-queues at its preserved priority.
    async fn split_nonroot(
        &self,
        prefix: &str,
        token: &str,
        priority: Option<u64>,
    ) -> Result<(), TransError> {
        let (node, _) = self
            .shards
            .load_node(prefix, token, Freshness::Latest)
            .await?;
        if !node.over_soft_cap(self.candidates.policy()) {
            return Ok(());
        }
        let (split, prio) = self.mint_split_tx(priority);
        self.tmon.begin_tx(&split);
        let outcome = self.coordinate_nonroot_split(prefix, token, &split).await;
        // The split's structure-write is released by the shrink CAS (or left for
        // wound-wait reclaim on a failed attempt); finalize it either way so any
        // mutation waiting on it wakes and its liveness is not tracked forever.
        self.finalize_split(&split).await;
        match outcome? {
            SplitStep::Published {
                split_key,
                right_token,
                record_id,
            } => {
                // Step 4: publish the separator into the parent as a
                // separately-locked follow-on; recurse if the parent overflows.
                self.publish_separators(prefix, &split_key, &right_token)
                    .await?;
                // Step 5: finalize — the shrink landed (the sibling is now
                // reachable via the right-link, so it is safe from GC on its own)
                // and the separator is published, so the record has done its job
                // (ADR-032). A crash before this delete leaves recovery to
                // idempotently re-drive step 4 and reclaim the record.
                self.shards
                    .delete_structural_log(&self.db_root, &record_id)
                    .await?;
                Ok(())
            }
            SplitStep::Deferred => {
                // An older structure holder kept the node through the budget:
                // re-queue at the preserved priority so a later sweep retries
                // it as (eventually) the oldest contender (ADR-032).
                self.candidates.requeue(Candidate {
                    path: paths::from_node(prefix, token),
                    priority: Some(prio),
                });
                Ok(())
            }
            SplitStep::NoOp => Ok(()),
        }
    }

    /// The structure-write-coordinated core of a non-root split: acquire, create
    /// sibling, shrink-and-release. Returns the produced separator to publish,
    /// or a defer/no-op signal. Split out from [`split_nonroot`](Self::split_nonroot)
    /// so its caller can finalize the split transaction on every exit path.
    async fn coordinate_nonroot_split(
        &self,
        prefix: &str,
        token: &str,
        split: &TxId,
    ) -> Result<SplitStep, TransError> {
        // 1. Acquire structure-write (wound-wait). `None` ⇒ an older holder kept
        //    the node: defer and re-queue at the preserved priority.
        let Some((mut node, version)) = self.acquire_node_structure_w(prefix, token, split).await?
        else {
            return Ok(SplitStep::Deferred);
        };
        let right_token = paths::random_node_token();
        let Some((right, split_key)) = node.split(&right_token) else {
            // Too small to divide now (a concurrent shrink got there first):
            // release our structure-write so the node is not left locked.
            let mut locks = node.locks().clone();
            locks.release(split);
            node.set_locks(locks);
            let _ = self
                .shards
                .store_node(prefix, token, &node, Some(&version))
                .await;
            return Ok(SplitStep::NoOp);
        };
        // Release the source's structure-write as part of the shrink CAS: the
        // shrink is the linearization point and the split holds at most one
        // node's structure-write at a time (ADR-032 steps 2-3).
        let mut locks = node.locks().clone();
        locks.release(split);
        node.set_locks(locks);
        // Write-ahead the structural-log record *before* creating any node
        // object (ADR-032 step 1), so a crash-orphaned sibling is always
        // discoverable by recovery, which resolves it by tree-reachability. The
        // record id is the sibling token — fresh and collision-free.
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
        // 2. Create the right sibling. A fresh random token never collides, so a
        //    `false` here means someone else created it; defer.
        if !self
            .shards
            .store_node(prefix, &right_token, &right, None)
            .await?
        {
            return Ok(SplitStep::NoOp);
        }
        // 3. Shrink-and-release the source in one CAS (the linearization point).
        //    Lost to a concurrent CAS (e.g. an older mutation wounded the split):
        //    leave the orphan sibling and record for recovery to reclaim.
        if !self
            .shards
            .store_node(prefix, token, &node, Some(&version))
            .await?
        {
            return Ok(SplitStep::NoOp);
        }
        Ok(SplitStep::Published {
            split_key,
            right_token,
            record_id,
        })
    }

    /// Grows the collection root in place: the root cannot move, so an
    /// overflowing `_i` splits into two freshly created children and is rewritten
    /// into a two-entry index over them (preserving collection metadata), raising
    /// the tree's height by one.
    async fn split_root(&self, prefix: &str, priority: Option<u64>) -> Result<(), TransError> {
        let (root, _) = match self.shards.load_root(prefix).await {
            Ok(rv) => rv,
            Err(StorageError::NotFound) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        if !root.node().over_soft_cap(self.candidates.policy()) {
            return Ok(());
        }
        let (split, prio) = self.mint_split_tx(priority);
        self.tmon.begin_tx(&split);
        let outcome = self.coordinate_root_split(prefix, &split).await;
        // The root rewrite releases the split's structure-write (or a failed
        // attempt leaves it for wound-wait reclaim); finalize either way.
        self.finalize_split(&split).await;
        match outcome? {
            SplitStep::Deferred => {
                self.candidates.requeue(Candidate {
                    path: paths::collection_info(prefix),
                    priority: Some(prio),
                });
                Ok(())
            }
            // The root split is complete once the root is rewritten in place;
            // it has no parent separator to publish, so finalize the record.
            SplitStep::Published { record_id, .. } => {
                self.shards
                    .delete_structural_log(&self.db_root, &record_id)
                    .await?;
                Ok(())
            }
            SplitStep::NoOp => Ok(()),
        }
    }

    /// The structure-write-coordinated core of an in-place root split: acquire
    /// the root's structure-write, create the two children, then rewrite `_i`
    /// into a two-entry index over them (the linearization point, which also
    /// releases the structure-write). Split out so [`split_root`](Self::split_root)
    /// can finalize the split transaction on every exit path.
    async fn coordinate_root_split(
        &self,
        prefix: &str,
        split: &TxId,
    ) -> Result<SplitStep, TransError> {
        let Some((root, version)) = self.acquire_root_structure_w(prefix, split).await? else {
            return Ok(SplitStep::Deferred);
        };
        let node = root.node().clone();
        if !node.over_soft_cap(self.candidates.policy()) {
            // Settled under our lock: drop the structure-write and stop.
            let mut node = node;
            let mut locks = node.locks().clone();
            locks.release(split);
            node.set_locks(locks);
            let mut settled = root.clone();
            settled.set_node(node);
            let _ = self.shards.store_root(prefix, &settled, &version).await;
            return Ok(SplitStep::NoOp);
        }
        let l_token = paths::random_node_token();
        let r_token = paths::random_node_token();
        let (left, right, split_key) = split_into_children(&node, &r_token);
        // Write-ahead the structural-log record before creating any child, so
        // orphaned children left by a crash are discoverable by recovery, which
        // resolves them from whether `_i` became an index over both (ADR-032).
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
        // Create both children before rewriting the root, so the root never
        // points at a missing child; on crash the unreferenced children are
        // reclaimed by recovery.
        if !self
            .shards
            .store_node(prefix, &l_token, &left, None)
            .await?
            || !self
                .shards
                .store_node(prefix, &r_token, &right, None)
                .await?
        {
            return Ok(SplitStep::NoOp);
        }
        // The rewritten root drops the split's structure-write: a fresh index
        // node carries no locks. The root rewrite is the linearization point;
        // a lost CAS orphans the two children (reclaimed by GC) and defers.
        let index = Node::index(IndexNode::from_children([
            (Vec::new(), l_token),
            (split_key, r_token),
        ]));
        let mut new_root = root.clone();
        new_root.set_node(index);
        if !self.shards.store_root(prefix, &new_root, &version).await? {
            return Ok(SplitStep::NoOp);
        }
        Ok(SplitStep::Published {
            split_key: Vec::new(),
            right_token: String::new(),
            record_id,
        })
    }

    /// Acquires the collection root's **structure-write** lock for the split,
    /// the root-object analogue of [`acquire_node_structure_w`](Self::acquire_node_structure_w):
    /// the root cannot move, so it is loaded and rewritten through the
    /// `CollectionRoot` wrapper (preserving collection metadata) rather than a
    /// standalone node object. Returns the loaded root (with the split holding
    /// structure-write on its node) and its version, or `None` when an older
    /// holder keeps it through the budget.
    async fn acquire_root_structure_w(
        &self,
        prefix: &str,
        split: &TxId,
    ) -> Result<Option<(glassdb_storage::CollectionRoot, glassdb_backend::Version)>, TransError>
    {
        let mut backoff = Duration::from_millis(1);
        for _ in 0..STRUCTURE_W_ATTEMPTS {
            let (root, version) = match self.shards.load_root(prefix).await {
                Ok(rv) => rv,
                Err(StorageError::NotFound) => return Ok(None),
                Err(e) => return Err(e.into()),
            };
            let mut node = root.node().clone();
            if !node.locks().structure.holds(split) {
                let mut locks = node.locks().clone();
                let holders: Vec<TxId> = locks.structure.holders.clone();
                if reconcile_node_lock(&self.tmon, split, &mut locks.structure, &holders)
                    .await?
                    .is_some()
                {
                    rt::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_millis(64));
                    continue;
                }
                locks.structure.acquire_write(split);
                node.set_locks(locks);
                let mut locked = root.clone();
                locked.set_node(node);
                if !self.shards.store_root(prefix, &locked, &version).await? {
                    continue;
                }
            }
            let (root2, version2) = match self.shards.load_root(prefix).await {
                Ok(rv) => rv,
                Err(StorageError::NotFound) => return Ok(None),
                Err(e) => return Err(e.into()),
            };
            if root2.node().locks().structure.holds(split) {
                return Ok(Some((root2, version2)));
            }
        }
        Ok(None)
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
            updated.set_bounds(
                parent.node.high_key().map(<[u8]>::to_vec),
                parent.node.right_sibling().map(str::to_string),
            );

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
                // The inserts landed; a now-overflowing parent splits in turn as
                // a fresh split (its own structure-write, ADR-032).
                if updated.over_soft_cap(self.candidates.policy()) {
                    Box::pin(self.split_path(&parent.path, None)).await?;
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
    use glassdb_backend::middleware::{BackendOp, HookBackend, HookFuture};
    use glassdb_concurr::RetryConfig;
    use glassdb_data::TxId;
    use glassdb_storage::{
        CollectionRoot, LockType, ObjectCache, ShardEntry, SharedCache, TLogger, ValueCache,
    };

    use crate::resolver::Resolver;

    // Builds a shard coordinator over `shards`/`mon`, so a leaf split acquires
    // its structure-write through the coordinator (ADR-032) exactly as it does
    // in a live database. Passed into the splitter at construction.
    fn test_coord(shards: &ShardStore, mon: &Monitor) -> ShardCoordinator {
        ShardCoordinator::new(
            shards.clone(),
            Resolver::new(shards.clone(), mon.clone()),
            mon.clone(),
            RetryConfig::default(),
        )
    }

    const COLL: &str = "db/coll";
    const DB: &str = "db";

    // A monitor for the splitter's wound-wait participation (ADR-032). The split
    // tests drive splits without concurrent mutations, so the node's structure
    // lock starts empty and reconciliation never queries a real holder's status;
    // the monitor only tracks the split's own local begin/finalize. It therefore
    // needs no shared backend and no live `Background` (splits finalize locally,
    // never starting the refresh loop).
    fn test_monitor() -> Monitor {
        let cache = SharedCache::new(1 << 20);
        let values = ValueCache::new(&cache);
        let objects = ObjectCache::new(Arc::new(MemoryBackend::new()) as Arc<dyn Backend>, &cache);
        let tl = TLogger::new(objects, "test");
        Monitor::new(values, tl, Weak::new())
    }

    // A soft cap so tight a two-entry leaf is at the cap and a third overflows it,
    // and any three-child index overflows — so splits are driven by a handful of
    // keys instead of hundreds.
    fn tiny() -> SplitPolicy {
        SplitPolicy {
            leaf_max_entries: 2,
            leaf_max_bytes: 1 << 20,
            index_max_children: 2,
            leaf_hard_cap_bytes: usize::MAX,
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
        n.set_bounds(high.map(<[u8]>::to_vec), right.map(str::to_string));
        n
    }

    fn splitter(shards: &ShardStore, bg: &Arc<Background>, policy: SplitPolicy) -> Splitter {
        let mon = test_monitor();
        Splitter::with_candidates(
            Arc::downgrade(bg),
            shards.clone(),
            mon.clone(),
            Clock::real(),
            DB,
            test_coord(shards, &mon),
            SplitCandidates::new(policy),
        )
    }

    // A splitter sharing an explicit monitor and clock, so a test can register
    // the transactions that hold a node's structure lock and control the
    // split's wound-wait priority against them (ADR-032).
    fn splitter_full(
        shards: &ShardStore,
        bg: &Arc<Background>,
        mon: &Monitor,
        clock: Clock,
        policy: SplitPolicy,
    ) -> Splitter {
        Splitter::with_candidates(
            Arc::downgrade(bg),
            shards.clone(),
            mon.clone(),
            clock,
            DB,
            test_coord(shards, mon),
            SplitCandidates::new(policy),
        )
    }

    // A monitor plus a clock anchored so the split's minted priority is a known
    // instant: transactions minted before it are older (higher priority) and
    // ones after it younger, letting a test pin the wound-wait outcome.
    fn monitor_at(base_secs: u64) -> (Monitor, Clock, u64) {
        let base = UNIX_EPOCH + Duration::from_secs(base_secs);
        let clock = Clock::anchored_at(base);
        let cache = SharedCache::new(1 << 20);
        let values = ValueCache::new(&cache);
        let objects = ObjectCache::new(Arc::new(MemoryBackend::new()) as Arc<dyn Backend>, &cache);
        let tl = TLogger::new(objects, "test");
        (
            Monitor::new(values, tl, Weak::new()),
            clock,
            base_secs * 1_000_000_000,
        )
    }

    // A leaf carrying a structure-read holder, as every data mutation leaves
    // while its write-back is outstanding (ADR-032).
    fn leaf_with_structure_r(keys: &[&[u8]], holder: &TxId) -> Node {
        let mut n = leaf_node(keys, None, None);
        let mut locks = n.locks().clone();
        locks.structure.acquire_read(holder);
        n.set_locks(locks);
        n
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
            .split_path(&paths::collection_info(COLL), None)
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
            .split_path(&paths::from_node(COLL, "L"), None)
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
            .split_path(&paths::collection_info(COLL), None)
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

        sp.split_path(&paths::collection_info(COLL), None)
            .await
            .unwrap();
        let after_first = Directory::new(s.clone())
            .leaves(COLL, Freshness::Latest)
            .await
            .unwrap();
        // Re-run: each resulting leaf holds two keys, which is at (not over) the
        // cap, so nothing changes.
        for leaf in &after_first {
            sp.split_path(&leaf.path, None).await.unwrap();
        }
        sp.split_path(&paths::collection_info(COLL), None)
            .await
            .unwrap();

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
        let sp = Splitter::with_candidates(
            Arc::downgrade(&bg),
            s.clone(),
            test_monitor(),
            Clock::real(),
            DB,
            test_coord(&s, &test_monitor()),
            candidates,
        );
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
            leaf_hard_cap_bytes: usize::MAX,
        };
        let candidates = SplitCandidates::new(policy);
        candidates.observe_leaf(
            &paths::collection_info(COLL),
            &Shard::from_entries([live(b"a"), live(b"b"), live(b"c"), live(b"d")]),
        );

        let sp = Splitter::with_candidates(
            Arc::downgrade(&bg),
            s.clone(),
            test_monitor(),
            Clock::real(),
            DB,
            test_coord(&s, &test_monitor()),
            candidates,
        );
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
            leaf_hard_cap_bytes: usize::MAX,
        };
        splitter(&s, &bg, policy)
            .split_path(&paths::from_node(COLL, "S"), None)
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
        sp.split_path(&paths::from_node(COLL, "L"), None)
            .await
            .unwrap();
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

    // ADR-032 anti-starvation: a split acquires the hot leaf's structure-write
    // by wounding a *younger* mutation still holding structure-read (its
    // write-back outstanding). The wounded transaction is aborted and the leaf
    // splits — the split is not starved by the mutation churn.
    #[tokio::test]
    async fn split_wounds_a_younger_structure_reader_and_lands() {
        let s = store();
        let (mon, clock, split_ts) = monitor_at(1_000_000);
        // A younger mutation (later timestamp) holds structure-read on L.
        let younger = TxId::with_priority(split_ts + 1_000_000_000, b"young");
        mon.begin_tx(&younger);
        s.store_node(
            COLL,
            "L",
            &leaf_with_structure_r(&[b"a", b"b", b"c", b"d"], &younger),
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

        splitter_full(&s, &bg, &mon, clock, tiny())
            .split_path(&paths::from_node(COLL, "L"), None)
            .await
            .unwrap();

        // The split wounded the younger structure-reader and landed.
        assert_eq!(
            mon.tx_status(&younger).await.unwrap(),
            TxCommitStatus::Aborted,
            "the younger structure-reader is wounded"
        );
        let leaves = Directory::new(s.clone())
            .leaves(COLL, Freshness::Latest)
            .await
            .unwrap();
        assert_eq!(leaves.len(), 2, "the hot leaf split despite the reader");
        // The shrunk source dropped the split's structure-write (released by the
        // shrink CAS), so it carries no node-level lock.
        let (source, _) = s.load_node(COLL, "L", Freshness::Latest).await.unwrap();
        assert!(
            source.locks().structure.is_empty(),
            "structure-write released by the shrink CAS"
        );
    }

    // ADR-032 wound-wait: a split cannot wound an *older* structure-read holder,
    // so it defers and re-queues at its preserved priority. Once the older
    // holder finalizes, a later sweep re-drives the deferred candidate and the
    // split lands — progress without violating wound-wait ordering.
    #[tokio::test]
    async fn split_defers_to_an_older_structure_reader_then_lands() {
        let s = store();
        let (mon, clock, split_ts) = monitor_at(1_000_000);
        // An older mutation (earlier timestamp) holds structure-read on L.
        let older = TxId::with_priority(split_ts - 1_000_000_000, b"old");
        mon.begin_tx(&older);
        s.store_node(
            COLL,
            "L",
            &leaf_with_structure_r(&[b"a", b"b", b"c", b"d"], &older),
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
        let sp = splitter_full(&s, &bg, &mon, clock, tiny());

        // First pass defers: the older holder keeps its structure-read.
        sp.split_path(&paths::from_node(COLL, "L"), None)
            .await
            .unwrap();
        assert_eq!(
            Directory::new(s.clone())
                .leaves(COLL, Freshness::Latest)
                .await
                .unwrap()
                .len(),
            1,
            "the split defers to the older structure-reader"
        );
        let (held, _) = s.load_node(COLL, "L", Freshness::Latest).await.unwrap();
        assert!(
            held.locks().structure.holds(&older),
            "the older reader's structure-read is untouched"
        );

        // The older holder finalizes; the deferred candidate was re-queued, so a
        // later sweep re-drives it and the split now lands.
        mon.abort_tx(&older).await.unwrap();
        sp.run_once().await;
        assert_eq!(
            Directory::new(s.clone())
                .leaves(COLL, Freshness::Latest)
                .await
                .unwrap()
                .len(),
            2,
            "the split lands once the older holder is gone"
        );
    }

    // ADR-032: a leaf split acquires its source's structure-write through the
    // attached ShardCoordinator (folding into the same round a mutation would),
    // not a bespoke direct CAS. A splitter built coordinator-less and then
    // attached lands the split end to end through that path: the sibling is
    // created, the source shrinks and links to it, and the split's
    // structure-write is released by the shrink CAS.
    #[tokio::test]
    async fn leaf_split_acquires_structure_w_through_the_coordinator() {
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

        // The splitter routes a leaf acquire through its coordinator; hold a
        // clone so the test can close it afterward.
        let mon = test_monitor();
        let coord = test_coord(&s, &mon);
        let sp = Splitter::with_candidates(
            Arc::downgrade(&bg),
            s.clone(),
            mon.clone(),
            Clock::real(),
            DB,
            coord.clone(),
            SplitCandidates::new(tiny()),
        );

        sp.split_path(&paths::from_node(COLL, "L"), None)
            .await
            .unwrap();
        coord.close().await;

        let leaves = Directory::new(s.clone())
            .leaves(COLL, Freshness::Latest)
            .await
            .unwrap();
        assert_eq!(leaves.len(), 2, "the leaf split through the coordinator");
        // The shrunk source dropped the split's structure-write (released by the
        // shrink CAS), so it carries no node-level lock.
        let (source, _) = s.load_node(COLL, "L", Freshness::Latest).await.unwrap();
        assert!(
            source.locks().structure.is_empty(),
            "structure-write acquired via the coordinator is released by the shrink"
        );
        assert!(
            source.right_sibling().is_some(),
            "the source links to the freshly split-out sibling"
        );
    }

    // Writes a structural-log record for a leaf split of `source` producing
    // `right`, keyed (like a real split) by the sibling token.
    async fn seed_leaf_record(s: &ShardStore, source: &str, right: &str, split_key: &[u8]) {
        s.write_structural_log(
            right,
            &StructuralLog {
                prefix: COLL.to_string(),
                source_token: source.to_string(),
                source_version: String::new(),
                created_tokens: vec![right.to_string()],
                split_key: split_key.to_vec(),
                is_root: false,
            },
        )
        .await
        .unwrap();
    }

    // ADR-032 recovery, roll-forward: a crash left a structural-log record for a
    // split whose shrink CAS *did* land (the source right-links to the sibling)
    // but whose parent separator was never published. Recovery proves the
    // sibling reachable via the right-link chain, idempotently publishes the
    // separator, and deletes the finalized record.
    #[tokio::test]
    async fn recovery_rolls_forward_a_landed_split_and_publishes_the_separator() {
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
        seed_leaf_record(&s, "L", "R", b"m").await;

        let bg = Arc::new(Background::new());
        splitter(&s, &bg, tiny()).run_once().await;

        let (root_node, _) = s
            .load_root_node(COLL, Freshness::Latest)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            root_node.as_index().unwrap().child_for(b"m"),
            Some("R"),
            "recovery rolled the landed split forward into the parent"
        );
        assert!(
            s.list_structural_logs(DB).await.unwrap().is_empty(),
            "the finalized record is deleted"
        );
    }

    // ADR-032 recovery, abort: a crash left a record for a split whose shrink
    // CAS never landed — the source has no right-link, so the created sibling is
    // unreachable, and the source carries no live structure-write holder. The
    // sibling is a *proven* orphan, so recovery deletes it and finalizes the
    // record; the source is left untouched for the size trigger to re-split.
    #[tokio::test]
    async fn recovery_reclaims_a_proven_orphan_and_finalizes_the_record() {
        let s = store();
        s.store_node(
            COLL,
            "L",
            &leaf_node(&[b"a", b"b", b"c", b"d"], None, None),
            None,
        )
        .await
        .unwrap();
        // An orphaned sibling nothing references.
        s.store_node(COLL, "R", &leaf_node(&[b"c", b"d"], None, None), None)
            .await
            .unwrap();
        let mut root = CollectionRoot::new();
        root.set_node(Node::index(IndexNode::from_children([(
            Vec::new(),
            "L".to_string(),
        )])));
        s.create_root(COLL, &root).await.unwrap();
        seed_leaf_record(&s, "L", "R", b"c").await;

        let bg = Arc::new(Background::new());
        splitter(&s, &bg, tiny()).run_once().await;

        assert!(
            s.load_node(COLL, "R", Freshness::Latest).await.is_err(),
            "the proven orphan is reclaimed"
        );
        assert!(
            s.load_node(COLL, "L", Freshness::Latest).await.is_ok(),
            "the source is left untouched"
        );
        assert!(
            s.list_structural_logs(DB).await.unwrap().is_empty(),
            "the record is finalized aborted"
        );
    }

    // ADR-032 recovery, ambiguity: the created sibling is unreachable but the
    // source still carries a *live* structure-write holder — a split may be in
    // flight (this or another process). Recovery must defer, never guess: the
    // orphan and its record are kept for a later sweep once the holder resolves.
    #[tokio::test]
    async fn recovery_defers_while_a_live_structure_holder_makes_it_ambiguous() {
        let s = store();
        let (mon, clock, _) = monitor_at(1_000_000);
        let holder = TxId::with_priority(1, b"splitter");
        mon.begin_tx(&holder);
        let mut l = leaf_node(&[b"a", b"b"], None, None);
        let mut locks = l.locks().clone();
        locks.structure.acquire_write(&holder);
        l.set_locks(locks);
        s.store_node(COLL, "L", &l, None).await.unwrap();
        s.store_node(COLL, "R", &leaf_node(&[b"c", b"d"], None, None), None)
            .await
            .unwrap();
        let mut root = CollectionRoot::new();
        root.set_node(Node::index(IndexNode::from_children([(
            Vec::new(),
            "L".to_string(),
        )])));
        s.create_root(COLL, &root).await.unwrap();
        seed_leaf_record(&s, "L", "R", b"c").await;

        let bg = Arc::new(Background::new());
        splitter_full(&s, &bg, &mon, clock, tiny()).run_once().await;

        assert!(
            s.load_node(COLL, "R", Freshness::Latest).await.is_ok(),
            "the orphan is kept while the outcome is ambiguous"
        );
        assert_eq!(
            s.list_structural_logs(DB).await.unwrap().len(),
            1,
            "the record is kept for a later sweep"
        );
    }
}
