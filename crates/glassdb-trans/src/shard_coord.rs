//! The shard-mutation coordinator (ADR-028): the single per-object mechanism
//! through which every shard/leaf entry mutation flows.
//!
//! The only coordination primitive is a content compare-and-swap on a B-link
//! leaf: a node (`{prefix}/_n/<token>`) or the collection root (`{prefix}/_i`,
//! the root leaf while the collection is small, ADR-031). Concurrent
//! transactions contending one object are **deduplicated** (ADR-025/026): each
//! per-object mutation is submitted to a [`Dedup`] keyed on the object path, so
//! several transactions merge into one owner-driven load + CAS. N GET+CAS
//! round-trips collapse to one; the [`Dedup`] fans out one shared result, so
//! each transaction's own outcome ([`FoldOutcome`]) travels back through a
//! per-submission slot the caller reads once its submission resolves.
//!
//! The coordinator is the *mechanism* and knows nothing of locks, transaction
//! ids, wound-wait, or commit. It loads the leaf object once, **folds** the
//! round's installed [`ShardResolver`]s over a running staged entry map (each
//! resolver observing the entries staged by the resolvers before it), drops any
//! entry left vestigial (no holder, no `current_writer`), CASes once, recovers
//! precondition/in-doubt by reload-and-re-fold, and deposits each member's
//! outcome (ADR-029). All lock/transaction *policy* lives in the resolvers the
//! callers install: [`Locker`](crate::Locker) installs the Acquire / WriteBack /
//! Release resolvers, and [`Algo`](crate::Algo) installs the single read-write
//! CommitInstall. Membership (create/delete) is coordinated per-key in the
//! owning leaf, so there is no separate root-membership path (ADR-031). The
//! per-transaction held-lock bookkeeping lives with its owner, the
//! [`Locker`](crate::Locker), not in the engine.

use std::cmp::Ordering as CmpOrdering;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use glassdb_concurr::{
    BatchHandle, Dedup, DedupError, DedupKeySnapshot, MergeRequest, RetryConfig, Worker, rt,
};
use glassdb_data::TxId;
use glassdb_storage::{
    Freshness, LockType, NodeLocks, Shard, ShardEntry, ShardStore, StorageError,
};

use crate::error::TransError;
use crate::monitor::Monitor;
use crate::resolver::Resolver;

/// Maximum inner CAS retries on a single shard/root before treating the
/// operation as conflicted and restarting the transaction.
pub(crate) const CAS_RETRIES: usize = 50;

/// Counters for CAS activity across every submitter (the
/// [`Locker`](crate::Locker) and [`Algo`](crate::Algo)).
#[derive(Default)]
struct Stats {
    n_retries: AtomicU64,
}

/// One transaction's outcome for a single deduplicated CAS round, deposited by
/// the engine into that transaction's [`OutcomeSlot`] and read by its caller once
/// the [`Dedup`] submission resolves. Heterogeneous across resolver kinds: the
/// engine treats it as an opaque payload it stages and delivers.
#[derive(Clone)]
pub(crate) enum FoldOutcome {
    /// A lock was installed (Acquire), held at the given strength — the
    /// strongest intent across the acquired keys. Self-describing so the caller
    /// records the hold from the outcome instead of re-deriving it.
    Locked(LockType),
    /// A touched key is held by a live holder this transaction does not
    /// outrank: wait for `holder` to finalize, then re-submit (hold-and-wait,
    /// ADR-024). Nothing was staged for this transaction in the round's CAS.
    Wait(TxId),
    /// The bounded CAS budget was exhausted under churn; release and re-lock.
    Conflict,
    /// A release or write-back completed (ADR-026). Idempotent and best-effort:
    /// there is nothing to wait on and nothing for the caller to retry.
    /// `superseded` carries the `current_writer` transaction ids a write-back
    /// overwrote — GC reverse-check candidates (ADR-022); empty for a release.
    Released { superseded: Vec<TxId> },
    /// The single read-write commit-install landed: this transaction's write
    /// lock is in the shard's version chain, or it was already there (idempotent,
    /// help-forwarded). The committed object may now be published (ADR-027).
    Landed,
    /// The commit-install lost the race: the entry moved to another writer (or
    /// the key is now genuinely locked by someone else), so the fast path must
    /// renew its id and re-run. Definitively did not land.
    Moved,
    /// The commit-install's lock CAS was in-doubt (`Unavailable`) and the entry
    /// then moved, so it cannot be told whether the lock landed first: the one
    /// irreducible ambiguity, surfaced rather than risking a double-apply.
    InDoubt(String),
}

/// Why the fold engine is (re-)running the resolvers this attempt: a `Fresh`
/// first pass, or a re-fold after a CAS that failed precondition
/// (`Reloaded { in_doubt: false }`) or came back in-doubt
/// (`Reloaded { in_doubt: true }`). Only the commit-install resolver consults
/// it — to distinguish a definitive `Moved` from an irreducible `InDoubt` — so
/// every other resolver ignores it and stays idempotent across re-folds.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReloadCause {
    Fresh,
    Reloaded { in_doubt: bool },
}

/// Per-submission mailbox carrying one transaction's [`FoldOutcome`] back from
/// the dedup worker. Owned by the caller and cloned into the merged request, so
/// it lives exactly as long as either side needs it and never leaks when a
/// caller's future is dropped mid-round.
type OutcomeSlot = Arc<Mutex<Option<FoldOutcome>>>;

/// One resolver's decision for the current fold step: either stage a set of
/// entry changes (threaded to the next resolver) alongside its member outcome,
/// or stage nothing and just return an outcome (e.g. it must wait).
pub(crate) enum Step {
    /// Apply these entry changes to the running staged map and replace the
    /// running node-level lock state with `locks` (ADR-032); on a confirmed CAS
    /// deliver `outcome` to the member. A resolver that touches only the
    /// per-key entries returns the node locks it observed unchanged.
    Stage {
        entries: Vec<(Vec<u8>, ShardEntry)>,
        locks: NodeLocks,
        outcome: FoldOutcome,
    },
    /// Stage nothing (neither entries nor node locks); deliver `outcome` to the
    /// member regardless of the CAS.
    Skip { outcome: FoldOutcome },
}

/// The shared handles a resolver may consult mid-fold: the effective-writer
/// [`Resolver`] (help-forwarding), the [`Monitor`] (wound-wait status), and why
/// this fold is running ([`ReloadCause`], for commit-install in-doubt).
pub(crate) struct ResolveCtx<'a> {
    pub(crate) resolver: &'a Resolver,
    pub(crate) tmon: &'a Monitor,
    pub(crate) cause: ReloadCause,
}

/// One operation's policy decision over a shard, folded by the coordinator. The
/// engine treats it as opaque: it calls [`resolve`](ShardResolver::resolve),
/// threads any staged entries, and deposits the returned outcome. All
/// lock/transaction semantics live in the resolvers the callers install
/// ([`Locker`](crate::Locker) and [`Algo`](crate::Algo)), not in the engine.
#[async_trait]
pub(crate) trait ShardResolver: Send + Sync {
    /// Resolves this member against the entries and node-level locks **as
    /// currently staged this round** (it observes the changes staged by earlier
    /// resolvers). Returns the changes to stage plus this member's outcome, or
    /// stages nothing. `staged_locks` carries the node's structure/membership
    /// locks and membership version (ADR-032), which a mutation, scan, or split
    /// installs into or releases from in the same CAS as the entries.
    async fn resolve(
        &self,
        ctx: &ResolveCtx<'_>,
        staged: &BTreeMap<Vec<u8>, ShardEntry>,
        staged_locks: &NodeLocks,
    ) -> Result<Step, TransError>;

    /// Whether this member may join any in-flight round instead of FIFO-blocking
    /// behind an unrelated writer: read-only acquires, releases, and write-backs
    /// never contend, so they always reorder (ADR-026). A scheduling hint only.
    fn reorderable(&self) -> bool;

    /// The outcome delivered when the bounded CAS budget is exhausted under
    /// churn: acquirers must release and re-lock (`Conflict`); releases and
    /// write-backs are best-effort (`Released`).
    fn exhausted_outcome(&self) -> FoldOutcome;

    /// The raw keys this member may **create or update**, so the coordinator can
    /// verify the loaded leaf still owns them before folding (ADR-031). A split
    /// can move a key to a right sibling after it was routed to this leaf;
    /// mutating the stale leaf would strand the key. The default is empty: a
    /// resolver that only touches entries already present (release, write-back)
    /// can never create a misplaced entry — a present entry is always owned,
    /// because a split removes the keys it moves — so it needs no check.
    fn owned_keys(&self) -> Vec<&[u8]> {
        Vec::new()
    }
}

/// One transaction's participation in a shard CAS batch: its installed resolver
/// and where to deliver its outcome.
#[derive(Clone)]
struct ShardMember {
    resolver: Arc<dyn ShardResolver>,
    slot: OutcomeSlot,
}

/// A deduplication request for one leaf CAS coordination object (ADR-025): the
/// unit merged by [`Dedup`], keyed on the object path. A single submission
/// carries one transaction; a merged request accumulates several compatible
/// ones.
///
/// The leaf is identified by its object `path` — the collection root `_i` for a
/// small collection's single leaf, else a standalone node `_n`, resolved by
/// descent. `members` maps each contending transaction to its installed
/// resolver and outcome slot. `first_freshness` is the cache freshness for the
/// round's first fold attempt: `AllowStale` lets a lone round reuse a leaf the
/// submitter just cached (the single read-write fast path) without a
/// revalidation round-trip; any reload uses `Latest`.
#[derive(Clone)]
struct CasReq {
    path: String,
    members: BTreeMap<TxId, ShardMember>,
    first_freshness: Freshness,
}

impl MergeRequest for CasReq {
    fn merge(&self, other: &Self) -> Option<Self> {
        // Always union leaf members into one round (ADR-028): even same-key
        // conflicting writers share a single load + CAS. The fold resolves the
        // conflict in-round by wound-wait order — the older member stages its
        // lock and the younger emits `Wait` — so there is no benefit to keeping
        // contenders in separate batches.
        let mut members = self.members.clone();
        for (tx, m) in &other.members {
            members.insert(tx.clone(), m.clone());
        }
        Some(CasReq {
            path: self.path.clone(),
            members,
            // A merged round has more than one member, so it loads the leaf
            // fresh; `AllowStale` is only a lone-round fast-path optimization
            // and is dropped once contenders join.
            first_freshness: Freshness::Latest,
        })
    }

    fn can_reorder(&self) -> bool {
        // Read-only acquires, releases, and write-backs can join any batch
        // instead of FIFO-blocking behind an unrelated writer (ADR-026); an
        // exclusive acquire / commit-install keeps FIFO order. A pure scheduling
        // hint — merging itself no longer depends on it.
        self.members.values().all(|m| m.resolver.reorderable())
    }
}

/// Sink for the leaf-write events the coordinator emits on its CAS path, so a
/// background growth policy can decide whether to split (ADR-031). The
/// coordinator depends only on this seam — never on the splitter's queue: it
/// reports the leaf it just stored and knows nothing of soft caps. The
/// [`Splitter`](crate::Splitter) supplies the implementation; a coordinator
/// with none attached uses [`NoSplitHints`].
pub trait SplitHinter: Send + Sync {
    /// Notes that `path`'s leaf was just stored holding `shard`. Best-effort: a
    /// spurious call only costs the splitter a reload and re-check, so the
    /// coordinator never blocks on it.
    fn observe_leaf(&self, path: &str, shard: &Shard);
}

/// The default [`SplitHinter`] that drops every hint: for a coordinator with no
/// background splitter attached (tests, tools). Leaf growth is never observed,
/// so the tree only ever grows through an explicitly wired splitter.
pub(crate) struct NoSplitHints;

impl SplitHinter for NoSplitHints {
    fn observe_leaf(&self, _path: &str, _shard: &Shard) {}
}

/// State shared by the [`ShardCoordinator`] and its dedup [`CasWorker`]: the
/// storage handles, retry config, and stats.
struct CoordCore {
    tmon: Monitor,
    shards: ShardStore,
    resolver: Resolver,
    retry: RetryConfig,
    stats: Stats,
    // Where over-cap leaf writes are reported (ADR-031): the background
    // [`Splitter`](crate::Splitter)'s queue when one is wired, else a no-op.
    // Emitted on the write path so growth needs no key-space enumeration.
    hinter: Arc<dyn SplitHinter>,
}

struct CoordState {
    core: Arc<CoordCore>,
    dedup: Dedup<CasReq, TransError, CasWorker>,
}

/// The [`Dedup`] worker driving one merged round per CAS object (ADR-025): it
/// loads the shard/root once, folds every merged member's resolver, does a single
/// CAS, and deposits each member's [`FoldOutcome`] into its slot.
struct CasWorker {
    core: Arc<CoordCore>,
}

/// Returns the merged request's members.
fn shard_members(batch: &BatchHandle<CasReq, TransError>) -> BTreeMap<TxId, ShardMember> {
    batch.merged().members
}

impl CasWorker {
    /// Drives one merged shard round: load once, fold every member's resolver
    /// (threading the staged entries), CAS once, and deposit each member's
    /// outcome. A member that stages nothing (e.g. it must wait) is delivered its
    /// own outcome, so the owner never blocks — its caller waits and re-submits
    /// while the other members make progress.
    async fn run_shard(
        &self,
        path: &str,
        batch: &BatchHandle<CasReq, TransError>,
    ) -> Result<(), TransError> {
        let mut backoff = self.core.retry.backoff();
        // Why the current fold is running: `Fresh` first, then re-folds carry
        // whether the prior CAS was in-doubt so commit-install can classify.
        let mut cause = ReloadCause::Fresh;
        // In-doubt is *sticky* across re-folds: once any CAS this round came back
        // in-doubt, its write may have landed durably (and been help-forwarded to
        // a peer), so a later precondition-miss must not downgrade the ambiguity
        // to a definitive loss. Commit-install would otherwise misclassify a
        // landed-but-unacked lock as `Moved` and unsafely abandon-and-rerun a
        // committed object a peer already observed.
        let mut saw_in_doubt = false;
        // The first fold attempt may reuse a cached shard the submitter just
        // loaded (a lone single read-write round; `AllowStale` serves it without
        // a revalidation round-trip, ADR-030). Any later attempt reloads
        // `Latest`. A stale cached shard only costs a CAS miss and a reload,
        // never correctness.
        let first_freshness = batch.merged().first_freshness;
        for attempt in 0..CAS_RETRIES {
            if attempt > 0 {
                rt::sleep(backoff.next_delay()).await;
                self.core.stats.n_retries.fetch_add(1, Ordering::Relaxed);
            }
            let ctx = ResolveCtx {
                resolver: &self.core.resolver,
                tmon: &self.core.tmon,
                cause,
            };
            let freshness = if attempt == 0 {
                first_freshness
            } else {
                Freshness::Latest
            };
            let loaded = self.core.shards.load_leaf(path, freshness).await?;
            // Read the merged set *after* obtaining the leaf so this round
            // absorbs every member that queued while the load I/O was in flight
            // (ADR-025) — the window that turns N contenders' loads+CASes into
            // one. A cache-served first attempt still folds every current member
            // over the cached leaf; the CAS arbitrates if that leaf was stale.
            let members = shard_members(batch);
            let mut entries: BTreeMap<Vec<u8>, ShardEntry> = loaded
                .entries
                .entries()
                .cloned()
                .map(|e| (e.key.clone(), e))
                .collect();
            // The node-level locks (ADR-032) are threaded alongside the entries:
            // each staging resolver observes the running state and returns its
            // updated version, so structure/membership holds and the membership
            // version are installed or released in the same CAS as the entries.
            let mut locks: NodeLocks = loaded.locks.clone();

            // Fold every member's resolver over the shared entry set, threading
            // the staged changes: resolver N observes the entries as staged by
            // resolvers 1..N (ADR-028 contract 1/2). Members are visited in
            // monotonic wound-wait order (oldest first, byte tiebreak) so that on
            // a same-key conflict the older member stages its lock and the
            // younger, observing that live staged holder it cannot wound, emits
            // `Wait` — never backtracking (ADR-028 contract 1).
            let mut ordered: Vec<(&TxId, &ShardMember)> = members.iter().collect();
            ordered.sort_by(|(a, _), (b, _)| fold_order(a, b));
            let mut results: Vec<(TxId, FoldOutcome)> = Vec::with_capacity(members.len());
            let mut staged = false;
            for (tx, m) in ordered {
                // Ownership re-check (ADR-031): a split may have moved one of this
                // member's keys to a right sibling after it was routed here.
                // Mutating this leaf would strand the key, so deliver the
                // member's re-route outcome — the same signal it uses to
                // re-descend after exhausting the CAS budget (an acquirer's
                // `Conflict` release-and-relock, the fast path's `Moved` renew) —
                // and fold nothing for it. Its caller re-resolves through the
                // directory and re-submits on the leaf that now owns the key.
                if m.resolver.owned_keys().iter().any(|&k| !loaded.owns(k)) {
                    results.push((tx.clone(), m.resolver.exhausted_outcome()));
                    continue;
                }
                match m.resolver.resolve(&ctx, &entries, &locks).await? {
                    Step::Stage {
                        entries: changes,
                        locks: new_locks,
                        outcome,
                    } => {
                        for (k, e) in changes {
                            entries.insert(k, e);
                        }
                        locks = new_locks;
                        staged = true;
                        results.push((tx.clone(), outcome));
                    }
                    Step::Skip { outcome } => results.push((tx.clone(), outcome)),
                }
            }

            if staged {
                // Drop entries a member left vestigial (no holder, no
                // `current_writer`): they name no transaction and are
                // indistinguishable from absent, so pruning them here — in the
                // same CAS that clears the last holder — keeps shards tidy on
                // every path (acquire / write-back / release, ADR-029) instead
                // of leaving dead entries for a later GC cycle.
                let new_shard = glassdb_storage::Shard::from_entries(
                    entries.into_values().filter(|e| !e.is_vestigial()),
                );
                match self
                    .core
                    .shards
                    .store_leaf(
                        path,
                        &new_shard,
                        &locks,
                        loaded.kind(),
                        loaded.version.as_ref(),
                    )
                    .await
                {
                    // Hint the background splitter if this write left the leaf
                    // over the soft cap (ADR-031); the splitter reloads and
                    // re-checks, so a spurious hint only costs one load.
                    Ok(true) => self.core.hinter.observe_leaf(path, &new_shard),
                    // Precondition: the shard changed under us; reload and
                    // re-fold. This CAS definitely did not land, but an *earlier*
                    // in-doubt CAS this round might have, so carry the sticky
                    // in-doubt flag rather than clearing it.
                    Ok(false) => {
                        cause = ReloadCause::Reloaded {
                            in_doubt: saw_in_doubt,
                        };
                        continue;
                    }
                    // In-doubt lock CAS (ADR-009): re-folding our own resolvers
                    // over a freshly-read shard is idempotent, so recover in place
                    // by reloading and re-folding. Commit-install must treat a
                    // subsequent move as irreducibly in-doubt (ADR-027).
                    Err(StorageError::Unavailable(_)) => {
                        saw_in_doubt = true;
                        cause = ReloadCause::Reloaded { in_doubt: true };
                        continue;
                    }
                    Err(e) => return Err(e.into()),
                }
            }

            // The CAS landed (or nothing needed staging): publish each member's
            // outcome into its slot before returning, so the deposit
            // happens-before the dedup delivers to the caller. Recording the held
            // lock is the caller's job (the [`Locker`](crate::Locker)), done when
            // it observes its own `Locked` outcome.
            for (tx, outcome) in results {
                if let Some(m) = members.get(&tx) {
                    *m.slot.lock().unwrap() = Some(outcome);
                }
            }
            return Ok(());
        }
        // Bounded CAS budget exhausted under churn: each member gets its
        // resolver's exhaustion outcome (acquirers `Conflict` and release/re-lock,
        // best-effort releases / write-backs `Released`, ADR-024/026).
        for m in shard_members(batch).values() {
            *m.slot.lock().unwrap() = Some(m.resolver.exhausted_outcome());
        }
        Ok(())
    }
}

#[async_trait]
impl Worker<CasReq, TransError> for CasWorker {
    async fn run(
        &self,
        _key: &str,
        batch: &BatchHandle<CasReq, TransError>,
    ) -> Result<(), TransError> {
        self.run_shard(&batch.merged().path, batch).await
    }
}

/// The single per-object mechanism through which every shard/root entry mutation
/// flows (ADR-028): a [`Dedup`] over the CAS coordination objects that loads each
/// object once, folds every contending transaction's resolver, does one CAS, and
/// deposits each transaction's outcome. It is a pure mechanism: the
/// per-transaction held-lock bookkeeping lives with its owner, the
/// [`Locker`](crate::Locker).
#[derive(Clone)]
pub struct ShardCoordinator {
    inner: Arc<CoordState>,
}

impl ShardCoordinator {
    /// Creates a coordinator over the shared shard store, resolver, and monitor.
    /// `retry` configures the exponential backoff applied both between CAS
    /// retries on a contended object and (in the [`Locker`](crate::Locker) above)
    /// between hold-and-wait re-polls of a conflicting holder.
    pub fn new(shards: ShardStore, resolver: Resolver, tmon: Monitor, retry: RetryConfig) -> Self {
        Self::with_hinter(shards, resolver, tmon, retry, Arc::new(NoSplitHints))
    }

    /// Creates a coordinator that reports over-cap leaf writes to `hinter` — the
    /// background [`Splitter`](crate::Splitter)'s queue (ADR-031). The
    /// coordinator only emits leaf-write events; the soft-cap policy and the
    /// candidate queue live behind the seam, so it never names the splitter.
    pub fn with_hinter(
        shards: ShardStore,
        resolver: Resolver,
        tmon: Monitor,
        retry: RetryConfig,
        hinter: Arc<dyn SplitHinter>,
    ) -> Self {
        let core = Arc::new(CoordCore {
            tmon,
            shards,
            resolver,
            retry,
            stats: Stats::default(),
            hinter,
        });
        let dedup = Dedup::new(CasWorker { core: core.clone() });
        ShardCoordinator {
            inner: Arc::new(CoordState { core, dedup }),
        }
    }

    /// Cancels in-flight coordination and awaits any spawned dedup owner tasks,
    /// so none leak when the database shuts down (ADR-025).
    pub async fn close(&self) {
        self.inner.dedup.close().await;
    }

    /// Returns and resets the count of inner CAS retries performed under
    /// contention across every submitter (acquire / release / write-back /
    /// commit-install).
    pub fn cas_retries_and_reset(&self) -> usize {
        self.inner.core.stats.n_retries.swap(0, Ordering::Relaxed) as usize
    }

    /// Returns a per-object dedup coordination snapshot (ADR-025).
    pub fn dedup_snapshot(&self) -> Vec<DedupKeySnapshot> {
        self.inner.dedup.snapshot()
    }

    /// Submits one shard member (any resolver installed by a caller — the
    /// [`Locker`](crate::Locker)'s acquire / write-back / release or the
    /// [`Algo`](crate::Algo)'s commit-install) through the [`Dedup`] and awaits
    /// its single-round [`FoldOutcome`]. The worker merges it into any in-flight
    /// round for the shard, folds it, retries CAS contention / in-doubt
    /// internally, and deposits the outcome into the slot. Returns `Ok(None)` if
    /// the coordinator was shut down before the round ran, so acquires can error
    /// while best-effort releases / write-backs treat it as a no-op.
    ///
    /// `first_freshness` chooses the cache freshness for the round's first fold
    /// attempt: a submitter that just read this leaf (the single read-write fast
    /// path, for its eligibility check) passes `AllowStale` so the round reuses
    /// the cached copy instead of revalidating it (ADR-030); callers with no
    /// fresh cached snapshot pass `Latest`.
    ///
    /// `path` is the leaf's object path — the collection root `_i` for a small
    /// collection's single leaf, else a standalone node `_n` resolved by descent
    /// ([`Directory`](glassdb_storage::Directory)).
    pub(crate) async fn submit_shard(
        &self,
        path: &str,
        id: &TxId,
        resolver: Arc<dyn ShardResolver>,
        first_freshness: Freshness,
    ) -> Result<Option<FoldOutcome>, TransError> {
        let slot: OutcomeSlot = Arc::new(Mutex::new(None));
        let mut members = BTreeMap::new();
        members.insert(
            id.clone(),
            ShardMember {
                resolver,
                slot: slot.clone(),
            },
        );
        let req = CasReq {
            path: path.to_string(),
            members,
            first_freshness,
        };
        match self.inner.dedup.run(path, req).await {
            // The worker deposits an outcome for every member before it returns
            // `Ok` (the CAS-landed and exhaustion paths both fill every slot), so
            // a completed round always leaves this member's slot filled — the
            // engine never fabricates a policy outcome of its own.
            Ok(()) => Ok(Some(slot.lock().unwrap().take().expect(
                "the CAS worker deposits an outcome for every member on success",
            ))),
            Err(DedupError::Work(e)) => Err((*e).clone()),
            Err(DedupError::Cancelled) => Ok(None),
        }
    }
}

/// Total order for the monotonic fold: oldest wound-wait priority first, with a
/// deterministic full-id byte tiebreak for equal-priority members. The tiebreak
/// is **round-local** — it only fixes who stages first this round, never who
/// wins a wound ([`should_wound`] ignores it) — so a renewed id (fresh prefix,
/// same priority) can reorder the fold without ever flipping a persistent wound
/// winner, which is what would let equal-priority peers livelock (ADR-002/028).
fn fold_order(a: &TxId, b: &TxId) -> CmpOrdering {
    if a.older(b) {
        CmpOrdering::Less
    } else if b.older(a) {
        CmpOrdering::Greater
    } else {
        a.as_bytes().cmp(b.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{
        BackendOp, HookBackend, HookFuture, OpLog, RecordingBackend,
    };
    use glassdb_concurr::Background;
    use glassdb_data::paths;
    use glassdb_storage::{LockType, Node, ObjectCache, Shard, SharedCache, TLogger, ValueCache};

    const COLL: &str = "coordp";

    // Every coordination round in these tests targets one leaf object. A
    // standalone node `_n/<token>` is the cleanest stand-in: it carries only key
    // entries (no collection metadata), exactly what the shard fold operates on.
    fn leaf() -> String {
        paths::from_node(COLL, "L")
    }

    // A coordinator over `backend` with its own (large, non-evicting) cache, plus
    // the shard store backing it (a clone sharing the cache, so a test can warm or
    // seed the cache the coordinator reads). The returned `Background` must be
    // kept alive for the monitor's lifetime.
    fn coord_over(backend: Arc<dyn Backend>) -> (ShardCoordinator, ShardStore, Arc<Background>) {
        let cache = SharedCache::new(1 << 20);
        let values = ValueCache::new(&cache);
        let objects = ObjectCache::new(backend, &cache);
        let tl = TLogger::new(objects.clone(), COLL);
        let bg = Arc::new(Background::new());
        let mon = Monitor::new(values, tl, Arc::downgrade(&bg));
        let shards = ShardStore::new(objects);
        let resolver = Resolver::new(shards.clone(), mon.clone());
        let coord = ShardCoordinator::new(shards.clone(), resolver, mon, RetryConfig::default());
        (coord, shards, bg)
    }

    // A cold shard store over `backend` (its own empty cache), for asserting what
    // actually landed in storage without touching the coordinator's cache.
    fn cold_store(backend: Arc<dyn Backend>) -> ShardStore {
        ShardStore::new(ObjectCache::new(backend, &SharedCache::new(1 << 20)))
    }

    fn entry(
        key: &[u8],
        lock_type: LockType,
        holder: Option<&TxId>,
        writer: Option<&TxId>,
    ) -> ShardEntry {
        ShardEntry {
            key: key.to_vec(),
            lock_type,
            locked_by: holder.into_iter().cloned().collect(),
            current_writer: writer.cloned(),
            deleted: false,
        }
    }

    // Replaces the leaf's entries with exactly `entries` (a plain CAS, no
    // coordinator).
    async fn store_shard_entries(store: &ShardStore, path: &str, entries: Vec<ShardEntry>) {
        let loaded = store.load_leaf(path, Freshness::Latest).await.unwrap();
        let shard = Shard::from_entries(entries);
        assert!(
            store
                .store_leaf(
                    path,
                    &shard,
                    &loaded.locks,
                    loaded.kind(),
                    loaded.version.as_ref()
                )
                .await
                .unwrap()
        );
    }

    fn shard_reads(log: &OpLog) -> usize {
        log.lock()
            .unwrap()
            .iter()
            .filter(|r| (r.op == "read" || r.op == "read_if_modified") && r.path.contains("/_n/"))
            .count()
    }

    fn shard_stores(log: &OpLog) -> usize {
        log.lock()
            .unwrap()
            .iter()
            .filter(|r| {
                (r.op == "write_if" || r.op == "write_if_not_exists") && r.path.contains("/_n/")
            })
            .count()
    }

    // Loads the leaf's entries from a cold store, for asserting what landed.
    async fn cold_entries(store: &ShardStore, path: &str) -> Shard {
        store
            .load_leaf(path, Freshness::Latest)
            .await
            .unwrap()
            .entries
    }

    // Stages a write lock for `tx` on `key`, preserving any fields already staged.
    struct StageLock {
        key: Vec<u8>,
        tx: TxId,
    }

    #[async_trait::async_trait]
    impl ShardResolver for StageLock {
        async fn resolve(
            &self,
            _ctx: &ResolveCtx<'_>,
            staged: &BTreeMap<Vec<u8>, ShardEntry>,
            staged_locks: &NodeLocks,
        ) -> Result<Step, TransError> {
            let mut e = staged
                .get(&self.key)
                .cloned()
                .unwrap_or_else(|| entry(&self.key, LockType::None, None, None));
            e.lock_type = LockType::Write;
            e.locked_by = vec![self.tx.clone()];
            Ok(Step::Stage {
                entries: vec![(self.key.clone(), e)],
                locks: staged_locks.clone(),
                outcome: FoldOutcome::Locked(LockType::Write),
            })
        }

        fn reorderable(&self) -> bool {
            false
        }

        fn exhausted_outcome(&self) -> FoldOutcome {
            FoldOutcome::Conflict
        }

        fn owned_keys(&self) -> Vec<&[u8]> {
            vec![self.key.as_slice()]
        }
    }

    // Stages nothing; always delivers a best-effort `Released`.
    struct SkipRelease;

    #[async_trait::async_trait]
    impl ShardResolver for SkipRelease {
        async fn resolve(
            &self,
            _ctx: &ResolveCtx<'_>,
            _staged: &BTreeMap<Vec<u8>, ShardEntry>,
            _staged_locks: &NodeLocks,
        ) -> Result<Step, TransError> {
            Ok(Step::Skip {
                outcome: FoldOutcome::Released {
                    superseded: Vec::new(),
                },
            })
        }

        fn reorderable(&self) -> bool {
            true
        }

        fn exhausted_outcome(&self) -> FoldOutcome {
            FoldOutcome::Released {
                superseded: Vec::new(),
            }
        }
    }

    // The fold trace: each member records its id and the keys it saw already
    // staged when its turn came, so a test can assert fold order and threading.
    type FoldTrace = Arc<Mutex<Vec<(TxId, Vec<Vec<u8>>)>>>;

    // Records what it observed mid-fold, then stages its own committed pointer.
    struct Recorder {
        key: Vec<u8>,
        tx: TxId,
        trace: FoldTrace,
    }

    #[async_trait::async_trait]
    impl ShardResolver for Recorder {
        async fn resolve(
            &self,
            _ctx: &ResolveCtx<'_>,
            staged: &BTreeMap<Vec<u8>, ShardEntry>,
            staged_locks: &NodeLocks,
        ) -> Result<Step, TransError> {
            self.trace
                .lock()
                .unwrap()
                .push((self.tx.clone(), staged.keys().cloned().collect()));
            Ok(Step::Stage {
                entries: vec![(
                    self.key.clone(),
                    entry(&self.key, LockType::None, None, Some(&self.tx)),
                )],
                locks: staged_locks.clone(),
                outcome: FoldOutcome::Landed,
            })
        }

        fn reorderable(&self) -> bool {
            false
        }

        fn exhausted_outcome(&self) -> FoldOutcome {
            FoldOutcome::Conflict
        }
    }

    // A hook that parks the next shard read while armed, letting a second submitter merge.
    struct Gate {
        notify: Arc<tokio::sync::Notify>,
        armed: std::sync::atomic::AtomicBool,
    }

    impl Gate {
        fn wrap(inner: Arc<dyn Backend>) -> (Arc<HookBackend>, Arc<Self>) {
            let gate = Arc::new(Gate {
                notify: Arc::new(tokio::sync::Notify::new()),
                armed: std::sync::atomic::AtomicBool::new(false),
            });
            let backend = HookBackend::new(inner);
            backend.set_before({
                let gate = gate.clone();
                move |op| {
                    let wait = matches!(
                        op,
                        BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                    ) && gate.armed.swap(false, std::sync::atomic::Ordering::SeqCst);
                    let notify = gate.notify.clone();
                    let future: HookFuture = Box::pin(async move {
                        if wait {
                            notify.notified().await;
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

        fn release(&self) {
            self.notify.notify_one();
        }
    }

    // A resolver that stages entries drives one CAS, and the staged entry is
    // durable — the coordinator loads, folds, and CASes the returned state.
    #[tokio::test]
    async fn shard_stage_is_cas_persisted() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (coord, _shards, _bg) = coord_over(backend.clone());
        let tx = TxId::with_priority(1, b"t");

        let out = coord
            .submit_shard(
                &leaf(),
                &tx,
                Arc::new(StageLock {
                    key: b"k".to_vec(),
                    tx: tx.clone(),
                }),
                Freshness::Latest,
            )
            .await
            .unwrap();
        assert!(matches!(out, Some(FoldOutcome::Locked(_))));
        coord.close().await;

        let shard = cold_entries(&cold_store(backend), &leaf()).await;
        let e = shard.lookup(b"k").expect("the staged lock is persisted");
        assert_eq!(e.lock_type, LockType::Write);
        assert_eq!(e.locked_by, vec![tx]);
    }

    // A split can move a key to a right sibling after it was routed to this
    // leaf. The coordinator must notice the loaded leaf no longer owns the key
    // and re-route (deliver the member's re-route outcome) rather than strand a
    // fresh entry in the wrong leaf (ADR-031, M1-S2).
    #[tokio::test]
    async fn reroutes_when_a_split_moved_the_key_out_of_the_leaf() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (coord, store, _bg) = coord_over(backend.clone());

        // Seed the leaf as a shrunk left half: it owns keys < "m" and links to a
        // right sibling. "z" now lives in that sibling, not here.
        let mut node = Node::leaf(Shard::from_entries([entry(
            b"a",
            LockType::None,
            None,
            None,
        )]));
        node.set_bounds(Some(b"m".to_vec()), Some("R".to_string()));
        assert!(store.store_node(COLL, "L", &node, None).await.unwrap());

        let tx = TxId::with_priority(1, b"t");
        let out = coord
            .submit_shard(
                &leaf(),
                &tx,
                Arc::new(StageLock {
                    key: b"z".to_vec(),
                    tx: tx.clone(),
                }),
                Freshness::Latest,
            )
            .await
            .unwrap();
        // Re-route: the acquire-shaped resolver's exhausted/re-route outcome is a
        // `Conflict`, which its caller turns into release-and-relock.
        assert!(matches!(out, Some(FoldOutcome::Conflict)));
        coord.close().await;

        // The wrong leaf was never mutated: "z" was not stranded here, and the
        // owned key "a" is untouched.
        let shard = cold_entries(&cold_store(backend), &leaf()).await;
        assert!(
            shard.lookup(b"z").is_none(),
            "moved key must not be recreated here"
        );
        assert!(shard.lookup(b"a").is_some());
    }

    // An owned key still folds normally: the ownership re-check is transparent
    // when the leaf legitimately owns the round's keys.
    #[tokio::test]
    async fn owned_key_folds_normally_despite_a_high_key() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (coord, store, _bg) = coord_over(backend.clone());

        let mut node = Node::leaf(Shard::new());
        node.set_bounds(Some(b"m".to_vec()), None);
        assert!(store.store_node(COLL, "L", &node, None).await.unwrap());

        let tx = TxId::with_priority(1, b"t");
        let out = coord
            .submit_shard(
                &leaf(),
                &tx,
                Arc::new(StageLock {
                    key: b"a".to_vec(),
                    tx: tx.clone(),
                }),
                Freshness::Latest,
            )
            .await
            .unwrap();
        assert!(matches!(out, Some(FoldOutcome::Locked(_))));
        coord.close().await;

        let shard = cold_entries(&cold_store(backend), &leaf()).await;
        assert!(
            shard.lookup(b"a").is_some(),
            "an owned key is locked as usual"
        );
    }

    // A resolver that stages nothing (`Skip`) still gets its outcome delivered,
    // and the round issues no CAS.
    #[tokio::test]
    async fn shard_skip_delivers_outcome_without_cas() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        let (coord, _shards, _bg) = coord_over(backend);
        let tx = TxId::with_priority(1, b"t");

        let out = coord
            .submit_shard(&leaf(), &tx, Arc::new(SkipRelease), Freshness::Latest)
            .await
            .unwrap();
        assert!(matches!(out, Some(FoldOutcome::Released { .. })));
        assert_eq!(shard_stores(&log), 0, "a skip stages nothing, so no CAS");
        coord.close().await;
    }

    // An entry left with no holder and no committed writer is indistinguishable
    // from absent, so the CAS that folds the round drops it (ADR-029) while
    // keeping live pointers and newly staged locks.
    #[tokio::test]
    async fn shard_prunes_vestigial_entries_on_cas() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (coord, shards, _bg) = coord_over(backend.clone());
        let writer = TxId::with_priority(1, b"w");
        store_shard_entries(
            &shards,
            &leaf(),
            vec![
                entry(b"vestige", LockType::None, None, None),
                entry(b"live", LockType::None, None, Some(&writer)),
            ],
        )
        .await;

        let tx = TxId::with_priority(2, b"t");
        coord
            .submit_shard(
                &leaf(),
                &tx,
                Arc::new(StageLock {
                    key: b"lock".to_vec(),
                    tx: tx.clone(),
                }),
                Freshness::Latest,
            )
            .await
            .unwrap();
        coord.close().await;

        let shard = cold_entries(&cold_store(backend), &leaf()).await;
        assert!(
            shard.lookup(b"vestige").is_none(),
            "the vestigial entry is dropped by the CAS"
        );
        assert!(shard.lookup(b"live").is_some(), "the live pointer is kept");
        assert!(
            shard.lookup(b"lock").is_some(),
            "the newly staged lock is kept"
        );
    }

    // ADR-030 at the coordinator: a lone round's first attempt reuses the cached
    // shard when the submitter asks for `AllowStale` (no backend read), while
    // `Latest` revalidates it with one conditional read.
    #[tokio::test]
    async fn allow_stale_first_attempt_reuses_cache() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);

        // Seed through a separate cache so the coordinator starts cold, then warm
        // its cache with one cold load.
        let writer = TxId::with_priority(1, b"w");
        store_shard_entries(
            &cold_store(backend.clone()),
            &leaf(),
            vec![entry(b"seed", LockType::None, None, Some(&writer))],
        )
        .await;
        let (coord, shards, _bg) = coord_over(backend.clone());
        shards.load_leaf(&leaf(), Freshness::Latest).await.unwrap();

        let tx = TxId::with_priority(2, b"t");
        log.lock().unwrap().clear();
        coord
            .submit_shard(&leaf(), &tx, Arc::new(SkipRelease), Freshness::AllowStale)
            .await
            .unwrap();
        assert_eq!(
            shard_reads(&log),
            0,
            "AllowStale serves the cached shard with no backend read"
        );

        log.lock().unwrap().clear();
        coord
            .submit_shard(&leaf(), &tx, Arc::new(SkipRelease), Freshness::Latest)
            .await
            .unwrap();
        assert_eq!(
            shard_reads(&log),
            1,
            "Latest revalidates the cached shard once"
        );
        coord.close().await;
    }

    // ADR-028: two transactions contending the same shard merge into one round —
    // a single shared load and a single CAS — folded oldest-first, with the
    // younger member observing the older's staged entry (threading).
    #[tokio::test(start_paused = true)]
    async fn same_shard_submits_merge_into_one_round() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (backend, gate) = Gate::wrap(mem);
        let recorder = Arc::new(RecordingBackend::new(backend));
        let log = recorder.log();
        let (coord, _shards, _bg) = coord_over(recorder as Arc<dyn Backend>);

        let trace: FoldTrace = Arc::new(Mutex::new(Vec::new()));
        let old = TxId::with_priority(1, b"old");
        let young = TxId::with_priority(2, b"young");

        // The older member submits first, becomes the dedup driver, and parks in
        // the gated load; the younger then queues into that open batch.
        gate.arm();
        let (c1, t1, tr1) = (coord.clone(), old.clone(), trace.clone());
        let driver = tokio::spawn(async move {
            c1.submit_shard(
                &leaf(),
                &t1,
                Arc::new(Recorder {
                    key: b"a".to_vec(),
                    tx: t1.clone(),
                    trace: tr1,
                }),
                Freshness::Latest,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;

        let (c2, t2, tr2) = (coord.clone(), young.clone(), trace.clone());
        let joiner = tokio::spawn(async move {
            c2.submit_shard(
                &leaf(),
                &t2,
                Arc::new(Recorder {
                    key: b"b".to_vec(),
                    tx: t2.clone(),
                    trace: tr2,
                }),
                Freshness::Latest,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        assert!(matches!(
            driver.await.unwrap().unwrap(),
            Some(FoldOutcome::Landed)
        ));
        assert!(matches!(
            joiner.await.unwrap().unwrap(),
            Some(FoldOutcome::Landed)
        ));

        assert_eq!(shard_reads(&log), 1, "both members share one shard load");
        assert_eq!(shard_stores(&log), 1, "both members land in one CAS");
        coord.close().await;

        let trace = trace.lock().unwrap();
        assert_eq!(trace.len(), 2, "both members are folded once");
        assert_eq!(trace[0].0, old, "the older member folds first");
        assert_eq!(trace[1].0, young);
        assert!(
            trace[1].1.contains(&b"a".to_vec()),
            "the younger member observes the older's staged entry"
        );
    }

    // A submit after shutdown is a cancelled no-op (`Ok(None)`), so best-effort
    // callers treat it as done and acquirers can distinguish it.
    #[tokio::test]
    async fn submit_after_close_is_cancelled() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (coord, _shards, _bg) = coord_over(backend);
        coord.close().await;

        let tx = TxId::with_priority(1, b"t");
        let out = coord
            .submit_shard(&leaf(), &tx, Arc::new(SkipRelease), Freshness::Latest)
            .await
            .unwrap();
        assert!(
            out.is_none(),
            "a submit after shutdown is a cancelled no-op"
        );
    }

    // Fails two leaf CASes before forwarding to isolate sticky in-doubt classification.
    fn in_doubt_then_miss(inner: Arc<dyn Backend>) -> Arc<HookBackend> {
        let backend = HookBackend::new(inner);
        let leaf_cas = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        backend.set_before(move |op| {
            let result = match op {
                BackendOp::WriteIf { path, .. }
                    if path.contains("/_n/") || path.ends_with("/_i") =>
                {
                    match leaf_cas.fetch_add(1, Ordering::SeqCst) {
                        0 => Err(glassdb_backend::BackendError::Unavailable(
                            "simulated in-doubt leaf CAS".into(),
                        )),
                        1 => Err(glassdb_backend::BackendError::Precondition),
                        _ => Ok(()),
                    }
                }
                _ => Ok(()),
            };
            let future: HookFuture = Box::pin(async move { result });
            future
        });
        backend
    }

    // A commit-install-shaped resolver: it stages its write lock (issuing a CAS)
    // on its first two folds, then on the third fold classifies its lost race the
    // same way `CommitInstallResolver` does — `InDoubt` if any earlier CAS this
    // round was in-doubt, else a definitive `Moved`. Records the deciding fold's
    // `in_doubt` cause so the test can pin the coordinator's state machine.
    struct StickyInstallProbe {
        key: Vec<u8>,
        tx: TxId,
        folds: std::sync::atomic::AtomicUsize,
        seen_in_doubt: Arc<Mutex<Option<bool>>>,
    }

    #[async_trait::async_trait]
    impl ShardResolver for StickyInstallProbe {
        async fn resolve(
            &self,
            ctx: &ResolveCtx<'_>,
            _staged: &BTreeMap<Vec<u8>, ShardEntry>,
            staged_locks: &NodeLocks,
        ) -> Result<Step, TransError> {
            if self.folds.fetch_add(1, Ordering::SeqCst) < 2 {
                return Ok(Step::Stage {
                    entries: vec![(
                        self.key.clone(),
                        entry(&self.key, LockType::Write, Some(&self.tx), None),
                    )],
                    locks: staged_locks.clone(),
                    outcome: FoldOutcome::Landed,
                });
            }
            let in_doubt = matches!(ctx.cause, ReloadCause::Reloaded { in_doubt: true });
            *self.seen_in_doubt.lock().unwrap() = Some(in_doubt);
            let outcome = if in_doubt {
                FoldOutcome::InDoubt("lost race after in-doubt CAS".into())
            } else {
                FoldOutcome::Moved
            };
            Ok(Step::Skip { outcome })
        }

        fn reorderable(&self) -> bool {
            false
        }

        fn exhausted_outcome(&self) -> FoldOutcome {
            FoldOutcome::Moved
        }

        fn owned_keys(&self) -> Vec<&[u8]> {
            vec![self.key.as_slice()]
        }
    }

    // Regression (single read-write fast path double-apply): once any CAS in a
    // round comes back in-doubt, its write may have landed durably and been
    // help-forwarded to a peer, so the in-doubt classification must stay *sticky*
    // across a later precondition-miss. Otherwise a commit-install whose lock
    // landed-but-unacked (then superseded) is misclassified `Moved`, and the fast
    // path abandons and re-runs a non-idempotent write a peer already observed —
    // breaking the `final <= started` serializability bound.
    //
    // This pins the coordinator half of the fix in isolation: the exact `cause`
    // state transition, driven with a stubbed member so no concurrent scheduling
    // is needed. The *end-to-end* manifestation (the real commit-install being
    // abandoned and double-applying under the true 3-way co-batched interleaving)
    // is covered deterministically by the committed fuzz reproducer
    // `fuzz/corpus/concurrent_tx/crash-95084997…`, which the corpus-replay test
    // (`crates/glassdb/tests/fuzz_corpus.rs`) replays through the sim scheduler.
    // That interleaving cannot be forced by the plain-tokio in-doubt harness
    // (`crates/glassdb/tests/in_doubt.rs`), whose 2-step lost-ack→moved case
    // classifies in-doubt without ever hitting the resetting precondition-miss.
    #[tokio::test]
    async fn in_doubt_cas_stays_in_doubt_across_a_later_precondition_miss() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let backend: Arc<dyn Backend> = in_doubt_then_miss(mem);
        let (coord, shards, _bg) = coord_over(backend);

        // The leaf must exist so the round's CAS is a `write_if` (the faulted op),
        // not a create.
        let seed = TxId::with_priority(1, b"seed");
        store_shard_entries(
            &shards,
            &leaf(),
            vec![entry(b"seed", LockType::None, None, Some(&seed))],
        )
        .await;

        let tx = TxId::with_priority(2, b"install");
        let seen_in_doubt = Arc::new(Mutex::new(None));
        let out = coord
            .submit_shard(
                &leaf(),
                &tx,
                Arc::new(StickyInstallProbe {
                    key: b"k".to_vec(),
                    tx: tx.clone(),
                    folds: std::sync::atomic::AtomicUsize::new(0),
                    seen_in_doubt: seen_in_doubt.clone(),
                }),
                Freshness::Latest,
            )
            .await
            .unwrap();
        coord.close().await;

        assert_eq!(
            *seen_in_doubt.lock().unwrap(),
            Some(true),
            "the precondition-miss after an in-doubt CAS must keep the cause in-doubt"
        );
        assert!(
            matches!(out, Some(FoldOutcome::InDoubt(_))),
            "a landed-but-unacked CAS that is then superseded must classify InDoubt, \
             not Moved (else the fast path abandons and double-applies)"
        );
    }
}
