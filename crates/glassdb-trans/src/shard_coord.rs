//! The shard-mutation coordinator (ADR-028): the transaction-aware shared
//! fold engine through which every shard/leaf entry mutation flows.
//!
//! The only coordination primitive is a content compare-and-swap on a B-link
//! leaf: a node (`{prefix}/_n/<token>`) or the collection root (`{prefix}/_r`,
//! the root leaf while the collection is small, ADR-031). Concurrent
//! transactions contending one object are **deduplicated** (ADR-025/026): each
//! per-object mutation is submitted to a [`Dedup`] keyed on the object path, so
//! several transactions merge into one owner-driven load + CAS. N GET+CAS
//! round-trips collapse to one; the [`Dedup`] fans out one shared result, so
//! each transaction's own outcome ([`FoldOutcome`]) travels back through a
//! per-submission slot the caller reads once its submission resolves.
//!
//! The coordinator owns the cross-operation protocol required to combine
//! heterogeneous mutations safely: transaction identity, oldest-first fold
//! order, per-member in-doubt attribution, routing and capacity admission, and
//! same-key exclusion for logless publication. It loads the leaf object once,
//! **folds** the round's installed [`ShardResolver`]s over a running staged entry
//! map, drops vestigial entries, CASes once, recovers by reload-and-re-fold, and
//! deposits each member's outcome (ADR-029). The resolvers own each
//! operation's mutation decision: [`Locker`](crate::tlocker::Locker) installs Acquire /
//! WriteBack / Release, and [`Algo`](crate::algo::Algo) installs direct commit. The
//! per-transaction held-lock bookkeeping and cross-shard strategy stay with the
//! [`Locker`](crate::tlocker::Locker), not in the engine.

use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::{AddAssign, Sub};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use glassdb_concurr::{
    BatchHandle, Dedup, DedupError, DedupKeySnapshot, MergeRequest, RetryConfig, Worker, rt,
};
use glassdb_data::{ObjectPath, TxId};
use glassdb_storage::{
    LeafEdit, LeafObservation, LockType, NodeLocks, Requirement, Shard, ShardEntry, ShardStore,
    SplitPolicy, StorageError,
};

use crate::error::TransError;
use crate::key_state_resolver::KeyStateResolver;
use crate::monitor::Monitor;

/// Maximum inner CAS retries on a single shard/root before treating the
/// operation as conflicted and restarting the transaction.
pub(crate) const CAS_RETRIES: usize = 50;

/// Counters for CAS activity across every submitter (the
/// [`Locker`](crate::tlocker::Locker) and [`Algo`](crate::algo::Algo)).
#[derive(Default)]
struct Stats {
    n_retries: AtomicU64,
}

/// Coordination work for one snapshot or accumulated interval.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShardCoordinatorStats {
    pub submissions: u64,
    pub rounds: u64,
    pub cas_retries: u64,
}

impl AddAssign for ShardCoordinatorStats {
    fn add_assign(&mut self, rhs: Self) {
        self.submissions += rhs.submissions;
        self.rounds += rhs.rounds;
        self.cas_retries += rhs.cas_retries;
    }
}

impl Sub for ShardCoordinatorStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            submissions: self.submissions.saturating_sub(rhs.submissions),
            rounds: self.rounds.saturating_sub(rhs.rounds),
            cas_retries: self.cas_retries.saturating_sub(rhs.cas_retries),
        }
    }
}

/// One transaction's outcome for a single deduplicated CAS round, deposited by
/// the engine into that transaction's [`OutcomeSlot`] and read by its caller once
/// the [`Dedup`] submission resolves. The worker transports values without
/// inspecting their variants, while this closed enum deliberately defines the
/// result vocabulary shared by the installed resolver kinds (ADR-028).
#[derive(Clone, Debug)]
pub(crate) enum FoldOutcome {
    /// A lock was installed (Acquire), carrying the strongest entry intention
    /// and the membership scope held on the leaf.
    Locked { typ: LockType, membership: LockType },
    /// A touched key is held by a live holder this transaction does not
    /// outrank: wait for `holder` to finalize, then re-submit (hold-and-wait,
    /// ADR-024). Nothing was staged for this transaction in the round's CAS.
    Wait(TxId),
    /// The bounded CAS budget was exhausted under churn, or a stage that does
    /// not add a user key reached the absolute object limit. Release and
    /// re-lock while the hinted split makes progress.
    Conflict,
    /// A create would exceed the leaf's reserved content limit. Nothing was
    /// staged for this member; retry after the pending split relieves the leaf.
    LeafFull,
    /// A release or write-back completed (ADR-026). The current node state
    /// proved that the holder was removed or the corresponding CAS landed.
    /// `superseded` carries the `current_writer` transaction ids a write-back
    /// overwrote — GC reverse-check candidates (ADR-022); empty for a release.
    Released { superseded: Vec<TxId> },
    /// The submitted leaf no longer owns one of this operation's keys. The
    /// caller must descend again and regroup before retrying.
    Reroute,
    /// A logless direct commit landed: this transaction's value is published in
    /// the shard's version chain, or it was already there (idempotent, ADR-051).
    Landed,
    /// A logless direct commit lost the race: the entry moved to another writer
    /// (or the key is now genuinely locked by someone else), so only the regular
    /// locked protocol can resolve it. Definitively did not land.
    Moved,
    /// A logless direct commit definitively staged nothing *and* the round
    /// certifies it left no durable state anywhere, so its read-modify-write
    /// body may be reevaluated against the current version under the same id
    /// rather than publishing a holder (ADR-053).
    Replay,
    /// A commit-critical CAS was in-doubt (`Unavailable`) and the re-fold could
    /// not prove whether it landed, so the commit may or may not have happened:
    /// the one irreducible ambiguity, surfaced rather than risking a
    /// double-apply.
    InDoubt(String),
}

/// One member's policy outcome and the physical precondition certified by the
/// coordinator's successful CAS. Only a staged member receives a
/// `cas_precondition`; higher layers decide whether that receipt proves their
/// own logical validation condition.
pub(crate) struct CoordinatedOutcome {
    pub(crate) outcome: FoldOutcome,
    pub(crate) cas_precondition: Option<LeafObservation>,
}

/// Why the fold engine is (re-)running one resolver this attempt: a `Fresh`
/// first pass, or a re-fold after a CAS that failed precondition
/// (`Reloaded { in_doubt: false }`) or came back in-doubt
/// (`Reloaded { in_doubt: true }`). The in-doubt bit is the member's own: it is
/// set only for the members whose stage rode the uncertain CAS. Only the direct
/// commit resolver consults it — to distinguish a definitive loss from an
/// irreducible `InDoubt` — so every other resolver ignores it and stays
/// idempotent across re-folds.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReloadCause {
    Fresh,
    Reloaded { in_doubt: bool },
}

/// Per-submission mailbox carrying one transaction's [`CoordinatedOutcome`]
/// back from the dedup worker. Owned by the caller and cloned into the merged
/// request, so it lives exactly as long as either side needs it and never leaks
/// when a caller's future is dropped mid-round.
type OutcomeSlot = Arc<Mutex<Option<CoordinatedOutcome>>>;

/// How a staged mutation participates in leaf-capacity admission.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum StageAdmission {
    /// The stage does not add a user key, so it may consume reserved headroom
    /// but must still fit under the absolute encoded-object limit.
    ExistingKeys,
    /// The stage publishes an inline value. In addition to the absolute object
    /// limit, each published entry must retain the per-entry split budget so it
    /// cannot leave behind an intrinsically unsplittable singleton.
    InlinePublication,
    /// The stage adds at least one user key and must fit below the content limit
    /// that reserves headroom for locks and the split's shrink CAS.
    AddsKey,
}

/// One resolver's decision for the current fold step: either stage entry and
/// node-lock changes alongside its member outcome, or stage nothing.
pub(crate) enum Step {
    /// Apply these entry changes and replace the running node-lock state. The
    /// coordinator alone owns the node's topology, body reconstruction, and
    /// capacity admission.
    Stage {
        entries: Vec<(Vec<u8>, ShardEntry)>,
        locks: NodeLocks,
        admission: StageAdmission,
        outcome: FoldOutcome,
    },
    /// Stage nothing; deliver `outcome` to the member regardless of the CAS.
    Skip { outcome: FoldOutcome },
}

/// The shared handles a resolver may consult mid-fold: loaded key-state
/// resolution, the transaction monitor, and why this fold is running.
pub(crate) struct ResolveCtx<'a> {
    pub(crate) key_state: &'a KeyStateResolver,
    pub(crate) tmon: &'a Monitor,
    pub(crate) requirement: Requirement,
    pub(crate) cause: ReloadCause,
}

/// One operation's mutation decision over a shard, folded by the coordinator.
/// The engine calls [`resolve`](ShardResolver::resolve), threads any staged
/// entries, and deposits the returned outcome. Resolver implementations own the
/// acquire, write-back, release, and direct-commit decisions; the coordinator
/// owns the ordering, admission, and recovery contract they share (ADR-028).
#[async_trait]
pub(crate) trait ShardResolver: Send + Sync {
    /// Resolves this member against entries and node locks as currently staged
    /// this round. Resolvers cannot mutate node topology.
    async fn resolve(
        &self,
        ctx: &ResolveCtx<'_>,
        staged: &BTreeMap<Vec<u8>, ShardEntry>,
        staged_locks: &NodeLocks,
    ) -> Result<Step, TransError>;

    /// Whether this member may join any in-flight round instead of FIFO-blocking
    /// behind an unrelated writer. Read-only acquires, releases, and write-backs
    /// are safe to reorder (ADR-026), even though a structural gate can make a
    /// cleanup member wait. A scheduling hint only.
    fn reorderable(&self) -> bool;

    /// The outcome delivered when this round cannot produce a definitive
    /// result. `in_doubt` reports whether a CAS carrying *this member's* stage
    /// may have landed, so a non-idempotent resolver cannot downgrade
    /// uncertainty while abandoning the round.
    fn exhausted_outcome(&self, in_doubt: bool) -> FoldOutcome;

    /// The outcome delivered when a structural change invalidated routing.
    fn reroute_outcome(&self, in_doubt: bool) -> FoldOutcome {
        self.exhausted_outcome(in_doubt)
    }

    /// The outcome delivered when a peer already claimed one of this member's
    /// [`logless_keys`](ShardResolver::logless_keys) this round, so nothing was
    /// folded for it at all. Distinct from exhaustion: the peer's claim proves
    /// this member staged nothing, which a spent CAS budget does not, so a
    /// resolver may treat it as a certified loss rather than an unknown one
    /// (ADR-053). `in_doubt` still reports whether an *earlier* attempt of this
    /// round carried this member's own stage.
    fn excluded_outcome(&self, in_doubt: bool) -> FoldOutcome {
        self.exhausted_outcome(in_doubt)
    }

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

    /// The raw keys this member commits loglessly (ADR-051): its staged entry is
    /// the commit's only record, so no second logless member may stage over it in
    /// the same CAS. The coordinator lets at most one of them stage per key per
    /// round and tells the rest they did not land. Disjoint keys still share a
    /// round. The default is empty: a member backed by a transaction object
    /// records its commit outside the leaf and needs no exclusivity.
    fn logless_keys(&self) -> Vec<&[u8]> {
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
/// The leaf is identified by its object `path` — the collection root `_r` for a
/// small collection's single leaf, else a standalone node `_n`, resolved by
/// descent. `members` maps each contending transaction to its installed
/// resolver and outcome slot. `first_requirement` is the cache requirement for the
/// round's first fold attempt: `Any` lets a lone round reuse a leaf the
/// submitter just cached (the logless direct commit) without a
/// revalidation round-trip. A failed mutation invalidates that exact seed, so
/// retries use `Any` to consume the winner or newer shared knowledge.
#[derive(Clone)]
struct CasReq {
    path: String,
    members: BTreeMap<TxId, ShardMember>,
    first_requirement: Requirement,
}

impl MergeRequest for CasReq {
    fn merge(&self, other: &Self) -> Option<Self> {
        // One transaction can have several operations in flight on the same leaf
        // at once — e.g. GC releasing a presumed-dead transaction's holds
        // (ADR-029) while that transaction's own acquire is still resolving on
        // the same object (ADR-025). Each submission carries its own outcome
        // slot, but a fold round runs at most one resolver per transaction id
        // and the dedup delivers to *every* merged submission. Merging two
        // submissions that share an id would collapse them to a single map
        // entry — silently dropping one submission's resolver and its outcome
        // slot, leaving that caller a delivered-but-empty slot. Decline the
        // merge on any id overlap so the colliding submission runs in its own
        // subsequent round instead.
        if other.members.keys().any(|tx| self.members.contains_key(tx)) {
            return None;
        }
        // Otherwise union distinct-id leaf members into one round (ADR-028):
        // even same-key conflicting writers share a single load + CAS. The fold
        // resolves the conflict in-round by wound-wait order — the older member
        // stages its lock and the younger emits `Wait` — so there is no benefit
        // to keeping contenders in separate batches.
        let mut members = self.members.clone();
        for (tx, m) in &other.members {
            members.insert(tx.clone(), m.clone());
        }
        Some(CasReq {
            path: self.path.clone(),
            members,
            first_requirement: self.first_requirement.stricter(other.first_requirement),
        })
    }

    fn can_reorder(&self) -> bool {
        // Read-only acquires, releases, and write-backs can join any batch
        // instead of FIFO-blocking behind an unrelated writer (ADR-026); an
        // exclusive acquire / direct commit keeps FIFO order. A pure scheduling
        // hint — merging itself no longer depends on it.
        self.members.values().all(|m| m.resolver.reorderable())
    }
}

/// Sink for stored-leaf capacity observations, so a background growth policy
/// can decide whether to split (ADR-031). The coordinator depends only on this
/// seam — never on the splitter's queue or policy. The splitter supplies the
/// implementation.
pub trait SplitHinter: Send + Sync {
    /// Notes that `path`'s leaf was just stored holding `shard`. Best-effort: a
    /// spurious call only costs the splitter a reload and re-check, so the
    /// coordinator never blocks on it.
    fn observe_leaf(&self, path: &str, shard: &Shard);
}

/// State shared by the [`ShardCoordinator`] and its dedup [`CasWorker`]: the
/// storage handles, retry config, and stats.
struct CoordCore {
    tmon: Monitor,
    shards: ShardStore,
    key_state: KeyStateResolver,
    retry: RetryConfig,
    stats: Stats,
    // Where stored over-cap leaves are reported: the background
    // [`Splitter`](crate::split::Splitter)'s queue when one is wired.
    hinter: Arc<dyn SplitHinter>,
    policy: SplitPolicy,
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum Participation {
    Skipped,
    Staged,
}

struct MemberFold {
    id: TxId,
    outcome: FoldOutcome,
    participation: Participation,
}

struct FoldPlan {
    entries: BTreeMap<Vec<u8>, ShardEntry>,
    locks: NodeLocks,
    members: Vec<MemberFold>,
}

impl FoldPlan {
    fn staged_ids(&self) -> impl Iterator<Item = &TxId> {
        self.members.iter().filter_map(|member| {
            (member.participation == Participation::Staged).then_some(&member.id)
        })
    }

    fn is_dirty(&self) -> bool {
        self.members
            .iter()
            .any(|member| member.participation == Participation::Staged)
    }
}

struct ProposedStage {
    entries: Vec<(Vec<u8>, ShardEntry)>,
    locks: NodeLocks,
    admission: StageAdmission,
    outcome: FoldOutcome,
}

enum CapacityDecision {
    Admitted(ProposedStage),
    Rejected(FoldOutcome),
}

enum PersistResult {
    Landed,
    PreconditionMiss,
    InDoubt(BTreeSet<TxId>),
}

impl CasWorker {
    /// Builds the ordered mutation plan for one loaded shard attempt.
    async fn fold_round(
        &self,
        path: &str,
        edit: &LeafEdit,
        members: &BTreeMap<TxId, ShardMember>,
        requirement: Requirement,
        reloaded: bool,
        in_doubt: &BTreeSet<TxId>,
    ) -> Result<FoldPlan, TransError> {
        let mut plan = FoldPlan {
            entries: edit
                .entries()
                .entries()
                .cloned()
                .map(|e| (e.key.clone(), e))
                .collect(),
            locks: edit.locks().clone(),
            members: Vec::with_capacity(members.len()),
        };

        // Oldest-first ordering makes the fold monotonic: a later member cannot
        // wound a member whose stage it has already observed (ADR-028).
        let mut ordered: Vec<(&TxId, &ShardMember)> = members.iter().collect();
        ordered.sort_by(|(a, _), (b, _)| fold_order(a, b));
        // A logless stage is its commit's only evidence, so another member may
        // not overwrite it before the shared CAS (ADR-051).
        let mut logless: BTreeSet<Vec<u8>> = BTreeSet::new();
        for (tx, member) in ordered {
            let member_in_doubt = in_doubt.contains(tx);
            let ctx = ResolveCtx {
                key_state: &self.core.key_state,
                tmon: &self.core.tmon,
                requirement,
                cause: if reloaded {
                    ReloadCause::Reloaded {
                        in_doubt: member_in_doubt,
                    }
                } else {
                    ReloadCause::Fresh
                },
            };

            let needs_reroute = member
                .resolver
                .owned_keys()
                .iter()
                .any(|&key| !edit.owns(key));
            if needs_reroute {
                plan.members.push(MemberFold {
                    id: tx.clone(),
                    outcome: member.resolver.reroute_outcome(member_in_doubt),
                    participation: Participation::Skipped,
                });
                continue;
            }
            let logless_conflict = member
                .resolver
                .logless_keys()
                .iter()
                .any(|&key| logless.contains(key));
            if logless_conflict {
                plan.members.push(MemberFold {
                    id: tx.clone(),
                    outcome: member.resolver.excluded_outcome(member_in_doubt),
                    participation: Participation::Skipped,
                });
                continue;
            }

            let step = member
                .resolver
                .resolve(&ctx, &plan.entries, &plan.locks)
                .await?;
            match step {
                Step::Stage {
                    entries: changes,
                    locks,
                    admission,
                    outcome,
                } => {
                    let proposed = ProposedStage {
                        entries: changes,
                        locks,
                        admission,
                        outcome,
                    };
                    match self.capacity_decision(
                        path,
                        edit,
                        member.resolver.as_ref(),
                        member_in_doubt,
                        &plan.entries,
                        proposed,
                    )? {
                        CapacityDecision::Admitted(proposed) => {
                            for (key, entry) in proposed.entries {
                                plan.entries.insert(key, entry);
                            }
                            logless.extend(
                                member
                                    .resolver
                                    .logless_keys()
                                    .into_iter()
                                    .map(<[u8]>::to_vec),
                            );
                            plan.locks = proposed.locks;
                            plan.members.push(MemberFold {
                                id: tx.clone(),
                                outcome: proposed.outcome,
                                participation: Participation::Staged,
                            });
                        }
                        CapacityDecision::Rejected(outcome) => {
                            plan.members.push(MemberFold {
                                id: tx.clone(),
                                outcome,
                                participation: Participation::Skipped,
                            });
                        }
                    }
                }
                Step::Skip { outcome } => plan.members.push(MemberFold {
                    id: tx.clone(),
                    outcome,
                    participation: Participation::Skipped,
                }),
            }
        }
        Ok(plan)
    }

    /// Classifies whether one proposed member stage fits the loaded leaf.
    fn capacity_decision(
        &self,
        path: &str,
        edit: &LeafEdit,
        resolver: &dyn ShardResolver,
        in_doubt: bool,
        entries: &BTreeMap<Vec<u8>, ShardEntry>,
        proposed: ProposedStage,
    ) -> Result<CapacityDecision, TransError> {
        let mut candidate_entries = entries.clone();
        for (key, entry) in &proposed.entries {
            candidate_entries.insert(key.clone(), entry.clone());
        }
        let candidate_shard = Shard::from_entries(
            candidate_entries
                .values()
                .filter(|entry| !entry.is_vestigial())
                .cloned(),
        );
        let mut candidate_node = edit.node().clone();
        candidate_node.set_leaf(candidate_shard.clone())?;
        candidate_node.set_locks(proposed.locks.clone());
        let create_full = proposed.admission == StageAdmission::AddsKey
            && candidate_node.content_encoded_len() > self.core.policy.content_limit();
        let inline_entry_full = proposed.admission == StageAdmission::InlinePublication
            && proposed
                .entries
                .iter()
                .any(|(_, entry)| !self.core.policy.entry_fits_split_budget(entry));
        if candidate_node.encoded_len() <= self.core.policy.node_max_bytes
            && !create_full
            && !inline_entry_full
        {
            return Ok(CapacityDecision::Admitted(proposed));
        }

        // Splitting cannot make an intrinsically oversized entry fit. The
        // direct publisher falls back to an external value instead.
        if !inline_entry_full {
            self.core.hinter.observe_leaf(path, &candidate_shard);
        }
        let outcome = if proposed.admission == StageAdmission::AddsKey {
            FoldOutcome::LeafFull
        } else if in_doubt {
            resolver.exhausted_outcome(true)
        } else {
            FoldOutcome::Conflict
        };
        Ok(CapacityDecision::Rejected(outcome))
    }

    /// Persists one fold plan and classifies what happened to its staged members.
    async fn persist(
        &self,
        path: &str,
        mut edit: LeafEdit,
        plan: &mut FoldPlan,
    ) -> Result<PersistResult, TransError> {
        if !plan.is_dirty() {
            return Ok(PersistResult::Landed);
        }

        // Drop entries a member left vestigial (no holder, no
        // `current_writer`): they name no transaction and are
        // indistinguishable from absent, so pruning them here — in the
        // same CAS that clears the last holder — keeps shards tidy on
        // every path (acquire / write-back / release, ADR-029) instead
        // of leaving dead entries for a later GC cycle.
        let new_shard = Shard::from_entries(
            std::mem::take(&mut plan.entries)
                .into_values()
                .filter(|entry| !entry.is_vestigial()),
        );
        edit.set_entries(new_shard.clone());
        edit.set_locks(plan.locks.clone());
        match self.core.shards.commit_leaf(edit).await {
            // Hint the background splitter if this write left the leaf
            // over the soft cap (ADR-031); the splitter reloads and
            // re-checks, so a spurious hint only costs one load.
            Ok(true) => {
                self.core.hinter.observe_leaf(path, &new_shard);
                Ok(PersistResult::Landed)
            }
            Ok(false) => Ok(PersistResult::PreconditionMiss),
            Err(StorageError::Unavailable(_)) => {
                Ok(PersistResult::InDoubt(plan.staged_ids().cloned().collect()))
            }
            Err(error) => Err(error.into()),
        }
    }

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
        let object_path = ObjectPath::try_from(path)
            .map_err(|error| TransError::with_source("parsing coordinated leaf path", error))?;
        let first_requirement = batch.merged().first_requirement;
        // A cache-served `Any` load may complete without yielding. Give peers
        // already scheduled for this object one opportunity to join the round,
        // so batching does not depend on backend I/O creating the collection
        // window. A bounded load already opens that window at its backend await.
        if first_requirement == Requirement::Any {
            rt::yield_now().await;
        }
        let mut backoff = self.core.retry.backoff();
        // Whether the current fold is a re-fold, so a resolver can tell its first
        // pass from a retry after a CAS that did not land.
        let mut reloaded = false;
        // The members whose changes rode a CAS that came back in-doubt. For them
        // in-doubt is *sticky* across re-folds: that write may have landed
        // durably (and been help-forwarded to a peer), so a later
        // precondition-miss must not downgrade the ambiguity to a definitive
        // loss. Commit-install would otherwise misclassify a landed-but-unacked
        // lock as `Moved` and unsafely abandon-and-rerun a committed object a
        // peer already observed.
        //
        // It is per member rather than per round: a member the uncertain CAS did
        // not carry — one skipped for a same-key logless claim, or merged into
        // the batch afterwards — definitively did not land, and inheriting the
        // batch's ambiguity would strand it in-doubt over a write it never made.
        let mut in_doubt: BTreeSet<TxId> = BTreeSet::new();
        // The first fold attempt may reuse a cached shard the submitter just
        // loaded (a lone single read-write round; `Any` serves it without
        // a revalidation round-trip, ADR-030). A failed or in-doubt CAS
        // invalidates the exact seed observation, so later attempts can also use
        // `Any`: they either read the winner or reuse newer knowledge another
        // operation already published. A stale cached shard only costs a CAS
        // miss and a reload, never correctness.
        for attempt in 0..CAS_RETRIES {
            if attempt > 0 {
                rt::sleep(backoff.next_delay()).await;
                self.core.stats.n_retries.fetch_add(1, Ordering::Relaxed);
            }
            let requirement = if attempt == 0 {
                first_requirement
            } else {
                Requirement::Any
            };
            let edit = match self.core.shards.load_leaf(&object_path, requirement).await {
                Ok(loaded) => loaded.into_edit(),
                // A root split can turn the routed root leaf into an index
                // between grouping and this load. Deliver each resolver's
                // reroute outcome so its caller rebuilds the current leaf set.
                Err(StorageError::Precondition) => {
                    let members = shard_members(batch);
                    for (tx, member) in &members {
                        *member.slot.lock().unwrap() = Some(CoordinatedOutcome {
                            outcome: member.resolver.reroute_outcome(in_doubt.contains(tx)),
                            cas_precondition: None,
                        });
                    }
                    return Ok(());
                }
                Err(e) => return Err(e.into()),
            };
            // Read the merged set *after* obtaining the leaf so this round
            // absorbs every member that queued while the load I/O was in flight
            // (ADR-025) — the window that turns N contenders' loads+CASes into
            // one. A cache-served first attempt still folds every current member
            // over the cached leaf; the CAS arbitrates if that leaf was stale.
            let members = shard_members(batch);
            let mut plan = self
                .fold_round(
                    path,
                    &edit,
                    &members,
                    first_requirement,
                    reloaded,
                    &in_doubt,
                )
                .await?;

            let cas_precondition = edit.observation().clone();
            let persist_result = self.persist(path, edit, &mut plan).await?;
            match persist_result {
                PersistResult::Landed => {}
                // This CAS definitely did not land, but an earlier in-doubt CAS
                // might have, so leave the members it carried marked.
                PersistResult::PreconditionMiss => {
                    reloaded = true;
                    continue;
                }
                // Re-folding over a freshly-read shard is idempotent. Only the
                // members this uncertain CAS actually carried inherit its doubt.
                PersistResult::InDoubt(staged_ids) => {
                    in_doubt.extend(staged_ids);
                    reloaded = true;
                    continue;
                }
            }

            // The CAS landed (or nothing needed staging): publish each member's
            // outcome into its slot before returning, so the deposit
            // happens-before the dedup delivers to the caller. Recording the held
            // lock is the caller's job (the [`Locker`](crate::tlocker::Locker)), done when
            // it observes its own `Locked` outcome.
            for member in plan.members {
                if let Some(m) = members.get(&member.id) {
                    *m.slot.lock().unwrap() = Some(CoordinatedOutcome {
                        outcome: member.outcome,
                        cas_precondition: (member.participation == Participation::Staged)
                            .then(|| cas_precondition.clone()),
                    });
                }
            }
            return Ok(());
        }
        // Bounded CAS budget exhausted under churn: each member gets its
        // resolver's exhaustion outcome. Acquirers conflict and release/re-lock;
        // write-backs re-descend because exhaustion does not prove convergence.
        for (tx, m) in &shard_members(batch) {
            *m.slot.lock().unwrap() = Some(CoordinatedOutcome {
                outcome: m.resolver.exhausted_outcome(in_doubt.contains(tx)),
                cas_precondition: None,
            });
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

/// The transaction-aware shared fold engine through which every shard/root entry
/// mutation flows (ADR-028): a [`Dedup`] over the CAS coordination objects
/// that orders contending transactions, loads each object once, folds their
/// resolvers, does one CAS, and deposits each transaction's outcome. Transaction
/// lifecycle and per-transaction held-lock bookkeeping remain with their
/// higher-level owners.
#[derive(Clone)]
pub struct ShardCoordinator {
    inner: Arc<CoordState>,
}

impl ShardCoordinator {
    /// Creates a coordinator that reports capacity observations to `hinter` —
    /// normally the background [`Splitter`](crate::split::Splitter)'s queue.
    /// `policy` governs the coordinator's hard node-size limit.
    pub fn with_hinter(
        shards: ShardStore,
        key_state: KeyStateResolver,
        tmon: Monitor,
        retry: RetryConfig,
        policy: SplitPolicy,
        hinter: Arc<dyn SplitHinter>,
    ) -> Self {
        let core = Arc::new(CoordCore {
            tmon,
            shards,
            key_state,
            retry,
            stats: Stats::default(),
            policy,
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

    /// Returns and resets submission, worker-round, and inner-CAS retry counts.
    pub fn stats_and_reset(&self) -> ShardCoordinatorStats {
        let dedup = self.inner.dedup.stats_and_reset();
        ShardCoordinatorStats {
            submissions: dedup.submissions,
            rounds: dedup.rounds,
            cas_retries: self.inner.core.stats.n_retries.swap(0, Ordering::Relaxed),
        }
    }

    /// Returns a per-object dedup coordination snapshot (ADR-025).
    pub fn dedup_snapshot(&self) -> Vec<DedupKeySnapshot> {
        self.inner.dedup.snapshot()
    }

    /// Submits one shard member (any resolver installed by a caller — the
    /// [`Locker`](crate::tlocker::Locker)'s acquire / write-back / release or the
    /// [`Algo`](crate::algo::Algo)'s direct commit) through the [`Dedup`] and awaits
    /// its single-round [`CoordinatedOutcome`]. The worker merges it into any
    /// in-flight round for the shard, folds it, retries CAS contention / in-doubt
    /// internally, and deposits the policy outcome plus any successful-CAS
    /// precondition receipt into the slot. Returns `Ok(None)` if the coordinator
    /// was shut down before the round ran, so callers can preserve their
    /// operation-specific best-effort behavior.
    ///
    /// `first_requirement` chooses the cache requirement for the round's first fold
    /// attempt: a submitter that just read this leaf (the single read-write fast
    /// path, for its eligibility check) passes `Any` so the round reuses
    /// the cached copy instead of revalidating it (ADR-030); skip-capable
    /// resolvers pass their phase's captured lower bound because their outcome
    /// may not be followed by a CAS.
    ///
    /// `path` is the leaf's object path — the collection root `_r` for a small
    /// collection's single leaf, else a standalone node `_n` resolved by descent
    /// ([`TreeRouter`](glassdb_storage::TreeRouter)).
    pub(crate) async fn submit_shard(
        &self,
        path: &str,
        id: &TxId,
        resolver: Arc<dyn ShardResolver>,
        first_requirement: Requirement,
    ) -> Result<Option<CoordinatedOutcome>, TransError> {
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
            first_requirement,
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
    use glassdb_data::{CollectionAddress, DbRoot, NodeToken, ObjectPath};
    use glassdb_storage::transaction::TLogger;
    use glassdb_storage::{CachedStore, CurrentState, LockType, Node, Shard, Timeline};

    const COLL: &str = "coordp";

    fn collection() -> CollectionAddress {
        CollectionAddress::root(COLL)
    }

    fn leaf_token() -> NodeToken {
        NodeToken::from_bytes([0; 16])
    }

    fn object_path(path: &str) -> ObjectPath {
        ObjectPath::try_from(path).unwrap()
    }

    struct NoSplitHints;

    impl SplitHinter for NoSplitHints {
        fn observe_leaf(&self, _path: &str, _shard: &Shard) {}
    }

    // Every coordination round in these tests targets one leaf object. A
    // standalone node `_n/<token>` is the cleanest stand-in: it carries only key
    // entries (no collection metadata), exactly what the shard fold operates on.
    fn leaf_path() -> ObjectPath {
        ObjectPath::Node {
            collection: collection(),
            token: leaf_token(),
        }
    }

    fn leaf() -> String {
        leaf_path().to_string()
    }

    // A coordinator over `backend` with its own (large, non-evicting) cache, plus
    // the shard store backing it (a clone sharing the cache, so a test can warm or
    // seed the cache the coordinator reads). The returned `Background` must be
    // kept alive for the monitor's lifetime.
    async fn coord_over(
        backend: Arc<dyn Backend>,
    ) -> (ShardCoordinator, ShardStore, Timeline, Arc<Background>) {
        coord_over_with(backend, SplitPolicy::default(), Arc::new(NoSplitHints)).await
    }

    async fn coord_over_with(
        backend: Arc<dyn Backend>,
        policy: SplitPolicy,
        hinter: Arc<dyn SplitHinter>,
    ) -> (ShardCoordinator, ShardStore, Timeline, Arc<Background>) {
        coord_over_retry(backend, policy, hinter, RetryConfig::default()).await
    }

    // A coordinator with a near-zero CAS backoff, so an exhaustion regression
    // does not pay the production retry delay.
    async fn coord_over_fast(
        backend: Arc<dyn Backend>,
    ) -> (ShardCoordinator, ShardStore, Timeline, Arc<Background>) {
        coord_over_retry(
            backend,
            SplitPolicy::default(),
            Arc::new(NoSplitHints),
            RetryConfig {
                initial_interval: Duration::from_nanos(1),
                max_interval: Duration::from_nanos(1),
            },
        )
        .await
    }

    async fn coord_over_retry(
        backend: Arc<dyn Backend>,
        policy: SplitPolicy,
        hinter: Arc<dyn SplitHinter>,
        retry: RetryConfig,
    ) -> (ShardCoordinator, ShardStore, Timeline, Arc<Background>) {
        let seed_timeline = Timeline::new();
        let seed_store = ShardStore::new(CachedStore::new(
            backend.clone(),
            1 << 20,
            seed_timeline,
            None,
        ));
        let _ = seed_store
            .store_node(
                &collection(),
                &leaf_token(),
                &Node::leaf(Shard::new()),
                None,
            )
            .await
            .unwrap();

        let timeline = Timeline::new();
        let objects = CachedStore::new(backend, 1 << 20, timeline.clone(), None);
        let tl = TLogger::new(objects.clone(), DbRoot::try_from(COLL).unwrap());
        let bg = Arc::new(Background::new());
        let mon = Monitor::with_config(
            tl,
            timeline.clone(),
            Arc::downgrade(&bg),
            RetryConfig::default(),
            crate::monitor::ProtocolTiming::default(),
        );
        let shards = ShardStore::new(objects);
        let key_state = KeyStateResolver::new(mon.clone());
        let coord =
            ShardCoordinator::with_hinter(shards.clone(), key_state, mon, retry, policy, hinter);
        (coord, shards, timeline, bg)
    }

    // A cold shard store over `backend` (its own empty cache), for asserting what
    // actually landed in storage without touching the coordinator's cache.
    fn cold_store(backend: Arc<dyn Backend>) -> ShardStore {
        let timeline = Timeline::new();
        ShardStore::new(CachedStore::new(backend, 1 << 20, timeline, None))
    }

    fn entry(
        key: &[u8],
        lock_type: LockType,
        holder: Option<&TxId>,
        writer: Option<&TxId>,
    ) -> ShardEntry {
        ShardEntry {
            lock_type,
            locked_by: holder.into_iter().cloned().collect(),
            current: writer.map_or(CurrentState::Absent, |writer| CurrentState::External {
                writer: writer.clone(),
            }),
            ..ShardEntry::new(key)
        }
    }

    // Replaces the leaf's entries with exactly `entries` (a plain CAS, no
    // coordinator).
    async fn store_shard_entries(store: &ShardStore, path: &str, entries: Vec<ShardEntry>) {
        let _ = store
            .store_node(
                &collection(),
                &leaf_token(),
                &Node::leaf(Shard::new()),
                None,
            )
            .await
            .unwrap();
        let loaded = store
            .load_leaf(&object_path(path), Requirement::Any)
            .await
            .unwrap();
        let shard = Shard::from_entries(entries);
        let mut edit = loaded.into_edit();
        edit.set_entries(shard);
        assert!(store.commit_leaf(edit).await.unwrap());
    }

    async fn replace_leaf_node(store: &ShardStore, node: &Node) {
        let observed = store
            .load_node_state(&collection(), &leaf_token(), Requirement::Any)
            .await
            .unwrap();
        assert!(
            store
                .store_node(&collection(), &leaf_token(), node, Some(&observed))
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
            .load_leaf(&object_path(path), Requirement::Any)
            .await
            .unwrap()
            .entries()
            .clone()
    }

    fn test_member(resolver: Arc<dyn ShardResolver>) -> ShardMember {
        ShardMember {
            resolver,
            slot: Arc::new(Mutex::new(None)),
        }
    }

    fn cas_worker(coord: &ShardCoordinator) -> CasWorker {
        CasWorker {
            core: coord.inner.core.clone(),
        }
    }

    fn planned_member<'a>(plan: &'a FoldPlan, id: &TxId) -> &'a MemberFold {
        plan.members
            .iter()
            .find(|member| &member.id == id)
            .expect("the fold plans every member")
    }

    // Stages a write lock for `tx` on `key`, preserving any fields already staged.
    struct StageLock {
        key: Vec<u8>,
        tx: TxId,
        admission: StageAdmission,
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
            e.lock_type = if self.admission == StageAdmission::AddsKey {
                LockType::Create
            } else {
                LockType::Write
            };
            e.locked_by = vec![self.tx.clone()];
            Ok(Step::Stage {
                entries: vec![(self.key.clone(), e)],
                locks: staged_locks.clone(),
                admission: self.admission,
                outcome: FoldOutcome::Locked {
                    typ: LockType::Write,
                    membership: LockType::None,
                },
            })
        }

        fn reorderable(&self) -> bool {
            false
        }

        fn exhausted_outcome(&self, _in_doubt: bool) -> FoldOutcome {
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

        fn exhausted_outcome(&self, _in_doubt: bool) -> FoldOutcome {
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
                admission: StageAdmission::ExistingKeys,
                outcome: FoldOutcome::Landed,
            })
        }

        fn reorderable(&self) -> bool {
            false
        }

        fn exhausted_outcome(&self, _in_doubt: bool) -> FoldOutcome {
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

    #[derive(Default)]
    struct HintCounter {
        calls: std::sync::atomic::AtomicUsize,
    }

    impl SplitHinter for HintCounter {
        fn observe_leaf(&self, _path: &str, _shard: &Shard) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    // A fold plan records the physical participation separately from the policy
    // outcome, so only an admitted stage dirties the plan and earns a receipt.
    #[tokio::test]
    async fn fold_plan_distinguishes_staged_and_skipped_members() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        let (coord, shards, _timeline, _bg) = coord_over(backend).await;
        let edit = shards
            .load_leaf(&leaf_path(), Requirement::Any)
            .await
            .unwrap()
            .into_edit();
        log.lock().unwrap().clear();

        let staged = TxId::with_priority(1, b"staged");
        let skipped = TxId::with_priority(2, b"skipped");
        let members = BTreeMap::from([
            (
                staged.clone(),
                test_member(Arc::new(StageLock {
                    key: b"k".to_vec(),
                    tx: staged.clone(),
                    admission: StageAdmission::ExistingKeys,
                })),
            ),
            (skipped.clone(), test_member(Arc::new(SkipRelease))),
        ]);

        let plan = cas_worker(&coord)
            .fold_round(
                &leaf(),
                &edit,
                &members,
                Requirement::Any,
                false,
                &BTreeSet::new(),
            )
            .await
            .unwrap();

        assert!(
            log.lock().unwrap().is_empty(),
            "fold planning performs no backend I/O"
        );
        assert!(plan.is_dirty());
        assert_eq!(
            plan.staged_ids().cloned().collect::<Vec<_>>(),
            vec![staged.clone()]
        );
        assert!(matches!(
            planned_member(&plan, &staged),
            MemberFold {
                outcome: FoldOutcome::Locked { .. },
                participation: Participation::Staged,
                ..
            }
        ));
        assert!(matches!(
            planned_member(&plan, &skipped),
            MemberFold {
                outcome: FoldOutcome::Released { .. },
                participation: Participation::Skipped,
                ..
            }
        ));
        assert_eq!(
            plan.entries.get(b"k".as_slice()).unwrap().locked_by,
            vec![staged]
        );
        coord.close().await;
    }

    // The first logless stage claims its key inside the plan; a later claimant
    // is excluded before resolution so it cannot erase the first commit marker.
    #[tokio::test]
    async fn fold_plan_excludes_a_second_same_key_logless_member() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (coord, shards, _timeline, _bg) = coord_over(backend).await;
        let edit = shards
            .load_leaf(&leaf_path(), Requirement::Any)
            .await
            .unwrap()
            .into_edit();
        let first = TxId::with_priority(1, b"first");
        let second = TxId::with_priority(2, b"second");
        let members = BTreeMap::from([
            (
                first.clone(),
                test_member(Arc::new(StageInline::logless(b"k", &first, b"first"))),
            ),
            (
                second.clone(),
                test_member(Arc::new(StageInline::logless(b"k", &second, b"second"))),
            ),
        ]);

        let plan = cas_worker(&coord)
            .fold_round(
                &leaf(),
                &edit,
                &members,
                Requirement::Any,
                false,
                &BTreeSet::new(),
            )
            .await
            .unwrap();

        assert!(matches!(
            planned_member(&plan, &first),
            MemberFold {
                outcome: FoldOutcome::Landed,
                participation: Participation::Staged,
                ..
            }
        ));
        assert!(matches!(
            planned_member(&plan, &second),
            MemberFold {
                outcome: FoldOutcome::Conflict,
                participation: Participation::Skipped,
                ..
            }
        ));
        assert_eq!(plan.staged_ids().cloned().collect::<Vec<_>>(), vec![first]);
        assert_eq!(
            plan.entries
                .get(b"k".as_slice())
                .unwrap()
                .current
                .inline()
                .map(|value| &**value),
            Some(b"first".as_slice())
        );
        coord.close().await;
    }

    // Capacity rejection is member-local: an admitted overwrite remains in the
    // plan when a later create crosses the reserved content limit.
    #[tokio::test]
    async fn fold_plan_keeps_an_admitted_stage_when_a_later_member_is_full() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        let writer = TxId::with_priority(0, b"writer");
        let staged = TxId::with_priority(1, b"staged");
        let rejected = TxId::with_priority(2, b"rejected");
        let seed = entry(b"a", LockType::None, None, Some(&writer));
        let mut overwritten = seed.clone();
        overwritten.lock_type = LockType::Write;
        overwritten.locked_by = vec![staged.clone()];
        let created = entry(b"z", LockType::Create, Some(&rejected), None);
        let overwrite_len =
            Node::leaf(Shard::from_entries([overwritten.clone()])).content_encoded_len();
        let full_node = Node::leaf(Shard::from_entries([overwritten, created]));
        let content_limit = overwrite_len - 1;
        let node_max_bytes = full_node.encoded_len() + 64;
        let policy = SplitPolicy {
            node_max_bytes,
            split_headroom_bytes: node_max_bytes - content_limit,
            ..SplitPolicy::default()
        };
        let hints = Arc::new(HintCounter::default());
        let (coord, shards, _timeline, _bg) = coord_over_with(backend, policy, hints.clone()).await;
        store_shard_entries(&shards, &leaf(), vec![seed]).await;
        let edit = shards
            .load_leaf(&leaf_path(), Requirement::Any)
            .await
            .unwrap()
            .into_edit();
        log.lock().unwrap().clear();
        let members = BTreeMap::from([
            (
                staged.clone(),
                test_member(Arc::new(StageLock {
                    key: b"a".to_vec(),
                    tx: staged.clone(),
                    admission: StageAdmission::ExistingKeys,
                })),
            ),
            (
                rejected.clone(),
                test_member(Arc::new(StageLock {
                    key: b"z".to_vec(),
                    tx: rejected.clone(),
                    admission: StageAdmission::AddsKey,
                })),
            ),
        ]);

        let plan = cas_worker(&coord)
            .fold_round(
                &leaf(),
                &edit,
                &members,
                Requirement::Any,
                false,
                &BTreeSet::new(),
            )
            .await
            .unwrap();

        assert!(
            log.lock().unwrap().is_empty(),
            "capacity admission performs no shard-store work"
        );
        assert!(matches!(
            planned_member(&plan, &staged),
            MemberFold {
                participation: Participation::Staged,
                ..
            }
        ));
        assert!(matches!(
            planned_member(&plan, &rejected),
            MemberFold {
                outcome: FoldOutcome::LeafFull,
                participation: Participation::Skipped,
                ..
            }
        ));
        assert_eq!(plan.staged_ids().cloned().collect::<Vec<_>>(), vec![staged]);
        assert!(plan.entries.contains_key(b"a".as_slice()));
        assert!(!plan.entries.contains_key(b"z".as_slice()));
        assert_eq!(hints.calls.load(Ordering::SeqCst), 1);
        coord.close().await;
    }

    // A resolver that stages entries drives one CAS, receives its exact
    // precondition observation, and persists the staged entry.
    #[tokio::test]
    async fn shard_stage_is_cas_persisted() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (coord, _shards, _timeline, _bg) = coord_over(backend.clone()).await;
        let tx = TxId::with_priority(1, b"t");

        let out = coord
            .submit_shard(
                &leaf(),
                &tx,
                Arc::new(StageLock {
                    key: b"k".to_vec(),
                    tx: tx.clone(),
                    admission: StageAdmission::ExistingKeys,
                }),
                Requirement::Any,
            )
            .await
            .unwrap();
        assert!(matches!(
            out,
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Locked { .. },
                cas_precondition: Some(_),
            })
        ));
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
        let (coord, store, _timeline, _bg) = coord_over(backend.clone()).await;

        // Seed the leaf as a shrunk left half: it owns keys < "m" and links to a
        // right sibling. "z" now lives in that sibling, not here.
        let node = Node::leaf(Shard::from_entries([entry(
            b"a",
            LockType::None,
            None,
            None,
        )]))
        .with_high_key(Some(b"m".to_vec()))
        .with_right_sibling(Some("R".to_string()));
        replace_leaf_node(&store, &node).await;

        let tx = TxId::with_priority(1, b"t");
        let out = coord
            .submit_shard(
                &leaf(),
                &tx,
                Arc::new(StageLock {
                    key: b"z".to_vec(),
                    tx: tx.clone(),
                    admission: StageAdmission::ExistingKeys,
                }),
                Requirement::Any,
            )
            .await
            .unwrap();
        // Re-route: the acquire-shaped resolver's exhausted/re-route outcome is a
        // `Conflict`, which its caller turns into release-and-relock.
        assert!(matches!(
            out,
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Conflict,
                cas_precondition: None,
            })
        ));
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
        let (coord, store, _timeline, _bg) = coord_over(backend.clone()).await;

        let node = Node::leaf(Shard::new()).with_high_key(Some(b"m".to_vec()));
        replace_leaf_node(&store, &node).await;

        let tx = TxId::with_priority(1, b"t");
        let out = coord
            .submit_shard(
                &leaf(),
                &tx,
                Arc::new(StageLock {
                    key: b"a".to_vec(),
                    tx: tx.clone(),
                    admission: StageAdmission::ExistingKeys,
                }),
                Requirement::Any,
            )
            .await
            .unwrap();
        assert!(matches!(
            out,
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Locked { .. },
                cas_precondition: Some(_),
            })
        ));
        coord.close().await;

        let shard = cold_entries(&cold_store(backend), &leaf()).await;
        assert!(
            shard.lookup(b"a").is_some(),
            "an owned key is locked as usual"
        );
    }

    // A resolver that stages nothing (`Skip`) still gets its outcome delivered,
    // but receives no CAS precondition and the round issues no CAS.
    #[tokio::test]
    async fn shard_skip_delivers_outcome_without_cas() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        let (coord, _shards, _timeline, _bg) = coord_over(backend).await;
        log.lock().unwrap().clear();
        let tx = TxId::with_priority(1, b"t");

        let out = coord
            .submit_shard(&leaf(), &tx, Arc::new(SkipRelease), Requirement::Any)
            .await
            .unwrap();
        assert!(matches!(
            out,
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Released { .. },
                cas_precondition: None,
            })
        ));
        assert_eq!(shard_stores(&log), 0, "a skip stages nothing, so no CAS");
        coord.close().await;
    }

    // An entry left with no holder and no committed writer is indistinguishable
    // from absent, so the CAS that folds the round drops it (ADR-029) while
    // keeping live pointers and newly staged locks.
    #[tokio::test]
    async fn shard_prunes_vestigial_entries_on_cas() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (coord, shards, _timeline, _bg) = coord_over(backend.clone()).await;
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
                    admission: StageAdmission::ExistingKeys,
                }),
                Requirement::Any,
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
    // shard when the submitter asks for `Any` (no backend read), while a current
    // lower bound revalidates it with one conditional read.
    #[tokio::test]
    async fn any_first_attempt_reuses_cache() {
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
        let (coord, shards, timeline, _bg) = coord_over(backend.clone()).await;
        shards
            .load_leaf(&leaf_path(), Requirement::Any)
            .await
            .unwrap();

        let tx = TxId::with_priority(2, b"t");
        log.lock().unwrap().clear();
        coord
            .submit_shard(&leaf(), &tx, Arc::new(SkipRelease), Requirement::Any)
            .await
            .unwrap();
        assert_eq!(
            shard_reads(&log),
            0,
            "Any serves the cached shard with no backend read"
        );

        log.lock().unwrap().clear();
        coord
            .submit_shard(
                &leaf(),
                &tx,
                Arc::new(SkipRelease),
                Requirement::AtLeast(timeline.now()),
            )
            .await
            .unwrap();
        assert_eq!(
            shard_reads(&log),
            1,
            "a current bound revalidates the cached shard once"
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
        let (coord, _shards, _timeline, _bg) = coord_over(recorder as Arc<dyn Backend>).await;
        log.lock().unwrap().clear();

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
                Requirement::Any,
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
                Requirement::Any,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        assert!(matches!(
            driver.await.unwrap().unwrap(),
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Landed,
                ..
            })
        ));
        assert!(matches!(
            joiner.await.unwrap().unwrap(),
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Landed,
                ..
            })
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

    // ADR-051: a logless commit's staged entry is the only record that it ran, so
    // a second one on the same key must not stage in the same CAS — it would
    // erase the first's evidence inside one uncertain write. The loser is told it
    // did not land and takes the logged protocol instead.
    #[tokio::test(start_paused = true)]
    async fn one_logless_commit_per_key_stages_per_round() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (backend, gate) = Gate::wrap(mem);
        let backend = backend as Arc<dyn Backend>;
        let (coord, _shards, _timeline, _bg) = coord_over(backend.clone()).await;
        let first = TxId::with_priority(1, b"first");
        let second = TxId::with_priority(2, b"second");

        // The older member drives the round and parks in the gated load; the
        // younger one queues into that still-open batch.
        gate.arm();
        let (c1, t1) = (coord.clone(), first.clone());
        let driver = tokio::spawn(async move {
            c1.submit_shard(
                &leaf(),
                &t1,
                Arc::new(StageInline::logless(b"k", &t1, b"first")),
                Requirement::Any,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        let (c2, t2) = (coord.clone(), second.clone());
        let joiner = tokio::spawn(async move {
            c2.submit_shard(
                &leaf(),
                &t2,
                Arc::new(StageInline::logless(b"k", &t2, b"second")),
                Requirement::Any,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        assert!(matches!(
            driver.await.unwrap().unwrap(),
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Landed,
                ..
            })
        ));
        assert!(
            matches!(
                joiner.await.unwrap().unwrap(),
                Some(CoordinatedOutcome {
                    outcome: FoldOutcome::Conflict,
                    cas_precondition: None,
                })
            ),
            "the second claimant folds nothing and does not land"
        );
        coord.close().await;

        let shard = cold_entries(&cold_store(backend), &leaf()).await;
        assert_eq!(
            shard.lookup(b"k").unwrap().current.inline().map(|v| &**v),
            Some(b"first".as_slice()),
            "the first commit survives the round intact"
        );
    }

    // A logless direct-commit-shaped resolver (ADR-051): the entry it stages is
    // the only record of its commit, so it claims its key for the round and
    // classifies an abandoned round the way `DirectCommitResolver` does — the
    // ambiguity is irreducible only if its own stage rode a CAS that may have
    // landed. `replayable` models a read-modify-write, whose certified losses are
    // `Replay` rather than `Moved` (ADR-053), and makes exclusion observably
    // distinct from exhaustion.
    struct LoglessCommitProbe {
        key: Vec<u8>,
        tx: TxId,
        value: Arc<[u8]>,
        replayable: bool,
    }

    impl LoglessCommitProbe {
        fn new(key: &[u8], tx: &TxId, value: &[u8]) -> Self {
            Self {
                key: key.to_vec(),
                tx: tx.clone(),
                value: Arc::from(value),
                replayable: false,
            }
        }

        fn replayable(key: &[u8], tx: &TxId, value: &[u8]) -> Self {
            Self {
                replayable: true,
                ..Self::new(key, tx, value)
            }
        }
    }

    #[async_trait::async_trait]
    impl ShardResolver for LoglessCommitProbe {
        async fn resolve(
            &self,
            _ctx: &ResolveCtx<'_>,
            _staged: &BTreeMap<Vec<u8>, ShardEntry>,
            staged_locks: &NodeLocks,
        ) -> Result<Step, TransError> {
            let e = ShardEntry {
                current: CurrentState::Inline {
                    writer: self.tx.clone(),
                    value: self.value.clone(),
                },
                ..ShardEntry::new(self.key.clone())
            };
            Ok(Step::Stage {
                entries: vec![(self.key.clone(), e)],
                locks: staged_locks.clone(),
                admission: StageAdmission::ExistingKeys,
                outcome: FoldOutcome::Landed,
            })
        }

        fn reorderable(&self) -> bool {
            false
        }

        fn exhausted_outcome(&self, in_doubt: bool) -> FoldOutcome {
            if in_doubt {
                return FoldOutcome::InDoubt("logless commit after an uncertain CAS".into());
            }
            FoldOutcome::Moved
        }

        fn excluded_outcome(&self, in_doubt: bool) -> FoldOutcome {
            if in_doubt {
                return FoldOutcome::InDoubt("logless commit after an uncertain CAS".into());
            }
            if self.replayable {
                return FoldOutcome::Replay;
            }
            FoldOutcome::Moved
        }

        fn owned_keys(&self) -> Vec<&[u8]> {
            vec![self.key.as_slice()]
        }

        fn logless_keys(&self) -> Vec<&[u8]> {
            vec![self.key.as_slice()]
        }
    }

    // Faults the first leaf CAS as in-doubt and lets every later one through.
    fn in_doubt_then_ok(inner: Arc<dyn Backend>) -> Arc<HookBackend> {
        let backend = HookBackend::new(inner);
        let leaf_cas = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        backend.set_before(move |op| {
            let result = match op {
                BackendOp::WriteIf { path, .. }
                    if path.contains("/_n/") || path.ends_with("/_r") =>
                {
                    match leaf_cas.fetch_add(1, Ordering::SeqCst) {
                        0 => Err(glassdb_backend::BackendError::Unavailable(
                            "simulated in-doubt leaf CAS".into(),
                        )),
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

    // A member that never stages, but exposes whether the coordinator attributed
    // an earlier uncertain CAS to it through its final outcome.
    struct SkipCauseProbe;

    #[async_trait::async_trait]
    impl ShardResolver for SkipCauseProbe {
        async fn resolve(
            &self,
            ctx: &ResolveCtx<'_>,
            _staged: &BTreeMap<Vec<u8>, ShardEntry>,
            _staged_locks: &NodeLocks,
        ) -> Result<Step, TransError> {
            let in_doubt = matches!(ctx.cause, ReloadCause::Reloaded { in_doubt: true });
            let outcome = if in_doubt {
                FoldOutcome::InDoubt("uncertain CAS attributed to skipped member".into())
            } else {
                FoldOutcome::Moved
            };
            Ok(Step::Skip { outcome })
        }

        fn reorderable(&self) -> bool {
            false
        }

        fn exhausted_outcome(&self, in_doubt: bool) -> FoldOutcome {
            if in_doubt {
                FoldOutcome::InDoubt("uncertain CAS attributed to skipped member".into())
            } else {
                FoldOutcome::Moved
            }
        }
    }

    // Regression: an uncertain CAS clouds the members it carried, not the whole
    // batch. Two logless commits on one key share a round, where the second is
    // deliberately skipped; when the first's CAS comes back in-doubt and the
    // round retries, that skipped member must still learn it definitively did
    // not land. Inheriting the batch's ambiguity would surface an unresolvable
    // in-doubt for a write it never issued.
    #[tokio::test(start_paused = true)]
    async fn a_skipped_member_does_not_inherit_the_rounds_in_doubt() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (gated, gate) = Gate::wrap(mem);
        let backend = in_doubt_then_ok(gated as Arc<dyn Backend>) as Arc<dyn Backend>;
        let (coord, _shards, _timeline, _bg) = coord_over(backend.clone()).await;
        let first = TxId::with_priority(1, b"first");
        let second = TxId::with_priority(2, b"second");

        // The older member drives the round and parks in the gated load; the
        // younger one queues into that still-open batch, where its key is
        // already claimed.
        gate.arm();
        let (c1, t1) = (coord.clone(), first.clone());
        let driver = tokio::spawn(async move {
            c1.submit_shard(
                &leaf(),
                &t1,
                Arc::new(LoglessCommitProbe::new(b"k", &t1, b"first")),
                Requirement::Any,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        let (c2, t2) = (coord.clone(), second.clone());
        let joiner = tokio::spawn(async move {
            c2.submit_shard(
                &leaf(),
                &t2,
                Arc::new(LoglessCommitProbe::new(b"k", &t2, b"second")),
                Requirement::Any,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        assert!(
            matches!(
                driver.await.unwrap().unwrap(),
                Some(CoordinatedOutcome {
                    outcome: FoldOutcome::Landed,
                    ..
                })
            ),
            "the member whose CAS was retried lands on the second attempt"
        );
        assert!(
            matches!(
                joiner.await.unwrap().unwrap(),
                Some(CoordinatedOutcome {
                    outcome: FoldOutcome::Moved,
                    cas_precondition: None,
                })
            ),
            "the skipped member never staged, so its loss stays definitive"
        );
        coord.close().await;
    }

    // ADR-053: a same-key claim is reported through `excluded_outcome`, not
    // `exhausted_outcome`. The distinction is load-bearing — the claim proves the
    // excluded member folded nothing at all, while a spent CAS budget proves
    // nothing about an earlier attempt — so a read-modify-write shaped member
    // learns a *replayable* loss where an exhausted round would only tell it the
    // entry moved.
    #[tokio::test(start_paused = true)]
    async fn an_excluded_logless_member_learns_a_replayable_loss() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (backend, gate) = Gate::wrap(mem);
        let (coord, _shards, _timeline, _bg) = coord_over(backend as Arc<dyn Backend>).await;
        let first = TxId::with_priority(1, b"first");
        let second = TxId::with_priority(2, b"second");

        gate.arm();
        let (c1, t1) = (coord.clone(), first.clone());
        let driver = tokio::spawn(async move {
            c1.submit_shard(
                &leaf(),
                &t1,
                Arc::new(LoglessCommitProbe::replayable(b"k", &t1, b"first")),
                Requirement::Any,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        let (c2, t2) = (coord.clone(), second.clone());
        let joiner = tokio::spawn(async move {
            c2.submit_shard(
                &leaf(),
                &t2,
                Arc::new(LoglessCommitProbe::replayable(b"k", &t2, b"second")),
                Requirement::Any,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        assert!(matches!(
            driver.await.unwrap().unwrap(),
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Landed,
                ..
            })
        ));
        assert!(
            matches!(
                joiner.await.unwrap().unwrap(),
                Some(CoordinatedOutcome {
                    outcome: FoldOutcome::Replay,
                    cas_precondition: None,
                })
            ),
            "the excluded member folded nothing, so its loss is replayable"
        );
        coord.close().await;
    }

    // ADR-053: an excluded member does not inherit the uncertainty of a *different*
    // member's write. Even when the round's first CAS comes back in-doubt, the
    // member that was skipped for a same-key claim issued no write of its own, so
    // its own lack of durable effects still certifies a replay rather than
    // stranding it in-doubt.
    #[tokio::test(start_paused = true)]
    async fn an_excluded_replayable_member_does_not_inherit_the_rounds_in_doubt() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (gated, gate) = Gate::wrap(mem);
        let backend = in_doubt_then_ok(gated as Arc<dyn Backend>) as Arc<dyn Backend>;
        let (coord, _shards, _timeline, _bg) = coord_over(backend).await;
        let first = TxId::with_priority(1, b"first");
        let second = TxId::with_priority(2, b"second");

        gate.arm();
        let (c1, t1) = (coord.clone(), first.clone());
        let driver = tokio::spawn(async move {
            c1.submit_shard(
                &leaf(),
                &t1,
                Arc::new(LoglessCommitProbe::replayable(b"k", &t1, b"first")),
                Requirement::Any,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        let (c2, t2) = (coord.clone(), second.clone());
        let joiner = tokio::spawn(async move {
            c2.submit_shard(
                &leaf(),
                &t2,
                Arc::new(LoglessCommitProbe::replayable(b"k", &t2, b"second")),
                Requirement::Any,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        assert!(
            matches!(
                driver.await.unwrap().unwrap(),
                Some(CoordinatedOutcome {
                    outcome: FoldOutcome::Landed,
                    ..
                })
            ),
            "the member whose CAS was retried lands on the second attempt"
        );
        assert!(
            matches!(
                joiner.await.unwrap().unwrap(),
                Some(CoordinatedOutcome {
                    outcome: FoldOutcome::Replay,
                    cas_precondition: None,
                })
            ),
            "the skipped member issued no write, so it replays rather than doubting"
        );
        coord.close().await;
    }

    // Regression (fuzz `concurrent_tx`,
    // corpus/cd4e97be8a631c59fe32bc49de539f38056bcb40): one transaction can have
    // two operations in flight on the same leaf at once — GC releasing a
    // presumed-dead transaction's holds (ADR-029) while that transaction's own
    // acquire is still resolving on the same object (ADR-025). Both submissions
    // carry their own outcome slot, but a fold round runs one resolver per id and
    // the dedup delivers to every merged submission; merging them would collapse
    // the two slots into one and leave the loser a delivered-but-empty slot. The
    // coordinator must instead serialize same-id submissions into separate rounds
    // so each gets its own outcome rather than panicking.
    #[tokio::test(start_paused = true)]
    async fn same_tx_concurrent_submits_each_get_an_outcome() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (backend, gate) = Gate::wrap(mem);
        let (coord, _shards, _timeline, _bg) = coord_over(backend as Arc<dyn Backend>).await;
        let tx = TxId::with_priority(1, b"t");

        // The acquire submits first, becomes the dedup driver, and parks in the
        // gated load; the release for the same id then arrives for the same leaf.
        gate.arm();
        let (c1, t1) = (coord.clone(), tx.clone());
        let acquire = tokio::spawn(async move {
            c1.submit_shard(
                &leaf(),
                &t1,
                Arc::new(StageLock {
                    key: b"k".to_vec(),
                    tx: t1.clone(),
                    admission: StageAdmission::ExistingKeys,
                }),
                Requirement::Any,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;

        let (c2, t2) = (coord.clone(), tx.clone());
        let release = tokio::spawn(async move {
            c2.submit_shard(&leaf(), &t2, Arc::new(SkipRelease), Requirement::Any)
                .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        // Neither submission may be left with an empty slot: both resolve to
        // their own outcome (the merge was declined, so they ran in separate
        // rounds instead of collapsing).
        assert!(matches!(
            acquire.await.unwrap().unwrap(),
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Locked { .. },
                ..
            })
        ));
        assert!(matches!(
            release.await.unwrap().unwrap(),
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Released { .. },
                ..
            })
        ));
        coord.close().await;
    }

    // Capacity is a member-local result: a create that crosses the reserved
    // content limit is rejected and re-hinted, while an overwrite already
    // staged in the same merged round still lands. Existing-key mutations may
    // consume the reserved headroom, but the absolute object limit still holds.
    #[tokio::test(start_paused = true)]
    async fn leaf_full_create_does_not_poison_merged_overwrite() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (backend, gate) = Gate::wrap(mem);
        let recorder = Arc::new(RecordingBackend::new(backend));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = recorder;

        let writer = TxId::with_priority(0, b"writer");
        let old = TxId::with_priority(1, b"old");
        let young = TxId::with_priority(2, b"young");
        let seed = entry(b"a", LockType::None, None, Some(&writer));
        let mut overwritten = seed.clone();
        overwritten.lock_type = LockType::Write;
        overwritten.locked_by = vec![old.clone()];
        let created = entry(b"z", LockType::Create, Some(&young), None);

        let base_len = Node::leaf(Shard::from_entries([seed.clone()])).content_encoded_len();
        let overwrite_len =
            Node::leaf(Shard::from_entries([overwritten.clone()])).content_encoded_len();
        let full_node = Node::leaf(Shard::from_entries([overwritten, created]));
        let content_limit = overwrite_len - 1;
        assert!(base_len <= content_limit);
        assert!(overwrite_len > content_limit);
        assert!(full_node.content_encoded_len() > content_limit);

        let node_max_bytes = full_node.encoded_len() + 64;
        let policy = SplitPolicy {
            node_max_bytes,
            split_headroom_bytes: node_max_bytes - content_limit,
            ..SplitPolicy::default()
        };
        let hints = Arc::new(HintCounter::default());
        let (coord, shards, _timeline, _bg) =
            coord_over_with(backend.clone(), policy, hints.clone()).await;
        store_shard_entries(&shards, &leaf(), vec![seed]).await;
        log.lock().unwrap().clear();

        gate.arm();
        let (c1, t1) = (coord.clone(), old.clone());
        let overwrite = tokio::spawn(async move {
            c1.submit_shard(
                &leaf(),
                &t1,
                Arc::new(StageLock {
                    key: b"a".to_vec(),
                    tx: t1.clone(),
                    admission: StageAdmission::ExistingKeys,
                }),
                Requirement::Any,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;

        let (c2, t2) = (coord.clone(), young.clone());
        let create = tokio::spawn(async move {
            c2.submit_shard(
                &leaf(),
                &t2,
                Arc::new(StageLock {
                    key: b"z".to_vec(),
                    tx: t2.clone(),
                    admission: StageAdmission::AddsKey,
                }),
                Requirement::Any,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        assert!(matches!(
            overwrite.await.unwrap().unwrap(),
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Locked { .. },
                cas_precondition: Some(_),
            })
        ));
        assert!(matches!(
            create.await.unwrap().unwrap(),
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::LeafFull,
                cas_precondition: None,
            })
        ));
        assert_eq!(shard_stores(&log), 1, "the admitted member still lands");
        assert_eq!(
            hints.calls.load(Ordering::SeqCst),
            2,
            "one hint follows the admitted store and one re-hints the rejected create"
        );
        coord.close().await;

        let shard = cold_entries(&cold_store(backend), &leaf()).await;
        assert_eq!(shard.lookup(b"a").unwrap().locked_by, vec![old]);
        assert!(
            shard.lookup(b"z").is_none(),
            "the full create was not staged"
        );
    }

    // Publishes `key`'s current value as a logless commit marker (ADR-051).
    struct StageInline {
        key: Vec<u8>,
        tx: TxId,
        value: Arc<[u8]>,
    }

    impl StageInline {
        fn logless(key: &[u8], tx: &TxId, value: &[u8]) -> Self {
            Self {
                key: key.to_vec(),
                tx: tx.clone(),
                value: Arc::from(value),
            }
        }
    }

    #[async_trait]
    impl ShardResolver for StageInline {
        async fn resolve(
            &self,
            _ctx: &ResolveCtx<'_>,
            _staged: &BTreeMap<Vec<u8>, ShardEntry>,
            staged_locks: &NodeLocks,
        ) -> Result<Step, TransError> {
            let e = ShardEntry {
                current: CurrentState::Inline {
                    writer: self.tx.clone(),
                    value: self.value.clone(),
                },
                ..ShardEntry::new(self.key.clone())
            };
            Ok(Step::Stage {
                entries: vec![(self.key.clone(), e)],
                locks: staged_locks.clone(),
                admission: StageAdmission::InlinePublication,
                outcome: FoldOutcome::Landed,
            })
        }

        fn reorderable(&self) -> bool {
            true
        }

        fn exhausted_outcome(&self, _in_doubt: bool) -> FoldOutcome {
            FoldOutcome::Conflict
        }

        fn logless_keys(&self) -> Vec<&[u8]> {
            vec![self.key.as_slice()]
        }
    }

    // A policy whose hard cap admits an external pointer for `key` but not the
    // same entry carrying `value` inline.
    fn policy_rejecting_inline(key: &[u8], tx: &TxId, value: &[u8]) -> SplitPolicy {
        let external = ShardEntry {
            current: CurrentState::External { writer: tx.clone() },
            ..ShardEntry::new(key)
        };
        let inline = ShardEntry {
            current: CurrentState::Inline {
                writer: tx.clone(),
                value: Arc::from(value),
            },
            ..ShardEntry::new(key)
        };
        let external_len = Node::leaf(Shard::from_entries([external])).encoded_len();
        let inline_len = Node::leaf(Shard::from_entries([inline])).encoded_len();
        assert!(
            inline_len > external_len,
            "the inline payload must add bytes"
        );
        SplitPolicy {
            node_max_bytes: external_len,
            split_headroom_bytes: 0,
            ..SplitPolicy::default()
        }
    }

    // A logless commit's leaf entry is the value's only copy, so an over-cap
    // stage must be rejected rather than silently losing the value.
    #[tokio::test]
    async fn an_oversized_logless_inline_payload_is_rejected() {
        let tx = TxId::with_priority(1, b"t");
        let value = b"a-value-that-does-not-fit";
        let policy = policy_rejecting_inline(b"k", &tx, value);
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (coord, _shards, _timeline, _bg) =
            coord_over_with(backend.clone(), policy, Arc::new(NoSplitHints)).await;

        let outcome = coord
            .submit_shard(
                &leaf(),
                &tx,
                Arc::new(StageInline::logless(b"k", &tx, value)),
                Requirement::Any,
            )
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Conflict,
                cas_precondition: None,
            })
        ));
        coord.close().await;

        let shard = cold_entries(&cold_store(backend), &leaf()).await;
        assert!(shard.lookup(b"k").is_none(), "nothing was written");
    }

    // An inline entry may fit the physical object while still consuming more
    // than its half of the content budget. Publishing it would let a later
    // accepted key strand this leaf as an unsplittable singleton, so the direct
    // attempt must fall back without issuing a futile split hint.
    #[tokio::test]
    async fn a_logless_inline_entry_must_preserve_the_split_budget() {
        let tx = TxId::with_priority(1, b"t");
        let value = b"inline";
        let inline = ShardEntry {
            current: CurrentState::Inline {
                writer: tx.clone(),
                value: Arc::from(value.as_slice()),
            },
            ..ShardEntry::new(b"k")
        };
        let entry_len = Node::leaf(Shard::from_entries([inline.clone()])).content_encoded_len();
        let policy = SplitPolicy {
            node_max_bytes: entry_len * 2 + 64,
            split_headroom_bytes: 65,
            ..SplitPolicy::default()
        };
        assert!(Node::leaf(Shard::from_entries([inline])).encoded_len() <= policy.node_max_bytes);
        assert!(!policy.entry_fits_split_budget(&ShardEntry {
            current: CurrentState::Inline {
                writer: tx.clone(),
                value: Arc::from(value.as_slice()),
            },
            ..ShardEntry::new(b"k")
        }));

        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let hints = Arc::new(HintCounter::default());
        let (coord, _shards, _timeline, _bg) =
            coord_over_with(backend.clone(), policy, hints.clone()).await;
        let outcome = coord
            .submit_shard(
                &leaf(),
                &tx,
                Arc::new(StageInline::logless(b"k", &tx, value)),
                Requirement::Any,
            )
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Conflict,
                cas_precondition: None,
            })
        ));
        assert_eq!(hints.calls.load(Ordering::SeqCst), 0);
        coord.close().await;

        let shard = cold_entries(&cold_store(backend), &leaf()).await;
        assert!(shard.lookup(b"k").is_none(), "nothing was written");
    }

    struct CapacityAfterInDoubt {
        key: Vec<u8>,
        tx: TxId,
        folds: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl ShardResolver for CapacityAfterInDoubt {
        async fn resolve(
            &self,
            _ctx: &ResolveCtx<'_>,
            _staged: &BTreeMap<Vec<u8>, ShardEntry>,
            staged_locks: &NodeLocks,
        ) -> Result<Step, TransError> {
            let value: Arc<[u8]> = if self.folds.fetch_add(1, Ordering::SeqCst) == 0 {
                Arc::from(b"x".as_slice())
            } else {
                Arc::from(vec![b'x'; 128])
            };
            let entry = ShardEntry {
                current: CurrentState::Inline {
                    writer: self.tx.clone(),
                    value,
                },
                ..ShardEntry::new(self.key.clone())
            };
            Ok(Step::Stage {
                entries: vec![(self.key.clone(), entry)],
                locks: staged_locks.clone(),
                admission: StageAdmission::ExistingKeys,
                outcome: FoldOutcome::Landed,
            })
        }

        fn reorderable(&self) -> bool {
            false
        }

        fn exhausted_outcome(&self, in_doubt: bool) -> FoldOutcome {
            if in_doubt {
                FoldOutcome::InDoubt("capacity changed after in-doubt CAS".into())
            } else {
                FoldOutcome::Moved
            }
        }
    }

    // Capacity rejection must preserve uncertainty only for the member carried
    // by the failed CAS; clouding a co-batched member that never staged would
    // manufacture ambiguity for a write it never issued.
    #[tokio::test(start_paused = true)]
    async fn capacity_rejection_after_in_doubt_preserves_uncertainty() {
        let tx = TxId::with_priority(1, b"t");
        let skipped = TxId::with_priority(2, b"skipped");
        let small = ShardEntry {
            current: CurrentState::Inline {
                writer: tx.clone(),
                value: Arc::from(b"x".as_slice()),
            },
            ..ShardEntry::new(b"k")
        };
        let large = ShardEntry {
            current: CurrentState::Inline {
                writer: tx.clone(),
                value: Arc::from(vec![b'x'; 128]),
            },
            ..ShardEntry::new(b"k")
        };
        let small_len = Node::leaf(Shard::from_entries([small])).encoded_len();
        let large_len = Node::leaf(Shard::from_entries([large])).encoded_len();
        assert!(large_len > small_len);
        let policy = SplitPolicy {
            node_max_bytes: small_len,
            split_headroom_bytes: 0,
            ..SplitPolicy::default()
        };

        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (gated, gate) = Gate::wrap(mem);
        let hooked = Arc::new(HookBackend::new(gated as Arc<dyn Backend>));
        let backend: Arc<dyn Backend> = hooked.clone();
        let (coord, _shards, _timeline, _bg) =
            coord_over_with(backend, policy, Arc::new(NoSplitHints)).await;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        hooked.set_before({
            let calls = calls.clone();
            move |op| {
                let result = match op {
                    BackendOp::WriteIf { path, .. }
                        if (path.contains("/_n/") || path.ends_with("/_r"))
                            && calls.fetch_add(1, Ordering::SeqCst) == 0 =>
                    {
                        Err(glassdb_backend::BackendError::Unavailable(
                            "simulated in-doubt leaf CAS".into(),
                        ))
                    }
                    _ => Ok(()),
                };
                let future: HookFuture = Box::pin(async move { result });
                future
            }
        });

        gate.arm();
        let (driver_coord, driver_tx) = (coord.clone(), tx.clone());
        let driver = tokio::spawn(async move {
            driver_coord
                .submit_shard(
                    &leaf(),
                    &driver_tx,
                    Arc::new(CapacityAfterInDoubt {
                        key: b"k".to_vec(),
                        tx: driver_tx.clone(),
                        folds: std::sync::atomic::AtomicUsize::new(0),
                    }),
                    Requirement::Any,
                )
                .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        let (joiner_coord, joiner_tx) = (coord.clone(), skipped.clone());
        let joiner = tokio::spawn(async move {
            joiner_coord
                .submit_shard(
                    &leaf(),
                    &joiner_tx,
                    Arc::new(SkipCauseProbe),
                    Requirement::Any,
                )
                .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        let outcome = driver.await.unwrap().unwrap();
        let skipped_outcome = joiner.await.unwrap().unwrap();
        assert!(matches!(
            outcome,
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::InDoubt(_),
                cas_precondition: None,
            })
        ));
        assert!(matches!(
            skipped_outcome,
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Moved,
                cas_precondition: None,
            })
        ));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the oversized retry did not CAS"
        );
        coord.close().await;
    }

    // A submit after shutdown is a cancelled no-op (`Ok(None)`), so best-effort
    // callers treat it as done and acquirers can distinguish it.
    #[tokio::test]
    async fn submit_after_close_is_cancelled() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (coord, _shards, _timeline, _bg) = coord_over(backend).await;
        coord.close().await;

        let tx = TxId::with_priority(1, b"t");
        let out = coord
            .submit_shard(&leaf(), &tx, Arc::new(SkipRelease), Requirement::Any)
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
                    if path.contains("/_n/") || path.ends_with("/_r") =>
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

    // A commit-shaped resolver: it stages a mutation (issuing a CAS) on its first
    // two folds, then on the third fold classifies its lost race the same way
    // `DirectCommitResolver` does — `InDoubt` if any earlier CAS this round was
    // in-doubt, else a definitive `Moved`. Records the deciding fold's
    // `in_doubt` cause so the test can pin the coordinator's state machine.
    struct StickyCommitProbe {
        key: Vec<u8>,
        tx: TxId,
        folds: std::sync::atomic::AtomicUsize,
        seen_in_doubt: Arc<Mutex<Option<bool>>>,
    }

    #[async_trait::async_trait]
    impl ShardResolver for StickyCommitProbe {
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
                    admission: StageAdmission::ExistingKeys,
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

        fn exhausted_outcome(&self, in_doubt: bool) -> FoldOutcome {
            if in_doubt {
                FoldOutcome::InDoubt("round abandoned after in-doubt CAS".into())
            } else {
                FoldOutcome::Moved
            }
        }

        fn owned_keys(&self) -> Vec<&[u8]> {
            vec![self.key.as_slice()]
        }
    }

    // Regression (logless commit double-apply): once any CAS in a round comes
    // back in-doubt, its write may have landed durably and been help-forwarded to
    // a peer, so the in-doubt classification must stay *sticky* across a later
    // precondition-miss. Otherwise a commit that landed-but-unacked and was then
    // superseded is misclassified `Moved`, and its caller abandons and re-runs a
    // non-idempotent write a peer already observed — breaking the
    // `final <= started` serializability bound.
    //
    // This pins the coordinator half of the fix in isolation, including exact
    // attribution alongside a co-batched member that never staged. The
    // *end-to-end* manifestation (a real commit being abandoned and
    // double-applying under the true 3-way co-batched interleaving) is covered
    // deterministically by the committed fuzz reproducer
    // `fuzz/corpus/concurrent_tx/crash-95084997…`, which the corpus-replay test
    // (`crates/glassdb/tests/fuzz_corpus.rs`) replays through the sim scheduler.
    // That interleaving cannot be forced by the plain-tokio in-doubt harness
    // (`crates/glassdb/tests/in_doubt.rs`), whose 2-step lost-ack→moved case
    // classifies in-doubt without ever hitting the resetting precondition-miss.
    #[tokio::test(start_paused = true)]
    async fn in_doubt_cas_stays_in_doubt_across_a_later_precondition_miss() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        // The leaf must exist so the round's CAS is a `write_if` (the faulted op),
        // not a create.
        let seed = TxId::with_priority(1, b"seed");
        store_shard_entries(
            &cold_store(mem.clone()),
            &leaf(),
            vec![entry(b"seed", LockType::None, None, Some(&seed))],
        )
        .await;
        let (gated, gate) = Gate::wrap(mem);
        let backend: Arc<dyn Backend> = in_doubt_then_miss(gated as Arc<dyn Backend>);
        let (coord, _shards, _timeline, _bg) = coord_over(backend).await;

        let tx = TxId::with_priority(2, b"install");
        let skipped = TxId::with_priority(3, b"skipped");
        let seen_in_doubt = Arc::new(Mutex::new(None));
        gate.arm();
        let (driver_coord, driver_tx, driver_seen) =
            (coord.clone(), tx.clone(), seen_in_doubt.clone());
        let driver = tokio::spawn(async move {
            driver_coord
                .submit_shard(
                    &leaf(),
                    &driver_tx,
                    Arc::new(StickyCommitProbe {
                        key: b"k".to_vec(),
                        tx: driver_tx.clone(),
                        folds: std::sync::atomic::AtomicUsize::new(0),
                        seen_in_doubt: driver_seen,
                    }),
                    Requirement::Any,
                )
                .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        let (joiner_coord, joiner_tx) = (coord.clone(), skipped.clone());
        let joiner = tokio::spawn(async move {
            joiner_coord
                .submit_shard(
                    &leaf(),
                    &joiner_tx,
                    Arc::new(SkipCauseProbe),
                    Requirement::Any,
                )
                .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        let out = driver.await.unwrap().unwrap();
        let skipped_outcome = joiner.await.unwrap().unwrap();

        assert_eq!(
            *seen_in_doubt.lock().unwrap(),
            Some(true),
            "the precondition-miss after an in-doubt CAS must keep the cause in-doubt"
        );
        assert!(
            matches!(
                out,
                Some(CoordinatedOutcome {
                    outcome: FoldOutcome::InDoubt(_),
                    ..
                })
            ),
            "a landed-but-unacked CAS that is then superseded must classify InDoubt, \
             not Moved (else the caller abandons and double-applies)"
        );
        assert!(
            matches!(
                skipped_outcome,
                Some(CoordinatedOutcome {
                    outcome: FoldOutcome::Moved,
                    cas_precondition: None,
                })
            ),
            "the member that never staged must not inherit either failed CAS"
        );
        coord.close().await;
    }

    // A commit-shaped resolver that keeps staging until the round's retry budget
    // is exhausted.
    struct AlwaysStageProbe {
        key: Vec<u8>,
        tx: TxId,
    }

    #[async_trait::async_trait]
    impl ShardResolver for AlwaysStageProbe {
        async fn resolve(
            &self,
            _ctx: &ResolveCtx<'_>,
            _staged: &BTreeMap<Vec<u8>, ShardEntry>,
            staged_locks: &NodeLocks,
        ) -> Result<Step, TransError> {
            Ok(Step::Stage {
                entries: vec![(
                    self.key.clone(),
                    entry(&self.key, LockType::Write, Some(&self.tx), None),
                )],
                locks: staged_locks.clone(),
                admission: StageAdmission::ExistingKeys,
                outcome: FoldOutcome::Landed,
            })
        }

        fn reorderable(&self) -> bool {
            false
        }

        fn exhausted_outcome(&self, in_doubt: bool) -> FoldOutcome {
            if in_doubt {
                FoldOutcome::InDoubt("round abandoned after in-doubt CAS".into())
            } else {
                FoldOutcome::Moved
            }
        }

        fn owned_keys(&self) -> Vec<&[u8]> {
            vec![self.key.as_slice()]
        }
    }

    // The first CAS becomes in-doubt and every subsequent CAS misses, driving
    // the coordinator through its exhaustion exit rather than a resolver exit.
    fn in_doubt_then_miss_forever(inner: Arc<dyn Backend>) -> Arc<HookBackend> {
        let backend = HookBackend::new(inner);
        let leaf_cas = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        backend.set_before(move |op| {
            let result = match op {
                BackendOp::WriteIf { path, .. }
                    if path.contains("/_n/") || path.ends_with("/_r") =>
                {
                    match leaf_cas.fetch_add(1, Ordering::SeqCst) {
                        0 => Err(glassdb_backend::BackendError::Unavailable(
                            "simulated in-doubt leaf CAS".into(),
                        )),
                        _ => Err(glassdb_backend::BackendError::Precondition),
                    }
                }
                _ => Ok(()),
            };
            let future: HookFuture = Box::pin(async move { result });
            future
        });
        backend
    }

    // Regression: exhausting the retry budget must not turn a possibly-landed
    // commit CAS into `Moved`, which would permit a non-idempotent retry.
    #[tokio::test]
    async fn exhausted_budget_after_in_doubt_cas_stays_in_doubt() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let seed = TxId::with_priority(1, b"seed");
        store_shard_entries(
            &cold_store(mem.clone()),
            &leaf(),
            vec![entry(b"seed", LockType::None, None, Some(&seed))],
        )
        .await;
        let backend: Arc<dyn Backend> = in_doubt_then_miss_forever(mem);
        let (coord, _shards, _timeline, _bg) = coord_over_fast(backend).await;

        let tx = TxId::with_priority(2, b"install");
        let out = coord
            .submit_shard(
                &leaf(),
                &tx,
                Arc::new(AlwaysStageProbe {
                    key: b"k".to_vec(),
                    tx: tx.clone(),
                }),
                Requirement::Any,
            )
            .await
            .unwrap();
        coord.close().await;

        assert!(
            matches!(
                out,
                Some(CoordinatedOutcome {
                    outcome: FoldOutcome::InDoubt(_),
                    ..
                })
            ),
            "exhaustion after an in-doubt CAS must preserve uncertainty"
        );
    }
}
