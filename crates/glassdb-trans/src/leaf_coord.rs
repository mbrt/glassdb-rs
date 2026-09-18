//! The leaf-mutation coordinator (ADR-028): the transaction-aware shared
//! mutation engine through which every leaf entry mutation flows.
//!
//! The only coordination primitive is a content compare-and-swap on a B-link
//! leaf: a node (`{prefix}/_n/<token>`) or the collection root (`{prefix}/_r`,
//! the root leaf while the collection is small, ADR-031). Concurrent
//! transactions contending one object are **deduplicated** (ADR-025/026): each
//! per-object mutation is submitted to a [`Dedup`] keyed on the object path, so
//! several transactions merge into one owner-driven load + CAS. N GET+CAS
//! round-trips collapse to one; the [`Dedup`] fans out one shared result, so
//! each transaction's own outcome ([`MemberOutcome`]) travels back through a
//! per-submission slot the caller reads once its submission resolves.
//!
//! The coordinator owns the cross-operation protocol required to combine
//! heterogeneous mutations safely: transaction identity, oldest-first member
//! order, per-member in-doubt attribution, routing and capacity admission, and
//! same-key exclusion for logless publication. Each attempt loads the leaf,
//! builds a mutation plan from the round's installed [`LeafOperation`] resolvers,
//! and persists staged changes with one CAS after removing vestigial entries.
//! Recovery reloads the leaf and rebuilds the plan before the coordinator
//! delivers each member's outcome (ADR-029). Each policy owner packages its
//! mutation decision and typed result in a `LeafOperation`:
//! [`Locker`](crate::tlocker::Locker) supplies acquire / write-back / release,
//! direct commit supplies atomic logless publication, and the tree rebalancer supplies
//! leaf structural-gate acquisition. Cross-leaf strategy stays with the
//! `Locker`, not in the engine.

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
    CasReceipt, CasResult, CurrentnessBarrier, LeafBody, LeafEdit, LeafEntry, LeafObservation,
    LeafObservationCheck, LockType, Node, NodeLocks, NodeStore, Requirement, SplitPolicy,
    StorageError,
};

use crate::error::TransError;
use crate::key_state_resolver::KeyStateResolver;
use crate::monitor::Monitor;
use crate::node_locking::StructuralGateRetry;

/// Maximum inner CAS retries on a single leaf/root before treating the
/// operation as conflicted and restarting the transaction.
pub(crate) const CAS_RETRIES: usize = 50;

/// Counters for CAS activity across all coordinated leaf operations.
#[derive(Default)]
struct Stats {
    n_retries: AtomicU64,
}

/// Coordination work for one snapshot or accumulated interval.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LeafCoordinatorStats {
    pub submissions: u64,
    pub rounds: u64,
    pub cas_retries: u64,
}

impl AddAssign for LeafCoordinatorStats {
    fn add_assign(&mut self, rhs: Self) {
        self.submissions += rhs.submissions;
        self.rounds += rhs.rounds;
        self.cas_retries += rhs.cas_retries;
    }
}

impl Sub for LeafCoordinatorStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            submissions: self.submissions.saturating_sub(rhs.submissions),
            rounds: self.rounds.saturating_sub(rhs.rounds),
            cas_retries: self.cas_retries.saturating_sub(rhs.cas_retries),
        }
    }
}

/// The policy outcome for one round member (ADR-028).
/// An outcome proposed with staged changes requires a successful CAS before
/// delivery. A skipped member's outcome can also depend on earlier staged
/// changes. [`CoordinatedOutcome`] pairs the delivered outcome with its evidence.
#[derive(Clone, Debug)]
pub(crate) enum MemberOutcome {
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
    /// `superseded` carries the `current_writer` transaction identities a write-back
    /// overwrote — GC reverse-check candidates (ADR-022); empty for a release.
    Released { superseded: Vec<TxId> },
    /// The submitted leaf no longer covers one of this operation's keys. The
    /// caller must descend again and regroup before retrying.
    Reroute,
    /// A logless direct commit landed: this transaction's value is published in
    /// the leaf's version chain, or it was already there (idempotent, ADR-051).
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
    /// A commit-critical CAS was in-doubt (`Unavailable`) and resolver evaluation
    /// could not prove whether it landed, so the commit may or may not have happened:
    /// the one irreducible ambiguity, surfaced rather than risking a
    /// double-apply.
    InDoubt(String),
}

/// The evidence that supports one coordinated outcome.
pub(crate) enum CoordinationEvidence {
    /// The member participated in the successful CAS that installed this state.
    Installed(CasReceipt<Node>),
    /// The loaded state retained by a member that staged no change. Its outcome
    /// can also depend on another member's changes in the mutation plan.
    Observed(LeafObservation),
}

impl CoordinationEvidence {
    /// Retains the exact observed state without the member's mutation proof.
    pub(crate) fn into_observation(self) -> LeafObservation {
        match self {
            Self::Installed(receipt) => receipt.into_installed(),
            Self::Observed(observation) => observation,
        }
    }

    /// Reports whether this round confirmed the retained leaf state at or after
    /// the validation barrier.
    pub(crate) fn validates(
        &self,
        observed: &LeafObservation,
        barrier: CurrentnessBarrier,
    ) -> bool {
        match self {
            Self::Installed(receipt) => receipt.confirms_expected(observed, barrier),
            Self::Observed(current) => {
                current.is_current_after(barrier) && current.same_state(observed)
            }
        }
    }
}

/// One member's policy outcome and the physical evidence from its round.
pub(crate) struct CoordinatedOutcome {
    pub(crate) outcome: MemberOutcome,
    pub(crate) evidence: Option<CoordinationEvidence>,
}

/// Whether a resolver is evaluated on the first attempt or after a reload.
/// Reloads can follow a CAS conflict, an uncertain CAS, or a stale transaction
/// dependency. `in_doubt` records unresolved uncertainty only for this member
/// from a prior CAS that included its staged changes.
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
    InlinePublication {
        /// Whether the publication creates at least one live user key and must
        /// therefore preserve the content headroom used by structural work.
        adds_key: bool,
        /// Whether a rejected publication should notify the tree rebalancer. ADR-061
        /// suppresses this for multi-key direct members because splitting can
        /// destroy their eligibility.
        pressure_hint: bool,
    },
    /// The stage adds at least one user key and must fit below the content limit
    /// that reserves headroom for locks and the split's shrink CAS.
    AddsKey,
}

/// One resolver's proposed decision: either stage entry and node-lock changes
/// alongside its member outcome, or stage nothing.
pub(crate) enum Step {
    /// Apply these entry changes and replace the running node-lock state. The
    /// coordinator alone owns the node's topology, body reconstruction, and
    /// capacity admission.
    Stage {
        entries: Vec<(Vec<u8>, LeafEntry)>,
        locks: NodeLocks,
        admission: StageAdmission,
        outcome: MemberOutcome,
    },
    /// Propose no changes. Delivery still waits for successful persistence if
    /// another member staged changes, because this outcome can depend on them.
    /// A logless member that reports `Landed` also protects its existing markers
    /// from later publishers in this mutation plan.
    Skip { outcome: MemberOutcome },
}

impl Step {
    fn outcome(&self) -> &MemberOutcome {
        match self {
            Step::Stage { outcome, .. } | Step::Skip { outcome } => outcome,
        }
    }
}

/// Transaction-state services and retry context for one resolver evaluation.
pub(crate) struct ResolveCtx<'a> {
    pub(crate) key_state: &'a KeyStateResolver,
    pub(crate) tmon: &'a Monitor,
    pub(crate) gate_retry: &'a StructuralGateRetry,
    /// The combined bound for dependent reads and eventual leaf evidence. The
    /// loaded and staged entries do not necessarily satisfy it yet.
    pub(crate) requirement: Requirement,
    pub(crate) cause: ReloadCause,
}

/// The policy for one round member's mutation decision on a staged leaf state.
/// Resolver implementations own the acquire, write-back, release, and
/// direct-commit decisions; the coordinator
/// owns the ordering, admission, and recovery contract they share (ADR-028).
#[async_trait]
pub(crate) trait LeafResolver: Send + Sync {
    /// Lets a resolver retain evidence from the leaf exactly as loaded, before
    /// any earlier-ordered member stages over it. Direct commit uses this to
    /// remember an exact own marker; other resolvers need no initial leaf state.
    fn observe_loaded(&self, _entries: &BTreeMap<Vec<u8>, LeafEntry>) {}

    /// Resolves this member against entries and node locks as currently staged
    /// this round. Resolvers cannot mutate node topology.
    ///
    /// Use `ctx.requirement` for dependent object reads. The leaf state can
    /// predate that bound; the coordinator confirms it by CAS or a currentness
    /// check before delivery. A changed state discards the plan and repeats
    /// resolution. Retained facts must remain valid when a plan is discarded.
    ///
    /// When `ctx.cause` carries unresolved uncertainty, returning `InDoubt`
    /// preserves it. Any other decision certifies that the resolver reconciled
    /// the earlier CAS; in particular, a new stage must already be safe to
    /// apply zero or one additional time. That reconciliation must remain valid
    /// even if this plan is discarded or its CAS conflicts.
    async fn resolve(
        &self,
        ctx: &ResolveCtx<'_>,
        staged: &BTreeMap<Vec<u8>, LeafEntry>,
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
    /// uncertainty while ending the round.
    fn exhausted_outcome(&self, in_doubt: bool) -> MemberOutcome;

    /// The outcome delivered when a structural change invalidated routing.
    fn reroute_outcome(&self, in_doubt: bool) -> MemberOutcome {
        self.exhausted_outcome(in_doubt)
    }

    /// The outcome delivered when a peer already claimed one of this member's
    /// [`publication_keys`](LeafResolver::publication_keys) as a logless
    /// publication this attempt, so this member staged nothing. Distinct
    /// from exhaustion: the peer's claim proves this member staged nothing,
    /// which a spent CAS budget does not, so a resolver may treat it as a
    /// certified loss rather than an unknown one (ADR-053). `in_doubt` still
    /// reports whether an *earlier* attempt of this round carried this member's
    /// own stage.
    fn excluded_outcome(&self, in_doubt: bool) -> MemberOutcome {
        self.exhausted_outcome(in_doubt)
    }

    /// The raw keys defining this member's leaf-local scope. The coordinator
    /// verifies that the loaded leaf still covers every key before evaluation
    /// (ADR-031). This includes read-only dependencies when their placement
    /// matters. A resolver whose decision is valid for the leaf as a whole may
    /// leave the scope empty.
    fn leaf_scope_keys(&self) -> Vec<&[u8]> {
        Vec::new()
    }

    /// The raw keys whose current committed state this member may replace.
    /// Once a logless member claims a key, any later publisher intersecting it
    /// is excluded as a whole. Lock-only and release-only mutations leave this
    /// empty because they preserve current-state markers.
    fn publication_keys(&self) -> Vec<&[u8]> {
        self.logless_publication_keys()
    }

    /// The [`publication_keys`](LeafResolver::publication_keys) this member
    /// commits loglessly (ADR-051): their leaf state is the commit's only durable
    /// record, so no later publisher may stage over them in the same CAS. The
    /// coordinator lets at most one member stage per key per round and tells the
    /// rest they did not land. Disjoint keys still share a round. The default is
    /// empty: a member backed by a transaction object records its commit outside
    /// the leaf and needs no exclusivity.
    fn logless_publication_keys(&self) -> Vec<&[u8]> {
        Vec::new()
    }
}

/// One complete operation submitted to the shared leaf-mutation engine.
///
/// The operation owns its target, transaction identity, freshness requirement,
/// resolver policy, and typed result. The coordinator runs the shared mutation
/// mechanism and returns the raw round result to the operation for translation.
pub(crate) trait LeafOperation: LeafResolver {
    /// The result vocabulary exposed to this operation's caller.
    type Output;

    /// Returns the leaf object this operation mutates.
    fn path(&self) -> &ObjectPath;

    /// Returns the transaction identity used to order this operation.
    fn id(&self) -> &TxId;

    /// Returns the bound for dependent reads and completed leaf evidence.
    /// This requirement is retained across joined attempts and retries.
    fn requirement(&self) -> Requirement;

    /// Translates the shared round result into this operation's result.
    fn complete(&self, outcome: Option<CoordinatedOutcome>) -> Result<Self::Output, TransError>;
}

/// One transaction's participation in a leaf CAS batch: its installed resolver
/// and where to deliver its outcome.
#[derive(Clone)]
struct LeafMember {
    resolver: Arc<dyn LeafResolver>,
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
/// resolver and outcome slot. `requirement` combines the members' bounds for
/// dependent reads and completion checks. The first attempt can reuse any
/// cached leaf as a CAS precondition. Failed mutations invalidate their seed;
/// retries retain the requirement and use the winner or newer shared knowledge
/// only when it meets that bound.
#[derive(Clone)]
struct CasReq {
    path: ObjectPath,
    members: BTreeMap<TxId, LeafMember>,
    requirement: Requirement,
}

impl MergeRequest for CasReq {
    fn merge(&self, other: &Self) -> Option<Self> {
        // One transaction can have several operations in flight on the same leaf
        // at once — e.g. GC releasing a presumed-dead transaction's holds
        // (ADR-029) while that transaction's own acquire is still resolving on
        // the same object (ADR-025). Each submission carries its own outcome
        // slot, but a coordinator round runs at most one resolver per transaction identity
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
        // even same-key conflicting writers share a single load + CAS. Planning
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
            requirement: self.requirement.stricter(other.requirement),
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
/// seam — never on the tree rebalancer's queue or policy. The tree rebalancer supplies the
/// implementation.
pub trait SplitHinter: Send + Sync {
    /// Notes that `path`'s leaf was just stored holding `leaf`. Best-effort: a
    /// spurious call only costs the tree rebalancer a reload and re-check, so the
    /// coordinator never blocks on it.
    fn observe_leaf(&self, path: &ObjectPath, leaf: &LeafBody);
}

/// State shared by the [`LeafCoordinator`] and its dedup [`CasWorker`]: the
/// storage handles, retry config, and stats.
struct CoordCore {
    tmon: Monitor,
    nodes: NodeStore,
    key_state: KeyStateResolver,
    gate_retry: StructuralGateRetry,
    retry: RetryConfig,
    stats: Stats,
    // Where stored over-cap leaves are reported: the background
    // [`TreeRebalancer`](crate::tree_rebalancer::TreeRebalancer)'s queue when one is wired.
    hinter: Arc<dyn SplitHinter>,
    policy: SplitPolicy,
}

struct CoordState {
    core: Arc<CoordCore>,
    dedup: Dedup<CasReq, TransError, CasWorker>,
}

/// The [`Dedup`] worker responsible for planning, persistence, retries, and
/// outcome delivery for one coordinator round per leaf (ADR-025).
struct CasWorker {
    core: Arc<CoordCore>,
}

/// Returns the merged request's members.
fn leaf_members(batch: &BatchHandle<CasReq, TransError>) -> BTreeMap<TxId, LeafMember> {
    batch.merged().members
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Participation {
    Skipped,
    Staged,
}

struct PlannedMember {
    id: TxId,
    outcome: MemberOutcome,
    participation: Participation,
}

struct MutationPlan {
    entries: BTreeMap<Vec<u8>, LeafEntry>,
    locks: NodeLocks,
    members: Vec<PlannedMember>,
}

impl MutationPlan {
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
    entries: Vec<(Vec<u8>, LeafEntry)>,
    locks: NodeLocks,
    admission: StageAdmission,
    outcome: MemberOutcome,
}

enum CapacityDecision {
    Admitted(ProposedStage),
    Rejected(MemberOutcome),
}

enum PersistResult {
    Applied(CasReceipt<Node>),
    Unchanged(LeafObservation),
    PreconditionMiss,
    InDoubt(BTreeSet<TxId>),
}

impl CasWorker {
    /// Builds the ordered mutation plan for one loaded leaf attempt.
    async fn plan_mutation(
        &self,
        path: &ObjectPath,
        edit: &LeafEdit,
        members: &BTreeMap<TxId, LeafMember>,
        requirement: Requirement,
        reloaded: bool,
        in_doubt: &mut BTreeSet<TxId>,
    ) -> Result<MutationPlan, TransError> {
        let mut plan = MutationPlan {
            entries: edit
                .entries()
                .entries()
                .cloned()
                .map(|e| (e.key.clone(), e))
                .collect(),
            locks: edit.locks().clone(),
            members: Vec::with_capacity(members.len()),
        };

        // Oldest-first planning prevents backtracking: a later member cannot
        // wound a member whose stage it has already observed (ADR-028).
        let mut ordered: Vec<(&TxId, &LeafMember)> = members.iter().collect();
        ordered.sort_by(|(a, _), (b, _)| compare_member_priority(a, b));
        // Marker evidence belongs to the loaded leaf observation, not to the
        // member evaluation order. Give every member a chance to retain it before a
        // preceding publisher can replace the corresponding entry in memory.
        for member in members.values() {
            member.resolver.observe_loaded(&plan.entries);
        }
        // A logless stage is its commit's only evidence, so another member may
        // not overwrite it before the shared CAS (ADR-051).
        let mut protected_markers: BTreeSet<Vec<u8>> = BTreeSet::new();
        for (tx, member) in ordered {
            let member_in_doubt = in_doubt.contains(tx);
            let ctx = ResolveCtx {
                key_state: &self.core.key_state,
                tmon: &self.core.tmon,
                gate_retry: &self.core.gate_retry,
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
                .leaf_scope_keys()
                .iter()
                .any(|&key| !edit.covers(key));
            if needs_reroute {
                plan.members.push(PlannedMember {
                    id: tx.clone(),
                    outcome: member.resolver.reroute_outcome(member_in_doubt),
                    participation: Participation::Skipped,
                });
                continue;
            }
            let protected_marker_conflict = member
                .resolver
                .publication_keys()
                .iter()
                .any(|&key| protected_markers.contains(key));
            if protected_marker_conflict {
                plan.members.push(PlannedMember {
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
            if member_in_doubt && !matches!(step.outcome(), MemberOutcome::InDoubt(_)) {
                in_doubt.remove(tx);
            }
            let member_in_doubt = in_doubt.contains(tx);
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
                            protected_markers.extend(
                                member
                                    .resolver
                                    .logless_publication_keys()
                                    .into_iter()
                                    .map(<[u8]>::to_vec),
                            );
                            plan.locks = proposed.locks;
                            plan.members.push(PlannedMember {
                                id: tx.clone(),
                                outcome: proposed.outcome,
                                participation: Participation::Staged,
                            });
                        }
                        CapacityDecision::Rejected(outcome) => {
                            plan.members.push(PlannedMember {
                                id: tx.clone(),
                                outcome,
                                participation: Participation::Skipped,
                            });
                        }
                    }
                }
                Step::Skip { outcome } => {
                    if matches!(&outcome, MemberOutcome::Landed) {
                        protected_markers.extend(
                            member
                                .resolver
                                .logless_publication_keys()
                                .into_iter()
                                .map(<[u8]>::to_vec),
                        );
                    }
                    plan.members.push(PlannedMember {
                        id: tx.clone(),
                        outcome,
                        participation: Participation::Skipped,
                    })
                }
            }
        }
        Ok(plan)
    }

    /// Classifies whether one proposed member stage fits the loaded leaf.
    fn capacity_decision(
        &self,
        path: &ObjectPath,
        edit: &LeafEdit,
        resolver: &dyn LeafResolver,
        in_doubt: bool,
        entries: &BTreeMap<Vec<u8>, LeafEntry>,
        proposed: ProposedStage,
    ) -> Result<CapacityDecision, TransError> {
        let mut candidate_entries = entries.clone();
        for (key, entry) in &proposed.entries {
            candidate_entries.insert(key.clone(), entry.clone());
        }
        let candidate_leaf = LeafBody::from_entries(
            candidate_entries
                .values()
                .filter(|entry| !entry.is_vestigial())
                .cloned(),
        );
        let mut candidate_node = edit.node().clone();
        candidate_node.set_leaf(candidate_leaf.clone())?;
        candidate_node.set_locks(proposed.locks.clone());
        let (inline_publication, direct_adds_key, pressure_hint) = match proposed.admission {
            StageAdmission::InlinePublication {
                adds_key,
                pressure_hint,
            } => (true, adds_key, pressure_hint),
            _ => (false, false, true),
        };
        let create_full = (proposed.admission == StageAdmission::AddsKey || direct_adds_key)
            && candidate_node.content_encoded_len() > self.core.policy.content_limit();
        let inline_entry_full = inline_publication
            && proposed
                .entries
                .iter()
                .any(|(_, entry)| !self.core.policy.entry_fits_split_budget(entry));
        if candidate_node.encoded_len() <= self.core.policy.node_max_bytes()
            && !create_full
            && !inline_entry_full
        {
            return Ok(CapacityDecision::Admitted(proposed));
        }

        // Splitting cannot make an intrinsically oversized entry fit. The
        // direct publisher falls back to an external value instead.
        if pressure_hint && !inline_entry_full {
            self.core.hinter.observe_leaf(path, &candidate_leaf);
        }
        let outcome = if proposed.admission == StageAdmission::AddsKey {
            MemberOutcome::LeafFull
        } else if in_doubt {
            resolver.exhausted_outcome(true)
        } else {
            MemberOutcome::Conflict
        };
        Ok(CapacityDecision::Rejected(outcome))
    }

    /// Persists one mutation plan and classifies its storage result.
    async fn persist(
        &self,
        path: &ObjectPath,
        mut edit: LeafEdit,
        plan: &mut MutationPlan,
        requirement: Requirement,
    ) -> Result<PersistResult, TransError> {
        if !plan.is_dirty() {
            // A late member can require evidence newer than the loaded leaf.
            // A dirty plan gets that evidence from its CAS; a clean plan needs
            // an exact-state check before its decisions can be delivered.
            return Ok(
                match self
                    .core
                    .nodes
                    .check_leaf_current(edit.observation(), requirement)
                    .await?
                {
                    LeafObservationCheck::Current => {
                        PersistResult::Unchanged(edit.observation().clone())
                    }
                    LeafObservationCheck::Changed(_) => PersistResult::PreconditionMiss,
                },
            );
        }

        // Drop entries a member left vestigial (no holder, no
        // `current_writer`): they name no transaction and are
        // indistinguishable from absent, so pruning them here — in the
        // same CAS that clears the last holder — keeps nodes tidy on
        // every path (acquire / write-back / release, ADR-029) instead
        // of leaving dead entries for a later GC cycle.
        let new_leaf = LeafBody::from_entries(
            std::mem::take(&mut plan.entries)
                .into_values()
                .filter(|entry| !entry.is_vestigial()),
        );
        edit.set_entries(new_leaf.clone());
        edit.set_locks(plan.locks.clone());
        match self.core.nodes.commit_leaf(edit).await {
            // Hint the background tree rebalancer if this write left the leaf
            // over the soft cap (ADR-031); the tree rebalancer reloads and
            // re-checks, so a spurious hint only costs one load.
            Ok(CasResult::Applied(receipt)) => {
                self.core.hinter.observe_leaf(path, &new_leaf);
                Ok(PersistResult::Applied(receipt))
            }
            Ok(CasResult::Conflict) => Ok(PersistResult::PreconditionMiss),
            Err(StorageError::Unavailable(_)) => {
                Ok(PersistResult::InDoubt(plan.staged_ids().cloned().collect()))
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Completes one coordinator round and delivers each member's outcome.
    /// A member that must wait for a holder receives its own wait outcome,
    /// so other members can complete while that caller waits and re-submits.
    async fn run_leaf(
        &self,
        path: &ObjectPath,
        batch: &BatchHandle<CasReq, TransError>,
    ) -> Result<(), TransError> {
        let mut requirement = batch.merged().requirement;
        // A cache-served `Any` load may complete without yielding. Give peers
        // already scheduled for this object one opportunity to join the round,
        // so batching does not depend on backend I/O creating the collection
        // window. The first load accepts any cached leaf, even for members
        // that require bounded evidence before completion.
        rt::yield_now().await;
        let mut backoff = self.core.retry.backoff();
        // Resolvers must distinguish the first attempt from recovery after a CAS
        // failure or a stale transaction dependency.
        let mut reloaded = false;
        // The members whose changes rode a CAS that came back in-doubt. For them
        // in-doubt is *sticky* across planning retries until their resolver returns a
        // reconciled, non-InDoubt decision: that write may have landed durably
        // (and been help-forwarded to a peer), so a later precondition-miss must
        // not downgrade the ambiguity to a definitive loss. Commit-install
        // would otherwise misclassify a landed-but-unacked lock as `Moved` and
        // unsafely discard the outcome and rerun a commit a peer already observed.
        //
        // It is per member rather than per round: a member the uncertain CAS did
        // not carry — one skipped for a same-key logless claim, or merged into
        // the batch afterwards — definitively did not land, and inheriting the
        // batch's ambiguity would strand it in-doubt over a write it never made.
        let mut in_doubt: BTreeSet<TxId> = BTreeSet::new();
        // Retain both submitted and resolver-requested bounds across retries.
        // ANY seeds need no preliminary check when a CAS confirms their state.
        let mut load_requirement = Requirement::ANY;
        for attempt in 0..CAS_RETRIES {
            if attempt > 0 {
                rt::sleep(backoff.next_delay()).await;
                self.core.stats.n_retries.fetch_add(1, Ordering::Relaxed);
                load_requirement = requirement;
            }
            let loaded = self.core.nodes.load_leaf(path, load_requirement).await;
            // Absence cannot supply a leaf CAS precondition or reach persist's
            // no-change check. Preserve the submitted bound on this error path.
            let loaded = match loaded {
                Err(StorageError::NotFound) if load_requirement != requirement => {
                    self.core.nodes.load_leaf(path, requirement).await
                }
                result => result,
            };
            let edit = match loaded {
                Ok(loaded) => loaded.into_edit(),
                // A root split can turn the routed root leaf into an index
                // between grouping and this load. Deliver each resolver's
                // reroute outcome so its caller rebuilds the current leaf set.
                Err(StorageError::Precondition) => {
                    let members = leaf_members(batch);
                    for (tx, member) in &members {
                        *member.slot.lock().unwrap() = Some(CoordinatedOutcome {
                            outcome: member.resolver.reroute_outcome(in_doubt.contains(tx)),
                            evidence: None,
                        });
                    }
                    return Ok(());
                }
                Err(e) => return Err(e.into()),
            };
            // Read the merged set *after* obtaining the leaf so this round
            // absorbs every member that queued while the load I/O was in flight
            // (ADR-025) — the window that turns N contenders' loads+CASes into
            // one. Keep these members with their combined requirement: their
            // dependent reads need that bound even when the leaf CAS can confirm
            // an older seed without a preliminary check.
            let merged = batch.merged();
            requirement = requirement.stricter(merged.requirement);
            let members = merged.members;
            let mut plan = match self
                .plan_mutation(path, &edit, &members, requirement, reloaded, &mut in_doubt)
                .await
            {
                Ok(plan) => plan,
                Err(TransError::ValidateRetry(fresh)) => {
                    requirement = requirement.stricter(fresh);
                    reloaded = true;
                    continue;
                }
                Err(error) => return Err(error),
            };

            let loaded_observation = edit.observation().clone();
            let persist_result = self.persist(path, edit, &mut plan, requirement).await?;
            let (loaded_observation, applied) = match persist_result {
                PersistResult::Applied(receipt) => (loaded_observation, Some(receipt)),
                PersistResult::Unchanged(observed) => (observed, None),
                // The CAS did not land, or the clean plan's loaded state
                // changed. Neither resolves an earlier uncertain mutation.
                PersistResult::PreconditionMiss => {
                    reloaded = true;
                    continue;
                }
                // Rebuilding the plan from a reloaded leaf is idempotent. Only the
                // members this uncertain CAS actually carried inherit its doubt.
                PersistResult::InDoubt(staged_ids) => {
                    in_doubt.extend(staged_ids);
                    reloaded = true;
                    continue;
                }
            };

            // The CAS landed (or nothing needed staging): publish each member's
            // outcome into its slot before returning, so the deposit
            // happens-before the dedup delivers to the caller. Recording the held
            // lock is the caller's job (the [`Locker`](crate::tlocker::Locker)), done when
            // it observes its own `Locked` outcome.
            for member in plan.members {
                if let Some(m) = members.get(&member.id) {
                    *m.slot.lock().unwrap() = Some(CoordinatedOutcome {
                        outcome: member.outcome,
                        evidence: Some(match member.participation {
                            Participation::Staged => {
                                let receipt = applied.as_ref().ok_or_else(|| {
                                    TransError::other("staged leaf member has no successful CAS")
                                })?;
                                CoordinationEvidence::Installed(receipt.clone())
                            }
                            Participation::Skipped => {
                                CoordinationEvidence::Observed(loaded_observation.clone())
                            }
                        }),
                    });
                }
            }
            return Ok(());
        }
        // Bounded CAS budget exhausted under churn: each member gets its
        // resolver's exhaustion outcome. Acquirers conflict and release/re-lock;
        // write-backs re-descend and releases re-submit, because exhaustion does
        // not prove convergence.
        for (tx, m) in &leaf_members(batch) {
            *m.slot.lock().unwrap() = Some(CoordinatedOutcome {
                outcome: m.resolver.exhausted_outcome(in_doubt.contains(tx)),
                evidence: None,
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
        self.run_leaf(&batch.merged().path, batch).await
    }
}

/// Coordinates leaf entry mutations and leaf structural-gate acquisition
/// across transactions (ADR-028). Owns batching, mutation planning, persistence,
/// and recovery; transaction lifecycle remains with its higher-level owner.
#[derive(Clone)]
pub struct LeafCoordinator {
    inner: Arc<CoordState>,
}

impl LeafCoordinator {
    /// Cancels in-flight coordination and awaits any spawned dedup owner tasks,
    /// so none leak when the database shuts down (ADR-025).
    pub async fn close(&self) {
        self.inner.dedup.close().await;
    }

    /// Returns and resets submission, worker-round, and inner-CAS retry counts.
    pub fn stats_and_reset(&self) -> LeafCoordinatorStats {
        let dedup = self.inner.dedup.stats_and_reset();
        LeafCoordinatorStats {
            submissions: dedup.submissions,
            rounds: dedup.rounds,
            cas_retries: self.inner.core.stats.n_retries.swap(0, Ordering::Relaxed),
        }
    }

    /// Returns a per-object dedup coordination snapshot (ADR-025).
    pub fn dedup_snapshot(&self) -> Vec<DedupKeySnapshot> {
        self.inner.dedup.snapshot()
    }

    /// Creates a coordinator that reports capacity observations to `hinter` —
    /// normally the background [`TreeRebalancer`](crate::tree_rebalancer::TreeRebalancer)'s queue.
    /// `policy` governs the coordinator's hard node-size limit.
    pub(crate) fn with_hinter(
        nodes: NodeStore,
        key_state: KeyStateResolver,
        tmon: Monitor,
        gate_retry: StructuralGateRetry,
        retry: RetryConfig,
        policy: SplitPolicy,
        hinter: Arc<dyn SplitHinter>,
    ) -> Self {
        let core = Arc::new(CoordCore {
            tmon,
            nodes,
            key_state,
            gate_retry,
            retry,
            stats: Stats::default(),
            policy,
            hinter,
        });
        let dedup = Dedup::new(CasWorker { core: core.clone() });
        LeafCoordinator {
            inner: Arc::new(CoordState { core, dedup }),
        }
    }

    /// Coordinates one complete operation and returns its operation-specific
    /// result.
    pub(crate) async fn coordinate<O>(&self, operation: O) -> Result<O::Output, TransError>
    where
        O: LeafOperation + 'static,
    {
        let operation = Arc::new(operation);
        let requirement = operation.requirement();
        let resolver: Arc<dyn LeafResolver> = operation.clone();
        let outcome = self
            .submit_leaf(operation.path(), operation.id(), resolver, requirement)
            .await?;
        operation.complete(outcome)
    }

    /// Submits one operation's resolver through the [`Dedup`] and awaits its
    /// single-round [`CoordinatedOutcome`]. The worker merges it into any
    /// in-flight round for the leaf, evaluates it, retries CAS contention / in-doubt
    /// internally, and deposits the policy outcome plus its physical evidence
    /// into the slot. Returns `Ok(None)` if the coordinator was shut down before
    /// the round ran, so the operation can preserve its best-effort behavior.
    ///
    /// `requirement` bounds dependent reads and completed leaf evidence across
    /// attempts. The first attempt accepts any cached leaf as a CAS precondition
    /// (ADR-030); retries load against the retained bound. Members merged during
    /// a load can raise the bound: a successful CAS confirms the loaded state,
    /// while a plan with no changes checks it explicitly. A changed state
    /// requires a new plan, not just newer evidence attached to the old outcome.
    ///
    /// `path` is the leaf's object path — the collection root `_r` for a small
    /// collection's single leaf, else a standalone node `_n` resolved by descent
    /// ([`TreeRouter`](glassdb_storage::TreeRouter)).
    async fn submit_leaf(
        &self,
        path: &ObjectPath,
        id: &TxId,
        resolver: Arc<dyn LeafResolver>,
        requirement: Requirement,
    ) -> Result<Option<CoordinatedOutcome>, TransError> {
        let slot: OutcomeSlot = Arc::new(Mutex::new(None));
        let mut members = BTreeMap::new();
        members.insert(
            id.clone(),
            LeafMember {
                resolver,
                slot: slot.clone(),
            },
        );
        let req = CasReq {
            path: path.clone(),
            members,
            requirement,
        };
        let key = path.to_string();
        match self.inner.dedup.run(&key, req).await {
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

/// Member evaluation order: oldest wound-wait priority first, with a
/// deterministic full-id byte tiebreak for equal-priority members. The tiebreak
/// is **round-local** — it only fixes who stages first this round, never who
/// wins a wound ([`should_wound`] ignores it) — so a renewed id (fresh prefix,
/// same priority) can reorder member evaluation without ever flipping a persistent wound
/// winner, which is what would let equal-priority peers livelock (ADR-002/028).
fn compare_member_priority(a: &TxId, b: &TxId) -> CmpOrdering {
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

    use crate::engine::{AssemblyFixture, EngineConfig};

    use std::time::Duration;

    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{
        BackendOp, HookBackend, HookFuture, OpLog, RecordingBackend,
    };
    use glassdb_concurr::Background;
    use glassdb_data::{CollectionAddress, DbRoot, NodeToken, ObjectPath};
    use glassdb_storage::{CachedStore, CurrentState, LeafBody, LockType, Node, Timeline};

    const COLL: &str = "coordp";

    fn collection() -> CollectionAddress {
        CollectionAddress::root(COLL)
    }

    fn leaf_token() -> NodeToken {
        NodeToken::from_bytes([0; 16])
    }

    struct NoSplitHints;

    impl SplitHinter for NoSplitHints {
        fn observe_leaf(&self, _path: &ObjectPath, _leaf: &LeafBody) {}
    }

    // Every coordination round in these tests targets one leaf object. A
    // standalone node `_n/<token>` is the cleanest stand-in: it carries only key
    // entries (no collection metadata), exactly what leaf mutation planning operates on.
    fn leaf_path() -> ObjectPath {
        ObjectPath::Node {
            collection: collection(),
            token: leaf_token(),
        }
    }

    fn leaf() -> ObjectPath {
        leaf_path()
    }

    // A coordinator over `backend` with its own (large, non-evicting) cache, plus
    // the leaf store backing it (a clone sharing the cache, so a test can warm or
    // seed the cache the coordinator reads). The returned `Background` must be
    // kept alive for the monitor's lifetime.
    async fn coord_over(
        backend: Arc<dyn Backend>,
    ) -> (LeafCoordinator, NodeStore, Timeline, Arc<Background>) {
        coord_over_with(backend, SplitPolicy::default(), Arc::new(NoSplitHints)).await
    }

    async fn coord_over_with(
        backend: Arc<dyn Backend>,
        policy: SplitPolicy,
        hinter: Arc<dyn SplitHinter>,
    ) -> (LeafCoordinator, NodeStore, Timeline, Arc<Background>) {
        coord_over_retry(backend, policy, hinter, RetryConfig::default()).await
    }

    // A coordinator with a near-zero CAS backoff, so an exhaustion regression
    // does not pay the production retry delay.
    async fn coord_over_fast(
        backend: Arc<dyn Backend>,
    ) -> (LeafCoordinator, NodeStore, Timeline, Arc<Background>) {
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
    ) -> (LeafCoordinator, NodeStore, Timeline, Arc<Background>) {
        let seed_timeline = Timeline::new();
        let seed_store = NodeStore::new(
            CachedStore::new(backend.clone(), 1 << 20, seed_timeline.clone(), None),
            std::num::NonZeroUsize::MIN,
        );
        let _ = seed_store
            .store_node(
                &collection(),
                &leaf_token(),
                &Node::leaf(LeafBody::new()),
                None,
            )
            .await
            .unwrap();

        let mut config = EngineConfig::default();
        config.set_cache_size(1 << 20);
        let foundation = AssemblyFixture::new(backend, DbRoot::try_from(COLL).unwrap(), &config);
        let timeline = foundation.timeline.clone();
        let bg = foundation.background.clone();
        let mon = foundation.monitor.clone();
        let nodes = foundation.nodes.clone();
        let key_state = KeyStateResolver::new(mon.clone());
        let coord = LeafCoordinator::with_hinter(
            nodes.clone(),
            key_state,
            mon,
            crate::node_locking::StructuralGateRetry::new(timeline.clone(), Arc::default()),
            retry,
            policy,
            hinter,
        );
        (coord, nodes, timeline, bg)
    }

    // A cold leaf store over `backend` (its own empty cache), for asserting what
    // actually landed in storage without touching the coordinator's cache.
    fn cold_store(backend: Arc<dyn Backend>) -> NodeStore {
        let timeline = Timeline::new();
        NodeStore::new(
            CachedStore::new(backend, 1 << 20, timeline.clone(), None),
            std::num::NonZeroUsize::MIN,
        )
    }

    async fn coordinator_with_intent_gate(
        backend: Arc<dyn Backend>,
        recovery_wake: Arc<tokio::sync::Notify>,
    ) -> (AssemblyFixture, LeafCoordinator) {
        use glassdb_storage::transaction::{TxCommitStatus, TxLog};

        let base = AssemblyFixture::new(
            backend,
            DbRoot::try_from(COLL).unwrap(),
            &EngineConfig::default(),
        );
        let owner = TxId::with_priority(1, b"merge");
        base.monitor.begin_tx(&owner);
        base.monitor
            .commit_tx(TxLog::new(owner.clone(), TxCommitStatus::Ok))
            .await
            .unwrap();
        let mut node = Node::leaf(LeafBody::from_entries([LeafEntry::new(b"key")
            .with_current(CurrentState::Inline {
                writer: owner.clone(),
                value: Arc::from(b"value".as_slice()),
            })]));
        node.set_structural_gate(owner.clone());
        let mut locks = node.locks().clone();
        locks
            .bind_structural_intent(&owner, leaf_token().into())
            .unwrap();
        node.set_locks(locks);
        assert!(
            base.nodes
                .store_node(&collection(), &leaf_token(), &node, None)
                .await
                .unwrap()
        );
        let coord = LeafCoordinator::with_hinter(
            base.nodes.clone(),
            KeyStateResolver::new(base.monitor.clone()),
            base.monitor.clone(),
            StructuralGateRetry::new(base.timeline.clone(), recovery_wake),
            RetryConfig {
                initial_interval: Duration::from_millis(1),
                max_interval: Duration::from_millis(1),
            },
            SplitPolicy::default(),
            Arc::new(NoSplitHints),
        );
        (base, coord)
    }

    #[tokio::test(start_paused = true)]
    async fn structural_acquisition_refreshes_an_intent_gate_after_a_delayed_read() {
        use crate::node_locking::{StructuralGateOperation, StructuralGateOutcome};
        use glassdb_storage::{IndexNode, TreeRouter};

        let memory = Arc::new(MemoryBackend::new());
        let recorder = RecordingBackend::new(memory.clone());
        let log = recorder.log();
        let hooks = Arc::new(HookBackend::new(Arc::new(recorder)));
        let wake = Arc::new(tokio::sync::Notify::new());
        let (base, coord) = coordinator_with_intent_gate(hooks.clone(), wake.clone()).await;
        base.nodes
            .create_root(
                &collection(),
                &Node::index(IndexNode::from_children([(
                    Vec::new(),
                    leaf_token().to_string(),
                )])),
            )
            .await
            .unwrap();
        let router = TreeRouter::new(
            base.nodes.clone(),
            base.timeline.clone(),
            std::num::NonZeroUsize::MIN,
        );
        let routed = tokio::time::timeout(
            Duration::from_secs(1),
            router.route_key(&collection(), b"key", Requirement::ANY),
        )
        .await
        .expect("reads must not wait for an intent-owned gate")
        .unwrap();
        assert!(routed.node().unwrap().structural_gate().intent().is_some());
        assert!(futures::poll!(Box::pin(wake.notified())).is_pending());
        log.lock().unwrap().clear();

        let (entered, release) = park_read_reply(&hooks, leaf(), 1);
        let id = TxId::with_priority(2, b"next split");
        let acquiring = coord.coordinate(StructuralGateOperation::new(id.clone(), leaf()));
        tokio::pin!(acquiring);
        tokio::time::timeout(Duration::from_secs(1), async {
            tokio::select! {
                _ = &mut acquiring => panic!("the bound gate must defer acquisition"),
                _ = entered.notified() => {}
            }
        })
        .await
        .expect("a bound gate must request a fresh read");
        assert_eq!(leaf_stores(&log), 0);
        tokio::time::timeout(Duration::from_secs(1), wake.notified())
            .await
            .expect("a blocked mutation must request recovery");

        // The parked reply still contains the gate. Clearing it through another
        // cache requires a new barrier after that reply completes.
        let peer = cold_store(memory);
        let loaded = tokio::time::timeout(
            Duration::from_secs(1),
            peer.load_leaf(&leaf(), Requirement::ANY),
        )
        .await
        .expect("storage must return the gate without waiting")
        .unwrap();
        let mut edit = loaded.into_edit();
        let gate = edit.locks().structural_gate();
        let owner = gate.holder().unwrap().clone();
        let intent = gate.intent().unwrap().clone();
        let mut locks = edit.locks().clone();
        assert!(locks.complete_structural_intent(&owner, &intent));
        edit.set_locks(locks);
        assert!(matches!(
            peer.commit_leaf(edit).await.unwrap(),
            CasResult::Applied(_)
        ));
        let after_release = base.timeline.currentness_barrier();
        release.notify_one();
        let outcome = tokio::time::timeout(Duration::from_secs(1), acquiring)
            .await
            .expect("acquisition must observe the released gate")
            .unwrap();
        let StructuralGateOutcome::Acquired(observation) = outcome else {
            panic!("acquisition must complete after recovery");
        };
        assert!(observation.is_current_after(after_release));
        let node = observation.value().unwrap();
        assert!(node.structural_gate().contains(&id));
        assert!(node.structural_gate().intent().is_none());
        assert_eq!(
            node.as_leaf()
                .unwrap()
                .lookup(b"key")
                .unwrap()
                .current
                .inline()
                .unwrap()
                .as_ref(),
            b"value"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_unfinished_intent_gate_uses_the_coordinators_retry_budget() {
        use crate::node_locking::{StructuralGateOperation, StructuralGateOutcome};

        let (base, coord) =
            coordinator_with_intent_gate(Arc::new(MemoryBackend::new()), Arc::default()).await;
        let outcome = tokio::time::timeout(
            Duration::from_secs(1),
            coord.coordinate(StructuralGateOperation::new(
                TxId::with_priority(2, b"next split"),
                leaf(),
            )),
        )
        .await
        .expect("an unfinished intent must not block a coordinator round forever")
        .unwrap();
        assert!(matches!(outcome, StructuralGateOutcome::Deferred));
        let stored = base
            .nodes
            .load_leaf(&leaf(), Requirement::ANY)
            .await
            .unwrap();
        assert!(stored.locks().structural_gate().intent().is_some());
    }

    fn entry(key: &[u8], typ: LockType, holder: Option<&TxId>, writer: Option<&TxId>) -> LeafEntry {
        let mut entry =
            LeafEntry::new(key).with_current(writer.map_or(CurrentState::Absent, |writer| {
                CurrentState::External {
                    writer: writer.clone(),
                }
            }));
        match (typ, holder) {
            (LockType::None | LockType::Unknown, None) => {}
            (LockType::Read, Some(holder)) => entry.acquire_read_lock(holder.clone()),
            (LockType::Write, Some(holder)) => entry.replace_write_lock(holder.clone()),
            (LockType::Create, Some(holder)) => entry.replace_create_lock(holder.clone()),
            _ => panic!("test entry requires a valid lock shape"),
        }
        entry
    }

    // Replaces the leaf's entries with exactly `entries` (a plain CAS, no
    // coordinator).
    async fn store_leaf_entries(store: &NodeStore, path: &ObjectPath, entries: Vec<LeafEntry>) {
        let _ = store
            .store_node(
                &collection(),
                &leaf_token(),
                &Node::leaf(LeafBody::new()),
                None,
            )
            .await
            .unwrap();
        let loaded = store.load_leaf(path, Requirement::ANY).await.unwrap();
        let leaf = LeafBody::from_entries(entries);
        let mut edit = loaded.into_edit();
        edit.set_entries(leaf);
        assert!(store.commit_leaf(edit).await.unwrap().is_applied());
    }

    async fn replace_leaf_node(store: &NodeStore, node: &Node) {
        let observed = store
            .load_node_state(&collection(), &leaf_token(), Requirement::ANY)
            .await
            .unwrap();
        assert!(
            store
                .store_node(&collection(), &leaf_token(), node, Some(&observed))
                .await
                .unwrap()
        );
    }

    fn leaf_reads(log: &OpLog) -> usize {
        log.lock()
            .unwrap()
            .iter()
            .filter(|r| (r.op == "read" || r.op == "read_if_modified") && r.path.contains("/_n/"))
            .count()
    }

    fn leaf_stores(log: &OpLog) -> usize {
        log.lock()
            .unwrap()
            .iter()
            .filter(|r| {
                (r.op == "write_if" || r.op == "write_if_not_exists") && r.path.contains("/_n/")
            })
            .count()
    }

    // Loads the leaf's entries from a cold store, for asserting what landed.
    async fn cold_entries(store: &NodeStore, path: &ObjectPath) -> LeafBody {
        store
            .load_leaf(path, Requirement::ANY)
            .await
            .unwrap()
            .entries()
            .clone()
    }

    // Stages a write lock for `tx` on `key`, preserving any fields already staged.
    struct StageLock {
        key: Vec<u8>,
        tx: TxId,
        admission: StageAdmission,
    }

    #[async_trait::async_trait]
    impl LeafResolver for StageLock {
        async fn resolve(
            &self,
            _ctx: &ResolveCtx<'_>,
            staged: &BTreeMap<Vec<u8>, LeafEntry>,
            staged_locks: &NodeLocks,
        ) -> Result<Step, TransError> {
            let mut e = staged
                .get(&self.key)
                .cloned()
                .unwrap_or_else(|| entry(&self.key, LockType::None, None, None));
            if self.admission == StageAdmission::AddsKey {
                e.replace_create_lock(self.tx.clone());
            } else {
                e.replace_write_lock(self.tx.clone());
            }
            Ok(Step::Stage {
                entries: vec![(self.key.clone(), e)],
                locks: staged_locks.clone(),
                admission: self.admission,
                outcome: MemberOutcome::Locked {
                    typ: LockType::Write,
                    membership: LockType::None,
                },
            })
        }

        fn reorderable(&self) -> bool {
            false
        }

        fn exhausted_outcome(&self, _in_doubt: bool) -> MemberOutcome {
            MemberOutcome::Conflict
        }

        fn leaf_scope_keys(&self) -> Vec<&[u8]> {
            vec![self.key.as_slice()]
        }
    }

    impl LeafOperation for StageLock {
        type Output = bool;

        fn path(&self) -> &ObjectPath {
            static PATH: std::sync::OnceLock<ObjectPath> = std::sync::OnceLock::new();
            PATH.get_or_init(leaf)
        }

        fn id(&self) -> &TxId {
            &self.tx
        }

        fn requirement(&self) -> Requirement {
            Requirement::ANY
        }

        fn complete(
            &self,
            outcome: Option<CoordinatedOutcome>,
        ) -> Result<Self::Output, TransError> {
            Ok(matches!(
                outcome,
                Some(CoordinatedOutcome {
                    outcome: MemberOutcome::Locked { .. },
                    evidence: Some(CoordinationEvidence::Installed(_)),
                    ..
                })
            ))
        }
    }

    // Stages nothing; always delivers a best-effort `Released`.
    struct SkipRelease;

    #[async_trait::async_trait]
    impl LeafResolver for SkipRelease {
        async fn resolve(
            &self,
            _ctx: &ResolveCtx<'_>,
            _staged: &BTreeMap<Vec<u8>, LeafEntry>,
            _staged_locks: &NodeLocks,
        ) -> Result<Step, TransError> {
            Ok(Step::Skip {
                outcome: MemberOutcome::Released {
                    superseded: Vec::new(),
                },
            })
        }

        fn reorderable(&self) -> bool {
            true
        }

        fn exhausted_outcome(&self, _in_doubt: bool) -> MemberOutcome {
            MemberOutcome::Released {
                superseded: Vec::new(),
            }
        }
    }

    // Each member records its id and the previously staged keys so tests can
    // check evaluation order and which admitted changes later members observe.
    type ResolverTrace = Arc<Mutex<Vec<(TxId, Vec<Vec<u8>>)>>>;

    // Retains the state seen during evaluation to check ordered staging.
    struct Recorder {
        key: Vec<u8>,
        tx: TxId,
        trace: ResolverTrace,
    }

    #[async_trait::async_trait]
    impl LeafResolver for Recorder {
        async fn resolve(
            &self,
            _ctx: &ResolveCtx<'_>,
            staged: &BTreeMap<Vec<u8>, LeafEntry>,
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
                outcome: MemberOutcome::Landed,
            })
        }

        fn reorderable(&self) -> bool {
            false
        }

        fn exhausted_outcome(&self, _in_doubt: bool) -> MemberOutcome {
            MemberOutcome::Conflict
        }
    }

    // A resolver whose outcome exposes the state used for its decision.
    struct RequirementProbe {
        tx: TxId,
        stage_until_present: bool,
        dependency: Option<(NodeStore, ObjectPath)>,
        requirements: Arc<Mutex<Vec<Requirement>>>,
    }

    #[async_trait]
    impl LeafResolver for RequirementProbe {
        async fn resolve(
            &self,
            ctx: &ResolveCtx<'_>,
            staged: &BTreeMap<Vec<u8>, LeafEntry>,
            locks: &NodeLocks,
        ) -> Result<Step, TransError> {
            self.requirements.lock().unwrap().push(ctx.requirement);
            if self.stage_until_present && !staged.contains_key(b"driver".as_slice()) {
                return StageLock {
                    tx: self.tx.clone(),
                    key: b"driver".to_vec(),
                    admission: StageAdmission::ExistingKeys,
                }
                .resolve(ctx, staged, locks)
                .await;
            }
            let has_peer = match &self.dependency {
                Some((nodes, path)) => nodes
                    .load_leaf(path, ctx.requirement)
                    .await?
                    .entries()
                    .lookup(b"peer")
                    .is_some(),
                None => staged.contains_key(b"peer".as_slice()),
            };
            Ok(Step::Skip {
                outcome: if has_peer {
                    MemberOutcome::Wait(TxId::with_priority(3, b"peer"))
                } else {
                    MemberOutcome::Released {
                        superseded: Vec::new(),
                    }
                },
            })
        }

        fn reorderable(&self) -> bool {
            true
        }

        fn exhausted_outcome(&self, _in_doubt: bool) -> MemberOutcome {
            MemberOutcome::Conflict
        }
    }

    fn park_read_reply(
        backend: &HookBackend,
        path: ObjectPath,
        read_number: usize,
    ) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let reads = std::sync::atomic::AtomicUsize::new(0);
        backend.set_after({
            let entered = entered.clone();
            let release = release.clone();
            move |op, _| {
                let park = matches!(
                    op,
                    BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                ) && op.path() == path.to_string()
                    && reads.fetch_add(1, Ordering::SeqCst) + 1 == read_number;
                let entered = entered.clone();
                let release = release.clone();
                Box::pin(async move {
                    if park {
                        entered.notify_one();
                        release.notified().await;
                    }
                    Ok(())
                })
            }
        });
        (entered, release)
    }

    async fn wait_for_joiner(coord: &LeafCoordinator) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if coord.dedup_snapshot().iter().any(|snapshot| {
                    snapshot.batch_count + snapshot.pending_count + snapshot.queue_count == 2
                }) {
                    return;
                }
                rt::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[derive(Clone, Copy)]
    enum JoinedLeafChange {
        Unchanged,
        Entry,
        Index,
    }

    async fn joined_no_change_plan(retry: bool, change: JoinedLeafChange) {
        let memory: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let hooks = HookBackend::new(memory.clone());
        let recorder = RecordingBackend::new(hooks.clone());
        let operations = recorder.log();
        let (coord, _nodes, timeline, _bg) = coord_over_fast(Arc::new(recorder)).await;
        let driver_id = TxId::with_priority(1, b"driver");
        if retry {
            hooks.set_before({
                let memory = memory.clone();
                let driver_id = driver_id.clone();
                let first = std::sync::atomic::AtomicBool::new(true);
                move |op| {
                    let conflict = matches!(op, BackendOp::WriteIf { .. })
                        && op.path() == leaf().to_string()
                        && first.swap(false, Ordering::SeqCst);
                    let peer = cold_store(memory.clone());
                    let driver_id = driver_id.clone();
                    Box::pin(async move {
                        if conflict {
                            store_leaf_entries(
                                &peer,
                                &leaf(),
                                vec![entry(b"driver", LockType::Write, Some(&driver_id), None)],
                            )
                            .await;
                        }
                        Ok(())
                    })
                }
            });
        }
        let (entered, release) = park_read_reply(&hooks, leaf(), if retry { 2 } else { 1 });
        operations.lock().unwrap().clear();
        let driver = tokio::spawn({
            let coord = coord.clone();
            async move {
                coord
                    .submit_leaf(
                        &leaf(),
                        &driver_id,
                        Arc::new(RequirementProbe {
                            tx: driver_id.clone(),
                            stage_until_present: retry,
                            dependency: None,
                            requirements: Arc::default(),
                        }),
                        Requirement::ANY,
                    )
                    .await
            }
        });
        entered.notified().await;
        let peer = cold_store(memory);
        if matches!(change, JoinedLeafChange::Entry) {
            let loaded = peer.load_leaf(&leaf(), Requirement::ANY).await.unwrap();
            let mut entries = loaded.entries().entries().cloned().collect::<Vec<_>>();
            entries.push(entry(
                b"peer",
                LockType::Write,
                Some(&TxId::with_priority(3, b"peer")),
                None,
            ));
            let mut edit = loaded.into_edit();
            edit.set_entries(LeafBody::from_entries(entries));
            assert!(peer.commit_leaf(edit).await.unwrap().is_applied());
        }
        if matches!(change, JoinedLeafChange::Index) {
            let child = NodeToken::from_bytes([2; 16]);
            assert!(
                peer.store_node(&collection(), &child, &Node::leaf(LeafBody::new()), None)
                    .await
                    .unwrap()
            );
            replace_leaf_node(
                &peer,
                &Node::index(glassdb_storage::IndexNode::from_children([(
                    Vec::new(),
                    child.to_string(),
                )])),
            )
            .await;
        }
        let barrier = timeline.currentness_barrier();
        let requirement = Requirement::after(barrier);
        let requirements = Arc::new(Mutex::new(Vec::new()));
        let joiner = tokio::spawn({
            let coord = coord.clone();
            let requirements = requirements.clone();
            async move {
                let tx = TxId::with_priority(2, b"joiner");
                coord
                    .submit_leaf(
                        &leaf(),
                        &tx,
                        Arc::new(RequirementProbe {
                            tx: tx.clone(),
                            stage_until_present: false,
                            dependency: None,
                            requirements,
                        }),
                        requirement,
                    )
                    .await
            }
        });
        wait_for_joiner(&coord).await;
        release.notify_one();
        driver.await.unwrap().unwrap();
        let outcome = joiner.await.unwrap().unwrap().unwrap();
        if matches!(change, JoinedLeafChange::Index) {
            assert!(matches!(outcome.outcome, MemberOutcome::Conflict));
            assert!(
                outcome.evidence.is_none(),
                "an index reroute must not carry leaf evidence"
            );
            assert_eq!(*requirements.lock().unwrap(), [requirement]);
            assert_eq!(leaf_reads(&operations), 2);
            assert_eq!(leaf_stores(&operations), 0);
            coord.close().await;
            return;
        }
        let observed = outcome.evidence.unwrap().into_observation();
        assert!(
            observed.is_current_after(barrier),
            "the joined member received insufficient leaf evidence"
        );
        if matches!(change, JoinedLeafChange::Entry) {
            assert!(
                matches!(outcome.outcome, MemberOutcome::Wait(ref id) if id == &TxId::with_priority(3, b"peer")),
                "the old no-change plan must be rebuilt after the leaf changes"
            );
        } else {
            assert!(matches!(outcome.outcome, MemberOutcome::Released { .. }));
        }
        assert_eq!(
            *requirements.lock().unwrap(),
            vec![
                requirement;
                if matches!(change, JoinedLeafChange::Entry) {
                    2
                } else {
                    1
                }
            ]
        );
        assert_eq!(leaf_reads(&operations), if retry { 3 } else { 2 });
        assert_eq!(leaf_stores(&operations), usize::from(retry));
        assert_eq!(coord.stats_and_reset().rounds, 1);
        coord.close().await;
    }

    #[tokio::test]
    async fn late_joiner_rechecks_a_changed_leaf_before_no_change_completion() {
        joined_no_change_plan(false, JoinedLeafChange::Entry).await;
    }

    #[tokio::test]
    async fn late_joiner_checks_an_unchanged_leaf_without_repeating_resolution() {
        joined_no_change_plan(false, JoinedLeafChange::Unchanged).await;
    }

    #[tokio::test]
    async fn late_joiner_keeps_its_requirement_after_a_cas_retry() {
        joined_no_change_plan(true, JoinedLeafChange::Entry).await;
    }

    #[tokio::test]
    async fn late_joiner_reroutes_when_the_no_change_check_finds_an_index() {
        joined_no_change_plan(false, JoinedLeafChange::Index).await;
    }

    struct ValidateOnce {
        timeline: Timeline,
        requested: Mutex<Option<Requirement>>,
    }

    #[async_trait]
    impl LeafResolver for ValidateOnce {
        async fn resolve(
            &self,
            ctx: &ResolveCtx<'_>,
            staged: &BTreeMap<Vec<u8>, LeafEntry>,
            locks: &NodeLocks,
        ) -> Result<Step, TransError> {
            {
                let mut requested = self.requested.lock().unwrap();
                if requested.is_none() {
                    let requirement = Requirement::after(self.timeline.currentness_barrier());
                    *requested = Some(requirement);
                    return Err(TransError::ValidateRetry(requirement));
                }
            }
            SkipRelease.resolve(ctx, staged, locks).await
        }

        fn reorderable(&self) -> bool {
            true
        }

        fn exhausted_outcome(&self, _: bool) -> MemberOutcome {
            MemberOutcome::Conflict
        }
    }

    #[tokio::test]
    async fn an_any_joiner_preserves_a_resolvers_retry_requirement() {
        let hooks = HookBackend::new(Arc::new(MemoryBackend::new()));
        let recorder = RecordingBackend::new(hooks.clone());
        let operations = recorder.log();
        let (coord, _, timeline, _bg) = coord_over_fast(Arc::new(recorder)).await;
        let resolver = Arc::new(ValidateOnce {
            timeline,
            requested: Mutex::new(None),
        });
        let (entered, release) = park_read_reply(&hooks, leaf(), 2);
        operations.lock().unwrap().clear();
        let driver = tokio::spawn({
            let coord = coord.clone();
            let resolver = resolver.clone();
            async move {
                coord
                    .submit_leaf(
                        &leaf(),
                        &TxId::with_priority(1, b"driver"),
                        resolver,
                        Requirement::ANY,
                    )
                    .await
            }
        });
        entered.notified().await;
        let requirement = resolver.requested.lock().unwrap().unwrap();
        let requirements = Arc::new(Mutex::new(Vec::new()));
        let joiner = tokio::spawn({
            let coord = coord.clone();
            let requirements = requirements.clone();
            async move {
                let tx = TxId::with_priority(2, b"joiner");
                coord
                    .submit_leaf(
                        &leaf(),
                        &tx,
                        Arc::new(RequirementProbe {
                            tx: tx.clone(),
                            stage_until_present: false,
                            dependency: None,
                            requirements,
                        }),
                        Requirement::ANY,
                    )
                    .await
            }
        });
        wait_for_joiner(&coord).await;
        release.notify_one();
        driver.await.unwrap().unwrap();
        let outcome = joiner.await.unwrap().unwrap().unwrap();
        assert!(
            outcome
                .evidence
                .unwrap()
                .into_observation()
                .satisfies(requirement)
        );
        assert_eq!(*requirements.lock().unwrap(), [requirement]);
        assert_eq!(leaf_reads(&operations), 2);
        assert_eq!(leaf_stores(&operations), 0);
        assert_eq!(coord.stats_and_reset().rounds, 1);
        coord.close().await;
    }

    #[tokio::test]
    async fn a_missing_leaf_load_does_not_absorb_a_late_bounded_member() {
        let memory: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let hooks = HookBackend::new(memory.clone());
        let (coord, _, timeline, _bg) = coord_over(hooks.clone()).await;
        let token = NodeToken::from_bytes([7; 16]);
        let path = ObjectPath::Node {
            collection: collection(),
            token: token.clone(),
        };
        let (entered, release) = park_read_reply(&hooks, path.clone(), 1);
        let driver = tokio::spawn({
            let coord = coord.clone();
            let path = path.clone();
            async move {
                coord
                    .submit_leaf(
                        &path,
                        &TxId::with_priority(1, b"driver"),
                        Arc::new(SkipRelease),
                        Requirement::ANY,
                    )
                    .await
            }
        });
        entered.notified().await;
        assert!(
            cold_store(memory)
                .store_node(&collection(), &token, &Node::leaf(LeafBody::new()), None)
                .await
                .unwrap()
        );
        let barrier = timeline.currentness_barrier();
        let requirements = Arc::new(Mutex::new(Vec::new()));
        let joiner = tokio::spawn({
            let coord = coord.clone();
            let requirements = requirements.clone();
            async move {
                let tx = TxId::with_priority(2, b"joiner");
                coord
                    .submit_leaf(
                        &path,
                        &tx,
                        Arc::new(RequirementProbe {
                            tx: tx.clone(),
                            stage_until_present: false,
                            dependency: None,
                            requirements,
                        }),
                        Requirement::after(barrier),
                    )
                    .await
            }
        });
        wait_for_joiner(&coord).await;
        release.notify_one();
        assert!(matches!(
            driver.await.unwrap(),
            Err(TransError::Storage(StorageError::NotFound))
        ));
        let outcome = joiner.await.unwrap().unwrap().unwrap();
        assert!(
            outcome
                .evidence
                .unwrap()
                .into_observation()
                .is_current_after(barrier)
        );
        assert_eq!(*requirements.lock().unwrap(), [Requirement::after(barrier)]);
        assert_eq!(coord.stats_and_reset().rounds, 2);
        coord.close().await;
    }

    #[tokio::test]
    async fn joined_requirement_bounds_dependencies_while_cas_supplies_leaf_evidence() {
        let memory: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let hooks = HookBackend::new(memory.clone());
        let recorder = RecordingBackend::new(hooks.clone());
        let operations = recorder.log();
        let (coord, nodes, timeline, _bg) = coord_over(Arc::new(recorder)).await;
        let token = NodeToken::from_bytes([1; 16]);
        let dependency = ObjectPath::Node {
            collection: collection(),
            token: token.clone(),
        };
        assert!(
            nodes
                .store_node(&collection(), &token, &Node::leaf(LeafBody::new()), None)
                .await
                .unwrap()
        );
        let (entered, release) = park_read_reply(&hooks, leaf(), 1);
        operations.lock().unwrap().clear();
        let driver = tokio::spawn({
            let coord = coord.clone();
            async move {
                let tx = TxId::with_priority(1, b"driver");
                coord
                    .submit_leaf(
                        &leaf(),
                        &tx,
                        Arc::new(StageLock {
                            key: b"driver".to_vec(),
                            tx: tx.clone(),
                            admission: StageAdmission::ExistingKeys,
                        }),
                        Requirement::ANY,
                    )
                    .await
            }
        });
        entered.notified().await;
        let peer = cold_store(memory);
        let mut edit = peer
            .load_leaf(&dependency, Requirement::ANY)
            .await
            .unwrap()
            .into_edit();
        edit.set_entries(LeafBody::from_entries([entry(
            b"peer",
            LockType::Write,
            Some(&TxId::with_priority(3, b"peer")),
            None,
        )]));
        assert!(peer.commit_leaf(edit).await.unwrap().is_applied());
        let barrier = timeline.currentness_barrier();
        let requirements = Arc::new(Mutex::new(Vec::new()));
        let joiner = tokio::spawn({
            let coord = coord.clone();
            let requirements = requirements.clone();
            let dependency = dependency.clone();
            async move {
                let tx = TxId::with_priority(2, b"joiner");
                coord
                    .submit_leaf(
                        &leaf(),
                        &tx,
                        Arc::new(RequirementProbe {
                            tx: tx.clone(),
                            stage_until_present: false,
                            dependency: Some((nodes, dependency)),
                            requirements,
                        }),
                        Requirement::after(barrier),
                    )
                    .await
            }
        });
        wait_for_joiner(&coord).await;
        release.notify_one();
        let driver = driver.await.unwrap().unwrap().unwrap();
        let joiner = joiner.await.unwrap().unwrap().unwrap();
        assert!(
            matches!(joiner.outcome, MemberOutcome::Wait(ref id) if id == &TxId::with_priority(3, b"peer")),
            "a leaf CAS cannot repair a dependent read that ignored the joined requirement"
        );
        let expected = joiner.evidence.unwrap().into_observation();
        assert!(expected.is_current_after(barrier));
        assert!(driver.evidence.unwrap().validates(&expected, barrier));
        assert_eq!(*requirements.lock().unwrap(), [Requirement::after(barrier)]);
        let calls = |path: &ObjectPath| {
            operations
                .lock()
                .unwrap()
                .iter()
                .filter(|op| op.path == path.to_string())
                .map(|op| op.op)
                .collect::<Vec<_>>()
        };
        assert_eq!(calls(&leaf()), ["read", "write_if"]);
        assert_eq!(calls(&dependency), ["read_if_modified"]);
        assert_eq!(coord.stats_and_reset().rounds, 1);
        coord.close().await;
    }

    // A hook that parks the next leaf read while armed, letting a second submitter merge.
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
        fn observe_leaf(&self, _path: &ObjectPath, _leaf: &LeafBody) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    // A typed operation drives one CAS and translates its exact precondition
    // receipt without exposing the shared outcome vocabulary to its caller.
    #[tokio::test]
    async fn leaf_stage_is_cas_persisted() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (coord, _nodes, _timeline, _bg) = coord_over(backend.clone()).await;
        let tx = TxId::with_priority(1, b"t");

        let landed = coord
            .coordinate(StageLock {
                key: b"k".to_vec(),
                tx: tx.clone(),
                admission: StageAdmission::ExistingKeys,
            })
            .await
            .unwrap();
        assert!(landed);
        coord.close().await;

        let leaf = cold_entries(&cold_store(backend), &leaf()).await;
        let e = leaf.lookup(b"k").expect("the staged lock is persisted");
        assert_eq!(e.lock_type(), LockType::Write);
        assert_eq!(e.lock_holders(), std::slice::from_ref(&tx));
    }

    #[tokio::test]
    async fn applied_evidence_keeps_its_installed_state_when_a_peer_writes_before_the_reply() {
        let memory: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let backend = HookBackend::new(memory.clone());
        let (coord, nodes, timeline, _bg) = coord_over(backend.clone()).await;
        let barrier = timeline.currentness_barrier();
        let expected = nodes
            .load_leaf(&leaf(), Requirement::ANY)
            .await
            .unwrap()
            .observation()
            .clone();
        backend.set_after(move |operation, outcome| {
            let overwrite = matches!(operation, BackendOp::WriteIf { .. })
                && operation.path() == leaf().to_string()
                && outcome.is_success();
            let peer = cold_store(memory.clone());
            let future: HookFuture = Box::pin(async move {
                if overwrite {
                    let loaded = peer.load_leaf(&leaf(), Requirement::ANY).await.unwrap();
                    let mut entries = loaded.entries().entries().cloned().collect::<Vec<_>>();
                    entries.push(entry(
                        b"peer",
                        LockType::None,
                        None,
                        Some(&TxId::with_priority(2, b"peer")),
                    ));
                    let mut edit = loaded.into_edit();
                    edit.set_entries(LeafBody::from_entries(entries));
                    assert!(peer.commit_leaf(edit).await.unwrap().is_applied());
                }
                Ok(())
            });
            future
        });

        let tx = TxId::with_priority(1, b"t");
        let outcome = coord
            .submit_leaf(
                &leaf(),
                &tx,
                Arc::new(StageLock {
                    key: b"k".to_vec(),
                    tx: tx.clone(),
                    admission: StageAdmission::ExistingKeys,
                }),
                Requirement::ANY,
            )
            .await
            .unwrap()
            .unwrap();
        let evidence = outcome.evidence.unwrap();
        assert!(matches!(&evidence, CoordinationEvidence::Installed(_)));
        assert!(evidence.validates(&expected, barrier));
        let CoordinationEvidence::Installed(receipt) = &evidence else {
            panic!("staged member must retain its CAS receipt");
        };
        let installed = receipt.installed();
        assert_ne!(installed.revision(), expected.revision());
        let entries = installed.value().unwrap().as_leaf().unwrap();
        assert_eq!(entries.lookup(b"k").unwrap().lock_holders(), &[tx]);
        assert!(entries.lookup(b"peer").is_none());

        let current = nodes
            .load_leaf(&leaf(), Requirement::after(timeline.currentness_barrier()))
            .await
            .unwrap();
        assert!(current.entries().lookup(b"peer").is_some());
        assert!(!installed.same_state(current.observation()));
        assert!(!evidence.validates(current.observation(), barrier));
        coord.close().await;
    }

    // A split can move a key to a right sibling after it was routed to this
    // leaf. The coordinator must notice the loaded leaf no longer covers the key
    // and re-route (deliver the member's re-route outcome) rather than strand a
    // fresh entry in the wrong leaf (ADR-031, M1-S2).
    #[tokio::test]
    async fn reroutes_when_a_split_moved_the_key_out_of_the_leaf() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (coord, store, _timeline, _bg) = coord_over(backend.clone()).await;

        // Seed the leaf as a shrunk left half: it covers keys < "m" and links to a
        // right sibling. "z" now lives in that sibling, not here.
        let node = Node::leaf(LeafBody::from_entries([entry(
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
            .submit_leaf(
                &leaf(),
                &tx,
                Arc::new(StageLock {
                    key: b"z".to_vec(),
                    tx: tx.clone(),
                    admission: StageAdmission::ExistingKeys,
                }),
                Requirement::ANY,
            )
            .await
            .unwrap();
        // Re-route: the acquire-shaped resolver's exhausted/re-route outcome is a
        // `Conflict`, which its caller turns into release-and-relock.
        assert!(matches!(
            out,
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::Conflict,
                evidence: Some(CoordinationEvidence::Observed(_)),
                ..
            })
        ));
        coord.close().await;

        // The wrong leaf was never mutated: "z" was not stranded here, and the
        // covered key "a" is untouched.
        let leaf = cold_entries(&cold_store(backend), &leaf()).await;
        assert!(
            leaf.lookup(b"z").is_none(),
            "moved key must not be recreated here"
        );
        assert!(leaf.lookup(b"a").is_some());
    }

    // A covered key can still be locked: the coverage re-check is transparent
    // when the leaf covers the round's keys.
    #[tokio::test]
    async fn covered_key_is_locked_despite_a_high_key() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (coord, store, _timeline, _bg) = coord_over(backend.clone()).await;

        let node = Node::leaf(LeafBody::new()).with_high_key(Some(b"m".to_vec()));
        replace_leaf_node(&store, &node).await;

        let tx = TxId::with_priority(1, b"t");
        let out = coord
            .submit_leaf(
                &leaf(),
                &tx,
                Arc::new(StageLock {
                    key: b"a".to_vec(),
                    tx: tx.clone(),
                    admission: StageAdmission::ExistingKeys,
                }),
                Requirement::ANY,
            )
            .await
            .unwrap();
        assert!(matches!(
            out,
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::Locked { .. },
                evidence: Some(CoordinationEvidence::Installed(_)),
                ..
            })
        ));
        coord.close().await;

        let leaf = cold_entries(&cold_store(backend), &leaf()).await;
        assert!(
            leaf.lookup(b"a").is_some(),
            "a covered key is locked as usual"
        );
    }

    // A resolver that stages nothing (`Skip`) still gets its outcome and the
    // loaded observation, but the round issues no CAS.
    #[tokio::test]
    async fn leaf_skip_delivers_outcome_without_cas() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        let (coord, nodes, timeline, _bg) = coord_over(backend).await;
        let barrier = timeline.currentness_barrier();
        let expected = nodes
            .load_leaf(&leaf(), Requirement::after(barrier))
            .await
            .unwrap()
            .observation()
            .clone();
        log.lock().unwrap().clear();
        let tx = TxId::with_priority(1, b"t");

        let out = coord
            .submit_leaf(&leaf(), &tx, Arc::new(SkipRelease), Requirement::ANY)
            .await
            .unwrap();
        assert!(matches!(
            &out,
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::Released { .. },
                evidence: Some(CoordinationEvidence::Observed(_)),
            })
        ));
        let evidence = out.unwrap().evidence.unwrap();
        let CoordinationEvidence::Observed(observation) = &evidence else {
            panic!("skipped member must retain its read observation");
        };
        assert!(observation.same_state(&expected));
        assert!(evidence.validates(&expected, barrier));
        assert!(!evidence.validates(&expected, timeline.currentness_barrier()));
        assert_eq!(leaf_stores(&log), 0, "a skip stages nothing, so no CAS");
        coord.close().await;
    }

    #[tokio::test]
    async fn skipped_member_keeps_read_evidence_when_another_member_applies() {
        let recorder = Arc::new(RecordingBackend::new(Arc::new(MemoryBackend::new())));
        let log = recorder.log();
        let (coord, nodes, timeline, _bg) = coord_over(recorder).await;
        let expected = nodes
            .load_leaf(&leaf(), Requirement::ANY)
            .await
            .unwrap()
            .observation()
            .clone();
        let barrier = timeline.currentness_barrier();
        let requirement = Requirement::after(barrier);
        log.lock().unwrap().clear();

        let tx = TxId::with_priority(1, b"stage");
        let skip = TxId::with_priority(2, b"skip");
        let path = leaf();
        let (staged, skipped) = tokio::join!(
            coord.submit_leaf(
                &path,
                &tx,
                Arc::new(StageLock {
                    key: b"k".to_vec(),
                    tx: tx.clone(),
                    admission: StageAdmission::ExistingKeys,
                }),
                requirement,
            ),
            coord.submit_leaf(&path, &skip, Arc::new(SkipRelease), requirement),
        );

        let applied = staged.unwrap().unwrap().evidence.unwrap();
        let observed = skipped.unwrap().unwrap().evidence.unwrap();
        assert!(matches!(&applied, CoordinationEvidence::Installed(_)));
        assert!(matches!(&observed, CoordinationEvidence::Observed(_)));
        assert!(applied.validates(&expected, barrier));
        let applied = applied.into_observation();
        let observed = observed.into_observation();
        assert!(observed.same_state(&expected));
        assert!(!applied.same_state(&observed));
        assert!(
            applied
                .value()
                .unwrap()
                .as_leaf()
                .unwrap()
                .lookup(b"k")
                .is_some()
        );
        assert_eq!(leaf_reads(&log), 0);
        assert_eq!(leaf_stores(&log), 1);
        assert_eq!(coord.stats_and_reset().rounds, 1);
        coord.close().await;
    }

    // An entry left with no holder and no committed writer is indistinguishable
    // from absent, so the plan's CAS drops it (ADR-029) while
    // keeping live pointers and newly staged locks.
    #[tokio::test]
    async fn leaf_prunes_vestigial_entries_on_cas() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (coord, nodes, _timeline, _bg) = coord_over(backend.clone()).await;
        let writer = TxId::with_priority(1, b"w");
        store_leaf_entries(
            &nodes,
            &leaf(),
            vec![
                entry(b"vestige", LockType::None, None, None),
                entry(b"live", LockType::None, None, Some(&writer)),
            ],
        )
        .await;

        let tx = TxId::with_priority(2, b"t");
        coord
            .submit_leaf(
                &leaf(),
                &tx,
                Arc::new(StageLock {
                    key: b"lock".to_vec(),
                    tx: tx.clone(),
                    admission: StageAdmission::ExistingKeys,
                }),
                Requirement::ANY,
            )
            .await
            .unwrap();
        coord.close().await;

        let leaf = cold_entries(&cold_store(backend), &leaf()).await;
        assert!(
            leaf.lookup(b"vestige").is_none(),
            "the vestigial entry is dropped by the CAS"
        );
        assert!(leaf.lookup(b"live").is_some(), "the live pointer is kept");
        assert!(
            leaf.lookup(b"lock").is_some(),
            "the newly staged lock is kept"
        );
    }

    // ADR-030 at the coordinator: a lone round's first attempt reuses the cached
    // leaf when the submitter asks for `Any` (no backend read), while a current
    // lower bound revalidates it with one conditional read.
    #[tokio::test]
    async fn any_first_attempt_reuses_cache() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);

        // Seed through a separate cache so the coordinator starts cold, then warm
        // its cache with one cold load.
        let writer = TxId::with_priority(1, b"w");
        store_leaf_entries(
            &cold_store(backend.clone()),
            &leaf(),
            vec![entry(b"seed", LockType::None, None, Some(&writer))],
        )
        .await;
        let (coord, nodes, timeline, _bg) = coord_over(backend.clone()).await;
        nodes
            .load_leaf(&leaf_path(), Requirement::ANY)
            .await
            .unwrap();

        let tx = TxId::with_priority(2, b"t");
        log.lock().unwrap().clear();
        coord
            .submit_leaf(&leaf(), &tx, Arc::new(SkipRelease), Requirement::ANY)
            .await
            .unwrap();
        assert_eq!(
            leaf_reads(&log),
            0,
            "Any serves the cached leaf with no backend read"
        );

        log.lock().unwrap().clear();
        coord
            .submit_leaf(
                &leaf(),
                &tx,
                Arc::new(SkipRelease),
                Requirement::after(timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        assert_eq!(
            leaf_reads(&log),
            1,
            "a current bound revalidates the cached leaf once"
        );
        coord.close().await;
    }

    // ADR-028: two transactions contending the same leaf merge into one round —
    // a single shared load and a single CAS — planned oldest-first, with the
    // younger member observing the older's staged entry (threading).
    #[tokio::test(start_paused = true)]
    async fn same_leaf_submits_merge_into_one_round() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (backend, gate) = Gate::wrap(mem);
        let recorder = Arc::new(RecordingBackend::new(backend));
        let log = recorder.log();
        let (coord, _nodes, _timeline, _bg) = coord_over(recorder as Arc<dyn Backend>).await;
        log.lock().unwrap().clear();

        let trace: ResolverTrace = Arc::new(Mutex::new(Vec::new()));
        let old = TxId::with_priority(1, b"old");
        let young = TxId::with_priority(2, b"young");

        // The older member submits first, becomes the dedup driver, and parks in
        // the gated load; the younger then queues into that open batch.
        gate.arm();
        let (c1, t1, tr1) = (coord.clone(), old.clone(), trace.clone());
        let driver = tokio::spawn(async move {
            c1.submit_leaf(
                &leaf(),
                &t1,
                Arc::new(Recorder {
                    key: b"a".to_vec(),
                    tx: t1.clone(),
                    trace: tr1,
                }),
                Requirement::ANY,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;

        let (c2, t2, tr2) = (coord.clone(), young.clone(), trace.clone());
        let joiner = tokio::spawn(async move {
            c2.submit_leaf(
                &leaf(),
                &t2,
                Arc::new(Recorder {
                    key: b"b".to_vec(),
                    tx: t2.clone(),
                    trace: tr2,
                }),
                Requirement::ANY,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        assert!(matches!(
            driver.await.unwrap().unwrap(),
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::Landed,
                ..
            })
        ));
        assert!(matches!(
            joiner.await.unwrap().unwrap(),
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::Landed,
                ..
            })
        ));

        assert_eq!(leaf_reads(&log), 1, "both members share one leaf load");
        assert_eq!(leaf_stores(&log), 1, "both members land in one CAS");
        coord.close().await;

        let trace = trace.lock().unwrap();
        assert_eq!(trace.len(), 2, "both resolvers are evaluated once");
        assert_eq!(trace[0].0, old, "the older member is evaluated first");
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
        let (coord, _nodes, _timeline, _bg) = coord_over(backend.clone()).await;
        let first = TxId::with_priority(1, b"first");
        let second = TxId::with_priority(2, b"second");

        // The older member drives the round and parks in the gated load; the
        // younger one queues into that still-open batch.
        gate.arm();
        let (c1, t1) = (coord.clone(), first.clone());
        let driver = tokio::spawn(async move {
            c1.submit_leaf(
                &leaf(),
                &t1,
                Arc::new(StageInline::logless(b"k", &t1, b"first")),
                Requirement::ANY,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        let (c2, t2) = (coord.clone(), second.clone());
        let joiner = tokio::spawn(async move {
            c2.submit_leaf(
                &leaf(),
                &t2,
                Arc::new(StageInline::logless(b"k", &t2, b"second")),
                Requirement::ANY,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        assert!(matches!(
            driver.await.unwrap().unwrap(),
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::Landed,
                ..
            })
        ));
        assert!(
            matches!(
                joiner.await.unwrap().unwrap(),
                Some(CoordinatedOutcome {
                    outcome: MemberOutcome::Conflict,
                    evidence: Some(CoordinationEvidence::Observed(_)),
                    ..
                })
            ),
            "the second claimant stages nothing and does not land"
        );
        coord.close().await;

        let leaf = cold_entries(&cold_store(backend), &leaf()).await;
        assert_eq!(
            leaf.lookup(b"k").unwrap().current.inline().map(|v| &**v),
            Some(b"first".as_slice()),
            "the first commit survives the round intact"
        );
    }

    // A logless direct-commit-shaped resolver (ADR-051): the entry it stages is
    // the only record of its commit, so it claims its key for the round and
    // classifies an unfinished round the way `DirectCommitOperation` does — the
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
    impl LeafResolver for LoglessCommitProbe {
        async fn resolve(
            &self,
            _ctx: &ResolveCtx<'_>,
            _staged: &BTreeMap<Vec<u8>, LeafEntry>,
            staged_locks: &NodeLocks,
        ) -> Result<Step, TransError> {
            let e = LeafEntry::new(self.key.clone()).with_current(CurrentState::Inline {
                writer: self.tx.clone(),
                value: self.value.clone(),
            });
            Ok(Step::Stage {
                entries: vec![(self.key.clone(), e)],
                locks: staged_locks.clone(),
                admission: StageAdmission::ExistingKeys,
                outcome: MemberOutcome::Landed,
            })
        }

        fn reorderable(&self) -> bool {
            false
        }

        fn exhausted_outcome(&self, in_doubt: bool) -> MemberOutcome {
            if in_doubt {
                return MemberOutcome::InDoubt("logless commit after an uncertain CAS".into());
            }
            MemberOutcome::Moved
        }

        fn excluded_outcome(&self, in_doubt: bool) -> MemberOutcome {
            if in_doubt {
                return MemberOutcome::InDoubt("logless commit after an uncertain CAS".into());
            }
            if self.replayable {
                return MemberOutcome::Replay;
            }
            MemberOutcome::Moved
        }

        fn leaf_scope_keys(&self) -> Vec<&[u8]> {
            vec![self.key.as_slice()]
        }

        fn logless_publication_keys(&self) -> Vec<&[u8]> {
            vec![self.key.as_slice()]
        }
    }

    struct MultiPublisherProbe {
        keys: Vec<Vec<u8>>,
        tx: TxId,
        logless: bool,
        already_landed: bool,
    }

    impl MultiPublisherProbe {
        fn direct(keys: &[&[u8]], tx: &TxId) -> Self {
            Self {
                keys: keys.iter().map(|key| key.to_vec()).collect(),
                tx: tx.clone(),
                logless: true,
                already_landed: false,
            }
        }

        fn publisher(keys: &[&[u8]], tx: &TxId) -> Self {
            Self {
                keys: keys.iter().map(|key| key.to_vec()).collect(),
                tx: tx.clone(),
                logless: false,
                already_landed: false,
            }
        }

        fn landed(keys: &[&[u8]], tx: &TxId) -> Self {
            Self {
                already_landed: true,
                ..Self::direct(keys, tx)
            }
        }
    }

    #[async_trait::async_trait]
    impl LeafResolver for MultiPublisherProbe {
        async fn resolve(
            &self,
            _ctx: &ResolveCtx<'_>,
            _staged: &BTreeMap<Vec<u8>, LeafEntry>,
            staged_locks: &NodeLocks,
        ) -> Result<Step, TransError> {
            if self.already_landed {
                return Ok(Step::Skip {
                    outcome: MemberOutcome::Landed,
                });
            }
            let entries = self
                .keys
                .iter()
                .map(|key| {
                    let entry = LeafEntry::new(key.clone()).with_current(CurrentState::Inline {
                        writer: self.tx.clone(),
                        value: Arc::from(self.tx.as_bytes()),
                    });
                    (key.clone(), entry)
                })
                .collect();
            Ok(Step::Stage {
                entries,
                locks: staged_locks.clone(),
                admission: StageAdmission::ExistingKeys,
                outcome: MemberOutcome::Landed,
            })
        }

        fn reorderable(&self) -> bool {
            false
        }

        fn exhausted_outcome(&self, in_doubt: bool) -> MemberOutcome {
            if in_doubt && self.logless {
                MemberOutcome::InDoubt("multi-key logless probe is uncertain".into())
            } else if self.logless {
                MemberOutcome::Moved
            } else {
                MemberOutcome::Reroute
            }
        }

        fn leaf_scope_keys(&self) -> Vec<&[u8]> {
            self.keys.iter().map(Vec::as_slice).collect()
        }

        fn logless_publication_keys(&self) -> Vec<&[u8]> {
            if self.logless {
                self.keys.iter().map(Vec::as_slice).collect()
            } else {
                Vec::new()
            }
        }

        fn publication_keys(&self) -> Vec<&[u8]> {
            self.keys.iter().map(Vec::as_slice).collect()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn overlapping_multi_key_publisher_is_excluded_as_a_whole() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (backend, gate) = Gate::wrap(mem);
        let backend = backend as Arc<dyn Backend>;
        let recording = Arc::new(RecordingBackend::new(backend.clone()));
        let log = recording.log();
        let (coord, _nodes, _timeline, _bg) = coord_over(recording.clone()).await;
        let first = TxId::with_priority(1, b"first");
        let second = TxId::with_priority(2, b"second");
        log.lock().unwrap().clear();

        gate.arm();
        let (c1, t1) = (coord.clone(), first.clone());
        let driver = tokio::spawn(async move {
            c1.submit_leaf(
                &leaf(),
                &t1,
                Arc::new(MultiPublisherProbe::direct(&[b"a", b"b"], &t1)),
                Requirement::ANY,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        let (c2, t2) = (coord.clone(), second.clone());
        let joiner = tokio::spawn(async move {
            c2.submit_leaf(
                &leaf(),
                &t2,
                Arc::new(MultiPublisherProbe::publisher(&[b"b", b"c"], &t2)),
                Requirement::ANY,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        assert!(matches!(
            driver.await.unwrap().unwrap(),
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::Landed,
                ..
            })
        ));
        let joined = joiner.await.unwrap().unwrap();
        let expected = matches!(
            &joined,
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::Reroute,
                evidence: Some(CoordinationEvidence::Observed(_)),
                ..
            })
        );
        if !expected {
            match joined {
                Some(outcome) => panic!("unexpected publisher outcome: {:?}", outcome.outcome),
                None => panic!("publisher received no outcome"),
            }
        }
        assert_eq!(leaf_stores(&log), 1);
        coord.close().await;

        let leaf = cold_entries(&cold_store(backend), &leaf()).await;
        assert_eq!(leaf.lookup(b"a").unwrap().current.writer(), Some(&first));
        assert_eq!(leaf.lookup(b"b").unwrap().current.writer(), Some(&first));
        assert!(leaf.lookup(b"c").is_none(), "the loser staged no subset");
    }

    #[tokio::test(start_paused = true)]
    async fn disjoint_multi_key_logless_members_share_one_cas() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (backend, gate) = Gate::wrap(mem);
        let backend = backend as Arc<dyn Backend>;
        let recording = Arc::new(RecordingBackend::new(backend));
        let log = recording.log();
        let (coord, _nodes, _timeline, _bg) = coord_over(recording).await;
        let first = TxId::with_priority(1, b"first");
        let second = TxId::with_priority(2, b"second");
        log.lock().unwrap().clear();

        gate.arm();
        let (c1, t1) = (coord.clone(), first.clone());
        let driver = tokio::spawn(async move {
            c1.submit_leaf(
                &leaf(),
                &t1,
                Arc::new(MultiPublisherProbe::direct(&[b"a", b"b"], &t1)),
                Requirement::ANY,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        let (c2, t2) = (coord.clone(), second.clone());
        let joiner = tokio::spawn(async move {
            c2.submit_leaf(
                &leaf(),
                &t2,
                Arc::new(MultiPublisherProbe::direct(&[b"c", b"d"], &t2)),
                Requirement::ANY,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        for outcome in [
            driver.await.unwrap().unwrap(),
            joiner.await.unwrap().unwrap(),
        ] {
            assert!(matches!(
                outcome,
                Some(CoordinatedOutcome {
                    outcome: MemberOutcome::Landed,
                    ..
                })
            ));
        }
        assert_eq!(leaf_stores(&log), 1);
        coord.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn observed_logless_marker_protects_later_publishers() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (backend, gate) = Gate::wrap(mem);
        let backend = backend as Arc<dyn Backend>;
        let (coord, _nodes, _timeline, _bg) = coord_over(backend.clone()).await;
        let first = TxId::with_priority(1, b"first");
        let second = TxId::with_priority(2, b"second");
        let seed_store = cold_store(backend.clone());
        store_leaf_entries(
            &seed_store,
            &leaf(),
            [b"a".as_slice(), b"b".as_slice()]
                .into_iter()
                .map(|key| {
                    LeafEntry::new(key).with_current(CurrentState::Inline {
                        writer: first.clone(),
                        value: Arc::from(b"landed".as_slice()),
                    })
                })
                .collect(),
        )
        .await;

        gate.arm();
        let (c1, t1) = (coord.clone(), first.clone());
        let driver = tokio::spawn(async move {
            c1.submit_leaf(
                &leaf(),
                &t1,
                Arc::new(MultiPublisherProbe::landed(&[b"a", b"b"], &t1)),
                Requirement::ANY,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        let (c2, t2) = (coord.clone(), second.clone());
        let joiner = tokio::spawn(async move {
            c2.submit_leaf(
                &leaf(),
                &t2,
                Arc::new(MultiPublisherProbe::publisher(&[b"b", b"c"], &t2)),
                Requirement::ANY,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        assert!(matches!(
            driver.await.unwrap().unwrap(),
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::Landed,
                evidence: Some(CoordinationEvidence::Observed(_)),
                ..
            })
        ));
        let joined = joiner.await.unwrap().unwrap();
        let expected = matches!(
            &joined,
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::Reroute,
                evidence: Some(CoordinationEvidence::Observed(_)),
                ..
            })
        );
        if !expected {
            match joined {
                Some(outcome) => panic!("unexpected publisher outcome: {:?}", outcome.outcome),
                None => panic!("publisher received no outcome"),
            }
        }
        coord.close().await;

        let leaf = cold_entries(&cold_store(backend), &leaf()).await;
        assert_eq!(leaf.lookup(b"b").unwrap().current.writer(), Some(&first));
        assert!(leaf.lookup(b"c").is_none());
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
    impl LeafResolver for SkipCauseProbe {
        async fn resolve(
            &self,
            ctx: &ResolveCtx<'_>,
            _staged: &BTreeMap<Vec<u8>, LeafEntry>,
            _staged_locks: &NodeLocks,
        ) -> Result<Step, TransError> {
            let in_doubt = matches!(ctx.cause, ReloadCause::Reloaded { in_doubt: true });
            let outcome = if in_doubt {
                MemberOutcome::InDoubt("uncertain CAS attributed to skipped member".into())
            } else {
                MemberOutcome::Moved
            };
            Ok(Step::Skip { outcome })
        }

        fn reorderable(&self) -> bool {
            false
        }

        fn exhausted_outcome(&self, in_doubt: bool) -> MemberOutcome {
            if in_doubt {
                MemberOutcome::InDoubt("uncertain CAS attributed to skipped member".into())
            } else {
                MemberOutcome::Moved
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
        let (coord, _nodes, _timeline, _bg) = coord_over(backend.clone()).await;
        let first = TxId::with_priority(1, b"first");
        let second = TxId::with_priority(2, b"second");

        // The older member drives the round and parks in the gated load; the
        // younger one queues into that still-open batch, where its key is
        // already claimed.
        gate.arm();
        let (c1, t1) = (coord.clone(), first.clone());
        let driver = tokio::spawn(async move {
            c1.submit_leaf(
                &leaf(),
                &t1,
                Arc::new(LoglessCommitProbe::new(b"k", &t1, b"first")),
                Requirement::ANY,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        let (c2, t2) = (coord.clone(), second.clone());
        let joiner = tokio::spawn(async move {
            c2.submit_leaf(
                &leaf(),
                &t2,
                Arc::new(LoglessCommitProbe::new(b"k", &t2, b"second")),
                Requirement::ANY,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        assert!(
            matches!(
                driver.await.unwrap().unwrap(),
                Some(CoordinatedOutcome {
                    outcome: MemberOutcome::Landed,
                    ..
                })
            ),
            "the member whose CAS was retried lands on the second attempt"
        );
        assert!(
            matches!(
                joiner.await.unwrap().unwrap(),
                Some(CoordinatedOutcome {
                    outcome: MemberOutcome::Moved,
                    evidence: Some(CoordinationEvidence::Observed(_)),
                    ..
                })
            ),
            "the skipped member never staged, so its loss stays definitive"
        );
        coord.close().await;
    }

    // ADR-053: a same-key claim is reported through `excluded_outcome`, not
    // `exhausted_outcome`. The distinction is load-bearing — the claim proves the
    // excluded member staged nothing at all, while a spent CAS budget proves
    // nothing about an earlier attempt — so a read-modify-write shaped member
    // learns a *replayable* loss where an exhausted round would only tell it the
    // entry moved.
    #[tokio::test(start_paused = true)]
    async fn an_excluded_logless_member_learns_a_replayable_loss() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (backend, gate) = Gate::wrap(mem);
        let (coord, _nodes, _timeline, _bg) = coord_over(backend as Arc<dyn Backend>).await;
        let first = TxId::with_priority(1, b"first");
        let second = TxId::with_priority(2, b"second");

        gate.arm();
        let (c1, t1) = (coord.clone(), first.clone());
        let driver = tokio::spawn(async move {
            c1.submit_leaf(
                &leaf(),
                &t1,
                Arc::new(LoglessCommitProbe::replayable(b"k", &t1, b"first")),
                Requirement::ANY,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        let (c2, t2) = (coord.clone(), second.clone());
        let joiner = tokio::spawn(async move {
            c2.submit_leaf(
                &leaf(),
                &t2,
                Arc::new(LoglessCommitProbe::replayable(b"k", &t2, b"second")),
                Requirement::ANY,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        assert!(matches!(
            driver.await.unwrap().unwrap(),
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::Landed,
                ..
            })
        ));
        assert!(
            matches!(
                joiner.await.unwrap().unwrap(),
                Some(CoordinatedOutcome {
                    outcome: MemberOutcome::Replay,
                    evidence: Some(CoordinationEvidence::Observed(_)),
                    ..
                })
            ),
            "the excluded member staged nothing, so its loss is replayable"
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
        let (coord, _nodes, _timeline, _bg) = coord_over(backend).await;
        let first = TxId::with_priority(1, b"first");
        let second = TxId::with_priority(2, b"second");

        gate.arm();
        let (c1, t1) = (coord.clone(), first.clone());
        let driver = tokio::spawn(async move {
            c1.submit_leaf(
                &leaf(),
                &t1,
                Arc::new(LoglessCommitProbe::replayable(b"k", &t1, b"first")),
                Requirement::ANY,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        let (c2, t2) = (coord.clone(), second.clone());
        let joiner = tokio::spawn(async move {
            c2.submit_leaf(
                &leaf(),
                &t2,
                Arc::new(LoglessCommitProbe::replayable(b"k", &t2, b"second")),
                Requirement::ANY,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        assert!(
            matches!(
                driver.await.unwrap().unwrap(),
                Some(CoordinatedOutcome {
                    outcome: MemberOutcome::Landed,
                    ..
                })
            ),
            "the member whose CAS was retried lands on the second attempt"
        );
        assert!(
            matches!(
                joiner.await.unwrap().unwrap(),
                Some(CoordinatedOutcome {
                    outcome: MemberOutcome::Replay,
                    evidence: Some(CoordinationEvidence::Observed(_)),
                    ..
                })
            ),
            "the skipped member issued no write, so it replays rather than doubting"
        );
        coord.close().await;
    }

    // Regression (fuzz `concurrent-tx`,
    // corpus/cd4e97be8a631c59fe32bc49de539f38056bcb40): one transaction can have
    // two operations in flight on the same leaf at once — GC releasing a
    // presumed-dead transaction's holds (ADR-029) while that transaction's own
    // acquire is still resolving on the same object (ADR-025). Both submissions
    // carry their own outcome slot, but a coordinator round runs one resolver per id and
    // the dedup delivers to every merged submission; merging them would collapse
    // the two slots into one and leave the loser a delivered-but-empty slot. The
    // coordinator must instead serialize same-identity submissions into separate rounds
    // so each gets its own outcome rather than panicking.
    #[tokio::test(start_paused = true)]
    async fn same_tx_concurrent_submits_each_get_an_outcome() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (backend, gate) = Gate::wrap(mem);
        let (coord, _nodes, _timeline, _bg) = coord_over(backend as Arc<dyn Backend>).await;
        let tx = TxId::with_priority(1, b"t");

        // The acquire submits first, becomes the dedup driver, and parks in the
        // gated load; the release for the same id then arrives for the same leaf.
        gate.arm();
        let (c1, t1) = (coord.clone(), tx.clone());
        let acquire = tokio::spawn(async move {
            c1.submit_leaf(
                &leaf(),
                &t1,
                Arc::new(StageLock {
                    key: b"k".to_vec(),
                    tx: t1.clone(),
                    admission: StageAdmission::ExistingKeys,
                }),
                Requirement::ANY,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;

        let (c2, t2) = (coord.clone(), tx.clone());
        let release = tokio::spawn(async move {
            c2.submit_leaf(&leaf(), &t2, Arc::new(SkipRelease), Requirement::ANY)
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
                outcome: MemberOutcome::Locked { .. },
                ..
            })
        ));
        assert!(matches!(
            release.await.unwrap().unwrap(),
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::Released { .. },
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
        overwritten.replace_write_lock(old.clone());
        let created = entry(b"z", LockType::Create, Some(&young), None);

        let base_len = Node::leaf(LeafBody::from_entries([seed.clone()])).content_encoded_len();
        let overwrite_len =
            Node::leaf(LeafBody::from_entries([overwritten.clone()])).content_encoded_len();
        let full_node = Node::leaf(LeafBody::from_entries([overwritten, created]));
        let content_limit = overwrite_len - 1;
        assert!(base_len <= content_limit);
        assert!(overwrite_len > content_limit);
        assert!(full_node.content_encoded_len() > content_limit);

        let node_max_bytes = full_node.encoded_len() + 64;
        let policy = SplitPolicy::builder()
            .node_max_bytes(node_max_bytes)
            .split_headroom_bytes(node_max_bytes - content_limit)
            .build()
            .unwrap();
        let hints = Arc::new(HintCounter::default());
        let (coord, nodes, _timeline, _bg) =
            coord_over_with(backend.clone(), policy, hints.clone()).await;
        store_leaf_entries(&nodes, &leaf(), vec![seed]).await;
        log.lock().unwrap().clear();

        gate.arm();
        let (c1, t1) = (coord.clone(), old.clone());
        let overwrite = tokio::spawn(async move {
            c1.submit_leaf(
                &leaf(),
                &t1,
                Arc::new(StageLock {
                    key: b"a".to_vec(),
                    tx: t1.clone(),
                    admission: StageAdmission::ExistingKeys,
                }),
                Requirement::ANY,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;

        let (c2, t2) = (coord.clone(), young.clone());
        let create = tokio::spawn(async move {
            c2.submit_leaf(
                &leaf(),
                &t2,
                Arc::new(StageLock {
                    key: b"z".to_vec(),
                    tx: t2.clone(),
                    admission: StageAdmission::AddsKey,
                }),
                Requirement::ANY,
            )
            .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        assert!(matches!(
            overwrite.await.unwrap().unwrap(),
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::Locked { .. },
                evidence: Some(CoordinationEvidence::Installed(_)),
                ..
            })
        ));
        assert!(matches!(
            create.await.unwrap().unwrap(),
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::LeafFull,
                evidence: Some(CoordinationEvidence::Observed(_)),
                ..
            })
        ));
        assert_eq!(leaf_stores(&log), 1, "the admitted member still lands");
        assert_eq!(
            hints.calls.load(Ordering::SeqCst),
            2,
            "one hint follows the admitted store and one re-hints the rejected create"
        );
        coord.close().await;

        let leaf = cold_entries(&cold_store(backend), &leaf()).await;
        assert_eq!(
            leaf.lookup(b"a").unwrap().lock_holders(),
            std::slice::from_ref(&old)
        );
        assert!(
            leaf.lookup(b"z").is_none(),
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
    impl LeafResolver for StageInline {
        async fn resolve(
            &self,
            _ctx: &ResolveCtx<'_>,
            _staged: &BTreeMap<Vec<u8>, LeafEntry>,
            staged_locks: &NodeLocks,
        ) -> Result<Step, TransError> {
            let e = LeafEntry::new(self.key.clone()).with_current(CurrentState::Inline {
                writer: self.tx.clone(),
                value: self.value.clone(),
            });
            Ok(Step::Stage {
                entries: vec![(self.key.clone(), e)],
                locks: staged_locks.clone(),
                admission: StageAdmission::InlinePublication {
                    adds_key: false,
                    pressure_hint: true,
                },
                outcome: MemberOutcome::Landed,
            })
        }

        fn reorderable(&self) -> bool {
            true
        }

        fn exhausted_outcome(&self, _in_doubt: bool) -> MemberOutcome {
            MemberOutcome::Conflict
        }

        fn logless_publication_keys(&self) -> Vec<&[u8]> {
            vec![self.key.as_slice()]
        }
    }

    // A policy whose hard cap admits an external pointer for `key` but not the
    // same entry carrying `value` inline.
    fn policy_rejecting_inline(key: &[u8], tx: &TxId, value: &[u8]) -> SplitPolicy {
        let external =
            LeafEntry::new(key).with_current(CurrentState::External { writer: tx.clone() });
        let inline = LeafEntry::new(key).with_current(CurrentState::Inline {
            writer: tx.clone(),
            value: Arc::from(value),
        });
        let external_len = Node::leaf(LeafBody::from_entries([external])).encoded_len();
        let inline_len = Node::leaf(LeafBody::from_entries([inline])).encoded_len();
        assert!(
            inline_len > external_len,
            "the inline payload must add bytes"
        );
        SplitPolicy::builder()
            .node_max_bytes(external_len)
            .split_headroom_bytes(0)
            .build()
            .unwrap()
    }

    // A logless commit's leaf entry is the value's only copy, so an over-cap
    // stage must be rejected rather than silently losing the value.
    #[tokio::test]
    async fn an_oversized_logless_inline_payload_is_rejected() {
        let tx = TxId::with_priority(1, b"t");
        let value = b"a-value-that-does-not-fit";
        let policy = policy_rejecting_inline(b"k", &tx, value);
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (coord, _nodes, _timeline, _bg) =
            coord_over_with(backend.clone(), policy, Arc::new(NoSplitHints)).await;

        let outcome = coord
            .submit_leaf(
                &leaf(),
                &tx,
                Arc::new(StageInline::logless(b"k", &tx, value)),
                Requirement::ANY,
            )
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::Conflict,
                evidence: Some(CoordinationEvidence::Observed(_)),
                ..
            })
        ));
        coord.close().await;

        let leaf = cold_entries(&cold_store(backend), &leaf()).await;
        assert!(leaf.lookup(b"k").is_none(), "nothing was written");
    }

    // An inline entry may fit the physical object while still consuming more
    // than its half of the content budget. Publishing it would let a later
    // accepted key strand this leaf as an unsplittable singleton, so the direct
    // attempt must fall back without issuing a futile split hint.
    #[tokio::test]
    async fn a_logless_inline_entry_must_preserve_the_split_budget() {
        let tx = TxId::with_priority(1, b"t");
        let value = b"inline";
        let inline = LeafEntry::new(b"k").with_current(CurrentState::Inline {
            writer: tx.clone(),
            value: Arc::from(value.as_slice()),
        });
        let entry_len = Node::leaf(LeafBody::from_entries([inline.clone()])).content_encoded_len();
        let policy = SplitPolicy::builder()
            .node_max_bytes(entry_len * 2 + 64)
            .split_headroom_bytes(65)
            .build()
            .unwrap();
        assert!(
            Node::leaf(LeafBody::from_entries([inline])).encoded_len() <= policy.node_max_bytes()
        );
        assert!(
            !policy.entry_fits_split_budget(&LeafEntry::new(b"k").with_current(
                CurrentState::Inline {
                    writer: tx.clone(),
                    value: Arc::from(value.as_slice()),
                },
            ))
        );

        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let hints = Arc::new(HintCounter::default());
        let (coord, _nodes, _timeline, _bg) =
            coord_over_with(backend.clone(), policy, hints.clone()).await;
        let outcome = coord
            .submit_leaf(
                &leaf(),
                &tx,
                Arc::new(StageInline::logless(b"k", &tx, value)),
                Requirement::ANY,
            )
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::Conflict,
                evidence: Some(CoordinationEvidence::Observed(_)),
                ..
            })
        ));
        assert_eq!(hints.calls.load(Ordering::SeqCst), 0);
        coord.close().await;

        let leaf = cold_entries(&cold_store(backend), &leaf()).await;
        assert!(leaf.lookup(b"k").is_none(), "nothing was written");
    }

    struct CapacityAfterInDoubt {
        key: Vec<u8>,
        tx: TxId,
        evaluations: std::sync::atomic::AtomicUsize,
        recovers_non_landing: bool,
    }

    #[async_trait]
    impl LeafResolver for CapacityAfterInDoubt {
        async fn resolve(
            &self,
            ctx: &ResolveCtx<'_>,
            _staged: &BTreeMap<Vec<u8>, LeafEntry>,
            staged_locks: &NodeLocks,
        ) -> Result<Step, TransError> {
            let evaluation = self.evaluations.fetch_add(1, Ordering::SeqCst);
            let in_doubt = matches!(ctx.cause, ReloadCause::Reloaded { in_doubt: true });
            if in_doubt && !self.recovers_non_landing {
                return Ok(Step::Skip {
                    outcome: MemberOutcome::InDoubt(
                        "capacity changed after an unreconciled CAS".into(),
                    ),
                });
            }
            let value: Arc<[u8]> = if evaluation == 0 {
                Arc::from(b"x".as_slice())
            } else {
                Arc::from(vec![b'x'; 128])
            };
            let entry = LeafEntry::new(self.key.clone()).with_current(CurrentState::Inline {
                writer: self.tx.clone(),
                value,
            });
            Ok(Step::Stage {
                entries: vec![(self.key.clone(), entry)],
                locks: staged_locks.clone(),
                admission: StageAdmission::ExistingKeys,
                outcome: MemberOutcome::Landed,
            })
        }

        fn reorderable(&self) -> bool {
            false
        }

        fn exhausted_outcome(&self, in_doubt: bool) -> MemberOutcome {
            if in_doubt {
                MemberOutcome::InDoubt("capacity changed after in-doubt CAS".into())
            } else {
                MemberOutcome::Moved
            }
        }
    }

    // An unresolved member must decline to propose a replacement stage. Its
    // uncertainty belongs only to the member carried by the failed CAS;
    // clouding a co-batched member that never staged would manufacture
    // ambiguity for a write it never issued.
    #[tokio::test(start_paused = true)]
    async fn unreconciled_member_does_not_restage_after_in_doubt() {
        let tx = TxId::with_priority(1, b"t");
        let skipped = TxId::with_priority(2, b"skipped");
        let small = LeafEntry::new(b"k").with_current(CurrentState::Inline {
            writer: tx.clone(),
            value: Arc::from(b"x".as_slice()),
        });
        let large = LeafEntry::new(b"k").with_current(CurrentState::Inline {
            writer: tx.clone(),
            value: Arc::from(vec![b'x'; 128]),
        });
        let small_len = Node::leaf(LeafBody::from_entries([small])).encoded_len();
        let large_len = Node::leaf(LeafBody::from_entries([large])).encoded_len();
        assert!(large_len > small_len);
        let policy = SplitPolicy::builder()
            .node_max_bytes(small_len)
            .split_headroom_bytes(0)
            .build()
            .unwrap();

        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (gated, gate) = Gate::wrap(mem);
        let hooked = Arc::new(HookBackend::new(gated as Arc<dyn Backend>));
        let backend: Arc<dyn Backend> = hooked.clone();
        let (coord, _nodes, _timeline, _bg) =
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
                .submit_leaf(
                    &leaf(),
                    &driver_tx,
                    Arc::new(CapacityAfterInDoubt {
                        key: b"k".to_vec(),
                        tx: driver_tx.clone(),
                        evaluations: std::sync::atomic::AtomicUsize::new(0),
                        recovers_non_landing: false,
                    }),
                    Requirement::ANY,
                )
                .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        let (joiner_coord, joiner_tx) = (coord.clone(), skipped.clone());
        let joiner = tokio::spawn(async move {
            joiner_coord
                .submit_leaf(
                    &leaf(),
                    &joiner_tx,
                    Arc::new(SkipCauseProbe),
                    Requirement::ANY,
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
                outcome: MemberOutcome::InDoubt(_),
                evidence: Some(CoordinationEvidence::Observed(_)),
                ..
            })
        ));
        assert!(matches!(
            skipped_outcome,
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::Moved,
                evidence: Some(CoordinationEvidence::Observed(_)),
                ..
            })
        ));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the unreconciled member issued no replacement CAS"
        );
        coord.close().await;
    }

    #[tokio::test]
    async fn proven_non_landing_clears_uncertainty_before_capacity_rejection() {
        let tx = TxId::with_priority(1, b"t");
        let small = LeafEntry::new(b"k").with_current(CurrentState::Inline {
            writer: tx.clone(),
            value: Arc::from(b"x".as_slice()),
        });
        let large = LeafEntry::new(b"k").with_current(CurrentState::Inline {
            writer: tx.clone(),
            value: Arc::from(vec![b'x'; 128]),
        });
        let small_len = Node::leaf(LeafBody::from_entries([small])).encoded_len();
        assert!(Node::leaf(LeafBody::from_entries([large])).encoded_len() > small_len);
        let policy = SplitPolicy::builder()
            .node_max_bytes(small_len)
            .split_headroom_bytes(0)
            .build()
            .unwrap();

        let hooked = Arc::new(HookBackend::new(Arc::new(MemoryBackend::new())));
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
                            "simulated non-landing unavailable CAS".into(),
                        ))
                    }
                    _ => Ok(()),
                };
                let future: HookFuture = Box::pin(async move { result });
                future
            }
        });
        let backend: Arc<dyn Backend> = hooked;
        let (coord, _nodes, _timeline, _bg) =
            coord_over_with(backend, policy, Arc::new(NoSplitHints)).await;

        let outcome = coord
            .submit_leaf(
                &leaf(),
                &tx,
                Arc::new(CapacityAfterInDoubt {
                    key: b"k".to_vec(),
                    tx: tx.clone(),
                    evaluations: std::sync::atomic::AtomicUsize::new(0),
                    recovers_non_landing: true,
                }),
                Requirement::ANY,
            )
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::Conflict,
                evidence: Some(CoordinationEvidence::Observed(_)),
                ..
            })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        coord.close().await;
    }

    // A submit after shutdown is a cancelled no-op (`Ok(None)`), so best-effort
    // callers treat it as done and acquirers can distinguish it.
    #[tokio::test]
    async fn submit_after_close_is_cancelled() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (coord, _nodes, _timeline, _bg) = coord_over(backend).await;
        coord.close().await;

        let tx = TxId::with_priority(1, b"t");
        let out = coord
            .submit_leaf(&leaf(), &tx, Arc::new(SkipRelease), Requirement::ANY)
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

    // A commit-shaped resolver that stages once, then refuses to restage until
    // its uncertain CAS can be reconciled. Records the later evaluation's cause so
    // tests can pin the coordinator's sticky attribution.
    struct StickyCommitProbe {
        key: Vec<u8>,
        tx: TxId,
        evaluations: std::sync::atomic::AtomicUsize,
        seen_in_doubt: Arc<Mutex<Option<bool>>>,
    }

    #[async_trait::async_trait]
    impl LeafResolver for StickyCommitProbe {
        async fn resolve(
            &self,
            ctx: &ResolveCtx<'_>,
            _staged: &BTreeMap<Vec<u8>, LeafEntry>,
            staged_locks: &NodeLocks,
        ) -> Result<Step, TransError> {
            if self.evaluations.fetch_add(1, Ordering::SeqCst) == 0 {
                return Ok(Step::Stage {
                    entries: vec![(
                        self.key.clone(),
                        entry(&self.key, LockType::Write, Some(&self.tx), None),
                    )],
                    locks: staged_locks.clone(),
                    admission: StageAdmission::ExistingKeys,
                    outcome: MemberOutcome::Landed,
                });
            }
            let in_doubt = matches!(ctx.cause, ReloadCause::Reloaded { in_doubt: true });
            *self.seen_in_doubt.lock().unwrap() = Some(in_doubt);
            let outcome = if in_doubt {
                MemberOutcome::InDoubt("lost race after in-doubt CAS".into())
            } else {
                MemberOutcome::Moved
            };
            Ok(Step::Skip { outcome })
        }

        fn reorderable(&self) -> bool {
            false
        }

        fn exhausted_outcome(&self, in_doubt: bool) -> MemberOutcome {
            if in_doubt {
                MemberOutcome::InDoubt("round ended after in-doubt CAS".into())
            } else {
                MemberOutcome::Moved
            }
        }

        fn leaf_scope_keys(&self) -> Vec<&[u8]> {
            vec![self.key.as_slice()]
        }
    }

    // Regression (logless commit double-apply): once any CAS in a round comes
    // back in-doubt, its write may have landed durably and been help-forwarded to
    // a peer, so the in-doubt classification must stay *sticky* across a later
    // precondition-miss. Otherwise a commit that landed-but-unacked and was then
    // superseded is misclassified `Moved`, and its caller executes a
    // non-idempotent transaction body again after a peer observed its write.
    // This breaks the `final <= started` serializability bound.
    //
    // This pins the coordinator half of the fix in isolation: the uncertain
    // member declines to restage while an idempotent peer drives the later CAS.
    // The *end-to-end* manifestation (a real commit being interrupted and
    // double-applying under the true 3-way co-batched interleaving) is covered
    // deterministically by the committed fuzz reproducer
    // `fuzz/corpus/concurrent-tx/crash-95084997…`, which the corpus-replay test
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
        store_leaf_entries(
            &cold_store(mem.clone()),
            &leaf(),
            vec![entry(b"seed", LockType::None, None, Some(&seed))],
        )
        .await;
        let (gated, gate) = Gate::wrap(mem);
        let backend: Arc<dyn Backend> = in_doubt_then_miss(gated as Arc<dyn Backend>);
        let (coord, _nodes, _timeline, _bg) = coord_over(backend).await;

        let tx = TxId::with_priority(2, b"install");
        let retrying = TxId::with_priority(3, b"retrying");
        let seen_in_doubt = Arc::new(Mutex::new(None));
        gate.arm();
        let (driver_coord, driver_tx, driver_seen) =
            (coord.clone(), tx.clone(), seen_in_doubt.clone());
        let driver = tokio::spawn(async move {
            driver_coord
                .submit_leaf(
                    &leaf(),
                    &driver_tx,
                    Arc::new(StickyCommitProbe {
                        key: b"k".to_vec(),
                        tx: driver_tx.clone(),
                        evaluations: std::sync::atomic::AtomicUsize::new(0),
                        seen_in_doubt: driver_seen,
                    }),
                    Requirement::ANY,
                )
                .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        let (joiner_coord, joiner_tx) = (coord.clone(), retrying.clone());
        let joiner = tokio::spawn(async move {
            joiner_coord
                .submit_leaf(
                    &leaf(),
                    &joiner_tx,
                    Arc::new(AlwaysStageProbe {
                        key: b"peer".to_vec(),
                        tx: joiner_tx.clone(),
                    }),
                    Requirement::ANY,
                )
                .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        let out = driver.await.unwrap().unwrap();
        let retrying_outcome = joiner.await.unwrap().unwrap();

        assert_eq!(
            *seen_in_doubt.lock().unwrap(),
            Some(true),
            "the precondition-miss after an in-doubt CAS must keep the cause in-doubt"
        );
        assert!(
            matches!(
                out,
                Some(CoordinatedOutcome {
                    outcome: MemberOutcome::InDoubt(_),
                    ..
                })
            ),
            "a landed-but-unacked CAS that is then superseded must classify InDoubt, \
             not Moved (else the caller executes again and double-applies)"
        );
        assert!(matches!(
            retrying_outcome,
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::Landed,
                ..
            })
        ));
        coord.close().await;
    }

    // An idempotent resolver that can safely acknowledge uncertainty by
    // proposing the same state again.
    struct AlwaysStageProbe {
        key: Vec<u8>,
        tx: TxId,
    }

    #[async_trait::async_trait]
    impl LeafResolver for AlwaysStageProbe {
        async fn resolve(
            &self,
            _ctx: &ResolveCtx<'_>,
            _staged: &BTreeMap<Vec<u8>, LeafEntry>,
            staged_locks: &NodeLocks,
        ) -> Result<Step, TransError> {
            Ok(Step::Stage {
                entries: vec![(
                    self.key.clone(),
                    entry(&self.key, LockType::Write, Some(&self.tx), None),
                )],
                locks: staged_locks.clone(),
                admission: StageAdmission::ExistingKeys,
                outcome: MemberOutcome::Landed,
            })
        }

        fn reorderable(&self) -> bool {
            false
        }

        fn exhausted_outcome(&self, in_doubt: bool) -> MemberOutcome {
            if in_doubt {
                MemberOutcome::InDoubt("round ended after in-doubt CAS".into())
            } else {
                MemberOutcome::Moved
            }
        }

        fn leaf_scope_keys(&self) -> Vec<&[u8]> {
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
    #[tokio::test(start_paused = true)]
    async fn exhausted_budget_after_in_doubt_cas_stays_in_doubt() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let seed = TxId::with_priority(1, b"seed");
        store_leaf_entries(
            &cold_store(mem.clone()),
            &leaf(),
            vec![entry(b"seed", LockType::None, None, Some(&seed))],
        )
        .await;
        let (gated, gate) = Gate::wrap(mem);
        let backend: Arc<dyn Backend> = in_doubt_then_miss_forever(gated as Arc<dyn Backend>);
        let (coord, _nodes, _timeline, _bg) = coord_over_fast(backend).await;

        let uncertain = TxId::with_priority(2, b"uncertain");
        let retrying = TxId::with_priority(3, b"retrying");
        let seen_in_doubt = Arc::new(Mutex::new(None));
        gate.arm();
        let (driver_coord, driver_tx, driver_seen) =
            (coord.clone(), uncertain.clone(), seen_in_doubt.clone());
        let driver = tokio::spawn(async move {
            driver_coord
                .submit_leaf(
                    &leaf(),
                    &driver_tx,
                    Arc::new(StickyCommitProbe {
                        key: b"uncertain".to_vec(),
                        tx: driver_tx.clone(),
                        evaluations: std::sync::atomic::AtomicUsize::new(0),
                        seen_in_doubt: driver_seen,
                    }),
                    Requirement::ANY,
                )
                .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        let (joiner_coord, joiner_tx) = (coord.clone(), retrying.clone());
        let joiner = tokio::spawn(async move {
            joiner_coord
                .submit_leaf(
                    &leaf(),
                    &joiner_tx,
                    Arc::new(AlwaysStageProbe {
                        key: b"retrying".to_vec(),
                        tx: joiner_tx.clone(),
                    }),
                    Requirement::ANY,
                )
                .await
        });
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        let out = driver.await.unwrap().unwrap();
        let retrying_outcome = joiner.await.unwrap().unwrap();
        coord.close().await;

        assert_eq!(*seen_in_doubt.lock().unwrap(), Some(true));
        assert!(
            matches!(
                out,
                Some(CoordinatedOutcome {
                    outcome: MemberOutcome::InDoubt(_),
                    ..
                })
            ),
            "exhaustion after an in-doubt CAS must preserve uncertainty"
        );
        assert!(matches!(
            retrying_outcome,
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::Moved,
                evidence: None,
                ..
            })
        ));
    }
}
